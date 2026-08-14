//! Collapses the watcher's event stream into periodic batches of work.
//!
//! The watcher reports every change the kernel sees. Acting on each one
//! individually is ruinous: a build writing an output tree produces thousands
//! of events across hundreds of paths, most of them redundant. This layer
//! accumulates events over a window and emits the distinct paths that need
//! reconciling.
//!
//! # Why this is simpler than lsyncd's combine matrix
//!
//! lsyncd resolves each new event against every pending one through a table of
//! `absorb` / `replace` / `stack` rules, with extra tiers for moves and for
//! paths that are prefixes of one another. It needs that because it hands the
//! resulting path list to `rsync` and the ordering of operations matters.
//!
//! treesync stats each path when the batch is flushed, so the batch only has to
//! answer *which paths are suspect*, not *what sequence of things happened to
//! them*. That collapses the matrix to last-writer-wins:
//!
//! - `Create` then `Delete`: lsyncd `replace`; here the delete wins, same thing.
//! - `Delete` then `Create`: lsyncd `replace`; here the create wins, same thing.
//! - `Create` then `Modify`: lsyncd `absorb`; here both mean only "this path
//!   is suspect", so they are already the same value.
//!
//! Ordering within a batch also stops mattering. `rm -rf dir && mkdir dir &&
//! touch dir/f` leaves `dir` and `dir/f` in the pending set; at flush time both
//! exist on disk and both get upserted, without the queue having tracked the
//! sequence. This falls out of reconciling against observed state rather than
//! replaying a log, the same reason a dropped event costs a re-walk instead of
//! corrupting the target.
//!
//! It is also the only design that survives the backend differences measured in
//! [`crate::watcher`]: on FSEvents a removal arrives labelled `Create`, so any
//! scheme that trusted the event kind would be wrong there. The batch therefore
//! reports *which paths are suspect* and nothing more. The reconciler stats
//! them, and the filesystem is the authority.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use tokio::time::Instant;

use crate::watcher::{EventKind, EventStream, WatchEvent};

/// How long to accumulate events before emitting a batch.
///
/// Shorter than lsyncd's 15s default: that number is sized for forking `rsync`
/// on every flush, and an in-process transfer makes frequent flushes cheap.
const DEFAULT_DELAY: Duration = Duration::from_secs(1);

/// How many distinct paths may accumulate before the window is cut short.
///
/// Bounds both the memory a batch holds and how long a single flush can take.
/// lsyncd's equivalent (`maxDelays`) is 1000, sized around the cost of its
/// n·log(n) collapse; a hash set has no such pressure.
const DEFAULT_MAX_PENDING: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueConfig {
    /// Length of the batching window, measured from the first event after a flush.
    pub delay: Duration,
    /// Distinct-path count that forces an early flush.
    pub max_pending: usize,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            delay: DEFAULT_DELAY,
            max_pending: DEFAULT_MAX_PENDING,
        }
    }
}

/// A rename observed within the window, with both halves matched.
///
/// Purely an optimization: the target can rename in place instead of
/// transferring `to` and deleting `from`, which for a renamed directory is the
/// difference between one operation and re-sending the subtree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rename {
    pub from: PathBuf,
    pub to: PathBuf,
}

/// The work accumulated during one window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Changes {
    /// Distinct paths whose current state must be examined. Never empty.
    ///
    /// No claim is made about *what* happened to them, so stat each one. See the
    /// module docs for why the event kind is not carried here.
    pub paths: Vec<PathBuf>,

    /// Renames whose halves were both seen in this window.
    ///
    /// Both endpoints of every rename also appear in `paths`, so a reconciler
    /// that ignores this field stays correct, since it re-transfers instead of
    /// renaming. Acting on it is an optimization, and one that is only
    /// available where the backend supplies rename cookies: inotify does,
    /// FSEvents does not, so on macOS this is always empty.
    pub renames: Vec<Rename>,
}

/// A unit of work for the reconciler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Batch {
    /// Paths that changed during the window.
    Changes(Changes),
    /// The event stream had a gap. Walk `root` and reconcile it; nothing
    /// incremental can be trusted for that subtree.
    Rescan { root: PathBuf },
}

/// One half of a rename, waiting for its partner.
#[derive(Debug)]
enum HalfMove {
    From(PathBuf),
    To(PathBuf),
}

/// Batches watcher events into units of work.
#[derive(Debug)]
pub struct EventQueue {
    stream: EventStream,
    config: QueueConfig,
    pending: HashSet<PathBuf>,
    /// Rename halves seen so far this window, keyed by the backend's cookie.
    half_moves: HashMap<usize, HalfMove>,
    renames: Vec<Rename>,
}

impl EventQueue {
    pub fn new(stream: EventStream, config: QueueConfig) -> Self {
        Self {
            stream,
            config,
            pending: HashSet::new(),
            half_moves: HashMap::new(),
            renames: Vec::new(),
        }
    }

    /// Waits for the next batch, or `None` once the watcher has stopped and
    /// everything accumulated has been emitted.
    ///
    /// The window opens on the first event after a flush rather than resetting
    /// with each event: a debounce would never fire under continuous change,
    /// which is exactly when a sync daemon most needs to make progress.
    pub async fn next_batch(&mut self) -> Option<Batch> {
        // Idle: nothing is pending, so wait indefinitely for something to do.
        // The window does not start until there is work in it.
        match self.stream.recv().await {
            None => return None,
            Some(WatchEvent::Rescan { root }) => return Some(self.take_rescan(root)),
            Some(event) => self.merge(event),
        }

        let deadline = Instant::now() + self.config.delay;

        while self.pending.len() < self.config.max_pending {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                event = self.stream.recv() => match event {
                    // Watcher stopped. Emit what was accumulated; the next call
                    // reports the end of the stream.
                    None => break,
                    Some(WatchEvent::Rescan { root }) => return Some(self.take_rescan(root)),
                    Some(event) => self.merge(event),
                },
            }
        }

        Some(Batch::Changes(self.flush()))
    }

    /// Emits whatever has already been observed, without waiting out the window.
    ///
    /// Returns `None` when nothing is pending, so a caller draining on shutdown
    /// stops immediately rather than sitting out a window that has nothing to
    /// batch.
    pub fn drain(&mut self) -> Option<Batch> {
        while let Some(event) = self.stream.try_recv() {
            if let WatchEvent::Rescan { root } = event {
                return Some(self.take_rescan(root));
            }

            self.merge(event);
        }

        if self.pending.is_empty() {
            return None;
        }

        Some(Batch::Changes(self.flush()))
    }

    /// Records an event.
    fn merge(&mut self, event: WatchEvent) {
        let WatchEvent::Fs(event) = event else {
            return;
        };

        // Every event makes its path suspect, including both halves of a
        // rename, so that ignoring `renames` is always safe.
        self.pending.insert(event.path.clone());

        let Some(cookie) = event.tracker else {
            // No cookie, so the halves cannot be correlated. The paths are
            // already recorded; only the rename optimization is lost.
            return;
        };

        match event.kind {
            EventKind::MovedFrom => match self.half_moves.remove(&cookie) {
                Some(HalfMove::To(to)) => self.renames.push(Rename {
                    from: event.path,
                    to,
                }),
                _ => {
                    self.half_moves.insert(cookie, HalfMove::From(event.path));
                }
            },
            EventKind::MovedTo => match self.half_moves.remove(&cookie) {
                Some(HalfMove::From(from)) => self.renames.push(Rename {
                    from,
                    to: event.path,
                }),
                _ => {
                    self.half_moves.insert(cookie, HalfMove::To(event.path));
                }
            },
            _ => {}
        }
    }

    /// Empties the window into a batch.
    fn flush(&mut self) -> Changes {
        let paths: Vec<PathBuf> = self.pending.drain().collect();
        let renames = std::mem::take(&mut self.renames);

        // Halves still unmatched had their partner fall outside this window, or
        // outside the watched tree entirely. Dropping them costs only the
        // optimization: both paths are already in `paths`. Clearing also keeps
        // this map bounded by one window rather than by uptime.
        self.half_moves.clear();

        tracing::debug!(
            paths = paths.len(),
            renames = renames.len(),
            "flushing batch"
        );

        Changes { paths, renames }
    }

    /// Discards accumulated work and reports that a subtree must be re-walked.
    fn take_rescan(&mut self, root: PathBuf) -> Batch {
        // The walk observes the tree as it is now, which subsumes every path
        // waiting here.
        self.pending.clear();
        self.half_moves.clear();
        self.renames.clear();

        Batch::Rescan { root }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watcher::FsEvent;
    use tokio::sync::mpsc;

    /// Tests run on a paused clock, so `delay` elapses only when a test
    /// advances time. Nothing here sleeps in wall-clock terms.
    pub(super) fn queue(max_pending: usize) -> (mpsc::Sender<WatchEvent>, EventQueue) {
        let (tx, rx) = mpsc::channel(64);
        let config = QueueConfig {
            delay: Duration::from_secs(1),
            max_pending,
        };

        (
            tx,
            EventQueue::new(EventStream::from_channel(rx, "/tree"), config),
        )
    }

    pub(super) async fn send(tx: &mpsc::Sender<WatchEvent>, kind: EventKind, path: &str) {
        send_tracked(tx, kind, path, None).await;
    }

    async fn send_tracked(
        tx: &mpsc::Sender<WatchEvent>,
        kind: EventKind,
        path: &str,
        tracker: Option<usize>,
    ) {
        tx.send(WatchEvent::Fs(FsEvent {
            kind,
            path: PathBuf::from(path),
            tracker,
        }))
        .await
        .expect("queue should still be listening");
    }

    fn changes(batch: Batch) -> Changes {
        match batch {
            Batch::Changes(mut changes) => {
                changes.paths.sort();
                changes
            }
            Batch::Rescan { root } => {
                panic!("expected changes, got a rescan of {}", root.display())
            }
        }
    }

    fn paths(batch: Batch) -> Vec<PathBuf> {
        changes(batch).paths
    }

    #[tokio::test(start_paused = true)]
    async fn collapses_repeated_events_on_one_path() {
        let (tx, mut queue) = queue(100);

        for _ in 0..50 {
            send(&tx, EventKind::Modify, "/tree/a").await;
        }

        assert_eq!(
            paths(queue.next_batch().await.expect("batch")),
            vec![PathBuf::from("/tree/a")],
            "50 events on one path is one unit of work"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn collapses_differing_kinds_on_one_path() {
        let (tx, mut queue) = queue(100);

        // The kind is not carried, so a create, a write and a delete on one
        // path are indistinguishable here, deliberately.
        send(&tx, EventKind::Create, "/tree/a").await;
        send(&tx, EventKind::Modify, "/tree/a").await;
        send(&tx, EventKind::Delete, "/tree/a").await;

        assert_eq!(
            paths(queue.next_batch().await.expect("batch")),
            vec![PathBuf::from("/tree/a")]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn keeps_distinct_paths_apart() {
        let (tx, mut queue) = queue(100);

        send(&tx, EventKind::Create, "/tree/a").await;
        send(&tx, EventKind::Delete, "/tree/b").await;

        assert_eq!(
            paths(queue.next_batch().await.expect("batch")),
            vec![PathBuf::from("/tree/a"), PathBuf::from("/tree/b")]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pairs_a_rename_by_cookie() {
        let (tx, mut queue) = queue(100);

        send_tracked(&tx, EventKind::MovedFrom, "/tree/before", Some(7)).await;
        send_tracked(&tx, EventKind::MovedTo, "/tree/after", Some(7)).await;

        let changes = changes(queue.next_batch().await.expect("batch"));

        assert_eq!(
            changes.renames,
            vec![Rename {
                from: PathBuf::from("/tree/before"),
                to: PathBuf::from("/tree/after"),
            }]
        );
        assert_eq!(
            changes.paths,
            vec![PathBuf::from("/tree/after"), PathBuf::from("/tree/before")],
            "both endpoints stay in paths so ignoring renames is still correct"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn pairs_a_rename_whose_halves_arrive_reversed() {
        let (tx, mut queue) = queue(100);

        send_tracked(&tx, EventKind::MovedTo, "/tree/after", Some(7)).await;
        send_tracked(&tx, EventKind::MovedFrom, "/tree/before", Some(7)).await;

        assert_eq!(
            changes(queue.next_batch().await.expect("batch")).renames,
            vec![Rename {
                from: PathBuf::from("/tree/before"),
                to: PathBuf::from("/tree/after"),
            }]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn keeps_concurrent_renames_separate() {
        let (tx, mut queue) = queue(100);

        send_tracked(&tx, EventKind::MovedFrom, "/tree/a1", Some(1)).await;
        send_tracked(&tx, EventKind::MovedFrom, "/tree/b1", Some(2)).await;
        send_tracked(&tx, EventKind::MovedTo, "/tree/b2", Some(2)).await;
        send_tracked(&tx, EventKind::MovedTo, "/tree/a2", Some(1)).await;

        let mut renames = changes(queue.next_batch().await.expect("batch")).renames;
        renames.sort_by(|a, b| a.from.cmp(&b.from));

        assert_eq!(
            renames,
            vec![
                Rename {
                    from: PathBuf::from("/tree/a1"),
                    to: PathBuf::from("/tree/a2"),
                },
                Rename {
                    from: PathBuf::from("/tree/b1"),
                    to: PathBuf::from("/tree/b2"),
                },
            ],
            "interleaved renames must not cross-pair"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_move_out_of_the_tree_is_not_a_rename() {
        let (tx, mut queue) = queue(100);

        // The partner half never arrives: the destination is outside the watch.
        send_tracked(&tx, EventKind::MovedFrom, "/tree/gone", Some(7)).await;

        let changes = changes(queue.next_batch().await.expect("batch"));

        assert!(changes.renames.is_empty());
        assert_eq!(changes.paths, vec![PathBuf::from("/tree/gone")]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_move_without_a_cookie_is_not_a_rename() {
        let (tx, mut queue) = queue(100);

        // FSEvents supplies no cookies, so pairing is impossible there.
        send(&tx, EventKind::MovedFrom, "/tree/before").await;
        send(&tx, EventKind::MovedTo, "/tree/after").await;

        let changes = changes(queue.next_batch().await.expect("batch"));

        assert!(
            changes.renames.is_empty(),
            "without a cookie these two events cannot be known to be one rename"
        );
        assert_eq!(changes.paths.len(), 2, "both paths are still reported");
    }

    #[tokio::test(start_paused = true)]
    async fn an_unmatched_half_does_not_leak_into_the_next_window() {
        let (tx, mut queue) = queue(100);

        send_tracked(&tx, EventKind::MovedFrom, "/tree/before", Some(7)).await;
        assert!(
            changes(queue.next_batch().await.expect("batch"))
                .renames
                .is_empty()
        );

        // Same cookie, next window. Cookies are reused by the kernel, so
        // pairing across a flush would fabricate a rename between two paths
        // that were never related.
        send_tracked(&tx, EventKind::MovedTo, "/tree/unrelated", Some(7)).await;

        assert!(
            changes(queue.next_batch().await.expect("batch"))
                .renames
                .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_window_holds_events_until_the_delay_elapses() {
        let (tx, mut queue) = queue(100);
        send(&tx, EventKind::Create, "/tree/a").await;

        let mut batch = Box::pin(queue.next_batch());

        tokio::time::advance(Duration::from_millis(600)).await;
        assert!(
            futures::poll!(&mut batch).is_pending(),
            "batch must not flush before the window closes"
        );

        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(batch.await.is_some(), "batch must flush once it does");
    }

    #[tokio::test(start_paused = true)]
    async fn reaching_max_pending_cuts_the_window_short() {
        let (tx, mut queue) = queue(3);

        send(&tx, EventKind::Create, "/tree/a").await;
        send(&tx, EventKind::Create, "/tree/b").await;
        send(&tx, EventKind::Create, "/tree/c").await;

        // No time is advanced: this can only return by hitting the cap.
        assert_eq!(paths(queue.next_batch().await.expect("batch")).len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_rescan_carries_its_scope_and_discards_pending_work() {
        let (tx, mut queue) = queue(100);

        send(&tx, EventKind::Create, "/tree/a").await;
        send_tracked(&tx, EventKind::MovedFrom, "/tree/b", Some(7)).await;
        tx.send(WatchEvent::Rescan {
            root: PathBuf::from("/tree/build"),
        })
        .await
        .expect("send");

        assert_eq!(
            queue.next_batch().await,
            Some(Batch::Rescan {
                root: PathBuf::from("/tree/build")
            }),
            "the scope must reach the reconciler intact"
        );

        // Nothing discarded may resurface, including the dangling rename half.
        send_tracked(&tx, EventKind::MovedTo, "/tree/c", Some(7)).await;
        let changes = changes(queue.next_batch().await.expect("batch"));

        assert_eq!(changes.paths, vec![PathBuf::from("/tree/c")]);
        assert!(changes.renames.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn flushes_what_it_has_when_the_watcher_stops() {
        let (tx, mut queue) = queue(100);

        send(&tx, EventKind::Create, "/tree/a").await;
        drop(tx);

        assert_eq!(
            paths(queue.next_batch().await.expect("pending work is not lost")).len(),
            1
        );
        assert_eq!(queue.next_batch().await, None, "then the stream ends");
    }

    #[tokio::test(start_paused = true)]
    async fn ends_when_the_watcher_stops_with_nothing_pending() {
        let (tx, mut queue) = queue(100);
        drop(tx);

        assert_eq!(queue.next_batch().await, None);
    }
}

#[cfg(test)]
mod drain_tests {
    use super::tests::{queue, send};
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn draining_an_idle_queue_yields_nothing() {
        let (_tx, mut queue) = queue(100);

        assert_eq!(
            queue.drain(),
            None,
            "a caller shutting down must not be made to wait out a window with nothing in it"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn draining_emits_what_was_observed_without_waiting() {
        let (tx, mut queue) = queue(100);
        send(&tx, EventKind::Create, "/tree/a").await;
        send(&tx, EventKind::Modify, "/tree/b").await;

        // No time is advanced: the window has not closed and must not need to.
        let batch = queue.drain().expect("pending work should be emitted");

        match batch {
            Batch::Changes(changes) => assert_eq!(changes.paths.len(), 2),
            Batch::Rescan { .. } => panic!("expected changes"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn draining_twice_yields_nothing_the_second_time() {
        let (tx, mut queue) = queue(100);
        send(&tx, EventKind::Create, "/tree/a").await;

        assert!(queue.drain().is_some());
        assert_eq!(queue.drain(), None, "the batch must not be emitted twice");
    }

    #[tokio::test(start_paused = true)]
    async fn draining_surfaces_a_pending_gap() {
        let (tx, mut queue) = queue(100);
        send(&tx, EventKind::Create, "/tree/a").await;
        tx.send(WatchEvent::Rescan {
            root: PathBuf::from("/tree/build"),
        })
        .await
        .expect("send");

        assert_eq!(
            queue.drain(),
            Some(Batch::Rescan {
                root: PathBuf::from("/tree/build")
            })
        );
    }
}
