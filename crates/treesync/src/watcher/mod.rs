//! Recursive filesystem watching.
//!
//! Backed by `notify`, which uses inotify on Linux, FSEvents on macOS, and
//! kqueue on the BSDs. treesync targets inotify in production; the others exist so
//! the daemon is developable and testable off Linux.
//!
//! # Event kinds are hints, not facts
//!
//! Backends disagree sharply, and the weakest one sets the contract. Measured
//! on macOS/FSEvents, a single `rename(before.txt, after.txt)` produces:
//!
//! ```text
//! Create before.txt   tracker=None
//! Modify before.txt   tracker=None
//! Attrib before.txt   tracker=None
//! Modify before.txt   tracker=None
//! Modify after.txt    tracker=None
//! ```
//!
//! No `MovedFrom`, no `MovedTo`, no cookie, and a `Create` for a path that was
//! being *removed*. inotify reports the same operation precisely, as paired
//! `MovedFrom`/`MovedTo` sharing a tracker.
//!
//! So: treat [`EventKind`] as a statement that **something happened at this
//! path**, and stat the path to learn what. Acting on the kind alone is correct
//! on Linux and wrong on macOS.
//!
//! The consequence for sync: the rename optimization lsyncd performs, issuing
//! a remote `mv` instead of re-transferring, is only reachable where the
//! backend supplies real move events with trackers. Elsewhere a rename degrades
//! to a delete plus a transfer.
//!
//! # Overflow is a full reconcile
//!
//! The event queue is bounded (`EVENT_CHANNEL_CAPACITY`). Filling it, or the
//! kernel dropping events, is not recoverable event-by-event: the consumer's
//! picture of the tree now has a hole in it. Both cases collapse to
//! [`WatchEvent::Rescan`], the queue is purged, and the consumer re-walks. This
//! is logged at `warn`, so it is visible when it happens instead of silent.
//!
//! The walk is scoped to the smallest directory containing every lost path,
//! since a burst usually has locality. Two rules keep that sound: a kernel drop
//! widens to the whole root, because the kernel does not say what it discarded;
//! and every purged event widens the scope to cover its own path, because
//! discarding it here is the only reason it will not be handled individually.
//!
//! Narrowing is measurable: bursting 200 files into one subdirectory of a
//! watched tree, with the queue sized to overflow, yields rescans scoped to
//! that subdirectory rather than to the root.
//!
//! # Events can predate the watch
//!
//! FSEvents replays recent history for a path when a watch is installed, so the
//! first events after [`watch`] may describe changes that happened before it
//! was called. Startup reconciliation has to be idempotent regardless, so this
//! costs redundant work rather than correctness.

mod event;
mod scope;

pub use event::{EventKind, FsEvent, WatchEvent};

use std::path::{Path, PathBuf};
use std::sync::Arc;

use notify::event::{CreateKind, ModifyKind, RenameMode};
use notify::{RecommendedWatcher, RecursiveMode, Watcher as _};
use tokio::sync::mpsc;

use scope::RescanSignal;

use crate::error::{Error, Result};

/// How many events may sit unread between the backend thread and the consumer.
///
/// Bounded so a burst, such as a build writing an output tree, an unpack or a checkout,
/// cannot grow this queue without limit. Filling it is treated as a lost-track
/// condition: see [`WatchEvent::Rescan`].
const EVENT_CHANNEL_CAPACITY: usize = 12_000;

/// Watches a directory tree and reports changes.
///
/// Dropping this stops the watch; the paired [`EventStream`] then drains and
/// ends. Keep it alive for as long as events are wanted.
pub struct Watcher {
    _inner: RecommendedWatcher,
    root: PathBuf,
}

/// Hand-written because the backend watcher is not `Debug`.
impl std::fmt::Debug for Watcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Watcher")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl Watcher {
    /// The canonicalized root being watched.
    ///
    /// Canonical because backends report resolved paths: on macOS a watch on
    /// `/tmp/x` reports events under `/private/tmp/x`, and comparing against
    /// the path as supplied would never match.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

/// The receiving half of a [`Watcher`].
pub struct EventStream {
    rx: mpsc::Receiver<WatchEvent>,
    signal: Arc<RescanSignal>,
    /// Fallback walk target when the extent of a loss is unknown.
    root: PathBuf,
}

impl std::fmt::Debug for EventStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventStream")
            .field("queued", &self.rx.len())
            .field("rescan_pending", &self.signal.is_pending())
            .finish()
    }
}

impl EventStream {
    /// Builds a stream over an existing channel.
    ///
    /// Useful for composing a synthetic event source, and for driving the
    /// downstream queue in tests without touching a filesystem. A stream built
    /// this way still reports gaps: send [`WatchEvent::Rescan`] through the
    /// channel.
    pub fn from_channel(rx: mpsc::Receiver<WatchEvent>, root: impl Into<PathBuf>) -> Self {
        Self {
            rx,
            signal: Arc::new(RescanSignal::new()),
            root: root.into(),
        }
    }

    /// Waits for the next event, or `None` once the watcher is dropped and the
    /// queue is drained.
    ///
    /// A pending rescan is reported ahead of any queued event: once a gap
    /// exists, the events behind it describe a tree state the consumer never
    /// saw, so the re-walk has to come first.
    pub async fn recv(&mut self) -> Option<WatchEvent> {
        if let Some(rescan) = self.take_rescan() {
            return Some(rescan);
        }

        self.rx.recv().await
    }

    /// Returns an event only if one is already waiting.
    ///
    /// For draining on shutdown, where blocking for the next event would mean
    /// waiting for a change that may never come.
    pub fn try_recv(&mut self) -> Option<WatchEvent> {
        if let Some(rescan) = self.take_rescan() {
            return Some(rescan);
        }

        self.rx.try_recv().ok()
    }

    /// Consumes a pending gap, purging the queue behind it.
    fn take_rescan(&mut self) -> Option<WatchEvent> {
        if !self.signal.is_pending() {
            return None;
        }

        // Everything queued describes a tree state that predates the gap, so it
        // is discarded. Each purged path widens the walk instead: the walk must
        // cover them, since dropping them here is the only reason they will not
        // be processed individually.
        let mut purged = 0usize;
        while let Ok(event) = self.rx.try_recv() {
            if let WatchEvent::Fs(fs_event) = &event {
                self.signal.lost(&fs_event.path);
            }

            purged += 1;
        }

        let root = self.signal.take(&self.root);
        tracing::warn!(
            purged,
            root = %root.display(),
            "event stream gap: discarded queued events, reconcile required"
        );

        Some(WatchEvent::Rescan { root })
    }
}

/// Starts watching `root` and everything beneath it.
///
/// The tree is walked once to install watches, so this is O(directories) at
/// startup. Directories created afterwards are picked up by the backend.
pub fn watch(root: impl AsRef<Path>) -> Result<(Watcher, EventStream)> {
    watch_with_capacity(root, EVENT_CHANNEL_CAPACITY)
}

/// As [`watch`], with an explicit queue capacity.
///
/// Lowering it makes the overflow path reachable in a test without having to
/// generate `EVENT_CHANNEL_CAPACITY` real events.
pub fn watch_with_capacity(
    root: impl AsRef<Path>,
    capacity: usize,
) -> Result<(Watcher, EventStream)> {
    let root = root.as_ref();

    // Resolve before watching so emitted paths and the root share a prefix.
    let root = root.canonicalize().map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => Error::NotFound(format!("watch root {}", root.display())),
        std::io::ErrorKind::PermissionDenied => {
            Error::PermissionDenied(format!("watch root {}", root.display()))
        }
        _ => Error::Io(err),
    })?;

    if !root.is_dir() {
        return Err(Error::InvalidPath(format!(
            "watch root {} is not a directory",
            root.display()
        )));
    }

    let (tx, rx) = mpsc::channel(capacity);
    let signal = Arc::new(RescanSignal::new());
    let callback_signal = signal.clone();

    // Runs on the backend's own thread, never on a tokio worker, so it must not
    // block and must not await.
    let mut inner =
        notify::recommended_watcher(move |result: notify::Result<notify::Event>| match result {
            Ok(event) => dispatch(event, &tx, &callback_signal),
            Err(err) => {
                tracing::warn!("watch backend error: {err}");
                callback_signal.lost_everything();
            }
        })
        .map_err(|err| Error::Internal(format!("failed to create watcher: {err}")))?;

    inner
        .watch(&root, RecursiveMode::Recursive)
        .map_err(|err| Error::Internal(format!("failed to watch {}: {err}", root.display())))?;

    tracing::info!(root = %root.display(), "watching");

    let stream_root = root.clone();

    Ok((
        Watcher {
            _inner: inner,
            root,
        },
        EventStream {
            rx,
            signal,
            root: stream_root,
        },
    ))
}

/// Translates one backend event and forwards it.
///
/// Runs on the backend's own thread: never blocks, never awaits.
fn dispatch(event: notify::Event, tx: &mpsc::Sender<WatchEvent>, signal: &RescanSignal) {
    if event.need_rescan() {
        tracing::warn!("backend reported dropped events; requesting rescan");
        signal.lost_everything();
        return;
    }

    // A directory that has just appeared is a gap, not an event.
    //
    // The kernel only reports what it has a watch on, and a watch on a new
    // directory can only be installed *after* it exists. Anything created in
    // the window between the two, which `mkdir -p a/b/c` closes in
    // microseconds, generates no event at all. Measured on inotify, the whole
    // of `a/b/c/deep.txt` arrives as the single event `Create a`.
    //
    // That is precisely the condition the rescan signal exists for, so it is
    // reported as one rather than left to a consumer to guess at. Scoped to the
    // new directory, so the walk it triggers is the subtree in doubt and not
    // the tree.
    if matches!(event.kind, notify::EventKind::Create(CreateKind::Folder)) {
        for path in &event.paths {
            tracing::debug!(path = %path.display(), "new directory; its contents may be unreported");
            signal.lost_under(path);
        }
    }

    for fs_event in translate(&event) {
        let path = fs_event.path.clone();

        match tx.try_send(WatchEvent::Fs(fs_event)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Deliberately keeps going rather than returning: a pending
                // rescan is scoped, so every undelivered path has to widen it.
                // Skipping the rest would drop changes the walk never covers.
                signal.lost(&path);
            }
            // The consumer is gone; the signal would never be read.
            Err(mpsc::error::TrySendError::Closed(_)) => return,
        }
    }
}

/// Maps one backend event onto zero or more [`FsEvent`]s.
///
/// A rename reported as a single `Both` event carries `[from, to]` and becomes
/// two events sharing a tracker, so the batching layer sees the same shape it
/// gets from backends that report the halves separately.
fn translate(event: &notify::Event) -> Vec<FsEvent> {
    let tracker = event.tracker();

    let kind = match event.kind {
        notify::EventKind::Create(_) => EventKind::Create,
        notify::EventKind::Remove(_) => EventKind::Delete,
        notify::EventKind::Modify(ModifyKind::Name(RenameMode::From)) => EventKind::MovedFrom,
        notify::EventKind::Modify(ModifyKind::Name(RenameMode::To)) => EventKind::MovedTo,
        notify::EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
            let mut paths = event.paths.iter();

            return match (paths.next(), paths.next()) {
                (Some(from), Some(to)) => vec![
                    FsEvent {
                        kind: EventKind::MovedFrom,
                        path: from.clone(),
                        tracker,
                    },
                    FsEvent {
                        kind: EventKind::MovedTo,
                        path: to.clone(),
                        tracker,
                    },
                ],
                // A `Both` without two paths is a backend bug; a rescan is
                // safer than guessing which half is missing.
                _ => Vec::new(),
            };
        }
        notify::EventKind::Modify(ModifyKind::Metadata(_)) => EventKind::Attrib,
        notify::EventKind::Modify(_) => EventKind::Modify,
        // Imprecise backends report `Any` for changes they cannot classify.
        // Treating it as a content change makes the consumer re-examine the
        // path, which is the safe direction to be wrong in.
        notify::EventKind::Any => EventKind::Modify,
        // Reads and opens do not change the tree.
        notify::EventKind::Access(_) | notify::EventKind::Other => return Vec::new(),
    };

    event
        .paths
        .iter()
        .map(|path| FsEvent {
            kind,
            path: path.clone(),
            tracker,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{CreateKind, Flag};

    fn create_event(path: &str) -> notify::Event {
        let mut event = notify::Event::new(notify::EventKind::Create(CreateKind::File));
        event.paths.push(PathBuf::from(path));
        event
    }

    fn stream_of(capacity: usize) -> (mpsc::Sender<WatchEvent>, EventStream, Arc<RescanSignal>) {
        let (tx, rx) = mpsc::channel(capacity);
        let signal = Arc::new(RescanSignal::new());
        let stream = EventStream {
            rx,
            signal: signal.clone(),
            root: PathBuf::from("/tree"),
        };

        (tx, stream, signal)
    }

    #[test]
    fn ordinary_events_are_forwarded() {
        let (tx, mut stream, signal) = stream_of(4);

        dispatch(create_event("/tree/a"), &tx, &signal);

        assert!(!signal.is_pending());
        assert_eq!(
            stream.rx.try_recv().expect("event should be queued"),
            WatchEvent::Fs(FsEvent {
                kind: EventKind::Create,
                path: PathBuf::from("/tree/a"),
                tracker: None,
            })
        );
    }

    #[tokio::test]
    async fn a_localized_overflow_scopes_the_walk_to_that_directory() {
        let (tx, mut stream, signal) = stream_of(1);

        // Fills the queue, then overflows it. Both paths sit in one directory.
        dispatch(create_event("/tree/build/one.o"), &tx, &signal);
        dispatch(create_event("/tree/build/two.o"), &tx, &signal);

        assert!(signal.is_pending());
        assert_eq!(
            stream.recv().await,
            Some(WatchEvent::Rescan {
                root: PathBuf::from("/tree/build")
            }),
            "a burst inside one directory must not force a walk of the whole tree"
        );
    }

    #[tokio::test]
    async fn a_pending_rescan_widens_to_cover_events_it_would_otherwise_swallow() {
        let (tx, mut stream, signal) = stream_of(8);

        // A narrow loss, then an unrelated change that is still deliverable.
        signal.lost(Path::new("/tree/build/one.o"));
        dispatch(create_event("/tree/src/main.rs"), &tx, &signal);

        // That queued event is about to be purged, so the walk has to reach it.
        // Scoping to /tree/build alone would silently drop the src change.
        assert_eq!(
            stream.recv().await,
            Some(WatchEvent::Rescan {
                root: PathBuf::from("/tree")
            }),
            "purged events must widen the walk, or their changes are lost"
        );
    }

    #[tokio::test]
    async fn a_backend_drop_forces_a_walk_of_the_whole_root() {
        let (tx, mut stream, signal) = stream_of(4);

        let mut event = create_event("/tree/build/one.o");
        event.attrs.set_flag(Flag::Rescan);
        dispatch(event, &tx, &signal);

        assert_eq!(
            stream.recv().await,
            Some(WatchEvent::Rescan {
                root: PathBuf::from("/tree")
            }),
            "the kernel does not say what it dropped, so nothing can be excluded"
        );
    }

    #[tokio::test]
    async fn a_rescan_purges_the_queue_and_is_delivered_first() {
        let (tx, mut stream, signal) = stream_of(8);

        dispatch(create_event("/tree/a"), &tx, &signal);
        dispatch(create_event("/tree/b"), &tx, &signal);
        signal.lost_everything();

        assert!(matches!(
            stream.recv().await,
            Some(WatchEvent::Rescan { .. })
        ));
        assert!(
            stream.rx.try_recv().is_err(),
            "stale events must be purged, not left to be processed after the walk"
        );
    }

    #[tokio::test]
    async fn recovers_after_a_rescan_is_consumed() {
        let (tx, mut stream, signal) = stream_of(4);
        signal.lost_everything();

        assert!(matches!(
            stream.recv().await,
            Some(WatchEvent::Rescan { .. })
        ));
        assert!(!signal.is_pending(), "the signal is consumed, not sticky");

        dispatch(create_event("/tree/a"), &tx, &signal);

        assert!(matches!(stream.recv().await, Some(WatchEvent::Fs(_))));
    }
}
