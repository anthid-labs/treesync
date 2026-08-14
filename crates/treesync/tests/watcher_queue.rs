//! Drives the real watcher into the real queue.
//!
//! The unit tests either side of this seam use synthetic events and a paused
//! clock. This one uses an actual filesystem and an actual timer, so it is the
//! only place the two layers are proven to fit together.

use std::path::Path;
use std::time::Duration;

use tempfile::TempDir;
use tokio::time::timeout;
use treesync::queue::{Batch, EventQueue, QueueConfig};
use treesync::watcher::{watch, watch_with_capacity};

const BUDGET: Duration = Duration::from_secs(10);

/// Collects batches until `predicate` is satisfied.
///
/// Backends replay pre-watch history and coalesce aggressively, so a batch may
/// carry unrelated paths and the interesting one may not land in the first
/// batch. Assertions are therefore about a path eventually appearing, never
/// about a batch containing exactly one thing.
async fn wait_for_batch(queue: &mut EventQueue, predicate: impl Fn(&Batch) -> bool) -> Batch {
    timeout(BUDGET, async {
        loop {
            match queue.next_batch().await {
                Some(batch) if predicate(&batch) => return batch,
                Some(_) => continue,
                None => panic!("queue ended before a matching batch arrived"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no matching batch within {BUDGET:?}"))
}

fn contains(batch: &Batch, path: &Path) -> bool {
    match batch {
        Batch::Changes(changes) => changes.paths.iter().any(|changed| changed == path),
        Batch::Rescan { .. } => false,
    }
}

fn short_window() -> QueueConfig {
    QueueConfig {
        delay: Duration::from_millis(200),
        max_pending: 10_000,
    }
}

#[tokio::test]
async fn a_new_file_reaches_the_queue() {
    let dir = TempDir::new().expect("temp dir");
    let (_watcher, stream) = watch(dir.path()).expect("watch");
    let mut queue = EventQueue::new(stream, short_window());

    let root = dir.path().canonicalize().expect("canonicalize");
    let file = root.join("created.txt");
    std::fs::write(&file, b"hello").expect("write");

    wait_for_batch(&mut queue, |batch| contains(batch, &file)).await;
}

#[tokio::test]
async fn a_removed_file_reaches_the_queue() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonicalize");
    let file = root.join("doomed.txt");
    std::fs::write(&file, b"hello").expect("write");

    let (_watcher, stream) = watch(dir.path()).expect("watch");
    let mut queue = EventQueue::new(stream, short_window());

    std::fs::remove_file(&file).expect("remove");

    // Only the path is asserted, because only the path is trustworthy: measured
    // on macOS/FSEvents this removal arrives labelled as a creation. Dropping
    // the kind from the batch is what makes that difference stop mattering.
    wait_for_batch(&mut queue, |batch| contains(batch, &file)).await;
}

#[tokio::test]
async fn many_writes_to_one_file_collapse_to_a_single_change() {
    let dir = TempDir::new().expect("temp dir");
    let (_watcher, stream) = watch(dir.path()).expect("watch");
    let mut queue = EventQueue::new(stream, short_window());

    let root = dir.path().canonicalize().expect("canonicalize");
    let file = root.join("busy.txt");

    // The case the queue exists for: a file rewritten repeatedly must not
    // become a hundred units of work.
    for i in 0..100 {
        std::fs::write(&file, format!("write {i}").as_bytes()).expect("write");
    }

    let batch = wait_for_batch(&mut queue, |batch| contains(batch, &file)).await;

    let Batch::Changes(changes) = batch else {
        unreachable!("predicate matched Changes");
    };

    let occurrences = changes.paths.iter().filter(|path| **path == file).count();
    assert_eq!(
        occurrences, 1,
        "one path must appear at most once in a batch, got {occurrences}"
    );
}

#[tokio::test]
async fn a_rename_reports_both_endpoints() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonicalize");
    let from = root.join("before.txt");
    let to = root.join("after.txt");
    std::fs::write(&from, b"hello").expect("write");

    let (_watcher, stream) = watch(dir.path()).expect("watch");
    let mut queue = EventQueue::new(stream, short_window());

    std::fs::rename(&from, &to).expect("rename");

    let batch = wait_for_batch(&mut queue, |batch| contains(batch, &to)).await;

    let Batch::Changes(changes) = batch else {
        unreachable!("predicate matched Changes");
    };

    // Pairing needs rename cookies: inotify supplies them, FSEvents does not,
    // so this list is populated on Linux and empty on macOS. What must hold on
    // both is that a reported rename names the right endpoints. A wrong pair
    // would make the target rename the wrong file.
    for rename in &changes.renames {
        assert_eq!(rename.from, from);
        assert_eq!(rename.to, to);
    }
}

#[tokio::test]
async fn overflowing_the_queue_produces_a_rescan_scoped_inside_the_tree() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonicalize");
    let build = root.join("build");
    std::fs::create_dir(&build).expect("create build dir");

    // A capacity this small guarantees the burst below overflows it.
    let (_watcher, stream) = watch_with_capacity(dir.path(), 2).expect("watch");
    let mut queue = EventQueue::new(stream, short_window());

    for i in 0..200 {
        std::fs::write(build.join(format!("{i}.o")), b"x").expect("write");
    }

    let batch = wait_for_batch(&mut queue, |batch| matches!(batch, Batch::Rescan { .. })).await;

    let Batch::Rescan { root: scope } = batch else {
        unreachable!("predicate matched Rescan");
    };

    // The walk target must never escape the watch, whatever the backend
    // reported: a rescan pointed outside the tree would walk unrelated
    // directories.
    assert!(
        scope.starts_with(&root),
        "rescan scope {} escaped the watch root {}",
        scope.display(),
        root.display()
    );
}
