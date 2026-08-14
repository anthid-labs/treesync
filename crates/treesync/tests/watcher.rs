//! Exercises the watcher against a real filesystem.
//!
//! These are timing-sensitive by nature: backends coalesce, reorder, and delay.
//! Assertions are therefore "an event of this kind arrived for this path within
//! the budget", never "exactly these events arrived in this order".

use std::path::Path;
use std::time::Duration;

use tempfile::TempDir;
use tokio::time::timeout;
use treesync::watcher::{EventKind, WatchEvent, watch};

/// Generous: FSEvents on macOS batches with a latency window, and CI is slower
/// than a laptop.
const BUDGET: Duration = Duration::from_secs(10);

/// Drains events until one matches, or the budget expires.
async fn wait_for(
    stream: &mut treesync::watcher::EventStream,
    predicate: impl Fn(&WatchEvent) -> bool,
) -> WatchEvent {
    let found = timeout(BUDGET, async {
        loop {
            match stream.recv().await {
                Some(event) if predicate(&event) => return Some(event),
                Some(_) => continue,
                None => return None,
            }
        }
    })
    .await;

    match found {
        Ok(Some(event)) => event,
        Ok(None) => panic!("watcher stream ended before a matching event arrived"),
        Err(_) => panic!("no matching event within {BUDGET:?}"),
    }
}

fn is_kind_for(event: &WatchEvent, kind: EventKind, path: &Path) -> bool {
    match event {
        WatchEvent::Fs(fs) => fs.kind == kind && fs.path == path,
        WatchEvent::Rescan { .. } => false,
    }
}

/// Whether a rescan would re-walk `path`.
///
/// A gap the backend cannot describe event by event is still covered as long as
/// the walk it asks for reaches the change.
fn rescan_covering(event: &WatchEvent, path: &Path) -> bool {
    match event {
        WatchEvent::Rescan { root } => path.starts_with(root),
        WatchEvent::Fs(_) => false,
    }
}

#[tokio::test]
async fn reports_file_creation() {
    let dir = TempDir::new().expect("temp dir");
    let (_watcher, mut stream) = watch(dir.path()).expect("watch");

    // Canonicalized: on macOS the temp dir is under /var, a symlink to /private/var.
    let root = dir.path().canonicalize().expect("canonicalize");
    let file = root.join("created.txt");

    std::fs::write(&file, b"hello").expect("write");

    wait_for(&mut stream, |event| {
        is_kind_for(event, EventKind::Create, &file)
    })
    .await;
}

#[tokio::test]
async fn reports_file_deletion() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonicalize");
    let file = root.join("doomed.txt");
    std::fs::write(&file, b"hello").expect("write");

    let (_watcher, mut stream) = watch(dir.path()).expect("watch");

    std::fs::remove_file(&file).expect("remove");

    wait_for(&mut stream, |event| {
        is_kind_for(event, EventKind::Delete, &file)
    })
    .await;
}

#[tokio::test]
async fn reports_changes_in_subdirectories_created_after_the_watch_started() {
    let dir = TempDir::new().expect("temp dir");
    let (_watcher, mut stream) = watch(dir.path()).expect("watch");

    let root = dir.path().canonicalize().expect("canonicalize");
    let nested = root.join("a").join("b");
    std::fs::create_dir_all(&nested).expect("create nested dirs");

    let file = nested.join("deep.txt");
    std::fs::write(&file, b"hello").expect("write");

    // `a/b` did not exist when the watch was installed, and a watch on a new
    // directory can only be installed once it exists, so anything created in
    // between generates no event at all. `mkdir -p` plus an immediate write
    // closes that window in microseconds, and measured on inotify the entire
    // subtree arrives as the single event `Create a`.
    //
    // What the watcher owes is therefore not an event per path, which the
    // kernel cannot supply, but never to lose the subtree *silently*: either
    // the change is reported directly, or a rescan covering it is. That is the
    // contract the rest of the design leans on: notification is an
    // optimization, reconciliation is the source of truth.
    let event = wait_for(&mut stream, |event| {
        is_kind_for(event, EventKind::Create, &file) || rescan_covering(event, &file)
    })
    .await;

    if let WatchEvent::Rescan { root } = &event {
        assert!(
            file.starts_with(root),
            "a rescan has to cover the change it stands in for; {} is not under {}",
            file.display(),
            root.display()
        );
    }
}

#[tokio::test]
async fn reports_renames_within_the_tree() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonicalize");
    let from = root.join("before.txt");
    let to = root.join("after.txt");
    std::fs::write(&from, b"hello").expect("write");

    let (_watcher, mut stream) = watch(dir.path()).expect("watch");

    std::fs::rename(&from, &to).expect("rename");

    // Asserts only on the destination: it did not exist before the rename, so
    // any event naming it was necessarily caused by the rename. The source path
    // is also reported, but it already had events from its initial write, so an
    // assertion there would not prove the rename produced anything.
    //
    // The kind is deliberately not pinned. inotify reports a rename as paired
    // MovedFrom/MovedTo with a shared cookie; FSEvents reports the destination
    // as a plain Modify and supplies no cookie at all. Both are correct
    // observations, and the sync engine only needs to know the path is dirty.
    wait_for(&mut stream, |event| match event {
        WatchEvent::Fs(fs) => {
            fs.path == to
                && matches!(
                    fs.kind,
                    EventKind::MovedTo | EventKind::Create | EventKind::Modify
                )
        }
        WatchEvent::Rescan { .. } => false,
    })
    .await;
}

#[tokio::test]
async fn rejects_a_root_that_does_not_exist() {
    let dir = TempDir::new().expect("temp dir");
    let missing = dir.path().join("nope");

    let err = watch(&missing).expect_err("should not watch a missing root");

    assert!(
        matches!(err, treesync::error::Error::NotFound(_)),
        "expected NotFound, got {err:?}"
    );
}

#[tokio::test]
async fn rejects_a_root_that_is_a_file() {
    let dir = TempDir::new().expect("temp dir");
    let file = dir.path().join("regular.txt");
    std::fs::write(&file, b"hello").expect("write");

    let err = watch(&file).expect_err("should not watch a file");

    assert!(
        matches!(err, treesync::error::Error::InvalidPath(_)),
        "expected InvalidPath, got {err:?}"
    );
}
