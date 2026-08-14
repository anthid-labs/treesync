//! Walk, plan, apply, against real directories.
//!
//! The property these exist to prove is convergence: applying a plan must leave
//! the target matching the source, such that planning again produces nothing.
//! A pipeline that re-copies the same files forever passes every unit test in
//! isolation and is useless.

use std::path::{Path, PathBuf};

use tempfile::TempDir;
use treesync::reconcile::{Index, IndexOptions, Preserve, ReconcileConfig, Scope, plan, walk};
use treesync::sink::{LocalSink, apply};

fn tree(entries: &[(&str, &str)]) -> TempDir {
    let dir = TempDir::new().expect("temp dir");

    for (path, contents) in entries {
        let full = dir.path().join(path);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).expect("create parents");
        }

        std::fs::write(&full, contents).expect("write");
    }

    dir
}

fn everything() -> Scope {
    Scope::Subtree(PathBuf::new())
}

fn deleting() -> ReconcileConfig {
    ReconcileConfig {
        delete: true,
        preserve: Preserve {
            mode: false,
            ownership: false,
        },
        ..Default::default()
    }
}

/// Walks both trees, plans, and applies. Returns how many actions ran.
async fn sync_once(source: &Path, target: &Path, config: &ReconcileConfig) -> usize {
    let source_index = walk(source, &IndexOptions::quick()).expect("walk source");
    let target_index = walk(target, &IndexOptions::quick()).expect("walk target");
    let plan = plan(&source_index, &target_index, &everything(), config);

    let sink = LocalSink::new(target).expect("sink");
    let report = apply(&plan, source, &sink, config.preserve).await;

    assert!(
        report.is_complete(),
        "actions failed: {:?}",
        report.failures
    );

    report.applied
}

/// Asserts the two trees are indistinguishable to the reconciler.
fn assert_converged(source: &Path, target: &Path) {
    let remaining = plan(
        &walk(source, &IndexOptions::quick()).expect("walk source"),
        &walk(target, &IndexOptions::quick()).expect("walk target"),
        &everything(),
        &deleting(),
    );

    assert!(
        remaining.is_empty(),
        "trees still differ after syncing: {:?}",
        remaining.actions
    );
}

#[tokio::test]
async fn an_empty_target_receives_the_whole_tree() {
    let source = tree(&[
        ("a.txt", "one"),
        ("sub/b.txt", "two"),
        ("sub/deep/c.txt", "three"),
    ]);
    let target = TempDir::new().expect("temp dir");

    sync_once(source.path(), target.path(), &deleting()).await;

    assert_eq!(
        std::fs::read_to_string(target.path().join("sub/deep/c.txt")).expect("read"),
        "three"
    );
    assert_converged(source.path(), target.path());
}

#[tokio::test]
async fn syncing_twice_does_no_work_the_second_time() {
    let source = tree(&[("a.txt", "one"), ("sub/b.txt", "two")]);
    let target = TempDir::new().expect("temp dir");

    let first = sync_once(source.path(), target.path(), &deleting()).await;
    let second = sync_once(source.path(), target.path(), &deleting()).await;

    assert!(first > 0, "the first pass should have done something");
    assert_eq!(
        second, 0,
        "the second pass must be a no-op; anything else means the sync never settles"
    );
}

#[tokio::test]
async fn a_modified_file_is_resynced_and_then_settles() {
    let source = tree(&[("a.txt", "original")]);
    let target = TempDir::new().expect("temp dir");
    sync_once(source.path(), target.path(), &deleting()).await;

    std::fs::write(source.path().join("a.txt"), "changed and longer").expect("write");

    assert_eq!(
        sync_once(source.path(), target.path(), &deleting()).await,
        1
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join("a.txt")).expect("read"),
        "changed and longer"
    );
    assert_converged(source.path(), target.path());
}

#[tokio::test]
async fn a_removed_file_is_deleted_from_the_target() {
    let source = tree(&[("keep.txt", "one"), ("remove.txt", "two")]);
    let target = TempDir::new().expect("temp dir");
    sync_once(source.path(), target.path(), &deleting()).await;

    std::fs::remove_file(source.path().join("remove.txt")).expect("remove");
    sync_once(source.path(), target.path(), &deleting()).await;

    assert!(!target.path().join("remove.txt").exists());
    assert!(target.path().join("keep.txt").exists());
    assert_converged(source.path(), target.path());
}

#[tokio::test]
async fn a_removed_directory_is_deleted_deepest_first() {
    let source = tree(&[("keep.txt", "one"), ("doomed/deep/file.txt", "two")]);
    let target = TempDir::new().expect("temp dir");
    sync_once(source.path(), target.path(), &deleting()).await;

    std::fs::remove_dir_all(source.path().join("doomed")).expect("remove");

    // Ordering is the whole test: removing `doomed` before `doomed/deep` would
    // fail, because the sink never removes recursively.
    sync_once(source.path(), target.path(), &deleting()).await;

    assert!(!target.path().join("doomed").exists());
    assert_converged(source.path(), target.path());
}

#[tokio::test]
async fn without_delete_the_target_keeps_what_the_source_dropped() {
    let source = tree(&[("keep.txt", "one"), ("removed.txt", "two")]);
    let target = TempDir::new().expect("temp dir");
    sync_once(source.path(), target.path(), &ReconcileConfig::default()).await;

    std::fs::remove_file(source.path().join("removed.txt")).expect("remove");
    sync_once(source.path(), target.path(), &ReconcileConfig::default()).await;

    assert!(
        target.path().join("removed.txt").exists(),
        "deletion is opt-in; the target must keep the file"
    );
}

#[tokio::test]
async fn symlinks_survive_a_round_trip_as_links() {
    let source = TempDir::new().expect("temp dir");
    std::fs::write(source.path().join("real.txt"), "content").expect("write");
    std::os::unix::fs::symlink("real.txt", source.path().join("relative")).expect("symlink");
    std::os::unix::fs::symlink("/etc/hosts", source.path().join("absolute")).expect("symlink");

    let target = TempDir::new().expect("temp dir");
    sync_once(source.path(), target.path(), &deleting()).await;

    assert_eq!(
        std::fs::read_link(target.path().join("relative")).expect("read link"),
        PathBuf::from("real.txt")
    );
    assert_eq!(
        std::fs::read_link(target.path().join("absolute")).expect("read link"),
        PathBuf::from("/etc/hosts"),
        "a link out of the tree stays a link; its contents were never ours to copy"
    );
    assert_converged(source.path(), target.path());
}

#[tokio::test]
async fn a_file_replacing_a_directory_is_applied_in_the_right_order() {
    let source = TempDir::new().expect("temp dir");
    let target = TempDir::new().expect("temp dir");

    std::fs::create_dir(source.path().join("thing")).expect("mkdir");
    std::fs::write(source.path().join("thing/inner.txt"), "data").expect("write");
    sync_once(source.path(), target.path(), &deleting()).await;

    // Now `thing` becomes a regular file on the source.
    std::fs::remove_dir_all(source.path().join("thing")).expect("remove");
    std::fs::write(source.path().join("thing"), "now a file").expect("write");

    sync_once(source.path(), target.path(), &deleting()).await;

    assert!(target.path().join("thing").is_file());
    assert_eq!(
        std::fs::read_to_string(target.path().join("thing")).expect("read"),
        "now a file"
    );
    assert_converged(source.path(), target.path());
}

#[tokio::test]
async fn an_incremental_scope_syncs_only_the_named_paths() {
    let source = tree(&[("touched.txt", "one"), ("untouched.txt", "two")]);
    let target = TempDir::new().expect("temp dir");

    let plan = plan(
        &walk(source.path(), &IndexOptions::quick()).expect("walk"),
        &Index::new(),
        &Scope::Paths(vec![PathBuf::from("touched.txt")]),
        &deleting(),
    );

    let sink = LocalSink::new(target.path()).expect("sink");
    apply(&plan, source.path(), &sink, Preserve::default()).await;

    assert!(target.path().join("touched.txt").exists());
    assert!(
        !target.path().join("untouched.txt").exists(),
        "an incremental batch must not touch paths it did not name"
    );
}

#[tokio::test]
async fn a_failing_action_does_not_strand_the_rest_of_the_batch() {
    let source = tree(&[("a.txt", "one"), ("b.txt", "two"), ("c.txt", "three")]);
    let target = TempDir::new().expect("temp dir");

    // Remove one source file after planning, so its copy fails while the
    // others still succeed.
    let source_index = walk(source.path(), &IndexOptions::quick()).expect("walk");
    let plan = plan(&source_index, &Index::new(), &everything(), &deleting());
    std::fs::remove_file(source.path().join("b.txt")).expect("remove");

    let sink = LocalSink::new(target.path()).expect("sink");
    let report = apply(&plan, source.path(), &sink, Preserve::default()).await;

    assert_eq!(report.applied, 2);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(
        report.failed_paths().collect::<Vec<_>>(),
        vec![Path::new("b.txt")],
        "the failure must name the path so the caller can requeue exactly it"
    );
    assert!(target.path().join("a.txt").exists());
    assert!(target.path().join("c.txt").exists());
}

// --- metadata fidelity -----------------------------------------------------

use std::os::unix::fs::PermissionsExt;

fn mode_of(path: &Path) -> u32 {
    std::fs::metadata(path)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o7777
}

fn preserving() -> ReconcileConfig {
    ReconcileConfig {
        delete: true,
        preserve: Preserve {
            mode: true,
            ownership: false,
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn an_executable_arrives_executable() {
    let source = tree(&[("run.sh", "#!/bin/sh\necho hi\n")]);
    let target = TempDir::new().expect("temp dir");
    std::fs::set_permissions(
        source.path().join("run.sh"),
        std::fs::Permissions::from_mode(0o755),
    )
    .expect("chmod");

    sync_once(source.path(), target.path(), &preserving()).await;

    assert_eq!(
        mode_of(&target.path().join("run.sh")),
        0o755,
        "a mirrored executable without its execute bit is not a useful mirror"
    );
    assert_converged(source.path(), target.path());
}

#[tokio::test]
async fn a_restrictive_directory_mode_is_reproduced() {
    let source = TempDir::new().expect("temp dir");
    let target = TempDir::new().expect("temp dir");
    let locked = source.path().join("locked");
    std::fs::create_dir(&locked).expect("mkdir");
    std::fs::write(locked.join("inner.txt"), "data").expect("write");
    // Read and execute only: writing into this after chmod would fail, which
    // is what makes the ordering rule load-bearing.
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500)).expect("chmod");

    sync_once(source.path(), target.path(), &preserving()).await;

    assert_eq!(mode_of(&target.path().join("locked")), 0o500);
    assert_eq!(
        std::fs::read_to_string(target.path().join("locked/inner.txt")).expect("read"),
        "data",
        "the file must have been written before the directory was tightened"
    );

    // Loosen it again so the temp dir can be cleaned up.
    std::fs::set_permissions(
        target.path().join("locked"),
        std::fs::Permissions::from_mode(0o755),
    )
    .expect("chmod");
}

#[tokio::test]
async fn a_mode_change_alone_is_applied_without_recopying() {
    let source = tree(&[("a.txt", "unchanged")]);
    let target = TempDir::new().expect("temp dir");
    sync_once(source.path(), target.path(), &preserving()).await;

    std::fs::set_permissions(
        source.path().join("a.txt"),
        std::fs::Permissions::from_mode(0o600),
    )
    .expect("chmod");

    let applied = sync_once(source.path(), target.path(), &preserving()).await;

    assert_eq!(applied, 1, "only the mode should have needed changing");
    assert_eq!(mode_of(&target.path().join("a.txt")), 0o600);
    assert_converged(source.path(), target.path());
}

#[tokio::test]
async fn modes_are_left_alone_when_preservation_is_off() {
    let source = tree(&[("run.sh", "#!/bin/sh\n")]);
    let target = TempDir::new().expect("temp dir");
    std::fs::set_permissions(
        source.path().join("run.sh"),
        std::fs::Permissions::from_mode(0o755),
    )
    .expect("chmod");

    let config = ReconcileConfig {
        delete: true,
        preserve: Preserve {
            mode: false,
            ownership: false,
        },
        ..Default::default()
    };
    sync_once(source.path(), target.path(), &config).await;

    // Nothing asserts the resulting mode: with preservation off, whatever the
    // copy produced is acceptable. What must hold is that no metadata action
    // was planned, which convergence under the same config demonstrates.
    let remaining = plan(
        &walk(source.path(), &IndexOptions::quick()).expect("walk"),
        &walk(target.path(), &IndexOptions::quick()).expect("walk"),
        &everything(),
        &config,
    );
    assert!(remaining.is_empty(), "got {:?}", remaining.actions);
}

#[tokio::test]
async fn preserving_modes_still_converges() {
    let source = tree(&[("a.txt", "one"), ("sub/b.txt", "two")]);
    let target = TempDir::new().expect("temp dir");

    let first = sync_once(source.path(), target.path(), &preserving()).await;
    let second = sync_once(source.path(), target.path(), &preserving()).await;

    assert!(first > 0);
    assert_eq!(
        second, 0,
        "stamping metadata must not make the sync re-do work forever"
    );
}
