//! Adversarial conditions on the filesystem itself.
//!
//! Everything here is about what happens when the tree fights back: a file that
//! cannot be read, a target that cannot be written, a path whose *type* changed
//! since it was indexed, a file that is modified out from under a transfer.
//!
//! Two properties are asserted throughout, and they are the ones that matter
//! more than any individual error message:
//!
//! - **A failure is reported, never silent.** The worst outcome for a mirroring
//!   tool is a target that quietly does not match, because nothing downstream
//!   can tell that from one that does.
//! - **A failure is confined.** One unreadable file must not strand the rest of
//!   the batch, and must not damage what is already on the target.
//!
//! Tests that need privileges this process may not have, such as the immutable
//! bit needing `CAP_LINUX_IMMUTABLE`, skip loudly instead of passing vacuously.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use treesync::reconcile::{IndexOptions, Preserve, ReconcileConfig, Scope, plan, walk};
use treesync::sink::{ApplyReport, LocalSink, apply};

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

fn config(delete: bool) -> ReconcileConfig {
    ReconcileConfig {
        delete,
        preserve: Preserve {
            mode: false,
            ownership: false,
        },
        ..Default::default()
    }
}

fn chmod(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
}

/// Runs a pass and hands back the report rather than asserting success.
async fn try_sync(source: &Path, target: &Path, config: &ReconcileConfig) -> ApplyReport {
    let source_index = walk(source, &IndexOptions::quick()).expect("walk source");
    let target_index = walk(target, &IndexOptions::quick()).expect("walk target");
    let plan = plan(&source_index, &target_index, &everything(), config);
    let sink = LocalSink::new(target).expect("sink");

    apply(&plan, source, &sink, config.preserve).await
}

/// Whether this process can set the immutable bit here.
///
/// Needs `CAP_LINUX_IMMUTABLE`, which an ordinary user does not have and a
/// container does not get by default. Probing is the only reliable test: the
/// capability can be present and the *filesystem* still not support the flag.
fn can_set_immutable(probe: &Path) -> bool {
    std::fs::write(probe, b"probe").expect("write probe");

    let set = std::process::Command::new("chattr")
        .arg("+i")
        .arg(probe)
        .output();

    match set {
        Ok(output) if output.status.success() => {
            // Leave the tree clean for the caller either way.
            let _ = std::process::Command::new("chattr")
                .arg("-i")
                .arg(probe)
                .output();
            true
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Unreadable source
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_unreadable_source_file_fails_only_its_own_action() {
    let source = tree(&[("readable.txt", "fine"), ("secret.txt", "classified")]);
    let target = TempDir::new().expect("target");

    chmod(&source.path().join("secret.txt"), 0o000);

    let report = try_sync(source.path(), target.path(), &config(false)).await;

    // Restore before the TempDir drop, or cleanup fails on some systems.
    chmod(&source.path().join("secret.txt"), 0o644);

    assert_eq!(
        report.failures.len(),
        1,
        "exactly the unreadable file should fail; got {:?}",
        report.failures
    );
    assert_eq!(
        report.failures[0].action.path(),
        Path::new("secret.txt"),
        "the failure must name the path that caused it"
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join("readable.txt")).expect("read"),
        "fine",
        "one unreadable file must not strand the rest of the batch"
    );
    assert!(
        !target.path().join("secret.txt").exists(),
        "a file that could not be read must not be published as an empty one"
    );
}

#[tokio::test]
async fn an_unreadable_source_directory_is_an_error_not_an_empty_tree() {
    // The dangerous one. With `delete` on, a source that reads as empty plans
    // the removal of the entire target.
    let source = tree(&[("keep/a.txt", "a"), ("keep/b.txt", "b")]);
    let target = tree(&[("keep/a.txt", "a"), ("keep/b.txt", "b")]);

    chmod(&source.path().join("keep"), 0o000);

    let walked = walk(source.path(), &IndexOptions::quick());

    chmod(&source.path().join("keep"), 0o755);

    assert!(
        walked.is_err(),
        "an unreadable directory must fail the walk, not read as empty"
    );
    assert!(
        target.path().join("keep/a.txt").exists(),
        "and nothing may have been removed from the target"
    );
}

// ---------------------------------------------------------------------------
// Unwritable target
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_read_only_target_directory_is_opened_for_the_write_and_closed_again() {
    // A source tree may hold a directory nobody is meant to write into, and
    // mirroring its mode makes the target directory read-only too. Refusing the
    // write here would mean the mirror could never converge: the file is in the
    // source, it is not in the target, and every later pass fails identically
    // because nothing about either side changes.
    //
    // So the owner bits are widened for the length of the write and put straight
    // back. What must not change is anything else about the outcome.
    let source = tree(&[("locked/new.txt", "new"), ("free/ok.txt", "ok")]);
    let target = tree(&[("locked/existing.txt", "existing")]);

    chmod(&target.path().join("locked"), 0o555);

    let report = try_sync(source.path(), target.path(), &config(false)).await;

    let mode = std::fs::metadata(target.path().join("locked"))
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;

    chmod(&target.path().join("locked"), 0o755);

    assert!(
        report.is_complete(),
        "a read-only directory must not strand the file the source put in it: {:?}",
        report.failures
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join("locked/new.txt")).expect("read"),
        "new"
    );
    assert_eq!(
        mode, 0o555,
        "the mode has to be exactly as it was found; a mirror that quietly \
         leaves directories more permissive than the source is a worse outcome \
         than the transfer failing"
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join("locked/existing.txt")).expect("read"),
        "existing",
        "the file already there must be untouched"
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join("free/ok.txt")).expect("read"),
        "ok",
        "and the rest of the batch must still have run"
    );
}

#[tokio::test]
async fn a_target_directory_that_cannot_be_opened_is_reported_rather_than_forced() {
    // The other half of the rule above: treesync widens what it owns and reports
    // what it does not. An immutable directory refuses new entries whatever its
    // mode says, so there is nothing to widen and nothing that would help.
    let source = tree(&[("locked/new.txt", "new"), ("free/ok.txt", "ok")]);
    let target = tree(&[("locked/existing.txt", "existing")]);
    let locked = target.path().join("locked");

    if !can_set_immutable(&target.path().join(".immutable-probe")) {
        eprintln!(
            "SKIPPED a_target_directory_that_cannot_be_opened_is_reported_rather_than_forced: \
             setting the immutable bit needs CAP_LINUX_IMMUTABLE and a filesystem \
             that supports it. Run as root, or in a container with \
             --cap-add=LINUX_IMMUTABLE, to exercise this."
        );
        return;
    }

    let set = std::process::Command::new("chattr")
        .arg("+i")
        .arg(&locked)
        .output()
        .expect("chattr");
    assert!(set.status.success(), "could not set the immutable bit");

    let report = try_sync(source.path(), target.path(), &config(false)).await;

    // Always clear it, or the TempDir cannot be removed.
    let _ = std::process::Command::new("chattr")
        .arg("-i")
        .arg(&locked)
        .output();

    assert!(
        !report.is_complete(),
        "a directory treesync cannot open has to be reported, not silently skipped"
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join("locked/existing.txt")).expect("read"),
        "existing",
        "the file already there must be untouched"
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join("free/ok.txt")).expect("read"),
        "ok",
        "and the rest of the batch must still have run"
    );
}

#[tokio::test]
async fn a_read_only_target_file_is_still_replaced() {
    // The publish is a rename over the path, which needs write permission on
    // the *directory*, not on the file being replaced. Worth pinning: the
    // opposite behaviour would strand any target file someone had chmod'd.
    let source = tree(&[("a.txt", "new contents")]);
    let target = tree(&[("a.txt", "old contents")]);

    chmod(&target.path().join("a.txt"), 0o444);

    let report = try_sync(source.path(), target.path(), &config(false)).await;

    assert!(
        report.is_complete(),
        "a read-only file should still be replaceable: {:?}",
        report.failures
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join("a.txt")).expect("read"),
        "new contents"
    );
}

#[tokio::test]
async fn an_immutable_target_file_fails_loudly_rather_than_silently() {
    let source = tree(&[("a.txt", "new contents")]);
    let target = tree(&[("a.txt", "old contents")]);
    let victim = target.path().join("a.txt");

    if !can_set_immutable(&target.path().join(".immutable-probe")) {
        eprintln!(
            "SKIPPED an_immutable_target_file_fails_loudly_rather_than_silently: \
             setting the immutable bit needs CAP_LINUX_IMMUTABLE and a filesystem \
             that supports it. Run as root, or in a container with \
             --cap-add=LINUX_IMMUTABLE, to exercise this."
        );
        return;
    }

    std::fs::write(&victim, "old contents").expect("seed");
    let set = std::process::Command::new("chattr")
        .arg("+i")
        .arg(&victim)
        .output()
        .expect("chattr");
    assert!(set.status.success(), "could not set the immutable bit");

    let report = try_sync(source.path(), target.path(), &config(false)).await;

    // Always clear it, or the TempDir cannot be removed.
    let _ = std::process::Command::new("chattr")
        .arg("-i")
        .arg(&victim)
        .output();

    assert!(
        !report.is_complete(),
        "an immutable target must be reported as a failed action, not skipped"
    );
    assert_eq!(
        std::fs::read_to_string(&victim).expect("read"),
        "old contents",
        "and the immutable file must be exactly as it was"
    );
}

// ---------------------------------------------------------------------------
// Type conflicts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_file_replaces_a_directory_of_the_same_name_on_the_target() {
    // The target holds a directory where the source now holds a file. Getting
    // this wrong strands the old directory forever, because every later pass
    // sees the same conflict and makes the same non-decision.
    let source = tree(&[("thing", "now a file")]);
    let target = tree(&[("thing/inside.txt", "was a directory")]);

    let report = try_sync(source.path(), target.path(), &config(true)).await;

    assert!(report.is_complete(), "{:?}", report.failures);
    assert_eq!(
        std::fs::read_to_string(target.path().join("thing")).expect("read"),
        "now a file",
        "the directory has to give way to the file"
    );
}

#[tokio::test]
async fn a_directory_replaces_a_file_of_the_same_name_on_the_target() {
    let source = tree(&[("thing/inside.txt", "now a directory")]);
    let target = tree(&[("thing", "was a file")]);

    let report = try_sync(source.path(), target.path(), &config(true)).await;

    assert!(report.is_complete(), "{:?}", report.failures);
    assert_eq!(
        std::fs::read_to_string(target.path().join("thing/inside.txt")).expect("read"),
        "now a directory",
        "the file has to give way to the directory"
    );
}

#[tokio::test]
async fn a_type_conflict_converges_rather_than_repeating_every_pass() {
    // The property that matters more than either swap: once resolved, it stays
    // resolved. A conflict that is "fixed" every pass is a sync that never
    // settles, and looks identical to one that is working.
    let source = tree(&[("thing", "now a file")]);
    let target = tree(&[("thing/inside.txt", "was a directory")]);

    try_sync(source.path(), target.path(), &config(true)).await;

    let source_index = walk(source.path(), &IndexOptions::quick()).expect("walk");
    let target_index = walk(target.path(), &IndexOptions::quick()).expect("walk");
    let settled = plan(&source_index, &target_index, &everything(), &config(true));

    assert!(
        settled.actions.is_empty(),
        "the pass after a type conflict must find nothing left; got {:?}",
        settled.actions
    );
}

// ---------------------------------------------------------------------------
// The tree moving underneath a pass
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_file_that_vanishes_between_planning_and_applying_fails_only_itself() {
    let source = tree(&[("staying.txt", "here"), ("leaving.txt", "not for long")]);
    let target = TempDir::new().expect("target");

    let source_index = walk(source.path(), &IndexOptions::quick()).expect("walk source");
    let target_index = walk(target.path(), &IndexOptions::quick()).expect("walk target");
    let plan = plan(&source_index, &target_index, &everything(), &config(false));

    // Between the plan and the apply, which is routine in a tree under active write.
    std::fs::remove_file(source.path().join("leaving.txt")).expect("remove");

    let sink = LocalSink::new(target.path()).expect("sink");
    let report = apply(&plan, source.path(), &sink, config(false).preserve).await;

    assert_eq!(
        report.failures.len(),
        1,
        "only the vanished file should fail; got {:?}",
        report.failures
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join("staying.txt")).expect("read"),
        "here"
    );
    assert!(
        !target.path().join("leaving.txt").exists(),
        "a file that vanished must not be published as a partial or empty one"
    );
}

#[tokio::test]
async fn a_file_that_grows_during_a_pass_still_converges_on_a_later_one() {
    // The transfer reads whatever is there and stamps the mtime it finds
    // *after* the content, so a file rewritten mid-copy arrives looking newer
    // than the copy, and the next pass catches it. The property is
    // convergence, not that any single pass wins the race.
    let source = tree(&[("growing.txt", "small")]);
    let target = TempDir::new().expect("target");

    try_sync(source.path(), target.path(), &config(false)).await;

    std::fs::write(source.path().join("growing.txt"), "much larger than before").expect("rewrite");

    try_sync(source.path(), target.path(), &config(false)).await;

    assert_eq!(
        std::fs::read_to_string(target.path().join("growing.txt")).expect("read"),
        "much larger than before"
    );

    // And having converged, a further pass must find nothing to do.
    let source_index = walk(source.path(), &IndexOptions::quick()).expect("walk");
    let target_index = walk(target.path(), &IndexOptions::quick()).expect("walk");
    let settled = plan(&source_index, &target_index, &everything(), &config(false));

    assert!(
        settled.actions.is_empty(),
        "the pass after a rewrite must settle; got {:?}",
        settled.actions
    );
}

#[tokio::test]
async fn a_symlink_pointing_outside_the_tree_is_copied_as_a_link_not_followed() {
    // Following it would pull an arbitrary amount of the filesystem into the
    // mirror, and a link to `/` would pull all of it.
    let source = TempDir::new().expect("source");
    let target = TempDir::new().expect("target");
    std::os::unix::fs::symlink("/etc/passwd", source.path().join("escape")).expect("symlink");

    let report = try_sync(source.path(), target.path(), &config(false)).await;

    assert!(report.is_complete(), "{:?}", report.failures);

    let landed = target.path().join("escape");
    let metadata = std::fs::symlink_metadata(&landed).expect("metadata");

    assert!(
        metadata.file_type().is_symlink(),
        "it must arrive as a link, not as a copy of what it points at"
    );
    assert_eq!(
        std::fs::read_link(&landed).expect("read_link"),
        PathBuf::from("/etc/passwd")
    );
}

// ---------------------------------------------------------------------------
// Containment: a target that redirects a write out of itself
// ---------------------------------------------------------------------------
//
// The target tree is not necessarily under treesync's sole control. Anything
// that can write into it can leave a symlink there, and a symlink is the one
// kind of entry that turns a perfectly ordinary path into a write somewhere
// else. Every path is confined by name before it is used, but a name says
// nothing about where it leads.

#[tokio::test]
async fn a_symlinked_directory_on_the_target_does_not_let_a_write_escape() {
    // The source has a directory; the target has a link where that directory
    // should be. `delete` is off, so nothing is allowed to clear the link, and
    // the write has to be refused rather than followed.
    let source = tree(&[("a/secret.txt", "must not leave the target")]);
    let target = TempDir::new().expect("target");
    let elsewhere = TempDir::new().expect("elsewhere");

    std::os::unix::fs::symlink(elsewhere.path(), target.path().join("a")).expect("symlink");

    let report = try_sync(source.path(), target.path(), &config(false)).await;

    assert!(
        !elsewhere.path().join("secret.txt").exists(),
        "a sync must never write outside the root it was given"
    );
    assert!(
        !report.is_complete(),
        "and refusing has to be reported: a target that silently does not match \
         is the one outcome nothing downstream can detect"
    );
}

#[tokio::test]
async fn a_symlinked_directory_is_replaced_when_deletions_are_allowed() {
    // The other half. With `delete` on, the reconciler clears the conflicting
    // entry first, so the mirror converges instead of failing forever.
    let source = tree(&[("a/secret.txt", "stays inside")]);
    let target = TempDir::new().expect("target");
    let elsewhere = TempDir::new().expect("elsewhere");

    std::os::unix::fs::symlink(elsewhere.path(), target.path().join("a")).expect("symlink");

    let report = try_sync(source.path(), target.path(), &config(true)).await;

    assert!(report.is_complete(), "{:?}", report.failures);
    assert!(!elsewhere.path().join("secret.txt").exists());
    assert_eq!(
        std::fs::read_to_string(target.path().join("a/secret.txt")).expect("read"),
        "stays inside"
    );
    assert!(
        target
            .path()
            .join("a")
            .symlink_metadata()
            .expect("metadata")
            .is_dir(),
        "the link has to have been replaced by a real directory"
    );
}

#[tokio::test]
async fn a_symlink_at_the_temporary_path_is_not_written_through() {
    // The temporary's name is derived from the destination's, so anything that
    // can write to the target directory can predict it and put a link there
    // first. Following it would send the transfer's bytes through the link,
    // destroying what it points at, and then publish the link itself in place of
    // the file.
    let source = tree(&[("a.txt", "new contents")]);
    let target = TempDir::new().expect("target");
    let elsewhere = TempDir::new().expect("elsewhere");

    let victim = elsewhere.path().join("victim.txt");
    std::fs::write(&victim, "precious").expect("write");
    std::os::unix::fs::symlink(&victim, target.path().join(".treesync-tmp-a.txt"))
        .expect("symlink");

    let report = try_sync(source.path(), target.path(), &config(false)).await;

    assert!(report.is_complete(), "{:?}", report.failures);
    assert_eq!(
        std::fs::read_to_string(&victim).expect("read"),
        "precious",
        "the file the link pointed at must be untouched"
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join("a.txt")).expect("read"),
        "new contents"
    );
    assert!(
        !std::fs::symlink_metadata(target.path().join("a.txt"))
            .expect("metadata")
            .file_type()
            .is_symlink(),
        "what was published has to be the content, not the link"
    );
}

// ---------------------------------------------------------------------------
// Entries that are not tree content
// ---------------------------------------------------------------------------

/// Creates a FIFO, or says why it could not.
fn make_fifo(path: &Path) -> bool {
    std::process::Command::new("mkfifo")
        .arg(path)
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[tokio::test]
async fn a_named_pipe_in_the_source_does_not_stall_the_sync() {
    // A FIFO has nothing to mirror: what it holds is whatever a writer is
    // putting through it right now. Worse, opening one with no writer on the
    // other end never returns, and a plan is applied one action at a time, so a
    // single FIFO anywhere in the tree stops the sync with no error, no timeout
    // and nothing in the log to say why.
    let source = tree(&[("ordinary.txt", "please copy me")]);
    let target = TempDir::new().expect("target");

    if !make_fifo(&source.path().join("pipe")) {
        eprintln!("SKIPPED a_named_pipe_in_the_source_does_not_stall_the_sync: no usable mkfifo");
        return;
    }

    // Asserted before the sync as well as after, because this is the invariant
    // that keeps the sync from hanging rather than a consequence of it: if the
    // FIFO ever reaches an index, the copy that follows is the one that blocks,
    // and a blocked copy takes the runtime with it rather than failing a test.
    let index = walk(source.path(), &IndexOptions::quick()).expect("walk");
    assert!(
        !index.contains(Path::new("pipe")),
        "a FIFO must not be indexed, or the transfer opens it"
    );

    let report = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        try_sync(source.path(), target.path(), &config(false)),
    )
    .await
    .expect("the sync must finish; a FIFO in the tree used to hang it forever");

    assert!(report.is_complete(), "{:?}", report.failures);
    assert_eq!(
        std::fs::read_to_string(target.path().join("ordinary.txt")).expect("read"),
        "please copy me",
        "and the rest of the tree still has to be mirrored"
    );
    assert!(
        !target.path().join("pipe").exists(),
        "nothing should have been created for it on the target"
    );
}

#[tokio::test]
async fn a_named_pipe_on_the_target_is_not_read_as_a_deletion() {
    // Both trees are indexed through the same code, so a special file on the
    // target is invisible there too. If it were not, `delete` would plan the
    // removal of something the source never had a say in.
    let source = tree(&[("a.txt", "one")]);
    let target = tree(&[("a.txt", "one")]);

    if !make_fifo(&target.path().join("pipe")) {
        eprintln!("SKIPPED a_named_pipe_on_the_target_is_not_read_as_a_deletion: no usable mkfifo");
        return;
    }

    let source_index = walk(source.path(), &IndexOptions::quick()).expect("walk source");
    let target_index = walk(target.path(), &IndexOptions::quick()).expect("walk target");
    let planned = plan(&source_index, &target_index, &everything(), &config(true));

    // Only the pipe matters here. The two trees were built separately, so
    // `a.txt` differs by mtime and is legitimately planned for copying.
    let touching_the_pipe: Vec<_> = planned
        .actions
        .iter()
        .filter(|action| action.path() == Path::new("pipe"))
        .collect();

    assert!(
        touching_the_pipe.is_empty(),
        "a special file on the target is not something the source deleted; got {touching_the_pipe:?}"
    );
    assert!(
        target.path().join("pipe").exists(),
        "and it is still there afterwards"
    );
}

// ---------------------------------------------------------------------------
// Names at the filesystem's limit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_name_at_the_filesystems_limit_still_arrives() {
    // The transfer goes through a temporary whose name carries a prefix, so a
    // source name that is long but perfectly legal can produce a temporary the
    // kernel refuses. That file could never be published, on any pass, ever.
    let long = format!("{}.txt", "a".repeat(245));
    let source = tree(&[(long.as_str(), "content"), ("short.txt", "content")]);
    let target = TempDir::new().expect("target");

    let report = try_sync(source.path(), target.path(), &config(false)).await;

    assert!(
        report.is_complete(),
        "a name the source filesystem accepted has to be one the target can \
         take: {:?}",
        report.failures
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join(&long)).expect("read"),
        "content"
    );
}

#[tokio::test]
async fn a_long_name_converges_rather_than_being_copied_every_pass() {
    // The shortened temporary still has to end up renamed onto the real name,
    // with the source's mtime, or the reconciler sees a difference forever.
    let long = format!("{}.json", "b".repeat(240));
    let source = tree(&[(long.as_str(), "content")]);
    let target = TempDir::new().expect("target");

    try_sync(source.path(), target.path(), &config(false)).await;

    let source_index = walk(source.path(), &IndexOptions::quick()).expect("walk");
    let target_index = walk(target.path(), &IndexOptions::quick()).expect("walk");
    let settled = plan(&source_index, &target_index, &everything(), &config(false));

    assert!(
        settled.actions.is_empty(),
        "the pass after a long name lands must find nothing left; got {:?}",
        settled.actions
    );

    let leftovers: Vec<_> = std::fs::read_dir(target.path())
        .expect("read dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with(".treesync-"))
        .collect();

    assert!(leftovers.is_empty(), "left {leftovers:?} behind");
}

// ---------------------------------------------------------------------------
// A transfer that cannot be published
// ---------------------------------------------------------------------------
//
// These used to induce a genuine `ENOSPC` by pointing the transfer's temporary
// at `/dev/full` through a symlink. That worked only because a write followed
// whatever it found at the temporary path, which is exactly the hole
// `sink::local::copy_into_fresh` now closes: the temporary is unlinked and
// created exclusively, so a planted symlink is removed rather than written
// through. `a_symlink_at_the_temporary_path_is_not_written_through` below pins
// that, and the trick cannot be used to simulate a full disk any more.
//
// The properties those tests protected are not about `ENOSPC` in particular.
// They are about a transfer that fails *after* the copy, at the moment of
// publishing: the failure is reported, the version already on the target
// survives, the rest of the batch still runs, the temporary is cleaned up, and
// nothing about the failed attempt blocks the next one. A rename over a
// directory that still has something in it fails at precisely that point, needs
// no privileges, and is a real case rather than a contrived one: it is what a
// type conflict looks like with `delete` off.
//
// A genuinely full filesystem is still covered end to end, against a real 1 MB
// tmpfs, by `docker/remote-test.sh`.

#[tokio::test]
async fn a_transfer_that_cannot_be_published_keeps_what_is_already_there() {
    // The source has a file where the target has a directory, and `delete` is
    // off, so nothing clears the way. The copy succeeds and the rename that
    // would publish it fails.
    let source = tree(&[("a.txt", "the new version"), ("b.txt", "fine")]);
    let target = tree(&[("a.txt/inside.txt", "a directory, not a file")]);

    let report = try_sync(source.path(), target.path(), &config(false)).await;

    assert!(
        !report.is_complete(),
        "a transfer that could not be published has to be reported, not swallowed"
    );

    let failure = report
        .failures
        .iter()
        .find(|failure| failure.action.path() == Path::new("a.txt"))
        .expect("the failure must name the file that could not be published");

    assert!(
        !failure.error.to_string().is_empty(),
        "the error should say what went wrong"
    );

    assert_eq!(
        std::fs::read_to_string(target.path().join("a.txt/inside.txt")).expect("read"),
        "a directory, not a file",
        "a transfer that failed to publish must leave what was there intact"
    );

    assert_eq!(
        std::fs::read_to_string(target.path().join("b.txt")).expect("read"),
        "fine",
        "and must not strand the rest of the batch"
    );

    let leftovers: Vec<_> = std::fs::read_dir(target.path())
        .expect("read dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with(".treesync-tmp-"))
        .collect();

    assert!(
        leftovers.is_empty(),
        "the temporary has to be cleaned up, or every retry leaks one: {leftovers:?}"
    );
}

#[tokio::test]
async fn a_transfer_recovers_once_the_obstruction_is_gone() {
    // The property that separates a usable failure from a stuck one: nothing
    // about the failed attempt may prevent the next from succeeding.
    let source = tree(&[("a.txt", "eventually this lands")]);
    let target = tree(&[("a.txt/inside.txt", "in the way")]);

    let blocked = try_sync(source.path(), target.path(), &config(false)).await;
    assert!(!blocked.is_complete(), "the first attempt should fail");

    std::fs::remove_dir_all(target.path().join("a.txt")).expect("clear the obstruction");

    let recovered = try_sync(source.path(), target.path(), &config(false)).await;

    assert!(
        recovered.is_complete(),
        "the retry after the obstruction is gone must succeed: {:?}",
        recovered.failures
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join("a.txt")).expect("read"),
        "eventually this lands"
    );
}
