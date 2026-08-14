//! One test per defect that reached a released build, kept apart from the rest.
//!
//! Everything here reproduces a specific failure exactly as it was first
//! observed, and records what the binary did at the time. That is the point of
//! the file: a test named after a property can be quietly weakened by someone
//! who does not know which property, and a test that says "this used to write
//! outside the target root, and here is the shape of the sync that did it"
//! cannot be.
//!
//! Each case states the observed behaviour before the fix. If one of these
//! starts failing, the reading is not "the assertion is out of date". It is that
//! the defect is back.
//!
//! These duplicate coverage in `hostile.rs` and `syncer.rs` on purpose. Those
//! files test the properties, at whatever level reads most clearly; this one
//! tests the exact reported case, and neither is a substitute for the other.
//!
//! Two cases are not here because they cannot be reached from outside the
//! crate, and splitting them off would mean copying private constants that would
//! then rot. They live beside the code instead:
//!
//! - a compressed frame that expands past the frame limit, in
//!   `remote::protocol` (`a_frame_that_expands_past_the_limit_is_refused`),
//!   which needs the limit and the compression flag;
//! - the reply an agent sends when a reply will not fit, in `remote::agent`
//!   (`a_reply_too_large_to_send_becomes_an_error_the_client_can_read`).

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tempfile::TempDir;
use treesync::reconcile::{
    IndexOptions, Preserve, ReconcileConfig, Scope, index_scope, plan, walk,
};
use treesync::sink::{ApplyReport, LocalSink, apply};

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

fn preserving() -> ReconcileConfig {
    ReconcileConfig {
        delete: false,
        preserve: Preserve {
            mode: true,
            ownership: false,
        },
        ..Default::default()
    }
}

/// Reconciles two trees in full and hands back the report.
async fn sync(source: &Path, target: &Path, config: &ReconcileConfig) -> ApplyReport {
    let scope = Scope::Subtree(PathBuf::new());
    let source_index = walk(source, &IndexOptions::quick()).expect("walk source");
    let target_index = walk(target, &IndexOptions::quick()).expect("walk target");
    let plan = plan(&source_index, &target_index, &scope, config);
    let sink = LocalSink::new(target).expect("sink");

    apply(&plan, source, &sink, config.preserve).await
}

// ---------------------------------------------------------------------------
// 1. A symlinked directory on the target let a write escape the root
// ---------------------------------------------------------------------------

/// Observed before the fix:
///
/// ```text
/// complete=true escaped=true failures=[]
/// ```
///
/// The source held `a/secret.txt` and the target held `a` as a symlink pointing
/// out of the tree. `LocalSink::resolve` checked path *components*, which are
/// all ordinary names here, so the write went through the link and landed
/// outside the root. The pass then reported complete success, which is the worse
/// half: nothing downstream could tell this sync from one that worked.
///
/// The same shape reaches the agent without any `..` at all. A client sends
/// `CreateSymlink { path: "a", target: "/etc" }` and then
/// `WriteFile { path: "a/passwd" }`, and both paths pass the component check.
#[tokio::test]
async fn a_write_never_escapes_the_target_root_through_a_symlink() {
    let source = TempDir::new().expect("source");
    let target = TempDir::new().expect("target");
    let elsewhere = TempDir::new().expect("elsewhere");

    std::fs::create_dir_all(source.path().join("a")).expect("mkdir");
    std::fs::write(source.path().join("a/secret.txt"), "escaped!").expect("write");
    std::os::unix::fs::symlink(elsewhere.path(), target.path().join("a")).expect("symlink");

    // `delete` off, which is the default, and what made this reachable: with it
    // on, the reconciler removes the conflicting entry before writing.
    let report = sync(source.path(), target.path(), &config(false)).await;

    assert!(
        !elsewhere.path().join("secret.txt").exists(),
        "REGRESSION: a write escaped the target root through a symlinked directory"
    );
    assert!(
        !report.is_complete(),
        "REGRESSION: the escape was reported as a successful sync"
    );
}

// ---------------------------------------------------------------------------
// 2. A symlink at the temporary path destroyed a file outside the root
// ---------------------------------------------------------------------------

/// Observed before the fix:
///
/// ```text
/// complete=true victim_now=Ok("new content") target_a_is_symlink=true
/// ```
///
/// The transfer's temporary is named after its destination, so it is entirely
/// predictable. A symlink placed at that name was followed by `fs::copy`, so the
/// source content was written *through* it, overwriting a file outside the
/// target root. The rename that publishes a transfer then moved the link rather
/// than a file, leaving the target holding a symlink where a regular file
/// belonged. Reported as success.
///
/// Note that `hostile.rs` used to induce a genuine `ENOSPC` by exactly this
/// route, pointing the temporary at `/dev/full`. That the test suite depended on
/// the behaviour is why it survived as long as it did.
#[tokio::test]
async fn a_transfer_never_writes_through_a_symlink_at_its_temporary() {
    let source = TempDir::new().expect("source");
    let target = TempDir::new().expect("target");
    let elsewhere = TempDir::new().expect("elsewhere");

    std::fs::write(source.path().join("a.txt"), "new content").expect("write");

    let victim = elsewhere.path().join("victim.txt");
    std::fs::write(&victim, "precious data that must survive").expect("write");
    std::os::unix::fs::symlink(&victim, target.path().join(".treesync-tmp-a.txt"))
        .expect("symlink");

    let report = sync(source.path(), target.path(), &config(false)).await;

    assert_eq!(
        std::fs::read_to_string(&victim).expect("read"),
        "precious data that must survive",
        "REGRESSION: a transfer wrote through a planted symlink and destroyed a \
         file outside the target root"
    );
    assert!(
        !std::fs::symlink_metadata(target.path().join("a.txt"))
            .expect("metadata")
            .file_type()
            .is_symlink(),
        "REGRESSION: the symlink was published in place of the file"
    );
    assert!(report.is_complete(), "{:?}", report.failures);
    assert_eq!(
        std::fs::read_to_string(target.path().join("a.txt")).expect("read"),
        "new content"
    );
}

// ---------------------------------------------------------------------------
// 3. A vanished source root planned the deletion of the target
// ---------------------------------------------------------------------------

/// Observed before the fix:
///
/// ```text
/// PROBE3 source index EMPTY (len=0), planned actions=[Remove("b.txt"), Remove("a.txt")]
/// ```
///
/// `walk` has always refused to read an unreadable root as an empty tree,
/// because with deletions on that is a plan to empty the target. `stat_paths`,
/// which is what the incremental path uses, made no such check: a missing path
/// was simply skipped. So after the source was unmounted, renamed or removed,
/// the watcher's last batch indexed as empty and every path in it read as a
/// deletion to propagate.
///
/// The guard existed for the pass a daemon runs once, and was missing from the
/// one it runs continuously.
#[test]
fn a_vanished_source_root_is_not_read_as_a_deleted_tree() {
    let dir = TempDir::new().expect("temp dir");
    let source = dir.path().join("src");
    std::fs::create_dir(&source).expect("mkdir");
    std::fs::write(source.join("a.txt"), "one").expect("write");
    std::fs::write(source.join("b.txt"), "two").expect("write");

    std::fs::remove_dir_all(&source).expect("the source volume goes away");

    // Exactly what the watch loop does with a batch of reported paths.
    let scope = Scope::Paths(vec![PathBuf::from("a.txt"), PathBuf::from("b.txt")]);
    let indexed = index_scope(&source, &scope, &IndexOptions::quick());

    assert!(
        indexed.is_err(),
        "REGRESSION: a source root that is gone indexed as an empty tree, which \
         with deletions on plans the removal of everything on the target"
    );
}

// ---------------------------------------------------------------------------
// 4. A named pipe in the source hung the sync forever
// ---------------------------------------------------------------------------

/// Observed before the fix:
///
/// ```text
/// PROBE4 fifo indexed as: Some(File { size: 0, ... })
/// PROBE4 SYNC HUNG (3s timeout). ordinary.txt copied=true
/// ```
///
/// The walk classified anything that was not a directory or a symlink as a
/// regular file, so a FIFO entered the index with `size: 0`. Copying it then
/// blocked in `open`, which for a FIFO with no writer never returns. A plan is
/// applied one action at a time, so the whole sync stopped: no error, no
/// timeout, nothing in the log.
///
/// The probe that found this could not even shut its runtime down afterwards,
/// because the blocking thread never came back. That is why the timeout below is
/// generous but present: if this regresses, the process wedges, and a bounded
/// wait at least names the reason first.
#[tokio::test]
async fn a_named_pipe_never_reaches_a_transfer() {
    let source = TempDir::new().expect("source");
    let target = TempDir::new().expect("target");
    std::fs::write(source.path().join("ordinary.txt"), "please copy me").expect("write");

    let made = std::process::Command::new("mkfifo")
        .arg(source.path().join("pipe"))
        .status()
        .map(|status| status.success())
        .unwrap_or(false);

    if !made {
        eprintln!("SKIPPED a_named_pipe_never_reaches_a_transfer: no usable mkfifo");
        return;
    }

    let index = walk(source.path(), &IndexOptions::quick()).expect("walk");
    assert!(
        !index.contains(Path::new("pipe")),
        "REGRESSION: a FIFO was indexed as a regular file, and copying one blocks \
         forever"
    );

    let report = tokio::time::timeout(
        Duration::from_secs(10),
        sync(source.path(), target.path(), &config(false)),
    )
    .await
    .expect("REGRESSION: a FIFO in the source tree hung the sync");

    assert!(report.is_complete(), "{:?}", report.failures);
    assert_eq!(
        std::fs::read_to_string(target.path().join("ordinary.txt")).expect("read"),
        "please copy me"
    );
}

// ---------------------------------------------------------------------------
// 5. A long but legal filename could never be mirrored
// ---------------------------------------------------------------------------

/// Observed before the fix:
///
/// ```text
/// PROBE5 name_len=249 complete=false arrived=false
///   failures=["io error: File name too long (os error 36)"]
/// ```
///
/// The temporary carries a prefix, `.treesync-tmp-` locally and
/// `.treesync-incoming-` on the agent, so a source name within the filesystem's
/// 255 byte limit could produce a temporary past it. Any name of 242 characters
/// or more was therefore impossible to mirror, and the agent's lower ceiling put
/// its limit at 237.
///
/// Reported rather than silent, which is something, but permanently unfixable by
/// retrying: nothing about the file changed between attempts, so every pass
/// failed identically.
#[tokio::test]
async fn a_name_the_source_filesystem_allows_can_be_mirrored() {
    let source = TempDir::new().expect("source");
    let target = TempDir::new().expect("target");

    // 249 bytes, comfortably legal on the source and comfortably over the limit
    // once the prefix is added.
    let long = format!("{}.txt", "a".repeat(245));
    std::fs::write(source.path().join(&long), "content").expect("the source name is legal");

    let report = sync(source.path(), target.path(), &config(false)).await;

    assert!(
        report.is_complete(),
        "REGRESSION: a legal source name produced a temporary the kernel refuses, \
         so the file could never be published: {:?}",
        report.failures
    );
    assert_eq!(
        std::fs::read_to_string(target.path().join(&long)).expect("read"),
        "content"
    );
}

// ---------------------------------------------------------------------------
// 6. A read-only directory in the source never converged
// ---------------------------------------------------------------------------

/// Observed before the fix:
///
/// ```text
/// PROBE6 pass1 complete=true target_mode=555
/// PROBE6 pass2 complete=false arrived=false
///   failures=["locked/second.txt: permission denied", "locked/second.txt: not found"]
/// ```
///
/// The first pass mirrors the directory and its mode correctly, because metadata
/// is applied after everything inside it has been created. The second pass, with
/// a new file to put in that directory, cannot: the target directory is now
/// read-only, and the write fails. Every later pass fails the same way, so the
/// mirror diverges permanently from a source that is doing nothing unusual.
#[tokio::test]
async fn a_file_added_to_a_read_only_directory_still_converges() {
    let source = TempDir::new().expect("source");
    let target = TempDir::new().expect("target");
    let locked = source.path().join("locked");

    std::fs::create_dir(&locked).expect("mkdir");
    std::fs::write(locked.join("first.txt"), "one").expect("write");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).expect("chmod");

    let first = sync(source.path(), target.path(), &preserving()).await;
    assert!(first.is_complete(), "{:?}", first.failures);

    let mirrored = target.path().join("locked");
    assert_eq!(
        std::fs::metadata(&mirrored)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777,
        0o555,
        "the first pass has to reproduce the mode, or this is not the case at all"
    );

    // A second file appears in the source's read-only directory.
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    std::fs::write(locked.join("second.txt"), "two").expect("write");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).expect("chmod");

    let second = sync(source.path(), target.path(), &preserving()).await;

    // Leave both ends removable whatever the assertions do.
    let _ = std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755));
    let restored = std::fs::metadata(&mirrored)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;
    let _ = std::fs::set_permissions(&mirrored, std::fs::Permissions::from_mode(0o755));

    assert!(
        second.is_complete(),
        "REGRESSION: a file added to a read-only directory can never be mirrored: {:?}",
        second.failures
    );
    assert_eq!(
        std::fs::read_to_string(mirrored.join("second.txt")).expect("read"),
        "two"
    );
    assert_eq!(
        restored, 0o555,
        "and the mode has to be put back, or the mirror is left more permissive \
         than the source"
    );
}

// ---------------------------------------------------------------------------
// 7. A misplaced frame could have desynchronised a session
// ---------------------------------------------------------------------------

/// Not a defect that shipped, but the property that kept several from being
/// worse, pinned so it cannot be lost.
///
/// Every frame carries its own length, so a reader that cannot make sense of a
/// message still knows where the next one starts. That is what makes "one bad
/// request must not cost the session" achievable at all: the agent records the
/// error, answers it, and carries on from a known boundary.
///
/// The failure this guards against is not subtle but it is opaque. A reader that
/// resumed mid-message would decode garbage from then on, and the error the
/// client reported would name a byte offset in a frame that has nothing to do
/// with whatever actually went wrong.
///
/// A related case is covered in `remote::agent`
/// (`a_misplaced_frame_does_not_cost_the_session`), which drives a whole session
/// with a request sitting where a file chunk belongs and checks the request
/// behind it is still served.
#[tokio::test]
async fn a_frame_that_cannot_be_decoded_costs_only_itself() {
    use std::io::Cursor;
    use treesync::remote::protocol::{Chunk, Request, WirePath, WireTime, read_frame, write_frame};

    let mut buffer = Vec::new();

    // A request where a chunk belongs, which is what a desynchronised or hostile
    // peer sends.
    write_frame(
        &mut buffer,
        &Request::Remove {
            path: WirePath::new(Path::new("a")),
        },
    )
    .await
    .expect("write");

    let expected = Chunk::Commit {
        mtime: WireTime::new(std::time::UNIX_EPOCH),
    };
    write_frame(&mut buffer, &expected).await.expect("write");

    let mut reader = Cursor::new(buffer);

    // Read as the wrong type. Any outcome is fine; what matters is how far it
    // left the cursor.
    let _: treesync::error::Result<Option<Chunk>> = read_frame(&mut reader).await;

    let next: Option<Chunk> = read_frame(&mut reader)
        .await
        .expect("REGRESSION: a frame that could not be decoded consumed more than itself");

    assert_eq!(
        next,
        Some(expected),
        "REGRESSION: the stream lost sync after a bad frame, so every message \
         behind it decodes as garbage"
    );
}

// ---------------------------------------------------------------------------
// 8. An index too large to send wedged the daemon in a reconnect loop
// ---------------------------------------------------------------------------

/// Observed by reading the code rather than by running it, because reproducing
/// it needs a target tree of roughly half a million entries.
///
/// A `Response::Index` past the frame limit failed inside `serve`'s write, after
/// `handle` had already returned `Ok`. Nothing upstream knew anything was wrong,
/// so the agent simply exited. From the client that is indistinguishable from a
/// link that dropped: it classified the end of stream as a transport failure,
/// reconnected, reissued the same index request, and got the same result. Under
/// `watch`, whose policy is to retry until the host comes back, that is a daemon
/// that has stopped mirroring while looking exactly like one that is retrying.
///
/// The check itself is what this pins. That an unsendable reply comes back as a
/// `Response::Error` rather than silence is covered in `remote::agent`, and that
/// the client does not retry an error the agent *answered* with is covered by
/// the CLI's `an_agent_error_does_not_trigger_a_reconnect`.
#[test]
fn a_message_too_large_for_a_frame_is_refused_rather_than_sent() {
    use treesync::remote::protocol::{
        Response, WireEntry, WireIndex, WireMetadata, WirePath, encode_frame,
    };

    let mut index = WireIndex::default();

    // A few very large paths rather than the half a million ordinary entries it
    // would otherwise take. The encoder counts bytes, not entries.
    for _ in 0..70 {
        index.entries.push((
            WirePath(vec![b'a'; 1024 * 1024]),
            WireEntry::Dir {
                meta: WireMetadata {
                    mode: 0o755,
                    uid: 0,
                    gid: 0,
                },
            },
        ));
    }

    let error = encode_frame(&Response::Index(index))
        .expect_err("REGRESSION: a message over the frame limit was accepted for sending");

    assert!(
        error.to_string().contains("exclude"),
        "the error has to tell an operator what to do about it: {error}"
    );
}
