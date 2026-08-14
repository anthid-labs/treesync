//! Runs a [`Syncer`] against real directories.
//!
//! This is the first test of the whole chain, watcher through queue,
//! reconciler and sink, driven by actual filesystem activity instead of
//! synthetic events.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use treesync::config::file::{ResolvedSync, Target};
use treesync::queue::QueueConfig;
use treesync::reconcile::ReconcileConfig;
use treesync::syncer::{Mode, Syncer};

/// Long enough to absorb FSEvents' latency, short enough that a hung test is
/// noticed rather than waited out.
const BUDGET: Duration = Duration::from_secs(15);

struct Fixture {
    _dir: TempDir,
    source: PathBuf,
    target: PathBuf,
}

fn fixture() -> Fixture {
    let dir = TempDir::new().expect("temp dir");
    let source = dir.path().join("src");
    let target = dir.path().join("dst");
    std::fs::create_dir_all(&source).expect("create source");

    Fixture {
        _dir: dir,
        source,
        target,
    }
}

fn config(fixture: &Fixture, delete: bool) -> ResolvedSync {
    ResolvedSync {
        name: "test".to_string(),
        source: fixture.source.clone(),
        target: Target::Local {
            path: fixture.target.clone(),
        },
        exclude: Vec::new(),
        queue: QueueConfig {
            // Short so the tests are not dominated by the batching window.
            delay: Duration::from_millis(100),
            max_pending: 10_000,
        },
        reconcile: ReconcileConfig {
            delete,
            ..Default::default()
        },
        // Irrelevant to a local target, which always copies whole.
        delta: Default::default(),
    }
}

/// Polls until `condition` holds, or the budget expires.
async fn eventually(what: &str, condition: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + BUDGET;

    while tokio::time::Instant::now() < deadline {
        if condition() {
            return;
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    panic!("{what} did not happen within {BUDGET:?}");
}

fn contents(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// Starts a syncer in the background, returning its cancellation handle.
fn start(config: ResolvedSync) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let cancel = CancellationToken::new();
    let token = cancel.clone();

    let handle = tokio::spawn(async move {
        // The syncer's own token, not a fresh one: cancelling it is what these
        // tests use to stop the loop, and it also has to reach a remote sink
        // waiting out an outage.
        let syncer = Syncer::open(&config, Mode::Watch, token)
            .await
            .expect("syncer should build");
        syncer.run().await.expect("syncer should not fail");
    });

    (cancel, handle)
}

#[tokio::test]
async fn the_startup_pass_copies_what_was_already_there() {
    let fixture = fixture();
    // Written before the syncer exists, so no event will ever describe it.
    std::fs::write(fixture.source.join("preexisting.txt"), "data").expect("write");
    std::fs::create_dir(fixture.source.join("sub")).expect("mkdir");
    std::fs::write(fixture.source.join("sub/nested.txt"), "more").expect("write");

    let (cancel, handle) = start(config(&fixture, true));

    let nested = fixture.target.join("sub/nested.txt");
    eventually("startup pass", || {
        contents(&nested).as_deref() == Some("more")
    })
    .await;

    cancel.cancel();
    handle.await.expect("task should finish");
}

#[tokio::test]
async fn a_file_created_while_running_is_copied() {
    let fixture = fixture();
    let (cancel, handle) = start(config(&fixture, true));

    // Let the startup pass finish so this is genuinely event-driven.
    tokio::time::sleep(Duration::from_millis(300)).await;
    std::fs::write(fixture.source.join("live.txt"), "created").expect("write");

    let copied = fixture.target.join("live.txt");
    eventually("live copy", || {
        contents(&copied).as_deref() == Some("created")
    })
    .await;

    cancel.cancel();
    handle.await.expect("task should finish");
}

#[tokio::test]
async fn a_modified_file_is_resynced() {
    let fixture = fixture();
    std::fs::write(fixture.source.join("a.txt"), "before").expect("write");

    let (cancel, handle) = start(config(&fixture, true));
    let copied = fixture.target.join("a.txt");
    eventually("initial copy", || {
        contents(&copied).as_deref() == Some("before")
    })
    .await;

    std::fs::write(fixture.source.join("a.txt"), "after the change").expect("write");

    eventually("treesync", || {
        contents(&copied).as_deref() == Some("after the change")
    })
    .await;

    cancel.cancel();
    handle.await.expect("task should finish");
}

#[tokio::test]
async fn a_deleted_file_is_removed_from_the_target() {
    let fixture = fixture();
    std::fs::write(fixture.source.join("doomed.txt"), "data").expect("write");

    let (cancel, handle) = start(config(&fixture, true));
    let copied = fixture.target.join("doomed.txt");
    eventually("initial copy", || copied.exists()).await;

    std::fs::remove_file(fixture.source.join("doomed.txt")).expect("remove");

    eventually("removal", || !copied.exists()).await;

    cancel.cancel();
    handle.await.expect("task should finish");
}

#[tokio::test]
async fn a_deletion_is_not_propagated_when_delete_is_off() {
    let fixture = fixture();
    std::fs::write(fixture.source.join("kept.txt"), "data").expect("write");

    let (cancel, handle) = start(config(&fixture, false));
    let copied = fixture.target.join("kept.txt");
    eventually("initial copy", || copied.exists()).await;

    std::fs::remove_file(fixture.source.join("kept.txt")).expect("remove");
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert!(
        copied.exists(),
        "deletion is opt-in; the target must keep the file"
    );

    cancel.cancel();
    handle.await.expect("task should finish");
}

#[tokio::test]
async fn a_source_root_that_vanishes_does_not_empty_the_target() {
    // The most destructive thing a mirror can do, and the easiest to trigger: a
    // source volume unmounts, or the directory is renamed or removed while the
    // daemon is running. The watcher reports its paths, they no longer resolve,
    // and with deletions on each one reads as a file the source dropped.
    //
    // A full pass has always refused to treat an unreadable root as an empty
    // tree. The incremental path beside it did not, which is the path a running
    // daemon actually uses.
    let fixture = fixture();
    std::fs::write(fixture.source.join("a.txt"), "one").expect("write");
    std::fs::write(fixture.source.join("b.txt"), "two").expect("write");

    let (cancel, handle) = start(config(&fixture, true));

    let mirrored = fixture.target.join("a.txt");
    eventually("initial copy", || mirrored.exists()).await;
    eventually("initial copy", || fixture.target.join("b.txt").exists()).await;

    // The source goes away underneath the running syncer.
    std::fs::remove_dir_all(&fixture.source).expect("remove the source root");

    // Long enough for several batching windows, so this is not just a race that
    // has not been lost yet.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    assert_eq!(
        contents(&mirrored).as_deref(),
        Some("one"),
        "a source that cannot be read is not a source that deleted everything"
    );
    assert_eq!(
        contents(&fixture.target.join("b.txt")).as_deref(),
        Some("two")
    );

    cancel.cancel();
    handle.await.expect("task should finish");
}

#[tokio::test]
async fn a_nested_directory_created_while_running_is_copied() {
    let fixture = fixture();
    let (cancel, handle) = start(config(&fixture, true));
    tokio::time::sleep(Duration::from_millis(300)).await;

    std::fs::create_dir_all(fixture.source.join("a/b/c")).expect("mkdir");
    std::fs::write(fixture.source.join("a/b/c/deep.txt"), "deep").expect("write");

    let copied = fixture.target.join("a/b/c/deep.txt");
    eventually("nested copy", || {
        contents(&copied).as_deref() == Some("deep")
    })
    .await;

    cancel.cancel();
    handle.await.expect("task should finish");
}

#[tokio::test]
async fn cancelling_stops_the_syncer() {
    let fixture = fixture();
    let (cancel, handle) = start(config(&fixture, true));

    tokio::time::sleep(Duration::from_millis(300)).await;
    cancel.cancel();

    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("cancellation must stop the loop promptly")
        .expect("task should finish");
}

#[tokio::test]
async fn the_target_root_is_created_if_missing() {
    let fixture = fixture();
    assert!(!fixture.target.exists());

    let syncer = Syncer::open(
        &config(&fixture, true),
        Mode::Watch,
        CancellationToken::new(),
    )
    .await
    .expect("build");

    assert!(fixture.target.is_dir());
    assert_eq!(syncer.name(), "test");
}

#[tokio::test]
async fn a_missing_source_fails_at_construction() {
    let fixture = fixture();
    let mut config = config(&fixture, true);
    config.source = fixture.source.join("nope");

    let err = Syncer::open(&config, Mode::Watch, CancellationToken::new())
        .await
        .expect_err("should fail");

    assert!(
        matches!(err, treesync::error::Error::NotFound(_)),
        "a misconfigured sync must be apparent at startup, got {err:?}"
    );
}

#[tokio::test]
async fn an_unreachable_ssh_target_fails_at_construction() {
    let fixture = fixture();
    let mut config = config(&fixture, true);
    config.target = Target::Ssh {
        // `.invalid` is reserved by RFC 2606 and can never resolve, so this
        // fails on the resolver in milliseconds and cannot accidentally reach
        // a real host from a test.
        host: "deploy@treesync-test.invalid".to_string(),
        path: PathBuf::from("/srv/app"),
        port: None,
        identity_file: None,
        agent_path: None,
        agent_binary: None,
        ssh_options: Vec::new(),
    };

    let err = Syncer::open(&config, Mode::Watch, CancellationToken::new())
        .await
        .expect_err("should fail");

    // The point is *when* it fails. Opening the remote at construction is what
    // makes an unreachable host a startup error rather than something
    // discovered on the first file change, hours later.
    assert!(
        err.to_string().contains("treesync-test.invalid"),
        "the error has to name the host that could not be reached: {err}"
    );
}

#[tokio::test]
async fn cancelling_applies_work_that_was_still_batched() {
    let fixture = fixture();
    let mut config = config(&fixture, true);
    // Long enough that the change below is definitely still inside the window
    // when cancellation arrives.
    config.queue.delay = Duration::from_secs(30);

    let (cancel, handle) = start(config);

    // Let the startup pass settle, then change something and cancel before the
    // window could ever close on its own.
    tokio::time::sleep(Duration::from_millis(400)).await;
    std::fs::write(fixture.source.join("late.txt"), "batched").expect("write");
    tokio::time::sleep(Duration::from_millis(400)).await;

    cancel.cancel();

    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("shutdown must not hang")
        .expect("task should finish");

    assert_eq!(
        contents(&fixture.target.join("late.txt")).as_deref(),
        Some("batched"),
        "a change observed before cancellation must be applied on the way out, \
         not left for the next startup walk to rediscover"
    );
}

#[tokio::test]
async fn an_idle_syncer_shuts_down_promptly() {
    let fixture = fixture();
    let mut config = config(&fixture, true);
    // If shutdown waited out a batching window, this delay would dominate.
    config.queue.delay = Duration::from_secs(30);

    let (cancel, handle) = start(config);
    tokio::time::sleep(Duration::from_millis(400)).await;

    let started = std::time::Instant::now();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("shutdown must not hang")
        .expect("task should finish");

    assert!(
        started.elapsed() < Duration::from_secs(3),
        "an idle sync must not sit out the batching window on the way out, took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn shutdown_is_bounded_when_changes_keep_arriving() {
    let fixture = fixture();
    let (cancel, handle) = start(config(&fixture, true));
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Keep writing throughout the shutdown so the drain always has more work.
    let source = fixture.source.clone();
    let churn = tokio::spawn(async move {
        for i in 0..2_000 {
            let _ = std::fs::write(source.join(format!("churn-{i}.txt")), "x");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = std::time::Instant::now();
    cancel.cancel();

    tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("shutdown must be bounded even under continuous change")
        .expect("task should finish");

    churn.abort();

    assert!(
        started.elapsed() < Duration::from_secs(15),
        "the grace period must cap the flush, took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn the_startup_pass_reports_a_source_it_cannot_read() {
    let fixture = fixture();
    let config = config(&fixture, true);
    let syncer = Syncer::open(&config, Mode::Watch, CancellationToken::new())
        .await
        .expect("build");

    // Removed after construction, so only the startup walk can notice.
    std::fs::remove_dir_all(&fixture.source).expect("remove source");

    let result = syncer.run().await;

    assert!(
        result.is_err(),
        "an unreadable source at startup is fatal: no later event fixes it"
    );
}

#[tokio::test]
async fn a_path_that_failed_is_retried_on_a_later_batch() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = fixture();
    std::fs::write(fixture.source.join("blocked.txt"), "data").expect("write");

    // A directory sitting where the source has a file, with `delete` off so
    // nothing is allowed to clear it. The rename that would publish the file
    // cannot replace a directory, so the startup pass records a failure for this
    // path.
    //
    // The obstruction is on the target and has nothing to do with permissions,
    // for two reasons. Making the *source* unreadable would generate a watcher
    // event when it was made readable again, so the path would come back on its
    // own and the retry would prove nothing. Making the *target* read-only no
    // longer fails at all: treesync widens a directory it owns for the length of
    // a write and puts the mode back.
    let blocked = fixture.target.join("blocked.txt");
    std::fs::create_dir_all(&blocked).expect("create the obstruction");
    std::fs::write(blocked.join("in-the-way.txt"), "occupied").expect("write");

    let (cancel, handle) = start(config(&fixture, false));
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(
        blocked.is_dir(),
        "the copy should have failed while the target path was a directory"
    );

    // The obstruction clears. Nothing touches blocked.txt in the source again,
    // so only the carried-forward retry can bring it across.
    //
    // The failed pass also stamped the source file's mode onto the directory
    // standing in its place, which takes the execute bit off and leaves it
    // untraversable, so the mode goes back before the directory can be removed.
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    std::fs::remove_dir_all(&blocked).expect("clear the obstruction");
    std::fs::write(fixture.source.join("unrelated.txt"), "trigger").expect("write");

    eventually("retry of the failed path", || {
        contents(&blocked).as_deref() == Some("data")
    })
    .await;

    cancel.cancel();
    handle.await.expect("task should finish");
}

#[tokio::test]
async fn many_files_churning_faster_than_the_interval_still_converge() {
    // Changes arriving faster than a pass completes. The queue coalesces a
    // burst into the distinct paths that changed, so the work per pass is
    // bounded by the *tree* rather than by how many times each file was
    // touched, but the property that matters is simply that it settles, with
    // every file holding its final content.
    let fixture = fixture();
    let (cancel, handle) = start(config(&fixture, true));
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Ten files rewritten repeatedly, well inside the 100ms batching window.
    let mut expected = Vec::new();
    for round in 0..12 {
        for file in 0..10 {
            let name = format!("churn-{file}.txt");
            let body = format!("file {file} round {round}");
            std::fs::write(fixture.source.join(&name), &body).expect("write");

            if round == 11 {
                expected.push((name, body));
            }
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    for (name, body) in &expected {
        let path = fixture.target.join(name);
        eventually(&format!("{name} converged"), || {
            contents(&path).as_deref() == Some(body.as_str())
        })
        .await;
    }

    cancel.cancel();
    handle.await.expect("task should finish");
}

#[tokio::test]
async fn a_deep_tree_created_all_at_once_is_mirrored_whole() {
    // The nested-directory race, widened: several branches created in one go,
    // each populated immediately, so watches are installed well after the
    // files exist. Nothing here may be lost silently.
    let fixture = fixture();
    let (cancel, handle) = start(config(&fixture, true));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut expected = Vec::new();
    for branch in 0..4 {
        let dir = fixture.source.join(format!("b{branch}/deep/deeper"));
        std::fs::create_dir_all(&dir).expect("mkdir");

        for file in 0..3 {
            let name = format!("b{branch}/deep/deeper/f{file}.txt");
            let body = format!("branch {branch} file {file}");
            std::fs::write(fixture.source.join(&name), &body).expect("write");
            expected.push((name, body));
        }
    }

    for (name, body) in &expected {
        let path = fixture.target.join(name);
        eventually(&format!("{name} arrived"), || {
            contents(&path).as_deref() == Some(body.as_str())
        })
        .await;
    }

    cancel.cancel();
    handle.await.expect("task should finish");
}
