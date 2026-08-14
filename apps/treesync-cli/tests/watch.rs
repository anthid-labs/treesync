//! The `watch` command, driven as a real subprocess.
//!
//! A daemon's contract is mostly things a unit test cannot see: that it keeps
//! going, that a change made while it is running lands without anyone asking,
//! that it stops when told and finishes what it had in hand first. All of that
//! needs a process, a real watcher and a real clock, so these tests run the
//! built binary and poll the target for the state they expect.
//!
//! Every assertion is "eventually", never "after n milliseconds". Watchers
//! coalesce and delay events by design, and a test that pins an exact timing
//! is testing the machine it happens to run on.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

/// How long an assertion waits before giving up.
///
/// Generous, because it bounds a failure rather than describing an
/// expectation: in practice these settle in well under a second, and a slow
/// CI box should report a real failure rather than a timing one.
const DEADLINE: Duration = Duration::from_secs(15);

/// How long the batching window is in these tests.
///
/// Short so the tests are quick, but not so short that a burst of writes is
/// split across several passes for no reason.
const DELAY: &str = "200ms";

struct Fixture {
    _dir: TempDir,
    source: PathBuf,
    target: PathBuf,
    config: PathBuf,
}

impl Fixture {
    fn new(delete: bool) -> Self {
        Self::with_delay(delete, DELAY)
    }

    fn with_delay(delete: bool, delay: &str) -> Self {
        let dir = TempDir::new().expect("temp dir");
        let source = dir.path().join("src");
        let target = dir.path().join("dst");
        std::fs::create_dir_all(&source).expect("create source");

        let config = dir.path().join("config.toml");
        std::fs::write(
            &config,
            format!(
                r#"
[[sync]]
name = "live"
source = "{}"
delay = "{delay}"
delete = {delete}

  [sync.target]
  type = "local"
  path = "{}"
"#,
                source.display(),
                target.display()
            ),
        )
        .expect("write config");

        Self {
            _dir: dir,
            source,
            target,
            config,
        }
    }

    fn write(&self, relative: &str, contents: &str) {
        let path = self.source.join(relative);

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }

        std::fs::write(path, contents).expect("write");
    }

    fn target_contents(&self, relative: &str) -> Option<String> {
        std::fs::read_to_string(self.target.join(relative)).ok()
    }

    fn start(&self) -> Daemon {
        Daemon::start(&self.config)
    }
}

/// A running `treesync watch`.
///
/// The handle is an `Option` so `stop` can take ownership of the child while
/// `Drop` still has something to check: a test that panics partway through
/// must not leave a watcher running against a temp directory that is about to
/// be deleted.
struct Daemon {
    child: Option<Child>,
}

impl Daemon {
    fn start(config: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_treesync"))
            .arg("--config")
            .arg(config)
            .arg("watch")
            // Cleared so a developer's environment cannot change what is read
            // or how loud the daemon is.
            .env_remove("TREESYNC_CONFIG")
            .env_remove("RUST_LOG")
            .env_remove("LOG_LEVEL")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("treesync should be runnable");

        Self { child: Some(child) }
    }

    fn child(&mut self) -> &mut Child {
        self.child.as_mut().expect("the daemon is still running")
    }

    /// Sends SIGTERM and waits for the process to finish.
    ///
    /// `kill` rather than a `libc` dependency: these tests are Unix-only
    /// anyway, and the point is to send the same signal a supervisor would.
    fn stop(mut self) -> Stopped {
        let pid = self.child().id().to_string();

        let signalled = Command::new("kill")
            .arg("-TERM")
            .arg(&pid)
            .status()
            .expect("kill should run")
            .success();
        assert!(signalled, "could not signal the daemon");

        let deadline = Instant::now() + DEADLINE;
        loop {
            if self.child().try_wait().expect("wait").is_some() {
                let output = self
                    .child
                    .take()
                    .expect("still running")
                    .wait_with_output()
                    .expect("output");

                return Stopped {
                    code: output.status.code(),
                    stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                    stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                };
            }

            if Instant::now() >= deadline {
                let _ = self.child().kill();
                panic!("the daemon ignored SIGTERM; a supervisor would have to SIGKILL it");
            }

            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
        }
    }
}

struct Stopped {
    code: Option<i32>,
    stdout: String,
    #[allow(dead_code)]
    stderr: String,
}

/// Polls until `condition` holds, or fails the test naming what it waited for.
fn eventually(description: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + DEADLINE;

    while Instant::now() < deadline {
        if condition() {
            return;
        }

        std::thread::sleep(Duration::from_millis(20));
    }

    panic!("timed out waiting for {description}");
}

#[test]
fn a_file_created_while_running_is_mirrored() {
    let fixture = Fixture::new(false);
    let daemon = fixture.start();

    fixture.write("a.txt", "one");

    eventually("a.txt to arrive", || {
        fixture.target_contents("a.txt").as_deref() == Some("one")
    });

    daemon.stop();
}

#[test]
fn changes_keep_being_mirrored_pass_after_pass() {
    // The point of a daemon: not that it works once, but that it goes on
    // working without being asked again.
    let fixture = Fixture::new(false);
    let daemon = fixture.start();

    for round in 0..5 {
        let name = format!("file-{round}.txt");
        let contents = format!("round {round}");
        fixture.write(&name, &contents);

        eventually(&format!("{name} to arrive"), || {
            fixture.target_contents(&name).as_deref() == Some(contents.as_str())
        });
    }

    // Everything from earlier rounds is still there: a later pass must not
    // undo an earlier one.
    for round in 0..5 {
        assert_eq!(
            fixture.target_contents(&format!("file-{round}.txt")),
            Some(format!("round {round}")),
            "an earlier file went missing"
        );
    }

    daemon.stop();
}

#[test]
fn an_edit_to_an_existing_file_is_mirrored() {
    let fixture = Fixture::new(false);
    fixture.write("a.txt", "before");

    let daemon = fixture.start();
    eventually("the startup pass", || {
        fixture.target_contents("a.txt").is_some()
    });

    fixture.write("a.txt", "after");

    eventually("the edit to arrive", || {
        fixture.target_contents("a.txt").as_deref() == Some("after")
    });

    daemon.stop();
}

#[test]
fn a_file_created_in_a_new_directory_is_mirrored() {
    let fixture = Fixture::new(false);
    let daemon = fixture.start();

    fixture.write("sub/deep/c.txt", "three");

    eventually("the nested file to arrive", || {
        fixture.target_contents("sub/deep/c.txt").as_deref() == Some("three")
    });

    daemon.stop();
}

#[test]
fn a_deletion_is_mirrored_when_configured() {
    let fixture = Fixture::new(true);
    fixture.write("gone.txt", "one");
    fixture.write("kept.txt", "two");

    let daemon = fixture.start();
    eventually("the startup pass", || {
        fixture.target_contents("gone.txt").is_some()
    });

    std::fs::remove_file(fixture.source.join("gone.txt")).expect("remove");

    eventually("the deletion to propagate", || {
        !fixture.target.join("gone.txt").exists()
    });
    assert!(
        fixture.target.join("kept.txt").exists(),
        "only the deleted file should go"
    );

    daemon.stop();
}

#[test]
fn a_deletion_is_withheld_when_not_configured() {
    let fixture = Fixture::new(false);
    fixture.write("a.txt", "one");

    let daemon = fixture.start();
    eventually("the startup pass", || {
        fixture.target_contents("a.txt").is_some()
    });

    std::fs::remove_file(fixture.source.join("a.txt")).expect("remove");

    // Nothing to wait *for*, so this gives the daemon a window in which it
    // would have acted had it been going to.
    std::thread::sleep(Duration::from_millis(600));

    assert!(
        fixture.target.join("a.txt").exists(),
        "destructive propagation must stay opt-in in the daemon too"
    );

    daemon.stop();
}

#[test]
fn the_startup_pass_catches_what_changed_while_it_was_not_running() {
    let fixture = Fixture::new(false);
    // Written before the daemon exists, so no event will ever report it. Only
    // the full comparison at startup can find this.
    fixture.write("existing.txt", "from before");

    let daemon = fixture.start();

    eventually("the startup reconcile", || {
        fixture.target_contents("existing.txt").as_deref() == Some("from before")
    });

    daemon.stop();
}

#[test]
fn sigterm_stops_it_cleanly() {
    let fixture = Fixture::new(false);
    let daemon = fixture.start();

    fixture.write("a.txt", "one");
    eventually("the first pass", || {
        fixture.target_contents("a.txt").is_some()
    });

    let stopped = daemon.stop();

    assert_eq!(
        stopped.code,
        Some(0),
        "a signalled shutdown is not a failure; stderr: {}",
        stopped.stderr
    );
    assert!(
        stopped.stdout.contains("SIGTERM"),
        "the daemon should say why it stopped: {}",
        stopped.stdout
    );
}

#[test]
fn work_still_batched_at_shutdown_is_flushed() {
    // A window long enough that the write below is certainly still inside it
    // when the signal arrives.
    let fixture = Fixture::with_delay(false, "30s");
    let daemon = fixture.start();

    eventually("the daemon to be watching", || {
        // The startup pass creates the target root, so its existence is the
        // signal that the watch is established.
        fixture.target.exists()
    });

    fixture.write("late.txt", "written just before shutdown");

    // Long enough for the watcher to have reported it, far short of the
    // 30s batching window.
    std::thread::sleep(Duration::from_millis(500));

    let stopped = daemon.stop();

    // Without the shutdown flush this file would be lost until the next
    // startup pass rediscovered it by walking the whole tree.
    assert_eq!(
        fixture.target_contents("late.txt").as_deref(),
        Some("written just before shutdown"),
        "observed-but-unapplied work must survive shutdown; stderr: {}",
        stopped.stderr
    );
    assert_eq!(stopped.code, Some(0));
}

#[test]
fn it_announces_what_it_is_watching() {
    let fixture = Fixture::new(false);
    let daemon = fixture.start();

    fixture.write("a.txt", "one");
    eventually("the first pass", || {
        fixture.target_contents("a.txt").is_some()
    });

    let stopped = daemon.stop();

    assert!(stopped.stdout.contains("[live]"), "{}", stopped.stdout);
    assert!(stopped.stdout.contains("watching"), "{}", stopped.stdout);
}

#[test]
fn a_missing_source_fails_at_startup_rather_than_running_broken() {
    let dir = TempDir::new().expect("temp dir");
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[[sync]]
name = "live"
source = "{}/nope"

  [sync.target]
  type = "local"
  path = "{}/dst"
"#,
            dir.path().display(),
            dir.path().display()
        ),
    )
    .expect("write");

    let output = Command::new(env!("CARGO_BIN_EXE_treesync"))
        .arg("--config")
        .arg(&config)
        .arg("watch")
        .env_remove("TREESYNC_CONFIG")
        .output()
        .expect("runnable");

    assert!(
        !output.status.success(),
        "a daemon that cannot do its job must not sit there looking healthy"
    );
    assert!(
        !dir.path().join("dst").exists(),
        "a sync that failed to open must not have created its target"
    );
}

#[test]
fn an_unknown_sync_name_is_rejected() {
    let fixture = Fixture::new(false);

    let output = Command::new(env!("CARGO_BIN_EXE_treesync"))
        .arg("--config")
        .arg(&fixture.config)
        .args(["watch", "--name", "nope"])
        .env_remove("TREESYNC_CONFIG")
        .output()
        .expect("runnable");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("live"),
        "the error should list the syncs that do exist"
    );
}

#[test]
fn several_syncs_run_at_once() {
    let dir = TempDir::new().expect("temp dir");
    for name in ["one", "two"] {
        std::fs::create_dir_all(dir.path().join(format!("src-{name}"))).expect("create");
    }

    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[defaults]
delay = "{DELAY}"

[[sync]]
name = "one"
source = "{root}/src-one"
target = {{ type = "local", path = "{root}/dst-one" }}

[[sync]]
name = "two"
source = "{root}/src-two"
target = {{ type = "local", path = "{root}/dst-two" }}
"#,
            root = dir.path().display()
        ),
    )
    .expect("write");

    let daemon = Daemon::start(&config);

    std::fs::write(dir.path().join("src-one/a.txt"), "first").expect("write");
    std::fs::write(dir.path().join("src-two/b.txt"), "second").expect("write");

    eventually("both syncs to mirror their own tree", || {
        dir.path().join("dst-one/a.txt").exists() && dir.path().join("dst-two/b.txt").exists()
    });

    // Each sync is confined to its own pair of trees.
    assert!(!dir.path().join("dst-one/b.txt").exists());
    assert!(!dir.path().join("dst-two/a.txt").exists());

    daemon.stop();
}
