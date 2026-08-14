//! Drives the built `treesync` binary as a subprocess.
//!
//! Calling the command functions directly would not exercise argument parsing,
//! exit codes, or the split between stdout and stderr, which is most of what a
//! CLI's contract actually is.

use std::path::Path;
use std::process::{Command, Output};

use tempfile::TempDir;

struct Run {
    output: Output,
}

impl Run {
    fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.output.stdout).to_string()
    }

    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.output.stderr).to_string()
    }

    fn succeeded(&self) -> bool {
        self.output.status.success()
    }
}

fn treesync(args: &[&str]) -> Run {
    let output = Command::new(env!("CARGO_BIN_EXE_treesync"))
        // Cleared so a value in the developer's environment cannot change
        // which file a test reads.
        .env_remove("TREESYNC_CONFIG")
        .env_remove("RUST_LOG")
        .env_remove("LOG_LEVEL")
        .args(args)
        .output()
        .expect("treesync should be runnable");

    Run { output }
}

/// A source tree, an empty target, and a config wiring them together.
fn fixture(delete: bool) -> (TempDir, String) {
    let dir = TempDir::new().expect("temp dir");
    let source = dir.path().join("src");
    let target = dir.path().join("dst");
    std::fs::create_dir_all(source.join("sub")).expect("create source");
    std::fs::write(source.join("a.txt"), "one").expect("write");
    std::fs::write(source.join("sub/b.txt"), "two").expect("write");

    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[[sync]]
name = "demo"
source = "{}"
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

    let path = config.display().to_string();

    (dir, path)
}

fn source_of(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("src")
}

fn target_of(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("dst")
}

#[test]
fn check_reports_the_resolved_configuration() {
    let (_dir, config) = fixture(true);

    let run = treesync(&["--config", &config, "check"]);

    assert!(run.succeeded(), "stderr: {}", run.stderr());
    let stdout = run.stdout();
    assert!(stdout.contains("[demo]"), "stdout: {stdout}");
    assert!(stdout.contains("delete      true"), "stdout: {stdout}");
    assert!(
        stdout.contains("max pending 10000"),
        "defaults should be shown, not just what the file set: {stdout}"
    );
}

#[test]
fn a_missing_config_fails_with_a_message_naming_the_path() {
    let run = treesync(&["--config", "/nonexistent/treesync.toml", "check"]);

    assert!(!run.succeeded(), "a missing config must not exit 0");
    assert!(
        run.stderr().contains("/nonexistent/treesync.toml"),
        "stderr: {}",
        run.stderr()
    );
}

#[test]
fn a_misspelled_key_fails_and_lists_the_valid_ones() {
    let dir = TempDir::new().expect("temp dir");
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        r#"
[[sync]]
name = "x"
source = "/a"
dely = "5s"
target = { type = "local", path = "/b" }
"#,
    )
    .expect("write");

    let run = treesync(&["--config", &config.display().to_string(), "check"]);

    assert!(!run.succeeded());
    let stderr = run.stderr();
    assert!(stderr.contains("dely"), "must name the bad key: {stderr}");
    assert!(
        stderr.contains("max_pending"),
        "must list what was expected: {stderr}"
    );
}

#[test]
fn a_dry_run_changes_nothing() {
    let (dir, config) = fixture(true);

    let run = treesync(&["--config", &config, "sync", "--dry-run"]);

    assert!(run.succeeded(), "stderr: {}", run.stderr());
    assert!(run.stdout().contains("(dry run)"), "{}", run.stdout());
    assert!(
        !target_of(&dir).exists(),
        "a dry run must not even create the target root"
    );
}

#[test]
fn sync_copies_the_tree_and_then_settles() {
    let (dir, config) = fixture(true);

    let first = treesync(&["--config", &config, "sync"]);
    assert!(first.succeeded(), "stderr: {}", first.stderr());
    // Not an exact count: preserving modes adds a stamp per path, so pinning a
    // number here would break every time preservation defaults change. What
    // matters is that work happened and that it converged.
    assert!(first.stdout().contains("applied "), "{}", first.stdout());

    assert_eq!(
        std::fs::read_to_string(target_of(&dir).join("sub/b.txt")).expect("read"),
        "two"
    );

    let second = treesync(&["--config", &config, "sync"]);
    assert!(second.succeeded());
    assert!(
        second.stdout().contains("0 action(s)"),
        "a second pass must find nothing to do: {}",
        second.stdout()
    );
}

#[test]
fn sync_creates_a_target_root_that_does_not_exist() {
    let (dir, config) = fixture(true);
    assert!(!target_of(&dir).exists());

    assert!(treesync(&["--config", &config, "sync"]).succeeded());

    assert!(target_of(&dir).is_dir());
}

#[test]
fn deletions_propagate_only_when_configured() {
    for delete in [false, true] {
        let (dir, config) = fixture(delete);
        assert!(treesync(&["--config", &config, "sync"]).succeeded());

        std::fs::remove_file(source_of(&dir).join("a.txt")).expect("remove");
        assert!(treesync(&["--config", &config, "sync"]).succeeded());

        assert_eq!(
            target_of(&dir).join("a.txt").exists(),
            !delete,
            "delete = {delete} did not behave"
        );
    }
}

#[test]
fn an_unknown_sync_name_fails_and_lists_the_real_ones() {
    let (_dir, config) = fixture(true);

    let run = treesync(&["--config", &config, "sync", "--name", "typo"]);

    assert!(!run.succeeded());
    assert!(
        run.stderr().contains("demo"),
        "must list the configured names: {}",
        run.stderr()
    );
}

/// A config naming a source that exists and a host that cannot.
///
/// `.invalid` is reserved by RFC 2606, so it never resolves anywhere: the
/// failure is the resolver's, in milliseconds, and no test can accidentally
/// reach a real host.
fn unreachable_remote() -> (TempDir, String) {
    let dir = TempDir::new().expect("temp dir");
    let source = dir.path().join("src");
    std::fs::create_dir_all(&source).expect("create source");
    std::fs::write(source.join("a.txt"), "one").expect("write");

    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[[sync]]
name = "remote"
source = "{}"
target = {{ type = "ssh", host = "deploy@treesync-test.invalid", path = "/srv/app" }}
"#,
            source.display()
        ),
    )
    .expect("write");

    let path = config.display().to_string();

    (dir, path)
}

#[test]
fn an_ssh_target_checks_without_contacting_the_host() {
    let (_dir, config) = unreachable_remote();

    // Validation is structural: it must not need the host to be up, or
    // `check` would be useless for exactly the case it is run for.
    let checked = treesync(&["--config", &config, "check"]);

    assert!(checked.succeeded(), "stderr: {}", checked.stderr());
    assert!(checked.stdout().contains("(ssh"), "{}", checked.stdout());
}

#[test]
fn syncing_to_an_unreachable_host_fails_naming_the_host() {
    let (_dir, config) = unreachable_remote();

    let synced = treesync(&["--config", &config, "sync"]);

    assert!(!synced.succeeded());
    assert!(
        synced.stderr().contains("treesync-test.invalid"),
        "an operator needs to know which host did not answer: {}",
        synced.stderr()
    );
}

#[test]
fn a_dry_run_against_a_remote_refuses_to_install_an_agent() {
    let (_dir, config) = unreachable_remote();

    // Installing the agent writes a binary to the host. --dry-run promises to
    // change nothing, so it has to decline rather than quietly provision.
    let synced = treesync(&["--config", &config, "sync", "--dry-run"]);

    assert!(!synced.succeeded());
    assert!(
        synced.stderr().contains("--dry-run will not install"),
        "stderr: {}",
        synced.stderr()
    );
}

#[test]
fn the_config_path_can_come_from_the_environment() {
    let (_dir, config) = fixture(true);

    let output = Command::new(env!("CARGO_BIN_EXE_treesync"))
        .env("TREESYNC_CONFIG", &config)
        .arg("check")
        .output()
        .expect("runnable");

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("[demo]"));
}

#[test]
fn command_output_goes_to_stdout_and_diagnostics_to_stderr() {
    let (_dir, config) = fixture(true);

    let run = treesync(&["--config", &config, "check"]);

    // So `treesync check > report.txt` captures the report and still shows logs.
    assert!(run.stdout().contains("[demo]"));
    assert!(!run.stderr().contains("[demo]"), "stderr: {}", run.stderr());
}

#[test]
fn running_with_no_subcommand_fails() {
    let run = treesync(&[]);

    assert!(
        !run.succeeded(),
        "a bare invocation must not be mistaken for a request to sync"
    );
}

#[test]
fn the_example_config_passes_check() {
    let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../treesync.example.toml");

    // Only `check`, since the example points at /var/www, which does not exist here.
    let run = treesync(&["--config", &example.display().to_string(), "check"]);

    assert!(run.succeeded(), "stderr: {}", run.stderr());
}

#[test]
fn check_reports_verify_and_exclusions_accurately() {
    let dir = TempDir::new().expect("temp dir");
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        r#"
[[sync]]
name = "filtered"
source = "/srv/app"
verify = "checksum"
exclude = ["*.tmp", "node_modules/"]
target = { type = "local", path = "/backup/app" }
"#,
    )
    .expect("write");

    let run = treesync(&["--config", &config.display().to_string(), "check"]);

    assert!(run.succeeded(), "stderr: {}", run.stderr());
    let stdout = run.stdout();

    assert!(
        stdout.contains("Checksum"),
        "verify must be shown: {stdout}"
    );
    assert!(
        stdout.contains("*.tmp"),
        "exclusions must be shown: {stdout}"
    );
    // Guards against the disclaimer outliving the limitation it described.
    assert!(
        !stdout.contains("NOT ENFORCED"),
        "exclusions are enforced now; the caveat must not linger: {stdout}"
    );
}
