//! The metrics endpoint, scraped from a real daemon.
//!
//! Everything here runs the built binary and reads what a Prometheus server
//! would read. A unit test can assert that a counter was incremented; it cannot
//! tell you that the recorder was installed before the first pass, that the
//! descriptions survived, or that the numbers on the wire are the ones the sync
//! actually produced. Those are the ways this breaks in practice, so the test
//! is a scrape.
//!
//! No HTTP client dependency: the request is one line of text and the response
//! is read to EOF, which is what `Connection: close` is asked for.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

/// How long an assertion waits before giving up.
const DEADLINE: Duration = Duration::from_secs(15);

/// Batching window for these tests. Short, so a change lands quickly.
const DELAY: &str = "200ms";

/// Takes a port from the OS and gives it straight back.
///
/// Racy in principle, since something else could take it in between. In
/// practice nothing does on a test host, and the alternative is a hardcoded
/// port, which fails the moment two of these run at once or a developer
/// happens to have something on it.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let port = listener.local_addr().expect("local addr").port();

    drop(listener);

    port
}

/// `GET /metrics`, or the reason it could not be read.
fn scrape(addr: SocketAddr) -> Result<String, String> {
    let mut stream =
        TcpStream::connect_timeout(&addr, Duration::from_secs(2)).map_err(|err| err.to_string())?;

    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|err| err.to_string())?;

    // `Connection: close` so the server hangs up when it is done and the read
    // below ends at EOF. Without it a keep-alive connection would sit there
    // until the read timeout on every scrape.
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .map_err(|err| err.to_string())?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|err| err.to_string())?;

    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("no header break in {response:?}"))?;

    if !head.starts_with("HTTP/1.1 200") {
        return Err(format!("not a 200: {head}"));
    }

    Ok(body.to_string())
}

/// Scrapes until the endpoint answers, or fails the test.
fn scrape_eventually(addr: SocketAddr) -> String {
    let deadline = Instant::now() + DEADLINE;
    let mut last = String::from("never attempted");

    while Instant::now() < deadline {
        match scrape(addr) {
            Ok(body) => return body,
            Err(err) => last = err,
        }

        std::thread::sleep(Duration::from_millis(50));
    }

    panic!("the metrics endpoint at {addr} never answered: {last}");
}

/// Polls the endpoint until `condition` holds against the scraped body.
fn eventually(
    addr: SocketAddr,
    description: &str,
    mut condition: impl FnMut(&str) -> bool,
) -> String {
    let deadline = Instant::now() + DEADLINE;
    let mut last = String::new();

    while Instant::now() < deadline {
        if let Ok(body) = scrape(addr) {
            if condition(&body) {
                return body;
            }

            last = body;
        }

        std::thread::sleep(Duration::from_millis(50));
    }

    panic!("timed out waiting for {description}; last scrape was:\n{last}");
}

/// The value of the first sample whose line contains every part of `needle`.
///
/// Matched on substrings rather than parsed, because the label order in the
/// exposition format is not something this test should be pinned to.
fn sample(body: &str, needle: &[&str]) -> Option<f64> {
    body.lines()
        .filter(|line| !line.starts_with('#'))
        .find(|line| needle.iter().all(|part| line.contains(part)))
        .and_then(|line| line.rsplit_once(' '))
        .and_then(|(_, value)| value.trim().parse().ok())
}

struct Fixture {
    _dir: TempDir,
    source: PathBuf,
    target: PathBuf,
    config: PathBuf,
    addr: SocketAddr,
}

impl Fixture {
    /// `in_config` puts the address in the file; otherwise it is passed as a
    /// flag, so both routes to the same setting are covered.
    fn new(in_config: bool) -> Self {
        let dir = TempDir::new().expect("temp dir");
        let source = dir.path().join("src");
        let target = dir.path().join("dst");
        std::fs::create_dir_all(&source).expect("create source");

        let addr: SocketAddr = format!("127.0.0.1:{}", free_port())
            .parse()
            .expect("a literal address");

        let metrics = if in_config {
            format!("[metrics]\nlisten = \"{addr}\"\n")
        } else {
            String::new()
        };

        let config = dir.path().join("config.toml");
        std::fs::write(
            &config,
            format!(
                r#"{metrics}
[[sync]]
name = "measured"
source = "{}"
delay = "{DELAY}"
delete = true

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
            addr,
        }
    }

    fn write(&self, relative: &str, contents: &str) {
        let path = self.source.join(relative);

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }

        std::fs::write(path, contents).expect("write");
    }

    fn mirrored(&self, relative: &str) -> bool {
        self.target.join(relative).exists()
    }

    fn start(&self, with_flag: bool) -> Daemon {
        Daemon::start(&self.config, with_flag.then_some(self.addr))
    }
}

/// A running `treesync watch`.
struct Daemon {
    child: Option<Child>,
}

impl Daemon {
    fn start(config: &Path, listen: Option<SocketAddr>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_treesync"));
        command.arg("--config").arg(config);

        if let Some(listen) = listen {
            command.arg("--metrics-listen").arg(listen.to_string());
        }

        let child = command
            .arg("watch")
            // Cleared so a developer's environment cannot change what is read
            // or where the endpoint lands.
            .env_remove("TREESYNC_CONFIG")
            .env_remove("TREESYNC_METRICS_LISTEN")
            .env_remove("RUST_LOG")
            .env_remove("LOG_LEVEL")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("treesync should be runnable");

        Self { child: Some(child) }
    }

    fn stop(mut self) {
        let Some(child) = self.child.as_mut() else {
            return;
        };

        let pid = child.id().to_string();
        let _ = Command::new("kill").arg("-TERM").arg(&pid).status();

        let deadline = Instant::now() + DEADLINE;
        while Instant::now() < deadline {
            if child.try_wait().expect("wait").is_some() {
                self.child.take();

                return;
            }

            std::thread::sleep(Duration::from_millis(20));
        }

        let _ = child.kill();
        panic!("the daemon ignored SIGTERM");
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
        }
    }
}

#[test]
fn the_endpoint_serves_help_text_before_anything_has_happened() {
    // `describe` has to run after the recorder is installed and before the
    // first pass. Get that wrong and the scrape is a wall of bare numbers with
    // nothing saying what they are.
    let fixture = Fixture::new(true);
    let daemon = fixture.start(false);

    let body = scrape_eventually(fixture.addr);

    assert!(
        body.contains("# HELP treesync_sync_passes_total"),
        "no help text for a metric that must be described:\n{body}"
    );
    assert!(
        body.contains("# TYPE treesync_sync_pass_duration_seconds histogram"),
        "durations should render as histograms, not summaries:\n{body}"
    );
    assert!(
        body.contains("treesync_build_info"),
        "the running version should be published:\n{body}"
    );

    daemon.stop();
}

#[test]
fn a_mirrored_file_shows_up_in_the_counters() {
    let fixture = Fixture::new(true);
    let daemon = fixture.start(false);

    scrape_eventually(fixture.addr);
    fixture.write("a.txt", "hello");

    let body = eventually(fixture.addr, "the copy to be counted", |body| {
        sample(body, &["treesync_actions_applied_total", "copy_file"]).unwrap_or_default() >= 1.0
    });

    assert!(
        body.contains(r#"sync="measured""#),
        "every series should carry the sync's name from its config block:\n{body}"
    );

    let bytes = sample(&body, &["treesync_transfer_bytes_total"]).expect("transfer bytes");
    assert!(
        bytes >= 5.0,
        "the five bytes written should be accounted for, got {bytes}:\n{body}"
    );

    let logical = sample(&body, &["treesync_transfer_logical_bytes_total"]).expect("logical bytes");
    assert_eq!(
        logical, bytes,
        "a local copy moves the whole file, so the two totals match"
    );

    daemon.stop();
}

#[test]
fn a_pass_that_finds_nothing_still_counts_as_a_success() {
    // The one that matters for alerting. An idle mirror is a mirror that is up
    // to date, and if agreeing with the source did not refresh this gauge then
    // "time since last sync" would grow without bound on a healthy sync and
    // every alert built on it would be noise.
    let fixture = Fixture::new(true);
    let daemon = fixture.start(false);

    let body = eventually(fixture.addr, "the startup pass to be recorded", |body| {
        sample(body, &["treesync_sync_last_success_timestamp_seconds"]).unwrap_or_default() > 0.0
    });

    let first = sample(&body, &["treesync_sync_last_success_timestamp_seconds"]).expect("gauge");

    // Nothing is written to the tree. The only thing that moves this is a
    // later pass agreeing that there is nothing to do.
    fixture.write("trigger.txt", "one");
    eventually(fixture.addr, "a second pass", |body| {
        sample(body, &["treesync_sync_last_success_timestamp_seconds"]).unwrap_or_default() > first
    });

    assert!(
        sample(&body, &["treesync_sync_passes_total", r#"scope="full""#]).unwrap_or_default()
            >= 1.0,
        "the startup pass covers the whole tree and should be labelled as such:\n{body}"
    );

    daemon.stop();
}

#[test]
fn tree_totals_come_from_the_whole_tree_pass() {
    let fixture = Fixture::new(true);

    // Written before startup, so the first whole-tree pass sees them.
    fixture.write("one.txt", "aaaa");
    fixture.write("two.txt", "bbbb");
    fixture.write("nested/three.txt", "cc");

    let daemon = fixture.start(false);

    let body = eventually(fixture.addr, "the source tree to be measured", |body| {
        sample(body, &["treesync_tree_entries", r#"side="source""#]).unwrap_or_default() > 0.0
    });

    let entries = sample(&body, &["treesync_tree_entries", r#"side="source""#]).expect("entries");
    let bytes = sample(&body, &["treesync_tree_bytes", r#"side="source""#]).expect("bytes");

    // Three files and the directory holding one of them.
    assert_eq!(entries, 4.0, "{body}");
    assert_eq!(bytes, 10.0, "{body}");

    // A later incremental batch must not overwrite these with its own handful
    // of paths: the gauge would collapse to almost nothing every time a single
    // file changed.
    fixture.write("one.txt", "aaaaa");
    eventually(fixture.addr, "the edit to be mirrored", |_| {
        fixture.mirrored("one.txt")
            && std::fs::read_to_string(fixture.target.join("one.txt")).unwrap_or_default()
                == "aaaaa"
    });

    let after = scrape(fixture.addr).expect("scrape");
    assert_eq!(
        sample(&after, &["treesync_tree_entries", r#"side="source""#]),
        Some(4.0),
        "an incremental batch indexes a few paths and must not report them as the tree:\n{after}"
    );

    daemon.stop();
}

#[test]
fn the_flag_serves_metrics_when_the_config_does_not() {
    let fixture = Fixture::new(false);
    let daemon = fixture.start(true);

    let body = scrape_eventually(fixture.addr);
    assert!(body.contains("treesync_build_info"), "{body}");

    daemon.stop();
}

#[test]
fn metrics_are_off_unless_asked_for() {
    let fixture = Fixture::new(false);
    let daemon = fixture.start(false);

    // Give the daemon time to come up and start mirroring, so this is not just
    // a race against startup.
    fixture.write("a.txt", "one");
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline && !fixture.mirrored("a.txt") {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        fixture.mirrored("a.txt"),
        "the daemon never mirrored anything"
    );

    assert!(
        scrape(fixture.addr).is_err(),
        "nothing asked for a listener, so no port should have been opened"
    );

    daemon.stop();
}

#[test]
fn a_port_that_cannot_be_bound_fails_startup() {
    // A daemon that silently came up without its metrics looks exactly like one
    // that is working.
    let fixture = Fixture::new(false);
    let held = TcpListener::bind(fixture.addr).expect("hold the port");

    let output = Command::new(env!("CARGO_BIN_EXE_treesync"))
        .arg("--config")
        .arg(&fixture.config)
        .arg("--metrics-listen")
        .arg(fixture.addr.to_string())
        .arg("watch")
        .env_remove("TREESYNC_CONFIG")
        .env_remove("TREESYNC_METRICS_LISTEN")
        .env_remove("RUST_LOG")
        .env_remove("LOG_LEVEL")
        .output()
        .expect("treesync should be runnable");

    drop(held);

    assert!(!output.status.success(), "it should not have started");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("serving metrics on"),
        "the error should name what failed: {stderr}"
    );
}

#[test]
fn a_malformed_flag_is_rejected() {
    let fixture = Fixture::new(false);

    let output = Command::new(env!("CARGO_BIN_EXE_treesync"))
        .arg("--config")
        .arg(&fixture.config)
        .arg("--metrics-listen")
        .arg("not-an-address")
        .arg("watch")
        .env_remove("TREESYNC_CONFIG")
        .env_remove("TREESYNC_METRICS_LISTEN")
        .env_remove("RUST_LOG")
        .env_remove("LOG_LEVEL")
        .output()
        .expect("treesync should be runnable");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--metrics-listen") && stderr.contains("not-an-address"),
        "the error should name the flag and the value: {stderr}"
    );
}
