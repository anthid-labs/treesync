//! The remote path, driven end to end against the real agent.
//!
//! The agent is started as a local child process rather than over SSH. That
//! keeps the test hermetic, with no host, no keys and no network, while still
//! exercising every part that can be wrong: the framing, the serialisation of
//! paths and timestamps, the streaming of file content, the index coming back
//! from another process, and the agent applying a plan through `LocalSink`.
//!
//! What it deliberately does not cover is SSH itself: argument construction,
//! shell quoting and agent installation. Those are unit-tested in
//! `remote::ssh` and `remote::ship`, and proven together against a real sshd
//! by `docker/remote-test.sh`.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use tempfile::TempDir;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use treesync::error::Error;
use treesync::reconcile::{
    Entry, Filter, IndexOptions, Metadata, Preserve, ReconcileConfig, Scope, Verify, plan, walk,
};
use treesync::remote::{Reconnect, SshSink};
use treesync::sink::{Sink, apply};

/// A source tree, an empty target, and an agent serving the target.
struct Remote {
    _dir: TempDir,
    source: PathBuf,
    target: PathBuf,
    sink: SshSink,
}

impl Remote {
    async fn new() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let source = dir.path().join("src");
        let target = dir.path().join("dst");
        std::fs::create_dir_all(&source).expect("create source");

        let sink = agent_for(&target).await;

        Self {
            _dir: dir,
            source,
            target,
            sink,
        }
    }

    fn write(&self, relative: &str, contents: &str) {
        let path = self.source.join(relative);

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }

        std::fs::write(path, contents).expect("write");
    }

    fn target_path(&self, relative: &str) -> PathBuf {
        self.target.join(relative)
    }

    /// Reconciles the whole tree and asserts every action succeeded.
    async fn sync(&self, config: &ReconcileConfig) -> usize {
        let report = self.sync_allowing_failures(config).await;

        assert!(
            report.is_complete(),
            "actions failed: {:?}",
            report
                .failures
                .iter()
                .map(|failure| format!("{}: {}", failure.action.path().display(), failure.error))
                .collect::<Vec<_>>()
        );

        report.applied
    }

    async fn sync_allowing_failures(
        &self,
        config: &ReconcileConfig,
    ) -> treesync::sink::ApplyReport {
        let options = IndexOptions {
            filter: Filter::allow_all(),
            verify: config.verify,
        };

        let scope = Scope::Subtree(PathBuf::new());
        let source_index = walk(&self.source, &options).expect("walk source");
        let target_index = self
            .sink
            .index(&scope, &options)
            .await
            .expect("index target");

        let plan = plan(&source_index, &target_index, &scope, config);

        apply(&plan, &self.source, &self.sink, config.preserve).await
    }
}

/// Starts the agent on `root` and connects to it.
///
/// `CARGO_BIN_EXE_treesync` is the binary under test, so the agent here is the
/// same executable an operator would have shipped to the host.
async fn agent_for(root: &Path) -> SshSink {
    let root = root.to_path_buf();

    SshSink::over_command(
        move || agent_command(&root, None),
        "local agent".to_string(),
    )
    .await
    .expect("the agent should start and handshake")
}

/// The command that starts an agent on `root`.
///
/// When `pidfile` is given the agent is wrapped in a shell that records its
/// process id first. `exec` replaces the shell, so the recorded id is the
/// agent's own, which lets a test kill it and watch the client
/// notice.
fn agent_command(root: &Path, pidfile: Option<&Path>) -> Command {
    let mut command = match pidfile {
        None => {
            let mut command = Command::new(env!("CARGO_BIN_EXE_treesync"));
            command.arg("agent").arg("--root").arg(root);
            command
        }
        Some(pidfile) => {
            let mut command = Command::new("sh");
            // Written to a temporary and renamed, not straight to the file.
            // `> file` truncates before `echo` writes, so a reader looking in
            // that window sees an empty file, and a chaos thread then runs
            // `kill` with no argument at all. A rename is atomic: a reader
            // sees the previous pid or the new one, never nothing. The
            // temporary carries `$$` so two shells restarting back to back
            // cannot collide on one name.
            command.arg("-c").arg(format!(
                "echo $$ > {pidfile}.$$.tmp && mv {pidfile}.$$.tmp {pidfile}; \
                 exec {binary} agent --root {root}",
                pidfile = shell_quote(&pidfile.to_string_lossy()),
                binary = shell_quote(env!("CARGO_BIN_EXE_treesync")),
                root = shell_quote(&root.to_string_lossy()),
            ));
            command
        }
    };

    // Otherwise a developer's RUST_LOG makes the agent chatty on a stream the
    // test is measuring.
    command.env_remove("RUST_LOG").env_remove("LOG_LEVEL");

    command
}

/// Quotes one shell word, the same way the SSH path does.
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

fn preserving_mode() -> ReconcileConfig {
    ReconcileConfig {
        delete: false,
        verify: Verify::Quick,
        preserve: Preserve {
            mode: true,
            ownership: false,
        },
    }
}

fn deleting() -> ReconcileConfig {
    ReconcileConfig {
        delete: true,
        ..preserving_mode()
    }
}

#[tokio::test]
async fn a_tree_is_mirrored_through_the_agent() {
    let remote = Remote::new().await;
    remote.write("a.txt", "one");
    remote.write("sub/b.txt", "two");
    remote.write("sub/deep/c.txt", "three");

    remote.sync(&preserving_mode()).await;

    assert_eq!(
        std::fs::read_to_string(remote.target_path("a.txt")).expect("read"),
        "one"
    );
    assert_eq!(
        std::fs::read_to_string(remote.target_path("sub/deep/c.txt")).expect("read"),
        "three"
    );
}

#[tokio::test]
async fn a_second_pass_transfers_nothing() {
    let remote = Remote::new().await;
    remote.write("a.txt", "one");
    remote.write("sub/b.txt", "two");

    let first = remote.sync(&preserving_mode()).await;
    assert!(first > 0, "the first pass has work to do");

    // The whole design rests on this. A copy that did not land the source's
    // mtime on the target makes every file differ on the next pass, and the
    // sync re-transfers the entire tree forever without ever converging.
    let second = remote.sync(&preserving_mode()).await;

    assert_eq!(second, 0, "a settled tree must produce no actions");
}

#[tokio::test]
async fn the_source_mtime_arrives_with_the_file() {
    let remote = Remote::new().await;
    remote.write("a.txt", "one");

    let stamp = filetime::FileTime::from_unix_time(1_000_000, 0);
    filetime::set_file_mtime(remote.source.join("a.txt"), stamp).expect("set mtime");

    remote.sync(&preserving_mode()).await;

    let landed = std::fs::metadata(remote.target_path("a.txt")).expect("metadata");
    assert_eq!(
        filetime::FileTime::from_last_modification_time(&landed),
        stamp
    );
}

#[tokio::test]
async fn a_sub_second_mtime_survives_the_transfer() {
    let remote = Remote::new().await;
    remote.write("a.txt", "one");

    // Nanosecond precision is where a timestamp that round-trips through a
    // whole number of seconds silently loses fidelity, and every file then
    // looks changed on the next pass.
    let stamp = filetime::FileTime::from_unix_time(1_700_000_000, 123_456_789);
    filetime::set_file_mtime(remote.source.join("a.txt"), stamp).expect("set mtime");

    remote.sync(&preserving_mode()).await;

    let landed = std::fs::metadata(remote.target_path("a.txt")).expect("metadata");
    let landed = filetime::FileTime::from_last_modification_time(&landed);

    assert_eq!(landed.unix_seconds(), stamp.unix_seconds());
    assert_eq!(
        landed.nanoseconds(),
        stamp.nanoseconds(),
        "sub-second precision was lost in transit"
    );
    assert_eq!(remote.sync(&preserving_mode()).await, 0);
}

#[tokio::test]
async fn a_file_larger_than_one_chunk_arrives_intact() {
    let remote = Remote::new().await;

    // Over the 256 KiB chunk size, and deliberately not a multiple of it, so
    // the final short chunk is exercised too.
    let contents: String = (0..200_000)
        .map(|index| char::from(b'a' + (index % 26) as u8))
        .collect();
    remote.write("big.bin", &contents);
    remote.write("bigger.bin", &contents.repeat(3));

    remote.sync(&preserving_mode()).await;

    assert_eq!(
        std::fs::read_to_string(remote.target_path("bigger.bin")).expect("read"),
        contents.repeat(3)
    );
    assert_eq!(remote.sync(&preserving_mode()).await, 0);
}

#[tokio::test]
async fn an_empty_file_arrives_as_an_empty_file() {
    let remote = Remote::new().await;
    remote.write("empty.txt", "");

    remote.sync(&preserving_mode()).await;

    let landed = remote.target_path("empty.txt");
    assert!(landed.is_file(), "an empty file still has to be created");
    assert_eq!(std::fs::metadata(&landed).expect("metadata").len(), 0);
}

/// A file whose name is not valid UTF-8.
///
/// Legal on Linux, where a filename is any sequence of non-NUL bytes, and
/// rejected outright by APFS, where `write` fails with `EILSEQ`. So this cannot be
/// asserted everywhere treesync is developed, only everywhere it is deployed.
/// The protocol's own handling is covered unconditionally by the round-trip
/// tests in `remote::protocol`; what needs a real filesystem is the walk, the
/// transfer and the create landing the same bytes, which is what runs here on
/// Linux.
#[tokio::test]
async fn a_path_that_is_not_utf8_is_mirrored() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let remote = Remote::new().await;
    let name = OsString::from_vec(vec![b'w', 0xff, 0xfe, b'.', b'b', b'i', b'n']);

    if let Err(error) = std::fs::write(remote.source.join(&name), b"raw") {
        // Loud rather than silent: a test that quietly does nothing is worse
        // than one that is not there.
        eprintln!(
            "SKIPPED a_path_that_is_not_utf8_is_mirrored: this filesystem will not \
             create a non-UTF-8 filename ({error}). Runs on Linux."
        );

        return;
    }

    remote.sync(&preserving_mode()).await;

    // Routing paths through String would drop this file and never say so.
    assert_eq!(
        std::fs::read(remote.target.join(&name)).expect("read"),
        b"raw"
    );
}

#[tokio::test]
async fn a_path_containing_a_newline_is_mirrored() {
    let remote = Remote::new().await;
    remote.write("two\nlines.txt", "content");

    remote.sync(&preserving_mode()).await;

    assert_eq!(
        std::fs::read_to_string(remote.target_path("two\nlines.txt")).expect("read"),
        "content"
    );
}

#[tokio::test]
async fn a_symlink_is_replicated_rather_than_followed() {
    let remote = Remote::new().await;
    remote.write("real.txt", "content");
    std::os::unix::fs::symlink("/etc/hosts", remote.source.join("outside")).expect("symlink");

    remote.sync(&preserving_mode()).await;

    let landed = remote.target_path("outside");
    assert_eq!(
        std::fs::read_link(&landed).expect("read link"),
        PathBuf::from("/etc/hosts"),
        "a link out of the tree must stay a link, never be dereferenced"
    );
}

#[tokio::test]
async fn permissions_are_mirrored() {
    use std::os::unix::fs::PermissionsExt;

    let remote = Remote::new().await;
    remote.write("script.sh", "#!/bin/sh\n");
    std::fs::set_permissions(
        remote.source.join("script.sh"),
        std::fs::Permissions::from_mode(0o755),
    )
    .expect("chmod");

    remote.sync(&preserving_mode()).await;

    let mode = std::fs::metadata(remote.target_path("script.sh"))
        .expect("metadata")
        .permissions()
        .mode()
        & 0o7777;

    assert_eq!(
        mode, 0o755,
        "an executable without its execute bit is not a mirror"
    );
}

#[tokio::test]
async fn a_changed_file_is_re_transferred() {
    let remote = Remote::new().await;
    remote.write("a.txt", "before");
    remote.sync(&preserving_mode()).await;

    remote.write("a.txt", "after, and longer");
    remote.sync(&preserving_mode()).await;

    assert_eq!(
        std::fs::read_to_string(remote.target_path("a.txt")).expect("read"),
        "after, and longer"
    );
}

/// A large JSON-ish document, which is the shape delta exists for.
fn json_blob(entries: usize) -> String {
    let mut out = String::from("[\n");

    for index in 0..entries {
        out.push_str(&format!(
            "  {{\"id\": {index}, \"name\": \"record-{index}\", \"payload\": \"{}\"}},\n",
            "x".repeat(64)
        ));
    }

    out.push(']');
    out
}

#[tokio::test]
async fn a_small_edit_to_a_large_file_sends_almost_nothing() {
    // The requirement this whole feature exists for: the cost of a change
    // should track the change, not the file.
    let remote = Remote::new().await;

    let original = json_blob(20_000);
    remote.write("big.json", &original);
    remote.sync(&preserving_mode()).await;

    let after_first = remote.sink.bytes_sent();
    assert!(
        after_first >= original.len() as u64,
        "the first sync has nothing to reuse and must send the file whole"
    );

    // One value, in the middle.
    let edited = original.replacen("\"name\": \"record-10000\"", "\"name\": \"CHANGED\"", 1);
    assert_ne!(edited, original, "the edit must actually change something");
    remote.write("big.json", &edited);

    remote.sync(&preserving_mode()).await;

    let sent = remote.sink.bytes_sent() - after_first;

    assert_eq!(
        std::fs::read_to_string(remote.target_path("big.json")).expect("read"),
        edited,
        "the patched file must be byte identical to the source"
    );
    assert!(
        sent < original.len() as u64 / 10,
        "a one field edit sent {sent} bytes of a {} byte file; the delta is not working",
        original.len()
    );
}

#[tokio::test]
async fn an_edit_that_shifts_the_rest_of_the_file_still_sends_almost_nothing() {
    // The case a fixed-block scheme fails and a rolling one does not: the
    // inserted text is longer than what it replaced, so every byte after it
    // moves. JSON does this constantly: any number that gains a digit.
    let remote = Remote::new().await;

    let original = json_blob(20_000);
    remote.write("big.json", &original);
    remote.sync(&preserving_mode()).await;

    let after_first = remote.sink.bytes_sent();

    let edited = original.replacen(
        "  {\"id\": 5,",
        "  {\"id\": 5, \"inserted\": \"a run of text that shifts everything after it\",",
        1,
    );
    assert_ne!(edited, original);
    remote.write("big.json", &edited);

    remote.sync(&preserving_mode()).await;

    let sent = remote.sink.bytes_sent() - after_first;

    assert_eq!(
        std::fs::read_to_string(remote.target_path("big.json")).expect("read"),
        edited
    );
    assert!(
        sent < original.len() as u64 / 10,
        "an insert near the start shifted the file and cost {sent} bytes of {}; \
         the rolling window is not finding the shifted blocks",
        original.len()
    );
}

#[tokio::test]
async fn a_patched_file_keeps_its_source_mtime() {
    let remote = Remote::new().await;

    let original = json_blob(20_000);
    remote.write("big.json", &original);
    remote.sync(&preserving_mode()).await;

    remote.write("big.json", &original.replacen("record-1", "record-X", 1));
    remote.sync(&preserving_mode()).await;

    let source = std::fs::metadata(remote.source.join("big.json")).expect("source metadata");
    let target = std::fs::metadata(remote.target_path("big.json")).expect("target metadata");

    assert_eq!(
        source.modified().expect("source mtime"),
        target.modified().expect("target mtime"),
        "a patch that did not carry the mtime would re-transfer on every pass"
    );
}

#[tokio::test]
async fn a_file_below_the_delta_threshold_is_sent_whole() {
    // Under the threshold the round trip costs more than the file, so the
    // delta must not fire at all.
    let remote = Remote::new().await;
    remote.write("small.txt", "before");
    remote.sync(&preserving_mode()).await;

    remote.write("small.txt", "after");
    remote.sync(&preserving_mode()).await;

    assert_eq!(
        std::fs::read_to_string(remote.target_path("small.txt")).expect("read"),
        "after"
    );
}

#[tokio::test]
async fn a_same_size_rewrite_is_caught_under_checksum_verification() {
    let remote = Remote::new().await;
    remote.write("a.txt", "aaaa");

    let checksum = ReconcileConfig {
        verify: Verify::Checksum,
        ..preserving_mode()
    };
    remote.sync(&checksum).await;

    // Same size, same timestamp: invisible to a quick comparison, which is
    // exactly what checksum verification exists for. The hash has to be
    // computed on the agent's side and come back over the wire for this to
    // work at all.
    let stamp = filetime::FileTime::from_last_modification_time(
        &std::fs::metadata(remote.source.join("a.txt")).expect("metadata"),
    );
    remote.write("a.txt", "bbbb");
    filetime::set_file_mtime(remote.source.join("a.txt"), stamp).expect("restore mtime");

    remote.sync(&checksum).await;

    assert_eq!(
        std::fs::read_to_string(remote.target_path("a.txt")).expect("read"),
        "bbbb"
    );
}

#[tokio::test]
async fn deletions_are_withheld_unless_asked_for() {
    let remote = Remote::new().await;
    remote.write("keep.txt", "one");
    remote.sync(&preserving_mode()).await;

    std::fs::write(remote.target_path("extra.txt"), "not from the source").expect("write");

    remote.sync(&preserving_mode()).await;

    assert!(
        remote.target_path("extra.txt").exists(),
        "destructive propagation must be opt-in across the wire too"
    );
}

#[tokio::test]
async fn deletions_propagate_when_asked_for() {
    let remote = Remote::new().await;
    remote.write("gone.txt", "one");
    remote.write("kept.txt", "two");
    remote.sync(&deleting()).await;

    std::fs::remove_file(remote.source.join("gone.txt")).expect("remove");

    remote.sync(&deleting()).await;

    assert!(!remote.target_path("gone.txt").exists());
    assert!(remote.target_path("kept.txt").exists());
}

#[tokio::test]
async fn a_removed_directory_is_emptied_before_it_is_removed() {
    let remote = Remote::new().await;
    remote.write("tree/one.txt", "1");
    remote.write("tree/deep/two.txt", "2");
    remote.sync(&deleting()).await;

    std::fs::remove_dir_all(remote.source.join("tree")).expect("remove");

    remote.sync(&deleting()).await;

    assert!(
        !remote.target_path("tree").exists(),
        "children before parents, or the parent removal fails"
    );
}

#[tokio::test]
async fn the_target_root_is_created_on_first_use() {
    let remote = Remote::new().await;

    // The agent was started against a root that does not exist. Indexing it
    // must report an empty tree rather than failing, so a first sync to a
    // fresh host works.
    assert!(!remote.target.exists());

    remote.write("a.txt", "one");
    remote.sync(&preserving_mode()).await;

    assert!(remote.target_path("a.txt").is_file());
}

#[tokio::test]
async fn indexing_a_missing_root_does_not_create_it() {
    let remote = Remote::new().await;

    let index = remote
        .sink
        .index(&Scope::Subtree(PathBuf::new()), &IndexOptions::quick())
        .await
        .expect("index");

    assert!(index.is_empty());
    assert!(
        !remote.target.exists(),
        "a read must not write; --dry-run depends on this"
    );
}

#[tokio::test]
async fn a_path_escaping_the_root_is_refused_by_the_agent() {
    let remote = Remote::new().await;
    remote.write("a.txt", "one");
    remote.sync(&preserving_mode()).await;

    // The containment check has to hold against paths arriving over a socket,
    // not just ones produced by a local walk. This is the request an attacker
    // sends.
    let escaped = remote
        .sink
        .create_dir(Path::new("../escaped"))
        .await
        .expect_err("a path outside the root must be refused");

    assert!(matches!(escaped, Error::InvalidPath(_)), "got {escaped:?}");
    assert!(
        !remote
            .target
            .parent()
            .expect("parent")
            .join("escaped")
            .exists(),
        "nothing may be created outside the root"
    );

    // And the session survives it: one refused request must not cost the rest
    // of the batch its connection.
    remote
        .sink
        .create_dir(Path::new("legitimate"))
        .await
        .expect("the session must still work");
    assert!(remote.target_path("legitimate").is_dir());
}

#[tokio::test]
async fn a_symlink_cannot_be_used_to_walk_the_agent_out_of_its_root() {
    // The containment check is on path *components*, and a client that wanted
    // out does not need a `..` to get there. It asks for a symlink, which is an
    // entirely ordinary request that any tree with a link in it produces, and
    // then writes to a path underneath it. Every component of both requests is a
    // plain name.
    let remote = Remote::new().await;
    remote.sync(&preserving_mode()).await;

    let elsewhere = TempDir::new().expect("elsewhere");

    remote
        .sink
        .create_symlink(Path::new("a"), elsewhere.path())
        .await
        .expect("replicating a link is a legitimate request");

    let source = remote.source.join("passwd");
    std::fs::write(&source, "escaped!").expect("write");

    let refused = remote
        .sink
        .write_file(&source, Path::new("a/passwd"))
        .await
        .expect_err("a write through a symlinked ancestor must be refused");

    assert!(matches!(refused, Error::InvalidPath(_)), "got {refused:?}");
    assert!(
        !elsewhere.path().join("passwd").exists(),
        "nothing may be written outside the root the agent was given"
    );

    // And the session survives it.
    remote
        .sink
        .create_dir(Path::new("legitimate"))
        .await
        .expect("the session must still work");
    assert!(remote.target_path("legitimate").is_dir());
}

#[tokio::test]
async fn a_symlink_at_the_agents_temporary_is_not_written_through() {
    // The agent's temporary is named after its destination, so any account on
    // the target host can predict it. Following what it finds there would send
    // the transfer's bytes through the link and then publish the link.
    let remote = Remote::new().await;
    remote.write("a.txt", "new contents");
    remote.sync(&preserving_mode()).await;

    let elsewhere = TempDir::new().expect("elsewhere");
    let victim = elsewhere.path().join("victim.txt");
    std::fs::write(&victim, "precious").expect("write");

    std::fs::remove_file(remote.target_path("a.txt")).expect("clear the target");
    std::os::unix::fs::symlink(&victim, remote.target_path(".treesync-incoming-a.txt"))
        .expect("symlink");

    remote
        .sink
        .write_file(&remote.source.join("a.txt"), Path::new("a.txt"))
        .await
        .expect("the transfer should succeed on its own terms");

    assert_eq!(
        std::fs::read_to_string(&victim).expect("read"),
        "precious",
        "the file the link pointed at must be untouched"
    );
    assert_eq!(
        std::fs::read_to_string(remote.target_path("a.txt")).expect("read"),
        "new contents"
    );
    assert!(
        !std::fs::symlink_metadata(remote.target_path("a.txt"))
            .expect("metadata")
            .file_type()
            .is_symlink(),
        "what was published has to be the content, not the link"
    );
}

#[tokio::test]
async fn a_name_at_the_filesystems_limit_survives_the_transfer() {
    // The agent's prefix is longer than the local sink's, so its ceiling on a
    // source name is lower: anything past 236 characters used to produce a
    // temporary the kernel refuses, and the file could never be received.
    let remote = Remote::new().await;
    let long = format!("{}.json", "a".repeat(240));
    remote.write(&long, "content");

    remote.sync(&preserving_mode()).await;

    assert_eq!(
        std::fs::read_to_string(remote.target_path(&long)).expect("read"),
        "content"
    );

    // And it settles, so the shortened temporary really was renamed onto the
    // full name with the source's timestamp.
    assert_eq!(
        remote.sync(&preserving_mode()).await,
        0,
        "a settled tree must produce no actions"
    );
}

#[tokio::test]
async fn a_file_added_to_a_read_only_directory_reaches_the_agent() {
    use std::os::unix::fs::PermissionsExt;

    // A source tree holding a read-only directory makes the mirrored directory
    // read-only too, and every later file the source puts inside it has to
    // still arrive.
    let remote = Remote::new().await;
    std::fs::create_dir_all(remote.source.join("locked")).expect("mkdir");
    remote.write("locked/first.txt", "one");
    std::fs::set_permissions(
        remote.source.join("locked"),
        std::fs::Permissions::from_mode(0o555),
    )
    .expect("chmod");

    remote.sync(&preserving_mode()).await;

    let mirrored = remote.target_path("locked");
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
    std::fs::set_permissions(
        remote.source.join("locked"),
        std::fs::Permissions::from_mode(0o755),
    )
    .expect("chmod");
    remote.write("locked/second.txt", "two");
    std::fs::set_permissions(
        remote.source.join("locked"),
        std::fs::Permissions::from_mode(0o555),
    )
    .expect("chmod");

    let report = remote.sync_allowing_failures(&preserving_mode()).await;

    // Leave both ends removable whatever the assertions do.
    let restored = std::fs::metadata(&mirrored)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;
    let _ = std::fs::set_permissions(
        remote.source.join("locked"),
        std::fs::Permissions::from_mode(0o755),
    );
    let _ = std::fs::set_permissions(&mirrored, std::fs::Permissions::from_mode(0o755));

    assert!(
        report.is_complete(),
        "a mirror that cannot add a file to this directory never converges: {:?}",
        report.failures
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

#[tokio::test]
async fn a_named_pipe_in_the_source_does_not_stall_a_remote_sync() {
    // A FIFO with no writer blocks the open forever, and the connection lock is
    // held for the length of a transfer, so one of these in the tree used to
    // wedge the whole sink rather than one action.
    let remote = Remote::new().await;
    remote.write("ordinary.txt", "please copy me");

    let made = std::process::Command::new("mkfifo")
        .arg(remote.source.join("pipe"))
        .status()
        .map(|status| status.success())
        .unwrap_or(false);

    if !made {
        eprintln!("SKIPPED a_named_pipe_in_the_source_does_not_stall_a_remote_sync: no mkfifo");
        return;
    }

    let applied = tokio::time::timeout(Duration::from_secs(20), remote.sync(&preserving_mode()))
        .await
        .expect("the sync must finish; a FIFO in the tree used to hang it forever");

    assert!(applied > 0);
    assert_eq!(
        std::fs::read_to_string(remote.target_path("ordinary.txt")).expect("read"),
        "please copy me"
    );
    assert!(
        !remote.target_path("pipe").exists(),
        "nothing should have been created for it on the target"
    );
}

#[tokio::test]
async fn an_absolute_path_is_refused_by_the_agent() {
    let remote = Remote::new().await;

    let error = remote
        .sink
        .create_dir(Path::new("/tmp/treesync-should-never-exist"))
        .await
        .expect_err("an absolute path would ignore the root entirely");

    assert!(matches!(error, Error::InvalidPath(_)), "got {error:?}");
    assert!(!Path::new("/tmp/treesync-should-never-exist").exists());
}

#[tokio::test]
async fn a_missing_source_file_fails_that_action_alone() {
    let remote = Remote::new().await;
    remote.write("present.txt", "here");

    // A file the plan names but that is gone by the time it is read: routine
    // in a tree under active write.
    let error = remote
        .sink
        .write_file(
            &remote.source.join("vanished.txt"),
            Path::new("vanished.txt"),
        )
        .await
        .expect_err("should fail");

    assert!(matches!(error, Error::NotFound(_)), "got {error:?}");
    assert!(
        !remote.target_path("vanished.txt").exists(),
        "a failed transfer must not publish a partial file"
    );

    // The connection is still usable for the rest of the batch.
    remote.sync(&preserving_mode()).await;
    assert!(remote.target_path("present.txt").is_file());
}

#[tokio::test]
async fn a_failed_transfer_leaves_the_existing_file_untouched() {
    let remote = Remote::new().await;
    remote.write("a.txt", "the good copy");
    remote.sync(&preserving_mode()).await;

    let _ = remote
        .sink
        .write_file(&remote.source.join("nope.txt"), Path::new("a.txt"))
        .await
        .expect_err("should fail");

    assert_eq!(
        std::fs::read_to_string(remote.target_path("a.txt")).expect("read"),
        "the good copy",
        "a transfer that could not start must not destroy what is there"
    );
}

#[tokio::test]
async fn a_failed_transfer_leaves_no_temporary_behind() {
    let remote = Remote::new().await;
    remote.write("a.txt", "content");
    remote.sync(&preserving_mode()).await;

    let _ = remote
        .sink
        .write_file(&remote.source.join("nope.txt"), Path::new("b.txt"))
        .await;

    let leftovers: Vec<String> = std::fs::read_dir(&remote.target)
        .expect("read dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with(".treesync-"))
        .collect();

    assert!(
        leftovers.is_empty(),
        "failed transfers would otherwise accumulate: {leftovers:?}"
    );
}

#[tokio::test]
async fn the_index_reports_what_the_target_actually_holds() {
    let remote = Remote::new().await;
    remote.write("a.txt", "one");
    remote.write("sub/b.txt", "two");
    std::os::unix::fs::symlink("target", remote.source.join("link")).expect("symlink");
    remote.sync(&preserving_mode()).await;

    let index = remote
        .sink
        .index(&Scope::Subtree(PathBuf::new()), &IndexOptions::quick())
        .await
        .expect("index");

    assert!(matches!(
        index.get(Path::new("a.txt")),
        Some(Entry::File { .. })
    ));
    assert!(matches!(
        index.get(Path::new("sub")),
        Some(Entry::Dir { .. })
    ));
    assert!(matches!(
        index.get(Path::new("link")),
        Some(Entry::Symlink { .. })
    ));
}

#[tokio::test]
async fn exclusions_apply_to_the_target_index_too() {
    let remote = Remote::new().await;
    remote.write("keep.txt", "one");
    std::fs::create_dir_all(&remote.target).expect("create target");
    std::fs::write(remote.target.join("scratch.tmp"), "excluded").expect("write");

    let options = IndexOptions {
        filter: Filter::new(&["*.tmp".to_string()]).expect("filter"),
        verify: Verify::Quick,
    };

    let index = remote
        .sink
        .index(&Scope::Subtree(PathBuf::new()), &options)
        .await
        .expect("index");

    // The patterns have to reach the agent and be recompiled there. If only
    // the source were filtered, every excluded file on the target would look
    // like something the source deleted, and with deletions on, treesync
    // would remove exactly the files the operator protected.
    assert!(
        !index.contains(Path::new("scratch.tmp")),
        "the agent indexed a path the exclusions cover: {:?}",
        index.paths().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_named_path_scope_stats_only_those_paths() {
    let remote = Remote::new().await;
    remote.write("named.txt", "one");
    remote.write("other.txt", "two");
    remote.sync(&preserving_mode()).await;

    let index = remote
        .sink
        .index(
            &Scope::Paths(vec![PathBuf::from("named.txt")]),
            &IndexOptions::quick(),
        )
        .await
        .expect("index");

    assert_eq!(
        index.len(),
        1,
        "an incremental batch only asks about what changed"
    );
    assert!(index.contains(Path::new("named.txt")));
}

#[tokio::test]
async fn metadata_can_be_applied_on_its_own() {
    use std::os::unix::fs::PermissionsExt;

    let remote = Remote::new().await;
    remote.write("a.txt", "one");
    remote.sync(&preserving_mode()).await;

    remote
        .sink
        .set_metadata(
            Path::new("a.txt"),
            &Metadata {
                mode: 0o600,
                uid: 0,
                gid: 0,
            },
            Preserve {
                mode: true,
                ownership: false,
            },
        )
        .await
        .expect("set metadata");

    let mode = std::fs::metadata(remote.target_path("a.txt"))
        .expect("metadata")
        .permissions()
        .mode()
        & 0o7777;

    assert_eq!(mode, 0o600);
}

#[tokio::test]
async fn a_rename_moves_the_file_on_the_target() {
    let remote = Remote::new().await;
    remote.write("before.txt", "content");
    remote.sync(&preserving_mode()).await;

    remote
        .sink
        .rename(Path::new("before.txt"), Path::new("sub/after.txt"))
        .await
        .expect("rename");

    assert!(!remote.target_path("before.txt").exists());
    assert_eq!(
        std::fs::read_to_string(remote.target_path("sub/after.txt")).expect("read"),
        "content"
    );
}

#[tokio::test]
async fn a_far_future_timestamp_survives_the_transfer() {
    let remote = Remote::new().await;
    remote.write("a.txt", "one");

    let future = SystemTime::UNIX_EPOCH + Duration::from_secs(4_000_000_000);
    filetime::set_file_mtime(
        remote.source.join("a.txt"),
        filetime::FileTime::from_system_time(future),
    )
    .expect("set mtime");

    remote.sync(&preserving_mode()).await;

    let landed = std::fs::metadata(remote.target_path("a.txt"))
        .expect("metadata")
        .modified()
        .expect("modified");

    assert_eq!(landed, future);
    assert_eq!(remote.sync(&preserving_mode()).await, 0);
}

#[tokio::test]
async fn the_session_closes_cleanly() {
    let remote = Remote::new().await;
    remote.write("a.txt", "one");
    remote.sync(&preserving_mode()).await;

    // Without a goodbye the agent is killed with the child handle, which works
    // but logs an SSH failure on the host after every successful sync.
    remote.sink.close().await;
}

// ---------------------------------------------------------------------------
// Surviving a lost connection
//
// A daemon holds one connection for days, so an outage is an ordinary event
// rather than an exceptional one. These kill the agent underneath a live sink
// and check what the client does next: rebuild the connection and carry on,
// or report the failure, depending on what it was asked for.
// ---------------------------------------------------------------------------

/// A sink whose agent can be killed, and whose restarts can be blocked.
struct Killable {
    dir: TempDir,
    target: PathBuf,
    pidfile: PathBuf,
    blocked: PathBuf,
    sink: SshSink,
}

impl Killable {
    async fn new(reconnect: Reconnect, cancel: CancellationToken) -> Self {
        let dir = TempDir::new().expect("temp dir");
        let target = dir.path().join("dst");
        let pidfile = dir.path().join("agent.pid");
        let blocked = dir.path().join("blocked");

        let sink = {
            let target = target.clone();
            let pidfile = pidfile.clone();
            let blocked = blocked.clone();

            SshSink::over_command(
                move || {
                    // Checked at spawn time, so a test can make every
                    // subsequent restart fail without touching the sink.
                    if blocked.exists() {
                        let mut command = Command::new("sh");
                        command
                            .arg("-c")
                            .arg("echo 'agent unavailable' >&2; exit 1");

                        return command;
                    }

                    agent_command(&target, Some(&pidfile))
                },
                "killable agent".to_string(),
            )
            .await
            .expect("the agent should start")
            .with_reconnect(reconnect, cancel)
        };

        Self {
            dir,
            target,
            pidfile,
            blocked,
            sink,
        }
    }

    fn agent_pid(&self) -> String {
        std::fs::read_to_string(&self.pidfile)
            .expect("the agent should have recorded its pid")
            .trim()
            .to_string()
    }

    /// Makes every future restart fail, standing in for a host that is down.
    fn block_restarts(&self) {
        std::fs::write(&self.blocked, b"").expect("write");
    }

    /// Kills the running agent, the way a dropped link takes one away.
    fn kill_agent(&self) {
        let pid = self.agent_pid();

        let killed = std::process::Command::new("kill")
            .arg("-KILL")
            .arg(&pid)
            .status()
            .expect("kill should run");
        assert!(killed.success(), "could not kill the agent at {pid}");

        // The client learns about it when its read returns end of stream,
        // which happens as soon as the kernel tears the process down and
        // closes its pipes, before the parent reaps it. So wait for the
        // process to stop *running*, not for it to disappear: until it is
        // reaped it lingers as a zombie, which `kill -0` still reports as
        // alive.
        for _ in 0..500 {
            let state = std::process::Command::new("ps")
                .args(["-o", "stat=", "-p", &pid])
                .output()
                .expect("ps should run");

            let state = String::from_utf8_lossy(&state.stdout).trim().to_string();

            if state.is_empty() || state.starts_with('Z') {
                return;
            }

            std::thread::sleep(Duration::from_millis(10));
        }

        panic!("the agent at {pid} would not die");
    }

    fn source_file(&self, contents: &str) -> PathBuf {
        let path = self.dir.path().join("source.txt");
        std::fs::write(&path, contents).expect("write");

        path
    }
}

#[tokio::test]
async fn a_dropped_connection_is_rebuilt_and_the_action_retried() {
    let killable = Killable::new(Reconnect::forever(), CancellationToken::new()).await;

    killable
        .sink
        .create_dir(Path::new("before"))
        .await
        .expect("the first action should work");
    let first = killable.agent_pid();

    killable.kill_agent();

    // The client has no idea yet. It finds out when this request's reply
    // never arrives, and the whole point is that the caller does not have to
    // care.
    killable
        .sink
        .create_dir(Path::new("after"))
        .await
        .expect("the action should succeed against a rebuilt connection");

    assert!(killable.target.join("after").is_dir());
    assert_ne!(
        killable.agent_pid(),
        first,
        "a different agent should be serving now"
    );
}

#[tokio::test]
async fn a_file_transfer_survives_a_dropped_connection() {
    let killable = Killable::new(Reconnect::forever(), CancellationToken::new()).await;
    // Larger than one chunk, so the transfer is several frames rather than one.
    let contents = "x".repeat(700_000);
    let source = killable.source_file(&contents);

    killable
        .sink
        .create_dir(Path::new("sub"))
        .await
        .expect("first action");
    killable.kill_agent();

    killable
        .sink
        .write_file(&source, Path::new("sub/big.txt"))
        .await
        .expect("the transfer should be restarted on a new connection");

    // Restarted from the beginning, not resumed: a half file plus a half file
    // is not the file.
    assert_eq!(
        std::fs::read_to_string(killable.target.join("sub/big.txt")).expect("read"),
        contents
    );
}

#[tokio::test]
async fn a_patch_survives_a_dropped_connection() {
    let killable = Killable::new(Reconnect::forever(), CancellationToken::new()).await;

    // Over the delta threshold, so this takes the patch path rather than being
    // sent whole.
    let original = json_blob(20_000);
    let source = killable.source_file(&original);

    killable
        .sink
        .patch_file(&source, Path::new("big.json"))
        .await
        .expect("the first transfer");

    let edited = original.replacen("record-9999", "CHANGED", 1);
    assert_ne!(edited, original);
    std::fs::write(&source, &edited).expect("rewrite the source");

    killable.kill_agent();

    // Whether this resumes onto what survived or starts clean, the file has to
    // arrive correct. That is the guarantee, and the commit hash is what makes
    // it one rather than a hope.
    killable
        .sink
        .patch_file(&source, Path::new("big.json"))
        .await
        .expect("the patch should survive the drop");

    assert_eq!(
        std::fs::read_to_string(killable.target.join("big.json")).expect("read"),
        edited
    );
}

#[tokio::test]
async fn a_stale_partial_transfer_is_not_resumed_onto() {
    let killable = Killable::new(Reconnect::forever(), CancellationToken::new()).await;

    let original = json_blob(20_000);
    let source = killable.source_file(&original);

    killable
        .sink
        .patch_file(&source, Path::new("big.json"))
        .await
        .expect("the first transfer");

    // A leftover from some entirely different file. Resuming onto it would
    // corrupt the result, and the prefix check is what stops that.
    std::fs::write(
        killable.target.join(".treesync-incoming-big.json"),
        "wreckage from another transfer entirely",
    )
    .expect("plant a stale partial");

    let edited = original.replacen("record-1", "CHANGED", 1);
    std::fs::write(&source, &edited).expect("rewrite");

    killable.kill_agent();

    killable
        .sink
        .patch_file(&source, Path::new("big.json"))
        .await
        .expect("the patch should still complete");

    assert_eq!(
        std::fs::read_to_string(killable.target.join("big.json")).expect("read"),
        edited,
        "a stale partial must be discarded, not built upon"
    );
}

#[tokio::test]
async fn a_partial_transfer_is_invisible_to_the_index() {
    // It is treesync's working state, not tree content. An index that reported
    // it would look like a file the source does not have, and with `delete`
    // on, the next pass would remove the very thing a resume needs.
    let remote = Remote::new().await;
    remote.write("a.txt", "one");
    remote.sync(&preserving_mode()).await;

    std::fs::write(
        remote.target_path(".treesync-incoming-b.txt"),
        "half a transfer",
    )
    .expect("plant a partial");

    let index = remote
        .sink
        .index(&Scope::Subtree(PathBuf::new()), &IndexOptions::quick())
        .await
        .expect("index");

    assert!(
        !index.contains(Path::new(".treesync-incoming-b.txt")),
        "a transfer temporary must never reach the reconciler"
    );

    // And with deletions on it survives the pass that would otherwise remove it.
    remote.sync(&deleting()).await;

    assert!(
        remote.target_path(".treesync-incoming-b.txt").exists(),
        "delete = true must not sweep away a resumable transfer"
    );
}

#[tokio::test]
async fn the_index_survives_a_dropped_connection() {
    let killable = Killable::new(Reconnect::forever(), CancellationToken::new()).await;
    killable
        .sink
        .create_dir(Path::new("present"))
        .await
        .expect("first action");

    killable.kill_agent();

    let index = killable
        .sink
        .index(&Scope::Subtree(PathBuf::new()), &IndexOptions::quick())
        .await
        .expect("indexing should survive a reconnect");

    assert!(index.contains(Path::new("present")));
}

#[tokio::test]
async fn several_drops_in_a_row_are_each_survived() {
    // An outage is not always one clean break. What matters is that the client
    // keeps recovering rather than recovering once.
    let killable = Killable::new(Reconnect::forever(), CancellationToken::new()).await;

    for round in 0..3 {
        killable.kill_agent();

        killable
            .sink
            .create_dir(Path::new(&format!("round-{round}")))
            .await
            .unwrap_or_else(|error| panic!("round {round} should recover: {error}"));

        assert!(killable.target.join(format!("round-{round}")).is_dir());
    }
}

#[tokio::test]
async fn a_dropped_connection_is_reported_when_reconnecting_is_off() {
    let killable = Killable::new(Reconnect::never(), CancellationToken::new()).await;
    killable
        .sink
        .create_dir(Path::new("before"))
        .await
        .expect("first action");

    killable.kill_agent();

    let error = killable
        .sink
        .create_dir(Path::new("after"))
        .await
        .expect_err("a one-shot pass must report a lost link, not wait for it");

    assert!(
        error.to_string().contains("lost the connection"),
        "the error should say what happened: {error}"
    );
}

#[tokio::test]
async fn a_bounded_policy_gives_up_and_says_how_many_times_it_tried() {
    let killable = Killable::new(Reconnect::bounded(2), CancellationToken::new()).await;
    killable
        .sink
        .create_dir(Path::new("before"))
        .await
        .expect("first action");

    killable.block_restarts();
    killable.kill_agent();

    let error = killable
        .sink
        .create_dir(Path::new("after"))
        .await
        .expect_err("should give up");

    assert!(
        error.to_string().contains("2 attempt"),
        "the error should say how hard it tried: {error}"
    );
}

#[tokio::test]
async fn cancelling_breaks_out_of_a_reconnect_wait() {
    let cancel = CancellationToken::new();
    let killable = Killable::new(Reconnect::forever(), cancel.clone()).await;
    killable
        .sink
        .create_dir(Path::new("before"))
        .await
        .expect("first action");

    // Unstartable, so the retry loop would otherwise run forever, which is
    // exactly what `forever` promises.
    killable.block_restarts();
    killable.kill_agent();

    let cancelling = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel.cancel();
    });

    // Without cancellation reaching inside the retry loop, `watch` told to
    // stop during an outage would sit here until something killed it, losing
    // the shutdown flush and making `docker stop` wait out its timeout.
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        killable.sink.create_dir(Path::new("after")),
    )
    .await
    .expect("cancellation must break the retry loop")
    .expect_err("a cancelled reconnect is a failure");

    assert!(
        error.to_string().contains("shutting down"),
        "the error should say why it stopped trying: {error}"
    );

    cancelling.await.expect("join");
}

#[tokio::test]
async fn an_agent_error_does_not_trigger_a_reconnect() {
    // The distinction the whole retry rests on: an agent that answers "no" is
    // a working connection reporting a real problem. Reconnecting would loop
    // without ever addressing it.
    let killable = Killable::new(Reconnect::forever(), CancellationToken::new()).await;
    killable
        .sink
        .create_dir(Path::new("legitimate"))
        .await
        .expect("first action");
    let before = killable.agent_pid();

    let error = killable
        .sink
        .create_dir(Path::new("../escaped"))
        .await
        .expect_err("a path outside the root must be refused");

    assert!(matches!(error, Error::InvalidPath(_)), "got {error:?}");
    assert_eq!(
        killable.agent_pid(),
        before,
        "the connection was fine; nothing should have been rebuilt"
    );
}

// ---------------------------------------------------------------------------
// Adversarial: a link that keeps failing under a real workload
// ---------------------------------------------------------------------------

impl Killable {
    /// Kills the agent if one is running, tolerating every way that can miss.
    ///
    /// Unlike [`Killable::kill_agent`] this never asserts: it runs from a
    /// background task racing the transfer, so the agent may be mid-restart,
    /// already dead, or not yet have written its pid. All of those are the
    /// normal shape of a link that keeps dropping, not test failures.
    fn kill_agent_if_running(&self) {
        let Ok(pid) = std::fs::read_to_string(&self.pidfile) else {
            return;
        };

        let _ = std::process::Command::new("kill")
            .arg("-KILL")
            .arg(pid.trim())
            .status();
    }

    /// A file of `size` bytes in the source, filled with compressible content.
    fn source_file_named(&self, name: &str, size: usize) -> PathBuf {
        let path = self.dir.path().join(name);
        let unit = format!(r#"{{"file":"{name}","payload":"{}"}},"#, "x".repeat(96));
        let mut contents = String::with_capacity(size + unit.len());

        while contents.len() < size {
            contents.push_str(&unit);
        }

        std::fs::write(&path, &contents).expect("write");

        path
    }
}

/// A reconnect policy that retries hard and fast, so a test that drops the link
/// a dozen times finishes in seconds rather than minutes.
fn impatient() -> Reconnect {
    Reconnect {
        interval: Duration::from_millis(20),
        // Backing off to the real ten-second ceiling would make a test that
        // drops the link a dozen times take minutes. The *shape* is what these
        // exercise; `Reconnect::wait_before` has its own unit tests for the
        // numbers.
        max_interval: Duration::from_millis(80),
        attempts: None,
    }
}

#[tokio::test]
async fn ten_large_files_survive_a_link_that_keeps_dropping() {
    // The workload this exists for, scaled down to a test: several files well
    // past one protocol chunk, transferred over a link that fails repeatedly
    // and at unpredictable points: mid-chunk, between files, during a
    // reconnect.
    let killable = Killable::new(impatient(), CancellationToken::new()).await;

    let mut expected = Vec::new();
    for index in 0..10 {
        let name = format!("big-{index}.json");
        let path = killable.source_file_named(&name, 400_000);
        expected.push((name, path));
    }

    // Chaos runs alongside the transfer, on its own thread so it is not
    // serialised behind the async work it is trying to interrupt.
    //
    // The successful kills are counted, and asserted on afterwards. Without
    // that this test passes just as happily when every kill misses, which is
    // the failure mode of a chaos test: it proves the transfer works on a link
    // that never actually broke.
    //
    // The kills stop after a target count instead of running for as long as
    // the transfer does. Unbounded, they cannot be outrun on a machine where
    // one reconnect plus one file takes longer than the interval: every
    // attempt is killed before it lands, `impatient()` sets no attempt limit
    // so the sink retries forever, and the test hangs rather than fails. That
    // is a livelock, a loaded CI runner is exactly where it appears, and a
    // hung test holds a job until GitHub's six-hour ceiling. Stopping the
    // kills leaves a window the transfer is guaranteed to get through.
    const TARGET_KILLS: usize = 5;

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let kills = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let chaos = {
        let stop = stop.clone();
        let kills = kills.clone();
        let pidfile = killable.pidfile.clone();

        std::thread::spawn(move || {
            let mut last: Option<u32> = None;

            while !stop.load(std::sync::atomic::Ordering::Relaxed)
                && kills.load(std::sync::atomic::Ordering::Relaxed) < TARGET_KILLS
            {
                std::thread::sleep(Duration::from_millis(60));

                // Anything that is not a number is not a target. There is no
                // file at all between an agent dying and its replacement
                // recording itself, and `kill ''` is an error message rather
                // than a kill.
                let Some(pid) = std::fs::read_to_string(&pidfile)
                    .ok()
                    .and_then(|contents| contents.trim().parse::<u32>().ok())
                else {
                    continue;
                };

                // One kill per pid. The file still names the dead agent until
                // its replacement rewrites it, and pids are recycled: stock
                // Linux wraps at 32768 and a CI runner churns through them, so
                // signalling a stale one risks hitting whatever took the
                // number over. A test that SIGKILLs an unrelated process is
                // not a failure anyone would think to look for here.
                if last == Some(pid) {
                    continue;
                }

                let killed = std::process::Command::new("kill")
                    .arg("-KILL")
                    .arg(pid.to_string())
                    .status();

                // `kill` fails on a pid that has already gone, so only a
                // success means a live agent was actually torn down.
                if matches!(killed, Ok(status) if status.success()) {
                    last = Some(pid);
                    kills.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        })
    };

    // A ceiling, so that if the transfer ever does livelock it says so in a
    // minute instead of hanging. Unloaded this takes about a second, so a
    // minute is slack for a busy machine rather than a tuned figure.
    let transferred = tokio::time::timeout(Duration::from_secs(60), async {
        for (name, path) in &expected {
            killable
                .sink
                .write_file(path, Path::new(name))
                .await
                .unwrap_or_else(|error| panic!("{name} should survive the drops: {error}"));
        }
    })
    .await;

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    chaos.join().expect("chaos thread");

    let killed = kills.load(std::sync::atomic::Ordering::Relaxed);

    assert!(
        transferred.is_ok(),
        "the transfer never finished: {killed} kill(s) landed and the sink went on \
         retrying without completing a file, which is the livelock an unbounded chaos \
         thread produces on a slow machine"
    );
    assert!(
        killed > 0,
        "no agent was actually killed, so this proved nothing about a choppy link"
    );

    for (name, path) in &expected {
        let landed = killable.target.join(name);

        assert_eq!(
            std::fs::read(&landed).expect("read the landed file"),
            std::fs::read(path).expect("read the source"),
            "{name} must be byte identical after {killed} dropped connection(s)"
        );
    }
}

#[tokio::test]
async fn a_delta_patch_survives_repeated_drops_mid_transfer() {
    // A patch is a longer exchange than a whole-file send, a signature then a
    // token stream, so there is more of it to interrupt. Every attempt has to
    // end with the file byte-identical or not published at all; a half-applied
    // patch is the one outcome that must be impossible.
    let killable = Killable::new(impatient(), CancellationToken::new()).await;

    let original = json_blob(20_000);
    let source = killable.source_file(&original);

    killable
        .sink
        .patch_file(&source, Path::new("big.json"))
        .await
        .expect("the first transfer");

    for round in 0..3 {
        let edited = original.replacen(
            &format!("record-{}", round * 1000 + 1),
            &format!("EDITED-ROUND-{round}"),
            1,
        );
        assert_ne!(edited, original, "round {round} must change something");
        std::fs::write(&source, &edited).expect("rewrite");

        killable.kill_agent_if_running();

        killable
            .sink
            .patch_file(&source, Path::new("big.json"))
            .await
            .unwrap_or_else(|error| panic!("round {round} should recover: {error}"));

        assert_eq!(
            std::fs::read_to_string(killable.target.join("big.json")).expect("read"),
            edited,
            "round {round}: a patch must land whole or not at all"
        );
    }
}

#[tokio::test]
async fn a_transfer_slower_than_the_link_still_converges_under_churn() {
    // Changes arriving faster than transfers complete. The property is that
    // the target ends up matching the source, not that any particular pass
    // wins the race, and that nothing is left half-written along the way.
    let killable = Killable::new(impatient(), CancellationToken::new()).await;
    let path = killable.dir.path().join("churning.json");

    let mut latest = String::new();

    for round in 0..8 {
        latest = format!(
            r#"{{"round":{round},"payload":"{}"}}"#,
            "y".repeat(120_000 + round * 1000)
        );
        std::fs::write(&path, &latest).expect("write");

        // Every other round the link dies underneath the transfer.
        if round % 2 == 0 {
            killable.kill_agent_if_running();
        }

        killable
            .sink
            .write_file(&path, Path::new("churning.json"))
            .await
            .unwrap_or_else(|error| panic!("round {round}: {error}"));
    }

    assert_eq!(
        std::fs::read_to_string(killable.target.join("churning.json")).expect("read"),
        latest,
        "after the churn the target must hold the newest content, whole"
    );

    let strays: Vec<_> = std::fs::read_dir(&killable.target)
        .expect("read target")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with(".treesync-"))
        .collect();

    assert!(
        strays.is_empty(),
        "a settled transfer must leave no temporary behind; found {strays:?}"
    );
}
