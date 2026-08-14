//! A [`Sink`] backed by an agent running on another host.
//!
//! # The transport is one SSH child process
//!
//! `ssh host -- treesync agent --root /srv/app`, with the protocol on the
//! child's stdin and stdout. There is no listening socket, no port to open, no
//! second authentication system and no daemon to keep running on the target:
//! the connection's lifetime is the sync's, and its access is whatever the SSH
//! login already had.
//!
//! # Why not fork rsync
//!
//! Because the decision of what to transfer is already made by then. rsync
//! would rebuild its own file list over the link on every pass, the O(tree)
//! cost this whole design avoids, and it would make correctness
//! depend on matching flags to semantics treesync defines itself. The agent is
//! handed a plan and executes it through the same [`LocalSink`] the local path
//! uses.
//!
//! [`LocalSink`]: crate::sink::LocalSink

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::delta;
use super::protocol::{
    self, CHUNK_SIZE, Chunk, PROTOCOL_VERSION, Request, Response, Token, WireMetadata, WirePath,
    WirePreserve, WireScope, WireTime, WireVerify,
};
use crate::error::{Error, Result};
use crate::reconcile::{Index, IndexOptions, Metadata, Preserve, Scope};
use crate::sink::Sink;

/// How long to wait for the TCP connection and the SSH banner.
///
/// Bounded so an unreachable host fails the sync instead of hanging it. SSH's
/// own default has no ceiling worth relying on.
const CONNECT_TIMEOUT_SECS: u32 = 15;

/// How often to send a keepalive on an idle connection, and how many may go
/// unanswered before the connection is declared dead.
///
/// `watch` holds one connection open for as long as it runs, which for an
/// idle tree means hours of silence. Two things go wrong without keepalives:
/// a NAT or firewall drops the mapping for a conversation it thinks has ended,
/// and a peer that vanished is never noticed, so the next action after a
/// change blocks on a socket nobody is listening to. Ninety seconds of
/// unanswered probes is the ceiling on both.
const KEEPALIVE_SECS: u32 = 30;
const KEEPALIVE_RETRIES: u32 = 3;

/// Where a sync's target lives, and how to reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshTarget {
    /// `user@host`, or an alias from `~/.ssh/config`.
    pub host: String,
    /// Absolute path to the target tree on that host.
    pub path: PathBuf,
    pub port: Option<u16>,
    pub identity_file: Option<PathBuf>,
    /// Where the agent binary lives on the remote host.
    pub agent_path: RemoteAgentPath,
    /// Extra `-o` options for the SSH client.
    ///
    /// The escape hatch for everything `ssh_config` can express and this
    /// struct does not: a `ProxyJump` to reach a host behind a bastion, a
    /// `UserKnownHostsFile` for a daemon that does not run as a human with a
    /// home directory, a `ControlPath` to reuse one connection.
    ///
    /// Needed more often than it looks, because OpenSSH finds `~/.ssh/config`
    /// through the password database instead of `$HOME`, so a service
    /// account cannot be pointed at a different config by setting an
    /// environment variable.
    pub options: Vec<String>,
}

impl SshTarget {
    /// The `ssh` invocation, minus the remote command.
    ///
    /// `BatchMode` is the important one. Without it, a host whose key is
    /// unknown or whose auth fails drops to an interactive prompt on a stdin
    /// that is carrying a binary protocol, so the sync hangs instead of
    /// reporting anything. With it, SSH fails immediately and says why.
    ///
    /// Shared with [`ship`](super::ship), so installing the agent reaches the
    /// host through exactly the options the sync itself will use.
    pub(crate) fn command(&self) -> Command {
        let mut command = Command::new("ssh");

        // Order matters: ssh takes the *first* value it is given for an
        // option. What comes before the operator's own options is therefore
        // fixed, and what comes after is a default they can override.
        //
        // `BatchMode` is not negotiable. Turning it off puts an interactive
        // prompt on a stdin that is carrying a binary protocol, and the sync
        // hangs rather than failing.
        command.arg("-o").arg("BatchMode=yes");

        for option in &self.options {
            command.arg("-o").arg(option);
        }

        command
            // Overridable above, because a slow or distant link is a real
            // reason to want longer than this.
            .arg("-o")
            .arg(format!("ConnectTimeout={CONNECT_TIMEOUT_SECS}"))
            .arg("-o")
            .arg(format!("ServerAliveInterval={KEEPALIVE_SECS}"))
            .arg("-o")
            .arg(format!("ServerAliveCountMax={KEEPALIVE_RETRIES}"))
            // Multiplexing is left to the operator rather than forced on: a
            // ControlMaster this process did not create is not this process's
            // to tear down.
            .arg("-o")
            .arg("ClearAllForwardings=yes");

        if let Some(port) = self.port {
            command.arg("-p").arg(port.to_string());
        }

        if let Some(identity) = &self.identity_file {
            // `IdentitiesOnly` so an agent holding a dozen keys does not offer
            // them all and trip MaxAuthTries before reaching this one.
            command
                .arg("-i")
                .arg(identity)
                .arg("-o")
                .arg("IdentitiesOnly=yes");
        }

        command.arg(&self.host);

        command
    }

    /// The remote shell command that starts the agent.
    fn agent_command(&self) -> String {
        format!(
            "{} agent --root {}",
            self.agent_path.shell_word(),
            shell_quote(&self.path.to_string_lossy())
        )
    }
}

/// Where the agent binary sits on the remote host.
///
/// Kept apart from a plain `PathBuf` because the two cases render differently
/// into a remote shell command: an absolute path is quoted whole, while the
/// default is relative to the login account's home directory and has to let
/// `$HOME` expand. Building it as a string and hoping would put an unquoted,
/// operator-supplied path into a shell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteAgentPath {
    /// Relative to the remote `$HOME`.
    UnderHome(String),
    /// Used exactly as given.
    Absolute(PathBuf),
}

impl Default for RemoteAgentPath {
    fn default() -> Self {
        Self::UnderHome(".cache/treesync/treesync".to_string())
    }
}

impl RemoteAgentPath {
    pub fn new(path: &Path) -> Self {
        if path.is_absolute() {
            Self::Absolute(path.to_path_buf())
        } else {
            Self::UnderHome(path.to_string_lossy().to_string())
        }
    }

    /// One shell word naming the binary.
    pub fn shell_word(&self) -> String {
        match self {
            // The quoting is placed so `$HOME` expands and nothing else does.
            Self::UnderHome(relative) => format!("\"$HOME\"/{}", shell_quote(relative)),
            Self::Absolute(path) => shell_quote(&path.to_string_lossy()),
        }
    }

    /// The directory the binary lives in, as a shell word.
    pub fn parent_shell_word(&self) -> String {
        match self {
            Self::UnderHome(relative) => {
                let parent = Path::new(relative)
                    .parent()
                    .map(|parent| parent.to_string_lossy().to_string())
                    .unwrap_or_default();

                if parent.is_empty() {
                    "\"$HOME\"".to_string()
                } else {
                    format!("\"$HOME\"/{}", shell_quote(&parent))
                }
            }
            Self::Absolute(path) => match path.parent() {
                Some(parent) => shell_quote(&parent.to_string_lossy()),
                None => "/".to_string(),
            },
        }
    }
}

/// Wraps a string so a remote shell reads it as one literal word.
///
/// Single quotes, because inside them a POSIX shell expands nothing at all:
/// no `$`, no backtick, no backslash. The only character that cannot appear is
/// a single quote itself, which is closed, escaped and reopened.
///
/// This is not cosmetic. A target path is operator-supplied and reaches a shell
/// on another machine; `/srv/$(rm -rf ~)` has to arrive as a directory name.
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

/// A live connection to an agent.
pub(crate) struct Connection {
    child: Child,
    input: BufReader<ChildStdout>,
    output: BufWriter<ChildStdin>,
}

/// What to do when the link to the agent drops.
///
/// A network outage is an ordinary event, not an exceptional one, and the two
/// commands want opposite things from it. `watch` is supposed to survive the
/// outage: it holds one connection for days, and a link that comes back should
/// find the daemon still mirroring. A one-shot `sync` is supposed to fail: run
/// from cron against a host that is down, an unbounded retry would pile up a
/// process per tick and never report anything.
/// # Why the wait grows
///
/// The first retry is quick because most interruptions are: a NAT mapping
/// dropped, sshd restarted, a laptop's wifi blinked. Waiting ten seconds to
/// discover the link was fine all along wastes the whole outage.
///
/// What must not happen is a *sustained* outage being met with a connection
/// attempt every second for an hour. Each one forks an ssh process, completes a
/// TCP handshake and starts a key exchange against a host that is already
/// struggling, and several syncs doing that together is a small denial of service
/// aimed at exactly the host you want back. So the wait doubles up to
/// [`Reconnect::max_interval`] and stays there.
///
/// The backoff resets whenever a connection is rebuilt, since the next drop is
/// a new outage rather than a continuation of the last one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reconnect {
    /// How long to wait before the first retry.
    pub interval: Duration,
    /// The ceiling the wait doubles up to.
    pub max_interval: Duration,
    /// Attempts before giving up. `None` keeps trying until cancelled.
    pub attempts: Option<u32>,
}

/// The default first wait: long enough not to spin, short enough that a blip
/// costs almost nothing.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

/// The default ceiling. Past this, waiting longer only delays recovery. A host
/// that has been down for a while is not helped by being probed less than every
/// ten seconds.
const RECONNECT_MAX_INTERVAL: Duration = Duration::from_secs(10);

impl Reconnect {
    /// Report the failure instead of rebuilding the connection.
    pub fn never() -> Self {
        Self {
            interval: RECONNECT_INTERVAL,
            max_interval: RECONNECT_MAX_INTERVAL,
            attempts: Some(0),
        }
    }

    /// Retry for as long as it takes, backing off from one second to ten.
    ///
    /// For the daemon, where the alternative to waiting is a mirror that
    /// stopped and a process that has to be noticed and restarted by hand.
    pub fn forever() -> Self {
        Self {
            interval: RECONNECT_INTERVAL,
            max_interval: RECONNECT_MAX_INTERVAL,
            attempts: None,
        }
    }

    /// As [`Reconnect::forever`], but giving up after `attempts` tries.
    ///
    /// For a one-shot pass, where a blip in the middle of a large transfer
    /// should not throw the pass away but the command still has to terminate.
    pub fn bounded(attempts: u32) -> Self {
        Self {
            interval: RECONNECT_INTERVAL,
            max_interval: RECONNECT_MAX_INTERVAL,
            attempts: Some(attempts),
        }
    }

    /// How long to wait before attempt number `attempt`, counting from one.
    ///
    /// Doubles each time and then holds at [`Self::max_interval`]. Saturating
    /// throughout: the shift is clamped and the multiply is checked, so a long
    /// outage settles at the ceiling rather than overflowing into a nonsense
    /// duration.
    fn wait_before(&self, attempt: u32) -> Duration {
        let doublings = attempt.saturating_sub(1).min(31);

        self.interval
            .checked_mul(1u32 << doublings)
            .unwrap_or(self.max_interval)
            .min(self.max_interval)
    }
}

impl Default for Reconnect {
    fn default() -> Self {
        Self::never()
    }
}

/// Builds the reconnect strategy for an SSH target.
pub(crate) fn reopen_over_ssh(target: &SshTarget, binary: Option<&Path>) -> Reopen {
    Reopen::Ssh {
        target: Box::new(target.clone()),
        binary: binary.map(Path::to_path_buf),
    }
}

/// How to build a fresh connection after one is lost.
pub(crate) enum Reopen {
    /// Re-run a command that starts an agent.
    Command(Box<dyn Fn() -> Command + Send + Sync>),

    /// Reconnect over SSH, reinstalling the agent if it is no longer there.
    ///
    /// Reinstalling matters because the reasons a connection drops overlap
    /// with the reasons a host comes back changed: an instance replaced by its
    /// autoscaler answers SSH perfectly well and has no agent on it.
    Ssh {
        target: Box<SshTarget>,
        binary: Option<PathBuf>,
    },
}

impl Reopen {
    async fn open(&self, description: &str) -> Result<Connection> {
        match self {
            Self::Command(factory) => open(factory(), description).await,
            Self::Ssh { target, binary } => {
                super::ship::open_connection(target, binary.as_deref()).await
            }
        }
    }
}

/// Why a request did not complete.
///
/// The distinction is the whole basis of reconnecting safely. An agent that
/// answers "permission denied" is a working connection reporting a real
/// problem with one file, and retrying the transport would loop forever without
/// ever addressing it. A stream that ended mid-frame is the link itself, and
/// the same request against a new connection will very likely succeed.
enum Failure {
    /// The connection is gone.
    Transport(Error),
    /// The agent answered, and its answer was an error.
    Remote(Error),
}

/// Applies a plan to a tree served by an agent.
pub struct SshSink {
    /// How the agent was reached, for logs and for errors that have to tell an
    /// operator what to fix.
    description: String,
    /// The protocol is one stream taking strict turns, so a second request
    /// entering it mid-exchange would interleave frames and desynchronise both
    /// ends. `Sink` takes `&self`, so the exclusion has to live here.
    ///
    /// An async mutex rather than a `std` one because the lock is held across
    /// the awaits that read the reply, which is the whole critical section.
    /// It is also essentially uncontended: `apply` runs actions in sequence.
    ///
    /// Holding it across a reconnect is deliberate: a request that arrived
    /// while the link was down waits for the new one rather than failing
    /// against the old.
    connection: Mutex<Connection>,
    reopen: Reopen,
    reconnect: Reconnect,
    /// When to send only what differs instead of the whole file.
    delta: delta::Options,
    /// File content bytes put on the wire, whole-file and literal alike.
    ///
    /// The figure that actually says whether a delta is earning its keep: not
    /// how large the files were, but how much of them had to cross the link.
    /// Counted for both paths so the two are directly comparable.
    sent: std::sync::atomic::AtomicU64,
    /// Breaks the retry loop when the process is shutting down.
    ///
    /// Without it, `watch` told to stop during an outage would sit in a
    /// one-second loop until something killed it, the shutdown flush lost,
    /// and `docker stop` waiting out its timeout.
    cancel: CancellationToken,
}

impl std::fmt::Debug for SshSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshSink")
            .field("agent", &self.description)
            .finish_non_exhaustive()
    }
}

impl SshSink {
    /// Opens an SSH connection and completes the handshake.
    ///
    /// Fails here rather than at the first action if the host is unreachable,
    /// the agent is missing, or it speaks a different protocol version. A
    /// misconfigured remote should be apparent before a plan is half applied.
    pub async fn connect(target: &SshTarget) -> Result<Self> {
        let description = describe(target);
        let connection = open_for(target).await?;

        Ok(Self::from_parts(
            connection,
            Reopen::Ssh {
                target: Box::new(target.clone()),
                binary: None,
            },
            description,
        ))
    }

    /// Speaks the protocol to an agent started by an arbitrary command.
    ///
    /// The seam that makes the remote path testable: the same client code,
    /// the same agent binary and the same protocol, with the SSH hop replaced
    /// by a local child process. A test that stubbed the protocol instead
    /// would be testing the stub.
    ///
    /// Takes a factory rather than a command because a `Command` cannot be
    /// used twice, and reconnecting means starting another agent.
    pub async fn over_command<F>(factory: F, description: String) -> Result<Self>
    where
        F: Fn() -> Command + Send + Sync + 'static,
    {
        let connection = open(factory(), &description).await?;

        Ok(Self::from_parts(
            connection,
            Reopen::Command(Box::new(factory)),
            description,
        ))
    }

    pub(crate) fn from_parts(connection: Connection, reopen: Reopen, description: String) -> Self {
        Self {
            description,
            connection: Mutex::new(connection),
            reopen,
            reconnect: Reconnect::never(),
            delta: delta::Options::default(),
            sent: std::sync::atomic::AtomicU64::new(0),
            cancel: CancellationToken::new(),
        }
    }

    /// File content bytes sent so far, across every transfer on this link.
    pub fn bytes_sent(&self) -> u64 {
        self.sent.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Sets when a changed file is sent as a delta rather than whole.
    pub fn with_delta(mut self, delta: delta::Options) -> Self {
        self.delta = delta;

        self
    }

    /// Sets what happens when the link drops, and what cancels the wait.
    ///
    /// Off by default: rebuilding a connection behind the caller's back is
    /// only right when the caller has said it wants that, and a command that
    /// is supposed to fail fast must not be made to hang.
    pub fn with_reconnect(mut self, reconnect: Reconnect, cancel: CancellationToken) -> Self {
        self.reconnect = reconnect;
        self.cancel = cancel;

        self
    }

    /// How the agent was reached.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Sends a request and returns the agent's reply, reconnecting if needed.
    async fn request(&self, request: Request) -> Result<Response> {
        let mut connection = self.connection.lock().await;

        loop {
            match exchange(&mut connection, &request).await {
                Ok(response) => return Ok(response),
                // The agent is alive and said no. Reconnecting would loop
                // forever without ever addressing what it objected to.
                Err(Failure::Remote(error)) => return Err(error),
                Err(Failure::Transport(error)) => {
                    self.restore(&mut connection, error).await?;
                }
            }
        }
    }

    /// Rebuilds the connection in place, or gives up according to the policy.
    ///
    /// Every action treesync applies is idempotent: `create_dir_all` on an
    /// existing directory, a write that replaces, a remove that tolerates
    /// absence. So the caller can simply reissue whatever was in flight. The
    /// one exception is `Rename`, which no plan currently emits.
    async fn restore(&self, connection: &mut Connection, error: Error) -> Result<()> {
        tracing::warn!(
            agent = %self.description,
            %error,
            "lost the connection to the agent"
        );

        let mut attempt: u32 = 0;

        loop {
            if let Some(limit) = self.reconnect.attempts
                && attempt >= limit
            {
                return Err(Error::Internal(format!(
                    "lost the connection to {} and could not rebuild it after \
                     {attempt} attempt(s): {error}",
                    self.description
                )));
            }

            attempt += 1;
            let wait = self.reconnect.wait_before(attempt);

            // Cancellation is checked as one arm of the wait, so a shutdown
            // during an outage does not have to sit out the backoff, which
            // matters more the longer the outage has run, since by then the
            // wait is the full ceiling.
            tokio::select! {
                () = self.cancel.cancelled() => {
                    return Err(Error::Internal(format!(
                        "shutting down while reconnecting to {}",
                        self.description
                    )));
                }
                () = tokio::time::sleep(wait) => {}
            }

            match self.reopen.open(&self.description).await {
                Ok(fresh) => {
                    tracing::info!(
                        agent = %self.description,
                        attempt,
                        "reconnected to the agent"
                    );

                    // Replaces the dead connection; dropping it kills the old
                    // child, which `kill_on_drop` handles.
                    *connection = fresh;

                    return Ok(());
                }
                Err(error) => report_retry(
                    &self.description,
                    attempt,
                    self.reconnect.wait_before(attempt + 1),
                    &error,
                ),
            }
        }
    }

    /// Sends a request expecting nothing back but acknowledgement.
    async fn request_ok(&self, request: Request) -> Result<()> {
        match self.request(request).await? {
            Response::Ok => Ok(()),
            other => Err(Error::Internal(format!(
                "expected an acknowledgement from the agent, got {other:?}"
            ))),
        }
    }

    /// Closes the session and waits for the agent to exit.
    ///
    /// Best effort by design: this runs on the shutdown path, where the useful
    /// outcome is that the child does not outlive the process. A failure here
    /// says nothing about whether the sync succeeded.
    pub async fn close(&self) {
        let mut connection = self.connection.lock().await;

        if let Err(error) = protocol::write_frame(&mut connection.output, &Request::Goodbye).await {
            tracing::debug!(%error, "could not send goodbye to the agent");
        }

        match connection.child.wait().await {
            Ok(status) if status.success() => {}
            Ok(status) => tracing::warn!(%status, "the agent exited with a failure"),
            Err(error) => tracing::warn!(%error, "could not wait for the agent"),
        }
    }
}

/// Logs a failed reconnect without drowning a long outage in noise.
///
/// The first failure and then roughly one a minute at `warn`, because an
/// operator needs to see that the mirror is down and needs to still be able to
/// read the log an hour later. Everything in between is `debug`.
fn report_retry(description: &str, attempt: u32, next_wait: Duration, error: &Error) {
    // Tuned to the backoff rather than to the attempt count. Once the wait has
    // reached its ceiling every six attempts is about a minute, which is the
    // cadence a long outage should report at: often enough that an operator
    // watching the log sees it is still trying, rare enough not to bury the
    // reason. Counting attempts alone would drift to one line every ten minutes
    // as the interval grew.
    const LOUD_EVERY: u32 = 6;

    if attempt == 1 || attempt.is_multiple_of(LOUD_EVERY) {
        tracing::warn!(
            agent = %description,
            attempt,
            retry_in = ?next_wait,
            %error,
            "cannot reach the agent; still retrying"
        );
    } else {
        tracing::debug!(
            agent = %description,
            attempt,
            retry_in = ?next_wait,
            %error,
            "reconnect failed"
        );
    }
}

/// How an SSH target is named in logs and errors.
pub(crate) fn describe(target: &SshTarget) -> String {
    format!("{} (started as `{}`)", target.host, target.agent_command())
}

/// Starts the agent on `target` over SSH and completes the handshake.
pub(crate) async fn open_for(target: &SshTarget) -> Result<Connection> {
    let mut command = target.command();
    command.arg("--").arg(target.agent_command());

    open(command, &describe(target)).await
}

/// One request and its reply, classifying anything that goes wrong.
async fn exchange(
    connection: &mut Connection,
    request: &Request,
) -> std::result::Result<Response, Failure> {
    protocol::write_frame(&mut connection.output, request)
        .await
        .map_err(Failure::Transport)?;

    let response: Response = protocol::expect_frame(&mut connection.input, "a reply")
        .await
        .map_err(Failure::Transport)?;

    response.into_result().map_err(Failure::Remote)
}

/// Sends one file's content and waits for the agent to publish it.
///
/// Opening the source is a local failure, not a transport one: the file being
/// gone is routine in a tree under active write, and no amount of reconnecting
/// will bring it back.
/// How far a previous attempt got, once that much has been *verified*.
///
/// This is the check the old whole-file path could not make, and the reason it
/// restarted from zero instead: resuming means building on bytes already on the
/// far side, and "they are there" is not the same claim as "they are the ones
/// this file has now".
///
/// Verifying it turns out to be cheap and exact. A reconstruction is supposed
/// to equal the source, so the first *n* bytes of the target's partial file
/// must equal the first *n* bytes of the source. The agent hashes what it has;
/// this hashes the same span locally and compares. A mismatch, whether a stale
/// leftover from a different version of the file or a truncated write, returns
/// zero and the transfer starts clean.
///
/// Returns zero on anything unexpected. Starting over is always correct, just
/// slower, so nothing here is worth failing a transfer over.
async fn resumable_from(
    connection: &mut Connection,
    source: &Path,
    relative: &Path,
) -> std::result::Result<u64, Failure> {
    let response = exchange(
        connection,
        &Request::ResumeState {
            path: WirePath::new(relative),
        },
    )
    .await?;

    let (bytes, hash) = match response.into_result().map_err(Failure::Remote)? {
        Response::ResumeState { bytes, hash } => (bytes, hash),
        _ => return Ok(0),
    };

    if bytes == 0 {
        return Ok(0);
    }

    let local = match hash_prefix(source, bytes).await {
        Ok(Some(hash)) => hash,
        // The source is now shorter than what is on the target, so that partial
        // file belongs to a version of it that no longer exists.
        Ok(None) => return Ok(0),
        Err(error) => return Err(Failure::Remote(error)),
    };

    if local != hash {
        tracing::debug!(
            path = %relative.display(),
            bytes,
            "a partial transfer does not match the source; starting over"
        );

        return Ok(0);
    }

    tracing::info!(
        path = %relative.display(),
        bytes,
        "resuming a transfer that was interrupted"
    );

    Ok(bytes)
}

/// BLAKE3 of the first `bytes` of a file, or `None` if it is shorter than that.
async fn hash_prefix(path: &Path, bytes: u64) -> Result<Option<[u8; blake3::OUT_LEN]>> {
    let mut file = tokio::fs::File::open(path).await.map_err(Error::from)?;

    if file.metadata().await.map_err(Error::from)?.len() < bytes {
        return Ok(None);
    }

    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; CHUNK_SIZE];
    let mut remaining = bytes;

    while remaining > 0 {
        let want = remaining.min(buffer.len() as u64) as usize;

        file.read_exact(&mut buffer[..want])
            .await
            .map_err(Error::from)?;
        hasher.update(&buffer[..want]);

        remaining -= want as u64;
    }

    Ok(Some(*hasher.finalize().as_bytes()))
}

/// Streams a file to the agent as the difference from what it already has.
///
/// Returns the number of literal bytes sent, which is the figure worth logging:
/// it is what the delta actually cost against the file's size.
///
/// The same failure discipline as [`send_file`]. A source that cannot be read
/// through is an `Abort`, not a hangup, because the agent is mid-transfer and
/// owes a reply. It discards its temporary, so the file already on the
/// target is untouched.
async fn send_patch(
    connection: &mut Connection,
    source: &Path,
    relative: &Path,
    signature: &delta::Signature,
    resume_from: u64,
) -> std::result::Result<u64, Failure> {
    let file = tokio::fs::File::open(source)
        .await
        .map_err(|err| Failure::Remote(Error::from(err)))?;

    protocol::write_frame(
        &mut connection.output,
        &Request::PatchFile {
            path: WirePath::new(relative),
            resume_from,
        },
    )
    .await
    .map_err(Failure::Transport)?;

    let mut matcher = delta::Matcher::new(file, signature);
    let mut sent = 0u64;
    // Where in the reconstructed file the next token starts. The scan runs from
    // the beginning either way, since it has to, both to find matches and to
    // hash the whole source, but on a resume the tokens covering ground the target
    // already has are computed and then dropped rather than sent.
    let mut produced = 0u64;

    let scanned = loop {
        match matcher.next_token().await {
            Ok(Some(token)) => {
                let length = match &token {
                    Token::Copy { len, .. } => *len,
                    Token::Literal(bytes) => bytes.len() as u64,
                    _ => 0,
                };

                let start = produced;
                produced += length;

                // Wholly behind the resume point: already on the target.
                if produced <= resume_from {
                    continue;
                }

                // Straddling it. Both token kinds carry a length that can be
                // trimmed from the front, so the split lands on the exact byte
                // rather than the nearest block.
                let token = if start < resume_from {
                    let skip = resume_from - start;

                    match token {
                        Token::Copy { offset, len } => Token::Copy {
                            offset: offset + skip,
                            len: len - skip,
                        },
                        Token::Literal(bytes) => Token::Literal(bytes[skip as usize..].to_vec()),
                        other => other,
                    }
                } else {
                    token
                };

                if let Token::Literal(bytes) = &token {
                    sent += bytes.len() as u64;
                }

                protocol::write_frame(&mut connection.output, &token)
                    .await
                    .map_err(Failure::Transport)?;
            }
            Ok(None) => break Ok(()),
            Err(error) => break Err(error),
        }
    };

    match scanned {
        Ok(()) => {
            // Stat'd after the content is read, not before, for the same reason
            // the whole-file path does it: a file rewritten mid-transfer then
            // arrives with the newer mtime and is caught by the next pass
            // rather than looking settled.
            let mtime = tokio::fs::metadata(source)
                .await
                .and_then(|metadata| metadata.modified())
                .map_err(|err| Failure::Remote(Error::from(err)))?;

            protocol::write_frame(
                &mut connection.output,
                &Token::Commit {
                    mtime: WireTime::new(mtime),
                    hash: matcher.hash(),
                },
            )
            .await
            .map_err(Failure::Transport)?;
        }
        Err(error) => {
            protocol::write_frame(
                &mut connection.output,
                &Token::Abort {
                    reason: error.to_string(),
                },
            )
            .await
            .map_err(Failure::Transport)?;
        }
    }

    let response: Response = protocol::expect_frame(&mut connection.input, "a patch reply")
        .await
        .map_err(Failure::Transport)?;

    match response.into_result().map_err(Failure::Remote)? {
        Response::Ok => Ok(sent),
        other => Err(Failure::Remote(Error::Internal(format!(
            "expected an acknowledgement for a patch, got {other:?}"
        )))),
    }
}

async fn send_file(
    connection: &mut Connection,
    source: &Path,
    relative: &Path,
) -> std::result::Result<u64, Failure> {
    let mut file = tokio::fs::File::open(source)
        .await
        .map_err(|err| Failure::Remote(Error::from(err)))?;

    protocol::write_frame(
        &mut connection.output,
        &Request::WriteFile {
            path: WirePath::new(relative),
        },
    )
    .await
    .map_err(Failure::Transport)?;

    let mut buffer = vec![0u8; CHUNK_SIZE];
    let mut sent = 0u64;

    let read = loop {
        match file.read(&mut buffer).await {
            Ok(0) => break Ok(()),
            Ok(read) => {
                sent += read as u64;

                protocol::write_frame(
                    &mut connection.output,
                    &Chunk::Data(buffer[..read].to_vec()),
                )
                .await
                .map_err(Failure::Transport)?;
            }
            Err(error) => break Err(Error::from(error)),
        }
    };

    match read {
        Ok(()) => {
            // Read after the content, not before, so a file rewritten during
            // the transfer arrives stamped with the newer time and is caught
            // by the next pass rather than looking settled.
            let mtime = file
                .metadata()
                .await
                .and_then(|metadata| metadata.modified())
                .map_err(|err| Failure::Remote(Error::from(err)))?;

            protocol::write_frame(
                &mut connection.output,
                &Chunk::Commit {
                    mtime: WireTime::new(mtime),
                },
            )
            .await
            .map_err(Failure::Transport)?;
        }
        Err(error) => {
            // The agent is mid-transfer and owes a reply, so it has to be told
            // to stop rather than left waiting. It discards its temporary and
            // the file on the target is untouched.
            protocol::write_frame(
                &mut connection.output,
                &Chunk::Abort {
                    reason: error.to_string(),
                },
            )
            .await
            .map_err(Failure::Transport)?;
        }
    }

    let response: Response = protocol::expect_frame(&mut connection.input, "a transfer reply")
        .await
        .map_err(Failure::Transport)?;

    match response.into_result().map_err(Failure::Remote)? {
        Response::Ok => Ok(sent),
        other => Err(Failure::Remote(Error::Internal(format!(
            "expected an acknowledgement for a transfer, got {other:?}"
        )))),
    }
}

/// Spawns an agent and completes the handshake.
async fn open(mut command: Command, description: &str) -> Result<Connection> {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Without this the agent survives a client that panics or is killed,
        // holding the SSH session open until the target host times it out.
        .kill_on_drop(true);

    tracing::debug!(agent = %description, "starting the agent");

    let mut child = command.spawn().map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => {
            Error::Config("no `ssh` on PATH; treesync shells out to it for remote targets".into())
        }
        _ => Error::from(err),
    })?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| Error::Internal("ssh child has no stdin".to_string()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Internal("ssh child has no stdout".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::Internal("ssh child has no stderr".to_string()))?;

    // SSH's own diagnostics and the agent's logs both arrive here. Drained
    // rather than inherited so they cannot interleave with this process's
    // stdout, and so a full pipe buffer cannot wedge the child mid-transfer.
    // The task ends when the child closes the stream, which `kill_on_drop`
    // above bounds to the sink's own lifetime.
    let label = description.to_string();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr);
        let mut buffer = String::new();

        loop {
            buffer.clear();

            match lines.read_line(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => tracing::debug!(agent = %label, "{}", buffer.trim_end()),
            }
        }
    });

    let mut connection = Connection {
        child,
        input: BufReader::new(stdout),
        output: BufWriter::new(stdin),
    };

    handshake(&mut connection, description).await?;

    Ok(connection)
}

/// Agrees a protocol version before anything is asked of the agent.
async fn handshake(connection: &mut Connection, description: &str) -> Result<()> {
    protocol::write_frame(
        &mut connection.output,
        &Request::Hello {
            version: PROTOCOL_VERSION,
        },
    )
    .await
    .map_err(|error| unreachable_agent(description, error))?;

    let response: Response = protocol::expect_frame(&mut connection.input, "the handshake")
        .await
        .map_err(|error| unreachable_agent(description, error))?;

    match response.into_result()? {
        Response::Hello { version, build } if version == PROTOCOL_VERSION => {
            tracing::debug!(agent = %description, build = %build, "agent ready");

            Ok(())
        }
        Response::Hello { version, .. } => Err(Error::Unsupported(format!(
            "the agent at {description} speaks protocol {version}, \
             this treesync speaks {PROTOCOL_VERSION}"
        ))),
        other => Err(Error::Internal(format!(
            "expected a handshake from the agent, got {other:?}"
        ))),
    }
}

/// Turns a failed handshake into something an operator can act on.
///
/// The raw failure is a closed pipe, which is what a missing binary, a refused
/// login and an unknown host key all look like from here. The remedy is the
/// same in every case, so say it.
fn unreachable_agent(description: &str, error: Error) -> Error {
    Error::Internal(format!(
        "no agent answered at {description}: {error}. Check that the SSH login \
         itself succeeds and that the agent binary on the host is runnable there"
    ))
}

#[async_trait]
impl Sink for SshSink {
    async fn index(&self, scope: &Scope, options: &IndexOptions) -> Result<Index> {
        let request = Request::Index {
            scope: WireScope::new(scope),
            // The patterns rather than the compiled matcher, so the agent
            // indexes the target under exactly the exclusions this side used.
            exclude: options.filter.patterns().to_vec(),
            verify: WireVerify::new(options.verify),
        };

        match self.request(request).await? {
            Response::Index(index) => Ok(index.into_index()),
            other => Err(Error::Internal(format!(
                "expected an index from the agent, got {other:?}"
            ))),
        }
    }

    async fn create_dir(&self, relative: &Path) -> Result<()> {
        self.request_ok(Request::CreateDir {
            path: WirePath::new(relative),
        })
        .await
    }

    /// Streams the whole file to the agent, which publishes it atomically.
    ///
    /// The content is read and sent in [`CHUNK_SIZE`] pieces rather than loaded
    /// whole: a sync should not need memory proportional to its largest file.
    ///
    /// This is the path for when there is nothing on the target worth building
    /// on: a first transfer, or a file small enough that sending it beats
    /// working out what not to send. Where the target does have a copy,
    /// [`Sink::patch_file`] sends only the difference.
    ///
    /// A link that drops mid-file restarts from the beginning here. Resumption
    /// belongs to the delta path, where the client can check what survived
    /// against its own source before continuing; on this path the same check
    /// would cost a read of everything it was about to re-send anyway.
    async fn write_file(&self, source: &Path, relative: &Path) -> Result<()> {
        let mut connection = self.connection.lock().await;

        loop {
            match send_file(&mut connection, source, relative).await {
                Ok(sent) => {
                    self.sent
                        .fetch_add(sent, std::sync::atomic::Ordering::Relaxed);

                    return Ok(());
                }
                Err(Failure::Remote(error)) => return Err(error),
                Err(Failure::Transport(error)) => {
                    self.restore(&mut connection, error).await?;
                }
            }
        }
    }

    /// Sends only the parts of `source` the target does not already hold.
    ///
    /// Three ways out before any delta happens, each because the delta would
    /// cost more than it saved:
    ///
    /// - turned off in config;
    /// - a file small enough that sending it beats working out what not to
    ///   send;
    /// - a target that has nothing at this path, where every block would be a
    ///   literal and the signature round trip buys nothing.
    ///
    /// The signature is fetched and then the connection lock is retaken, so a
    /// target that changes in between would produce copies of blocks that are
    /// no longer there. That is not guarded against here because it does not
    /// need to be: the commit hash will not match, the agent refuses to
    /// publish, and the action is retried. A stale signature costs a pass, not
    /// correctness.
    async fn patch_file(&self, source: &Path, relative: &Path) -> Result<()> {
        if !self.delta.enabled {
            return self.write_file(source, relative).await;
        }

        let length = tokio::fs::metadata(source)
            .await
            .map_err(Error::from)?
            .len();

        if length < self.delta.min_size {
            return self.write_file(source, relative).await;
        }

        let block_size = self.delta.block_size(length);

        let signature = match self
            .request(Request::Signature {
                path: WirePath::new(relative),
                block_size,
            })
            .await?
        {
            Response::Signature(signature) => signature.into_signature(),
            other => {
                return Err(Error::Internal(format!(
                    "expected a signature, got {other:?}"
                )));
            }
        };

        if signature.blocks.is_empty() {
            return self.write_file(source, relative).await;
        }

        let mut connection = self.connection.lock().await;
        let mut resume_from = 0u64;

        loop {
            match send_patch(&mut connection, source, relative, &signature, resume_from).await {
                Ok(sent) => {
                    self.sent
                        .fetch_add(sent, std::sync::atomic::Ordering::Relaxed);

                    tracing::debug!(
                        path = %relative.display(),
                        length,
                        sent,
                        "patched a file"
                    );

                    return Ok(());
                }
                Err(Failure::Remote(error)) => return Err(error),
                Err(Failure::Transport(error)) => {
                    self.restore(&mut connection, error).await?;

                    // The link is back. Whatever the interrupted attempt left
                    // on the target is worth continuing from rather than
                    // discarding, but only once it has been checked. See
                    // `resumable_from`.
                    resume_from = match resumable_from(&mut connection, source, relative).await {
                        Ok(offset) => offset,
                        Err(Failure::Transport(_)) => 0,
                        Err(Failure::Remote(error)) => {
                            tracing::debug!(
                                path = %relative.display(),
                                %error,
                                "could not read back a partial transfer; starting over"
                            );

                            0
                        }
                    };
                }
            }
        }
    }

    async fn create_symlink(&self, relative: &Path, target: &Path) -> Result<()> {
        self.request_ok(Request::CreateSymlink {
            path: WirePath::new(relative),
            target: WirePath::new(target),
        })
        .await
    }

    async fn remove(&self, relative: &Path) -> Result<()> {
        self.request_ok(Request::Remove {
            path: WirePath::new(relative),
        })
        .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.request_ok(Request::Rename {
            from: WirePath::new(from),
            to: WirePath::new(to),
        })
        .await
    }

    async fn set_metadata(
        &self,
        relative: &Path,
        metadata: &Metadata,
        preserve: Preserve,
    ) -> Result<()> {
        self.request_ok(Request::SetMetadata {
            path: WirePath::new(relative),
            metadata: WireMetadata::new(metadata),
            preserve: WirePreserve::new(preserve),
        })
        .await
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn the_reconnect_wait_doubles_up_to_its_ceiling() {
        let policy = Reconnect::forever();

        // One second, then doubling, then held at ten. The first retry is quick
        // because most interruptions are; the ceiling is what stops a long
        // outage becoming a connection attempt every second against a host that
        // is already struggling.
        assert_eq!(policy.wait_before(1), Duration::from_secs(1));
        assert_eq!(policy.wait_before(2), Duration::from_secs(2));
        assert_eq!(policy.wait_before(3), Duration::from_secs(4));
        assert_eq!(policy.wait_before(4), Duration::from_secs(8));
        assert_eq!(policy.wait_before(5), Duration::from_secs(10));
        assert_eq!(policy.wait_before(6), Duration::from_secs(10));
    }

    #[test]
    fn the_reconnect_wait_holds_at_the_ceiling_however_long_the_outage() {
        let policy = Reconnect::forever();

        // A daemon can sit in this loop for days. The shift is clamped and the
        // multiply checked, so a large attempt number settles at the ceiling
        // rather than overflowing into a nonsense duration.
        for attempt in [30u32, 100, 10_000, u32::MAX] {
            assert_eq!(
                policy.wait_before(attempt),
                Duration::from_secs(10),
                "attempt {attempt} should wait exactly the ceiling"
            );
        }
    }

    #[test]
    fn every_policy_starts_at_one_second_and_tops_out_at_ten() {
        for policy in [
            Reconnect::forever(),
            Reconnect::bounded(5),
            Reconnect::never(),
        ] {
            assert_eq!(policy.interval, Duration::from_secs(1));
            assert_eq!(policy.max_interval, Duration::from_secs(10));
        }
    }

    #[test]
    fn a_bounded_policy_still_backs_off_between_its_attempts() {
        // `sync` retries five times. Without backoff that was five seconds of
        // hammering; with it the command spends about twenty-five seconds
        // trying before reporting, which is the useful trade for a one-shot run.
        let policy = Reconnect::bounded(5);
        let total: Duration = (1..=5).map(|attempt| policy.wait_before(attempt)).sum();

        assert_eq!(total, Duration::from_secs(1 + 2 + 4 + 8 + 10));
    }

    use super::*;

    fn target() -> SshTarget {
        SshTarget {
            host: "deploy@example.com".to_string(),
            path: PathBuf::from("/srv/app"),
            port: None,
            identity_file: None,
            agent_path: RemoteAgentPath::default(),
            options: Vec::new(),
        }
    }

    fn args(command: &Command) -> Vec<String> {
        command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect()
    }

    #[test]
    fn a_plain_word_is_quoted_as_itself() {
        assert_eq!(shell_quote("/srv/app"), "'/srv/app'");
    }

    #[test]
    fn a_path_that_would_be_a_shell_expansion_stays_a_path() {
        // The whole reason this function exists: the target path is operator
        // input and reaches a shell on another machine.
        let quoted = shell_quote("/srv/$(rm -rf ~)");

        assert_eq!(quoted, "'/srv/$(rm -rf ~)'");
    }

    #[test]
    fn a_single_quote_is_escaped_rather_than_ending_the_quoting() {
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
    }

    #[test]
    fn a_path_with_a_space_stays_one_word() {
        assert_eq!(shell_quote("/srv/my app"), "'/srv/my app'");
    }

    #[test]
    fn a_semicolon_cannot_start_a_second_command() {
        assert_eq!(shell_quote("/srv/a; reboot"), "'/srv/a; reboot'");
    }

    #[test]
    fn batch_mode_is_always_set() {
        // Without it a failed login prompts on a stdin carrying a binary
        // protocol, and the sync hangs instead of reporting anything.
        assert!(args(&target().command()).contains(&"BatchMode=yes".to_string()));
    }

    #[test]
    fn a_port_is_passed_through() {
        let mut target = target();
        target.port = Some(2222);

        let args = args(&target.command());

        assert!(args.contains(&"-p".to_string()));
        assert!(args.contains(&"2222".to_string()));
    }

    #[test]
    fn no_port_means_no_port_flag() {
        assert!(!args(&target().command()).contains(&"-p".to_string()));
    }

    #[test]
    fn an_identity_file_is_passed_with_identities_only() {
        let mut target = target();
        target.identity_file = Some(PathBuf::from("/root/.ssh/id_ed25519"));

        let args = args(&target.command());

        assert!(args.contains(&"/root/.ssh/id_ed25519".to_string()));
        assert!(
            args.contains(&"IdentitiesOnly=yes".to_string()),
            "an agent holding many keys would otherwise exhaust MaxAuthTries first"
        );
    }

    #[test]
    fn an_idle_connection_is_kept_alive() {
        // `watch` holds one connection for as long as it runs. Without this a
        // firewall drops an idle mapping and the next change after a quiet
        // spell blocks on a socket nobody is listening to.
        let args = args(&target().command());

        assert!(
            args.iter()
                .any(|arg| arg.starts_with("ServerAliveInterval=")),
            "{args:?}"
        );
        assert!(
            args.iter()
                .any(|arg| arg.starts_with("ServerAliveCountMax=")),
            "{args:?}"
        );
    }

    #[test]
    fn extra_options_are_passed_through() {
        let mut target = target();
        target.options = vec!["ProxyJump=bastion".to_string()];

        assert!(args(&target.command()).contains(&"ProxyJump=bastion".to_string()));
    }

    #[test]
    fn batch_mode_is_set_before_any_operator_option() {
        // ssh takes the first value it is given, so this is what makes
        // BatchMode non-negotiable: an interactive prompt on a stdin carrying
        // a binary protocol hangs the sync instead of failing it.
        let mut target = target();
        target.options = vec!["BatchMode=no".to_string()];

        let args = args(&target.command());
        let first = args
            .iter()
            .position(|arg| arg == "BatchMode=yes")
            .expect("BatchMode=yes is always set");
        let operator = args
            .iter()
            .position(|arg| arg == "BatchMode=no")
            .expect("the operator's option is still passed");

        assert!(first < operator, "{args:?}");
    }

    #[test]
    fn an_operator_option_precedes_the_overridable_defaults() {
        // ConnectTimeout is a default rather than a rule: a slow link is a
        // real reason to want longer.
        let mut target = target();
        target.options = vec!["ConnectTimeout=120".to_string()];

        let args = args(&target.command());
        let operator = args
            .iter()
            .position(|arg| arg == "ConnectTimeout=120")
            .expect("the operator's option is passed");
        let default = args
            .iter()
            .position(|arg| arg.starts_with("ConnectTimeout=") && arg != "ConnectTimeout=120")
            .expect("the default is still there");

        assert!(operator < default, "{args:?}");
    }

    #[test]
    fn the_host_is_the_last_ssh_argument() {
        let args = args(&target().command());

        assert_eq!(args.last().map(String::as_str), Some("deploy@example.com"));
    }

    #[test]
    fn the_default_agent_lives_under_the_remote_home() {
        let word = RemoteAgentPath::default().shell_word();

        assert!(
            word.starts_with("\"$HOME\"/"),
            "the login account's home is the one directory it is certain to be \
             able to write to: {word}"
        );
        assert!(word.contains("treesync"), "{word}");
    }

    #[test]
    fn an_absolute_agent_path_is_used_as_given() {
        let path = RemoteAgentPath::new(Path::new("/usr/local/bin/treesync"));

        assert_eq!(path.shell_word(), "'/usr/local/bin/treesync'");
    }

    #[test]
    fn an_agent_path_is_quoted_against_the_remote_shell() {
        let path = RemoteAgentPath::new(Path::new("/opt/my agent/treesync"));

        assert_eq!(path.shell_word(), "'/opt/my agent/treesync'");
    }

    #[test]
    fn the_agent_directory_is_derived_from_its_path() {
        let path = RemoteAgentPath::new(Path::new("/usr/local/bin/treesync"));

        assert_eq!(path.parent_shell_word(), "'/usr/local/bin'");
    }

    #[test]
    fn a_home_relative_agent_directory_keeps_the_home_expansion() {
        let path = RemoteAgentPath::UnderHome(".cache/treesync/treesync".to_string());

        assert_eq!(path.parent_shell_word(), "\"$HOME\"/'.cache/treesync'");
    }

    #[test]
    fn an_agent_in_the_home_directory_itself_has_home_as_its_parent() {
        let path = RemoteAgentPath::UnderHome("treesync".to_string());

        assert_eq!(path.parent_shell_word(), "\"$HOME\"");
    }

    #[test]
    fn the_remote_command_quotes_the_target_path() {
        let mut target = target();
        target.path = PathBuf::from("/srv/an app");

        let command = target.agent_command();

        assert!(command.contains("'/srv/an app'"), "{command}");
        assert!(command.contains("agent --root"), "{command}");
    }
}
