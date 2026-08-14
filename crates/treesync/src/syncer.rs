//! One config entry, running.
//!
//! Composes the pieces into the unit a supervisor will eventually spawn per
//! `[[sync]]` block: watch the source, batch what changes, work out what the
//! target needs, apply it.
//!
//! # Order of operations at startup
//!
//! The watch is established *before* the first full reconcile, not after.
//! Between establishing a watch and finishing a walk of the tree, anything can
//! change; if the watch came second those changes would fall in the gap and
//! nothing would ever report them. Watching first means they queue up and are
//! handled after the initial pass. That is redundant work, which is the right side to
//! err on.
//!
//! # Why each batch re-stats
//!
//! A batch naming three files stats three files. The index is not carried
//! between batches and the target is not remembered: what treesync believes it
//! wrote is not evidence of what is on disk, and the whole design rests on the
//! filesystem being the authority. The saving over rsync is not from caching,
//! it is from never walking the tree when nothing asked us to.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config::file::{ResolvedSync, Target};

/// How long a cancelled sync keeps working to apply what it already observed.
///
/// Bounded, because a tree under continuous change would otherwise never let
/// the process exit. Sized to sit inside Docker's default ten seconds between
/// SIGTERM and SIGKILL, so the flush finishes rather than being killed midway.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Cap on paths carried forward after a failed action.
///
/// A target that is failing wholesale would otherwise grow this without bound
/// while retrying work that is not going to succeed.
const MAX_RETRIES: usize = 1_000;
use crate::error::{Error, Result};
use crate::queue::{Batch, EventQueue};
use crate::reconcile::{
    Filter, Index, IndexOptions, Plan, ReconcileConfig, Scope, index_scope, plan,
};
use crate::remote::{Reconnect, SshSink, ship};
use crate::sink::{ApplyReport, LocalSink, Sink, apply};
use crate::watcher;

/// What a syncer is being opened to do.
///
/// Carried from construction rather than passed per call, because the
/// differences start before any plan exists: opening a writable target creates
/// a missing local root and installs the agent on a remote host, and how long
/// to wait out a network outage depends on whether anything will ever ask
/// again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// One whole-tree pass, then stop.
    ///
    /// A dropped link is retried a few times so a blip does not throw away a
    /// large transfer, then reported. Waiting indefinitely would leave a
    /// command run from cron hanging, and the next tick would start another.
    Once,

    /// Keep mirroring until stopped.
    ///
    /// A dropped link is retried until it comes back. For a daemon the
    /// alternative is a mirror that silently stopped, which looks exactly like
    /// one that is up to date.
    Watch,

    /// Work out what would happen and stop.
    ///
    /// Nothing is created locally and nothing is installed remotely, so a dry
    /// run against a host that has never been synced to reports that rather
    /// than quietly provisioning it.
    DryRun,
}

/// Attempts a one-shot pass makes before giving up on a dropped link.
///
/// Enough to ride out a brief blip mid-transfer, short enough that a host
/// which is genuinely down is reported in seconds rather than never.
const ONCE_RECONNECT_ATTEMPTS: u32 = 5;

impl Mode {
    /// Whether the target may be written to.
    fn writes(self) -> bool {
        !matches!(self, Mode::DryRun)
    }

    /// How this mode wants a lost connection handled.
    fn reconnect(self) -> Reconnect {
        match self {
            Mode::Watch => Reconnect::forever(),
            Mode::Once => Reconnect::bounded(ONCE_RECONNECT_ATTEMPTS),
            // A dry run makes one request and stops; there is nothing for a
            // rebuilt connection to go on to do.
            Mode::DryRun => Reconnect::never(),
        }
    }
}

/// A single `[[sync]]` entry, wired end to end.
///
/// The one place a tree is compared and a plan applied. A one-shot pass and
/// the watch loop are the same code reached two ways: `sync` plans the whole
/// tree once and stops, `watch` keeps planning the scopes the queue reports.
/// Two implementations of that would drift, and the one that drifted would be
/// the one nobody ran.
pub struct Syncer {
    name: String,
    /// Canonical, because watcher events arrive with symlinks resolved and
    /// paths are made relative against this.
    source_root: PathBuf,
    target: OpenTarget,
    mode: Mode,
    /// Shared with the remote sink, so a shutdown reaches a reconnect wait.
    cancel: CancellationToken,
    reconcile: ReconcileConfig,
    queue_config: crate::queue::QueueConfig,
    index_options: IndexOptions,
}

impl std::fmt::Debug for Syncer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Syncer")
            .field("name", &self.name)
            .field("source_root", &self.source_root)
            .field("target", &self.target.label)
            .finish_non_exhaustive()
    }
}

/// An opened target: somewhere to apply to, and what to call it in output.
struct OpenTarget {
    label: String,
    sink: OpenSink,
}

enum OpenSink {
    Local(LocalSink),
    /// Boxed because an open SSH session, with its child handle and both
    /// buffered streams, is an order of magnitude larger than a local root, and every
    /// value of this enum would otherwise be sized for the largest.
    Remote(Box<SshSink>),
    /// A local target that does not exist yet, seen under [`Mode::DryRun`],
    /// where creating it would be the change the flag promises not to make.
    Absent,
}

impl OpenTarget {
    fn sink(&self) -> Option<&dyn Sink> {
        match &self.sink {
            OpenSink::Local(sink) => Some(sink),
            OpenSink::Remote(sink) => Some(sink.as_ref()),
            OpenSink::Absent => None,
        }
    }

    /// Ends a remote session cleanly. A no-op for a local target.
    ///
    /// Without it the agent is killed when the child handle drops, which works
    /// but leaves a nonzero exit and an SSH error in the host's logs after
    /// every successful sync.
    async fn close(self) {
        if let OpenSink::Remote(sink) = self.sink {
            sink.close().await;
        }
    }
}

/// Opens whatever a sync targets.
async fn open_target(
    config: &ResolvedSync,
    mode: Mode,
    cancel: CancellationToken,
) -> Result<OpenTarget> {
    match &config.target {
        Target::Local { path } => {
            if !path.exists() {
                if !mode.writes() {
                    return Ok(OpenTarget {
                        label: format!("{} (local, does not exist yet)", path.display()),
                        sink: OpenSink::Absent,
                    });
                }

                // A first sync into a target that does not exist yet is the
                // normal case.
                tracing::info!(path = %path.display(), "creating target root");
                tokio::fs::create_dir_all(path).await.map_err(Error::from)?;
            }

            Ok(OpenTarget {
                label: path.display().to_string(),
                sink: OpenSink::Local(LocalSink::new(path.clone())?),
            })
        }

        Target::Ssh { host, path, .. } => {
            let ssh = config
                .target
                .ssh()
                .ok_or_else(|| Error::Internal("an ssh target with no details".to_string()))?;

            let sink = if mode.writes() {
                ship::connect(&ssh, config.target.agent_binary(), mode.reconnect(), cancel).await?
            } else {
                // Uploading a binary is a change to the host, and a dry run
                // makes none, so an uninstalled agent is reported instead of
                // quietly provisioned.
                SshSink::connect(&ssh).await.map_err(|error| {
                    Error::Unsupported(format!(
                        "cannot dry-run against {host}: {error}. \
                         --dry-run will not install the agent; run once without it"
                    ))
                })?
            };

            Ok(OpenTarget {
                label: format!("{host}:{} (ssh)", path.display()),
                sink: OpenSink::Remote(Box::new(sink.with_delta(config.delta))),
            })
        }
    }
}

impl Syncer {
    /// Opens a syncer from a resolved config entry.
    ///
    /// Fails here rather than at the first event if the source is missing or
    /// the target cannot be opened. A misconfigured sync should be apparent at
    /// startup, not the first time a file changes.
    ///
    /// Async because a remote target is opened by connecting to it: the agent
    /// is installed if need be and the protocol version agreed, so an
    /// unreachable host is a startup failure rather than a surprise on the
    /// first batch.
    /// `cancel` is the process's shutdown token. It is stored rather than
    /// passed to [`Syncer::run`] because it has to reach further than the
    /// watch loop: a remote sink waiting out a network outage is inside an
    /// action, and without the token a shutdown during an outage would sit in
    /// a retry loop until something killed it.
    pub async fn open(
        config: &ResolvedSync,
        mode: Mode,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let source_root = config
            .source
            .canonicalize()
            .map_err(|err| match err.kind() {
                std::io::ErrorKind::NotFound => Error::NotFound(format!(
                    "sync {:?}: source {}",
                    config.name,
                    config.source.display()
                )),
                std::io::ErrorKind::PermissionDenied => Error::PermissionDenied(format!(
                    "sync {:?}: source {}",
                    config.name,
                    config.source.display()
                )),
                _ => Error::Io(err),
            })?;

        // After the source is resolved, so a sync with an unreadable source
        // fails without having created a target directory or installed
        // anything on a remote host.
        let target = open_target(config, mode, cancel.clone()).await?;

        Ok(Self {
            name: config.name.clone(),
            source_root,
            target,
            mode,
            cancel,
            index_options: IndexOptions {
                filter: Filter::new(&config.exclude)?,
                verify: config.reconcile.verify,
            },
            reconcile: config.reconcile.clone(),
            queue_config: config.queue.clone(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The canonical source root being watched.
    pub fn source(&self) -> &Path {
        &self.source_root
    }

    /// How the target is named in operator-facing output.
    pub fn target_label(&self) -> &str {
        &self.target.label
    }

    /// Ends a remote session cleanly. A no-op for a local target.
    pub async fn close(self) {
        self.target.close().await;
    }

    /// Watches and reconciles until cancelled.
    ///
    /// Returns once `cancel` fires or the watcher stops. Errors from a single
    /// batch are logged and the loop continues: one unreadable file must not
    /// take the sync down.
    pub async fn run(&self) -> Result<()> {
        let cancel = self.cancel.clone();

        // Held for the lifetime of the loop; dropping it stops the watch.
        let (_watcher, stream) = watcher::watch(&self.source_root)?;
        let mut queue = EventQueue::new(stream, self.queue_config.clone());

        tracing::info!(sync = %self.name, "watching");

        // Everything that changed while treesync was not running is invisible to
        // the watcher, so the first pass has to compare the trees in full.
        // Failing here is fatal: it means the source or target cannot be read
        // at all, which no amount of later events will fix.
        let startup = self
            .reconcile_scope(&Scope::Subtree(PathBuf::new()))
            .await?;
        let mut retries = self.bound(startup.failed_paths().map(Path::to_path_buf).collect());

        loop {
            let batch = tokio::select! {
                biased;

                () = cancel.cancelled() => {
                    tracing::info!(sync = %self.name, "cancelled, flushing");
                    break;
                }
                batch = queue.next_batch() => batch,
            };

            let Some(batch) = batch else {
                tracing::warn!(sync = %self.name, "watcher stopped");
                break;
            };

            retries = self.handle(batch, retries).await;
        }

        self.flush(&mut queue, retries).await;

        tracing::info!(sync = %self.name, "stopped");

        Ok(())
    }

    /// Applies what has already been observed, then stops.
    ///
    /// Without this, cancelling would discard changes that were seen but whose
    /// batching window had not yet closed. They would not be lost outright,
    /// since the next startup pass finds them, but only by walking the whole tree to
    /// rediscover work that was already in hand.
    async fn flush(&self, queue: &mut EventQueue, mut retries: Vec<PathBuf>) {
        let deadline = tokio::time::Instant::now() + SHUTDOWN_GRACE;

        // `drain` yields `None` the moment nothing is pending, so an idle sync
        // shuts down immediately instead of sitting out the grace period.
        while let Some(batch) = queue.drain() {
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    sync = %self.name,
                    "shutdown grace elapsed; the next startup pass will pick up the rest"
                );

                return;
            }

            retries = self.handle(batch, retries).await;
        }

        if retries.is_empty() {
            return;
        }

        // One last attempt at whatever was failing, so a transient error does
        // not leave the target diverged until the next run.
        tracing::info!(sync = %self.name, paths = retries.len(), "retrying failed paths");
        if let Err(error) = self.reconcile_scope(&Scope::Paths(retries)).await {
            tracing::error!(sync = %self.name, %error, "final retry failed");
        }
    }

    /// Reconciles one batch, returning the paths that still need attention.
    async fn handle(&self, batch: Batch, mut retries: Vec<PathBuf>) -> Vec<PathBuf> {
        let scope = match batch {
            Batch::Changes(changes) => {
                let mut paths: Vec<PathBuf> = changes
                    .paths
                    .iter()
                    .filter_map(|path| self.relativize(path))
                    .collect();

                // Folded in here rather than reconciled separately: a path that
                // failed and has since changed again should be looked at once.
                paths.append(&mut retries);
                paths.sort();
                paths.dedup();

                if paths.is_empty() {
                    return Vec::new();
                }

                tracing::debug!(sync = %self.name, paths = paths.len(), "batch");

                Scope::Paths(paths)
            }
            Batch::Rescan { root } => {
                let prefix = self.relativize(&root).unwrap_or_default();

                // The walk covers everything beneath it, so those retries are
                // subsumed. Ones outside stay queued for the next batch.
                retries.retain(|path| !path.starts_with(&prefix));

                tracing::warn!(
                    sync = %self.name,
                    prefix = %prefix.display(),
                    "reconciling a subtree in full after an event gap"
                );

                Scope::Subtree(prefix)
            }
        };

        let mut carried = match self.reconcile_scope(&scope).await {
            Ok(report) => report.failed_paths().map(Path::to_path_buf).collect(),
            Err(error) => {
                // Logged rather than returned: the next batch may well succeed,
                // and stopping the sync would leave the target frozen.
                tracing::error!(sync = %self.name, %error, "reconcile failed");

                Vec::new()
            }
        };

        carried.append(&mut retries);

        self.bound(carried)
    }

    /// Deduplicates and caps the retry set.
    fn bound(&self, mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
        paths.sort();
        paths.dedup();

        if paths.len() > MAX_RETRIES {
            tracing::warn!(
                sync = %self.name,
                dropped = paths.len() - MAX_RETRIES,
                "too many failing paths to retry; dropping the excess"
            );

            paths.truncate(MAX_RETRIES);
        }

        paths
    }

    /// Compares both sides within `scope` and returns what the target needs.
    ///
    /// Pure with respect to both trees: it reads two snapshots and returns a
    /// plan. That is what lets `--dry-run` be the same code path as a real
    /// pass, stopping one step earlier, rather than a second implementation
    /// that has to be kept honest.
    pub async fn plan_for(&self, scope: &Scope) -> Result<Plan> {
        let source_root = self.source_root.clone();
        let owned = scope.clone();
        let options = self.index_options.clone();

        // Statting is blocking work, and a full-tree scope can be a lot of it;
        // hashing under `Verify::Checksum` is more so.
        let source_index =
            tokio::task::spawn_blocking(move || index_scope(&source_root, &owned, &options))
                .await
                .map_err(|err| Error::Internal(format!("index task failed: {err}")))??;

        let target_index = match self.target.sink() {
            Some(sink) => sink.index(scope, &self.index_options).await?,
            // Only reachable under `Mode::DryRun` against a target that does
            // not exist yet, which nothing created.
            None => Index::default(),
        };

        Ok(plan(&source_index, &target_index, scope, &self.reconcile))
    }

    /// Applies a plan produced by [`Syncer::plan_for`].
    ///
    /// Fails only when there is nothing to apply to, which means a dry run
    /// asked to write. That is a bug in the caller and not an operational
    /// failure. Individual actions that fail are reported in the
    /// [`ApplyReport`], not here.
    pub async fn apply_plan(&self, plan: &Plan) -> Result<ApplyReport> {
        let sink = self.target.sink().ok_or_else(|| {
            Error::Internal(format!(
                "sync {:?} was opened in {:?} mode and has no target to apply to",
                self.name, self.mode
            ))
        })?;

        let report = apply(plan, &self.source_root, sink, self.reconcile.preserve).await;

        if report.is_complete() {
            tracing::info!(sync = %self.name, applied = report.applied, "reconciled");
        } else {
            tracing::warn!(
                sync = %self.name,
                applied = report.applied,
                failed = report.failures.len(),
                "reconciled with failures"
            );
        }

        Ok(report)
    }

    /// Compares both sides within `scope` and applies the difference.
    async fn reconcile_scope(&self, scope: &Scope) -> Result<ApplyReport> {
        let plan = self.plan_for(scope).await?;

        if plan.is_empty() {
            return Ok(ApplyReport {
                applied: 0,
                failures: Vec::new(),
            });
        }

        self.apply_plan(&plan).await
    }

    /// Makes a watcher path relative to the source root.
    fn relativize(&self, path: &Path) -> Option<PathBuf> {
        match path.strip_prefix(&self.source_root) {
            Ok(relative) if relative.as_os_str().is_empty() => Some(PathBuf::new()),
            Ok(relative) => Some(relative.to_path_buf()),
            Err(_) => {
                tracing::warn!(
                    sync = %self.name,
                    path = %path.display(),
                    "ignoring a path outside the source root"
                );

                None
            }
        }
    }
}
