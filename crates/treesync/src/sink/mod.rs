//! Applies a [`Plan`] to a target.
//!
//! [`Sink`] is the seam between deciding and doing. The reconciler produces
//! relative paths, so the same plan applies to a directory on this machine or
//! to a tree on another host. The difference is which implementation executes
//! it, not what gets decided.

pub mod local;

pub use local::LocalSink;

use std::path::Path;

use async_trait::async_trait;

use crate::error::{Error, Result};
use crate::reconcile::{Action, Index, IndexOptions, Metadata, Plan, Preserve, Scope};

/// Somewhere a plan can be applied.
///
/// Every path is relative to the sink's own root. An implementation must reject
/// any path that would resolve outside it: for a remote sink these paths arrive
/// over a socket, and `../../etc/ssh` is what an attacker sends.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Reports what the target currently holds within `scope`.
    ///
    /// On the trait because the target is not necessarily reachable from here.
    /// A local sink walks it; a remote one asks its agent. Either way the
    /// reconciler compares two indexes and does not care which side produced
    /// them.
    ///
    /// `options` is passed in rather than held by the sink so both sides of a
    /// comparison are guaranteed to have used the same exclusions. A filter
    /// applied to only one tree turns every excluded file on the target into an
    /// apparent deletion.
    async fn index(&self, scope: &Scope, options: &IndexOptions) -> Result<Index>;

    /// Creates a directory and any missing parents.
    async fn create_dir(&self, relative: &Path) -> Result<()>;

    /// Copies `source` to `relative`, replacing whatever is there.
    ///
    /// Must preserve the source's modification time. The reconciler compares
    /// size and mtime to decide what to transfer, so a copy that stamps the
    /// target with the current time makes every file differ on the next pass
    /// and the sync never converges.
    ///
    /// Must also be atomic: a reader on the target sees the old file or the new
    /// one, never a half-written one.
    async fn write_file(&self, source: &Path, relative: &Path) -> Result<()>;

    /// Copies `source` to `relative`, sending only what the target lacks.
    ///
    /// Same contract as [`Self::write_file`] in every respect that matters:
    /// preserves mtime, publishes atomically, replaces whatever was there. The
    /// difference is only in what crosses the wire, which is why the reconciler
    /// calls this and never has to know which it got.
    ///
    /// Defaults to a whole-file copy. Overriding it pays only where the
    /// transfer is the expensive part: comparing two local files costs more
    /// than copying one of them.
    async fn patch_file(&self, source: &Path, relative: &Path) -> Result<()> {
        self.write_file(source, relative).await
    }

    /// Creates or replaces a symlink at `relative` pointing at `target`.
    async fn create_symlink(&self, relative: &Path, target: &Path) -> Result<()>;

    /// Removes a single path. Never recursive. See [`local::LocalSink`].
    async fn remove(&self, relative: &Path) -> Result<()>;

    /// Moves an existing path within the sink.
    async fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /// Applies ownership and permissions to an existing path.
    ///
    /// `preserve` selects which parts to apply, so a sink never guesses: a
    /// `chown` attempted without privilege fails per file, and doing that for
    /// a whole tree buries the failures that matter.
    async fn set_metadata(
        &self,
        relative: &Path,
        metadata: &Metadata,
        preserve: Preserve,
    ) -> Result<()>;
}

/// What happened when a plan was applied.
#[derive(Debug)]
pub struct ApplyReport {
    /// Actions that succeeded.
    pub applied: usize,
    /// Actions that did not, with the reason.
    pub failures: Vec<ApplyFailure>,
}

#[derive(Debug)]
pub struct ApplyFailure {
    pub action: Action,
    pub error: Error,
}

impl ApplyReport {
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }

    /// Paths that did not make it, for the caller to retry.
    pub fn failed_paths(&self) -> impl Iterator<Item = &Path> {
        self.failures
            .iter()
            .map(|failure| failure.action.path().as_path())
    }
}

/// Executes every action in order.
///
/// A failing action does not abort the run. One unreadable file should not
/// strand the rest of a batch, and the failures are reported so the caller can
/// requeue exactly those paths.
///
/// The order within the plan is still honoured, so a failure can cascade: if a
/// directory cannot be created, the files inside it fail too. That is reported
/// instead of hidden, and re-running the plan is safe, since every action is
/// idempotent.
pub async fn apply(
    plan: &Plan,
    source_root: &Path,
    sink: &dyn Sink,
    preserve: Preserve,
) -> ApplyReport {
    let mut report = ApplyReport {
        applied: 0,
        failures: Vec::new(),
    };

    for action in &plan.actions {
        let outcome = match action {
            Action::CreateDir(path) => sink.create_dir(path).await,
            Action::CopyFile(path) => sink.patch_file(&source_root.join(path), path).await,
            Action::CreateSymlink { path, target } => sink.create_symlink(path, target).await,
            Action::Remove(path) => sink.remove(path).await,
            Action::Rename { from, to } => sink.rename(from, to).await,
            Action::SetMetadata { path, metadata } => {
                sink.set_metadata(path, metadata, preserve).await
            }
        };

        match outcome {
            Ok(()) => report.applied += 1,
            Err(error) => {
                tracing::warn!(
                    path = %action.path().display(),
                    %error,
                    "action failed"
                );

                report.failures.push(ApplyFailure {
                    action: action.clone(),
                    error,
                });
            }
        }
    }

    tracing::debug!(
        applied = report.applied,
        failed = report.failures.len(),
        "applied plan"
    );

    report
}
