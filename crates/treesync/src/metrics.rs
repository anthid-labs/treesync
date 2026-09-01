//! What treesync publishes about itself.
//!
//! Split the same way logging is. This crate *records* through the [`metrics`]
//! facade and installs no recorder; the binary at the top of the stack decides
//! where the numbers go. With nothing installed every call here is a branch on
//! an atomic, so an embedder that wants none of this pays nothing for it.
//!
//! # What is a counter and what is a gauge
//!
//! Rates are not published. `treesync_actions_applied_total` is a counter and
//! `rate(treesync_actions_applied_total[5m])` is the files per second, which is
//! both cheaper to record and correct across restarts and scrape gaps. A gauge
//! holding "files per second" would be treesync's own average over a window
//! nobody chose, and would be wrong the moment a scrape was missed.
//!
//! Time since the last sync is the same case. What is published is
//! [`SYNC_LAST_SUCCESS`], the wall-clock instant of the last successful pass,
//! and `time() - treesync_sync_last_success_timestamp_seconds` is the age. A
//! counter of seconds-since would only be right at the instant it was set.
//!
//! # Cardinality
//!
//! Every series is labelled `sync`, the `name` from its `[[sync]]` block, and
//! by nothing else that varies at runtime. Paths are never labels: a label per
//! file would produce one series per file in the tree, which is how a metrics
//! backend is taken down by the thing it is meant to be watching.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};

use crate::reconcile::ActionCounts;
use crate::sink::TransferStats;

/// Version and build identity, always `1`. The value carries nothing; the
/// labels are the point, so a dashboard can tell two releases apart.
pub const BUILD_INFO: &str = "treesync_build_info";

/// Reconcile passes started, by scope. One per whole-tree pass, per batch, and
/// per rescan.
pub const SYNC_PASSES: &str = "treesync_sync_passes_total";
/// Passes that ended without an error, however many individual actions failed.
pub const SYNC_PASS_DURATION: &str = "treesync_sync_pass_duration_seconds";
/// Passes abandoned entirely, which means a tree could not be read at all.
pub const SYNC_PASS_FAILURES: &str = "treesync_sync_pass_failures_total";
/// Unix time of the last pass that completed with no failed action.
pub const SYNC_LAST_SUCCESS: &str = "treesync_sync_last_success_timestamp_seconds";
/// `1` while a pass is running.
pub const SYNC_IN_PROGRESS: &str = "treesync_sync_in_progress";

/// Time spent building one side's index. The walk, and the hashing under
/// `verify = "checksum"`.
pub const INDEX_DURATION: &str = "treesync_index_duration_seconds";
/// Time spent comparing the two indexes. The diff alone, with no I/O in it.
pub const PLAN_DURATION: &str = "treesync_plan_duration_seconds";
/// Time spent applying a plan, which is where a transfer actually happens.
pub const APPLY_DURATION: &str = "treesync_apply_duration_seconds";

/// Entries in the tree, from the last whole-tree pass. See [`Side`].
pub const TREE_ENTRIES: &str = "treesync_tree_entries";
/// Bytes of file content in the tree, from the last whole-tree pass.
pub const TREE_BYTES: &str = "treesync_tree_bytes";
/// Unix time those two were last refreshed.
///
/// They are only meaningful for a whole-tree pass, since an incremental batch
/// indexes the handful of paths it was told about. Publishing this alongside
/// them is what makes a stale value detectable rather than misleading.
pub const TREE_WALK_TIMESTAMP: &str = "treesync_tree_walk_timestamp_seconds";

/// Actions a plan called for, by kind.
pub const PLAN_ACTIONS: &str = "treesync_plan_actions_total";
/// Actions that succeeded, by kind.
pub const ACTIONS_APPLIED: &str = "treesync_actions_applied_total";
/// Actions that failed, by kind. Retried, not lost.
pub const ACTIONS_FAILED: &str = "treesync_actions_failed_total";

/// Batches taken off the queue, by kind. See [`BatchKind`].
pub const BATCHES: &str = "treesync_batches_total";
/// Distinct paths in a batch.
pub const BATCH_PATHS: &str = "treesync_batch_paths";
/// Paths accumulated in the queue but not yet batched.
pub const QUEUE_PENDING: &str = "treesync_queue_pending";
/// Seconds from the first event of a batch to that batch being applied.
///
/// The number an operator means by "how far behind is the mirror". It includes
/// the batching window, so it has a floor of roughly `delay`.
pub const SYNC_LAG: &str = "treesync_sync_lag_seconds";
/// Paths carried forward after a failed action, waiting for another attempt.
pub const RETRY_PATHS: &str = "treesync_retry_paths";
/// Paths dropped because too many were failing at once to keep retrying.
pub const RETRY_DROPPED: &str = "treesync_retry_dropped_total";

/// File content bytes actually moved: written locally, or put on the wire.
pub const TRANSFER_BYTES: &str = "treesync_transfer_bytes_total";
/// Bytes of the files those transfers covered.
///
/// Against [`TRANSFER_BYTES`], this is what the delta is worth:
/// `1 - rate(treesync_transfer_bytes_total[1h]) /
/// rate(treesync_transfer_logical_bytes_total[1h])` is the fraction not sent.
pub const TRANSFER_LOGICAL_BYTES: &str = "treesync_transfer_logical_bytes_total";
/// Files transferred, by method. See [`TransferMethod`].
pub const TRANSFER_FILES: &str = "treesync_transfer_files_total";
/// Times a remote link dropped and was rebuilt.
pub const REMOTE_RECONNECTS: &str = "treesync_remote_reconnects_total";

/// Which part of the tree a pass covered.
///
/// A label rather than three metric names, so a dashboard can sum passes
/// without knowing the variants, and `scope="full"` alone answers "how long
/// does a whole tree take".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassScope {
    /// The whole tree, root to leaf.
    Full,
    /// Everything beneath one directory, after an event gap.
    Subtree,
    /// Only the paths a batch named.
    Paths,
}

impl PassScope {
    /// Classifies the scope a pass was given.
    ///
    /// An empty subtree prefix is the whole tree, and is worth separating: it
    /// is the startup pass and the repair after a total gap, and its duration
    /// is the "time to sync the full tree" figure. Rolled in with subtree
    /// repairs it would be lost among much shorter passes.
    pub fn of(scope: &crate::reconcile::Scope) -> Self {
        match scope {
            crate::reconcile::Scope::Subtree(prefix) if prefix.as_os_str().is_empty() => Self::Full,
            crate::reconcile::Scope::Subtree(_) => Self::Subtree,
            crate::reconcile::Scope::Paths(_) => Self::Paths,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Subtree => "subtree",
            Self::Paths => "paths",
        }
    }

    /// Whether a pass at this scope saw the whole tree, and so may publish
    /// [`TREE_ENTRIES`] and [`TREE_BYTES`].
    pub fn is_whole_tree(self) -> bool {
        matches!(self, Self::Full)
    }
}

/// Which tree an index was built from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Source,
    Target,
}

impl Side {
    fn label(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Target => "target",
        }
    }
}

/// Why a batch was emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchKind {
    /// Paths the watcher reported.
    Changes,
    /// A gap in the event stream, which costs a walk.
    Rescan,
}

impl BatchKind {
    fn label(self) -> &'static str {
        match self {
            Self::Changes => "changes",
            Self::Rescan => "rescan",
        }
    }
}

/// How a file's content reached the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMethod {
    /// Sent or copied in full.
    Whole,
    /// Only the blocks the target did not already hold.
    Delta,
}

impl TransferMethod {
    fn label(self) -> &'static str {
        match self {
            Self::Whole => "whole",
            Self::Delta => "delta",
        }
    }
}

/// Registers help text and units for everything above.
///
/// Descriptions are attached to a name, not to a series, so a metric that has
/// not been recorded yet still renders with its `# HELP` and `# TYPE` lines.
/// Call this once, after installing a recorder and before any sync runs, or
/// the first scrape of a quiet daemon is a wall of bare numbers.
pub fn describe() {
    describe_gauge!(BUILD_INFO, "Version of the running treesync. Always 1.");

    describe_counter!(SYNC_PASSES, Unit::Count, "Reconcile passes started.");
    describe_histogram!(
        SYNC_PASS_DURATION,
        Unit::Seconds,
        "End to end duration of a reconcile pass."
    );
    describe_counter!(
        SYNC_PASS_FAILURES,
        Unit::Count,
        "Passes abandoned because a tree could not be read."
    );
    describe_gauge!(
        SYNC_LAST_SUCCESS,
        Unit::Seconds,
        "Unix time of the last pass in which every action succeeded."
    );
    describe_gauge!(SYNC_IN_PROGRESS, Unit::Count, "1 while a pass is running.");

    describe_histogram!(
        INDEX_DURATION,
        Unit::Seconds,
        "Time spent building one side's index."
    );
    describe_histogram!(
        PLAN_DURATION,
        Unit::Seconds,
        "Time spent comparing the two indexes."
    );
    describe_histogram!(APPLY_DURATION, Unit::Seconds, "Time spent applying a plan.");

    describe_gauge!(
        TREE_ENTRIES,
        Unit::Count,
        "Entries seen in the tree on the last whole-tree pass."
    );
    describe_gauge!(
        TREE_BYTES,
        Unit::Bytes,
        "File content in the tree on the last whole-tree pass."
    );
    describe_gauge!(
        TREE_WALK_TIMESTAMP,
        Unit::Seconds,
        "Unix time of the whole-tree pass those totals came from."
    );

    describe_counter!(PLAN_ACTIONS, Unit::Count, "Actions a plan called for.");
    describe_counter!(ACTIONS_APPLIED, Unit::Count, "Actions that succeeded.");
    describe_counter!(ACTIONS_FAILED, Unit::Count, "Actions that failed.");

    describe_counter!(BATCHES, Unit::Count, "Batches taken off the queue.");
    describe_histogram!(BATCH_PATHS, Unit::Count, "Distinct paths in a batch.");
    describe_gauge!(
        QUEUE_PENDING,
        Unit::Count,
        "Paths observed but not yet batched."
    );
    describe_histogram!(
        SYNC_LAG,
        Unit::Seconds,
        "First event of a batch to that batch being applied."
    );
    describe_gauge!(RETRY_PATHS, Unit::Count, "Paths awaiting another attempt.");
    describe_counter!(
        RETRY_DROPPED,
        Unit::Count,
        "Paths dropped from the retry set because too many were failing."
    );

    describe_counter!(
        TRANSFER_BYTES,
        Unit::Bytes,
        "File content written to the target, or put on the wire."
    );
    describe_counter!(
        TRANSFER_LOGICAL_BYTES,
        Unit::Bytes,
        "Size of the files those transfers covered."
    );
    describe_counter!(TRANSFER_FILES, Unit::Count, "Files transferred.");
    describe_counter!(
        REMOTE_RECONNECTS,
        Unit::Count,
        "Times a remote link dropped and was rebuilt."
    );
}

/// Publishes the running version.
///
/// Takes the version rather than reading `CARGO_PKG_VERSION` so the number is
/// the binary's, not this library's. Those are the same in this workspace and
/// need not be for an embedder.
pub fn set_build_info(version: &str) {
    gauge!(BUILD_INFO, "version" => version.to_string()).set(1.0);
}

/// Records for one `[[sync]]` entry.
///
/// Holds the name so no call site has to pass it, which is the only label any
/// of these series carry. Cloning it per call allocates; every method here
/// fires once per pass or per batch, never per file, so that is not on a hot
/// path. The per-file counters are aggregated first and recorded once, which
/// is what [`ActionCounts`] and [`TransferStats`] are for.
#[derive(Debug, Clone)]
pub struct SyncMetrics {
    sync: String,
}

impl SyncMetrics {
    pub fn new(sync: impl Into<String>) -> Self {
        Self { sync: sync.into() }
    }

    /// The sync these series are labelled with.
    pub fn sync(&self) -> &str {
        &self.sync
    }

    /// A pass is about to start.
    pub fn pass_started(&self, scope: PassScope) {
        counter!(SYNC_PASSES, "sync" => self.sync.clone(), "scope" => scope.label()).increment(1);
        gauge!(SYNC_IN_PROGRESS, "sync" => self.sync.clone()).set(1.0);
    }

    /// A pass finished, whatever it found.
    ///
    /// `complete` is whether every action in it succeeded, which is what moves
    /// [`SYNC_LAST_SUCCESS`]. A pass that applied nine of ten files has not
    /// brought the target into line, and an alert on the age of that gauge
    /// should fire for it.
    pub fn pass_finished(&self, scope: PassScope, elapsed: Duration, complete: bool) {
        gauge!(SYNC_IN_PROGRESS, "sync" => self.sync.clone()).set(0.0);
        histogram!(SYNC_PASS_DURATION, "sync" => self.sync.clone(), "scope" => scope.label())
            .record(elapsed.as_secs_f64());

        if complete {
            gauge!(SYNC_LAST_SUCCESS, "sync" => self.sync.clone()).set(unix_now());
        }
    }

    /// A pass was abandoned before it produced a plan.
    pub fn pass_failed(&self, scope: PassScope, elapsed: Duration) {
        gauge!(SYNC_IN_PROGRESS, "sync" => self.sync.clone()).set(0.0);
        counter!(SYNC_PASS_FAILURES, "sync" => self.sync.clone(), "scope" => scope.label())
            .increment(1);
        histogram!(SYNC_PASS_DURATION, "sync" => self.sync.clone(), "scope" => scope.label())
            .record(elapsed.as_secs_f64());
    }

    /// One side's index was built.
    ///
    /// `totals` is `None` for a scope that saw only part of the tree. Passing
    /// a batch's handful of entries as the tree's size would make the gauge
    /// swing to near zero every time a file changed.
    pub fn indexed(&self, side: Side, elapsed: Duration, totals: Option<TreeTotals>) {
        histogram!(INDEX_DURATION, "sync" => self.sync.clone(), "side" => side.label())
            .record(elapsed.as_secs_f64());

        let Some(totals) = totals else {
            return;
        };

        gauge!(TREE_ENTRIES, "sync" => self.sync.clone(), "side" => side.label())
            .set(totals.entries as f64);
        gauge!(TREE_BYTES, "sync" => self.sync.clone(), "side" => side.label())
            .set(totals.bytes as f64);
        gauge!(TREE_WALK_TIMESTAMP, "sync" => self.sync.clone()).set(unix_now());
    }

    /// The two indexes were compared.
    pub fn planned(&self, elapsed: Duration, counts: &ActionCounts) {
        histogram!(PLAN_DURATION, "sync" => self.sync.clone()).record(elapsed.as_secs_f64());
        self.record_actions(PLAN_ACTIONS, counts);
    }

    /// A plan was applied.
    pub fn applied(&self, elapsed: Duration, applied: &ActionCounts, failed: &ActionCounts) {
        histogram!(APPLY_DURATION, "sync" => self.sync.clone()).record(elapsed.as_secs_f64());
        self.record_actions(ACTIONS_APPLIED, applied);
        self.record_actions(ACTIONS_FAILED, failed);
    }

    /// A batch came off the queue.
    pub fn batch(&self, kind: BatchKind, paths: usize) {
        counter!(BATCHES, "sync" => self.sync.clone(), "kind" => kind.label()).increment(1);
        histogram!(BATCH_PATHS, "sync" => self.sync.clone()).record(paths as f64);
    }

    /// How far behind the mirror was when a batch finished.
    pub fn lag(&self, lag: Duration) {
        histogram!(SYNC_LAG, "sync" => self.sync.clone()).record(lag.as_secs_f64());
    }

    /// Paths observed but not yet batched.
    pub fn queue_pending(&self, pending: usize) {
        gauge!(QUEUE_PENDING, "sync" => self.sync.clone()).set(pending as f64);
    }

    /// The retry set after a batch: how many are carried, how many were given
    /// up on.
    pub fn retries(&self, carried: usize, dropped: usize) {
        gauge!(RETRY_PATHS, "sync" => self.sync.clone()).set(carried as f64);

        if dropped > 0 {
            counter!(RETRY_DROPPED, "sync" => self.sync.clone()).increment(dropped as u64);
        }
    }

    /// Mirrors a sink's running totals.
    ///
    /// Set absolutely rather than incremented, because the sink already holds
    /// the total. Reading it and setting the counter to what it says cannot
    /// drift, and cannot double count when a caller records twice; a delta
    /// computed at each call site would do both.
    pub fn transfer(&self, stats: TransferStats) {
        counter!(TRANSFER_BYTES, "sync" => self.sync.clone()).absolute(stats.bytes);
        counter!(TRANSFER_LOGICAL_BYTES, "sync" => self.sync.clone()).absolute(stats.logical_bytes);
        counter!(
            TRANSFER_FILES,
            "sync" => self.sync.clone(),
            "method" => TransferMethod::Whole.label()
        )
        .absolute(stats.whole_files);
        counter!(
            TRANSFER_FILES,
            "sync" => self.sync.clone(),
            "method" => TransferMethod::Delta.label()
        )
        .absolute(stats.delta_files);
        counter!(REMOTE_RECONNECTS, "sync" => self.sync.clone()).absolute(stats.reconnects);
    }

    /// Emits one series per action kind, skipping the kinds with nothing in
    /// them.
    ///
    /// Skipping keeps a mirror that only ever writes files from carrying five
    /// permanently-zero series per sync. The cost is that a kind stays absent
    /// until it first happens, which `describe` covers: the name is still
    /// documented in the scrape.
    fn record_actions(&self, metric: &'static str, counts: &ActionCounts) {
        for (kind, count) in counts.iter() {
            if count == 0 {
                continue;
            }

            counter!(metric, "sync" => self.sync.clone(), "action" => kind).increment(count as u64);
        }
    }
}

/// The size of one tree, as of a whole-tree walk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TreeTotals {
    /// Files, directories and symlinks.
    pub entries: usize,
    /// File content only. A directory has no size worth reporting and a
    /// symlink's is its target string, neither of which is what "how big is
    /// this tree" means.
    pub bytes: u64,
}

/// Seconds since the Unix epoch, as Prometheus wants a timestamp gauge.
///
/// A clock set before 1970 yields zero rather than failing. The alternative is
/// a metrics call that can panic, which is not a trade worth making for a
/// machine whose clock is that wrong.
fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs_f64())
        .unwrap_or_default()
}
