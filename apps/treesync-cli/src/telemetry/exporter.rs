//! The Prometheus scrape endpoint.
//!
//! Pull, not push. Prometheus asks this process for its numbers over HTTP, so
//! nothing has to run alongside treesync to collect them. A push exporter would
//! mean operating a gateway next to a daemon whose whole point is that it has
//! no dependencies to operate, so that half of the exporter crate is not
//! compiled in.
//!
//! Lives in the CLI for the same reason the log subscriber does: installing a
//! recorder is process-wide, and a library that did it would fight whatever its
//! host application had already set up.
//!
//! # No idle timeout
//!
//! Series are served for as long as the process runs, rather than expiring
//! after a period with no updates. treesync does not reload its config, so the
//! set of syncs is fixed for the lifetime of the process and nothing can go
//! stale. Expiring a quiet one would be actively wrong: a mirror whose tree
//! nobody touched is working correctly, and dropping its counters would break
//! `rate()` across the gap and make it look like the sync had gone away.

use std::net::SocketAddr;

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};

/// Bucket edges for every `_seconds` histogram.
///
/// Spans a millisecond to a quarter of an hour, because these measure both a
/// batch of three files and the whole-tree walk that a restart begins with, and
/// those are five or six orders of magnitude apart. Roughly a decade in four
/// steps, which keeps a quantile useful without a series per bucket per sync
/// getting out of hand.
const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0,
];

/// Bucket edges for [`treesync::metrics::BATCH_PATHS`].
///
/// A count, not a duration, and it runs from one file saved in an editor to the
/// ten thousand a build drops at once, so it needs its own scale. The top edge
/// is the queue's own `max_pending` default: a batch at or above it was cut
/// short by that limit rather than by the batching window, which is worth
/// seeing as its own bucket.
const BATCH_BUCKETS: &[f64] = &[
    1.0, 2.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0,
];

/// Starts serving metrics on `listen`, and registers what they mean.
///
/// Returns the operator-facing message on a port that cannot be bound, which is
/// worth failing startup over: a daemon that silently came up without its
/// metrics looks exactly like one that is working.
///
/// Must be called from inside the tokio runtime. The listener is spawned onto
/// the current handle, and it is what answers a scrape.
pub fn install(listen: SocketAddr, version: &str) -> Result<(), String> {
    PrometheusBuilder::new()
        .with_http_listener(listen)
        .set_buckets(DURATION_BUCKETS)
        .map_err(|err| format!("metrics duration buckets: {err}"))?
        .set_buckets_for_metric(
            Matcher::Full(treesync::metrics::BATCH_PATHS.to_string()),
            BATCH_BUCKETS,
        )
        .map_err(|err| format!("metrics batch buckets: {err}"))?
        .install()
        .map_err(|err| format!("serving metrics on {listen}: {err}"))?;

    // After the recorder, never before. A description recorded while the no-op
    // recorder is still installed is dropped, and the scrape then has numbers
    // with no `# HELP` or `# TYPE` line above them.
    treesync::metrics::describe();
    treesync::metrics::set_build_info(version);

    tracing::info!(%listen, "serving metrics");

    Ok(())
}
