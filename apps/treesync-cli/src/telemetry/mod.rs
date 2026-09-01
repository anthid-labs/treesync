//! Process-wide logging and metrics setup.
//!
//! Lives in the CLI rather than the library because both are a property of a
//! *process*, not of syncing: one installs a global subscriber and the other a
//! global recorder, which are decisions only the binary at the top of the stack
//! gets to make. A library that did either would fight whatever its host
//! application had already set up. [`treesync`] emits `tracing` events and
//! `metrics` measurements, and leaves collecting both to the caller.

pub mod exporter;
pub mod provider;

pub use provider::{LogSink, TelemetryProvider, TelemetryProviderConfig};

/// Builds the process-wide telemetry provider, choosing where the logs go.
///
/// `log_level` is the fallback filter used when `RUST_LOG` is not set; pass
/// `None` to fall back to `info`. Returns the operator-facing message when
/// either filter is unparseable.
///
/// The sink is a parameter because a process whose stdout is a data stream
/// instead of a terminal has to send its diagnostics elsewhere. `treesync
/// agent` speaks a binary protocol on stdout, and a log line written there
/// lands inside a frame.
pub fn setup_telemetry_client_to(
    app_name: &str,
    log_level: Option<&str>,
    sink: LogSink,
) -> Result<TelemetryProvider, String> {
    let config = TelemetryProviderConfig {
        app_name: app_name.to_string(),
        log_level: log_level.map(str::to_string),
        sink,
    };

    TelemetryProvider::new(config)
}
