//! The `treesync` command.
//!
//! Thin by design: everything that decides or moves anything lives in the
//! [`treesync`] library, and this crate is the argument parsing, the logging
//! setup and the exit code around it.

mod cli;
mod telemetry;

use clap::Parser;
use treesync::error::{Error, Result};

use crate::cli::Cli;
use crate::telemetry::{LogSink, setup_telemetry_client_to};

/// Returning `Result` from `main` would print the error's `Debug` form,
/// `Config("no config file at /etc/treesync/config.toml")`, because that is
/// what `Termination` uses. Handling it here prints `Display` instead, which is
/// what the messages were written for, and keeps the exit code explicit.
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("treesync: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    // The agent writes a binary protocol on stdout, so its logs go to stderr.
    // A log line in the middle of a frame desynchronises the connection, and
    // what the client reports is a decode failure at some byte offset, which
    // says nothing about the cause.
    let sink = if cli.logs_to_stderr() {
        LogSink::Stderr
    } else {
        LogSink::Stdout
    };

    // Set up before any work so failures are reported through the same path as
    // everything else.
    let telemetry =
        setup_telemetry_client_to(env!("CARGO_PKG_NAME"), cli.log_level.as_deref(), sink)
            .map_err(Error::Config)?;

    let outcome = cli.run().await;

    // On the way out either way. A no-op while logging is stdout-only, but the
    // command's own shutdown flush would be worth little if the telemetry that
    // recorded it were dropped on the floor.
    telemetry.shutdown().await;

    outcome
}
