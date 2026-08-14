//! The `treesync` command line.
//!
//! Four commands, three of them for operators:
//!
//! - `check` parses and validates a config and prints what it resolves to.
//! - `sync` performs one reconcile pass and exits.
//! - `watch` keeps running, mirroring changes as they happen.
//! - `agent` is the far half of a remote sync. It is started over SSH by
//!   another treesync, speaks a binary protocol on stdin and stdout, and is
//!   hidden from `--help` because running it by hand does nothing useful.
//!
//! `sync` and `watch` are the same machinery reached two ways. Both open a
//! [`Syncer`] and reconcile through it; `sync` does one whole-tree pass and
//! stops, `watch` goes on to reconcile whatever the watcher reports. Nothing
//! in this module compares trees or applies a plan itself, so the two cannot
//! drift apart.

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use treesync::config::file::{Config, ResolvedSync, Target};
use treesync::error::{Error, Result};

use treesync::reconcile::{Action, Plan, Scope};
use treesync::syncer::{Mode, Syncer};

/// Where the config is read from when `--config` is not given.
const DEFAULT_CONFIG_PATH: &str = "/etc/treesync/config.toml";

#[derive(Debug, Parser)]
#[command(
    name = "treesync",
    version,
    about = "Watches directories and mirrors them"
)]
pub struct Cli {
    /// Path to the configuration file.
    #[arg(
        long,
        short,
        global = true,
        env = "TREESYNC_CONFIG",
        default_value = DEFAULT_CONFIG_PATH
    )]
    pub config: PathBuf,

    /// Log filter. `RUST_LOG` takes precedence when set.
    #[arg(long, global = true, env = "LOG_LEVEL")]
    pub log_level: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum Command {
    /// Validate the configuration and print what it resolves to.
    Check,

    /// Run a single reconcile pass and exit.
    Sync {
        /// Only the sync with this name. Defaults to every sync.
        #[arg(long)]
        name: Option<String>,

        /// Print what would be done without changing anything.
        #[arg(long)]
        dry_run: bool,
    },

    /// Watch the source trees and mirror changes until stopped.
    Watch {
        /// Only the sync with this name. Defaults to every sync.
        #[arg(long)]
        name: Option<String>,
    },

    /// Serve the remote half of a sync on stdin and stdout.
    ///
    /// Hidden because it is not an operator-facing command: the client starts
    /// it over SSH, and a human running it gets a process waiting for a binary
    /// protocol on a terminal.
    #[command(hide = true)]
    Agent {
        /// The tree this agent may write to. Everything it is asked to do is
        /// confined beneath it.
        #[arg(long)]
        root: PathBuf,
    },
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        // Before the config is read, because the agent has none. It is started
        // by a client that passes everything it needs on the command line, and
        // requiring a config file on the target host would mean provisioning
        // one there, which is the thing shipping the agent exists to avoid.
        if let Command::Agent { root } = self.command {
            return treesync::remote::agent::serve(root, tokio::io::stdin(), tokio::io::stdout())
                .await;
        }

        let config = Config::load(&self.config)?;

        match self.command {
            Command::Check => check(&config, &self.config),
            Command::Sync { name, dry_run } => sync(&config, name.as_deref(), dry_run).await,
            Command::Watch { name } => watch(&config, name.as_deref()).await,
            Command::Agent { .. } => unreachable!("handled above"),
        }
    }

    /// Whether this invocation owns stdout as a protocol stream.
    ///
    /// Logging to stdout would land in the middle of a frame and desynchronise
    /// the connection, so the agent's diagnostics go to stderr instead.
    pub fn logs_to_stderr(&self) -> bool {
        matches!(self.command, Command::Agent { .. })
    }
}

/// Reports what the configuration resolves to after defaults are applied.
fn check(config: &Config, path: &Path) -> Result<()> {
    let resolved = config.resolve();

    println!("{}: {} sync(s)", path.display(), resolved.len());

    for sync in &resolved {
        println!();
        println!("  [{}]", sync.name);
        println!("    source      {}", sync.source.display());

        match &sync.target {
            Target::Local { path } => println!("    target      {} (local)", path.display()),
            Target::Ssh {
                host, path, port, ..
            } => println!(
                "    target      {}:{} (ssh{})",
                host,
                path.display(),
                port.map(|port| format!(", port {port}"))
                    .unwrap_or_default()
            ),
        }

        println!("    delay       {:?}", sync.queue.delay);
        println!("    max pending {}", sync.queue.max_pending);
        println!("    delete      {}", sync.reconcile.delete);
        println!("    verify      {:?}", sync.reconcile.verify);
        println!(
            "    preserve    mode={} ownership={}",
            sync.reconcile.preserve.mode, sync.reconcile.preserve.ownership
        );

        // Only for a remote target: a local copy always sends the whole file,
        // so printing a delta setting there would describe something that
        // cannot happen.
        if matches!(sync.target, Target::Ssh { .. }) {
            if sync.delta.enabled {
                println!(
                    "    delta       on, over {} bytes, {}",
                    sync.delta.min_size,
                    match sync.delta.block_size {
                        Some(size) => format!("{size} byte blocks"),
                        None => "blocks sized per file".to_string(),
                    }
                );
            } else {
                println!("    delta       off (whole files)");
            }
        }

        if !sync.exclude.is_empty() {
            println!("    exclude     {:?}", sync.exclude);
        }
    }

    Ok(())
}

/// The syncs a `--name` selects, or all of them.
///
/// An unmatched name is an error rather than an empty run: silently doing
/// nothing is the worst outcome for a command whose whole job is to move data.
fn select(config: &Config, only: Option<&str>) -> Result<Vec<ResolvedSync>> {
    let resolved = config.resolve();

    if let Some(name) = only
        && !resolved.iter().any(|sync| sync.name == name)
    {
        return Err(Error::Config(format!(
            "no sync named {name:?}; configured: {}",
            resolved
                .iter()
                .map(|sync| sync.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    Ok(resolved
        .into_iter()
        .filter(|sync| only.is_none_or(|name| sync.name == name))
        .collect())
}

/// Runs one reconcile pass over every selected sync.
async fn sync(config: &Config, only: Option<&str>, dry_run: bool) -> Result<()> {
    for entry in select(config, only)? {
        sync_once(&entry, dry_run).await?;
    }

    Ok(())
}

/// Runs one full-tree pass for a single sync.
///
/// The comparison and the apply both belong to [`Syncer`], the same code the
/// watch loop runs, reached with a whole-tree scope and stopped after one pass.
/// What lives here is only what a one-shot command adds: printing the plan, and
/// turning failed actions into an exit code.
async fn sync_once(entry: &ResolvedSync, dry_run: bool) -> Result<()> {
    let mode = if dry_run { Mode::DryRun } else { Mode::Once };

    // A one-shot pass has no shutdown token to share: its reconnect policy is
    // bounded, so the command terminates on its own either way.
    let syncer = Syncer::open(entry, mode, CancellationToken::new()).await?;

    let plan = syncer.plan_for(&Scope::Subtree(PathBuf::new())).await?;

    println!(
        "[{}] {} -> {}: {} action(s){}",
        syncer.name(),
        syncer.source().display(),
        syncer.target_label(),
        plan.len(),
        if dry_run { "  (dry run)" } else { "" }
    );

    print_plan(&plan);

    if dry_run || plan.is_empty() {
        syncer.close().await;

        return Ok(());
    }

    let report = syncer.apply_plan(&plan).await?;
    syncer.close().await;

    if report.is_complete() {
        println!("  applied {}", report.applied);
        return Ok(());
    }

    // Reported per path rather than as a single count: the operator needs to
    // know which files did not make it.
    println!(
        "  applied {}, failed {}",
        report.applied,
        report.failures.len()
    );
    for failure in &report.failures {
        println!("    {}: {}", failure.action.path().display(), failure.error);
    }

    Err(Error::Internal(format!(
        "sync {:?} completed with {} failed action(s)",
        entry.name,
        report.failures.len()
    )))
}

/// Runs every selected sync until the process is asked to stop.
///
/// # Why every sync is opened before any of them runs
///
/// A daemon that starts three mirrors and discovers on the fourth that a host
/// is unreachable has already begun writing. Opening them all first makes a
/// misconfiguration a startup failure, the thing a supervisor will report and
/// an operator will see, instead of a partial state discovered later.
///
/// # Why one sync stopping stops all of them
///
/// A tree that has quietly stopped being mirrored looks exactly like one that
/// is up to date, and nothing downstream can tell the difference. Exiting
/// makes the failure visible and lets a supervisor restart the process;
/// carrying on would hide it for as long as the daemon stayed up.
async fn watch(config: &Config, only: Option<&str>) -> Result<()> {
    let selected = select(config, only)?;

    // Created before the syncers so each one can share it: a remote sink
    // waiting out an outage watches this token too, and a shutdown during an
    // outage should not have to wait for the link to come back.
    let cancel = CancellationToken::new();

    let mut syncers = Vec::with_capacity(selected.len());
    for entry in &selected {
        syncers.push(Syncer::open(entry, Mode::Watch, cancel.clone()).await?);
    }

    for syncer in &syncers {
        println!(
            "[{}] watching {} -> {}",
            syncer.name(),
            syncer.source().display(),
            syncer.target_label()
        );
    }

    let mut running = JoinSet::new();

    for syncer in syncers {
        running.spawn(async move {
            let outcome = syncer.run().await;
            let name = syncer.name().to_string();

            // Inside the task, so a remote session is closed on the way out
            // whichever way the loop ended.
            syncer.close().await;

            (name, outcome)
        });
    }

    // Not a detached task: the signal is one arm of this select, so it stops
    // being watched for the moment the syncs are done.
    let results = tokio::select! {
        signal = shutdown_signal() => {
            let signal = signal?;
            println!("received {signal}, flushing");
            tracing::info!(%signal, "shutting down");
            cancel.cancel();

            // Each syncer applies what it has already observed, bounded by its
            // own shutdown grace, and then returns.
            drain(&mut running, &cancel).await
        }
        results = drain(&mut running, &cancel) => results,
    };

    let failed: Vec<String> = results
        .into_iter()
        .filter_map(|(name, outcome)| outcome.err().map(|error| format!("{name}: {error}")))
        .collect();

    if failed.is_empty() {
        println!("stopped");

        return Ok(());
    }

    Err(Error::Internal(format!(
        "sync(s) stopped with an error: {}",
        failed.join("; ")
    )))
}

/// Waits for every sync to finish, stopping the rest if one ends early.
///
/// A syncer only returns when it is cancelled or when something went wrong, so
/// one finishing while the others run means the daemon is already degraded.
async fn drain(
    running: &mut JoinSet<(String, Result<()>)>,
    cancel: &CancellationToken,
) -> Vec<(String, Result<()>)> {
    let mut results = Vec::new();

    while let Some(joined) = running.join_next().await {
        match joined {
            Ok((name, outcome)) => {
                if let Err(error) = &outcome {
                    tracing::error!(sync = %name, %error, "sync stopped with an error");
                }

                if !cancel.is_cancelled() {
                    tracing::warn!(sync = %name, "sync ended on its own; stopping the rest");
                    cancel.cancel();
                }

                results.push((name, outcome));
            }
            Err(error) => {
                // A panicked task. The sync it was running is gone, so the
                // same rule applies as for one that ended early.
                tracing::error!(%error, "a sync task did not finish cleanly");
                cancel.cancel();

                results.push((
                    "unknown".to_string(),
                    Err(Error::Internal(format!("sync task panicked: {error}"))),
                ));
            }
        }
    }

    results
}

/// Resolves when the process is asked to stop, naming the signal.
///
/// Both signals are handled explicitly because the shipped image runs treesync
/// as PID 1, and the kernel installs no default handlers for PID 1: a process
/// that does not catch SIGTERM ignores `docker stop` entirely and is SIGKILLed
/// at the timeout, which discards the shutdown flush.
async fn shutdown_signal() -> Result<&'static str> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())
        .map_err(|err| Error::Internal(format!("cannot handle SIGTERM: {err}")))?;
    let mut interrupt = signal(SignalKind::interrupt())
        .map_err(|err| Error::Internal(format!("cannot handle SIGINT: {err}")))?;

    Ok(tokio::select! {
        _ = terminate.recv() => "SIGTERM",
        _ = interrupt.recv() => "SIGINT",
    })
}

fn print_plan(plan: &Plan) {
    for action in &plan.actions {
        let (verb, detail) = match action {
            Action::CreateDir(path) => ("mkdir ", path.display().to_string()),
            Action::CopyFile(path) => ("copy  ", path.display().to_string()),
            Action::CreateSymlink { path, target } => (
                "link  ",
                format!("{} -> {}", path.display(), target.display()),
            ),
            Action::Remove(path) => ("remove", path.display().to_string()),
            Action::Rename { from, to } => {
                ("rename", format!("{} -> {}", from.display(), to.display()))
            }
            Action::SetMetadata { path, metadata } => {
                ("chmod ", format!("{} {:o}", path.display(), metadata.mode))
            }
        };

        println!("  {verb}  {detail}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).expect("should parse")
    }

    #[test]
    fn the_command_definition_is_well_formed() {
        // Catches conflicting flags, bad defaults and duplicate names, none of
        // which are compile errors.
        Cli::command().debug_assert();
    }

    #[test]
    fn config_defaults_when_not_given() {
        assert_eq!(
            parse(&["treesync", "check"]).config,
            PathBuf::from(DEFAULT_CONFIG_PATH)
        );
    }

    #[test]
    fn config_is_accepted_before_or_after_the_subcommand() {
        // `global = true`, so operators are not made to remember an order.
        let before = parse(&["treesync", "--config", "/tmp/a.toml", "check"]);
        let after = parse(&["treesync", "check", "--config", "/tmp/a.toml"]);

        assert_eq!(before.config, PathBuf::from("/tmp/a.toml"));
        assert_eq!(before.config, after.config);
    }

    #[test]
    fn the_short_config_flag_works() {
        assert_eq!(
            parse(&["treesync", "-c", "/tmp/a.toml", "check"]).config,
            PathBuf::from("/tmp/a.toml")
        );
    }

    #[test]
    fn sync_defaults_to_every_sync_and_to_applying() {
        assert_eq!(
            parse(&["treesync", "sync"]).command,
            Command::Sync {
                name: None,
                dry_run: false,
            }
        );
    }

    #[test]
    fn sync_accepts_a_name_and_a_dry_run() {
        assert_eq!(
            parse(&["treesync", "sync", "--name", "www", "--dry-run"]).command,
            Command::Sync {
                name: Some("www".to_string()),
                dry_run: true,
            }
        );
    }

    #[test]
    fn a_subcommand_is_required() {
        assert!(
            Cli::try_parse_from(["treesync"]).is_err(),
            "running with no command must not silently do nothing"
        );
    }

    #[test]
    fn an_unknown_subcommand_is_rejected() {
        assert!(Cli::try_parse_from(["treesync", "frobnicate"]).is_err());
    }

    #[test]
    fn an_unknown_flag_is_rejected() {
        assert!(Cli::try_parse_from(["treesync", "sync", "--delete-everything"]).is_err());
    }
}
