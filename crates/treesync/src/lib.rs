//! One-way directory mirroring.
//!
//! Watches a tree and mirrors it to another directory on this machine, or to
//! a host over SSH, sending only what actually changed. An experiment in
//! replacing [lsyncd](https://github.com/lsyncd/lsyncd).
//!
//! For the command line tool, install `treesync-cli`. This crate is the engine
//! underneath it, for embedding the same machinery in another program.
//!
//! # The shape of a sync
//!
//! Four stages, and the boundaries between them are where the design lives:
//!
//! 1. [`watcher`] reports what the kernel saw.
//! 2. [`queue`] collapses a burst of events into the distinct paths that
//!    changed.
//! 3. [`reconcile`] compares those paths across both trees and produces a
//!    [`Plan`](reconcile::Plan).
//! 4. [`sink`] applies the plan, either to a local directory or through an
//!    agent on a remote host ([`remote`]).
//!
//! [`syncer`] is what holds those together, and is the type most callers want.
//!
//! # Two things follow from that shape
//!
//! **The filesystem is the authority, not the event stream.** Event kinds are
//! not trusted. On macOS/FSEvents, deleting a file arrives labelled as a
//! creation. A batch says only *which paths are suspect*; the reconciler stats
//! them.
//!
//! **Lost events cost a re-walk, never correctness.** When the kernel drops
//! events or the queue fills, treesync reconciles that subtree in full rather
//! than replaying a log it knows has a hole in it.
//!
//! # Getting started
//!
//! A [`Syncer`](syncer::Syncer) is opened from a
//! [`ResolvedSync`](config::file::ResolvedSync), which is what a parsed
//! [`Config`](config::file::Config) resolves to once defaults are applied:
//!
//! ```no_run
//! use tokio_util::sync::CancellationToken;
//! use treesync::config::file::Config;
//! use treesync::syncer::{Mode, Syncer};
//!
//! # async fn example() -> treesync::error::Result<()> {
//! let config = Config::load("/etc/treesync/config.toml")?;
//!
//! for entry in config.resolve() {
//!     let syncer = Syncer::open(&entry, Mode::Once, CancellationToken::new()).await?;
//!     // `run` for a resident mirror; here, one whole-tree pass.
//!     syncer.run().await?;
//!     syncer.close().await;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Logging
//!
//! Events are emitted through [`tracing`]. No subscriber is installed here;
//! that belongs to whatever binary is at the top of the stack.
//!
//! # Remote targets
//!
//! Nothing has to be installed on the host first. treesync connects over SSH
//! and, if no usable agent answers, uploads one and connects again. The agent
//! *is* this binary, run as `treesync agent`, so there is no second artifact to
//! build or keep in step. A changed file is sent as a rolling-checksum delta
//! against the copy the target already holds, verified end to end with BLAKE3,
//! and resumable if the link drops. See [`remote`] and [`remote::delta`].

pub mod config;
pub mod error;
pub mod queue;
pub mod reconcile;
pub mod remote;
pub mod sink;
pub mod syncer;
pub mod watcher;
