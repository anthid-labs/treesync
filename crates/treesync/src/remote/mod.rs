//! Syncing to a tree on another host.
//!
//! Three pieces, in the order a sync meets them:
//!
//! - [`ship`] gets the agent onto the host and opens a connection to it.
//! - [`ssh`] is the client half: a [`Sink`](crate::sink::Sink) whose actions
//!   travel over that connection.
//! - [`agent`] is the far half, applying them through the same
//!   [`LocalSink`](crate::sink::LocalSink) a local target would use.
//!
//! [`protocol`] is what the two halves say to each other, and [`delta`] is how
//! a changed file is reduced to the parts of it that actually changed.
//!
//! # What is deliberately not here
//!
//! No listening daemon and no port. The agent lives exactly as long as the SSH
//! child that started it, and it has precisely the access the SSH login had.
//! there is no second authentication system to configure, and nothing left
//! running on the host between syncs.

pub mod agent;
pub mod delta;
pub mod protocol;
pub mod ship;
pub mod ssh;

pub use ssh::{Reconnect, RemoteAgentPath, SshSink, SshTarget};
