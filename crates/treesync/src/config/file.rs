//! The treesync configuration file.
//!
//! TOML, for three reasons that rule out the alternatives:
//!
//! - **It cannot execute anything.** lsyncd's config *is* a Lua program, so
//!   reading one means running it. A config file is data.
//! - **It is line-oriented and indentation-insensitive**, so generating one
//!   from a template is mechanical. JSON is not: a templated loop leaves a
//!   trailing comma, and there is nowhere to put a comment explaining why a
//!   value is what it is.
//! - **Unknown keys are rejected.** A misspelled option is a startup error, not
//!   a setting that silently never applied.
//!
//! ```toml
//! [defaults]
//! delay = "1s"
//!
//! [[sync]]
//! name = "www"
//! source = "/var/www"
//! exclude = ["*.tmp"]
//!
//!   [sync.target]
//!   type = "local"
//!   path = "/backup/www"
//! ```

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::queue::QueueConfig;
use crate::reconcile::{Filter, Preserve, ReconcileConfig, Verify};
use crate::remote::delta::Options as DeltaOptions;
use crate::remote::{RemoteAgentPath, SshTarget};

/// A parsed configuration file.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Applied to every sync that does not override them.
    #[serde(default)]
    pub defaults: Defaults,

    /// One entry per `[[sync]]` block.
    #[serde(rename = "sync", default)]
    pub syncs: Vec<Sync>,
}

/// Values inherited by every sync block.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Defaults {
    /// How long to batch events before acting. Accepts `500ms`, `2s`, `1m`.
    #[serde(default, with = "humantime_serde")]
    pub delay: Option<Duration>,

    /// Distinct paths that force an early flush.
    pub max_pending: Option<usize>,

    /// Whether to remove target paths the source no longer has.
    pub delete: Option<bool>,

    /// `"quick"` (size and mtime) or `"checksum"` (also compare content).
    pub verify: Option<Verify>,

    /// Which attributes are mirrored. See [`Preserve`].
    pub preserve: Option<Preserve>,

    /// Whether a changed file is sent as a delta. See [`Delta`].
    pub delta: Option<Delta>,
}

/// Whether a changed file is sent whole, or only where it differs.
///
/// Only remote targets use this. A local copy is already cheap, and comparing
/// the two files to avoid copying one of them would cost more than the copy.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Delta {
    /// On by default. Turning it off restores whole-file transfers, which is
    /// the right trade only when the link is faster than the disks.
    #[serde(default = "yes")]
    pub enabled: bool,

    /// Files below this go whole.
    ///
    /// A delta costs a round trip and a read of both copies. Under a megabyte
    /// or so, sending the file is simply cheaper than working out what not to
    /// send.
    #[serde(default = "default_min_size")]
    pub min_size: u64,

    /// Block size for the signature, or `None` to size it from the file.
    ///
    /// The default scales with the square root of the file's length, which is
    /// almost always what you want; set this only to measure against it.
    #[serde(default)]
    pub block_size: Option<u32>,
}

fn yes() -> bool {
    true
}

fn default_min_size() -> u64 {
    1024 * 1024
}

impl Default for Delta {
    fn default() -> Self {
        Self {
            enabled: yes(),
            min_size: default_min_size(),
            block_size: None,
        }
    }
}

impl Delta {
    /// The running shape of these settings.
    pub fn options(&self) -> DeltaOptions {
        DeltaOptions {
            enabled: self.enabled,
            min_size: self.min_size,
            block_size: self.block_size,
        }
    }
}

/// One source tree and where it is mirrored to.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Sync {
    /// Identifies this sync in logs and metrics. Must be unique.
    pub name: String,

    /// Absolute path to the tree being watched.
    pub source: PathBuf,

    pub target: Target,

    /// Patterns excluded from the sync, applied to both trees.
    ///
    /// `*.tmp` matches at any depth; `node_modules/` matches the directory and
    /// its contents; `build/*.o` is anchored at the source root.
    #[serde(default)]
    pub exclude: Vec<String>,

    #[serde(default, with = "humantime_serde")]
    pub delay: Option<Duration>,
    pub max_pending: Option<usize>,
    pub delete: Option<bool>,
    pub verify: Option<Verify>,
    pub preserve: Option<Preserve>,
    pub delta: Option<Delta>,
}

/// Where a source tree is mirrored to.
///
/// Tagged explicitly rather than inferred from which keys are present: an
/// untagged enum reports a typo in `host` as "data did not match any variant",
/// which tells the operator nothing about what to fix.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Target {
    /// Another directory on this machine.
    Local { path: PathBuf },

    /// A directory on a host reachable over SSH.
    Ssh {
        /// `user@host`, or a `~/.ssh/config` alias.
        host: String,
        path: PathBuf,
        port: Option<u16>,
        /// Private key to authenticate with. Defaults to the agent's usual
        /// lookup when unset.
        identity_file: Option<PathBuf>,

        /// Where the agent binary lives on the remote host.
        ///
        /// A relative path is taken from the login account's home directory,
        /// which is the one place it is certain to be able to write. Set an
        /// absolute path to put the agent somewhere shared instead.
        agent_path: Option<PathBuf>,

        /// Local binary to install on the host when no usable agent is there.
        ///
        /// Defaults to this executable, which is right whenever both ends run
        /// the same platform. Syncing from a mac to a Linux host needs a build
        /// for the host, and treesync checks that before uploading anything
        /// rather than leaving an unrunnable file behind.
        agent_binary: Option<PathBuf>,

        /// Extra `-o` options for the SSH client, as `Key=value`.
        ///
        /// For everything `ssh_config` can express that has no key here:
        /// `ProxyJump` for a bastion, `UserKnownHostsFile` for a daemon with
        /// no home directory, `ControlPath` to reuse a connection.
        #[serde(default)]
        ssh_options: Vec<String>,
    },
}

impl Target {
    /// The destination path, whichever kind of target this is.
    pub fn path(&self) -> &Path {
        match self {
            Target::Local { path } | Target::Ssh { path, .. } => path,
        }
    }

    /// The connection details, for an ssh target.
    ///
    /// Built here rather than in the sink so the config stays the one place
    /// that knows what a `[sync.target]` block means.
    pub fn ssh(&self) -> Option<SshTarget> {
        match self {
            Target::Local { .. } => None,
            Target::Ssh {
                host,
                path,
                port,
                identity_file,
                agent_path,
                ssh_options,
                ..
            } => Some(SshTarget {
                host: host.clone(),
                path: path.clone(),
                port: *port,
                identity_file: identity_file.clone(),
                agent_path: agent_path
                    .as_deref()
                    .map(RemoteAgentPath::new)
                    .unwrap_or_default(),
                options: ssh_options.clone(),
            }),
        }
    }

    /// The local binary to install on the host, when one is configured.
    pub fn agent_binary(&self) -> Option<&Path> {
        match self {
            Target::Local { .. } => None,
            Target::Ssh { agent_binary, .. } => agent_binary.as_deref(),
        }
    }
}

/// A sync with defaults applied, in the shape the running components take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSync {
    pub name: String,
    pub source: PathBuf,
    pub target: Target,
    pub exclude: Vec<String>,
    pub queue: QueueConfig,
    pub reconcile: ReconcileConfig,
    pub delta: DeltaOptions,
}

impl Config {
    /// Reads and validates a configuration file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let contents = std::fs::read_to_string(path).map_err(|err| match err.kind() {
            std::io::ErrorKind::NotFound => {
                Error::Config(format!("no config file at {}", path.display()))
            }
            std::io::ErrorKind::PermissionDenied => {
                Error::PermissionDenied(format!("config file {}", path.display()))
            }
            _ => Error::Io(err),
        })?;

        Self::parse(&contents).map_err(|err| Error::Config(format!("{}: {err}", path.display())))
    }

    /// Parses and validates configuration text.
    ///
    /// Structural only: no path is stat'd here, so this stays pure and a config
    /// can be checked without the trees existing.
    pub fn parse(contents: &str) -> Result<Self> {
        let config: Config =
            toml::from_str(contents).map_err(|err| Error::Config(err.to_string()))?;

        config.validate()?;

        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.syncs.is_empty() {
            return Err(Error::Config(
                "no [[sync]] blocks: treesync would have nothing to do".to_string(),
            ));
        }

        let mut seen = HashSet::new();

        for sync in &self.syncs {
            if sync.name.trim().is_empty() {
                return Err(Error::Config("a [[sync]] has an empty name".to_string()));
            }

            if !seen.insert(sync.name.as_str()) {
                return Err(Error::Config(format!(
                    "two [[sync]] blocks are both named {:?}; names identify them in logs",
                    sync.name
                )));
            }

            // Relative paths would resolve against whatever directory the
            // daemon happened to be started from.
            if !sync.source.is_absolute() {
                return Err(Error::Config(format!(
                    "sync {:?}: source {} must be an absolute path",
                    sync.name,
                    sync.source.display()
                )));
            }

            if !sync.target.path().is_absolute() {
                return Err(Error::Config(format!(
                    "sync {:?}: target path {} must be an absolute path",
                    sync.name,
                    sync.target.path().display()
                )));
            }

            // Compiled here so a malformed pattern is a startup error rather
            // than a surprise the first time a file changes.
            Filter::new(&sync.exclude)
                .map_err(|err| Error::Config(format!("sync {:?}: {err}", sync.name)))?;

            if let Target::Ssh { host, .. } = &sync.target
                && host.trim().is_empty()
            {
                return Err(Error::Config(format!(
                    "sync {:?}: ssh target has an empty host",
                    sync.name
                )));
            }

            // Writing into the tree being watched makes every write a new
            // event, which produces another write. The daemon would never go
            // idle and the target would grow without bound.
            if let Target::Local { path } = &sync.target
                && overlaps(&sync.source, path)
            {
                return Err(Error::Config(format!(
                    "sync {:?}: source {} and target {} overlap, which would feed the sync its own writes",
                    sync.name,
                    sync.source.display(),
                    path.display()
                )));
            }
        }

        Ok(())
    }

    /// Applies defaults to every sync.
    pub fn resolve(&self) -> Vec<ResolvedSync> {
        let fallback = QueueConfig::default();

        self.syncs
            .iter()
            .map(|sync| ResolvedSync {
                name: sync.name.clone(),
                source: sync.source.clone(),
                target: sync.target.clone(),
                exclude: sync.exclude.clone(),
                queue: QueueConfig {
                    delay: sync.delay.or(self.defaults.delay).unwrap_or(fallback.delay),
                    max_pending: sync
                        .max_pending
                        .or(self.defaults.max_pending)
                        .unwrap_or(fallback.max_pending),
                },
                reconcile: ReconcileConfig {
                    delete: sync.delete.or(self.defaults.delete).unwrap_or_default(),
                    verify: sync.verify.or(self.defaults.verify).unwrap_or_default(),
                    preserve: sync.preserve.or(self.defaults.preserve).unwrap_or_default(),
                },
                delta: sync
                    .delta
                    .or(self.defaults.delta)
                    .unwrap_or_default()
                    .options(),
            })
            .collect()
    }
}

/// Whether either path contains the other, or they are the same.
///
/// Lexical, so a symlink can defeat it. The paths are canonicalized when the
/// watch is established, which is where a disguised overlap surfaces.
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
[[sync]]
name = "www"
source = "/var/www"

  [sync.target]
  type = "local"
  path = "/backup/www"
"#;

    fn parse(contents: &str) -> Config {
        Config::parse(contents).expect("config should be valid")
    }

    fn reject(contents: &str) -> String {
        match Config::parse(contents) {
            Err(Error::Config(message)) => message,
            Err(other) => panic!("expected a config error, got {other:?}"),
            Ok(_) => panic!("expected this config to be rejected"),
        }
    }

    #[test]
    fn parses_a_minimal_config() {
        let config = parse(MINIMAL);

        assert_eq!(config.syncs.len(), 1);
        assert_eq!(config.syncs[0].name, "www");
        assert_eq!(config.syncs[0].source, PathBuf::from("/var/www"));
        assert_eq!(
            config.syncs[0].target,
            Target::Local {
                path: PathBuf::from("/backup/www")
            }
        );
    }

    #[test]
    fn parses_an_ssh_target() {
        let config = parse(
            r#"
[[sync]]
name = "app"
source = "/srv/app"

  [sync.target]
  type = "ssh"
  host = "deploy@example.com"
  path = "/srv/app"
  port = 2222
  identity_file = "/root/.ssh/id_ed25519"
"#,
        );

        assert_eq!(
            config.syncs[0].target,
            Target::Ssh {
                host: "deploy@example.com".to_string(),
                path: PathBuf::from("/srv/app"),
                port: Some(2222),
                identity_file: Some(PathBuf::from("/root/.ssh/id_ed25519")),
                agent_path: None,
                agent_binary: None,
                ssh_options: Vec::new(),
            }
        );
    }

    #[test]
    fn indentation_is_irrelevant() {
        let indented = parse(MINIMAL);
        let flat = parse(
            r#"
[[sync]]
name = "www"
source = "/var/www"
[sync.target]
type = "local"
path = "/backup/www"
"#,
        );

        assert_eq!(indented, flat);
    }

    #[test]
    fn durations_are_written_the_way_people_say_them() {
        let config = parse(
            r#"
[defaults]
delay = "500ms"

[[sync]]
name = "www"
source = "/var/www"
delay = "2m"

  [sync.target]
  type = "local"
  path = "/backup/www"
"#,
        );

        assert_eq!(config.defaults.delay, Some(Duration::from_millis(500)));
        assert_eq!(config.syncs[0].delay, Some(Duration::from_secs(120)));
    }

    #[test]
    fn a_misspelled_key_is_rejected() {
        // The failure this prevents: `dely = "5s"` parsing fine and the setting
        // silently never applying.
        let message = reject(
            r#"
[[sync]]
name = "www"
source = "/var/www"
dely = "5s"

  [sync.target]
  type = "local"
  path = "/backup/www"
"#,
        );

        assert!(
            message.contains("dely"),
            "the error must name the offending key, got: {message}"
        );
    }

    #[test]
    fn an_unknown_target_type_is_rejected() {
        let message = reject(
            r#"
[[sync]]
name = "www"
source = "/var/www"

  [sync.target]
  type = "carrier-pigeon"
  path = "/backup/www"
"#,
        );

        assert!(!message.is_empty());
    }

    #[test]
    fn a_config_with_no_syncs_is_rejected() {
        let message = reject("[defaults]\ndelay = \"1s\"\n");

        assert!(message.contains("nothing to do"), "got: {message}");
    }

    #[test]
    fn duplicate_names_are_rejected() {
        let message = reject(
            r#"
[[sync]]
name = "dupe"
source = "/a"
target = { type = "local", path = "/b" }

[[sync]]
name = "dupe"
source = "/c"
target = { type = "local", path = "/d" }
"#,
        );

        assert!(message.contains("dupe"), "got: {message}");
    }

    #[test]
    fn a_relative_source_is_rejected() {
        let message = reject(
            r#"
[[sync]]
name = "www"
source = "relative/path"
target = { type = "local", path = "/backup" }
"#,
        );

        assert!(message.contains("absolute"), "got: {message}");
    }

    #[test]
    fn a_relative_target_is_rejected() {
        let message = reject(
            r#"
[[sync]]
name = "www"
source = "/var/www"
target = { type = "local", path = "relative" }
"#,
        );

        assert!(message.contains("absolute"), "got: {message}");
    }

    #[test]
    fn a_target_inside_the_source_is_rejected() {
        // Every write to the target lands inside the watched tree, producing an
        // event, producing another write.
        let message = reject(
            r#"
[[sync]]
name = "loop"
source = "/var/www"
target = { type = "local", path = "/var/www/backup" }
"#,
        );

        assert!(message.contains("overlap"), "got: {message}");
    }

    #[test]
    fn a_source_inside_the_target_is_rejected() {
        let message = reject(
            r#"
[[sync]]
name = "loop"
source = "/var/www/site"
target = { type = "local", path = "/var/www" }
"#,
        );

        assert!(message.contains("overlap"), "got: {message}");
    }

    #[test]
    fn identical_source_and_target_are_rejected() {
        let message = reject(
            r#"
[[sync]]
name = "loop"
source = "/var/www"
target = { type = "local", path = "/var/www" }
"#,
        );

        assert!(message.contains("overlap"), "got: {message}");
    }

    #[test]
    fn a_remote_target_may_share_the_source_path() {
        // Only local targets can feed the sync its own writes; the same path on
        // another host is the normal case.
        parse(
            r#"
[[sync]]
name = "app"
source = "/srv/app"
target = { type = "ssh", host = "deploy@host", path = "/srv/app" }
"#,
        );
    }

    #[test]
    fn an_empty_ssh_host_is_rejected() {
        let message = reject(
            r#"
[[sync]]
name = "app"
source = "/srv/app"
target = { type = "ssh", host = "  ", path = "/srv/app" }
"#,
        );

        assert!(message.contains("host"), "got: {message}");
    }

    #[test]
    fn a_sync_overrides_the_defaults() {
        let resolved = parse(
            r#"
[defaults]
delay = "10s"
max_pending = 500
delete = false

[[sync]]
name = "override"
source = "/a"
target = { type = "local", path = "/b" }
delay = "1s"
delete = true

[[sync]]
name = "inherit"
source = "/c"
target = { type = "local", path = "/d" }
"#,
        )
        .resolve();

        assert_eq!(resolved[0].queue.delay, Duration::from_secs(1));
        assert_eq!(resolved[0].queue.max_pending, 500, "inherited");
        assert!(resolved[0].reconcile.delete, "overridden");

        assert_eq!(resolved[1].queue.delay, Duration::from_secs(10));
        assert!(!resolved[1].reconcile.delete);
    }

    #[test]
    fn unset_values_fall_back_to_the_component_defaults() {
        let resolved = parse(MINIMAL).resolve();
        let expected = QueueConfig::default();

        assert_eq!(resolved[0].queue, expected);
        assert!(
            !resolved[0].reconcile.delete,
            "deletion stays off unless a config asks for it"
        );
    }

    #[test]
    fn verify_defaults_to_quick() {
        assert_eq!(parse(MINIMAL).resolve()[0].reconcile.verify, Verify::Quick);
    }

    #[test]
    fn verify_can_be_set_and_overridden() {
        let resolved = parse(
            r#"
[defaults]
verify = "checksum"

[[sync]]
name = "hashed"
source = "/a"
target = { type = "local", path = "/b" }

[[sync]]
name = "quick"
source = "/c"
target = { type = "local", path = "/d" }
verify = "quick"
"#,
        )
        .resolve();

        assert_eq!(resolved[0].reconcile.verify, Verify::Checksum, "inherited");
        assert_eq!(resolved[1].reconcile.verify, Verify::Quick, "overridden");
    }

    #[test]
    fn an_unknown_verify_mode_is_rejected() {
        let message = reject(
            r#"
[[sync]]
name = "x"
source = "/a"
target = { type = "local", path = "/b" }
verify = "paranoid"
"#,
        );

        assert!(!message.is_empty());
    }

    #[test]
    fn exclude_patterns_are_compiled_at_load() {
        // A malformed pattern must fail at startup, not the first time a file
        // changes and the filter is finally exercised.
        let message = reject(
            r#"
[[sync]]
name = "x"
source = "/a"
target = { type = "local", path = "/b" }
exclude = ["[unclosed"]
"#,
        );

        assert!(message.contains("[unclosed"), "got: {message}");
        assert!(message.contains("\"x\""), "must name the sync: {message}");
    }

    #[test]
    fn valid_exclude_patterns_are_accepted_and_carried_through() {
        let resolved = parse(
            r#"
[[sync]]
name = "x"
source = "/a"
target = { type = "local", path = "/b" }
exclude = ["*.tmp", "node_modules/", "build/*.o"]
"#,
        )
        .resolve();

        assert_eq!(resolved[0].exclude.len(), 3);
    }

    #[test]
    fn preserve_defaults_to_mode_only() {
        let resolved = parse(MINIMAL).resolve();

        assert!(resolved[0].reconcile.preserve.mode, "modes are preserved");
        assert!(
            !resolved[0].reconcile.preserve.ownership,
            "chown is privileged, so ownership stays opt-in"
        );
    }

    #[test]
    fn preserve_can_be_set_and_overridden() {
        let resolved = parse(
            r#"
[defaults]
preserve = { mode = true, ownership = true }

[[sync]]
name = "inherit"
source = "/a"
target = { type = "local", path = "/b" }

[[sync]]
name = "override"
source = "/c"
target = { type = "local", path = "/d" }
preserve = { mode = false, ownership = false }
"#,
        )
        .resolve();

        assert!(resolved[0].reconcile.preserve.ownership, "inherited");
        assert!(!resolved[1].reconcile.preserve.mode, "overridden");
    }

    #[test]
    fn preserve_fields_are_individually_optional() {
        let resolved = parse(
            r#"
[[sync]]
name = "x"
source = "/a"
target = { type = "local", path = "/b" }
preserve = { ownership = true }
"#,
        )
        .resolve();

        assert!(
            resolved[0].reconcile.preserve.mode,
            "an unmentioned field keeps its default rather than becoming false"
        );
        assert!(resolved[0].reconcile.preserve.ownership);
    }

    #[test]
    fn an_unknown_preserve_field_is_rejected() {
        let message = reject(
            r#"
[[sync]]
name = "x"
source = "/a"
target = { type = "local", path = "/b" }
preserve = { hardlinks = true }
"#,
        );

        assert!(message.contains("hardlinks"), "got: {message}");
    }

    #[test]
    fn comments_are_allowed() {
        // The reason TOML and not JSON: an operator can record why a value is
        // what it is, next to the value.
        parse(
            r#"
# Staging mirrors slowly; the box is on a metered link.
[defaults]
delay = "30s"  # trailing comments too

[[sync]]
name = "www"
source = "/var/www"
target = { type = "local", path = "/backup/www" }
"#,
        );
    }
}
