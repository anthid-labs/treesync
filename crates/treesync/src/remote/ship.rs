//! Getting the agent onto the target host.
//!
//! The agent is the treesync binary itself, so "installing" it is copying one
//! file and marking it executable. That is done over the SSH connection that
//! is already configured, rather than by asking the operator to provision the
//! host separately: a sync that works is the only setup step.
//!
//! # The order matters
//!
//! Connect first, ship only if that fails. The common case by far is an agent
//! that is already there and current, and probing it by connecting costs
//! nothing extra: the connection is the one the sync goes on to use. Shipping
//! first would upload a binary on every single pass.
//!
//! # A binary is not portable
//!
//! The client cannot compile for the target. Before uploading anything it asks
//! the host what it is and compares that against the binary it has, so a
//! mismatch is one clear sentence at startup instead of an `Exec format error`
//! surfacing later as a closed pipe.

use std::path::Path;
use std::process::Stdio;

use tokio::io::AsyncWriteExt;

use tokio_util::sync::CancellationToken;

use super::ssh::{self, Connection, Reconnect, SshSink, SshTarget, shell_quote};
use crate::error::{Error, Result};

/// Opens a connection to the target's agent, installing it if need be.
///
/// `binary` is the local file to upload. `None` means use this executable,
/// which is correct whenever the two hosts run the same platform and is
/// checked before anything is sent.
///
/// Also the reconnect path, which is why installing is part of it rather than
/// a separate step done once at startup: the reasons a link drops overlap with
/// the reasons a host comes back without an agent on it.
pub(crate) async fn open_connection(
    target: &SshTarget,
    binary: Option<&Path>,
) -> Result<Connection> {
    match ssh::open_for(target).await {
        Ok(connection) => {
            tracing::debug!(host = %target.host, "the agent was already installed");

            return Ok(connection);
        }
        Err(error) => {
            tracing::info!(
                host = %target.host,
                %error,
                "no usable agent on the host; installing one"
            );
        }
    }

    install(target, binary).await?;

    // A second failure is reported as itself. The first one was a reason to
    // try installing; this one is the answer after installing worked, so it is
    // the one an operator needs to see.
    ssh::open_for(target).await
}

/// Opens a sink on the target's agent, installing it if need be.
///
/// The returned sink rebuilds its own connection through this same path, so a
/// host that drops off and comes back, even rebuilt, even without the agent,
/// is picked up again without the sync being restarted.
pub async fn connect(
    target: &SshTarget,
    binary: Option<&Path>,
    reconnect: Reconnect,
    cancel: CancellationToken,
) -> Result<SshSink> {
    let connection = open_connection(target, binary).await?;

    Ok(SshSink::from_parts(
        connection,
        ssh::reopen_over_ssh(target, binary),
        ssh::describe(target),
    )
    .with_reconnect(reconnect, cancel))
}

/// What an in-flight agent upload is called, next to the binary it replaces.
///
/// Shared with the walk in [`crate::reconcile::index`], which has to recognise
/// it: an `agent_path` inside the target tree puts this file inside the tree
/// too, for as long as an upload is running. Two spellings of the same name in
/// two crates' worth of code apart is how the walk quietly stops matching it.
pub(crate) const UPLOAD_SUFFIX: &str = ".incoming";

/// Uploads the agent binary and marks it executable.
pub async fn install(target: &SshTarget, binary: Option<&Path>) -> Result<()> {
    let binary: std::path::PathBuf = match binary {
        Some(path) => path.to_path_buf(),
        None => std::env::current_exe().map_err(|err| {
            Error::Internal(format!("cannot locate this treesync executable: {err}"))
        })?,
    };

    // Read once and held: it is checked, then sent. A few megabytes, and the
    // alternative is reading the same file twice and hoping it did not change
    // in between.
    let contents = tokio::fs::read(&binary)
        .await
        .map_err(|err| Error::Config(format!("agent binary {}: {err}", binary.display())))?;

    let remote = platform(target).await?;
    remote.accepts(&binary, &contents)?;

    tracing::info!(
        host = %target.host,
        binary = %binary.display(),
        bytes = contents.len(),
        platform = %remote,
        "uploading the agent"
    );

    let mut command = target.command();
    command
        .arg("--")
        // Written to a temporary and moved into place, so a host running the
        // agent right now keeps a complete binary rather than having one
        // rewritten underneath it, and a transfer cut halfway leaves nothing
        // that looks installed.
        .arg(format!(
            "set -e; mkdir -p {parent}; cat > {temporary}; chmod 755 {temporary}; mv {temporary} {destination}",
            parent = target.agent_path.parent_shell_word(),
            temporary = shell_quote_suffix(&target.agent_path.shell_word(), UPLOAD_SUFFIX),
            destination = target.agent_path.shell_word(),
        ))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = command.spawn().map_err(Error::from)?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| Error::Internal("upload child has no stdin".to_string()))?;

    let sent = async {
        stdin.write_all(&contents).await?;
        stdin.shutdown().await
    }
    .await;
    drop(stdin);

    let output = child.wait_with_output().await.map_err(Error::from)?;

    // Checked after the child has been reaped: a remote command that exited
    // early makes the write fail with a broken pipe, and the exit status says
    // far more about why than the pipe error does.
    if !output.status.success() {
        return Err(Error::Internal(format!(
            "installing the agent on {} failed ({}): {}",
            target.host,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    sent.map_err(|err| Error::Internal(format!("sending the agent to {}: {err}", target.host)))?;

    tracing::info!(host = %target.host, "agent installed");

    Ok(())
}

/// What `uname -sm` reported for the target host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Platform {
    /// `Linux`, `Darwin`, and so on.
    pub system: String,
    /// `x86_64`, `aarch64`, `arm64`.
    pub machine: String,
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.system, self.machine)
    }
}

impl Platform {
    /// This machine, named the way `uname` would.
    pub fn local() -> Self {
        Self {
            system: match std::env::consts::OS {
                "macos" => "Darwin".to_string(),
                "linux" => "Linux".to_string(),
                other => other.to_string(),
            },
            machine: std::env::consts::ARCH.to_string(),
        }
    }

    pub fn parse(output: &str) -> Result<Self> {
        let mut parts = output.split_whitespace();

        match (parts.next(), parts.next()) {
            (Some(system), Some(machine)) => Ok(Self {
                system: system.to_string(),
                machine: machine.to_string(),
            }),
            _ => Err(Error::Internal(format!(
                "could not read the remote platform from `uname -sm` output {output:?}"
            ))),
        }
    }

    /// Whether a binary built for `self` runs on `other`.
    ///
    /// Architecture names are normalised because the two sources disagree:
    /// Rust calls 64-bit ARM `aarch64`, and `uname` on macOS calls the same
    /// chip `arm64`. Treating those as different platforms would refuse a
    /// binary that runs perfectly well.
    pub fn matches(&self, other: &Self) -> bool {
        self.system.eq_ignore_ascii_case(&other.system)
            && normalise_machine(&self.machine) == normalise_machine(&other.machine)
    }

    /// Refuses to upload a binary that cannot run on the host.
    ///
    /// The comparison is against the *binary*, read from its own header, not
    /// against the machine treesync happens to be running on. Those differ in
    /// exactly the case that matters: a mac syncing to a Linux host, where
    /// `agent_binary` points at a cross-built Linux binary that is correct
    /// precisely because it does not match the client.
    ///
    /// The failure this prevents is not subtle but it is opaque. An
    /// incompatible binary uploads fine, and then the exec fails on the far
    /// side of an SSH pipe, so what the client sees is a connection that
    /// closed without a word.
    fn accepts(&self, path: &Path, contents: &[u8]) -> Result<()> {
        let Some(binary) = BinaryTarget::read(contents) else {
            // Not a format this recognises. That includes things which work
            // perfectly well, such as a shell wrapper around the real binary,
            // so this declines to judge instead of blocking a setup it
            // does not understand.
            tracing::debug!(
                binary = %path.display(),
                "not a recognised executable header; skipping the platform check"
            );

            return Ok(());
        };

        if binary.runs_on(self) {
            return Ok(());
        }

        Err(Error::Config(format!(
            "the host is {self} but the agent binary {} is {binary}. \
             Set `agent_binary` on the target to a treesync built for {self}. \
             `docker build --target builder` produces a static one.",
            path.display()
        )))
    }
}

/// What an executable is built to run on, read from its own header.
///
/// Only as precise as the header actually is. An ELF header does not say
/// "Linux". It says ELF, which Linux and the BSDs share, so this reports the
/// format and the architecture and nothing it would have to guess at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryTarget {
    pub format: BinaryFormat,
    /// `None` when the header names an architecture this does not know, which
    /// is not a reason to refuse the upload.
    pub machine: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryFormat {
    /// Linux and the BSDs.
    Elf,
    /// macOS.
    MachO,
    /// A macOS fat binary, holding more than one architecture.
    Universal,
}

impl std::fmt::Display for BinaryTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let format = match self.format {
            BinaryFormat::Elf => "an ELF binary (Linux/BSD)",
            BinaryFormat::MachO => "a Mach-O binary (macOS)",
            BinaryFormat::Universal => "a universal binary (macOS)",
        };

        match self.machine {
            Some(machine) => write!(f, "{format} for {machine}"),
            None => write!(f, "{format}"),
        }
    }
}

impl BinaryTarget {
    /// Identifies a binary from the first bytes of its header.
    pub fn read(contents: &[u8]) -> Option<Self> {
        match contents {
            // ELF: magic, then `e_machine` as a 16-bit field at offset 18.
            // Byte order is the file's own, given by `EI_DATA` at offset 5.
            [0x7f, b'E', b'L', b'F', ..] if contents.len() >= 20 => {
                let raw = [contents[18], contents[19]];
                let machine = if contents[5] == 2 {
                    u16::from_be_bytes(raw)
                } else {
                    u16::from_le_bytes(raw)
                };

                Some(Self {
                    format: BinaryFormat::Elf,
                    machine: match machine {
                        0x3e => Some("x86_64"),
                        0xb7 => Some("aarch64"),
                        0x28 => Some("arm"),
                        0x03 => Some("i386"),
                        0xf3 => Some("riscv64"),
                        _ => None,
                    },
                })
            }

            // Mach-O, either byte order, with `cputype` at offset 4.
            [0xcf, 0xfa, 0xed, 0xfe, ..] | [0xce, 0xfa, 0xed, 0xfe, ..] if contents.len() >= 8 => {
                Some(Self {
                    format: BinaryFormat::MachO,
                    machine: match u32::from_le_bytes([
                        contents[4],
                        contents[5],
                        contents[6],
                        contents[7],
                    ]) {
                        0x0100_0007 => Some("x86_64"),
                        0x0100_000c => Some("aarch64"),
                        0x0000_0007 => Some("i386"),
                        _ => None,
                    },
                })
            }

            // A fat binary carries several architectures, so it is not pinned
            // to one; what identifies it is that it is a macOS artefact.
            [0xca, 0xfe, 0xba, 0xbe, ..] | [0xbe, 0xba, 0xfe, 0xca, ..] => Some(Self {
                format: BinaryFormat::Universal,
                machine: None,
            }),

            _ => None,
        }
    }

    /// Whether this binary can execute on a host.
    pub fn runs_on(&self, host: &Platform) -> bool {
        let darwin = host.system.eq_ignore_ascii_case("Darwin");

        let format_fits = match self.format {
            BinaryFormat::MachO | BinaryFormat::Universal => darwin,
            BinaryFormat::Elf => !darwin,
        };

        if !format_fits {
            return false;
        }

        match self.machine {
            // A fat binary holds several, and an architecture this does not
            // recognise is not evidence of a mismatch.
            None => true,
            Some(machine) => normalise_machine(machine) == normalise_machine(&host.machine),
        }
    }
}

fn normalise_machine(machine: &str) -> &str {
    match machine {
        "arm64" | "aarch64" => "aarch64",
        "amd64" | "x86_64" => "x86_64",
        other => other,
    }
}

/// Asks the host what it is.
async fn platform(target: &SshTarget) -> Result<Platform> {
    let mut command = target.command();
    command
        .arg("--")
        .arg("uname -sm")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let output = command.output().await.map_err(Error::from)?;

    if !output.status.success() {
        return Err(Error::Internal(format!(
            "could not reach {} to identify it ({}): {}",
            target.host,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    Platform::parse(String::from_utf8_lossy(&output.stdout).trim())
}

/// Appends a suffix to an already-quoted shell word.
///
/// The word may be `"$HOME"/'.cache/treesync/treesync'`, so the suffix cannot
/// simply be concatenated inside the quoting. It gets its own quoted word,
/// which the shell then joins to the one before it.
fn shell_quote_suffix(word: &str, suffix: &str) -> String {
    format!("{word}{}", shell_quote(suffix))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn platform(system: &str, machine: &str) -> Platform {
        Platform {
            system: system.to_string(),
            machine: machine.to_string(),
        }
    }

    #[test]
    fn uname_output_is_parsed() {
        assert_eq!(
            Platform::parse("Linux aarch64").expect("parse"),
            platform("Linux", "aarch64")
        );
    }

    #[test]
    fn trailing_noise_in_uname_output_is_ignored() {
        assert_eq!(
            Platform::parse("Darwin arm64\n").expect("parse"),
            platform("Darwin", "arm64")
        );
    }

    #[test]
    fn unusable_uname_output_is_an_error() {
        assert!(Platform::parse("").is_err());
        assert!(Platform::parse("Linux").is_err());
    }

    #[test]
    fn the_same_platform_matches() {
        assert!(platform("Linux", "x86_64").matches(&platform("Linux", "x86_64")));
    }

    #[test]
    fn arm64_and_aarch64_are_the_same_chip() {
        // uname on macOS says arm64, Rust says aarch64. Treating them as
        // different would refuse a binary that runs fine.
        assert!(platform("Darwin", "arm64").matches(&platform("Darwin", "aarch64")));
    }

    #[test]
    fn amd64_and_x86_64_are_the_same_chip() {
        assert!(platform("Linux", "amd64").matches(&platform("Linux", "x86_64")));
    }

    #[test]
    fn a_different_system_does_not_match() {
        assert!(!platform("Linux", "aarch64").matches(&platform("Darwin", "aarch64")));
    }

    #[test]
    fn a_different_architecture_does_not_match() {
        assert!(!platform("Linux", "aarch64").matches(&platform("Linux", "x86_64")));
    }

    /// An ELF header: magic, 64-bit, little-endian, then `e_machine`.
    fn elf(machine: u16) -> Vec<u8> {
        let mut header = vec![0x7f, b'E', b'L', b'F', 2, 1, 1, 0];
        header.resize(18, 0);
        header.extend_from_slice(&machine.to_le_bytes());
        header.resize(64, 0);

        header
    }

    fn mach_o(cputype: u32) -> Vec<u8> {
        let mut header = vec![0xcf, 0xfa, 0xed, 0xfe];
        header.extend_from_slice(&cputype.to_le_bytes());
        header.resize(32, 0);

        header
    }

    #[test]
    fn an_elf_binary_is_identified() {
        let target = BinaryTarget::read(&elf(0xb7)).expect("recognised");

        assert_eq!(target.format, BinaryFormat::Elf);
        assert_eq!(target.machine, Some("aarch64"));
    }

    #[test]
    fn a_mach_o_binary_is_identified() {
        let target = BinaryTarget::read(&mach_o(0x0100_000c)).expect("recognised");

        assert_eq!(target.format, BinaryFormat::MachO);
        assert_eq!(target.machine, Some("aarch64"));
    }

    #[test]
    fn a_universal_binary_is_identified() {
        let target = BinaryTarget::read(&[0xca, 0xfe, 0xba, 0xbe, 0, 0, 0, 2]).expect("recognised");

        assert_eq!(target.format, BinaryFormat::Universal);
    }

    #[test]
    fn something_that_is_not_a_binary_is_not_identified() {
        assert!(BinaryTarget::read(b"#!/bin/sh\nexec treesync\n").is_none());
        assert!(BinaryTarget::read(b"").is_none());
        assert!(BinaryTarget::read(&[0x7f, b'E', b'L', b'F']).is_none());
    }

    #[test]
    fn a_linux_binary_runs_on_a_linux_host() {
        let binary = BinaryTarget::read(&elf(0xb7)).expect("recognised");

        assert!(binary.runs_on(&platform("Linux", "aarch64")));
    }

    #[test]
    fn a_cross_built_linux_binary_is_accepted_from_a_mac() {
        // The case the whole check exists to get right: syncing from a mac to
        // a Linux host, where the correct binary is precisely the one that
        // does not match the client.
        let binary = BinaryTarget::read(&elf(0xb7)).expect("recognised");
        let host = platform("Linux", "aarch64");

        host.accepts(Path::new("/tmp/treesync-linux"), &elf(0xb7))
            .expect("a Linux binary is what a Linux host needs");
        assert!(binary.runs_on(&host));
    }

    #[test]
    fn a_mach_o_binary_does_not_run_on_linux() {
        let binary = BinaryTarget::read(&mach_o(0x0100_000c)).expect("recognised");

        assert!(!binary.runs_on(&platform("Linux", "aarch64")));
    }

    #[test]
    fn an_elf_binary_does_not_run_on_a_mac() {
        let binary = BinaryTarget::read(&elf(0xb7)).expect("recognised");

        assert!(!binary.runs_on(&platform("Darwin", "arm64")));
    }

    #[test]
    fn the_wrong_architecture_does_not_run() {
        let binary = BinaryTarget::read(&elf(0x3e)).expect("recognised");

        assert!(!binary.runs_on(&platform("Linux", "aarch64")));
    }

    #[test]
    fn a_binary_for_another_platform_is_refused_with_the_remedy() {
        let host = platform("Linux", "aarch64");

        let error = host
            .accepts(Path::new("/usr/local/bin/treesync"), &mach_o(0x0100_000c))
            .expect_err("a mac binary must not be uploaded to a Linux host");

        let message = error.to_string();
        assert!(message.contains("Linux/aarch64"), "{message}");
        assert!(message.contains("Mach-O"), "{message}");
        assert!(
            message.contains("agent_binary"),
            "the error has to say what to set: {message}"
        );
    }

    #[test]
    fn an_unrecognised_file_is_not_blocked() {
        // A shell wrapper around the real binary works fine, and this cannot
        // tell that from anything else, so it declines to judge.
        let host = platform("Linux", "aarch64");

        host.accepts(
            Path::new("/usr/local/bin/treesync"),
            b"#!/bin/sh\nexec real\n",
        )
        .expect("an unrecognised header must not block a working setup");
    }

    #[test]
    fn the_local_platform_matches_itself() {
        assert!(Platform::local().matches(&Platform::local()));
    }

    #[test]
    fn the_local_platform_is_named_the_way_uname_names_it() {
        let local = Platform::local();

        assert!(
            matches!(local.system.as_str(), "Linux" | "Darwin"),
            "got {local}"
        );
    }

    #[test]
    fn a_suffix_becomes_its_own_quoted_word() {
        // The base may already mix an expansion with a quoted literal, so the
        // suffix cannot go inside its quoting.
        assert_eq!(
            shell_quote_suffix("\"$HOME\"/'.cache/treesync'", ".incoming"),
            "\"$HOME\"/'.cache/treesync''.incoming'"
        );
    }
}
