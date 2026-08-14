# TreeSync
[![🛡️ Container image scan](https://github.com/anthid-labs/treesync/actions/workflows/container-scan.yml/badge.svg)](https://github.com/anthid-labs/treesync/actions/workflows/container-scan.yml)
[![🔐 static security analysis](https://github.com/anthid-labs/treesync/actions/workflows/security-static.yml/badge.svg)](https://github.com/anthid-labs/treesync/actions/workflows/security-static.yml)
[![🏗️ Build and Push Docker Image](https://github.com/anthid-labs/treesync/actions/workflows/build-and-push.yml/badge.svg)](https://github.com/anthid-labs/treesync/actions/workflows/build-and-push.yml)

One-way directory mirroring, in Rust. An experiment in replacing
[lsyncd](https://github.com/lsyncd/lsyncd).

[![Crates.io](https://img.shields.io/crates/v/treesync.svg)](https://crates.io/crates/treesync)
[![Docs.rs](https://docs.rs/treesync/badge.svg)](https://docs.rs/treesync)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Buy Me A Coffee](https://img.shields.io/badge/buy%20me%20a%20coffee-support-yellow.svg)](https://buymeacoffee.com/dallinwright)

**Status: early, but the core is done and covered.** Mirroring works end to end,
one-shot or continuous, to a local directory or to a host over SSH. A changed
file is sent as a rolling-checksum delta instead of whole, compressed, verified
end to end, and resumable if the link drops. See [Not done](#not-done) for what
is still missing.

What that costs, measured on a 210 MB JSON document with a single field edited:
**27 KB on the wire**, and the target byte-identical to the source.

How it behaves when the disk fills, the link goes, or a path cannot be read is
documented in [Behaviour when things go wrong](#behaviour-when-things-go-wrong)
instead of left to be discovered. Each of those conditions has a test that
reproduces it for real: a full filesystem, a link killed repeatedly
mid-transfer, an actual `chattr +i`.

## Install

```bash
cargo install treesync-cli
```

The package is `treesync-cli`; the command it installs is `treesync`. Two crates
are published, because the two audiences are different:

| Crate                                                   | What it is                                            |
| ------------------------------------------------------- | ----------------------------------------------------- |
| [`treesync`](https://crates.io/crates/treesync)         | The library: the engine, for embedding in a program.   |
| [`treesync-cli`](https://crates.io/crates/treesync-cli) | The `treesync` command.                                |

Library documentation is on [docs.rs](https://docs.rs/treesync).

Or with Docker:

```bash
docker pull ghcr.io/anthid-labs/treesync:latest
```

Three tags per build from the default branch:

| Tag | What it points at |
| --- | --- |
| `sha-<commit>` | Exactly one build. The only tag that never moves. |
| `<version>` | The version in `Cargo.toml`, matching the published crates. |
| `latest` | The newest build on the default branch. |

So an image can be pinned to the same number as the crate:

```bash
docker pull ghcr.io/anthid-labs/treesync:0.1.16
```

Built natively per architecture and merged into one multi-arch manifest, so
`linux/amd64` and `linux/arm64` both resolve from the same tag. Or build from
source:

```bash
cargo build --release --package treesync-cli
```

## Quick start

```toml
# config.toml
[[sync]]
name = "www"
source = "/var/www"
exclude = ["*.tmp", ".git/"]

  [sync.target]
  type = "local"
  path = "/backup/www"
```

```bash
treesync --config ./config.toml check      # validate, print what it resolves to
treesync --config ./config.toml sync --dry-run
treesync --config ./config.toml sync       # one pass, then exit
treesync --config ./config.toml watch      # keep mirroring until stopped
```

`sync` and `watch` are the same machinery: both compare the trees and apply the
difference through one implementation, so a one-shot pass and a resident daemon
cannot disagree about what a config means. `sync` does a whole-tree pass and
stops; `watch` goes on to reconcile whatever the watcher reports, and stops on
SIGTERM or SIGINT after flushing what it has already seen. Both run every
`[[sync]]` unless `--name` selects one, and a name matching nothing is an error
rather than an empty run.

[`treesync.example.toml`](treesync.example.toml) documents every option.

## How it works

The watcher reports what the kernel saw, the queue collapses a burst of events
into the distinct paths that changed, the reconciler compares those paths across
both trees, and a sink applies the difference.

Two things follow from that shape and explain most of the design:

- **The filesystem is the authority, not the event stream.** Event kinds are not
  trusted. Measured on macOS/FSEvents, deleting a file arrives labelled as a
  creation. A batch says *which paths are suspect*; the reconciler stats them.
- **Lost events cost a re-walk, never correctness.** When the kernel drops
  events or the queue fills, treesync reconciles that subtree in full instead of
  replaying a log it knows has a hole in it.

Where it differs from rsync: rsync rebuilds its file list on every invocation,
so its cost is proportional to the tree. A batch naming three files here stats
three files.

## Configuration

The config file is TOML. Unknown keys are a startup error, not a setting that
silently never applied.

| Key                    | Default    | Purpose                                                     |
| ---------------------- | ---------- | ----------------------------------------------------------- |
| `name`                 | *required* | Identifies the sync in logs. Must be unique.                 |
| `source`               | *required* | Absolute path to the watched tree.                           |
| `target.type`          | *required* | `local` or `ssh`.                                            |
| `target.path`          | *required* | Absolute destination path.                                   |
| `exclude`              | `[]`       | Globs, applied to **both** trees. `*.tmp` at any depth, `node_modules/` a whole directory, `build/*.o` anchored at the root. |
| `delay`                | `1s`       | How long events are batched before acting.                   |
| `max_pending`          | `10000`    | Distinct paths that force an early flush.                    |
| `delete`               | `false`    | Whether removals propagate to the target.                    |
| `verify`               | `quick`    | `quick` (size + mtime) or `checksum` (also BLAKE3 content).  |
| `preserve.mode`        | `true`     | Mirror permission bits.                                      |
| `preserve.ownership`   | `false`    | Mirror uid/gid. Needs privilege.                             |
| `delta.enabled`        | `true`     | Send only what differs, for remote targets.                  |
| `delta.min_size`       | `1048576`  | Files below this are sent whole.                             |
| `delta.block_size`     | *derived*  | Signature block size. Defaults to √length, 16 KiB to 128 KiB. |

Anything in `[defaults]` applies to every `[[sync]]` that does not override it.

A few behaviours to know before trusting it with data:

- **`delete` is off by default.** The target is usually the copy without a
  backup, and a source that is briefly unreadable is hard to distinguish from
  one whose files were really deleted.
- **`exclude` applies to both trees.** Filtering only the source would make
  every excluded file on the target look like a deletion.
- **`quick` verification misses a rewrite that preserved size and mtime.** That
  happens on filesystems with one-second mtime granularity and whenever content
  is restored with its timestamp (`cp -p`, `tar -x`). Use `checksum` where that
  matters; it reads every candidate file on both sides.
- **Source and target may not overlap.** Rejected at startup, because writing
  into the watched tree would feed the sync its own writes.

### Environment

| Variable          | Purpose                                                       |
| ----------------- | ------------------------------------------------------------- |
| `TREESYNC_CONFIG` | Config path. Same as `--config`. Defaults to `/etc/treesync/config.toml`. |
| `RUST_LOG`        | Log filter. Takes precedence over `--log-level`.               |
| `LOG_LEVEL`       | Fallback filter when `RUST_LOG` is unset.                      |

A malformed filter is a startup failure, not silently dropped logs.

## Remote targets

```toml
[[sync]]
name = "app"
source = "/srv/app"

  [sync.target]
  type = "ssh"
  host = "deploy@example.com"
  path = "/srv/app"
```

What a `[sync.target]` block takes when `type = "ssh"`, beyond `path`:

| Key             | Default                          | Purpose                                                     |
| --------------- | -------------------------------- | ----------------------------------------------------------- |
| `host`          | *required*                       | `user@host`, or a `~/.ssh/config` alias.                     |
| `port`          | *ssh's own*                      | Port for this target.                                        |
| `identity_file` | *ssh's own lookup*               | Path to a private key, never the key itself. Sets `IdentitiesOnly`, so an agent holding a dozen keys cannot trip `MaxAuthTries` before reaching this one. |
| `agent_path`    | `$HOME/.cache/treesync/treesync` | Where the agent lives on the host. An absolute path may point inside the target tree, so removing that tree removes treesync from the host; it needs no `exclude` entry either way. |
| `agent_binary`  | *this executable*                | The local build to upload when no usable agent is there.     |
| `ssh_options`   | `[]`                             | Extra `-o Key=value` for the ssh client.                     |

Nothing has to be installed on the host first. treesync connects, and if no
usable agent answers it uploads one and connects again. The agent *is* the
treesync binary, run as `treesync agent --root <path>`, so there is no second
artifact to build or version.

```
treesync ──ssh──> treesync agent ──> LocalSink ──> the target tree
```

The connection is one SSH child process with the protocol on its stdin and
stdout. No listening daemon, no port, no second authentication system: the
agent lives exactly as long as the SSH session and has precisely the access the
SSH login had.

Three consequences:

- **The agent runs on the target, so the target is indexed there.** A batch
  naming three files asks about three files. Forking rsync would rebuild the
  whole file list over the link on every pass, which is the cost this design
  avoids.
- **A binary is not portable, and treesync checks before uploading.** It reads
  the host's `uname` and the candidate binary's own header, so shipping a
  Mach-O build to a Linux host is one clear sentence at startup instead of an
  `Exec format error` arriving as a closed connection. Syncing from a mac to
  Linux needs `agent_binary` pointing at a Linux build, and `docker build
  --target builder` produces a static one.
- **`--dry-run` will not install the agent.** Uploading a binary changes the
  host, and a dry run changes nothing. Against a host with no agent yet it says
  so instead of provisioning one.

### What actually crosses the link

A changed file is sent as the difference from the copy the target already has,
not whole. The agent describes its existing file as a rolling checksum plus a
truncated BLAKE3 per block, around twenty bytes per block, so a 1.5 GB file is
described in about half a megabyte. The client then slides a window over its
source, emitting "reuse the bytes you have at *x*" and "here are bytes you do
not". Frames above 4 KiB are zstd-compressed, which for text is most of what is
left.

Measured on a 210 MB JSON document with one field edited: **27 KB on the wire,
against 220 MB for the whole file.**

The rolling window is what makes that hold up. An edit that changes a value's
length shifts every byte after it, and a scheme that compared block *n* to block
*n* would call the entire remainder of the file different. A rolling checksum
finds the target's existing blocks wherever they have moved to, so the cost
tracks the edit and not its position in the file.

Three properties:

- **Every transfer is verified end to end.** The client sends BLAKE3 of the
  source; the agent hashes what it reconstructed and refuses to publish on a
  mismatch. A file already on the target is never replaced by something that did
  not match, which covers a stale block read back from the target's own copy,
  and corruption on disk. Corruption *in flight* is already SSH's job.
- **An interrupted transfer resumes**, onto bytes it has checked instead of
  merely found. See [A choppy link](#a-choppy-link).
- **It trades network for disk.** Both ends read the whole file: the agent to
  describe it, the client to scan it. That is a large win when the link is the
  bottleneck and a loss when it is not. `delta.enabled = false` is the way out.

`BatchMode=yes` is always set, so an unknown host key or a failed login fails
immediately instead of blocking on a prompt that nothing can answer. Use
`ssh_options` for anything `ssh_config` expresses that has no key of its own:
`ProxyJump` for a bastion, `UserKnownHostsFile` for a service account with no
home directory.

[`docker/remote-test.sh`](docker/remote-test.sh) exercises all of this against
a throwaway sshd container.

## Behaviour when things go wrong

One rule underlies all of this:

> A failure is **reported**, **confined to the path that caused it**, and never
> leaves the target holding something that is neither the old file nor the new
> one.

The last part is what the temporary-plus-rename dance is for. Content is built
up beside its destination and moved into place in one atomic step, so a reader
on the target sees the previous version or the complete new one, never a
half-written file, whatever failed in between.

| Condition | What happens | Recovery |
| --- | --- | --- |
| Destination disk full | That file's action fails; the previous version is untouched; the partial temporary is removed | Automatic on the next pass, once there is room |
| Source file unreadable | Only that action fails; nothing is published for it | Automatic once readable |
| Source **directory** unreadable | The whole pass fails; **no deletions are planned** | Automatic once readable |
| Source **root** gone (unmounted, renamed, removed) | Every pass fails, incremental ones included; **no deletions are planned** | Automatic once it is back |
| Target directory read-only (`0555`) | Owner write is added for the one write and the original mode put straight back | n/a |
| Target directory unwritable for any other reason | Actions into it fail; files already there are untouched | Automatic once writable |
| Target file read-only (`0444`) | Replaced anyway; the publish needs write on the *directory* | n/a |
| Target file immutable (`chattr +i`) | The action fails loudly | Automatic once the bit is cleared |
| Target entry is a symlink where a directory belongs | The action fails; **nothing is written through it** | Set `delete = true` to let it be replaced |
| Special file (FIFO, socket, device) in either tree | Skipped, on both sides, so it is never read as a deletion | n/a |
| Link drops mid-transfer | Reconnect with backoff, then resume (delta) or restart (whole file) | Automatic: 1s, 2s, 4s, 8s, then every 10s |
| Link is slow | Passes take longer; events keep coalescing | n/a. The queue is bounded by the tree, not the churn |
| Kernel drops events | The affected subtree is re-walked in full | Automatic |
| A reply too large to send (an index past the frame limit) | Answered as an error naming the limit; the session stays up | Narrow the tree with `exclude`, or split it across syncs |
| A malformed or misplaced frame | That one request fails; the session continues from the next frame | Automatic |

`sync` exits **1** if any action failed, after printing each failed path with its
reason. `watch` keeps running and re-queues the failed paths onto a later batch,
so a transient problem clears itself without intervention.

Anything wrong with the config itself, an unreachable host included, is a
startup failure: `watch` opens every selected sync before running any of them,
so a mistake is reported before a single file has been written. If one sync
later stops on its own, the rest are stopped with it and the process exits
non-zero, because a tree that has quietly stopped being mirrored looks exactly
like one that is up to date.

### A full destination disk

The write fails partway through, with bytes already committed to the temporary.
That temporary is removed, the file already on the target is left exactly as it
was, and the rest of the batch still runs: one oversized file does not strand
the twenty behind it. Nothing is left to make the *next* attempt fail for a new
reason, which is checked explicitly. A failure that leaves the destination
littered with its own debris is barely better than a silent one.

Both halves of this are tested against a full filesystem, not a simulated error.
See [Development](#development).

### Permission problems

Two cases behave very differently, and the difference matters:

- **An unreadable file** fails its own action and nothing else. It is not
  published as an empty file, and the rest of the batch lands.
- **An unreadable directory** fails the entire pass. This is deliberate, and it
  is the more important of the two: a directory that read as *empty* instead of
  *unreadable* would, with `delete = true`, plan the removal of everything under
  it on the target. Refusing to proceed is the only safe reading.

On the target, a read-only file is still replaced. Publishing is a rename, which
needs write permission on the containing directory, not on the file being
replaced. A file that truly cannot be replaced, such as one with the immutable
bit set, fails as an action and leaves the existing content alone.

A read-only *directory* is a different problem, and the only one where treesync
changes something it was not asked to. Mirroring a source directory's mode makes
the target directory read-only too, so the next file the source puts inside it
can never be written: the mirror stops converging, and no retry helps, because
nothing about the target changes between attempts. So owner write and execute are
added for exactly the length of one operation and the original mode is put back
immediately. Group and other bits are never touched, and a directory treesync
cannot change, an immutable one for instance, is reported rather than forced.

### Containment

Every path is checked against the target root before it is used, and by
components rather than by string, so `a/../../etc` is refused rather than
normalised. That check alone is not enough: it proves a path *spells* something
inside the root, not that it *leads* there. A symlink anywhere along a path
redirects everything below it, so the directories leading to a path are checked
too, and a path reached through a link is refused. This matters most on the agent,
where paths arrive over the network: a client that wanted out would not need a
`..` to get there, only a symlink and an ordinary-looking path beneath it.

The temporary a transfer goes through is named after its destination, so it is
predictable to anyone who can write to the target directory. It is unlinked and
then created exclusively, so a link planted at that name is removed rather than
written through. Unlinking never follows a symlink, and the exclusive create
refuses anything that reappears, so the worst an interfering process can do is
fail the transfer.

### What is not mirrored

FIFOs, sockets and device nodes are skipped. There is nothing in them to mirror:
a FIFO holds whatever a writer is putting through it at that instant, a device
node is a number meaningful only on one machine, and recreating either needs
privileges a sync should not want. Opening one is worse than useless, since a
FIFO with no writer blocks forever and a plan runs one action at a time. They are
skipped on *both* sides, so a special file on the target is never mistaken for
something the source deleted.

`preserve.ownership` is off by default because `chown` needs privilege: an
unprivileged run with it on reports a failure per file that it can do nothing
about, and the uids have to mean the same thing on both hosts anyway.

### A choppy link

Transport failures and agent failures are treated as different things, and that
distinction is the whole basis of reconnecting safely. An agent that answers
"permission denied" is a working connection reporting a real problem, and
retrying would loop forever without addressing it. A stream that ended mid-frame
is the link, and the same request against a new connection will very likely
succeed.

So a dropped link reconnects and reissues; a refused request does not. Every
action is idempotent, so reissuing is safe.

What happens to the transfer in flight depends on which path it was on:

- **A delta transfer resumes.** What survived on the target is kept, and the
  client checks it before building on it. It asks how many bytes are there and
  their hash, then hashes the same prefix of its own source. Only if the two
  agree does it continue from that point. A leftover from a different version of
  the file fails that check and the transfer starts clean.
- **A whole-file transfer restarts.** There is nothing cheaper to do: verifying
  what survived would cost a read of everything it was about to re-send anyway.

Either way the commit hash is checked before publishing, so a transfer stitched
together across several connections is byte-identical to the source or is not
published at all.

**The wait between attempts backs off: 1s, 2s, 4s, 8s, then 10s from there on.**

The first retry is quick because most interruptions are quick: a NAT mapping
dropped, sshd restarted, wifi blinked. Waiting ten seconds to find out the link
was fine all along wastes the whole outage. The ceiling prevents the opposite
case, a host that is down for an hour being met with a connection attempt every
second. Each attempt forks an ssh process, completes a TCP handshake and starts
a key exchange, and several syncs doing that together is a small denial of
service aimed at exactly the host you want back.

The backoff resets once a connection is rebuilt, since the next drop is a new
outage and not a continuation of the last one.

How long it keeps trying depends on what it was asked to do. `watch` reconnects
until it is cancelled, because a daemon whose target is unreachable for an hour
should mirror the changes when it comes back. `sync` gives up after five
attempts, about 25 seconds with the backoff, so a one-shot run from cron against
a host that is down fails and reports instead of piling up a process per tick.
`--dry-run` never reconnects: it makes one request and stops, and there is
nothing for a rebuilt connection to go on to do.

A shutdown during an outage does not sit out the wait, which matters more the
longer the outage has run.

SSH keepalives are set to notice a peer that vanished without closing:
`ServerAliveInterval=30` with `ServerAliveCountMax=3`, so 90 seconds of silence
declares the link dead instead of blocking forever on a socket nobody is
listening to. New connections time out after 15 seconds.

### A slow link, or a fast interval

A `delay` shorter than a transfer takes is fine, and is better understood than
tuned away. Events accumulate while a pass runs, and the queue collapses a burst
into the *distinct paths* that changed, so work per pass is bounded by the size
of the tree, not by how many times each file was touched. A file rewritten fifty
times during a slow transfer is one entry in the next batch, and is read once.

`max_pending` bounds how much a single pass can carry; crossing it flushes early.
If the kernel's own queue overflows, or the event channel fills, that is reported
as a gap and not a list of events, and the affected subtree is re-walked in full.
Reconciliation is the source of truth and notification is only an optimization,
so a missed event costs a walk and never correctness.

The same rule covers a case that is easy to miss: a directory tree created all at
once. The kernel only reports inside directories it already watches, and a watch
can only be installed once the directory exists, so `mkdir -p a/b/c` plus an
immediate write can arrive as the single event `Create a`. That is treated as a
gap in the subtree and reconciled in full, instead of mirroring an empty
directory and losing what was inside it.

Shutdown stays bounded under all of this. `watch` stops on SIGTERM or SIGINT
after flushing what it has already observed, and a shutdown during an outage does
not wait out the reconnect interval.

## Docker

```bash
docker run --rm \
  -v /path/to/config:/etc/treesync:ro \
  -v /path/to/source:/data/src \
  -v /path/to/target:/data/dst \
  ghcr.io/anthid-labs/treesync:latest sync
```

Static musl build on Alpine, about 25 MB. `/etc/treesync` is the CLI's default
config location, so a bind mount there needs no `--config`. There is no default
`CMD`: `sync` would mutate a target the moment the container started.

Or with Compose, mirroring one volume into another and staying up:

```yaml
services:
  treesync:
    image: ghcr.io/anthid-labs/treesync:latest
    command: watch
    # `watch` handles SIGTERM itself. This is a second line of defence for the
    # other commands, which do not.
    init: true
    # Room for the shutdown flush to finish rather than being killed midway.
    stop_grace_period: 15s
    volumes:
      - ./config:/etc/treesync:ro
      - ./src:/data/src
      - ./dst:/data/dst
```

```toml
# ./config/config.toml
[[sync]]
name = "example"
source = "/data/src"

  [sync.target]
  type = "local"
  path = "/data/dst"
```

[`examples/`](examples) has fuller versions of this, plus Kubernetes and Docker
Swarm. [`docker/compose.yaml`](docker/compose.yaml) is the one wired to a local
build for development.

Two container-specific things to know:

- **`watch` handles SIGTERM itself, which at PID 1 it has to.** The kernel
  installs no default handlers for PID 1, so a process that does not catch
  SIGTERM ignores `docker stop` entirely and is SIGKILLed at the timeout,
  discarding the shutdown flush. Measured with this image: `sleep` as PID 1
  takes 10s to stop, under `--init` it takes 0s. Running with `--init` is still
  worth it as a second line of defence for the other commands.
- **inotify limits come from the host.** `max_user_watches` and
  `max_queued_events` are kernel-wide and shared by every container. Exhausting
  them costs a re-walk and not correctness, but the fix is raising the limit
  on the host.

## Layout

| Path                 | Purpose                                                        |
| -------------------- | -------------------------------------------------------------- |
| `crates/treesync`    | The library: watcher, queue, reconciler, sinks, remote agent.   |
| `apps/treesync-cli`  | The `treesync` binary: argument parsing and logging setup.      |
| `examples/`          | Deployment examples: Compose, Kubernetes, Docker Swarm.         |
| `docker/`            | Dockerfile, the development compose file, and the sshd test.    |
| `.github/workflows/` | Lint, test, security scans, image publishing, the crates.io release. |

The split is along process boundaries. Anything that decides or moves data is in
the library, so it can be embedded and tested without a binary; the CLI is
argument parsing, the global log subscriber, and an exit code. A library that
installed a global subscriber would fight whatever its host application had
already set up, which is why that lives in the binary.

Workspace members are globbed, so a new crate under `apps/` or `crates/` is
picked up without editing the root `Cargo.toml`. Shared dependency versions live
in `[workspace.dependencies]`; depend on them with `<name> = { workspace = true }`.

## Development

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

Most of the filesystem tests use real directories and a real watcher instead of
mocks, so they are timing-sensitive by nature and assert that something
*eventually* happened without pinning an exact event sequence.

The remote protocol is covered without a network: the CLI's tests drive a real
agent as a local child process, so framing, path and timestamp encoding, delta
reconstruction and resumption are all exercised hermetically. What that cannot
reach is covered by [`docker/remote-test.sh`](docker/remote-test.sh) against a
throwaway sshd container: argument construction, shell quoting, host key
handling, and installing the agent on a host that has never seen it. It needs
Docker and takes a few minutes.

Adversarial conditions get their own coverage, because the failure that matters
for a mirroring tool is the quiet one, a target that does not match with nothing
to say so:

- `crates/treesync/tests/hostile.rs` covers unreadable files and directories,
  unwritable and read-only targets, file/directory type conflicts, symlinks
  planted to redirect a write out of the target root, special files, names at
  the filesystem's length limit, and paths that change type or vanish between
  planning and applying. An unreadable source *directory* has its own test: with
  `delete` on, a source that read as empty would plan the removal of the entire
  target.
- `crates/treesync/tests/regression.rs` holds one test per defect that reached a
  released build, each reproducing the original case and recording what the
  binary did at the time. They duplicate coverage elsewhere on purpose: a test
  named after a property can be weakened by someone who does not know which
  property, and one that says "this used to write outside the target root"
  cannot.
- The CLI's remote tests kill the agent repeatedly *during* multi-file and
  delta transfers, and assert the kills actually landed. A chaos test that never
  broke anything passes just as happily as one that did.
- The immutable bit (`chattr +i`) is exercised in `docker/remote-test.sh`,
  which is why that container gets `--cap-add=LINUX_IMMUTABLE`. The same test
  exists hermetically and reports a **skip** instead of a pass when the
  capability is absent.
- **A full destination disk**, twice over, and neither is a mocked error. The
  hermetic test points the transfer's temporary at `/dev/full`, which returns
  `ENOSPC` on every write; the container test syncs 4 MB into a 1 MB tmpfs, so
  the failure lands *after* bytes are already on disk and there is a
  half-written file to not publish and to clean up. Both then assert the target
  still works once there is room again. A failure that leaves the destination
  unusable is barely better than a silent one.
- **Churn faster than the sync can keep up with**: files rewritten repeatedly
  inside the batching window, and whole directory trees created at once, where
  the assertion is convergence and not the outcome of any single pass.

## Not done

- **Pipelined remote transfer.** The protocol is strict request/response, so
  each action costs a round trip. That is the right shape for a plan whose
  order is load-bearing, but many small files on a high-latency link are
  dominated by it.
- **Hardlinks.** A file with two names is copied twice and arrives as two
  independent files. Permissions and ownership *are* preserved. See `preserve`
  above.
- **Move optimisation.** A rename is re-transferred instead of moved in place.
  The queue pairs rename halves where the backend supplies cookies (inotify
  does, FSEvents does not), but nothing consumes that yet.

## Contributing

Contributions are welcome: issues, bug reports, and pull requests alike. The
project is early, so the areas under [Not done](#not-done) are the most useful
places to start.

### Getting set up

Rust 1.87 or newer. Edition 2024 puts the floor at 1.85; one call to
`u32::is_multiple_of` raises it to 1.87. `cargo clippy` enforces this, so the
declared version cannot drift from what the code actually uses.

```bash
git clone https://github.com/anthid-labs/treesync
cd treesync
cargo test --workspace
```

Docker is needed only for `docker/remote-test.sh`, which is the one test that
uses a real sshd.

### Before opening a pull request

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

CI runs exactly these, with warnings denied. If a change touches the remote
path, also run the end-to-end test against a throwaway sshd container:

```bash
./docker/remote-test.sh
```

### What a change is expected to carry

- **A test that would fail without it.** Most of the filesystem tests use real
  directories and a real watcher instead of mocks, so they assert that
  something *eventually* happened without pinning an exact event sequence.
- **Comments that say why, not what.** The existing code explains the reasoning
  behind decisions that look arbitrary: why the mtime is read after the content,
  why `delete` is off by default, why a variant may not be reordered. That is
  the house style, and it is the part that survives.
- **Anything that changes on-disk behaviour, the config format, or the CLI
  surface called out separately** in the pull request description.

[`AGENTS.md`](AGENTS.md) has the conventions in full, including the writing
style and the dependency rules. treesync is a self-contained daemon with no
broker, datastore, or service mesh, and new dependencies in that direction need
a reason.

### Things to know

- **The filesystem is the authority, not the event stream.** Any design has to
  survive a missed event; reconciliation is the source of truth and
  notification is an optimisation.
- **Never widen the blast radius of a delete.** Destructive propagation needs an
  explicit opt-in and a dry-run path.
- **The watched tree is hostile input.** Paths may contain newlines, invalid
  UTF-8, symlinks pointing outside the root, and entries that vanish between
  `stat` and `open`.
- **The wire protocol is versioned.** `PROTOCOL_VERSION` is bumped whenever two
  versions would misread each other, and new enum variants go at the *end*.
  bincode encodes a variant as its position, so inserting one renumbers the
  rest and breaks the frame that reports the mismatch.

### Releasing

Bump `version` under `[workspace.package]` and merge to `main`. CI publishes
both crates in dependency order, `treesync` before `treesync-cli`, and only once
the lint, the tests, the image build and the image scan have all passed: a
version number is burned on crates.io even if the crate is yanked a minute
later. A version already published is skipped, so a merge that does not bump it
releases nothing.

Versions are inherited from `[workspace.package]`, so both crates and the image
move together.

## Support

treesync is free and Apache-2.0, and stays that way. If it saved you some
trouble and you feel like saying thanks,
[buy me a coffee](https://buymeacoffee.com/dallinwright).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

Contributions are accepted under the same license, per section 5 of the Apache
License: any contribution intentionally submitted for inclusion is licensed
Apache-2.0, with no additional terms.
