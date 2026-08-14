# treesync

One-way directory mirroring, in Rust. Watches a tree and mirrors it to another
directory on this machine, or to a host over SSH, sending only what actually
changed.

[![Crates.io](https://img.shields.io/crates/v/treesync.svg)](https://crates.io/crates/treesync)
[![Docs.rs](https://docs.rs/treesync/badge.svg)](https://docs.rs/treesync)
[![Buy Me A Coffee](https://img.shields.io/badge/buy%20me%20a%20coffee-support-yellow.svg)](https://buymeacoffee.com/dallinwright)

This is the library. For the command line tool:

```bash
cargo install treesync-cli
```

## Using it

```rust,no_run
use tokio_util::sync::CancellationToken;
use treesync::config::file::Config;
use treesync::syncer::{Mode, Syncer};

# async fn example() -> treesync::error::Result<()> {
let config = Config::load("/etc/treesync/config.toml")?;

for entry in config.resolve() {
    let syncer = Syncer::open(&entry, Mode::Once, CancellationToken::new()).await?;
    syncer.run().await?;
    syncer.close().await;
}
# Ok(())
# }
```

## How it works

The watcher reports what the kernel saw, the queue collapses a burst of events
into the distinct paths that changed, the reconciler compares those paths across
both trees, and a sink applies the difference.

Two things follow from that shape:

- **The filesystem is the authority, not the event stream.** Event kinds are not
  trusted. On macOS/FSEvents, deleting a file arrives labelled as a creation. A
  batch says only which paths are suspect; the reconciler stats them.
- **Lost events cost a re-walk, never correctness.** When the kernel drops
  events or the queue fills, treesync reconciles that subtree in full rather
  than replaying a log it knows has a hole in it.

Where it differs from rsync: rsync rebuilds its file list on every invocation,
so its cost is proportional to the tree. A batch naming three files stats three
files.

## Remote targets

Nothing has to be installed on the host first. treesync connects over SSH and,
if no usable agent answers, uploads one and connects again. The agent *is* the
treesync binary, run as `treesync agent`, so there is no second artifact to
build or version.

A changed file is sent as a rolling-checksum delta against the copy the target
already holds. Measured on a 210 MB JSON document with one field edited: 27 KB
on the wire. Every transfer is verified end to end with BLAKE3 and refused
rather than published on a mismatch, and an interrupted transfer resumes from
where it stopped after checking that what survived matches the source.

## Logging

Events go through [`tracing`](https://docs.rs/tracing). No subscriber is
installed by this crate; that belongs to the binary at the top of the stack.

## Documentation

Full documentation, configuration reference and Docker usage live in the
[repository README](https://github.com/anthid-labs/treesync).

## Support

treesync is free and Apache-2.0, and stays that way. If it saved you some
trouble and you feel like saying thanks,
[buy me a coffee](https://buymeacoffee.com/dallinwright).

## License

[Apache-2.0](LICENSE).
