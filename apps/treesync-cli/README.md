# treesync-cli

The `treesync` command: watches directories and mirrors them, to a local path or
to a host over SSH, sending only what actually changed.

[![Crates.io](https://img.shields.io/crates/v/treesync-cli.svg)](https://crates.io/crates/treesync-cli)
[![Buy Me A Coffee](https://img.shields.io/badge/buy%20me%20a%20coffee-support-yellow.svg)](https://buymeacoffee.com/dallinwright)

```bash
cargo install treesync-cli
```

The package is `treesync-cli`; the command it installs is `treesync`. The
library it is built on is [`treesync`](https://crates.io/crates/treesync).

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
cannot disagree about what a config means. `watch` stops on SIGTERM or SIGINT
after flushing what it has already seen. Both run every `[[sync]]` unless
`--name` selects one.

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

Nothing has to be installed on the host first. treesync uploads its own binary
as the agent and connects again. A changed file is sent as a rolling-checksum
delta, verified end to end, and resumable if the link drops.

## Documentation

Full configuration reference, Docker usage and design notes live in the
[repository README](https://github.com/anthid-labs/treesync).

## Support

treesync is free and Apache-2.0, and stays that way. If it saved you some
trouble and you feel like saying thanks,
[buy me a coffee](https://buymeacoffee.com/dallinwright).

## License

[Apache-2.0](LICENSE).
