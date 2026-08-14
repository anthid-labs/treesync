# treesync Agent Guide

## Scope

This is the treesync Rust workspace: an experiment in replacing lsyncd. It is
early, and most of the tree is an empty skeleton. Make the smallest cohesive
change that fits the layout below, and do not scaffold directories or crates
that the current task does not need.

## Architecture at a glance

- `apps/*` contains independently built binaries. A package can expose multiple
  binaries; use its `[[bin]]` declarations rather than assuming the package name
  is the executable name. `treesync-cli` builds a command called `treesync`.
- `crates/*` contains shared, domain-neutral Rust code. Prefer an existing
  library over duplicating logic in an app.
- `crates/treesync` is the engine and `apps/treesync-cli` is the command around
  it. Both are published to crates.io, so the split is a published API boundary,
  not just a directory: anything that decides or moves data belongs in the
  library, and argument parsing, the global log subscriber and the exit code
  belong in the binary. A library that installed a subscriber would fight
  whatever its host application had already set up.
- `tests/*` contains cross-cutting test crates. Gate anything that needs a real
  filesystem, network peer, or remote host behind a non-default feature so
  `cargo test --workspace` stays hermetic.
- Workspace members are globbed. A new crate under `apps/`, `crates/`, or
  `tests/` joins the workspace without a root `Cargo.toml` edit.

## Dependencies

treesync is a self-contained daemon. It has no message broker, no external
datastore, and no service mesh: **no NATS, Redis/Dragonfly, ClickHouse,
Postgres, sqlx, tonic/prost, or axum.** Do not reach for the anthid
infrastructure stack out of habit. Patterns from that monorepo apply to code
structure, not to its dependency set.

**Never use `dashmap`.** Holding a reference into one shard while touching
another deadlocks, and the API does nothing to stop you. The failure is a hung
daemon, not a compile error. Prefer state owned by a single task and passed by
message; where sharing is unavoidable, use `std::sync::Mutex`/`RwLock` around a
plain `HashMap` so the lock scope is visible at the call site.

All state is local: in memory while running, and on local disk when it must
survive a restart. Anything that would require operating a service alongside
treesync needs an explicit decision from the user first.

## Rust conventions

- The workspace uses Rust 2024. Shared dependency versions belong in
  `[workspace.dependencies]`; consume them with `<name> = { workspace = true }`.
  Add a crate-local version only when a package genuinely needs to diverge.
- Keep optional integrations behind feature flags, and keep code that uses them
  correctly gated.
- Prefer `tracing` with useful context over `println!`. Never log credentials,
  keys, or the contents of synced files.
- Preserve cancellation and error propagation in async code. Do not detach tasks
  without a clear lifecycle, shutdown path, and error reporting.

## Filesystem and sync work

- Treat the watched tree as hostile input: paths may contain newlines, invalid
  UTF-8, symlinks pointing outside the root, and entries that vanish between
  `stat` and `open`. Handle these rather than assuming them away.
- Filesystem event streams drop, coalesce, and reorder events. Any design must
  survive a missed event: reconciliation is the source of truth, notification
  is an optimization.
- Never widen the blast radius of a delete. Destructive propagation needs an
  explicit opt-in and a dry-run path.
- Platform watchers differ (inotify, FSEvents, kqueue). Keep platform-specific
  behavior isolated and documented rather than spread through call sites.

## Verification

Run checks from the repository root and scope them to the package changed:

```bash
cargo fmt --check
cargo clippy -p <package> --all-targets --all-features -- -D warnings
cargo test -p <package> --lib --bins --tests
```

Do not claim a test passed if it was skipped for a missing dependency.

## Writing style

This applies to everything you write: code comments, documentation, commit
messages, pull request text, and replies to the user.

- Never use an em-dash or an en-dash. Rewrite the sentence with a comma, a
  colon, a full stop, or brackets. A hyphen inside a compound word is fine.
- Use simple, direct language. Prefer short sentences and plain words.
- Cut filler and hedging. Say the thing once, then stop.

## Change handoff

State which package and binaries are affected, and report the exact
verification commands run and their outcome. Call out anything that changes
on-disk behavior, config format, or the CLI surface separately.
