# -----------------------
# Builder
# -----------------------
FROM rust:alpine3.23 AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

# musl-dev and build-base are needed for the static link and for blake3's
# assembly backend. There is deliberately no openssl here: treesync's only
# network transport is an `ssh` child process, so nothing in the tree links TLS.
RUN apk add --no-cache \
    musl-dev \
    build-base \
    pkgconfig \
    git

WORKDIR /src

COPY .cargo/ .cargo/
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY apps/ apps/

RUN cargo build --release --package treesync-cli --bin treesync

# -----------------------
# Runtime
# -----------------------
FROM alpine:3.23 AS runner

# openssh-client is what a remote target is reached through: treesync spawns
# `ssh` rather than speaking the protocol itself, so without this an ssh target
# in a container fails at the first connection.
#
# The same image is also what runs on the far end, since the agent is this binary,
# but the agent needs nothing from this list, since it is started by the
# client's ssh session and talks over its own stdin and stdout.
#
# procps is for looking at a stuck process without rebuilding the image.
RUN apk add --no-cache \
    openssh-client \
    procps

COPY --from=builder /src/target/release/treesync /usr/local/bin/treesync

# Runs as root by default, and that is deliberate rather than an oversight: a
# sync tool has to read every file in the source tree and write files it does
# not own in the target. Override with `--user` where the trees are owned by a
# single known uid.

# Where a bind-mounted config is expected. Matches the CLI's default so no
# --config flag is needed in the common case.
VOLUME ["/etc/treesync"]

# SIGNALS, and this matters more than it looks.
#
# An ENTRYPOINT process runs as PID 1, and the kernel installs no default signal
# handlers for PID 1. A process that does not explicitly handle SIGTERM simply
# ignores it, so `docker stop` waits out its timeout and then SIGKILLs.
#
# Measured with this image:
#
#   sleep as PID 1                  docker stop took 10s   (SIGTERM ignored)
#   sleep under `--init`            docker stop took  0s
#
# That is the difference between the shutdown flush running and not running at
# all: SIGKILL cannot be caught, so the batched-but-unapplied work is discarded
# and the next start has to re-walk the tree to find it again.
#
# `watch` therefore installs handlers of its own for SIGTERM and SIGINT, which
# cancel the token and let each sync flush what it has already observed. That
# works correctly at PID 1. Running with `--init` is a belt-and-braces second
# line of defence for the other commands, not a substitute.
ENTRYPOINT ["/usr/local/bin/treesync"]

# No CMD on purpose: there is no safe default subcommand. `sync` and `watch`
# both mutate a target the moment the container starts, so the operator states
# which one they want.
