#!/usr/bin/env bash
#
# End-to-end proof of a remote sync, against a real sshd.
#
# The hermetic tests in apps/treesync-cli/tests/remote.rs drive the agent as a
# local child process, which covers the protocol but not SSH. This covers what
# they cannot: argument construction, shell quoting, host key handling, the
# platform check, and installing the agent on a host that has never seen it.
#
# What it does:
#
#   1. builds a static Linux treesync, which is both the client's payload and
#      the agent that ends up running on the host
#   2. starts a throwaway sshd container
#   3. syncs into it over SSH, with no agent installed on the far side
#   4. checks the tree arrived, that a second pass is a no-op, and that
#      changes and deletions propagate
#   5. leaves `watch` running against the host and makes changes underneath it,
#      checking each one lands without anything being run again
#
# Everything it creates is namespaced `treesync-remote-test` and removed on
# exit, including on failure.
#
# Usage: docker/remote-test.sh

set -euo pipefail

readonly NAME=treesync-remote-test
readonly IMAGE="$NAME-sshd"
readonly BUILDER="$NAME-builder"
readonly PORT=${TREESYNC_TEST_PORT:-22022}

readonly ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly WORK="$(mktemp -d)"

# Set by `check`; reported at the end so one failure does not hide the rest.
FAILURES=0

cleanup() {
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker rm -f "$BUILDER" >/dev/null 2>&1 || true
    rm -rf "$WORK"
}
trap cleanup EXIT

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
ok() { printf '  \033[32mok\033[0m   %s\n' "$*"; }
bad() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAILURES=$((FAILURES + 1)); }

check() {
    local description="$1" expected="$2" actual="$3"

    if [[ "$expected" == "$actual" ]]; then
        ok "$description"
    else
        bad "$description"
        printf '       expected: %q\n       actual:   %q\n' "$expected" "$actual"
    fi
}

# Every regular file the container holds, as `path sha256`.
#
# Hashes rather than contents so the comparison holds for a multi-line file, a
# binary one, and one larger than a protocol chunk alike, and so "the tree
# arrived" means byte-identical, not merely present.
remote_manifest() {
    docker exec "$NAME" sh -c '
        cd /home/deploy/target 2>/dev/null || exit 0
        find . -type f | sort | while read -r f; do
            printf "%s %s\n" "${f#./}" "$(sha256sum "$f" | cut -d" " -f1)"
        done
    '
}

# The same listing for the source, minus what the config excludes.
source_manifest() {
    (
        cd "$WORK/src" || exit 0
        find . -type f ! -name '*.tmp' | sort | while read -r f; do
            printf "%s %s\n" "${f#./}" "$(shasum -a 256 "$f" | cut -d" " -f1)"
        done
    )
}

treesync_sync() {
    "$ROOT/target/debug/treesync" --config "$WORK/config.toml" sync "$@"
}

# Waits for a command to report the value expected, or gives up.
#
# Every daemon assertion is "eventually": the watcher coalesces and delays
# events by design, so pinning an exact interval would be testing this machine
# rather than treesync.
eventually() {
    local description="$1" expected="$2" command="$3"
    local deadline=$((SECONDS + 30)) actual=""

    while [[ "$SECONDS" -lt "$deadline" ]]; do
        actual="$(eval "$command" 2>/dev/null || true)"

        if [[ "$actual" == "$expected" ]]; then
            ok "$description"
            return 0
        fi

        sleep 0.2
    done

    bad "$description (timed out)"
    printf '       expected: %q\n       actual:   %q\n' "$expected" "$actual"
}

# --------------------------------------------------------------------------
say "Building a static Linux treesync (the agent payload)"
# --------------------------------------------------------------------------

# The same builder stage the published image uses, so what gets shipped to the
# host is the binary that image would contain.
docker build --quiet --target builder -t "$BUILDER" -f "$ROOT/docker/Dockerfile" "$ROOT" >/dev/null
container=$(docker create "$BUILDER")
docker cp "$container:/src/target/release/treesync" "$WORK/treesync-linux" >/dev/null
docker rm -f "$container" >/dev/null

file "$WORK/treesync-linux" | sed 's/^/  /'

# --------------------------------------------------------------------------
say "Starting a throwaway sshd"
# --------------------------------------------------------------------------

mkdir -p "$WORK/ssh"
chmod 700 "$WORK/ssh"
ssh-keygen -q -t ed25519 -N '' -C "$NAME" -f "$WORK/ssh/id_ed25519"

docker build --quiet -t "$IMAGE" -f "$ROOT/docker/test-sshd.Dockerfile" "$ROOT/docker" >/dev/null
# Two things the default container cannot do, both needed by the adversarial
# checks below:
#
#   LINUX_IMMUTABLE  is not in Docker's default capability set, and without it
#                    `chattr +i` fails, turning the immutable-target check into
#                    a skip rather than a test.
#   the small tmpfs  is a genuinely full filesystem to sync into. Mounted at
#                    start rather than from inside, which would need CAP_SYS_ADMIN.
#                    `mode=1777` so the unprivileged `deploy` user can write to it.
docker run -d --name "$NAME" --cap-add=LINUX_IMMUTABLE \
    --tmpfs "/home/deploy/small:size=1M,mode=1777" \
    -p "127.0.0.1:$PORT:22" "$IMAGE" >/dev/null

docker exec -i "$NAME" sh -c \
    'cat > /home/deploy/.ssh/authorized_keys \
     && chmod 600 /home/deploy/.ssh/authorized_keys \
     && chown deploy:deploy /home/deploy/.ssh/authorized_keys' \
    < "$WORK/ssh/id_ed25519.pub"

# Wait for sshd rather than sleeping a guessed interval.
for _ in $(seq 1 50); do
    if ssh-keyscan -p "$PORT" -t ed25519 127.0.0.1 2>/dev/null | grep -q ssh-ed25519; then
        break
    fi
    sleep 0.2
done

# The host key is pinned rather than the check disabled: BatchMode refuses an
# unknown host, and turning verification off would leave that untested.
#
# It goes in a file of this script's own, reached through `ssh_options`,
# because OpenSSH finds `~/.ssh/known_hosts` through the password database and
# not `$HOME`, so there is no environment variable that would redirect it, and
# the alternative is writing to the developer's real known_hosts.
ssh-keyscan -p "$PORT" 127.0.0.1 > "$WORK/ssh/known_hosts" 2>/dev/null
echo "  sshd up on 127.0.0.1:$PORT"

# --------------------------------------------------------------------------
say "Preparing the source tree"
# --------------------------------------------------------------------------

mkdir -p "$WORK/src/sub/deep"
echo "one"   > "$WORK/src/a.txt"
echo "two"   > "$WORK/src/sub/b.txt"
echo "three" > "$WORK/src/sub/deep/c.txt"
echo "junk"  > "$WORK/src/scratch.tmp"
printf '#!/bin/sh\necho hi\n' > "$WORK/src/script.sh"
chmod 755 "$WORK/src/script.sh"
# Larger than one protocol chunk, so the streaming path is exercised.
head -c 900000 /dev/urandom | base64 > "$WORK/src/big.dat"
ln -sf /etc/hosts "$WORK/src/outside-link"

cat > "$WORK/config.toml" <<EOF
[[sync]]
name = "remote"
source = "$WORK/src"
exclude = ["*.tmp"]
delete = true

  [sync.target]
  type = "ssh"
  host = "deploy@127.0.0.1"
  port = $PORT
  path = "/home/deploy/target"
  identity_file = "$WORK/ssh/id_ed25519"
  agent_binary = "$WORK/treesync-linux"
  ssh_options = [
    "UserKnownHostsFile=$WORK/ssh/known_hosts",
    "StrictHostKeyChecking=yes",
    "GlobalKnownHostsFile=/dev/null",
  ]
EOF

# --------------------------------------------------------------------------
say "check: validating without contacting the host"
# --------------------------------------------------------------------------

"$ROOT/target/debug/treesync" --config "$WORK/config.toml" check | sed 's/^/  /'

# --------------------------------------------------------------------------
say "First sync: no agent on the host yet"
# --------------------------------------------------------------------------

check "the host starts with no agent installed" "absent" \
    "$(docker exec "$NAME" sh -c 'test -e /home/deploy/.cache/treesync/treesync && echo present || echo absent')"

treesync_sync | sed 's/^/  /'

check "the agent was installed on the host" "present" \
    "$(docker exec "$NAME" sh -c 'test -x /home/deploy/.cache/treesync/treesync && echo present || echo absent')"

# Covers the large file too: it is in the manifest, hashed like everything
# else, so a chunked transfer that dropped or duplicated a block shows up here.
check "every file arrived byte for byte" "$(source_manifest)" "$(remote_manifest)"

check "the excluded file was not transferred" "absent" \
    "$(docker exec "$NAME" sh -c 'test -e /home/deploy/target/scratch.tmp && echo present || echo absent')"

check "permissions were mirrored" "755" \
    "$(docker exec "$NAME" stat -c '%a' /home/deploy/target/script.sh)"

check "the symlink was replicated, not followed" "/etc/hosts" \
    "$(docker exec "$NAME" readlink /home/deploy/target/outside-link)"

# --------------------------------------------------------------------------
say "Second sync: nothing should move"
# --------------------------------------------------------------------------

# The one that catches a transfer that did not preserve mtime: without it every
# file differs on every pass and the sync never converges.
second=$(treesync_sync)
echo "$second" | sed 's/^/  /'
check "a settled tree produces no actions" "0 action(s)" \
    "$(echo "$second" | grep -o '[0-9]* action(s)' | head -1)"

# --------------------------------------------------------------------------
say "Third sync: a change and a deletion"
# --------------------------------------------------------------------------

echo "one, edited" > "$WORK/src/a.txt"
rm "$WORK/src/sub/deep/c.txt"

treesync_sync | sed 's/^/  /'

check "the edit propagated" "one, edited" \
    "$(docker exec "$NAME" cat /home/deploy/target/a.txt)"
check "the deletion propagated" "absent" \
    "$(docker exec "$NAME" sh -c 'test -e /home/deploy/target/sub/deep/c.txt && echo present || echo absent')"

# --------------------------------------------------------------------------
say "Fourth sync: a large file changed in one place"
# --------------------------------------------------------------------------

# The case delta transfer exists for. The edit is longer than what it replaces,
# so every byte after it shifts, which is what defeats a fixed-block scheme and
# what the rolling window is for.
python3 - "$WORK/src/big.json" <<'PY'
import json, sys
with open(sys.argv[1], 'w') as out:
    out.write('[\n')
    for i in range(60_000):
        out.write('  ' + json.dumps({'id': i, 'name': f'record-{i}', 'note': 'x' * 120}) + ',\n')
    out.write(']\n')
PY

treesync_sync >/dev/null 2>&1

python3 - "$WORK/src/big.json" <<'PY'
import sys
path = sys.argv[1]
data = open(path).read()
open(path, 'w').write(data.replace('"name": "record-30000"',
                                   '"name": "record-30000-EDITED-AND-LONGER"', 1))
PY

size=$(stat -c '%s' "$WORK/src/big.json")
# Debug logging, because the bytes actually sent is the thing being asserted
# and the summary line does not carry it. The log is styled even when it is not
# a terminal, and the escapes land *between* `sent` and `=`, so they have to
# come out before anything can match.
patched=$(RUST_LOG=treesync=debug treesync_sync 2>&1 | sed 's/\x1b\[[0-9;]*m//g')
echo "$patched" | sed 's/^/  /'

check "the patched file is byte for byte the source" \
    "$(sha256sum < "$WORK/src/big.json" | cut -d' ' -f1)" \
    "$(docker exec "$NAME" sha256sum /home/deploy/target/big.json | cut -d' ' -f1)"

# The actual requirement: the cost tracks the edit, not the file.
# `|| true` because a miss here must report a failed check, not abandon the run
# under `set -e` and skip every section below.
sent=$(echo "$patched" | grep -o 'sent=[0-9]*' | head -1 | cut -d= -f2 || true)
check "the delta reported what it sent" "yes" \
    "$([ -n "$sent" ] && echo yes || echo no)"
check "a one field edit cost far less than the whole file (${sent:-?} of $size bytes)" "yes" \
    "$([ -n "$sent" ] && [ "$sent" -lt $((size / 10)) ] && echo yes || echo no)"

# --------------------------------------------------------------------------
say "Reusing the installed agent"
# --------------------------------------------------------------------------

installed_before=$(docker exec "$NAME" stat -c '%Y' /home/deploy/.cache/treesync/treesync)
echo "four" > "$WORK/src/d.txt"
treesync_sync >/dev/null 2>&1
installed_after=$(docker exec "$NAME" stat -c '%Y' /home/deploy/.cache/treesync/treesync)

check "an agent that already works is not re-uploaded" "$installed_before" "$installed_after"
check "the new file still arrived" "four" \
    "$(docker exec "$NAME" cat /home/deploy/target/d.txt)"

# --------------------------------------------------------------------------
say "Adversarial: a target file the host will not let anyone replace"
# --------------------------------------------------------------------------

# The immutable bit defeats even root: the rename that publishes a transfer
# fails with EPERM. What matters is that this is *reported* rather than
# swallowed, and that the file already there is untouched. A mirroring tool
# that silently leaves a stale file is worse than one that fails loudly.
#
# Needs CAP_LINUX_IMMUTABLE, added to this throwaway container above. Reported
# as a skip rather than a pass if the kernel or filesystem still refuses.
echo "immutable" > "$WORK/src/immutable.txt"
treesync_sync >/dev/null 2>&1

if docker exec "$NAME" chattr +i /home/deploy/target/immutable.txt 2>/dev/null; then
    echo "locked" > "$WORK/src/immutable.txt"

    if treesync_sync >/dev/null 2>&1; then
        bad "an immutable target must fail the sync, not report success"
    else
        ok "an unreplaceable target is reported as a failure"
    fi

    check "the immutable file was left exactly as it was" "immutable" \
        "$(docker exec "$NAME" cat /home/deploy/target/immutable.txt)"

    check "no temporary was left beside it" "0" \
        "$(docker exec "$NAME" sh -c 'ls -a /home/deploy/target | grep -c "^\.treesync-" || true')"

    docker exec "$NAME" chattr -i /home/deploy/target/immutable.txt

    # And once the obstruction is gone, the next pass has to converge rather
    # than staying stuck on the file that failed.
    treesync_sync >/dev/null 2>&1
    check "it converges once the file can be written again" "locked" \
        "$(docker exec "$NAME" cat /home/deploy/target/immutable.txt)"
else
    echo "  SKIPPED: chattr +i unavailable (needs CAP_LINUX_IMMUTABLE and a"
    echo "           filesystem that supports it)"
fi

# --------------------------------------------------------------------------
say "Adversarial: the destination disk is full"
# --------------------------------------------------------------------------

# A real filesystem with no room, not a simulated error: the agent's temporary
# fills the tmpfs and the write fails partway through. That is the interesting
# shape: the failure lands *after* bytes are already on disk, so the target has
# a half-written file to not publish and to clean up.
check "the small filesystem is mounted and tiny" "1.0M" \
    "$(docker exec "$NAME" sh -c "df -h /home/deploy/small | awk 'NR==2 {print \$2}'")"

mkdir -p "$WORK/toobig"
# Comfortably past the 1M target, and incompressible so the frame layer cannot
# quietly make it fit.
head -c 4000000 /dev/urandom > "$WORK/toobig/big.dat"

cat > "$WORK/full.toml" <<EOF
[[sync]]
name = "full"
source = "$WORK/toobig"

  [sync.target]
  type = "ssh"
  host = "deploy@127.0.0.1"
  port = $PORT
  path = "/home/deploy/small"
  identity_file = "$WORK/ssh/id_ed25519"
  agent_binary = "$WORK/treesync-linux"
  ssh_options = [
    "UserKnownHostsFile=$WORK/ssh/known_hosts",
    "StrictHostKeyChecking=yes",
    "GlobalKnownHostsFile=/dev/null",
  ]
EOF

if "$ROOT/target/debug/treesync" --config "$WORK/full.toml" sync >/dev/null 2>&1; then
    bad "syncing 4M into a 1M filesystem must fail, not report success"
else
    ok "a destination with no room is reported as a failure"
fi

check "the oversized file was not published" "absent" \
    "$(docker exec "$NAME" sh -c 'test -e /home/deploy/small/big.dat && echo present || echo absent')"

check "the half-written temporary was cleaned up" "0" \
    "$(docker exec "$NAME" sh -c 'ls -a /home/deploy/small | grep -c "^\.treesync-" || true')"

# And the filesystem is usable again afterwards: a failure that left the disk
# full of its own debris would make the next attempt fail for a new reason.
rm "$WORK/toobig/big.dat"
echo "small enough" > "$WORK/toobig/ok.txt"

if "$ROOT/target/debug/treesync" --config "$WORK/full.toml" sync >/dev/null 2>&1; then
    ok "a file that does fit still syncs afterwards"
else
    bad "the target was left unusable by the failed transfer"
fi

check "and it arrived intact" "small enough" \
    "$(docker exec "$NAME" cat /home/deploy/small/ok.txt)"

# --------------------------------------------------------------------------
say "Nothing is left running on the host"
# --------------------------------------------------------------------------

sleep 1
check "no agent process outlived the sync" "0" \
    "$(docker exec "$NAME" sh -c 'ps -o args= | grep -c "[t]reesync agent" || true')"

# --------------------------------------------------------------------------
say "watch: mirroring changes as they happen, over SSH"
# --------------------------------------------------------------------------

# One SSH connection and one agent for the daemon's whole lifetime. Every
# change below travels over the connection opened here.
"$ROOT/target/debug/treesync" --config "$WORK/config.toml" watch \
    > "$WORK/watch.log" 2>&1 &
watcher=$!

# The daemon has to be up before a change means anything. Its startup pass is
# a no-op here (the tree is already in sync), so wait for the line it prints.
eventually "the daemon started watching" "yes" \
    'grep -q "watching" "'"$WORK"'/watch.log" && echo yes'

echo "five" > "$WORK/src/e.txt"
eventually "a new file reached the host" "five" \
    'docker exec '"$NAME"' cat /home/deploy/target/e.txt'

echo "five, edited" > "$WORK/src/e.txt"
eventually "an edit reached the host" "five, edited" \
    'docker exec '"$NAME"' cat /home/deploy/target/e.txt'

mkdir -p "$WORK/src/live/deep"
echo "six" > "$WORK/src/live/deep/f.txt"
eventually "a file in a new directory reached the host" "six" \
    'docker exec '"$NAME"' cat /home/deploy/target/live/deep/f.txt'

rm "$WORK/src/e.txt"
eventually "a deletion reached the host" "absent" \
    'docker exec '"$NAME"' sh -c "test -e /home/deploy/target/e.txt && echo present || echo absent"'

# Three rounds, so this is a daemon that keeps working rather than one that
# worked once.
for round in 1 2 3; do
    echo "round $round" > "$WORK/src/repeat.txt"
    eventually "round $round propagated" "round $round" \
        'docker exec '"$NAME"' cat /home/deploy/target/repeat.txt'
done

check "one agent served the whole session" "1" \
    "$(docker exec "$NAME" sh -c 'ps -o args= | grep -c "[t]reesync agent" || true')"

# --------------------------------------------------------------------------
say "watch: riding out a brief network outage"
# --------------------------------------------------------------------------

# The container is cut off from the network entirely, which is what a dying switch
# looks like from here.
docker network disconnect bridge "$NAME"
ok "the host was cut off"

echo "written during the outage" > "$WORK/src/outage.txt"
sleep 3

check "the daemon stayed up through the outage" "running" \
    "$(kill -0 "$watcher" 2>/dev/null && echo running || echo gone)"

docker network connect bridge "$NAME"
ok "the host came back"

# Deliberately no assertion that this reconnected. A few seconds of dropped
# packets does not break an established TCP connection: it stalls, and the
# kernel delivers what was queued once the link returns. Surviving that
# without reconnecting is the better outcome, and asserting a reconnect here
# would be asserting that treesync gives up sooner than TCP does.
eventually "the change made during the outage reached the host" "written during the outage" \
    'docker exec '"$NAME"' cat /home/deploy/target/outage.txt'

# --------------------------------------------------------------------------
say "watch: recovering from a connection that really dies"
# --------------------------------------------------------------------------

# A reboot, an sshd restart, an idle connection reaped by a firewall: all of
# them end with the agent gone and the socket closed, which is what this does.
docker exec "$NAME" pkill -KILL -f 'treesync agent'
ok "the agent on the host was killed"

echo "after the agent died" > "$WORK/src/revived.txt"

eventually "the change reached the host on a rebuilt connection" "after the agent died" \
    'docker exec '"$NAME"' cat /home/deploy/target/revived.txt'

check "it logged losing the connection" "yes" \
    "$(grep -q 'lost the connection' "$WORK/watch.log" && echo yes || echo no)"
check "it logged reconnecting" "yes" \
    "$(grep -q 'reconnected to the agent' "$WORK/watch.log" && echo yes || echo no)"

# --------------------------------------------------------------------------
say "watch: recovering from a host that came back without an agent"
# --------------------------------------------------------------------------

# An instance replaced by its autoscaler answers SSH perfectly well and has
# nothing installed on it. Reconnecting has to notice and reinstall, or the
# daemon retries forever against a host that will never answer.
docker exec "$NAME" pkill -KILL -f 'treesync agent'
docker exec "$NAME" rm -f /home/deploy/.cache/treesync/treesync
check "the agent binary is gone from the host" "absent" \
    "$(docker exec "$NAME" sh -c 'test -e /home/deploy/.cache/treesync/treesync && echo present || echo absent')"

echo "after the host was rebuilt" > "$WORK/src/reinstalled.txt"

eventually "the agent was reinstalled by the reconnect" "present" \
    'docker exec '"$NAME"' sh -c "test -x /home/deploy/.cache/treesync/treesync && echo present || echo absent"'
eventually "the change reached the rebuilt host" "after the host was rebuilt" \
    'docker exec '"$NAME"' cat /home/deploy/target/reinstalled.txt'

# --------------------------------------------------------------------------
say "watch: stopping on SIGTERM"
# --------------------------------------------------------------------------

kill -TERM "$watcher"
wait "$watcher" && watch_status=0 || watch_status=$?

check "the daemon exited cleanly on SIGTERM" "0" "$watch_status"
check "it said why it stopped" "yes" \
    "$(grep -q 'SIGTERM' "$WORK/watch.log" && echo yes || echo no)"

sleep 1
check "the agent was shut down with it" "0" \
    "$(docker exec "$NAME" sh -c 'ps -o args= | grep -c "[t]reesync agent" || true')"

# The daemon and the one-shot command share one reconcile path, so after a
# clean shutdown a `sync` must find nothing left to do.
final=$(treesync_sync)
# Printed, because "1 action(s)" on a settled tree says nothing about which
# file failed to converge, which is the only useful part.
echo "$final" | sed 's/^/  /'
check "a sync after the daemon finds nothing to do" "0 action(s)" \
    "$(echo "$final" | grep -o '[0-9]* action(s)' | head -1)"

# --------------------------------------------------------------------------
if [[ "$FAILURES" -eq 0 ]]; then
    printf '\n\033[32mAll checks passed.\033[0m\n'
else
    printf '\n\033[31m%s check(s) failed.\033[0m\n' "$FAILURES"
    exit 1
fi
