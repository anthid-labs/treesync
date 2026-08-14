# Deployment examples

Working starting points for the three ways treesync usually gets run. Each is
self-contained and commented with the reasoning behind the settings that are not
obvious, since most of them exist to avoid a specific failure.

| File | What it is |
| --- | --- |
| [`docker-compose.yml`](docker-compose.yml) | One container mirroring a volume, locally or over SSH |
| [`kubernetes.yaml`](kubernetes.yaml) | Deployment with a PersistentVolume and PersistentVolumeClaim for each side |
| [`docker-swarm-stack.yml`](docker-swarm-stack.yml) | Swarm stack, with configs and secrets instead of bind mounts |

## What they all share

**`/etc/treesync` is the default config location.** Mount a config there and no
`--config` flag is needed.

**There is no default command.** The image ships without a `CMD` on purpose,
because `sync` would mutate the target the moment the container started. Pass
`check`, `sync` or `watch` explicitly.

**Give the shutdown flush room.** `watch` stops on SIGTERM after applying what
it has already observed. A grace period shorter than that turns a clean stop
into a SIGKILL partway through writing a file, so each example sets one.

**Source and target may not overlap.** treesync rejects that at startup, since
writing into the watched tree would feed the sync its own writes.

**inotify limits come from the host kernel.** `max_user_watches` and
`max_queued_events` are shared by every container on the node. Exhausting them
costs a re-walk and not correctness, but the fix is on the host:

```bash
sysctl -w fs.inotify.max_user_watches=524288
```

**The target has to be writable by the user the container runs as.** Each file
is built in a temporary *inside* the destination directory and renamed into
place, so write permission on that directory is required, not just on the files
in it. This is easy to miss because it applies cleanly and starts fine, then
fails on the first transfer with permission denied.

The Kubernetes example runs as uid 65532 against a `hostPath` volume, which
Kubernetes creates root-owned and 0755. `fsGroup` does not help: kubelet applies
it only to volume types that support ownership management, and `hostPath` is not
one. That example therefore carries a small init container to chown the target.
With a real storageClass, `fsGroup` does the job and the init container can go.

## Local target or remote target

Every example mirrors between two volumes in one container, which is the
simplest thing that works and needs no keys. To mirror to another host instead,
change the target block:

```toml
  [sync.target]
  type = "ssh"
  host = "deploy@example.com"
  path = "/srv/app"
  identity_file = "/etc/treesync/ssh/id_ed25519"
```

Then mount a key at that path. Nothing has to be installed on the far host:
treesync uploads its own binary as the agent on first use. The Kubernetes and
Swarm examples both show the key wired in as a secret, commented out.

See the [configuration reference](../README.md#configuration) for every option,
and [`treesync.example.toml`](../treesync.example.toml) for one annotated file
covering all of them.
