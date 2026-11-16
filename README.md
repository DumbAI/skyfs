# skyfs

A POSIX filesystem whose durable copy lives in blob storage (Cloudflare R2,
S3), mounted with FUSE by one writer at a time, on any cloud.

- **Mounts in seconds, whatever the volume size.** Only metadata is
  downloaded; every file is listed and `stat`-able at once.
- **Reads on demand.** File contents are fetched by range on first read,
  verified and cached on local disk.
- **Every change is a log record.** Operations are appended to an immutable
  write-ahead log of segments in blob storage, replicated synchronously or
  asynchronously (`--replication sync|async`).
- **Everything mountpoint-s3 refuses works.** Random and partial writes,
  in-place edits, appends, truncate, file and directory renames, hard links,
  symlinks, chmod/chown, xattrs. Each is a small log record, never a
  rewrite of existing objects. Replaying the log from the bucket alone
  restores the exact state, at the latest op or any retained earlier one.
- **Blob storage holds a snapshot plus a short log.** The mounted container
  folds the log into a new snapshot in the background and on shutdown; the
  snapshot's manifest is the restore pointer. Mount replays only minutes of
  log.
- **Crash-consistent.** After any crash the volume is the result of a prefix
  of the operations, never a mixture.

- **No long-lived keys.** A mount holds one credential at a time, scoped
  to its volume's prefix and valid for at most an hour: workload identity
  on AWS, or the skyfs credential broker anywhere else, refreshed in place.

Status: v1 implemented and tested end to end through FUSE on Linux;
passes the full pjdfstest suite (8,798 tests). Open items (xfstests,
fault injection, the broker service) are in
[design §12](docs/design.md#12-build-order-and-what-settles-the-open-questions).

## Use

```sh
# Create a volume (prints its id).
skyfs create --store s3://bucket/tenant --endpoint https://<account>.r2.cloudflarestorage.com --region auto \
  --credentials broker --broker-url https://skyfs.internal --broker-token-file /run/skyfs/token

# Mount it (Linux, /dev/fuse). SIGTERM unmounts cleanly.
skyfs mount --store s3://bucket/tenant --volume <id> --cache-dir /var/cache/skyfs \
  --replication async --max-lag 1s /mnt/data

skyfs info --store ... --volume <id>   # restore pointer and log head
skyfs fsck --store ... --volume <id>   # replay from the bucket and check invariants
```

`--store` also takes `file:///dir` (a local bucket, for testing). Every flag
has an environment form (`SKYFS_STORE`, `SKYFS_VOLUME`, `SKYFS_CREDENTIALS`,
...). Credentials (`--credentials`, [design §10.1](docs/design.md#101-authentication-and-credentials)):

| Source | Use it for |
|---|---|
| `ambient` (default) | EKS IRSA / Pod Identity, ECS, EC2 roles. Static keys are refused without `--allow-static-credentials` |
| `broker` | Any Kubernetes (projected service-account token) or orchestrated Docker (one-time mount token); S3 and R2 |
| `file` | A `credential_process` JSON file kept fresh by Vault Agent or a sidecar |
| `process` | A command printing `credential_process` JSON |

In a container: `--device /dev/fuse --cap-add SYS_ADMIN`, or run skyfs on
the host and bind-propagate the mount so the app container holds no
credentials at all.

## Documents

| | |
|---|---|
| [docs/design.md](docs/design.md) | Requirements, review of the previous design, alternatives, architecture, credentials, guarantees, build order |
| [docs/wal-format.md](docs/wal-format.md) | The log format, normative |
| [proto/skyfs/wal/v1/wal.proto](proto/skyfs/wal/v1/wal.proto) | The log schema, source of truth |
| [proto/skyfs/snapshot/v1/snapshot.proto](proto/skyfs/snapshot/v1/snapshot.proto) | The snapshot manifest (restore pointer) |
| [docs/workspace-storage.md](docs/workspace-storage.md) | Background: object store comparison and the read-only predecessor design |

## Layout

| Crate | Role |
|---|---|
| `crates/skyfs-format` | Log records and snapshot manifest (generated from `proto/`), segment encoding. No I/O. |
| `crates/skyfs-meta` | The POSIX state machine: validation (`plan`), `apply`, the SQLite store that is also the snapshot format, fsck |
| `crates/skyfs-store` | Object keys (the only place that spells one), the object client, credentials, the local chunk cache |
| `crates/skyfs-volume` | The engine: local WAL, replicator, blobs, read path, mount and recovery, restorer and GC. Platform-independent |
| `crates/skyfs` | CLI and the FUSE adapter (Linux) |

Dependencies point one way:
`skyfs → skyfs-volume → skyfs-meta, skyfs-store → skyfs-format`.

## Develop

```sh
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
```

Everything but the FUSE adapter builds and tests on macOS and Linux; the
engine's end-to-end tests run against in-memory buckets. To check the
Linux build and run the kernel smoke test from macOS:

```sh
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined \
  -v "$PWD":/src -w /src -e CARGO_TARGET_DIR=/tmp/t rust:1.92 bash -c \
  'apt-get update -qq && apt-get install -y -qq fuse3 attr python3 >/dev/null &&
   rustup component add clippy && cargo clippy --workspace --all-targets -- -D warnings &&
   cargo build -p skyfs && PATH=/tmp/t/debug:$PATH scripts/fuse-smoke.sh'
```

No `protoc` is needed: `build.rs` compiles the proto with `protox`.
