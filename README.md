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

Status: design and log format. Nothing mounts yet.

## Documents

| | |
|---|---|
| [docs/design.md](docs/design.md) | Requirements, review of the previous design, alternatives, architecture, guarantees, build order |
| [docs/wal-format.md](docs/wal-format.md) | The log format, normative |
| [proto/skyfs/wal/v1/wal.proto](proto/skyfs/wal/v1/wal.proto) | The log schema, source of truth |
| [proto/skyfs/snapshot/v1/snapshot.proto](proto/skyfs/snapshot/v1/snapshot.proto) | The snapshot manifest (restore pointer) |
| [docs/workspace-storage.md](docs/workspace-storage.md) | Background: object store comparison and the read-only predecessor design |

## Layout

| Crate | Role | Status |
|---|---|---|
| `crates/skyfs-format` | Log records and snapshot manifest (generated from `proto/`), segment encoding. No I/O. | done |
| `crates/skyfs-store` | Object key layout (the only place that spells a key); later the object store client and local cache | keys done |
| `crates/skyfs-meta` | The POSIX state machine: `apply`, SQLite metadata store, restore replica, snapshots | planned |
| `crates/skyfs` | FUSE daemon, replicator, mount and recovery (Linux) | planned |

Dependencies point one way: `skyfs → skyfs-meta, skyfs-store → skyfs-format`.

## Develop

```sh
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
```

The core crates build and test on macOS and Linux. The FUSE daemon will be
Linux-only; develop it in a Linux VM or container with `/dev/fuse`. No
`protoc` is needed: `build.rs` compiles the proto with `protox`.
