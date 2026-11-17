# CLAUDE.md

skyfs: a single-writer POSIX filesystem on FUSE whose source of truth is
an immutable write-ahead log in blob storage. Read `docs/design.md` and
`docs/wal-format.md` before structural changes.

## Invariants (never break these)

- **Snapshot + log tail is the source of truth.** State =
  `apply(snapshot, segments after its restore pointer)`. Local metadata and
  caches are derived and rebuildable. The log stays short: restore passes
  fold it into snapshots; it is truncated only at its old end, never
  rewritten.
- **One `apply`.** The live writer and replay use the same function in
  `skyfs-meta`. Local metadata changes only through it.
- **Validate, then log, then apply.** A failing operation returns an errno
  and is never logged. Applying a logged record cannot fail; a failure is
  corruption and refuses the mount.
- **Records are deterministic.** They carry resolved effects (inode numbers,
  offsets, gid, mode after umask, `time_ns`). Replay never reads a clock or
  the environment.
- **Objects are immutable.** Segments are created with `If-None-Match: *`.
  The only mutable object is `volume.json`, a hint never trusted for
  correctness. No correctness path uses `LIST`.
- **Data before reference.** A segment is PUT only after every blob it
  references is fully uploaded. A restore pass writes packs, then the
  metadata file, then the manifest; the manifest PUT moves the pointer.
- **Snapshots contain only committed log.** The restorer applies committed
  segments to its own replica, never the live store or the local tail.
- **Commits are in seq order.** Never complete seq n+1 before n.
- **Fail closed.** Unknown ops, CRC mismatches, broken hash chains and
  identity mismatches (`Remove.ino`, `Rename.replaced_ino`, …) and a
  snapshot whose `restored_digest` does not chain to the log stop the mount. Never
  skip a record.
- **Never overwrite an immutable object.** Only `volume.json` is PUT
  without `If-None-Match: *`; snapshot metadata and packs are
  content-addressed so a retry never needs to overwrite.
- **Chunks are the unit of verification.** An extent points at a whole
  written chunk plus a `skip`; every read verifies the chunk's CRC32C.

## Format rules

- `proto/skyfs/wal/v1/wal.proto` is the schema. Never renumber, reuse or
  retype a tag. A new operation is a new `Record.op` tag. Optional fields
  only if ignoring them yields the same state.
- Update `docs/wal-format.md` in the same change as the proto or codec.
- `proto/skyfs/snapshot/v1/snapshot.proto` follows the same rules.
- Layout changes bump the version byte in `skyfs_format::segment::MAGIC`.

## Boundaries

- Crates: `skyfs → skyfs-volume → skyfs-meta, skyfs-store → skyfs-format`.
  skyfs is standalone (library, daemon or sidecar): it depends on no
  service but the bucket and its STS. No control plane, no lease service;
  single-writer safety is the in-band fence.
- `skyfs-format`: on-bucket schemas and codecs, no I/O.
- `skyfs-meta`: validation (`plan`) and `apply`; no I/O beyond SQLite.
- `skyfs-store::Key` spells every object key; no other module formats one.
  `ObjectClient` is the only code that talks to the bucket.
- `skyfs-volume`: the engine. Platform-independent; its end-to-end tests
  use in-memory buckets.
- FUSE code is Linux-only behind `cfg(target_os = "linux")` in `skyfs`,
  and stays a thin adapter over `skyfs_volume::Volume`.
- Credentials (`skyfs-store::credentials`, design §10.1): OIDC workload
  identity (`AssumeRoleWithWebIdentity`, per-volume session policy),
  the platform's ambient identity, or static keys from a re-read file.
  Never commit `.env` or keys; never log a secret or a token.
- Every bucket retry must be idempotent: a lost response means the
  mutation may have landed. Retry the same bytes (compare on
  `AlreadyExists`) or use content-addressed keys. `tests/faults.rs`
  enumerates every mutation with every fault; extend its workload when
  adding a new kind of object or mutation.

## Commands

```sh
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
```

`libc` constants differ in type between macOS and Linux (`mode_t`), so
check Linux too (Docker command in README, "Develop"). Through the kernel:
`scripts/fuse-smoke.sh`, `scripts/pjdfstest.sh`, `scripts/xfstests.sh`;
real bucket: `scripts/s3-e2e.sh`. Test map and gaps: `docs/testing.md`.
