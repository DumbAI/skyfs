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

## Format rules

- `proto/skyfs/wal/v1/wal.proto` is the schema. Never renumber, reuse or
  retype a tag. A new operation is a new `Record.op` tag. Optional fields
  only if ignoring them yields the same state.
- Update `docs/wal-format.md` in the same change as the proto or codec.
- `proto/skyfs/snapshot/v1/snapshot.proto` follows the same rules.
- Layout changes bump the version byte in `skyfs_format::segment::MAGIC`.

## Boundaries

- `skyfs-format`: on-bucket schemas and codecs, no I/O.
- `skyfs-store::Key` spells every object key; no other module formats one.
- FUSE code is Linux-only behind `cfg(target_os = "linux")`. Core crates
  stay platform-independent and testable on macOS.
- Credentials come from the environment. Never commit `.env` or keys.

## Commands

```sh
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
```
