# skyfs WAL format, version 1

Normative. The schema is [`proto/skyfs/wal/v1/wal.proto`](../proto/skyfs/wal/v1/wal.proto);
the codec is `crates/skyfs-format`. Why the log looks like this is in
[design.md](design.md) §4–6.

## Segment object

Key: `wal/<seq>.seg`, `seq` zero-padded to 20 digits, dense from 0. Written
once with `PUT If-None-Match: *`; never modified.

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `SKYWAL\0` + major version byte (`0x01`) |
| 8 | 8 | `records_len`, u64 little-endian |
| 16 | 8 | `data_len`, u64 LE |
| 24 | 4 | `records_crc32c`: CRC32C of the record region, u32 LE |
| 28 | 4 | reserved, zero |
| 32 | `records_len` | record region |
| 32 + `records_len` | `data_len` | data region |

**Record region:** a varint-length-delimited `SegmentHeader`, then exactly
`record_count` varint-length-delimited `Record`s, then nothing.

**Data region:** raw bytes. `DataRef.inline` offsets are relative to its
start. Absolute object offset = `32 + records_len + inline.offset`, which is
how extents address inline bytes of older segments until a restore pass
moves them into packs.

A reader rebuilding metadata fetches bytes `[0, 32 + records_len)` only.

## Validity of a segment

A reader accepts segment `seq` only if all hold; otherwise the mount fails:

1. Magic matches; the major version is one it supports; reserved bytes are zero.
2. CRC32C of the record region matches.
3. `header.seq` equals the key's seq; `header.volume_id` is the volume's.
4. `header.prev_digest` equals BLAKE3 of segment `seq - 1`'s record region
   (empty for seq 0).
5. `header.first_op_id` equals the previous segment's last op_id + 1, and
   op_ids within the segment increase by exactly 1.
6. Every record has a known `op`. **Unknown ops fail the mount.**
7. `header.epoch` is ≥ the previous segment's; a higher epoch's first record
   is `MountBegin` with that epoch.

## Records

- `op_id`: dense across the volume, starting at 0 (`VolumeCreate`).
- `time_ns`: the writer's clock at the time of the op. Replay uses it for
  every implied timestamp (ctime, mtime, parent directory times). Replay
  never reads a clock, so replay is deterministic.
- Each record is the outcome of an operation the writer validated and
  applied. Records carry the identities they expect (`Remove.ino`,
  `Rename.ino`, `Rename.replaced_ino`). Replay checks them, and
  a mismatch is corruption.
- A record describes the operation's effect, never its request: `O_APPEND`
  offsets are resolved, gid inheritance from setgid directories is
  resolved, umask is applied.

### Data references

- `Write.data` and extents point either **inline** (this segment's data
  region) or to a **blob** (`blobs/<epoch:016x>/<n:016x>`, or
  `packs/<blake3>` in snapshots; keys relative to the volume root).
- **Data before reference:** a segment is PUT only after every blob it
  references has been fully uploaded.
- `crc32c` covers exactly the bytes the record writes. Readers verify it
  before serving or caching them.

### Replay rules beyond the obvious

- `VolumeCreate` creates ino 1, a directory with nlink 2 and the given mode,
  uid and gid.
- `MountBegin` first frees every inode at nlink 0 (open at the previous
  crash), then records the epoch.
- `Remove` of a directory sets nlink 0 and decrements the parent's nlink.
- `Rename` with mode `UNSPECIFIED` onto an existing entry unlinks
  `replaced_ino` exactly as `Remove` would. With `EXCHANGE`, both entries
  swap; nlinks of the parents change if directories cross parents.
- `SetAttr.size` smaller than the current size drops extents beyond it;
  larger adds a hole.
- A `Rename` within one directory keeps the entry's readdir cookie; a
  rename across directories gives it the target's next cookie.
- `SetAttr` sets ctime to the record's `time_ns` unless `ctime_ns` is
  present. An access-time update (relatime, design §9) carries only
  `atime_ns` and the inode's unchanged `ctime_ns`.
- `Reclaim` requires nlink 0.

## Other objects (non-normative here; owned by `skyfs-store::Key`)

| Key | Content | Mutability |
|---|---|---|
| `snapshots/<gen:020>.manifest` | `SnapshotManifest` ([snapshot.proto](../proto/skyfs/snapshot/v1/snapshot.proto)): the restore pointer | immutable, `If-None-Match: *`, written last |
| `snapshots/<gen:020>-<blake3>.meta.zst` | zstd SQLite metadata at the pointer, named by the BLAKE3 of its bytes (also in the manifest) | immutable, content-addressed; a crashed pass leaves an orphan, never an object to overwrite |
| `packs/<blake3 hex>` | inline data repacked by restore | immutable, content-addressed |
| `blobs/<epoch:016x>/<n:016x>` | large write data | immutable |
| `volume.json` | hint `{snapshot_gen, head_seq}` | mutable; never trusted for correctness |

The log holds only the tail after the latest snapshot's `restored_seq`
(plus whatever retention keeps). Segments are deleted from the old end,
never rewritten.

## Evolution

- Never renumber, reuse or retype a field tag.
- A new operation is a new `Record.op` tag. Old readers refuse it.
- A new optional field is allowed only when a reader that ignores it
  computes the same state. Otherwise it is a new op.
- An incompatible layout change bumps the magic's version byte.
- A golden segment (`crates/skyfs-format/tests/golden.rs`) holds one
  record of every op with fixed values and pins its bytes by BLAKE3;
  changing any encoding fails it, and is a format change.

## Replay notes

- Extents of inline writes point at the segment's data region; a reader
  needs that segment's `records_len` to find the bytes, which it has from
  replaying the tail. Snapshots never reference segments (§7 of the design).
- `MountBegin` and `MountEnd` also record, in derived state, whether the
  latest mount ended cleanly, so a mount whose snapshot already covers the
  `MountEnd` still knows.
