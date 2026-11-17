# skyfs design

Status: implemented (v1), 2026-11-16; open items in §12. Supersedes the filesystem parts of
[workspace-storage.md](workspace-storage.md) (§4.2, §5.2–5.4, §5.6, §5.10),
whose object-store comparison (§3) and option survey (§4) still stand.
The log format is specified in [wal-format.md](wal-format.md); the schema
is [`proto/skyfs/wal/v1/wal.proto`](../proto/skyfs/wal/v1/wal.proto).

## 1. Summary

skyfs is a POSIX filesystem whose only durable copy lives in blob storage
(R2, S3). A container mounts it with FUSE. At mount it downloads metadata
only, so every file is listed and `stat`-able at once; file contents are
fetched on first read and cached on local disk. Every change is an
operation appended to an immutable write-ahead log, which is replicated to
blob storage either synchronously or asynchronously, by the user's choice.
One writer mounts a volume at a time.

What blob storage holds is **a snapshot of the filesystem plus a short log
tail**. The snapshot's manifest is the *restore pointer*: the log position
the snapshot reflects. The mounted container applies the log to the
snapshot in the background, at shutdown as far as it can, and the next
container resumes from the pointer. The log only ever holds the last few
minutes of changes, so replaying it at mount takes seconds.

skyfs is a standalone daemon (or sidecar) and depends on no service but
the bucket. It authenticates with OIDC workload identity, exchanging a
platform-issued token for a credential valid for at most an hour and
narrowed to its volume, or, for stores without identity federation, with
static keys from a mounted secret (§10.1).

```
┌─────────────────────────── container (or its host) ───────────────────────────┐
│  app ──syscalls──▶ kernel FUSE ──▶ skyfs daemon                                │
│                                     ├─ metadata: SQLite on local disk          │
│                                     │    = apply(snapshot, log tail)           │
│                                     ├─ open segment: local file, append-only   │
│                                     ├─ data cache: blocks of immutable objects │
│                                     ├─ replicator: seal → PUT, sync or async   │
│                                     └─ restorer: log → new snapshot, async     │
└────────────────────────────────────────┬─────────────────────▲─────────────────┘
          range GETs on read (no egress)  │                     │ PUT If-None-Match: *
┌────────────────────────────────────────▼─────────────────────┴─────────────────┐
│ R2 / S3   volumes/<id>/ snapshots/<gen>.{manifest,meta.zst} · packs/ · blobs/ │
│                         wal/<seq>.seg (tail after the pointer) · volume.json   │
└────────────────────────────────────────────────────────────────────────────────┘
```

## 2. Requirements

| # | Requirement |
|---|---|
| R1 | Data is stored in blob storage (R2, S3). |
| R2 | Containers mount skyfs as a filesystem, fully POSIX-compatible. |
| R3 | Local disk caches data. |
| R4 | Data is fetched on demand, never in full at bootstrap. |
| R5 | Metadata, stat and listings are available immediately at bootstrap; the user sees every file. |
| R6 | One writer at a time; a writer shuts down completely before the next starts. |
| R7 | Writes replicate to blob storage through an immutable, append-only WAL, with synchronous or asynchronous replication chosen by the user. |
| R8 | The WAL schema expresses every POSIX operation. |

Non-goals: several writers on one volume; coherent caching across
machines; Windows or macOS mounts (macOS is a development host only).

## 3. Review of the previous design

[workspace-storage.md](workspace-storage.md) stacked read-only layers
(Nydus for the app image, a read-only skyfs over manifests for data) under
a kernel overlayfs whose upper directory on NVMe took all writes; a daemon
uploaded each *closed* file and appended `path → hash` entries to a JSON
journal every 30–60 s.

| Req | Previous design | Gap |
|---|---|---|
| R1 | Met | — |
| R2 | **Not met.** skyfs was read-only; writes went to overlayfs | Overlayfs needs `CAP_SYS_ADMIN` or a user namespace in a container. It is not full POSIX either: inode numbers change on copy-up, a hard link to a lower file splits on copy-up, renaming a lower directory needs `redirect_dir`, and editing an old file downloads all of it first. |
| R3 | Met | — |
| R4 | Met | — |
| R5 | Met for committed files; files written this session were visible only locally | Met in substance |
| R6 | Met (conditional commits) | — |
| R7 | **Not met.** The journal was a batch of `path → hash` for closed files, every 30–60 s | Not a WAL of operations. No synchronous mode. Open files, appends, in-place edits, renames and directory changes reached blob storage only as their end state, if at all. App state needed a second mechanism (SQLite snapshots). |
| R8 | **Not met.** The schema could express "this path has this content" and whiteouts | No hard links, directory renames, partial writes, truncate, xattrs, special files, ownership, or open-but-unlinked files. |

The read-path ideas carry over: metadata local, data in immutable objects
fetched by range and verified, a cache keyed by immutable object identity,
and conditional PUTs as the commit point. What changes is the write path:
skyfs becomes a writable filesystem in its own right, and the log is the
source of truth.

## 4. Alternatives for the write path

| Option | How | For | Against | Verdict |
|---|---|---|---|---|
| A. Overlay + file journal (previous) | kernel overlayfs, upload closed files | kernel speed for writes | fails R2, R7, R8 (§3) | Rejected |
| B. JuiceFS CE, local SQLite metadata + Litestream | proven POSIX FS; data chunks in S3, metadata WAL shipped by Litestream | full POSIX today | Two logs in two durability domains (data blocks, SQLite WAL) with no common commit point, so no synchronous mode spanning both. Litestream has no synchronous replication. Data is opaque and not deduplicated. | Runner-up: the fallback if skyfs slips |
| C. Block-level WAL (ext4 on a replicated block device: NBD/ublk, ZeroFS-like) | log of 4 KiB block writes | full POSIX for free, from ext4 | Containers cannot attach block devices without privileges. A cold `ls` faults in inode-table blocks one round trip at a time, which fails R5. Files are invisible in the bucket, and blocks cannot be deduplicated or shared. | Rejected |
| D. Logical op log, data in separate content-addressed objects | each write uploads a blob first, then a record references it | dedup at write time | Every small write becomes its own object: two round trips per sync and millions of tiny PUTs. | Rejected as the default; used for large writes (§6.3) |
| **E. Logical op log, small data inline in the segment, large data as blobs** | a segment holds records and the bytes of small writes; it is one PUT | one round trip per commit; one object per batch; metadata readable without data (§6.1); restore folds the log into snapshots | We implement a filesystem: the POSIX state machine, restore, GC | **Chosen** |

E is a log-structured filesystem (Sprite LFS, NOVA) whose log lives in an
object store, with the operation-level records of a database WAL. Its risk
is correctness of the state machine, which §9 addresses by construction
(one `apply` for writer and replay) and by testing.

## 5. The state machine

**Snapshot plus log is the source of truth.** The filesystem state after
op *n* is `apply(snapshot, records after its pointer, up to n)`. Everything
on local disk is a cache of that, rebuildable at any time.

- **Inodes**: `ino → {kind, mode, uid, gid, nlink, size, atime, mtime,
  ctime, rdev, symlink target, xattrs}`. Inode numbers are allocated by the
  writer, stored in the record, never reused, and stable across remounts.
- **Directory entries**: `(parent, name bytes) → ino`, with a per-directory
  insertion cookie so `readdir` offsets survive concurrent changes
  (`telldir`/`seekdir`).
- **Extents**: `ino → ordered [file_offset, len) → (location, chunk,
  skip)`. Holes are absent extents. Overwrites split extents;
  `CloneRange` shares them (reflink). The unit of verification is the
  **chunk**: the bytes of one `Write` (≤ `max_write`, 1 MiB) with the
  record's CRC32C. A CRC cannot be recomputed for part of an extent after
  an overwrite splits it, so every piece keeps pointing at its whole chunk
  plus a `skip`; a reader fetches and verifies the chunk, then serves the
  slice. The restorer gives repacked extents fresh, exact chunks. A
  location is either a segment's data region (live store only) or an
  object key (`blobs/…`, `packs/…`).

**Writer path:** a FUSE request is validated against the current state
(permissions, existence, `ENOTEMPTY`, `EXDEV`, …). If it fails, an errno is
returned and nothing is logged. If it succeeds, the writer builds the
record, appends it to the open segment, applies it with the same `apply`
used by replay, and answers the kernel. Applying a logged record never
fails; a failure on replay means corruption, and the mount is refused.

### 5.1 Metadata store: SQLite, not RocksDB

The same engine serves three roles, so it is chosen for all three at once:
the **live store** behind every FUSE call, the **restore replica** (§7.2),
and the **snapshot format** that a mount downloads and opens.

What skyfs asks of it:
- Point lookups (`lookup`, `getattr`) and ordered range scans (`readdir`,
  the extents of a file) at FUSE latency.
- Small multi-key atomic transactions (a rename touches two entries and an
  inode), from one writer, with concurrent readers.
- No local durability needed: the store is derived and rebuilt after an OS
  crash.
- A state that can be **serialized to one immutable object and opened
  quickly** on another machine, and later uploaded incrementally (§7.5,
  option C).
- Easy offline inspection and queries: fsck, GC reference sets, debugging.

| | **SQLite** (`rusqlite`, bundled) | **RocksDB** (`rust-rocksdb`) |
|---|---|---|
| Structure | B-tree, updated in place; `WITHOUT ROWID` tables are clustered by key, like a sorted KV store | LSM: memtable, then immutable SST files merged by background compaction |
| Reads | One B-tree descent, predictable | Bloom filters plus several levels; fast but with more variance |
| Small writes | Rewrites a 4 KiB page per change, plus its WAL. Fine at FUSE rates | Faster and lower write amplification for heavy random writes |
| Latency under load | No background threads, no write stalls | Compaction can stall writes when it falls behind, a tail-latency risk on the FUSE path |
| Transactions | Full ACID, one writer plus WAL-mode readers: exactly skyfs's model | `WriteBatch` atomic writes; transactions via `TransactionDB` |
| Snapshot to an object | `VACUUM INTO`: one compact file that opens instantly. Measured 0.2 s for 10⁶ files | Checkpoint API: hard-linked SST files plus a MANIFEST, a directory rather than one object |
| Incremental snapshots (§7.5 C) | Changed pages, as Litestream's LTX files do | **Natural:** SSTs are immutable, so upload only new ones (rocksdb-cloud works this way). Compaction outputs count as new, so bytes ≈ changes × levels |
| Lazy metadata for huge volumes | Range-GET VFS, paging the file in on demand | rocksdb-cloud style: fetch SSTs on demand |
| Inspection | `sqlite3 snapshot.db`, SQL for fsck and GC (`SELECT DISTINCT obj FROM extents`) | Custom tools over an encoded key space |
| Schema evolution | Tables and migrations | Hand-versioned key and value encodings |
| Build and operations | C amalgamation, seconds to build, no tuning | Large C++ build (minutes), harder static and musl builds, a larger binary, dozens of tuning knobs (block cache, memtables, compaction style) |
| Used as file system metadata by | JuiceFS's single-node metadata engine; Litestream and LiteFS for replication | Alluxio's metastore, Ceph BlueStore |

**Measured** (1M files: one inode, entry and extent each; Python
`sqlite3` on an Apple-silicon laptop; Rust is faster):

| Operation | Result |
|---|---|
| Create a file as its own transaction (3 statements) | 75,900 ops/s, 13 µs each |
| `lookup` + `getattr` | 223,000/s, 4.5 µs |
| `readdir` of 1,000 entries | 0.3 ms |
| Store size | 163 MB |
| `VACUUM INTO` (snapshot) | 0.2 s, 149 MB |
| zstd -3 | 0.2 s; 13 MB on this synthetic data, real names compress less |

These rates are well above what a FUSE mount delivers (tens of thousands
of operations a second), so SQLite is not the bottleneck. RocksDB's write
advantage would not show; its costs (build, tuning, stalls, a directory as
the snapshot) would.

**Decision: SQLite.** It matches the single-writer model and makes the
snapshot a single file that opens instantly. It also keeps fsck and GC as
SQL. RocksDB's real advantage is incremental snapshots of very large
metadata, which matters only past ~256 MB, the point where §7.5 moves to
option C. SQLite page deltas reach the same place. Other engines
considered:
- **LMDB** (fastest reads, copy-on-write B-tree). Rejected: no SQL, a
  fixed map size, no replication ecosystem.
- **redb** (pure Rust B-tree). Rejected: younger, no tooling.
- **fjall** (Rust LSM). Rejected: young.
- **An in-memory map with our own snapshot format.** Rejected: RAM bounds
  the volume size.

**Revisit if** a benchmark at 10⁷ inodes shows restore passes, or snapshot
deltas built from SQLite pages, are too costly. The store sits behind a
domain-level interface in `skyfs-meta` (`lookup`, `getattr`, `readdir`,
`extents`, `apply(record)`, `export_snapshot`), not a generic KV trait.
Swapping engines then changes one module.

**Settings.** Live store and replica: `journal_mode=WAL`,
`synchronous=OFF`, `mmap_size` sized to the file. `applied_op_id` is
updated in the same transaction as each apply. Tables are `inodes(ino)`,
`dirents(parent, name)`, `extents(ino, offset)` and `xattrs(ino, name)`,
all `WITHOUT ROWID` except `inodes`. Each record is one transaction; a
group-commit window may batch several. After an OS crash the file is
discarded and rebuilt from the snapshot and the log.

## 6. The log

### 6.1 Segments

A segment is one immutable object, `wal/<seq:020>.seg`, written with a
single `PUT … If-None-Match: *`. Layout ([wal-format.md](wal-format.md)):
a 32-byte preamble, then the **record region** (a `SegmentHeader` and the
records, length-delimited protobuf), then the **data region** (bytes of
small writes). Metadata comes first, so rebuilding metadata reads
`32 + records_len` bytes and never touches file data (R5).

- **Ordering:** seq numbers are dense. Each header carries `prev_digest`
  (BLAKE3 of the previous record region), making the log a hash chain;
  a missing or foreign segment is detected, never skipped.
- **Atomicity:** a record is atomic (a rename is one record), and a segment
  is atomic (one PUT). After any crash, the durable state is the state after
  some **prefix** of the operations the daemon received. That is stronger
  than ext4: a later operation is never durable without every earlier one.
- **Size:** a segment is sealed at 16 MiB, at the replication deadline, or
  at a synchronous durability point (§6.4). Writes of up to
  `inline_data_max` (default 1 MiB) go inline.

**Why not multipart for segments:** R2 requires all parts but the last to be
the same size, and its docs do not list `If-None-Match` on
CompleteMultipartUpload ([R2 multipart rules](https://developers.cloudflare.com/r2/objects/multipart-objects/),
[R2 S3 compatibility](https://developers.cloudflare.com/r2/api/s3/api/)).
The commit object is therefore always a single, verifiably conditional PUT,
and large data travels separately.

### 6.2 Local write path

The open segment is an append-only file on local disk (the local WAL).
Inline write bytes go into its data region. Ops are acknowledged to the
kernel only after they are in this file. In async mode, `fdatasync` on the
local file runs at each group commit (every 10 ms by default), so a daemon
crash or container restart on the same disk loses nothing. Reads of
not-yet-uploaded data are served from the local segment files.

A local sync is the platform's `fsync`: `fdatasync` on Linux, `fsync(2)`
on macOS. macOS's `fsync(2)` hands data to the drive without flushing the
drive's cache; `F_FULLFSYNC` does that too, at about 100 times the cost
(4.4 ms against 0.04 ms on an Apple SSD), and Rust's `sync_data` issues
it. skyfs gives an `fsync` caller what every macOS filesystem gives it.
Recovery never depends on synced writes persisting in order: it keeps the
prefix of records whose bytes verify (§6.6), and in async mode every op
reaches the bucket within `max-lag`.

### 6.3 Large writes

A contiguous run of written bytes larger than `inline_data_max` (a
checkpoint, a model, a video) streams to a local blob file and is uploaded
with multipart to `blobs/<epoch:016x>/<n:016x>`. Blob keys are unique per
mount epoch, so they need no conditional write. The rule is **data before
reference**: a segment whose records reference a blob is committed only
after that blob's upload completed. A blob never referenced by a committed
segment is garbage, removed by GC (§8.3).

The writer tracks a sequential run per inode. The first `inline_data_max`
bytes of a run go inline; once a run grows past it, later writes append to
a blob whose key is allocated when it opens, so each `Write` record is
logged at once, in order, pointing at `BlobRange{key, offset}`; nothing is
buffered outside the log. A blob closes when the run breaks, the file is
closed, it reaches `blob_max` (64 MiB), or the segment referencing it
seals; closing starts its upload, and that segment's PUT waits for it.
Sealing closes blobs so that replication lag stays bounded by `max-lag`
plus one blob upload.

**Slow links.** Every bucket request must finish within
`--request-timeout` (default 5 min), body included, or it is retried from
the start. A fixed timeout and fixed parallelism together set the slowest
uplink that still makes progress: in-flight bytes ÷ timeout. Blobs go up
in 8 MiB parts, two per blob and `blob_uploads` (4) blobs at once, at most
64 MiB in flight, so the floor is about 2 Mbit/s. More parallelism would
not be faster on a slow link, only worse: every part would share the link,
none would finish within the timeout, and the upload would retry forever.
A failed multipart upload is aborted before its retry, so its parts are
not left behind. Still, give the bucket a lifecycle rule that aborts
incomplete multipart uploads after a day, for daemons killed mid-upload.

### 6.4 Replication modes (R7)

The mode is chosen per mount (`--replication sync|async`). Both write the
same log; only when the kernel gets its answer differs.

| | `sync` | `async` (default) |
|---|---|---|
| Namespace and attribute ops (create, mkdir, rename, unlink, link, chmod, chown, truncate, setxattr, …) | Return after the segment holding them is committed in blob storage | Return after the local WAL append |
| `write()` | Returns after the local WAL append; durable at the next durability point of the same file or later op | Same |
| `fsync`, `fdatasync`, `syncfs`, `sync`, `O_SYNC`/`O_DSYNC` writes | Return after commit in blob storage | `--fsync remote` (default): after commit in blob storage. `--fsync local`: after the local `fdatasync` |
| `close()` of a file written through this handle | Returns after commit (close-to-open durability) | Returns after the local append |
| Replication lag | 0 for acknowledged durability points | Bounded by `--max-lag` (default 1 s): a segment is sealed and uploaded at most this long after its first record |
| Loss if the machine dies | Nothing acknowledged as durable | Up to `max-lag` of operations; never an `fsync`ed one with `--fsync remote` |

Plain `write()` is not synchronous in either mode, because POSIX defines
`write()` as not durable until `fsync`, and making each FUSE write a round
trip to R2 (20–80 ms) would turn a 20 GB checkpoint into hours. Users who
want each write durable open the file with `O_SYNC`.

Sync mode costs one round trip per namespace operation for a single-threaded
caller: `tar x` of 10,000 files takes 10,000 × ~50 ms. Sync mode suits
workloads where every acknowledged change must survive the machine; bulk
work should use async and call `sync` at the end.

**Group commit:** commits are pipelined. While segment *n* is being PUT,
new records go into *n+1*; synchronous callers waiting on *n+1* are
answered together when it lands. Completions are strictly in seq order.

**Cost:** each sealed segment is one Class A operation ($4.50 per million on
R2). A database calling `fsync` 10 times a second in sync mode produces
about 26 M segments a month (~$117). The daemon exports the commit rate per
mount, and such workloads should run with `--fsync local` or a longer
group-commit window.

### 6.5 Single writer and fencing (R6)

R6 is enforced in band, by the log itself, so nothing outside the bucket
can fork the volume:

1. On mount, the writer finds the head (§7.2) and appends a `MountBegin`
   record with `epoch = previous + 1` as the first record of the next seq.
2. Committing that segment with `If-None-Match: *` claims the seq. A stale
   writer from an older epoch tries to commit the same seq, gets `412`, and
   stops: it must remount and replay, and it learns that it lost its ops
   after the new writer's `MountBegin`.
3. Commits are issued strictly in seq order, so a stale writer can never
   land a later seq after losing an earlier one.

A clean unmount drains the log, commits `MountEnd`, restores as far as it
can within a time budget (§7.4).

Fencing makes two writers *safe*, not useful: each new mount fences the
previous one, whose writes then fail with `EIO`. Running one mount per
volume is the deployment's job, and it is a job orchestrators already do:
a StatefulSet (or a Deployment with the `Recreate` strategy) of one
replica, a systemd unit, a scheduler that places the volume once. skyfs
deliberately runs no lease service of its own; one would have to be
available for every mount and still could not replace the fence, since a
paused process can outlive any lease.

### 6.6 Recovery after a crash

- **Same disk** (container restart): local segments not yet committed are
  valid only if the first one's seq is the remote head + 1 and its epoch is
  the remote's latest epoch. Then they are uploaded in order and the mount
  continues. Otherwise they are kept for inspection and reported as lost.
- **Different machine:** the remote log is the truth. With async replication
  the last ≤ `max-lag` of operations are lost; `MountBegin.previous_clean =
  false` and `lost_after_op_id` record this.
- **Open-but-unlinked files** at crash time (nlink 0, no `Reclaim`) are
  freed when the next `MountBegin` is replayed, like ext4's orphan list.

## 7. Snapshot, restore pointer and a short log (R5)

### 7.1 What blob storage holds

At any moment a volume in blob storage is:

```
snapshots/<g>.manifest           ← restore pointer: "state after segment S, op N"
snapshots/<g>-<blake3>.meta.zst  metadata: inodes, dirents, xattrs, extents
packs/…, blobs/…         file data the snapshot's extents point at
wal/<S+1> … wal/<head>   the log tail: changes since the snapshot
```

- A **snapshot** is the complete filesystem as of the end of segment
  `restored_seq`. Its extents point only at `packs/` and `blobs/`, never at
  segments, so it needs no log to be read.
- The **manifest** (`proto/skyfs/snapshot/v1/snapshot.proto`) carries the
  restore pointer: `restored_seq`, `restored_op_id`, and
  `restored_digest`, the BLAKE3 of that segment's record region. The digest
  ties the snapshot to exactly one log history.
- The **log tail** is the only part replayed at mount. Segments at or below
  the pointer are deleted once no retained snapshot or running mount needs
  them (§8.3). The log is append-only and immutable, and it is short: it
  holds minutes of changes, not the volume's history.
- Generations `g` are dense, and each manifest is written once with
  `If-None-Match: *`, so every object except the `volume.json` hint is
  immutable. Manifests are never deleted (they are tiny), so existence of
  generations stays a dense prefix and probing finds the latest.
- The metadata file is **content-addressed** (its BLAKE3 is in its key and
  in the manifest). A pass that crashed after uploading it leaves an
  orphan, never an object a retry must overwrite: overwriting would let a
  fenced writer replace metadata a newer manifest already points at.

### 7.2 Restore passes

The **restorer** is a background task in the mounted container. A pass
advances the snapshot from pointer `a` to a committed segment `b`:

1. **Apply.** Apply the records of segments `a+1 … b` to the *restore
   replica*, a second local SQLite that holds the snapshot state, using the
   same `apply` as the live mount.
2. **Repack data.** Extents that point into those segments (inline writes)
   are copied into content-addressed `packs/<blake3>` objects (about 32 MiB
   each, `If-None-Match: *`, where an existing pack counts as success), and
   the replica's extents are rewritten to point at them. Bytes come from
   the local cache, which already holds them because this container wrote
   them, or from range GETs. Large-write `blobs/` are referenced as they
   are, never copied. Packs whose live fraction has fallen below 50% are
   rewritten in the same step, which replaces a separate compactor.
3. **Write metadata.** `VACUUM INTO`, zstd, then PUT
   `snapshots/<g+1>-<blake3>.meta.zst` (`If-None-Match: *`; an existing
   object holds these exact bytes).
4. **Commit.** PUT `snapshots/<g+1>.manifest` with `If-None-Match: *`.
   **This PUT moves the restore pointer.** Data comes before metadata, and
   metadata before the manifest, so a crash at any step leaves pointer `g`
   valid. The work lost is at most one pass; uploaded packs are
   content-addressed and get reused.

The restorer reads **only committed segments**, never the local
uncommitted tail. A snapshot therefore never contains an operation the log
could still lose, and any machine replaying the same segments would
produce the same snapshot.

**Triggers** keep the tail short, so mount replay stays fast:

| Trigger | Default |
|---|---|
| Time since the oldest unrestored segment | 5 min |
| Unrestored record bytes | 64 MiB |
| Unrestored segments | 512 |
| Clean shutdown | always, best effort (§7.4) |

A pass folds **at most 256 segments** (`restore_batch`) and publishes a
snapshot; a longer backlog (a long tail replayed at mount, a slow
uplink, a restorer that was off) continues with the next pass at once.
Each step is durable, so a mount that ends early, or a shutdown restore
that runs out of time, keeps the progress it made instead of abandoning
one oversized pass forever. The shutdown restore also retries transient
bucket errors until its deadline. xfstests found this: short-lived mounts
of a volume with a 1,300-segment tail never finished a pass, so the tail
and every mount's replay grew without bound.

Passes run at low priority with a bandwidth cap, so they never compete with
the app for the network. **Cost:** each pass rewrites the whole metadata
file, which is ~10 MB at 10⁵ inodes, so a pass every few minutes is cheap.
At 10⁷ inodes (~1 GB) passes must be rarer. The next step at that size is
incremental snapshots (only changed SQLite pages, as Litestream's LTX
files do). Not in v1.

**Why a separate replica, not a dump of the live metadata:** the live
store runs ahead of the committed log in async mode, and contains ops that
might still be lost. Dumping it at exactly a committed boundary would need
a consistent read at that point while writes continue. The replica costs a
second metadata-only apply, cheap next to the network, and makes a snapshot
a pure function of committed segments.

### 7.3 Mount: snapshot, then the tail

1. Read `volume.json` (hint only).
2. Find the latest snapshot: `HEAD snapshots/<g>.manifest` forward from the
   hint until `404`. Download the manifest and its metadata (megabytes for
   ~10⁵ files). It seeds both the live store and the restore replica.
3. Find the log head: `HEAD wal/<seq>` forward from `restored_seq + 1`
   (galloping, then binary search). No `LIST`. Probing finds the *first*
   missing seq, so a hole in the middle of the log would look like its
   end, and the next `MountBegin` would fork history into it. The mount
   fails closed instead if `volume.json` records a later head or any of
   the next 16 seqs exists.
4. Check continuity: segment `restored_seq + 1` must carry `prev_digest ==
   restored_digest`. Otherwise the snapshot belongs to another history and
   the mount is refused.
5. Replay the tail into the live store: one range GET per segment fetches
   its record region only, in parallel, verifying the hash chain. The tail
   is minutes of changes, so this takes seconds.
6. Recover this disk's uncommitted segments if they continue the remote
   log (§6.6): their blobs are uploaded first, then the segments in order.
7. Commit `MountBegin` (`base_snapshot_gen = g`) and mount. Whether the
   previous mount ended cleanly comes from the last tail record, or, when
   the snapshot already covers it, from state that `apply` keeps. `ls -l`,
   `stat`, `find` and `os.walk` are answered locally (R5); no file data has
   been fetched (R4).
8. The restorer resumes in the background from `restored_seq`. The tail it
   inherited is restored first, re-using the records fetched in step 5.

### 7.4 Shutdown: drain, then restore best effort

1. Stop taking operations. Seal and commit every segment, then `MountEnd`.
   This step is required: it is what makes the data durable.
2. Run restore passes until the pointer reaches the head or
   `--restore-on-exit-timeout` (default 60 s) expires. On timeout the pass
   in flight is abandoned: nothing half-done is visible, because the
   manifest is written last.
3. Update the `volume.json` hint, exit.

If the container is killed during step 2, nothing is lost. The log is
complete, and the next container replays a longer tail, then restores it
in the background. Between mounts the tail can only stay the same, since
no one writes; an offline restore job (the same code, mounted as the writer)
can shorten it for volumes left with a long tail.

**Scale limit:** mount time is the snapshot metadata download plus the tail
replay. Up to a few million inodes that takes seconds. Beyond that, the
next step is to open the snapshot metadata lazily through a range-GET SQLite
VFS, paging it in on demand. Not in v1.

### 7.5 Full snapshots or a compacted log?

Three ways to keep replay short. They differ in what a mount downloads and
in what each restore pass rewrites:

| | **A. Full snapshot + short tail** (chosen for v1) | **B. Compacted log only** | **C. Base image + compacted deltas + tail** |
|---|---|---|---|
| What it is | The whole state at a pointer, then raw segments | Older segments merged into bigger ones with superseded entries dropped; no base image | A rare full image, levelled delta files of changed state, then raw segments |
| Precedent | etcd and Raft snapshots, ZooKeeper, Delta Lake checkpoints | Kafka log compaction, Delta Lake's `x.y.compact.json` files ([docs](https://docs.delta.io/optimizations-oss/)) | Litestream 0.5: L1 30 s, L2 5 min, L3 1 h, daily snapshot ([how it works](https://litestream.io/how-it-works/), [v0.5](https://fly.io/blog/litestream-v050-is-here/)); Neon image and delta layers ([GetPage@LSN](https://neon.com/blog/get-page-at-lsn)) |
| Mount downloads | One metadata file plus a minutes-long tail | Every compacted level, merged and applied: at least the state's size, as records | One image, a few deltas, the tail |
| Mount work | Open a ready SQLite file | Apply records equal to the whole state | Open the image, apply a few deltas |
| Cost of a restore pass | Rewrites **all** metadata, O(state) | O(changes × levels) | O(changes × levels), plus an occasional full image |
| Fits filesystem ops? | Yes: the snapshot is state | **Poorly.** Ops are not keyed records: `Rename`, `Link` and `Remove` interact across inodes and directory entries. To compact them we must first turn them into state upserts and tombstones, which is C's delta format without its base image. | Yes: deltas are state, keyed by inode, entry, extent and xattr |
| Moving parts | One format; one commit (manifest) | Op-to-state conversion, level merging, tombstone retention | Delta format, merge order, level policy, image cadence |
| Point-in-time restore | Retained snapshot + segments | Retained levels + segments | Retained images and deltas + segments |
| Data (packs, blobs) | Repacked in passes | Same | Same |

Data handling is identical in all three: inline bytes move to packs in the
same way. The choice affects only metadata.

**Where A breaks.** A pass rewrites the whole metadata file. At ~100 bytes
per inode compressed:

| Inodes | Metadata | Pass every 5 min | Pass every hour |
|---|---|---|---|
| 10⁵ | ~10 MB | ~2.9 GB/day uploaded | ~0.24 GB/day |
| 10⁶ | ~100 MB | ~29 GB/day | ~2.4 GB/day |
| 10⁷ | ~1 GB | ~290 GB/day: too much | ~24 GB/day, but the tail grows to an hour |

The ~100 bytes per inode is conservative: §5.1 measured 149 bytes
uncompressed and 13 compressed on synthetic names, so real uploads should
fall between the two. R2 does not bill ingress, and uploaded snapshots replace each other, so the
cost at 10⁵–10⁶ inodes is bandwidth and CPU in the container. At 10⁷,
keeping the tail short and keeping passes cheap pull against each other.

**Decision.**
- **v1: A.** It has the fastest mount, the fewest formats and the simplest
  proof: a snapshot is the replica after applying committed segments,
  serialized. The volumes we expect (app workspaces, models, outputs) hold
  10⁴–10⁶ inodes.
- **Not B.** For a filesystem, a compacted op log is either C without the
  image or a slower A. It has no advantage over C on any axis that matters
  here.
- **Upgrade path: C, when metadata passes ~256 MB.** The restore replica
  is SQLite, so the cheapest delta is page-level: the pages a pass changed,
  levelled the way Litestream 0.5 levels its LTX files, with a full image
  daily. The manifest then lists an image plus deltas instead of one meta
  file. Mounting, the pointer, the commit order and GC stay as they are.
  Listing deltas is an additive manifest field; v1 writes only full images.

## 8. Data

### 8.1 Read path (R3, R4)

A read maps `[offset, len)` to extents, then each extent to its chunk
(§5). Chunks come, cheapest first, from the local WAL (uncommitted
segments and blobs), from committed local files the cache adopted, from
cached chunks, or by range GET; all of a read's chunks are fetched in
parallel and the kernel's read-ahead keeps several reads in flight. Every
chunk is verified against its CRC32C before it is served or cached, from
any source. The cache key is `(object key, offset, length)`. Objects are
immutable, so the cache never goes stale and survives remounts. Holes
read as zeros without I/O. (Coalescing adjacent small chunks into one GET
and 16–64-stream read-ahead for cold sequential reads are open
optimizations, measured by the 8 GB model benchmark below.)
`FOPEN_KEEP_CACHE` keeps the page cache across opens. On Linux 6.9+, a file
whose data is entirely in the local cache can be opened in FUSE passthrough
mode.

The local disk holds three things with different eviction rules:

| Area | Contents | Eviction |
|---|---|---|
| `meta/` | SQLite metadata store | never (derived; rebuilt if lost) |
| `wal/` | open and uncommitted segments (`<seq>.rec` framed records, `<seq>.dat` data region) and blobs being uploaded | **never** until committed. A full disk returns `ENOSPC` |
| `cache/` | blocks of committed objects | LRU, with a size limit |

After commit, a local segment or blob file is moved into the cache rather
than copied.

### 8.2 Compaction

There is no separate compactor. Restore passes repack inline data into
packs and rewrite sparse packs (§7.2). The log itself is never rewritten.

### 8.3 Garbage collection, log truncation and retention

- **Retained snapshots:** the latest, plus any within the retention window
  (default 7 days). A retained snapshot plus the segments after its pointer
  can rebuild the state at any later op, which gives point-in-time restore
  and read-only mounts of the past.
- **Log truncation:** segment `s` is deleted when `s ≤ restored_seq` of the
  oldest retained snapshot *and* `s ≤` the `restored_seq` of the running
  mount's `base_snapshot_gen`. A running mount's extents may still point
  into its tail segments; the next mount starts from a newer snapshot.
  Without retention the log holds only the tail.
- **Packs and blobs** are deleted when no retained snapshot and no
  running mount can reference them, **without LIST**. After each pass the
  restorer diffs the replica's referenced keys against the previous pass
  and records newly unreferenced ones in a `garbage(key, gen, time)` table
  inside the replica, so the record travels with the snapshot. A key
  becomes deletable once it was dropped before the retention cutoff (every
  snapshot that referenced it has expired) and at a generation no later
  than the running mount's base (the base does not reference it). A key
  referenced again (a pack with identical content) leaves the table. Live
  extents never reference anything the replica dropped: the replica
  applies a prefix of the same log, and a dropped key is never referenced
  again except by content-addressed re-creation, handled above.
- Blobs orphaned by a crash between upload and commit, and metadata files
  of crashed passes, are only findable by `LIST`; an offline sweep may
  list for them. No correctness path depends on a listing.

## 9. POSIX coverage (R2, R8)

| Operation | Record | Notes |
|---|---|---|
| `creat`, `open(O_CREAT)`, `mkdir`, `mknod`, `mkfifo`, `symlink`, `bind(AF_UNIX)` | `Make` | kind, mode after umask, uid/gid resolved (setgid dirs), rdev, target |
| `open(O_TRUNC)`, `truncate`, `ftruncate` | `SetAttr{size}` | |
| `link` | `Link` | hard links are first-class |
| `unlink`, `rmdir` | `Remove` | nlink 0 + open → kept until `Reclaim` |
| `rename`, `renameat2` (`NOREPLACE`, `EXCHANGE`) | `Rename` | atomic, including directories and replace |
| `write`, `pwrite`, `writev`, `O_APPEND`, mmap writeback | `Write` | offset resolved by the writer; data inline or blob |
| `fallocate`, `posix_fallocate` (incl. punch hole, zero, collapse, insert) | `Fallocate` | |
| `copy_file_range`, `FICLONE`, `FICLONERANGE` | `CloneRange` | shares extents, no copy |
| `chmod`, `chown`, `utimensat`, set-id clearing | `SetAttr` / `Write.cleared_setid` | |
| `setxattr`, `removexattr`, POSIX ACLs | `SetXattr`, `RemoveXattr` | ACLs via `FUSE_POSIX_ACL` |
| last close of an unlinked file | `Reclaim` | |
| `fsync`, `fdatasync`, `syncfs`, `sync` | none | durability point (§6.4) |
| `read`, `readdir`, `stat`, `open` without create/trunc, `access` | none | |
| `flock`, `fcntl` locks, leases | none | local to the single mount; POSIX locks never survive a crash |
| `open_by_handle_at`, NFS export | none | handles stay valid across inode eviction and remounts: inode numbers are never reused, and `lookup(ino, ".")` finds any live inode |
| atime | `SetAttr{atime, ctime unchanged}` | `--atime relatime` (default), `noatime` or `strictatime`. Recorded at open for reading, read, `readdir` and `readlink`: the kernel serves cached reads without asking skyfs, so the open is where a read is sure to be seen; the daemon then invalidates the kernel's cached attributes. Rides in the next segment; nobody waits for it |
| `inotify`/`fanotify` | none | the kernel raises events for changes made through the mount, the only writer |

### 9.1 What mountpoint-s3 cannot do, and how skyfs logs and replays it

mountpoint-s3 maps one file to one object, so any change that is not
"write a new object from start to end" is either refused or emulated
([SEMANTICS.md](https://github.com/awslabs/mountpoint-s3/blob/main/doc/SEMANTICS.md)).
skyfs never maps a file to an object. A file is an inode with a list of
extents, and every change is a small record appended to the log, so none of
these operations rewrites existing objects. Replaying the records restores
the exact state, on any machine, from the bucket alone.

| Operation | mountpoint-s3 | skyfs record | What replay does | Bytes uploaded |
|---|---|---|---|---|
| Write at a random offset, partial write | refused (sequential only) | `Write{ino, offset, len}` | splits the extent list; new bytes cover `[offset, +len)` | only `len` |
| In-place edit of an existing file (`O_RDWR`, no `O_TRUNC`) | refused | `Write` | same | only the edited bytes |
| Append | S3 Express directory buckets only | `Write{offset = old size}` | adds an extent, grows size | only the appended bytes |
| Overwrite with `O_TRUNC` | needs `--allow-overwrite`; uploads the whole file again | `SetAttr{size: 0}` + `Write`s | drops old extents, adds new | the new content |
| `truncate` / `ftruncate`, sparse files | not supported (beyond `O_TRUNC`) | `SetAttr{size}`, `Fallocate{PUNCH_HOLE}` | drops extents past size or in the hole; holes read as zeros | none |
| Read and write the same file at once | `EPERM` | no record for reads | n/a | n/a |
| Write after `fsync` | refused | `Write` | same | only new bytes |
| Rename a file | S3 Express only; otherwise copy + delete | `Rename` | moves one directory entry; extents untouched | none |
| Rename a directory, any size | not supported | `Rename` | moves one directory entry; the subtree follows | none |
| Replace by rename (write `tmp`, `rename(tmp, f)`), `RENAME_NOREPLACE`, `RENAME_EXCHANGE` | partial; exchange unsupported | `Rename{mode, replaced_ino}` | atomic swap or replace; replaced inode loses a link | none |
| `rmdir` / remove an existing directory | only new local ones | `Remove{is_dir}` | removes the entry | none |
| Hard links, symlinks | not supported | `Link`, `Make{SYMLINK}` | entry → existing inode; symlink inode | none |
| `chmod`, `chown`, `utimensat` | not supported (mount-wide modes) | `SetAttr` | updates the inode | none |
| xattrs, ACLs | not supported | `SetXattr`, `RemoveXattr` | updates the inode | value only |
| Unlink while open | delete is immediate; readers fail later | `Remove`, later `Reclaim` | nlink 0, data kept until last close | none |
| FIFOs, sockets, device nodes | not covered | `Make{kind, rdev}` | creates the inode | none |
| `fallocate`, `copy_file_range`, reflink | not supported | `Fallocate`, `CloneRange` | adjusts or shares extents | none |
| `fsync` on a directory, `flock`/`fcntl` locks | not supported | none needed | log order already makes directory ops durable in order; locks are local to the single mount | n/a |

Worked example: an editor saves `notes.txt` safely, and a training job
appends to a log and renames its output directory.

```
op 100  Make     {parent 1, name ".notes.txt.swp", ino 40, REGULAR, 0644}
op 101  Write    {ino 40, offset 0, len 2048, inline @0}
op 102  Rename   {old 1/".notes.txt.swp" → new 1/"notes.txt", ino 40, replaced_ino 12}
op 103  Write    {ino 17, offset 1048576, len 512, inline @2048}     # append to train.log
op 104  Write    {ino 18, offset 4096, len 8, inline @2560}          # patch 8 bytes in place
op 105  Rename   {old 1/"run-tmp" → new 1/"run-17", ino 30}          # directory, 10k files inside
op 106  Remove   {parent 1, name "old.ckpt", ino 21}
```

The segment holding these is about 3 KB of data plus a few hundred bytes
of records. Replay on a fresh machine applies the seven records to the
snapshot's metadata: `notes.txt` now resolves to ino 40, and ino 12 is
freed. `train.log` gains one 512-byte extent. Ino 18 has an 8-byte extent
overlaying its old data. `run-17` holds the same 10k entries, none of them
touched. With mountpoint-s3, the same session would mean re-uploading
`notes.txt`, which is allowed; failing the append and the 8-byte patch;
and copying and deleting 10k objects for the directory rename, which is
not supported at all.

**Restoring state from the bucket alone.** Every input to replay is in blob
storage: the snapshot, the log tail, packs and blobs. So:
- any machine can rebuild the volume (§7.2);
- the state at any retained `op_id` can be rebuilt, which gives
  point-in-time restore and read-only mounts of the past;
- `skyfs fsck` can replay a volume offline and check every invariant.

**Trade-off: files are not plain objects at their paths.** Bytes live in
packs and blobs addressed by extents, not at `bucket/path/to/file`.
That is what makes rename, append and partial writes cheap. If other tools
need a path-per-object view, `skyfs export --at <op_id> s3://dest/` replays
the log and writes each file as one object. It is a one-way snapshot, not
the live format: keeping a live path-per-object mirror would bring back
copy-and-delete on every rename. Planned after v1.

**Ordering caveat:** with FUSE writeback caching, the kernel may hand dirty
pages to the daemon after a later namespace operation, for example
`write; rename` without `close` or `fsync` in between. Data written before
`close()` (the kernel flushes on close) or `fsync()` is always ordered
before later operations. This is the same contract as ext4 without
`auto_da_alloc`.

**Conformance:** pjdfstest and the generic xfstests (on a Linux CI runner)
are the bar for "full POSIX". Kernel FUSE has a few limits of its own,
which we document rather than work around: `st_dev` is synthetic, `O_DIRECT`
goes through the daemon, and device nodes are inert under `nodev`.

## 10. Mounting in containers (R2)

| Environment | How |
|---|---|
| Docker / any runtime with a device and a capability | `--device /dev/fuse --cap-add SYS_ADMIN`, skyfs runs inside the container |
| Unprivileged container, privileged host (Kubernetes, our own VMs) | skyfs runs on the host or in a privileged sidecar; the mount is bind-propagated into the app container. Later a CSI driver. |
| No `/dev/fuse` at all (RunPod pods) | No mount possible. A degraded "sync a directory" mode can reuse the same log format; out of scope for v1. |
| macOS (a developer's laptop) | `--transport nfs` (the default there): the daemon serves NFSv3 on localhost and the system's NFS client mounts it, as the user, with nothing installed (§10.2). |

`mount -t fuse.skyfs VOLUME_ID DIR -o store=...` and `/etc/fstab` work
through `scripts/mount.fuse.skyfs` (installed as `/sbin/mount.fuse.skyfs`):
it maps `key=value` options onto `skyfs mount` flags, detaches the daemon
and returns once the mount is up. `mount -o remount,ro|rw` changes the
existing mount's flags (a bind remount: FUSE refuses superblock
reconfiguration). `-o ro` / `--read-only` mounts serve the volume as of
mount time and write nothing: no epoch, no log, no restore. A daemon holds an exclusive lock on its
cache directory, and a second one waits for it (a remount racing the
previous unmount). Device nodes and set-id bits are ignored unless the
mount asks for `--dev`/`--suid`, as for any FUSE mount.

The daemon runs as root (or with `CAP_CHOWN`/`CAP_FOWNER`) so ownership is
real. Permission checks use the kernel's `default_permissions`, plus ACLs.
uid/gid are stored numerically; containers with user namespaces see the
mapped ids.

### 10.1 Authentication and credentials

A mount reads and writes its volume in blob storage, so it needs a
credential. skyfs is a library and a standalone daemon or sidecar: it
talks to the bucket and, for OIDC, to the bucket's STS, and to nothing
else. There is no skyfs control plane.

**Requirements.**

| # | Requirement |
|---|---|
| C1 | No skyfs service to run: a mount needs only the bucket (and its STS), so nothing else can stop it. |
| C2 | Prefer no stored secret at all: an identity the platform issues and rotates, exchanged for a credential that expires within an hour. |
| C3 | Least privilege: the daemon's credential reaches one volume's prefix, read-only for read-only mounts, where the store can express it. |
| C4 | Refresh in place: a mount runs for days without remounting; a rotated secret is picked up without a restart. |
| C5 | Attributable: the store's audit log names the mount. |
| C6 | Works on S3 and on stores without identity federation (R2). |

**What the stores offer** (November 2026).

| Store | Federation | Short-lived credential without a stored secret |
|---|---|---|
| AWS S3 | STS `AssumeRoleWithWebIdentity` trusts any OIDC issuer registered as an IAM identity provider: EKS natively; any other Kubernetes cluster whose service-account issuer and JWKS are publicly reachable (e.g. published to a bucket); GitHub Actions; SPIFFE/SPIRE JWT-SVIDs. EKS Pod Identity, ECS task roles and instance roles through the AWS chain. | Yes |
| MinIO, Ceph RGW, SeaweedFS | `AssumeRoleWithWebIdentity` at their own STS endpoint | Yes |
| Backblaze B2 | OIDC workload identity at its STS | Yes |
| Cloudflare R2 | None. Temporary credentials exist, but are minted from a parent API token, which is itself a stored secret | No: static, bucket-scoped API token |

**Options.**

| Option | For | Against | Verdict |
|---|---|---|---|
| A. OIDC workload identity, exchanged by skyfs (`--credentials oidc`) | no secret anywhere; the platform rotates the token; skyfs adds a per-volume session policy and names the session | needs a federated store and a public issuer | **Preferred** |
| B. The platform's ambient identity (`--credentials ambient`) | no secret; EKS Pod Identity, ECS, EC2 need nothing else | AWS only; scope is the role's | **Supported** |
| C. Static keys from a mounted secret (`--credentials static`) | works everywhere, including R2 | a stored secret; rotation is the operator's job | **Supported**, for stores without federation; scope the key to one bucket (an R2 token can be) |
| D. A skyfs credential broker and lease service | one story for R2 and S3, per-volume R2 tokens | a service every mount depends on (fails C1); it holds parent keys; the single-writer lease it offered is already enforced in band (§6.5) | **Rejected** (built, then removed) |
| E. Presigned URLs | the container holds nothing reusable | a signer on the data path; conditional-PUT headers must be signed in | Rejected |

Running skyfs on the host or in a privileged sidecar (§10) composes with
all of them: the application container receives the mount and no
credential.

**Design.** One interface in `skyfs-store::credentials`: a refreshing
provider plugged into the S3 client. It caches one credential, fetches
the next when less than min(5 min, ¼ of its lifetime) remains (undated
ones every 5 minutes), and on a failed refresh keeps the current one
until it truly expires.

| `--credentials` | Source | Typical deployment |
|---|---|---|
| `oidc` | `AssumeRoleWithWebIdentity` with the token in `--web-identity-token-file`, re-read on every exchange; `--role-arn`; `--sts-endpoint` for non-AWS STS. The flags default to `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ENDPOINT_URL_STS`, which the EKS webhook already injects | Kubernetes anywhere with S3, MinIO, Ceph, B2 |
| `ambient` (default) | the AWS chain: EKS Pod Identity / ECS (`AWS_CONTAINER_CREDENTIALS_*`), IRSA, instance roles. Refuses `AWS_ACCESS_KEY_ID` without a session token, so static keys are never used by accident | EKS, ECS, EC2 |
| `static` | `--credentials-file`, AWS `credential_process` JSON, re-read so a rotated Kubernetes Secret is picked up; or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | R2; development |

**OIDC exchange.** The request is unsigned; the JWT is the proof:

```
POST https://sts.<region>.amazonaws.com/
Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn=...
&RoleSessionName=<writer id>&WebIdentityToken=<JWT>&DurationSeconds=3600
&Policy=<session policy>
```

On Kubernetes the token is a projected service-account token with the
audience the IAM identity provider expects (`sts.amazonaws.com`),
rotated in place by the kubelet. The role's trust policy pins the
issuer, the audience and the `sub` claim
(`system:serviceaccount:<namespace>:<name>`), so only that service
account can assume it.

**Session policy.** Unless `--no-session-policy` (for an STS that rejects
`Policy`), skyfs narrows each credential to its volume:

```json
{"Version": "2012-10-17", "Statement": [
  {"Effect": "Allow",
   "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
   "Resource": "arn:aws:s3:::BUCKET/PREFIX/volumes/VOLUME_ID/*"},
  {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::BUCKET",
   "Condition": {"StringLike": {"s3:prefix": "PREFIX/volumes/VOLUME_ID/*"}}}]}
```

Read-only mounts, `fsck` and `info` get `GetObject` and `ListBucket` only.
`s3:ListBucket` on the prefix is required even though skyfs never lists
on a correctness path: without it, S3 answers `HEAD` of a missing key
with 403 instead of 404, and head probing (§7.3) cannot tell "absent"
from "forbidden". The session policy bounds what the *daemon* can do,
which limits the damage of a bug; it is not a boundary against a
compromised workload, which holds the token and can call STS without it.
Isolation between tenants is the role's policy: one role per service
account, allowed only that tenant's prefix.

**Inside the container.** Credentials live in the daemon's memory only and
are never logged (debug output shows the key id; an STS error shows its
code and message, never the token). Token and key files sit on a tmpfs
readable only by the daemon.

**Failure behaviour.** A refresh failure keeps the current credential
until it expires. After that, object requests fail and the existing retry
loops take over, exactly as when the bucket is unreachable (§11): writes
continue into the local WAL until `wal/` is full, cached reads keep
working, and sync-mode durability points wait. An STS outage shorter than
the credential lifetime is invisible.

**What an attacker gets** by stealing everything in the container. With
`oidc`: the current credential (one volume, at most an hour) and the
token (minutes of validity, one audience), so the role's permissions
until the token expires. With `static`: the key, until an operator
rotates it, which is why `static` is for stores without federation.

### 10.2 macOS: the NFS transport

Linux and FUSE are the production path. On macOS, FUSE needs either a
kernel extension (macFUSE; on Apple Silicon only after rebooting into
Reduced Security) or a third-party transport. FUSE-T, tried with skyfs,
mounts but intermittently loses requests, most likely because the Rust
FUSE library reads FUSE-T's stream socket as if it were `/dev/fuse` (one
request per `read`). Instead skyfs uses the one network filesystem client
every Mac already has.

```
app ──syscalls──► macOS NFS client ──NFSv3, TCP 127.0.0.1:<port>──► skyfs daemon ──► Volume ──► S3 / R2
```

`skyfs mount --transport nfs` binds an NFSv3 server to `127.0.0.1` on a
free port, mounts it with `mount_nfs` (as the user, no root), and serves
until a signal or a `umount`. The server is a thin adapter over `Volume`,
like the FUSE one (`crates/skyfs/src/nfs.rs`), on `nessie-nfsserve` (a
vendored fork of Hugging Face's `nfsserve`, with one fix:
`vendor/nessie-nfsserve/PATCHES.md`). It also runs on Linux (root,
`nfs-common`), which is how CI tests it there.

| Concern | Choice |
|---|---|
| Library | `nessie-nfsserve`, the one Rust NFSv3 server that honours `UNSTABLE` writes and `COMMIT`; the others answer every write `FILE_SYNC`, which skyfs could only keep with a sync per write. Same 1 MiB transfers and per-RPC concurrency as the rest. |
| Writes | Answered `UNSTABLE`; the client batches them and sends `COMMIT`, which is `Volume::fsync`. The write verifier changes at every daemon start, so the client resends what it had not seen committed. |
| `--fsync` default | `local`: the macOS client commits at every `close`, not only at `fsync`, so `remote` would make each close a bucket round trip. `--fsync remote` is still available. |
| Open files | NFS has no open/close. A write handle opens at a file's first write and is released at `COMMIT`, at remove (so an unlinked file is freed at once), or after 5 s idle. Unlink-while-open is the client's `.nfsXXXX` rename. |
| File handles | Volume id prefix + inode number, no boot generation: inode numbers belong to the volume, so open files survive a daemon restart with same-disk recovery, and a handle from another volume is `STALE`. |
| Directory listing | skyfs's stable cookies are the NFS cookies (hard links make the fileid unsuitable). |
| Mount | NFSv3 over TCP, hard, `intr`, `locallocks` (locks stay in the kernel: one writer), 1 MiB `rsize`/`wsize`, `deadtimeout=60` (a dead daemon's mount is removed after a minute instead of hanging Finder), `nodev`, `nosuid`. |
| Identity | Creates take the caller's uid/gid from AUTH_UNIX; symlinks and other credential-less creates get the daemon's user. Permissions are not enforced between users: it is a single-user transport for a laptop. Any local process can reach the port. |
| Locks | `flock` and `fcntl` locks work between processes on the Mac: the kernel keeps them (`locallocks`), which is right for a single writer. |
| Not supported | xattrs (not in NFSv3; macOS keeps them in `._` files), device nodes and FIFOs (no MKNOD), `fallocate` and `copy_file_range` (not in NFSv3; the client emulates copies). |

Measured on an Apple Silicon MacBook (release build, local bucket): 512 MiB
sequential write 1.2 GB/s, cold read on an empty cache 1.4 GB/s, 2000
1 KiB files created in 4.4 s (2.2 ms each), `ls -l` of them 0.2 s, `git
add` and `commit` of them 3 s. Against S3 (us-east-2) the mount takes
2.8 s and a remount on an empty cache 2.6 s.

## 11. Guarantees and failure cases

| Event | Result |
|---|---|
| Daemon or container crash, same disk | No loss of acknowledged ops: the local WAL is recovered and uploaded (§6.6) |
| Machine lost, `sync` | No loss of anything acknowledged as durable |
| Machine lost, `async` | Loss of ≤ `max-lag` of ops, reported in the next `MountBegin`; `fsync`ed data kept with `--fsync remote` |
| Crash at any point | Durable state is a prefix of the operation sequence; never a mixture |
| Stale writer after a new mount | Its commits fail with `412`; the new writer's history wins |
| Corrupt or truncated object | Detected (preamble CRC32C, hash chain, per-write CRC32C, R2 CRC64NVME on upload); never served; mount refused if in the log |
| Unknown record type (newer writer) | Mount refused; never skipped |
| Killed during a restore pass or the shutdown restore | Nothing lost; the pointer stays at the last manifest; the next mount replays a longer tail and resumes restoring |
| Restorer falls behind (offline, slow uplink) | Still correct: mount replays a longer tail. The tail length is exported as a metric |
| Snapshot from another history (restored volume, bug) | `prev_digest` of segment `restored_seq + 1` ≠ `restored_digest`; mount refused |
| Uplink slower than about 2 Mbit/s | Uploads time out and retry without progress; raise `--request-timeout` (§6.3) |
| Blob storage unreachable | Cached reads work; uncached reads `EIO`; writes continue locally until `wal/` is full, then block; sync-mode durability points block |
| Credential refresh fails (STS down, token file missing) | The current credential is used until it expires (≤ 1 h); then as "blob storage unreachable" |
| Bucket unreachable at mount | Mount fails after `mount_timeout` (5 min) if `MountBegin` cannot commit; nothing is claimed |
| Bucket unreachable at unmount | The unmount waits; a second SIGINT/SIGTERM abandons it, keeping the local WAL for the next mount on this disk |
| A PUT lands but its response is lost | Segments, packs and snapshot metadata are compared or content-addressed; a manifest is retried byte for byte, so the retry recognises its own object |
| Power loss tears the local WAL | Recovery keeps the prefix of records whose frames, inline bytes and blob chunks all verify |
| NFS transport: the daemon dies | Calls on the mount fail with `EINTR` when interrupted; after `deadtimeout` (60 s) macOS removes the mount. A restart on the same cache recovers the local WAL, and the client resends uncommitted writes (the verifier changed) |
| Two daemons on one cache directory | The second waits for the first's lock (`local_lock_wait`), then fails |
| A segment deleted from the middle of the log | Mount refused: the head probe would stop at the hole, so it checks the hint and the next 16 seqs (§7.3) |

## 12. Build order and what settles the open questions

Crates (dependencies point one way):
`skyfs → skyfs-volume → skyfs-meta, skyfs-store → skyfs-format`.
The engine lives in `skyfs-volume`, not in the Linux-only binary, so the
replicator, mount, recovery and restorer are tested on any platform
against in-memory and local-directory buckets; `skyfs` is the CLI and a
thin FUSE adapter.

1. `skyfs-format`: log and snapshot-manifest schemas, segment codec. **Done.**
2. `skyfs-meta`: state machine and `apply`, validation (`plan`), SQLite
   store, snapshot export, fsck. **Done.** Property test: random op
   sequences applied live vs replayed from encoded segments give identical
   state, pass fsck, and match a byte-level reference model. One scenario
   test per row of §9.1, plus snapshot-plus-tail equals full replay.
3. `skyfs-store`: key layout, object client (create-only PUT, range GET,
   multipart for blobs), credentials (§10.1), chunk cache. **Done.**
4. `skyfs-volume`: local WAL, replicator with group commit, blobs, mount,
   same-disk recovery, fencing. **Done.** End-to-end tests: clean
   remount elsewhere, blobs read back remotely, same-disk crash recovery,
   machine loss in async mode, sync-mode acknowledgement, fencing of a
   stale writer, a broken log refusing the mount.
5. Restorer: replica, packs, sparse-pack rewrite, snapshot commit,
   shutdown restore, snapshot expiry, log truncation, LIST-free object GC.
   **Done.**
6. `skyfs`: CLI (`create`, `mount`, `fsck`, `info`) and the FUSE adapter.
   **Done**; smoke-tested through the kernel on Linux (create, blob
   writes, in-place patch, links, renames, xattrs, unlink-while-open,
   remount elsewhere, `kill -9` and same-disk recovery, fsck).
   **pjdfstest passes in full** (238 files, 8,798 tests; `scripts/pjdfstest.sh`,
   run in CI). The kernel does not enforce `NAME_MAX` for FUSE, so
   `lookup` returns `ENAMETOOLONG` itself.
7. **Fault injection. Done** (`crates/skyfs-volume/tests/faults.rs`,
   `skyfs_store::faults`). A fault-injecting bucket numbers every
   mutation (PUT, multipart completion, DELETE) and injects one fault at
   the k-th: a transient failure, a lost response, or a crash just
   before or just after it lands. A fixed workload (inline and blob
   writes, patches, truncation, renames, unlink, restore passes with GC)
   runs once per k and fault. Oracle: transient faults and lost
   responses change nothing (clean, fully restored unmount, exact final
   state); after a crash, another machine sees the state after a prefix
   of the operations that includes every `fsync`ed one, and the same
   disk recovers every operation. Two more enumerations crash the
   recovery mount at each of its mutations, and tear the newest local
   segment, its data and a blob at every byte near the end. Every
   volume passes fsck. It found and fixed: a lost manifest response
   stopping the restorer as if fenced; one transient error ending the
   unmount restore; a failed read during repack treated as corruption;
   torn blobs accepted by WAL recovery; mount waiting forever for an
   unreachable bucket.
8. **Credentials without a control plane. Done** (§10.1): OIDC workload
   identity exchanged by skyfs with a per-volume session policy, the
   platform's ambient identity, and static keys from a re-read secret.
   A credential broker and lease service were built first and removed:
   a service every mount depends on, holding parent keys, to provide a
   lease the log's fence already makes unnecessary for safety.
9. **xfstests. Done** (`scripts/xfstests.sh`, the generic `quick` group,
   650 tests, through `mount -t fuse.skyfs`, in CI). The first runs found
   and fixed: restore passes that never finished on short-lived mounts
   (now bounded batches); `readdir` running forever while a program
   renames what it lists (a rename within a directory now keeps the
   entry's cookie); stale file handles (`open_by_handle_at`, NFS export:
   export support and `lookup(ino, ".")`); `fallocate` beyond the
   reported capacity; atime missing for reads served from the page cache;
   read-only mounts and remounts; `nodev` blocking device-node tests. The
   12 exclusions, each with its reason, are in `scripts/xfstests.exclude`:
   ACLs, the kernel's old S_ISGID rule for FUSE, two behaviours of the
   test kernel to re-check elsewhere, and the single-writer rule.
10. **Read-only mounts. Done** (`--read-only`, `-o ro`): snapshot plus
    tail as of mount time, no epoch claimed, nothing written, read-only
    credentials, the local WAL left for the next read-write mount.
11. **macOS. Done** (§10.2): the NFS transport, tested in-process against
    the adapter, through the macOS and Linux kernels' NFS clients
    (`scripts/nfs-smoke.sh`, in CI on both), with pjdfstest over NFS on
    Linux (90.4% pass; the rest is MKNOD and multi-user permission checks,
    which the single-user transport leaves out), and against S3 from a
    MacBook. It found a READDIR bug in the NFS library
    (patched) and that `F_FULLFSYNC` made every close cost 28 ms (§6.2).
12. **Open:** an offline LIST sweep for crash orphans;
    FUSE writeback cache and passthrough; POSIX ACL enforcement
    (`FUSE_POSIX_ACL`; ACL xattrs are stored but not enforced).

Benchmark before step 4 ships:

- R2 conditional PUT latency and throughput per seq (group commit ceiling).
- R2 behaviour on `If-None-Match` races, and whether CompleteMultipartUpload
  honours it (affects nothing in this design, but would simplify blobs).
- Mount time vs inode count (10⁴, 10⁶, 10⁷) and tail length; snapshot
  pass time and bytes per pass at each size (decides when §7.5's option C
  is needed).
- Cold read throughput of an 8 GB model through skyfs vs direct GETs.
- `fsync` latency in sync mode from Lambda, Hyperstack and DigitalOcean.
- SQLite on skyfs (InvokeAI) under both modes.
