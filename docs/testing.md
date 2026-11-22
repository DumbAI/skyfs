# Testing skyfs: integration tests, failure injection and coverage

skyfs makes two promises that ordinary tests cannot check by example.
After any crash the volume is the result of a *prefix* of the operations,
never a mixture. And every bucket retry is idempotent, because a lost
response means the mutation may have landed. The test suite is built to
prove those promises exhaustively, and then to prove POSIX behaviour
through the real kernel and the real network.

This document lists every suite, what it proves, how to run it, the bugs
each one found, and what is still not covered. As of November 2026 the
workspace has **88 Rust tests** (`cargo test`, about a minute), plus five
suites that run through the kernel, one of them against a real bucket.
Linux with FUSE is the production path; the NFS transport (macOS) has its
own adapter tests and kernel smoke test on both macOS and Linux.

## 1. The layers

| # | Layer | Where | Runs on | Proves | Size |
|---|---|---|---|---|---|
| 1 | Format and codec | `skyfs-format` unit tests, `tests/golden.rs` | any OS | segments encode, decode and fail closed; the byte layout of every op is pinned | 6 tests |
| 2 | State machine | `skyfs-meta` `tests/scenarios.rs`, `tests/replay_prop.rs`, `plan.rs` | any OS | POSIX semantics of `plan` + `apply`; live state = replay of the log | 22 scenarios, 1 property × 256 cases, 2 unit |
| 3 | Store | `skyfs-store` unit tests | any OS | keys, create-only PUTs, cache, credentials, the fault store itself | 19 tests |
| 4 | Engine integration | `skyfs-volume` `tests/engine.rs`, `wal.rs` | any OS, in-memory bucket | mount, write, replicate, recover, restore, fence, end to end | 18 + 2 tests |
| 5 | Failure injection | `skyfs-volume` `tests/faults.rs` | any OS, in-memory bucket | every fault at every bucket mutation, every crash of recovery, every torn byte of the local WAL | 7 tests, thousands of cases |
| 6 | Kernel smoke | `scripts/fuse-smoke.sh` | Linux, root | the FUSE adapter end to end, remount on another machine, SIGKILL recovery | 1 script |
| 6b | NFS adapter, in process | `crates/skyfs/src/nfs.rs` tests | any OS, in-memory bucket | the NFSv3 adapter over a real `Volume`: unstable writes + COMMIT, handle lifetime, readdir paging, file handles, errors | 8 tests |
| 6c | NFS through the kernel | `scripts/nfs-smoke.sh` | macOS (as the user) and Linux (root) | the same story as the FUSE smoke test through each kernel's NFS client | 1 script |
| 7 | POSIX conformance | `scripts/pjdfstest.sh` | Linux, root | pjdfstest | 8,798 tests |
| 8 | Filesystem regression | `scripts/xfstests.sh` | Linux, root | xfstests generic `quick` group through `mount -t fuse.skyfs` | 639 run, 12 excluded |
| 9 | Real bucket | `scripts/s3-e2e.sh` | Linux, root, S3 | latency, throughput, cold reads, fencing and recovery over the network | 6 scenarios |

Layers 1 to 5 and 6b run on every push (`cargo test` in CI). Layers 6 to 8
and 6c run in CI. CI is Linux only; macOS (`cargo test`, 6c as the user) is
checked by hand.
Layer 9 needs a bucket and credentials and is run by hand.

The layering follows the crates. Each layer tests one crate through its
public interface and trusts the layers below, so a failure points at one
crate. The engine (`skyfs-volume`) is platform-independent precisely so
that layers 4 and 5, the expensive reasoning, run anywhere in seconds
against `object_store::memory::InMemory`.

## 2. Format, state machine and store (layers 1 to 3)

**Format** (`crates/skyfs-format`). `segment.rs` checks a round trip that
reads metadata without data, and that a corrupt record region, a truncated
segment, a newer major version and an unknown op are each *rejected*,
never skipped. `tests/golden.rs` encodes every log operation and compares
the bytes with a pinned copy: if it fails, the format changed, which is a
`docs/wal-format.md` change, not a refactor.

**State machine** (`crates/skyfs-meta`). Every scenario runs `plan`, logs
the record, applies it, then replays the log into a fresh store and
compares. The 22 scenarios:

| Area | Scenarios |
|---|---|
| Data | partial write at a random offset; in-place edit splits extents and keeps chunk CRCs; append; `O_TRUNC`; truncate and sparse files; `fallocate` collapse, insert and clone |
| Namespace | rename of files and directories with contents; replace-by-rename frees the old file; `RENAME_NOREPLACE` and `RENAME_EXCHANGE`; rename errors; `rmdir` and `unlink`; hard links and symlinks; special files; names longer than `NAME_MAX` |
| Attributes | chmod, chown, utimens; xattrs; setgid directories pass on their group |
| Lifecycle | unlink while open, then reclaim or remount; a worked example from the design |
| Directory listing | readdir cookies stay stable across changes; renaming while listing ends |
| Replay | snapshot plus tail equals full replay; a corrupt identity (`Remove.ino`) refuses replay |

`replay_prop.rs` is a property test: 256 random sequences of mkdir,
create, symlink, write, truncate, rename (all modes), link, unlink, xattr
and fallocate ops. For each one, the live store equals a replay of the
encoded segments, both pass fsck, and every file's bytes equal a reference
model. `plan.rs` adds the relatime rule (Linux semantics) and that an
access never changes ctime.

**Store** (`crates/skyfs-store`). Keys sort in log order, round-trip,
live under their class prefix and reject foreign strings. A create-only
PUT never overwrites. The head probe gallops to the end of the log. The
chunk cache evicts LRU and adopts committed segment data. Credentials:
`credential_process` parsing; caching until near expiry; refresh inside
the margin; keeping a still-valid credential when refresh fails; a file
source re-read after rotation; and the OIDC exchange against a fake STS.
That last test checks the exact request (role, session name, current
token, one-hour duration, session policy), that a rotated token file is
used on the next exchange, and that an STS error never echoes the token.
`faults_fire_once_at_their_index` tests the fault store itself.

## 3. Engine integration tests (layer 4)

`crates/skyfs-volume/tests/engine.rs` mounts real volumes on an in-memory
bucket and a temporary local directory, and drives them through the same
`Volume` API the FUSE adapter uses. Each test is a whole lifecycle, not a
unit:

| Test | What it proves |
|---|---|
| `clean_unmount_then_mount_elsewhere` | a second machine with an empty disk sees the exact state |
| `large_writes_go_to_blobs_and_read_back_remotely` | runs past `inline_data_max` become blobs, uploaded before the segment that references them, read back by range |
| `crash_on_same_disk_recovers_the_local_wal` | a container restart loses nothing acknowledged |
| `machine_loss_in_async_mode_loses_only_the_unreplicated_tail` | another machine sees a prefix; `MountBegin` reports the loss |
| `sync_mode_acknowledges_after_commit` | `--replication sync` acknowledges only committed ops |
| `a_new_mount_fences_the_old_writer` | the old writer's commit gets `412` and stops; its ops never land |
| `restore_passes_move_the_pointer_and_truncate_the_log` | snapshot + manifest move the restore pointer; old segments are deleted |
| `a_long_backlog_is_restored_in_published_steps` | restore passes are bounded batches, each published |
| `a_broken_hash_chain_refuses_the_mount` | fail closed on a broken chain |
| `unlink_while_open_keeps_data_until_close` | orphan semantics across the engine |
| `names_longer_than_name_max_are_too_long` | `ENAMETOOLONG` from lookup |
| `relatime_updates_atime_once_per_change_and_replays`, `noatime_never_logs_an_access`, `an_open_for_reading_is_an_access` | the three atime modes, and that atime is logged and replayed |
| `one_process_per_local_directory` | a second daemon on one cache directory waits, then fails |
| `fallocate_beyond_the_reported_capacity_is_enospc` | capacity matches what `statfs` reports |
| `a_read_only_mount_serves_and_writes_nothing` | no epoch, no log, no restore, `EROFS` |
| `dot_finds_any_inode_for_file_handles` | `lookup(ino, ".")` for NFS export and `open_by_handle_at` |

`tests/published.rs` covers published volumes and clones (design §13):

| Test | Proves |
|---|---|
| `publish_seals_a_pruned_copy_of_the_source` | only the kept files' objects are copied; the copy is sealed (read-write mount refused) and never references its source; a retry is recognised; a source that moved on cannot be republished to the same id |
| `a_clone_copies_metadata_only_and_writes_copy_on_write` | no data object is created by a clone; files, hard links, symlinks and xattrs read through; overwrite, append, add and delete stay in the clone; the base's objects are byte-for-byte unchanged |
| `a_clone_never_collects_its_base` | eager GC over several mounts after deleting everything inherited leaves the base intact, and the clone's own live data (with keys equal to the base's) survives |
| `clones_of_published_clones_reference_the_owning_volumes` | publishing a clone copies only its own objects; a clone of that has both bases and reads one hop |
| `publish_needs_a_quiescent_source_and_clone_a_sealed_base` | publish refuses a mounted or crashed source and commits nothing; clone refuses an unsealed base |
| `a_clone_refuses_to_mount_over_an_unsealed_base_and_never_overwrites` | a lost seal fails mount and fsck closed; a clone over another base's clone is `Exists` |
| `publish_survives_a_fault_at_every_mutation`, `clone_survives_a_fault_at_every_mutation` | every fault at every mutation (copies, metadata, seal, manifest, hint): what is visible is complete, and a retry converges |

`skyfs-meta/tests/genesis.rs` covers the metadata side: pruning with hard
links, externalizing without chains, genesis identity, refusing a store
that is not a snapshot, fsck of an unlisted base, and migrating a
schema-1 file.

`wal.rs` unit tests check that a sealed local segment decodes like the
builder made it, and that recovery cuts a torn tail and rebuilds the same
record region.

## 4. Failure injection (layer 5)

### 4.1 The fault store

`skyfs_store::faults::FaultStore` (feature `testing`) wraps any
`ObjectStore` and numbers every **mutation** in the order it reaches the
bucket: PUT, completed multipart upload, DELETE. A plan picks one
mutation index *k* and one fault:

| Fault | What happens at mutation *k* | What it simulates |
|---|---|---|
| `Fail` | refused before it lands | a transient error, throttling, a dropped connection |
| `LostAck` | lands, but the caller gets an error | a lost response: the retry repeats something that already happened |
| `CrashBefore` | the process dies before it lands | a crash between deciding and doing |
| `CrashAfter` | the process dies after it lands, before hearing | a crash between doing and recording |

After a crash every later operation of the dead process fails, reads
included, so nothing of it reaches the bucket again. The wrapper sits
under `ObjectClient`, the only code that talks to the bucket, so no path
can bypass it.

### 4.2 The workload and the oracle

`tests/faults.rs` runs one fixed workload: inline writes, writes large
enough to become blobs, in-place patches, truncation, symlinks, file and
directory renames, unlink, and `fsync` durability points throughout, so
the log has many segments and restore passes run during the workload. A
fault-free run makes **31 mutations**, from every engine path:

| Mutation | Count | Engine path |
|---|---|---|
| PUT `wal/<seq>.seg` (create-only) | 12 | committer |
| PUT `blobs/<epoch>/<n>` | 2 | blob uploads |
| PUT `packs/<blake3>` | 4 | restore: repacked data |
| PUT `snapshots/<g>-<digest>.meta.zst` | 4 | restore: snapshot metadata |
| PUT `snapshots/<g>.manifest` (create-only) | 4 | restore: moves the pointer |
| PUT `volume.json` | 4 | hint, after restore and at unmount |
| DELETE `wal/<seq>.seg` | 1 | log truncation (GC) |

A model replays the workload on an in-memory tree and records the tree
after **every log operation**, not every step: a segment may be sealed in
the middle of a step, so durability is a prefix of operations. The oracle
for every (*k*, fault):

- **`Fail` and `LostAck` change nothing.** The run ends with a clean
  unmount, fully restored, holding exactly the final state.
- **After `CrashBefore` or `CrashAfter`**, a mount on another machine
  (empty disk) succeeds and holds one of the recorded states. That state
  includes every operation an `fsync` acknowledged. A mount on the same
  disk recovers every operation.
- **Every resulting volume passes fsck** (replay from the bucket and check
  every invariant).

### 4.3 The seven tests

| Test | Cases |
|---|---|
| `baseline` | the fault-free run: counts and prints the mutations; asserts the workload makes more than 25 |
| `a_transient_failure_at_every_mutation` | `Fail` at each *k* |
| `a_lost_response_at_every_mutation` | `LostAck` at each *k* |
| `a_crash_before_every_mutation` | `CrashBefore` at each *k*, then both recoveries |
| `a_crash_after_every_mutation` | `CrashAfter` at each *k*, then both recoveries |
| `a_crash_at_every_point_of_recovery` | the bucket goes away early, the process keeps logging locally (a long local WAL with blobs), then dies. The *recovery* mount crashes before and after each of its own mutations (recovered blobs, recovered segments, `MountBegin`, a restore pass); a second recovery must still bring back every operation |
| `a_torn_local_wal_recovers_a_prefix` | power loss tears the newest local files (segment frames, inline data, a blob) at every one of the last 64 bytes and at about 40 points before them; same-disk recovery must mount, serve a prefix, and never serve bytes that fail their checksum |

To reproduce one case: `FAULT_CASE=LostAck:12 cargo test -p skyfs-volume
--test faults baseline -- --nocapture`.

### 4.4 What it found

Each of these was a real bug, fixed in the engine:

| Bug | Found by | Fix |
|---|---|---|
| A lost manifest response stopped the restorer as if fenced | `LostAck` on a manifest | the manifest is retried byte for byte; `AlreadyExists` with identical bytes is success |
| One transient error ended the unmount's restore | `Fail` during unmount | drain retries with backoff |
| A failed read during repack was treated as corruption | `Fail` during restore | read errors map to store errors, retried |
| Torn blobs were accepted by WAL recovery | torn-WAL sweep | recovery verifies each local blob chunk's CRC |
| A mount waited forever for an unreachable bucket | crash sweep | `mount_timeout` (5 min) |

## 5. Through the kernel (layers 6 to 8)

All three run as root on Linux with `/dev/fuse`, in CI and in the Docker
command from the README.

**`scripts/fuse-smoke.sh`.** Creates a volume and mounts it. It exercises
every operation mountpoint-s3 refuses: random and in-place writes, a 24 MiB
file patched at an offset, appends, hard links and symlinks, file and
directory renames, xattrs, chmod, truncate, a FIFO, an unlinked-but-open
file, and an fsynced in-place database edit. Then it unmounts and runs
fsck. It remounts on an empty cache (another machine) and checks every
byte. Finally it crashes the mount with SIGKILL and checks same-disk
recovery (`recovered=1`) and a second fsck. It takes a local bucket by
default; `STORE=s3://bucket/prefix` runs the same checks on a real
bucket (30 s against S3 in us-east-2).

**`scripts/pjdfstest.sh`.** Builds [pjdfstest](https://github.com/pjd/pjdfstest)
and runs the whole suite against a skyfs mount: **8,798 tests, all pass**.
It found that lookup must return `ENAMETOOLONG` for long names.

**`scripts/xfstests.sh`.** Runs the generic
[xfstests](https://github.com/kdave/xfstests) `quick` group through
`mount -t fuse.skyfs` with `scripts/mount.fuse.skyfs`, so mount, remount
and unmount go through mount(8) exactly as in production. Release build;
CI job timeout 180 min. Last result: **"Passed all 639 tests"**, fsck
clean.

It found:
- restore passes that never finished on short mounts (now bounded batches);
- `readdir` that never ended while a program renamed what it listed (a
  rename within a directory now keeps the entry's cookie);
- stale file handles (export support and `lookup(ino, ".")`);
- `fallocate` beyond the reported capacity;
- atime missing for reads served from the page cache;
- read-only mounts and remounts;
- `nodev` blocking device-node tests.

The 12 exclusions are in `scripts/xfstests.exclude`, each with its reason:

| Tests | Why excluded |
|---|---|
| generic/099, 319, 375, 444 | POSIX ACLs are stored but not enforced or inherited (open item) |
| generic/683, 684, 685 | clearing `S_ISGID` on fallocate when the caller is not in the group: the kernel applies its older rule for FUSE, and FUSE does not send supplementary groups |
| generic/633, 688 | file capabilities: the test kernel (linuxkit 7.0) answers `EINVAL` before reaching the filesystem; re-check on other kernels |
| generic/504 | the test kernel does not list FUSE flocks in `/proc/locks` (the lock itself works); re-check on other kernels |
| generic/732 | mounts one volume twice and uses both; skyfs is single-writer, the second mount fences the first |
| generic/798 | `cachestat` expects written pages to stay dirty until `fsync`; skyfs's FUSE mount writes through (no writeback cache, an open item) |

**The CI runner.** Two things the job needs that a laptop hides:
- *Disk.* A hosted runner has about 14 GiB free, and a full disk kills the
  runner without leaving a log. Test volumes mount with `retention=0s`
  (GC deletes what each restore pass supersedes) and `cache_gib=1`; the
  job first frees about 30 GiB of preinstalled SDKs; a watchdog in
  `xfstests.sh` stops the run with a usage breakdown below 2 GiB free.
  Measured peak: 11.4 GiB.
- *GNU coreutils.* The expected outputs are GNU's messages; Ubuntu 26.04
  defaults to the Rust coreutils, so the job installs `coreutils-from-gnu`.

## 5b. The NFS transport (layers 6b, 6c)

The macOS transport (design §10.2) is a second thin adapter over
`Volume`, so it gets the same two kinds of test as the FUSE one.

**In process** (`crates/skyfs/src/nfs.rs`). The `NFSFileSystem` methods are
plain async functions, so the tests call them against a mounted `Volume`
on an in-memory bucket, no kernel or client involved:

| Test | What it proves |
|---|---|
| `unstable_writes_commit_and_survive_a_remount` | UNSTABLE writes answer UNSTABLE, FILE_SYNC ones sync first; after COMMIT and a remount elsewhere the same file handle names the same bytes |
| `handles_are_released_at_commit_remove_and_idle` | the only adapter state (write handles) cannot leak: released at COMMIT, by the idle sweep, and at remove, where the unlinked file is freed at once |
| `readdir_pages_by_cookie` | paging with skyfs's stable cookies returns every entry exactly once |
| `reads_across_split_extents_end_at_eof` | short reads and `eof` across extents split by an in-place write |
| `namespace_operations` | mkdir, hard and symbolic links, rename over a file, chmod/truncate/utimes, errno → NFS status |
| `file_handles_are_scoped_to_the_volume` | a handle from another volume is STALE, a short one BADHANDLE |
| `a_read_only_mount_says_so` | read-only capability and ROFS |
| `apple_double_siblings_are_stored_but_not_listed` | `._` siblings are stored and found by name, never listed; pages of visible entries stay exact across runs of hidden ones |
| `rmdir_clears_leftover_siblings` | a directory holding only hidden siblings removes; one with a real entry stays |
| `conversions` | time, device-number and status conversions |

**Through the kernel** (`scripts/nfs-smoke.sh`). The same story as
`fuse-smoke.sh`: create, write and patch a 24 MiB file, append, hard and
symbolic links, renames, chmod, truncate, unlink-while-open, an fsynced
in-place edit, 200 files in a directory; unmount, fsck, remount on an empty
cache and check every byte; SIGKILL the daemon and check same-disk
recovery; fsck. It runs as the user on macOS with nothing installed, and as
root on Linux; CI runs both.

It found:
- **A READDIR bug in the NFS library.** Plain READDIR ignored the client's
  cookie and restarted at the first entry, so the Linux client, which pages
  with READDIR after its first READDIRPLUS, listed a 201-entry directory
  forever. Patched in `vendor/nessie-nfsserve` (`PATCHES.md`); macOS only
  uses READDIRPLUS, which was correct.
- **`uv pip install` was slow and its cache failed.** Profiling showed two
  causes. The NFS library swept its whole duplicate-request cache (about
  180,000 entries at load) on every RPC under a global lock, so each RPC
  grew from 90 µs to 360 µs (patched: `PATCHES.md` 2); and macOS's hidden
  AppleDouble `._` siblings were listed, so uv tried to hard-link them
  (EPERM). Siblings are now stored but not listed. numpy, pandas, requests
  and rich: 145 s → 22.9 s; with uv's cache on the volume 2.4 s.
- **Every close cost 28 ms on macOS.** The NFS client commits at each
  close, and the local WAL sync used `F_FULLFSYNC` (Rust's `sync_data` on
  macOS). Syncing as the platform's `fsync(2)` does (design §6.2) took 2000
  small files from 57 s to 4.4 s.

**pjdfstest over NFS** (Linux, `SKYFS_TRANSPORT=nfs scripts/pjdfstest.sh`):
90.4% of the 8,798 tests pass, and the volume passes fsck afterwards. The
842 failures are what design §10.2 leaves out: device nodes and FIFOs
(`mknod`, `mkfifo`: no MKNOD) and permission checks between users
(`chmod`, `chown`, `open`, `rename`, `unlink`… run as other uids: the
transport is single-user and does not enforce permissions between users).
It is not in CI; FUSE stays the conformance bar.

On top of that, by hand: the NFS transport from a MacBook against S3
(us-east-2): mount in 2.8 s, a git repository committed, unmount, remount
on an empty cache in 2.6 s, a 32 MiB cold read in 1.4 s with its checksum,
fsck clean.

## 6. Against a real bucket (layer 9)

`scripts/s3-e2e.sh` runs six scenarios against S3 (or any S3-compatible
store) through the kernel. They check what an in-memory bucket cannot
show. Last run: November 2026, a laptop in Docker against
`s3.us-east-2.amazonaws.com`, `--credentials static` from a re-read
credentials file, a 64 MiB large file.

| # | Scenario | Result |
|---|---|---|
| 1 | Mount with an empty cache; write 64 MiB + fsync; 2000 small files; `ls -l` | mount 2.6 s; 18.1 s; 0.44 s; 0.12 s |
| 2 | Read-only mount beside the writer | mount 3.1 s; write refused with `EROFS`; cold 64 MiB read 8.3 s, checksum matches |
| 3 | A second writer mounts while the first runs | the first is fenced at seq 4; its writes fail with `EIO`; its last write is absent |
| 4 | SIGKILL, local cache deleted, recover from the bucket alone | mount 3.7 s; the fsynced file is present; 0 of 50 unsynced files survived (a valid prefix, killed within `max-lag`); every byte of the rest intact |
| 5 | Offline fsck and info | clean through seq 9; 2005 inodes, 64 MiB of data |
| 6 | Warm remount (cache kept) | mount 2.5 s, unmount 1.4 s |

**What it found: congestion collapse on slow uplinks.** The first run
used a 512 MiB file and never finished. Each request had a 30 s total
timeout, and up to 32 parts of 16 MiB were in flight, so a part only
completed above roughly 140 Mbit/s of uplink. On a 3 Mbit/s link every part
timed out and restarted, forever. Failed multipart uploads were also left
behind and billed. The fix: 8 MiB parts, at most 64 MiB in flight, a
5 min request timeout (`--request-timeout`), and an abort before each
retry. The floor is now about 2 Mbit/s (design §6.3).

## 7. Coverage

### 7.1 Invariants and failure cases

Each guarantee in `CLAUDE.md` and design §11, and the tests that prove it:

| Guarantee | Proved by |
|---|---|
| State = snapshot + log tail; one `apply` for live and replay | `replay_prop` (256 random sequences); every scenario replays; `snapshot_plus_tail_equals_full_replay`; fsck after every fault case |
| Validate, then log, then apply; a failing op is never logged | scenarios with errors (`rename_errors`, `names_longer_than_name_max_are_too_long`); pjdfstest |
| Records are deterministic (no clock on replay) | `replay_prop`; atime tests replay to identical times |
| Objects are immutable; create-only PUTs | `create_only_put_never_overwrites`; `LostAck` and `CrashAfter` at every segment and manifest PUT |
| Data before reference | crash before and after every blob PUT; `large_writes_go_to_blobs_and_read_back_remotely` |
| Commits in seq order | crash sweeps (a later seq never lands without an earlier one); fencing tests |
| Fail closed (unknown op, CRC, hash chain, identity) | `unknown_op_fails_closed`, `corrupt_record_region_is_rejected`, `a_broken_hash_chain_refuses_the_mount`, `corrupt_identity_refuses_replay` |
| Crash ⇒ a prefix of operations, including every fsynced one | `a_crash_before/after_every_mutation`; `machine_loss_in_async_mode…`; s3-e2e scenario 4 |
| Same disk recovers everything | crash sweeps (same-disk branch); `crash_on_same_disk…`; fuse-smoke SIGKILL; `a_crash_at_every_point_of_recovery` |
| Power loss tears the local WAL | `a_torn_local_wal_recovers_a_prefix`; `recovery_cuts_torn_tails…` |
| Every retry is idempotent | `Fail` and `LostAck` at every mutation |
| Single writer (fencing) | `a_new_mount_fences_the_old_writer`; s3-e2e scenario 3 |
| Chunks are verified on every read | in-place edit scenario; torn-WAL sweep (never serves bad bytes) |
| The log stays short | `restore_passes_move_the_pointer…`; `a_long_backlog_is_restored_in_published_steps` |
| Credentials refresh and never leak | credentials unit tests; s3-e2e with a re-read file |
| POSIX behaviour | 22 scenarios; pjdfstest 8,798; xfstests quick |
| NFS transport (macOS) | 8 adapter tests; `nfs-smoke.sh` on macOS and Linux; a manual S3 run from a MacBook |

### 7.2 Gaps

What the suites do **not** cover yet, most important first:

| Gap | Risk | How to close it |
|---|---|---|
| OIDC against a real STS | the exchange is tested against a fake STS only | a test IAM OIDC provider and role; run `s3-e2e.sh` with `--credentials oidc` from a pod |
| R2 and other S3-compatible stores | `If-None-Match` and multipart behaviour differ by store | run `s3-e2e.sh` against R2 and MinIO; add MinIO to CI |
| One fault per run | two faults in one run (a lost response, then a crash during the retry) are not enumerated | pairs of (*k*, fault) on a smaller workload |
| Mutation order varies a little between runs | restore passes interleave with commits, so "mutation *k*" is not always the same object | record each case's trace; optionally a deterministic scheduler for restore |
| Faults on reads (GET, HEAD) | a slow or failing read path is tested only by unit tests and `Fail` during repack | extend `FaultStore` to reads: errors, latency, short bodies |
| Slow links and latency | found by hand on S3 | a bandwidth- and latency-shaped proxy (toxiproxy) in CI |
| Long-running mounts | credential expiry and refresh over hours, cache eviction under pressure | a soak test: hours of fsstress against a real bucket |
| Scale | 10⁶+ inodes, very long logs, mount time | a benchmark that builds a large volume and tracks mount and replay time |
| Kernels | 3 xfstests exclusions are blamed on the test kernel | run xfstests on a stock Ubuntu kernel |
| Code coverage numbers | not measured | `cargo llvm-cov` over layers 1 to 5 in CI |
| NFS transport conformance | pjdfstest over NFS passes 90.4%, run by hand; the failures are MKNOD and multi-user permission checks | permission checks against the AUTH_UNIX caller (`ACCESS` and each operation); MKNOD; then pjdfstest over NFS in CI with what remains listed, like `xfstests.exclude` |
| Concurrency | FUSE threads are exercised by the kernel suites, not stressed | xfstests `auto` group and fsstress with many processes |

## 8. Running the tests

```sh
# Layers 1-5: every OS, about a minute.
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test

# One fault case, verbose.
FAULT_CASE=CrashAfter:8 cargo test -p skyfs-volume --test faults baseline -- --nocapture

# Layers 6-8: Linux, root, /dev/fuse (from macOS: the Docker command in the README).
cargo build -p skyfs && sudo env PATH="$PWD/target/debug:$PATH" scripts/fuse-smoke.sh
cargo build -p skyfs && sudo env PATH="$PWD/target/debug:$PATH" scripts/pjdfstest.sh
cargo build --release -p skyfs && sudo env PATH="$PWD/target/release:$PATH" scripts/xfstests.sh -g quick

# The NFS transport: macOS as your user (Linux: sudo, nfs-common).
cargo build -p skyfs && PATH="$PWD/target/debug:$PATH" scripts/nfs-smoke.sh

# Layer 9: a real bucket. Leaves the volume under STORE; delete the prefix after.
cargo build --release -p skyfs
sudo env PATH="$PWD/target/release:$PATH" STORE=s3://bucket/prefix SKYFS_REGION=us-east-2 \
  SKYFS_CREDENTIALS=static SKYFS_CREDENTIALS_FILE=/run/skyfs/creds.json scripts/s3-e2e.sh
```

CI (`.github/workflows/ci.yml`) runs five jobs on every push: `rust`
(fmt, clippy, `cargo test`), `fuse` (smoke), `nfs` (NFS smoke),
`pjdfstest`, and `xfstests`, all on Linux. macOS is not in CI.

**Rules for extending the suites:**

- A new kind of object or mutation extends the workload in
  `tests/faults.rs`, so every fault reaches it (`CLAUDE.md`).
- A new log operation gets a scenario in `tests/scenarios.rs`, an action
  in `replay_prop.rs`, and a line in `tests/golden.rs`.
- A new xfstests exclusion needs its reason in `scripts/xfstests.exclude`.
  An exclusion blamed on the kernel names the kernel.
- A bug found by any suite gets a test at the lowest layer that can show
  it.
