# Sky Compute — workspace storage across clouds

Status: superseded in part by [design.md](design.md) (2026-11-16): the
write path (§5.4, §5.6), the read-only skyfs (§4.2, §5.2) and §5.10 are
replaced by a writable, log-structured skyfs. The object-store comparison
(§3) and the option survey (§4) still apply.

Originally: proposal (2026-11-15, revised for on-demand reads). Companion to
[workspace-design.md](https://github.com/DumbAI/sky-compute-site/blob/main/docs/workspace-design.md), which defines *what* a workspace
holds. This document decides *where those bytes live*, how a GPU machine
on any cloud mounts them, and exactly when written data is safe.

## 1. Decision in brief

**Store everything in Cloudflare R2. On start, download only metadata;
mount the whole workspace as a filesystem whose listings and file stats are
answered locally, and fetch file contents from R2 only when they are read,
caching them on local NVMe.**

After the environment is built, code, the venv, models, datasets and past
outputs are read-only; nearly every write is a *new* file. So the workspace
is a stack of read-only, lazily loaded layers with one local writable layer
on top:

| Layer | Contents | Mounted as (lazy, read-only) | Metadata (listing, stat) | Contents |
|---|---|---|---|---|
| **Application image** | app code at a pinned commit + venv | Nydus (`nydusd`, FUSE) | Nydus bootstrap, a few MB, downloaded at mount | chunks fetched from R2 on read, NVMe blob cache |
| **Customization layers** | custom nodes, extra packages, code edits | skyfs (ours, read-only FUSE) | layer manifest | blobs fetched on read |
| **Project files** | past outputs, runs, checkpoints | skyfs | project manifest | blobs fetched on read |
| **Library** | models, datasets, HF cache | skyfs | library manifests | blobs fetched on read |
| **Writable layer** | everything written this session | overlayfs upper dir on local NVMe | local | local; each new file sealed, uploaded and committed (§5.4) |
| **App state** | settings, SQLite DBs, `HOME` | plain local NVMe | local | restored at start (small); snapshot every few minutes |

- **Start time no longer depends on workspace size.** A machine downloads
  manifests and bootstraps (megabytes), mounts, and starts the app. A
  500 GB workspace starts as fast as an empty one; the first read of each
  file pays its download once.
- **Listing and stat never touch the network.** Every read-only layer is
  described by a manifest that holds each file's path, size, mode, mtime and
  content hash, so `ls -l`, `os.walk`, `stat` and a gallery listing are
  answered from local metadata. Because layers are immutable, the kernel
  may cache those answers forever.
- **Reliability** rests on one invariant: every durable byte in R2 is either
  an immutable, content-addressed object or a small manifest committed by a
  conditional PUT. A crash leaves the previous committed state or the next
  one, never a mixture. Lazy loading affects only reads; the write path and
  its durability point are unchanged.

One machine per project at a time (enforced by a lease).

## 2. Requirements this decision is judged on

1. **Reliable and consistent.** No silent data loss; a clear, per-file
   durability point; crash-consistent recovery.
2. **Mounted as a filesystem, loaded on demand.** Starting a machine must
   not download the workspace. Files are fetched when read and kept in a
   local NVMe cache.
3. **Listing and stat work without fetching contents**, with true sizes,
   modes and times.
4. **No egress tax.** Reads from any cloud must not be billed per GB.
5. **Python imports fast after the first touch**, and the first touch of a
   catalog app's startup set prefetched.
6. **Store each byte once** across projects, **tenant isolation**, and
   **one person can run it**: no storage cluster.

Scope: full VMs with root (Lambda, Hyperstack, Nebius, DigitalOcean, AWS,
Verda), where FUSE and overlayfs are available. RunPod pods are containers
without FUSE and are already out of the MVP (§5.8).

## 3. Where the bytes are stored: object store comparison

List prices on 2026-11-15, US regions.

| Store | Storage $/GB-mo | Egress | Requests (write / read per million) | Problem for this workload |
|---|---|---|---|---|
| **Cloudflare R2** | 0.015 | **$0** | $4.50 / $0.36 | One location per bucket ([location hints](https://developers.cloudflare.com/r2/reference/data-location/)); per-bucket request limits unpublished; 1 write/s per key |
| **Tigris** | 0.02 | **$0** | $5.00 / $0.50 | Younger vendor; multi-region storage price not published |
| Backblaze B2 | 0.00695 | Free up to 3x stored, then $0.01/GB | free | Our reads are ~9x stored; rejects `If-None-Match` writes |
| Wasabi | 0.00799 | "Free" only up to 1x stored; overuse risks suspension | free | 90-day minimum duration, 1 TB minimum: unusable |
| AWS S3 | 0.023 | $0.09/GB | $5.00 / $0.40 | Egress |
| Native GPU-cloud volumes (RunPod, Lambda, Nebius, Verda, DO) | 0.07–0.20 | n/a | n/a | Locked to one region or data center; none cross clouds |

Worked example: 200 GB stored, 60 sessions a month, each reading 30 GB
and writing 5 GB from non-AWS clouds, counted as 4 MiB objects with no cache hits.

| Store | Monthly cost |
|---|---|
| **R2** | **$3.51** ($3.00 storage, $0.52 requests) |
| Tigris | $4.61 |
| B2 | $13.39 (egress beyond the 3x allowance) |
| AWS S3 | ~$160 |

Egress charged by the GPU clouds on the way *out* (uploads) is zero on
RunPod, Lambda, Hyperstack and Nebius, pooled at 10–15 TiB per GPU Droplet
on DigitalOcean, and $0.09/GB on AWS after 100 GB/month free. Reads are
ingress, which is free everywhere. Verda and Prime Intellect egress terms
are unconfirmed.

R2 properties this design relies on: a PUT or CompleteMultipartUpload
returns 200 only once data is persisted
([durability](https://developers.cloudflare.com/r2/reference/durability/));
reads and listings are strongly consistent
([consistency](https://developers.cloudflare.com/r2/reference/consistency/));
conditional PUT (`If-None-Match: *`) and full-object CRC64NVME checksums
are supported ([S3 API](https://developers.cloudflare.com/r2/api/s3/api/),
[release notes](https://developers.cloudflare.com/r2/platform/release-notes/)).

**Why R2 over Tigris:** 25% cheaper storage, Cloudflare's scale, and every
feature this design needs. Tigris wins only if users span continents and
one-location latency hurts. The design is S3-API-only, so switching is a
config change plus a background copy.

**Business plan check:** storage is sold at $0.10/GB-month; at R2's
$0.015 the example user's storage line is ~$20 of revenue against ~$3.50 of cost.

## 4. Options for mounting R2 with a local cache

Judged first on consistency and on *when written data is safe in R2*, then
on whether reads are lazy and listings are local.

| Option | R2 | Consistency | Durability point | Lazy reads, local stat | Rename / overwrite | Verdict |
|---|---|---|---|---|---|---|
| **Nydus** (Dragonfly/CNCF, Apache-2.0) | S3 backend with custom endpoint; R2 untested | Read-only images | n/a | Yes: bootstrap is local, chunks on demand, NVMe blobcache, chunk digests, prefetch lists | Read-only | **Use for the application image** |
| **skyfs** (ours, read-only FUSE over manifests) | Yes, plain range GETs | Read-only, immutable | n/a | Yes: manifest is local, blobs on demand, NVMe chunk cache | Read-only | **Use for customization, project files, library** (§4.2) |
| mountpoint-s3 (AWS, Apache-2.0) | Not officially supported; Modal's R2 bucket mounts are built on it | Strong; TTL-bounded with cache | `close()`/`fsync()` | Lazy, but every first `stat` is a HEAD/LIST round trip, and mtime is the upload time | No rename, no append, no random write | Fallback for the library |
| JuiceFS CE 1.4, local SQLite metadata + Litestream | Yes; `gc`/`fsck`/backup accept unsorted listings since v1.4.0 ([PR #6389](https://github.com/juicedata/juicefs/pull/6389)) | Close-to-open | Data before metadata, but off-machine durability waits for Litestream (~1 s) | Yes | Full POSIX | Runner-up (§4.1) |
| TigrisFS (GeeseFS fork) | Writes untested; Cloudflare's own example mounts R2 read-only | Write-back, async flush | Only on `fsync`; write errors not reported, retried forever | Lazy | Rename = copy + delete | Rejected: durability is implicit |
| rclone mount, full VFS cache | Yes | Write-back | After async upload; `close()` is not durable | Lazy; listings are LIST calls | Emulated | Rejected: same |
| ZeroFS | Yes, with open R2 bugs ([#586](https://github.com/Barre/ZeroFS/issues/586), [#642](https://github.com/Barre/ZeroFS/issues/642)) | Fenced single writer | On fsync (9P/NBD) | Yes | Full POSIX; data opaque | Rejected: young, AGPL |
| Archil | Bring-your-own bucket | Strong on its tier; bucket sync eventual | `fsync` lands in Archil's AWS/GCP tier | Yes | Full POSIX | Rejected: GPUs outside AWS/GCP cross the internet; $0.20/GiB-mo |
| [LayerFS](https://github.com/Ephemeral-AI-Lab/layerfs) 1.1 (MIT, FUSE) | 2.0 branch only: backs up the *whole* SQLite Store file to R2; nothing runs from the bucket | Commit/branch over an overlay | **None by design** (`synchronous=OFF`, no fsync) | Store is local, not lazy | Full POSIX minus FIFOs, sockets, set-id | Rejected: agent tool-call snapshots, not GB-scale data |
| AWS S3 Files, 3FS, Alluxio, SeaweedFS, CephFS, cunoFS | No R2, AWS-only, cluster-scale ops, or undocumented durability | | | | | Rejected |

### 4.1 Why not JuiceFS for the whole workspace, even on one machine

JuiceFS CE 1.4 with metadata in SQLite on the machine and Litestream
replicating it to R2 is the best "one filesystem" design. With writeback
off, it uploads data blocks before committing metadata, so a crash leaves
orphan blocks, not dangling metadata. It loses to the layered design on
four points:

1. **Two durability domains.** Blocks are in R2 on `close()`, but the
   metadata that makes them a file reaches R2 only when Litestream ships
   it. Lose the machine in that window and a checkpoint that `close()`
   acknowledged is gone. Fly.io ran exactly this stack (JuiceFS + SQLite +
   Litestream) for Sprites, hit cases where "the metadata points at a chunk
   that doesn't exist", and replaced JuiceFS
   ([Fly SBD](https://fly.io/blog/sprites-block-device/)).
2. **Opaque data.** Outputs and models are JuiceFS blocks, readable only
   through JuiceFS. In the layered design they are plain objects anyone can
   download with a presigned URL, and a model is the same object in every
   project that uses it.
3. **No deduplication.** The same model in two projects is stored twice.
4. **Metadata is mutable and remote-backed.** Every lookup goes through a
   metadata engine that must be replicated and recovered. In the chosen
   design all read-only metadata is an immutable manifest the kernel may
   cache forever, and the only mutable state is ordinary local files.

JuiceFS becomes the right tool only if several machines must write one
project at once (§5.9).

### 4.2 The read path: why a small read-only filesystem of our own

Lazy reads with local listings need two things: metadata for the whole tree
on the machine, and file contents addressable by range in R2. Three ways to
get them for data (models, datasets, past outputs):

| Option | Listing and stat | Files in R2 | Verdict |
|---|---|---|---|
| A. mountpoint-s3 on the blob prefix + a local symlink tree from the manifest | Names are local, but `stat` follows the link to mountpoint: one HEAD per file on first touch (20–80 ms from another cloud), so a 500-image gallery lists in ~15–40 s cold; mtime and mode are lost; apps see symlinks | plain, content-addressed | Fallback |
| B. Nydus images for data too | Local (bootstrap) | Nydus blobs: an output or model is no longer a plain object; every commit builds and merges an image layer | Rejected for data |
| **C. skyfs: read-only FUSE that serves a manifest** | **Local, exact** (size, mode, mtime from the manifest); kernel caches forever because layers are immutable | **plain, content-addressed**: the same object a presigned URL downloads, shared by every project | **Chosen** |

skyfs is small because it is read-only and immutable: no write path, no
locking, no cache invalidation, no metadata database. It is the same idea
as Modal's ImageFS and Beam's CLIP: a content-addressed index served lazily.

For the **environment** the trade-off flips. A venv is tens of thousands of
small files read in sequence at import time; per-file objects in R2 would
make a cold `import torch` thousands of sequential round trips. Nydus
already solves this: files are packed into chunked blobs, chunks are
deduplicated across images (torch is stored once for every environment),
digests are verified on read, and a prefetch list pulls an app's startup
set in parallel. It runs in production at Alibaba, ByteDance and CoreWeave.
Its RAFS v6 format is EROFS-compatible, so the same images can move to the
kernel's own lazy path (fanotify pre-content hooks, Linux 6.14+) once GPU
clouds ship such kernels. If Nydus fails against R2 in the benchmark, skyfs
gains small-file packs and a prefetch list and serves the environment too.

## 5. Design

```
┌──────────────────────────── GPU machine (any cloud, root) ─────────────────────────────┐
│ /sky/app        overlayfs  upper: /nvme/upper/app                (writable)            │
│                            lower: skyfs(customization manifests) : nydusd(app image)    │
│ /sky/projects/p overlayfs  upper: /nvme/upper/p                  (writable)            │
│                            lower: skyfs(project manifest)                               │
│ /sky/library    skyfs(library manifests)                         (read-only)           │
│ /sky/state,/sky/home       plain NVMe (small, restored at start)                        │
│ caches: /nvme/nydus-blobcache  /nvme/skyfs-chunks   (LRU, keyed by content hash)        │
│ skyd: lease · fetch manifests · mount · seal+upload · journal commit · state snapshot   │
└──────────────┬───────────────────────────────────────────────────────▲─────────────────┘
               │ range GETs on demand (zero egress)                      │ multipart + conditional PUT
┌──────────────▼───────────────────────────────────────────────────────┴─────────────────┐
│ R2: images/<env>/{bootstrap,blobs} · users/<u>/blobs/sha256/… · users/<u>/projects/<p>/  │
│     {journal,manifest}/… · users/<u>/manifests/library/… · mirror/hf/… per location hint │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.1 Buckets, credentials and leases

- **One bucket for user data**, prefixes `users/<user>/`. The machine gets
  a short-lived token scoped to that user's prefix for the session, never a
  long-lived key. Shared buckets per R2 location hint hold application
  images and public HF weights (license permitting).
- **Lease.** The control plane grants one machine a lease on a project; a
  second start is refused while it is held. Every commit (§5.4) is a
  conditional PUT, so a stale machine whose lease expired cannot overwrite
  a newer state even if the lease logic has a bug.

### 5.2 Manifests and skyfs

A **manifest** describes one immutable tree:

```json
{ "version": 1, "parent": "sha256:…",
  "entries": {
    "outputs/run-17/img_0042.png": { "type": "file", "size": 1843211, "mode": 420,
                                     "mtime": "2026-11-15T09:12:03Z", "blob": "sha256:9f…",
                                     "chunks": ["sha256:…"] },
    "models/hub/models--Wan-AI--Wan2.2-TI2V-5B/snapshots/6a2f/model.safetensors":
                                   { "type": "symlink", "target": "../../blobs/3c1e…" } } }
```

- Files larger than the chunk size (8 MiB) list a hash per chunk, so every
  chunk is verified on arrival, and a corrupt byte is never served.
- Symlinks are first-class entries, so the Hugging Face cache's
  `snapshots/` → `blobs/` layout works unchanged.
- A project's manifest is the latest compacted manifest plus the journal
  entries after it (§5.4). Library and customization layers are manifests
  of the same shape.

**skyfs** mounts one or more manifests, read-only:

- `lookup`, `getattr` and `readdir` come from the manifest in memory.
  Entry and attribute timeouts are set very long, because the tree cannot
  change while mounted.
- `read` maps the offset to a chunk, serves it from the NVMe chunk cache or
  fetches it with a range GET, verifies its hash, and caches it. Sequential
  reads trigger parallel read-ahead of the next chunks (16–64 streams), so a
  model file streams at network speed.
- `FOPEN_KEEP_CACHE` lets the page cache keep file data across opens. On
  Linux 6.9+, a file already complete in the local cache is opened in FUSE
  passthrough mode, so the kernel reads it from NVMe directly with no daemon
  in the path.
- The chunk cache is keyed by content hash, so it never goes stale, survives
  remounts, and is shared by every project on the machine.

### 5.3 Application image and customization layers

- The catalog builds each environment (app at a pinned commit + venv at the
  fixed path `/sky/app`) into a **Nydus RAFS v6 image**: a bootstrap
  (metadata, a few MB) and data blobs, published to the shared image bucket
  under `env_hash`, built with a chunk dictionary so unchanged chunks
  (torch, CUDA libraries) are reused across environments.
- The nightly catalog test records which files the app reads during startup
  and its first workflow. That list ships with the image as a **prefetch
  list**: nydusd fetches it in parallel at mount time, so the first launch
  does not import torch one round trip at a time.
- `/sky/app` is an overlayfs mount: committed customization layers (skyfs)
  above the image (nydusd), and `/nvme/upper/app` as the writable layer.
  Installing a custom node or a pip package writes only there. On stop,
  skyd commits the upper directory as a new customization manifest (§5.4);
  the drift check (workspace-design §5) records new packages in
  `overlay.lock` so the layer can also be rebuilt from scratch.

### 5.4 Writes: local first, then an explicit durability point

Every writable path is an overlayfs upper directory on local NVMe, with full
POSIX semantics: temp files, renames, appends, in-place edits. The protocol:

1. **Seal.** skyd treats a file in an upper directory as complete on
   close-after-write plus a short quiet period (fanotify on the NVMe
   filesystem, with a periodic scan as the correctness backstop), or
   immediately when a training job calls `sky checkpoint`.
2. **Upload.** Hash the file and multipart-upload it to
   `blobs/sha256/<hash>` with a full-object CRC64NVME checksum and
   `If-None-Match: *`. An existing object means it is already safe. The
   object appears only when CompleteMultipartUpload returns 200, and R2
   returns 200 only once the data is persisted.
3. **Commit.** Append the batch `{path → hash, size, mode, mtime}` to the
   project journal as `journal/<seq>.json` with `If-None-Match: *`, at most
   every 30–60 s. **That PUT is the durability point** the UI reports
   ("saved to cloud").
4. **Deletions** are overlayfs whiteouts in the upper directory, committed as
   journal entries. An app rotating checkpoints deletes locally at will,
   but the R2 copy of the previous checkpoint is released only after its
   successor is committed.
5. **Compaction.** Every few hundred entries skyd writes a compacted
   `manifest/<seq>.json`. On the next start, everything committed is in the
   lower layer (lazy) and the upper directory starts empty.

Two consequences of the overlay worth stating:
- Editing, appending to or renaming a *file from an earlier session* copies
  it up first, which means fetching it in full. In this access pattern that
  is rare (old outputs are not edited); `metacopy=on` keeps `chmod`/`chown`
  cheap.
- The kernel requires overlay lower layers not to change while mounted.
  They are immutable by construction; a new commit becomes visible as a
  lower layer at the next mount, while the running session already sees it
  in its upper directory.

Training jobs get one rule: call `sky checkpoint <path>` (or write a
`_COMPLETE` marker, as PyTorch DCP does) and wait for it before deleting
the previous checkpoint. On a preempted VM, the loss is bounded by the
last committed checkpoint.

### 5.5 Library

- `/sky/library` is skyfs over the library manifests a project references
  (`sky.toml`), presenting real files with true sizes: `models/<name>/<n>/…`,
  `datasets/<name>/<n>/…`, and the Hugging Face cache layout under
  `hf/hub/`. App adapters (workspace-design §4) point at it, read-only.
- Public HF weights resolve to the regional mirror bucket; user models to
  the user's blobs. Nothing is downloaded until read.
- **Library writes** (registering a model version) upload new blobs, then
  write `manifests/library/<kind>/<name>/<n>.json` with `If-None-Match: *`;
  a race yields version n+1. Versions never change.
- vLLM can stream weights straight from R2 (`--load-format runai_streamer`
  with `AWS_ENDPOINT_URL`), skipping the filesystem when that is faster.

### 5.6 App state

Settings, saved workflows, SQLite DBs and `HOME` stay on local NVMe. They
are saved by the **same journal and blob store as new files (§5.4)**, with
one difference: state is saved as an all-or-nothing snapshot, not file by
file, because a settings file and a database must match each other.

**The change log is the project journal in R2**, not a database:
`users/<u>/projects/<p>/journal/<seq>.json`, each written with
`If-None-Match: *`. R2 is the only system of record, so durability never
depends on the control plane being reachable, and the conditional PUT on the
next sequence number fences stale machines. The control plane reads the
journal asynchronously into Postgres for the UI, billing and GC. A journal
entry is either `files` (new outputs, §5.4) or `state` (a snapshot), and
every few hundred entries skyd writes a compacted `manifest/<seq>.json`.

A state snapshot, every 2–5 minutes when something changed and always on stop:

1. **SQLite:** for each database the app adapter declares, `VACUUM INTO`
   (or the online backup API) a consistent copy while the app runs. Never
   copy the live file and its `-wal`.
2. **Small files:** pack the changed files of `state/` and `HOME` into one
   `tar.zst` object named by its hash, with an index of path → content hash,
   so unchanged files cost nothing.
3. Upload the pack and database copies as blobs, then commit one `state`
   journal entry naming them. Restore takes the latest `state` entry.

Tools considered:

| Tool | Verdict |
|---|---|
| **Our journal + blobs (chosen)** | One mechanism, one format, one durability rule for the whole workspace. A few hundred lines on top of the uploader §5.4 already needs |
| Litestream (SQLite WAL replication to any S3 endpoint) | Best recovery point (~1 s) and mature. But a second durability system with its own format, and every sync is a Class A write: at a 1 s interval a busy DB costs ~2.6M PUTs, ~$12/month, more than the user's whole storage bill. **Adopt per database** with a 10–30 s interval if an app's DB proves too valuable for a few minutes of loss (InvokeAI's gallery is the first candidate) |
| restic / kopia | Atomic snapshots and dedup, but an opaque second format, and the repo key lives on the GPU machine |
| rclone sync | Copies files one by one: a crash mid-sync leaves a mixed state; no atomic snapshot |
| git (state dir as a repo) | Nice diffs for JSON workflows, but SQLite and binaries bloat it; a third format to operate |
| LiteFS | FUSE plus a lease service, built for replicas across machines, which a single-machine project does not need |

### 5.7 Start and stop

**Start**, in order, with nothing proportional to workspace size:

1. Acquire the lease; read the project's head manifest and journal tail,
   the referenced library manifests, the customization manifests and the
   image bootstrap (megabytes in total).
2. Mount nydusd (prefetch list starts in the background), skyfs, and the
   overlays.
3. Restore app state and `HOME` from the latest `state` snapshot (§5.6).
4. Start the app. Everything else is fetched when read.

**Stop:** stop the app, seal and commit remaining files, snapshot state,
commit the customization layer, release the lease, then stop the VM. The
VM stops only after the last commit succeeds.

### 5.8 Fallback for machines without FUSE

On container clouds, skyd downloads the referenced files into local disk
instead of mounting them lazily. The write path and state snapshots are
unchanged, so durability is identical; only start time and laziness differ.

### 5.9 What would bring a shared filesystem back

Several machines writing one project at once (multi-node training, a team
editing one dataset live). That calls for JuiceFS CE 1.4 with metadata in a
managed Postgres in the machines' region, one volume per user, VMs only.

### 5.10 What we build and what we reuse

| Piece | Reused | Built by us |
|---|---|---|
| Writable files | ext4/xfs on local NVMe, kernel overlayfs | — |
| Lazy environment image | Nydus (`nydus-image`, `nydusd`), S3 backend → R2 | catalog build step, recorded prefetch lists |
| Lazy data view | `fuser` crate (FUSE in Rust), kernel FUSE passthrough | **skyfs**: read-only manifest filesystem, chunk cache, read-ahead. Estimated 2–3k lines |
| R2 access | `object_store` crate (already used by `skyscan`): ranged GET, multipart, `PutMode::Create` = `If-None-Match: *` | — |
| SQLite snapshots | SQLite `VACUUM INTO` / backup API | list of DBs per app adapter |
| File watching | fanotify | periodic scan as backstop |
| **skyd** | — | lease, mounts, seal + upload, journal commit, compaction, state snapshot, layer commit. Estimated 2–4k lines |
| Control plane | Postgres | lease service, journal indexer, reference-counting GC |

skyfs is the only filesystem we write, and it is read-only over immutable
data, the easiest kind to get right. The write path stays on kernel
filesystems. The correctness risk sits in the commit protocol, tested by
killing the process at every step of save and restore and checking that the
result is always the previous or the next committed state.

## 6. Guarantees and failure cases

| Event | What survives | Why |
|---|---|---|
| VM killed mid-write of a checkpoint | everything committed before it | the object appears only on CompleteMultipartUpload; the journal never names an incomplete file |
| VM killed between upload and journal commit | the blob exists but is unreferenced; GC removes it later | data first, commit last |
| VM killed with sealed but uncommitted files | lost (≤ 60 s of outputs), shown as unsaved in the UI | explicit, bounded window |
| Stale VM wakes up after a new machine took over | the new state | commits are conditional PUTs on the next sequence number |
| Corrupt or truncated download | detected, never served | every chunk is verified against its hash before it is cached |
| R2 unreachable mid-session | cached files keep working; reads of uncached files fail with `EIO`; writes continue locally and upload on recovery | nothing is acknowledged as saved until committed |
| skyfs or nydusd crashes | no data loss: they are read-only | restart the daemon and remount; the writable layers are kernel overlayfs on NVMe |

GC: the control plane counts references from manifests and journals and
deletes unreferenced blobs after the retention window. It never lists the
bucket, so R2's unsorted listings do not matter.

## 7. Open questions and the benchmark that settles them

A one-day benchmark on Lambda, Hyperstack and DigitalOcean:

- **Cold start, lazily:** ComfyUI + Wan2.2 5B from Nydus on R2, with and
  without a prefetch list, against downloading the image whole.
- **Model load through skyfs:** GB/s from R2 with parallel read-ahead,
  against direct parallel GETs and vLLM's Run:ai streamer.
- **Listing:** `os.walk` + `stat` over 10,000 committed outputs (expect
  milliseconds; no network).
- **Kernel features per cloud's default image:** overlayfs with FUSE lower
  layers and `metacopy`, FUSE passthrough (6.9+), fanotify.
- Nydus against R2 (untested upstream): range reads, authentication, error
  handling.
- Time from `close()` of a 20 GB checkpoint to "saved to cloud".

Also open: local NVMe size per instance type, Verda and Prime Intellect
egress terms, and R2's unpublished per-bucket request limits at a few
hundred concurrent machines.
