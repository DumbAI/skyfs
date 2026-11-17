//! Creating and mounting a volume (design §6.5, §6.6, §7.3).
//!
//! Mount: find the latest snapshot (the restore pointer), download its
//! metadata, find the log head by probing, replay the tail after the
//! pointer while checking the hash chain, recover this disk's uncommitted
//! segments if they continue the remote log, then claim a new epoch by
//! committing `MountBegin` with `If-None-Match: *`.

use std::collections::HashMap;
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;
use skyfs_format::snapshot::v1::SnapshotManifest;
use skyfs_format::wal::v1::{MountBegin, Record, SegmentHeader, VolumeCreate, record::Op};
use skyfs_format::{Preamble, Segment, SegmentBuilder};
use skyfs_meta::MetaStore;
use skyfs_store::{Cache, Created, Key, ObjectClient, find_end};
use tokio::sync::{Semaphore, mpsc, watch};

use crate::config::{CreateOptions, VolumeConfig};
use crate::data::{DataReader, segment_data_id};
use crate::error::VolumeError;
use crate::restorer::{Base, CommittedSegment, RestoreMsg, Restorer, tool_id};
use crate::snapshot::{self, Hint, SnapshotId, now_ns};
use crate::wal::{self, Sealed};
use crate::writer::{CommitState, LogPosition, Shared, State};

/// Bytes fetched first when reading a segment's metadata; most record
/// regions fit, so one GET usually suffices.
const META_PREFETCH: u64 = 1 << 20;

/// Seqs probed past the apparent head to detect a hole in the log.
const GAP_PROBE: u64 = 16;

/// Creates a volume: segment 0 holding `VolumeCreate`, and snapshot 0.
pub async fn create(
    client: &ObjectClient,
    volume_id: [u8; 16],
    opts: &CreateOptions,
    scratch: &Path,
) -> Result<(), VolumeError> {
    let exists = || VolumeError::Exists(client.root().to_string());
    if client.exists(&Key::SnapshotManifest(0)).await? {
        return Err(exists());
    }
    let mut b = SegmentBuilder::new(SegmentHeader {
        volume_id: volume_id.to_vec(),
        seq: 0,
        epoch: 0,
        prev_digest: Vec::new(),
        first_op_id: 0,
        record_count: 0,
        snapshot_gen_hint: 0,
    });
    let record = Record {
        op_id: 0,
        time_ns: now_ns(),
        op: Some(Op::VolumeCreate(VolumeCreate {
            volume_id: volume_id.to_vec(),
            root_mode: opts.root_mode,
            root_uid: opts.root_uid,
            root_gid: opts.root_gid,
            inline_data_max: opts.inline_data_max,
        })),
    };
    b.push_record(&record);
    let (bytes, digest) = b.finish();
    if client.put_new(&Key::Segment(0), Bytes::from(bytes)).await? == Created::AlreadyExists {
        return Err(exists());
    }
    let mut replica = MetaStore::in_memory()?;
    replica.apply(&record, 0)?;
    replica.finish_segment(0, &digest, 0)?;
    std::fs::create_dir_all(scratch)?;
    let exported = snapshot::export(&replica, scratch, 0)?;
    drop(replica);
    let restorer_id = tool_id(&volume_id);
    let published = snapshot::publish(
        client,
        exported,
        SnapshotId {
            generation: 0,
            parent_gen: 0,
            volume_id: &volume_id,
            restorer_id: &restorer_id,
        },
    )
    .await?;
    if published.is_none() {
        return Err(exists());
    }
    snapshot::write_hint(client, Hint::default()).await;
    Ok(())
}

/// A segment fetched from the bucket: decoded metadata and its layout.
struct Fetched {
    seg: Segment,
    data_offset: u64,
    record_bytes: u64,
}

async fn fetch_segment(client: &ObjectClient, seq: u64) -> Result<Fetched, VolumeError> {
    let key = Key::Segment(seq);
    let missing = || VolumeError::Corrupt(format!("segment {seq} disappeared during mount"));
    let mut bytes = client
        .get_prefix(&key, META_PREFETCH)
        .await?
        .ok_or_else(missing)?;
    let pre = Preamble::decode(&bytes)?;
    if pre.metadata_len() > bytes.len() as u64 {
        bytes = client.get_range(&key, 0..pre.metadata_len()).await?;
    }
    let (pre, seg) = Segment::decode_metadata(&bytes)?;
    Ok(Fetched {
        seg,
        data_offset: pre.data_offset(),
        record_bytes: pre.records_len,
    })
}

/// Checks segment continuity (wal-format.md "Validity of a segment").
struct Chain {
    volume_id: [u8; 16],
    next_seq: u64,
    next_op_id: u64,
    digest: Vec<u8>,
    epoch: u64,
    last: Option<Op>,
}

impl Chain {
    fn check(
        &mut self,
        h: &SegmentHeader,
        records: &[Record],
        digest: &[u8],
    ) -> Result<(), VolumeError> {
        let seq = h.seq;
        let bad = |why: &str| Err(VolumeError::Corrupt(format!("segment {seq}: {why}")));
        if seq != self.next_seq {
            return bad("out of sequence");
        }
        if h.volume_id != self.volume_id {
            return bad("belongs to another volume");
        }
        if h.prev_digest != self.digest {
            return bad("hash chain broken (prev_digest does not match)");
        }
        if h.first_op_id != self.next_op_id || h.record_count != records.len() as u64 {
            return bad("op ids do not continue the log");
        }
        if h.epoch < self.epoch {
            return bad("epoch went backwards");
        }
        if h.epoch > self.epoch
            && !matches!(records.first().and_then(|r| r.op.as_ref()), Some(Op::MountBegin(m)) if m.epoch == h.epoch)
        {
            return bad("a new epoch must start with its MountBegin");
        }
        for (i, r) in records.iter().enumerate() {
            if r.op_id != self.next_op_id + i as u64 {
                return bad("op ids are not dense");
            }
        }
        self.next_seq += 1;
        self.next_op_id += records.len() as u64;
        self.digest = digest.to_vec();
        self.epoch = h.epoch;
        if let Some(r) = records.last() {
            self.last.clone_from(&r.op);
        }
        Ok(())
    }
}

fn apply_segment(
    store: &mut MetaStore,
    seq: u64,
    epoch: u64,
    records: &[Record],
    digest: &[u8],
) -> Result<(), VolumeError> {
    for r in records {
        store.apply(r, seq)?;
    }
    store.finish_segment(seq, digest, epoch)?;
    Ok(())
}

/// What a finished mount hands to the `Volume`.
pub(crate) struct Mounted {
    pub shared: Arc<Shared>,
    /// None for a read-only mount.
    pub restorer: Option<Restorer>,
    pub restore_rx: mpsc::UnboundedReceiver<RestoreMsg>,
    pub report: MountReport,
}

/// What happened at mount.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MountReport {
    /// The snapshot mounted from.
    pub snapshot_gen: u64,
    /// Segments replayed after the restore pointer.
    pub tail_segments: u64,
    /// Local uncommitted segments recovered and uploaded.
    pub recovered_segments: u64,
    /// Local segments that could not be recovered (kept under `lost/`).
    pub lost_segments: u64,
    /// The previous mount ended with `MountEnd`.
    pub previous_clean: bool,
    /// This mount's epoch.
    pub epoch: u64,
}

struct Dirs {
    meta: PathBuf,
    wal: PathBuf,
    cache: PathBuf,
    scratch: PathBuf,
    lost: PathBuf,
}

fn dirs(root: &Path) -> Result<Dirs, VolumeError> {
    let d = Dirs {
        meta: root.join("meta"),
        wal: root.join("wal"),
        cache: root.join("cache"),
        scratch: root.join("scratch"),
        lost: root.join("lost"),
    };
    for p in [&d.meta, &d.wal, &d.cache, &d.scratch] {
        std::fs::create_dir_all(p)?;
    }
    Ok(d)
}

/// Takes the local directory for this process: two daemons on one
/// directory would interleave WAL files and metadata. A previous mount
/// still finishing its unmount holds it, so wait for it, up to `wait`. The
/// lock is released when the returned file closes, or the process dies.
async fn lock_local_dir(root: &Path, wait: Duration) -> Result<File, VolumeError> {
    std::fs::create_dir_all(root)?;
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join("lock"))?;
    let deadline = Instant::now() + wait;
    let mut logged = false;
    loop {
        match f.try_lock() {
            Ok(()) => return Ok(f),
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                if !logged {
                    tracing::info!(dir = %root.display(), "waiting for another skyfs process to release the local directory");
                    logged = true;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(TryLockError::WouldBlock) => {
                return Err(VolumeError::Busy(root.display().to_string()));
            }
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
    }
}

fn fresh_copy(src: &Path, dest: &Path) -> Result<(), VolumeError> {
    for suffix in ["", "-wal", "-shm"] {
        let mut p = dest.as_os_str().to_owned();
        p.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(p));
    }
    std::fs::copy(src, dest)?;
    Ok(())
}

fn move_lost(lost: &Path, files: &[PathBuf]) {
    let _ = std::fs::create_dir_all(lost);
    for f in files {
        if let Some(name) = f.file_name() {
            let _ = std::fs::rename(f, lost.join(name));
        }
    }
}

/// The state of the volume in the bucket, replayed: the latest snapshot
/// plus the log tail after its restore pointer.
pub(crate) struct Replayed {
    pub manifest: SnapshotManifest,
    /// Snapshot + tail.
    pub live: MetaStore,
    /// A copy of the snapshot alone (the restore replica's start), if asked.
    pub replica: Option<MetaStore>,
    pub tail: Vec<CommittedSegment>,
    /// Data-region offsets of the tail segments.
    pub offsets: Vec<(u64, u64)>,
    chain: Chain,
}

/// Loads the latest snapshot into `meta_dir` and replays the tail, checking
/// the hash chain. Used by mount and by offline tools (fsck).
pub(crate) async fn replay(
    client: &ObjectClient,
    volume_id: [u8; 16],
    meta_dir: &Path,
    with_replica: bool,
) -> Result<Replayed, VolumeError> {
    std::fs::create_dir_all(meta_dir)?;
    let not_found = || VolumeError::NotFound(client.root().to_string());

    // 1. The restore pointer.
    let hint = snapshot::read_hint(client).await;
    let manifest = snapshot::latest(client, hint, &volume_id)
        .await?
        .ok_or_else(not_found)?;
    let base_db = meta_dir.join("base.db");
    snapshot::download_meta(client, &manifest, &base_db).await?;
    let live_db = meta_dir.join("live.db");
    fresh_copy(&base_db, &live_db)?;
    let replica = if with_replica {
        let replica_db = meta_dir.join("replica.db");
        fresh_copy(&base_db, &replica_db)?;
        Some(MetaStore::open(&replica_db)?)
    } else {
        None
    };
    let _ = std::fs::remove_file(&base_db);
    let mut live = MetaStore::open(&live_db)?;
    let pos = live.position()?;
    if pos.applied_seq != Some(manifest.restored_seq)
        || pos.applied_digest != manifest.restored_digest
    {
        return Err(VolumeError::Corrupt(
            "snapshot metadata does not match its manifest".into(),
        ));
    }

    // 2. The log head, by probing forward (no LIST).
    let after = manifest.restored_seq + 1;
    let end = find_end(after, |s| {
        let c = client.clone();
        async move { c.exists(&Key::Segment(s)).await }
    })
    .await?;
    // Probing finds the first missing seq; a hole in the middle of the log
    // would look like its end, and MountBegin would fork history there.
    // Fail closed if anything says the log goes further.
    if hint.head_seq >= end {
        return Err(VolumeError::Corrupt(format!(
            "segment {end} is missing but volume.json records head {}",
            hint.head_seq
        )));
    }
    for probe in end + 1..=end + GAP_PROBE {
        if client.exists(&Key::Segment(probe)).await? {
            return Err(VolumeError::Corrupt(format!(
                "segment {end} is missing but {probe} exists"
            )));
        }
    }

    // 3. Replay the tail, verifying the chain from the snapshot's digest.
    let fetched: Vec<Fetched> = futures::stream::iter(after..end)
        .map(|seq| {
            let c = client.clone();
            async move { fetch_segment(&c, seq).await }
        })
        .buffered(16)
        .try_collect()
        .await?;
    let mut chain = Chain {
        volume_id,
        next_seq: after,
        next_op_id: manifest.restored_op_id + 1,
        digest: manifest.restored_digest.clone(),
        epoch: manifest.restored_epoch,
        last: None,
    };
    let mut tail = Vec::with_capacity(fetched.len());
    let mut offsets = Vec::with_capacity(fetched.len());
    for f in fetched {
        let h = &f.seg.header;
        chain.check(h, &f.seg.records, &f.seg.digest)?;
        apply_segment(&mut live, h.seq, h.epoch, &f.seg.records, &f.seg.digest)?;
        offsets.push((h.seq, f.data_offset));
        tail.push(CommittedSegment {
            seq: h.seq,
            epoch: h.epoch,
            digest: f.seg.digest,
            records: Arc::new(f.seg.records),
            record_bytes: f.record_bytes,
        });
    }
    Ok(Replayed {
        manifest,
        live,
        replica,
        tail,
        offsets,
        chain,
    })
}

/// Mounts the volume `client` points at. Must run on the engine runtime.
#[allow(clippy::too_many_lines)]
pub(crate) async fn mount(
    client: ObjectClient,
    volume_id: [u8; 16],
    cfg: VolumeConfig,
    rt: tokio::runtime::Handle,
) -> Result<Mounted, VolumeError> {
    let local_lock = lock_local_dir(&cfg.local_dir, cfg.local_lock_wait).await?;
    let d = dirs(&cfg.local_dir)?;
    let not_found = || VolumeError::NotFound(client.root().to_string());
    let Replayed {
        manifest,
        mut live,
        replica,
        mut tail,
        offsets,
        mut chain,
        ..
    } = replay(&client, volume_id, &d.meta, !cfg.read_only).await?;
    let cache = Arc::new(Cache::open(&d.cache, cfg.cache_limit)?);
    let data = DataReader::new(client.clone(), cache.clone(), d.wal.clone());
    for (seq, off) in offsets {
        data.set_data_offset(seq, off);
    }
    let remote_digests: HashMap<u64, [u8; 32]> = tail.iter().map(|s| (s.seq, s.digest)).collect();

    // 4. Same-disk recovery (§6.6): local segments continue the remote log
    //    only if they start right after its head, in its latest epoch.
    let mut report = MountReport {
        snapshot_gen: manifest.r#gen,
        tail_segments: tail.len() as u64,
        ..MountReport::default()
    };
    // A read-only mount writes nothing: this disk's WAL is left for the
    // next read-write mount to recover.
    let (local, blobs): (Vec<Sealed>, _) = if cfg.read_only {
        (Vec::new(), Vec::new())
    } else {
        (wal::recover(&d.wal)?, wal::local_blobs(&d.wal)?)
    };
    let mut lost_files = Vec::new();
    let mut continuing = true;
    for seg in local {
        let seq = seg.seq();
        if seq < chain.next_seq {
            // Committed before a crash, not yet retired: ours if it matches.
            if remote_digests.get(&seq) == Some(&seg.digest) {
                seg.retire();
                let _ = cache.adopt(&segment_data_id(seq), &seg.data_path());
            } else {
                lost_files.extend([wal::rec_path(&d.wal, seq), seg.data_path()]);
                report.lost_segments += 1;
            }
            continue;
        }
        if continuing
            && seg.header.epoch == chain.epoch
            && chain.check(&seg.header, &seg.records, &seg.digest).is_ok()
        {
            // Data before reference: this epoch's blobs go up first.
            for (epoch, n, path) in &blobs {
                if *epoch == seg.header.epoch && path.exists() {
                    let key = Key::Blob {
                        epoch: *epoch,
                        n: *n,
                    };
                    client.upload_file(&key, path).await?;
                    let _ = cache.adopt(&key.to_string(), path);
                }
            }
            let bytes = Bytes::from(seg.object_bytes()?);
            if client.put_new(&Key::Segment(seq), bytes.clone()).await? == Created::AlreadyExists
                && client.get(&Key::Segment(seq)).await?.as_ref() != Some(&bytes)
            {
                return Err(VolumeError::Fenced(format!(
                    "segment {seq} was taken during recovery"
                )));
            }
            apply_segment(&mut live, seq, seg.header.epoch, &seg.records, &seg.digest)?;
            data.set_data_offset(seq, seg.data_offset());
            seg.retire();
            let _ = cache.adopt(&segment_data_id(seq), &seg.data_path());
            tail.push(CommittedSegment::from_sealed(&seg));
            report.recovered_segments += 1;
        } else {
            continuing = false;
            lost_files.extend([wal::rec_path(&d.wal, seq), seg.data_path()]);
            report.lost_segments += 1;
        }
    }
    if !cfg.read_only {
        for (_, _, path) in wal::local_blobs(&d.wal)? {
            lost_files.push(path);
        }
    }
    if !lost_files.is_empty() {
        tracing::warn!(segments = report.lost_segments, dir = %d.lost.display(), "local WAL could not be recovered; kept for inspection");
        move_lost(&d.lost, &lost_files);
    }

    // 5. Claim a new epoch.
    let head = chain.next_seq.checked_sub(1).ok_or_else(not_found)?;
    let previous_clean = match &chain.last {
        Some(op) => matches!(op, Op::MountEnd(_)),
        // Nothing after the snapshot: it knows how the last mount ended.
        None => live.position()?.clean,
    };
    let last_op_id = chain.next_op_id - 1;
    let epoch = live.position()?.epoch.max(chain.epoch) + u64::from(!cfg.read_only);
    report.previous_clean = previous_clean;
    report.epoch = epoch;
    let pos = LogPosition {
        next_seq: head + 1,
        next_op_id: chain.next_op_id,
        epoch,
        prev_digest: chain.digest.clone(),
        snapshot_gen: manifest.r#gen,
    };
    let inline_max = u64::from(live.volume()?.ok_or_else(not_found)?.inline_data_max);
    let (restore_tx, restore_rx) = mpsc::unbounded_channel();
    let (commit, _) = watch::channel(CommitState {
        committed: Some(head),
        ..CommitState::default()
    });
    let shared = Arc::new(Shared {
        upload_slots: Arc::new(Semaphore::new(cfg.blob_uploads.max(1))),
        cfg,
        client,
        cache,
        data,
        wal_dir: d.wal,
        scratch: d.scratch,
        volume_id,
        inline_max,
        state: Mutex::new(State::new(live, &pos)),
        kick: tokio::sync::Notify::new(),
        sync_requested: AtomicBool::new(false),
        commit,
        uploads: Mutex::new(HashMap::new()),
        restore_tx,
        abort: AtomicBool::new(false),
        rt,
        local_lock,
    });
    let Some(replica) = replica else {
        // Read-only: no epoch claimed, nothing logged, no restorer.
        return Ok(Mounted {
            shared,
            restorer: None,
            restore_rx,
            report,
        });
    };
    {
        let mut st = shared.state.lock();
        shared
            .log_op(
                &mut st,
                Op::MountBegin(MountBegin {
                    epoch,
                    writer_id: shared.cfg.writer_id.as_bytes().to_vec(),
                    previous_clean,
                    lost_after_op_id: if previous_clean { 0 } else { last_op_id },
                    base_snapshot_gen: manifest.r#gen,
                }),
            )
            .map_err(|e| VolumeError::Io(e.into()))?;
        shared
            .seal(&mut st)
            .map_err(|e| VolumeError::Io(e.into()))?;
    }

    let restorer = Restorer::new(
        shared.clone(),
        replica,
        Base {
            generation: manifest.r#gen,
            restored_seq: manifest.restored_seq,
            created_ns: manifest.created_ns,
        },
        tail,
    )?;
    Ok(Mounted {
        shared,
        restorer: Some(restorer),
        restore_rx,
        report,
    })
}

/// Offline check (`skyfs fsck`): replays the volume from the bucket into a
/// scratch directory and checks every structural invariant. Returns the
/// problems found (empty when healthy) and the head seq.
pub async fn fsck(
    client: &ObjectClient,
    volume_id: [u8; 16],
    scratch: &Path,
) -> Result<(Vec<String>, u64), VolumeError> {
    let r = replay(client, volume_id, scratch, false).await?;
    let head = r.chain.next_seq.saturating_sub(1);
    Ok((r.live.fsck()?, head))
}
