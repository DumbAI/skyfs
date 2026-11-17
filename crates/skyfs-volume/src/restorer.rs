//! The restorer (design §7.2): folds committed segments into a new
//! snapshot, moving the restore pointer, then collects garbage.
//!
//! A pass applies committed segments to the restore replica (a second
//! SQLite holding the snapshot state) with the same `apply` as the live
//! mount, repacks inline data into content-addressed packs (and rewrites
//! packs that fell below half live), writes the metadata file, and finally
//! the manifest. The manifest PUT moves the pointer; everything before it
//! is invisible until then, so a pass killed at any step loses nothing.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use skyfs_format::snapshot::v1::SnapshotManifest;
use skyfs_format::wal::v1::Record;
use skyfs_meta::{Ino, MetaStore, Relocation};
use skyfs_store::{Key, hex};
use tokio::sync::{mpsc, oneshot};

use crate::data::ReadError;
use crate::error::VolumeError;
use crate::snapshot::{self, Hint, SnapshotId, now_ns};
use crate::wal::Sealed;
use crate::writer::Shared;

/// A segment committed to the bucket, with its decoded records.
#[derive(Debug, Clone)]
pub struct CommittedSegment {
    /// Seq.
    pub seq: u64,
    /// Writer epoch.
    pub epoch: u64,
    /// BLAKE3 of its record region.
    pub digest: [u8; 32],
    /// Its records.
    pub records: Arc<Vec<Record>>,
    /// Size of its record region.
    pub record_bytes: u64,
}

impl CommittedSegment {
    /// From a segment this mount sealed and committed.
    #[must_use]
    pub fn from_sealed(s: &Sealed) -> Self {
        Self {
            seq: s.seq(),
            epoch: s.header.epoch,
            digest: s.digest,
            records: s.records.clone(),
            record_bytes: s.region.len() as u64,
        }
    }
}

/// Messages to the restorer task.
#[derive(Debug)]
pub enum RestoreMsg {
    /// A segment was committed.
    Segment(CommittedSegment),
    /// Restore everything received so far, until `deadline`; answers
    /// whether the pointer reached the head.
    Drain {
        /// Give up after this.
        deadline: Instant,
        /// Reply.
        done: oneshot::Sender<bool>,
    },
}

/// A snapshot kept for point-in-time restore.
#[derive(Debug, Clone)]
struct Retained {
    generation: u64,
    created_ns: i64,
    restored_seq: u64,
    meta_key: String,
}

/// The snapshot a mount started from: GC keeps what it references.
#[derive(Debug, Clone, Copy)]
pub struct Base {
    /// Its generation.
    pub generation: u64,
    /// Its restore pointer.
    pub restored_seq: u64,
    /// When it was written.
    pub created_ns: i64,
}

/// Chunk reads in flight while repacking.
const REPACK_READS: usize = 16;

/// The background restorer.
pub struct Restorer {
    shared: Arc<Shared>,
    replica: parking_lot::Mutex<MetaStore>,
    generation: u64,
    base: Base,
    pending: VecDeque<CommittedSegment>,
    pending_bytes: u64,
    oldest: Option<Instant>,
    /// Applied to the replica but not yet in a published snapshot.
    unpublished: bool,
    /// A manifest whose PUT failed: it may have landed with its response
    /// lost, so it is retried byte for byte before anything else changes
    /// the replica.
    inflight: Option<SnapshotManifest>,
    refs: BTreeSet<String>,
    retained: VecDeque<Retained>,
    retained_loaded: bool,
    low_seq: Option<u64>,
    stopped: bool,
    retry_at: Option<Instant>,
}

impl std::fmt::Debug for Restorer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Restorer")
            .field("generation", &self.generation)
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl Restorer {
    /// A restorer whose replica holds snapshot `base` exactly; `tail` are
    /// the committed segments after it, already fetched by the mount.
    pub fn new(
        shared: Arc<Shared>,
        replica: MetaStore,
        base: Base,
        tail: Vec<CommittedSegment>,
    ) -> Result<Self, VolumeError> {
        let refs = replica.referenced_objects()?;
        let mut r = Self {
            shared,
            replica: parking_lot::Mutex::new(replica),
            generation: base.generation,
            base,
            pending: VecDeque::new(),
            pending_bytes: 0,
            oldest: None,
            unpublished: false,
            inflight: None,
            refs,
            retained: VecDeque::new(),
            retained_loaded: false,
            low_seq: None,
            stopped: false,
            retry_at: None,
        };
        for seg in tail {
            r.push(seg);
        }
        Ok(r)
    }

    fn push(&mut self, seg: CommittedSegment) {
        self.pending_bytes += seg.record_bytes;
        self.oldest.get_or_insert_with(Instant::now);
        self.pending.push_back(seg);
    }

    fn due(&self) -> bool {
        let cfg = &self.shared.cfg;
        if self.retry_at.is_some_and(|t| Instant::now() < t) {
            return false;
        }
        !self.pending.is_empty()
            && (self.pending_bytes >= cfg.restore_bytes
                || self.pending.len() >= cfg.restore_segments
                || self
                    .oldest
                    .is_some_and(|t| t.elapsed() >= cfg.restore_after))
            || (self.unpublished && self.retry_at.is_some())
    }

    fn next_wake(&self) -> Option<Instant> {
        let by_age = self.oldest.map(|t| t + self.shared.cfg.restore_after);
        match (by_age, self.retry_at) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }

    /// Runs until the channel closes.
    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<RestoreMsg>) {
        loop {
            // A backlog larger than one pass continues at once.
            let wake = if self.due() && !self.stopped {
                Some(Instant::now())
            } else {
                self.next_wake()
            };
            let msg = tokio::select! {
                m = rx.recv() => match m {
                    Some(m) => Some(m),
                    None => return,
                },
                () = sleep_until(wake) => None,
            };
            match msg {
                Some(RestoreMsg::Segment(s)) => self.push(s),
                Some(RestoreMsg::Drain { deadline, done }) => {
                    // Segments sent before the drain are already queued.
                    while let Ok(RestoreMsg::Segment(s)) = rx.try_recv() {
                        self.push(s);
                    }
                    let complete = self.drain(deadline).await;
                    let _ = done.send(complete);
                    continue;
                }
                None => {}
            }
            if self.due() && !self.stopped {
                self.attempt().await;
            }
        }
    }

    /// Stops for good on errors no retry can fix: another writer owns the
    /// volume, or the replica cannot follow the log. Returns whether it
    /// stopped.
    fn stop_if_fatal(&mut self, e: &VolumeError) -> bool {
        match e {
            VolumeError::Fenced(why) => {
                tracing::error!(why, "restorer stopped: another writer owns the volume");
            }
            VolumeError::Apply(_) | VolumeError::Corrupt(_) => {
                tracing::error!(error = %e, "restorer stopped: the replica cannot follow the log");
            }
            _ => return false,
        }
        self.stopped = true;
        true
    }

    async fn attempt(&mut self) {
        match self.pass().await {
            Ok(()) => self.retry_at = None,
            Err(e) if self.stop_if_fatal(&e) => {}
            Err(e) => {
                tracing::warn!(error = %e, "restore pass failed; will retry");
                self.retry_at = Some(Instant::now() + Duration::from_secs(10));
            }
        }
    }

    /// Restores until the pointer reaches everything received, or
    /// `deadline`. Transient bucket errors are retried with backoff while
    /// time remains.
    async fn drain(&mut self, deadline: Instant) -> bool {
        let mut backoff = Duration::from_millis(50);
        while !self.stopped && (!self.pending.is_empty() || self.unpublished) {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() || self.shared.abort.load(Ordering::Relaxed) {
                return false;
            }
            match tokio::time::timeout(left, self.pass()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) if self.stop_if_fatal(&e) => return false,
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "restore at unmount failed; retrying");
                    tokio::time::sleep(backoff.min(left)).await;
                    backoff = (backoff * 2).min(Duration::from_secs(2));
                }
                Err(_) => return false,
            }
        }
        !self.stopped
    }

    /// One pass: apply, repack, publish, collect.
    async fn pass(&mut self) -> Result<(), VolumeError> {
        if let Some(m) = self.inflight.clone() {
            self.commit(m).await?;
        }
        // At most one batch per pass: a long backlog (a tail replayed at
        // mount, a slow uplink) then advances the pointer in published
        // steps, so a mount that ends early still keeps its progress.
        //
        // Cancel-safe: a segment leaves the queue only once applied, and
        // the yield between segments lets the shutdown deadline stop the
        // pass there; the next pass publishes what was applied.
        let mut n = 0;
        while n < self.shared.cfg.restore_batch {
            let Some(seg) = self.pending.front() else {
                break;
            };
            {
                let mut replica = self.replica.lock();
                for r in seg.records.iter() {
                    replica.apply(r, seg.seq)?;
                }
                replica.finish_segment(seg.seq, &seg.digest, seg.epoch)?;
            }
            let seg = self.pending.pop_front().expect("peeked");
            self.pending_bytes = self.pending_bytes.saturating_sub(seg.record_bytes);
            self.unpublished = true;
            n += 1;
            tokio::task::yield_now().await;
        }
        if self.pending.is_empty() {
            self.pending_bytes = 0;
            self.oldest = None;
        }
        if !self.unpublished {
            return Ok(());
        }

        self.repack().await?;
        let next = self.generation + 1;
        let shared = self.shared.clone();
        let exported = snapshot::export(&self.replica.lock(), &shared.scratch, next)?;
        let m = snapshot::upload_meta(
            &shared.client,
            exported,
            SnapshotId {
                generation: next,
                parent_gen: self.generation,
                volume_id: &shared.volume_id,
                restorer_id: &shared.cfg.writer_id,
            },
        )
        .await?;
        self.commit(m).await
    }

    /// Commits the manifest of the replica's current state (the restore
    /// pointer moves), then tracks garbage and collects.
    async fn commit(&mut self, m: SnapshotManifest) -> Result<(), VolumeError> {
        let shared = self.shared.clone();
        self.inflight = Some(m.clone());
        if !snapshot::commit_manifest(&shared.client, &m).await? {
            self.inflight = None;
            return Err(VolumeError::Fenced(format!(
                "snapshot {} already exists",
                m.r#gen
            )));
        }
        self.inflight = None;
        self.generation = m.r#gen;
        self.unpublished = false;
        tracing::info!(
            generation = m.r#gen,
            restored_seq = m.restored_seq,
            "restore pointer moved"
        );
        snapshot::write_hint(
            &shared.client,
            Hint {
                snapshot_gen: m.r#gen,
                head_seq: m.restored_seq,
            },
        )
        .await;
        self.retained.push_back(Retained {
            generation: m.r#gen,
            created_ns: m.created_ns,
            restored_seq: m.restored_seq,
            meta_key: m.meta_key.clone(),
        });
        self.track_garbage(m.r#gen, m.created_ns)?;
        if let Err(e) = self.collect().await {
            tracing::warn!(error = %e, "garbage collection failed; will retry after the next pass");
        }
        Ok(())
    }

    /// Moves inline extents into packs, and rewrites packs under half live.
    async fn repack(&mut self) -> Result<(), VolumeError> {
        let todo: Vec<(Ino, skyfs_meta::Extent)> = {
            let mut replica = self.replica.lock();
            let mut todo = replica.inline_extents()?;
            for (key, size, live) in replica.pack_usage()? {
                if live.saturating_mul(2) < size {
                    todo.extend(replica.extents_in_object(&key)?);
                }
            }
            todo
        };
        let mut buf: Vec<u8> = Vec::new();
        let mut moves: Vec<Relocation> = Vec::new();
        let shared = self.shared.clone();
        let mut reads = futures::stream::iter(todo)
            .map(|(ino, e)| {
                let shared = shared.clone();
                async move {
                    let bytes = shared.data.extent(&e).await;
                    (ino, e, bytes)
                }
            })
            .buffered(REPACK_READS);
        while let Some((ino, e, bytes)) = reads.next().await {
            let bytes = bytes.map_err(|err| match err {
                // Unreachable bucket: retry later. Anything else is bytes
                // that fail verification.
                ReadError::Store(s) => VolumeError::Store(s),
                other => VolumeError::Corrupt(other.to_string()),
            })?;
            moves.push(Relocation {
                ino,
                off: e.off,
                len: e.len,
                key: String::new(),
                chunk_off: buf.len() as u64,
                crc: crc32c::crc32c(&bytes),
            });
            buf.extend_from_slice(&bytes);
            if buf.len() as u64 >= self.shared.cfg.pack_size {
                self.flush_pack(&mut buf, &mut moves).await?;
            }
        }
        if !buf.is_empty() {
            self.flush_pack(&mut buf, &mut moves).await?;
        }
        Ok(())
    }

    async fn flush_pack(
        &mut self,
        buf: &mut Vec<u8>,
        moves: &mut Vec<Relocation>,
    ) -> Result<(), VolumeError> {
        let bytes = std::mem::take(buf);
        let key = Key::Pack(*blake3::hash(&bytes).as_bytes());
        let len = bytes.len() as u64;
        // Content-addressed: an existing pack holds these exact bytes.
        self.shared.client.put_new(&key, Bytes::from(bytes)).await?;
        let key_s = key.to_string();
        for m in moves.iter_mut() {
            m.key.clone_from(&key_s);
        }
        self.replica.lock().relocate(&key_s, len, moves)?;
        moves.clear();
        Ok(())
    }

    fn track_garbage(&mut self, generation: u64, time_ns: i64) -> Result<(), VolumeError> {
        let now = self.replica.lock().referenced_objects()?;
        let dropped: Vec<String> = self.refs.difference(&now).cloned().collect();
        let revived: Vec<String> = now.difference(&self.refs).cloned().collect();
        // Revived keys are rare (identical pack content); new keys are the
        // usual case and simply not in the table.
        self.replica
            .lock()
            .track_garbage(&dropped, &revived, generation, time_ns)?;
        self.refs = now;
        Ok(())
    }

    /// Snapshot expiry, log truncation and object deletion. No LIST.
    async fn collect(&mut self) -> Result<(), VolumeError> {
        let client = self.shared.client.clone();
        let retention = i64::try_from(self.shared.cfg.retention.as_nanos()).unwrap_or(i64::MAX);
        let cutoff = now_ns().saturating_sub(retention);
        if !self.retained_loaded {
            self.load_retained(cutoff).await?;
        }

        // Expire snapshots past retention, keeping the latest and the base.
        while let Some(front) = self.retained.front() {
            let keep = front.generation == self.generation
                || front.generation == self.base.generation
                || front.created_ns >= cutoff;
            if keep {
                break;
            }
            if let Ok(k) = front.meta_key.parse::<Key>() {
                client.delete(&k).await?;
            }
            self.retained.pop_front();
        }

        // Truncate the log below what every retained snapshot and this
        // mount's base still need.
        let oldest = self
            .retained
            .front()
            .map_or(self.base.restored_seq, |r| r.restored_seq);
        let bound = oldest.min(self.base.restored_seq);
        let low = match self.low_seq {
            Some(l) => l,
            None => self.find_low(bound).await?,
        };
        for seq in low..=bound {
            client.delete(&Key::Segment(seq)).await?;
        }
        self.low_seq = Some(low.max(bound + 1));

        // Objects unreferenced since before the window, and after the base.
        let dead = self
            .replica
            .lock()
            .collectable(cutoff, self.base.generation)?;
        for key in &dead {
            if let Ok(k) = key.parse::<Key>() {
                client.delete(&k).await?;
            }
        }
        if !dead.is_empty() {
            tracing::info!(
                objects = dead.len(),
                "collected unreferenced packs and blobs"
            );
            self.replica.lock().forget_garbage(&dead)?;
        }
        Ok(())
    }

    /// Reads manifests back from the latest while they are within the
    /// retention window (bounded), to know what point-in-time restore keeps.
    async fn load_retained(&mut self, cutoff: i64) -> Result<(), VolumeError> {
        let client = self.shared.client.clone();
        let mut found = VecDeque::new();
        let mut g = self.generation;
        for _ in 0..4096 {
            let m = snapshot::read_manifest(&client, g, &self.shared.volume_id).await?;
            let r = Retained {
                generation: g,
                created_ns: m.created_ns,
                restored_seq: m.restored_seq,
                meta_key: m.meta_key,
            };
            let expired =
                r.created_ns < cutoff && g != self.generation && g != self.base.generation;
            found.push_front(r);
            if expired || g == 0 {
                break;
            }
            g -= 1;
        }
        // Keep newer entries this mount already added.
        for r in self.retained.drain(..) {
            if found.back().is_none_or(|b| b.generation < r.generation) {
                found.push_back(r);
            }
        }
        self.retained = found;
        self.retained_loaded = true;
        Ok(())
    }

    /// The lowest segment still in the bucket at or below `bound`, found by
    /// probing downward (once per mount).
    async fn find_low(&self, bound: u64) -> Result<u64, VolumeError> {
        let client = &self.shared.client;
        let mut low = bound + 1;
        while low > 0 && client.exists(&Key::Segment(low - 1)).await? {
            low -= 1;
            if bound + 1 - low > 1_000_000 {
                break;
            }
        }
        Ok(low)
    }
}

async fn sleep_until(t: Option<Instant>) {
    match t {
        Some(t) => tokio::time::sleep_until(t.into()).await,
        None => futures::future::pending().await,
    }
}

/// The restorer id of a snapshot written outside a mount.
#[must_use]
pub fn tool_id(volume_id: &[u8; 16]) -> String {
    format!("skyfs-tool/{}", hex(volume_id))
}
