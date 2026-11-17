//! The writer: live metadata, the open segment, large-write blobs and file
//! handles, behind one lock.
//!
//! Every mutation goes "validate, then log, then apply": a `plan` function
//! validates and builds the operation, then [`Shared::log_op`] applies it
//! to the live store and appends it to the open segment in one step (the
//! SQLite transaction commits only after the append succeeds).

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use skyfs_format::wal::v1::{Record, SegmentHeader, record::Op};
use skyfs_meta::{Errno, Ino, MetaStore, plan};
use skyfs_store::{Cache, Key, ObjectClient};
use tokio::sync::{Notify, Semaphore, mpsc, watch};

use crate::config::VolumeConfig;
use crate::data::DataReader;
use crate::restorer::RestoreMsg;
use crate::snapshot::now_ns;
use crate::wal::{self, OpenSegment, Sealed};

/// How far the log has been committed to the bucket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitState {
    /// Highest committed seq.
    pub committed: Option<u64>,
    /// Another writer took the volume; nothing more will commit.
    pub fenced: bool,
    /// The mount was abandoned (process exit, or an operator giving up on
    /// an unmount the bucket cannot complete). Nothing more will commit
    /// from this process; the local WAL stays for the next mount on this
    /// disk.
    pub aborted: bool,
}

/// A sequential write run on one inode, to route large writes to blobs.
#[derive(Debug, Clone, Default)]
struct Run {
    end: u64,
    len: u64,
    blob: Option<String>,
}

#[derive(Debug)]
struct OpenBlob {
    file: File,
    len: u64,
    path: PathBuf,
}

/// An open file or directory handle.
#[derive(Debug, Clone, Copy)]
pub struct Handle {
    /// The inode.
    pub ino: Ino,
    /// Written through this handle (close waits in sync mode).
    pub wrote: bool,
}

/// Everything behind the writer lock.
#[derive(Debug)]
pub struct State {
    /// The live metadata store.
    pub meta: MetaStore,
    open: Option<OpenSegment>,
    /// Sealed segments not yet committed, in seq order.
    pub sealed: VecDeque<Arc<Sealed>>,
    next_seq: u64,
    next_op_id: u64,
    epoch: u64,
    prev_digest: Vec<u8>,
    /// Seq of the segment holding the latest logged record.
    pub last_seq: Option<u64>,
    blobs: HashMap<String, OpenBlob>,
    next_blob: u64,
    runs: HashMap<Ino, Run>,
    handles: HashMap<u64, Handle>,
    open_counts: HashMap<Ino, u32>,
    next_fh: u64,
    /// A validated operation failed to apply: a bug. Refuse writes.
    broken: bool,
    /// Unmounting: refuse new mutations.
    pub closing: bool,
    snapshot_gen_hint: u64,
}

/// Where the log resumes when a writer starts.
#[derive(Debug, Clone)]
pub struct LogPosition {
    /// Seq of the next segment.
    pub next_seq: u64,
    /// Op id of the next record.
    pub next_op_id: u64,
    /// This mount's epoch.
    pub epoch: u64,
    /// Digest of the last committed segment.
    pub prev_digest: Vec<u8>,
    /// Latest snapshot generation known.
    pub snapshot_gen: u64,
}

impl State {
    /// A writer at `pos` over `meta`.
    #[must_use]
    pub fn new(meta: MetaStore, pos: &LogPosition) -> Self {
        Self {
            meta,
            open: None,
            sealed: VecDeque::new(),
            next_seq: pos.next_seq,
            next_op_id: pos.next_op_id,
            epoch: pos.epoch,
            prev_digest: pos.prev_digest.clone(),
            last_seq: None,
            blobs: HashMap::new(),
            next_blob: 0,
            runs: HashMap::new(),
            handles: HashMap::new(),
            open_counts: HashMap::new(),
            next_fh: 1,
            broken: false,
            closing: false,
            snapshot_gen_hint: pos.snapshot_gen,
        }
    }

    /// This mount's epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The handle `fh`.
    pub fn handle(&self, fh: u64) -> Result<Handle, Errno> {
        self.handles.get(&fh).copied().ok_or(Errno::EBADF)
    }

    /// Whether `ino` has open handles.
    #[must_use]
    pub fn is_open(&self, ino: Ino) -> bool {
        self.open_counts.get(&ino).is_some_and(|&n| n > 0)
    }

    /// Whether the open segment holds records due for sealing.
    pub(crate) fn open_due(&self, max_lag: Duration, force: bool) -> bool {
        self.open.as_ref().is_some_and(|o| {
            o.record_count() > 0 && (force || o.first_at.is_some_and(|t| t.elapsed() >= max_lag))
        })
    }

    /// When the open segment's replication deadline falls, if it has records.
    pub(crate) fn open_deadline(&self, max_lag: Duration) -> Option<std::time::Instant> {
        self.open
            .as_ref()
            .and_then(|o| o.first_at)
            .map(|t| t + max_lag)
    }

    pub(crate) fn open_sync_handles(&self) -> Option<(File, File)> {
        self.open
            .as_ref()
            .filter(|o| o.record_count() > 0)
            .and_then(|o| o.sync_handles().ok())
    }
}

/// State shared by the API, the committer and the restorer.
#[derive(Debug)]
pub struct Shared {
    /// Mount options.
    pub cfg: VolumeConfig,
    /// The bucket.
    pub client: ObjectClient,
    /// Local cache.
    pub cache: Arc<Cache>,
    /// Read path.
    pub data: DataReader,
    /// Local WAL directory.
    pub wal_dir: PathBuf,
    /// Scratch directory for snapshot exports.
    pub scratch: PathBuf,
    /// Volume id.
    pub volume_id: [u8; 16],
    /// Writes in a run beyond this go to blobs.
    pub inline_max: u64,
    /// The writer lock.
    pub state: Mutex<State>,
    /// Wakes the committer.
    pub kick: Notify,
    /// A caller is waiting for durability: seal now.
    pub sync_requested: AtomicBool,
    /// Commit progress.
    pub commit: watch::Sender<CommitState>,
    /// Blob uploads in flight: key -> done.
    pub uploads: Mutex<HashMap<String, watch::Receiver<bool>>>,
    /// Bounds concurrent blob uploads.
    pub upload_slots: Arc<Semaphore>,
    /// Committed segments for the restorer.
    pub restore_tx: mpsc::UnboundedSender<RestoreMsg>,
    /// Stop retry loops (process exit).
    pub abort: AtomicBool,
    /// The engine's runtime, for spawning from FUSE threads.
    pub rt: tokio::runtime::Handle,
    /// Held while mounted: this process owns the local directory.
    pub local_lock: File,
}

/// Retry delays for bucket operations that must eventually succeed.
pub(crate) struct Backoff(Duration);

impl Backoff {
    pub(crate) fn new() -> Self {
        Self(Duration::from_millis(100))
    }
    pub(crate) async fn wait(&mut self) {
        tokio::time::sleep(self.0).await;
        self.0 = (self.0 * 2).min(Duration::from_secs(30));
    }
}

impl Shared {
    /// The commit state now.
    pub fn commit_state(&self) -> CommitState {
        *self.commit.borrow()
    }

    fn writable(&self, st: &State) -> Result<(), Errno> {
        let c = self.commit_state();
        if st.broken || c.fenced || c.aborted {
            return Err(Errno::EIO);
        }
        if st.closing || self.cfg.read_only {
            return Err(Errno::EROFS);
        }
        Ok(())
    }

    /// Abandons the mount: retry loops stop at their next failure, and
    /// every waiter and later mutation fails with EIO now.
    pub fn abort(&self) {
        self.abort.store(true, Ordering::SeqCst);
        self.commit.send_modify(|c| c.aborted = true);
        self.kick.notify_one();
    }

    /// Fails unless mutations are accepted.
    pub fn check_writable(&self, st: &State) -> Result<(), Errno> {
        self.writable(st)
    }

    fn ensure_open(&self, st: &mut State) -> Result<(), Errno> {
        if st.open.is_none() {
            let header = SegmentHeader {
                volume_id: self.volume_id.to_vec(),
                seq: st.next_seq,
                epoch: st.epoch,
                prev_digest: st.prev_digest.clone(),
                first_op_id: st.next_op_id,
                record_count: 0,
                snapshot_gen_hint: st.snapshot_gen_hint,
            };
            st.open = Some(OpenSegment::create(&self.wal_dir, header).map_err(io_errno)?);
        }
        Ok(())
    }

    /// Appends inline bytes to the open segment, for the `Write` that
    /// follows. Returns the data-region offset.
    pub fn push_inline(&self, st: &mut State, bytes: &[u8]) -> Result<u64, Errno> {
        self.writable(st)?;
        self.ensure_open(st)?;
        st.open
            .as_mut()
            .expect("opened")
            .push_data(bytes)
            .map_err(io_errno)
    }

    /// Validated `op` → record → applied and appended. Returns the seq of
    /// the segment holding it.
    pub fn log_op(self: &Arc<Self>, st: &mut State, op: Op) -> Result<u64, Errno> {
        self.writable(st)?;
        self.ensure_open(st)?;
        let record = Record {
            op_id: st.next_op_id,
            time_ns: now_ns(),
            op: Some(op),
        };
        let State { meta, open, .. } = st;
        let open = open.as_mut().expect("opened");
        let seq = open.seq();
        match meta.apply_logged(&record, seq, || open.push_record(&record)) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(io_errno(e)),
            Err(e) => {
                tracing::error!(error = %e, "a validated operation failed to apply; refusing writes");
                st.broken = true;
                return Err(Errno::EIO);
            }
        }
        st.next_op_id += 1;
        st.last_seq = Some(seq);
        if st
            .open
            .as_ref()
            .is_some_and(|o| o.size() >= self.cfg.segment_max)
        {
            self.seal(st)?;
        }
        Ok(seq)
    }

    /// Seals the open segment if it holds records. Every open blob is
    /// closed first: the segment may reference it, and commits only after
    /// its blobs are uploaded.
    pub fn seal(self: &Arc<Self>, st: &mut State) -> Result<(), Errno> {
        if st.open.as_ref().is_none_or(|o| o.record_count() == 0) {
            return Ok(());
        }
        let keys: Vec<String> = st.blobs.keys().cloned().collect();
        for key in keys {
            self.close_blob(st, &key);
        }
        for run in st.runs.values_mut() {
            run.blob = None;
        }
        let open = st.open.take().expect("checked");
        let sealed = match open.seal() {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "sealing a segment failed; refusing writes");
                st.broken = true;
                return Err(Errno::EIO);
            }
        };
        st.prev_digest = sealed.digest.to_vec();
        st.next_seq = sealed.seq() + 1;
        st.sealed.push_back(Arc::new(sealed));
        self.kick.notify_one();
        Ok(())
    }

    // ---------------------------------------------------------------- data

    /// Places the bytes of a write at `offset` of `ino`: inline in the open
    /// segment, or, once a sequential run exceeds `inline_data_max`, in a
    /// blob. Returns where they went.
    pub fn place_write(
        self: &Arc<Self>,
        st: &mut State,
        ino: Ino,
        offset: u64,
        bytes: &[u8],
    ) -> Result<plan::WriteData, Errno> {
        let n = bytes.len() as u64;
        let run = st.runs.entry(ino).or_default().clone();
        let mut run = if run.end == offset {
            run
        } else {
            if let Some(k) = &run.blob {
                self.close_blob(st, k);
            }
            Run {
                end: offset,
                ..Run::default()
            }
        };
        let data = if run.blob.is_some() || run.len + n > self.inline_max {
            let key = match &run.blob {
                Some(k)
                    if st
                        .blobs
                        .get(k)
                        .is_some_and(|b| b.len + n <= self.cfg.blob_max) =>
                {
                    k.clone()
                }
                stale => {
                    if let Some(k) = stale {
                        self.close_blob(st, k);
                    }
                    self.new_blob(st)?
                }
            };
            let blob = st.blobs.get_mut(&key).expect("open blob");
            let off = blob.len;
            blob.file.write_all(bytes).map_err(io_errno)?;
            blob.len += n;
            run.blob = Some(key.clone());
            plan::WriteData::Blob { key, offset: off }
        } else {
            plan::WriteData::Inline {
                offset: self.push_inline(st, bytes)?,
            }
        };
        run.end = offset + n;
        run.len += n;
        st.runs.insert(ino, run);
        Ok(data)
    }

    fn new_blob(&self, st: &mut State) -> Result<String, Errno> {
        let n = st.next_blob;
        st.next_blob += 1;
        let path = wal::blob_path(&self.wal_dir, st.epoch, n);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(io_errno)?;
        let key = Key::Blob { epoch: st.epoch, n }.to_string();
        st.blobs
            .insert(key.clone(), OpenBlob { file, len: 0, path });
        Ok(key)
    }

    /// Ends the write run on `ino`, starting its blob's upload early.
    pub fn end_run(self: &Arc<Self>, st: &mut State, ino: Ino) {
        if let Some(Run { blob: Some(k), .. }) = st.runs.remove(&ino) {
            self.close_blob(st, &k);
        }
    }

    /// Closes a blob and uploads it in the background, with retries. The
    /// segment referencing it waits for the upload (data before reference).
    fn close_blob(self: &Arc<Self>, st: &mut State, key: &str) {
        let Some(blob) = st.blobs.remove(key) else {
            return;
        };
        for run in st.runs.values_mut() {
            if run.blob.as_deref() == Some(key) {
                run.blob = None;
            }
        }
        let _ = crate::wal::sync_data(&blob.file);
        drop(blob.file);
        let (tx, rx) = watch::channel(false);
        self.uploads.lock().insert(key.to_owned(), rx);
        let me = self.clone();
        let key_s = key.to_owned();
        let path = blob.path;
        self.rt.spawn(async move {
            me.upload_blob(&key_s, &path).await;
            let _ = tx.send(true);
        });
    }

    /// Uploads a local blob file until it succeeds, then hands it to the cache.
    pub(crate) async fn upload_blob(&self, key: &str, path: &std::path::Path) {
        let Ok(k) = key.parse::<Key>() else { return };
        let _slot = self.upload_slots.acquire().await;
        let mut backoff = Backoff::new();
        loop {
            match self.client.upload_file(&k, path).await {
                Ok(()) => break,
                Err(e) if !self.abort.load(Ordering::Relaxed) => {
                    tracing::warn!(error = %e, key, "blob upload failed; retrying");
                    backoff.wait().await;
                }
                Err(_) => return,
            }
        }
        if let Err(e) = self.cache.adopt(key, path) {
            tracing::warn!(error = %e, key, "could not move an uploaded blob into the cache");
        }
    }

    // ------------------------------------------------------------ handles

    /// Registers an open handle on `ino`.
    pub fn open_handle(&self, st: &mut State, ino: Ino) -> u64 {
        let fh = st.next_fh;
        st.next_fh += 1;
        st.handles.insert(fh, Handle { ino, wrote: false });
        *st.open_counts.entry(ino).or_default() += 1;
        fh
    }

    /// Marks `fh` as having written.
    pub fn mark_wrote(st: &mut State, fh: u64) {
        if let Some(h) = st.handles.get_mut(&fh) {
            h.wrote = true;
        }
    }

    /// Drops handle `fh`; frees the inode if it was its last handle and it
    /// is unlinked.
    pub fn release_handle(self: &Arc<Self>, st: &mut State, fh: u64) -> Result<Handle, Errno> {
        let h = st.handles.remove(&fh).ok_or(Errno::EBADF)?;
        let count = st.open_counts.entry(h.ino).or_default();
        *count = count.saturating_sub(1);
        if *count == 0 {
            st.open_counts.remove(&h.ino);
            self.end_run(st, h.ino);
            self.reclaim_if_unused(st, h.ino)?;
        }
        Ok(h)
    }

    /// Frees `ino` if it is at nlink 0 and nothing has it open.
    pub fn reclaim_if_unused(self: &Arc<Self>, st: &mut State, ino: Ino) -> Result<(), Errno> {
        if st.is_open(ino) {
            return Ok(());
        }
        if let Some(op) = plan::reclaim(&st.meta, ino)? {
            self.end_run(st, ino);
            self.log_op(st, op)?;
        }
        Ok(())
    }

    // --------------------------------------------------------- durability

    /// Waits until segment `seq` is committed.
    pub async fn wait_committed(&self, seq: u64) -> Result<(), Errno> {
        self.sync_requested.store(true, Ordering::SeqCst);
        self.kick.notify_one();
        let mut rx = self.commit.subscribe();
        loop {
            let c = *rx.borrow_and_update();
            if c.committed.is_some_and(|s| s >= seq) {
                return Ok(());
            }
            if c.fenced || c.aborted {
                return Err(Errno::EIO);
            }
            if rx.changed().await.is_err() {
                return Err(Errno::EIO);
            }
        }
    }

    /// `fdatasync` of the open segment: local durability of everything
    /// logged so far.
    pub fn sync_local(&self) -> Result<(), Errno> {
        let handles = self.state.lock().open_sync_handles();
        if let Some((dat, rec)) = handles {
            crate::wal::sync_data(&dat).map_err(io_errno)?;
            crate::wal::sync_data(&rec).map_err(io_errno)?;
        }
        Ok(())
    }
}

/// Maps a local I/O failure to the errno the caller sees.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn io_errno(e: std::io::Error) -> Errno {
    match e.raw_os_error() {
        Some(libc::ENOSPC) => Errno::ENOSPC,
        _ => Errno::EIO,
    }
}
