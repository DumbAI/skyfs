//! The mounted volume: the POSIX operations the FUSE layer calls.
//!
//! Every call answers from local state. A mutating call returns a
//! [`Done`] whose `wait` names the segment that must be committed before
//! the caller may be told it succeeded, when the replication mode demands
//! it (design §6.4); the FUSE layer waits off its dispatch thread.

use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use skyfs_format::wal::v1::{FallocateMode, MountEnd, RenameMode, XattrMode, record::Op};
use skyfs_meta::plan::{self, AttrChanges, Caller, MakeRequest};
use skyfs_meta::{Attr, DirEntry, Errno, Ino, Kind, MetaError, ROOT_INO};
use skyfs_store::{Key, ObjectClient};
use tokio::runtime::Runtime;
use tokio::sync::{oneshot, watch};

use crate::committer;
use crate::config::{CreateOptions, FsyncMode, Replication, VolumeConfig};
use crate::error::VolumeError;
use crate::mount::{self, MountReport};
use crate::restorer::RestoreMsg;
use crate::snapshot::{self, Hint};
use crate::writer::{Shared, State};

/// A result, and the segment to wait for before reporting it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct Done<T> {
    /// The result.
    pub value: T,
    /// Wait until this seq is committed before answering, if set.
    pub wait: Option<u64>,
}

impl<T> Done<T> {
    fn now(value: T) -> Self {
        Self { value, wait: None }
    }
}

/// The capacity a volume reports. Blob storage has no fixed size, but
/// `statfs` must answer something and `fallocate` must agree with it.
pub const CAPACITY_BYTES: u64 = 1 << 52;
/// The inode count a volume reports.
pub const CAPACITY_INODES: u64 = 1 << 40;

/// Filesystem totals for `statfs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsStats {
    /// Live inodes.
    pub inodes: u64,
    /// Logical bytes stored.
    pub bytes: u64,
    /// Reported capacity ([`CAPACITY_BYTES`]).
    pub capacity: u64,
    /// Reported inode capacity ([`CAPACITY_INODES`]).
    pub max_inodes: u64,
}

impl FsStats {
    /// Bytes reported free.
    #[must_use]
    pub fn free(&self) -> u64 {
        self.capacity.saturating_sub(self.bytes)
    }
}

/// What happened at unmount.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnmountReport {
    /// The last committed seq.
    pub head_seq: u64,
    /// The restore pointer reached the head before the timeout.
    pub fully_restored: bool,
}

/// Abandons a mount from another thread (see [`Volume::abort_handle`]).
#[derive(Debug, Clone)]
pub struct AbortHandle(Weak<Shared>);

impl AbortHandle {
    /// Abandons the mount, if it is still alive: waiters and mutations
    /// fail with EIO, and a blocked [`Volume::unmount`] returns
    /// [`VolumeError::Aborted`]. Committed state is untouched; the local
    /// WAL is recovered by the next mount on this disk, as after a crash.
    pub fn abort(&self) {
        if let Some(shared) = self.0.upgrade() {
            shared.abort();
        }
    }
}

/// A mounted volume.
#[derive(Debug)]
pub struct Volume {
    shared: Arc<Shared>,
    rt: Option<Runtime>,
    stop: watch::Sender<bool>,
    committer: Option<tokio::task::JoinHandle<()>>,
    restorer: Option<tokio::task::JoinHandle<()>>,
    report: MountReport,
}

fn meta_err(_: MetaError) -> Errno {
    Errno::EIO
}

fn runtime() -> Result<Runtime, VolumeError> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("skyfs-engine")
        .enable_all()
        .build()?)
}

/// Creates a new volume in the bucket.
pub fn create_volume(
    client: &ObjectClient,
    volume_id: [u8; 16],
    opts: &CreateOptions,
    scratch: &std::path::Path,
) -> Result<(), VolumeError> {
    runtime()?.block_on(mount::create(client, volume_id, opts, scratch))
}

/// Publishes quiescent volume `src` as the new sealed volume `dst`
/// ([`crate::published::publish`]).
pub fn publish_volume(
    src: &ObjectClient,
    src_id: [u8; 16],
    dst: &ObjectClient,
    dst_id: [u8; 16],
    keep: &mut crate::published::Keep<'_>,
    scratch: &std::path::Path,
) -> Result<crate::Published, VolumeError> {
    runtime()?.block_on(crate::published::publish(
        src, src_id, dst, dst_id, keep, scratch,
    ))
}

/// Creates volume `dst` as a metadata-only clone of sealed volume `base`
/// ([`crate::published::clone_volume`]).
pub fn clone_volume(
    base: &ObjectClient,
    base_id: [u8; 16],
    dst: &ObjectClient,
    dst_id: [u8; 16],
    scratch: &std::path::Path,
) -> Result<crate::Cloned, VolumeError> {
    runtime()?.block_on(crate::published::clone_volume(
        base, base_id, dst, dst_id, scratch,
    ))
}

impl Volume {
    /// Mounts the volume: snapshot, tail replay, recovery, a new epoch.
    /// Returns once `MountBegin` is committed.
    pub fn mount(
        client: ObjectClient,
        volume_id: [u8; 16],
        cfg: VolumeConfig,
    ) -> Result<Self, VolumeError> {
        let rt = runtime()?;
        let handle = rt.handle().clone();
        let m = rt.block_on(mount::mount(client, volume_id, cfg, handle))?;
        let (stop, stop_rx) = watch::channel(false);
        let read_only = m.shared.cfg.read_only;
        let committer = (!read_only).then(|| rt.spawn(committer::run(m.shared.clone(), stop_rx)));
        let restorer = m
            .restorer
            .filter(|_| m.shared.cfg.restorer)
            .map(|r| rt.spawn(r.run(m.restore_rx)));
        let vol = Self {
            shared: m.shared,
            rt: Some(rt),
            stop,
            committer,
            restorer,
            report: m.report,
        };
        if read_only {
            return Ok(vol);
        }
        // MountBegin claims the epoch: it must land before we serve.
        // If the bucket stays unreachable, give up (dropping `vol` abandons
        // the mount; the next mount on this disk sets the segment aside).
        let seq = vol.shared.state.lock().last_seq.unwrap_or(0);
        let limit = vol.shared.cfg.mount_timeout;
        let shared = vol.shared.clone();
        let landed = vol
            .shared
            .rt
            .block_on(async move { tokio::time::timeout(limit, shared.wait_committed(seq)).await });
        match landed {
            Ok(Ok(())) => Ok(vol),
            Ok(Err(_)) => Err(VolumeError::Fenced(
                "MountBegin could not be committed".into(),
            )),
            Err(_) => Err(VolumeError::Timeout(format!(
                "MountBegin was not committed within {limit:?}"
            ))),
        }
    }

    /// What mount found.
    #[must_use]
    pub fn report(&self) -> &MountReport {
        &self.report
    }

    /// The engine runtime, for waiting off the FUSE thread.
    #[must_use]
    pub fn runtime(&self) -> tokio::runtime::Handle {
        self.shared.rt.clone()
    }

    /// Blocks until segment `seq` is committed (or the volume is fenced).
    pub fn wait(&self, seq: u64) -> Result<(), Errno> {
        self.shared.rt.block_on(self.shared.wait_committed(seq))
    }

    /// Waits for whatever `done` requires, then returns its value.
    pub fn finish<T>(&self, done: Done<T>) -> Result<T, Errno> {
        if let Some(seq) = done.wait {
            self.wait(seq)?;
        }
        Ok(done.value)
    }

    /// An async wait for `seq`, for callers on their own tasks.
    pub fn waiter(
        &self,
        seq: u64,
    ) -> impl std::future::Future<Output = Result<(), Errno>> + Send + 'static {
        let shared = self.shared.clone();
        async move { shared.wait_committed(seq).await }
    }

    /// A handle that abandons this mount from another thread, e.g. on a
    /// second signal while an unmount waits for an unreachable bucket. It
    /// does not keep the volume alive.
    #[must_use]
    pub fn abort_handle(&self) -> AbortHandle {
        AbortHandle(Arc::downgrade(&self.shared))
    }

    /// The commit state: highest committed seq, fenced.
    #[must_use]
    pub fn commit_state(&self) -> crate::writer::CommitState {
        self.shared.commit_state()
    }

    fn lock(&self) -> parking_lot::MutexGuard<'_, State> {
        self.shared.state.lock()
    }

    /// A namespace or attribute op answers after commit in sync mode.
    fn ns_wait(&self, seq: u64) -> Option<u64> {
        (self.shared.cfg.replication == Replication::Sync).then_some(seq)
    }

    fn attr_of(st: &State, ino: Ino) -> Result<Attr, Errno> {
        st.meta.getattr(ino).map_err(meta_err)?.ok_or(Errno::ENOENT)
    }

    /// Logs the atime update an access to `a` calls for (design §9). It
    /// rides in the open segment with whatever comes next and nobody waits
    /// for it; an access never fails because its atime could not be logged
    /// (fenced, unmounting, local disk full). Returns whether atime moved.
    fn accessed(&self, st: &mut State, a: &Attr) -> bool {
        if self.shared.check_writable(st).is_err() {
            return false;
        }
        match plan::access(a, snapshot::now_ns(), self.shared.cfg.atime) {
            Some(op) => self.shared.log_op(st, op).is_ok(),
            None => false,
        }
    }

    // ------------------------------------------------------------ queries

    /// Resolves `name` in `parent`.
    pub fn lookup(&self, parent: Ino, name: &[u8]) -> Result<Attr, Errno> {
        // The kernel does not enforce NAME_MAX for FUSE; every path
        // operation starts with a lookup, so this is where it is checked.
        if name.len() > skyfs_meta::NAME_MAX {
            return Err(Errno::ENAMETOOLONG);
        }
        let st = self.lock();
        let p = Self::attr_of(&st, parent)?;
        // `.` of any inode is the inode: the kernel revives a file handle
        // with `lookup(ino, ".")`. An unlinked, closed inode is gone (ESTALE
        // to the caller); one still open is found.
        if name == b"." {
            return Ok(p);
        }
        if p.kind != Kind::Directory {
            return Err(Errno::ENOTDIR);
        }
        let ino = match name {
            b"." => parent,
            b".." => st
                .meta
                .parent(parent)
                .map_err(meta_err)?
                .unwrap_or(ROOT_INO),
            _ => st
                .meta
                .lookup(parent, name)
                .map_err(meta_err)?
                .ok_or(Errno::ENOENT)?,
        };
        Self::attr_of(&st, ino)
    }

    /// Attributes of `ino`.
    pub fn getattr(&self, ino: Ino) -> Result<Attr, Errno> {
        Self::attr_of(&self.lock(), ino)
    }

    /// Target of symlink `ino`.
    pub fn readlink(&self, ino: Ino) -> Result<Vec<u8>, Errno> {
        let mut st = self.lock();
        let a = Self::attr_of(&st, ino)?;
        if a.kind != Kind::Symlink {
            return Err(Errno::EINVAL);
        }
        let target = st
            .meta
            .readlink(ino)
            .map_err(meta_err)?
            .ok_or(Errno::EINVAL)?;
        self.accessed(&mut st, &a);
        Ok(target)
    }

    /// Entries of `dir` after cookie `after` (`.`/`..` not included), and
    /// the parent for `..`. A listing from the start (`after` ≤ 2, the
    /// cookies of `.` and `..`) is an access for atime.
    pub fn readdir(
        &self,
        dir: Ino,
        after: u64,
        limit: usize,
    ) -> Result<(Vec<DirEntry>, Ino), Errno> {
        let mut st = self.lock();
        let a = Self::attr_of(&st, dir)?;
        if a.kind != Kind::Directory {
            return Err(Errno::ENOTDIR);
        }
        let parent = st.meta.parent(dir).map_err(meta_err)?.unwrap_or(ROOT_INO);
        let entries = st.meta.readdir(dir, after, limit).map_err(meta_err)?;
        if after <= 2 {
            self.accessed(&mut st, &a);
        }
        Ok((entries, parent))
    }

    /// Value of xattr `name`.
    pub fn getxattr(&self, ino: Ino, name: &[u8]) -> Result<Vec<u8>, Errno> {
        let st = self.lock();
        Self::attr_of(&st, ino)?;
        st.meta
            .getxattr(ino, name)
            .map_err(meta_err)?
            .ok_or(Errno::ENODATA)
    }

    /// Xattr names of `ino`.
    pub fn listxattr(&self, ino: Ino) -> Result<Vec<Vec<u8>>, Errno> {
        let st = self.lock();
        Self::attr_of(&st, ino)?;
        st.meta.listxattr(ino).map_err(meta_err)
    }

    /// Totals for `statfs`.
    pub fn statfs(&self) -> Result<FsStats, Errno> {
        let s = self.lock().meta.stats().map_err(meta_err)?;
        Ok(FsStats {
            inodes: s.inodes,
            bytes: s.data_bytes,
            capacity: CAPACITY_BYTES,
            max_inodes: CAPACITY_INODES,
        })
    }

    /// Reads up to `size` bytes at `offset`. Holes read as zeros; chunks are
    /// fetched in parallel and verified.
    pub fn read(&self, ino: Ino, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let (end, extents) = {
            let mut st = self.lock();
            let a = Self::attr_of(&st, ino)?;
            match a.kind {
                Kind::Regular => {}
                Kind::Directory => return Err(Errno::EISDIR),
                _ => return Err(Errno::EINVAL),
            }
            self.accessed(&mut st, &a);
            let end = offset.saturating_add(u64::from(size)).min(a.size);
            if offset >= end {
                return Ok(Vec::new());
            }
            (end, st.meta.extents(ino, offset, end).map_err(meta_err)?)
        };
        let pieces: Vec<_> = extents.iter().filter_map(|e| e.clip(offset, end)).collect();
        let data = &self.shared.data;
        let bytes = self.shared.rt.block_on(futures::future::try_join_all(
            pieces.iter().map(|e| data.extent(e)),
        ))?;
        let mut out = vec![0u8; usize::try_from(end - offset).map_err(|_| Errno::EINVAL)?];
        for (e, b) in pieces.iter().zip(bytes) {
            let at = usize::try_from(e.off - offset).map_err(|_| Errno::EINVAL)?;
            out[at..at + b.len()].copy_from_slice(&b);
        }
        Ok(out)
    }

    // ---------------------------------------------------------- namespace

    /// `mknod`, `mkdir`, `symlink`, `creat` without a handle.
    pub fn make(&self, caller: Caller, req: &MakeRequest<'_>) -> Result<Done<Attr>, Errno> {
        let mut st = self.lock();
        self.shared.check_writable(&st)?;
        let op = plan::make(&st.meta, caller, req)?;
        let Op::Make(m) = &op else {
            unreachable!("plan::make makes")
        };
        let ino = m.ino;
        let seq = self.shared.log_op(&mut st, op)?;
        Ok(Done {
            value: Self::attr_of(&st, ino)?,
            wait: self.ns_wait(seq),
        })
    }

    /// `open(O_CREAT)`: creates a regular file and opens a handle on it.
    pub fn create(
        &self,
        caller: Caller,
        parent: Ino,
        name: &[u8],
        mode: u32,
        umask: u32,
    ) -> Result<Done<(Attr, u64)>, Errno> {
        let mut st = self.lock();
        self.shared.check_writable(&st)?;
        let req = MakeRequest {
            parent,
            name,
            kind: Kind::Regular,
            mode,
            umask,
            rdev: 0,
            target: b"",
        };
        let op = plan::make(&st.meta, caller, &req)?;
        let Op::Make(m) = &op else {
            unreachable!("plan::make makes")
        };
        let ino = m.ino;
        let seq = self.shared.log_op(&mut st, op)?;
        let fh = self.shared.open_handle(&mut st, ino);
        Ok(Done {
            value: (Self::attr_of(&st, ino)?, fh),
            wait: self.ns_wait(seq),
        })
    }

    /// `unlink` / `rmdir`. The inode is freed now, or at its last close.
    pub fn remove(&self, parent: Ino, name: &[u8], is_dir: bool) -> Result<Done<()>, Errno> {
        let mut st = self.lock();
        self.shared.check_writable(&st)?;
        let op = plan::remove(&st.meta, parent, name, is_dir)?;
        let Op::Remove(r) = &op else {
            unreachable!("plan::remove removes")
        };
        let ino = r.ino;
        self.shared.log_op(&mut st, op)?;
        self.shared.reclaim_if_unused(&mut st, ino)?;
        let seq = st.last_seq.unwrap_or(0);
        Ok(Done {
            value: (),
            wait: self.ns_wait(seq),
        })
    }

    /// `rename` / `renameat2`.
    pub fn rename(
        &self,
        old_parent: Ino,
        old_name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        mode: RenameMode,
    ) -> Result<Done<()>, Errno> {
        let mut st = self.lock();
        self.shared.check_writable(&st)?;
        let Some(op) = plan::rename(&st.meta, old_parent, old_name, new_parent, new_name, mode)?
        else {
            return Ok(Done::now(()));
        };
        let Op::Rename(r) = &op else {
            unreachable!("plan::rename renames")
        };
        let replaced =
            (mode != RenameMode::Exchange && r.replaced_ino != 0).then_some(r.replaced_ino);
        self.shared.log_op(&mut st, op)?;
        if let Some(victim) = replaced {
            self.shared.reclaim_if_unused(&mut st, victim)?;
        }
        let seq = st.last_seq.unwrap_or(0);
        Ok(Done {
            value: (),
            wait: self.ns_wait(seq),
        })
    }

    /// `link`.
    pub fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> Result<Done<Attr>, Errno> {
        let mut st = self.lock();
        self.shared.check_writable(&st)?;
        let op = plan::link(&st.meta, ino, new_parent, new_name)?;
        let seq = self.shared.log_op(&mut st, op)?;
        Ok(Done {
            value: Self::attr_of(&st, ino)?,
            wait: self.ns_wait(seq),
        })
    }

    // ---------------------------------------------------------- attributes

    /// `chmod`, `chown`, `truncate`, `utimensat`.
    pub fn setattr(&self, ino: Ino, ch: &AttrChanges) -> Result<Done<Attr>, Errno> {
        let mut st = self.lock();
        if *ch == AttrChanges::default() {
            return Ok(Done::now(Self::attr_of(&st, ino)?));
        }
        self.shared.check_writable(&st)?;
        let op = plan::set_attr(&st.meta, ino, ch, snapshot::now_ns())?;
        if ch.size.is_some() {
            // Truncation breaks any sequential run.
            self.shared.end_run(&mut st, ino);
        }
        let seq = self.shared.log_op(&mut st, op)?;
        Ok(Done {
            value: Self::attr_of(&st, ino)?,
            wait: self.ns_wait(seq),
        })
    }

    /// `setxattr`.
    pub fn setxattr(
        &self,
        ino: Ino,
        name: &[u8],
        value: &[u8],
        mode: XattrMode,
    ) -> Result<Done<()>, Errno> {
        let mut st = self.lock();
        self.shared.check_writable(&st)?;
        let op = plan::set_xattr(&st.meta, ino, name, value, mode)?;
        let seq = self.shared.log_op(&mut st, op)?;
        Ok(Done {
            value: (),
            wait: self.ns_wait(seq),
        })
    }

    /// `removexattr`.
    pub fn removexattr(&self, ino: Ino, name: &[u8]) -> Result<Done<()>, Errno> {
        let mut st = self.lock();
        self.shared.check_writable(&st)?;
        let op = plan::remove_xattr(&st.meta, ino, name)?;
        let seq = self.shared.log_op(&mut st, op)?;
        Ok(Done {
            value: (),
            wait: self.ns_wait(seq),
        })
    }

    // -------------------------------------------------------------- data

    /// Opens a handle on `ino` (file or directory).
    pub fn open(&self, ino: Ino) -> Result<u64, Errno> {
        let mut st = self.lock();
        Self::attr_of(&st, ino)?;
        Ok(self.shared.open_handle(&mut st, ino))
    }

    /// An open for reading is an access: the kernel serves reads from its
    /// page cache without asking skyfs, so the open is where a read is
    /// sure to be seen. Returns whether atime moved (the kernel's cached
    /// attributes are then stale).
    pub fn access(&self, ino: Ino) -> Result<bool, Errno> {
        let mut st = self.lock();
        let a = Self::attr_of(&st, ino)?;
        Ok(a.kind == Kind::Regular && self.accessed(&mut st, &a))
    }

    /// The inode behind handle `fh`.
    pub fn handle_ino(&self, fh: u64) -> Result<Ino, Errno> {
        Ok(self.lock().handle(fh)?.ino)
    }

    /// `write` through handle `fh`. Returns the bytes written. Durable at
    /// the next durability point (`fsync`, close in sync mode).
    pub fn write(&self, fh: u64, offset: u64, data: &[u8]) -> Result<u32, Errno> {
        let mut st = self.lock();
        self.shared.check_writable(&st)?;
        let ino = st.handle(fh)?.ino;
        plan::check_write(&st.meta, ino, offset, data.len() as u64)?;
        if data.is_empty() {
            return Ok(0);
        }
        let n = u32::try_from(data.len()).map_err(|_| Errno::EINVAL)?;
        let placed = self.shared.place_write(&mut st, ino, offset, data)?;
        self.shared
            .log_op(&mut st, plan::write_op(ino, offset, data, placed))?;
        Shared::mark_wrote(&mut st, fh);
        Ok(n)
    }

    /// `fallocate`.
    pub fn fallocate(
        &self,
        ino: Ino,
        offset: u64,
        len: u64,
        mode: FallocateMode,
    ) -> Result<Done<()>, Errno> {
        let mut st = self.lock();
        self.shared.check_writable(&st)?;
        // A reservation beyond the free space `statfs` reports cannot be
        // honoured, as on any filesystem.
        let reserves = matches!(
            mode,
            FallocateMode::Unspecified | FallocateMode::KeepSize | FallocateMode::InsertRange
        );
        let used = st.meta.stats().map_err(meta_err)?.data_bytes;
        if reserves && len > CAPACITY_BYTES.saturating_sub(used) {
            return Err(Errno::ENOSPC);
        }
        let op = plan::fallocate(&st.meta, ino, offset, len, mode)?;
        self.shared.end_run(&mut st, ino);
        let seq = self.shared.log_op(&mut st, op)?;
        Ok(Done {
            value: (),
            wait: self.ns_wait(seq),
        })
    }

    /// `copy_file_range` / reflink: shares extents. Returns bytes copied.
    pub fn copy_range(
        &self,
        src: Ino,
        src_off: u64,
        dst: Ino,
        dst_off: u64,
        len: u64,
    ) -> Result<Done<u64>, Errno> {
        let mut st = self.lock();
        self.shared.check_writable(&st)?;
        let Some((op, n)) = plan::clone_range(&st.meta, src, src_off, dst, dst_off, len)? else {
            return Ok(Done::now(0));
        };
        self.shared.end_run(&mut st, dst);
        let seq = self.shared.log_op(&mut st, op)?;
        Ok(Done {
            value: n,
            wait: self.ns_wait(seq),
        })
    }

    /// The durability point for `fsync`/`fdatasync`/`syncfs`/`sync`: every
    /// op so far. In async mode with `--fsync local`, the local WAL is
    /// synced and nothing is awaited.
    pub fn fsync(&self) -> Result<Done<()>, Errno> {
        if self.shared.cfg.replication == Replication::Async
            && self.shared.cfg.fsync == FsyncMode::Local
        {
            self.shared.sync_local()?;
            return Ok(Done::now(()));
        }
        let seq = self.lock().last_seq;
        Ok(Done {
            value: (),
            wait: seq,
        })
    }

    /// `flush` (each `close`): in sync mode, a handle that wrote answers
    /// after commit (close-to-open durability).
    pub fn flush(&self, fh: u64) -> Result<Done<()>, Errno> {
        let st = self.lock();
        let h = st.handle(fh)?;
        let wait = if h.wrote && self.shared.cfg.replication == Replication::Sync {
            st.last_seq
        } else {
            None
        };
        Ok(Done { value: (), wait })
    }

    /// `release` / `releasedir`: drops the handle; an unlinked inode is
    /// freed at its last close.
    pub fn release(&self, fh: u64) -> Result<(), Errno> {
        let mut st = self.lock();
        match self.shared.release_handle(&mut st, fh) {
            // A fenced or closing volume still releases; only the Reclaim
            // is skipped, and MountBegin frees the orphan later.
            Ok(_) | Err(Errno::EIO | Errno::EROFS) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // ------------------------------------------------------------- unmount

    /// Unmounts cleanly (design §7.4): commits everything and `MountEnd`,
    /// restores as far as the time budget allows, updates the hint.
    pub fn unmount(mut self) -> Result<UnmountReport, VolumeError> {
        let shared = self.shared.clone();
        let rt = self.rt.take().expect("runtime present until unmount");
        if shared.cfg.read_only {
            // Nothing was written: nothing to commit, restore or hint.
            rt.shutdown_timeout(Duration::from_secs(5));
            return Ok(UnmountReport {
                head_seq: shared.commit_state().committed.unwrap_or(0),
                fully_restored: true,
            });
        }
        let budget = shared.cfg.restore_on_exit_timeout;
        let result = rt.block_on(async {
            let seq = {
                let mut st = shared.state.lock();
                let last = st.last_seq;
                let logged = shared.log_op(&mut st, Op::MountEnd(MountEnd {}));
                st.closing = true;
                let _ = shared.seal(&mut st);
                logged.ok().or(last)
            };
            if let Some(seq) = seq {
                shared.wait_committed(seq).await.map_err(|_| {
                    if shared.commit_state().aborted {
                        VolumeError::Aborted
                    } else {
                        VolumeError::Fenced("could not commit the log at unmount".into())
                    }
                })?;
            }
            let _ = self.stop.send(true);
            if let Some(c) = self.committer.take() {
                let _ = c.await;
            }
            let mut fully_restored = false;
            if let Some(r) = self.restorer.take() {
                let (done, rx) = oneshot::channel();
                let _ = shared.restore_tx.send(RestoreMsg::Drain {
                    deadline: Instant::now() + budget,
                    done,
                });
                fully_restored = tokio::time::timeout(budget + Duration::from_secs(5), rx)
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .unwrap_or(false);
                r.abort();
            }
            let head = shared.commit_state().committed.unwrap_or(0);
            let generation = snapshot::latest(
                &shared.client,
                snapshot::read_hint(&shared.client).await,
                &shared.volume_id,
            )
            .await
            .ok()
            .flatten()
            .map_or(0, |m| m.r#gen);
            snapshot::write_hint(
                &shared.client,
                Hint {
                    snapshot_gen: generation,
                    head_seq: head,
                },
            )
            .await;
            Ok(UnmountReport {
                head_seq: head,
                fully_restored,
            })
        });
        rt.shutdown_timeout(Duration::from_secs(5));
        result
    }

    /// Fetches an object's raw bytes (tools and tests).
    pub fn fetch(&self, key: &Key) -> Result<Option<bytes::Bytes>, VolumeError> {
        Ok(self.shared.rt.block_on(self.shared.client.get(key))?)
    }
}

impl Drop for Volume {
    /// Dropping without `unmount` is a crash: tasks stop where they are,
    /// and the local WAL is recovered by the next mount on this disk.
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            self.shared.abort();
            rt.shutdown_background();
        }
    }
}
