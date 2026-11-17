//! The FUSE adapter (Linux): translates kernel requests to `Volume` calls.
//!
//! It holds no filesystem state of its own. When a call's `Done` names a
//! segment to wait for (sync replication, fsync, close-to-open), the reply
//! is moved to an engine task that answers once that segment is committed,
//! so the FUSE worker thread is free for the next request.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    AccessFlags, BsdFileFlags, Config, CopyFileRangeFlags, FileAttr, FileHandle, FileType,
    Filesystem, FopenFlags, Generation, INodeNo, InitFlags, KernelConfig, LockOwner, MountOption,
    OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, SessionACL, TimeOrNow,
    WriteFlags,
};
use skyfs_format::wal::v1::{FallocateMode, RenameMode, XattrMode};
use skyfs_meta::plan::{AttrChanges, Caller, MakeRequest};
use skyfs_meta::{Attr, Errno, Kind};
use skyfs_store::ObjectClient;
use skyfs_volume::{Done, Volume, VolumeConfig};

use crate::cli::MountArgs;

/// Attribute and entry cache lifetime. skyfs is the only writer, so the
/// kernel's caches never go stale behind its back.
const TTL: Duration = Duration::from_secs(60);

struct SkyFs {
    vol: Arc<Volume>,
    rt: tokio::runtime::Handle,
    destroyed: Arc<tokio::sync::Notify>,
    /// Tells the kernel its cached attributes are stale (set once mounted).
    notifier: Arc<std::sync::OnceLock<fuser::Notifier>>,
}

/// `FS_IOC_FSGETXATTR`: `_IOR('X', 31, struct fsxattr)`.
const FS_IOC_FSGETXATTR: u32 = 0x801c_581f;
/// `FS_IOC_GETFLAGS`: `_IOR('f', 1, long)`.
const FS_IOC_GETFLAGS: u32 = 0x8008_6601;

fn errno(e: Errno) -> fuser::Errno {
    fuser::Errno::from_i32(e.0)
}

fn time(ns: i64) -> SystemTime {
    match u64::try_from(ns) {
        Ok(n) => UNIX_EPOCH + Duration::from_nanos(n),
        Err(_) => UNIX_EPOCH - Duration::from_nanos(ns.unsigned_abs()),
    }
}

fn ns(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => -i64::try_from(e.duration().as_nanos()).unwrap_or(i64::MAX),
    }
}

fn resolve(t: TimeOrNow) -> i64 {
    match t {
        TimeOrNow::SpecificTime(t) => ns(t),
        TimeOrNow::Now => ns(SystemTime::now()),
    }
}

fn file_type(k: Kind) -> FileType {
    match k {
        Kind::Regular => FileType::RegularFile,
        Kind::Directory => FileType::Directory,
        Kind::Symlink => FileType::Symlink,
        Kind::Fifo => FileType::NamedPipe,
        Kind::Socket => FileType::Socket,
        Kind::CharDevice => FileType::CharDevice,
        Kind::BlockDevice => FileType::BlockDevice,
    }
}

#[allow(clippy::cast_possible_truncation)]
fn attr(a: &Attr) -> FileAttr {
    FileAttr {
        ino: INodeNo(a.ino),
        size: a.size,
        blocks: a.size.div_ceil(512),
        atime: time(a.atime_ns),
        mtime: time(a.mtime_ns),
        ctime: time(a.ctime_ns),
        crtime: time(a.ctime_ns),
        kind: file_type(a.kind),
        perm: (a.mode & 0o7777) as u16,
        nlink: a.nlink,
        uid: a.uid,
        gid: a.gid,
        rdev: a.rdev as u32,
        blksize: 4096,
        flags: 0,
    }
}

fn caller(req: &Request) -> Caller {
    Caller {
        uid: req.uid(),
        gid: req.gid(),
    }
}

impl SkyFs {
    /// Answers now, or after the commit `done` waits for.
    fn deliver<T, R>(
        &self,
        r: Result<Done<T>, Errno>,
        reply: R,
        ok: impl FnOnce(R, T) + Send + 'static,
        err: impl FnOnce(R, fuser::Errno) + Send + 'static,
    ) where
        T: Send + 'static,
        R: Send + 'static,
    {
        match r {
            Err(e) => err(reply, errno(e)),
            Ok(Done { value, wait: None }) => ok(reply, value),
            Ok(Done {
                value,
                wait: Some(seq),
            }) => {
                let waiter = self.vol.waiter(seq);
                self.rt.spawn(async move {
                    match waiter.await {
                        Ok(()) => ok(reply, value),
                        Err(e) => err(reply, errno(e)),
                    }
                });
            }
        }
    }

    fn entry(&self, r: Result<Done<Attr>, Errno>, reply: ReplyEntry) {
        self.deliver(
            r,
            reply,
            |reply, a| reply.entry(&TTL, &attr(&a), Generation(0)),
            fuser::ReplyEntry::error,
        );
    }

    fn empty(&self, r: Result<Done<()>, Errno>, reply: ReplyEmpty) {
        self.deliver(r, reply, |reply, ()| reply.ok(), fuser::ReplyEmpty::error);
    }

    fn make(&self, req: &Request, m: &MakeRequest<'_>, reply: ReplyEntry) {
        self.entry(self.vol.make(caller(req), m), reply);
    }
}

impl Filesystem for SkyFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let _ = config.set_max_write(1 << 20);
        let _ = config.set_max_readahead(4 << 20);
        let _ = config.set_time_granularity(Duration::from_nanos(1));
        // Export support: file handles (NFS export, open_by_handle_at)
        // stay valid after the kernel drops an inode, because skyfs inode
        // numbers are never reused and `lookup(ino, ".")` finds any inode.
        let _ = config.add_capabilities(
            InitFlags::FUSE_ASYNC_READ
                | InitFlags::FUSE_BIG_WRITES
                | InitFlags::FUSE_EXPORT_SUPPORT,
        );
        Ok(())
    }

    fn destroy(&mut self) {
        self.destroyed.notify_one();
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        match self.vol.lookup(parent.0, name.as_bytes()) {
            Ok(a) => reply.entry(&TTL, &attr(&a), Generation(0)),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.vol.getattr(ino.0) {
            Ok(a) => reply.attr(&TTL, &attr(&a)),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let ch = AttrChanges {
            mode,
            uid,
            gid,
            size,
            atime_ns: atime.map(resolve),
            mtime_ns: mtime.map(resolve),
        };
        self.deliver(
            self.vol.setattr(ino.0, &ch),
            reply,
            |reply, a| reply.attr(&TTL, &attr(&a)),
            fuser::ReplyAttr::error,
        );
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.vol.readlink(ino.0) {
            Ok(t) => reply.data(&t),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let kind = match skyfs_meta::plan::kind_of_mode(mode) {
            Ok(k) if k != Kind::Directory && k != Kind::Symlink => k,
            _ => return reply.error(fuser::Errno::EINVAL),
        };
        let m = MakeRequest {
            parent: parent.0,
            name: name.as_bytes(),
            kind,
            mode,
            umask,
            rdev: u64::from(rdev),
            target: b"",
        };
        self.make(req, &m, reply);
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let m = MakeRequest {
            parent: parent.0,
            name: name.as_bytes(),
            kind: Kind::Directory,
            mode,
            umask,
            rdev: 0,
            target: b"",
        };
        self.make(req, &m, reply);
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.empty(self.vol.remove(parent.0, name.as_bytes(), false), reply);
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.empty(self.vol.remove(parent.0, name.as_bytes(), true), reply);
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let m = MakeRequest {
            parent: parent.0,
            name: link_name.as_bytes(),
            kind: Kind::Symlink,
            mode: 0o777,
            umask: 0,
            rdev: 0,
            target: target.as_os_str().as_bytes(),
        };
        self.make(req, &m, reply);
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let mode = if flags.contains(RenameFlags::RENAME_EXCHANGE) {
            RenameMode::Exchange
        } else if flags.contains(RenameFlags::RENAME_NOREPLACE) {
            RenameMode::Noreplace
        } else if flags.is_empty() {
            RenameMode::Unspecified
        } else {
            return reply.error(fuser::Errno::EINVAL);
        };
        self.empty(
            self.vol.rename(
                parent.0,
                name.as_bytes(),
                newparent.0,
                newname.as_bytes(),
                mode,
            ),
            reply,
        );
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        self.entry(self.vol.link(ino.0, newparent.0, newname.as_bytes()), reply);
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        // Reads may never reach skyfs (page cache), so an open for reading
        // is where atime is kept.
        let reading = flags.0 & libc::O_ACCMODE != libc::O_WRONLY;
        let moved = reading && self.vol.access(ino.0).unwrap_or(false);
        match self.vol.open(ino.0) {
            // Objects are immutable and this mount is the only writer, so
            // the page cache stays valid across opens.
            Ok(fh) => reply.opened(FileHandle(fh), FopenFlags::FOPEN_KEEP_CACHE),
            Err(e) => return reply.error(errno(e)),
        }
        if moved && let Some(n) = self.notifier.get() {
            // Attributes only (offset < 0): the cached atime is stale.
            let _ = n.inval_inode(ino, -1, 0);
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        reply: ReplyData,
    ) {
        match self.vol.read(ino.0, offset, size) {
            Ok(b) => reply.data(&b),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _wf: WriteFlags,
        flags: OpenFlags,
        _lock: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let n = match self.vol.write(fh.0, offset, data) {
            Ok(n) => n,
            Err(e) => return reply.error(errno(e)),
        };
        if flags.0 & (libc::O_SYNC | libc::O_DSYNC) != 0 {
            let done = self.vol.fsync().map(|d| Done {
                value: n,
                wait: d.wait,
            });
            self.deliver(
                done,
                reply,
                fuser::ReplyWrite::written,
                fuser::ReplyWrite::error,
            );
        } else {
            reply.written(n);
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock: LockOwner,
        reply: ReplyEmpty,
    ) {
        self.empty(self.vol.flush(fh.0), reply);
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.vol.release(fh.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.empty(self.vol.fsync(), reply);
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match self.vol.getattr(ino.0) {
            Ok(a) if a.kind == Kind::Directory => match self.vol.open(ino.0) {
                Ok(fh) => reply.opened(FileHandle(fh), FopenFlags::empty()),
                Err(e) => reply.error(errno(e)),
            },
            Ok(_) => reply.error(fuser::Errno::ENOTDIR),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        // Offsets: 1 is ".", 2 is "..", entries use their stable cookies (>= 3).
        let (entries, parent) = match self.vol.readdir(ino.0, offset.max(2), 512) {
            Ok(r) => r,
            Err(e) => return reply.error(errno(e)),
        };
        if offset < 1 && reply.add(ino, 1, FileType::Directory, ".") {
            return reply.ok();
        }
        if offset < 2 && reply.add(INodeNo(parent), 2, FileType::Directory, "..") {
            return reply.ok();
        }
        for e in entries {
            if reply.add(
                INodeNo(e.ino),
                e.cookie,
                file_type(e.kind),
                OsStr::from_bytes(&e.name),
            ) {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        match self.vol.release(fh.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.empty(self.vol.fsync(), reply);
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        const BLOCK: u64 = 4096;
        match self.vol.statfs() {
            Ok(s) => {
                let free = s.free() / BLOCK;
                reply.statfs(
                    s.capacity / BLOCK,
                    free,
                    free,
                    s.max_inodes,
                    s.max_inodes.saturating_sub(s.inodes),
                    4096,
                    255,
                    4096,
                );
            }
            Err(e) => reply.error(errno(e)),
        }
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        if position != 0 {
            return reply.error(fuser::Errno::EINVAL);
        }
        let mode = match flags {
            0 => XattrMode::Unspecified,
            libc::XATTR_CREATE => XattrMode::Create,
            libc::XATTR_REPLACE => XattrMode::Replace,
            _ => return reply.error(fuser::Errno::EINVAL),
        };
        self.empty(
            self.vol.setxattr(ino.0, name.as_bytes(), value, mode),
            reply,
        );
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        match self.vol.getxattr(ino.0, name.as_bytes()) {
            Ok(v) => xattr_reply(reply, &v, size),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        match self.vol.listxattr(ino.0) {
            Ok(names) => {
                let mut buf = Vec::new();
                for n in names {
                    buf.extend_from_slice(&n);
                    buf.push(0);
                }
                xattr_reply(reply, &buf, size);
            }
            Err(e) => reply.error(errno(e)),
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.empty(self.vol.removexattr(ino.0, name.as_bytes()), reply);
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        // `default_permissions`: the kernel already checked.
        reply.ok();
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if skyfs_meta::plan::kind_of_mode(mode).is_ok_and(|k| k != Kind::Regular) {
            return reply.error(fuser::Errno::EINVAL);
        }
        self.deliver(
            self.vol
                .create(caller(req), parent.0, name.as_bytes(), mode, umask),
            reply,
            |reply, (a, fh)| {
                reply.created(
                    &TTL,
                    &attr(&a),
                    Generation(0),
                    FileHandle(fh),
                    FopenFlags::FOPEN_KEEP_CACHE,
                );
            },
            fuser::ReplyCreate::error,
        );
    }

    fn fallocate(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        let keep = mode & libc::FALLOC_FL_KEEP_SIZE != 0;
        let m = match mode & !libc::FALLOC_FL_KEEP_SIZE {
            0 if keep => FallocateMode::KeepSize,
            0 => FallocateMode::Unspecified,
            libc::FALLOC_FL_PUNCH_HOLE if keep => FallocateMode::PunchHole,
            libc::FALLOC_FL_ZERO_RANGE if keep => FallocateMode::ZeroRangeKeepSize,
            libc::FALLOC_FL_ZERO_RANGE => FallocateMode::ZeroRange,
            libc::FALLOC_FL_COLLAPSE_RANGE if !keep => FallocateMode::CollapseRange,
            libc::FALLOC_FL_INSERT_RANGE if !keep => FallocateMode::InsertRange,
            _ => return reply.error(fuser::Errno::EOPNOTSUPP),
        };
        self.empty(self.vol.fallocate(ino.0, offset, length, m), reply);
    }

    fn copy_file_range(
        &self,
        _req: &Request,
        ino_in: INodeNo,
        _fh_in: FileHandle,
        offset_in: u64,
        ino_out: INodeNo,
        _fh_out: FileHandle,
        offset_out: u64,
        len: u64,
        _flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        let len = len.min(u64::from(u32::MAX));
        self.deliver(
            self.vol
                .copy_range(ino_in.0, offset_in, ino_out.0, offset_out, len),
            reply,
            |reply, n| reply.written(u32::try_from(n).unwrap_or(u32::MAX)),
            fuser::ReplyWrite::error,
        );
    }

    /// Inode flags as `lsattr` and `xfs_io stat` ask for them: skyfs has
    /// none (no immutable, append-only, project quota or extent hints).
    fn ioctl(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _flags: fuser::IoctlFlags,
        cmd: u32,
        _in_data: &[u8],
        out_size: u32,
        reply: fuser::ReplyIoctl,
    ) {
        if let Err(e) = self.vol.getattr(ino.0) {
            return reply.error(errno(e));
        }
        let len = match cmd {
            FS_IOC_FSGETXATTR => 28,
            FS_IOC_GETFLAGS => 8,
            _ => return reply.error(fuser::Errno::ENOTTY),
        };
        let zeros = [0u8; 28];
        reply.ioctl(0, &zeros[..len.min(out_size as usize)]);
    }
}

fn xattr_reply(reply: ReplyXattr, value: &[u8], size: u32) {
    let len = u32::try_from(value.len()).unwrap_or(u32::MAX);
    if size == 0 {
        reply.size(len);
    } else if len > size {
        reply.error(fuser::Errno::ERANGE);
    } else {
        reply.data(value);
    }
}

/// Resolves at the next SIGINT or SIGTERM.
async fn signalled() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
    let terminated = async {
        match term.as_mut() {
            Some(t) => {
                t.recv().await;
            }
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        () = terminated => {}
    }
}

/// Mounts and serves until the mount goes away or SIGINT/SIGTERM, then
/// unmounts cleanly (drain, `MountEnd`, restore within the budget).
pub fn serve(
    client: ObjectClient,
    volume_id: [u8; 16],
    cfg: VolumeConfig,
    args: &MountArgs,
) -> Result<(), String> {
    let vol = Arc::new(Volume::mount(client, volume_id, cfg).map_err(|e| e.to_string())?);
    let r = vol.report().clone();
    tracing::info!(
        epoch = r.epoch,
        snapshot = r.snapshot_gen,
        tail = r.tail_segments,
        recovered = r.recovered_segments,
        lost = r.lost_segments,
        previous_clean = r.previous_clean,
        "volume mounted"
    );
    let destroyed = Arc::new(tokio::sync::Notify::new());
    let notifier = Arc::new(std::sync::OnceLock::new());
    let fs = SkyFs {
        vol: vol.clone(),
        rt: vol.runtime(),
        destroyed: destroyed.clone(),
        notifier: notifier.clone(),
    };
    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::FSName(args.fsname.clone()),
        MountOption::DefaultPermissions,
    ];
    if !args.subtype.is_empty() {
        config
            .mount_options
            .push(MountOption::Subtype(args.subtype.clone()));
    }
    if args.read_only {
        // The kernel refuses writes itself, before they reach skyfs.
        config.mount_options.push(MountOption::RO);
    }
    if args.atime == crate::cli::AtimeArg::Noatime {
        // The kernel then also skips invalidating cached atimes after reads.
        config.mount_options.push(MountOption::NoAtime);
    }
    // Device nodes and set-id binaries from a network volume are a host
    // risk: off unless asked for, as for any unprivileged FUSE mount.
    config.mount_options.push(if args.dev {
        MountOption::Dev
    } else {
        MountOption::NoDev
    });
    config.mount_options.push(if args.suid {
        MountOption::Suid
    } else {
        MountOption::NoSuid
    });
    if args.allow_other {
        config.acl = SessionACL::All;
    }
    config.n_threads = Some(args.threads.max(1));
    let session =
        fuser::spawn_mount(fs, &args.mountpoint, &config).map_err(|e| format!("mount: {e}"))?;
    let _ = notifier.set(session.notifier());
    tracing::info!(mountpoint = %args.mountpoint.display(), "serving");

    let waiter = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    let by_kernel = waiter.block_on(async {
        tokio::select! {
            () = signalled() => { tracing::info!("signalled; unmounting"); false }
            () = destroyed.notified() => { tracing::info!("unmounted by the kernel"); true }
        }
    });
    drop(waiter);
    // Join the FUSE workers so the filesystem (and its handle on the
    // volume) is gone before the volume unmounts.
    let ended = if by_kernel {
        session.join()
    } else {
        session.umount_and_join()
    };
    ended.map_err(|e| format!("unmounting {}: {e}", args.mountpoint.display()))?;

    let vol = Arc::into_inner(vol).ok_or("volume still referenced after unmount")?;
    // The unmount waits for the bucket; a second signal gives up on it.
    let abort = vol.abort_handle();
    std::thread::spawn(move || {
        if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            rt.block_on(signalled());
            tracing::warn!(
                "signalled again: abandoning the unmount; the local WAL is kept for the next mount on this disk"
            );
            abort.abort();
        }
    });
    let report = vol.unmount().map_err(|e| e.to_string())?;
    tracing::info!(
        head = report.head_seq,
        fully_restored = report.fully_restored,
        "clean unmount"
    );
    Ok(())
}
