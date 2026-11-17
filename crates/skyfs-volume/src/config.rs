//! Mount options (design §6.4, §7.2, §8.3).

use std::path::PathBuf;
use std::time::Duration;

pub use skyfs_meta::plan::AtimeMode;

/// When the kernel gets its answer for namespace and attribute operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replication {
    /// After the segment holding the op is committed in blob storage.
    Sync,
    /// After the local WAL append; uploaded within `max_lag`.
    Async,
}

/// Where `fsync` waits in async mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// Until the data is committed in blob storage.
    Remote,
    /// Until the local WAL is on disk.
    Local,
}

/// Everything a mount needs besides the bucket.
#[derive(Debug, Clone)]
pub struct VolumeConfig {
    /// Local directory for the metadata store, WAL and cache.
    pub local_dir: PathBuf,
    /// Opaque identity of this writer (host, container, process).
    pub writer_id: String,
    /// Sync or async replication.
    pub replication: Replication,
    /// Where `fsync` waits in async mode.
    pub fsync: FsyncMode,
    /// Async mode: seal and upload at most this long after a segment's
    /// first record.
    pub max_lag: Duration,
    /// Group commit window: local `fdatasync` cadence.
    pub group_commit: Duration,
    /// Seal a segment at this size.
    pub segment_max: u64,
    /// Close a large-write blob at this size.
    pub blob_max: u64,
    /// Concurrent blob uploads.
    pub blob_uploads: usize,
    /// Local cache size limit.
    pub cache_limit: u64,
    /// Restore when the oldest unrestored segment is this old.
    pub restore_after: Duration,
    /// ... or this many unrestored record bytes accumulate.
    pub restore_bytes: u64,
    /// ... or this many unrestored segments.
    pub restore_segments: usize,
    /// Target size of a pack.
    pub pack_size: u64,
    /// Segments folded per restore pass; each pass publishes a snapshot.
    pub restore_batch: usize,
    /// How long to keep restoring at unmount.
    pub restore_on_exit_timeout: Duration,
    /// Keep snapshots (and the log after them) this long, for
    /// point-in-time restore. Zero keeps only what mounts need.
    pub retention: Duration,
    /// Run the background restorer. Disabled only by tests and tools.
    pub restorer: bool,
    /// Serve the volume as of mount time without writing anything: no
    /// epoch claimed, no log, no restore; every mutation is EROFS.
    pub read_only: bool,
    /// How reads, `readdir` and `readlink` update atime.
    pub atime: AtimeMode,
    /// How long mount waits for its `MountBegin` to commit before giving
    /// up (bucket unreachable).
    pub mount_timeout: Duration,
    /// How long mount waits for another process (a previous mount still
    /// finishing its unmount) to release the local directory.
    pub local_lock_wait: Duration,
}

impl VolumeConfig {
    /// Defaults from the design for a local directory.
    #[must_use]
    pub fn new(local_dir: PathBuf) -> Self {
        Self {
            local_dir,
            writer_id: default_writer_id(),
            replication: Replication::Async,
            fsync: FsyncMode::Remote,
            max_lag: Duration::from_secs(1),
            group_commit: Duration::from_millis(10),
            segment_max: 16 << 20,
            blob_max: 64 << 20,
            blob_uploads: 4,
            cache_limit: 20 << 30,
            restore_after: Duration::from_mins(5),
            restore_bytes: 64 << 20,
            restore_segments: 512,
            pack_size: 32 << 20,
            restore_batch: 256,
            restore_on_exit_timeout: Duration::from_mins(1),
            retention: Duration::from_hours(7 * 24),
            restorer: true,
            read_only: false,
            atime: AtimeMode::Relatime,
            local_lock_wait: Duration::from_mins(2),
            mount_timeout: Duration::from_mins(5),
        }
    }
}

fn default_writer_id() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into());
    format!("{host}/{}", std::process::id())
}

/// Options for creating a volume.
#[derive(Debug, Clone)]
pub struct CreateOptions {
    /// Permission bits of the root directory.
    pub root_mode: u32,
    /// Owner of the root directory.
    pub root_uid: u32,
    /// Group of the root directory.
    pub root_gid: u32,
    /// Largest write stored inline in a segment.
    pub inline_data_max: u32,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            root_mode: 0o755,
            root_uid: 0,
            root_gid: 0,
            inline_data_max: 1 << 20,
        }
    }
}
