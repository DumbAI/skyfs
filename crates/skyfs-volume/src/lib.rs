//! The skyfs engine, platform-independent: everything between the FUSE
//! adapter and the bucket.
//!
//! - [`wal`]: the local WAL (open and sealed-but-uncommitted segments).
//! - [`writer`]: the live store, open segment, blobs and handles, and the
//!   one path every mutation takes (validate, log, apply).
//! - `committer`: seals and commits segments in seq order (group commit).
//! - [`data`]: the verified read path.
//! - [`mount`]: create, mount, tail replay, same-disk recovery.
//! - [`restorer`]: restore passes, the restore pointer, GC.
//! - [`published`]: sealed published volumes and metadata-only clones.
//! - [`volume`]: the POSIX operations the FUSE layer calls.

mod committer;
pub mod config;
pub mod data;
pub mod error;
pub mod mount;
pub mod published;
pub mod restorer;
pub mod snapshot;
pub mod volume;
pub mod wal;
pub mod writer;

pub use config::{AtimeMode, CreateOptions, FsyncMode, Replication, VolumeConfig};
pub use error::VolumeError;
pub use mount::MountReport;
pub use published::{Cloned, Published};
pub use volume::{
    AbortHandle, Done, FsStats, UnmountReport, Volume, clone_volume, create_volume, publish_volume,
};
