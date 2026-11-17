//! Engine errors. FUSE-facing calls return an `Errno`; these are for
//! mount, unmount, create and the background tasks.

use skyfs_meta::{ApplyError, MetaError};
use skyfs_store::StoreError;

/// Why a mount, create or restore step failed.
#[derive(Debug, thiserror::Error)]
pub enum VolumeError {
    /// The bucket failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The metadata store failed.
    #[error(transparent)]
    Meta(#[from] MetaError),
    /// A logged record did not apply: the log or the store is corrupt.
    #[error(transparent)]
    Apply(#[from] ApplyError),
    /// Local disk I/O failed.
    #[error("local I/O: {0}")]
    Io(#[from] std::io::Error),
    /// A segment did not decode.
    #[error("segment: {0}")]
    Segment(#[from] skyfs_format::DecodeError),
    /// A manifest or hint did not decode.
    #[error("decode: {0}")]
    Decode(String),
    /// No volume exists at this root.
    #[error("no volume at {0}")]
    NotFound(String),
    /// A volume already exists at this root.
    #[error("a volume already exists at {0}")]
    Exists(String),
    /// The bucket's content contradicts itself: broken hash chain, foreign
    /// snapshot, missing segment. The mount is refused.
    #[error("refusing to mount: {0}")]
    Corrupt(String),
    /// Another process holds this mount's local directory.
    #[error("local directory {0} is in use by another skyfs process")]
    Busy(String),
    /// The bucket did not answer in time.
    #[error("timed out: {0}")]
    Timeout(String),
    /// The mount was abandoned before the log was committed; the local WAL
    /// is kept for the next mount on this disk.
    #[error(
        "abandoned with uncommitted operations; the local WAL is kept for the next mount on this disk"
    )]
    Aborted,
    /// Another writer committed the seq this one needed: this writer is
    /// stale and must stop.
    #[error("fenced: another writer took over the volume ({0})")]
    Fenced(String),
}

impl From<prost::DecodeError> for VolumeError {
    fn from(e: prost::DecodeError) -> Self {
        Self::Decode(e.to_string())
    }
}
