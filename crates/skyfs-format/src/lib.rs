//! skyfs on-bucket formats: log segments and snapshot manifests.
//!
//! The log is the filesystem's source of truth: an ordered series of
//! immutable *segment* objects. A *snapshot* is the state after applying the
//! log through some segment, and its manifest is the restore pointer saying
//! how far blob storage has been restored. This crate defines both formats
//! (generated from `proto/skyfs/`) and the byte layout of a segment. It does
//! no I/O; see `docs/wal-format.md`.

pub mod segment;

/// Log record types, format version 1.
pub mod wal {
    /// Generated from `proto/skyfs/wal/v1/wal.proto`.
    #[allow(missing_docs, clippy::pedantic)]
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/skyfs.wal.v1.rs"));
    }
}

/// Snapshot manifest types, format version 1.
pub mod snapshot {
    /// Generated from `proto/skyfs/snapshot/v1/snapshot.proto`.
    #[allow(missing_docs, clippy::pedantic)]
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/skyfs.snapshot.v1.rs"));
    }
}

pub use segment::{DecodeError, PREAMBLE_LEN, Preamble, Segment, SegmentBuilder, records_digest};
