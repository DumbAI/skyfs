//! The skyfs state machine.
//!
//! The filesystem state after op *n* is `apply(snapshot, records up to n)`.
//! This crate owns both halves of that sentence for the writer and for
//! replay alike:
//!
//! - [`plan`] validates a POSIX request against the current state and turns
//!   it into a log operation, or returns an errno. Nothing is logged for a
//!   failed request.
//! - [`MetaStore::apply`] applies a logged record. It is the only way local
//!   metadata changes, it is used by the live writer, by mount replay and by
//!   the restore replica, and it never fails on a valid log: a failure is
//!   corruption.
//!
//! The store is SQLite (design §5.1). A snapshot is the store serialized with
//! `VACUUM INTO`, so the same file format is the live store, the restore
//! replica and the snapshot a mount downloads.

mod apply;
mod errno;
mod extents;
mod model;
pub mod plan;
mod schema;
mod store;

pub use errno::Errno;
pub use model::{
    Attr, DirEntry, Extent, Ino, Kind, Location, Position, ROOT_INO, StoreStats, VolumeInfo,
};
pub use schema::SCHEMA_VERSION;
pub use store::{ApplyError, MetaError, MetaStore, Relocation};

/// Permission, set-id and sticky bits: everything in `mode` but the file type.
pub const MODE_MASK: u32 = 0o7777;

/// `st_mode` bits. POSIX fixes these values, and they are the same on every
/// platform skyfs builds on; `libc` types them differently (u16 on macOS).
pub mod mode {
    /// File type mask.
    pub const S_IFMT: u32 = 0o170_000;
    /// Socket.
    pub const S_IFSOCK: u32 = 0o140_000;
    /// Symbolic link.
    pub const S_IFLNK: u32 = 0o120_000;
    /// Regular file.
    pub const S_IFREG: u32 = 0o100_000;
    /// Block device.
    pub const S_IFBLK: u32 = 0o060_000;
    /// Directory.
    pub const S_IFDIR: u32 = 0o040_000;
    /// Character device.
    pub const S_IFCHR: u32 = 0o020_000;
    /// FIFO.
    pub const S_IFIFO: u32 = 0o010_000;
    /// Set-user-id.
    pub const S_ISUID: u32 = 0o4000;
    /// Set-group-id.
    pub const S_ISGID: u32 = 0o2000;
    /// Group execute.
    pub const S_IXGRP: u32 = 0o010;
}

/// Longest name a directory entry may have.
pub const NAME_MAX: usize = 255;
