//! The filesystem model: inodes, directory entries and extents.

use skyfs_format::wal::v1::FileKind;

/// An inode number. Allocated by the writer, never reused.
pub type Ino = u64;

/// The root directory, created by `VolumeCreate`.
pub const ROOT_INO: Ino = 1;

/// The type of an inode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    /// Regular file.
    Regular,
    /// Directory.
    Directory,
    /// Symbolic link.
    Symlink,
    /// Named pipe.
    Fifo,
    /// Unix domain socket.
    Socket,
    /// Character device.
    CharDevice,
    /// Block device.
    BlockDevice,
}

impl Kind {
    /// Decodes the log's `FileKind`; `None` for unspecified or unknown.
    #[must_use]
    pub fn from_wire(kind: i32) -> Option<Self> {
        Some(match FileKind::try_from(kind).ok()? {
            FileKind::Unspecified => return None,
            FileKind::Regular => Self::Regular,
            FileKind::Directory => Self::Directory,
            FileKind::Symlink => Self::Symlink,
            FileKind::Fifo => Self::Fifo,
            FileKind::Socket => Self::Socket,
            FileKind::CharDevice => Self::CharDevice,
            FileKind::BlockDevice => Self::BlockDevice,
        })
    }

    /// The log's `FileKind` for this kind. Also the value stored in SQLite.
    #[must_use]
    pub fn to_wire(self) -> FileKind {
        match self {
            Self::Regular => FileKind::Regular,
            Self::Directory => FileKind::Directory,
            Self::Symlink => FileKind::Symlink,
            Self::Fifo => FileKind::Fifo,
            Self::Socket => FileKind::Socket,
            Self::CharDevice => FileKind::CharDevice,
            Self::BlockDevice => FileKind::BlockDevice,
        }
    }

    /// The `S_IFMT` bits for this kind.
    #[must_use]
    pub fn mode_bits(self) -> u32 {
        match self {
            Self::Regular => crate::mode::S_IFREG,
            Self::Directory => crate::mode::S_IFDIR,
            Self::Symlink => crate::mode::S_IFLNK,
            Self::Fifo => crate::mode::S_IFIFO,
            Self::Socket => crate::mode::S_IFSOCK,
            Self::CharDevice => crate::mode::S_IFCHR,
            Self::BlockDevice => crate::mode::S_IFBLK,
        }
    }

    /// The kind encoded in the `S_IFMT` bits of a full `mode`.
    #[must_use]
    pub fn from_mode(mode: u32) -> Option<Self> {
        let fmt = mode & crate::mode::S_IFMT;
        [
            Self::Regular,
            Self::Directory,
            Self::Symlink,
            Self::Fifo,
            Self::Socket,
            Self::CharDevice,
            Self::BlockDevice,
        ]
        .into_iter()
        .find(|k| k.mode_bits() == fmt)
    }
}

/// Attributes of an inode, as `stat` reports them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attr {
    /// Inode number.
    pub ino: Ino,
    /// File type.
    pub kind: Kind,
    /// Permission, set-id and sticky bits (no file type).
    pub mode: u32,
    /// Owner.
    pub uid: u32,
    /// Group.
    pub gid: u32,
    /// Hard links; for directories 2 + subdirectories.
    pub nlink: u32,
    /// Size in bytes; for symlinks the target length.
    pub size: u64,
    /// Access time, ns since the epoch.
    pub atime_ns: i64,
    /// Modification time.
    pub mtime_ns: i64,
    /// Status change time.
    pub ctime_ns: i64,
    /// Device number of a device node.
    pub rdev: u64,
}

/// One entry of a directory listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    /// Entry name, raw bytes.
    pub name: Vec<u8>,
    /// Target inode.
    pub ino: Ino,
    /// Target kind.
    pub kind: Kind,
    /// Stable position in the directory, for `readdir` offsets. Cookies
    /// start at 3 (1 and 2 are `.` and `..`) and only grow.
    pub cookie: u64,
}

/// Where the bytes of a chunk live.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Location {
    /// The data region of log segment `seq`. Only the live store and the
    /// unrestored part of the replica hold these; snapshots never do.
    Segment {
        /// The segment's seq.
        seq: u64,
    },
    /// An immutable object (`blobs/…` or `packs/…`), volume-relative key.
    Object {
        /// Volume-relative key, as written in the log or by the restorer.
        key: String,
    },
    /// An immutable object of a sealed base volume, read-only (design §13).
    /// Only snapshots written by clone or publish create these; the log
    /// never names one, and this volume's GC never collects one.
    External {
        /// The base volume's 16-byte id (a row of the `bases` table).
        volume: [u8; 16],
        /// Key relative to the base volume's root.
        key: String,
    },
}

/// A mapping of file bytes `[off, off + len)` onto stored bytes.
///
/// The unit of verification is the *chunk*: the bytes of one write (or one
/// repacked extent), `[chunk_off, chunk_off + chunk_len)` of the location,
/// with one CRC32C. An overwrite may split an extent, but its pieces keep
/// pointing at the whole chunk with a `skip`, so a reader can always verify
/// what it serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extent {
    /// File offset.
    pub off: u64,
    /// Length in the file.
    pub len: u64,
    /// Where the chunk lives.
    pub loc: Location,
    /// Chunk start within the location (relative to the data region for
    /// [`Location::Segment`]).
    pub chunk_off: u64,
    /// Chunk length.
    pub chunk_len: u64,
    /// CRC32C of the whole chunk.
    pub crc: u32,
    /// Offset of this extent's first byte within the chunk.
    pub skip: u64,
}

impl Extent {
    /// End offset in the file.
    #[must_use]
    pub fn end(&self) -> u64 {
        self.off + self.len
    }

    /// The part of this extent inside `[start, end)`, if any.
    #[must_use]
    pub fn clip(&self, start: u64, end: u64) -> Option<Self> {
        let s = self.off.max(start);
        let e = self.end().min(end);
        (s < e).then(|| Self {
            off: s,
            len: e - s,
            skip: self.skip + (s - self.off),
            ..self.clone()
        })
    }
}

/// Volume identity, from `VolumeCreate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeInfo {
    /// 16-byte volume id.
    pub volume_id: Vec<u8>,
    /// Largest write stored inside a segment.
    pub inline_data_max: u32,
}

/// How far into the log a store has applied.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Position {
    /// The op id the next record must carry.
    pub next_op_id: u64,
    /// Last segment applied completely, if any.
    pub applied_seq: Option<u64>,
    /// BLAKE3 of that segment's record region.
    pub applied_digest: Vec<u8>,
    /// Epoch of that segment.
    pub applied_epoch: u64,
    /// Latest mount epoch seen in a `MountBegin`.
    pub epoch: u64,
    /// The latest mount ended with `MountEnd`.
    pub clean: bool,
}

/// Counters for `statfs` and the snapshot manifest.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreStats {
    /// Live inodes.
    pub inodes: u64,
    /// Logical bytes referenced by extents.
    pub data_bytes: u64,
}
