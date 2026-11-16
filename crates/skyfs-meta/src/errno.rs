//! POSIX error numbers returned by a failed validation.

use std::fmt;

/// An errno for the platform skyfs runs on. Validation failures carry one;
/// they are returned to the caller and never logged.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Errno(pub i32);

impl Errno {
    /// Operation not permitted.
    pub const EPERM: Self = Self(libc::EPERM);
    /// No such file or directory.
    pub const ENOENT: Self = Self(libc::ENOENT);
    /// I/O error: also used for internal store failures.
    pub const EIO: Self = Self(libc::EIO);
    /// File exists.
    pub const EEXIST: Self = Self(libc::EEXIST);
    /// Cross-device link.
    pub const EXDEV: Self = Self(libc::EXDEV);
    /// Not a directory.
    pub const ENOTDIR: Self = Self(libc::ENOTDIR);
    /// Is a directory.
    pub const EISDIR: Self = Self(libc::EISDIR);
    /// Invalid argument.
    pub const EINVAL: Self = Self(libc::EINVAL);
    /// File too large.
    pub const EFBIG: Self = Self(libc::EFBIG);
    /// No space left on device.
    pub const ENOSPC: Self = Self(libc::ENOSPC);
    /// Read-only file system.
    pub const EROFS: Self = Self(libc::EROFS);
    /// Too many links.
    pub const EMLINK: Self = Self(libc::EMLINK);
    /// File name too long.
    pub const ENAMETOOLONG: Self = Self(libc::ENAMETOOLONG);
    /// Directory not empty.
    pub const ENOTEMPTY: Self = Self(libc::ENOTEMPTY);
    /// No data available (missing xattr).
    pub const ENODATA: Self = Self(libc::ENODATA);
    /// Operation not supported.
    pub const EOPNOTSUPP: Self = Self(libc::EOPNOTSUPP);
    /// Result too large (xattr buffer).
    pub const ERANGE: Self = Self(libc::ERANGE);
    /// Bad file descriptor.
    pub const EBADF: Self = Self(libc::EBADF);
    /// Function not implemented.
    pub const ENOSYS: Self = Self(libc::ENOSYS);
}

impl fmt::Debug for Errno {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Errno({})", self.0)
    }
}

impl fmt::Display for Errno {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", std::io::Error::from_raw_os_error(self.0))
    }
}

impl std::error::Error for Errno {}

impl From<Errno> for std::io::Error {
    fn from(e: Errno) -> Self {
        std::io::Error::from_raw_os_error(e.0)
    }
}
