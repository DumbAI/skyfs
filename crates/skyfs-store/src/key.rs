//! Object layout of a skyfs volume in blob storage.
//!
//! This module is the only place that spells an object key. Every key below
//! is relative to a volume root `<prefix>/volumes/<volume_id>/`, and every
//! object except `volume.json` is immutable once written:
//!
//! ```text
//! snapshots/<gen:020>.manifest      snapshot <gen>: the restore pointer; written last
//! snapshots/<gen:020>-<blake3>.meta.zst  metadata of snapshot <gen>, content-addressed
//! packs/<blake3>                    small-write data repacked by restore, content-addressed
//! blobs/<epoch:016x>/<n:016x>       large-write data, uploaded before the segment that references it
//! wal/<seq:020>.seg                 log tail after the restore pointer, If-None-Match: *
//! volume.json                       hint {snapshot gen, head seq}; mutable, never trusted
//! ```

use std::fmt;

/// A volume's root prefix in the bucket, e.g. `users/u1/volumes/<id>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeRoot(String);

impl VolumeRoot {
    /// `prefix` is the tenant prefix (e.g. `users/u1`); `volume_id` is the
    /// 16-byte volume id.
    #[must_use]
    pub fn new(prefix: &str, volume_id: &[u8; 16]) -> Self {
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            Self(format!("volumes/{}", hex(volume_id)))
        } else {
            Self(format!("{prefix}/volumes/{}", hex(volume_id)))
        }
    }

    /// Absolute prefix under which every object of `class` lives.
    #[must_use]
    pub fn class_prefix(&self, class: KeyClass) -> String {
        format!("{}/{}", self.0, class.prefix())
    }

    /// The volume-relative key of an absolute key under this root.
    #[must_use]
    pub fn relative<'a>(&self, absolute: &'a str) -> Option<&'a str> {
        absolute.strip_prefix(self.0.as_str())?.strip_prefix('/')
    }

    /// Absolute key of an object given its volume-relative key.
    #[must_use]
    pub fn join(&self, key: &Key) -> String {
        format!("{}/{}", self.0, key)
    }
}

impl fmt::Display for VolumeRoot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A volume-relative object key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Key {
    /// The mutable hint object.
    Hint,
    /// Log segment `seq`.
    Segment(u64),
    /// Manifest of snapshot `gen`: the restore pointer.
    SnapshotManifest(u64),
    /// Metadata file of snapshot `gen`, named by the BLAKE3 of its bytes.
    /// Content addressing lets a restore pass that crashed after uploading
    /// it retry without ever overwriting an object: a retry with other
    /// bytes gets another key, and the orphan is garbage.
    SnapshotMeta {
        /// Snapshot generation.
        generation: u64,
        /// BLAKE3 of the compressed metadata file.
        hash: [u8; 32],
    },
    /// Large-write blob `n` of mount epoch `epoch`.
    Blob {
        /// Mount epoch that wrote the blob.
        epoch: u64,
        /// Counter within the epoch.
        n: u64,
    },
    /// Content-addressed pack of repacked data (BLAKE3 of its bytes).
    Pack([u8; 32]),
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Key::Hint => f.write_str("volume.json"),
            Key::Segment(seq) => write!(f, "wal/{seq:020}.seg"),
            Key::SnapshotManifest(generation) => write!(f, "snapshots/{generation:020}.manifest"),
            Key::SnapshotMeta { generation, hash } => {
                write!(f, "snapshots/{generation:020}-{}.meta.zst", hex(hash))
            }
            Key::Blob { epoch, n } => write!(f, "blobs/{epoch:016x}/{n:016x}"),
            Key::Pack(hash) => write!(f, "packs/{}", hex(hash)),
        }
    }
}

/// A family of keys sharing a prefix, for listing. Only GC lists; no
/// correctness path does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyClass {
    /// `wal/`
    Segments,
    /// `snapshots/`
    Snapshots,
    /// `packs/`
    Packs,
    /// `blobs/`
    Blobs,
}

impl KeyClass {
    /// The volume-relative prefix, ending in `/`.
    #[must_use]
    pub fn prefix(self) -> &'static str {
        match self {
            Self::Segments => "wal/",
            Self::Snapshots => "snapshots/",
            Self::Packs => "packs/",
            Self::Blobs => "blobs/",
        }
    }
}

/// A string that is not a key this layout produces.
#[derive(Debug, thiserror::Error)]
#[error("not a skyfs object key: {0:?}")]
pub struct ParseKeyError(String);

impl std::str::FromStr for Key {
    type Err = ParseKeyError;

    /// Parses exactly the strings `Display` produces (as stored in
    /// `BlobRange.key`), and nothing else.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || ParseKeyError(s.to_owned());
        let dec = |d: &str, width: usize| {
            (d.len() == width && d.bytes().all(|b| b.is_ascii_digit()))
                .then(|| d.parse::<u64>().ok())
                .flatten()
        };
        let hex64 = |h: &str| {
            (h.len() == 16 && h.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
                .then(|| u64::from_str_radix(h, 16).ok())
                .flatten()
        };

        if s == "volume.json" {
            return Ok(Key::Hint);
        }
        if let Some(seq) = s.strip_prefix("wal/").and_then(|r| r.strip_suffix(".seg")) {
            return dec(seq, 20).map(Key::Segment).ok_or_else(err);
        }
        if let Some(rest) = s.strip_prefix("snapshots/") {
            if let Some(g) = rest.strip_suffix(".manifest") {
                return dec(g, 20).map(Key::SnapshotManifest).ok_or_else(err);
            }
            if let Some((g, h)) = rest
                .strip_suffix(".meta.zst")
                .and_then(|r| r.split_once('-'))
            {
                return match (dec(g, 20), unhex32(h)) {
                    (Some(generation), Some(hash)) => Ok(Key::SnapshotMeta { generation, hash }),
                    _ => Err(err()),
                };
            }
            return Err(err());
        }
        if let Some(rest) = s.strip_prefix("blobs/") {
            let (epoch, n) = rest.split_once('/').ok_or_else(err)?;
            return match (hex64(epoch), hex64(n)) {
                (Some(epoch), Some(n)) => Ok(Key::Blob { epoch, n }),
                _ => Err(err()),
            };
        }
        if let Some(h) = s.strip_prefix("packs/") {
            return unhex32(h).map(Key::Pack).ok_or_else(err);
        }
        Err(err())
    }
}

/// Lower-case hex of `bytes`.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    use fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            write!(s, "{b:02x}").expect("String grows");
            s
        })
}

/// Parses 64 lower-case hex digits.
#[must_use]
pub fn unhex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, pair) in s.as_bytes().chunks(2).enumerate() {
        let digit = |b: u8| match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            _ => None,
        };
        out[i] = digit(pair[0])? << 4 | digit(pair[1])?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_sort_in_log_order() {
        let a = Key::Segment(9).to_string();
        let b = Key::Segment(10).to_string();
        assert_eq!(a, "wal/00000000000000000009.seg");
        assert!(a < b);
    }

    #[test]
    fn every_key_round_trips() {
        for key in [
            Key::Hint,
            Key::Segment(42),
            Key::SnapshotManifest(7),
            Key::SnapshotMeta {
                generation: u64::MAX,
                hash: [7; 32],
            },
            Key::Blob { epoch: 3, n: 255 },
            Key::Pack([0xab; 32]),
        ] {
            assert_eq!(key.to_string().parse::<Key>().unwrap(), key);
        }
    }

    #[test]
    fn foreign_strings_are_rejected() {
        for s in [
            "wal/1.seg",
            "wal/0000000000000000000x.seg",
            "blobs/3/ff",
            "packs/ab",
            "snapshots/1.manifest",
            "snapshots/00000000000000000001.ckpt",
            "snapshots/00000000000000000001.meta.zst",
            "../x",
            "",
        ] {
            assert!(s.parse::<Key>().is_err(), "{s}");
        }
    }

    #[test]
    fn every_key_lives_under_its_class_prefix() {
        let classes = [
            (Key::Segment(1), KeyClass::Segments),
            (Key::SnapshotManifest(1), KeyClass::Snapshots),
            (Key::Pack([1; 32]), KeyClass::Packs),
            (Key::Blob { epoch: 1, n: 1 }, KeyClass::Blobs),
        ];
        for (key, class) in classes {
            assert!(key.to_string().starts_with(class.prefix()), "{key}");
        }
        let root = VolumeRoot::new("", &[0; 16]);
        let abs = root.join(&Key::Segment(3));
        assert_eq!(root.relative(&abs), Some("wal/00000000000000000003.seg"));
        assert!(abs.starts_with(&root.class_prefix(KeyClass::Segments)));
    }

    #[test]
    fn volume_root_joins_keys() {
        let root = VolumeRoot::new("users/u1/", &[0x01; 16]);
        assert_eq!(
            root.join(&Key::Segment(0)),
            "users/u1/volumes/01010101010101010101010101010101/wal/00000000000000000000.seg"
        );
    }
}
