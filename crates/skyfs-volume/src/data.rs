//! The read path (design §8.1): resolve a chunk to bytes, verify its
//! CRC32C, cache it.
//!
//! Sources, cheapest first: the local WAL (uncommitted segments and blobs
//! being uploaded), adopted local files in the cache (committed segments
//! and blobs this mount wrote), cached chunks, and finally a range GET.
//! Bytes from any source are verified before they are served, and a source
//! that fails verification is skipped, never trusted.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use skyfs_format::{PREAMBLE_LEN, Preamble};
use skyfs_meta::{Errno, Extent, Location};
use skyfs_store::{Cache, Key, ObjectClient};

use crate::wal;

/// Why a chunk could not be read.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// No source returned bytes matching the chunk's CRC.
    #[error("chunk {loc:?}+{off}..{len} fails its checksum from every source")]
    Corrupt {
        /// Where.
        loc: Location,
        /// Chunk offset.
        off: u64,
        /// Chunk length.
        len: u64,
    },
    /// The bucket could not be reached.
    #[error(transparent)]
    Store(#[from] skyfs_store::StoreError),
    /// A location that is not a key of this layout.
    #[error("bad object key {0:?}")]
    BadKey(String),
}

impl From<ReadError> for Errno {
    fn from(_: ReadError) -> Self {
        Errno::EIO
    }
}

/// The cache id of a committed segment's data region.
#[must_use]
pub fn segment_data_id(seq: u64) -> String {
    format!("{}#data", Key::Segment(seq))
}

/// Resolves and verifies chunks.
#[derive(Debug)]
pub struct DataReader {
    client: ObjectClient,
    cache: Arc<Cache>,
    wal_dir: PathBuf,
    /// Data-region offsets of committed segments, for range GETs.
    offsets: Mutex<HashMap<u64, u64>>,
}

fn read_file(path: &Path, off: u64, len: u64) -> Option<Vec<u8>> {
    let mut f = File::open(path).ok()?;
    f.seek(SeekFrom::Start(off)).ok()?;
    let mut buf = vec![0u8; usize::try_from(len).ok()?];
    f.read_exact(&mut buf).ok()?;
    Some(buf)
}

impl DataReader {
    /// A reader over `client`, `cache` and the local WAL in `wal_dir`.
    #[must_use]
    pub fn new(client: ObjectClient, cache: Arc<Cache>, wal_dir: PathBuf) -> Self {
        Self {
            client,
            cache,
            wal_dir,
            offsets: Mutex::new(HashMap::new()),
        }
    }

    /// Records where segment `seq`'s data region starts in its object.
    pub fn set_data_offset(&self, seq: u64, offset: u64) {
        self.offsets.lock().insert(seq, offset);
    }

    async fn data_offset(&self, seq: u64) -> Result<u64, ReadError> {
        if let Some(&off) = self.offsets.lock().get(&seq) {
            return Ok(off);
        }
        let key = Key::Segment(seq);
        let head = self.client.get_range(&key, 0..PREAMBLE_LEN as u64).await?;
        let pre = Preamble::decode(&head).map_err(|_| ReadError::Corrupt {
            loc: Location::Segment { seq },
            off: 0,
            len: 0,
        })?;
        self.set_data_offset(seq, pre.data_offset());
        Ok(pre.data_offset())
    }

    /// The bytes of the whole chunk `[off, off + len)` of `loc`, verified.
    pub async fn chunk(
        &self,
        loc: &Location,
        off: u64,
        len: u64,
        crc: u32,
    ) -> Result<Vec<u8>, ReadError> {
        let ok = |b: &Vec<u8>| crc32c::crc32c(b) == crc;
        let (local, adopted_id, remote_key) = match loc {
            Location::Segment { seq } => (
                Some(wal::dat_path(&self.wal_dir, *seq)),
                segment_data_id(*seq),
                Key::Segment(*seq),
            ),
            Location::Object { key } => {
                let k: Key = key.parse().map_err(|_| ReadError::BadKey(key.clone()))?;
                let local = match k {
                    Key::Blob { epoch, n } => Some(wal::blob_path(&self.wal_dir, epoch, n)),
                    _ => None,
                };
                (local, key.clone(), k)
            }
        };
        if let Some(b) = local.and_then(|p| read_file(&p, off, len)).filter(ok) {
            return Ok(b);
        }
        if let Some(b) = self.cache.read_object(&adopted_id, off, len).filter(ok) {
            return Ok(b);
        }
        let abs = match loc {
            Location::Segment { seq } => self.data_offset(*seq).await? + off,
            Location::Object { .. } => off,
        };
        let cache_key = remote_key.to_string();
        if let Some(b) = self.cache.get_chunk(&cache_key, abs, len).filter(ok) {
            return Ok(b);
        }
        let b = self
            .client
            .get_range(&remote_key, abs..abs + len)
            .await?
            .to_vec();
        if !ok(&b) {
            return Err(ReadError::Corrupt {
                loc: loc.clone(),
                off,
                len,
            });
        }
        self.cache.put_chunk(&cache_key, abs, len, &b);
        Ok(b)
    }

    /// The bytes an extent maps (its part of the chunk), verified.
    pub async fn extent(&self, e: &Extent) -> Result<Vec<u8>, ReadError> {
        let chunk = self.chunk(&e.loc, e.chunk_off, e.chunk_len, e.crc).await?;
        let s = usize::try_from(e.skip).unwrap_or(usize::MAX);
        let n = usize::try_from(e.len).unwrap_or(usize::MAX);
        chunk
            .get(s..s.saturating_add(n))
            .map(<[u8]>::to_vec)
            .ok_or(ReadError::Corrupt {
                loc: e.loc.clone(),
                off: e.chunk_off,
                len: e.chunk_len,
            })
    }
}
