//! Local disk cache of immutable bytes (design §8.1).
//!
//! Two kinds of entries share one LRU and one size limit:
//!
//! - **chunks**: verified byte ranges fetched from the bucket, keyed by
//!   `(object key, offset, length)`;
//! - **objects**: whole local files adopted after they were committed (a
//!   segment's data region, an uploaded blob), keyed by an id the caller
//!   chooses, and read by range.
//!
//! Everything cached is immutable in the bucket, so entries never go stale
//! and the cache survives remounts. Files that are not yet committed never
//! live here; they stay in the WAL directory, which is never evicted.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

/// A size-bounded LRU cache in one directory.
#[derive(Debug)]
pub struct Cache {
    dir: PathBuf,
    limit: u64,
    lru: Mutex<Lru>,
}

#[derive(Debug, Default)]
struct Lru {
    entries: HashMap<String, (u64, u64)>, // name -> (size, tick)
    order: BTreeMap<u64, String>,         // tick -> name
    used: u64,
    tick: u64,
}

impl Lru {
    fn touch(&mut self, name: &str) -> bool {
        let Some((_, tick)) = self.entries.get_mut(name) else {
            return false;
        };
        self.order.remove(tick);
        self.tick += 1;
        *tick = self.tick;
        self.order.insert(self.tick, name.to_owned());
        true
    }

    fn insert(&mut self, name: String, size: u64) {
        self.remove(&name);
        self.tick += 1;
        self.order.insert(self.tick, name.clone());
        self.entries.insert(name, (size, self.tick));
        self.used += size;
    }

    fn remove(&mut self, name: &str) {
        if let Some((size, tick)) = self.entries.remove(name) {
            self.order.remove(&tick);
            self.used -= size;
        }
    }
}

fn digest_name(prefix: char, parts: &[&[u8]]) -> String {
    let mut h = blake3::Hasher::new();
    for p in parts {
        h.update(&(p.len() as u64).to_le_bytes());
        h.update(p);
    }
    format!(
        "{prefix}{}",
        crate::key::hex(&h.finalize().as_bytes()[..16])
    )
}

impl Cache {
    /// Opens (or creates) the cache in `dir`, indexing what is there,
    /// oldest first by modification time.
    pub fn open(dir: &Path, limit: u64) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let mut found = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let meta = entry.metadata()?;
            if Path::new(&name).extension().is_some_and(|e| e == "tmp") || !meta.is_file() {
                let _ = fs::remove_file(entry.path());
                continue;
            }
            found.push((meta.modified()?, name, meta.len()));
        }
        found.sort();
        let mut lru = Lru::default();
        for (_, name, size) in found {
            lru.insert(name, size);
        }
        let cache = Self {
            dir: dir.to_path_buf(),
            limit,
            lru: Mutex::new(lru),
        };
        cache.evict();
        Ok(cache)
    }

    /// Bytes currently held.
    pub fn used(&self) -> u64 {
        self.lru.lock().used
    }

    fn chunk_name(key: &str, off: u64, len: u64) -> String {
        digest_name(
            'c',
            &[key.as_bytes(), &off.to_le_bytes(), &len.to_le_bytes()],
        )
    }

    fn object_name(id: &str) -> String {
        digest_name('o', &[id.as_bytes()])
    }

    /// A cached chunk, if present.
    pub fn get_chunk(&self, key: &str, off: u64, len: u64) -> Option<Vec<u8>> {
        let name = Self::chunk_name(key, off, len);
        if !self.lru.lock().touch(&name) {
            return None;
        }
        match fs::read(self.dir.join(&name)) {
            Ok(b) if b.len() as u64 == len => Some(b),
            _ => {
                self.lru.lock().remove(&name);
                None
            }
        }
    }

    /// Stores a verified chunk. Failures only cost a future refetch.
    pub fn put_chunk(&self, key: &str, off: u64, len: u64, bytes: &[u8]) {
        let name = Self::chunk_name(key, off, len);
        if self.write_atomic(&name, bytes).is_ok() {
            self.lru.lock().insert(name, bytes.len() as u64);
            self.evict();
        }
    }

    /// Moves a local file into the cache as object `id`.
    pub fn adopt(&self, id: &str, file: &Path) -> io::Result<()> {
        let name = Self::object_name(id);
        let size = fs::metadata(file)?.len();
        fs::rename(file, self.dir.join(&name))?;
        self.lru.lock().insert(name, size);
        self.evict();
        Ok(())
    }

    /// `len` bytes at `off` of adopted object `id`, if it is still cached.
    pub fn read_object(&self, id: &str, off: u64, len: u64) -> Option<Vec<u8>> {
        let name = Self::object_name(id);
        if !self.lru.lock().touch(&name) {
            return None;
        }
        let read = || -> io::Result<Vec<u8>> {
            let mut f = File::open(self.dir.join(&name))?;
            f.seek(SeekFrom::Start(off))?;
            let mut buf = vec![0u8; usize::try_from(len).map_err(io::Error::other)?];
            f.read_exact(&mut buf)?;
            Ok(buf)
        };
        if let Ok(b) = read() {
            Some(b)
        } else {
            self.lru.lock().remove(&name);
            None
        }
    }

    /// Drops adopted object `id` (e.g. a segment that turned out foreign).
    pub fn forget_object(&self, id: &str) {
        let name = Self::object_name(id);
        self.lru.lock().remove(&name);
        let _ = fs::remove_file(self.dir.join(name));
    }

    fn write_atomic(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        let tmp = self.dir.join(format!("{name}.{}.tmp", std::process::id()));
        fs::write(&tmp, bytes)?;
        fs::rename(&tmp, self.dir.join(name))
    }

    fn evict(&self) {
        let victims: Vec<String> = {
            let mut lru = self.lru.lock();
            let mut out = Vec::new();
            while lru.used > self.limit {
                let Some((_, name)) = lru.order.pop_first() else {
                    break;
                };
                if let Some((size, _)) = lru.entries.remove(&name) {
                    lru.used -= size;
                }
                out.push(name);
            }
            out
        };
        for name in victims {
            let _ = fs::remove_file(self.dir.join(name));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_round_trip_and_evict_lru() {
        let dir = tempfile::tempdir().unwrap();
        let c = Cache::open(dir.path(), 25).unwrap();
        c.put_chunk("packs/a", 0, 10, &[1; 10]);
        c.put_chunk("packs/a", 10, 10, &[2; 10]);
        assert_eq!(c.get_chunk("packs/a", 0, 10).unwrap(), vec![1; 10]); // now most recent
        c.put_chunk("packs/b", 0, 10, &[3; 10]); // evicts (a, 10)
        assert!(c.get_chunk("packs/a", 10, 10).is_none());
        assert!(c.get_chunk("packs/a", 0, 10).is_some());
        assert_eq!(c.used(), 20);
    }

    #[test]
    fn adopted_objects_are_read_by_range_and_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("seg.data");
        fs::write(&src, b"0123456789").unwrap();
        {
            let c = Cache::open(&dir.path().join("cache"), 1 << 20).unwrap();
            c.adopt("wal/7#data", &src).unwrap();
            assert_eq!(c.read_object("wal/7#data", 3, 4).unwrap(), b"3456");
            assert!(c.read_object("wal/7#data", 8, 4).is_none());
        }
        let c = Cache::open(&dir.path().join("cache"), 1 << 20).unwrap();
        assert_eq!(c.read_object("wal/7#data", 0, 2).unwrap(), b"01");
        c.forget_object("wal/7#data");
        assert!(c.read_object("wal/7#data", 0, 2).is_none());
    }
}
