//! A miniature writer for tests: validates with `plan`, logs into real
//! encoded segments, applies live, and keeps a reference model of every
//! regular file's bytes.

#![allow(
    dead_code,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation
)]

use std::collections::{BTreeMap, HashMap};

use skyfs_format::wal::v1::{
    FallocateMode, MountBegin, Record, RenameMode, SegmentHeader, VolumeCreate, XattrMode,
    record::Op,
};
use skyfs_format::{PREAMBLE_LEN, Preamble, Segment, SegmentBuilder};
use skyfs_meta::plan::{self, AttrChanges, Caller, MakeRequest, WriteData};
use skyfs_meta::{Errno, Ino, Kind, Location, MetaStore, ROOT_INO};

pub const VOLUME_ID: [u8; 16] = [9; 16];
pub const CALLER: Caller = Caller {
    uid: 1000,
    gid: 1000,
};
/// Writes larger than this go to a blob, as the real writer does.
pub const INLINE_MAX: usize = 1024;

pub struct Harness {
    pub live: MetaStore,
    /// Finished segment objects, indexed by seq.
    pub segments: Vec<Vec<u8>>,
    open: SegmentBuilder,
    seq: u64,
    epoch: u64,
    next_op: u64,
    prev_digest: Vec<u8>,
    time: i64,
    pub blobs: HashMap<String, Vec<u8>>,
    /// Reference contents of regular files.
    pub model: HashMap<Ino, Vec<u8>>,
}

fn header(seq: u64, epoch: u64, first_op_id: u64, prev_digest: Vec<u8>) -> SegmentHeader {
    SegmentHeader {
        volume_id: VOLUME_ID.to_vec(),
        seq,
        epoch,
        prev_digest,
        first_op_id,
        ..Default::default()
    }
}

impl Default for Harness {
    fn default() -> Self {
        Self::new()
    }
}

impl Harness {
    /// A new volume: segment 0 holds `VolumeCreate`, then a mount begins.
    pub fn new() -> Self {
        let mut h = Self {
            live: MetaStore::in_memory().unwrap(),
            segments: Vec::new(),
            open: SegmentBuilder::new(header(0, 0, 0, Vec::new())),
            seq: 0,
            epoch: 0,
            next_op: 0,
            prev_digest: Vec::new(),
            time: 1_700_000_000_000_000_000,
            blobs: HashMap::new(),
            model: HashMap::new(),
        };
        h.log(Op::VolumeCreate(VolumeCreate {
            volume_id: VOLUME_ID.to_vec(),
            root_mode: 0o755,
            root_uid: 0,
            root_gid: 0,
            inline_data_max: INLINE_MAX as u32,
        }));
        h.seal();
        h.remount();
        h
    }

    /// Appends and applies one operation.
    pub fn log(&mut self, op: Op) {
        self.time += 1_000;
        let record = Record {
            op_id: self.next_op,
            time_ns: self.time,
            op: Some(op),
        };
        self.next_op += 1;
        self.live
            .apply(&record, self.seq)
            .expect("a planned op applies");
        self.open.push_record(&record);
    }

    /// Seals the open segment (if it holds anything) and opens the next.
    pub fn seal(&mut self) {
        if self.open.record_count() == 0 {
            return;
        }
        let next = SegmentBuilder::new(header(self.seq + 1, self.epoch, self.next_op, Vec::new()));
        let (bytes, digest) = std::mem::replace(&mut self.open, next).finish();
        self.live
            .finish_segment(self.seq, &digest, self.epoch)
            .unwrap();
        self.segments.push(bytes);
        self.prev_digest = digest.to_vec();
        self.seq += 1;
        self.open = SegmentBuilder::new(header(
            self.seq,
            self.epoch,
            self.next_op,
            self.prev_digest.clone(),
        ));
    }

    /// Ends the current mount and starts a new epoch, as a remount does.
    /// Open-but-unlinked files are freed by the `MountBegin`.
    pub fn remount(&mut self) {
        self.seal();
        self.epoch += 1;
        self.open = SegmentBuilder::new(header(
            self.seq,
            self.epoch,
            self.next_op,
            self.prev_digest.clone(),
        ));
        self.log(Op::MountBegin(MountBegin {
            epoch: self.epoch,
            writer_id: b"test".to_vec(),
            previous_clean: true,
            ..Default::default()
        }));
        let live: Vec<Ino> = self.model.keys().copied().collect();
        for ino in live {
            if self.live.getattr(ino).unwrap().is_none() {
                self.model.remove(&ino);
            }
        }
    }

    // ------------------------------------------------------------ requests

    pub fn make(
        &mut self,
        parent: Ino,
        name: &str,
        kind: Kind,
        target: &str,
    ) -> Result<Ino, Errno> {
        let op = plan::make(
            &self.live,
            CALLER,
            &MakeRequest {
                parent,
                name: name.as_bytes(),
                kind,
                mode: 0o755,
                umask: 0o022,
                rdev: 0,
                target: target.as_bytes(),
            },
        )?;
        let Op::Make(m) = &op else { unreachable!() };
        let ino = m.ino;
        self.log(op);
        if kind == Kind::Regular {
            self.model.insert(ino, Vec::new());
        }
        Ok(ino)
    }

    pub fn mkdir(&mut self, parent: Ino, name: &str) -> Result<Ino, Errno> {
        self.make(parent, name, Kind::Directory, "")
    }

    pub fn create(&mut self, parent: Ino, name: &str) -> Result<Ino, Errno> {
        self.make(parent, name, Kind::Regular, "")
    }

    pub fn write(&mut self, ino: Ino, offset: u64, bytes: &[u8]) -> Result<(), Errno> {
        plan::check_write(&self.live, ino, offset, bytes.len() as u64)?;
        if bytes.is_empty() {
            return Ok(());
        }
        let data = if bytes.len() > INLINE_MAX {
            let key = format!("blobs/{:016x}/{:016x}", self.epoch, self.blobs.len());
            self.blobs.insert(key.clone(), bytes.to_vec());
            WriteData::Blob { key, offset: 0 }
        } else {
            WriteData::Inline {
                offset: self.open.push_inline(bytes).offset,
            }
        };
        self.log(plan::write_op(ino, offset, bytes, data));
        let m = self.model.get_mut(&ino).unwrap();
        let end = offset as usize + bytes.len();
        if m.len() < end {
            m.resize(end, 0);
        }
        m[offset as usize..end].copy_from_slice(bytes);
        Ok(())
    }

    pub fn truncate(&mut self, ino: Ino, size: u64) -> Result<(), Errno> {
        let op = plan::set_attr(
            &self.live,
            ino,
            &AttrChanges {
                size: Some(size),
                ..Default::default()
            },
            self.time,
        )?;
        self.log(op);
        self.model.get_mut(&ino).unwrap().resize(size as usize, 0);
        Ok(())
    }

    pub fn set_attr(&mut self, ino: Ino, ch: &AttrChanges) -> Result<(), Errno> {
        let op = plan::set_attr(&self.live, ino, ch, self.time)?;
        self.log(op);
        if let Some(size) = ch.size {
            self.model.get_mut(&ino).unwrap().resize(size as usize, 0);
        }
        Ok(())
    }

    pub fn remove(&mut self, parent: Ino, name: &str, is_dir: bool) -> Result<Ino, Errno> {
        let op = plan::remove(&self.live, parent, name.as_bytes(), is_dir)?;
        let Op::Remove(r) = &op else { unreachable!() };
        let ino = r.ino;
        self.log(op);
        Ok(ino)
    }

    /// Unlink, then reclaim at once (no open handle).
    pub fn unlink_closed(&mut self, parent: Ino, name: &str, is_dir: bool) -> Result<(), Errno> {
        let ino = self.remove(parent, name, is_dir)?;
        self.reclaim(ino);
        Ok(())
    }

    pub fn reclaim(&mut self, ino: Ino) {
        if let Some(op) = plan::reclaim(&self.live, ino).unwrap() {
            self.log(op);
            self.model.remove(&ino);
        }
    }

    /// Frees every inode at nlink 0, as if all handles were closed.
    pub fn close_all(&mut self) {
        for ino in self.live.orphans().unwrap() {
            self.reclaim(ino);
        }
    }

    pub fn rename(
        &mut self,
        old_parent: Ino,
        old_name: &str,
        new_parent: Ino,
        new_name: &str,
        mode: RenameMode,
    ) -> Result<(), Errno> {
        if let Some(op) = plan::rename(
            &self.live,
            old_parent,
            old_name.as_bytes(),
            new_parent,
            new_name.as_bytes(),
            mode,
        )? {
            self.log(op);
        }
        Ok(())
    }

    pub fn link(&mut self, ino: Ino, parent: Ino, name: &str) -> Result<(), Errno> {
        let op = plan::link(&self.live, ino, parent, name.as_bytes())?;
        self.log(op);
        Ok(())
    }

    pub fn fallocate(
        &mut self,
        ino: Ino,
        off: u64,
        len: u64,
        mode: FallocateMode,
    ) -> Result<(), Errno> {
        let op = plan::fallocate(&self.live, ino, off, len, mode)?;
        self.log(op);
        let m = self.model.get_mut(&ino).unwrap();
        let (off, len, end) = (off as usize, len as usize, (off + len) as usize);
        let size = m.len();
        match mode {
            FallocateMode::Unspecified | FallocateMode::ZeroRange => {
                if m.len() < end {
                    m.resize(end, 0);
                }
                if mode == FallocateMode::ZeroRange {
                    m[off..end].fill(0);
                }
            }
            FallocateMode::KeepSize => {}
            FallocateMode::PunchHole | FallocateMode::ZeroRangeKeepSize => {
                if off < size {
                    m[off..end.min(size)].fill(0);
                }
            }
            FallocateMode::CollapseRange => {
                m.drain(off..end);
            }
            FallocateMode::InsertRange => {
                m.splice(off..off, std::iter::repeat_n(0, len));
            }
        }
        Ok(())
    }

    pub fn clone_range(
        &mut self,
        src: Ino,
        so: u64,
        dst: Ino,
        d_off: u64,
        len: u64,
    ) -> Result<(), Errno> {
        let Some((op, len)) = plan::clone_range(&self.live, src, so, dst, d_off, len)? else {
            return Ok(());
        };
        self.log(op);
        let bytes = self.model[&src][so as usize..(so + len) as usize].to_vec();
        let m = self.model.get_mut(&dst).unwrap();
        let end = (d_off + len) as usize;
        if m.len() < end {
            m.resize(end, 0);
        }
        m[d_off as usize..end].copy_from_slice(&bytes);
        Ok(())
    }

    pub fn set_xattr(
        &mut self,
        ino: Ino,
        name: &str,
        value: &[u8],
        mode: XattrMode,
    ) -> Result<(), Errno> {
        let op = plan::set_xattr(&self.live, ino, name.as_bytes(), value, mode)?;
        self.log(op);
        Ok(())
    }

    pub fn remove_xattr(&mut self, ino: Ino, name: &str) -> Result<(), Errno> {
        let op = plan::remove_xattr(&self.live, ino, name.as_bytes())?;
        self.log(op);
        Ok(())
    }

    // ------------------------------------------------------------- reading

    pub fn resolve(&self, path: &str) -> Option<Ino> {
        let mut ino = ROOT_INO;
        for part in path.split('/').filter(|p| !p.is_empty()) {
            ino = self.live.lookup(ino, part.as_bytes()).unwrap()?;
        }
        Some(ino)
    }

    /// Reads a whole file from `store`, resolving extents against this
    /// harness's segments and blobs and verifying every chunk's CRC.
    pub fn read(&self, store: &MetaStore, ino: Ino) -> Vec<u8> {
        let size = store.getattr(ino).unwrap().unwrap().size;
        let mut out = vec![0u8; size as usize];
        for e in store.extents(ino, 0, size).unwrap() {
            let chunk = self.chunk(&e.loc, e.chunk_off, e.chunk_len);
            assert_eq!(crc32c::crc32c(&chunk), e.crc, "chunk crc of ino {ino}");
            out[e.off as usize..e.end() as usize]
                .copy_from_slice(&chunk[e.skip as usize..(e.skip + e.len) as usize]);
        }
        out
    }

    fn chunk(&self, loc: &Location, off: u64, len: u64) -> Vec<u8> {
        let (off, len) = (off as usize, len as usize);
        match loc {
            Location::Segment { seq } => {
                let seg = self.segment_bytes(*seq);
                let base = Preamble::decode(&seg).unwrap().data_offset() as usize;
                seg[base + off..base + off + len].to_vec()
            }
            // In these tests a base volume's objects are the harness's own
            // blobs under the same keys (externalize keeps the key).
            Location::Object { key } | Location::External { key, .. } => {
                self.blobs[key][off..off + len].to_vec()
            }
        }
    }

    fn segment_bytes(&self, seq: u64) -> Vec<u8> {
        self.segments.get(seq as usize).cloned().unwrap_or_else(|| {
            // The open segment: finish a copy without disturbing it.
            panic!("segment {seq} is still open; seal before reading")
        })
    }

    /// Replays every sealed segment into a fresh store, through the real
    /// decoder, checking the hash chain.
    pub fn replay(&self) -> MetaStore {
        let mut store = MetaStore::in_memory().unwrap();
        replay_into(&mut store, &self.segments, 0);
        store
    }

    /// Every (path, ino, kind) reachable from the root, sorted by path.
    pub fn tree(&self, store: &MetaStore) -> BTreeMap<String, (Ino, Kind)> {
        let mut out = BTreeMap::new();
        let mut stack = vec![(String::new(), ROOT_INO)];
        while let Some((path, dir)) = stack.pop() {
            for e in store.readdir(dir, 0, usize::MAX).unwrap() {
                let p = format!("{path}/{}", String::from_utf8_lossy(&e.name));
                if e.kind == Kind::Directory {
                    stack.push((p.clone(), e.ino));
                }
                out.insert(p, (e.ino, e.kind));
            }
        }
        out
    }

    /// Asserts live == replay, both pass fsck, and every file's bytes equal
    /// the model after replay.
    pub fn check(&mut self) {
        self.seal();
        let replayed = self.replay();
        assert_eq!(
            self.live.dump().unwrap(),
            replayed.dump().unwrap(),
            "live vs replay"
        );
        assert_eq!(self.live.fsck().unwrap(), Vec::<String>::new(), "fsck live");
        assert_eq!(
            replayed.fsck().unwrap(),
            Vec::<String>::new(),
            "fsck replay"
        );
        for (ino, bytes) in &self.model {
            assert_eq!(&self.read(&replayed, *ino), bytes, "contents of ino {ino}");
        }
    }
}

/// Decodes and applies `segments[from..]`, checking seq, chain and op ids.
pub fn replay_into(store: &mut MetaStore, segments: &[Vec<u8>], from: usize) {
    for (i, bytes) in segments.iter().enumerate().skip(from) {
        let meta_len = Preamble::decode(bytes).unwrap().metadata_len() as usize;
        assert!(meta_len >= PREAMBLE_LEN);
        let (_, seg) = Segment::decode_metadata(&bytes[..meta_len]).unwrap();
        assert_eq!(seg.header.seq, i as u64);
        let pos = store.position().unwrap();
        assert_eq!(
            seg.header.prev_digest, pos.applied_digest,
            "hash chain at seq {i}"
        );
        assert_eq!(seg.header.first_op_id, pos.next_op_id);
        for r in &seg.records {
            store.apply(r, seg.header.seq).unwrap();
        }
        store
            .finish_segment(seg.header.seq, &seg.digest, seg.header.epoch)
            .unwrap();
    }
}
