//! The local WAL: segments that are open or sealed but not yet committed
//! to the bucket, as append-only files on local disk (design §6.2).
//!
//! A segment `seq` is two files:
//!
//! - `<seq>.rec`: frames `[len u32][crc32c u32][protobuf]`, the first a
//!   `SegmentHeader`, then one per `Record`;
//! - `<seq>.dat`: the data region, inline write bytes appended in order.
//!
//! The bucket object is built from them when the segment is uploaded. This
//! layout is local only: it can be appended in place, and a torn tail after
//! a crash is detected by the frame CRC and cut off. Recovery rebuilds the
//! same record region bytes, so the hash chain still holds.
//!
//! Large-write blobs being written are `blob-<epoch:016x>-<n:016x>` files in
//! the same directory.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use prost::Message;
use skyfs_format::wal::v1::{BlobRange, Record, SegmentHeader, data_ref, record::Op};
use skyfs_format::{PREAMBLE_LEN, Preamble, records_digest};
use skyfs_store::Key;

const FRAME_HEADER: usize = 8;

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(FRAME_HEADER + payload.len());
    let len = u32::try_from(payload.len()).expect("records are far below 4 GiB");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Makes a local file's data durable as the platform's `fsync` does.
///
/// On macOS that is `fsync(2)`, as every native filesystem gives an app
/// that calls `fsync`: the data reaches the drive, without the drive-cache
/// flush of `F_FULLFSYNC` (which Rust's `sync_data` issues, at 100 times
/// the cost). Elsewhere it is `fdatasync`. Recovery does not depend on the
/// order in which synced writes persist: it keeps only the prefix of
/// records whose bytes verify (design §6.6), and in async mode every op
/// reaches the bucket within `max-lag` anyway.
pub(crate) fn sync_data(f: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        rustix::fs::fsync(f).map_err(io::Error::from)
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        f.sync_data()
    }
}

/// Paths of segment `seq`'s local files.
#[must_use]
pub fn rec_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:020}.rec"))
}

/// Path of segment `seq`'s local data region.
#[must_use]
pub fn dat_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:020}.dat"))
}

/// Local file of blob `(epoch, n)` while it is written and uploaded.
#[must_use]
pub fn blob_path(dir: &Path, epoch: u64, n: u64) -> PathBuf {
    dir.join(format!("blob-{epoch:016x}-{n:016x}"))
}

/// The segment currently receiving records.
#[derive(Debug)]
pub struct OpenSegment {
    dir: PathBuf,
    header: SegmentHeader,
    rec: File,
    dat: File,
    data_len: u64,
    records: Vec<Record>,
    records_bytes: usize,
    /// Blob keys referenced by records in this segment.
    pub blob_deps: BTreeSet<String>,
    /// When the first record arrived, for the replication deadline.
    pub first_at: Option<Instant>,
}

impl OpenSegment {
    /// Creates the local files of a new segment.
    pub fn create(dir: &Path, header: SegmentHeader) -> io::Result<Self> {
        let seq = header.seq;
        let open = |p: PathBuf| {
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(p)
        };
        let mut rec = open(rec_path(dir, seq))?;
        let dat = open(dat_path(dir, seq))?;
        rec.write_all(&frame(&header.encode_to_vec()))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            header,
            rec,
            dat,
            data_len: 0,
            records: Vec::new(),
            records_bytes: 0,
            blob_deps: BTreeSet::new(),
            first_at: None,
        })
    }

    /// This segment's seq.
    #[must_use]
    pub fn seq(&self) -> u64 {
        self.header.seq
    }

    /// Records appended so far.
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Approximate size of the object this segment will become.
    #[must_use]
    pub fn size(&self) -> u64 {
        PREAMBLE_LEN as u64 + self.records_bytes as u64 + self.data_len + 64
    }

    /// Appends inline bytes to the data region; returns their offset.
    pub fn push_data(&mut self, bytes: &[u8]) -> io::Result<u64> {
        let off = self.data_len;
        self.dat.write_all(bytes)?;
        self.data_len += bytes.len() as u64;
        Ok(off)
    }

    /// Cuts the data region back to `len`, undoing a [`push_data`](Self::push_data)
    /// whose record could not be logged.
    pub fn truncate_data(&mut self, len: u64) -> io::Result<()> {
        self.dat.set_len(len)?;
        self.dat.seek(SeekFrom::Start(len))?;
        self.data_len = len;
        Ok(())
    }

    /// Current data region length.
    #[must_use]
    pub fn data_len(&self) -> u64 {
        self.data_len
    }

    /// Appends a record frame.
    pub fn push_record(&mut self, record: &Record) -> io::Result<()> {
        self.rec.write_all(&frame(&record.encode_to_vec()))?;
        self.records_bytes += record.encoded_len() + 10;
        self.records.push(record.clone());
        if self.first_at.is_none() {
            self.first_at = Some(Instant::now());
        }
        if let Some(Op::Write(w)) = &record.op
            && let Some(data_ref::Location::Blob(b)) =
                w.data.as_ref().and_then(|d| d.location.as_ref())
        {
            self.blob_deps.insert(b.key.clone());
        }
        Ok(())
    }

    /// Handles for an `fdatasync` outside the writer's lock.
    pub fn sync_handles(&self) -> io::Result<(File, File)> {
        Ok((self.dat.try_clone()?, self.rec.try_clone()?))
    }

    /// Makes the segment durable locally and freezes it.
    pub fn seal(self) -> io::Result<Sealed> {
        // Data first: a record on disk must never point at missing bytes.
        sync_data(&self.dat)?;
        sync_data(&self.rec)?;
        Ok(Sealed::new(
            &self.dir,
            self.header,
            self.records,
            self.data_len,
            self.blob_deps.into_iter().collect(),
        ))
    }
}

/// A segment whose records are final, waiting to be (or being) uploaded.
#[derive(Debug)]
pub struct Sealed {
    dir: PathBuf,
    /// Header with the final `record_count`.
    pub header: SegmentHeader,
    /// The records, in order.
    pub records: Arc<Vec<Record>>,
    /// The encoded record region.
    pub region: Vec<u8>,
    /// BLAKE3 of the record region.
    pub digest: [u8; 32],
    /// Data region length.
    pub data_len: u64,
    /// Blob keys that must be uploaded before this segment.
    pub blob_deps: Vec<String>,
}

impl Sealed {
    fn new(
        dir: &Path,
        mut header: SegmentHeader,
        records: Vec<Record>,
        data_len: u64,
        blob_deps: Vec<String>,
    ) -> Self {
        header.record_count = records.len() as u64;
        let mut region = Vec::new();
        header
            .encode_length_delimited(&mut region)
            .expect("Vec grows");
        for r in &records {
            r.encode_length_delimited(&mut region).expect("Vec grows");
        }
        let digest = records_digest(&region);
        Self {
            dir: dir.to_path_buf(),
            header,
            records: Arc::new(records),
            region,
            digest,
            data_len,
            blob_deps,
        }
    }

    /// The segment's seq.
    #[must_use]
    pub fn seq(&self) -> u64 {
        self.header.seq
    }

    /// Offset of the data region in the object.
    #[must_use]
    pub fn data_offset(&self) -> u64 {
        (PREAMBLE_LEN + self.region.len()) as u64
    }

    /// Last op id in the segment (or the one before the first, if empty).
    #[must_use]
    pub fn last_op_id(&self) -> Option<u64> {
        self.records.last().map(|r| r.op_id)
    }

    /// Builds the object: preamble, record region, data region.
    pub fn object_bytes(&self) -> io::Result<Vec<u8>> {
        let preamble = Preamble {
            records_len: self.region.len() as u64,
            data_len: self.data_len,
            records_crc32c: crc32c::crc32c(&self.region),
        };
        let mut out = Vec::with_capacity(
            PREAMBLE_LEN + self.region.len() + usize::try_from(self.data_len).unwrap_or(0),
        );
        out.extend_from_slice(&preamble.encode());
        out.extend_from_slice(&self.region);
        let mut f = File::open(dat_path(&self.dir, self.seq()))?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;
        if (data.len() as u64) < self.data_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "local data region is short",
            ));
        }
        out.extend_from_slice(&data[..usize::try_from(self.data_len).map_err(io::Error::other)?]);
        Ok(out)
    }

    /// Removes the record file once the segment is committed; the data file
    /// is handed to the cache by the caller.
    pub fn retire(&self) {
        let _ = fs::remove_file(rec_path(&self.dir, self.seq()));
    }

    /// Path of the local data region.
    #[must_use]
    pub fn data_path(&self) -> PathBuf {
        dat_path(&self.dir, self.seq())
    }
}

/// Reads every local segment in `dir`, in seq order, cutting each at its
/// first torn or invalid frame. Empty segments are deleted.
pub fn recover(dir: &Path) -> io::Result<Vec<Sealed>> {
    let mut seqs: Vec<u64> = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        if let Some(seq) = name.strip_suffix(".rec").and_then(|s| s.parse().ok()) {
            seqs.push(seq);
        }
    }
    seqs.sort_unstable();
    let mut out = Vec::new();
    for seq in seqs {
        if let Some(s) = recover_one(dir, seq)? {
            out.push(s);
        } else {
            let _ = fs::remove_file(rec_path(dir, seq));
            let _ = fs::remove_file(dat_path(dir, seq));
        }
    }
    Ok(out)
}

/// Whether the bytes a blob write references are intact on this disk, or
/// no longer here because the blob was uploaded.
fn local_blob_ok(dir: &Path, b: &BlobRange, crc: u32) -> bool {
    let Ok(Key::Blob { epoch, n }) = b.key.parse::<Key>() else {
        return false;
    };
    let path = blob_path(dir, epoch, n);
    let Ok(mut f) = File::open(&path) else {
        return !path.exists();
    };
    let Ok(len) = usize::try_from(b.len) else {
        return false;
    };
    let mut buf = vec![0u8; len];
    f.seek(SeekFrom::Start(b.offset)).is_ok()
        && f.read_exact(&mut buf).is_ok()
        && crc32c::crc32c(&buf) == crc
}

fn recover_one(dir: &Path, seq: u64) -> io::Result<Option<Sealed>> {
    let rec = fs::read(rec_path(dir, seq))?;
    let data = fs::read(dat_path(dir, seq)).unwrap_or_default();
    let mut frames = Vec::new();
    let mut at = 0usize;
    while let Some(hdr) = rec.get(at..at + FRAME_HEADER) {
        let len = u32::from_le_bytes(hdr[0..4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(hdr[4..8].try_into().expect("4 bytes"));
        let Some(payload) = rec.get(at + FRAME_HEADER..at + FRAME_HEADER + len) else {
            break;
        };
        if crc32c::crc32c(payload) != crc {
            break;
        }
        frames.push(payload);
        at += FRAME_HEADER + len;
    }
    let Some((head, rest)) = frames.split_first() else {
        return Ok(None);
    };
    let Ok(header) = SegmentHeader::decode(*head) else {
        return Ok(None);
    };
    if header.seq != seq {
        return Ok(None);
    }
    let mut records = Vec::new();
    let mut deps = BTreeSet::new();
    for payload in rest {
        let Ok(r) = Record::decode(*payload) else {
            break;
        };
        // A record whose inline bytes did not reach the disk ends the
        // valid prefix.
        if let Some(Op::Write(w)) = &r.op {
            match w.data.as_ref().and_then(|d| d.location.as_ref()) {
                Some(data_ref::Location::Inline(i)) => {
                    let ok = usize::try_from(i.offset)
                        .ok()
                        .zip(usize::try_from(i.len).ok())
                        .and_then(|(o, l)| data.get(o..o + l))
                        .is_some_and(|b| crc32c::crc32c(b) == w.crc32c);
                    if !ok {
                        break;
                    }
                }
                Some(data_ref::Location::Blob(b)) => {
                    // Likewise for a blob still on this disk. One that is
                    // gone was uploaded whole before it left (it moves to
                    // the cache only after its upload).
                    if !local_blob_ok(dir, b, w.crc32c) {
                        break;
                    }
                    deps.insert(b.key.clone());
                }
                None => break,
            }
        }
        records.push(r);
    }
    if records.is_empty() {
        return Ok(None);
    }
    Ok(Some(Sealed::new(
        dir,
        header,
        records,
        data.len() as u64,
        deps.into_iter().collect(),
    )))
}

/// Local blob files in `dir`: `(epoch, n, path)`.
pub fn local_blobs(dir: &Path) -> io::Result<Vec<(u64, u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(rest) = name.strip_prefix("blob-") else {
            continue;
        };
        let Some((e, n)) = rest.split_once('-') else {
            continue;
        };
        if let (Ok(e), Ok(n)) = (u64::from_str_radix(e, 16), u64::from_str_radix(n, 16)) {
            out.push((e, n, entry.path()));
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use skyfs_format::Segment;
    use skyfs_format::wal::v1::{DataRef, InlineRange, MountEnd, Write as WriteOp};

    fn header(seq: u64) -> SegmentHeader {
        SegmentHeader {
            volume_id: vec![1; 16],
            seq,
            epoch: 2,
            first_op_id: 10,
            ..Default::default()
        }
    }

    fn write_rec(op_id: u64, off: u64, bytes: &[u8]) -> Record {
        Record {
            op_id,
            time_ns: 1,
            op: Some(Op::Write(WriteOp {
                ino: 5,
                offset: 0,
                len: bytes.len() as u64,
                data: Some(DataRef {
                    location: Some(data_ref::Location::Inline(InlineRange {
                        offset: off,
                        len: bytes.len() as u64,
                    })),
                }),
                crc32c: crc32c::crc32c(bytes),
                cleared_setid: false,
            })),
        }
    }

    #[test]
    fn sealed_object_decodes_like_the_builder() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = OpenSegment::create(dir.path(), header(4)).unwrap();
        let off = s.push_data(b"hello").unwrap();
        s.push_record(&write_rec(10, off, b"hello")).unwrap();
        s.push_record(&Record {
            op_id: 11,
            time_ns: 2,
            op: Some(Op::MountEnd(MountEnd {})),
        })
        .unwrap();
        let sealed = s.seal().unwrap();
        let bytes = sealed.object_bytes().unwrap();
        let (pre, seg) = Segment::decode_metadata(&bytes).unwrap();
        assert_eq!(seg.records.len(), 2);
        assert_eq!(seg.digest, sealed.digest);
        assert_eq!(pre.data_offset(), sealed.data_offset());
        let at = usize::try_from(pre.data_offset()).unwrap();
        assert_eq!(&bytes[at..at + 5], b"hello");
    }

    #[test]
    fn recovery_cuts_torn_tails_and_rebuilds_the_same_region() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = OpenSegment::create(dir.path(), header(7)).unwrap();
        let off = s.push_data(b"abc").unwrap();
        s.push_record(&write_rec(10, off, b"abc")).unwrap();
        // A record whose data never hit the disk.
        s.push_record(&write_rec(11, 100, b"zzz")).unwrap();
        drop(s);
        // And a torn frame.
        let mut f = OpenOptions::new()
            .append(true)
            .open(rec_path(dir.path(), 7))
            .unwrap();
        f.write_all(&[9, 0, 0, 0, 1, 2]).unwrap();

        let rec = recover(dir.path()).unwrap();
        assert_eq!(rec.len(), 1);
        assert_eq!(rec[0].records.len(), 1);
        assert_eq!(rec[0].header.record_count, 1);

        let mut again = OpenSegment::create(&dir.path().join("x").tap_mkdir(), header(7)).unwrap();
        again.push_data(b"abc").unwrap();
        again.push_record(&write_rec(10, 0, b"abc")).unwrap();
        assert_eq!(again.seal().unwrap().digest, rec[0].digest);
    }

    trait TapMkdir {
        fn tap_mkdir(self) -> Self;
    }
    impl TapMkdir for PathBuf {
        fn tap_mkdir(self) -> Self {
            fs::create_dir_all(&self).unwrap();
            self
        }
    }
}
