//! Byte layout of a segment object.
//!
//! ```text
//! +-----------------------------+  offset 0
//! | preamble (32 bytes)         |  magic, records_len, data_len, records_crc32c
//! +-----------------------------+  PREAMBLE_LEN
//! | record region               |  SegmentHeader, then Record x record_count,
//! |                             |  each varint-length-delimited protobuf
//! +-----------------------------+  PREAMBLE_LEN + records_len
//! | data region                 |  bytes of inline writes
//! +-----------------------------+  PREAMBLE_LEN + records_len + data_len
//! ```
//!
//! The record region comes first so a reader rebuilding metadata fetches
//! `PREAMBLE_LEN + records_len` bytes with one range GET and never downloads
//! file data. A segment is written with a single PUT, so it appears whole or
//! not at all.

use prost::Message;

use crate::wal::v1::{InlineRange, Record, SegmentHeader};

/// Magic bytes; the last byte is the major format version.
pub const MAGIC: [u8; 8] = *b"SKYWAL\0\x01";

/// Size of the fixed preamble at the start of every segment.
pub const PREAMBLE_LEN: usize = 32;

/// The fixed-size start of a segment, enough to locate both regions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Preamble {
    /// Length of the record region in bytes.
    pub records_len: u64,
    /// Length of the data region in bytes.
    pub data_len: u64,
    /// CRC32C of the record region.
    pub records_crc32c: u32,
}

impl Preamble {
    /// Encodes the preamble: magic, `records_len`, `data_len` (u64 LE),
    /// `records_crc32c` (u32 LE), four reserved zero bytes.
    #[must_use]
    pub fn encode(&self) -> [u8; PREAMBLE_LEN] {
        let mut out = [0u8; PREAMBLE_LEN];
        out[0..8].copy_from_slice(&MAGIC);
        out[8..16].copy_from_slice(&self.records_len.to_le_bytes());
        out[16..24].copy_from_slice(&self.data_len.to_le_bytes());
        out[24..28].copy_from_slice(&self.records_crc32c.to_le_bytes());
        out
    }

    /// Decodes and checks the magic and reserved bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let bytes: &[u8; PREAMBLE_LEN] = bytes
            .get(..PREAMBLE_LEN)
            .and_then(|b| b.try_into().ok())
            .ok_or(DecodeError::Truncated)?;
        if bytes[0..7] != MAGIC[0..7] {
            return Err(DecodeError::BadMagic);
        }
        if bytes[7] != MAGIC[7] {
            return Err(DecodeError::UnsupportedVersion(bytes[7]));
        }
        if bytes[28..32] != [0; 4] {
            return Err(DecodeError::ReservedBitsSet);
        }
        let [
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            r0,
            r1,
            r2,
            r3,
            r4,
            r5,
            r6,
            r7,
            d0,
            d1,
            d2,
            d3,
            d4,
            d5,
            d6,
            d7,
            c0,
            c1,
            c2,
            c3,
            ..,
        ] = *bytes;
        Ok(Self {
            records_len: u64::from_le_bytes([r0, r1, r2, r3, r4, r5, r6, r7]),
            data_len: u64::from_le_bytes([d0, d1, d2, d3, d4, d5, d6, d7]),
            records_crc32c: u32::from_le_bytes([c0, c1, c2, c3]),
        })
    }

    /// Bytes a reader must fetch to rebuild metadata: preamble + record region.
    #[must_use]
    pub fn metadata_len(&self) -> u64 {
        PREAMBLE_LEN as u64 + self.records_len
    }

    /// Offset of the data region from the start of the object.
    #[must_use]
    pub fn data_offset(&self) -> u64 {
        self.metadata_len()
    }
}

/// BLAKE3 of a record region; the next segment stores it as `prev_digest`,
/// so the segments of a volume form a hash chain.
#[must_use]
pub fn records_digest(records: &[u8]) -> [u8; 32] {
    *blake3::hash(records).as_bytes()
}

/// Accumulates records and inline data for one segment.
#[derive(Debug)]
pub struct SegmentBuilder {
    header: SegmentHeader,
    records: Vec<u8>,
    data: Vec<u8>,
}

impl SegmentBuilder {
    /// Starts a segment. `header.record_count` is filled in by [`finish`](Self::finish).
    #[must_use]
    pub fn new(header: SegmentHeader) -> Self {
        Self {
            header,
            records: Vec::new(),
            data: Vec::new(),
        }
    }

    /// Appends bytes to the data region and returns where they landed, for
    /// use in a `Write` record's `DataRef::Inline`.
    pub fn push_inline(&mut self, bytes: &[u8]) -> InlineRange {
        let range = InlineRange {
            offset: self.data.len() as u64,
            len: bytes.len() as u64,
        };
        self.data.extend_from_slice(bytes);
        range
    }

    /// Appends a record.
    pub fn push_record(&mut self, record: &Record) {
        record
            .encode_length_delimited(&mut self.records)
            .expect("Vec grows");
        self.header.record_count += 1;
    }

    /// Records pushed so far.
    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.header.record_count
    }

    /// Size the finished object will have, for sealing by size.
    #[must_use]
    pub fn encoded_len_estimate(&self) -> usize {
        PREAMBLE_LEN + self.header.encoded_len() + 10 + self.records.len() + self.data.len()
    }

    /// Produces the object bytes and the digest of its record region.
    #[must_use]
    pub fn finish(self) -> (Vec<u8>, [u8; 32]) {
        let mut region = Vec::with_capacity(self.records.len() + 64);
        self.header
            .encode_length_delimited(&mut region)
            .expect("Vec grows");
        region.extend_from_slice(&self.records);

        let preamble = Preamble {
            records_len: region.len() as u64,
            data_len: self.data.len() as u64,
            records_crc32c: crc32c::crc32c(&region),
        };
        let mut out = Vec::with_capacity(PREAMBLE_LEN + region.len() + self.data.len());
        out.extend_from_slice(&preamble.encode());
        out.extend_from_slice(&region);
        out.extend_from_slice(&self.data);
        (out, records_digest(&region))
    }
}

/// A decoded segment's metadata: header and records, without the data region.
#[derive(Debug, Clone, PartialEq)]
pub struct Segment {
    /// The segment header.
    pub header: SegmentHeader,
    /// Records in log order.
    pub records: Vec<Record>,
    /// BLAKE3 of the record region (the next segment's `prev_digest`).
    pub digest: [u8; 32],
}

impl Segment {
    /// Decodes the metadata part of a segment: `bytes` must start at offset 0
    /// and hold at least [`Preamble::metadata_len`] bytes; any data region
    /// after that is ignored.
    pub fn decode_metadata(bytes: &[u8]) -> Result<(Preamble, Self), DecodeError> {
        let preamble = Preamble::decode(bytes)?;
        let end = usize::try_from(preamble.metadata_len()).map_err(|_| DecodeError::Truncated)?;
        let region = bytes.get(PREAMBLE_LEN..end).ok_or(DecodeError::Truncated)?;
        if crc32c::crc32c(region) != preamble.records_crc32c {
            return Err(DecodeError::ChecksumMismatch);
        }

        let mut buf = region;
        let header = SegmentHeader::decode_length_delimited(&mut buf)?;
        let count = usize::try_from(header.record_count).map_err(|_| DecodeError::Truncated)?;
        let mut records = Vec::with_capacity(count.min(1 << 20));
        for _ in 0..count {
            let record = Record::decode_length_delimited(&mut buf)?;
            // An op this reader does not know decodes as `None`. Skipping it
            // would silently diverge the filesystem, so refuse.
            if record.op.is_none() {
                return Err(DecodeError::UnknownOp {
                    op_id: record.op_id,
                });
            }
            records.push(record);
        }
        if !buf.is_empty() {
            return Err(DecodeError::TrailingBytes);
        }
        Ok((
            preamble,
            Self {
                header,
                records,
                digest: records_digest(region),
            },
        ))
    }
}

/// Why a segment could not be decoded. Every variant means the mount must
/// stop: a segment is either read exactly or not used.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// Fewer bytes than the preamble or record region requires.
    #[error("segment truncated")]
    Truncated,
    /// Not a skyfs segment.
    #[error("bad magic")]
    BadMagic,
    /// Written by a newer major format version.
    #[error("unsupported segment format version {0}")]
    UnsupportedVersion(u8),
    /// Reserved preamble bytes are not zero.
    #[error("reserved preamble bytes set")]
    ReservedBitsSet,
    /// The record region does not match its CRC32C.
    #[error("record region checksum mismatch")]
    ChecksumMismatch,
    /// A record or header is not valid protobuf.
    #[error("invalid record encoding: {0}")]
    Protobuf(#[from] prost::DecodeError),
    /// A record holds an operation this reader does not know.
    #[error("unknown operation in record op_id {op_id}; upgrade skyfs")]
    UnknownOp {
        /// The record's op id.
        op_id: u64,
    },
    /// Bytes after the declared records.
    #[error("trailing bytes after the last record")]
    TrailingBytes,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::v1::{DataRef, FileKind, Make, Write, data_ref, record};

    fn header(seq: u64) -> SegmentHeader {
        SegmentHeader {
            volume_id: vec![7; 16],
            seq,
            epoch: 1,
            ..Default::default()
        }
    }

    fn make(op_id: u64) -> Record {
        Record {
            op_id,
            time_ns: 1_700_000_000_000_000_000,
            op: Some(record::Op::Make(Make {
                parent: 1,
                name: b"out.png".to_vec(),
                ino: 2,
                kind: FileKind::Regular as i32,
                mode: 0o644,
                ..Default::default()
            })),
        }
    }

    fn sample() -> (Vec<u8>, [u8; 32]) {
        let mut b = SegmentBuilder::new(header(3));
        b.push_record(&make(10));
        let range = b.push_inline(b"hello");
        b.push_record(&Record {
            op_id: 11,
            time_ns: 1,
            op: Some(record::Op::Write(Write {
                ino: 2,
                offset: 0,
                len: 5,
                data: Some(DataRef {
                    location: Some(data_ref::Location::Inline(range)),
                }),
                crc32c: crc32c::crc32c(b"hello"),
                cleared_setid: false,
            })),
        });
        b.finish()
    }

    #[test]
    fn round_trip_reads_metadata_without_data() {
        let (bytes, digest) = sample();
        let preamble = Preamble::decode(&bytes).unwrap();
        let meta_len = usize::try_from(preamble.metadata_len()).unwrap();
        let (_, seg) = Segment::decode_metadata(&bytes[..meta_len]).unwrap();

        assert_eq!(seg.header.seq, 3);
        assert_eq!(seg.header.record_count, 2);
        assert_eq!(seg.records.len(), 2);
        assert_eq!(seg.digest, digest);

        let Some(record::Op::Write(w)) = &seg.records[1].op else {
            panic!("write")
        };
        let Some(data_ref::Location::Inline(r)) = w.data.as_ref().and_then(|d| d.location.clone())
        else {
            panic!("inline")
        };
        let start = usize::try_from(preamble.data_offset() + r.offset).unwrap();
        assert_eq!(&bytes[start..start + 5], b"hello");
    }

    #[test]
    fn corrupt_record_region_is_rejected() {
        let (mut bytes, _) = sample();
        bytes[PREAMBLE_LEN + 4] ^= 0xff;
        assert!(matches!(
            Segment::decode_metadata(&bytes),
            Err(DecodeError::ChecksumMismatch)
        ));
    }

    #[test]
    fn newer_major_version_is_rejected() {
        let (mut bytes, _) = sample();
        bytes[7] = 2;
        assert!(matches!(
            Segment::decode_metadata(&bytes),
            Err(DecodeError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn truncated_segment_is_rejected() {
        let (bytes, _) = sample();
        assert!(matches!(
            Segment::decode_metadata(&bytes[..PREAMBLE_LEN + 3]),
            Err(DecodeError::Truncated)
        ));
    }

    #[test]
    fn unknown_op_fails_closed() {
        // A record with an op tag this reader does not know decodes with
        // `op == None`; simulate by encoding a record with no op.
        let mut b = SegmentBuilder::new(header(0));
        b.push_record(&Record {
            op_id: 1,
            time_ns: 0,
            op: None,
        });
        let (bytes, _) = b.finish();
        assert!(matches!(
            Segment::decode_metadata(&bytes),
            Err(DecodeError::UnknownOp { op_id: 1 })
        ));
    }
}
