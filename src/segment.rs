//! Immutable on-disk segment format.
//!
//! ```text
//! +-----------------+ offset 0
//! | file header     | 56 B  (magic, series, policy, block count, crc)
//! +-----------------+
//! | block header 0  | 40 B  (count, t0, t_last, ts_len, val_len, payload crc)
//! | block payload 0 | ts bytes (zigzag LEB128, byte aligned) ++ value bits
//! | block header 1  |
//! | block payload 1 |
//! | ...             |
//! +-----------------+
//! | footer          | 64 B  (min/max t, counts, sizes, file_len,
//! |                 |       crc over all block headers, end magic)
//! +-----------------+
//! ```
//!
//! # Integrity
//! * Every block payload has a CRC32C (Castagnoli). Queries verify only the
//!   blocks they touch — reading one minute never checksummed/decoded the
//!   whole segment.
//! * The footer carries a CRC32C over all block headers and a declared file
//!   length; opening validates magic + header CRC + footer CRC + structural
//!   bounds with two small fixed-size reads plus the block-header region.
//! * Failures return [`Corruption`] with exact byte ranges, e.g. a bad block
//!   payload reports `[payload_start, payload_end)`.
//!
//! # Atomicity of sealing
//! Writers stream to `<id>.seg.tmp`. A segment becomes visible only by:
//! flush → fsync file → close → rename to `<id>.seg` → fsync directory.
//! Only after that does the catalog transaction insert the row. A torn tail
//! write leaves a `.tmp` file that is never registered and is reaped on
//! startup; `.seg` files are immutable.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::codec::{decode_timestamps, encode_timestamps, ValueDecoder, ValueEncoder};
use crate::error::{Error, IoCtx, Result};
use crate::model::{DuplicatePolicy, Sample};

pub const MAGIC: &[u8; 8] = b"SSEG0001";
pub const END_MAGIC: &[u8; 8] = b"SSEGEND1";
pub const SERIES_ID_LEN: usize = 32;
pub const HEADER_LEN: usize = 56;
pub const BLOCK_HEADER_LEN: usize = 40;
pub const FOOTER_LEN: usize = 64;

/// Samples per block. A block is the random-access unit: ~512 samples means
/// a one-minute slice at high rates only ever decodes a handful of blocks.
pub const BLOCK_CAPACITY: usize = 512;

/// Byte range reported as corrupt, half-open `[start, end)`.
#[derive(Debug, Clone, Serialize)]
pub struct CorruptRange {
    pub start: u64,
    pub end: u64,
    pub what: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Corruption {
    pub path: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub file_len: u64,
    pub ranges: Vec<CorruptRange>,
}

impl Corruption {
    pub fn summary(&self) -> String {
        match self.ranges.as_slice() {
            [] => self.kind.clone(),
            [r] => format!("{} at bytes [{}, {}) — {}", r.what, r.start, r.end, r.detail),
            rs => format!(
                "{} corrupt region(s), first: {} at bytes [{}, {})",
                rs.len(),
                rs[0].what,
                rs[0].start,
                rs[0].end
            ),
        }
    }

    fn one(path: &str, kind: &str, file_len: u64, r: CorruptRange) -> Self {
        Self { path: path.to_string(), kind: kind.to_string(), file_len, ranges: vec![r] }
    }
}

/// In-memory view of a block header (the on-disk index).
#[derive(Debug, Clone)]
pub struct BlockMeta {
    pub count: u32,
    pub t0: i64,
    pub t_last: i64,
    pub ts_len: u32,
    pub val_len: u32,
    pub payload_crc: u32,
    pub payload_start: u64,
    pub next_offset: u64,
}

impl BlockMeta {
    fn encode(&self) -> [u8; BLOCK_HEADER_LEN] {
        let mut b = [0u8; BLOCK_HEADER_LEN];
        b[0..4].copy_from_slice(&self.count.to_le_bytes());
        b[4..12].copy_from_slice(&self.t0.to_le_bytes());
        b[12..20].copy_from_slice(&self.t_last.to_le_bytes());
        b[20..24].copy_from_slice(&self.ts_len.to_le_bytes());
        b[24..28].copy_from_slice(&self.val_len.to_le_bytes());
        b[28..32].copy_from_slice(&self.payload_crc.to_le_bytes());
        // last 8 bytes reserved/zero
        b
    }

    fn decode(buf: &[u8], payload_start: u64, next_offset: u64) -> Self {
        BlockMeta {
            count: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            t0: i64::from_le_bytes(buf[4..12].try_into().unwrap()),
            t_last: i64::from_le_bytes(buf[12..20].try_into().unwrap()),
            ts_len: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
            val_len: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            payload_crc: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
            payload_start,
            next_offset,
        }
    }

    pub fn payload_end(&self) -> u64 {
        self.payload_start + self.ts_len as u64 + self.val_len as u64
    }
}

/// Streaming writer: append samples; blocks are cut automatically. The file
/// is invisible to readers until [`SegmentWriter::seal`].
pub struct SegmentWriter {
    dir: PathBuf,
    final_path: PathBuf,
    tmp_path: PathBuf,
    file: Option<File>,
    series_id: String,
    policy: DuplicatePolicy,
    block_count: u32,
    /// Block header bytes retained so sealing can CRC them into the footer.
    header_bytes: Vec<u8>,
    current: Vec<Sample>,
    /// Last timestamp written, including across flushed blocks, so
    /// non-decreasing order is enforced for the whole stream.
    last_t: Option<i64>,
    offset: u64,
    min_t: Option<i64>,
    max_t: Option<i64>,
    total: u64,
    payload_bytes: u64,
    sealed: bool,
}

impl SegmentWriter {
    pub fn create(dir: &Path, segment_id: &str, series_id: &str, policy: DuplicatePolicy) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(|e| {
            Error::io(e, IoCtx::new("create segment directory", dir.display()))
        })?;
        let tmp_path = dir.join(format!("{segment_id}.seg.tmp"));
        let final_path = dir.join(format!("{segment_id}.seg"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(|e| Error::io(e, IoCtx::new("create temp segment", tmp_path.display())))?;
        file.set_len(0).ok();
        file.write_all(MAGIC).map_err(|e| Error::io(e, IoCtx::new("write header", tmp_path.display())))?;
        // Rest of header is zero until seal; reserve full 56 bytes.
        file.write_all(&[0u8; HEADER_LEN - MAGIC.len()])
            .map_err(|e| Error::io(e, IoCtx::new("reserve header", tmp_path.display())))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            final_path,
            tmp_path,
            file: Some(file),
            series_id: series_id.to_string(),
            policy,
            block_count: 0,
            header_bytes: Vec::new(),
            current: Vec::with_capacity(BLOCK_CAPACITY),
            last_t: None,
            offset: HEADER_LEN as u64,
            min_t: None,
            max_t: None,
            total: 0,
            payload_bytes: 0,
            sealed: false,
        })
    }

    /// Apply the per-series duplicate rule inside the pending block.
    /// Returns Err(Reject) when policy is Reject; callers abort the whole
    /// write so no partial batch is persisted.
    fn observe(&mut self, s: Sample) -> std::result::Result<(), Error> {
        // Ordering is enforced against the whole stream (including already
        // flushed blocks), not just the pending block.
        let bound = self
            .current
            .last()
            .map(|x| x.t)
            .or(self.last_t);
        if let Some(prev) = bound {
            if s.t < prev {
                return Err(Error::OutOfOrder { t: s.t, previous: prev });
            }
        }
        if let Some(prev) = self.current.iter().rev().find(|x| x.t <= s.t) {
            if prev.t == s.t {
                match self.policy {
                    DuplicatePolicy::KeepAll => {}
                    DuplicatePolicy::KeepFirst => return Ok(()),
                    DuplicatePolicy::KeepLast => {
                        // Replace the stored occurrence of this timestamp.
                        // Duplicates are adjacent in a non-decreasing stream.
                        for slot in self.current.iter_mut().rev() {
                            if slot.t == s.t {
                                slot.bits = s.bits;
                                return Ok(());
                            }
                            if slot.t < s.t {
                                break;
                            }
                        }
                        return Ok(());
                    }
                    DuplicatePolicy::Reject => {
                        return Err(Error::DuplicateTimestamp {
                            series: self.series_id.clone(),
                            t: s.t,
                        });
                    }
                }
            }
        }
        self.current.push(s);
        Ok(())
    }

    pub fn append(&mut self, samples: &[Sample]) -> Result<u64> {
        let mut added = 0u64;
        for &s in samples {
            let before = self.current.len();
            self.observe(s)?;
            if self.current.len() > before {
                added += 1;
            }
            if self.current.len() == BLOCK_CAPACITY {
                self.flush_block()?;
            }
        }
        Ok(added)
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }
        let block: Vec<Sample> = std::mem::take(&mut self.current);
        let count = block.len() as u32;
        let t0 = block.first().unwrap().t;
        let t_last = block.last().unwrap().t;

        let ts_payload = encode_timestamps(&block);
        let mut ve = ValueEncoder::new();
        for s in &block {
            ve.push(s.bits);
        }
        let val_payload = ve.finish();

        let mut crc = Crc32c::new();
        crc.update(&ts_payload);
        crc.update(&val_payload);

        let meta = BlockMeta {
            count,
            t0,
            t_last,
            ts_len: ts_payload.len() as u32,
            val_len: val_payload.len() as u32,
            payload_crc: crc.finalize(),
            payload_start: self.offset + BLOCK_HEADER_LEN as u64,
            next_offset: 0,
        };
        let hb = meta.encode();
        self.file_mut()
            .write_all(&hb)
            .map_err(|e| Error::io(e, IoCtx::new("write block header", self.tmp_path.display())))?;
        self.file_mut()
            .write_all(&ts_payload)
            .map_err(|e| Error::io(e, IoCtx::new("write timestamps", self.tmp_path.display())))?;
        self.file_mut()
            .write_all(&val_payload)
            .map_err(|e| Error::io(e, IoCtx::new("write values", self.tmp_path.display())))?;

        self.offset += BLOCK_HEADER_LEN as u64
            + ts_payload.len() as u64
            + val_payload.len() as u64;
        self.payload_bytes += ts_payload.len() as u64 + val_payload.len() as u64;
        self.total += count as u64;
        self.last_t = Some(t_last);
        self.min_t = Some(self.min_t.map_or(t0, |m| m.min(t0)));
        self.max_t = Some(self.max_t.map_or(t_last, |m| m.max(t_last)));
        self.block_count += 1;
        self.header_bytes.extend_from_slice(&hb);
        Ok(())
    }

    fn file_mut(&mut self) -> &mut File {
        self.file.as_mut().expect("writer already sealed")
    }

    /// Finish the file and atomically publish it. Returns the final path and
    /// sealing metadata. The catalog row must be written *after* this returns.
    pub fn seal(mut self) -> Result<SealedSegment> {
        self.flush_block()?;
        if self.total == 0 {
            return Err(Error::BadRequest(
                "cannot seal an empty segment; write at least one sample".into(),
            ));
        }
        let footer_start = self.offset;
        let block_count = self.block_count;
        let min_t = self.min_t.unwrap_or(0);
        let max_t = self.max_t.unwrap_or(0);
        let total = self.total;
        let payload_bytes = self.payload_bytes;

        let mut header_crc = Crc32c::new();
        // CRC covers header bytes after the crc field itself.
        let mut footer = Vec::with_capacity(FOOTER_LEN);
        footer.extend_from_slice(END_MAGIC); // 0..8
        footer.extend_from_slice(&min_t.to_le_bytes()); // 8..16
        footer.extend_from_slice(&max_t.to_le_bytes()); // 16..24
        footer.extend_from_slice(&total.to_le_bytes()); // 24..32
        footer.extend_from_slice(&payload_bytes.to_le_bytes()); // 32..40
        footer.extend_from_slice(&footer_start.to_le_bytes()); // 40..48
        let file_len = footer_start + FOOTER_LEN as u64;
        footer.extend_from_slice(&file_len.to_le_bytes()); // 48..56
        // crc of block header region: 56..60
        let mut index_crc = Crc32c::new();
        index_crc.update(&self.header_bytes);
        footer.extend_from_slice(&index_crc.finalize().to_le_bytes());
        // 60..64 reserved
        footer.extend_from_slice(&[0u8; 4]);
        assert_eq!(footer.len(), FOOTER_LEN);

        // Patch header (rewrite at offset 0).
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(MAGIC); // 0..8
        let mut sid = [0u8; SERIES_ID_LEN];
        let bytes = self.series_id.as_bytes();
        if bytes.len() != SERIES_ID_LEN {
            return Err(Error::BadRequest(format!(
                "series id must be {SERIES_ID_LEN} hex chars"
            )));
        }
        sid.copy_from_slice(bytes);
        header.extend_from_slice(&sid); // 8..40
        header.push(self.policy as u8); // 40
        header.push(1); // value codec = XOR64 v1  // 41
        header.extend_from_slice(&block_count.to_le_bytes()); // 42..46
        header.extend_from_slice(&[0u8; 6]); // reserved 46..52
        let hc = {
            header_crc.update(&header[0..52]);
            header_crc.finalize()
        };
        header.extend_from_slice(&hc.to_le_bytes()); // 52..56
        debug_assert_eq!(header.len(), HEADER_LEN);

        self.file_mut()
            .seek(SeekFrom::Start(0))
            .map_err(|e| Error::io(e, IoCtx::new("seek header", self.tmp_path.display())))?;
        self.file_mut()
            .write_all(&header)
            .map_err(|e| Error::io(e, IoCtx::new("write header", self.tmp_path.display())))?;
        self.file_mut()
            .seek(SeekFrom::Start(footer_start))
            .map_err(|e| Error::io(e, IoCtx::new("seek footer", self.tmp_path.display())))?;
        self.file_mut()
            .write_all(&footer)
            .map_err(|e| Error::io(e, IoCtx::new("write footer", self.tmp_path.display())))?;
        self.file_mut()
            .flush()
            .map_err(|e| Error::io(e, IoCtx::new("flush segment", self.tmp_path.display())))?;
        self.file_mut()
            .sync_all()
            .map_err(|e| Error::io(e, IoCtx::new("fsync segment", self.tmp_path.display())))?;
        drop(self.file.take());

        std::fs::rename(&self.tmp_path, &self.final_path).map_err(|e| {
            Error::io(e, IoCtx::new("rename temp segment", self.tmp_path.display()))
        })?;
        fsync_dir(&self.dir)?;

        self.sealed = true;
        Ok(SealedSegment {
            path: self.final_path.clone(),
            block_count,
            min_t,
            max_t,
            total,
            payload_bytes,
            file_len,
        })
    }
}

impl Drop for SegmentWriter {
    /// An unsealed writer (error/abort mid-batch) removes its temp tail so it
    /// can never be mistaken for committed data.
    fn drop(&mut self) {
        if !self.sealed {
            if let Some(f) = self.file.take() {
                let _ = f.sync_all();
            }
            let _ = std::fs::remove_file(&self.tmp_path);
        }
    }
}

fn fsync_dir(dir: &Path) -> Result<()> {
    let f = File::open(dir).map_err(|e| Error::io(e, IoCtx::new("open dir for fsync", dir.display())))?;
    f.sync_all()
        .map_err(|e| Error::io(e, IoCtx::new("fsync segment directory", dir.display())))?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct SealedSegment {
    pub path: PathBuf,
    pub block_count: u32,
    pub min_t: i64,
    pub max_t: i64,
    pub total: u64,
    pub payload_bytes: u64,
    pub file_len: u64,
}

/// Opened, validated immutable segment. Construction performs cheap
/// structural verification (header + block-index region + footer); block
/// payloads are checked lazily on read.
pub struct SegmentReader {
    file: File,
    path: PathBuf,
    pub series_id: String,
    pub policy: DuplicatePolicy,
    pub blocks: Vec<BlockMeta>,
    pub min_t: i64,
    pub max_t: i64,
    pub total: u64,
    pub payload_bytes: u64,
    pub file_len: u64,
    footer_start: u64,
}

impl SegmentReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)
            .map_err(|e| Error::io(e, IoCtx::new("open segment", path.display())))?;
        let len = file
            .metadata()
            .map_err(|e| Error::io(e, IoCtx::new("stat segment", path.display())))?
            .len();
        let p = path.display().to_string();
        if len < (HEADER_LEN + FOOTER_LEN) as u64 {
            return Err(Error::Corruption(Corruption::one(
                &p,
                "file_truncated",
                len,
                CorruptRange {
                    start: 0,
                    end: len,
                    what: "whole file".into(),
                    detail: format!(
                        "file is {len} bytes, smaller than header+footer ({})",
                        HEADER_LEN + FOOTER_LEN
                    ),
                },
            )));
        }

        let mut header = vec![0u8; HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|e| Error::io(e, IoCtx::new("read header", path.display())))?;
        if &header[0..8] != MAGIC {
            return Err(Error::Corruption(Corruption::one(
                &p,
                "bad_magic",
                len,
                CorruptRange { start: 0, end: 8, what: "file magic".into(), detail: "not SSEG0001".into() },
            )));
        }
        let stored_hc = u32::from_le_bytes(header[52..56].try_into().unwrap());
        let mut hc = Crc32c::new();
        hc.update(&header[0..52]);
        if hc.finalize() != stored_hc {
            return Err(Error::Corruption(Corruption::one(
                &p,
                "header_checksum",
                len,
                CorruptRange {
                    start: 0,
                    end: HEADER_LEN as u64,
                    what: "file header".into(),
                    detail: "CRC32C mismatch".into(),
                },
            )));
        }
        let series_id = String::from_utf8(header[8..40].to_vec())
            .map_err(|_| {
                Error::Corruption(Corruption::one(
                    &p,
                    "bad_header",
                    len,
                    CorruptRange {
                        start: 8,
                        end: 40,
                        what: "series id".into(),
                        detail: "non-UTF8".into(),
                    },
                ))
            })?;
        let policy = match header[40] {
            0 => DuplicatePolicy::KeepAll,
            1 => DuplicatePolicy::KeepFirst,
            2 => DuplicatePolicy::KeepLast,
            n => {
                return Err(Error::Corruption(Corruption::one(
                    &p,
                    "bad_header",
                    len,
                    CorruptRange {
                        start: 40,
                        end: 41,
                        what: "duplicate policy".into(),
                        detail: format!("unknown policy code {n}"),
                    },
                )))
            }
        };
        if header[41] != 1 {
            return Err(Error::Corruption(Corruption::one(
                &p,
                "bad_header",
                len,
                CorruptRange {
                    start: 41,
                    end: 42,
                    what: "value codec".into(),
                    detail: format!("unsupported codec version {}", header[41]),
                },
            )));
        }
        let block_count = u32::from_le_bytes(header[42..46].try_into().unwrap());

        // Footer.
        let footer_start = len - FOOTER_LEN as u64;
        let mut footer = vec![0u8; FOOTER_LEN];
        file.seek(SeekFrom::Start(footer_start))
            .map_err(|e| Error::io(e, IoCtx::new("seek footer", path.display())))?;
        file.read_exact(&mut footer)
            .map_err(|e| Error::io(e, IoCtx::new("read footer", path.display())))?;
        if &footer[0..8] != END_MAGIC {
            return Err(Error::Corruption(Corruption::one(
                &p,
                "bad_footer",
                len,
                CorruptRange {
                    start: footer_start,
                    end: footer_start + 8,
                    what: "footer magic".into(),
                    detail: "segment likely truncated during write (missing/ripped footer)"
                        .into(),
                },
            )));
        }
        let declared_len = u64::from_le_bytes(footer[48..56].try_into().unwrap());
        if declared_len != len {
            return Err(Error::Corruption(Corruption::one(
                &p,
                "length_mismatch",
                len,
                CorruptRange {
                    start: footer_start + 48,
                    end: footer_start + 56,
                    what: "declared file length".into(),
                    detail: format!("footer says {declared_len} bytes but file is {len}"),
                },
            )));
        }

        // Block headers are interleaved with payloads. The footer's index
        // CRC protects the *concatenation* of header bytes in block order
        // (the exact byte sequence the writer fed to its CRC). Validate that
        // CRC first via a layout walk; this is also what lets us locate a
        // flipped header byte at its real file offset.
        let stored_index_crc = u32::from_le_bytes(footer[56..60].try_into().unwrap());
        let mut index_bytes: Vec<u8> = Vec::with_capacity(block_count as usize * BLOCK_HEADER_LEN);
        let mut raw_headers: Vec<[u8; BLOCK_HEADER_LEN]> =
            Vec::with_capacity(block_count as usize);
        let mut header_file_ranges: Vec<(u64, u64)> = Vec::with_capacity(block_count as usize);
        let mut cursor = HEADER_LEN as u64;
        for i in 0..block_count {
            let hstart = cursor;
            let mut hdr = [0u8; BLOCK_HEADER_LEN];
            file.seek(SeekFrom::Start(hstart))
                .map_err(|e| Error::io(e, IoCtx::new("seek block header", path.display())))?;
            if let Err(e) = file.read_exact(&mut hdr) {
                return Err(Error::Corruption(Corruption::one(
                    &p,
                    "truncated_block_header",
                    len,
                    CorruptRange {
                        start: hstart,
                        end: (hstart + BLOCK_HEADER_LEN as u64).min(len),
                        what: format!("block {i} header"),
                        detail: format!("{e}"),
                    },
                )));
            }
            let ts_len = u32::from_le_bytes(hdr[20..24].try_into().unwrap()) as u64;
            let val_len = u32::from_le_bytes(hdr[24..28].try_into().unwrap()) as u64;
            header_file_ranges.push((hstart, hstart + BLOCK_HEADER_LEN as u64));
            raw_headers.push(hdr);
            index_bytes.extend_from_slice(&hdr);
            cursor = hstart + BLOCK_HEADER_LEN as u64 + ts_len + val_len;
        }

        let mut ic = Crc32c::new();
        ic.update(&index_bytes);
        if ic.finalize() != stored_index_crc {
            let ranges = header_file_ranges
                .iter()
                .enumerate()
                .map(|(i, &(start, end))| CorruptRange {
                    start,
                    end,
                    what: format!("block {i} header"),
                    detail: "index CRC32C mismatch; a block header is corrupted".into(),
                })
                .collect();
            return Err(Error::Corruption(Corruption {
                path: p,
                kind: "index_checksum".into(),
                file_len: len,
                ranges,
            }));
        }

        // Index CRC is valid, so the length fields are trustworthy; verify
        // the structural invariants against the footer position and ranges.
        if cursor != footer_start {
            return Err(Error::Corruption(Corruption::one(
                &p,
                "structure_mismatch",
                len,
                CorruptRange {
                    start: HEADER_LEN as u64,
                    end: footer_start,
                    what: "block index/payload region".into(),
                    detail: format!(
                        "walking {block_count} blocks reaches byte {cursor}, footer starts at {footer_start}"
                    ),
                },
            )));
        }
        let mut blocks = Vec::with_capacity(block_count as usize);
        let mut payload_cursor = HEADER_LEN as u64;
        for (i, h) in raw_headers.iter().enumerate() {
            let hstart = payload_cursor;
            let pstart = hstart + BLOCK_HEADER_LEN as u64;
            let count = u32::from_le_bytes(h[0..4].try_into().unwrap());
            let t0 = i64::from_le_bytes(h[4..12].try_into().unwrap());
            let t_last = i64::from_le_bytes(h[12..20].try_into().unwrap());
            let ts_len = u32::from_le_bytes(h[20..24].try_into().unwrap());
            let val_len = u32::from_le_bytes(h[24..28].try_into().unwrap());
            let pend = pstart + ts_len as u64 + val_len as u64;
            let mut detail = None;
            if count == 0 || count as usize > BLOCK_CAPACITY {
                detail = Some(format!("count {count} out of range 1..={BLOCK_CAPACITY}"));
            }
            if ts_len == 0 {
                detail.get_or_insert_with(|| "timestamp payload length is 0".to_string());
            }
            if t_last < t0 {
                detail.get_or_insert_with(|| format!("t_last {t_last} < t0 {t0}"));
            }
            if pend > footer_start {
                detail.get_or_insert_with(|| {
                    format!("payload ends at {pend}, past footer at {footer_start}")
                });
            }
            if let Some(detail) = detail {
                return Err(Error::Corruption(Corruption::one(
                    &p,
                    "bad_block_header",
                    len,
                    CorruptRange {
                        start: hstart,
                        end: hstart + BLOCK_HEADER_LEN as u64,
                        what: format!("block {i} header"),
                        detail,
                    },
                )));
            }
            blocks.push(BlockMeta::decode(h, pstart, pend));
            payload_cursor = pend;
        }

        let min_t = i64::from_le_bytes(footer[8..16].try_into().unwrap());
        let max_t = i64::from_le_bytes(footer[16..24].try_into().unwrap());
        let total = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        let payload_bytes = u64::from_le_bytes(footer[32..40].try_into().unwrap());
        let sum_count: u64 = blocks.iter().map(|b| b.count as u64).sum();
        let real_min = blocks.iter().map(|b| b.t0).min().unwrap_or(0);
        let real_max = blocks.iter().map(|b| b.t_last).max().unwrap_or(0);
        if sum_count != total || real_min != min_t || real_max != max_t {
            return Err(Error::Corruption(Corruption::one(
                &p,
                "footer_mismatch",
                len,
                CorruptRange {
                    start: footer_start + 8,
                    end: footer_start + 40,
                    what: "footer summary".into(),
                    detail: format!(
                        "footer says total={total} min={min_t} max={max_t}; index implies total={sum_count} min={real_min} max={real_max}"
                    ),
                },
            )));
        }

        Ok(Self {
            file,
            path: path.to_path_buf(),
            series_id,
            policy,
            blocks,
            min_t,
            max_t,
            total,
            payload_bytes,
            file_len: len,
            footer_start,
        })
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Decode one block after verifying its CRC32C. Only blocks the query
    /// touches are ever read/checksummed/decoded.
    fn read_block(&mut self, index: usize) -> Result<Vec<Sample>> {
        let m = self.blocks[index].clone();
        let pstart = m.payload_start;
        let ts_len = m.ts_len as usize;
        let val_len = m.val_len as usize;
        let mut payload = vec![0u8; ts_len + val_len];
        self.file
            .seek(SeekFrom::Start(pstart))
            .map_err(|e| Error::io(e, IoCtx::new("seek block payload", self.path.display())))?;
        self.file
            .read_exact(&mut payload)
            .map_err(|e| Error::io(e, IoCtx::new("read block payload", self.path.display())))?;
        let mut crc = Crc32c::new();
        crc.update(&payload);
        let computed_crc = crc.finalize();
        if computed_crc != m.payload_crc {
            return Err(Error::Corruption(Corruption::one(
                &self.path.display().to_string(),
                "block_checksum",
                self.file_len,
                CorruptRange {
                    start: pstart,
                    end: pstart + payload.len() as u64,
                    what: format!("block {index} payload"),
                    detail: format!(
                        "CRC32C mismatch: stored {:#010x}, computed {:#010x}",
                        m.payload_crc, computed_crc
                    ),
                },
            )));
        }
        let (ts_buf, val_buf) = payload.split_at(ts_len);
        let timestamps = decode_timestamps(ts_buf, m.count as usize).map_err(|e| {
            Error::Corruption(Corruption::one(
                &self.path.display().to_string(),
                "block_decode",
                self.file_len,
                CorruptRange {
                    start: pstart,
                    end: pstart + ts_len as u64,
                    what: format!("block {index} timestamps"),
                    detail: e,
                },
            ))
        })?;
        let mut vd = ValueDecoder::new(val_buf);
        let mut samples = Vec::with_capacity(m.count as usize);
        for (i, &t) in timestamps.iter().enumerate() {
            let bits = vd.read_next().map_err(|e| {
                Error::Corruption(Corruption::one(
                    &self.path.display().to_string(),
                    "block_decode",
                    self.file_len,
                    CorruptRange {
                        start: pstart + ts_len as u64,
                        end: pstart + payload.len() as u64,
                        what: format!("block {index} value stream"),
                        detail: format!("value #{i}: {e}"),
                    },
                ))
            })?;
            samples.push(Sample { t, bits });
        }
        if samples.last().map(|s| s.t) != Some(m.t_last) {
            return Err(Error::Corruption(Corruption::one(
                &self.path.display().to_string(),
                "block_decode",
                self.file_len,
                CorruptRange {
                    start: pstart,
                    end: pstart + payload.len() as u64,
                    what: format!("block {index}"),
                    detail: "decoded last timestamp disagrees with block header".into(),
                },
            )));
        }
        Ok(samples)
    }

    /// Read samples with `from <= t <= to`, using the block index to choose
    /// blocks. Paging is applied within the resulting non-decreasing
    /// sequence. Returns `(page, matched_total)` where `matched_total` is the
    /// exact number of in-range samples in this segment (before offset/limit),
    /// so callers can compute `has_more` without a second read. All selected
    /// blocks are decoded; at most ~512 samples each, and only blocks the
    /// index says overlap the window.
    pub fn query(
        &mut self,
        from: Option<i64>,
        to: Option<i64>,
        limit: usize,
        offset: u64,
    ) -> Result<(Vec<Sample>, u64)> {
        let from = from.unwrap_or(i64::MIN);
        let to = to.unwrap_or(i64::MAX);
        if self.blocks.is_empty() || to < self.min_t || from > self.max_t {
            return Ok((Vec::new(), 0));
        }
        let starts: Vec<i64> = self.blocks.iter().map(|b| b.t0).collect();
        let ends: Vec<i64> = self.blocks.iter().map(|b| b.t_last).collect();
        let first = ends.partition_point(|&e| e < from);
        let last_excl = starts.partition_point(|&s| s <= to);
        // Note on boundary duplicates: with keep_all a timestamp equal to a
        // block-cut time is adjacent to its duplicate, hence both copies fall
        // in the same block. Strict `e < from` start makes that block included.
        let mut out = Vec::new();
        let mut matched = 0u64;
        for bi in first..last_excl {
            let block = self.read_block(bi)?;
            for s in block {
                if s.t < from || s.t > to {
                    continue;
                }
                matched += 1;
                if matched <= offset {
                    continue;
                }
                if out.len() < limit {
                    out.push(s);
                }
            }
        }
        Ok((out, matched))
    }

    /// Verify every block payload CRC and round-trip-decode it. Used by the
    /// explicit verify endpoint; normal queries stay lazy.
    pub fn deep_verify(&mut self) -> Result<Vec<u64>> {
        let mut counts = Vec::with_capacity(self.blocks.len());
        for i in 0..self.blocks.len() {
            counts.push(self.read_block(i)?.len() as u64);
        }
        Ok(counts)
    }

    pub fn footer_start(&self) -> u64 {
        self.footer_start
    }
}

/// Reap stale temp segments on startup. A `.tmp` file by definition was never
/// renamed and therefore has no catalog row and is not queryable.
pub fn reap_temp_segments(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut removed = Vec::new();
    if !dir.exists() {
        return Ok(removed);
    }
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let p = e.path();
        if p.extension().and_then(|x| x.to_str()) == Some("tmp") {
            std::fs::remove_file(&p)?;
            removed.push(p);
        }
    }
    Ok(removed)
}

// ---------------------------------------------------------------------------
// CRC32C (Castagnoli), software table, no external C dependency.
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct Crc32c {
    state: u32,
}

const CRC32C_TABLE: [u32; 256] = build_crc32c_table();

const fn build_crc32c_table() -> [u32; 256] {
    // CRC32C normal poly is 0x1EDC6F41; the bytewise LSB-first ("reflected")
    // table needs its bit reversal, which is 0x82F63B78. NOTE: the commonly
    // quoted 0x82F63B79 is off by one and produces wrong checksums — verified
    // against the CRC32C check value 0xE3069283 for "123456789".
    const REFLECTED_POLY: u32 = reverse_bits_32(0x1edc_6f41);
    let mut table = [0u32; 256];
    let mut i = 0u32;
    while i < 256 {
        let mut crc = i;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ REFLECTED_POLY;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
}

const fn reverse_bits_32(mut x: u32) -> u32 {
    let mut r = 0u32;
    let mut n = 0;
    while n < 32 {
        r = (r << 1) | (x & 1);
        x >>= 1;
        n += 1;
    }
    r
}

impl Crc32c {
    pub fn new() -> Self {
        Self { state: !0u32 }
    }

    pub fn update(&mut self, buf: &[u8]) {
        let mut c = self.state;
        for &b in buf {
            c = CRC32C_TABLE[((c ^ b as u32) & 0xff) as usize] ^ (c >> 8);
        }
        self.state = c;
    }

    pub fn finalize(self) -> u32 {
        self.state ^ !0u32
    }

    pub fn checksum(buf: &[u8]) -> u32 {
        let mut c = Self::new();
        c.update(buf);
        c.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples(ts: &[i64], f: impl Fn(usize) -> f64) -> Vec<Sample> {
        ts.iter()
            .enumerate()
            .map(|(i, &t)| Sample::new(t, f(i)))
            .collect()
    }

    #[test]
    fn crc32c_known_vector() {
        // RFC 3720 / standard check value for "123456789".
        assert_eq!(Crc32c::checksum(b"123456789"), 0xe306_9283);
    }

    fn write_and_open(
        tmp: &Path,
        policy: DuplicatePolicy,
        batches: &[&[Sample]],
    ) -> Result<SealedSegment> {
        let mut w = SegmentWriter::create(
            tmp,
            "0123456789abcdef0123456789abcdef",
            "0123456789abcdef0123456789abcdef",
            policy,
        )?;
        for b in batches {
            w.append(b)?;
        }
        w.seal()
    }

    #[test]
    fn roundtrip_multi_block_and_range() {
        let dir = tempdir();
        let n = BLOCK_CAPACITY * 3 + 7;
        let data: Vec<Sample> = (0..n)
            .map(|i| Sample::new(1_000 + i as i64 * 100, (i as f64) * 0.5))
            .collect();
        let sealed = write_and_open(&dir, DuplicatePolicy::KeepAll, &[data.as_slice()]).unwrap();
        let mut r = SegmentReader::open(&sealed.path).unwrap();
        assert_eq!(r.block_count(), 4);
        assert_eq!(r.total, n as u64);
        assert_eq!(r.min_t, 1000);
        assert_eq!(r.max_t, 1000 + (n as i64 - 1) * 100);
        let (got, matched) = r.query(None, None, usize::MAX, 0).unwrap();
        assert_eq!(matched as usize, n);
        assert_eq!(got.len(), n);
        for (a, b) in got.iter().zip(data.iter()) {
            assert_eq!(a.t, b.t);
            assert_eq!(a.bits, b.bits);
        }
        // Narrow window spanning block boundary: 550..600 in sample indices.
        let (got, matched) = r
            .query(Some(1000 + 550 * 100), Some(1000 + 600 * 100), usize::MAX, 0)
            .unwrap();
        assert_eq!(matched as usize, 51);
        assert_eq!(got.len(), 51);
        assert_eq!(got.first().unwrap().t, 1000 + 550 * 100);
        // Paging within the same window.
        let (page1, m2) = r
            .query(Some(1000 + 550 * 100), Some(1000 + 600 * 100), 10, 0)
            .unwrap();
        assert_eq!(page1.len(), 10);
        assert_eq!(m2, 51);
        let (page2, _) = r
            .query(Some(1000 + 550 * 100), Some(1000 + 600 * 100), 10, 10)
            .unwrap();
        assert_eq!(page2.first().unwrap().t, 1000 + 560 * 100);
    }

    #[test]
    fn special_float_patterns_survive_segment() {
        let dir = tempdir();
        let bits = [
            0x0000_0000_0000_0000,
            0x8000_0000_0000_0000,
            0x7ff0_0000_0000_0000,
            0xfff0_0000_0000_0000,
            0x7ff8_0000_0000_0000,
            0xffff_ffff_ffff_ffff,
            0x4009_21fb_5444_2d18,
        ];
        let data: Vec<Sample> = bits.iter().enumerate().map(|(i, b)| Sample::from_bits(i as i64, *b)).collect();
        let sealed = write_and_open(&dir, DuplicatePolicy::KeepAll, &[data.as_slice()]).unwrap();
        let mut r = SegmentReader::open(&sealed.path).unwrap();
        let got = r.query(None, None, 100, 0).unwrap().0;
        assert_eq!(got.len(), bits.len());
        for (g, b) in got.iter().zip(bits) {
            assert_eq!(g.bits, b);
        }
    }

    #[test]
    fn duplicate_policies() {
        let dir = tempdir();
        let mk = |(t, b): (i64, f64)| Sample::new(t, b);
        let seq: Vec<Sample> = [(1, 1.0), (2, 2.0), (2, 20.0), (3, 3.0)]
            .into_iter()
            .map(mk)
            .collect();

        for (policy, expect_bits) in [
            (DuplicatePolicy::KeepAll, vec![1.0f64.to_bits(), 2.0f64.to_bits(), 20.0f64.to_bits(), 3.0f64.to_bits()]),
            (DuplicatePolicy::KeepFirst, vec![1.0f64.to_bits(), 2.0f64.to_bits(), 3.0f64.to_bits()]),
            (DuplicatePolicy::KeepLast, vec![1.0f64.to_bits(), 20.0f64.to_bits(), 3.0f64.to_bits()]),
        ] {
            let d = tempdir();
            let sealed = write_and_open(&d, policy, &[seq.as_slice()]).unwrap();
            let mut r = SegmentReader::open(&sealed.path).unwrap();
            let got = r.query(None, None, 100, 0).unwrap().0;
            assert_eq!(got.iter().map(|s| s.bits).collect::<Vec<_>>(), expect_bits, "{policy:?}");
            let _ = &dir;
        }
        // Reject: error, and no segment file remains (temp reaped).
        let d = tempdir();
        let w = write_and_open(&d, DuplicatePolicy::Reject, &[seq.as_slice()]);
        assert!(matches!(w, Err(Error::DuplicateTimestamp { .. })));
        assert!(std::fs::read_dir(&d).unwrap().next().is_none());
    }

    #[test]
    fn out_of_order_rejected() {
        let dir = tempdir();
        let err = write_and_open(
            &dir,
            DuplicatePolicy::KeepAll,
            &[samples(&[1, 2, 0], |i| i as f64).as_slice()],
        );
        assert!(matches!(err, Err(Error::OutOfOrder { t: 0, previous: 2 })));
    }

    #[test]
    fn torn_footer_is_not_openable_and_temp_is_not_final() {
        let dir = tempdir();
        let data: Vec<Sample> = (0..1000).map(|i| Sample::new(i, i as f64)).collect();
        // Simulate a crash mid-seal by writing a file then truncating it and
        // leaving a .tmp name: it must not look like a sealed segment.
        let sealed = write_and_open(&dir, DuplicatePolicy::KeepAll, &[data.as_slice()]).unwrap();
        let torn = dir.join("deadbeef.seg.tmp");
        std::fs::copy(&sealed.path, &torn).unwrap();
        let f = OpenOptions::new().write(true).open(&torn).unwrap();
        f.set_len(70).unwrap(); // keep header-ish prefix, rip footer
        drop(f);
        let removed = reap_temp_segments(&dir).unwrap();
        assert!(removed.iter().any(|p| p == &torn));
        assert!(!torn.exists());
    }

    #[test]
    fn corruption_localized_to_payload_range() {
        let dir = tempdir();
        let data: Vec<Sample> = (0..(BLOCK_CAPACITY * 2 + 10) as i64)
            .map(|i| Sample::new(i, i as f64))
            .collect();
        let sealed = write_and_open(&dir, DuplicatePolicy::KeepAll, &[data.as_slice()]).unwrap();
        // Flip one byte inside block 1's payload while keeping header intact.
        let r = SegmentReader::open(&sealed.path).unwrap();
        let target = r.blocks[1].payload_start + 3;
        drop(r);
        let mut bytes = std::fs::read(&sealed.path).unwrap();
        bytes[target as usize] ^= 0xff;
        std::fs::write(&sealed.path, &bytes).unwrap();

        let mut r = SegmentReader::open(&sealed.path).unwrap(); // index still fine
        let err = r.query(Some(BLOCK_CAPACITY as i64), None, 10, 0).unwrap_err();
        match err {
            Error::Corruption(c) => {
                assert_eq!(c.ranges.len(), 1);
                let rg = &c.ranges[0];
                assert!(rg.start <= target && target < rg.end);
                assert!(rg.what.contains("block 1"));
            }
            other => panic!("expected corruption, got {other:?}"),
        }
        // A query touching only block 0 still succeeds: lazy verification.
        let mut r = SegmentReader::open(&sealed.path).unwrap();
        let ok = r.query(Some(0), Some(10), 100, 0).unwrap();
        assert!(!ok.0.is_empty());
    }

    #[test]
    fn corrupted_index_detected_at_open_with_range() {
        let dir = tempdir();
        let data: Vec<Sample> = (0..600i64).map(|i| Sample::new(i, i as f64)).collect();
        let sealed = write_and_open(&dir, DuplicatePolicy::KeepAll, &[data.as_slice()]).unwrap();
        let mut bytes = std::fs::read(&sealed.path).unwrap();
        bytes[HEADER_LEN + 5] ^= 0x01;
        std::fs::write(&sealed.path, &bytes).unwrap();
        let err = match SegmentReader::open(&sealed.path) {
            Err(e) => e,
            Ok(_) => panic!("expected open to fail"),
        };
        match err {
            Error::Corruption(c) => {
                assert_eq!(c.kind, "index_checksum");
                assert!(c.ranges[0].start == HEADER_LEN as u64);
            }
            other => panic!("expected corruption, got {other:?}"),
        }
    }

    fn tempdir() -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "sseg-test-{}-{}",
            std::process::id(),
            use_count()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }
    fn use_count() -> usize {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static N: AtomicUsize = AtomicUsize::new(0);
        N.fetch_add(1, Ordering::Relaxed)
    }
}
