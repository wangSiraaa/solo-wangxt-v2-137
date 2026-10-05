//! 不可变段文件格式（全部小端）。
//!
//! 布局：
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │ Header (64B)                                                 │
//! │   0..4   magic "SGT1"                                        │
//! │   4      version = 1                                         │
//! │   5      value codec: 0 = raw, 1 = xor                       │
//! │   6..8   reserved (0)                                        │
//! │   8..16  series_id  u64                                      │
//! │   16..24 segment_id u64                                      │
//! │   24..32 start_ts   i64                                      │
//! │   32..40 end_ts     i64                                      │
//! │   40..44 point count u32                                     │
//! │   44..48 block count u32                                     │
//! │   48..56 index_offset u64                                    │
//! │   56..60 header crc32 (bytes 0..56)                          │
//! │   60..64 reserved (0)                                        │
//! ├──────────────────────────────────────────────────────────────┤
//! │ Data blocks × N（每块 ≤ BLOCK_POINTS 个点）                  │
//! │   0..8   block_first_ts i64                                  │
//! │   8..10  count u16                                           │
//! │   10..14 payload_len u32                                     │
//! │   14..18 block crc32 (header 0..14 + payload)                │
//! │   18..   payload: ts_len u32 | ts bytes | value bytes        │
//! ├──────────────────────────────────────────────────────────────┤
//! │ Index（位于 index_offset，每 24B 一条）                      │
//! │   first_ts i64 | file_offset u64 | block_len u32 | crc u32   │
//! ├──────────────────────────────────────────────────────────────┤
//! │ Footer (16B)                                                 │
//! │   0..8   magic "SGT1END!"                                    │
//! │   8..12  index crc32                                         │
//! │   12..16 file crc32（除本字段外的全部字节）                  │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! 区间查询只读 header + footer + index，再用索引二分定位到重叠的块，
//! 只读取并解压这些块 —— 不会为了读一分钟数据而解压整段。

use std::fmt;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::codec::{self, CodecError};

pub const MAGIC: &[u8; 4] = b"SGT1";
pub const FOOTER_MAGIC: &[u8; 8] = b"SGT1END!";
pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 64;
pub const BLOCK_HEADER_LEN: usize = 18;
pub const INDEX_ENTRY_LEN: usize = 24;
pub const FOOTER_LEN: usize = 16;
/// 每个数据块的最大点数。区间查询最多多解压 2×BLOCK_POINTS 个点。
pub const BLOCK_POINTS: usize = 256;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ValueCodec {
    /// 按位保存：每个值 8 字节原样存放
    Raw = 0,
    /// Gorilla 风格 XOR，逐位无损、可验证
    Xor = 1,
}

impl ValueCodec {
    pub fn from_u8(b: u8) -> Result<Self, SegmentError> {
        match b {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Xor),
            other => Err(SegmentError::BadCodec(other)),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Xor => "xor",
        }
    }

    pub fn from_name(s: &str) -> Option<Self> {
        match s {
            "raw" => Some(Self::Raw),
            "xor" => Some(Self::Xor),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum SegmentError {
    TooSmall,
    BadMagic,
    UnsupportedVersion(u8),
    BadCodec(u8),
    HeaderCrc,
    FooterMagic,
    IndexCrc,
    IndexOutOfBounds,
    BlockOutOfBounds { index: usize },
    BlockCrc { index: usize, first_ts: i64 },
    BlockLength { index: usize },
    Codec(CodecError),
    Io(std::io::Error),
}

impl fmt::Display for SegmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SegmentError::TooSmall => write!(f, "file smaller than header"),
            SegmentError::BadMagic => write!(f, "bad magic"),
            SegmentError::UnsupportedVersion(v) => write!(f, "unsupported version {v}"),
            SegmentError::BadCodec(c) => write!(f, "unknown codec id {c}"),
            SegmentError::HeaderCrc => write!(f, "header crc mismatch"),
            SegmentError::FooterMagic => write!(f, "bad footer magic"),
            SegmentError::IndexCrc => write!(f, "index crc mismatch"),
            SegmentError::IndexOutOfBounds => write!(f, "index section out of bounds"),
            SegmentError::BlockOutOfBounds { index } => write!(f, "block {index} out of bounds"),
            SegmentError::BlockCrc { index, first_ts } => {
                write!(f, "block {index} (first_ts={first_ts}) crc mismatch")
            }
            SegmentError::BlockLength { index } => write!(f, "block {index} length mismatch"),
            SegmentError::Codec(e) => write!(f, "codec: {e}"),
            SegmentError::Io(e) => write!(f, "io: {e}"),
        }
    }
}

impl From<CodecError> for SegmentError {
    fn from(e: CodecError) -> Self {
        SegmentError::Codec(e)
    }
}

impl From<std::io::Error> for SegmentError {
    fn from(e: std::io::Error) -> Self {
        SegmentError::Io(e)
    }
}

/// 段头解析结果。series_id / segment_id / start_ts / count 等字段
/// 主要供校验与未来工具使用，当前查询路径不读取全部字段。
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct SegmentMeta {
    pub series_id: u64,
    pub segment_id: u64,
    pub codec: ValueCodec,
    pub start_ts: i64,
    pub end_ts: i64,
    pub count: u32,
    pub block_count: u32,
    pub index_offset: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct IndexEntry {
    pub first_ts: i64,
    pub offset: u64,
    pub len: u32,
    pub crc: u32,
}

// ---------------------------------------------------------------------------
// 构建（写入侧，一次性完整构建后原子落盘）
// ---------------------------------------------------------------------------

/// 把一批点编码成完整段文件字节。points 必须按 ts 严格升序且已去重。
pub fn build_segment(series_id: u64, segment_id: u64, codec: ValueCodec, points: &[(i64, u64)]) -> Vec<u8> {
    assert!(!points.is_empty());
    debug_assert!(
        points.windows(2).all(|w| w[0].0 < w[1].0),
        "points must be sorted by ts and deduplicated"
    );

    let mut body = Vec::new();
    let mut entries: Vec<IndexEntry> = Vec::new();

    for chunk in points.chunks(BLOCK_POINTS) {
        let offset = (HEADER_LEN + body.len()) as u64;
        let first_ts = chunk[0].0;
        let count = chunk.len() as u16;

        let ts: Vec<i64> = chunk.iter().map(|p| p.0).collect();
        let vals: Vec<u64> = chunk.iter().map(|p| p.1).collect();
        let ts_enc = codec::encode_timestamps(&ts);
        let val_enc = match codec {
            ValueCodec::Raw => codec::encode_values_raw(&vals),
            ValueCodec::Xor => codec::encode_values_xor(&vals),
        };

        let mut payload = Vec::with_capacity(4 + ts_enc.len() + val_enc.len());
        payload.extend_from_slice(&(ts_enc.len() as u32).to_le_bytes());
        payload.extend_from_slice(&ts_enc);
        payload.extend_from_slice(&val_enc);

        let mut hdr = [0u8; BLOCK_HEADER_LEN];
        hdr[0..8].copy_from_slice(&first_ts.to_le_bytes());
        hdr[8..10].copy_from_slice(&count.to_le_bytes());
        hdr[10..14].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        let mut h = crc32fast::Hasher::new();
        h.update(&hdr[0..14]);
        h.update(&payload);
        let crc = h.finalize();
        hdr[14..18].copy_from_slice(&crc.to_le_bytes());

        body.extend_from_slice(&hdr);
        body.extend_from_slice(&payload);
        entries.push(IndexEntry {
            first_ts,
            offset,
            len: (BLOCK_HEADER_LEN + payload.len()) as u32,
            crc,
        });
    }

    let index_offset = (HEADER_LEN + body.len()) as u64;
    let mut index = Vec::with_capacity(entries.len() * INDEX_ENTRY_LEN);
    for e in &entries {
        index.extend_from_slice(&e.first_ts.to_le_bytes());
        index.extend_from_slice(&e.offset.to_le_bytes());
        index.extend_from_slice(&e.len.to_le_bytes());
        index.extend_from_slice(&e.crc.to_le_bytes());
    }
    let index_crc = crc32fast::hash(&index);

    let mut header = [0u8; HEADER_LEN];
    header[0..4].copy_from_slice(MAGIC);
    header[4] = VERSION;
    header[5] = codec as u8;
    header[8..16].copy_from_slice(&series_id.to_le_bytes());
    header[16..24].copy_from_slice(&segment_id.to_le_bytes());
    header[24..32].copy_from_slice(&points[0].0.to_le_bytes());
    header[32..40].copy_from_slice(&points[points.len() - 1].0.to_le_bytes());
    header[40..44].copy_from_slice(&(points.len() as u32).to_le_bytes());
    header[44..48].copy_from_slice(&(entries.len() as u32).to_le_bytes());
    header[48..56].copy_from_slice(&index_offset.to_le_bytes());
    let hcrc = crc32fast::hash(&header[0..56]);
    header[56..60].copy_from_slice(&hcrc.to_le_bytes());

    let mut out = Vec::with_capacity(HEADER_LEN + body.len() + index.len() + FOOTER_LEN);
    out.extend_from_slice(&header);
    out.extend_from_slice(&body);
    out.extend_from_slice(&index);
    out.extend_from_slice(FOOTER_MAGIC);
    out.extend_from_slice(&index_crc.to_le_bytes());
    let file_crc = crc32fast::hash(&out);
    out.extend_from_slice(&file_crc.to_le_bytes());
    out
}

// ---------------------------------------------------------------------------
// 解析（读取侧）
// ---------------------------------------------------------------------------

pub fn parse_header(data: &[u8]) -> Result<SegmentMeta, SegmentError> {
    if data.len() < HEADER_LEN {
        return Err(SegmentError::TooSmall);
    }
    if &data[0..4] != MAGIC {
        return Err(SegmentError::BadMagic);
    }
    if data[4] != VERSION {
        return Err(SegmentError::UnsupportedVersion(data[4]));
    }
    let codec = ValueCodec::from_u8(data[5])?;
    let stored = u32::from_le_bytes(data[56..60].try_into().unwrap());
    if crc32fast::hash(&data[0..56]) != stored {
        return Err(SegmentError::HeaderCrc);
    }
    Ok(SegmentMeta {
        series_id: u64::from_le_bytes(data[8..16].try_into().unwrap()),
        segment_id: u64::from_le_bytes(data[16..24].try_into().unwrap()),
        codec,
        start_ts: i64::from_le_bytes(data[24..32].try_into().unwrap()),
        end_ts: i64::from_le_bytes(data[32..40].try_into().unwrap()),
        count: u32::from_le_bytes(data[40..44].try_into().unwrap()),
        block_count: u32::from_le_bytes(data[44..48].try_into().unwrap()),
        index_offset: u64::from_le_bytes(data[48..56].try_into().unwrap()),
    })
}

fn parse_index_section(section: &[u8], block_count: usize) -> Vec<IndexEntry> {
    let mut out = Vec::with_capacity(block_count);
    for i in 0..block_count {
        let b = &section[i * INDEX_ENTRY_LEN..(i + 1) * INDEX_ENTRY_LEN];
        out.push(IndexEntry {
            first_ts: i64::from_le_bytes(b[0..8].try_into().unwrap()),
            offset: u64::from_le_bytes(b[8..16].try_into().unwrap()),
            len: u32::from_le_bytes(b[16..20].try_into().unwrap()),
            crc: u32::from_le_bytes(b[20..24].try_into().unwrap()),
        });
    }
    out
}

/// 从完整文件字节解析索引（校验 footer magic 与 index crc）。
pub fn parse_index(data: &[u8], meta: &SegmentMeta) -> Result<Vec<IndexEntry>, SegmentError> {
    let start = meta.index_offset as usize;
    let len = meta.block_count as usize * INDEX_ENTRY_LEN;
    let beyond = match start.checked_add(len) {
        Some(end) => end + FOOTER_LEN > data.len(),
        None => true,
    };
    if start < HEADER_LEN || beyond {
        return Err(SegmentError::IndexOutOfBounds);
    }
    let footer = &data[data.len() - FOOTER_LEN..];
    if &footer[0..8] != FOOTER_MAGIC {
        return Err(SegmentError::FooterMagic);
    }
    let index_crc = u32::from_le_bytes(footer[8..12].try_into().unwrap());
    let section = &data[start..start + len];
    if crc32fast::hash(section) != index_crc {
        return Err(SegmentError::IndexCrc);
    }
    Ok(parse_index_section(section, meta.block_count as usize))
}

/// 解码一个块的原始字节（含块头）。调用方负责切片。
pub fn decode_block_bytes(
    blk: &[u8],
    codec: ValueCodec,
    block_index: usize,
) -> Result<Vec<(i64, u64)>, SegmentError> {
    if blk.len() < BLOCK_HEADER_LEN {
        return Err(SegmentError::BlockOutOfBounds { index: block_index });
    }
    let first_ts = i64::from_le_bytes(blk[0..8].try_into().unwrap());
    let count = u16::from_le_bytes(blk[8..10].try_into().unwrap()) as usize;
    let payload_len = u32::from_le_bytes(blk[10..14].try_into().unwrap()) as usize;
    let stored_crc = u32::from_le_bytes(blk[14..18].try_into().unwrap());
    if blk.len() != BLOCK_HEADER_LEN + payload_len {
        return Err(SegmentError::BlockLength { index: block_index });
    }
    let mut h = crc32fast::Hasher::new();
    h.update(&blk[0..14]);
    h.update(&blk[BLOCK_HEADER_LEN..]);
    if h.finalize() != stored_crc {
        return Err(SegmentError::BlockCrc { index: block_index, first_ts });
    }
    let payload = &blk[BLOCK_HEADER_LEN..];
    if payload.len() < 4 {
        return Err(SegmentError::BlockLength { index: block_index });
    }
    let ts_len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    if ts_len > payload.len() - 4 {
        return Err(SegmentError::BlockLength { index: block_index });
    }
    let ts = codec::decode_timestamps(first_ts, count, &payload[4..4 + ts_len])?;
    let vals = match codec {
        ValueCodec::Raw => codec::decode_values_raw(count, &payload[4 + ts_len..])?,
        ValueCodec::Xor => codec::decode_values_xor(count, &payload[4 + ts_len..])?,
    };
    Ok(ts.into_iter().zip(vals).collect())
}

/// 从完整文件字节解码一个块（按索引条目切片，并交叉核对索引中的 CRC）。
pub fn decode_block(
    data: &[u8],
    entry: &IndexEntry,
    codec: ValueCodec,
    block_index: usize,
) -> Result<Vec<(i64, u64)>, SegmentError> {
    let start = entry.offset as usize;
    let end = start
        .checked_add(entry.len as usize)
        .ok_or(SegmentError::BlockOutOfBounds { index: block_index })?;
    if end > data.len() {
        return Err(SegmentError::BlockOutOfBounds { index: block_index });
    }
    let blk = &data[start..end];
    if blk.len() >= BLOCK_HEADER_LEN {
        let stored = u32::from_le_bytes(blk[14..18].try_into().unwrap());
        if stored != entry.crc {
            return Err(SegmentError::BlockCrc { index: block_index, first_ts: entry.first_ts });
        }
    }
    decode_block_bytes(blk, codec, block_index)
}

// ---------------------------------------------------------------------------
// 随机访问读取：只读 header/footer/index 和需要的块
// ---------------------------------------------------------------------------

pub struct SegmentFile {
    file: File,
    pub meta: SegmentMeta,
}

impl SegmentFile {
    /// 只读取并校验 64 字节头。
    pub fn open(path: &Path) -> Result<Self, SegmentError> {
        let mut file = File::open(path)?;
        let mut hdr = [0u8; HEADER_LEN];
        file.read_exact(&mut hdr)?;
        let meta = parse_header(&hdr)?;
        Ok(Self { file, meta })
    }

    /// 读取索引区（另读 16 字节 footer 校验 index crc）。
    pub fn read_index(&mut self) -> Result<Vec<IndexEntry>, SegmentError> {
        let len = self.meta.block_count as usize * INDEX_ENTRY_LEN;
        let mut buf = vec![0u8; len];
        self.file.seek(SeekFrom::Start(self.meta.index_offset))?;
        self.file.read_exact(&mut buf)?;

        let mut footer = [0u8; FOOTER_LEN];
        self.file.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
        self.file.read_exact(&mut footer)?;
        if &footer[0..8] != FOOTER_MAGIC {
            return Err(SegmentError::FooterMagic);
        }
        let index_crc = u32::from_le_bytes(footer[8..12].try_into().unwrap());
        if crc32fast::hash(&buf) != index_crc {
            return Err(SegmentError::IndexCrc);
        }
        Ok(parse_index_section(&buf, self.meta.block_count as usize))
    }

    /// 只读取并解码指定块。
    pub fn read_block(
        &mut self,
        entry: &IndexEntry,
        block_index: usize,
    ) -> Result<Vec<(i64, u64)>, SegmentError> {
        let mut buf = vec![0u8; entry.len as usize];
        self.file.seek(SeekFrom::Start(entry.offset))?;
        self.file.read_exact(&mut buf)?;
        if buf.len() >= BLOCK_HEADER_LEN {
            let stored = u32::from_le_bytes(buf[14..18].try_into().unwrap());
            if stored != entry.crc {
                return Err(SegmentError::BlockCrc { index: block_index, first_ts: entry.first_ts });
            }
        }
        decode_block_bytes(&buf, self.meta.codec, block_index)
    }
}

// ---------------------------------------------------------------------------
// 全量校验：逐块 CRC + 解码 + 单调性检查，输出可定位的损坏范围
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct VerifyReport {
    pub ok: bool,
    pub header_ok: bool,
    pub footer_ok: bool,
    pub index_ok: bool,
    pub file_crc_ok: bool,
    pub blocks: Vec<BlockStatus>,
    /// 损坏块对应的时间范围 [from_ts, to_ts]，可直接用于定位与重采
    pub corrupt_ranges: Vec<(i64, i64)>,
    pub errors: Vec<String>,
}

#[derive(Debug)]
pub struct BlockStatus {
    pub index: usize,
    pub first_ts: i64,
    pub last_ts: i64,
    pub points: usize,
    pub ok: bool,
    pub error: Option<String>,
}

pub fn verify_segment(data: &[u8]) -> VerifyReport {
    let mut rep = VerifyReport::default();
    if data.len() < HEADER_LEN + FOOTER_LEN {
        rep.errors.push(format!("file too small: {} bytes", data.len()));
        return rep;
    }

    let meta = match parse_header(data) {
        Ok(m) => {
            rep.header_ok = true;
            m
        }
        Err(e) => {
            rep.errors.push(format!("header: {e}"));
            return rep;
        }
    };

    let footer = &data[data.len() - FOOTER_LEN..];
    rep.footer_ok = &footer[0..8] == FOOTER_MAGIC;
    if rep.footer_ok {
        let stored_file_crc = u32::from_le_bytes(footer[12..16].try_into().unwrap());
        rep.file_crc_ok = crc32fast::hash(&data[..data.len() - 4]) == stored_file_crc;
        if !rep.file_crc_ok {
            rep.errors.push("file crc mismatch".to_string());
        }
    } else {
        rep.errors.push("bad footer magic".to_string());
    }

    let entries = match parse_index(data, &meta) {
        Ok(e) => {
            rep.index_ok = true;
            e
        }
        Err(e) => {
            rep.errors.push(format!("index: {e}"));
            return rep;
        }
    };

    let mut all_ok = true;
    for (i, e) in entries.iter().enumerate() {
        // 损坏块的时间范围：从本块第一个时间戳到下一块第一个时间戳
        //（最后一块到段 end_ts），用于精确定位损坏区间。
        let range_end = entries.get(i + 1).map(|n| n.first_ts).unwrap_or(meta.end_ts);
        match decode_block(data, e, meta.codec, i) {
            Ok(points) => {
                let monotonic = points.windows(2).all(|w| w[0].0 < w[1].0);
                let last = points.last().map(|p| p.0).unwrap_or(e.first_ts);
                if monotonic {
                    rep.blocks.push(BlockStatus {
                        index: i,
                        first_ts: e.first_ts,
                        last_ts: last,
                        points: points.len(),
                        ok: true,
                        error: None,
                    });
                } else {
                    all_ok = false;
                    rep.blocks.push(BlockStatus {
                        index: i,
                        first_ts: e.first_ts,
                        last_ts: last,
                        points: points.len(),
                        ok: false,
                        error: Some("timestamps not strictly increasing".into()),
                    });
                    rep.corrupt_ranges.push((e.first_ts, range_end.max(e.first_ts)));
                }
            }
            Err(err) => {
                all_ok = false;
                rep.blocks.push(BlockStatus {
                    index: i,
                    first_ts: e.first_ts,
                    last_ts: range_end,
                    points: 0,
                    ok: false,
                    error: Some(err.to_string()),
                });
                rep.corrupt_ranges.push((e.first_ts, range_end.max(e.first_ts)));
            }
        }
    }
    rep.ok = rep.header_ok && rep.footer_ok && rep.index_ok && rep.file_crc_ok && all_ok;
    rep
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_points(n: usize) -> Vec<(i64, u64)> {
        (0..n)
            .map(|i| (1_000_000 + i as i64 * 100, (i as f64 * 0.5).to_bits()))
            .collect()
    }

    fn decode_all(seg: &[u8]) -> Vec<(i64, u64)> {
        let meta = parse_header(seg).unwrap();
        let entries = parse_index(seg, &meta).unwrap();
        let mut out = Vec::new();
        for (i, e) in entries.iter().enumerate() {
            out.extend(decode_block(seg, e, meta.codec, i).unwrap());
        }
        out
    }

    #[test]
    fn build_and_verify_roundtrip_bit_exact() {
        for codec in [ValueCodec::Xor, ValueCodec::Raw] {
            let pts = sample_points(1000); // 4 块：256*3 + 232
            let seg = build_segment(1, 7, codec, &pts);
            let rep = verify_segment(&seg);
            assert!(rep.ok, "codec {codec:?} verify failed: {:?}", rep.errors);
            assert_eq!(rep.blocks.len(), 4);
            assert_eq!(decode_all(&seg), pts, "codec {codec:?} must round-trip bit-exactly");
        }
    }

    #[test]
    fn specials_survive_full_segment_roundtrip() {
        let mut pts = sample_points(300);
        pts[5].1 = f64::NAN.to_bits();
        pts[6].1 = 0x7ff8_0000_0000_0001; // 带负载 NaN
        pts[7].1 = (-0.0f64).to_bits();
        pts[8].1 = f64::INFINITY.to_bits();
        pts[9].1 = f64::NEG_INFINITY.to_bits();
        let seg = build_segment(1, 8, ValueCodec::Xor, &pts);
        assert!(verify_segment(&seg).ok);
        assert_eq!(decode_all(&seg), pts);
    }

    #[test]
    fn corrupt_block_is_located() {
        let pts = sample_points(1000);
        let mut seg = build_segment(1, 7, ValueCodec::Xor, &pts);
        let meta = parse_header(&seg).unwrap();
        let entries = parse_index(&seg, &meta).unwrap();

        // 破坏第 2 个块（index 1）载荷中的一个字节
        let pos = (entries[1].offset + 30) as usize;
        seg[pos] ^= 0xff;

        let rep = verify_segment(&seg);
        assert!(!rep.ok);
        assert!(rep.blocks[0].ok);
        assert!(!rep.blocks[1].ok, "corrupted block must be flagged");
        assert!(rep.blocks[2].ok);
        assert!(rep.blocks[3].ok);
        // 损坏范围必须精确覆盖第 2 块的时间区间
        assert_eq!(rep.corrupt_ranges.len(), 1);
        let (lo, hi) = rep.corrupt_ranges[0];
        assert_eq!(lo, entries[1].first_ts);
        assert_eq!(hi, entries[2].first_ts);
    }

    #[test]
    fn corrupt_header_is_detected() {
        let pts = sample_points(10);
        let mut seg = build_segment(1, 7, ValueCodec::Raw, &pts);
        seg[30] ^= 0x01; // start_ts 字段
        let rep = verify_segment(&seg);
        assert!(!rep.ok);
        assert!(!rep.header_ok);
    }

    #[test]
    fn corrupt_index_is_detected() {
        let pts = sample_points(300);
        let mut seg = build_segment(1, 7, ValueCodec::Xor, &pts);
        let meta = parse_header(&seg).unwrap();
        let pos = meta.index_offset as usize + 5;
        seg[pos] ^= 0xff;
        let rep = verify_segment(&seg);
        assert!(!rep.ok);
        assert!(!rep.index_ok);
    }

    #[test]
    fn truncated_file_is_detected() {
        let pts = sample_points(300);
        let seg = build_segment(1, 7, ValueCodec::Xor, &pts);
        let truncated = &seg[..seg.len() - 100];
        let rep = verify_segment(truncated);
        assert!(!rep.ok);
    }
}
