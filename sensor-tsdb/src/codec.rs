//! 底层编码原语：zigzag varint、位流、时间戳 delta-of-delta、浮点值 XOR（Gorilla）。
//!
//! 关键约束：浮点值一律按 IEEE-754 位模式（u64）处理，编解码全程不做任何
//! 量化或舍入，因此 NaN（含负载位）、-0.0、±∞、次正规数都能逐位还原。

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    UnexpectedEof,
    VarintOverflow,
    InvalidStream(&'static str),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::UnexpectedEof => write!(f, "unexpected end of buffer"),
            CodecError::VarintOverflow => write!(f, "varint exceeds 64 bits"),
            CodecError::InvalidStream(msg) => write!(f, "invalid stream: {msg}"),
        }
    }
}

impl std::error::Error for CodecError {}

// ---------------------------------------------------------------------------
// zigzag
// ---------------------------------------------------------------------------

pub fn zigzag_encode(v: i64) -> u64 {
    // 用无符号移位实现，避免 i64::MIN << 1 在 debug 下溢出 panic
    ((v as u64) << 1) ^ ((v >> 63) as u64)
}

pub fn zigzag_decode(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

// ---------------------------------------------------------------------------
// varint (LEB128)
// ---------------------------------------------------------------------------

pub fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(b);
            return;
        }
        buf.push(b | 0x80);
    }
}

pub fn read_varint(buf: &[u8], pos: &mut usize) -> Result<u64, CodecError> {
    let mut v: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= buf.len() {
            return Err(CodecError::UnexpectedEof);
        }
        let b = buf[*pos];
        *pos += 1;
        if shift == 63 && b > 1 {
            return Err(CodecError::VarintOverflow);
        }
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(v);
        }
        shift += 7;
        if shift > 63 {
            return Err(CodecError::VarintOverflow);
        }
    }
}

// ---------------------------------------------------------------------------
// 位流（MSB 优先）
// ---------------------------------------------------------------------------

pub struct BitWriter {
    buf: Vec<u8>,
    cur: u8,
    nbits: u32,
}

impl BitWriter {
    pub fn new() -> Self {
        Self { buf: Vec::new(), cur: 0, nbits: 0 }
    }

    pub fn write_bit(&mut self, b: bool) {
        self.cur <<= 1;
        if b {
            self.cur |= 1;
        }
        self.nbits += 1;
        if self.nbits == 8 {
            self.buf.push(self.cur);
            self.cur = 0;
            self.nbits = 0;
        }
    }

    /// 写入 v 的低 n 位（MSB 优先），n 取值 0..=64。
    pub fn write_bits(&mut self, v: u64, n: u32) {
        debug_assert!(n <= 64);
        for i in (0..n).rev() {
            self.write_bit((v >> i) & 1 == 1);
        }
    }

    pub fn finish(mut self) -> Vec<u8> {
        if self.nbits > 0 {
            self.cur <<= 8 - self.nbits;
            self.buf.push(self.cur);
        }
        self.buf
    }
}

pub struct BitReader<'a> {
    buf: &'a [u8],
    byte: usize,
    bit: u32,
}

impl<'a> BitReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, byte: 0, bit: 0 }
    }

    pub fn read_bit(&mut self) -> Result<bool, CodecError> {
        if self.byte >= self.buf.len() {
            return Err(CodecError::UnexpectedEof);
        }
        let b = (self.buf[self.byte] >> (7 - self.bit)) & 1;
        self.bit += 1;
        if self.bit == 8 {
            self.bit = 0;
            self.byte += 1;
        }
        Ok(b == 1)
    }

    pub fn read_bits(&mut self, n: u32) -> Result<u64, CodecError> {
        debug_assert!(n <= 64);
        let mut v = 0u64;
        for _ in 0..n {
            v = (v << 1) | self.read_bit()? as u64;
        }
        Ok(v)
    }
}

// ---------------------------------------------------------------------------
// 时间戳：delta-of-delta + zigzag varint（显式增量编码）
//
// 块内第一个时间戳由块头原样保存（i64 LE），本函数从第二个点开始编码：
//   点 1：delta = ts[1] - ts[0]              -> zigzag varint
//   点 i：dod   = delta[i] - delta[i-1]      -> zigzag varint
// ---------------------------------------------------------------------------

pub fn encode_timestamps(ts: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    if ts.len() < 2 {
        return out;
    }
    let mut prev_delta = ts[1].wrapping_sub(ts[0]);
    write_varint(&mut out, zigzag_encode(prev_delta));
    for i in 2..ts.len() {
        let delta = ts[i].wrapping_sub(ts[i - 1]);
        let dod = delta.wrapping_sub(prev_delta);
        write_varint(&mut out, zigzag_encode(dod));
        prev_delta = delta;
    }
    out
}

pub fn decode_timestamps(first: i64, count: usize, buf: &[u8]) -> Result<Vec<i64>, CodecError> {
    let mut out = Vec::with_capacity(count);
    if count == 0 {
        return Ok(out);
    }
    out.push(first);
    if count == 1 {
        return Ok(out);
    }
    let mut pos = 0usize;
    let mut prev_delta = zigzag_decode(read_varint(buf, &mut pos)?);
    out.push(first.wrapping_add(prev_delta));
    for i in 2..count {
        let dod = zigzag_decode(read_varint(buf, &mut pos)?);
        let delta = prev_delta.wrapping_add(dod);
        let next = out[i - 1].wrapping_add(delta);
        out.push(next);
        prev_delta = delta;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// 浮点值：raw（按位保存）
// ---------------------------------------------------------------------------

pub fn encode_values_raw(bits: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bits.len() * 8);
    for b in bits {
        out.extend_from_slice(&b.to_le_bytes());
    }
    out
}

pub fn decode_values_raw(count: usize, buf: &[u8]) -> Result<Vec<u64>, CodecError> {
    if buf.len() < count * 8 {
        return Err(CodecError::UnexpectedEof);
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        out.push(u64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap()));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// 浮点值：XOR（Gorilla 风格，可验证、逐位无损）
//
// 位流格式：
//   第一个值：64 位原样
//   后续值：xor = v[i] ^ v[i-1]
//     xor == 0                -> '0'
//     复用上一窗口            -> '10' + 有效位（窗口 = 上次的前导零/尾零）
//     新窗口                  -> '11' + 5bit 前导零 + 6bit 有效长度 + 有效位
//   前导零按 Gorilla 论文截断到 31（5 bit 能表示的最大值）；
//   有效长度 64 编码为 0（6 bit 放不下 64）。
// ---------------------------------------------------------------------------

pub fn encode_values_xor(bits: &[u64]) -> Vec<u8> {
    let mut w = BitWriter::new();
    if bits.is_empty() {
        return w.finish();
    }
    w.write_bits(bits[0], 64);
    let mut prev = bits[0];
    let mut prev_leading: Option<u32> = None;
    let mut prev_trailing: u32 = 0;

    for &v in &bits[1..] {
        let xor = v ^ prev;
        if xor == 0 {
            w.write_bit(false);
        } else {
            let leading = xor.leading_zeros().min(31);
            let trailing = xor.trailing_zeros();
            let fits_prev = match prev_leading {
                Some(pl) => leading >= pl && trailing >= prev_trailing,
                None => false,
            };
            if fits_prev {
                let pl = prev_leading.unwrap();
                w.write_bit(true);
                w.write_bit(false);
                let sig = 64 - pl - prev_trailing;
                w.write_bits(xor >> prev_trailing, sig);
            } else {
                w.write_bit(true);
                w.write_bit(true);
                w.write_bits(leading as u64, 5);
                let sig = 64 - leading - trailing;
                w.write_bits((sig % 64) as u64, 6);
                w.write_bits(xor >> trailing, sig);
                prev_leading = Some(leading);
                prev_trailing = trailing;
            }
        }
        prev = v;
    }
    w.finish()
}

pub fn decode_values_xor(count: usize, buf: &[u8]) -> Result<Vec<u64>, CodecError> {
    let mut out = Vec::with_capacity(count);
    if count == 0 {
        return Ok(out);
    }
    let mut r = BitReader::new(buf);
    let first = r.read_bits(64)?;
    out.push(first);
    let mut prev = first;
    let mut prev_leading = 0u32;
    let mut prev_trailing = 0u32;
    let mut have_window = false;

    while out.len() < count {
        if !r.read_bit()? {
            // xor == 0，值不变
            out.push(prev);
            continue;
        }
        let (leading, sig, trailing);
        if !r.read_bit()? {
            // 复用上一窗口
            if !have_window {
                return Err(CodecError::InvalidStream("window reuse before any window"));
            }
            leading = prev_leading;
            trailing = prev_trailing;
            sig = 64 - leading - trailing;
        } else {
            leading = r.read_bits(5)? as u32;
            let s = r.read_bits(6)? as u32;
            sig = if s == 0 { 64 } else { s };
            if leading + sig > 64 {
                return Err(CodecError::InvalidStream("leading + significant > 64"));
            }
            trailing = 64 - leading - sig;
            prev_leading = leading;
            prev_trailing = trailing;
            have_window = true;
        }
        let bits = r.read_bits(sig)?;
        prev ^= bits << trailing;
        out.push(prev);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zigzag_roundtrip() {
        for v in [0i64, 1, -1, 2, -2, 63, -64, 127, -128, i64::MAX, i64::MIN, i64::MIN + 1] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v, "zigzag roundtrip {v}");
        }
    }

    #[test]
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, 16_384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            let mut pos = 0;
            assert_eq!(read_varint(&buf, &mut pos).unwrap(), v);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn varint_rejects_truncated() {
        let buf = [0x80u8]; // 只有延续位，没有后续字节
        let mut pos = 0;
        assert_eq!(read_varint(&buf, &mut pos), Err(CodecError::UnexpectedEof));
    }

    #[test]
    fn bitstream_roundtrip() {
        let mut w = BitWriter::new();
        w.write_bits(0b101, 3);
        w.write_bits(u64::MAX, 64);
        w.write_bit(true);
        w.write_bits(0, 5);
        w.write_bits(0xdead_beef, 32);
        let buf = w.finish();
        let mut r = BitReader::new(&buf);
        assert_eq!(r.read_bits(3).unwrap(), 0b101);
        assert_eq!(r.read_bits(64).unwrap(), u64::MAX);
        assert!(r.read_bit().unwrap());
        assert_eq!(r.read_bits(5).unwrap(), 0);
        assert_eq!(r.read_bits(32).unwrap(), 0xdead_beef);
    }

    #[test]
    fn timestamps_regular_interval() {
        // 等间隔：每个点的 dod 都是 0，编码应极小
        let ts: Vec<i64> = (0..10_000).map(|i| 1_000_000 + i * 100).collect();
        let enc = encode_timestamps(&ts);
        assert!(enc.len() < 10_500, "regular interval should be ~1 byte/point, got {}", enc.len());
        assert_eq!(decode_timestamps(ts[0], ts.len(), &enc).unwrap(), ts);
    }

    #[test]
    fn timestamps_irregular_interval() {
        // 抖动间隔（模拟不同采样间隔混合）
        let mut s = 0x1234_5678_9abc_def0u64;
        let mut ts = Vec::new();
        let mut t = 1_700_000_000_000i64;
        for _ in 0..5_000 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            t += (s % 997) as i64; // 0..996ms 的随机间隔
            ts.push(t);
        }
        let enc = encode_timestamps(&ts);
        assert_eq!(decode_timestamps(ts[0], ts.len(), &enc).unwrap(), ts);
    }

    #[test]
    fn timestamps_single_point() {
        let ts = vec![42i64];
        let enc = encode_timestamps(&ts);
        assert!(enc.is_empty());
        assert_eq!(decode_timestamps(42, 1, &enc).unwrap(), ts);
    }

    #[test]
    fn xor_special_values_bit_exact() {
        // NaN（含负载位）、负零、无穷、次正规数：必须逐位还原，不得量化
        let specials: Vec<u64> = vec![
            0x7ff8_0000_0000_0000, // 安静 NaN
            0x7ff8_0000_0000_0001, // 带负载 NaN
            0xfff8_0000_0000_0000, // 负 NaN
            0x7ff4_0000_0000_0001, // 类 signaling NaN 位型
            0x8000_0000_0000_0000, // -0.0
            0x0000_0000_0000_0000, // +0.0
            0x7ff0_0000_0000_0000, // +inf
            0xfff0_0000_0000_0000, // -inf
            0x0000_0000_0000_0001, // 最小次正规
            0x7fef_ffff_ffff_ffff, // 最大有限
            25.5f64.to_bits(),
            (-13.25f64).to_bits(),
        ];
        let enc = encode_values_xor(&specials);
        let dec = decode_values_xor(specials.len(), &enc).unwrap();
        assert_eq!(dec, specials, "special values must round-trip bit-exactly");
    }

    #[test]
    fn xor_constant_is_tiny() {
        let vals = vec![25.0f64.to_bits(); 10_000];
        let enc = encode_values_xor(&vals);
        // 常值段：首值 64 位，之后每点 1 bit -> 约 8 + 10000/8 字节
        assert!(enc.len() < 1_400, "constant stream ~1 bit/point, got {}", enc.len());
        assert_eq!(decode_values_xor(vals.len(), &enc).unwrap(), vals);
    }

    #[test]
    fn xor_random_roundtrip() {
        let mut s = 0x9e37_79b9_7f4a_7c15u64;
        let vals: Vec<u64> = (0..10_000)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                s
            })
            .collect();
        let enc = encode_values_xor(&vals);
        assert_eq!(decode_values_xor(vals.len(), &enc).unwrap(), vals);
    }

    #[test]
    fn xor_empty_and_single() {
        assert_eq!(decode_values_xor(0, &encode_values_xor(&[])).unwrap(), Vec::<u64>::new());
        let one = vec![0xdead_beef_cafe_babeu64];
        assert_eq!(decode_values_xor(1, &encode_values_xor(&one)).unwrap(), one);
    }

    #[test]
    fn raw_roundtrip() {
        let vals: Vec<u64> = vec![0, 1, u64::MAX, 0x8000_0000_0000_0000, f64::NAN.to_bits()];
        let enc = encode_values_raw(&vals);
        assert_eq!(enc.len(), vals.len() * 8);
        assert_eq!(decode_values_raw(vals.len(), &enc).unwrap(), vals);
    }
}
