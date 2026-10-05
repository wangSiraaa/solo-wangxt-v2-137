//! Value and timestamp codecs.
//!
//! ## Values — Gorilla-style XOR, bit-verifiable
//!
//! `f64` bit patterns are XOR-compressed against the previous value:
//! * if XOR is 0 a single `0` bit is emitted;
//! * otherwise a `1` bit, then the leading/trailing zero window of the XOR:
//!   * one control bit telling whether the leading/trailing-zero window is
//!     the same as for the previous non-zero XOR;
//!   * if changed: 6 bits leading-zero count, 6 bits meaningful-length,
//!     then the meaningful XOR bits;
//!   * if unchanged: just the meaningful bits with the stored window.
//!
//! XOR is a bitwise operation: NaN payloads, the sign of zero and infinities
//! are never normalized. A value is reconstructed by XOR again, so encoding
//! then decoding yields the **identical u64** for every pattern (asserted in
//! tests). The first value is stored raw (64 bits).
//!
//! ## Timestamps — explicit delta-of-delta, byte aligned
//!
//! Timestamps are int64 Unix nanoseconds. Block 1 stores the raw first
//! timestamp as zig-zag LEB128, followed by the first delta (`t1 - t0`)
//! zig-zag LEB128; every later timestamp stores delta-of-delta
//! `(ti - t_{i-1}) - prev_delta` zig-zag LEB128. Signed zig-zag makes
//! jitter and non-uniform sampling explicit and lossless. This is entirely
//! separate from the bit-packed value region, so both can be decoded
//! independently.

use crate::bits::{BitReader, BitWriter};
use crate::model::Sample;

/// 6 bits each; windows are clamped at 64 (encoded 0..=63 => actual +0,
/// with length 0 impossible for a non-zero XOR, so stored `n` means length
/// `n` directly except leading: leading zeros can be up to 63 for a 64-bit
/// non-zero word; meaningful length ranges 1..=64 and is stored as len-1).
const WINDOW_BITS: u32 = 6;

#[derive(Debug, Clone, Default)]
pub struct ValueEncoder {
    w: BitWriter,
    prev_bits: u64,
    first_done: bool,
    prev_leading: u32,
    prev_trailing: u32,
}

impl ValueEncoder {
    pub fn new() -> Self {
        Self {
            w: BitWriter::with_capacity(64),
            prev_bits: 0,
            first_done: false,
            prev_leading: 0,
            prev_trailing: 0,
        }
    }

    pub fn push(&mut self, bits: u64) {
        if !self.first_done {
            self.w.write_bits(bits, 64);
            self.prev_bits = bits;
            self.first_done = true;
            return;
        }
        let xor = self.prev_bits ^ bits;
        if xor == 0 {
            self.w.write_bit(false);
        } else {
            self.w.write_bit(true);
            let leading = xor.leading_zeros().min(63);
            let trailing = xor.trailing_zeros();
            debug_assert!(leading + trailing <= 63);
            if leading >= self.prev_leading && trailing >= self.prev_trailing {
                // Previous window still covers the changed bits.
                self.w.write_bit(false);
            } else {
                self.w.write_bit(true);
                self.w.write_bits(leading as u64, WINDOW_BITS);
                // meaningful length 1..=64 -> store 0..=63.
                let meaningful = 64 - leading - trailing;
                self.w.write_bits((meaningful - 1) as u64, WINDOW_BITS);
                self.prev_leading = leading;
                self.prev_trailing = trailing;
            }
            let meaningful = 64 - self.prev_leading - self.prev_trailing;
            let mask = if meaningful == 64 {
                u64::MAX
            } else {
                (1u64 << meaningful) - 1
            };
            let meaningful_bits = (xor >> self.prev_trailing) & mask;
            self.w.write_bits(meaningful_bits, meaningful);
        }
        self.prev_bits = bits;
    }

    pub fn finish(self) -> Vec<u8> {
        self.w.into_bytes()
    }
}

#[derive(Debug, Clone)]
pub struct ValueDecoder<'a> {
    r: BitReader<'a>,
    prev_bits: u64,
    first_done: bool,
    prev_leading: u32,
    prev_trailing: u32,
}

impl<'a> ValueDecoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            r: BitReader::new(buf),
            prev_bits: 0,
            first_done: false,
            prev_leading: 0,
            prev_trailing: 0,
        }
    }

    pub fn read_next(&mut self) -> Result<u64, String> {
        if !self.first_done {
            let bits = self
                .r
                .read_bits(64)
                .map_err(|e| format!("value stream: {e}"))?;
            self.prev_bits = bits;
            self.first_done = true;
            return Ok(bits);
        }
        let nonzero = self.r.read_bit().map_err(|e| format!("value stream: {e}"))?;
        if !nonzero {
            return Ok(self.prev_bits);
        }
        let new_window = self.r.read_bit().map_err(|e| format!("value stream: {e}"))?;
        if new_window {
            self.prev_leading = self
                .r
                .read_bits(WINDOW_BITS)
                .map_err(|e| format!("value stream: {e}"))? as u32;
            let len_minus_one = self
                .r
                .read_bits(WINDOW_BITS)
                .map_err(|e| format!("value stream: {e}"))? as u32;
            let meaningful = len_minus_one + 1;
            if self.prev_leading + meaningful > 64 {
                return Err(
                    "value stream: invalid leading-zero/meaningful-length window".to_string()
                );
            }
            self.prev_trailing = 64 - self.prev_leading - meaningful;
        }
        let meaningful = 64 - self.prev_leading - self.prev_trailing;
        let chunk = self
            .r
            .read_bits(meaningful)
            .map_err(|e| format!("value stream: {e}"))?;
        let xor = chunk << self.prev_trailing;
        let bits = self.prev_bits ^ xor;
        self.prev_bits = bits;
        Ok(bits)
    }

    pub fn consumed_bits(&self) -> usize {
        self.r.consumed_bits()
    }
}

/// Encode timestamps with delta-of-delta zig-zag LEB128. Output is byte
/// aligned. `samples` must be non-empty.
pub fn encode_timestamps(samples: &[Sample]) -> Vec<u8> {
    debug_assert!(!samples.is_empty());
    let mut out = Vec::with_capacity(samples.len() * 2);
    write_zigzag(&mut out, samples[0].t);
    if samples.len() > 1 {
        write_zigzag(&mut out, samples[1].t.wrapping_sub(samples[0].t));
        let mut prev_delta = samples[1].t.wrapping_sub(samples[0].t);
        for w in samples.windows(2).skip(1) {
            let delta = w[1].t.wrapping_sub(w[0].t);
            let dod = delta.wrapping_sub(prev_delta);
            write_zigzag(&mut out, dod);
            prev_delta = delta;
        }
    }
    out
}

/// Decode timestamps for a block with `count` samples.
pub fn decode_timestamps(buf: &[u8], count: usize) -> Result<Vec<i64>, String> {
    if count == 0 {
        return if buf.is_empty() {
            Ok(Vec::new())
        } else {
            Err("timestamp stream: expected empty stream for empty block".into())
        };
    }
    let mut pos = 0;
    let t0 = read_zigzag(buf, &mut pos)?;
    let mut ts = Vec::with_capacity(count);
    ts.push(t0);
    if count == 1 {
        if pos != buf.len() {
            return Err("timestamp stream: trailing bytes after single timestamp".into());
        }
        return Ok(ts);
    }
    let first_delta = read_zigzag(buf, &mut pos)?;
    let mut prev_t = t0;
    let mut prev_delta = first_delta;
    // second timestamp
    prev_t = prev_t.wrapping_add(first_delta);
    ts.push(prev_t);
    for _ in 2..count {
        let dod = read_zigzag(buf, &mut pos)?;
        let delta = prev_delta.wrapping_add(dod);
        prev_t = prev_t.wrapping_add(delta);
        prev_delta = delta;
        ts.push(prev_t);
    }
    if pos != buf.len() {
        return Err(format!(
            "timestamp stream: {0} trailing byte(s) after {count} timestamps",
            buf.len() - pos
        ));
    }
    Ok(ts)
}

pub fn write_zigzag(out: &mut Vec<u8>, v: i64) {
    let mut u = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        let mut b = (u & 0x7f) as u8;
        u >>= 7;
        if u != 0 {
            b |= 0x80;
        }
        out.push(b);
        if u == 0 {
            break;
        }
    }
}

pub fn read_zigzag(buf: &[u8], pos: &mut usize) -> Result<i64, String> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        if *pos >= buf.len() {
            return Err("timestamp stream: truncated LEB128".into());
        }
        if shift >= 64 {
            return Err("timestamp stream: LEB128 too long".into());
        }
        let b = buf[*pos];
        *pos += 1;
        result |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(((result >> 1) as i64) ^ -((result & 1) as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_values(bits: &[u64]) {
        let mut enc = ValueEncoder::new();
        for b in bits {
            enc.push(*b);
        }
        let raw = enc.finish();
        let mut dec = ValueDecoder::new(&raw);
        for b in bits {
            assert_eq!(dec.read_next().unwrap(), *b);
        }
    }

    #[test]
    fn value_bit_exact_specials_and_noise() {
        let patterns = [
            0x0000_0000_0000_0000, // +0
            0x8000_0000_0000_0000, // -0 differs by exactly sign bit
            0x7ff0_0000_0000_0000, // +inf
            0xfff0_0000_0000_0000, // -inf
            0x7ff8_0000_0000_0000, // canonical quiet NaN
            0x7ff0_0000_0000_0001, // sNaN-ish payload
            0xffff_ffff_ffff_ffff, // NaN, all mantissa bits, sign set
            0x3ff0_0000_0000_0000, // 1.0
            0x0000_0000_0000_0001, // smallest subnormal
            0x000f_ffff_ffff_ffff, // largest subnormal
            0x4009_21fb_5444_2d18, // pi
        ];
        roundtrip_values(&patterns);
        // Constant long run: must compress to 64 + N bits.
        let mut many = vec![0x4009_21fb_5444_2d18u64; 10_000];
        roundtrip_values(&many);
        let mut enc = ValueEncoder::new();
        for b in &many {
            enc.push(*b);
        }
        let raw = enc.finish();
        // 64 raw bits + 9999 zero-bits = 10063 bits => 1258 bytes padded.
        assert_eq!(raw.len(), (64 + many.len() - 1).div_ceil(8));
        // Random-ish data must still round trip.
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        many.clear();
        for _ in 0..5000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            many.push(x);
        }
        roundtrip_values(&many);
    }

    #[test]
    fn timestamp_roundtrips_intervals() {
        let mk = |ts: &[i64]| ts.iter().map(|&t| Sample::new(t, 1.0)).collect::<Vec<_>>();
        for ts in [
            vec![1_000_000_000, 1_000_001_000, 1_000_002_000], // 1kHz fixed
            vec![0, 1, 2, 3, 1000, 1001, 1003, 1004],          // interval jump + jitter
            vec![-5_000_000_000, -4_999_999_999],              // negative epoch
            vec![42],
            vec![100, 50, 200, -10, -9],                        // negative deltas
        ] {
            let s = mk(&ts);
            let enc = encode_timestamps(&s);
            let dec = decode_timestamps(&enc, s.len()).unwrap();
            assert_eq!(dec, ts);
        }
    }

    #[test]
    fn constant_segment_compresses_timestamps_hard() {
        let s: Vec<_> = (0..10_000)
            .map(|i| Sample::new(1_000_000 + i * 1_000_000, 7.0))
            .collect();
        let enc = encode_timestamps(&s);
        // t0 (~5 bytes) + delta (~2 bytes) + 9998 zero varints = 1 byte each.
        assert!(enc.len() < 10_100);
        assert_eq!(decode_timestamps(&enc, s.len()).unwrap().len(), 10_000);
    }

    #[test]
    fn detects_truncated_streams() {
        let s = [Sample::new(0, 1.0), Sample::new(1, 2.0)];
        let enc = encode_timestamps(&s);
        assert!(decode_timestamps(&enc[..enc.len() - 1], 2).is_err());
        assert!(decode_timestamps(&enc, 1).is_err());
    }
}
