//! MSB-first bit writer/reader used by the XOR value codec.
//!
//! Within one byte the first written bit occupies bit 7, then bit 6, ...
//! This is the bit ordering documented in `docs/FORMAT.md`; timestamps in
//! block payloads are byte-aligned (zig-zag LEB128), value streams are the
//! only bit-packed region.

#[derive(Debug, Clone, Default)]
pub struct BitWriter {
    out: Vec<u8>,
    /// Bits already accumulated into the current (last) byte, 0..=7.
    used: u32,
}

impl BitWriter {
    pub fn new() -> Self {
        Self { out: Vec::new(), used: 0 }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self { out: Vec::with_capacity(cap), used: 0 }
    }

    /// Write the low `nbits` bits of `bits`, MSB-first. `nbits` must be <= 64.
    pub fn write_bits(&mut self, bits: u64, nbits: u32) {
        debug_assert!(nbits <= 64);
        if nbits == 0 {
            return;
        }
        // Mask off anything above nbits; handle nbits == 64 without a shift
        // by 64 (which is undefined/overflow-panic in safe Rust).
        let bits = if nbits == 64 {
            bits
        } else {
            bits & ((1u64 << nbits) - 1)
        };
        let mut remaining = nbits;
        let mut value = bits;
        while remaining > 0 {
            if self.used == 0 {
                self.out.push(0);
            }
            let free = 8 - self.used; // 1..=8
            let take = remaining.min(free);
            // Shift the next `take` MSB-aligned chunk of `value` into place.
            let shift = remaining - take;
            let chunk = if shift == 64 { 0 } else { (value >> shift) as u8 };
            let last = self.out.last_mut().unwrap();
            *last |= chunk << (free - take);
            if shift < 64 {
                let keep_mask = if shift == 0 { 0 } else { (1u64 << shift) - 1 };
                value &= keep_mask;
            } else {
                value = 0;
            }
            remaining -= take;
            self.used = (self.used + take) & 7;
        }
    }

    pub fn write_bit(&mut self, bit: bool) {
        self.write_bits(bit as u64, 1);
    }

    pub fn byte_len(&self) -> usize {
        self.out.len()
    }

    /// Finish: zero pad the final byte (padding bits are never read because
    /// the decoder consumes exactly `count` values and stops).
    pub fn into_bytes(self) -> Vec<u8> {
        self.out
    }
}

#[derive(Debug, Clone)]
pub struct BitReader<'a> {
    buf: &'a [u8],
    /// Absolute bit position.
    pos: usize,
}

impl<'a> BitReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn total_bits(&self) -> usize {
        self.buf.len() * 8
    }

    pub fn read_bit(&mut self) -> Result<bool, &'static str> {
        if self.pos >= self.buf.len() * 8 {
            return Err("bitstream truncated");
        }
        let byte = self.buf[self.pos >> 3];
        let bit = (byte >> (7 - (self.pos & 7))) & 1;
        self.pos += 1;
        Ok(bit != 0)
    }

    pub fn read_bits(&mut self, nbits: u32) -> Result<u64, &'static str> {
        if nbits > 64 {
            return Err("read_bits nbits > 64");
        }
        let mut value: u64 = 0;
        for _ in 0..nbits {
            value = (value << 1) | self.read_bit()? as u64;
        }
        Ok(value)
    }

    /// Number of bits consumed so far.
    pub fn consumed_bits(&self) -> usize {
        self.pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_mixed_widths() {
        let mut w = BitWriter::new();
        let cases: &[(u64, u32)] = &[
            (1, 1),
            (0, 1),
            (0b101, 3),
            (0b11110000, 8),
            (0xdead_beef_cafe_babe, 64),
            (7, 5),
            (0, 1),
            (0xab, 12),
        ];
        for (v, n) in cases {
            w.write_bits(*v, *n);
        }
        let bytes = w.into_bytes();
        let mut r = BitReader::new(&bytes);
        for (v, n) in cases {
            assert_eq!(r.read_bits(*n).unwrap(), *v, "nbits={n}");
        }
    }

    #[test]
    fn all_ones_64() {
        let mut w = BitWriter::new();
        w.write_bits(u64::MAX, 64);
        w.write_bit(true);
        let b = w.into_bytes();
        assert_eq!(&b[..8], &[0xff; 8]);
        let mut r = BitReader::new(&b);
        assert_eq!(r.read_bits(64).unwrap(), u64::MAX);
        assert!(r.read_bit().unwrap());
    }
}
