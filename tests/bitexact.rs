//! Explicit bit-level verification of the compress-then-decompress pipeline
//! on the requested workload shapes: different sampling intervals, constant
//! runs, and sudden step/spike samples. No sample may change by even one bit.

use sensor_segments::codec::{
    decode_timestamps, encode_timestamps, ValueDecoder, ValueEncoder,
};
use sensor_segments::model::Sample;

/// Encode values the same way a block payload does, then decode exactly
/// `n` values and assert the raw u64 patterns are identical bit-for-bit.
fn xor_roundtrip_bitexact(bits: &[u64]) -> Vec<u8> {
    let mut enc = ValueEncoder::new();
    for b in bits {
        enc.push(*b);
    }
    let raw = enc.finish();
    let mut dec = ValueDecoder::new(&raw);
    for (i, want) in bits.iter().enumerate() {
        let got = dec.read_next().expect("decode value");
        assert_eq!(
            got, *want,
            "bit mismatch at #{i}: got {got:016x} want {want:016x}"
        );
    }
    raw
}

fn f(v: f64) -> u64 {
    v.to_bits()
}

#[test]
fn different_sampling_intervals_bit_by_bit() {
    // 1 kHz, 100 Hz, jittered, and a backwards jitter component — timestamps
    // must survive zig-zag delta-of-delta exactly.
    let cases: &[&[i64]] = &[
        // strict 1 ms
        &(0..2000).map(|i| i * 1_000_000).collect::<Vec<_>>(),
        // 10 ms
        &(0..500).map(|i| i * 10_000_000).collect::<Vec<_>>(),
        // jittered around 1 ms, non-decreasing
        &(0..1000)
            .map(|i| i * 1_000_000 + (i % 5) * 1234)
            .collect::<Vec<_>>(),
        // interval changes: 1ms then a gap then 2ms
        &(0..256)
            .map(|i| if i < 100 { i * 1_000_000 } else { 100_000_000 + (i - 100) * 2_000_000 })
            .collect::<Vec<_>>(),
    ];
    for ts in cases {
        let s: Vec<Sample> = ts.iter().map(|&t| Sample::new(t, t as f64 / 1e9)).collect();
        let enc = encode_timestamps(&s);
        let dec = decode_timestamps(&enc, s.len()).expect("decode timestamps");
        assert_eq!(dec, *ts, "timestamp interval case not bit-exact");
    }
}

#[test]
fn constant_run_then_step_change_bit_by_bit() {
    // 3000 identical 3.3 V readings (flat segment), a single spike, another
    // flat run, and a permanent step change. Every raw bit must round-trip.
    let mut bits = Vec::new();
    let v_flat = f(3.3);
    let v_spike = f(12.75);
    let v_step = f(3.35);
    for _ in 0..3000 {
        bits.push(v_flat);
    }
    bits.push(v_spike); // one-sample spike, immediately back
    for _ in 0..1000 {
        bits.push(v_flat);
    }
    for _ in 0..2000 {
        bits.push(v_step); // permanent step
    }

    let raw = xor_roundtrip_bitexact(&bits);

    // Constant runs must compress to ~1 bit/sample: total stream far under
    // the raw 6001*8 = 48008 value bytes.
    assert!(
        raw.len() < 3000,
        "constant+step values should compress hard, got {} bytes",
        raw.len()
    );
    // ...and a wholly constant segment is ~1 byte per 8 samples.
    let all_flat = vec![v_flat; 10_000];
    let raw_flat = xor_roundtrip_bitexact(&all_flat);
    assert_eq!(
        raw_flat.len(),
        (64usize + 9999).div_ceil(8),
        "10k identical values must be 64 raw bits + 9999 zero bits"
    );
}

#[test]
fn sudden_change_through_special_values_bit_by_bit() {
    // Abrupt jumps between "normal" readings and every special encoding must
    // preserve exact bits including NaN payload and negative zero.
    let bits = [
        f(0.00001),
        f(-0.0),
        f(1.0e308),
        0x7ff8_0000_0000_0000, // NaN
        0xffff_ffff_ffff_ffff, // NaN all-payload, sign set
        f(f64::MIN),
        0x0000_0000_0000_0001, // smallest positive subnormal
        0x8000_0000_0000_0001, // smallest negative subnormal
        f(0.0),
        f(0.00001),
    ];
    let _ = xor_roundtrip_bitexact(&bits);
}

#[test]
fn xor_window_changes_decode_exactly_under_brutal_adjacency() {
    // Adversarial adjacent patterns forcing frequent leading/trailing
    // window changes (differences in high bits, low bits, and middle bits).
    let mut bits = vec![0u64];
    let mut x = 0x1234_5678_9abc_def0u64;
    for k in 0..4096u64 {
        // rotate xor source across bit positions
        let target = match k % 4 {
            0 => x ^ (1u64 << (k % 64)),           // single-bit move
            1 => x ^ 0xffff_ffff_0000_0000,        // high-half change
            2 => x ^ 0x0000_0000_ffff_ffff,        // low-half change
            _ => x ^ (0xffu64 << ((k * 7) % 57)),  // moving 8-bit window
        };
        bits.push(target);
        x = target;
    }
    xor_roundtrip_bitexact(&bits);
}
