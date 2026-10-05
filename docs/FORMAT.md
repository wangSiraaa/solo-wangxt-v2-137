# Segment file format (SSEG0001)

Immutable, append-only per-segment files holding compressed high-frequency
sensor samples. All integer fields are **little-endian**. Timestamps are
explicit **int64 Unix nanoseconds** everywhere; there is no implicit unit.

```
offset 0
┌─────────────────────────── file header (56 B) ───────────────────────────┐
│ 0..8   magic            "SSEG0001"                                       │
│ 8..40  series_id        32 ASCII hex chars                               │
│ 40     duplicate_policy 0=keep_all 1=keep_first 2=keep_last 3=reject     │
│ 41     value_codec      1 = XOR64 v1                                     │
│ 42..46 block_count      u32 (+ 2 reserved bytes)                         │
│ 46..52 reserved (zero)                                                   │
│ 52..56 header_crc32c    CRC32C over bytes 0..52                          │
└──────────────────────────────────────────────────────────────────────────┘
offset 56
┌──────── repeated block_count times ────────┐
│ block header  (40 B)                        │
│   0..4   count      u32, 1..=512            │
│   4..12  t0         i64  (first timestamp)  │
│   12..20 t_last     i64  (last timestamp)   │
│   20..24 ts_len     u32 bytes               │
│   24..28 val_len    u32 bytes               │
│   28..32 payload_crc32c                     │
│   32..40 reserved (zero)                    │
│ timestamp payload (ts_len B, byte aligned)  │
│ value payload     (val_len B, bit packed)   │
└─────────────────────────────────────────────┘
┌─────────────────────────── footer (64 B) ───────────────────────────────┐
│ 0..8   end magic   "SSEGEND1"                                            │
│ 8..16  min_t i64      16..24 max_t i64                                   │
│ 24..32 total sample count u64     32..40 payload_bytes u64               │
│ 40..48 footer_start u64           48..56 declared file_len u64           │
│ 56..60 index_crc32c  (CRC of all block-header bytes, in block order)     │
│ 60..64 reserved                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

A block is the random-access unit (≤ **512 samples**). Range queries pick
blocks via the in-memory block headers (`t0` / `t_last`, binary search) and
read+checksum+decode only the touched blocks — a one-minute window never
decompresses a whole day.

## Timestamp encoding (explicit delta-of-delta)

Byte-aligned, independent of the value stream:

* `zigzag_leb128(t0)` — absolute first timestamp.
* `zigzag_leb128(t1 - t0)` — first delta.
* for every later sample: `zigzag_leb128((ti - t_{i-1}) - previous_delta)`.

Zig-zag encodes signed values, so irregular sampling, jitter and even
negative deltas are represented explicitly and losslessly. Constant sample
intervals collapse to one `0` byte per timestamp.

## Value encoding (verifiable XOR, bit-exact)

Values are always the raw **IEEE-754 binary64 bit pattern**
(`f64::to_bits`). The compressor performs only bitwise operations — never
floating-point arithmetic — so encoding then decoding is the identical u64:

1. First value: raw 64 bits.
2. `xor = prev XOR current`:
   * xor == 0 → single `0` bit (constant runs cost ~1 bit/sample);
   * otherwise `1`, then a control bit:
     * `0` = reuse previous leading/trailing-zero window;
     * `1` = new window: 6 bits leading-zero count, 6 bits
       (meaningful-length − 1), then the meaningful XOR bits MSB-first.

Bits are packed MSB-first within bytes; the last byte is zero-padded and
padding is never read (decoder consumes exactly `count` values).

### Special values — explicitly preserved, never normalized

| Pattern                      | Bits                 | Wire `kind` |
|------------------------------|----------------------|-------------|
| `+0.0`                       | `0000000000000000`   | `finite`    |
| `-0.0`                       | `8000000000000000`   | `finite`    |
| `+Inf`                       | `7ff0000000000000`   | `pos_inf`   |
| `-Inf`                       | `fff0000000000000`   | `neg_inf`   |
| quiet NaN (canonical)        | `7ff8000000000000`   | `nan`       |
| any NaN payload / sign       | e.g. `fff8000000000123` | `nan`     |
| subnormals                   | raw bits             | `finite`    |

Equality in the compressor is **bit equality**: `-0.0` differs from `+0.0`
by the sign bit and is stored as a distinct value; NaN payloads (including
the quiet/signaling bit and sign bit) survive bit-for-bit. JSON responses
always carry the authoritative hex `bits` plus `kind`; `v` is a JSON number
only for finite values and `null` otherwise.

Inputs accept JSON numbers, the token `"nan"`/`"+inf"`/`"-inf"`, or exactly
16 hex digits (`"0x…"` or bare) for arbitrary bit patterns. A JSON integer
that is not exactly representable as f64 (mantissa > 53 bits) is **rejected
with 400** — it is never silently rounded/quantized.

## Duplicate timestamps — explicit per-series policy

Within one write stream timestamps must be non-decreasing (backwards = 400,
no client-side reordering). Equal timestamps follow the series policy,
configured at creation and stored in the file header:

* `keep_all` (default): every occurrence retained in arrival order;
* `keep_first`: later duplicates dropped;
* `keep_last`: earlier duplicates replaced;
* `reject`: the whole batch fails with 409 and is not persisted.

Segment time ranges in the catalog are **strictly disjoint**
(`new.min_t > previous max_t`, enforced in a DB transaction), so duplicates
can only occur inside one segment where this policy applies.

## Integrity checks

* **per block payload**: CRC32C (Castagnoli) over `timestamps ‖ values`,
  verified lazily only for blocks a query touches;
* **block index**: CRC32C over concatenated block headers in the footer;
* **file header**: its own CRC32C;
* **footer**: end magic + declared file length must match the actual size.

CRC32C uses the reflected polynomial (bit reversal of `0x1EDC6F41` =
`0x82F63B78`; note `0x82F63B79` seen in some sources is off by one and fails
the standard check vector). Check value: CRC32C("123456789") = `0xE3069283`.

Failures return HTTP **422** with structured `corruption`:
`{type, file_len, ranges:[{start,end,what,detail}]}` — the reported
half-open byte range pinpoints the damaged block header/payload.

## Sealing and crash safety

Writers stream to `<id>.seg.tmp`. A segment becomes visible only via:

1. flush + `fsync` the temp file;
2. close;
3. atomic `rename` to `<id>.seg` + `fsync` directory;
4. **then** insert the catalog row (transactional gap check).

A crash at step 1–3 leaves a `.tmp` tail that is never registered and is
reaped on startup. A crash between 3 and 4 leaves an *orphan* `.seg`: valid
on disk but absent from the catalog — startup reports it but does **not**
serve it (the catalog row is the sole source of queryability). `.seg` files
are never modified after the rename.
