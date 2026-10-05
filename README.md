# sensor-segments

High-frequency sensor values compressed into randomly queryable file
segments: **Rust + Axum** JSON API, **SQLite** series/segment catalog,
local **immutable segment files** carrying delta-of-delta timestamps and
bit-exact XOR-compressed f64 values. No frontend.

See [`docs/FORMAT.md`](docs/FORMAT.md) for the on-disk format, float/NaN
rules, duplicate semantics and crash-safety guarantees.

## Run

```bash
cargo run --release -- --data-dir ./sensor-data --bind 0.0.0.0:8080
# env: SENSOR_MAX_BODY_BYTES (default 256 MiB)
```

Layout:

```
<data-dir>/catalog.db                     # SQLite: series + segment index
<data-dir>/segments/<series-id>/<seg-id>.seg   # immutable data files
```

## API

All timestamps are **int64 Unix nanoseconds**. Values are JSON numbers,
`"nan"` / `"+inf"` / `"-inf"`, or exactly 16 hex digits (`"0x…"`) for raw
binary64 bits. Every returned point has authoritative `bits` + `kind`
(`finite|pos_inf|neg_inf|nan`); `v` is the number only when finite.

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/series` | `{name, duplicate_policy: keep_all\|keep_first\|keep_last\|reject}` |
| GET  | `/v1/series` / `/v1/series/:id` / `/v1/series/by-name/:name` | catalog |
| POST | `/v1/series/:id/segments` | seal one batch into a new immutable segment |
| GET  | `/v1/series/:id/segments` | list segment rows |
| GET  | `/v1/series/:id/points?from&to&limit&offset` | indexed range query |
| GET  | `/v1/segments/:id` | segment metadata |
| POST | `/v1/segments/:id/verify` | CRC+decode every block (deep verify) |
| GET  | `/v1/reconcile` | re-run startup reconciliation |

### Write

```bash
curl -s -X POST localhost:8080/v1/series -H 'content-type: application/json' \
  -d '{"name":"vib-1khz","duplicate_policy":"keep_all"}'
# -> {"id":"0000...","name":"vib-1khz","duplicate_policy":"keep_all",...}

curl -s -X POST localhost:8080/v1/series/$SID/segments \
  -H 'content-type: application/json' -d '{"samples":[
    {"t":1700000000000000000,"v":1.25},
    {"t":1700000000001000000,"bits":"8000000000000000"},
    {"t":1700000000002000000,"v":"nan"},
    {"t":1700000000003000000,"v":"-inf"}
  ]}'
```

Response reports `samples_written`, `duplicates_dropped`, `block_count`,
`file_len`, and the on-disk path. Samples must be non-decreasing in `t`;
each batch becomes one segment, and segment ranges must not overlap.

### Indexed range query

```bash
curl -s "localhost:8080/v1/series/$SID/points?from=1700000000000000000&to=1700000060000000000&limit=10000&offset=0"
```

The catalog first narrows to overlapping *segments* (`min_t/max_t` index);
each segment's block headers then narrow to overlapping *blocks*. The
response shows `blocks_touched` per segment — untouched blocks are never
read, checksummed or decoded, so reading a minute never decompresses a day.
`has_more` + `offset` page across segments in time order.

### Corruption reporting

A flipped byte yields HTTP 422 with the exact byte range, e.g.

```json
{"error":{"code":"segment_corrupt","message":"...",
 "corruption":{"type":"block_checksum","file_len":14572,
   "ranges":[{"start":9959,"end":14508,"what":"block 2 payload",
              "detail":"CRC32C mismatch: stored 0x39476e48, computed 0x942c3d77"}]}}}
```

### Crash recovery

* `.seg.tmp` tails from an interrupted seal are deleted on startup
  (logged as `removed unfinished temp segment`) and never queryable.
* A sealed `.seg` missing its catalog row is reported as an **orphan** via
  startup logs and `/v1/reconcile`, and is not auto-adopted.

## Tests

```bash
cargo test
```

* Unit: bit-stream round-trip, XOR bit-exactness for NaN/-0/inf/subnormals,
  zig-zag timestamps with jitter/negative deltas, CRC32C known vector,
  duplicate policies, corruption localization, torn-temp/orphan handling.
* E2E (Axum router + real files): mixed sampling intervals bit-checked end
  to end, constant vs sudden-change compression, index selectivity
  (`blocks_touched==[97]` of 200 for a single-point window), paging across
  segments, byte-flip corruption ranges, and tmp-tail reaping.
