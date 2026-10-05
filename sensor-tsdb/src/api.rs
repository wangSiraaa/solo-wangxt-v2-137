//! Axum HTTP API。
//!
//! 特殊值策略（显式，绝不悄悄量化）：
//! - 写入：value 可以是 JSON 数值、"NaN"/"Infinity"/"-Infinity"/"-0.0" 字符串，
//!   或 {"bits": "0x..."} 直接给 IEEE-754 位模式。数值经 serde_json 按 IEEE
//!   最近舍入解析一次（这是 JSON 本身的语义），之后全程只搬运位模式。
//! - 读取：每个点都同时返回 value（有限值为数值，非有限值为字符串）和
//!   bits（16 位十六进制），调用方可逐位核对。

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::{
    extract::{Path as AxPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::catalog::{Catalog, SegmentRow, STATE_CORRUPT, STATE_SEALED};
use crate::samples;
use crate::segment::{self, SegmentFile, ValueCodec};

pub struct AppState {
    pub catalog: Mutex<Catalog>,
    pub data_dir: PathBuf,
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/series", post(create_series).get(list_series))
        .route("/api/series/:id/points", post(append_points))
        .route("/api/series/:id/query", get(query_range))
        .route("/api/series/:id/segments", get(list_segments))
        .route("/api/segments/:id/verify", post(verify_segment_handler))
        .route("/api/seed", post(seed))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// 错误
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, msg: impl Into<String>) -> Self {
        Self { status, message: msg.into() }
    }
    fn bad_request(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, msg)
    }
    fn not_found(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, msg)
    }
    fn conflict(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, msg)
    }
    fn internal(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, msg)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

impl From<segment::SegmentError> for ApiError {
    fn from(e: segment::SegmentError) -> Self {
        ApiError::internal(format!("segment: {e}"))
    }
}
impl From<rusqlite::Error> for ApiError {
    fn from(e: rusqlite::Error) -> Self {
        ApiError::internal(format!("catalog: {e}"))
    }
}
impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self {
        ApiError::internal(format!("io: {e}"))
    }
}

// ---------------------------------------------------------------------------
// 值表示：显式的特殊值处理
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(untagged)]
enum ValueIn {
    Num(f64),
    Str(String),
    Bits { bits: String },
}

impl ValueIn {
    /// 值 -> IEEE-754 位模式。除「JSON 数值 -> f64」这一次由 serde_json
    /// 完成的正确舍入外，不做任何量化；NaN/-0.0/±∞ 全部按位保留。
    fn to_bits(&self) -> Result<u64, ApiError> {
        match self {
            ValueIn::Num(f) => Ok(f.to_bits()),
            ValueIn::Str(s) => parse_special(s).ok_or_else(|| {
                ApiError::bad_request(format!(
                    "unrecognized value string '{s}'; use a JSON number, \"NaN\", \
                     \"Infinity\", \"-Infinity\", \"-0.0\", or {{\"bits\":\"0x...\"}}"
                ))
            }),
            ValueIn::Bits { bits } => parse_bits(bits).ok_or_else(|| {
                ApiError::bad_request(format!("invalid bits '{bits}', expected hex with 0x prefix"))
            }),
        }
    }
}

fn parse_bits(s: &str) -> Option<u64> {
    let hex = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"))?;
    if hex.is_empty() || hex.len() > 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

fn parse_special(s: &str) -> Option<u64> {
    match s {
        "NaN" => Some(f64::NAN.to_bits()),
        "Infinity" | "+Infinity" => Some(f64::INFINITY.to_bits()),
        "-Infinity" => Some(f64::NEG_INFINITY.to_bits()),
        "-0.0" | "-0" => Some((-0.0f64).to_bits()),
        _ => parse_bits(s),
    }
}

fn point_json(ts: i64, bits: u64) -> Value {
    let f = f64::from_bits(bits);
    // 非有限值序列化为字符串（JSON 无法表示 NaN/Inf），bits 十六进制永远携带
    let value = if f.is_nan() {
        json!("NaN")
    } else if f == f64::INFINITY {
        json!("Infinity")
    } else if f == f64::NEG_INFINITY {
        json!("-Infinity")
    } else {
        json!(f)
    };
    json!({ "ts": ts, "value": value, "bits": format!("0x{bits:016x}") })
}

// ---------------------------------------------------------------------------
// 处理器
// ---------------------------------------------------------------------------

async fn index() -> &'static str {
    "sensor-tsdb\n\
     POST /api/series                 {name, codec?}\n\
     GET  /api/series\n\
     POST /api/series/:id/points      {points:[{ts,value}], codec?}\n\
     GET  /api/series/:id/query?from=&to=\n\
     GET  /api/series/:id/segments\n\
     POST /api/segments/:id/verify\n\
     POST /api/seed                   生成样本并逐位核对\n"
}

#[derive(Deserialize)]
struct CreateSeriesReq {
    name: String,
    codec: Option<String>,
}

async fn create_series(
    State(st): State<Arc<AppState>>,
    Json(req): Json<CreateSeriesReq>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let codec = match req.codec.as_deref() {
        None => ValueCodec::Xor,
        Some(s) => ValueCodec::from_name(s)
            .ok_or_else(|| ApiError::bad_request(format!("unknown codec '{s}', expected 'xor' or 'raw'")))?,
    };
    let cat = st.catalog.lock().unwrap();
    if let Some(existing) = cat.get_series_by_name(&req.name)? {
        return Err(ApiError::conflict(format!(
            "series '{}' already exists with id {}",
            req.name, existing.id
        )));
    }
    let id = cat.create_series(&req.name, codec as i64)?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "id": id, "name": req.name, "codec": codec.name() })),
    ))
}

async fn list_series(State(st): State<Arc<AppState>>) -> Result<Json<Value>, ApiError> {
    let cat = st.catalog.lock().unwrap();
    let rows = cat.list_series()?;
    Ok(Json(json!({
        "series": rows.iter().map(|r| json!({
            "id": r.id,
            "name": r.name,
            "codec": ValueCodec::from_u8(r.codec as u8).map(|c| c.name()).unwrap_or("unknown"),
            "created_at": r.created_at,
        })).collect::<Vec<_>>()
    })))
}

#[derive(Deserialize)]
struct PointIn {
    ts: i64,
    value: ValueIn,
}

#[derive(Deserialize)]
struct AppendReq {
    points: Vec<PointIn>,
    codec: Option<String>,
}

async fn append_points(
    State(st): State<Arc<AppState>>,
    AxPath(series_id): AxPath<i64>,
    Json(req): Json<AppendReq>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if req.points.is_empty() {
        return Err(ApiError::bad_request("points must not be empty"));
    }
    // 先全部转成位模式：任何非法值直接 400，不产生部分写入
    let mut parsed = Vec::with_capacity(req.points.len());
    for p in &req.points {
        parsed.push((p.ts, p.value.to_bits()?));
    }
    // 重复时间戳规则（显式）：同一批次内相同 ts 保留最后一条（keep-last），
    // 随后按 ts 升序排列；跨批次重叠由 ingest 拒绝（409）。
    let mut dedup: BTreeMap<i64, u64> = BTreeMap::new();
    for (ts, bits) in parsed {
        dedup.insert(ts, bits);
    }
    let dropped = req.points.len() - dedup.len();
    let points: Vec<(i64, u64)> = dedup.into_iter().collect();

    let codec = {
        let cat = st.catalog.lock().unwrap();
        let series = cat
            .get_series(series_id)?
            .ok_or_else(|| ApiError::not_found(format!("series {series_id} not found")))?;
        match req.codec.as_deref() {
            Some(s) => ValueCodec::from_name(s)
                .ok_or_else(|| ApiError::bad_request(format!("unknown codec '{s}'")))?,
            None => ValueCodec::from_u8(series.codec as u8)
                .map_err(|e| ApiError::internal(format!("series codec: {e}")))?,
        }
    };

    let outcome = ingest(&st, series_id, codec, &points)?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "series_id": series_id,
            "segment_id": outcome.segment_id,
            "count": points.len(),
            "duplicates_dropped": dropped,
            "duplicate_rule": "keep-last within a batch; batches must be strictly newer than sealed data (overlap -> 409)",
            "start_ts": points[0].0,
            "end_ts": points[points.len() - 1].0,
            "codec": codec.name(),
            "bytes": outcome.bytes,
        })),
    ))
}

#[derive(Deserialize)]
struct QueryParams {
    from: i64,
    to: i64,
}

async fn query_range(
    State(st): State<Arc<AppState>>,
    AxPath(series_id): AxPath<i64>,
    Query(q): Query<QueryParams>,
) -> Result<Json<Value>, ApiError> {
    if q.from > q.to {
        return Err(ApiError::bad_request("from must be <= to"));
    }
    let (points, stats) = query_points(&st, series_id, q.from, q.to)?;
    let pts_json: Vec<Value> = points.iter().map(|(ts, bits)| point_json(*ts, *bits)).collect();
    Ok(Json(json!({
        "series_id": series_id,
        "from": q.from,
        "to": q.to,
        "count": points.len(),
        "points": pts_json,
        "segments": stats,
    })))
}

async fn list_segments(
    State(st): State<Arc<AppState>>,
    AxPath(series_id): AxPath<i64>,
) -> Result<Json<Value>, ApiError> {
    let cat = st.catalog.lock().unwrap();
    let rows = cat.list_segments(series_id)?;
    Ok(Json(json!({
        "segments": rows.iter().map(segment_row_json).collect::<Vec<_>>()
    })))
}

fn segment_row_json(s: &SegmentRow) -> Value {
    json!({
        "id": s.id,
        "series_id": s.series_id,
        "start_ts": s.start_ts,
        "end_ts": s.end_ts,
        "count": s.count,
        "codec": ValueCodec::from_u8(s.codec as u8).map(|c| c.name()).unwrap_or("unknown"),
        "state": s.state,
        "path": s.path,
        "file_crc": format!("0x{:08x}", s.file_crc as u32),
        "created_at": s.created_at,
    })
}

async fn verify_segment_handler(
    State(st): State<Arc<AppState>>,
    AxPath(seg_id): AxPath<i64>,
) -> Result<Json<Value>, ApiError> {
    let seg = {
        let cat = st.catalog.lock().unwrap();
        cat.get_segment(seg_id)?
            .ok_or_else(|| ApiError::not_found(format!("segment {seg_id} not found")))?
    };
    if seg.state != STATE_SEALED && seg.state != STATE_CORRUPT {
        return Err(ApiError::conflict(format!(
            "segment {seg_id} is in state '{}'; only sealed/corrupt segments can be verified",
            seg.state
        )));
    }
    let path = st.data_dir.join(&seg.path);
    let data = match fs::read(&path) {
        Ok(d) => d,
        Err(e) => {
            // 文件缺失同样视为损坏：登记并从查询中排除
            let cat = st.catalog.lock().unwrap();
            let _ = cat.mark_corrupt(seg_id);
            return Ok(Json(json!({
                "segment_id": seg_id,
                "ok": false,
                "state": "corrupt",
                "errors": [format!("file missing: {}: {e}", seg.path)],
                "corrupt_ranges": [{ "from_ts": seg.start_ts, "to_ts": seg.end_ts }],
            })));
        }
    };
    let rep = segment::verify_segment(&data);
    if !rep.ok {
        // 校验失败：登记为 corrupt，之后的查询不再命中该段
        let cat = st.catalog.lock().unwrap();
        let _ = cat.mark_corrupt(seg_id);
    }
    Ok(Json(json!({
        "segment_id": seg_id,
        "series_id": seg.series_id,
        "state": if rep.ok { seg.state.clone() } else { STATE_CORRUPT.to_string() },
        "ok": rep.ok,
        "header_ok": rep.header_ok,
        "footer_ok": rep.footer_ok,
        "index_ok": rep.index_ok,
        "file_crc_ok": rep.file_crc_ok,
        "errors": rep.errors,
        "blocks": rep.blocks.iter().map(|b| json!({
            "index": b.index,
            "first_ts": b.first_ts,
            "last_ts": b.last_ts,
            "points": b.points,
            "ok": b.ok,
            "error": b.error,
        })).collect::<Vec<_>>(),
        "corrupt_ranges": rep.corrupt_ranges.iter()
            .map(|(a, b)| json!({ "from_ts": a, "to_ts": b }))
            .collect::<Vec<_>>(),
    })))
}

/// 生成全部样本序列，写完后完整读回并逐位核对 (ts, bits)。
async fn seed(State(st): State<Arc<AppState>>) -> Result<Json<Value>, ApiError> {
    let specs = samples::generate();
    let mut reports = Vec::new();

    for spec in &specs {
        // 幂等：序列已存在则跳过写入，仅重新核对
        let existing = {
            let cat = st.catalog.lock().unwrap();
            cat.get_series_by_name(spec.name)?
        };
        let series_id = match existing {
            Some(row) => row.id,
            None => {
                let id = {
                    let cat = st.catalog.lock().unwrap();
                    cat.create_series(spec.name, spec.codec as i64)?
                };
                ingest(&st, id, spec.codec, &spec.points)?;
                id
            }
        };

        let lo = spec.points[0].0;
        let hi = spec.points[spec.points.len() - 1].0;
        let (read_back, stats) = query_points(&st, series_id, lo, hi)?;

        let mut mismatches = 0usize;
        if read_back.len() == spec.points.len() {
            for (a, b) in read_back.iter().zip(spec.points.iter()) {
                if a != b {
                    mismatches += 1;
                }
            }
        } else {
            mismatches = spec.points.len().max(read_back.len());
        }

        reports.push(json!({
            "series": spec.name,
            "series_id": series_id,
            "codec": spec.codec.name(),
            "points": spec.points.len(),
            "points_read_back": read_back.len(),
            "mismatches": mismatches,
            "bit_exact": mismatches == 0,
            "raw_bytes_uncompressed": spec.points.len() * 16,
            "segments": stats,
        }));
    }
    let all_ok = reports.iter().all(|r| r["bit_exact"] == json!(true));
    Ok(Json(json!({ "all_bit_exact": all_ok, "reports": reports })))
}

// ---------------------------------------------------------------------------
// 写入路径：两阶段提交（tmp 文件 -> fsync -> 原子 rename -> 目录登记 sealed）
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct IngestOutcome {
    pub segment_id: i64,
    pub bytes: usize,
}

pub fn ingest(
    st: &AppState,
    series_id: i64,
    codec: ValueCodec,
    points: &[(i64, u64)],
) -> Result<IngestOutcome, ApiError> {
    debug_assert!(!points.is_empty());
    let cat = st.catalog.lock().unwrap();

    // 显式重复规则：批次必须严格晚于该序列已封存数据的最大时间戳，
    // 跨批次重叠/重复一律 409 拒绝（批次内重复已在入口处 keep-last）。
    if let Some(max_ts) = cat.series_max_ts(series_id)? {
        if points[0].0 <= max_ts {
            return Err(ApiError::conflict(format!(
                "batch starts at {} but series already has sealed data up to {}; \
                 overlapping/duplicate timestamps across batches are rejected \
                 (within a batch, duplicates are keep-last)",
                points[0].0, max_ts
            )));
        }
    }

    let start = points[0].0;
    let end = points[points.len() - 1].0;

    // 1) 目录登记 writing —— 若此时崩溃，重启恢复会把该行标记为 aborted
    let seg_id = cat.insert_segment_writing(series_id, start, end, points.len() as i64, codec as i64)?;

    // 2) 完整编码到内存（段是不可变的，一次构建完成）
    let bytes = segment::build_segment(series_id as u64, seg_id as u64, codec, points);

    // 3) 写临时文件 + fsync + 原子 rename + fsync 目录
    let rel_path = format!("segments/{series_id}/seg_{seg_id}.seg");
    if let Err(e) = write_segment_file(&st.data_dir, &rel_path, &bytes) {
        let _ = cat.abort_segment(seg_id);
        return Err(e);
    }

    // 4) 登记为可查询。只有走到这里的段才会被查询命中。
    let file_crc = crc32fast::hash(&bytes);
    cat.seal_segment(seg_id, &rel_path, file_crc)?;
    Ok(IngestOutcome { segment_id: seg_id, bytes: bytes.len() })
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn write_segment_file(data_dir: &Path, rel_path: &str, bytes: &[u8]) -> Result<(), ApiError> {
    let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = data_dir
        .join("tmp")
        .join(format!("seg_{}_{}_{n}.tmp", std::process::id(), now_ms()));
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?; // 数据落盘后再 rename
    }
    let abs = data_dir.join(rel_path);
    if let Some(parent) = abs.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::rename(&tmp, &abs)?; // 原子发布
    if let Some(parent) = abs.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all(); // 确保持久化 rename
        }
    }
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// 查询路径：目录找段 -> 段内索引二分定位 -> 只解压重叠块
// ---------------------------------------------------------------------------

pub fn query_points(
    st: &AppState,
    series_id: i64,
    from: i64,
    to: i64,
) -> Result<(Vec<(i64, u64)>, Vec<Value>), ApiError> {
    let segments = {
        let cat = st.catalog.lock().unwrap();
        cat.overlapping_segments(series_id, from, to)?
    };
    let mut out = Vec::new();
    let mut stats = Vec::new();

    for seg in segments {
        let path = st.data_dir.join(&seg.path);
        let mut file = SegmentFile::open(&path)
            .map_err(|e| ApiError::internal(format!("segment {} unreadable: {e}", seg.id)))?;
        let index = file.read_index()?;
        // 索引二分定位：只读取与 [from, to] 重叠的块，绝不整段解压
        let start_idx = index.partition_point(|e| e.first_ts <= from).saturating_sub(1);
        let mut blocks_read = 0usize;
        let mut i = start_idx;
        while i < index.len() && index[i].first_ts <= to {
            let pts = file.read_block(&index[i], i)?;
            blocks_read += 1;
            for (ts, bits) in pts {
                if ts >= from && ts <= to {
                    out.push((ts, bits));
                }
            }
            i += 1;
        }
        stats.push(json!({
            "segment_id": seg.id,
            "blocks_total": index.len(),
            "blocks_read": blocks_read,
        }));
    }
    Ok((out, stats))
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::STATE_ABORTED;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(name: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "sensor_tsdb_api_{name}_{}_{n}",
            std::process::id()
        ));
        fs::create_dir_all(dir.join("tmp")).unwrap();
        fs::create_dir_all(dir.join("segments")).unwrap();
        dir
    }

    fn state_at(dir: &Path) -> AppState {
        let catalog = Catalog::open(&dir.join("catalog.db")).unwrap();
        AppState { catalog: Mutex::new(catalog), data_dir: dir.to_path_buf() }
    }

    #[test]
    fn ingest_then_query_bit_exact() {
        let dir = temp_dir("roundtrip");
        let st = state_at(&dir);
        let sid = {
            let cat = st.catalog.lock().unwrap();
            cat.create_series("t", ValueCodec::Xor as i64).unwrap()
        };
        let points: Vec<(i64, u64)> = (0..1_000)
            .map(|i| (i * 100, (i as f64 * 1.5).to_bits()))
            .collect();
        ingest(&st, sid, ValueCodec::Xor, &points).unwrap();

        let (back, _) = query_points(&st, sid, 0, 99_900).unwrap();
        assert_eq!(back, points, "full range must round-trip bit-exactly");

        // 一分钟窗口只应读取 1~2 个块，而不是全部 4 个块
        let (window, stats) = query_points(&st, sid, 10_000, 10_199).unwrap();
        assert_eq!(window.len(), 2);
        let blocks_read: u64 = stats[0]["blocks_read"].as_u64().unwrap();
        let blocks_total: u64 = stats[0]["blocks_total"].as_u64().unwrap();
        assert_eq!(blocks_total, 4);
        assert!(blocks_read <= 2, "range query must not decompress the whole segment");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn overlapping_batch_is_rejected() {
        let dir = temp_dir("overlap");
        let st = state_at(&dir);
        let sid = {
            let cat = st.catalog.lock().unwrap();
            cat.create_series("t", ValueCodec::Xor as i64).unwrap()
        };
        let p1: Vec<(i64, u64)> = (0..100).map(|i| (i, 0u64)).collect();
        ingest(&st, sid, ValueCodec::Xor, &p1).unwrap();

        // 与已封存数据重叠（50 <= 99）-> 409
        let p2: Vec<(i64, u64)> = (50..150).map(|i| (i, 1u64)).collect();
        let err = ingest(&st, sid, ValueCodec::Xor, &p2).unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);

        // 严格更新（从 100 开始）-> 接受
        let p3: Vec<(i64, u64)> = (100..150).map(|i| (i, 1u64)).collect();
        assert!(ingest(&st, sid, ValueCodec::Xor, &p3).is_ok());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn interrupted_tail_write_is_never_queryable() {
        let dir = temp_dir("crash");
        // 第一次「进程」：登记 writing 后崩溃（不写文件、不 seal）
        {
            let st = state_at(&dir);
            let cat = st.catalog.lock().unwrap();
            let sid = cat.create_series("t", ValueCodec::Xor as i64).unwrap();
            cat.insert_segment_writing(sid, 0, 100, 2, ValueCodec::Xor as i64)
                .unwrap();
        }
        // 第二次「进程」：启动恢复后，中断的段必须不可查询
        {
            let st = state_at(&dir);
            let cat = st.catalog.lock().unwrap();
            let segs = cat.list_segments(1).unwrap();
            assert_eq!(segs.len(), 1);
            assert_eq!(segs[0].state, STATE_ABORTED);
            assert!(cat.overlapping_segments(1, 0, 1000).unwrap().is_empty());
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn special_values_roundtrip_through_api_path() {
        let dir = temp_dir("specials");
        let st = state_at(&dir);
        let sid = {
            let cat = st.catalog.lock().unwrap();
            cat.create_series("t", ValueCodec::Xor as i64).unwrap()
        };
        let points = vec![
            (0i64, f64::NAN.to_bits()),
            (1, 0x7ff8_0000_0000_0001), // 带负载 NaN
            (2, (-0.0f64).to_bits()),
            (3, f64::INFINITY.to_bits()),
            (4, f64::NEG_INFINITY.to_bits()),
            (5, 25.5f64.to_bits()),
        ];
        ingest(&st, sid, ValueCodec::Xor, &points).unwrap();
        let (back, _) = query_points(&st, sid, 0, 5).unwrap();
        assert_eq!(back, points, "NaN/-0.0/inf must survive bit-exactly");
        let _ = fs::remove_dir_all(&dir);
    }
}
