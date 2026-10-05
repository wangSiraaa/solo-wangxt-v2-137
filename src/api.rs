//! HTTP API (JSON). All timestamps are explicit int64 Unix **nanoseconds**
//! on both the wire and disk; there is no implicit unit conversion.

use std::sync::Arc;

use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use crate::error::{Error, Result};
use crate::model::{parse_value_input, DuplicatePolicy, Sample};
use crate::store::{ReconcileReport, Store};

/// Default maximum write-batch body size (256 MiB). High-frequency batches
/// are the point of the platform; override with SENSOR_MAX_BODY_BYTES.
pub fn default_body_limit() -> usize {
    std::env::var("SENSOR_MAX_BODY_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256 * 1024 * 1024)
}

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub startup: Arc<tokio::sync::Mutex<Option<ReconcileReport>>>,
}

pub fn router(store: Arc<Store>, startup: ReconcileReport) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/series", get(list_series).post(create_series))
        .route("/v1/series/by-name/:name", get(series_by_name))
        .route("/v1/series/:id", get(get_series))
        .route("/v1/series/:id/segments", get(list_segments).post(write_segment))
        .route("/v1/series/:id/points", get(query_points))
        .route("/v1/segments/:id", get(get_segment))
        .route("/v1/segments/:id/verify", post(verify_segment))
        .route("/v1/reconcile", get(get_reconcile))
        .layer(DefaultBodyLimit::max(default_body_limit()))
        .with_state(AppState {
            store,
            startup: Arc::new(tokio::sync::Mutex::new(Some(startup))),
        })
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

#[derive(Deserialize)]
struct CreateSeriesBody {
    name: String,
    #[serde(default)]
    duplicate_policy: DuplicatePolicy,
}

async fn create_series(
    State(s): State<AppState>,
    Json(body): Json<CreateSeriesBody>,
) -> Result<(StatusCode, Json<serde_json::Value>)> {
    let name = body.name.trim().to_string();
    if name.is_empty() || name.len() > 200 {
        return Err(Error::BadRequest("series name must be 1..=200 chars".into()));
    }
    let row = s.store.create_series(&name, body.duplicate_policy).await?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::to_value(&row).unwrap()),
    ))
}

async fn list_series(State(s): State<AppState>) -> Result<Json<serde_json::Value>> {
    let rows = s.store.catalog.list_series()?;
    Ok(Json(serde_json::json!({ "series": rows })))
}

async fn get_series(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let row = s
        .store
        .catalog
        .get_series_by_id(&id)?
        .ok_or_else(|| Error::NotFound(format!("series {id}")))?;
    Ok(Json(serde_json::to_value(row).unwrap()))
}

async fn series_by_name(
    State(s): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let row = s
        .store
        .catalog
        .get_series_by_name(&name)?
        .ok_or_else(|| Error::NotFound(format!("series named {name:?}")))?;
    Ok(Json(serde_json::to_value(row).unwrap()))
}

/// Flexible sample encoding (all forms are bit-exact, no quantization):
/// `{"t": 123, "v": 3.5}`, `{"t": 123, "v": "0x400c..."}`,
/// `{"t": 123, "v": "nan"|"+inf"|"-inf"}`, `{"t":123,"bits":"..."}`,
/// or compact `[t, v]`.
#[derive(Deserialize)]
#[serde(untagged)]
enum SampleInput {
    Tup((i64, serde_json::Value)),
    Obj {
        t: i64,
        v: Option<serde_json::Value>,
        bits: Option<String>,
    },
}

impl SampleInput {
    fn into_sample(self) -> Result<Sample> {
        match self {
            SampleInput::Tup((t, v)) => Ok(Sample::from_bits(
                t,
                parse_value_input(&v).map_err(Error::BadRequest)?,
            )),
            SampleInput::Obj { t, v, bits } => {
                let b = if let Some(hex) = bits {
                    parse_value_input(&serde_json::Value::String(hex))
                        .map_err(Error::BadRequest)?
                } else if let Some(v) = v {
                    parse_value_input(&v).map_err(Error::BadRequest)?
                } else {
                    return Err(Error::BadRequest(
                        "sample requires \"v\" or \"bits\"".into(),
                    ));
                };
                Ok(Sample::from_bits(t, b))
            }
        }
    }
}

#[derive(Deserialize)]
struct WriteBody {
    samples: Vec<SampleInput>,
}

async fn write_segment(
    State(s): State<AppState>,
    Path(id): Path<String>,
    raw_body: axum::body::Bytes,
) -> Result<(StatusCode, Json<serde_json::Value>)> {
    let body: WriteBody = serde_json::from_slice(&raw_body).map_err(|e| {
        Error::BadRequest(format!("invalid write body: {e}"))
    })?;
    let series = s
        .store
        .catalog
        .get_series_by_id(&id)?
        .ok_or_else(|| Error::NotFound(format!("series {id}")))?;
    let mut samples = Vec::with_capacity(body.samples.len());
    for (i, inp) in body.samples.into_iter().enumerate() {
        samples.push(inp.into_sample().map_err(|e| match e {
            Error::BadRequest(msg) => Error::BadRequest(format!("samples[{i}]: {msg}")),
            other => other,
        })?);
    }
    let outcome = s.store.write_segment(&series, samples).await?;
    Ok((StatusCode::CREATED, Json(serde_json::to_value(outcome).unwrap())))
}

#[derive(Deserialize)]
struct RangeParams {
    from: i64,
    to: i64,
    #[serde(default = "default_limit")]
    limit: u64,
    #[serde(default)]
    offset: u64,
}

fn default_limit() -> u64 {
    10_000
}

const MAX_LIMIT: u64 = 1_000_000;

async fn query_points(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(p): Query<RangeParams>,
) -> Result<Json<serde_json::Value>> {
    if p.limit == 0 || p.limit > MAX_LIMIT {
        return Err(Error::BadRequest(format!(
            "limit must be in 1..={MAX_LIMIT}"
        )));
    }
    // Must exist so an unknown series is 404, not silently empty.
    if s.store.catalog.get_series_by_id(&id)?.is_none() {
        return Err(Error::NotFound(format!("series {id}")));
    }
    let report = s
        .store
        .query(&id, p.from, p.to, p.limit as usize, p.offset)
        .await?;
    Ok(Json(serde_json::to_value(report).unwrap()))
}

async fn list_segments(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>> {
    if s.store.catalog.get_series_by_id(&id)?.is_none() {
        return Err(Error::NotFound(format!("series {id}")));
    }
    let rows = s.store.list_segments(&id).await?;
    Ok(Json(serde_json::json!({ "segments": rows })))
}

async fn get_segment(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let row = s
        .store
        .catalog
        .get_segment(&id)?
        .ok_or_else(|| Error::NotFound(format!("segment {id}")))?;
    Ok(Json(serde_json::to_value(row).unwrap()))
}

async fn verify_segment(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>> {
    Ok(Json(s.store.verify_segment(&id).await?))
}

async fn get_reconcile(State(s): State<AppState>) -> Result<Json<serde_json::Value>> {
    let report = s.store.reconcile()?;
    *s.startup.lock().await = Some(report.clone());
    Ok(Json(serde_json::json!({ "reconcile": report })))
}
