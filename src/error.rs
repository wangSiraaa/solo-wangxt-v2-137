//! Error type shared by storage, catalog and HTTP layers.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// Operation context attached to an IO error so failure messages name the
/// affected file (important when locating corruption).
#[derive(Debug)]
pub struct IoCtx {
    pub what: String,
    pub path: String,
}

impl IoCtx {
    pub fn new(what: impl std::fmt::Display, path: impl std::fmt::Display) -> Self {
        Self { what: what.to_string(), path: path.to_string() }
    }
}

impl std::fmt::Display for IoCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.what, self.path)
    }
}

#[derive(Debug)]
pub enum Error {
    /// A segment file failed a checksum or structure check. The structured
    /// payload identifies the byte range(s) that are corrupt.
    Corruption(super::segment::Corruption),
    /// Segment is not sealed / not present in the catalog.
    NotQueryable(String),
    /// Duplicate timestamp rejected by the `reject` policy.
    DuplicateTimestamp { series: String, t: i64 },
    /// Sample timestamps went backwards; streams are required to be
    /// non-decreasing inside one write stream.
    OutOfOrder { t: i64, previous: i64 },
    /// A JSON value could not be turned into an IEEE-754 double without
    /// silently quantizing it (integer with >53 bits of mantissa).
    UnrepresentableNumber(String),
    /// Malformed hex bit pattern / special-value token / request body.
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    Sql(rusqlite::Error),
    Io {
        source: std::io::Error,
        ctx: IoCtx,
    },
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::Sql(e)
    }
}

impl Error {
    pub fn io(source: std::io::Error, ctx: impl Into<IoCtx>) -> Self {
        Error::Io { source, ctx: ctx.into() }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Corruption(c) => write!(f, "corruption in {}: {}", c.path, c.summary()),
            Error::NotQueryable(s) => write!(f, "segment not queryable: {s}"),
            Error::DuplicateTimestamp { series, t } => {
                write!(f, "duplicate timestamp {t} in series {series}")
            }
            Error::OutOfOrder { t, previous } => write!(
                f,
                "timestamps must be non-decreasing; got {t} after {previous}"
            ),
            Error::UnrepresentableNumber(s) => write!(
                f,
                "number {s} cannot be represented exactly as f64; send it as 0x<16 hex bits> instead"
            ),
            Error::BadRequest(s) => write!(f, "bad request: {s}"),
            Error::NotFound(s) => write!(f, "not found: {s}"),
            Error::Conflict(s) => write!(f, "conflict: {s}"),
            Error::Sql(e) => write!(f, "sqlite error: {e}"),
            Error::Io { source, ctx } => write!(f, "io error {ctx}: {source}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Sql(e) => Some(e),
            Error::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            Error::Corruption(c) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "segment_corrupt",
                format!("corruption in {}: {}", c.path, c.summary()),
            ),
            Error::NotQueryable(_) => (StatusCode::CONFLICT, "not_queryable", self.to_string()),
            Error::DuplicateTimestamp { .. } => {
                (StatusCode::CONFLICT, "duplicate_timestamp", self.to_string())
            }
            Error::OutOfOrder { .. } => (StatusCode::BAD_REQUEST, "out_of_order", self.to_string()),
            Error::UnrepresentableNumber(_) => (
                StatusCode::BAD_REQUEST,
                "unrepresentable_number",
                self.to_string(),
            ),
            Error::BadRequest(_) => (StatusCode::BAD_REQUEST, "bad_request", self.to_string()),
            Error::NotFound(_) => (StatusCode::NOT_FOUND, "not_found", self.to_string()),
            Error::Conflict(_) => (StatusCode::CONFLICT, "conflict", self.to_string()),
            Error::Sql(e) => {
                let msg = e.to_string();
                if msg.contains("UNIQUE constraint failed") {
                    (StatusCode::CONFLICT, "conflict", self.to_string())
                } else {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "catalog_error",
                        self.to_string(),
                    )
                }
            }
            Error::Io { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "io_error",
                self.to_string(),
            ),
        };
        let mut body = json!({ "error": { "code": code, "message": message } });
        if let Error::Corruption(c) = &self {
            body["error"]["corruption"] = serde_json::to_value(c).unwrap_or(json!(null));
        }
        (status, Json(body)).into_response()
    }
}
