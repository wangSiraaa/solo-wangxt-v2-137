//! Service layer: orchestrates catalog + immutable segments.
//!
//! Write ordering per series is serialized by an in-process keyed mutex:
//! only one seal+register sequence can run for a series at a time, so the
//! catalog gap check cannot race with a concurrent writer. File sealing is
//! blocking (fsync) and runs on the blocking thread pool.
//!
//! Registration order is deliberately: **file sealed & fsynced first,
//! catalog row second**. If the process dies in between, startup reconcile
//! finds an orphan `.seg` and refuses to silently guess — it reports the
//! orphan and leaves the file in place. A torn `.tmp` tail, by contrast, is
//! always reaped, since nothing could ever have made it queryable.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::Mutex as AsyncMutex;

use crate::catalog::{now_ns, Catalog, SegmentRow, SeriesRow};
use crate::error::{Error, IoCtx, Result};
use crate::model::{DuplicatePolicy, Sample};
use crate::segment::{
    reap_temp_segments, SealedSegment, SegmentReader, SegmentWriter,
};

pub struct Store {
    pub catalog: Catalog,
    data_dir: PathBuf,
    write_locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OrphanSegment {
    pub path: String,
    pub series_id: String,
    pub min_t: i64,
    pub max_t: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReconcileReport {
    pub temp_reaped: Vec<String>,
    pub orphans: Vec<OrphanSegment>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct WriteOutcome {
    pub segment_id: String,
    pub series_id: String,
    pub samples_written: u64,
    pub samples_submitted: u64,
    pub duplicates_dropped: u64,
    pub min_t: i64,
    pub max_t: i64,
    pub block_count: u32,
    pub file: String,
    pub file_len: u64,
    pub payload_crc32c: u32,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryPoint {
    pub t: i64,
    #[serde(flatten)]
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct QueriedSegment {
    pub segment_id: String,
    pub min_t: i64,
    pub max_t: i64,
    pub block_count: usize,
    pub blocks_touched: Vec<usize>,
    pub samples: Vec<QueryPoint>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryReport {
    pub series_id: String,
    pub from: i64,
    pub to: i64,
    pub segments: Vec<QueriedSegment>,
    pub total_samples: usize,
    pub has_more: bool,
    pub offset: u64,
    pub limit: usize,
}

impl Store {
    pub fn new(catalog: Catalog, data_dir: PathBuf) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| Error::io(e, IoCtx::new("create data dir", data_dir.display())))?;
        Ok(Arc::new(Self {
            catalog,
            data_dir,
            write_locks: Mutex::new(HashMap::new()),
        }))
    }

    fn lock_for(&self, series_id: &str) -> Arc<AsyncMutex<()>> {
        let mut map = self.write_locks.lock().unwrap();
        map.entry(series_id.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    fn series_dir(&self, series_id: &str) -> PathBuf {
        self.data_dir.join(series_id)
    }

    /// Startup reconciliation: reap torn `.tmp` tails; surface sealed `.seg`
    /// files that have no catalog row as orphans (never auto-register: the
    /// row is the source of queryability).
    pub fn reconcile(&self) -> Result<ReconcileReport> {
        let mut temp_reaped = Vec::new();
        let mut orphans = Vec::new();
        if !self.data_dir.exists() {
            return Ok(ReconcileReport { temp_reaped, orphans });
        }
        for entry in std::fs::read_dir(&self.data_dir)
            .map_err(|e| Error::io(e, IoCtx::new("read data dir", self.data_dir.display())))?
        {
            let entry = entry.map_err(|e| {
                Error::io(e, IoCtx::new("read data dir entry", self.data_dir.display()))
            })?;
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let dir = entry.path();
            for reaped in reap_temp_segments(&dir)
                .map_err(|e| Error::io(e, IoCtx::new("reap temp segments", dir.display())))?
            {
                temp_reaped.push(reaped.display().to_string());
            }
            for f in std::fs::read_dir(&dir)
                .map_err(|e| Error::io(e, IoCtx::new("read series dir", dir.display())))?
            {
                let f = f.map_err(|e| {
                    Error::io(e, IoCtx::new("read segment entry", dir.display()))
                })?;
                let p = f.path();
                if p.extension().and_then(|x| x.to_str()) != Some("seg") {
                    continue;
                }
                let stem = p
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or_default()
                    .to_string();
                if self.catalog.get_segment(&stem)?.is_none() {
                    let info = SegmentReader::open(&p)
                        .map(|r| (r.series_id.clone(), r.min_t, r.max_t))
                        .ok();
                    orphans.push(OrphanSegment {
                        path: p.display().to_string(),
                        series_id: info
                            .as_ref()
                            .map(|x| x.0.clone())
                            .unwrap_or_else(|| {
                                dir.file_name()
                                    .and_then(|n| n.to_str())
                                    .unwrap_or("?")
                                    .to_string()
                            }),
                        min_t: info.as_ref().map(|x| x.1).unwrap_or(0),
                        max_t: info.as_ref().map(|x| x.2).unwrap_or(0),
                    });
                }
            }
        }
        Ok(ReconcileReport { temp_reaped, orphans })
    }

    pub async fn create_series(
        self: &Arc<Self>,
        name: &str,
        policy: DuplicatePolicy,
    ) -> Result<SeriesRow> {
        let id = crate::catalog::new_id()?;
        let dir = self.series_dir(&id);
        let name = name.to_string();
        let self2 = self.clone();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&dir)
                .map_err(|e| Error::io(e, IoCtx::new("create series dir", dir.display())))?;
            self2.catalog.create_series(&id, &name, policy, now_ns())
        })
        .await
        .map_err(|e| Error::BadRequest(format!("join error: {e}")))?
    }

    /// Validate, sort-independent append of one batch into a fresh immutable
    /// segment. All ordering/duplicate rules apply to the batch as a whole.
    pub async fn write_segment(
        self: &Arc<Self>,
        series: &SeriesRow,
        mut samples: Vec<Sample>,
    ) -> Result<WriteOutcome> {
        if samples.is_empty() {
            return Err(Error::BadRequest("samples is empty".into()));
        }
        // Timestamps must arrive non-decreasing after stable sort; document:
        // we do NOT reorder client data — fail instead. (Stable-sorting would
        // hide producer bugs and change duplicate-arrival semantics.)
        for w in samples.windows(2) {
            if w[1].t < w[0].t {
                return Err(Error::OutOfOrder {
                    t: w[1].t,
                    previous: w[0].t,
                });
            }
        }
        let submitted = samples.len() as u64;
        let series_id = series.id.clone();
        let lock = self.lock_for(&series_id);
        let _guard = lock.lock().await;
        self.write_segment_locked(series, &mut samples, submitted).await
    }

    async fn write_segment_locked(
        self: &Arc<Self>,
        series: &SeriesRow,
        samples: &mut Vec<Sample>,
        submitted: u64,
    ) -> Result<WriteOutcome> {
        let series_id = series.id.clone();
        // Gap check happens against committed data *before* touching disk.
        if let Some(prev_end) = self.catalog.last_range_end(&series_id)? {
            if samples[0].t <= prev_end {
                return Err(Error::Conflict(format!(
                    "first sample t={} is not after previous range end t={prev_end}",
                    samples[0].t
                )));
            }
        }
        let segment_id = crate::catalog::new_id()?;
        let dir = self.series_dir(&series_id);
        let policy = series.duplicate_policy;
        let sid = series_id.clone();
        let seg_id = segment_id.clone();
        let taken = std::mem::take(samples);

        let sealed: std::result::Result<SealedSegment, (Error, Option<PathBuf>)> = {
            let dir2 = dir.clone();
            let sid2 = sid.clone();
            let seg2 = seg_id.clone();
            tokio::task::spawn_blocking(move || -> std::result::Result<SealedSegment, Error> {
                let mut w = SegmentWriter::create(&dir2, &seg2, &sid2, policy)?;
                w.append(&taken)?;
                w.seal()
            })
            .await
            .map_err(|e| Error::BadRequest(format!("join error: {e}")))?
            .map_err(|e| (e, Some(dir.join(format!("{seg_id}.seg")))))
        };

        let sealed = match sealed {
            Ok(s) => s,
            Err((e, path)) => {
                // Best-effort removal of a fully renamed file when a later
                // step fails; a .tmp is already removed by the writer's Drop.
                if let Some(p) = path {
                    let _ = std::fs::remove_file(p);
                }
                return Err(e);
            }
        };

        // Re-open to verify what we are about to register is readable and to
        // read back exact stats (defense in depth).
        let verify = {
            let p = sealed.path.clone();
            tokio::task::spawn_blocking(move || SegmentReader::open(&p))
                .await
                .map_err(|e| Error::BadRequest(format!("join error: {e}")))?
        }?;
        let block_count = verify.block_count() as i64;
        let min_t = verify.min_t;
        let max_t = verify.max_t;
        let total = verify.total;
        let payload_bytes = verify.payload_bytes as i64;
        let file_len = sealed.file_len as i64;
        let path_str = sealed.path.display().to_string();
        // XOR-fold of per-block CRC32C values as a coarse segment-level
        // marker; the authoritative checks are the per-block payload CRCs.
        let payload_crc32c = verify.blocks.iter().fold(0u32, |fold, b| fold ^ b.payload_crc);

        let row = SegmentRow {
            id: segment_id.clone(),
            series_id: series_id.clone(),
            path: path_str.clone(),
            min_t,
            max_t,
            count: total as i64,
            file_len,
            payload_bytes,
            block_count,
            sealed_unix_ns: now_ns(),
        };
        // Final authoritative gap check + row insert, in a DB transaction.
        if let Err(e) = self.catalog.register_segment(&row) {
            let _ = std::fs::remove_file(&sealed.path);
            return Err(e);
        }

        Ok(WriteOutcome {
            segment_id,
            series_id,
            samples_written: total,
            samples_submitted: submitted,
            duplicates_dropped: submitted - total,
            min_t,
            max_t,
            block_count: block_count as u32,
            file: path_str,
            file_len: sealed.file_len,
            payload_crc32c,
        })
    }

    /// Range query: catalog index narrows to overlapping segments; each
    /// segment's block index narrows further. No untouched segment or block
    /// is read, checksummed or decoded.
    pub async fn query(
        self: &Arc<Self>,
        series_id: &str,
        from: i64,
        to: i64,
        limit: usize,
        offset: u64,
    ) -> Result<QueryReport> {
        if to < from {
            return Err(Error::BadRequest(format!(
                "`to` ({to}) must be >= `from` ({from})"
            )));
        }
        let rows = self.catalog.segments_for_range(series_id, from, to)?;
        let mut report = QueryReport {
            series_id: series_id.to_string(),
            from,
            to,
            segments: Vec::new(),
            total_samples: 0,
            has_more: false,
            offset,
            limit,
        };
        let mut skip_remaining = offset;
        let mut take_remaining = limit;
        let mut total_matched: u64 = 0;

        for (idx, row) in rows.iter().enumerate() {
            let path = PathBuf::from(&row.path);
            let (samples, matched, touched, nblocks) =
                tokio::task::spawn_blocking(move || -> Result<_> {
                    let mut r = SegmentReader::open(&path)?;
                    let touched: Vec<usize> = r
                        .blocks
                        .iter()
                        .enumerate()
                        .filter(|(_, b)| b.t_last >= from && b.t0 <= to)
                        .map(|(i, _)| i)
                        .collect();
                    let (samples, matched) =
                        r.query(Some(from), Some(to), take_remaining, skip_remaining)?;
                    Ok((samples, matched, touched, r.blocks.len()))
                })
                .await
                .map_err(|e| Error::BadRequest(format!("join error: {e}")))??;

            total_matched += matched;
            let n = samples.len();
            // Offset is measured across segments in time order.
            skip_remaining -= skip_remaining.min(matched);
            if !samples.is_empty() {
                let points = samples
                    .iter()
                    .map(|s| QueryPoint {
                        t: s.t,
                        value: crate::model::value_json(s.bits),
                    })
                    .collect();
                report.segments.push(QueriedSegment {
                    segment_id: row.id.clone(),
                    min_t: row.min_t,
                    max_t: row.max_t,
                    block_count: nblocks,
                    blocks_touched: touched,
                    samples: points,
                });
            }
            report.total_samples += n;
            take_remaining -= n;
            if take_remaining == 0 {
                // More exists if a later catalog-selected segment remains,
                // or this segment had matched rows beyond the page.
                if idx + 1 < rows.len() || total_matched > offset + limit as u64 {
                    report.has_more = true;
                }
                break;
            }
        }
        Ok(report)
    }

    /// Explicit full verification of one segment: every block CRC + decode.
    pub async fn verify_segment(self: &Arc<Self>, segment_id: &str) -> Result<serde_json::Value> {
        let row = self
            .catalog
            .get_segment(segment_id)?
            .ok_or_else(|| Error::NotFound(format!("segment {segment_id}")))?;
        let path = PathBuf::from(&row.path);
        tokio::task::spawn_blocking(move || {
            let mut r = SegmentReader::open(&path)?;
            let blocks = r
                .blocks
                .iter()
                .map(|b| {
                    serde_json::json!({
                        "payload_start": b.payload_start,
                        "payload_end": b.payload_end(),
                        "count": b.count,
                        "t0": b.t0,
                        "t_last": b.t_last,
                        "payload_crc32c": format!("{:08x}", b.payload_crc),
                    })
                })
                .collect::<Vec<_>>();
            let verified_counts = r.deep_verify()?;
            Ok(serde_json::json!({
                "segment_id": row.id,
                "file": row.path,
                "file_len": r.file_len,
                "footer_start": r.footer_start(),
                "blocks": blocks,
                "verified_sample_counts": verified_counts,
                "status": "ok: every block payload CRC32C verified and bit-exact decoded",
            }))
        })
        .await
        .map_err(|e| Error::BadRequest(format!("join error: {e}")))?
    }

    pub async fn list_segments(self: &Arc<Self>, series_id: &str) -> Result<Vec<SegmentRow>> {
        self.catalog.list_segments(series_id)
    }
}
