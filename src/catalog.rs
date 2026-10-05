//! SQLite-backed catalog: series directory and the segment index.
//!
//! The catalog stores *metadata only*; sample bytes live in immutable
//! `.seg` files. Segment rows are inserted only after the file has been
//! sealed and fsynced (see [`crate::segment::SegmentWriter::seal`]), inside a
//! transaction that also enforces the strict no-overlap invariant:
//!
//! > for one series, segment `[min_t, max_t]` ranges are pairwise disjoint
//! > and ordered: every new segment has `min_t > previous max_t`.
//!
//! Because ranges cannot touch, duplicate timestamps can only exist *inside*
//! one segment, where the series' duplicate policy governs them.

use std::path::Path;

use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;

use crate::error::{Error, Result};
use crate::model::DuplicatePolicy;

pub const CATALOG_VERSION: &str = "1";

#[derive(Debug, Clone, Serialize)]
pub struct SeriesRow {
    pub id: String,
    pub name: String,
    pub duplicate_policy: DuplicatePolicy,
    pub created_unix_ns: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SegmentRow {
    pub id: String,
    pub series_id: String,
    pub path: String,
    pub min_t: i64,
    pub max_t: i64,
    pub count: i64,
    pub file_len: i64,
    pub payload_bytes: i64,
    pub block_count: i64,
    pub sealed_unix_ns: i64,
}

pub struct Catalog {
    conn: std::sync::Mutex<Connection>,
}

impl Catalog {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "busy_timeout", 10_000)?;
        conn.execute_batch(SCHEMA)?;
        let version: String = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or_else(|| CATALOG_VERSION.to_string());
        if version != CATALOG_VERSION {
            return Err(Error::Conflict(format!(
                "catalog schema version {version} unsupported (want {CATALOG_VERSION})"
            )));
        }
        Ok(Self { conn: std::sync::Mutex::new(conn) })
    }

    /// In-memory catalog (tests / ephemeral runs).
    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn: std::sync::Mutex::new(conn) })
    }

    pub fn create_series(&self, id: &str, name: &str, policy: DuplicatePolicy, now_ns: i64) -> Result<SeriesRow> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO series (id, name, duplicate_policy, created_unix_ns) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![id, name, policy.as_str(), now_ns],
        )?;
        Ok(SeriesRow {
            id: id.to_string(),
            name: name.to_string(),
            duplicate_policy: policy,
            created_unix_ns: now_ns,
        })
    }

    pub fn get_series_by_name(&self, name: &str) -> Result<Option<SeriesRow>> {
        let conn = self.conn.lock().unwrap();
        Self::query_series(&conn, "name", name)
    }

    pub fn get_series_by_id(&self, id: &str) -> Result<Option<SeriesRow>> {
        let conn = self.conn.lock().unwrap();
        Self::query_series(&conn, "id", id)
    }

    fn query_series(conn: &Connection, col: &str, val: &str) -> Result<Option<SeriesRow>> {
        let sql = format!(
            "SELECT id, name, duplicate_policy, created_unix_ns FROM series WHERE {col} = ?1"
        );
        conn.query_row(&sql, rusqlite::params![val], |r| {
            let policy: String = r.get(2)?;
            Ok(SeriesRow {
                id: r.get(0)?,
                name: r.get(1)?,
                duplicate_policy: DuplicatePolicy::parse(&policy)
                    .ok_or_else(|| rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("bad policy in catalog: {policy}"),
                        )),
                    ))?,
                created_unix_ns: r.get(3)?,
            })
        })
        .optional()
        .map_err(Error::from)
    }

    pub fn list_series(&self) -> Result<Vec<SeriesRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, duplicate_policy, created_unix_ns FROM series ORDER BY name",
        )?;
        let rows = stmt.query_map([], |r| {
            let policy: String = r.get(2)?;
            Ok(SeriesRow {
                id: r.get(0)?,
                name: r.get(1)?,
                duplicate_policy: DuplicatePolicy::parse(&policy).unwrap_or(DuplicatePolicy::KeepAll),
                created_unix_ns: r.get(3)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Largest max_t of any sealed segment for the series, or None if the
    /// series has no segments yet.
    pub fn last_range_end(&self, series_id: &str) -> Result<Option<i64>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT MAX(max_t) FROM segments WHERE series_id = ?1",
                rusqlite::params![series_id],
                |r| r.get::<_, Option<i64>>(0),
            )
            .optional()?
            .flatten())
    }

    /// Register a freshly sealed segment. The gap check and insert run in one
    /// immediate transaction: a segment whose first timestamp does not
    /// strictly exceed the previous range end is refused and never queryable.
    pub fn register_segment(&self, row: &SegmentRow) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let prev_end: Option<i64> =
            tx.query_row(
                "SELECT MAX(max_t) FROM segments WHERE series_id = ?1",
                rusqlite::params![row.series_id],
                |r| r.get(0),
            )?;
        if let Some(prev_end) = prev_end {
            if row.min_t <= prev_end {
                return Err(Error::Conflict(format!(
                    "new segment for series {} starts at t={} but catalog already covers through t={prev_end}; \
                     segment ranges must be strictly increasing with no overlap",
                    row.series_id, row.min_t
                )));
            }
        }
        tx.execute(
            "INSERT INTO segments \
             (id, series_id, path, min_t, max_t, start_exclusive, count, file_len, \
              payload_bytes, block_count, sealed_unix_ns) \
             VALUES (?1,?2,?3,?4,?5, COALESCE((SELECT MAX(max_t) FROM segments WHERE series_id = ?2), ?9), \
                     ?6,?7,?8,?10,?11)",
            rusqlite::params![
                row.id,
                row.series_id,
                row.path,
                row.min_t,
                row.max_t,
                row.count,
                row.file_len,
                row.payload_bytes,
                i64::MIN,
                row.block_count,
                row.sealed_unix_ns,
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_segment(&self, id: &str) -> Result<Option<SegmentRow>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id, series_id, path, min_t, max_t, count, file_len, \
                    payload_bytes, block_count, sealed_unix_ns \
             FROM segments WHERE id = ?1",
            rusqlite::params![id],
            map_segment,
        )
        .optional()
        .map_err(Error::from)
    }

    /// Segments overlapping `[from, to]` for a series, in time order. Only
    /// these rows are returned; the query layer opens exactly these files.
    pub fn segments_for_range(
        &self,
        series_id: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<SegmentRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, series_id, path, min_t, max_t, count, file_len, \
                    payload_bytes, block_count, sealed_unix_ns \
             FROM segments \
             WHERE series_id = ?1 AND max_t >= ?2 AND min_t <= ?3 \
             ORDER BY min_t ASC",
        )?;
        let rows = stmt.query_map(rusqlite::params![series_id, from, to], map_segment)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_segments(&self, series_id: &str) -> Result<Vec<SegmentRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, series_id, path, min_t, max_t, count, file_len, \
                    payload_bytes, block_count, sealed_unix_ns \
             FROM segments WHERE series_id = ?1 ORDER BY min_t ASC",
        )?;
        let rows = stmt.query_map(rusqlite::params![series_id], map_segment)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

fn map_segment(r: &rusqlite::Row<'_>) -> rusqlite::Result<SegmentRow> {
    Ok(SegmentRow {
        id: r.get(0)?,
        series_id: r.get(1)?,
        path: r.get(2)?,
        min_t: r.get(3)?,
        max_t: r.get(4)?,
        count: r.get(5)?,
        file_len: r.get(6)?,
        payload_bytes: r.get(7)?,
        block_count: r.get(8)?,
        sealed_unix_ns: r.get(9)?,
    })
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', '1');

CREATE TABLE IF NOT EXISTS series (
  id               TEXT PRIMARY KEY,
  name             TEXT NOT NULL UNIQUE,
  duplicate_policy TEXT NOT NULL CHECK (duplicate_policy IN
                       ('keep_all','keep_first','keep_last','reject')),
  created_unix_ns  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS segments (
  id               TEXT PRIMARY KEY,
  series_id        TEXT NOT NULL REFERENCES series(id),
  path             TEXT NOT NULL,
  min_t            INTEGER NOT NULL,
  max_t            INTEGER NOT NULL,
  start_exclusive  INTEGER NOT NULL,
  count            INTEGER NOT NULL CHECK (count > 0),
  file_len         INTEGER NOT NULL,
  payload_bytes    INTEGER NOT NULL,
  block_count      INTEGER NOT NULL CHECK (block_count > 0),
  sealed_unix_ns   INTEGER NOT NULL,
  UNIQUE (series_id, min_t)
);
CREATE INDEX IF NOT EXISTS idx_segments_series_time
  ON segments(series_id, min_t, max_t);
"#;

/// 32-hex-char identifier: 8 bytes millisecond timestamp prefix + 8 random
/// bytes from /dev/urandom (no CSPRNG dependency). Monotonic-ish prefix sorts
/// ids by creation time as well.
pub fn new_id() -> Result<String> {
    use std::io::Read;
    let mut rnd = [0u8; 8];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut rnd))
        .map_err(|e| Error::io(e, crate::error::IoCtx::new("read /dev/urandom", "/dev/urandom")))?;
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    Ok(format!("{ms:016x}{:016x}", u64::from_be_bytes(rnd)))
}

pub fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_crud_and_overlap() {
        let cat = Catalog::open_memory().unwrap();
        cat.create_series("a".repeat(32).as_str(), "temp", DuplicatePolicy::KeepAll, 1)
            .unwrap();
        assert!(cat
            .create_series("b".repeat(32).as_str(), "temp", DuplicatePolicy::KeepAll, 2)
            .is_err());
        let sid = "a".repeat(32);

        let mk = |id: &str, lo: i64, hi: i64| SegmentRow {
            id: id.to_string(),
            series_id: sid.clone(),
            path: format!("/tmp/{id}.seg"),
            min_t: lo,
            max_t: hi,
            count: 1,
            file_len: 100,
            payload_bytes: 10,
            block_count: 1,
            sealed_unix_ns: 1,
        };
        cat.register_segment(&mk("11111111111111111111111111111111", 10, 20)).unwrap();
        // Overlap rejected.
        assert!(matches!(
            cat.register_segment(&mk("22222222222222222222222222222222", 20, 30)),
            Err(Error::Conflict(_))
        ));
        // Gap allowed.
        cat.register_segment(&mk("33333333333333333333333333333333", 21, 40)).unwrap();
        let hit = cat.segments_for_range(&sid, 15, 22).unwrap();
        assert_eq!(hit.len(), 2);
        assert_eq!(hit[0].min_t, 10);
        assert_eq!(hit[1].min_t, 21);
        let none = cat.segments_for_range(&sid, 100, 200).unwrap();
        assert!(none.is_empty());
    }
}
