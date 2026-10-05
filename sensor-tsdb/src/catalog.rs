//! SQLite 序列目录：series 表 + segments 表。
//!
//! 段状态机：
//!   writing  —— 已登记、文件尚未完整落盘（查询不可见）
//!   sealed   —— 文件已 fsync + 原子 rename，可查询
//!   aborted  —— 写入被中断（启动恢复时由 writing 转换而来），永不查询
//!   corrupt  —— 校验失败，查询不再命中
//!
//! 查询只认 state = 'sealed'，因此尾部写入中断不会产生可查询的半成品段。

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};

pub const STATE_WRITING: &str = "writing";
pub const STATE_SEALED: &str = "sealed";
pub const STATE_ABORTED: &str = "aborted";
pub const STATE_CORRUPT: &str = "corrupt";

#[derive(Clone, Debug)]
pub struct SeriesRow {
    pub id: i64,
    pub name: String,
    pub codec: i64,
    pub created_at: i64,
}

#[derive(Clone, Debug)]
pub struct SegmentRow {
    pub id: i64,
    pub series_id: i64,
    pub start_ts: i64,
    pub end_ts: i64,
    pub count: i64,
    pub codec: i64,
    pub path: String,
    pub state: String,
    pub file_crc: i64,
    pub created_at: i64,
}

pub struct Catalog {
    conn: Connection,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

const SEGMENT_COLS: &str = "id, series_id, start_ts, end_ts, count, codec, path, state, file_crc, created_at";

fn segment_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<SegmentRow> {
    Ok(SegmentRow {
        id: r.get(0)?,
        series_id: r.get(1)?,
        start_ts: r.get(2)?,
        end_ts: r.get(3)?,
        count: r.get(4)?,
        codec: r.get(5)?,
        path: r.get(6)?,
        state: r.get(7)?,
        file_crc: r.get(8)?,
        created_at: r.get(9)?,
    })
}

impl Catalog {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS series (
                 id INTEGER PRIMARY KEY,
                 name TEXT NOT NULL UNIQUE,
                 codec INTEGER NOT NULL DEFAULT 1,
                 created_at INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS segments (
                 id INTEGER PRIMARY KEY,
                 series_id INTEGER NOT NULL REFERENCES series(id),
                 start_ts INTEGER NOT NULL,
                 end_ts INTEGER NOT NULL,
                 count INTEGER NOT NULL,
                 codec INTEGER NOT NULL,
                 path TEXT NOT NULL DEFAULT '',
                 state TEXT NOT NULL,
                 file_crc INTEGER NOT NULL DEFAULT 0,
                 created_at INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_segments_query
                 ON segments(series_id, state, start_ts, end_ts);",
        )?;
        let cat = Self { conn };
        cat.recover()?;
        Ok(cat)
    }

    /// 崩溃恢复：上次退出时仍处于 writing 的段来自被中断的尾部写入，
    /// 一律标记为 aborted —— 永远不会被查询命中（查询只认 sealed）。
    fn recover(&self) -> rusqlite::Result<()> {
        self.conn.execute(
            "UPDATE segments SET state = ?1 WHERE state = ?2",
            params![STATE_ABORTED, STATE_WRITING],
        )?;
        Ok(())
    }

    // ---------------- series ----------------

    pub fn create_series(&self, name: &str, codec: i64) -> rusqlite::Result<i64> {
        self.conn.execute(
            "INSERT INTO series(name, codec, created_at) VALUES (?1, ?2, ?3)",
            params![name, codec, now_ms()],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn get_series(&self, id: i64) -> rusqlite::Result<Option<SeriesRow>> {
        self.conn
            .query_row(
                "SELECT id, name, codec, created_at FROM series WHERE id = ?1",
                params![id],
                |r| {
                    Ok(SeriesRow {
                        id: r.get(0)?,
                        name: r.get(1)?,
                        codec: r.get(2)?,
                        created_at: r.get(3)?,
                    })
                },
            )
            .optional()
    }

    pub fn get_series_by_name(&self, name: &str) -> rusqlite::Result<Option<SeriesRow>> {
        self.conn
            .query_row(
                "SELECT id, name, codec, created_at FROM series WHERE name = ?1",
                params![name],
                |r| {
                    Ok(SeriesRow {
                        id: r.get(0)?,
                        name: r.get(1)?,
                        codec: r.get(2)?,
                        created_at: r.get(3)?,
                    })
                },
            )
            .optional()
    }

    pub fn list_series(&self) -> rusqlite::Result<Vec<SeriesRow>> {
        let mut st = self
            .conn
            .prepare("SELECT id, name, codec, created_at FROM series ORDER BY id")?;
        let rows = st
            .query_map([], |r| {
                Ok(SeriesRow {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    codec: r.get(2)?,
                    created_at: r.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ---------------- segments ----------------

    /// 已封存数据的最大时间戳（用于跨批次重叠拒绝）。
    pub fn series_max_ts(&self, series_id: i64) -> rusqlite::Result<Option<i64>> {
        let mut st = self.conn.prepare(
            "SELECT MAX(end_ts) FROM segments WHERE series_id = ?1 AND state = ?2",
        )?;
        st.query_row(params![series_id, STATE_SEALED], |r| r.get::<_, Option<i64>>(0))
    }

    pub fn insert_segment_writing(
        &self,
        series_id: i64,
        start_ts: i64,
        end_ts: i64,
        count: i64,
        codec: i64,
    ) -> rusqlite::Result<i64> {
        self.conn.execute(
            "INSERT INTO segments(series_id, start_ts, end_ts, count, codec, path, state, file_crc, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, '', ?6, 0, ?7)",
            params![series_id, start_ts, end_ts, count, codec, STATE_WRITING, now_ms()],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// 文件完整落盘后登记为可查询。只有 sealed 的段会被查询命中。
    pub fn seal_segment(&self, id: i64, path: &str, file_crc: u32) -> rusqlite::Result<usize> {
        self.conn.execute(
            "UPDATE segments SET state = ?1, path = ?2, file_crc = ?3 WHERE id = ?4 AND state = ?5",
            params![STATE_SEALED, path, file_crc as i64, id, STATE_WRITING],
        )
    }

    pub fn abort_segment(&self, id: i64) -> rusqlite::Result<usize> {
        self.conn.execute(
            "UPDATE segments SET state = ?1 WHERE id = ?2",
            params![STATE_ABORTED, id],
        )
    }

    pub fn mark_corrupt(&self, id: i64) -> rusqlite::Result<usize> {
        self.conn.execute(
            "UPDATE segments SET state = ?1 WHERE id = ?2",
            params![STATE_CORRUPT, id],
        )
    }

    pub fn get_segment(&self, id: i64) -> rusqlite::Result<Option<SegmentRow>> {
        self.conn
            .query_row(
                &format!("SELECT {SEGMENT_COLS} FROM segments WHERE id = ?1"),
                params![id],
                segment_row,
            )
            .optional()
    }

    pub fn list_segments(&self, series_id: i64) -> rusqlite::Result<Vec<SegmentRow>> {
        let mut st = self.conn.prepare(&format!(
            "SELECT {SEGMENT_COLS} FROM segments WHERE series_id = ?1 ORDER BY start_ts, id"
        ))?;
        let rows = st
            .query_map(params![series_id], segment_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// 与 [from, to] 重叠的已封存段，按时间升序。
    pub fn overlapping_segments(
        &self,
        series_id: i64,
        from: i64,
        to: i64,
    ) -> rusqlite::Result<Vec<SegmentRow>> {
        let mut st = self.conn.prepare(&format!(
            "SELECT {SEGMENT_COLS} FROM segments
             WHERE series_id = ?1 AND state = ?2 AND start_ts <= ?3 AND end_ts >= ?4
             ORDER BY start_ts"
        ))?;
        let rows = st
            .query_map(params![series_id, STATE_SEALED, to, from], segment_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// 所有已登记了路径的段（sealed / corrupt），用于启动时清理孤儿文件。
    pub fn all_segment_paths(&self) -> rusqlite::Result<Vec<String>> {
        let mut st = self
            .conn
            .prepare("SELECT path FROM segments WHERE path != ''")?;
        let rows = st
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_db(name: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "sensor_tsdb_test_{name}_{}_{n}.db",
            std::process::id()
        ))
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[test]
    fn writing_segments_are_aborted_on_reopen() {
        let path = temp_db("recover");
        {
            let cat = Catalog::open(&path).unwrap();
            let sid = cat.create_series("s", 1).unwrap();
            cat.insert_segment_writing(sid, 0, 100, 2, 1).unwrap();
            // 模拟崩溃：不写文件、不 seal，直接丢弃连接
        }
        {
            let cat = Catalog::open(&path).unwrap();
            let segs = cat.list_segments(1).unwrap();
            assert_eq!(segs.len(), 1);
            assert_eq!(segs[0].state, STATE_ABORTED, "interrupted write must be aborted");
            // 中断的段对查询不可见
            assert!(cat.overlapping_segments(1, 0, 1000).unwrap().is_empty());
        }
        cleanup(&path);
    }

    #[test]
    fn sealed_segments_are_queryable() {
        let path = temp_db("sealed");
        {
            let cat = Catalog::open(&path).unwrap();
            let sid = cat.create_series("s", 1).unwrap();
            let id = cat.insert_segment_writing(sid, 0, 100, 2, 1).unwrap();
            cat.seal_segment(id, "segments/1/seg_1.seg", 0xdead_beef).unwrap();
            let hits = cat.overlapping_segments(sid, 50, 60).unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].state, STATE_SEALED);
            assert_eq!(cat.series_max_ts(sid).unwrap(), Some(100));
        }
        cleanup(&path);
    }

    #[test]
    fn corrupt_segments_are_excluded_from_query() {
        let path = temp_db("corrupt");
        {
            let cat = Catalog::open(&path).unwrap();
            let sid = cat.create_series("s", 1).unwrap();
            let id = cat.insert_segment_writing(sid, 0, 100, 2, 1).unwrap();
            cat.seal_segment(id, "segments/1/seg_1.seg", 1).unwrap();
            cat.mark_corrupt(id).unwrap();
            assert!(cat.overlapping_segments(sid, 0, 1000).unwrap().is_empty());
        }
        cleanup(&path);
    }
}
