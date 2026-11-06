use crate::api::{AnnotatedTrade, TradeQuery};
use crate::config::{RetentionHours, StorageBytes};
use crate::stream;

use flowsurface_exchange::adapter::{Event, Exchange};
use flowsurface_exchange::unit::{price::Price, qty::Qty};
use flowsurface_exchange::{Ticker, TickerInfo, UnixMs};

use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use axum::http::StatusCode;
use duckdb::Connection;
use tokio::task::JoinHandle;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Tracked pair with the timestamp range stored.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct PairInfo {
    pub ticker: Ticker,
    pub earliest: Option<UnixMs>,
    pub latest: Option<UnixMs>,
}

/// A conservative contiguous interval of recorded executions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct CoverageSegment {
    pub from: u64,
    pub to: u64,
}

/// Result of one retention purge.  `retain_from` is the first timestamp the
/// server is still allowed to certify after the purge, even when the expired
/// interval happened to contain no executions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPurge {
    pub deleted: u64,
    pub retain_from: i64,
}

/// DuckDB storage usage split into real filesystem allocation and live data.
///
/// DuckDB can reuse freed blocks without shrinking the database file, so both
/// values matter: `physical` protects the VPS disk, while `active` tells the
/// cleanup loop whether deleting more truthful history can actually help.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageUsage {
    pub physical: StorageBytes,
    pub active: StorageBytes,
}

const RETENTION_COVERAGE_FLOOR_KEY: &str = "coverage_retention_floor_ms";
/// Capture semantics v2 identify Hyperliquid executions by the documented
/// `(block_time, coin, tid)` key and reject/reconcile subscription replays.
/// Legacy Hyperliquid intervals cannot prove those guarantees and must never
/// be returned as verified coverage.
const CURRENT_CAPTURE_SEMANTICS_VERSION: u8 = 2;
const HISTORY_SORT_WINDOW_MS: i64 = 3_600_000;

/// Only interrupts the connection owned by one history request. Live writes
/// and health queries use different connections to the same database.
struct HistoryQueryGuard {
    cancelled: Arc<AtomicBool>,
    interrupt: Arc<duckdb::InterruptHandle>,
}

impl Drop for HistoryQueryGuard {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        self.interrupt.interrupt();
    }
}

/// Manages the DuckDB database lifecycle and provides high-level read access.
///
/// Write access is handled through a separate [`BatchWriter`] that uses
/// DuckDB's `Appender` API for efficient bulk insertion.
///
/// Internally holds a single root `duckdb_database` handle.  All
/// sub-connections (readers, writer) are created via `try_clone()`,
/// ensuring they share the same buffer pool, catalog cache and WAL —
/// so writes are immediately visible to subsequent reads.
#[derive(Clone)]
pub struct Storage {
    db: Arc<parking_lot::Mutex<duckdb::Connection>>,
    data_dir: PathBuf,
    start_instant: std::time::Instant,
    start_wall_ms: i64,
}

impl Storage {
    /// Open (or create) the database at `data_dir / trades.duckdb` and ensure
    /// the schema exists.  Creates `data_dir` if it does not exist.
    ///
    /// The returned `Storage` keeps a single `duckdb_database` handle alive;
    /// all sub-connections (readers and the batch writer) must be obtained
    /// via [`open_writer`](Self::open_writer) / [`connection`](Self::connection)
    /// so they share the same database instance.
    pub fn open(
        data_dir: &Path,
        memory_limit_mb: u64,
        threads: u64,
        max_storage_mb: u64,
    ) -> Result<Self> {
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("creating data directory {}", data_dir.display()))?;

        let db_path = data_dir.join("trades.duckdb");

        let root = Connection::open(&db_path)
            .with_context(|| format!("opening DuckDB at {}", db_path.display()))?;

        root.execute_batch(
            "CREATE TABLE IF NOT EXISTS trades (
                exchange   VARCHAR NOT NULL,
                symbol     VARCHAR NOT NULL,
                ts  BIGINT  NOT NULL,
                price      DOUBLE  NOT NULL,
                qty   DOUBLE  NOT NULL,
                is_sell    BOOLEAN NOT NULL
            );

            -- DuckDB cannot use a multi-column ART index for these range
            -- scans. Its non-evictable buffers otherwise grow with live
            -- recording and consume the memory needed by history queries.
            -- Drop only this derived index; every trade remains in place.
            DROP INDEX IF EXISTS idx_trades_ex_sym_t;

            CREATE TABLE IF NOT EXISTS ticker_info (
                exchange        VARCHAR NOT NULL,
                symbol          VARCHAR NOT NULL,
                min_ticksize    INTEGER NOT NULL,
                min_qty         INTEGER NOT NULL,
                contract_size   INTEGER,
                PRIMARY KEY (exchange, symbol)
            );

            CREATE TABLE IF NOT EXISTS metadata (
                key   VARCHAR NOT NULL PRIMARY KEY,
                value VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS capture_intervals (
                exchange         VARCHAR NOT NULL,
                symbol           VARCHAR NOT NULL,
                connected_at     BIGINT NOT NULL,
                observed_through BIGINT,
                closed_at        BIGINT,
                semantics_version UTINYINT NOT NULL DEFAULT 1,
                PRIMARY KEY (exchange, symbol, connected_at)
            );

            ALTER TABLE capture_intervals
                ADD COLUMN IF NOT EXISTS semantics_version UTINYINT DEFAULT 1;

            CREATE TABLE IF NOT EXISTS coverage_floors (
                exchange   VARCHAR NOT NULL,
                symbol     VARCHAR NOT NULL,
                retain_from BIGINT NOT NULL,
                PRIMARY KEY (exchange, symbol)
            );",
        )
        .context("creating DuckDB schema")?;

        // Max temp directory size: 20% of the main storage cap, clamped to
        // [512 MiB, 4096 MiB].  When max_storage_mb is 0 (unlimited) we
        // use the ceiling as the reference point.
        let max_temp_mb = {
            let base = if max_storage_mb == 0 {
                4096
            } else {
                max_storage_mb
            };
            ((base as f64 * 0.20) as u64).clamp(512, 4096)
        };

        let mut pragmas: Vec<String> = vec!["SET enable_external_access = false;".to_string()];
        if memory_limit_mb > 0 {
            pragmas.push(format!("SET memory_limit = '{memory_limit_mb}MB';"));
        }
        if threads > 0 {
            pragmas.push(format!("SET threads = {threads};"));
        }
        pragmas.push(format!("SET max_temp_directory_size = '{max_temp_mb}MB';"));
        root.execute_batch(&pragmas.join(" "))
            .context("applying DuckDB resource limits")?;

        Ok(Self {
            db: Arc::new(parking_lot::Mutex::new(root)),
            data_dir: data_dir.to_path_buf(),
            start_instant: std::time::Instant::now(),
            start_wall_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        })
    }

    /// Timestamp before this process starts discovery and market-data streams.
    pub fn started_at(&self) -> UnixMs {
        UnixMs::new(self.start_wall_ms as u64)
    }

    /// Return the size (in bytes) of the main database file plus the
    /// WAL file.  If a file does not (yet) exist its size is counted as 0.
    pub fn current_storage_bytes(&self) -> Result<StorageBytes> {
        let db_path = self.data_dir.join("trades.duckdb");
        let wal_path = self.data_dir.join("trades.duckdb.wal");

        let db_size = match std::fs::metadata(&db_path) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => {
                return Err(e).with_context(|| format!("checking size of {}", db_path.display()));
            }
        };

        let wal_size = match std::fs::metadata(&wal_path) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => {
                return Err(e).with_context(|| format!("checking size of {}", wal_path.display()));
            }
        };

        Ok(StorageBytes::from_bytes(db_size + wal_size))
    }

    /// Return real DB+WAL allocation and the portion occupied by live DuckDB
    /// blocks.  Freed blocks remain in the physical file for reuse, so the
    /// active value may be lower than the filesystem value after cleanup.
    pub fn storage_usage(&self) -> Result<StorageUsage> {
        let physical = self.current_storage_bytes()?;
        let wal_bytes = std::fs::metadata(self.data_dir.join("trades.duckdb.wal"))
            .map(|meta| meta.len())
            .or_else(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    Ok(0)
                } else {
                    Err(error)
                }
            })
            .context("checking DuckDB WAL size")?;
        let conn = self.connection()?;
        let (block_size, used_blocks): (i64, i64) = conn
            .query_row(
                "SELECT block_size, used_blocks
                 FROM pragma_database_size()
                 WHERE database_name = current_database()",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .context("querying DuckDB live block usage")?;
        let live_blocks = u64::try_from(block_size)
            .unwrap_or_default()
            .saturating_mul(u64::try_from(used_blocks).unwrap_or_default());
        Ok(StorageUsage {
            physical,
            active: StorageBytes::from_bytes(live_blocks.saturating_add(wal_bytes)),
        })
    }

    /// Return the total number of rows in the `trades` table.
    pub fn count_trades(&self) -> Result<u64> {
        let conn = self.connection()?;
        conn.query_row("SELECT COUNT(*) FROM trades", [], |r| r.get(0))
            .context("counting trades")
    }

    /// Delete the `batch_size` oldest trades and atomically advance a
    /// per-source coverage floor past every deleted timestamp.
    ///
    /// The floor is deliberately conservative when a batch splits rows that
    /// share one timestamp: retained rows at that exact millisecond still
    /// exist, but the server will not certify that millisecond as complete.
    pub fn delete_oldest_trades_batch(&self, batch_size: i64) -> Result<u64> {
        if batch_size <= 0 {
            return Ok(0);
        }
        let conn = self.connection()?;
        conn.execute_batch("BEGIN TRANSACTION")
            .context("starting oldest-trade purge transaction")?;
        let result = (|| {
            conn.execute_batch(
                "CREATE TEMP TABLE purge_batch AS
                 SELECT rowid AS doomed_rowid, exchange, symbol, ts
                 FROM trades
                 WHERE FALSE;",
            )
            .context("creating oldest-trade purge batch")?;
            conn.execute(
                "INSERT INTO purge_batch
                 SELECT rowid, exchange, symbol, ts
                 FROM trades
                 ORDER BY ts ASC, rowid ASC
                 LIMIT ?1",
                duckdb::params![batch_size],
            )
            .context("selecting oldest-trade purge batch")?;

            let selected: u64 = conn
                .query_row("SELECT COUNT(*) FROM purge_batch", [], |row| row.get(0))
                .context("counting oldest-trade purge batch")?;
            if selected == 0 {
                return Ok(0);
            }

            conn.execute_batch(
                "INSERT OR REPLACE INTO coverage_floors
                    (exchange, symbol, retain_from)
                 SELECT doomed.exchange,
                        doomed.symbol,
                        GREATEST(
                            MAX(doomed.ts) + 1,
                            COALESCE(existing.retain_from, 0)
                        )
                 FROM purge_batch AS doomed
                 LEFT JOIN coverage_floors AS existing
                   ON existing.exchange = doomed.exchange
                  AND existing.symbol = doomed.symbol
                 GROUP BY doomed.exchange, doomed.symbol, existing.retain_from;",
            )
            .context("advancing coverage floors for size purge")?;
            let deleted = conn
                .execute(
                    "DELETE FROM trades
                     WHERE rowid IN (SELECT doomed_rowid FROM purge_batch)",
                    [],
                )
                .context("deleting oldest trades batch")?;
            anyhow::ensure!(
                deleted as u64 == selected,
                "oldest-trade purge selected {selected} rows but deleted {deleted}"
            );
            Ok(deleted as u64)
        })();

        match result {
            Ok(deleted) => {
                conn.execute_batch("COMMIT")
                    .context("committing oldest-trade purge transaction")?;
                Ok(deleted)
            }
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// Merge the DuckDB WAL into the main database file, truncate the WAL,
    /// and let DuckDB reclaim sufficiently deleted row groups for reuse.
    pub fn run_checkpoint(&self) -> Result<()> {
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);

        let mut last_err = None;
        for attempt in 0..MAX_RETRIES {
            let conn = self.connection()?;
            match conn.execute_batch("CHECKPOINT;") {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_err = Some(anyhow::Error::new(error));
                    if attempt + 1 < MAX_RETRIES {
                        std::thread::sleep(RETRY_DELAY);
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("CHECKPOINT was not attempted")))
            .context("checkpointing DuckDB WAL after retries")
    }

    /// Open a dedicated connection for the batch writer (shares the
    /// underlying `duckdb_database`).
    pub fn open_writer(&self) -> Result<BatchWriter> {
        let locked = self.db.lock();
        let conn = locked
            .try_clone()
            .context("cloning root connection for writer")?;
        Ok(BatchWriter { conn })
    }

    /// Obtain a fresh read-only connection that shares the same
    /// `duckdb_database` as the root connection.
    fn connection(&self) -> Result<duckdb::Connection> {
        let locked = self.db.lock();
        locked
            .try_clone()
            .context("cloning root connection for query")
    }

    pub fn query_trades(conn: &Connection, q: &TradeQuery) -> Result<Vec<AnnotatedTrade>> {
        use arrow::array::{BooleanArray, Float64Array, Int64Array};
        let limit = q.limit.unwrap_or(1000).min(10_000);
        let mut trades = Vec::new();
        Self::for_each_trade_batch(conn, q, limit, |batch| {
            let exchange: Exchange = q.exchange_filter().parse().map_err(anyhow::Error::msg)?;
            let ticker = Ticker::new(&q.symbol.to_lowercase(), exchange);
            let ts = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("trade timestamps")?;
            let price = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("trade prices")?;
            let qty = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("trade quantities")?;
            let sell = batch
                .column(3)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("trade sides")?;
            for i in 0..batch.num_rows() {
                trades.push(AnnotatedTrade {
                    ticker,
                    trade: flowsurface_exchange::Trade {
                        time: UnixMs::new(ts.value(i) as u64),
                        is_sell: sell.value(i),
                        price: Price::from_f64(price.value(i)),
                        qty: Qty::from_f64(qty.value(i)),
                    },
                });
            }
            Ok(())
        })?;
        Ok(trades)
    }

    /// Sort bounded time ranges in one MVCC snapshot. Adjacent windows are
    /// disjoint inclusive ranges: duplicates at the same millisecond remain
    /// together, and live flushes/purges cannot change the page between windows.
    fn for_each_trade_batch(
        conn: &Connection,
        q: &TradeQuery,
        limit: usize,
        mut visit: impl FnMut(arrow::record_batch::RecordBatch) -> Result<()>,
    ) -> Result<()> {
        if limit == 0 {
            return Ok(());
        }
        let transaction = conn
            .unchecked_transaction()
            .context("starting history snapshot")?;
        let exchange = q.exchange_filter();
        let symbol = q.symbol.to_lowercase();
        let (from, to): (Option<i64>, Option<i64>) = transaction.query_row(
            "SELECT min(ts), max(ts) FROM trades WHERE symbol = ?1 AND exchange = ?2 AND ts >= ?3 AND ts <= ?4",
            duckdb::params![symbol, exchange, q.from.unwrap_or(i64::MIN), q.to.unwrap_or(i64::MAX)],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).context("finding stored history bounds")?;
        if let (Some(mut from), Some(mut to)) = (from, to) {
            let ascending = q.from.is_some();
            let direction = if ascending { "ASC" } else { "DESC" };
            let tie_order = if q.stable_order {
                format!(", rowid {direction}")
            } else {
                String::new()
            };
            let mut statement = transaction
                .prepare(&format!(
                    "SELECT ts, price, qty, is_sell FROM trades \
                 WHERE symbol = ?1 AND exchange = ?2 AND ts >= ?3 AND ts <= ?4 \
                 ORDER BY ts {direction}{tie_order} LIMIT ?5"
                ))
                .context("preparing bounded history query")?;
            let mut remaining = limit;
            while remaining > 0 {
                let before = remaining;
                let window_from = if ascending {
                    from
                } else {
                    from.max(to.saturating_sub(HISTORY_SORT_WINDOW_MS - 1))
                };
                let window_to = if ascending {
                    to.min(from.saturating_add(HISTORY_SORT_WINDOW_MS - 1))
                } else {
                    to
                };
                // Consume through fallible step(), so a failed fetch cannot
                // masquerade as a successful empty or truncated history page.
                drop(
                    statement
                        .stream_arrow(duckdb::params![
                            symbol,
                            exchange,
                            window_from,
                            window_to,
                            remaining as i64
                        ])
                        .context("executing bounded history query")?,
                );
                while let Some(chunk) = statement.step().context("fetching history record batch")? {
                    let batch = arrow::record_batch::RecordBatch::from(&chunk);
                    remaining -= batch.num_rows();
                    visit(batch)?;
                }
                if ascending {
                    if window_to == to {
                        break;
                    }
                    from = window_to + 1;
                } else {
                    if window_from == from {
                        break;
                    }
                    to = window_from - 1;
                }
                // A sparse or disconnected source may have a very long gap.
                // Jump to the next stored interval instead of probing every
                // empty hour; this aggregate never sorts/materializes rows.
                if remaining == before {
                    let bounds: (Option<i64>, Option<i64>) = transaction.query_row(
                        "SELECT min(ts), max(ts) FROM trades WHERE symbol = ?1 AND exchange = ?2 AND ts >= ?3 AND ts <= ?4",
                        duckdb::params![symbol, exchange, from, to],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    ).context("finding next stored history window")?;
                    let (Some(next_from), Some(next_to)) = bounds else {
                        break;
                    };
                    from = next_from;
                    to = next_to;
                }
            }
        }
        transaction.commit().context("finishing history snapshot")
    }

    /// Returns the complete Arrow IPC streaming format payload suitable
    /// for HTTP response with `Content-Type: application/vnd.apache.arrow.stream`.
    pub fn query_trades_arrow_ipc(conn: &Connection, q: &TradeQuery) -> Result<Vec<u8>> {
        let limit = q.limit.unwrap_or(50_000).min(400_000);

        let schema: Arc<Schema> = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("qty", DataType::Float64, false),
            Field::new("is_sell", DataType::Boolean, false),
        ]));

        let mut buf = Vec::with_capacity(4096);
        let mut batch_count = 0usize;
        {
            let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())
                .context("creating Arrow IPC stream writer")?;

            Self::for_each_trade_batch(conn, q, limit, |batch| {
                writer.write(&batch).context("writing Arrow record batch")?;
                batch_count += 1;
                Ok(())
            })?;

            writer.finish().context("finishing Arrow IPC stream")?;
        }

        tracing::debug!(
            "Exported {} bytes of Arrow IPC data ({} batch(es))",
            buf.len(),
            batch_count,
        );
        Ok(buf)
    }

    /// Persist ticker metadata for every resolved pair so the API can
    /// look up tick sizes at query time.
    pub fn store_ticker_infos(&self, infos: &[TickerInfo]) -> Result<()> {
        let conn = self.connection()?;

        let mut stmt = conn
            .prepare(
                "INSERT OR REPLACE INTO ticker_info
                    (exchange, symbol, min_ticksize, min_qty, contract_size)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .context("preparing ticker_info upsert")?;

        for info in infos {
            stmt.execute(duckdb::params![
                info.exchange().to_string(),
                info.ticker
                    .display_symbol()
                    .map(|s| s.to_lowercase())
                    .unwrap_or_else(|| info.ticker.to_string().to_lowercase()),
                info.min_ticksize.power,
                info.min_qty.power,
                info.contract_size.map(|cs| cs.power),
            ])
            .with_context(|| {
                format!(
                    "inserting ticker_info for {}/{}",
                    info.exchange(),
                    info.ticker
                )
            })?;
        }
        Ok(())
    }

    /// Return every (venue, symbol) that has at least one stored trade,
    /// along with the earliest and latest timestamps.
    pub fn pairs_with_bounds(&self) -> Result<Vec<PairInfo>> {
        let conn = self.connection()?;

        let mut stmt = conn.prepare(
            "SELECT exchange, symbol,
                    MIN(ts) AS earliest,
                    MAX(ts) AS latest
             FROM trades
             GROUP BY exchange, symbol
             ORDER BY exchange, symbol",
        )?;

        let rows = stmt.query_map([], |row| {
            let exchange_str: String = row.get(0)?;
            let symbol_str: String = row.get(1)?;
            let exchange: Exchange = exchange_str.parse().map_err(|e: String| {
                duckdb::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("cannot parse exchange '{exchange_str}': {e}"),
                )))
            })?;

            Ok(PairInfo {
                ticker: Ticker::new(&symbol_str, exchange),
                earliest: row.get::<_, Option<i64>>(2)?.map(|v| UnixMs::new(v as u64)),
                latest: row.get::<_, Option<i64>>(3)?.map(|v| UnixMs::new(v as u64)),
            })
        })?;

        let mut pairs = Vec::new();
        for row in rows {
            pairs.push(row?);
        }
        Ok(pairs)
    }

    /// Return durable recorder-capture intervals for a requested range.
    ///
    /// Coverage never comes from the mere presence of trades. A segment begins
    /// only after a WebSocket `Connected` event and ends at the last execution
    /// durably observed on that connection. This keeps sleep, process restart,
    /// heartbeat timeout and network-outage tails explicitly uncovered.
    pub fn trade_coverage_segments(&self, q: &TradeQuery) -> Result<Vec<CoverageSegment>> {
        let Some(from) = q.from.filter(|value| *value >= 0) else {
            return Ok(Vec::new());
        };
        let Some(to) = q.to.filter(|value| *value >= from) else {
            return Ok(Vec::new());
        };
        let conn = self.connection()?;
        let exchange = q.exchange_filter();
        let minimum_semantics = if exchange == Exchange::HyperliquidLinear.to_string()
            || exchange == Exchange::HyperliquidSpot.to_string()
        {
            CURRENT_CAPTURE_SEMANTICS_VERSION
        } else {
            1
        };
        let symbol = q.symbol.to_lowercase();
        let mut stmt = conn
            .prepare(
                "WITH coverage_floor AS (
                     SELECT GREATEST(
                         ?3,
                         COALESCE(
                             TRY_CAST((
                                 SELECT value FROM metadata
                                 WHERE key = 'coverage_retention_floor_ms'
                             ) AS BIGINT),
                             ?3
                         ),
                         COALESCE((
                             SELECT retain_from FROM coverage_floors
                             WHERE exchange = ?2 AND symbol = ?1
                         ), ?3)
                     ) AS effective_from
                 )
                 SELECT GREATEST(connected_at, effective_from) AS segment_from,
                        LEAST(observed_through, ?4) AS segment_to
                 FROM capture_intervals, coverage_floor
                 WHERE symbol = ?1 AND exchange = ?2
                   AND observed_through IS NOT NULL
                   AND effective_from <= ?4
                   AND observed_through >= effective_from
                   AND connected_at <= ?4
                   AND COALESCE(semantics_version, 1) >= ?5
                 ORDER BY segment_from ASC, segment_to ASC",
            )
            .context("preparing capture coverage query")?;
        let mut rows = stmt
            .query(duckdb::params![
                symbol,
                exchange,
                from,
                to,
                minimum_semantics
            ])
            .context("querying capture coverage")?;

        let mut segments = Vec::<CoverageSegment>::new();
        while let Some(row) = rows.next()? {
            let start: i64 = row.get(0)?;
            let end: i64 = row.get(1)?;
            if end < start {
                continue;
            }
            if let Some(previous) = segments.last_mut()
                && start <= (previous.to as i64).saturating_add(1)
            {
                previous.to = previous.to.max(end as u64);
            } else {
                segments.push(CoverageSegment {
                    from: start as u64,
                    to: end as u64,
                });
            }
        }
        Ok(segments)
    }

    /// Set a metadata key-value pair.
    pub fn set_metadata(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.connection()?;
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, ?2)",
            duckdb::params![key, value],
        )
        .context("setting metadata")?;
        Ok(())
    }

    /// Get a metadata value by key, returning `None` if absent.
    pub fn get_metadata(&self, key: &str) -> Result<Option<String>> {
        let conn = self.connection()?;
        let mut stmt = conn
            .prepare("SELECT value FROM metadata WHERE key = ?1")
            .context("preparing metadata lookup")?;
        let mut rows = stmt.query_map(duckdb::params![key], |row| row.get::<_, String>(0))?;
        match rows.next() {
            Some(Ok(v)) => Ok(Some(v)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// Run a bounded history read on its own cancellable connection. The
    /// blocking worker owns admission until it actually exits, even if its
    /// HTTP handler times out or is dropped by a disconnected client.
    pub async fn run_history_query<T: Send + 'static>(
        self,
        deadline: Duration,
        label: &str,
        permit: tokio::sync::OwnedSemaphorePermit,
        f: impl FnOnce(&Connection) -> anyhow::Result<T> + Send + 'static,
    ) -> Result<T, (StatusCode, &'static str)> {
        let conn = self.connection().map_err(|error| {
            tracing::error!("{label} query connection failed: {error:#}");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        })?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let _guard = HistoryQueryGuard {
            cancelled: cancelled.clone(),
            interrupt: conn.interrupt_handle(),
        };
        let worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            anyhow::ensure!(
                !cancelled.load(Ordering::Acquire),
                "history query cancelled"
            );
            f(&conn)
        });
        match tokio::time::timeout(deadline, worker).await {
            Ok(Ok(Ok(data))) => Ok(data),
            Ok(Ok(Err(error))) => {
                tracing::error!("{label} query failed: {error:#}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, "internal error"))
            }
            Ok(Err(error)) => {
                tracing::error!("{label} query task panicked: {error:#}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, "internal error"))
            }
            Err(_) => {
                tracing::warn!("{label} query timed out after {deadline:?}");
                Err((StatusCode::SERVICE_UNAVAILABLE, "query timed out"))
            }
        }
    }

    /// Run a blocking DuckDB query on the blocking thread pool with a timeout.
    ///
    /// # Errors
    ///
    /// Returns `Err((StatusCode, &'static str))` suitable for `json_err` on:
    /// - Query failure (the closure returned `Err`)
    /// - Task panic (`spawn_blocking` panicked)
    /// - Timeout (the deadline elapsed)
    pub async fn run_blocking_query<T: Send + 'static>(
        self,
        deadline: Duration,
        label: &str,
        f: impl FnOnce(Storage) -> anyhow::Result<T> + Send + 'static,
    ) -> Result<T, (StatusCode, &'static str)> {
        match tokio::time::timeout(deadline, tokio::task::spawn_blocking(move || f(self))).await {
            Ok(Ok(Ok(data))) => Ok(data),
            Ok(Ok(Err(e))) => {
                tracing::error!("{label} query failed: {e:#}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, "internal error"))
            }
            Ok(Err(join_err)) => {
                tracing::error!("{label} query task panicked: {join_err:#}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, "internal error"))
            }
            Err(_) => {
                tracing::warn!("{label} query timed out after {deadline:?}");
                Err((StatusCode::SERVICE_UNAVAILABLE, "query timed out"))
            }
        }
    }

    /// Return a monotonically-increasing timestamp (ms since Unix epoch)
    /// that is immune to NTP jumps and manual clock changes after startup.
    ///
    /// Wall-clock time is captured once in [`open`](Self::open); all
    /// subsequent calls derive the timestamp from [`Instant::now`],
    /// which never regresses.
    pub(crate) fn now_ms(&self) -> i64 {
        self.start_wall_ms + self.start_instant.elapsed().as_millis() as i64
    }

    /// Delete every trade row whose `ts` is older than `retention_hours` and
    /// atomically move the global coverage floor to the same cutoff.
    ///
    /// When `retention_hours` is `0` (unlimited) the purge is skipped
    /// and `Ok(0)` is returned immediately.
    pub fn purge_old_trades(&self, retention_hours: RetentionHours) -> Result<RetentionPurge> {
        if retention_hours.as_hours() == 0 {
            return Ok(RetentionPurge {
                deleted: 0,
                retain_from: i64::MIN,
            });
        }
        let conn = self.connection()?;
        let cutoff_ms = self.now_ms() - retention_hours.as_millis();
        conn.execute_batch("BEGIN TRANSACTION")
            .context("starting retention purge transaction")?;
        let result = (|| {
            let deleted = conn
                .execute(
                    "DELETE FROM trades WHERE ts < ?1",
                    duckdb::params![cutoff_ms],
                )
                .context("purging old trades")?;
            conn.execute(
                "INSERT OR REPLACE INTO metadata (key, value)
                 VALUES (?1, CAST(GREATEST(
                     ?2,
                     COALESCE(TRY_CAST((
                         SELECT value FROM metadata WHERE key = ?1
                     ) AS BIGINT), ?2)
                 ) AS VARCHAR))",
                duckdb::params![RETENTION_COVERAGE_FLOOR_KEY, cutoff_ms],
            )
            .context("advancing retention coverage floor")?;
            Ok(RetentionPurge {
                deleted: deleted as u64,
                retain_from: cutoff_ms,
            })
        })();
        match result {
            Ok(purge) => {
                conn.execute_batch("COMMIT")
                    .context("committing retention purge transaction")?;
                Ok(purge)
            }
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    pub fn record_cleanup(&self) -> Result<i64> {
        let now_ms = self.now_ms();
        self.set_metadata("last_cleanup", &now_ms.to_string())?;
        Ok(now_ms)
    }

    /// Spawn a background task that receives [`Event`]s on the
    /// persist channel, matches on variant, and persists each
    /// data type to its DuckDB table.
    ///
    /// - `TradesReceived` → buffered and flushed via `Appender` API.
    /// - `DepthReceived` → logged (future tables).
    pub fn spawn_batch_flusher(
        &self,
        rx: &mut stream::StreamReceivers,
        flush_interval: Duration,
        max_buffered_trades: usize,
    ) -> JoinHandle<()> {
        let mut rx = rx
            .persist
            .take()
            .expect("spawn_batch_flusher needs the persist receiver");
        let store = self.clone();

        tokio::spawn(async move {
            let mut flusher = BatchFlusher::new(max_buffered_trades);
            let mut interval = tokio::time::interval(flush_interval);
            interval.reset_immediately();

            loop {
                tokio::select! {
                    biased;
                    _ = interval.tick() => {
                        // Drain only up to the configured batch threshold. Any
                        // excess remains in the bounded channel and naturally
                        // backpressures the recorder without losing events.
                        while !flusher.should_flush() {
                            let Ok(event) = rx.try_recv() else { break };
                            flusher.ingest(event);
                        }

                        if flusher.has_pending() {
                            let batch = flusher.take_all();
                            flush_batch_lossless(store.clone(), batch).await;
                        }
                    }
                    maybe = rx.recv() => {
                        match maybe {
                            Some(event) => {
                                flusher.ingest(event);
                                if flusher.should_flush() {
                                    let batch = flusher.take_all();
                                    flush_batch_lossless(store.clone(), batch).await;
                                }
                            }
                            None => {
                                // Channel closed: durably flush everything
                                // accepted before allowing shutdown to finish.
                                if flusher.has_pending() {
                                    let batch = flusher.take_all();
                                    flush_batch_lossless(store.clone(), batch).await;
                                }
                                tracing::info!("Batch flusher channel closed.");
                                break;
                            }
                        }
                    }
                }
            }
        })
    }
}

/// Persist a batch transactionally and retry it in place until durable.
/// The bounded channels upstream provide backpressure while DuckDB is
/// unavailable, so an I/O failure cannot turn into a silent market-data gap.
async fn flush_batch_lossless(store: Storage, mut batch: Vec<PersistableData>) {
    let mut attempts = 0u32;
    loop {
        let attempt_store = store.clone();
        let (returned, result) = tokio::task::spawn_blocking(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut writer = attempt_store.open_writer()?;
                writer.flush_all(&batch)
            }))
            .unwrap_or_else(|_| Err(anyhow::anyhow!("batch flush panicked")));
            (batch, result)
        })
        .await
        .unwrap_or_else(|join_error| {
            // The closure catches its own panics, so a join failure means the
            // runtime cancelled the blocking task. Preserve the accepted data
            // by terminating rather than pretending the batch was durable.
            panic!("lossless batch flush task was cancelled: {join_error}")
        });
        batch = returned;

        match result {
            Ok(()) => return,
            Err(error) => {
                attempts = attempts.saturating_add(1);
                let delay_secs = 1u64 << attempts.min(5);
                tracing::error!(
                    attempts,
                    rows = batch.len(),
                    retry_secs = delay_secs,
                    "Batch flush failed; retaining the complete batch for retry: {error:#}"
                );
                tokio::time::sleep(Duration::from_secs(delay_secs)).await;
            }
        }
    }
}

/// All market-data types that the flusher can persist to DuckDB.
#[derive(Clone)]
enum PersistableData {
    Trade(AnnotatedTrade),
    CaptureConnected { ticker: Ticker, at: i64 },
    CaptureRecovered { ticker: Ticker, from: i64 },
    CaptureObserved { ticker: Ticker, through: i64 },
    CaptureDisconnected { ticker: Ticker, at: i64 },
}

/// Routes [`Event`]s into a single [`DataBuffer`], separating the
/// concern of "does this event get persisted?" from *how* it's buffered.
///
/// Events that don't need persistence (depth, kline, connect/disconnect)
/// are logged or ignored.
struct BatchFlusher {
    buf: DataBuffer,
    flush_threshold: usize,
}

impl BatchFlusher {
    fn new(max_items: usize) -> Self {
        Self {
            buf: DataBuffer::new(),
            flush_threshold: max_items.max(1),
        }
    }

    /// Route an [`Event`] into the buffer or log it.
    fn ingest(&mut self, event: Event) {
        match event {
            Event::TradesReceived(stream_kind, _tss, trades) => {
                let ticker_info = stream_kind.ticker_info();
                for ft_trade in trades.iter() {
                    self.buf.push(PersistableData::Trade(AnnotatedTrade::new(
                        ticker_info.ticker,
                        *ft_trade,
                    )));
                }
                if let Some(last) = trades.iter().map(|trade| trade.time.as_u64()).max() {
                    self.buf.push(PersistableData::CaptureObserved {
                        ticker: ticker_info.ticker,
                        through: last as i64,
                    });
                }
            }
            Event::DepthReceived(stream_kind, _update_t, _depth) => {
                tracing::trace!(?stream_kind, "Depth update received");
            }
            Event::KlineReceived(stream_kind, _kline) => {
                tracing::trace!(?stream_kind, "Kline update received");
            }
            Event::Connected(streams) => {
                let at = unix_now_ms();
                for stream in streams.iter() {
                    if matches!(
                        stream,
                        flowsurface_exchange::adapter::StreamKind::Trades { .. }
                    ) {
                        self.buf.push(PersistableData::CaptureConnected {
                            ticker: stream.ticker_info().ticker,
                            at,
                        });
                    }
                }
            }
            Event::ReplayRecovered(streams, from) => {
                for stream in streams.iter() {
                    if matches!(
                        stream,
                        flowsurface_exchange::adapter::StreamKind::Trades { .. }
                    ) {
                        self.buf.push(PersistableData::CaptureRecovered {
                            ticker: stream.ticker_info().ticker,
                            from: from.as_u64() as i64,
                        });
                    }
                }
            }
            Event::Disconnected(streams, _) => {
                let at = unix_now_ms();
                for stream in streams.iter() {
                    if matches!(
                        stream,
                        flowsurface_exchange::adapter::StreamKind::Trades { .. }
                    ) {
                        self.buf.push(PersistableData::CaptureDisconnected {
                            ticker: stream.ticker_info().ticker,
                            at,
                        });
                    }
                }
            }
        }
    }

    fn has_pending(&self) -> bool {
        !self.buf.is_empty()
    }

    fn pending_count(&self) -> usize {
        self.buf.len()
    }

    fn should_flush(&self) -> bool {
        self.pending_count() >= self.flush_threshold
    }

    fn take_all(&mut self) -> Vec<PersistableData> {
        self.buf.take()
    }
}

fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// Pending data already accepted from the lossless persist channel.
struct DataBuffer {
    items: Vec<PersistableData>,
}

impl DataBuffer {
    fn new() -> Self {
        Self { items: Vec::new() }
    }

    fn push(&mut self, item: PersistableData) {
        self.items.push(item);
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn take(&mut self) -> Vec<PersistableData> {
        std::mem::take(&mut self.items)
    }
}

/// Writes buffered market data to DuckDB using the `Appender` API.
///
/// Each [`PersistableData`] variant is dispatched to its own table.
pub struct BatchWriter {
    conn: Connection,
}

impl BatchWriter {
    /// Flush a batch of persistable items to their respective tables.
    fn flush_all(&mut self, items: &[PersistableData]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        self.conn.execute_batch("BEGIN TRANSACTION")?;
        let result = (|| {
            let mut trade_appender = self
                .conn
                .appender("trades")
                .context("creating DuckDB appender for trades")?;

            for item in items {
                match item {
                    PersistableData::Trade(t) => {
                        trade_appender
                            .append_row((
                                &t.ticker.exchange.to_string(),
                                &t.ticker
                                    .display_symbol()
                                    .map(|s| s.to_lowercase())
                                    .unwrap_or_else(|| t.ticker.to_string().to_lowercase()),
                                t.trade.time.as_u64() as i64,
                                t.trade.price.to_f64(),
                                t.trade.qty.to_f64(),
                                t.trade.is_sell,
                            ))
                            .context("appending trade row via DuckDB appender")?;
                    }
                    PersistableData::CaptureConnected { .. }
                    | PersistableData::CaptureRecovered { .. }
                    | PersistableData::CaptureObserved { .. }
                    | PersistableData::CaptureDisconnected { .. } => {}
                }
            }

            trade_appender
                .flush()
                .context("flushing DuckDB trade appender")?;
            drop(trade_appender);

            for item in items {
                match item {
                    PersistableData::Trade(_) => {}
                    PersistableData::CaptureConnected { ticker, at } => {
                        let (exchange, symbol) = ticker_storage_key(*ticker);
                        self.conn.execute(
                            "UPDATE capture_intervals SET closed_at = ?3
                             WHERE exchange = ?1 AND symbol = ?2 AND closed_at IS NULL",
                            duckdb::params![exchange, symbol, at],
                        )?;
                        self.conn.execute(
                            "INSERT OR IGNORE INTO capture_intervals
                                (exchange, symbol, connected_at, observed_through, closed_at,
                                 semantics_version)
                             VALUES (?1, ?2, ?3, NULL, NULL, ?4)",
                            duckdb::params![
                                exchange,
                                symbol,
                                at,
                                CURRENT_CAPTURE_SEMANTICS_VERSION
                            ],
                        )?;
                    }
                    PersistableData::CaptureRecovered { ticker, from } => {
                        let (exchange, symbol) = ticker_storage_key(*ticker);
                        // Fail closed: bridge only when the repeated execution
                        // overlaps a durable closed interval for this source.
                        self.conn.execute(
                            "UPDATE capture_intervals
                             SET connected_at = LEAST(connected_at, ?3)
                             WHERE exchange = ?1 AND symbol = ?2
                               AND closed_at IS NULL
                               AND ?3 <= COALESCE((
                                   SELECT MAX(observed_through)
                                   FROM capture_intervals
                                   WHERE exchange = ?1 AND symbol = ?2
                                     AND closed_at IS NOT NULL
                               ), -1)",
                            duckdb::params![exchange, symbol, from],
                        )?;
                    }
                    PersistableData::CaptureObserved { ticker, through } => {
                        let (exchange, symbol) = ticker_storage_key(*ticker);
                        self.conn.execute(
                            "UPDATE capture_intervals
                             SET observed_through = GREATEST(COALESCE(observed_through, ?3), ?3)
                             WHERE exchange = ?1 AND symbol = ?2 AND closed_at IS NULL",
                            duckdb::params![exchange, symbol, through],
                        )?;
                    }
                    PersistableData::CaptureDisconnected { ticker, at } => {
                        let (exchange, symbol) = ticker_storage_key(*ticker);
                        self.conn.execute(
                            "UPDATE capture_intervals SET closed_at = ?3
                             WHERE exchange = ?1 AND symbol = ?2 AND closed_at IS NULL",
                            duckdb::params![exchange, symbol, at],
                        )?;
                    }
                }
            }
            Ok(())
        })();

        match result {
            Ok(()) => self
                .conn
                .execute_batch("COMMIT")
                .context("committing trade batch"),
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }
}

fn ticker_storage_key(ticker: Ticker) -> (String, String) {
    (
        ticker.exchange.to_string(),
        ticker
            .display_symbol()
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| ticker.to_string().to_ascii_lowercase()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::TradeQuery;

    fn test_dir(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "flowsurface-server-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn linear_query(venue: &str, symbol: &str, from: i64, to: i64) -> TradeQuery {
        TradeQuery {
            venue: venue.into(),
            market: "linear".into(),
            symbol: symbol.into(),
            from: Some(from),
            to: Some(to),
            limit: None,
            stable_order: false,
        }
    }

    fn trade_at(ticker: Ticker, at: i64) -> PersistableData {
        PersistableData::Trade(AnnotatedTrade::new(
            ticker,
            flowsurface_exchange::Trade {
                time: UnixMs::new(at as u64),
                is_sell: false,
                price: Price::from_f64(100.0),
                qty: Qty::from_f64(1.0),
            },
        ))
    }

    fn arrow_rows(bytes: &[u8]) -> Vec<(i64, u64, u64, bool)> {
        use arrow::array::{BooleanArray, Float64Array, Int64Array};
        let reader = arrow::ipc::reader::StreamReader::try_new(bytes, None).unwrap();
        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let ts = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let price = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let qty = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let sell = batch
                .column(3)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((
                    ts.value(i),
                    price.value(i).to_bits(),
                    qty.value(i).to_bits(),
                    sell.value(i),
                ));
            }
        }
        rows
    }

    #[test]
    fn removing_legacy_trade_index_preserves_trades_and_coverage() {
        let dir = test_dir("legacy-index-migration");
        let storage = Storage::open(&dir, 64, 1, 0).unwrap();
        let ticker = Ticker::new("btcusdt", Exchange::BinanceLinear);
        storage
            .open_writer()
            .unwrap()
            .flush_all(&[
                PersistableData::CaptureConnected { ticker, at: 100 },
                trade_at(ticker, 101),
                trade_at(ticker, 101),
                trade_at(ticker, 102),
                PersistableData::CaptureObserved {
                    ticker,
                    through: 200,
                },
            ])
            .unwrap();
        let conn = storage.connection().unwrap();
        conn.execute_batch("CREATE INDEX idx_trades_ex_sym_t ON trades (exchange, symbol, ts)")
            .unwrap();
        let query = linear_query("binance", "btcusdt", 100, 200);
        let before = arrow_rows(&Storage::query_trades_arrow_ipc(&conn, &query).unwrap());
        let coverage = storage.trade_coverage_segments(&query).unwrap();
        storage.set_metadata("history-test", "preserve-me").unwrap();
        drop(conn);
        drop(storage);

        let storage = Storage::open(&dir, 64, 1, 0).unwrap();
        let conn = storage.connection().unwrap();
        let indexes: i64 = conn
            .query_row(
                "SELECT count(*) FROM duckdb_indexes() WHERE index_name = 'idx_trades_ex_sym_t'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexes, 0);
        assert_eq!(
            arrow_rows(&Storage::query_trades_arrow_ipc(&conn, &query).unwrap()),
            before
        );
        assert_eq!(storage.count_trades().unwrap(), 3);
        assert_eq!(storage.trade_coverage_segments(&query).unwrap(), coverage);
        assert_eq!(
            storage.get_metadata("history-test").unwrap().as_deref(),
            Some("preserve-me")
        );
        // A second open must remain idempotent and retain identical duplicates.
        drop(conn);
        drop(storage);
        let storage = Storage::open(&dir, 64, 1, 0).unwrap();
        assert_eq!(storage.count_trades().unwrap(), 3);
        drop(storage);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn streamed_arrow_preserves_bounds_limits_direction_and_duplicate_rows() {
        let dir = test_dir("streamed-history-parity");
        let storage = Storage::open(&dir, 64, 1, 0).unwrap();
        let conn = storage.connection().unwrap();
        conn.execute_batch("INSERT INTO trades SELECT 'Binance Linear', 'btcusdt', 100 + i // 2, 60000.125, 0.001, i % 4 < 2 FROM range(50006) t(i)").unwrap();
        for (from, to, limit, ascending, expected_count, first, last) in [
            (Some(100), Some(25102), None, true, 50000, 100, 25099),
            (
                Some(100),
                Some(25102),
                Some(400000),
                true,
                50006,
                100,
                25102,
            ),
            (
                Some(100),
                Some(25102),
                Some(usize::MAX),
                true,
                50006,
                100,
                25102,
            ),
            (Some(1124), Some(1125), Some(400000), true, 4, 1124, 1125),
            (None, Some(25102), Some(4), false, 4, 25102, 25101),
            (Some(100), None, Some(4), true, 4, 100, 101),
            (Some(100), Some(200), Some(0), true, 0, 0, 0),
            (Some(30000), Some(40000), Some(400000), true, 0, 0, 0),
        ] {
            let query = TradeQuery {
                venue: "binance".into(),
                market: "linear".into(),
                symbol: "BTCUSDT".into(),
                from,
                to,
                limit,
                stable_order: false,
            };
            let bytes = Storage::query_trades_arrow_ipc(&conn, &query).unwrap();
            let rows = arrow_rows(&bytes);
            assert_eq!(rows.len(), expected_count);
            if let (Some(a), Some(b)) = (rows.first(), rows.last()) {
                assert_eq!((a.0, b.0), (first, last));
            }
            assert!(rows.windows(2).all(|pair| if ascending {
                pair[0].0 <= pair[1].0
            } else {
                pair[0].0 >= pair[1].0
            }));
            assert!(
                rows.iter()
                    .all(|row| row.1 == 60000.125_f64.to_bits() && row.2 == 0.001_f64.to_bits())
            );
            // Two identical execution records at each timestamp survive every
            // batch boundary. Timestamp order alone is the existing contract.
            if ascending && rows.len().is_multiple_of(2) {
                assert!(rows.chunks_exact(2).all(|pair| pair[0] == pair[1]));
            }
        }
        drop(conn);
        drop(storage);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn history_windows_keep_boundaries_duplicates_gaps_and_extreme_bounds() {
        let dir = test_dir("history-window-boundaries");
        let storage = Storage::open(&dir, 64, 1, 0).unwrap();
        let conn = storage.connection().unwrap();
        let w = HISTORY_SORT_WINDOW_MS;
        let timestamps = [
            0,
            w - 1,
            w - 1,
            w,
            w,
            w + 1,
            2 * w - 1,
            2 * w,
            4 * w + 1,
            i64::MIN,
            i64::MIN + 1,
            i64::MAX - 1,
            i64::MAX,
        ];
        for ts in timestamps {
            conn.execute("INSERT INTO trades VALUES ('Binance Linear', 'btcusdt', ?1, 60000.0, 0.001, false)", duckdb::params![ts]).unwrap();
        }
        for (from, to, limit, expected) in [
            (
                Some(0),
                Some(4 * w + 1),
                400000,
                vec![0, w - 1, w - 1, w, w, w + 1, 2 * w - 1, 2 * w, 4 * w + 1],
            ),
            (Some(w - 1), Some(w), 400000, vec![w - 1, w - 1, w, w]),
            (Some(0), Some(4 * w + 1), 3, vec![0, w - 1, w - 1]),
            (
                Some(i64::MIN),
                Some(i64::MIN + 1),
                400000,
                vec![i64::MIN, i64::MIN + 1],
            ),
            (
                Some(i64::MAX - 1),
                None,
                400000,
                vec![i64::MAX - 1, i64::MAX],
            ),
            (None, Some(i64::MAX), 2, vec![i64::MAX, i64::MAX - 1]),
        ] {
            let query = TradeQuery {
                venue: "binance".into(),
                market: "linear".into(),
                symbol: "btcusdt".into(),
                from,
                to,
                limit: Some(limit),
                stable_order: false,
            };
            let rows = arrow_rows(&Storage::query_trades_arrow_ipc(&conn, &query).unwrap());
            assert_eq!(rows.iter().map(|row| row.0).collect::<Vec<_>>(), expected);
        }
        // Remove extreme fixtures so descending reads can also cross empty windows.
        conn.execute(
            "DELETE FROM trades WHERE ts < 0 OR ts > ?1",
            duckdb::params![4 * w + 1],
        )
        .unwrap();
        let query = TradeQuery {
            venue: "binance".into(),
            market: "linear".into(),
            symbol: "btcusdt".into(),
            from: None,
            to: None,
            limit: Some(400000),
            stable_order: false,
        };
        let expected = vec![4 * w + 1, 2 * w, 2 * w - 1, w + 1, w, w, w - 1, w - 1, 0];
        assert_eq!(
            arrow_rows(&Storage::query_trades_arrow_ipc(&conn, &query).unwrap())
                .iter()
                .map(|row| row.0)
                .collect::<Vec<_>>(),
            expected
        );
        drop(conn);
        drop(storage);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn history_windows_share_one_snapshot_during_live_insert_and_purge() {
        use arrow::array::Int64Array;
        let dir = test_dir("history-window-snapshot");
        let storage = Storage::open(&dir, 64, 1, 0).unwrap();
        let ticker = Ticker::new("btcusdt", Exchange::BinanceLinear);
        storage
            .open_writer()
            .unwrap()
            .flush_all(&[
                trade_at(ticker, 0),
                trade_at(ticker, 2 * HISTORY_SORT_WINDOW_MS),
            ])
            .unwrap();
        let conn = storage.connection().unwrap();
        let query = linear_query("binance", "btcusdt", 0, 3 * HISTORY_SORT_WINDOW_MS);
        let mut timestamps = Vec::new();
        let mut changed = false;
        Storage::for_each_trade_batch(&conn, &query, 400000, |batch| {
            timestamps.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied(),
            );
            if !changed {
                storage
                    .open_writer()
                    .unwrap()
                    .flush_all(&[trade_at(ticker, HISTORY_SORT_WINDOW_MS + 1)])
                    .unwrap();
                storage
                    .connection()
                    .unwrap()
                    .execute(
                        "DELETE FROM trades WHERE ts = ?1",
                        duckdb::params![2 * HISTORY_SORT_WINDOW_MS],
                    )
                    .unwrap();
                changed = true;
            }
            Ok(())
        })
        .unwrap();
        assert!(changed);
        assert_eq!(timestamps, vec![0, 2 * HISTORY_SORT_WINDOW_MS]);
        let current = arrow_rows(&Storage::query_trades_arrow_ipc(&conn, &query).unwrap());
        assert_eq!(
            current.iter().map(|row| row.0).collect::<Vec<_>>(),
            vec![0, HISTORY_SORT_WINDOW_MS + 1]
        );
        drop(conn);
        drop(storage);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn timed_out_history_retains_admission_and_does_not_block_writer() {
        let dir = test_dir("history-timeout-admission");
        let storage = Storage::open(&dir, 64, 1, 0).unwrap();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let (release, held) = std::sync::mpsc::channel();
        let (started, running) = tokio::sync::oneshot::channel();
        let query = tokio::spawn(storage.clone().run_history_query(
            Duration::from_millis(500),
            "test",
            permit,
            move |_| {
                started.send(()).unwrap();
                held.recv().unwrap();
                Ok(())
            },
        ));
        running.await.unwrap();
        assert_eq!(
            query.await.unwrap(),
            Err((StatusCode::SERVICE_UNAVAILABLE, "query timed out"))
        );
        assert_eq!(semaphore.available_permits(), 0);
        storage
            .open_writer()
            .unwrap()
            .flush_all(&[trade_at(
                Ticker::new("btcusdt", Exchange::BinanceLinear),
                123,
            )])
            .unwrap();
        assert_eq!(storage.count_trades().unwrap(), 1);
        release.send(()).unwrap();
        let permit = tokio::time::timeout(Duration::from_secs(3), semaphore.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        drop(storage);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn cancelling_history_interrupts_only_its_own_connection() {
        let dir = test_dir("history-cancel-interrupt");
        let storage = Storage::open(&dir, 64, 1, 0).unwrap();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let (started, running) = tokio::sync::oneshot::channel();
        let (finished, result) = tokio::sync::oneshot::channel();
        let query = tokio::spawn(storage.clone().run_history_query(
            Duration::from_secs(30),
            "test",
            permit,
            move |conn| {
                started.send(()).unwrap();
                let result = conn.query_row(
                    "SELECT sum(a.i * b.i) FROM range(1000000000) a(i), range(1000) b(i)",
                    [],
                    |row| row.get::<_, i128>(0),
                );
                finished
                    .send(result.as_ref().map(|_| ()).map_err(ToString::to_string))
                    .unwrap();
                Ok(())
            },
        ));
        running.await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        // The writer uses a separate connection and never waits for history admission.
        storage
            .open_writer()
            .unwrap()
            .flush_all(&[trade_at(
                Ticker::new("btcusdt", Exchange::BinanceLinear),
                123,
            )])
            .unwrap();
        query.abort();
        assert!(query.await.unwrap_err().is_cancelled());
        let error = tokio::time::timeout(Duration::from_secs(3), result)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_lowercase().contains("interrupt"), "{error}");
        let permit = tokio::time::timeout(Duration::from_secs(3), semaphore.acquire())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(storage.count_trades().unwrap(), 1);
        drop(permit);
        drop(storage);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn history_burst_preserves_every_accepted_live_execution() {
        use flowsurface_exchange::adapter::StreamKind;
        let dir = test_dir("history-burst-live-recording");
        let storage = Storage::open(&dir, 64, 1, 0).unwrap();
        let conn = storage.connection().unwrap();
        conn.execute_batch("INSERT INTO trades SELECT 'Binance Linear', 'btcusdt', i, 60000.0, 0.001, false FROM range(60000) t(i)").unwrap();
        drop(conn);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            crate::api::query_concurrency_limit(64),
        ));
        let mut readers = Vec::new();
        for _ in 0..8 {
            let semaphore = semaphore.clone();
            let store = storage.clone();
            readers.push(tokio::spawn(async move {
                let permit = semaphore.acquire_owned().await.unwrap();
                let query = linear_query("binance", "btcusdt", 0, 59999);
                let bytes = store
                    .run_history_query(Duration::from_secs(10), "test", permit, move |conn| {
                        Storage::query_trades_arrow_ipc(conn, &query)
                    })
                    .await
                    .unwrap();
                assert_eq!(arrow_rows(&bytes).len(), 50_000);
            }));
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let mut receivers = stream::StreamReceivers {
            persist: Some(receiver),
        };
        let flusher = storage.spawn_batch_flusher(&mut receivers, Duration::from_millis(2), 500);
        let info = TickerInfo::new(
            Ticker::new("btcusdt", Exchange::BinanceLinear),
            0.1,
            0.001,
            None,
        );
        for batch in 0..100 {
            let at = UnixMs::new(1_000_000 + batch);
            let trade = flowsurface_exchange::Trade {
                time: at,
                is_sell: batch % 2 == 0,
                price: Price::from_f64(60001.0),
                qty: Qty::from_f64(0.001),
            };
            sender
                .send(Event::TradesReceived(
                    StreamKind::Trades { ticker_info: info },
                    at,
                    vec![trade; 100].into_boxed_slice(),
                ))
                .await
                .unwrap();
        }
        drop(sender);
        flusher.await.unwrap();
        for reader in readers {
            reader.await.unwrap();
        }
        assert_eq!(storage.count_trades().unwrap(), 70_000);
        let conn = storage.connection().unwrap();
        let counts: Vec<(i64, i64)> = conn
            .prepare("SELECT ts, count(*) FROM trades WHERE ts >= 1000000 GROUP BY ts ORDER BY ts")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<duckdb::Result<_>>()
            .unwrap();
        assert_eq!(
            counts,
            (0..100)
                .map(|batch| (1_000_000 + batch, 100))
                .collect::<Vec<_>>()
        );
        drop(conn);
        drop(storage);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    #[ignore = "manual bounded-memory history qualification"]
    fn history_memory_pressure_probe() {
        let dir = test_dir("history-memory-probe");
        let storage = Storage::open(&dir, 512, 2, 4096).unwrap();
        let conn = storage.connection().unwrap();
        for batch in 0..52 {
            let from = batch * 500_000;
            let to = from + 500_000;
            let result = conn.execute_batch(&format!(
                "INSERT INTO trades SELECT \
                 CASE i % 3 WHEN 0 THEN 'Binance Linear' WHEN 1 THEN 'Bybit Linear' \
                 ELSE 'Hyperliquid Linear' END, \
                 CASE i % 3 WHEN 2 THEN 'btcusdc' ELSE 'btcusdt' END, \
                 1790000000000 + i // 2, 60000.0 + i % 1000, 0.001 + (i % 10) * 0.001, \
                 i % 2 = 0 FROM range({from}, {to}) t(i)"
            ));
            if let Err(error) = result {
                panic!("fixture insert failed at {from}: {error}");
            }
            if batch % 4 == 3 {
                let memory: i64 = conn
                    .query_row(
                        "SELECT sum(memory_usage_bytes) FROM duckdb_memory()",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                eprintln!("probe rows={to} memory_bytes={memory}");
            }
        }
        let mut query = linear_query("binance", "btcusdt", 1790000000000, 1790010000000);
        query.limit = Some(400_000);
        let result = Storage::query_trades_arrow_ipc(&conn, &query);
        eprintln!("probe history result={:?}", result.as_ref().map(Vec::len));
        let rows = arrow_rows(&result.unwrap());
        assert_eq!(rows.len(), 400_000);
        for (n, &(ts, price, qty, sell)) in rows.iter().enumerate() {
            let i = n as i64 * 3;
            assert_eq!(ts, 1790000000000 + i / 2);
            assert_eq!(price, (60000.0 + (i % 1000) as f64).to_bits());
            assert_eq!(qty, ((1 + i % 10) as f64 / 1000.0).to_bits());
            assert_eq!(sell, i % 2 == 0);
        }
        assert_eq!(storage.count_trades().unwrap(), 26_000_000);
        drop(conn);
        drop(storage);
        if std::env::var_os("FLOWSURFACE_KEEP_HISTORY_FIXTURE").is_some() {
            eprintln!("fixture_directory={}", dir.display());
        } else {
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    #[ignore = "manual HTTP qualification; set FLOWSURFACE_HISTORY_PAGES_DIR"]
    fn verify_history_http_pages() {
        use sha2::{Digest, Sha256};
        let dir = PathBuf::from(
            std::env::var_os("FLOWSURFACE_HISTORY_PAGES_DIR").expect("pages directory"),
        );
        let mut verified = std::collections::HashSet::new();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|value| value.to_str()) != Some("arrow") {
                continue;
            }
            let filename = path.file_name().unwrap().to_str().unwrap();
            let venue_offset = if filename.contains("binance") {
                0
            } else if filename.contains("bybit") {
                1
            } else if filename.contains("hyperliquid") {
                2
            } else {
                panic!("unknown page {filename}")
            };
            let bytes = std::fs::read(&path).unwrap();
            let rows = arrow_rows(&bytes);
            assert_eq!(rows.len(), 400_000, "{filename}");
            for (n, &(ts, price, qty, sell)) in rows.iter().enumerate() {
                let i = n as i64 * 3 + venue_offset;
                assert_eq!(
                    (ts, price, qty, sell),
                    (
                        1790000000000 + i / 2,
                        (60000.0 + (i % 1000) as f64).to_bits(),
                        ((1 + i % 10) as f64 / 1000.0).to_bits(),
                        i % 2 == 0
                    ),
                    "{filename} row {n}"
                );
            }
            let hash: String = Sha256::digest(&bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            verified.insert((venue_offset, hash));
        }
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join("report.json")).unwrap()).unwrap();
        let requests = report["requests"].as_array().unwrap();
        assert_eq!(requests.len(), 24);
        for request in requests {
            assert_eq!(request["status"], 200);
            let filename = request["filename"].as_str().unwrap();
            let venue = if filename.contains("binance") {
                0
            } else if filename.contains("bybit") {
                1
            } else {
                assert!(filename.contains("hyperliquid"));
                2
            };
            assert!(
                verified.contains(&(venue, request["sha256"].as_str().unwrap().to_string())),
                "unverified payload {filename}"
            );
        }
        let count = requests.len();
        eprintln!(
            "verified {count} HTTP pages, {} exact trade rows",
            count * 400_000
        );
    }

    #[test]
    fn capture_coverage_keeps_disconnect_gap_explicit() {
        let dir = test_dir("coverage");
        let storage = Storage::open(&dir, 0, 1, 0).expect("test storage");
        let ticker = Ticker::new("btcusdt", Exchange::BinanceLinear);
        {
            let mut writer = storage.open_writer().expect("writer");
            writer
                .flush_all(&[
                    PersistableData::CaptureConnected { ticker, at: 100 },
                    PersistableData::CaptureObserved {
                        ticker,
                        through: 150,
                    },
                    PersistableData::CaptureDisconnected { ticker, at: 160 },
                    PersistableData::CaptureConnected { ticker, at: 200 },
                    PersistableData::CaptureObserved {
                        ticker,
                        through: 250,
                    },
                ])
                .expect("coverage batch");
        }
        let query = linear_query("binance", "btcusdt", 90, 300);
        assert_eq!(
            storage.trade_coverage_segments(&query).expect("coverage"),
            vec![
                CoverageSegment { from: 100, to: 150 },
                CoverageSegment { from: 200, to: 250 },
            ]
        );
        drop(storage);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn replay_overlap_bridges_capture_coverage() {
        let dir = test_dir("coverage-replay-bridge");
        let storage = Storage::open(&dir, 0, 1, 0).expect("test storage");
        let ticker = Ticker::new("btcusdc", Exchange::HyperliquidLinear);
        {
            let mut writer = storage.open_writer().expect("writer");
            writer
                .flush_all(&[
                    PersistableData::CaptureConnected { ticker, at: 100 },
                    PersistableData::CaptureObserved {
                        ticker,
                        through: 150,
                    },
                    PersistableData::CaptureDisconnected { ticker, at: 160 },
                    PersistableData::CaptureConnected { ticker, at: 200 },
                    PersistableData::CaptureRecovered { ticker, from: 140 },
                    PersistableData::CaptureObserved {
                        ticker,
                        through: 250,
                    },
                ])
                .expect("coverage batch");
        }

        let query = linear_query("hyperliquid", "btcusdc", 90, 300);
        assert_eq!(
            storage.trade_coverage_segments(&query).expect("coverage"),
            vec![CoverageSegment { from: 100, to: 250 }]
        );
        drop(storage);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn replay_without_prior_overlap_keeps_gap_explicit() {
        let dir = test_dir("coverage-replay-no-overlap");
        let storage = Storage::open(&dir, 0, 1, 0).expect("test storage");
        let ticker = Ticker::new("btcusdc", Exchange::HyperliquidLinear);
        {
            let mut writer = storage.open_writer().expect("writer");
            writer
                .flush_all(&[
                    PersistableData::CaptureConnected { ticker, at: 100 },
                    PersistableData::CaptureObserved {
                        ticker,
                        through: 150,
                    },
                    PersistableData::CaptureDisconnected { ticker, at: 160 },
                    PersistableData::CaptureConnected { ticker, at: 200 },
                    PersistableData::CaptureRecovered { ticker, from: 170 },
                    PersistableData::CaptureObserved {
                        ticker,
                        through: 250,
                    },
                ])
                .expect("coverage batch");
        }

        let query = linear_query("hyperliquid", "btcusdc", 90, 300);
        assert_eq!(
            storage.trade_coverage_segments(&query).expect("coverage"),
            vec![
                CoverageSegment { from: 100, to: 150 },
                CoverageSegment { from: 200, to: 250 },
            ]
        );
        drop(storage);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn legacy_hyperliquid_capture_semantics_are_not_certified() {
        let dir = test_dir("legacy-hyperliquid-semantics");
        let storage = Storage::open(&dir, 0, 1, 0).expect("test storage");
        {
            let conn = storage.connection().expect("connection");
            for (exchange, symbol) in [
                (Exchange::HyperliquidLinear, "btcusdc"),
                (Exchange::BinanceLinear, "btcusdt"),
            ] {
                conn.execute(
                    "INSERT INTO capture_intervals
                         (exchange, symbol, connected_at, observed_through, closed_at,
                          semantics_version)
                     VALUES (?1, ?2, 100, 150, 160, 1)",
                    duckdb::params![exchange.to_string(), symbol],
                )
                .expect("legacy interval");
            }
        }

        assert!(
            storage
                .trade_coverage_segments(&linear_query("hyperliquid", "btcusdc", 90, 200))
                .expect("hyperliquid coverage")
                .is_empty()
        );
        assert_eq!(
            storage
                .trade_coverage_segments(&linear_query("binance", "btcusdt", 90, 200))
                .expect("binance coverage"),
            vec![CoverageSegment { from: 100, to: 150 }]
        );

        drop(storage);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn retention_purge_clips_coverage_and_rows_in_one_transaction() {
        let dir = test_dir("retention-floor");
        let storage = Storage::open(&dir, 0, 1, 0).expect("test storage");
        let ticker = Ticker::new("btcusdt", Exchange::BinanceLinear);
        let now = storage.now_ms();
        let old = now - 2 * 3_600_000;
        let recent = now - 30 * 60_000;
        {
            let mut writer = storage.open_writer().expect("writer");
            writer
                .flush_all(&[
                    PersistableData::CaptureConnected { ticker, at: old },
                    trade_at(ticker, old),
                    trade_at(ticker, recent),
                    PersistableData::CaptureObserved {
                        ticker,
                        through: recent,
                    },
                ])
                .expect("retention fixture");
        }

        let purge = storage
            .purge_old_trades(RetentionHours::new(1).expect("retention"))
            .expect("retention purge");
        assert_eq!(purge.deleted, 1);
        assert!(purge.retain_from > old && purge.retain_from < recent);

        let query = linear_query("binance", "btcusdt", old, recent);
        let conn = storage.connection().expect("query connection");
        let rows = Storage::query_trades(&conn, &query).expect("remaining trades");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].trade.time.as_u64(), recent as u64);
        assert_eq!(
            storage.trade_coverage_segments(&query).expect("coverage"),
            vec![CoverageSegment {
                from: purge.retain_from as u64,
                to: recent as u64,
            }]
        );

        // Advancing retention with no additional deletions must still advance
        // the proof floor; zero-trade time is not retained forever.
        let later = storage
            .purge_old_trades(RetentionHours::new(1).expect("retention"))
            .expect("second retention purge");
        assert_eq!(later.deleted, 0);
        assert!(later.retain_from >= purge.retain_from);

        drop(conn);
        drop(storage);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn size_purge_advances_only_affected_source_coverage() {
        let dir = test_dir("size-floor");
        let storage = Storage::open(&dir, 0, 1, 0).expect("test storage");
        let binance = Ticker::new("btcusdt", Exchange::BinanceLinear);
        let bybit = Ticker::new("BTCUSDT", Exchange::BybitLinear);
        {
            let mut writer = storage.open_writer().expect("writer");
            writer
                .flush_all(&[
                    PersistableData::CaptureConnected {
                        ticker: binance,
                        at: 50,
                    },
                    PersistableData::CaptureObserved {
                        ticker: binance,
                        through: 300,
                    },
                    PersistableData::CaptureConnected {
                        ticker: bybit,
                        at: 50,
                    },
                    PersistableData::CaptureObserved {
                        ticker: bybit,
                        through: 300,
                    },
                    trade_at(binance, 100),
                    trade_at(binance, 100),
                    trade_at(bybit, 150),
                    trade_at(binance, 200),
                ])
                .expect("size fixture");
        }

        assert_eq!(storage.delete_oldest_trades_batch(2).expect("purge"), 2);
        let binance_query = linear_query("binance", "btcusdt", 50, 300);
        let bybit_query = linear_query("bybit", "btcusdt", 50, 300);
        assert_eq!(
            storage
                .trade_coverage_segments(&binance_query)
                .expect("binance coverage"),
            vec![CoverageSegment { from: 101, to: 300 }]
        );
        assert_eq!(
            storage
                .trade_coverage_segments(&bybit_query)
                .expect("bybit coverage"),
            vec![CoverageSegment { from: 50, to: 300 }]
        );

        assert_eq!(storage.delete_oldest_trades_batch(1).expect("purge"), 1);
        assert_eq!(
            storage
                .trade_coverage_segments(&bybit_query)
                .expect("bybit coverage"),
            vec![CoverageSegment { from: 151, to: 300 }]
        );

        // Floors are durable across recorder restarts.
        drop(storage);
        let reopened = Storage::open(&dir, 0, 1, 0).expect("reopened storage");
        assert_eq!(
            reopened
                .trade_coverage_segments(&binance_query)
                .expect("reopened coverage"),
            vec![CoverageSegment { from: 101, to: 300 }]
        );
        let usage = reopened.storage_usage().expect("storage usage");
        assert!(usage.physical.as_bytes() > 0);
        assert!(usage.active.as_bytes() > 0);

        drop(reopened);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn data_buffer_never_discards_accepted_rows() {
        let ticker = Ticker::new("btcusdt", Exchange::BinanceLinear);
        let trade = flowsurface_exchange::Trade {
            time: UnixMs::new(1),
            is_sell: false,
            price: Price::from_f64(1.0),
            qty: Qty::from_f64(1.0),
        };
        let mut buffer = DataBuffer::new();
        for _ in 0..10_001 {
            buffer.push(PersistableData::Trade(AnnotatedTrade::new(ticker, trade)));
        }
        assert_eq!(buffer.len(), 10_001);
        assert_eq!(buffer.take().len(), 10_001);
    }

    #[test]
    fn opt_in_source_order_is_stable_across_queries_limits_and_directions() {
        let dir = test_dir("trev-stable-order");
        let storage = Storage::open(&dir, 64, 2, 0).unwrap();
        let conn = storage.connection().unwrap();
        conn.execute_batch("INSERT INTO trades SELECT 'Binance Linear', 'btcusdt', 1000+i//5, 60000.0+i, 0.001, i%2=0 FROM range(60000) t(i)").unwrap();
        for from in [1000, 1001, 2000] {
            let mut query = linear_query("binance", "btcusdt", from, 13000);
            query.stable_order = true;
            query.limit = Some(50000);
            let expected_start = (from - 1000) * 5;
            for _ in 0..3 {
                let rows = arrow_rows(&Storage::query_trades_arrow_ipc(&conn, &query).unwrap());
                for (i, row) in rows.iter().enumerate() {
                    assert_eq!(
                        f64::from_bits(row.1),
                        60000. + (expected_start + i as i64) as f64
                    );
                }
            }
        }
        let mut query = linear_query("binance", "btcusdt", 1000, 13000);
        query.from = None;
        query.stable_order = true;
        query.limit = Some(50000);
        let rows = arrow_rows(&Storage::query_trades_arrow_ipc(&conn, &query).unwrap());
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(f64::from_bits(row.1), 60000. + (59999 - i) as f64);
        }
        assert_eq!(storage.count_trades().unwrap(), 60000);
        conn.execute_batch("CHECKPOINT").unwrap();
        drop(conn);
        drop(storage);
        let reopened = Storage::open(&dir, 64, 2, 0).unwrap();
        let conn = reopened.connection().unwrap();
        assert_eq!(
            arrow_rows(&Storage::query_trades_arrow_ipc(&conn, &query).unwrap()),
            rows,
            "a recorder reopen must preserve the execution sequence"
        );
        assert_eq!(reopened.count_trades().unwrap(), 60000);
        drop(conn);
        drop(reopened);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
