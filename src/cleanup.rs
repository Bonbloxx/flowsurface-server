use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    config::{RetentionHours, StorageBytes},
    storage::Storage,
};

/// Floor on delay between cleanup passes; prevents tight retry loops
/// when `last_cleanup` is stale (first run or previous pass failed).
const MIN_CLEANUP_DELAY: Duration = Duration::from_secs(60);

/// Wake interval for the size-cap check.  Only matters when a size cap
/// is configured and the retention window is longer than this.
const SIZE_CHECK_INTERVAL: Duration = Duration::from_secs(300);

/// Safety ceiling for the size-based purge loop (200 x 100k = 20M rows).
const MAX_PURGE_ITERATIONS: usize = 200;

/// Re-check real DuckDB usage after at most this many delete batches.  This
/// bounds WAL growth and makes the size decision evidence-based rather than a
/// one-shot row-size estimate.
const MEASURE_EVERY_BATCHES: usize = 20;

/// On small-to-medium caps use a 20 % headroom so the purge leaves
/// breathing room.  On very large caps the percentage would waste
/// too much space, so we cap the absolute headroom at 10 GiB
const MAX_HEADROOM: StorageBytes = StorageBytes::from_bytes(10 * 1024 * 1024 * 1024);

/// Smallest sane target after headroom subtraction — prevents
/// pathological behaviour on tiny caps.
const MIN_TARGET: StorageBytes = StorageBytes::from_bytes(100 * 1024 * 1024);

/// Compute the headroom to leave free below the cap.
/// Returns `cap / 5` (20 %) but at most `MAX_HEADROOM`.
fn purge_headroom(cap: StorageBytes) -> StorageBytes {
    let pct = cap.as_bytes() / 5;
    if pct > MAX_HEADROOM.as_bytes() {
        MAX_HEADROOM
    } else {
        StorageBytes::from_bytes(pct)
    }
}

const MIN_PURGE_ROWS: u64 = 100_000;
const MAX_PURGE_ROWS: u64 = 100_000;

#[derive(Debug, Clone, Copy)]
pub struct CleanupConfig {
    /// Trades older than this are purged.
    pub retention_hours: RetentionHours,
    /// Optional hard cap on total DB+WAL size in bytes.
    pub max_storage_bytes: Option<StorageBytes>,
}

impl CleanupConfig {
    pub fn from_config(retention_hours: u64, max_storage_mb: u64) -> anyhow::Result<Self> {
        let retention_hours = RetentionHours::new(retention_hours)?;
        let max_storage_bytes = if max_storage_mb > 0 {
            Some(StorageBytes::from_mb(max_storage_mb))
        } else {
            None
        };
        Ok(Self {
            retention_hours,
            max_storage_bytes,
        })
    }
}

#[derive(Clone)]
pub struct CleanupScheduler {
    storage: Storage,
    config: CleanupConfig,
}

impl CleanupScheduler {
    /// Create the scheduler and check the startup guard: if the database
    /// is > 2× the configured cap the process exits (this is a
    /// misconfiguration that would take too long to recover from at
    /// startup).
    ///
    /// Returns an error when the storage size cannot be read at all.
    pub fn new(storage: &Storage, config: CleanupConfig) -> anyhow::Result<Self> {
        if config.retention_hours.as_hours() > 0 {
            tracing::info!(
                "Data retention enabled: trades older than {}h ({} days) \
                 will be purged during cleanup cycles.",
                config.retention_hours.as_hours(),
                config.retention_hours.as_hours() / 24,
            );
        };

        if let Some(max) = config.max_storage_bytes {
            tracing::info!(
                "Storage hard cap enabled: {} MB (will purge oldest trades when exceeded)",
                max.as_mb(),
            );

            match storage.current_storage_bytes() {
                Ok(current) if current > max.saturating_mul(2) => {
                    anyhow::bail!(
                        "Database is {} MB — more than 2x the configured \
                         max_storage_mb cap ({} MB).  Purging that much data \
                         at startup would take too long; this likely indicates \
                         a misconfiguration.  Either raise max_storage_mb \
                         (e.g. to {} MB or higher) or manually shrink the \
                         database and restart.",
                        current.as_mb(),
                        max.as_mb(),
                        current.as_mb().saturating_add(1),
                    );
                }
                Ok(current) if current > max => {
                    tracing::warn!(
                        "Database is {} MB — above the {} MB cap; \
                         startup cleanup will purge oldest trades.",
                        current.as_mb(),
                        max.as_mb(),
                    );
                }
                Err(e) => {
                    anyhow::bail!("Failed to check storage size at startup: {e:#}");
                }
                _ => {}
            }
        }

        Ok(Self {
            storage: storage.clone(),
            config,
        })
    }

    /// Run one cleanup pass: time-based retention, optional size-cap
    /// purge, checkpoint, and record `last_cleanup`.
    ///
    /// Returns `Some(last_cleanup_ts)` on success, or `None` if the
    /// pass failed (e.g. time-based purge errored or recording the
    /// timestamp failed).
    pub fn run_pass(&self) -> Option<i64> {
        let mut any_deleted = false;
        let mut time_cleanup_ok = false;

        match self.storage.purge_old_trades(self.config.retention_hours) {
            Ok(purge) => {
                time_cleanup_ok = true;
                if purge.deleted > 0 {
                    tracing::info!(
                        "Cleaned up {} trade(s) older than {}h; coverage now starts no earlier than {}",
                        purge.deleted,
                        self.config.retention_hours.as_hours(),
                        purge.retain_from
                    );
                    any_deleted = true;
                }
            }
            Err(e) => {
                tracing::error!("Data retention cleanup failed: {e:#}");
            }
        }

        if let Some(max_bytes) = self.config.max_storage_bytes {
            match self.purge_oldest_trades_until_below(max_bytes) {
                Ok(n) if n > 0 => any_deleted = true,
                Err(e) => tracing::error!("Storage-cap cleanup failed: {e:#}"),
                _ => {}
            }
        }

        if any_deleted && let Err(e) = self.storage.run_checkpoint() {
            tracing::warn!("Failed to checkpoint DuckDB WAL after cleanup: {e:#}");
        }

        if time_cleanup_ok {
            match self.storage.record_cleanup() {
                Ok(ts) => return Some(ts),
                Err(e) => tracing::warn!("Failed to record last_cleanup timestamp: {e:#}"),
            }
        }
        None
    }

    /// Delete oldest trades until DuckDB's live blocks are below the target.
    ///
    /// Targets `cap - headroom` so there is breathing room for incoming
    /// trades between five-minute checks.  Decisions are verified using both
    /// the real DB+WAL filesystem allocation and DuckDB's live block count.
    /// Freed physical blocks are retained by DuckDB for reuse; once live usage
    /// is below target, deleting more history would not shrink that allocation
    /// and is therefore forbidden.
    fn purge_oldest_trades_until_below(&self, max_bytes: StorageBytes) -> Result<u64> {
        let headroom = purge_headroom(max_bytes);
        let minimum = MIN_TARGET.as_bytes().min(max_bytes.as_bytes());
        let target_bytes = max_bytes
            .as_bytes()
            .saturating_sub(headroom.as_bytes())
            .max(minimum);
        let target_bytes = StorageBytes::from_bytes(target_bytes);

        // Cheap fast path: no DuckDB query or checkpoint while the real files
        // are comfortably inside the headroom-adjusted target.
        if self.storage.current_storage_bytes()? <= target_bytes {
            return Ok(0);
        }

        // CHECKPOINT is the operation DuckDB uses to merge the WAL and reclaim
        // sufficiently deleted row groups. VACUUM does not reclaim deletes.
        self.storage.run_checkpoint()?;
        let mut usage = self.storage.storage_usage()?;
        let mut total_deleted = 0u64;
        let mut iterations = 0usize;

        while usage.active > target_bytes && iterations < MAX_PURGE_ITERATIONS {
            let remaining_rows = self.storage.count_trades()?;
            if remaining_rows == 0 {
                anyhow::bail!(
                    "DuckDB live storage is {} MB with no trade rows left; refusing to delete metadata",
                    usage.active.as_mb()
                );
            }

            let excess = usage
                .active
                .as_bytes()
                .saturating_sub(target_bytes.as_bytes());
            let proportional = ((excess as u128)
                .saturating_mul(remaining_rows as u128)
                .saturating_add(usage.active.as_bytes() as u128 - 1)
                / usage.active.as_bytes() as u128) as u64;
            // Add 10% to avoid repeated tiny passes and always remove at least
            // one near-row-group batch so CHECKPOINT can reclaim old groups.
            let planned = proportional
                .saturating_add(proportional / 10)
                .max(MIN_PURGE_ROWS)
                .min(remaining_rows);
            let mut planned_left = planned;
            let measurement_deadline =
                (iterations + MEASURE_EVERY_BATCHES).min(MAX_PURGE_ITERATIONS);

            while planned_left > 0 && iterations < measurement_deadline {
                let batch = planned_left.min(MAX_PURGE_ROWS) as i64;
                let deleted = self.storage.delete_oldest_trades_batch(batch)?;
                if deleted == 0 {
                    break;
                }
                total_deleted = total_deleted.saturating_add(deleted);
                planned_left = planned_left.saturating_sub(deleted);
                iterations += 1;
            }

            if total_deleted == 0 {
                break;
            }
            let previous_active = usage.active;
            self.storage.run_checkpoint()?;
            usage = self.storage.storage_usage()?;
            if usage.active >= previous_active && self.storage.count_trades()? == 0 {
                break;
            }
        }

        if usage.active > target_bytes {
            anyhow::bail!(
                "storage-cap purge stopped after {iterations} batches and {total_deleted} deleted trades: \
                 live usage is still {} MB (target {} MB, physical {} MB)",
                usage.active.as_mb(),
                target_bytes.as_mb(),
                usage.physical.as_mb(),
            );
        }

        if total_deleted > 0 {
            tracing::info!(
                "Cleaned up {total_deleted} oldest trade(s) for the {} MB cap: \
                 live usage {} MB, physical DB+WAL {} MB, target {} MB",
                max_bytes.as_mb(),
                usage.active.as_mb(),
                usage.physical.as_mb(),
                target_bytes.as_mb(),
            );
        }
        if usage.physical > max_bytes {
            tracing::warn!(
                "DuckDB's allocated DB+WAL is {} MB, above the {} MB cap, but live data is {} MB \
                 and freed blocks will be reused. Refusing to delete additional truthful history solely \
                 to chase an allocated file that DuckDB does not shrink in place.",
                usage.physical.as_mb(),
                max_bytes.as_mb(),
                usage.active.as_mb(),
            );
        }

        Ok(total_deleted)
    }

    /// Spawn the periodic background cleanup task.
    ///
    /// A one-shot startup pass must have been run beforehand (via
    /// [`run_startup_pass`](Self::run_startup_pass)) so that
    /// `last_cleanup` is initialised.  The loop then computes the
    /// next wake-up from `last_cleanup` and only polls when work
    /// is actually due (or every `SIZE_CHECK_INTERVAL` when a size
    /// cap is configured).
    pub fn spawn(self, last_cleanup: Option<i64>, shutdown: CancellationToken) -> JoinHandle<()> {
        let storage = self.storage.clone();
        let config = self.config;

        let retention_ms = config.retention_hours.as_millis();

        tokio::spawn(async move {
            let mut last_cleanup = last_cleanup;

            loop {
                let time_delay = {
                    let scheduler = CleanupScheduler {
                        storage: storage.clone(),
                        config,
                    };
                    scheduler.delay_until_next_cleanup(last_cleanup, retention_ms)
                };
                let delay =
                    if config.max_storage_bytes.is_some() && SIZE_CHECK_INTERVAL < time_delay {
                        SIZE_CHECK_INTERVAL
                    } else {
                        time_delay
                    }
                    .max(MIN_CLEANUP_DELAY);

                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        tracing::info!("Periodic cleanup shut down.");
                        break;
                    }
                    _ = tokio::time::sleep(delay) => {
                        let result = tokio::task::spawn_blocking({
                            let storage = storage.clone();
                            move || {
                                CleanupScheduler { storage, config }.run_pass()
                            }
                        })
                        .await
                        .unwrap_or(None);
                        if let Some(ts) = result {
                            last_cleanup = Some(ts);
                        }
                    }
                }
            }
        })
    }

    fn delay_until_next_cleanup(
        &self,
        last_cleanup_ms: Option<i64>,
        retention_ms: i64,
    ) -> Duration {
        let now_ms = self.storage.now_ms();

        let Some(anchor_ms) = last_cleanup_ms else {
            // No recorded timestamp — retry quickly (caller applies
            // `MIN_CLEANUP_DELAY`).
            return Duration::ZERO;
        };

        let next_ms = anchor_ms + retention_ms;
        if next_ms <= now_ms {
            Duration::ZERO
        } else {
            Duration::from_millis((next_ms - now_ms) as u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::TradeQuery;

    fn test_dir(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "flowsurface-server-cleanup-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[test]
    fn size_cap_converges_from_measured_live_blocks_and_clips_coverage() {
        let dir = test_dir("measured-cap");
        std::fs::create_dir_all(&dir).expect("test directory");
        let db_path = dir.join("trades.duckdb");
        {
            let conn = duckdb::Connection::open(&db_path).expect("fixture database");
            conn.execute_batch(
                "CREATE TABLE trades (
                    exchange VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL,
                    ts BIGINT NOT NULL,
                    price DOUBLE NOT NULL,
                    qty DOUBLE NOT NULL,
                    is_sell BOOLEAN NOT NULL
                 );
                 CREATE TABLE capture_intervals (
                    exchange VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL,
                    connected_at BIGINT NOT NULL,
                    observed_through BIGINT,
                    closed_at BIGINT,
                    PRIMARY KEY (exchange, symbol, connected_at)
                 );
                 INSERT INTO capture_intervals VALUES
                    ('binance_linear', 'btcusdt', 0, 499999, 500000);
                 INSERT INTO trades
                 SELECT 'binance_linear',
                        'btcusdt',
                        i,
                        CAST(hash(i) AS DOUBLE),
                        CAST(hash(i * 17) % 100000 AS DOUBLE),
                        i % 2 = 0
                 FROM range(500000) AS generated(i);
                 CHECKPOINT;",
            )
            .expect("large fixture");
        }

        let storage = Storage::open(&dir, 128, 1, 0).expect("test storage");
        let before = storage.storage_usage().expect("initial usage");
        let before_rows = storage.count_trades().expect("initial row count");
        let cap_bytes = before
            .physical
            .as_bytes()
            .min(before.active.as_bytes())
            .saturating_mul(19)
            / 20;
        assert!(cap_bytes > 0);
        let cap = StorageBytes::from_bytes(cap_bytes);
        let scheduler = CleanupScheduler {
            storage: storage.clone(),
            config: CleanupConfig {
                retention_hours: RetentionHours::new(0).expect("unlimited"),
                max_storage_bytes: Some(cap),
            },
        };

        let deleted = scheduler
            .purge_oldest_trades_until_below(cap)
            .expect("measured size purge");
        let after = storage.storage_usage().expect("final usage");
        assert!(deleted > 0);
        assert!(storage.count_trades().expect("final row count") < before_rows);
        assert!(after.active <= cap, "{after:?} must fit under {cap:?}");

        let query = TradeQuery {
            venue: "binance".into(),
            market: "linear".into(),
            symbol: "btcusdt".into(),
            from: Some(0),
            to: Some(499999),
            limit: None,
            stable_order: false,
        };
        let coverage = storage
            .trade_coverage_segments(&query)
            .expect("coverage after cap purge");
        assert!(
            coverage
                .first()
                .is_none_or(|segment| segment.from > 0 && segment.to == 499999),
            "purged history must never remain certified: {coverage:?}"
        );

        drop(storage);
        let _ = std::fs::remove_dir_all(dir);
    }
}
