//! Compare closed recorder snapshots without opening the live database.
//! Usage: audit_history_integrity BEFORE.duckdb AFTER.duckdb REPORT.json
use anyhow::{Context, Result, ensure};
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array};
use duckdb::{AccessMode, Config, Connection};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, path::Path};

#[derive(Debug, Default, Serialize, PartialEq, Eq)]
struct Fingerprint {
    rows: u64,
    sum: [u64; 4],
    xor: [u64; 4],
}

impl Fingerprint {
    fn add(&mut self, row: &[u8; 25]) {
        let hash = Sha256::digest(row);
        let mut carry = 0u128;
        for i in 0..4 {
            let word = u64::from_le_bytes(hash[i * 8..i * 8 + 8].try_into().unwrap());
            let sum = self.sum[i] as u128 + word as u128 + carry;
            self.sum[i] = sum as u64;
            carry = sum >> 64;
            self.xor[i] ^= word;
        }
        self.rows += 1;
    }
}

#[derive(Serialize)]
struct Pair {
    exchange: String,
    symbol: String,
    rows: u64,
    earliest: i64,
    latest: i64,
}

fn connect(path: &Path) -> Result<Connection> {
    let conn =
        Connection::open_with_flags(path, Config::default().access_mode(AccessMode::ReadOnly)?)?;
    conn.execute_batch("SET memory_limit = '256MB'; SET threads = 1;")?;
    Ok(conn)
}

fn pairs(conn: &Connection) -> Result<Vec<Pair>> {
    Ok(conn.prepare("SELECT exchange, symbol, count(*), min(ts), max(ts) FROM trades GROUP BY exchange, symbol ORDER BY exchange, symbol")?
        .query_map([], |row| Ok(Pair { exchange: row.get(0)?, symbol: row.get(1)?, rows: row.get(2)?, earliest: row.get(3)?, latest: row.get(4)? }))?
        .collect::<duckdb::Result<_>>()?)
}

fn visit_rows(
    conn: &Connection,
    pair: &Pair,
    floor: i64,
    mut visit: impl FnMut(i64, [u8; 25]),
) -> Result<()> {
    let mut statement = conn.prepare("SELECT ts, price, qty, is_sell FROM trades WHERE exchange = ?1 AND symbol = ?2 AND ts >= ?3 AND ts <= ?4")?;
    drop(statement.stream_arrow(duckdb::params![
        pair.exchange,
        pair.symbol,
        floor,
        pair.latest
    ])?);
    while let Some(chunk) = statement.step()? {
        let ts = chunk
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("timestamps")?;
        let price = chunk
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("prices")?;
        let qty = chunk
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("quantities")?;
        let sell = chunk
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .context("sides")?;
        for i in 0..chunk.len() {
            let mut row = [0u8; 25];
            row[..8].copy_from_slice(&ts.value(i).to_le_bytes());
            row[8..16].copy_from_slice(&price.value(i).to_bits().to_le_bytes());
            row[16..24].copy_from_slice(&qty.value(i).to_bits().to_le_bytes());
            row[24] = u8::from(sell.value(i));
            visit(ts.value(i), row);
        }
    }
    Ok(())
}

fn metadata(conn: &Connection) -> Result<Vec<(String, String)>> {
    Ok(conn
        .prepare("SELECT key, value FROM metadata ORDER BY key")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<duckdb::Result<_>>()?)
}

fn audit_http(before: &Connection, manifest: &Path) -> Result<serde_json::Value> {
    let requests: Vec<serde_json::Value> = serde_json::from_slice(&std::fs::read(manifest)?)?;
    let mut results = Vec::new();
    for request in requests {
        ensure!(request["status"] == 200, "HTTP history request failed");
        let filename = request["filename"].as_str().context("filename")?;
        ensure!(
            Path::new(filename).file_name() == Some(std::ffi::OsStr::new(filename)),
            "unsafe filename"
        );
        let bytes = std::fs::read(
            manifest
                .parent()
                .context("manifest directory")?
                .join(filename),
        )?;
        let hash: String = Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        ensure!(request["sha256"] == hash, "HTTP payload hash mismatch");
        let from = request["from"].as_i64().context("from")?;
        let to = request["to"].as_i64().context("to")?;
        let limit = request["limit"].as_u64().context("limit")?;
        let exchange = match request["venue"].as_str() {
            Some("binance") => "Binance Linear",
            Some("bybit") => "Bybit Linear",
            Some("hyperliquid") => "Hyperliquid Linear",
            _ => anyhow::bail!("unexpected venue"),
        };
        let symbol = request["symbol"].as_str().context("symbol")?;
        let mut rows = Vec::new();
        for batch in arrow::ipc::reader::StreamReader::try_new(bytes.as_slice(), None)? {
            let batch = batch?;
            ensure!(
                batch.num_columns() == 4
                    && batch
                        .columns()
                        .iter()
                        .all(|column| column.null_count() == 0),
                "invalid history schema"
            );
            let ts = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("HTTP timestamps")?;
            let price = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("HTTP prices")?;
            let qty = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("HTTP quantities")?;
            let sell = batch
                .column(3)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("HTTP sides")?;
            for i in 0..batch.num_rows() {
                let mut row = [0u8; 25];
                row[..8].copy_from_slice(&ts.value(i).to_le_bytes());
                row[8..16].copy_from_slice(&price.value(i).to_bits().to_le_bytes());
                row[16..24].copy_from_slice(&qty.value(i).to_bits().to_le_bytes());
                row[24] = u8::from(sell.value(i));
                rows.push((ts.value(i), row));
            }
        }
        ensure!(
            !rows.is_empty() && rows.len() as u64 <= limit,
            "empty or oversized history page"
        );
        ensure!(
            rows.iter().all(|(ts, _)| *ts >= from && *ts <= to)
                && rows.windows(2).all(|pair| pair[0].0 <= pair[1].0),
            "history bounds/order mismatch"
        );
        let last = rows.last().unwrap().0;
        let mut received_prefix = Fingerprint::default();
        let mut boundary = HashMap::<[u8; 25], u64>::new();
        for &(ts, row) in &rows {
            if ts < last {
                received_prefix.add(&row);
            } else {
                *boundary.entry(row).or_default() += 1;
            }
        }
        let mut expected_prefix = Fingerprint::default();
        let pair = Pair {
            exchange: exchange.into(),
            symbol: symbol.into(),
            rows: 0,
            earliest: from,
            latest: last,
        };
        visit_rows(before, &pair, from, |ts, row| {
            if ts < last {
                expected_prefix.add(&row);
            } else if let Some(count) = boundary.get_mut(&row) {
                *count = count.saturating_sub(1);
            }
        })?;
        let expected_count: u64 = before.query_row("SELECT count(*) FROM trades WHERE exchange = ?1 AND symbol = ?2 AND ts >= ?3 AND ts <= ?4", duckdb::params![exchange, symbol, from, to], |row| row.get(0))?;
        let passed = expected_prefix == received_prefix
            && boundary.values().all(|count| *count == 0)
            && rows.len() as u64 == expected_count.min(limit);
        results.push(serde_json::json!({"filename": filename, "rows": rows.len(), "available_snapshot_rows": expected_count, "passed": passed}));
        eprintln!(
            "HTTP page {filename}: {} exact original trade records verified, passed={passed}",
            rows.len()
        );
    }
    Ok(
        serde_json::json!({"passed": results.iter().all(|result| result["passed"] == true), "pages": results}),
    )
}

fn run(before: &Connection, after: &Connection) -> Result<serde_json::Value> {
    let before_pairs = pairs(before)?;
    let after_pairs = pairs(after)?;
    let before_metadata = metadata(before)?;
    let after_metadata = metadata(after)?;
    let floor = after_metadata
        .iter()
        .find(|(key, _)| key == "coverage_retention_floor_ms")
        .map(|(_, value)| value.parse::<i64>())
        .transpose()?
        .unwrap_or(i64::MIN);
    let mut results = Vec::new();
    for pair in &before_pairs {
        eprintln!(
            "Auditing {}:{} ({} pre-restart trades)",
            pair.exchange, pair.symbol, pair.rows
        );
        let current = after_pairs
            .iter()
            .find(|other| other.exchange == pair.exchange && other.symbol == pair.symbol)
            .context("pre-existing source missing after restart")?;
        let cutoff = pair.latest.saturating_sub(60_000);
        let mut old_digest = Fingerprint::default();
        let mut tail = HashMap::<[u8; 25], u64>::new();
        let mut old_tail_rows = 0u64;
        visit_rows(before, pair, floor, |ts, row| {
            if ts <= cutoff {
                old_digest.add(&row);
            } else {
                *tail.entry(row).or_default() += 1;
                old_tail_rows += 1;
            }
        })?;
        let mut new_digest = Fingerprint::default();
        let mut new_tail_rows = 0u64;
        visit_rows(after, pair, floor, |ts, row| {
            if ts <= cutoff {
                new_digest.add(&row);
            } else {
                new_tail_rows += 1;
                if let Some(count) = tail.get_mut(&row) {
                    *count = count.saturating_sub(1);
                }
            }
        })?;
        let missing_tail: u64 = tail.values().sum();
        let retained = old_digest.rows + old_tail_rows;
        let passed = old_digest == new_digest && missing_tail == 0 && current.rows >= retained;
        results.push(serde_json::json!({
            "exchange": pair.exchange, "symbol": pair.symbol,
            "before_total_rows": pair.rows, "after_total_rows": current.rows,
            "retained_preexisting_rows": retained,
            "normal_retention_expired_rows": pair.rows - retained,
            "historical_before_fingerprint": old_digest, "historical_after_fingerprint": new_digest,
            "recent_before_rows": old_tail_rows, "recent_after_rows": new_tail_rows,
            "missing_preexisting_recent_rows": missing_tail,
            "additional_current_rows": current.rows.saturating_sub(retained),
            "passed": passed,
        }));
        eprintln!(
            "{}:{} preserved={} expired={} passed={passed}",
            pair.exchange,
            pair.symbol,
            retained,
            pair.rows - retained
        );
    }
    type Interval = (String, String, i64, Option<i64>, Option<i64>, u8);
    fn intervals(conn: &Connection) -> Result<Vec<Interval>> {
        Ok(conn.prepare("SELECT exchange, symbol, connected_at, observed_through, closed_at, COALESCE(semantics_version, 1) FROM capture_intervals ORDER BY exchange, symbol, connected_at")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)))?
            .collect::<duckdb::Result<_>>()?)
    }
    let old_intervals = intervals(before)?;
    let new_intervals = intervals(after)?;
    let intervals_preserved = old_intervals.iter().all(|old| {
        new_intervals.iter().any(|new| {
            old.0 == new.0
                && old.1 == new.1
                && old.2 == new.2
                && old.5 == new.5
                && new.3 >= old.3
                && (old.4.is_none() || new.4 == old.4)
        })
    });
    let old_floors: Vec<(String, String, i64)> = before
        .prepare(
            "SELECT exchange, symbol, retain_from FROM coverage_floors ORDER BY exchange, symbol",
        )?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<duckdb::Result<_>>()?;
    let new_floors: Vec<(String, String, i64)> = after
        .prepare(
            "SELECT exchange, symbol, retain_from FROM coverage_floors ORDER BY exchange, symbol",
        )?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<duckdb::Result<_>>()?;
    let obsolete_index_count: u64 = after.query_row(
        "SELECT count(*) FROM duckdb_indexes() WHERE index_name = 'idx_trades_ex_sym_t'",
        [],
        |row| row.get(0),
    )?;
    let passed = results.iter().all(|result| result["passed"] == true)
        && intervals_preserved
        && old_floors == new_floors
        && obsolete_index_count == 0;
    Ok(serde_json::json!({
        "passed": passed, "method": "per-source count plus sums and XORs of SHA256 hashes of exact timestamp/float-bit/side records; exact multiset comparison for the final minute",
        "retention_floor_after": floor, "sources": results,
        "before_metadata": before_metadata, "after_metadata": after_metadata,
        "before_pairs": before_pairs, "after_pairs": after_pairs,
        "capture_intervals_preserved": intervals_preserved,
        "capture_intervals_before": old_intervals.len(), "capture_intervals_after": new_intervals.len(),
        "source_coverage_floors_preserved": old_floors == new_floors,
        "obsolete_index_count": obsolete_index_count,
    }))
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() == 5 && args[1] == "--http" {
        let before = connect(Path::new(&args[2]))?;
        let report = audit_http(&before, Path::new(&args[3]))?;
        std::fs::write(&args[4], serde_json::to_vec_pretty(&report)?)?;
        ensure!(
            report["passed"] == true,
            "HTTP history values differ from original snapshot"
        );
        return Ok(());
    }
    ensure!(
        args.len() == 4,
        "usage: audit_history_integrity BEFORE.duckdb AFTER.duckdb REPORT.json"
    );
    let before = connect(Path::new(&args[1])).context("opening before snapshot read-only")?;
    let after = connect(Path::new(&args[2])).context("opening after snapshot read-only")?;
    let report = run(&before, &after)?;
    std::fs::write(&args[3], serde_json::to_vec_pretty(&report)?)?;
    ensure!(
        report["passed"] == true,
        "integrity comparison failed; inspect report"
    );
    eprintln!(
        "All retained pre-existing trades, duplicate counts, and capture intervals are intact."
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn directory(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "recorder-integrity-{label}-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        path
    }

    fn seed(path: &Path) {
        let c = Connection::open(path).unwrap();
        c.execute_batch("CREATE TABLE trades(exchange VARCHAR, symbol VARCHAR, ts BIGINT, price DOUBLE, qty DOUBLE, is_sell BOOLEAN);
            CREATE TABLE metadata(key VARCHAR, value VARCHAR);
            CREATE TABLE coverage_floors(exchange VARCHAR, symbol VARCHAR, retain_from BIGINT);
            CREATE TABLE capture_intervals(exchange VARCHAR, symbol VARCHAR, connected_at BIGINT, observed_through BIGINT, closed_at BIGINT, semantics_version UTINYINT);
            INSERT INTO trades VALUES ('Binance Linear', 'btcusdt', 1, 60000, 0.001, false), ('Binance Linear', 'btcusdt', 10000, 60000, 0.001, false), ('Binance Linear', 'btcusdt', 10000, 60000, 0.001, false), ('Binance Linear', 'btcusdt', 110000, 60000, 0.001, false);
            INSERT INTO capture_intervals VALUES ('Binance Linear', 'btcusdt', 1, 110000, NULL, 2);").unwrap();
    }

    #[test]
    fn detects_lost_duplicate_changed_values_and_lost_recent_trade() {
        for (label, mutation) in [
            (
                "duplicate",
                "DELETE FROM trades WHERE rowid = (SELECT min(rowid) FROM trades WHERE ts = 10000)",
            ),
            (
                "value",
                "UPDATE trades SET price = 60000.0000001 WHERE ts = 10000",
            ),
            ("side", "UPDATE trades SET is_sell = true WHERE ts = 10000"),
            ("recent", "DELETE FROM trades WHERE ts = 110000"),
        ] {
            let dir = directory(label);
            let old = dir.join("old.duckdb");
            let new = dir.join("new.duckdb");
            seed(&old);
            std::fs::copy(&old, &new).unwrap();
            Connection::open(&new)
                .unwrap()
                .execute_batch(mutation)
                .unwrap();
            let report = run(&connect(&old).unwrap(), &connect(&new).unwrap()).unwrap();
            assert_eq!(report["passed"], false, "{label}");
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn accepts_new_recording_and_accounts_for_configured_retention_only() {
        let dir = directory("retention");
        let old = dir.join("old.duckdb");
        let new = dir.join("new.duckdb");
        seed(&old);
        std::fs::copy(&old, &new).unwrap();
        Connection::open(&new).unwrap().execute_batch("DELETE FROM trades WHERE ts < 2;
            INSERT INTO metadata VALUES ('coverage_retention_floor_ms', '2');
            INSERT INTO trades VALUES ('Binance Linear', 'btcusdt', 120000, 60001, 0.002, true);
            UPDATE capture_intervals SET closed_at = 110001;
            INSERT INTO capture_intervals VALUES ('Binance Linear', 'btcusdt', 120000, 120000, NULL, 2);").unwrap();
        let report = run(&connect(&old).unwrap(), &connect(&new).unwrap()).unwrap();
        assert_eq!(report["passed"], true);
        assert_eq!(report["sources"][0]["normal_retention_expired_rows"], 1);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn read_only_audit_recovers_stable_wal_without_modifying_snapshot() {
        let dir = directory("wal");
        let original = dir.join("original.duckdb");
        let snapshot = dir.join("snapshot.duckdb");
        seed(&original);
        let writer = Connection::open(&original).unwrap();
        writer.execute_batch("INSERT INTO trades VALUES ('Binance Linear', 'btcusdt', 120000, 60001, 0.002, true)").unwrap();
        std::fs::copy(&original, &snapshot).unwrap();
        let wal_before = std::fs::read(original.with_extension("duckdb.wal")).unwrap();
        std::fs::write(snapshot.with_extension("duckdb.wal"), &wal_before).unwrap();
        let db_before = std::fs::read(&snapshot).unwrap();
        let reader = connect(&snapshot).unwrap();
        assert_eq!(pairs(&reader).unwrap()[0].rows, 5);
        drop(reader);
        assert_eq!(std::fs::read(&snapshot).unwrap(), db_before);
        assert_eq!(
            std::fs::read(snapshot.with_extension("duckdb.wal")).unwrap(),
            wal_before
        );
        drop(writer);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn http_audit_accepts_partial_timestamp_bucket_and_rejects_lost_or_changed_rows() {
        use arrow::{
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use std::sync::Arc;
        for (label, values, limit, expected) in [
            (
                "valid-boundary",
                vec![(1, 60000.0), (10000, 60000.0)],
                2,
                true,
            ),
            (
                "missing-duplicate",
                vec![(1, 60000.0), (10000, 60000.0), (110000, 60000.0)],
                4,
                false,
            ),
            (
                "changed-prefix",
                vec![(1, 60000.0000001), (10000, 60000.0)],
                2,
                false,
            ),
            (
                "invented-boundary",
                vec![(1, 60000.0), (10000, 66666.0)],
                2,
                false,
            ),
        ] {
            let dir = directory(label);
            let db = dir.join("before.duckdb");
            seed(&db);
            let schema = Arc::new(Schema::new(vec![
                Field::new("ts", DataType::Int64, false),
                Field::new("price", DataType::Float64, false),
                Field::new("qty", DataType::Float64, false),
                Field::new("is_sell", DataType::Boolean, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        values.iter().map(|row| row.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(Float64Array::from(
                        values.iter().map(|row| row.1).collect::<Vec<_>>(),
                    )),
                    Arc::new(Float64Array::from(vec![0.001; values.len()])),
                    Arc::new(BooleanArray::from(vec![false; values.len()])),
                ],
            )
            .unwrap();
            let mut bytes = Vec::new();
            {
                let mut writer =
                    arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }
            std::fs::write(dir.join("page.arrow"), &bytes).unwrap();
            let hash: String = Sha256::digest(&bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            let manifest = dir.join("requests.json");
            std::fs::write(&manifest, serde_json::to_vec(&serde_json::json!([{"filename": "page.arrow", "sha256": hash, "status": 200, "venue": "binance", "symbol": "btcusdt", "from": 0, "to": 200000, "limit": limit}])).unwrap()).unwrap();
            let report = audit_http(&connect(&db).unwrap(), &manifest).unwrap();
            assert_eq!(report["passed"], expected, "{label}");
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
