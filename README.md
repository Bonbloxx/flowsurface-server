# flowsurface-server

A trade data collector for crypto markets, with an embedded database and a REST API.

- Connects through the sibling Flowsurface exchange crate, including heartbeat timeout and reconnect handling
- Persists trades to [DuckDB](https://duckdb.org)
- Serves data via REST API, as JSON or [Arrow IPC](https://arrow.apache.org/) stream formats

It's a self-contained, portable server, designed to run on a small VPS for individual use; not for production or shared use.

## Quick start

### Prebuilt binaries

1. **Get the latest [release](https://github.com/akenshaw/fs-server/releases/latest)**
   (`linux-x86_64` or `linux-aarch64`).

2. **Copy the config template**:

    ```bash
    # in the extracted directory
    cp config.example.toml config.toml
    ```

3. **Run**:

    ```bash
    ./flowsurface-server
    ```

    Or with a custom config path:

    ```bash
    ./flowsurface-server --config /path/to/config.toml
    ```

### Build from source

This fork's tested recorder is on the `flowsurface-recorder` branch. Keep the
desktop fork beside this checkout because `Cargo.toml` uses its exchange crate:

```bash
git clone https://github.com/Bonbloxx/flowsurface.git
git clone --branch flowsurface-recorder https://github.com/Bonbloxx/flowsurface-server.git
cd flowsurface-server
cargo build --release

cp config.example.toml config.toml

# run
cargo run --release
```

By default the server looks for `config.toml` in the project root when the
executable path contains `/target/`; otherwise it looks next
to the executable (deployed binary). Use `--config /path/to/config.toml` to
override.

> If `config.toml` doesn't exist, the server writes the default
> template to that path and **exits** (code 2). Edit the created file to suit
> your needs, then re-run.

## Configuration

See [`config.example.toml`](config.example.toml) for all
available options with inline documentation.

### `[network]`

| Option         | Default          | Description                                      |
| -------------- | ---------------- | ------------------------------------------------ |
| `bind_address` | `127.0.0.1:8080` | Listen address; non-loopback enables TLS + auth. |
| `max_requests` | `500`            | Per-IP rate limit (req/10s); `0` = off.          |

### `[storage]`

| Option                 | Default  | Description                                   |
| ---------------------- | -------- | --------------------------------------------- |
| `data_dir`             | `"data"` | Directory for DB, auth token, and TLS certs.  |
| `max_storage_mb`       | `4096`   | Hard cap on DB+WAL (MB); `0` = unlimited.     |
| `data_retention_hours` | `168`    | Purge trades older than this; `0` = keep all. |
| `memory_limit_mb`      | `0`      | DuckDB RAM cap; `0` = DuckDB default (80%).   |

> **Low-memory hosts:** By default DuckDB is allowed to use 80% of your
> machine's RAM. On a small host, this can leave too little for
> the rest of the system, and queries can fail with an
> `Out of Memory Error`. For such cases, you better explicitly set `memory_limit_mb`
> to a lower value, such as `256` or `384` for 1 GB host, so that
> DuckDB can spill to disk instead of failing when it's at the cap.

History exports run one at a time, independently of the live batch writer.
Reads sort one-hour windows within a single database snapshot, preserving
inclusive bounds, ordering and duplicate executions across window boundaries.
Arrow results are fetched in batches; the HTTP response retains the existing
400,000-row cap and four-column schema. Cancelling or timing out a history
request interrupts only its reader connection, and admission stays held until
the blocking worker exits. No accepted recording events are discarded.

On startup, the server removes the obsolete `idx_trades_ex_sym_t` composite
index. DuckDB cannot use that index for these range scans, while maintaining it
can pin a growing share of the database memory pool. This removes derived index
data only: trades, duplicates, capture intervals, coverage floors and retention
settings are preserved.

### `[pairs]`

| Option           | Default | Description                                                      |
| ---------------- | ------- | ---------------------------------------------------------------- |
| `base_assets`    | `[]`    | Bases combined with whitelist quotes to determine tracked pairs. |
| `discovery_mode` | `true`  | Fetch metadata for all exchanges (populates `/exchanges`).       |

### Whitelist templates

The whitelist defines which pairs to track per venue and market kind.
See the `[whitelist.*]` sections in [`config.example.toml`](config.example.toml)
for the template. The server combines `base_assets × quote_assets`
per market kind and resolves the correct exchange-specific ticker strings
(handling separators, `_PERP`, `-SWAP` suffixes, etc.).

> **Wildcard:** An empty string `""` as a quote asset includes **every**
> ticker on that exchange whose base matches (e.g.
> `linear = [""]` would track all linear perpetuals with a matching base
> asset regardless of quote currency).

Pairs are only tracked when **both** `base_assets` and a matching whitelist
entry are configured. If either is empty, no pairs are tracked.
Use the `/exchanges` endpoint to browse available symbols before
deciding which pairs to track.

### Authentication

When binding to a **non-loopback** address, the server:

1. Generates a random 256-bit auth token on first boot and persists
   it as `.auth_token` inside the configured `data_dir` (see [`[storage]`](#storage)).
2. The token's first 4 characters are printed at startup.
3. Retrieve the full token at any time:
    ```bash
    # data_dir = "data"
    cat data/.auth_token
    ```

The token is reused across restarts. To set a specific token set the `AUTH_TOKEN` environment variable
(via `.env` or the environment).

## Remote deployment (VPS)

The server automatically enables HTTPS with a **self-signed certificate**
when binding to a non-loopback address (e.g. `0.0.0.0:8080`).

1. Set `bind_address = "0.0.0.0:8080"` in `config.toml`
2. Start the server, it generates (inside the configured `data_dir`, see [`[storage]`](#storage)):
    - `cert.pem` + `key.pem` (self-signed TLS cert/key pair)
    - `.auth_token` (random 256-bit token, unless already set)
3. Clients connect via `https://vps-ip:8080` using the auth token.
   The cert is self-signed, use `curl -k` or equivalent to accept
   it, or verify against `cert.pem` directly.

> **Port reachability:** Some cloud providers block inbound ports by
> default. Configure a firewall / security-group rule to allow traffic
> on your chosen port.

#### Connecting via a domain name

Set `tls_domain` to have the self-signed cert identify as your domain:

```toml
tls_domain = "data.mydomain.com"
```

When binding to `0.0.0.0`, verified TLS requires a domain name (no IP SAN
is added). When binding to a concrete IP, an IP SAN is added automatically.

The cert's SHA-256 fingerprint is logged at startup for pinning.

### Local development

For local-only use, keep `bind_address = "127.0.0.1:8080"`:

- Plain HTTP (no TLS overhead)
- No auth token required
- `curl http://127.0.0.1:8080/pairs` works directly

## Design limitations

This server records executions losslessly after they enter its bounded event
pipeline. It still cannot reconstruct frames an exchange never delivered:

- **Network gaps stay explicit**: heartbeat/disconnect/reconnect boundaries are
  persisted as capture intervals. `/coverage` reports only proven stored
  intervals so clients can backfill every complement from an exchange archive.
  It never treats `MIN(ts)..MAX(ts)` as contiguous evidence.

- **Hyperliquid replay proofs are versioned**: only capture intervals produced
  after execution-ID deduplication are eligible for verified Hyperliquid
  coverage. Legacy rows remain under normal retention but are not served as
  trusted history.

- **In-memory buffering**: trades are held in memory for up to
  `flush_interval_ms` (default 2s) before batch-writing to DuckDB.
  `max_buffered_trades` is a flush threshold; bounded channels apply
  backpressure and no accepted execution is shed. Failed transactional writes
  retain and retry the complete batch.

- **Configuration changes require a restart**: editing tracked pairs
  or anything in `config.toml` needs a server restart. In-memory
  buffered trades are drained to disk during shutdown. The interval until
  feeds reconnect is deliberately uncovered rather than silently claimed.

- **Configured venue discovery recovers automatically**: a failed metadata
  request, or metadata that resolves no configured trade pairs, stays pending.
  Healthy venues begin recording immediately while a managed producer retries
  only the unresolved exchange with a 2-second delay, doubling to at most 30
  seconds. Recovery adds the venue once through the same lossless pipeline.
  Cancellation stops retries and preserves the existing shutdown drain.

  `/status` returns HTTP 503 with `missing_exchanges` until every configured
  exchange resolves and stores fresh trades timestamped at or after this
  process started. Recent rows from a previous process cannot qualify startup
  health. `resolved_feeds_ok` describes the
  venues already recording; `recovery_in_progress` identifies live managed
  retry workers. The supplied health monitor preserves healthy recording during
  that recovery, while database failure, stale resolved feeds, or unavailable
  status still request a service restart. Missing history remains uncovered.

The server fails closed on coverage: missing source history remains visible as
missing until an authoritative archive fills it.

## API endpoints

`/status` is **public** (no auth).
All other endpoints require `Authorization: Bearer <token>` when
auth is configured.

| Method | Path            | Auth     | Description                                        |
| ------ | --------------- | -------- | -------------------------------------------------- |
| GET    | `/status`       | ✗ public | Server uptime, DB connectivity check               |
| GET    | `/exchanges`    | required | Available ticker symbols per exchange              |
| GET    | `/pairs`        | required | Configured pairs with time bounds & tracked count  |
| GET    | `/coverage`     | required | Proven recorder intervals for an exact range       |
| GET    | `/trades`       | required | Trade data (filtered by venue, symbol, time range) |
| GET    | `/trades.arrow` | required | Trade data as Arrow IPC stream                     |

### GET /status

Returns the server health status. No authentication required.

#### Response fields

| Field         | Type   | Description                   |
| ------------- | ------ | ----------------------------- |
| `status`      | string | `"ok"` or `"degraded"`         |
| `uptime_secs` | int    | Seconds since server start    |
| `db_ok`       | bool   | `true` if DuckDB is reachable |
| `feeds_ok`    | bool   | `true` when every tracked feed is fresh |
| `resolved_feeds_ok` | bool | Resolved feeds are nonempty and fresh, with a working DB |
| `missing_exchanges` | array | Configured exchanges still awaiting metadata/pair resolution |
| `recovery_in_progress` | bool | Every missing exchange has a live managed retry producer |
| `stale_pairs` | array  | Feeds beyond the freshness threshold |

#### Example

```bash
curl http://127.0.0.1:8080/status
```

```json
{
    "status": "ok",
    "uptime_secs": 7,
    "db_ok": true,
    "feeds_ok": true,
    "stale_after_ms": 30000,
    "stale_pairs": []
}
```

### GET /coverage

Returns capture intervals proven by durable WebSocket connection records and
their last stored execution. A client must fill every complement before
advancing a historical cache checkpoint.

```bash
curl -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:8080/coverage?venue=binance&market=linear&symbol=btcusdt&from=1787616000000&to=1787640000000"
```

### GET /exchanges

Returns every ticker symbol discovered on each exchange, grouped by
canonical exchange name. Useful for browsing available tickers before
configuring the whitelist.

```bash
curl -H "Authorization: Bearer <token>" \
  http://127.0.0.1:8080/exchanges
```

```json
{
    "exchanges": {
        "Binance Linear": ["BTCUSDT", "ETHUSDT", ...],
        "Binance Spot": ["BTCUSDT", "ETHUSDT", ...],
        "Bybit Linear": ["BTCUSDT", ...],
        ...
    }
}

```

### GET /pairs

Returns all configured pairs with their stored time ranges. Pairs that have
been configured but have not yet received any trades appear with `earliest`
and `latest` as `null`.

#### Response fields

| Field           | Type  | Description                      |
| --------------- | ----- | -------------------------------- |
| `pairs`         | array | Array of tracked pair objects    |
| `tracked_count` | int   | Total number of configured pairs |

Each pair object:

| Field      | Type   | Description                               |
| ---------- | ------ | ----------------------------------------- |
| `ticker`   | string | Ticker ID (`Exchange:pair`)               |
| `earliest` | int    | Unix ms of oldest stored trade, or `null` |
| `latest`   | int    | Unix ms of newest stored trade, or `null` |

#### Example

```bash
curl -H "Authorization: Bearer <token>" \
  http://127.0.0.1:8080/pairs
```

```json
{
    "pairs": [
        {
            "ticker": "BinanceSpot:btcusdt",
            "earliest": 1783873393043,
            "latest": 1784372262447
        },
        {
            "ticker": "HyperliquidLinear:btcusdc",
            "earliest": 1784372175005,
            "latest": 1784372262010
        }
    ],
    "tracked_count": 18
}
```

### GET /trades

Returns trade records matching the given filter.

#### Query parameters

| Param    | Type   | Description                                  |
| -------- | ------ | -------------------------------------------- |
| `venue`  | string | **Required.** Exchange name (e.g. `binance`) |
| `market` | string | **Required.** `spot`, `linear`, or `inverse` |
| `symbol` | string | **Required.** Ticker symbol (e.g. `btcusdt`) |
| `from`   | int    | Unix ms lower bound (inclusive)              |
| `to`     | int    | Unix ms upper bound (inclusive)              |
| `limit`  | int    | Max records (default 1000, max 10 000)       |

> **Sort order:** When `from` is specified, rows are returned oldest-first.
> When `from` is omitted, rows are returned newest-first.

#### Response fields

| Field    | Type  | Description                     |
| -------- | ----- | ------------------------------- |
| `trades` | array | Array of matching trade objects |

Each trade object:

| Field     | Type  | Description                                                                        |
| --------- | ----- | ---------------------------------------------------------------------------------- |
| `ts`      | int   | Unix millisecond timestamp                                                         |
| `price`   | float | Price                                                                              |
| `qty`     | float | Normalized quantity (base units for spot/linear, quote notional for inverse perps) |
| `is_sell` | bool  | `true` if a sell, `false` if a buy                                                 |

#### Example

```bash
curl -H "Authorization: Bearer <token>" \
  'http://127.0.0.1:8080/trades?venue=bybit&market=linear&symbol=btcusdt&limit=2'
```

```json
{
    "trades": [
        {
            "ts": 1784108317317,
            "price": 64774.8,
            "qty": 0.111,
            "is_sell": false
        },
        {
            "ts": 1784108317964,
            "price": 64774.7,
            "qty": 0.003,
            "is_sell": true
        }
    ]
}
```

### GET /trades.arrow

Same filtering as `/trades` but returns an **Arrow IPC streaming
format** payload (`Content-Type: application/vnd.apache.arrow.stream`
with `Content-Disposition: attachment; filename="trades.arrow"`)
with 4 columns:

> `ts (int64)`, `price (float64)`, `qty (float64)`, `is_sell (bool)`

This is ideal for high-volume data transfer to clients that support Arrow natively.

| Param    | Type   | Description                                  |
| -------- | ------ | -------------------------------------------- |
| `venue`  | string | **Required.** Exchange name (e.g. `binance`) |
| `market` | string | **Required.** `spot`, `linear`, or `inverse` |
| `symbol` | string | **Required.** Ticker symbol (e.g. `btcusdt`) |
| `from`   | int    | Unix ms lower bound (inclusive)              |
| `to`     | int    | Unix ms upper bound (inclusive)              |
| `limit`  | int    | Max records (default 50 000, max 400 000)    |
| `stable_order` | bool | Optional, default `false`; preserve source insertion order within timestamp ties, in the requested direction |

> **Sort order:** Same as `/trades`.

TRev requests `stable_order=true` because bar boundaries depend on the execution
sequence. The response confirms this contract with
`x-flowsurface-trade-order: timestamp-insertion-v1`. Requests without the flag
keep their existing timestamp ordering and return
`x-flowsurface-trade-order: timestamp`. Identical executions remain distinct.

## Bounded VPS deployment

The checked-in VPS deployment records only BTC linear perpetual executions from
Binance, Bybit, and Hyperliquid. It binds to the VPS Tailscale address rather
than a public interface and enforces both a 30-day retention window and a 4 GB
storage ceiling. Cleanup starts at roughly 80% of that ceiling, verifies DuckDB's
live blocks plus the real DB/WAL files, and checkpoints deleted row groups so the
reserved space is reused. DuckDB may retain already-allocated free blocks instead
of shrinking the file, so cleanup never destroys additional valid history merely
to make an allocated file smaller.

Deployment files:

- `deploy/vps/config.toml` — machine-local feed whitelist, retention, storage,
  and memory limits; excluded from Git. Start from `config.example.toml`.
- `deploy/systemd/flowsurface-trade-recorder.service` — isolated user service with
  CPU, memory, task, I/O, and filesystem limits.
- `deploy/systemd/flowsurface-trade-recorder-health.{service,timer}` — once-per-minute
  end-to-end `/status` check and isolated restart.
- `deploy/windows/configure-recorder.ps1` — stores the bearer token in Windows
  Credential Manager and changes only the serialized recorder URL/fetch mode,
  after writing a timestamped state backup.

The user service requires systemd lingering so it starts at VPS boot without an
interactive login. Never copy `.auth_token` into `saved-state.json`; pass its
temporary local copy to `configure-recorder.ps1`, verify the app, and remove the
temporary file. History before the recorder's first proven capture segment is
intentionally reported as partial. The desktop does not download daily exchange
archives merely to fill that pre-installation boundary, while later internal
recorder gaps remain eligible for exchange repair where the venue supports it.
Retention and size purges advance durable coverage floors in the same transaction
as their trade deletion. `/coverage` therefore cannot certify history that cleanup
has removed, including expired intervals that contained no executions.
