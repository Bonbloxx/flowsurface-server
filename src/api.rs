use parking_lot::Mutex;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Router,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use flowsurface_exchange::{
    Ticker, Trade,
    adapter::{Exchange, MarketKind, Venue},
};
use hyper_util::rt::TokioTimer;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::{
    config::BearerToken,
    discovery::SharedDiscovery,
    limiter::{
        AdmissionGate, ConnectionLimiter, LimiterAcceptor, MAX_CONCURRENT_CONNECTIONS, RateLimiter,
    },
    storage::{CoverageSegment, PairInfo, Storage},
};

/// Static body for 503 responses from the admission gate.
static GATE_BUDGET_EXHAUSTED: &str = r#"{"error":"server busy, try again later"}"#;

/// Static body for 429 responses
static RATE_LIMITED: &str = r#"{"error":"rate limit exceeded, slow down"}"#;

/// Static body for 503 responses when the query concurrency cap is hit.
static QUERY_BUSY: &str = r#"{"error":"server busy, query queue timed out"}"#;

#[derive(Serialize)]
#[serde(untagged)]
enum Response {
    Pairs {
        pairs: Vec<PairInfo>,
        tracked_count: usize,
    },
    Trades {
        trades: Vec<AnnotatedTrade>,
    },
}

/// Cached response for the frequently polled health endpoint.
pub struct CachedResponses {
    /// `/status` — cached JSON body with a ~1 second TTL.  Wrapped in a
    /// `Mutex` because the cache is refreshed on expiry.
    status: Mutex<StatusCache>,
}

/// Inner state for the cached `/status` response.
struct StatusCache {
    body: String,
    status: StatusCode,
    refreshed_at: Instant,
}

const MAX_HEALTH_LAG_MS: u64 = 30_000;
const COVERAGE_PROOF: &str = "recorder_capture_intervals_v1";
const QUERY_QUEUE_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Serialize)]
struct CoverageResponse {
    version: u8,
    venue: String,
    market: String,
    symbol: String,
    requested_from: u64,
    requested_to: u64,
    proof: &'static str,
    segments: Vec<CoverageSegment>,
    complete: bool,
}

/// A normalized trade record, used both in-memory and serialized to JSON.
#[derive(Debug, Clone)]
pub struct AnnotatedTrade {
    pub ticker: Ticker,
    pub trade: Trade,
}

impl AnnotatedTrade {
    pub fn new(ticker: Ticker, trade: Trade) -> Self {
        AnnotatedTrade { ticker, trade }
    }
}

impl Serialize for AnnotatedTrade {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("Trade", 4)?;
        s.serialize_field("ts", &self.trade.time)?;
        s.serialize_field("price", &self.trade.price.to_f64())?;
        s.serialize_field("qty", &self.trade.qty.to_f64())?;
        s.serialize_field("is_sell", &self.trade.is_sell)?;
        s.end()
    }
}

/// Query parameters shared by the `GET /trades` (JSON) and
/// `GET /trades.arrow` endpoints.
#[derive(Debug, Deserialize)]
pub struct TradeQuery {
    /// Venue filter, e.g. "binance" (used with `market` to derive exchange).
    pub venue: String,
    /// Symbol filter.
    pub symbol: String,
    /// Market filter: "spot", "linear", or "inverse" (used with `venue`).
    pub market: String,
    /// Inclusive lower bound (milliseconds since epoch). Optional.
    pub from: Option<i64>,
    /// Inclusive upper bound (milliseconds since epoch). Optional.
    pub to: Option<i64>,
    /// Maximum number of records to return.
    ///
    /// Endpoint-dependent caps:
    /// - `/trades` (JSON): default `1000`, max `10_000`.
    /// - `/trades.arrow`: default `50_000`, max `400_000`.
    pub limit: Option<usize>,
    /// Preserve recorder insertion order among equal timestamps. Opt-in for
    /// price-path consumers; existing clients keep their current ordering.
    #[serde(default)]
    pub stable_order: bool,
}

impl TradeQuery {
    /// Validate that `venue` and `market` parse into known enum variants.
    /// Returns a human-readable error string on failure.
    pub fn validate(&self) -> Result<(), String> {
        let _venue: Venue = self
            .venue
            .parse()
            .map_err(|_| format!("unknown venue: '{}'", self.venue))?;
        let _market: MarketKind = self.market.parse().map_err(|_| {
            format!(
                "unknown market kind: '{}' (expected spot, linear, or inverse)",
                self.market
            )
        })?;
        Ok(())
    }

    /// Derive an exchange filter string from a `TradeQuery`'s `venue` + `market`.
    pub fn exchange_filter(self: &TradeQuery) -> String {
        exchange_from_venue_market(&self.venue, &self.market)
            .unwrap_or_else(|| format!("{}/{}", self.venue, self.market))
    }
}

/// Derive the canonical exchange string from raw `venue` + `market` strings.
pub fn exchange_from_venue_market(venue: &str, market: &str) -> Option<String> {
    let venue_enum: Venue = venue.parse().ok()?;
    let market_enum: MarketKind = market.parse().ok()?;

    Exchange::from_venue_and_market(venue_enum, market_enum).map(|ex| ex.to_string())
}

pub struct Server {
    pub storage: Storage,
    pub startup: Instant,
    pub auth_token: Option<BearerToken>,
    /// Configured discovery and recovered sources, shared with managed producers.
    pub discovery: SharedDiscovery,
    /// TLS configuration for the HTTPS server (self-signed).
    /// `None` on loopback addresses (plain HTTP), `Some` for remote binds.
    pub tls_config: Option<RustlsConfig>,
    /// Per-IP rate limiter.  `None` when rate limiting is disabled.
    pub rate_limiter: Option<RateLimiter>,
    /// Admission gate that throttles unknown IPs to a small global budget.
    pub admission_gate: AdmissionGate,
    /// Connection‑level cap that prevents TCP/TLS connection‑storm.
    pub connection_limiter: ConnectionLimiter,
    /// History admission is independent of live recording. Both the shared
    /// DuckDB working set and buffered HTTP output must fit the service budget.
    pub query_semaphore: Arc<Semaphore>,
    /// Cached pre-serialised response bodies for `/status` and `/exchanges`.
    pub cached: CachedResponses,
}

impl Server {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Storage,
        auth_token: Option<BearerToken>,
        discovery: SharedDiscovery,
        tls_config: Option<RustlsConfig>,
        rate_limiter: Option<RateLimiter>,
        admission_gate: AdmissionGate,
        query_concurrency: usize,
    ) -> Self {
        let connection_limiter = ConnectionLimiter::new(MAX_CONCURRENT_CONNECTIONS);

        Self {
            storage,
            startup: Instant::now(),
            auth_token,
            discovery,
            tls_config,
            rate_limiter,
            admission_gate,
            connection_limiter,
            query_semaphore: Arc::new(Semaphore::new(query_concurrency)),
            cached: CachedResponses {
                status: Mutex::new(StatusCache {
                    body: String::new(),
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    refreshed_at: Instant::now(),
                }),
            },
        }
    }

    /// Check whether the request carries a valid Bearer token.
    /// Returns `Ok(())` if no auth is configured or the token matches.
    fn check_auth(
        &self,
        headers: &HeaderMap,
        peer_addr: SocketAddr,
    ) -> Result<(), (StatusCode, &'static str)> {
        let Some(ref expected_token) = self.auth_token else {
            return Ok(());
        };

        let provided = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !expected_token.is_valid_authorization(provided) {
            let token_len = provided.len();
            tracing::warn!(
                "Auth failure from {}: expected valid Bearer token (received header of {token_len} bytes)",
                peer_addr.ip(),
            );
            return Err((StatusCode::UNAUTHORIZED, "missing or invalid auth token"));
        }

        Ok(())
    }

    /// Build a JSON `200 OK` response from any serializable value.
    fn json_ok<T: Serialize>(data: &T) -> axum::response::Response {
        match serde_json::to_string(data) {
            Ok(json) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                json,
            )
                .into_response(),
            Err(e) => {
                tracing::error!("Failed to serialise response: {e:#}");
                Self::json_err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to serialise response",
                )
            }
        }
    }

    /// Build a JSON error response with the given status code.
    fn json_err(status: StatusCode, msg: &str) -> axum::response::Response {
        (
            status,
            [(header::CONTENT_TYPE, "application/json")],
            format!("{{\"error\":\"{msg}\"}}"),
        )
            .into_response()
    }

    /// Acquire a permit for a DB-backed query handler.  The semaphore still
    /// hard-caps memory-heavy work, but a normal desktop startup burst waits in
    /// a cheap Tokio queue instead of losing one pane's history request.
    async fn acquire_query(state: &Arc<Self>) -> Option<tokio::sync::OwnedSemaphorePermit> {
        tokio::time::timeout(
            QUERY_QUEUE_TIMEOUT,
            Arc::clone(&state.query_semaphore).acquire_owned(),
        )
        .await
        .ok()?
        .ok()
    }

    /// `503` response used when the query concurrency cap is hit.
    fn query_busy() -> axum::response::Response {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "application/json")],
            QUERY_BUSY,
        )
            .into_response()
    }

    /// GET /status  (public — no auth required)
    ///
    /// Returns server uptime and a basic DB connectivity check.
    /// Suitable for load-balancer / container health probes.
    ///
    /// The response is cached for ~1 second to avoid a DB query (and full
    /// serialization) on every health-check request.
    async fn status(State(state): State<Arc<Self>>) -> impl IntoResponse {
        // Fast path: serve from cache if fresh (< 1 second old).
        {
            let cache = state.cached.status.lock();
            if !cache.body.is_empty()
                && Instant::now()
                    .saturating_duration_since(cache.refreshed_at)
                    .as_secs()
                    < 1
            {
                return (
                    cache.status,
                    [(header::CONTENT_TYPE, "application/json")],
                    cache.body.clone(),
                )
                    .into_response();
            }
        }

        // Slow path: query the DB, build response, update cache.
        let uptime = state.startup.elapsed().as_secs();
        let pairs = state.storage.pairs_with_bounds();
        let db_ok = pairs.is_ok();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut latest_by_key = FxHashMap::default();
        if let Ok(pairs) = &pairs {
            for pair in pairs {
                let exchange = pair.ticker.exchange.to_string();
                let symbol = pair
                    .ticker
                    .display_symbol()
                    .map(|value| value.to_lowercase())
                    .unwrap_or_else(|| pair.ticker.to_string().to_lowercase());
                latest_by_key.insert(format!("{exchange}:{symbol}"), pair.latest);
            }
        }
        let discovery = state.discovery.read();
        let configured_pairs: Vec<Ticker> = discovery
            .resolved_pairs
            .iter()
            .map(|info| info.ticker)
            .collect();
        let missing_exchanges: Vec<String> = discovery
            .pending_exchanges
            .iter()
            .map(ToString::to_string)
            .collect();
        let recovery_in_progress = !missing_exchanges.is_empty()
            && discovery
                .pending_exchanges
                .iter()
                .all(|exchange| discovery.recovering_exchanges.contains(exchange));
        drop(discovery);
        let stale_pairs = configured_pairs
            .iter()
            .filter_map(|ticker| {
                let exchange = ticker.exchange.to_string();
                let symbol = ticker
                    .display_symbol()
                    .map(|value| value.to_lowercase())
                    .unwrap_or_else(|| ticker.to_string().to_lowercase());
                let key = format!("{exchange}:{symbol}");
                let latest = latest_by_key.get(&key).copied().flatten();
                let lag_ms = latest
                    .map(|value| now_ms.saturating_sub(value.as_u64()))
                    .unwrap_or(u64::MAX);
                let observed_this_run =
                    latest.is_some_and(|value| value >= state.storage.started_at());
                (lag_ms > MAX_HEALTH_LAG_MS || !observed_this_run).then_some(serde_json::json!({
                    "ticker": key,
                    "latest": latest.map(|value| value.as_u64()),
                    "lag_ms": (lag_ms != u64::MAX).then_some(lag_ms),
                    "observed_this_run": observed_this_run,
                }))
            })
            .collect::<Vec<_>>();
        let resolved_feeds_ok = db_ok && stale_pairs.is_empty() && !configured_pairs.is_empty();
        let feeds_ok = resolved_feeds_ok && missing_exchanges.is_empty();
        let response_status = if feeds_ok {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        let body = match serde_json::to_string(&serde_json::json!({
            "status": if feeds_ok { "ok" } else { "degraded" },
            "uptime_secs": uptime,
            "db_ok": db_ok,
            "feeds_ok": feeds_ok,
            "resolved_feeds_ok": resolved_feeds_ok,
            "missing_exchanges": missing_exchanges,
            "recovery_in_progress": recovery_in_progress,
            "stale_after_ms": MAX_HEALTH_LAG_MS,
            "stale_pairs": stale_pairs,
        })) {
            Ok(json) => json,
            Err(e) => {
                tracing::error!("Failed to serialise status: {e:#}");
                return Self::json_err(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
            }
        };

        {
            let mut cache = state.cached.status.lock();
            cache.body = body.clone();
            cache.status = response_status;
            cache.refreshed_at = Instant::now();
        }

        (
            response_status,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response()
    }

    /// GET /exchanges
    ///
    /// Returns all available ticker symbols per exchange, as discovered from
    /// the exchange APIs, including sources recovered after startup. Useful for discovering suffix
    /// patterns when configuring `config.toml`.
    ///
    async fn exchanges(State(state): State<Arc<Self>>) -> impl IntoResponse {
        let body = serde_json::json!({
            "exchanges": state.discovery.read().available_tickers,
        })
        .to_string();
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response()
    }

    /// GET /pairs
    ///
    /// Returns all configured pairs (from the startup config), enriched with
    /// earliest/latest timestamps from the database when data exists.
    /// Pairs that have been configured but have not yet received any trades
    /// appear with `earliest: null` / `latest: null`.
    async fn pairs(State(state): State<Arc<Self>>) -> impl IntoResponse {
        let storage = state.storage.clone();

        let db_pairs = match storage
            .run_blocking_query(Duration::from_secs(10), "pairs", |s| s.pairs_with_bounds())
            .await
        {
            Ok(pairs) => pairs,
            Err((status, msg)) => return Self::json_err(status, msg),
        };

        // Build a lookup keyed by "exchange:symbol" from DB results.
        let mut by_key: FxHashMap<String, &PairInfo> = FxHashMap::default();
        for p in &db_pairs {
            let ex_str = p.ticker.exchange.to_string();
            let sym_str = p.ticker.to_string().to_lowercase();
            by_key.insert(format!("{ex_str}:{sym_str}"), p);
        }

        // Merge: every configured pair gets a PairInfo; fill in DB bounds when available.
        let configured_pairs: Vec<Ticker> = state
            .discovery
            .read()
            .resolved_pairs
            .iter()
            .map(|info| info.ticker)
            .collect();
        let mut merged: Vec<PairInfo> = Vec::with_capacity(configured_pairs.len());
        for ticker in &configured_pairs {
            let ex_str = ticker.exchange.to_string();
            let sym_str = ticker
                .display_symbol()
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| ticker.to_string().to_lowercase());
            let key = format!("{ex_str}:{sym_str}");
            match by_key.get(&key) {
                Some(found) => merged.push(*(*found)),
                None => merged.push(PairInfo {
                    ticker: *ticker,
                    earliest: None,
                    latest: None,
                }),
            }
        }

        let count = merged.len();
        Self::json_ok(&Response::Pairs {
            pairs: merged,
            tracked_count: count,
        })
    }

    /// GET /coverage
    ///
    /// Return conservative contiguous recorder intervals for the exact
    /// requested execution range. Clients must fill every complement interval
    /// from an authoritative venue source before completing a cache checkpoint.
    async fn coverage(
        State(state): State<Arc<Self>>,
        query: Query<TradeQuery>,
    ) -> axum::response::Response {
        if let Err(msg) = query.validate() {
            return Self::json_err(StatusCode::BAD_REQUEST, &msg);
        }
        let Some(from) = query.from.filter(|value| *value >= 0) else {
            return Self::json_err(
                StatusCode::BAD_REQUEST,
                "coverage requires a non-negative from",
            );
        };
        let Some(to) = query.to.filter(|value| *value >= from) else {
            return Self::json_err(StatusCode::BAD_REQUEST, "coverage requires to >= from");
        };
        let Some(permit) = Self::acquire_query(&state).await else {
            return Self::query_busy();
        };

        let q = query.0;
        let venue = q.venue.to_lowercase();
        let market = q.market.to_lowercase();
        let symbol = q.symbol.to_lowercase();
        let storage = state.storage.clone();
        let segments = match storage
            .run_blocking_query(Duration::from_secs(20), "coverage", move |s| {
                let _permit = permit;
                s.trade_coverage_segments(&q)
            })
            .await
        {
            Ok(segments) => segments,
            Err((status, msg)) => return Self::json_err(status, msg),
        };
        let complete = matches!(segments.as_slice(), [segment]
            if segment.from == from as u64 && segment.to == to as u64);
        Self::json_ok(&CoverageResponse {
            version: 1,
            venue,
            market,
            symbol,
            requested_from: from as u64,
            requested_to: to as u64,
            proof: COVERAGE_PROOF,
            segments,
            complete,
        })
    }

    /// GET /trades
    async fn trades(State(state): State<Arc<Self>>, query: Query<TradeQuery>) -> impl IntoResponse {
        if let Err(msg) = query.validate() {
            return Self::json_err(StatusCode::BAD_REQUEST, &msg);
        }

        let storage = state.storage.clone();

        let Some(permit) = Self::acquire_query(&state).await else {
            return Self::query_busy();
        };

        let q = query.0;
        match storage
            .run_history_query(Duration::from_secs(10), "trades", permit, move |conn| {
                Storage::query_trades(conn, &q)
            })
            .await
        {
            Ok(trades) => Self::json_ok(&Response::Trades { trades }),
            Err((status, msg)) => Self::json_err(status, msg),
        }
    }

    /// GET /trades.arrow
    ///
    /// Returns trades as an **Arrow IPC stream**, exported directly from
    /// DuckDB via Arrow export.
    ///
    /// The Arrow IPC streaming format uses 4 columns:
    /// `ts (int64)`, `price (float64)`,
    /// `qty (float64)`, `is_sell (bool)`.
    ///
    /// This endpoint uses parameterised queries internally (safe from SQL
    /// injection).
    async fn trades_arrow(
        State(state): State<Arc<Self>>,
        query: Query<TradeQuery>,
    ) -> axum::response::Response {
        if let Err(msg) = query.validate() {
            return Server::json_err(StatusCode::BAD_REQUEST, &msg);
        }

        let limit = query.limit.unwrap_or(50_000).min(400_000);
        let mut bounded = query.0;
        bounded.limit = Some(limit);

        let stable_order = bounded.stable_order;
        let storage = state.storage.clone();

        let Some(permit) = Self::acquire_query(&state).await else {
            return Self::query_busy();
        };

        let arrow_bytes = match storage
            .run_history_query(Duration::from_secs(20), "arrow", permit, move |conn| {
                Storage::query_trades_arrow_ipc(conn, &bounded)
            })
            .await
        {
            Ok(bytes) => bytes,
            Err((status, msg)) => return Server::json_err(status, msg),
        };

        (
            StatusCode::OK,
            [
                (
                    axum::http::header::CONTENT_TYPE,
                    "application/vnd.apache.arrow.stream",
                ),
                (
                    axum::http::header::CONTENT_DISPOSITION,
                    "attachment; filename=\"trades.arrow\"",
                ),
                (
                    axum::http::header::HeaderName::from_static("x-flowsurface-trade-order"),
                    if stable_order {
                        "timestamp-insertion-v1"
                    } else {
                        "timestamp"
                    },
                ),
            ],
            arrow_bytes,
        )
            .into_response()
    }

    /// Bind to `bind_address` and spawn the axum HTTP(S) server.
    ///
    /// Uses plain HTTP for loopback addresses, HTTPS with a self-signed
    /// certificate for non-loopback (remote) binds.  Exits on bind failure.
    ///
    /// Middleware execution order (outermost → innermost):
    ///
    ///   1. **Priority gate** — throttles unknown IPs to a tiny budget
    ///      ([`crate::limiter::ADMISSION_PER_IP_BUDGET`] req/s per IP,
    ///      [`crate::limiter::ADMISSION_GLOBAL_CAP`] req/s total) so scanners
    ///      consume almost no CPU.
    ///   2. **Auth** — Bearer token verification for protected routes.
    ///   3. **Per-IP rate limiter** — token-bucket per IP (default 500 req/10s).
    ///      Only reached by authenticated requests — unauthenticated requests are
    ///      rejected by auth first, so they never consume rate-limiter bookkeeping.
    ///
    /// `/status` is public (no auth) but still goes through the priority gate
    /// and its response is cached to avoid a DB hit on every health check.
    ///
    /// **Connection‑level DoS protection:** both the TLS and plain‑HTTP paths
    /// use `axum_server` with a [`LimiterAcceptor`] that caps concurrent
    /// connections at [`MAX_CONCURRENT_CONNECTIONS`] (default 512).  Once the
    /// ceiling is hit, new connections are dropped immediately (TCP reset).
    pub async fn serve(
        self: Arc<Self>,
        bind_address: SocketAddr,
    ) -> (tokio::task::JoinHandle<()>, Handle<std::net::SocketAddr>) {
        let tls_config = self.tls_config.clone();
        let limiter = self.connection_limiter.clone();

        // Public route — /status is rate-gated but NOT auth-gated.
        let public = Router::new()
            .route("/status", get(Server::status))
            .layer(axum::middleware::from_fn_with_state(
                self.clone(),
                priority_gate_middleware,
            ))
            .with_state(self.clone());

        // Protected routes — require auth.
        // Layer order: outermost = priority gate, then auth,
        // then rate limiter (innermost).  Rate limiter is after auth
        // so unauthenticated requests never consume rate-limit state.
        let protected = Router::new()
            .route("/exchanges", get(Server::exchanges))
            .route("/pairs", get(Server::pairs))
            .route("/coverage", get(Server::coverage))
            .route("/trades", get(Server::trades))
            .route("/trades.arrow", get(Server::trades_arrow))
            .layer(axum::middleware::from_fn_with_state(
                self.clone(),
                rate_limit_middleware,
            ))
            .layer(axum::middleware::from_fn_with_state(
                self.clone(),
                auth_middleware,
            ))
            .layer(axum::middleware::from_fn_with_state(
                self.clone(),
                priority_gate_middleware,
            ))
            .with_state(self);

        let router = public.merge(protected);

        let handle = Handle::new();
        let handle_for_server = handle.clone();

        let join_handle = tokio::spawn(async move {
            // Bind synchronously for both paths — axum_server needs a
            // std::net::TcpListener regardless of TLS.
            let std_listener = match std::net::TcpListener::bind(bind_address) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(
                        "Failed to bind {bind_address}: {e:#}. \
                         Server will not accept connections."
                    );
                    return;
                }
            };
            let _ = std_listener.set_nonblocking(true);

            let make_svc = router.into_make_service_with_connect_info::<SocketAddr>();

            let result = if let Some(cfg) = tls_config {
                tracing::info!("Starting HTTPS API on {bind_address}");
                let mut server = axum_server::tls_rustls::from_tcp_rustls(std_listener, cfg)
                    .expect("failed to create TLS server from listener")
                    .handle(handle_for_server.clone())
                    .map(|acceptor| LimiterAcceptor {
                        inner: acceptor,
                        limiter: limiter.clone(),
                    });

                server
                    .http_builder()
                    .http1()
                    .timer(TokioTimer::new())
                    .header_read_timeout(Duration::from_secs(10))
                    .max_headers(100)
                    .keep_alive(false);

                server
                    .http_builder()
                    .http2()
                    .timer(TokioTimer::new())
                    .keep_alive_interval(Some(Duration::from_secs(30)))
                    .keep_alive_timeout(Duration::from_secs(5));

                server.serve(make_svc).await
            } else {
                tracing::info!("Starting HTTP API on {bind_address}");
                let mut server = axum_server::from_tcp(std_listener)
                    .expect("failed to create HTTP server from listener")
                    .handle(handle_for_server.clone())
                    .map(|acceptor| LimiterAcceptor {
                        inner: acceptor,
                        limiter: limiter.clone(),
                    });

                server
                    .http_builder()
                    .http1()
                    .timer(TokioTimer::new())
                    .header_read_timeout(Duration::from_secs(10))
                    .max_headers(100)
                    .keep_alive(false);

                server
                    .http_builder()
                    .http2()
                    .timer(TokioTimer::new())
                    .keep_alive_interval(Some(Duration::from_secs(30)))
                    .keep_alive_timeout(Duration::from_secs(5));

                server.serve(make_svc).await
            };

            if let Err(e) = result {
                tracing::error!("Server error on {bind_address}: {e:#}");
            }
        });

        (join_handle, handle)
    }
}

/// Axum middleware that enforces per-IP rate limits.
async fn rate_limit_middleware(
    State(state): State<Arc<Server>>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if peer_addr.ip().is_loopback() {
        return next.run(req).await;
    }

    if let Some(ref limiter) = state.rate_limiter
        && !limiter.check(peer_addr.ip()).await
    {
        tracing::warn!(
            "Rate limit exceeded for {} on {}",
            peer_addr.ip(),
            req.uri().path(),
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::CONTENT_TYPE, "application/json")],
            RATE_LIMITED,
        )
            .into_response();
    }
    next.run(req).await
}

/// Axum middleware that delegates auth checking to `Server::check_auth`.
/// Returns a JSON error body on auth failure for consistency with the rest of the API.
///
/// On success the peer's IP is **marked as trusted** so future requests from
/// that IP skip the priority admission gate entirely.
async fn auth_middleware(
    State(state): State<Arc<Server>>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if state.auth_token.is_some() {
        if let Err((status, msg)) = state.check_auth(&headers, peer_addr) {
            return Server::json_err(status, msg);
        }
        state.admission_gate.mark_trusted(peer_addr.ip()).await;
    }
    next.run(req).await
}

/// Axum middleware that applies the priority admission gate.
///
/// IPs that have previously authenticated successfully bypass the gate
/// entirely, the per-IP rate limiter (inner layer) is their ceiling.
async fn priority_gate_middleware(
    State(state): State<Arc<Server>>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if peer_addr.ip().is_loopback() {
        return next.run(req).await;
    }

    if !state.admission_gate.admit(peer_addr.ip()).await {
        tracing::warn!(
            "Admission gate blocked {} on {} (global budget exhausted)",
            peer_addr.ip(),
            req.uri().path(),
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "application/json")],
            GATE_BUDGET_EXHAUSTED,
        )
            .into_response();
    }
    next.run(req).await
}

/// History scans share DuckDB's pool with the durable writer. Serialize the
/// scans instead of assuming their output size bounds their working memory.
/// Live recording never acquires this semaphore.
pub fn query_concurrency_limit(_memory_limit_mb: u64) -> usize {
    1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{ConfigMarketKind, WhitelistTemplates},
        discovery::{DiscoveryState, MetadataCache, RecoveryGuard, ResolvedExchange},
        limiter::{ADMISSION_GLOBAL_CAP, ADMISSION_PER_IP_BUDGET},
        stream::StreamReceivers,
    };
    use flowsurface_exchange::{
        TickerInfo, UnixMs,
        adapter::{Event, StreamKind},
        unit::{price::Price, qty::Qty},
    };
    use parking_lot::RwLock;
    use tokio::sync::mpsc;

    async fn read_status(server: &Arc<Server>) -> (StatusCode, serde_json::Value) {
        server.cached.status.lock().body.clear();
        let response = Server::status(State(server.clone())).await.into_response();
        let code = response.status();
        let body = axum::body::to_bytes(response.into_body(), 16_384)
            .await
            .unwrap();
        (code, serde_json::from_slice(&body).unwrap())
    }

    async fn persist_trade(storage: &Storage, info: TickerInfo) {
        persist_trade_at(storage, info, UnixMs::now()).await;
    }

    async fn persist_trade_at(storage: &Storage, info: TickerInfo, at: UnixMs) {
        let (sender, receiver) = mpsc::channel(4);
        let mut receivers = StreamReceivers {
            persist: Some(receiver),
        };
        let flusher = storage.spawn_batch_flusher(&mut receivers, Duration::from_millis(1), 1);
        sender
            .send(Event::TradesReceived(
                StreamKind::Trades { ticker_info: info },
                at,
                vec![Trade {
                    time: at,
                    is_sell: false,
                    price: Price::from_f64(60_000.0),
                    qty: Qty::from_f64(0.01),
                }]
                .into_boxed_slice(),
            ))
            .await
            .unwrap();
        drop(sender);
        flusher.await.unwrap();
    }

    #[tokio::test]
    async fn recent_previous_process_rows_do_not_certify_current_feeds() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "recorder-api-startup-{}-{nonce}",
            std::process::id()
        ));
        let storage = Storage::open(&directory, 64, 1, 0).unwrap();
        let info = TickerInfo::new(
            Ticker::new("BTCUSDT", Exchange::BinanceLinear),
            0.1,
            0.001,
            None,
        );
        let whitelist: WhitelistTemplates = [(
            "binance".into(),
            [(ConfigMarketKind::Linear, vec!["USDT".into()])]
                .into_iter()
                .collect(),
        )]
        .into_iter()
        .collect();
        let cache: MetadataCache = [(
            Exchange::BinanceLinear,
            [(info.ticker, Some(info))].into_iter().collect(),
        )]
        .into_iter()
        .collect();
        let discovery = Arc::new(RwLock::new(DiscoveryState::new(
            &["BTC".into()],
            &whitelist,
            &cache,
            vec![info],
        )));
        storage.store_ticker_infos(&[info]).unwrap();
        persist_trade_at(&storage, info, storage.started_at().saturating_sub(1)).await;
        let server = Arc::new(Server::new(
            storage.clone(),
            None,
            discovery,
            None,
            None,
            AdmissionGate::new(ADMISSION_PER_IP_BUDGET, ADMISSION_GLOBAL_CAP),
            1,
        ));
        let (code, old) = read_status(&server).await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(old["feeds_ok"], false);
        assert_eq!(old["stale_pairs"][0]["observed_this_run"], false);
        assert!(old["stale_pairs"][0]["lag_ms"].as_u64().unwrap() < MAX_HEALTH_LAG_MS);
        persist_trade(&storage, info).await;
        let (code, fresh) = read_status(&server).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(fresh["feeds_ok"], true);
        assert_eq!(fresh["stale_pairs"], serde_json::json!([]));
        drop(server);
        drop(storage);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn missing_configured_exchange_cannot_report_healthy_after_partial_discovery() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "recorder-api-health-{}-{nonce}",
            std::process::id()
        ));
        let storage = Storage::open(&directory, 64, 1, 0).unwrap();
        let binance = TickerInfo::new(
            Ticker::new("BTCUSDT", Exchange::BinanceLinear),
            0.1,
            0.001,
            None,
        );
        let hyperliquid = TickerInfo::new(
            Ticker::new_with_display("BTC", Exchange::HyperliquidLinear, Some("BTCUSDC")),
            1.0,
            0.00001,
            None,
        );
        let whitelist: WhitelistTemplates = [
            (
                "binance".into(),
                [(ConfigMarketKind::Linear, vec!["USDT".into()])]
                    .into_iter()
                    .collect(),
            ),
            (
                "hyperliquid".into(),
                [(ConfigMarketKind::Linear, vec!["USDC".into()])]
                    .into_iter()
                    .collect(),
            ),
        ]
        .into_iter()
        .collect();
        let cache: MetadataCache = [(
            Exchange::BinanceLinear,
            [(binance.ticker, Some(binance))].into_iter().collect(),
        )]
        .into_iter()
        .collect();
        let discovery = Arc::new(RwLock::new(DiscoveryState::new(
            &["BTC".into()],
            &whitelist,
            &cache,
            vec![binance],
        )));
        let guard = RecoveryGuard::new(discovery.clone(), Exchange::HyperliquidLinear);
        storage.store_ticker_infos(&[binance]).unwrap();
        persist_trade(&storage, binance).await;
        let server = Arc::new(Server::new(
            storage.clone(),
            None,
            discovery.clone(),
            None,
            None,
            AdmissionGate::new(ADMISSION_PER_IP_BUDGET, ADMISSION_GLOBAL_CAP),
            1,
        ));
        let (code, pending) = read_status(&server).await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(pending["feeds_ok"], false);
        assert_eq!(pending["resolved_feeds_ok"], true);
        assert_eq!(pending["recovery_in_progress"], true);
        assert_eq!(
            pending["missing_exchanges"],
            serde_json::json!(["Hyperliquid Linear"])
        );
        drop(guard);
        assert_eq!(read_status(&server).await.1["recovery_in_progress"], false);
        storage.store_ticker_infos(&[hyperliquid]).unwrap();
        discovery.write().publish_recovered(&ResolvedExchange {
            exchange: Exchange::HyperliquidLinear,
            metadata: [(hyperliquid.ticker, Some(hyperliquid))]
                .into_iter()
                .collect(),
            pairs: vec![hyperliquid],
        });
        // Discovery alone cannot certify a feed that has stored no executions.
        assert_eq!(
            read_status(&server).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        persist_trade(&storage, hyperliquid).await;
        let (code, recovered) = read_status(&server).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(recovered["feeds_ok"], true);
        assert_eq!(recovered["missing_exchanges"], serde_json::json!([]));
        drop(server);
        drop(storage);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn old_trade_requests_keep_their_ordering_unless_explicitly_opted_in() {
        let old: TradeQuery =
            serde_json::from_str(r#"{"venue":"binance","market":"linear","symbol":"btcusdt"}"#)
                .unwrap();
        assert!(!old.stable_order);
        let ordered: TradeQuery = serde_json::from_str(
            r#"{"venue":"binance","market":"linear","symbol":"btcusdt","stable_order":true}"#,
        )
        .unwrap();
        assert!(ordered.stable_order);
    }
}
