use rustc_hash::{FxHashMap, FxHashSet};
use std::{future::Future, sync::Arc, time::Duration};

use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

use flowsurface_exchange::adapter::{AdapterHandles, Exchange, MarketKind, Venue};
use flowsurface_exchange::{Ticker, TickerInfo};

use crate::config::WhitelistTemplates;

pub type MetadataCache = FxHashMap<Exchange, FxHashMap<Ticker, Option<TickerInfo>>>;
pub type ExchangeMetadata = FxHashMap<Ticker, Option<TickerInfo>>;
pub type SharedDiscovery = Arc<RwLock<DiscoveryState>>;

pub const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(2);
pub const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Requested exchanges remain visible even when startup metadata cannot be fetched.
/// The HTTP server and stream producers share this state as discovery recovers.
pub struct DiscoveryState {
    pub resolved_pairs: Vec<TickerInfo>,
    pub pending_exchanges: Vec<Exchange>,
    pub recovering_exchanges: Vec<Exchange>,
    pub available_tickers: FxHashMap<String, Vec<String>>,
}

impl DiscoveryState {
    pub fn new(
        base_assets: &[String],
        whitelist: &WhitelistTemplates,
        cache: &MetadataCache,
        resolved_pairs: Vec<TickerInfo>,
    ) -> Self {
        // Quotes are eligible alternatives, not an assertion that every generated
        // symbol exists. Require metadata and at least one configured pair for
        // each requested exchange, preserving the existing expansion semantics.
        let required: FxHashSet<Exchange> = if base_assets.is_empty() {
            FxHashSet::default()
        } else {
            whitelist
                .iter()
                .flat_map(|(venue, markets)| {
                    let venue = venue.parse::<Venue>().ok();
                    markets.iter().filter_map(move |(market, quotes)| {
                        if quotes.is_empty() {
                            return None;
                        }
                        Exchange::from_venue_and_market(venue?, (*market).into())
                    })
                })
                .collect()
        };
        let pending_exchanges = Exchange::ALL
            .into_iter()
            .filter(|exchange| {
                required.contains(exchange)
                    && (!cache.contains_key(exchange)
                        || !resolved_pairs
                            .iter()
                            .any(|pair| pair.exchange() == *exchange))
            })
            .collect();
        Self {
            resolved_pairs,
            pending_exchanges,
            recovering_exchanges: Vec::new(),
            available_tickers: tickers_per_exchange(cache),
        }
    }

    pub fn publish_recovered(&mut self, recovered: &ResolvedExchange) {
        for pair in &recovered.pairs {
            if !self
                .resolved_pairs
                .iter()
                .any(|known| known.ticker == pair.ticker)
            {
                self.resolved_pairs.push(*pair);
            }
        }
        let cache = [(recovered.exchange, recovered.metadata.clone())]
            .into_iter()
            .collect();
        self.available_tickers.extend(tickers_per_exchange(&cache));
        self.pending_exchanges
            .retain(|exchange| *exchange != recovered.exchange);
    }
}

/// Health must not claim that recovery is running after its producer exits or panics.
pub struct RecoveryGuard {
    state: SharedDiscovery,
    exchange: Exchange,
}

impl RecoveryGuard {
    pub fn new(state: SharedDiscovery, exchange: Exchange) -> Self {
        let mut discovery = state.write();
        if !discovery.recovering_exchanges.contains(&exchange) {
            discovery.recovering_exchanges.push(exchange);
        }
        drop(discovery);
        Self { state, exchange }
    }
}

impl Drop for RecoveryGuard {
    fn drop(&mut self) {
        self.state
            .write()
            .recovering_exchanges
            .retain(|exchange| *exchange != self.exchange);
    }
}

pub struct ResolvedExchange {
    pub exchange: Exchange,
    pub metadata: ExchangeMetadata,
    pub pairs: Vec<TickerInfo>,
}

/// Retry only a missing exchange. The fetch seam also permits deterministic
/// transport failure tests without changing public exchange endpoints.
pub async fn retry_resolve_exchange<F, Fut>(
    exchange: Exchange,
    base_assets: &[String],
    whitelist: &WhitelistTemplates,
    shutdown: &CancellationToken,
    initial_delay: Duration,
    max_delay: Duration,
    mut fetch: F,
) -> Option<ResolvedExchange>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<ExchangeMetadata>>,
{
    let mut delay = initial_delay;
    let mut attempts = 0u64;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return None,
            _ = tokio::time::sleep(delay) => {}
        }
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return None,
            result = fetch() => result,
        };
        attempts = attempts.saturating_add(1);
        match result {
            Ok(metadata) => {
                let cache = [(exchange, metadata)].into_iter().collect();
                let pairs = resolve_pairs(base_assets, whitelist, &cache);
                if !pairs.is_empty() {
                    tracing::info!(%exchange, attempts, pairs = pairs.len(), "Missing exchange discovery recovered");
                    return Some(ResolvedExchange {
                        exchange,
                        metadata: cache.into_values().next().expect("one exchange cache"),
                        pairs,
                    });
                }
                tracing::warn!(%exchange, attempts, "Metadata resolved no configured trade pairs; discovery remains pending");
            }
            Err(error) => {
                tracing::warn!(%exchange, attempts, "Missing exchange metadata discovery failed: {error:#}");
            }
        }
        delay = delay.saturating_mul(2).min(max_delay);
    }
}

/// Orchestrate the full pair discovery pipeline: determine which venues to
/// use based on discovery mode, spawn adapter handles, fetch ticker metadata
/// from exchanges, and resolve concrete pair infos from the whitelist.
///
/// Returns the adapter handles, the metadata cache, and the resolved pairs.
pub async fn setup_pairs(
    base_assets: &[String],
    discovery_mode: bool,
    whitelist: &WhitelistTemplates,
) -> (AdapterHandles, MetadataCache, Vec<TickerInfo>) {
    let venues: Vec<Venue> = if discovery_mode {
        Venue::ALL.to_vec()
    } else {
        whitelist
            .keys()
            .filter_map(|v| v.parse::<Venue>().ok())
            .collect()
    };
    tracing::info!("Spawning venue adapters: {venues:?}");
    let http_client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("building exchange HTTP client");
    let adapter_handles = AdapterHandles::spawn_venues(&http_client, venues, None);

    tracing::info!("Fetching ticker metadata from exchanges…");
    let metadata_cache = build_metadata_cache(&adapter_handles, whitelist, discovery_mode).await;

    let resolved_pairs = resolve_pairs(base_assets, whitelist, &metadata_cache);

    (adapter_handles, metadata_cache, resolved_pairs)
}

/// Fetch metadata for exchanges and return a cache.
///
/// When `discovery_mode` is `true`, this ignores the whitelist templates
/// and fetches metadata for **all** supported exchange variants, so that
/// `/exchanges` is fully populated for discovery purposes.
async fn build_metadata_cache(
    handles: &AdapterHandles,
    templates: &WhitelistTemplates,
    discovery_mode: bool,
) -> MetadataCache {
    let mut cache = MetadataCache::default();

    if discovery_mode {
        for exchange in Exchange::ALL {
            let venue = exchange.venue();
            let market = exchange.market_type();

            match handles.fetch_ticker_metadata(venue, &[market]).await {
                Ok(meta) => {
                    tracing::info!("Fetched metadata for {exchange}: {} tickers", meta.len());
                    cache.insert(exchange, meta.into_iter().collect());
                }
                Err(e) => {
                    tracing::warn!("Failed to fetch metadata for {exchange}: {e:#}");
                }
            }
        }
    } else {
        for (venue_str, markets) in templates {
            let Ok(venue) = venue_str.parse::<Venue>() else {
                tracing::warn!("Unknown venue in whitelist: {venue_str}, skipping");
                continue;
            };

            for &market_kind in markets.keys() {
                let market: MarketKind = market_kind.into();

                let Some(exchange) = Exchange::from_venue_and_market(venue, market) else {
                    tracing::warn!(
                        "Unsupported venue+market combination: {venue_str} {market_kind}"
                    );
                    continue;
                };

                match handles.fetch_ticker_metadata(venue, &[market]).await {
                    Ok(meta) => {
                        tracing::info!("Fetched metadata for {exchange}: {} tickers", meta.len());
                        cache.insert(exchange, meta.into_iter().collect());
                    }
                    Err(e) => {
                        tracing::warn!("Failed to fetch metadata for {exchange}: {e:#}");
                    }
                }
            }
        }
    }

    cache
}

/// Flatten the metadata cache into a map of exchange → available ticker symbols.
///
/// Uses the display symbol when available (e.g. `"PURR/USDC"` instead of
/// Hyperliquid's opaque internal ID `"@107"`).
///
/// Returns symbols in UPPERCASE for consistent display in the `/exchanges` API.
pub fn tickers_per_exchange(cache: &MetadataCache) -> FxHashMap<String, Vec<String>> {
    cache
        .iter()
        .map(|(exchange, tickers)| {
            let mut symbols: Vec<String> = tickers
                .keys()
                .map(|t| {
                    t.display_symbol()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| t.to_string())
                        .to_uppercase()
                })
                .collect();
            symbols.sort();
            (exchange.to_string(), symbols)
        })
        .collect()
}

/// Generate the canonical ticker string for a given base asset, quote asset,
/// and exchange, based on the exchange's known ticker format.
///
/// Users only provide quote assets (e.g. `"USDT"`, `"USDC"`, `"USD"`) and
/// this function handles exchange-specific separators, perpetual suffixes,
/// and other formatting quirks.
fn format_ticker(base: &str, quote: &str, exchange: Exchange) -> String {
    let b = base.to_uppercase();
    let q = quote.to_uppercase();

    use Exchange::*;

    match exchange {
        // {base}{quote} — direct concatenation
        BinanceSpot | BinanceLinear | BybitSpot | BybitLinear | BybitInverse | MexcSpot => {
            format!("{b}{q}")
        }
        // {base}{quote}_PERP — inverse perpetuals with _PERP suffix (Binance only)
        BinanceInverse => {
            format!("{b}{q}_PERP")
        }
        // {base}_{quote} — underscore separator (MEXC linear/inverse)
        MexcLinear | MexcInverse => {
            format!("{b}_{q}")
        }
        // {base} — Hyperliquid linear (quote is implicit; just use the base asset)
        HyperliquidLinear => b,
        // Hyperliquid spot uses opaque internal IDs — matched via wildcard only
        HyperliquidSpot => b,
        // {base}-{quote} — OKX spot (dash separator)
        OkexSpot => format!("{b}-{q}"),
        // {base}-{quote}-SWAP — OKX linear & inverse perpetuals
        OkexLinear | OkexInverse => {
            format!("{b}-{q}-SWAP")
        }
    }
}

/// Resolve base assets against whitelist templates and metadata cache
/// into a list of concrete `ResolvedPair`s.
///
/// The whitelist provides **quote assets** per venue+market; this function
/// constructs the correct ticker string per exchange using [`format_ticker`]
/// and looks it up in the pre-fetched metadata cache.
fn resolve_pairs(
    base_assets: &[String],
    templates: &WhitelistTemplates,
    cache: &MetadataCache,
) -> Vec<TickerInfo> {
    let mut pairs = Vec::new();
    let mut seen: FxHashSet<(Exchange, Ticker)> = FxHashSet::default();

    for base in base_assets {
        let base_upper = base.to_uppercase();

        for (venue_str, markets) in templates {
            for (&market_kind, quotes) in markets {
                let Ok(venue) = venue_str.parse::<Venue>() else {
                    continue;
                };
                let market: MarketKind = market_kind.into();
                let Some(exchange) = Exchange::from_venue_and_market(venue, market) else {
                    continue;
                };

                let Some(metadata) = cache.get(&exchange) else {
                    continue;
                };

                for quote in quotes {
                    if quote.is_empty() {
                        // Empty quote acts as a wildcard: include every ticker
                        // on this exchange whose base matches.
                        for (ticker, opt_ti) in metadata {
                            let Some(ti) = opt_ti else {
                                continue;
                            };

                            // Check both the internal ticker string and the
                            // display symbol (e.g. Hyperliquid spot uses opaque
                            // internal IDs like "@107" with display "PURR/USDC").
                            let raw = ticker.to_string().to_uppercase();
                            let display = ticker.display_symbol().map(|s| s.to_uppercase());
                            let matches_base = raw.starts_with(&base_upper)
                                || display
                                    .as_deref()
                                    .is_some_and(|d| d.starts_with(&base_upper));

                            if matches_base && seen.insert((exchange, *ticker)) {
                                pairs.push(*ti);
                            }
                        }
                        continue;
                    }

                    let candidate = format_ticker(&base_upper, quote, exchange);
                    let search_ticker = Ticker::new(&candidate, exchange);

                    let found = metadata
                        .get(&search_ticker)
                        .copied()
                        .flatten()
                        // Fallback: case-insensitive search across all metadata keys for this exchange.
                        .or_else(|| {
                            metadata.iter().find_map(|(t, ti)| {
                                let ti = (*ti)?;
                                let (internal, _) = t.to_full_symbol_and_type();
                                if internal.eq_ignore_ascii_case(&candidate)
                                    || t.display_symbol()
                                        .is_some_and(|d| d.eq_ignore_ascii_case(&candidate))
                                {
                                    Some(ti)
                                } else {
                                    None
                                }
                            })
                        });

                    match found {
                        Some(ti) => {
                            if seen.insert((exchange, ti.ticker)) {
                                pairs.push(ti);
                            }
                        }
                        None => {
                            tracing::debug!(
                                "Ticker not found on {venue_str} {market_kind}: {candidate}"
                            );
                        }
                    }
                }
            }
        }
    }

    pairs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigMarketKind;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::atomic::{AtomicUsize, Ordering},
    };

    fn runtime() -> tokio::runtime::Runtime {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
    }

    fn whitelist() -> WhitelistTemplates {
        [
            (
                "binance".into(),
                [(ConfigMarketKind::Linear, vec!["USDT".into()])]
                    .into_iter()
                    .collect(),
            ),
            (
                "bybit".into(),
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
        .collect()
    }

    fn pair(exchange: Exchange) -> TickerInfo {
        let symbol = if exchange == Exchange::HyperliquidLinear {
            "BTC"
        } else {
            "BTCUSDT"
        };
        TickerInfo::new(Ticker::new(symbol, exchange), 0.1, 0.001, None)
    }

    fn metadata(exchange: Exchange) -> ExchangeMetadata {
        let info = pair(exchange);
        [(info.ticker, Some(info))].into_iter().collect()
    }

    fn missing_hyperliquid_state() -> SharedDiscovery {
        let pairs = vec![pair(Exchange::BinanceLinear), pair(Exchange::BybitLinear)];
        let cache = [
            (Exchange::BinanceLinear, metadata(Exchange::BinanceLinear)),
            (Exchange::BybitLinear, metadata(Exchange::BybitLinear)),
        ]
        .into_iter()
        .collect();
        Arc::new(RwLock::new(DiscoveryState::new(
            &["BTC".into()],
            &whitelist(),
            &cache,
            pairs,
        )))
    }

    #[test]
    fn only_missing_exchange_is_pending_and_dead_recovery_is_visible() {
        let state = missing_hyperliquid_state();
        assert_eq!(
            state.read().pending_exchanges,
            vec![Exchange::HyperliquidLinear]
        );
        assert!(state.read().recovering_exchanges.is_empty());
        let guard = RecoveryGuard::new(state.clone(), Exchange::HyperliquidLinear);
        assert_eq!(
            state.read().recovering_exchanges,
            vec![Exchange::HyperliquidLinear]
        );
        drop(guard);
        assert!(state.read().recovering_exchanges.is_empty());
        assert_eq!(
            state.read().pending_exchanges,
            vec![Exchange::HyperliquidLinear]
        );
    }

    #[test]
    fn successful_but_empty_metadata_remains_pending() {
        let cache = [(Exchange::HyperliquidLinear, ExchangeMetadata::default())]
            .into_iter()
            .collect();
        let state = DiscoveryState::new(&["BTC".into()], &whitelist(), &cache, Vec::new());
        assert!(
            state
                .pending_exchanges
                .contains(&Exchange::HyperliquidLinear)
        );
    }

    #[test]
    fn metadata_http_502_recovers_without_refetching_healthy_venues() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("fault HTTP listener");
        let url = format!("http://{}/metadata", listener.local_addr().unwrap());
        let requests = Arc::new(AtomicUsize::new(0));
        let request_count = requests.clone();
        let http = std::thread::spawn(move || {
            for status in ["502 Bad Gateway", "200 OK"] {
                let (mut socket, _) = listener.accept().expect("fault HTTP connection");
                let mut request = [0; 1024];
                let received = socket.read(&mut request).expect("HTTP request");
                assert!(received > 0, "HTTP request was empty");
                request_count.fetch_add(1, Ordering::SeqCst);
                let body = if status.starts_with("200") {
                    r#"{"symbol":"BTC"}"#
                } else {
                    "bad gateway"
                };
                write!(
                    socket,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .expect("fault HTTP response");
            }
        });
        runtime().block_on(async {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap();
            let state = missing_hyperliquid_state();
            let healthy = state.read().resolved_pairs.clone();
            let guard = RecoveryGuard::new(state.clone(), Exchange::HyperliquidLinear);
            let recovered = tokio::time::timeout(
                Duration::from_secs(5),
                retry_resolve_exchange(
                    Exchange::HyperliquidLinear,
                    &["BTC".into()],
                    &whitelist(),
                    &CancellationToken::new(),
                    Duration::from_millis(1),
                    Duration::from_millis(4),
                    || async {
                        let body = client
                            .get(&url)
                            .send()
                            .await?
                            .error_for_status()?
                            .text()
                            .await?;
                        let value: serde_json::Value = serde_json::from_str(&body)?;
                        let info = TickerInfo::new(
                            Ticker::new(
                                value["symbol"].as_str().expect("symbol"),
                                Exchange::HyperliquidLinear,
                            ),
                            0.1,
                            0.001,
                            None,
                        );
                        Ok([(info.ticker, Some(info))].into_iter().collect())
                    },
                ),
            )
            .await
            .expect("recovery bounded")
            .expect("recovered exchange");
            assert_eq!(recovered.pairs, vec![pair(Exchange::HyperliquidLinear)]);
            let mut discovery = state.write();
            discovery.publish_recovered(&recovered);
            discovery.publish_recovered(&recovered);
            assert_eq!(discovery.resolved_pairs.len(), 3, "recovery is idempotent");
            assert_eq!(&discovery.resolved_pairs[..2], healthy.as_slice());
            assert!(discovery.pending_exchanges.is_empty());
            assert_eq!(
                discovery.available_tickers[&Exchange::HyperliquidLinear.to_string()],
                vec!["BTC"]
            );
            drop(discovery);
            drop(guard);
            assert!(state.read().recovering_exchanges.is_empty());
        });
        http.join().expect("fault HTTP server finished");
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn unresolved_success_is_retried_until_cancelled() {
        runtime().block_on(async {
            let shutdown = CancellationToken::new();
            let mut calls = 0;
            let result = retry_resolve_exchange(
                Exchange::HyperliquidLinear,
                &["BTC".into()],
                &whitelist(),
                &shutdown,
                Duration::from_millis(1),
                Duration::from_millis(2),
                || {
                    calls += 1;
                    if calls == 3 {
                        shutdown.cancel();
                    }
                    std::future::ready(Ok(ExchangeMetadata::default()))
                },
            )
            .await;
            assert!(result.is_none());
            assert_eq!(calls, 3);
        });
    }

    #[test]
    fn cancellation_interrupts_fetch_and_backoff() {
        runtime().block_on(async {
            for during_fetch in [false, true] {
                let shutdown = CancellationToken::new();
                let worker_shutdown = shutdown.clone();
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let task = tokio::spawn(async move {
                    let mut started_tx = Some(started_tx);
                    retry_resolve_exchange(
                        Exchange::HyperliquidLinear,
                        &["BTC".into()],
                        &whitelist(),
                        &worker_shutdown,
                        if during_fetch {
                            Duration::ZERO
                        } else {
                            Duration::from_secs(30)
                        },
                        Duration::from_secs(30),
                        || {
                            if let Some(tx) = started_tx.take() {
                                let _ = tx.send(());
                            }
                            std::future::pending::<anyhow::Result<ExchangeMetadata>>()
                        },
                    )
                    .await
                });
                if during_fetch {
                    started_rx.await.expect("fetch started");
                } else {
                    tokio::task::yield_now().await;
                }
                shutdown.cancel();
                assert!(
                    tokio::time::timeout(Duration::from_secs(1), task)
                        .await
                        .unwrap()
                        .unwrap()
                        .is_none()
                );
            }
        });
    }
}
