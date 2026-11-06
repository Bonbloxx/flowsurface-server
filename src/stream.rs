use rustc_hash::FxHashMap;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use flowsurface_exchange::TickerInfo;
use flowsurface_exchange::adapter::{AdapterHandles, Event, StreamKind};

use flowsurface_exchange::adapter::{
    Exchange, MAX_KLINE_STREAMS_PER_STREAM, StreamConfig, StreamTicksize,
};
use flowsurface_exchange::{PushFrequency, TickMultiplier, Timeframe};
use futures::{StreamExt, stream::BoxStream, stream::select_all};

use crate::{
    config::WhitelistTemplates,
    discovery::{self, RecoveryGuard, SharedDiscovery},
    storage::Storage,
};

/// How many events are buffered for best-effort consumers before
/// the slowest lagged receiver starts dropping messages.
const BROADCAST_CAPACITY: usize = 1024;

/// Capacity of the channel from exchange stream tasks to the event
/// router. A full channel applies backpressure all the way to the adapter;
/// execution events are never discarded.
const EVENT_CHANNEL_CAPACITY: usize = 1_000_000;

/// Capacity of the channel from the event router to the batch flusher.
/// A full channel pauses the router rather than losing persisted truth.
const PERSIST_CHANNEL_CAPACITY: usize = 500_000;

/// Aggregates all outbound channels for market-data events.
///
/// Both channels carry raw [`Event`]s — no variant-specific typing
/// until the final consumer.
#[derive(Clone)]
struct EventOutlets {
    /// Persist channel (bounded; applies backpressure when full).
    /// Single consumer — the persister in `storage.rs`.
    persist: Option<mpsc::Sender<Event>>,
    /// Best-effort fan-out to WebSocket API, alerters, etc.
    broadcast: Option<broadcast::Sender<Event>>,
}

impl EventOutlets {
    fn new() -> Self {
        Self {
            persist: None,
            broadcast: None,
        }
    }

    fn with_persist(mut self, tx: mpsc::Sender<Event>) -> Self {
        self.persist = Some(tx);
        self
    }

    fn with_broadcast(mut self, tx: broadcast::Sender<Event>) -> Self {
        self.broadcast = Some(tx);
        self
    }

    /// Handle every [`Event`] from the stream engine and route it to
    /// whichever outlets are configured.
    async fn route(self, mut event_rx: mpsc::Receiver<Event>) {
        // Do not select on cancellation here. Producers observe shutdown and
        // drop their senders; receiving through channel closure drains every
        // event already accepted by the bounded lossless pipeline.
        while let Some(event) = event_rx.recv().await {
            match &event {
                Event::Connected(streams) => tracing::info!(?streams, "Stream connected"),
                Event::ReplayRecovered(streams, from) => {
                    tracing::info!(?streams, %from, "Reconnect snapshot proved stream continuity")
                }
                Event::Disconnected(streams, reason) => {
                    tracing::warn!(?streams, %reason, "Stream disconnected")
                }
                _ => {}
            }

            if let Some(ref tx) = self.broadcast
                && tx.receiver_count() > 0
            {
                let _ = tx.send(event.clone());
            }

            if let Some(ref tx) = self.persist
                && tx.send(event).await.is_err()
            {
                tracing::info!("Persist channel closed, stopping handler");
                return;
            }
        }
        tracing::info!("Event channel drained and closed; handler ending");
    }
}

#[allow(dead_code)]
pub struct StreamManager {
    /// All outbound channels.  The event handler gets its own clone.
    outlets: EventOutlets,
    /// Per-exchange stream engine tasks.
    stream_tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Event handler task.
    event_task: Option<tokio::task::JoinHandle<()>>,
}

/// Receiving end of the persist channel created by
/// [`StreamManager::start_streams`].  The caller hands this to the persister
/// in `storage.rs`, which is the first place that matches on [`Event`]
/// variants.
#[derive(Default)]
pub struct StreamReceivers {
    /// Persist channel — carries every [`Event`] as-is.
    pub persist: Option<mpsc::Receiver<Event>>,
}

impl StreamManager {
    /// Build the full streaming pipeline.
    ///
    /// Returns the manager plus a [`StreamReceivers`] containing the
    /// persist receiver.
    #[allow(clippy::too_many_arguments)]
    pub fn start_streams(
        handles: AdapterHandles,
        pairs: &[TickerInfo],
        shutdown: CancellationToken,
        storage: Storage,
        discovery: SharedDiscovery,
        base_assets: &[String],
        whitelist: &WhitelistTemplates,
    ) -> (Self, StreamReceivers) {
        let (persist_tx, persist_rx) = mpsc::channel(PERSIST_CHANNEL_CAPACITY);
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(BROADCAST_CAPACITY);

        let mut all_streams: Vec<StreamKind> = Vec::with_capacity(pairs.len());
        for ti in pairs {
            all_streams.push(StreamKind::Trades { ticker_info: *ti });
        }

        // Low-level per-exchange WebSocket tasks.
        let (event_rx, stream_tasks) = {
            let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

            let mut by_exchange: FxHashMap<Exchange, Vec<StreamKind>> = FxHashMap::default();
            for stream in &all_streams {
                let exchange = stream.ticker_info().exchange();
                by_exchange.entry(exchange).or_default().push(*stream);
            }

            let mut tasks = Vec::new();
            for (exchange, exchange_streams) in by_exchange {
                let tx = event_tx.clone();
                let shutdown = shutdown.child_token();
                let handles = handles.clone();

                tasks.push(tokio::spawn(async move {
                    if let Err(e) =
                        run_exchange_streams(handles, exchange, exchange_streams, tx, shutdown)
                            .await
                    {
                        tracing::error!("Stream engine for {exchange} exited: {e:#}");
                    }
                }));
            }

            // A retry producer owns its event sender for its entire lifetime,
            // including startup with zero resolved pairs. This keeps the router
            // and flusher alive until recovery or an orderly cancellation.
            let pending = discovery.read().pending_exchanges.clone();
            for exchange in pending {
                let tx = event_tx.clone();
                let shutdown = shutdown.child_token();
                let handles = handles.clone();
                let storage = storage.clone();
                let discovery = discovery.clone();
                let base_assets = base_assets.to_vec();
                let whitelist = whitelist.clone();
                let recovery_guard = RecoveryGuard::new(discovery.clone(), exchange);
                tasks.push(tokio::spawn(async move {
                    let Some(recovered) = discovery::retry_resolve_exchange(
                        exchange,
                        &base_assets,
                        &whitelist,
                        &shutdown,
                        discovery::INITIAL_RETRY_DELAY,
                        discovery::MAX_RETRY_DELAY,
                        || async {
                            handles
                                .fetch_ticker_metadata(exchange.venue(), &[exchange.market_type()])
                                .await
                                .map(|metadata| metadata.into_iter().collect())
                                .map_err(anyhow::Error::from)
                        },
                    )
                    .await
                    else {
                        return;
                    };

                    // A metadata storage failure must not silently abandon the
                    // recovered venue. Keep it pending and retry this write.
                    loop {
                        if shutdown.is_cancelled() {
                            return;
                        }
                        let storage = storage.clone();
                        let pairs = recovered.pairs.clone();
                        let result = tokio::task::spawn_blocking(move || {
                            storage.store_ticker_infos(&pairs)
                        })
                        .await;
                        match result {
                            Ok(Ok(())) => break,
                            error => {
                                tracing::error!(%exchange, ?error, "Failed to persist recovered ticker metadata; retrying");
                                tokio::select! {
                                    biased;
                                    _ = shutdown.cancelled() => return,
                                    _ = tokio::time::sleep(discovery::MAX_RETRY_DELAY) => {}
                                }
                            }
                        }
                    }
                    if shutdown.is_cancelled() {
                        return;
                    }
                    discovery.write().publish_recovered(&recovered);
                    drop(recovery_guard);
                    let streams = recovered
                        .pairs
                        .into_iter()
                        .map(|ticker_info| StreamKind::Trades { ticker_info })
                        .collect();
                    if let Err(error) =
                        run_exchange_streams(handles, exchange, streams, tx, shutdown).await
                    {
                        tracing::error!(%exchange, "Recovered stream engine exited: {error:#}");
                    }
                }));
            }

            // Drop the original sender so all clones live inside the tasks.
            // When the tasks finish (on cancellation) the receiver gets None.
            drop(event_tx);

            (event_rx, tasks)
        };

        let outlets = EventOutlets::new()
            .with_persist(persist_tx)
            .with_broadcast(broadcast_tx);

        let event_task = tokio::spawn(outlets.clone().route(event_rx));

        (
            Self {
                outlets,
                stream_tasks,
                event_task: Some(event_task),
            },
            StreamReceivers {
                persist: Some(persist_rx),
            },
        )
    }

    /// Return a new broadcast receiver for downstream consumers, or
    /// `None` if the broadcast channel hasn't been initialised yet
    /// (which should not happen after `start_streams`).
    #[allow(dead_code)]
    pub fn subscribe(&self) -> Option<broadcast::Receiver<Event>> {
        self.outlets.broadcast.as_ref().map(|tx| tx.subscribe())
    }

    /// Join all streaming tasks.  The caller must have cancelled the
    /// shutdown token and dropped the trade sender first.
    pub async fn shutdown(mut self) {
        for h in self.stream_tasks {
            let _ = h.await;
        }
        if let Some(h) = self.event_task.take() {
            let _ = h.await;
        }
        // outlets (and all its senders) are dropped here.
    }
}

/// Subscribe to **all** stream kinds for a single `exchange` and forward
/// every [`Event`] into `tx` until cancelled or all streams end.
async fn run_exchange_streams(
    handles: AdapterHandles,
    exchange: Exchange,
    streams: Vec<StreamKind>,
    tx: mpsc::Sender<Event>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    // Separate streams by kind so we can build the right StreamConfig.
    let mut trade_tickers: Vec<TickerInfo> = Vec::new();
    let mut depth_specs: Vec<(TickerInfo, StreamTicksize, PushFrequency)> = Vec::new();
    let mut kline_specs: Vec<(TickerInfo, Timeframe)> = Vec::new();

    for stream in &streams {
        match stream {
            StreamKind::Trades { ticker_info } => trade_tickers.push(*ticker_info),
            StreamKind::Depth {
                ticker_info,
                depth_aggr,
                push_freq,
            } => depth_specs.push((*ticker_info, *depth_aggr, *push_freq)),
            StreamKind::Kline {
                ticker_info,
                timeframe,
            } => kline_specs.push((*ticker_info, *timeframe)),
        }
    }

    let mut box_streams: Vec<BoxStream<'static, Event>> = Vec::new();

    // ── Trade stream ─────────────────────────────────────────────
    // All tickers batched into one WebSocket connection per exchange.
    if !trade_tickers.is_empty() {
        let symbols: Vec<String> = trade_tickers
            .iter()
            .map(|ti| ti.ticker.to_string().to_lowercase())
            .collect();
        let cfg = StreamConfig::new(trade_tickers, exchange, None, PushFrequency::ServerDefault);
        box_streams.push(handles.trade_stream(&cfg));
        tracing::info!(%exchange, symbols = ?symbols, "Subscribed to trade stream");
    }

    // ── Depth streams ────────────────────────────────────────────
    // One BoxStream per ticker because depth uses StreamConfig<TickerInfo>.
    for (ticker, aggr, push_freq) in &depth_specs {
        let tick_mltp: Option<TickMultiplier> = match aggr {
            StreamTicksize::Client => None,
            StreamTicksize::ServerSide(m) => Some(*m),
        };
        let cfg = StreamConfig::new(*ticker, exchange, tick_mltp, *push_freq);
        box_streams.push(handles.depth_stream(&cfg));
        tracing::info!(%exchange, ticker = %ticker.ticker, "Subscribed to depth stream");
    }

    // ── Kline streams ────────────────────────────────────────────
    // Chunked so each connection stays within MAX_KLINE_STREAMS_PER_STREAM.
    if !kline_specs.is_empty() {
        for chunk in kline_specs.chunks(MAX_KLINE_STREAMS_PER_STREAM) {
            let cfg =
                StreamConfig::new(chunk.to_vec(), exchange, None, PushFrequency::ServerDefault);
            box_streams.push(handles.kline_stream(&cfg));
            tracing::info!(%exchange, count = chunk.len(), "Subscribed to kline chunk");
        }
    }

    let mut merged = select_all(box_streams);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(%exchange, "Stream engine cancelled, shutting down");
                break;
            }
            event = merged.next() => {
                match event {
                    Some(event) => {
                        if tx.send(event).await.is_err() {
                            tracing::info!(%exchange, "Event channel closed, stopping stream engine");
                            break;
                        }
                    }
                    None => {
                        tracing::info!(%exchange, "All streams ended");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::ConfigMarketKind, discovery::DiscoveryState};
    use flowsurface_exchange::Ticker;
    use std::{sync::Arc, time::Duration};

    fn runtime() -> tokio::runtime::Runtime {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
    }

    #[test]
    fn router_drains_accepted_events_after_producers_close() {
        runtime().block_on(async {
            let (event_tx, event_rx) = mpsc::channel(64);
            let (persist_tx, mut persist_rx) = mpsc::channel(2);
            let task = tokio::spawn(EventOutlets::new().with_persist(persist_tx).route(event_rx));
            let pair = TickerInfo::new(
                Ticker::new("BTC", Exchange::HyperliquidLinear),
                0.1,
                0.001,
                None,
            );
            for _ in 0..32 {
                event_tx
                    .send(Event::Connected(
                        vec![StreamKind::Trades { ticker_info: pair }].into(),
                    ))
                    .await
                    .unwrap();
            }
            drop(event_tx);
            let mut accepted = 0;
            while persist_rx.recv().await.is_some() {
                accepted += 1;
            }
            task.await.expect("router joined");
            assert_eq!(
                accepted, 32,
                "backpressured router preserves every accepted event"
            );
        });
    }

    #[test]
    fn zero_resolved_pairs_keep_pipeline_alive_until_retry_cancels() {
        let dir = std::env::temp_dir().join(format!(
            "flowsurface-recovery-drain-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        runtime().block_on(async {
            let storage = Storage::open(&dir, 64, 1, 0).expect("test storage");
            let whitelist: WhitelistTemplates = [(
                "hyperliquid".into(),
                [(ConfigMarketKind::Linear, vec!["USDC".into()])]
                    .into_iter()
                    .collect(),
            )]
            .into_iter()
            .collect();
            let base_assets = vec!["BTC".into()];
            let state = Arc::new(parking_lot::RwLock::new(DiscoveryState::new(
                &base_assets,
                &whitelist,
                &discovery::MetadataCache::default(),
                Vec::new(),
            )));
            let shutdown = CancellationToken::new();
            let handles = AdapterHandles::spawn_venues(&reqwest::Client::new(), [], None);
            let (manager, mut receivers) = StreamManager::start_streams(
                handles,
                &[],
                shutdown.clone(),
                storage.clone(),
                state.clone(),
                &base_assets,
                &whitelist,
            );
            let flusher =
                storage.spawn_batch_flusher(&mut receivers, Duration::from_millis(10), 16);
            tokio::task::yield_now().await;
            assert!(
                !flusher.is_finished(),
                "unresolved producer keeps persistence channel open"
            );
            assert_eq!(
                state.read().recovering_exchanges,
                vec![Exchange::HyperliquidLinear]
            );
            shutdown.cancel();
            tokio::time::timeout(Duration::from_secs(2), manager.shutdown())
                .await
                .expect("retry cancellation joined");
            tokio::time::timeout(Duration::from_secs(2), flusher)
                .await
                .expect("flusher drained")
                .expect("flusher joined");
            assert!(state.read().recovering_exchanges.is_empty());
            assert_eq!(
                state.read().pending_exchanges,
                vec![Exchange::HyperliquidLinear]
            );
        });
        std::fs::remove_dir_all(&dir).expect("remove test storage");
    }
}
