mod api;
mod cleanup;
mod config;
mod discovery;
mod limiter;
mod storage;
mod stream;
mod tls;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use flowsurface_exchange::adapter::AdapterHandles;

use crate::api::{Server, query_concurrency_limit};
use crate::config::{Args, BearerToken, Config, WhitelistTemplates};
use crate::limiter::{ADMISSION_GLOBAL_CAP, ADMISSION_PER_IP_BUDGET, AdmissionGate, RateLimiter};
use crate::storage::Storage;

#[tokio::main]
async fn main() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install aws-lc-rs as the default rustls CryptoProvider");

    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,flowsurface_exchange=warn")),
        )
        .init();

    let args = Args::parse();
    let config_path = Config::resolve_path(args.config);
    let mut config = Config::load_or_write_template(&config_path);

    let data_dir = if Path::new(&config.storage.data_dir).is_relative() {
        config_path
            .parent()
            .expect("config path has no parent")
            .join(&config.storage.data_dir)
    } else {
        PathBuf::from(&config.storage.data_dir)
    };

    if let Err(e) = config.resolve_auth_token(&data_dir) {
        tracing::error!("{e:#}");
        std::process::exit(1);
    }

    let app = App::new(&config, &data_dir).await;
    let handles = app.serve().await;
    handles.shutdown().await;
}

struct App {
    storage: Storage,
    adapter_handles: AdapterHandles,
    discovery: discovery::SharedDiscovery,
    base_assets: Vec<String>,
    whitelist: WhitelistTemplates,
    bind_address: SocketAddr,
    auth_token: Option<BearerToken>,
    flush_interval: std::time::Duration,
    max_buffered_trades: usize,
    cleanup_scheduler: cleanup::CleanupScheduler,
    tls_config: Option<axum_server::tls_rustls::RustlsConfig>,
    rate_limiter: Option<RateLimiter>,
    query_concurrency: usize,
}

impl App {
    /// Open storage, resolve configured pairs, persist ticker metadata.
    async fn new(config: &Config, data_dir: &Path) -> Self {
        let storage = Storage::open(
            data_dir,
            config.storage.memory_limit_mb,
            config.storage.threads,
            config.storage.max_storage_mb,
        )
        .unwrap_or_else(|e| {
            tracing::error!("Failed to initialise storage: {e:#}");
            std::process::exit(1);
        });

        let cleanup_config = cleanup::CleanupConfig::from_config(
            config.storage.data_retention_hours,
            config.storage.max_storage_mb,
        )
        .unwrap_or_else(|e| {
            tracing::error!("Invalid cleanup configuration: {e:#}");
            std::process::exit(1);
        });

        let cleanup_scheduler = cleanup::CleanupScheduler::new(&storage, cleanup_config)
            .unwrap_or_else(|e| {
                tracing::error!("{e:#}");
                std::process::exit(1);
            });

        let whitelist = config.resolve_whitelist();
        if !config.pairs.discovery_mode
            && (whitelist.is_empty() || config.pairs.base_assets.is_empty())
        {
            tracing::error!("No pairs configured. Set base_assets and whitelist in config.toml");
            std::process::exit(1);
        }

        let (adapter_handles, metadata_cache, resolved_pairs) = discovery::setup_pairs(
            &config.pairs.base_assets,
            config.pairs.discovery_mode,
            &whitelist,
        )
        .await;

        let discovery = Arc::new(parking_lot::RwLock::new(discovery::DiscoveryState::new(
            &config.pairs.base_assets,
            &whitelist,
            &metadata_cache,
            resolved_pairs.clone(),
        )));

        if resolved_pairs.is_empty() {
            if !discovery.read().pending_exchanges.is_empty() {
                tracing::warn!(
                    "No configured pairs resolved yet; continuing with automatic exchange discovery recovery"
                );
            } else if config.pairs.discovery_mode {
                tracing::warn!(
                    "No matching pairs for base_assets {:?} with current whitelist \
                     - discovery mode is on, so /exchanges is still populated.",
                    config.pairs.base_assets
                );
            } else {
                tracing::error!(
                    "No matching pairs found for base_assets {:?} with current whitelist",
                    config.pairs.base_assets
                );
                std::process::exit(1);
            }
        }

        tracing::info!(
            "Tracking {} pair(s) across {} exchange(s)",
            resolved_pairs.len(),
            resolved_pairs
                .iter()
                .map(|ti| ti.exchange())
                .collect::<rustc_hash::FxHashSet<_>>()
                .len()
        );

        // Persist ticker metadata for the API layer.
        if let Err(e) = storage.store_ticker_infos(&resolved_pairs) {
            tracing::warn!("Failed to persist ticker metadata: {e:#}");
        }

        let tls_config = tls::setup_tls_config(
            data_dir,
            &storage,
            config.network.bind_address,
            &config.network.tls_domain,
        )
        .await
        .unwrap_or_else(|e| {
            tracing::error!("{e:#}");
            std::process::exit(1);
        });

        let rate_limiter = config.network.rate_limit_max().map(|max| {
            tracing::info!(
                "Rate limiting enabled: max {max} req/10s per IP (≈ {} req/s)",
                max / 10
            );
            RateLimiter::new(max, std::time::Duration::from_secs(10))
        });

        Self {
            storage,
            adapter_handles,
            discovery,
            base_assets: config.pairs.base_assets.clone(),
            whitelist,
            bind_address: config.network.bind_address,
            auth_token: config.auth_token.clone(),
            flush_interval: std::time::Duration::from_millis(config.storage.flush_interval_ms),
            max_buffered_trades: config.storage.max_buffered_trades,
            tls_config,
            cleanup_scheduler,
            rate_limiter,
            query_concurrency: query_concurrency_limit(config.storage.memory_limit_mb),
        }
    }

    /// Start the pipeline (stream engine, flusher, cleanup) and the HTTP server.
    async fn serve(self) -> AppHandles {
        let shutdown = CancellationToken::new();
        let resolved_pairs = self.discovery.read().resolved_pairs.clone();

        let (stream_mgr, mut rx) = stream::StreamManager::start_streams(
            self.adapter_handles,
            &resolved_pairs,
            shutdown.child_token(),
            self.storage.clone(),
            self.discovery.clone(),
            &self.base_assets,
            &self.whitelist,
        );

        let cleanup_last_run = tokio::task::spawn_blocking({
            let scheduler = self.cleanup_scheduler.clone();
            move || scheduler.run_pass()
        })
        .await
        .unwrap_or(None);

        let _cleanup = self
            .cleanup_scheduler
            .spawn(cleanup_last_run, shutdown.child_token());

        let flusher = self.storage.spawn_batch_flusher(
            &mut rx,
            self.flush_interval,
            self.max_buffered_trades,
        );

        let admission_gate = AdmissionGate::new(ADMISSION_PER_IP_BUDGET, ADMISSION_GLOBAL_CAP);
        if self.auth_token.is_some() {
            tracing::info!(
                "Admission gate active: {ADMISSION_PER_IP_BUDGET} req/s per unknown IP, \
                 max {ADMISSION_GLOBAL_CAP} req/s total; authenticated IPs bypass the gate",
            );
        }
        let server = Arc::new(Server::new(
            self.storage,
            self.auth_token,
            self.discovery,
            self.tls_config,
            self.rate_limiter,
            admission_gate,
            self.query_concurrency,
        ));
        let (server_task, server_shutdown_handle) = server.serve(self.bind_address).await;

        AppHandles {
            shutdown,
            stream_mgr: Some(stream_mgr),
            flusher,
            _cleanup,
            _server: server_task,
            server_shutdown_handle,
        }
    }
}

/// Runtime handles for the active pipeline - provides ordered shutdown.
struct AppHandles {
    shutdown: CancellationToken,
    stream_mgr: Option<stream::StreamManager>,
    flusher: tokio::task::JoinHandle<()>,
    _cleanup: tokio::task::JoinHandle<()>,
    _server: tokio::task::JoinHandle<()>,
    server_shutdown_handle: axum_server::Handle<std::net::SocketAddr>,
}

impl AppHandles {
    /// Wait for SIGINT/SIGTERM, then stop producers, drain every accepted
    /// event, and durably flush all trades before allowing process exit.
    async fn shutdown(mut self) {
        wait_for_shutdown_signal().await;

        tracing::info!("Shutting down…");

        // 1. Cancel all stream tasks (they share the root token).
        self.shutdown.cancel();

        // 2. Streaming tasks stop sending, then the router drains its input
        // channel and finally drops the persist sender.
        if let Some(mgr) = self.stream_mgr.take() {
            mgr.shutdown().await;
        }

        // 3. The flusher sees channel closure only after the router drain and
        // retries until every accepted batch is durable.
        let _ = self.flusher.await;

        // 4. Background cleanup may be mid-purge/CHECKPOINT; do not abandon it while it
        // owns the database.
        let _ = self._cleanup.await;

        // 5. Stop accepting HTTP work and drain in-flight responses.
        self.server_shutdown_handle
            .graceful_shutdown(Some(std::time::Duration::from_secs(3)));
        let _ = self._server.await;

        tracing::info!("Shutdown complete.");
    }
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    tokio::select! {
        _ = sigint.recv() => {}
        _ = sigterm.recv() => {}
    }
}

#[cfg(windows)]
async fn wait_for_shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to register Ctrl-C handler");
}
