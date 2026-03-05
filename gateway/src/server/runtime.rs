use std::sync::Arc;

use tokio::net::TcpListener;

use super::Gateway;
use crate::error::GatewayError;
use crate::router::create_router;

impl Gateway {
    /// Start serving requests
    ///
    /// # Errors
    /// Returns error if server fails to start
    pub async fn serve(&self) -> Result<(), GatewayError> {
        let state = self.state.as_ref().ok_or_else(|| {
            GatewayError::Config("Gateway not initialized. Call init() first.".to_string())
        })?;

        let router = create_router(Arc::clone(state), &self.custom_routes);

        let addr = &self.config.bind_address;
        tracing::info!("🚀 QAIL Gateway starting on {}", addr);
        tracing::info!("   POST /qail     - Execute QAIL queries");
        tracing::info!("   POST /qail/export - Stream COPY export queries");
        tracing::info!("   GET  /health   - Health check");
        tracing::info!("   GET  /api/*    - Auto-REST endpoints");
        if !self.custom_routes.is_empty() {
            tracing::info!("   {} custom handler(s)", self.custom_routes.len());
        }

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| GatewayError::Config(format!("Failed to bind to {}: {}", addr, e)))?;

        let state_metrics = Arc::clone(state);
        tokio::spawn(async move {
            loop {
                let active = state_metrics.pool.active_count();
                let max = state_metrics.pool.max_connections();
                let idle = state_metrics.pool.idle_count().await;

                crate::metrics::record_pool_stats(active, idle, max);

                if let Ok(stat) = std::fs::read_to_string("/proc/self/stat")
                    && let Some(rss_pages) = stat
                        .split_whitespace()
                        .nth(23)
                        .and_then(|s| s.parse::<u64>().ok())
                {
                    metrics::gauge!("process_resident_memory_bytes").set((rss_pages * 4096) as f64);
                }

                let cache_stats = state_metrics.cache.stats();
                metrics::gauge!("qail_cache_entries").set(cache_stats.entries as f64);
                metrics::gauge!("qail_cache_weighted_bytes").set(cache_stats.weighted_size as f64);

                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });

        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| GatewayError::Internal(e.into()))?;

        tracing::info!("In-flight requests drained. Closing connection pool...");
        state.pool.close().await;
        tracing::info!("Gateway shutdown complete.");

        Ok(())
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %err, "Failed to install Ctrl+C handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "Failed to install SIGTERM handler");
                std::future::pending::<Option<()>>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received Ctrl+C, starting graceful shutdown..."),
        _ = terminate => tracing::info!("Received SIGTERM, starting graceful shutdown..."),
    }
}
