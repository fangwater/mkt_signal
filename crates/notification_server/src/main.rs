mod config;
mod model;
mod server;
mod telegram;

use anyhow::{Context, Result};
use config::AppConfig;
use log::{error, info};
use model::QueuedNotification;
use server::{build_router, DeliveryStats};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use telegram::{NotificationSink, TelegramSink};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let config = AppConfig::from_env()?;
    let sink: Arc<dyn NotificationSink> = Arc::new(TelegramSink::new(
        config.bot_token.clone(),
        config.chat_id.clone(),
        config.message_thread_id,
        config.disable_notification,
        config.request_timeout,
        config.retry_attempts,
        config.retry_base_delay,
        config.dry_run,
    )?);
    let stats = Arc::new(DeliveryStats::default());
    let (sender, receiver) = mpsc::channel(config.queue_capacity);
    let worker_stats = stats.clone();
    let worker = tokio::spawn(run_delivery_worker(receiver, sink, worker_stats));

    let app = build_router(
        sender,
        stats,
        config.queue_capacity,
        config.max_message_chars,
        config.api_token.clone(),
        config.dry_run,
    );
    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .with_context(|| format!("bind notification server to {}", config.bind_addr))?;
    info!(
        "notification server listening addr={} provider=telegram dry_run={} topic={} silent={} auth={}",
        config.bind_addr,
        config.dry_run,
        config.message_thread_id.is_some(),
        config.disable_notification,
        config.api_token.is_some()
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("notification server stopped unexpectedly")?;
    worker
        .await
        .context("notification delivery worker failed")?;
    Ok(())
}

async fn run_delivery_worker(
    mut receiver: mpsc::Receiver<QueuedNotification>,
    sink: Arc<dyn NotificationSink>,
    stats: Arc<DeliveryStats>,
) {
    while let Some(notification) = receiver.recv().await {
        match sink.deliver(&notification).await {
            Ok(()) => {
                stats.delivered.fetch_add(1, Ordering::Relaxed);
                info!(
                    "notification delivered event_id={} accepted_at={} source={} severity={}",
                    notification.id,
                    notification.accepted_at,
                    notification.request.source,
                    notification.request.severity
                );
            }
            Err(err) => {
                stats.failed.fetch_add(1, Ordering::Relaxed);
                error!(
                    "notification delivery failed event_id={} accepted_at={} source={} severity={} error={:#}",
                    notification.id,
                    notification.accepted_at,
                    notification.request.source,
                    notification.request.severity,
                    err
                );
            }
        }
    }
    info!("notification delivery worker stopped after the queue closed");
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("notification server shutdown signal received");
}
