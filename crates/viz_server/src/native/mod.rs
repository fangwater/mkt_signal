pub mod config;
mod relay;
mod server;

use anyhow::Result;
use cn_futures_state::transport::StateSubscriber;
use config::{Config, Counter};
use relay::Hub;
use std::time::Duration;

const DASHBOARD: &str = include_str!("../../../../web/cn_futures_viz/index.html");
fn now_us() -> u64 {
    runtime_common::time_util::get_timestamp_us().max(0) as u64
}

fn spawn(source: Counter, hub: Hub) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_local(async move {
        let mut subscriber = None;
        let mut retry = tokio::time::Instant::now();
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if subscriber.is_none() && tokio::time::Instant::now() >= retry {
                match StateSubscriber::open(&source.position_namespace) {
                    Ok(s) => subscriber = Some(s),
                    Err(_) => hub.unavailable(&source, "等待持仓 IPC"),
                }
                retry = tokio::time::Instant::now() + Duration::from_secs(1);
            }
            if let Some(s) = &subscriber {
                let result = s.latest().and_then(|snapshot| {
                    if let Some(snapshot) = snapshot {
                        hub.apply(&source, snapshot)?;
                    }
                    Ok(())
                });
                if let Err(error) = result {
                    log::warn!("native IPC source {}: {error:#}", source.id);
                    hub.unavailable(&source, "持仓 IPC 无效，请检查来源与账户绑定");
                    subscriber = None;
                }
            }
        }
    })
}
pub async fn run(cfg: Config) -> Result<()> {
    cfg.validate()?;
    tokio::task::LocalSet::new()
        .run_until(async move {
            let hub = Hub::new(&cfg.counters, cfg.stale_after_ms);
            let listeners: Vec<_> = cfg
                .counters
                .into_iter()
                .map(|source| spawn(source, hub.clone()))
                .collect();
            let outcome = tokio::select! {
                result = server::serve(cfg.http, hub) => result,
                _ = tokio::signal::ctrl_c() => Ok(()),
            };
            for listener in listeners {
                listener.abort();
            }
            outcome
        })
        .await
}
#[cfg(test)]
mod tests;
