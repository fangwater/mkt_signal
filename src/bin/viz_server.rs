use anyhow::Result;
use log::{info, warn};
use mkt_signal::common::viz::config::VizCfg;
use mkt_signal::common::viz::sampler::Sampler;
use mkt_signal::common::viz::server::{serve_http, WsHub};
use mkt_signal::common::viz::state::SharedState;
use mkt_signal::common::viz::subscribers::{
    spawn_fr_resample_listener, spawn_pre_trade_resample_listeners,
};
use mkt_signal::exchange::Exchange;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let cfg_path = std::env::var("VIZ_CFG").unwrap_or_else(|_| "config/viz.toml".to_string());
    let cfg = VizCfg::load(&cfg_path).await?;
    info!("viz_server config loaded from {}", cfg_path);

    let state = SharedState::new();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let hub = WsHub::new(128, cfg.http.auth_token.clone());
            let http_cfg = cfg.http.clone();

            // 订阅 funding 与 pre_trade resample 流
            if let Err(err) = spawn_fr_resample_listener(state.clone(), hub.clone()) {
                warn!("spawn fr_resample listener failed: {err:#}");
            }
            if let Err(err) = spawn_pre_trade_resample_listeners(state.clone(), hub.clone()) {
                warn!("spawn pre_trade resample listener failed: {err:#}");
            }

            // 采样任务
            let mut sampler = Sampler::new(
                state.clone(),
                cfg.sampling.clone(),
                cfg.thresholds.clone(),
                Exchange::BinanceFutures,
            );
            let interval_ms = cfg.sampling.interval_ms;
            let hub_clone = hub.clone();
            tokio::task::spawn_local(async move {
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_millis(interval_ms));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticker.tick().await;
                    let snap = sampler.build_snapshot();
                    match sampler.encode_json(&snap) {
                        Ok(Some(json)) => hub_clone.broadcast(json),
                        Ok(None) => {}
                        Err(err) => warn!("encode snapshot failed: {err:#}"),
                    }
                }
            });

            serve_http(http_cfg, hub).await
        })
        .await
}
