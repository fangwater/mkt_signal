use anyhow::Result;
use log::{info, warn};
use mkt_signal::common::viz::config::VizCfg;
use mkt_signal::common::viz::sampler::Sampler;
use mkt_signal::common::viz::server::{serve_http, WsHub};
use mkt_signal::common::viz::state::SharedState;
use mkt_signal::common::viz::subscribers::{spawn_account_listener, spawn_derivatives_listener, spawn_fr_resample_listener};
use mkt_signal::exchange::Exchange;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() { std::env::set_var("RUST_LOG", "info"); }
    env_logger::init();

    let cfg_path = std::env::var("VIZ_CFG").unwrap_or_else(|_| "config/viz.toml".to_string());
    let cfg = VizCfg::load(&cfg_path).await?;
    info!("viz_server config loaded from {}", cfg_path);

    let state = SharedState::new();

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            // 订阅来源（在 LocalSet 上下文中启动）
            if let Some(account) = cfg.sources.account.clone() {
                if let Err(err) = spawn_account_listener(account.service, account.label, state.clone()) {
                    warn!("spawn account listener failed: {err:#}");
                }
            } else {
                warn!("no account source configured; exposure/pnl may be empty");
            }
            if let Some(der) = cfg.sources.derivatives.clone() {
                if let Err(err) = spawn_derivatives_listener(der.service, state.clone()) {
                    warn!("spawn derivatives listener failed: {err:#}");
                }
            } else {
                warn!("no derivatives source configured; price/factors may be empty");
            }

            // 订阅 resample 批量快照（默认启用）
            if let Err(err) = spawn_fr_resample_listener(state.clone()) {
                warn!("spawn fr_resample listener failed: {err:#}");
            }

            // WS Hub & HTTP server
            let hub = WsHub::new(128, cfg.http.auth_token.clone());
            let http_cfg = cfg.http.clone();

            // 采样任务
            let mut sampler = Sampler::new(state.clone(), cfg.sampling.clone(), cfg.thresholds.clone(), Exchange::BinanceFutures);
            let interval_ms = cfg.sampling.interval_ms;
            let hub_clone = hub.clone();
            tokio::task::spawn_local(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
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
