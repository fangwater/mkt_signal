use anyhow::Result;
use log::{info, warn};
use mkt_signal::viz::config::VizCfg;
use mkt_signal::viz::fr_state::spawn_fr_state_listeners;
use mkt_signal::viz::server::{serve_http, WsHub};
use mkt_signal::viz::subscribers::spawn_pre_trade_resample_listeners_with_cfg;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let cfg_path = std::env::var("VIZ_CFG").unwrap_or_else(|_| "config/viz.toml".to_string());
    let cfg = VizCfg::load(&cfg_path).await?;
    info!("viz_server config loaded from {}", cfg_path);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            for server in cfg.servers {
                let hub = WsHub::new(128);
                let http_cfg = server.http.clone();

                if let Err(err) = spawn_pre_trade_resample_listeners_with_cfg(hub.clone(), &server)
                {
                    warn!(
                        "spawn pre_trade resample listener failed (port={}): {err:#}",
                        http_cfg.port
                    );
                }

                if let Err(err) = spawn_fr_state_listeners(hub.clone(), &server) {
                    warn!(
                        "spawn fr_state listener failed (port={}): {err:#}",
                        http_cfg.port
                    );
                }

                tokio::task::spawn_local(async move {
                    if let Err(err) = serve_http(http_cfg, hub).await {
                        warn!("viz http server exited: {err:#}");
                    }
                });
            }

            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok(())
        })
        .await
}
