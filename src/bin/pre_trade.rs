use anyhow::Result;
use log::info;
use mkt_signal::pre_trade::{config::PreTradeCfg, PreTrade};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    let cfg_path = std::env::var("PRE_TRADE_CFG").unwrap_or_else(|_| "config/pre_trade.toml".to_string());
    let cfg = PreTradeCfg::load(&cfg_path).await?;
    info!("pre_trade base config loaded from {}", cfg_path);

    let pre_trade = PreTrade::new(cfg);
    let local = tokio::task::LocalSet::new();
    local.run_until(pre_trade.run()).await
}
