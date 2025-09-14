use anyhow::Result;
use log::info;
use mkt_signal::trade_engine::{config::TradeEngineCfg, TradeEngine};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    // 默认读取 config/trade_engine.toml
    let cfg_path = std::env::var("TRADE_ENGINE_CFG").unwrap_or_else(|_| "config/trade_engine.toml".to_string());
    let cfg = TradeEngineCfg::load(cfg_path).await?;

    info!("trade_engine config loaded");
    let engine = TradeEngine::new(cfg);
    engine.run().await
}

