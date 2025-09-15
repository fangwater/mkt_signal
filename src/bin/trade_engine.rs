use anyhow::Result;
use log::info;
use mkt_signal::trade_engine::{config::{TradeEngineCfg, ApiKey}, TradeEngine};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    // 默认读取 config/trade_engine.toml
    let cfg_path = std::env::var("TRADE_ENGINE_CFG").unwrap_or_else(|_| "config/trade_engine.toml".to_string());
    let mut cfg = TradeEngineCfg::load(cfg_path).await?;

    // 可选：用环境变量覆盖账户（适合临时实盘测试）
    if let (Ok(key), Ok(secret)) = (std::env::var("BINANCE_API_KEY"), std::env::var("BINANCE_API_SECRET")) {
        cfg.accounts.keys = vec![ApiKey{ name: std::env::var("BINANCE_API_NAME").unwrap_or_else(|_| "env".into()), key, secret }];
        info!("trade_engine accounts overridden by env");
    }

    info!("trade_engine config loaded");
    let engine = TradeEngine::new(cfg);
    let local = tokio::task::LocalSet::new();
    local.run_until(engine.run()).await
}
