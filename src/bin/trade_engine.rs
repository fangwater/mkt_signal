use anyhow::Result;
use log::{debug, info};
use mkt_signal::{ApiKey, TradeEngine, TradeEngineCfg};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    // 默认读取 config/trade_engine.toml
    let cfg_path = std::env::var("TRADE_ENGINE_CFG")
        .unwrap_or_else(|_| "config/trade_engine.toml".to_string());
    let mut cfg = TradeEngineCfg::load(cfg_path).await?;

    // 可选：用环境变量覆盖账户（适合临时实盘测试）
    if let (Ok(key_raw), Ok(secret_raw)) = (
        std::env::var("BINANCE_API_KEY"),
        std::env::var("BINANCE_API_SECRET"),
    ) {
        let key = key_raw.trim().to_string();
        let secret = secret_raw.trim().to_string();
        cfg.accounts.keys = vec![ApiKey {
            name: std::env::var("BINANCE_API_NAME").unwrap_or_else(|_| "env".into()),
            key: key.clone(),
            secret: secret.clone(),
        }];
        info!("trade_engine accounts overridden by env");
        debug!(
            "env api key length={}, secret length={}",
            key.len(),
            secret.len()
        );
    }

    // 可选：用环境变量覆盖 iceoryx 服务名，便于测试时快速切换隔离
    if let Ok(req_svc) = std::env::var("ORDER_REQ_SERVICE") {
        cfg.order_req_service = req_svc;
    }
    if let Ok(resp_svc) = std::env::var("ORDER_RESP_SERVICE") {
        cfg.order_resp_service = resp_svc;
    }

    info!("trade_engine config loaded");
    let engine = TradeEngine::new(cfg);
    let local = tokio::task::LocalSet::new();
    local.run_until(engine.run()).await
}
