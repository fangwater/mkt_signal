use anyhow::Result;
use log::info;
use mkt_signal::{ApiKey, TradeEngine, TradeEngineCfg};

fn credential_edges(value: &str) -> (String, String, usize) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return (String::new(), String::new(), 0);
    }
    let chars: Vec<char> = trimmed.chars().collect();
    let len = chars.len();
    let prefix_len = len.min(4);
    let suffix_len = len.min(4);
    let first: String = chars.iter().take(prefix_len).collect();
    let last: String = chars.iter().skip(len.saturating_sub(suffix_len)).collect();
    (first, last, len)
}

fn log_credential_preview(label: &str, value: &str) {
    let (first4, last4, len) = credential_edges(value);
    if len == 0 {
        info!("{} not set or empty", label);
    } else {
        info!(
            "{} preview len={} first4='{}' last4='{}'",
            label, len, first4, last4
        );
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    info!("trade_engine starting");
    info!("Required env vars: BINANCE_API_KEY, BINANCE_API_SECRET");
    info!("Optional env vars: TRADE_ENGINE_CFG, BINANCE_API_NAME");

    // 默认读取 config/trade_engine.toml
    let cfg_path = std::env::var("TRADE_ENGINE_CFG")
        .unwrap_or_else(|_| "config/trade_engine.toml".to_string());
    let mut cfg = TradeEngineCfg::load(cfg_path).await?;

    // 从环境变量读取账户配置（必须）
    let api_key_raw = std::env::var("BINANCE_API_KEY").map_err(|_| {
        anyhow::anyhow!("BINANCE_API_KEY not set. Export it before running trade_engine")
    })?;
    let api_key = api_key_raw.trim().to_string();

    let api_secret_raw = std::env::var("BINANCE_API_SECRET").map_err(|_| {
        anyhow::anyhow!("BINANCE_API_SECRET not set. Export it before running trade_engine")
    })?;
    let api_secret = api_secret_raw.trim().to_string();

    let api_name = std::env::var("BINANCE_API_NAME").unwrap_or_else(|_| "default".to_string());

    // 设置账户配置
    cfg.accounts.keys = vec![ApiKey {
        name: api_name.clone(),
        key: api_key.clone(),
        secret: api_secret.clone(),
    }];

    info!("trade_engine account name: {}", api_name);
    log_credential_preview("BINANCE_API_KEY", &api_key);
    log_credential_preview("BINANCE_API_SECRET", &api_secret);

    info!("trade_engine config loaded");
    let engine = TradeEngine::new(cfg);
    let local = tokio::task::LocalSet::new();
    local.run_until(engine.run()).await
}
