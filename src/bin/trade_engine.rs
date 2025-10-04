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
    }

    // 可选：用环境变量覆盖 iceoryx 服务名，便于测试时快速切换隔离
    if let Ok(req_svc) = std::env::var("ORDER_REQ_SERVICE") {
        cfg.order_req_service = req_svc;
    }
    if let Ok(resp_svc) = std::env::var("ORDER_RESP_SERVICE") {
        cfg.order_resp_service = resp_svc;
    }

    for api in &cfg.accounts.keys {
        let label_prefix = format!("trade_engine account '{}'", api.name);
        log_credential_preview(&format!("{} BINANCE_API_KEY", label_prefix), &api.key);
        log_credential_preview(&format!("{} BINANCE_API_SECRET", label_prefix), &api.secret);
    }

    info!("trade_engine config loaded");
    let engine = TradeEngine::new(cfg);
    let local = tokio::task::LocalSet::new();
    local.run_until(engine.run()).await
}
