use anyhow::Result;
use clap::{Parser, ValueEnum};
use log::info;
use mkt_signal::{ApiKey, TradeEngine};
use std::net::IpAddr;

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

#[derive(Parser, Debug)]
#[command(name = "trade_engine", about = "Unified trade execution engine")]
struct Args {
    /// Target exchange (binance, okex, bybit, bitget, gate)
    #[arg(long, value_enum)]
    exchange: TradeEngineTarget,

    /// Local IP addresses for outbound connections (comma-separated)
    /// Default: "172.31.33.133,172.31.46.90"
    #[arg(long, value_delimiter = ',')]
    local_ips: Option<Vec<IpAddr>>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum TradeEngineTarget {
    Binance,
    Okex,
    Bybit,
    Bitget,
    Gate,
}

impl TradeEngineTarget {
    fn as_str(&self) -> &'static str {
        match self {
            TradeEngineTarget::Binance => "binance",
            TradeEngineTarget::Okex => "okex",
            TradeEngineTarget::Bybit => "bybit",
            TradeEngineTarget::Bitget => "bitget",
            TradeEngineTarget::Gate => "gate",
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    let args = Args::parse();
    let exchange_name = args.exchange.as_str();
    info!("trade_engine starting (exchange={})", exchange_name);

    // 使用命令行参数或默认 IP 列表
    let local_ips = args.local_ips.unwrap_or_else(|| {
        use mkt_signal::DEFAULT_LOCAL_IPS;
        let ips: Vec<IpAddr> = DEFAULT_LOCAL_IPS
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        info!(
            "using default local IPs: {}",
            ips.iter()
                .map(|ip| ip.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        ips
    });

    if !local_ips.is_empty() {
        info!(
            "configured local IPs: {}",
            local_ips
                .iter()
                .map(|ip| ip.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // OKEx 不需要从环境变量读取 API key，因为在 WebSocket 客户端中会自动处理
    let accounts = if exchange_name == "okex" {
        info!("OKEx mode: API credentials will be loaded from OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE environment variables");
        vec![] // OKEx 不需要在这里配置
    } else {
        // Binance 等其他交易所需要从环境变量读取
        let env_prefix = exchange_name.to_ascii_uppercase();
        let api_key_var = format!("{}_API_KEY", env_prefix);
        let api_secret_var = format!("{}_API_SECRET", env_prefix);
        let api_name_var = format!("{}_API_NAME", env_prefix);

        info!("Required env vars: {}, {}", api_key_var, api_secret_var);
        info!(
            "Optional env vars: TRADE_ENGINE_CFG, {} (default=\"default\")",
            api_name_var
        );

        // 从环境变量读取账户配置（必须）
        let api_key_raw = std::env::var(&api_key_var).map_err(|_| {
            anyhow::anyhow!(
                "{} not set. Export it before running trade_engine",
                api_key_var
            )
        })?;
        let api_key = api_key_raw.trim().to_string();

        let api_secret_raw = std::env::var(&api_secret_var).map_err(|_| {
            anyhow::anyhow!(
                "{} not set. Export it before running trade_engine",
                api_secret_var
            )
        })?;
        let api_secret = api_secret_raw.trim().to_string();

        let api_name = std::env::var(&api_name_var).unwrap_or_else(|_| "default".to_string());

        info!("trade_engine account name: {}", api_name);
        log_credential_preview(&api_key_var, &api_key);
        log_credential_preview(&api_secret_var, &api_secret);

        vec![ApiKey {
            name: api_name,
            key: api_key,
            secret: api_secret,
        }]
    };

    info!("trade_engine initialized");
    let engine = TradeEngine::new(local_ips, accounts);
    let local = tokio::task::LocalSet::new();
    local
        .run_until(engine.run(exchange_name.to_string()))
        .await
}
