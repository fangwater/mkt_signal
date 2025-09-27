use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

/// Configuration for the pre-trade service.
#[derive(Debug, Clone, Deserialize)]
pub struct PreTradeCfg {
    pub account_stream: AccountStreamCfg,
    pub trade_engine: TradeEngineRespCfg,
    #[serde(default)]
    pub signals: SignalSubscriptionsCfg,
    #[serde(default)]
    pub risk_checks: RiskCheckCfg,
    #[serde(default)]
    pub params: Option<StrategyParamsCfg>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountStreamCfg {
    pub service: String,
    #[serde(default = "default_account_payload")]
    pub max_payload_bytes: usize,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradeEngineRespCfg {
    pub service: String,
    #[serde(default = "default_trade_resp_payload")]
    pub max_payload_bytes: usize,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub req_service: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SignalSubscriptionsCfg {
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default = "default_signal_payload")]
    pub max_payload_bytes: usize,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StrategyParamsCfg {
    #[serde(default, rename = "max_pos_u")]
    pub max_pos_u: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct RiskCheckCfg {
    #[serde(default)]
    pub binance_pm_um: Option<BinanceUmAccountCfg>,
    #[serde(default)]
    pub binance_spot: Option<BinanceSpotAccountCfg>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceUmAccountCfg {
    #[serde(default = "default_binance_pm_rest_base")]
    pub rest_base: String,
    #[serde(default = "default_binance_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_binance_api_secret_env")]
    pub api_secret_env: String,
    #[serde(default = "default_recv_window_ms")]
    pub recv_window_ms: u64,
}

impl Default for BinanceUmAccountCfg {
    fn default() -> Self {
        Self {
            rest_base: default_binance_pm_rest_base(),
            api_key_env: default_binance_api_key_env(),
            api_secret_env: default_binance_api_secret_env(),
            recv_window_ms: default_recv_window_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceSpotAccountCfg {
    #[serde(default = "default_binance_pm_rest_base")]
    pub rest_base: String,
    #[serde(default = "default_binance_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_binance_api_secret_env")]
    pub api_secret_env: String,
    #[serde(default = "default_recv_window_ms")]
    pub recv_window_ms: u64,
    #[serde(default)]
    pub asset: Option<String>,
}

impl Default for BinanceSpotAccountCfg {
    fn default() -> Self {
        Self {
            rest_base: default_binance_pm_rest_base(),
            api_key_env: default_binance_api_key_env(),
            api_secret_env: default_binance_api_secret_env(),
            recv_window_ms: default_recv_window_ms(),
            asset: None,
        }
    }
}

impl PreTradeCfg {
    pub async fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let data = tokio::fs::read_to_string(path).await?;
        let cfg: Self = toml::from_str(&data)?;
        Ok(cfg)
    }
}

const fn default_account_payload() -> usize {
    16_384
}
const fn default_trade_resp_payload() -> usize {
    16_384
}
const fn default_signal_payload() -> usize {
    1_024
}

fn default_binance_pm_rest_base() -> String {
    "https://papi.binance.com".to_string()
}

fn default_binance_api_key_env() -> String {
    "BINANCE_API_KEY".to_string()
}

fn default_binance_api_secret_env() -> String {
    "BINANCE_API_SECRET".to_string()
}

const fn default_recv_window_ms() -> u64 {
    5_000
}
