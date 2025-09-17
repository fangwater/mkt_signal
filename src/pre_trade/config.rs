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
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SignalSubscriptionsCfg {
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default = "default_signal_payload")]
    pub max_payload_bytes: usize,
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
