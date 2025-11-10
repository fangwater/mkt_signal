use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

/// Configuration for the pre-trade service.
#[derive(Debug, Clone, Deserialize)]
pub struct PreTradeCfg {
    pub trade_engine: TradeEngineRespCfg,
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

impl PreTradeCfg {
    pub async fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let data = tokio::fs::read_to_string(path).await?;
        let cfg: Self = toml::from_str(&data)?;
        Ok(cfg)
    }
}

fn default_trade_resp_payload() -> usize {
    16384
}
