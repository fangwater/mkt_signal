use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

/// Configuration for the pre-trade service.
#[derive(Debug, Clone, Deserialize)]
pub struct PreTradeCfg {
    pub account_stream: AccountStreamCfg,
    pub trade_engine: TradeEngineRespCfg,
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

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyParamsCfg {
    #[serde(default, rename = "max_pos_u")]
    pub max_pos_u: f64,
    #[serde(default = "default_symbol_exposure_ratio")]
    pub max_symbol_exposure_ratio: f64,
    #[serde(default = "default_total_exposure_ratio")]
    pub max_total_exposure_ratio: f64,
    #[serde(default = "default_max_leverage")]
    pub max_leverage: f64,
}

impl Default for StrategyParamsCfg {
    fn default() -> Self {
        Self {
            max_pos_u: 0.0,
            max_symbol_exposure_ratio: default_symbol_exposure_ratio(),
            max_total_exposure_ratio: default_total_exposure_ratio(),
            max_leverage: default_max_leverage(),
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
