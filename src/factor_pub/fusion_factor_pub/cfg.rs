//! Fusion Factor 发布配置

use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct FusionFactorPubConfig {
    pub symbols: Vec<String>,
    pub model_manager: ModelManagerConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelManagerConfig {
    pub base_url: String,
    pub model_name: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub bearer_token: Option<String>,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

impl FusionFactorPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let cfg: FusionFactorPubConfig = toml::from_str(&content)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.symbols.is_empty() {
            anyhow::bail!("symbols must not be empty");
        }
        if self.symbols.iter().any(|s| s.trim().is_empty()) {
            anyhow::bail!("symbols must not contain empty item");
        }

        if self.model_manager.base_url.trim().is_empty() {
            anyhow::bail!("model_manager.base_url must not be empty");
        }
        if self.model_manager.model_name.trim().is_empty() {
            anyhow::bail!("model_manager.model_name must not be empty");
        }
        if self.model_manager.request_timeout_ms == 0 {
            anyhow::bail!("model_manager.request_timeout_ms must be > 0");
        }
        Ok(())
    }

    /// 固定的 trade flow channel 名称
    pub fn trade_flow_channel(&self) -> &str {
        "trade_flow_feature"
    }

    /// 自动生成 output service path: fusion_factor/{model_name}
    pub fn output_service_path(&self) -> String {
        format!("fusion_factor/{}", self.model_manager.model_name)
    }
}

fn default_request_timeout_ms() -> u64 {
    5_000
}
