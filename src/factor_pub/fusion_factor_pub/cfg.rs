//! Fusion Factor 发布配置

use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct FusionFactorPubConfig {
    #[serde(default)]
    pub data_source: DataSourceConfig,
    pub symbols: Vec<String>,
    pub model_manager: ModelManagerConfig,
    #[serde(default)]
    pub output: OutputConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataSourceConfig {
    #[serde(default = "default_trade_flow_channel")]
    pub trade_flow_channel: String,
}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self {
            trade_flow_channel: default_trade_flow_channel(),
        }
    }
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

#[derive(Debug, Clone, Deserialize)]
pub struct OutputConfig {
    #[serde(default = "default_output_service_path")]
    pub service_path: String,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            service_path: default_output_service_path(),
        }
    }
}

fn default_output_service_path() -> String {
    "fusion_factor/default/default".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfigFile {
    Wrapped {
        fusion_factor_pub: FusionFactorPubConfig,
    },
    Direct(FusionFactorPubConfig),
}

impl FusionFactorPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)?;
        let cfg = match config_file {
            ConfigFile::Wrapped { fusion_factor_pub } => fusion_factor_pub,
            ConfigFile::Direct(cfg) => cfg,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.data_source.trade_flow_channel.trim().is_empty() {
            anyhow::bail!("data_source.trade_flow_channel must not be empty");
        }
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
}

fn default_trade_flow_channel() -> String {
    "trade_flow_feature".to_string()
}

fn default_request_timeout_ms() -> u64 {
    5_000
}
