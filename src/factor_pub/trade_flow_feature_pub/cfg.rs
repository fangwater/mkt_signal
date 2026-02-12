//! Trade flow feature 发布配置

use anyhow::Result;
use serde::Deserialize;
use std::fs;

use crate::common::redis_client::RedisSettings;

#[derive(Debug, Clone, Deserialize)]
pub struct TradeFlowFeaturePubConfig {
    #[serde(default)]
    pub data_source: DataSourceConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub redis: RedisSettings,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataSourceConfig {
    #[serde(default = "default_trade_channel")]
    pub trade_channel: String,
}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self {
            trade_channel: default_trade_channel(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_bar_ms")]
    pub bar_ms: i64,
    #[serde(default = "default_quantile_reload_secs")]
    pub quantile_reload_secs: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            bar_ms: default_bar_ms(),
            quantile_reload_secs: default_quantile_reload_secs(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfigFile {
    Wrapped {
        trade_flow_feature_pub: TradeFlowFeaturePubConfig,
    },
    Direct(TradeFlowFeaturePubConfig),
}

impl TradeFlowFeaturePubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)?;
        let cfg = match config_file {
            ConfigFile::Wrapped {
                trade_flow_feature_pub,
            } => trade_flow_feature_pub,
            ConfigFile::Direct(cfg) => cfg,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.data_source.trade_channel.trim().is_empty() {
            anyhow::bail!("data_source.trade_channel must not be empty");
        }
        if self.runtime.bar_ms <= 0 {
            anyhow::bail!("runtime.bar_ms must be > 0");
        }
        if self.runtime.quantile_reload_secs == 0 {
            anyhow::bail!("runtime.quantile_reload_secs must be > 0");
        }
        Ok(())
    }
}

fn default_trade_channel() -> String {
    "trade".to_string()
}

fn default_bar_ms() -> i64 {
    5_000
}

fn default_quantile_reload_secs() -> u64 {
    180
}
