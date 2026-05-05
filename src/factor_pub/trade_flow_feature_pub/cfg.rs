//! Trade flow feature 发布配置

use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct TradeFlowFeaturePubConfig {
    #[serde(default)]
    pub data_source: DataSourceConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub rl_factor: RlFactorConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataSourceConfig {
    #[serde(default = "default_depth_channel")]
    pub depth_channel: String,
}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self {
            depth_channel: default_depth_channel(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_bar_ms")]
    pub bar_ms: i64,
    #[serde(default = "default_threshold_reload_secs")]
    pub threshold_reload_secs: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            bar_ms: default_bar_ms(),
            threshold_reload_secs: default_threshold_reload_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct RlFactorConfig {
    #[serde(default = "default_rl_pct_change_period")]
    pub pct_change_period: usize,
    #[serde(default = "default_rl_rolling_window")]
    pub rolling_window: usize,
    #[serde(default = "default_rl_scale_factor")]
    pub scale_factor: f64,
}

impl Default for RlFactorConfig {
    fn default() -> Self {
        Self {
            pct_change_period: default_rl_pct_change_period(),
            rolling_window: default_rl_rolling_window(),
            scale_factor: default_rl_scale_factor(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PersistenceConfig {
    #[serde(default = "default_persistence_rocksdb_path")]
    pub rocksdb_path: String,
    #[serde(default = "default_persistence_retention_hours")]
    pub retention_hours: u64,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            rocksdb_path: default_persistence_rocksdb_path(),
            retention_hours: default_persistence_retention_hours(),
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
        match self.data_source.depth_channel.as_str() {
            "depth25" | "depth50" => {}
            other => {
                anyhow::bail!(
                    "data_source.depth_channel must be one of depth25/depth50, got '{}'",
                    other
                );
            }
        }
        if self.runtime.bar_ms <= 0 {
            anyhow::bail!("runtime.bar_ms must be > 0");
        }
        if self.runtime.threshold_reload_secs == 0 {
            anyhow::bail!("runtime.threshold_reload_secs must be > 0");
        }
        self.rl_factor.validate()?;
        if self.persistence.rocksdb_path.trim().is_empty() {
            anyhow::bail!("persistence.rocksdb_path must not be empty");
        }
        Ok(())
    }
}

fn default_bar_ms() -> i64 {
    5_000
}

fn default_depth_channel() -> String {
    "depth25".to_string()
}

fn default_threshold_reload_secs() -> u64 {
    180
}

fn default_rl_pct_change_period() -> usize {
    12
}

fn default_rl_rolling_window() -> usize {
    30
}

fn default_rl_scale_factor() -> f64 {
    1.0
}

fn default_persistence_rocksdb_path() -> String {
    "data/trade_flow_feature_pub_rocksdb".to_string()
}

fn default_persistence_retention_hours() -> u64 {
    0
}

impl RlFactorConfig {
    fn validate(&self) -> Result<()> {
        if self.pct_change_period == 0 {
            anyhow::bail!("rl_factor.pct_change_period must be > 0");
        }
        if self.rolling_window == 0 {
            anyhow::bail!("rl_factor.rolling_window must be > 0");
        }
        if !self.scale_factor.is_finite() || self.scale_factor <= 0.0 {
            anyhow::bail!("rl_factor.scale_factor must be finite and > 0");
        }
        Ok(())
    }
}
