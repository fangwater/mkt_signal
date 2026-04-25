//! Fusion Factor 发布配置

use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct FusionFactorPubConfig {
    pub tlen_server: TlenServerConfig,
    #[serde(default)]
    pub bootstrap: BootstrapConfig,
    #[serde(default)]
    pub rl_factor: RlFactorConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TlenServerConfig {
    pub base_url: String,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_symbol_reload_secs")]
    pub symbol_reload_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RlFactorConfig {
    #[serde(default = "default_rl_pct_change_period")]
    pub pct_change_period: usize,
    #[serde(default = "default_rl_rolling_window")]
    pub rolling_window: usize,
    #[serde(default = "default_rl_scale_factor")]
    pub scale_factor: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BootstrapConfig {
    #[serde(default = "default_bootstrap_enabled")]
    pub enabled: bool,
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

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            enabled: default_bootstrap_enabled(),
        }
    }
}

impl FusionFactorPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let cfg: FusionFactorPubConfig = toml::from_str(&content)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        self.tlen_server.validate()?;
        self.rl_factor.validate()?;
        Ok(())
    }

    /// 固定的 trade flow channel 名称
    pub fn trade_flow_channel(&self) -> &str {
        "trade_flow_feature"
    }

    /// 固定 output service path: fusion_factor/{venue_slug}
    pub fn output_service_path(&self, venue_slug: &str) -> String {
        format!("fusion_factor/{}", venue_slug)
    }
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

fn default_rl_pct_change_period() -> usize {
    12
}

fn default_rl_rolling_window() -> usize {
    30
}

fn default_rl_scale_factor() -> f64 {
    1.0
}

fn default_bootstrap_enabled() -> bool {
    true
}

fn default_request_timeout_ms() -> u64 {
    5_000
}

fn default_symbol_reload_secs() -> u64 {
    180
}

impl TlenServerConfig {
    fn validate(&self) -> Result<()> {
        if self.base_url.trim().is_empty() {
            anyhow::bail!("tlen_server.base_url must not be empty");
        }
        if self.request_timeout_ms == 0 {
            anyhow::bail!("tlen_server.request_timeout_ms must be > 0");
        }
        if self.symbol_reload_secs == 0 {
            anyhow::bail!("tlen_server.symbol_reload_secs must be > 0");
        }
        Ok(())
    }
}
