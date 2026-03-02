//! Fusion Factor 发布配置

use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct FusionFactorPubConfig {
    pub symbols: Vec<String>,
    pub model_manager: ModelManagerConfig,
    #[serde(default)]
    pub rl_factor: RlFactorConfig,
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
pub struct RlFactorConfig {
    #[serde(default = "default_rl_pct_change_period")]
    pub pct_change_period: usize,
    #[serde(default = "default_rl_rolling_window")]
    pub rolling_window: usize,
    #[serde(default = "default_rl_scale_factor")]
    pub scale_factor: f64,
    #[serde(default)]
    pub clip: Option<ClipRange>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClipRange {
    pub min: f64,
    pub max: f64,
}

impl Default for RlFactorConfig {
    fn default() -> Self {
        Self {
            pct_change_period: default_rl_pct_change_period(),
            rolling_window: default_rl_rolling_window(),
            scale_factor: default_rl_scale_factor(),
            clip: None,
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
        self.rl_factor.validate()?;
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
        if let Some(clip) = &self.clip {
            clip.validate("rl_factor.clip")?;
        }
        Ok(())
    }
}

impl ClipRange {
    fn validate(&self, name: &str) -> Result<()> {
        if !self.min.is_finite() || !self.max.is_finite() {
            anyhow::bail!("{} min/max must be finite", name);
        }
        if self.min >= self.max {
            anyhow::bail!("{} min must be < max", name);
        }
        Ok(())
    }
}

fn default_request_timeout_ms() -> u64 {
    5_000
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
