//! RL Return Volatility 配置模块

use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct RlReturnVolatilityConfig {
    pub pct_change_period: usize,
    pub rolling_window: usize,
    pub clip_min: f64,
    pub clip_max: f64,
    pub max_keep_count: usize,
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    rl_return_volatility: RlReturnVolatilityConfig,
}

impl RlReturnVolatilityConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)?;
        Ok(config_file.rl_return_volatility)
    }

    pub fn min_required_len(&self) -> usize {
        self.pct_change_period + self.rolling_window
    }

    pub fn validate(&self) -> Result<()> {
        if self.pct_change_period == 0 {
            anyhow::bail!("pct_change_period must be > 0");
        }
        if self.rolling_window == 0 {
            anyhow::bail!("rolling_window must be > 0");
        }
        if !self.clip_min.is_finite() || !self.clip_max.is_finite() {
            anyhow::bail!("clip_min/clip_max must be finite");
        }
        if self.clip_min <= 0.0 || self.clip_max <= 0.0 {
            anyhow::bail!("clip_min/clip_max must be > 0");
        }
        if self.clip_min >= self.clip_max {
            anyhow::bail!("clip_min must be < clip_max");
        }
        let required = self.min_required_len();
        if self.max_keep_count <= required {
            anyhow::bail!(
                "max_keep_count must be > pct_change_period + rolling_window ({} <= {})",
                self.max_keep_count,
                required
            );
        }
        Ok(())
    }
}
