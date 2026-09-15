use anyhow::Result;
use serde::Deserialize;
use std::fs;

use crate::factor_pub::fusion_factor_pub::cfg::TlenServerConfig;

pub const DEFAULT_WINDOW_SIZE: usize = 2_880;
pub const DEFAULT_MIN_SAMPLES: usize = 1_440;

#[derive(Debug, Clone, Deserialize)]
pub struct IntraFactorModelPubConfig {
    pub tlen_server: TlenServerConfig,
    #[serde(default)]
    pub percentile: PercentileConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PercentileConfig {
    #[serde(default = "default_window_size")]
    pub window_size: usize,
    #[serde(default = "default_min_samples")]
    pub min_samples: usize,
}

impl Default for PercentileConfig {
    fn default() -> Self {
        Self {
            window_size: default_window_size(),
            min_samples: default_min_samples(),
        }
    }
}

impl IntraFactorModelPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.tlen_server.base_url.trim().is_empty() {
            anyhow::bail!("tlen_server.base_url must not be empty");
        }
        if self.tlen_server.request_timeout_ms == 0 {
            anyhow::bail!("tlen_server.request_timeout_ms must be > 0");
        }
        if self.tlen_server.symbol_reload_secs == 0 {
            anyhow::bail!("tlen_server.symbol_reload_secs must be > 0");
        }
        if self.percentile.window_size == 0 {
            anyhow::bail!("percentile.window_size must be > 0");
        }
        if self.percentile.min_samples == 0 {
            anyhow::bail!("percentile.min_samples must be > 0");
        }
        if self.percentile.min_samples > self.percentile.window_size {
            anyhow::bail!(
                "percentile.min_samples ({}) must be <= percentile.window_size ({})",
                self.percentile.min_samples,
                self.percentile.window_size
            );
        }
        Ok(())
    }
}

fn default_window_size() -> usize {
    DEFAULT_WINDOW_SIZE
}

fn default_min_samples() -> usize {
    DEFAULT_MIN_SAMPLES
}

#[cfg(test)]
mod tests {
    use super::{IntraFactorModelPubConfig, DEFAULT_MIN_SAMPLES, DEFAULT_WINDOW_SIZE};

    #[test]
    fn defaults_to_notebook_percentile_window() {
        let config: IntraFactorModelPubConfig = toml::from_str(
            r#"
                [tlen_server]
                base_url = "http://127.0.0.1:6322"
            "#,
        )
        .expect("parse config");

        assert_eq!(config.percentile.window_size, DEFAULT_WINDOW_SIZE);
        assert_eq!(config.percentile.min_samples, DEFAULT_MIN_SAMPLES);
    }
}
