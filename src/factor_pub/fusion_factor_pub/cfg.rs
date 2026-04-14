//! Fusion Factor 发布配置

use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct FusionFactorPubConfig {
    pub tlen_server: TlenServerConfig,
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

impl Default for RlFactorConfig {
    fn default() -> Self {
        Self {
            pct_change_period: default_rl_pct_change_period(),
            rolling_window: default_rl_rolling_window(),
            scale_factor: default_rl_scale_factor(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn write_temp_config(body: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "fusion_factor_pub_cfg_test_{}_{}.toml",
            std::process::id(),
            suffix
        ));
        fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn output_service_path_follows_venue_slug() {
        let path = write_temp_config(
            r#"
[tlen_server]
base_url = "http://127.0.0.1:6322"
"#,
        );

        let cfg = FusionFactorPubConfig::load(path.to_str().unwrap()).unwrap();
        assert_eq!(
            cfg.output_service_path("binance-futures"),
            "fusion_factor/binance-futures"
        );

        let _ = fs::remove_file(path);
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
