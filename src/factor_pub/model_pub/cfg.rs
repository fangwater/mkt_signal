use anyhow::Result;
use serde::Deserialize;
use std::fs;

use super::score_rolling::ScoreRollingConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelPubVersion {
    Default,
    OneMinute,
}

impl ModelPubVersion {
    pub fn label(self) -> &'static str {
        match self {
            Self::Default => "5s",
            Self::OneMinute => "1m",
        }
    }

    pub fn factor_plan_config_type(self) -> &'static str {
        match self {
            Self::Default => "factor_plan",
            Self::OneMinute => "factor_plan_1m",
        }
    }

    pub fn node_prefix(self) -> &'static str {
        match self {
            Self::Default => "model_pub",
            Self::OneMinute => "model_1m_pub",
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ModelPubConfig {
    pub model_manager_base_url: String,
    #[serde(default = "default_model_manager_request_timeout_ms")]
    pub model_manager_request_timeout_ms: u64,
    pub tlen_server_base_url: String,
    #[serde(default = "default_tlen_server_request_timeout_ms")]
    pub tlen_server_request_timeout_ms: u64,
    pub input_service: String,
    pub output_service: String,
    #[serde(default)]
    pub score_rolling: ScoreRollingConfig,
}

impl ModelPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.model_manager_base_url.trim().is_empty() {
            anyhow::bail!("model_manager_base_url must not be empty");
        }
        if self.model_manager_request_timeout_ms == 0 {
            anyhow::bail!("model_manager_request_timeout_ms must be > 0");
        }
        if self.tlen_server_base_url.trim().is_empty() {
            anyhow::bail!("tlen_server_base_url must not be empty");
        }
        if self.tlen_server_request_timeout_ms == 0 {
            anyhow::bail!("tlen_server_request_timeout_ms must be > 0");
        }
        if self.input_service.trim().is_empty() {
            anyhow::bail!("input_service must not be empty");
        }
        if self.output_service.trim().is_empty() {
            anyhow::bail!("output_service must not be empty");
        }
        self.score_rolling.validate()?;
        Ok(())
    }
}

fn default_model_manager_request_timeout_ms() -> u64 {
    120_000
}

fn default_tlen_server_request_timeout_ms() -> u64 {
    5_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_minute_version_uses_independent_factor_plan() {
        assert_eq!(ModelPubVersion::OneMinute.label(), "1m");
        assert_eq!(
            ModelPubVersion::OneMinute.factor_plan_config_type(),
            "factor_plan_1m"
        );
        assert_eq!(ModelPubVersion::OneMinute.node_prefix(), "model_1m_pub");
    }
}
