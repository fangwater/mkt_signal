use anyhow::Result;
use serde::Deserialize;
use std::fs;

use super::score_rolling::ScoreRollingConfig;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ModelPubConfig {
    pub model_manager_base_url: String,
    #[serde(default = "default_model_manager_request_timeout_ms")]
    pub model_manager_request_timeout_ms: u64,
    #[serde(default = "default_input_service")]
    pub input_service: String,
    #[serde(default = "default_output_service")]
    pub output_service: String,
    #[serde(default = "default_window_size")]
    pub window_size: usize,
    #[serde(default = "default_min_samples")]
    pub min_samples: u64,
    #[serde(default = "default_zscore_cap")]
    pub zscore_cap: f64,
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
        if self.input_service.trim().is_empty() {
            anyhow::bail!("input_service must not be empty");
        }
        if self.output_service.trim().is_empty() {
            anyhow::bail!("output_service must not be empty");
        }
        if !self.input_service.contains("{model_name}") {
            anyhow::bail!("input_service must contain {{model_name}}");
        }
        if !self.output_service.contains("{model_name}") {
            anyhow::bail!("output_service must contain {{model_name}}");
        }
        if self.window_size == 0 {
            anyhow::bail!("window_size must be > 0");
        }
        if self.min_samples == 0 {
            anyhow::bail!("min_samples must be > 0");
        }
        self.score_rolling.validate()?;
        Ok(())
    }

    pub fn render_input_service(&self, model_name: &str) -> Result<String> {
        render_service(&self.input_service, model_name)
    }

    pub fn render_output_service(&self, model_name: &str) -> Result<String> {
        render_service(&self.output_service, model_name)
    }
}

fn render_service(template: &str, model_name: &str) -> Result<String> {
    let trimmed = model_name.trim();
    if trimmed.is_empty() {
        anyhow::bail!("model_name must not be empty");
    }
    let rendered = template.replace("{model_name}", trimmed);
    if rendered.trim().is_empty() {
        anyhow::bail!("service name renders to empty value");
    }
    Ok(rendered)
}

fn default_model_manager_request_timeout_ms() -> u64 {
    120_000
}

fn default_input_service() -> String {
    "fusion_factor/{model_name}".to_string()
}

fn default_output_service() -> String {
    "model_output/{model_name}".to_string()
}

fn default_window_size() -> usize {
    17280
}

fn default_min_samples() -> u64 {
    100
}

fn default_zscore_cap() -> f64 {
    3.0
}
