use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ModelPubConfig {
    pub model_name: String,
    pub model_path: String,
    pub n_features: usize,
    pub input_service: String,
    pub output_service: String,
}

impl ModelPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.model_name.trim().is_empty() {
            anyhow::bail!("model_name must not be empty");
        }
        if self.model_path.trim().is_empty() {
            anyhow::bail!("model_path must not be empty");
        }
        if self.n_features == 0 {
            anyhow::bail!("n_features must be > 0");
        }
        if self.input_service.trim().is_empty() {
            anyhow::bail!("input_service must not be empty");
        }
        if self.output_service.trim().is_empty() {
            anyhow::bail!("output_service must not be empty");
        }
        Ok(())
    }
}
