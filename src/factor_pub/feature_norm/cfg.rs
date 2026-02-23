use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct FeatureNormConfigRoot {
    pub feature_norm: FeatureNormConfig,
}

#[derive(Debug, Deserialize)]
pub struct FeatureNormConfig {
    pub model_name: String,
    #[serde(default = "default_window_size")]
    pub window_size: usize,
}

fn default_window_size() -> usize {
    17280
}

impl FeatureNormConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content =
            std::fs::read_to_string(path).with_context(|| format!("read config: {}", path))?;
        let root: FeatureNormConfigRoot =
            serde_yaml::from_str(&content).with_context(|| format!("parse config: {}", path))?;
        Ok(root.feature_norm)
    }
}
