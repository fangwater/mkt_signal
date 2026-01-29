use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct PairMmResampleConfig {
    pub resample_interval_ms: u64,
    pub online_symbols: Vec<String>,
    #[serde(default = "default_factor_names")]
    pub factor_names: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    pairmm_resample: PairMmResampleConfig,
}

impl PairMmResampleConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config_file: ConfigFile = serde_yaml::from_str(&content)?;
        Ok(config_file.pairmm_resample)
    }

    pub fn validate(&self) -> Result<()> {
        if self.resample_interval_ms == 0 {
            anyhow::bail!("resample_interval_ms must be > 0");
        }
        if self.online_symbols.is_empty() {
            anyhow::bail!("online_symbols must not be empty");
        }
        if self
            .online_symbols
            .iter()
            .any(|s| s.trim().is_empty())
        {
            anyhow::bail!("online_symbols contains empty symbol");
        }
        if self.factor_names.is_empty() {
            anyhow::bail!("factor_names must not be empty");
        }
        if self.factor_names.len() > 5 {
            anyhow::bail!("factor_names length exceeds 5");
        }
        let mut seen = std::collections::HashSet::new();
        for name in &self.factor_names {
            if name.trim().is_empty() {
                anyhow::bail!("factor_names contains empty name");
            }
            if !seen.insert(name.to_string()) {
                anyhow::bail!("factor_names contains duplicate: {}", name);
            }
            if factor_name_to_index(name).is_none() {
                anyhow::bail!("unknown factor name: {}", name);
            }
        }
        Ok(())
    }

    pub fn factor_indices(&self) -> Result<Vec<usize>> {
        let mut out = Vec::with_capacity(self.factor_names.len());
        for name in &self.factor_names {
            let Some(idx) = factor_name_to_index(name) else {
                anyhow::bail!("unknown factor name: {}", name);
            };
            out.push(idx);
        }
        Ok(out)
    }
}

fn default_factor_names() -> Vec<String> {
    vec![
        "history_threshold".to_string(),
        "model_threshold".to_string(),
        "cancel_threshold".to_string(),
        "factor1".to_string(),
        "factor2".to_string(),
    ]
}

fn factor_name_to_index(name: &str) -> Option<usize> {
    match name {
        "history_threshold" => Some(0),
        "model_threshold" => Some(1),
        "cancel_threshold" => Some(2),
        "factor1" => Some(3),
        "factor2" => Some(4),
        _ => None,
    }
}
