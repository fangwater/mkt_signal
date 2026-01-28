use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
pub struct IceoryxCfg {
    pub history_size: Option<usize>,
    pub max_subscribers: Option<usize>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct PairMmResampleConfig {
    pub resample_interval_ms: u64,
    pub symbol: String,
    pub iceoryx: Option<IceoryxCfg>,
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
        if self.symbol.trim().is_empty() {
            anyhow::bail!("symbol must not be empty");
        }
        Ok(())
    }

    pub fn history_size(&self) -> usize {
        self.iceoryx
            .as_ref()
            .and_then(|c| c.history_size)
            .unwrap_or(100)
    }

    pub fn max_subscribers(&self) -> usize {
        self.iceoryx
            .as_ref()
            .and_then(|c| c.max_subscribers)
            .unwrap_or(10)
    }
}
