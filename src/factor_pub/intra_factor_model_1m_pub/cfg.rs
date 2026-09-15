use anyhow::Result;
use serde::Deserialize;
use std::fs;

use crate::factor_pub::fusion_factor_pub::cfg::TlenServerConfig;
use period_pbs::kafka::KafkaConsumerConfig;

pub const DEFAULT_WINDOW_SIZE: usize = 2_880;
pub const DEFAULT_MIN_SAMPLES: usize = 1_440;

#[derive(Debug, Clone, Deserialize)]
pub struct IntraFactorModelPubConfig {
    pub tlen_server: TlenServerConfig,
    #[serde(default)]
    pub percentile: PercentileConfig,
    #[serde(default)]
    pub warmup: KafkaWarmupConfig,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct KafkaWarmupConfig {
    /// A failed Kafka replay keeps the publisher available but leaves score_ready false
    /// until the live feed has accumulated enough valid bars.
    pub enabled: bool,
    pub required: bool,
    pub lookback_secs: u64,
    pub tail_guard_secs: u64,
    pub max_wait_secs: u64,
    pub kafka: KafkaConsumerConfig,
}

impl Default for KafkaWarmupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            required: false,
            lookback_secs: 2 * 24 * 60 * 60,
            tail_guard_secs: 90,
            max_wait_secs: 300,
            kafka: KafkaConsumerConfig::default(),
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
        if self.warmup.enabled {
            if self.warmup.lookback_secs == 0 {
                anyhow::bail!("warmup.lookback_secs must be > 0 when warmup is enabled");
            }
            if self.warmup.max_wait_secs == 0 {
                anyhow::bail!("warmup.max_wait_secs must be > 0 when warmup is enabled");
            }
            self.warmup.kafka.validate()?;
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
        assert!(!config.warmup.enabled);
    }

    #[test]
    fn parses_enabled_kafka_warmup() {
        let config: IntraFactorModelPubConfig = toml::from_str(
            r#"
                [tlen_server]
                base_url = "http://tlen.example"

                [warmup]
                enabled = true
                lookback_secs = 60
                max_wait_secs = 10

                [warmup.kafka]
                topics = ["binance-futures"]
            "#,
        )
        .expect("parse config");

        config.validate().expect("validate config");
        assert!(config.warmup.enabled);
        assert_eq!(config.warmup.kafka.topics, ["binance-futures"]);
    }
}
