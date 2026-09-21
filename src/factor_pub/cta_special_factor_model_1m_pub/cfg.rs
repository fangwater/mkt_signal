use crate::factor_pub::fusion_factor_pub::cfg::TlenServerConfig;
use anyhow::{bail, Context, Result};
use period_pbs::kafka::KafkaConsumerConfig;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CtaSpecialFactorPubConfig {
    pub tlen_server: TlenServerConfig,
    pub normalize: NormalizeConfig,
    pub kafka: KafkaInputConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizeConfig {
    pub window_bars: usize,
    pub min_periods: usize,
    pub clip_zscore: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaInputConfig {
    pub lookback_secs: u64,
    pub catchup_timeout_secs: u64,
    pub topic: String,
    #[serde(flatten)]
    pub consumer: KafkaConsumerConfig,
}

impl CtaSpecialFactorPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let raw = fs::read_to_string(path).with_context(|| format!("read {path}"))?;
        let config: Self = toml::from_str(&raw).with_context(|| format!("parse {path}"))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.tlen_server.base_url.trim().is_empty() {
            bail!("tlen_server.base_url must not be empty");
        }
        if self.tlen_server.request_timeout_ms == 0 || self.tlen_server.symbol_reload_secs == 0 {
            bail!("tlen_server timeouts must be positive");
        }
        if self.normalize.window_bars < 2 {
            bail!("normalize.window_bars must be >= 2");
        }
        if self.normalize.min_periods == 0
            || self.normalize.min_periods > self.normalize.window_bars
        {
            bail!(
                "normalize.min_periods ({}) must be in 1..=normalize.window_bars ({})",
                self.normalize.min_periods,
                self.normalize.window_bars
            );
        }
        if !(self.normalize.clip_zscore.is_finite() && self.normalize.clip_zscore > 0.0) {
            bail!("normalize.clip_zscore must be a finite positive number");
        }
        if self.kafka.lookback_secs < 2 * 24 * 60 * 60 {
            bail!("kafka.lookback_secs must cover at least 2880 one-minute slots");
        }
        if self.kafka.catchup_timeout_secs == 0 || self.kafka.topic.trim().is_empty() {
            bail!("kafka catchup_timeout_secs and topic must be set");
        }
        self.kafka.consumer.validate()?;
        if self.kafka.consumer.topics != [self.kafka.topic.clone()] {
            bail!("kafka.topics must contain only the configured futures topic");
        }
        Ok(())
    }
}
