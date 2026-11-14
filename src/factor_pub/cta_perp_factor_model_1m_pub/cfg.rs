use crate::cta_perp::interval::BarInterval;
use crate::factor_pub::cta_factor_universe::CtaFactorUniverse;
use crate::factor_pub::fusion_factor_pub::cfg::TlenServerConfig;
use anyhow::{bail, Context, Result};
use period_pbs::kafka::KafkaConsumerConfig;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CtaPerpFactorPubConfig {
    #[serde(default)]
    pub bar_interval: BarInterval,
    pub universe: CtaFactorUniverse,
    pub tlen_server: TlenServerConfig,
    pub normalize: NormalizeConfig,
    pub kafka: KafkaInputConfig,
    /// Completed native baseline bars preceding the retained Kafka stream.
    #[serde(default)]
    pub baseline_seed_path: Option<std::path::PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cta_perp::factors::{parse_factor_table, table_services_for_interval};

    #[test]
    fn rx01_operator_list_has_complete_fifteen_minute_model_coverage() {
        let mut config: CtaPerpFactorPubConfig = toml::from_str(include_str!(
            "../../../config/cta_perp_rx01/cta_perp_factor_model_15m_pub.toml"
        ))
        .unwrap();
        assert_eq!(config.bar_interval, BarInterval::FifteenMinutes);
        config.validate().unwrap();
        assert_eq!(config.universe.symbols.len(), 38);
        assert_eq!(config.universe.factors.len(), 89);
        let table = parse_factor_table(include_str!(
            "../../../config/cta_perp_rx01/cta_perp_symbol_factors.json"
        ))
        .unwrap();
        for (symbol, row) in table {
            assert!(config.universe.symbols.contains(&symbol));
            for factor in row.factors {
                assert!(config.universe.factors.contains(&factor));
            }
        }
        config.universe.symbols.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn fifteen_minute_template_keeps_day_windows_and_validates_every_binding() {
        let mut config: CtaPerpFactorPubConfig = toml::from_str(include_str!(
            "../../../config/cta_perp_15m/cta_perp_factor_model_15m_pub.toml"
        ))
        .unwrap();
        config.validate().unwrap();
        assert_eq!(config.bar_interval, BarInterval::FifteenMinutes);
        assert_eq!(config.normalize.window_bars, 96);
        assert_eq!(config.normalize.min_periods, 67);
        assert_eq!(config.bar_interval.factor_quantile_window(), 192);
        assert_eq!(config.bar_interval.factor_min_samples(), 96);
        let table = parse_factor_table(include_str!(
            "../../../config/cta_perp_15m/cta_perp_symbol_factors.json"
        ))
        .unwrap();
        for (symbol, row) in table {
            assert!(config.universe.symbols.contains(&symbol));
            for factor in row.factors {
                assert!(config.universe.factors.contains(&factor));
            }
        }
        config.kafka.lookback_secs = 172799;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rx04_universe_accepts_all_requested_bindings_and_requires_5m_warmup() {
        let mut config: CtaPerpFactorPubConfig = toml::from_str(include_str!(
            "../../../config/cta_perp_rx04/cta_perp_factor_model_5m_pub.toml"
        ))
        .unwrap();
        config.validate().unwrap();
        assert_eq!(config.bar_interval, BarInterval::FiveMinutes);
        assert_eq!(config.universe.factors.len(), 35);
        let table = parse_factor_table(include_str!(
            "../../../config/cta_perp_rx04/cta_perp_symbol_factors.json"
        ))
        .unwrap();
        assert_eq!(table.len(), 15);
        assert_eq!(
            table.values().map(|row| row.factors.len()).sum::<usize>(),
            48
        );
        assert_eq!(
            table_services_for_interval(&table, BarInterval::FiveMinutes).len(),
            35
        );
        for (symbol, row) in table {
            assert!(config.universe.symbols.contains(&symbol));
            for factor in row.factors {
                assert!(config.universe.factors.contains(&factor));
            }
            assert!(row.weights.iter().all(|w| w.abs() == 1.0));
        }
        assert_eq!(config.normalize.window_bars, 288);
        assert_eq!(config.normalize.min_periods, 200);
        assert_eq!(config.bar_interval.factor_quantile_window(), 576);
        assert_eq!(config.bar_interval.factor_min_samples(), 288);
        config.kafka.lookback_secs = 172799; // Less than the two-day percentile window.
        assert!(config.validate().is_err());
    }
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

impl CtaPerpFactorPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let raw = fs::read_to_string(path).with_context(|| format!("read {path}"))?;
        let config: Self = toml::from_str(&raw).with_context(|| format!("parse {path}"))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        self.universe.validate()?;
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
        if self.kafka.lookback_secs
            < self.bar_interval.factor_quantile_window() as u64
                * (self.bar_interval.millis() as u64 / 1000)
        {
            bail!(
                "kafka.lookback_secs must cover at least {} {} slots",
                self.bar_interval.factor_quantile_window(),
                self.bar_interval.as_str()
            );
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
