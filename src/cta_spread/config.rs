use anyhow::{bail, Context, Result};
use order_common::TradingVenue;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

pub const CTA_SPREAD_VENUE: TradingVenue = TradingVenue::BinanceFutures;
pub const CTA_SPREAD_VENUE_NAME: &str = "binance-futures";
pub const CTA_SPREAD_SIGNAL_POLL_INTERVAL_MS: u64 = 10;
pub const CTA_SPREAD_MAX_QUOTE_AGE_MS: u64 = 5_000;
pub const CTA_SPREAD_MAX_MODEL_AGE_MS: u64 = 120_000;
pub const CTA_SPREAD_STATUS_PATH: &str = "run/cta_spread_status.json";

fn default_max_pos_u() -> f64 {
    200.0
}

fn default_enabled() -> bool {
    false
}

fn default_factor_long_quantile() -> f64 {
    0.9
}

fn default_factor_short_quantile() -> f64 {
    0.1
}

fn default_spread_long_quantile() -> f64 {
    0.7
}
fn default_spread_short_quantile() -> f64 {
    0.3
}
fn default_nq_quantile() -> f64 {
    0.5
}

fn default_factor_exit_quantile_long() -> f64 {
    0.7
}

fn default_factor_exit_quantile_short() -> f64 {
    0.3
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CtaSpreadConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// USDT entry amount for each enabled symbol/factor pair.
    #[serde(default = "default_max_pos_u")]
    pub max_pos_u: f64,
    #[serde(default)]
    pub entry: EntryConfig,
    pub execution: ExecutionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct EntryConfig {
    pub factor_long_quantile: f64,
    pub factor_short_quantile: f64,
    pub spread_long_quantile: f64,
    pub spread_short_quantile: f64,
    pub nq_long_quantile: f64,
    pub nq_short_quantile: f64,
}

impl Default for EntryConfig {
    fn default() -> Self {
        Self {
            factor_long_quantile: default_factor_long_quantile(),
            factor_short_quantile: default_factor_short_quantile(),
            spread_long_quantile: default_spread_long_quantile(),
            spread_short_quantile: default_spread_short_quantile(),
            nq_long_quantile: default_nq_quantile(),
            nq_short_quantile: default_nq_quantile(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionConfig {
    pub factor_exit_quantile_long: f64,
    pub factor_exit_quantile_short: f64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            factor_exit_quantile_long: default_factor_exit_quantile_long(),
            factor_exit_quantile_short: default_factor_exit_quantile_short(),
        }
    }
}

impl CtaSpreadConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("read CTA spread config {}", path.display()))?;
        let config: Self = serde_json::from_str(&raw)
            .with_context(|| format!("parse CTA spread config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn venue(&self) -> TradingVenue {
        CTA_SPREAD_VENUE
    }

    pub fn validate(&self) -> Result<()> {
        if !self.max_pos_u.is_finite() || self.max_pos_u <= 0.0 {
            bail!("max_pos_u must be a positive finite USDT amount");
        }
        if !(self.entry.factor_long_quantile.is_finite()
            && self.entry.factor_short_quantile.is_finite()
            && self.entry.factor_long_quantile > 0.0
            && self.entry.factor_long_quantile <= 1.0
            && self.entry.factor_short_quantile >= 0.0
            && self.entry.factor_short_quantile < 1.0
            && self.entry.factor_short_quantile < self.entry.factor_long_quantile)
        {
            bail!("entry factor quantiles must satisfy 0 <= short < long <= 1");
        }
        if ![
            self.entry.spread_long_quantile,
            self.entry.spread_short_quantile,
            self.entry.nq_long_quantile,
            self.entry.nq_short_quantile,
        ]
        .into_iter()
        .all(|value| value.is_finite() && (0.0..=1.0).contains(&value))
        {
            bail!("entry spread and NQ quantiles must be finite values in [0, 1]");
        }
        if !(self.execution.factor_exit_quantile_long.is_finite()
            && self.execution.factor_exit_quantile_long >= 0.0
            && self.execution.factor_exit_quantile_long < self.entry.factor_long_quantile)
        {
            bail!("factor_exit_quantile_long must be in [0, entry.factor_long_quantile)");
        }
        if !(self.execution.factor_exit_quantile_short.is_finite()
            && self.execution.factor_exit_quantile_short > self.entry.factor_short_quantile
            && self.execution.factor_exit_quantile_short <= 1.0)
        {
            bail!("factor_exit_quantile_short must be in (entry.factor_short_quantile, 1]");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_independent_factor_exit_contract() {
        let config: CtaSpreadConfig = serde_json::from_str(
            r#"{
              "execution":{
                "factor_exit_quantile_long":0.3,
                "factor_exit_quantile_short":0.7
              }
            }"#,
        )
        .expect("parse");
        config.validate().expect("validate");
        assert_eq!(config.venue(), TradingVenue::BinanceFutures);
    }

    #[test]
    fn rejects_unknown_top_level_fields() {
        let result = serde_json::from_str::<CtaSpreadConfig>(
            r#"{
              "venue":"binance-futures",
              "execution":{
                "factor_exit_quantile_long":0.3,
                "factor_exit_quantile_short":0.7
              }
            }"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn omitted_enabled_is_fail_closed() {
        let config: CtaSpreadConfig = serde_json::from_str(
            r#"{
              "execution":{
                "factor_exit_quantile_long":0.3,
                "factor_exit_quantile_short":0.7
              }
            }"#,
        )
        .expect("parse");
        assert!(!config.enabled);
    }
    #[test]
    fn removed_signal_controls_are_rejected() {
        for (section, field, value) in [
            ("entry", "trade_sides", serde_json::json!("long")),
            ("entry", "cooldown_seconds", serde_json::json!(60)),
            ("entry", "signal_delay_seconds", serde_json::json!(1)),
            ("execution", "factor_exit_enabled", serde_json::json!(false)),
        ] {
            let mut raw: serde_json::Value =
                serde_json::from_str(include_str!("../../config/cta_spread.json")).unwrap();
            raw[section][field] = value;
            assert!(
                serde_json::from_value::<CtaSpreadConfig>(raw).is_err(),
                "{field}"
            );
        }
    }
    #[test]
    fn global_full_position_amount_must_be_positive_and_finite() {
        let mut config: CtaSpreadConfig =
            serde_json::from_str(include_str!("../../config/cta_spread.json")).unwrap();
        for value in [0.0, -1.0, f64::INFINITY, f64::NAN] {
            config.max_pos_u = value;
            assert!(config.validate().is_err());
        }
        config.max_pos_u = 2000.0;
        config.validate().unwrap();
    }
}
