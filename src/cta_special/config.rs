use anyhow::{bail, Context, Result};
use order_common::TradingVenue;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::Path;

pub const CTA_SPECIAL_VENUE: TradingVenue = TradingVenue::BinanceFutures;
pub const CTA_SPECIAL_VENUE_NAME: &str = "binance-futures";
pub const CTA_SPECIAL_SIGNAL_POLL_INTERVAL_MS: u64 = 10;
pub const CTA_SPECIAL_MAX_QUOTE_AGE_MS: u64 = 5_000;
pub const CTA_SPECIAL_MAX_MODEL_AGE_MS: u64 = 120_000;
pub const CTA_SPECIAL_STATUS_PATH: &str = "run/cta_special_status.json";
pub const CTA_SPECIAL_SIGNAL_DELAY_US: i64 = 1_000_000;
pub const CTA_SPECIAL_OPEN_OFFSETS: [f64; 4] = [0.0, 0.0001, 0.0003, 0.0005];
pub const CTA_SPECIAL_OPEN_TTL_US: i64 = 120_000_000;
pub const CTA_SPECIAL_TRAILING_STOP_ENABLED: bool = true;

const MODEL_SERVICE_PREFIX: &str = "model_output/one-binance-futures-1m-";

fn default_enabled() -> bool {
    false
}

fn default_nq_enabled() -> bool {
    true
}

fn default_notional() -> f64 {
    100.0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CtaSpecialConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub rule_name: String,
    pub symbols: Vec<String>,
    #[serde(default)]
    pub entry: EntryConfig,
    pub execution: ExecutionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct EntryConfig {
    pub nq_change_enabled: bool,
}

impl Default for EntryConfig {
    fn default() -> Self {
        Self {
            nq_change_enabled: default_nq_enabled(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    #[serde(default = "default_notional")]
    pub order_notional_usdt: f64,
    pub factor_exit_quantile_long: f64,
    pub factor_exit_quantile_short: f64,
    pub trailing_stop_trigger_step: f64,
    pub trailing_stop_move_step: f64,
}

impl CtaSpecialConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("read CTA special config {}", path.display()))?;
        let mut config: Self = serde_json::from_str(&raw)
            .with_context(|| format!("parse CTA special config {}", path.display()))?;
        config.normalize();
        config.validate()?;
        Ok(config)
    }

    pub fn venue(&self) -> TradingVenue {
        CTA_SPECIAL_VENUE
    }

    pub fn model_service(&self) -> String {
        format!("{MODEL_SERVICE_PREFIX}{}", self.rule_name)
    }

    pub fn symbol_set(&self) -> HashSet<String> {
        self.symbols.iter().cloned().collect()
    }

    fn normalize(&mut self) {
        self.rule_name = self.rule_name.trim().to_ascii_lowercase();
        for symbol in &mut self.symbols {
            *symbol = symbol.trim().to_ascii_uppercase();
        }
        self.symbols.sort();
        self.symbols.dedup();
    }

    pub fn validate(&self) -> Result<()> {
        if !matches!(self.rule_name.as_str(), "tp_vpi_018" | "baseline_104") {
            bail!("cta_special rule_name must be tp_vpi_018 or baseline_104");
        }
        if self.symbols.is_empty()
            || self
                .symbols
                .iter()
                .any(|symbol| symbol.is_empty() || symbol.len() > 32)
        {
            bail!("symbols must contain at least one non-empty symbol of at most 32 bytes");
        }
        if !(self.execution.order_notional_usdt.is_finite()
            && self.execution.order_notional_usdt > 0.0)
        {
            bail!("execution.order_notional_usdt must be positive");
        }
        if !(0.0..0.9).contains(&self.execution.factor_exit_quantile_long) {
            bail!("factor_exit_quantile_long must be in [0, 0.9)");
        }
        if !(self.execution.factor_exit_quantile_short > 0.1
            && self.execution.factor_exit_quantile_short <= 1.0)
        {
            bail!("factor_exit_quantile_short must be in (0.1, 1]");
        }
        if !(self.execution.trailing_stop_trigger_step.is_finite()
            && self.execution.trailing_stop_trigger_step > 0.0)
            || !(self.execution.trailing_stop_move_step.is_finite()
                && self.execution.trailing_stop_move_step > 0.0
                && self.execution.trailing_stop_move_step
                    < self.execution.trailing_stop_trigger_step)
        {
            bail!("trailing_stop_move_step must be positive and below trigger_step");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_compact_v007_contract() {
        let config: CtaSpecialConfig = serde_json::from_str(
            r#"{
              "rule_name":"TP_VPI_018",
              "symbols":["btcusdt"],
              "execution":{
                "factor_exit_quantile_long":0.3,
                "factor_exit_quantile_short":0.7,
                "trailing_stop_trigger_step":0.02,
                "trailing_stop_move_step":0.01
              }
            }"#,
        )
        .expect("parse");
        let mut config = config;
        config.normalize();
        config.validate().expect("validate");
        assert_eq!(config.symbols, ["BTCUSDT"]);
        assert_eq!(
            config.model_service(),
            "model_output/one-binance-futures-1m-tp_vpi_018"
        );
        assert_eq!(config.venue(), TradingVenue::BinanceFutures);
    }

    #[test]
    fn rejects_removed_fixed_fields() {
        let result = serde_json::from_str::<CtaSpecialConfig>(
            r#"{
              "venue":"binance-futures",
              "rule_name":"baseline_104",
              "symbols":["BTCUSDT"],
              "execution":{
                "factor_exit_quantile_long":0.3,
                "factor_exit_quantile_short":0.7,
                "trailing_stop_trigger_step":0.02,
                "trailing_stop_move_step":0.01
              }
            }"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn omitted_enabled_is_fail_closed() {
        let config: CtaSpecialConfig = serde_json::from_str(
            r#"{
              "rule_name":"baseline_104",
              "symbols":["BTCUSDT"],
              "execution":{
                "factor_exit_quantile_long":0.3,
                "factor_exit_quantile_short":0.7,
                "trailing_stop_trigger_step":0.02,
                "trailing_stop_move_step":0.01
              }
            }"#,
        )
        .expect("parse");
        assert!(!config.enabled);
    }
}
