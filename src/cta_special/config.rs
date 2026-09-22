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

const MODEL_SERVICE_PREFIX: &str = "model_output/one-binance-futures-1m-";
const MAX_OPEN_LEVELS: usize = 8;
const MAX_OPEN_OFFSET: f64 = 0.01;

fn default_enabled() -> bool {
    false
}

fn default_nq_enabled() -> bool {
    true
}

fn default_trade_sides() -> String {
    "both".to_string()
}

fn default_factor_long_quantile() -> f64 {
    0.9
}

fn default_factor_short_quantile() -> f64 {
    0.1
}

fn default_nq_quantile() -> f64 {
    0.5
}

fn default_signal_delay_seconds() -> i64 {
    1
}

fn default_notional() -> f64 {
    100.0
}

fn default_open_offsets() -> Vec<f64> {
    vec![0.0, 0.0001, 0.0003, 0.0005]
}

fn default_maker_ttl_seconds() -> i64 {
    120
}

fn default_factor_exit_enabled() -> bool {
    true
}

fn default_factor_exit_quantile_long() -> f64 {
    0.3
}

fn default_factor_exit_quantile_short() -> f64 {
    0.7
}

fn default_trailing_stop_enabled() -> bool {
    true
}

fn default_trailing_stop_trigger_step() -> f64 {
    0.02
}

fn default_trailing_stop_move_step() -> f64 {
    0.01
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
    pub trade_sides: String,
    pub factor_long_quantile: f64,
    pub factor_short_quantile: f64,
    pub cooldown_seconds: i64,
    pub signal_delay_seconds: i64,
    pub nq_change_enabled: bool,
    pub nq_long_quantile: f64,
    pub nq_short_quantile: f64,
}

impl Default for EntryConfig {
    fn default() -> Self {
        Self {
            trade_sides: default_trade_sides(),
            factor_long_quantile: default_factor_long_quantile(),
            factor_short_quantile: default_factor_short_quantile(),
            cooldown_seconds: 0,
            signal_delay_seconds: default_signal_delay_seconds(),
            nq_change_enabled: default_nq_enabled(),
            nq_long_quantile: default_nq_quantile(),
            nq_short_quantile: default_nq_quantile(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionConfig {
    pub order_notional_usdt: f64,
    pub open_offsets: Vec<f64>,
    pub maker_ttl_seconds: i64,
    pub factor_exit_enabled: bool,
    pub factor_exit_quantile_long: f64,
    pub factor_exit_quantile_short: f64,
    pub trailing_stop_enabled: bool,
    pub trailing_stop_trigger_step: f64,
    pub trailing_stop_move_step: f64,
    pub max_holding_seconds: i64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            order_notional_usdt: default_notional(),
            open_offsets: default_open_offsets(),
            maker_ttl_seconds: default_maker_ttl_seconds(),
            factor_exit_enabled: default_factor_exit_enabled(),
            factor_exit_quantile_long: default_factor_exit_quantile_long(),
            factor_exit_quantile_short: default_factor_exit_quantile_short(),
            trailing_stop_enabled: default_trailing_stop_enabled(),
            trailing_stop_trigger_step: default_trailing_stop_trigger_step(),
            trailing_stop_move_step: default_trailing_stop_move_step(),
            max_holding_seconds: 0,
        }
    }
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
        self.entry.trade_sides = match self.entry.trade_sides.trim().to_ascii_lowercase().as_str() {
            "combine" | "long_short" | "long,short" | "short,long" => "both".to_string(),
            value => value.to_string(),
        };
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
        if !matches!(self.entry.trade_sides.as_str(), "long" | "short" | "both") {
            bail!("entry.trade_sides must be long, short, or both");
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
        if self.entry.cooldown_seconds < 0 || self.entry.signal_delay_seconds < 0 {
            bail!("entry cooldown_seconds and signal_delay_seconds cannot be negative");
        }
        if ![self.entry.nq_long_quantile, self.entry.nq_short_quantile]
            .into_iter()
            .all(|value| value.is_finite() && (0.0..=1.0).contains(&value))
        {
            bail!("entry NQ quantiles must be finite values in [0, 1]");
        }
        if self.execution.open_offsets.is_empty()
            || self.execution.open_offsets.len() > MAX_OPEN_LEVELS
        {
            bail!("execution.open_offsets must contain 1..={MAX_OPEN_LEVELS} levels");
        }
        let mut previous = None;
        for offset in &self.execution.open_offsets {
            if !(offset.is_finite() && (0.0..=MAX_OPEN_OFFSET).contains(offset)) {
                bail!("execution.open_offsets values must be finite and in [0, {MAX_OPEN_OFFSET}]");
            }
            if previous.is_some_and(|value| *offset <= value) {
                bail!("execution.open_offsets must be strictly increasing");
            }
            previous = Some(*offset);
        }
        if self.execution.maker_ttl_seconds <= 0 {
            bail!("execution.maker_ttl_seconds must be positive");
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
        if self.execution.trailing_stop_enabled
            && (!(self.execution.trailing_stop_trigger_step.is_finite()
                && self.execution.trailing_stop_trigger_step > 0.0)
                || !(self.execution.trailing_stop_move_step.is_finite()
                    && self.execution.trailing_stop_move_step > 0.0
                    && self.execution.trailing_stop_move_step
                        < self.execution.trailing_stop_trigger_step))
        {
            bail!("trailing_stop_move_step must be positive and below trigger_step");
        }
        if self.execution.max_holding_seconds < 0 {
            bail!("execution.max_holding_seconds cannot be negative");
        }
        Ok(())
    }

    pub fn allows_long(&self) -> bool {
        matches!(self.entry.trade_sides.as_str(), "long" | "both")
    }

    pub fn allows_short(&self) -> bool {
        matches!(self.entry.trade_sides.as_str(), "short" | "both")
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
    fn rejects_unknown_top_level_fields() {
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
