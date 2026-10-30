use super::interval::BarInterval;
use anyhow::{bail, Context, Result};
use order_common::TradingVenue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

pub const CTA_PERP_VENUE: TradingVenue = TradingVenue::BinanceFutures;
pub const CTA_PERP_VENUE_NAME: &str = "binance-futures";
pub const CTA_PERP_SIGNAL_POLL_INTERVAL_MS: u64 = 10;
pub const CTA_PERP_MAX_QUOTE_AGE_MS: u64 = 5_000;
pub const CTA_PERP_MAX_MODEL_AGE_MS: u64 = 120_000;
pub const CTA_PERP_STATUS_PATH: &str = "run/cta_perp_status.json";
pub const SYMBOL_MEAN_STRATEGY: &str = "symbol_mean";

fn default_max_pos_u() -> f64 {
    200.0
}

fn default_enabled() -> bool {
    false
}

fn default_nq_enabled() -> bool {
    true
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

fn default_factor_exit_quantile_long() -> f64 {
    0.3
}

fn default_factor_exit_quantile_short() -> f64 {
    0.7
}

/// How a factor percentile is used for position decisions, independent of sign.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PositionAlgorithm {
    #[default]
    V0,
    V1,
}

impl PositionAlgorithm {
    pub fn allocation(self) -> &'static str {
        match self {
            Self::V0 => "per_symbol_factor",
            Self::V1 => "per_symbol",
        }
    }

    pub fn decision_value(self, x: f64) -> f64 {
        match self {
            Self::V0 => x,
            Self::V1 => 2.0 * x - 1.0,
        }
    }

    fn minimum(self) -> f64 {
        self.decision_value(0.0)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SymbolThresholdOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nq_long_quantile: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nq_short_quantile: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factor_long_quantile: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factor_short_quantile: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factor_exit_quantile_long: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factor_exit_quantile_short: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FactorNqRule {
    #[serde(rename = "标的")]
    pub symbol: String,
    pub nq: String,
    #[serde(rename = "符号")]
    pub sign: i8,
    #[serde(rename = "因子")]
    pub factor: String,
}

impl FactorNqRule {
    pub fn venue_symbol(&self) -> String {
        if self.symbol.ends_with("USDT") {
            self.symbol.clone()
        } else {
            format!("{}USDT", self.symbol)
        }
    }
    pub fn thresholds(&self) -> Result<Option<(f64, f64)>> {
        if self.nq == "关闭" {
            return Ok(None);
        }
        let parts: Vec<_> = self.nq.split('/').collect();
        anyhow::ensure!(parts.len() == 1 || parts.len() == 2, "invalid nq format");
        let long: f64 = parts[0].trim().parse()?;
        let short: f64 = parts.last().unwrap().trim().parse()?;
        anyhow::ensure!(
            [long, short]
                .iter()
                .all(|x| x.is_finite() && (0.0..=1.0).contains(x)),
            "NQ thresholds must be in [0,1]"
        );
        Ok(Some((long, short)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CtaPerpConfig {
    #[serde(default)]
    pub bar_interval: BarInterval,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// One entry's USDT amount: per factor in v0, per symbol mean in v1.
    #[serde(default = "default_max_pos_u")]
    pub max_pos_u: f64,
    #[serde(default)]
    pub position_algorithm: PositionAlgorithm,
    #[serde(default)]
    pub symbol_overrides: BTreeMap<String, SymbolThresholdOverrides>,
    #[serde(default)]
    pub factor_rules: Vec<FactorNqRule>,
    #[serde(skip)]
    pub strict_nq: bool,
    #[serde(default)]
    pub entry: EntryConfig,
    pub execution: ExecutionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct EntryConfig {
    pub factor_long_quantile: f64,
    pub factor_short_quantile: f64,
    pub nq_change_enabled: bool,
    pub nq_long_quantile: f64,
    pub nq_short_quantile: f64,
}

impl Default for EntryConfig {
    fn default() -> Self {
        Self {
            factor_long_quantile: default_factor_long_quantile(),
            factor_short_quantile: default_factor_short_quantile(),
            nq_change_enabled: default_nq_enabled(),
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

impl CtaPerpConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("read CTA perp config {}", path.display()))?;
        let config: Self = serde_json::from_str(&raw)
            .with_context(|| format!("parse CTA perp config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn venue(&self) -> TradingVenue {
        CTA_PERP_VENUE
    }

    /// Resolve position thresholds and NQ entry gates independently per symbol.
    pub fn for_symbol(&self, symbol: &str) -> Self {
        let mut effective = Self {
            bar_interval: self.bar_interval,
            enabled: self.enabled,
            max_pos_u: self.max_pos_u,
            position_algorithm: self.position_algorithm,
            symbol_overrides: BTreeMap::new(),
            factor_rules: Vec::new(),
            strict_nq: false,
            entry: self.entry.clone(),
            execution: self.execution.clone(),
        };
        if let Some(row) = self.symbol_overrides.get(symbol) {
            if let Some(value) = row.nq_long_quantile {
                effective.entry.nq_long_quantile = value;
            }
            if let Some(value) = row.nq_short_quantile {
                effective.entry.nq_short_quantile = value;
            }
            if let Some(value) = row.factor_long_quantile {
                effective.entry.factor_long_quantile = value;
            }
            if let Some(value) = row.factor_short_quantile {
                effective.entry.factor_short_quantile = value;
            }
            if let Some(value) = row.factor_exit_quantile_long {
                effective.execution.factor_exit_quantile_long = value;
            }
            if let Some(value) = row.factor_exit_quantile_short {
                effective.execution.factor_exit_quantile_short = value;
            }
        }
        effective
    }

    pub fn for_strategy(&self, symbol: &str, factor: &str) -> Self {
        let mut effective = self.for_symbol(symbol);
        if let Some(rule) = self
            .factor_rules
            .iter()
            .find(|r| r.venue_symbol() == symbol && r.factor == factor)
        {
            effective.strict_nq = true;
            match rule.thresholds().ok().flatten() {
                Some((long, short)) => {
                    effective.entry.nq_change_enabled = true;
                    effective.entry.nq_long_quantile = long;
                    effective.entry.nq_short_quantile = short;
                }
                None => effective.entry.nq_change_enabled = false,
            }
        }
        effective
    }

    pub fn rule_table(&self) -> super::factors::SymbolFactorTable {
        let mut table = super::factors::SymbolFactorTable::new();
        for rule in &self.factor_rules {
            let row =
                table
                    .entry(rule.venue_symbol())
                    .or_insert_with(|| super::factors::SymbolFactors {
                        factors: vec![],
                        weights: vec![],
                    });
            row.factors.push(rule.factor.clone());
            row.weights.push(f64::from(rule.sign));
        }
        table
    }

    pub fn validate(&self) -> Result<()> {
        if !self.factor_rules.is_empty() {
            anyhow::ensure!(
                self.position_algorithm == PositionAlgorithm::V0
                    && self.bar_interval == BarInterval::OneMinute,
                "factor_rules requires v0 and 1m"
            );
            anyhow::ensure!(self.factor_rules.len() <= 8192, "too many factor rules");
            let mut seen = std::collections::BTreeSet::new();
            for rule in &self.factor_rules {
                let symbol = rule.venue_symbol();
                anyhow::ensure!(
                    !rule.symbol.is_empty()
                        && symbol.len() <= 32
                        && symbol
                            .bytes()
                            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
                    "invalid rule symbol"
                );
                anyhow::ensure!(rule.sign == 1 || rule.sign == -1, "rule sign must be +/-1");
                anyhow::ensure!(
                    !rule.factor.is_empty()
                        && rule
                            .factor
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || c == b'_'),
                    "invalid rule factor"
                );
                anyhow::ensure!(
                    seen.insert((symbol.clone(), rule.factor.clone())),
                    "duplicate symbol/factor rule"
                );
                rule.thresholds()?;
                self.for_strategy(&symbol, &rule.factor)
                    .validate_controls()?;
            }
        }

        if !self.factor_rules.is_empty() {
            super::factors::parse_factor_table(&serde_json::to_string(&self.rule_table())?)?;
        }
        self.validate_controls()?;
        if self.symbol_overrides.len() > 256 {
            bail!("symbol_overrides supports at most 256 symbols");
        }
        for symbol in self.symbol_overrides.keys() {
            if symbol.is_empty()
                || symbol.len() > 32
                || !symbol
                    .bytes()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
            {
                bail!("invalid symbol_overrides symbol: {symbol}");
            }
            self.for_symbol(symbol)
                .validate_controls()
                .with_context(|| format!("symbol_overrides.{symbol}"))?;
        }
        Ok(())
    }

    fn validate_controls(&self) -> Result<()> {
        if self.bar_interval.uses_linear_nq()
            && (self.entry.nq_long_quantile != 0.5 || self.entry.nq_short_quantile != 0.5)
        {
            bail!(
                "{} NQ uses fixed 0.50 linear quantile thresholds",
                self.bar_interval.as_str()
            );
        }
        let minimum = self.position_algorithm.minimum();
        if !self.max_pos_u.is_finite() || self.max_pos_u <= 0.0 {
            bail!("max_pos_u must be a positive finite USDT amount");
        }
        if !(self.entry.factor_long_quantile.is_finite()
            && self.entry.factor_short_quantile.is_finite()
            && self.entry.factor_long_quantile > minimum
            && self.entry.factor_long_quantile <= 1.0
            && self.entry.factor_short_quantile >= minimum
            && self.entry.factor_short_quantile < 1.0
            && self.entry.factor_short_quantile < self.entry.factor_long_quantile)
        {
            bail!("entry factor thresholds must satisfy {minimum} <= short < long <= 1");
        }
        if ![self.entry.nq_long_quantile, self.entry.nq_short_quantile]
            .into_iter()
            .all(|value| value.is_finite() && (0.0..=1.0).contains(&value))
        {
            bail!("entry NQ quantiles must be finite values in [0, 1]");
        }
        if !(self.execution.factor_exit_quantile_long.is_finite()
            && self.execution.factor_exit_quantile_long >= minimum
            && self.execution.factor_exit_quantile_long < self.entry.factor_long_quantile)
        {
            bail!("factor_exit_quantile_long must be in [{minimum}, entry.factor_long_quantile)");
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
        let config: CtaPerpConfig = serde_json::from_str(
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
        assert_eq!(config.position_algorithm, PositionAlgorithm::V0);
        assert!(config.symbol_overrides.is_empty());
    }

    #[test]
    fn symbol_thresholds_inherit_per_field_for_both_algorithms() {
        let mut config: CtaPerpConfig = serde_json::from_value(serde_json::json!({
            "execution": {},
            "symbol_overrides": {
                "ETHUSDT": {"factor_long_quantile": 0.8, "factor_exit_quantile_short": 0.6},
                "BTCUSDT": {"factor_short_quantile": 0.2, "factor_exit_quantile_long": 0.4}
            }
        }))
        .unwrap();
        for algorithm in [PositionAlgorithm::V0, PositionAlgorithm::V1] {
            config.position_algorithm = algorithm;
            config.validate().unwrap();
            let eth = config.for_symbol("ETHUSDT");
            assert_eq!(eth.entry.factor_long_quantile, 0.8);
            assert_eq!(eth.entry.factor_short_quantile, 0.1);
            assert_eq!(eth.execution.factor_exit_quantile_long, 0.3);
            assert_eq!(eth.execution.factor_exit_quantile_short, 0.6);
            assert_eq!(eth.entry.nq_long_quantile, config.entry.nq_long_quantile);
            assert_eq!(eth.max_pos_u, config.max_pos_u);
            assert_eq!(config.for_symbol("SOLUSDT").entry, config.entry);
        }
        config.entry.factor_long_quantile = 0.35;
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("BTCUSDT"));
    }

    #[test]
    fn symbol_nq_bounds_and_five_minute_contract_are_validated() {
        let mut config: CtaPerpConfig = serde_json::from_value(serde_json::json!({
            "execution": {}, "symbol_overrides": {
                "BTCUSDT": {"nq_long_quantile":0.65, "nq_short_quantile":0.35}
            }
        }))
        .unwrap();
        config.validate().unwrap();
        config.bar_interval = BarInterval::FiveMinutes;
        assert!(format!("{:#}", config.validate().unwrap_err()).contains("BTCUSDT"));
        config.bar_interval = BarInterval::OneMinute;
        for value in [-0.1, 1.1, f64::NAN, f64::INFINITY] {
            config
                .symbol_overrides
                .get_mut("BTCUSDT")
                .unwrap()
                .nq_long_quantile = Some(value);
            assert!(format!("{:#}", config.validate().unwrap_err()).contains("BTCUSDT"));
        }
    }

    #[test]
    fn algorithm_ranges_and_override_fields_are_validated() {
        let mut raw = serde_json::json!({"position_algorithm":"v1", "entry":{
            "factor_long_quantile":0.6, "factor_short_quantile":-0.6
        }, "execution":{"factor_exit_quantile_long":0.2,"factor_exit_quantile_short":-0.2}});
        serde_json::from_value::<CtaPerpConfig>(raw.clone())
            .unwrap()
            .validate()
            .unwrap();
        raw["position_algorithm"] = serde_json::json!("v0");
        assert!(serde_json::from_value::<CtaPerpConfig>(raw.clone())
            .unwrap()
            .validate()
            .is_err());
        raw["position_algorithm"] = serde_json::json!("v2");
        assert!(serde_json::from_value::<CtaPerpConfig>(raw).is_err());
        for overrides in [
            serde_json::json!({"ethusdt":{"factor_long_quantile":0.8}}),
            serde_json::json!({"ETHUSDT":{"factor_exit_quantile_long":0.95}}),
            serde_json::json!({"ETHUSDT":{"factor_short_quantile":-0.1}}),
        ] {
            let config: CtaPerpConfig = serde_json::from_value(serde_json::json!({
                "execution":{}, "symbol_overrides":overrides
            }))
            .unwrap();
            assert!(config.validate().is_err());
        }
        for field in ["max_pos_u", "position_algorithm"] {
            assert!(serde_json::from_value::<CtaPerpConfig>(serde_json::json!({
                "execution":{}, "symbol_overrides":{"ETHUSDT":{field:0.5}}
            }))
            .is_err());
        }
    }

    #[test]
    fn rejects_unknown_top_level_fields() {
        let result = serde_json::from_str::<CtaPerpConfig>(
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
        let config: CtaPerpConfig = serde_json::from_str(
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
                serde_json::from_str(include_str!("../../config/cta_perp.json")).unwrap();
            raw[section][field] = value;
            assert!(
                serde_json::from_value::<CtaPerpConfig>(raw).is_err(),
                "{field}"
            );
        }
    }
    #[test]
    fn global_full_position_amount_must_be_positive_and_finite() {
        let mut config: CtaPerpConfig =
            serde_json::from_str(include_str!("../../config/cta_perp.json")).unwrap();
        for value in [0.0, -1.0, f64::INFINITY, f64::NAN] {
            config.max_pos_u = value;
            assert!(config.validate().is_err());
        }
        config.max_pos_u = 2000.0;
        config.validate().unwrap();
    }
}
