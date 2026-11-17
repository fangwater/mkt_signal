use crate::cta_factor_calc::config::Interval;
use crate::cta_perp::config::{EntryConfig, ExecutionConfig};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub name: String,
    pub exchange: String,
    pub symbol: String,
    pub interval: String,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
    pub lots_per_rule: i32,
    pub dominant_url: String,
    pub history_grpc: String,
    pub history_days: i64,
    pub zmq_endpoint: String,
    pub data_directory: PathBuf,
    pub bind: String,
    pub entry: EntryConfig,
    pub exits: ExecutionConfig,
    pub rules: Vec<Rule>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub frequencies: BTreeMap<String, FrequencyConfig>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FrequencyConfig {
    pub enabled: bool,
    pub lots_per_rule: i32,
    pub entry: EntryConfig,
    pub exits: ExecutionConfig,
    pub rules: Vec<Rule>,
}
fn enabled_by_default() -> bool {
    true
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub symbol: String,
    pub factor: String,
    pub sign: i8,
    #[serde(rename = "NQ")]
    pub nq: String,
}
impl Rule {
    pub fn gates(&self) -> Result<Option<(f64, f64)>> {
        if matches!(self.nq.as_str(), "关" | "关闭") {
            return Ok(None);
        }
        let (a, b) = self.nq.split_once('/').unwrap_or((&self.nq, &self.nq));
        let (a, b) = (a.parse::<f64>()?, b.parse::<f64>()?);
        ensure!(
            a.is_finite() && b.is_finite() && (0.0..=1.0).contains(&a) && (0.0..=1.0).contains(&b),
            "invalid NQ gates"
        );
        Ok(Some((a, b)))
    }
}
impl Config {
    pub fn interval_kind(&self) -> Result<Interval> {
        match self.interval.as_str() {
            "1min" => Ok(Interval::Minute),
            "5min" => Ok(Interval::FiveMinutes),
            "15min" => Ok(Interval::FifteenMinutes),
            "1h" => Ok(Interval::Hour),
            _ => anyhow::bail!("unsupported native CTA interval {}", self.interval),
        }
    }
    pub fn load(path: &Path) -> Result<Self> {
        let cfg: Self =
            serde_json::from_slice(&std::fs::read(path).context("read CN CTA config")?)?;
        ensure!(
            cfg.interval == "1h",
            "the root strategy and its existing ledger must remain 1h"
        );
        cfg.validate()?;
        Ok(cfg)
    }
    pub fn for_interval(&self, interval: Interval) -> Self {
        let mut cfg = self.clone();
        cfg.frequencies.clear();
        if interval == Interval::Hour {
            return cfg;
        }
        let settings = self
            .frequencies
            .get(interval.name())
            .cloned()
            .unwrap_or_else(|| FrequencyConfig {
                enabled: false,
                lots_per_rule: 1,
                entry: self.entry.clone(),
                exits: self.exits.clone(),
                rules: Vec::new(),
            });
        cfg.name = format!("{}-{}", self.name, interval.name());
        cfg.interval = interval.name().into();
        cfg.enabled = settings.enabled;
        cfg.lots_per_rule = settings.lots_per_rule;
        cfg.entry = settings.entry;
        cfg.exits = settings.exits;
        cfg.rules = settings.rules;
        cfg
    }
    pub fn validate(&self) -> Result<()> {
        let cfg = self;
        ensure!(
            cfg.exchange == "SHFE" && cfg.symbol == "SN",
            "this deployment owns SHFE/SN only"
        );
        cfg.interval_kind()?;
        ensure!(
            (1..=100).contains(&cfg.lots_per_rule),
            "require 1..100 lots per rule"
        );
        ensure!(
            (60..=730).contains(&cfg.history_days),
            "history_days must be 60..730"
        );
        ensure!(
            cfg.rules.len() <= 128 && (!cfg.enabled || !cfg.rules.is_empty()),
            "enabled frequency requires 1..128 factor rules"
        );
        let mut seen = std::collections::BTreeSet::new();
        for rule in &cfg.rules {
            ensure!(
                rule.symbol == cfg.symbol && matches!(rule.sign, -1 | 1),
                "rule symbol/sign mismatch"
            );
            ensure!(
                seen.insert(rule.factor.to_ascii_lowercase()),
                "duplicate factor rule"
            );
            rule.gates()?;
        }
        if !cfg.rules.is_empty() {
            crate::factor_pub::cn_features::FuturesFactorPlan::from_factor_names(
                cfg.rules
                    .iter()
                    .map(|r| format!("cn_features_{}", r.factor))
                    .collect(),
            )?;
        }
        for key in cfg.frequencies.keys() {
            ensure!(
                matches!(key.as_str(), "1min" | "5min" | "15min"),
                "invalid additional frequency"
            );
        }
        for interval in [
            Interval::Minute,
            Interval::FiveMinutes,
            Interval::FifteenMinutes,
        ] {
            if cfg.frequencies.contains_key(interval.name()) {
                cfg.for_interval(interval).validate()?;
            }
        }
        ensure!(
            cfg.entry.factor_long_quantile > cfg.entry.factor_short_quantile
                && cfg.entry.factor_long_quantile <= 1.0
                && cfg.entry.factor_short_quantile >= 0.0,
            "invalid factor entry quantiles"
        );
        ensure!(
            [
                cfg.entry.factor_long_quantile,
                cfg.entry.factor_short_quantile,
                cfg.entry.nq_long_quantile,
                cfg.entry.nq_short_quantile,
                cfg.exits.factor_exit_quantile_long,
                cfg.exits.factor_exit_quantile_short,
            ]
            .iter()
            .all(|v| v.is_finite() && (0.0..=1.0).contains(v)),
            "quantiles must be finite values in 0..1"
        );
        let bind = cfg
            .bind
            .parse::<std::net::SocketAddr>()
            .context("invalid position HTTP bind")?;
        ensure!(
            bind.ip().is_loopback(),
            "position HTTP must listen on loopback"
        );
        Ok(())
    }
}
