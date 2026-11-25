use crate::cta_factor_calc::config::Interval;
use crate::cta_perp::config::{CombinationConfig, EntryConfig, ExecutionConfig, PositionAlgorithm};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddrV4,
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
    /// Lots held per v1/v2 direction (v0 ledgers keep their persisted multiplier).
    /// At least the exchange's minimum opening order (GFEX LC: 2 lots since
    /// 2026-09-22) so every opening is valid.
    #[serde(default = "one_lot")]
    pub lots_per_rule: i32,
    #[serde(default)]
    pub position_algorithm: PositionAlgorithm,
    pub dominant_url: String,
    pub history_grpc: String,
    pub history_days: i64,
    pub redis_endpoint: SocketAddrV4,
    pub data_directory: PathBuf,
    pub bind: String,
    pub entry: EntryConfig,
    pub exits: ExecutionConfig,
    /// v2 thresholds on the mean factor position; unused by v0/v1.
    #[serde(default, skip_serializing_if = "CombinationConfig::is_default")]
    pub combination: CombinationConfig,
    pub rules: Vec<Rule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication: Option<PublicationConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub frequencies: BTreeMap<String, FrequencyConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_products: Vec<PathBuf>,
    #[serde(skip)]
    product_runtime: Option<ProductRuntime>,
}

#[derive(Clone)]
struct ProductRuntime {
    state_directory: PathBuf,
    publication: Option<PublicationConfig>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FrequencyConfig {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) lots_per_rule: Option<i32>,
    #[serde(default)]
    pub position_algorithm: PositionAlgorithm,
    pub entry: EntryConfig,
    pub exits: ExecutionConfig,
    #[serde(default, skip_serializing_if = "CombinationConfig::is_default")]
    pub combination: CombinationConfig,
    pub rules: Vec<Rule>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationConfig {
    pub enabled: bool,
    pub manager_url: String,
    pub source_id: String,
    pub token_env: String,
    #[serde(default)]
    pub include_trading_day: bool,
    pub per_factor_intervals: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub retire_hourly_factor_members: bool,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub products: BTreeSet<String>,
}

impl PublicationConfig {
    pub fn enabled_for(&self, product: &str) -> bool {
        self.enabled && (self.products.is_empty() || self.products.contains(product))
    }
}

fn one_lot() -> i32 {
    1
}
fn enabled_by_default() -> bool {
    true
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub symbol: String,
    pub factor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub sign: i8,
    #[serde(rename = "NQ")]
    pub nq: String,
}
impl Rule {
    pub fn key(&self) -> &str {
        self.id.as_deref().unwrap_or(&self.factor)
    }

    pub fn formula_name(&self) -> String {
        format!("cn_features_{}", self.factor.to_ascii_lowercase())
    }

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
    pub fn factor_names(&self) -> Vec<String> {
        self.rules
            .iter()
            .map(Rule::formula_name)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn strategy_group(&self) -> Result<String> {
        let tail = self
            .name
            .strip_prefix("cta-cn_futures-")
            .context("native CTA name must be cta-cn_futures-NN")?;
        let (instance, frequency) = tail
            .split_once('-')
            .map_or((tail, None), |(id, f)| (id, Some(f)));
        ensure!(
            instance.len() == 2
                && instance.bytes().all(|b| b.is_ascii_digit())
                && instance != "00"
                && frequency.is_none_or(|f| f == self.interval),
            "native CTA requires instance 01..99 and its own frequency suffix"
        );
        let mut group = format!("cn-futures-{instance}");
        if self.is_additional_product() {
            group.push('-');
            group.push_str(&self.symbol.to_ascii_lowercase());
        }
        Ok(group)
    }
    /// Panel path relative to the instance root: empty for the primary product.
    pub fn panel_path(&self) -> String {
        if self.is_additional_product() {
            format!("products/{}/", self.symbol.to_ascii_lowercase())
        } else {
            String::new()
        }
    }
    pub fn is_additional_product(&self) -> bool {
        self.product_runtime.is_some()
    }
    pub fn attach_to(&mut self, primary: &Config) {
        let publication = primary.publication.clone().map(|mut settings| {
            settings.token_env.push('_');
            settings.token_env.push_str(&self.symbol);
            settings
        });
        self.product_runtime = Some(ProductRuntime {
            state_directory: primary.data_directory.join("products").join(&self.symbol),
            publication,
        });
    }
    pub fn state_directory(&self) -> &Path {
        self.product_runtime
            .as_ref()
            .map_or(self.data_directory.as_path(), |product| {
                product.state_directory.as_path()
            })
    }
    pub fn publication_settings(&self) -> Option<&PublicationConfig> {
        if let Some(product) = &self.product_runtime {
            product.publication.as_ref()
        } else {
            self.publication.as_ref()
        }
    }
    pub fn exchange_codes(
        &self,
    ) -> Result<(
        cn_futures_core::session::Exchange,
        cn_market_proto::Exchange,
    )> {
        use cn_futures_core::session::Exchange as Core;
        use cn_market_proto::Exchange as Grpc;
        match (
            self.exchange.as_str(),
            cn_futures_core::segments::exchange_of(&self.symbol),
        ) {
            ("CFFEX", Some("ccfx")) => Ok((Core::Ccfx, Grpc::Cffex)),
            ("SHFE", Some("xsge")) => Ok((Core::Xsge, Grpc::Shfe)),
            ("INE", Some("xsie")) => Ok((Core::Xsie, Grpc::Ine)),
            ("CZCE", Some("xzce")) => Ok((Core::Xzce, Grpc::Czce)),
            ("DCE", Some("xdce")) => Ok((Core::Xdce, Grpc::Dce)),
            ("GFEX", Some("xgfe")) => Ok((Core::Xgfe, Grpc::Gfex)),
            _ => anyhow::bail!(
                "unsupported native CTA market {}/{}",
                self.exchange,
                self.symbol
            ),
        }
    }
    pub fn native_prefix(&self) -> String {
        if matches!(self.exchange.as_str(), "CFFEX" | "CZCE") {
            self.symbol.clone()
        } else {
            self.symbol.to_ascii_lowercase()
        }
    }
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
        cfg.additional_products.clear();
        if interval == Interval::Hour {
            return cfg;
        }
        let settings = self
            .frequencies
            .get(interval.name())
            .cloned()
            .unwrap_or_else(|| FrequencyConfig {
                enabled: false,
                lots_per_rule: None,
                position_algorithm: self.position_algorithm,
                entry: self.entry.clone(),
                exits: self.exits.clone(),
                combination: self.combination.clone(),
                rules: Vec::new(),
            });
        cfg.name = format!("{}-{}", self.name, interval.name());
        cfg.interval = interval.name().into();
        cfg.enabled = settings.enabled;
        cfg.lots_per_rule = settings.lots_per_rule.unwrap_or(self.lots_per_rule);
        cfg.position_algorithm = settings.position_algorithm;
        cfg.entry = settings.entry;
        cfg.exits = settings.exits;
        cfg.combination = settings.combination;
        cfg.rules = settings.rules;
        cfg
    }
    pub fn validate(&self) -> Result<()> {
        let cfg = self;
        cfg.strategy_group()?;
        cn_futures_pbs::config::Redis {
            endpoint: cfg.redis_endpoint,
        }
        .validate()?;
        if let Some(publication) = cfg.publication_settings() {
            let url = reqwest::Url::parse(&publication.manager_url)
                .context("invalid native Manager URL")?;
            ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "Manager URL must be HTTP(S) without credentials/query/fragment"
            );
            ensure!(
                !publication.source_id.is_empty()
                    && publication
                        .source_id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')),
                "invalid native publication source_id"
            );
            ensure!(
                !publication.token_env.is_empty()
                    && publication
                        .token_env
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "invalid native publication token environment name"
            );
            ensure!(
                publication.per_factor_intervals.is_empty(),
                "one-lot targets require one aggregate Manager member per frequency; migrate old per-factor members first"
            );
        }

        cfg.exchange_codes()?;
        ensure!(
            cfg.symbol.bytes().all(|b| b.is_ascii_uppercase()),
            "expected upper-case product"
        );
        cfg.interval_kind()?;
        ensure!(
            (1..=100).contains(&cfg.lots_per_rule)
                && cfg
                    .frequencies
                    .values()
                    .all(|f| f.lots_per_rule.is_none_or(|lots| (1..=100).contains(&lots))),
            "lots_per_rule must be 1..100"
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
                rule.id.as_ref().is_none_or(|id| !id.is_empty()
                    && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')),
                "invalid factor rule ID"
            );
            ensure!(
                seen.insert(rule.key().to_ascii_lowercase()),
                "duplicate factor rule ID"
            );
            rule.gates()?;
        }
        if !cfg.rules.is_empty() {
            crate::factor_pub::cn_features::FuturesFactorPlan::from_factor_names(
                cfg.factor_names(),
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
        let min_factor = cfg.position_algorithm.decision_value(0.0);
        ensure!(
            cfg.entry.factor_long_quantile > cfg.entry.factor_short_quantile,
            "invalid factor entry thresholds"
        );
        if cfg.position_algorithm == PositionAlgorithm::V1 {
            ensure!(
                cfg.exits.factor_exit_quantile_long < cfg.entry.factor_long_quantile
                    && cfg.exits.factor_exit_quantile_short > cfg.entry.factor_short_quantile,
                "v1 exit thresholds must lie inside their corresponding entry thresholds"
            );
        }
        cfg.combination.validate()?;
        ensure!(
            [
                cfg.entry.factor_long_quantile,
                cfg.entry.factor_short_quantile,
                cfg.exits.factor_exit_quantile_long,
                cfg.exits.factor_exit_quantile_short,
            ]
            .iter()
            .all(|v| (min_factor..=1.0).contains(v)),
            "factor thresholds must be in {min_factor}..1"
        );
        ensure!(
            [cfg.entry.nq_long_quantile, cfg.entry.nq_short_quantile]
                .iter()
                .all(|v| (0.0..=1.0).contains(v)),
            "NQ quantiles must be in 0..1"
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
