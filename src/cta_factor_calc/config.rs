use anyhow::{bail, ensure, Context, Result};
use order_common::TradingVenue;
use period_pbs::kafka::KafkaConsumerConfig;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Market {
    #[serde(rename = "binance-futures")]
    BinanceFutures,
    #[serde(rename = "okex-futures")]
    OkexFutures,
    #[serde(rename = "binance-margin")]
    BinanceMargin,
}
impl Market {
    pub fn short_name(self) -> &'static str {
        match self {
            Self::BinanceFutures => "bn",
            Self::OkexFutures => "okx",
            Self::BinanceMargin => "bn-spot",
        }
    }
    pub const ALL: [Self; 3] = [Self::BinanceFutures, Self::OkexFutures, Self::BinanceMargin];
    pub fn name(self) -> &'static str {
        match self {
            Self::BinanceFutures => "binance-futures",
            Self::OkexFutures => "okex-futures",
            Self::BinanceMargin => "binance-margin",
        }
    }
    pub fn topic(self) -> &'static str {
        match self {
            Self::BinanceFutures => "binance-futures",
            Self::OkexFutures => "okex-swap",
            Self::BinanceMargin => "binance-spot",
        }
    }
    pub fn venue(self) -> TradingVenue {
        match self {
            Self::BinanceFutures => TradingVenue::BinanceFutures,
            Self::OkexFutures => TradingVenue::OkexFutures,
            Self::BinanceMargin => TradingVenue::BinanceMargin,
        }
    }
    fn prefix(raw: &str) -> Option<(Self, &str)> {
        Self::ALL.into_iter().find_map(|v| {
            raw.strip_prefix(&format!("{}-", v.name()))
                .map(|rest| (v, rest))
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Interval {
    #[serde(rename = "1min")]
    Minute,
    #[serde(rename = "5min")]
    FiveMinutes,
    #[serde(rename = "15min")]
    FifteenMinutes,
}
impl Interval {
    pub fn name(self) -> &'static str {
        match self {
            Self::Minute => "1min",
            Self::FiveMinutes => "5min",
            Self::FifteenMinutes => "15min",
        }
    }
    pub fn millis(self) -> i64 {
        match self {
            Self::Minute => 60_000,
            Self::FiveMinutes => 300_000,
            Self::FifteenMinutes => 900_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct FactorId {
    pub market: Market,
    pub reference_market: Option<Market>,
    pub symbol: String,
    pub interval: Interval,
    pub factor: String,
}
impl FactorId {
    pub fn parse(raw: &str) -> Result<Self> {
        ensure!(
            raw.len() <= 200 && raw == raw.trim().to_ascii_lowercase(),
            "factor ID must be canonical lowercase: {raw}"
        );
        let (market, rest) = Market::prefix(raw).context("factor ID has no supported market")?;
        let (reference_market, rest) =
            Market::prefix(rest).map_or((None, rest), |(v, s)| (Some(v), s));
        let mut parts = rest.split('-');
        let symbol = parts.next().unwrap_or("").to_ascii_uppercase();
        let interval = match parts.next() {
            Some("1min") => Interval::Minute,
            Some("5min") => Interval::FiveMinutes,
            Some("15min") => Interval::FifteenMinutes,
            _ => bail!("unsupported factor interval in {raw}"),
        };
        let name = parts.next().context("missing factor name")?;
        ensure!(
            parts.next().is_none()
                && !name.is_empty()
                && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "invalid factor name in {raw}"
        );
        ensure!(
            symbol.ends_with("USDT")
                && symbol.len() > 4
                && symbol.len() <= 32
                && symbol.bytes().all(|b| b.is_ascii_alphanumeric()),
            "factor requires an internal USDT symbol: {raw}"
        );
        ensure!(
            (name == "spread") == reference_market.is_some(),
            "spread requires two ordered markets; other factors require one"
        );
        ensure!(
            reference_market != Some(market),
            "spread markets must be distinct"
        );
        let factor = if name.starts_with("td_") || name.starts_with("tp_") {
            name.to_ascii_uppercase()
        } else {
            name.into()
        };
        if !matches!(name, "nq" | "spread") {
            let plan = crate::factor_pub::fusion_factor_pub::SymbolFactorPlan::from_factor_names(
                "cta-factor-calc",
                vec![factor.clone()],
            )?;
            crate::factor_pub::fusion_factor_pub::app::BaselineReplayState::validate_factor_plan(
                &plan,
            )?;
        }
        Ok(Self {
            market,
            reference_market,
            symbol,
            interval,
            factor,
        })
    }
    pub fn name(&self) -> String {
        format!(
            "{}-{}{}-{}-{}",
            self.market.name(),
            self.reference_market
                .map_or(String::new(), |v| format!("{}-", v.name())),
            self.symbol.to_ascii_lowercase(),
            self.interval.name(),
            self.factor.to_ascii_lowercase()
        )
    }
    pub fn is_formula(&self) -> bool {
        !matches!(self.factor.as_str(), "nq" | "spread")
    }
    pub fn markets(&self) -> impl Iterator<Item = Market> {
        std::iter::once(self.market).chain(self.reference_market)
    }
}

/// Time slots retain missing observations. NQ defaults to valid samples at
/// every frequency; the choice belongs to the factor definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingPolicy {
    Slots,
    ValidSamples,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Parameters {
    pub normalize_window: usize,
    pub normalize_min_periods: usize,
    pub clip_zscore: f64,
    pub rank_window: usize,
    pub rank_min_periods: usize,
    pub lookback: usize,
    pub missing_policy: MissingPolicy,
}
impl Parameters {
    pub fn for_id(id: &FactorId) -> Self {
        let slots = (172_800_000 / id.interval.millis()) as usize;
        Self {
            normalize_window: slots / 2,
            normalize_min_periods: match id.interval {
                Interval::Minute => 1000,
                Interval::FiveMinutes => 200,
                Interval::FifteenMinutes => 67,
            },
            clip_zscore: 3.0,
            rank_window: if id.factor == "nq" { 120 } else { slots },
            rank_min_periods: if id.factor == "nq" { 60 } else { slots / 2 },
            lookback: 120,
            missing_policy: if id.factor == "nq" {
                MissingPolicy::ValidSamples
            } else {
                MissingPolicy::Slots
            },
        }
    }
    pub fn validate(&self, id: &FactorId) -> Result<()> {
        ensure!(
            (2..=100_000).contains(&self.normalize_window)
                && (1..=self.normalize_window).contains(&self.normalize_min_periods),
            "invalid normalization window for {}",
            id.name()
        );
        ensure!(
            (1..=100_000).contains(&self.rank_window)
                && (1..=self.rank_window).contains(&self.rank_min_periods)
                && (1..=100_000).contains(&self.lookback),
            "invalid factor windows for {}",
            id.name()
        );
        ensure!(
            self.clip_zscore.is_finite() && self.clip_zscore > 0.0,
            "invalid clipping parameter"
        );
        ensure!(
            id.factor == "nq" || self.missing_policy == MissingPolicy::Slots,
            "only NQ supports valid_samples windows"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactorRule {
    #[serde(rename = "标的")]
    pub symbol: String,
    pub nq: String,
    #[serde(rename = "符号")]
    pub weight: f64,
    #[serde(rename = "因子")]
    pub factor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub venue: Option<Market>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<Interval>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_venue: Option<Market>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalParameters {
    pub enabled: bool,
    pub max_pos_u: f64,
    #[serde(default)]
    pub position_algorithm: crate::cta_perp::config::PositionAlgorithm,
    #[serde(default)]
    pub symbol_overrides: BTreeMap<String, crate::cta_perp::config::SymbolThresholdOverrides>,
    pub entry: crate::cta_perp::config::EntryConfig,
    pub execution: crate::cta_perp::config::ExecutionConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread: Option<SpreadFilters>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpreadFilters {
    pub long_quantile: f64,
    pub short_quantile: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Combination {
    pub name: String,
    pub venue: Market,
    pub interval: Interval,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_venue: Option<Market>,
    pub factor_rules: Vec<FactorRule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<SignalParameters>,
    #[serde(
        default,
        deserialize_with = "unique_map",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub parameters: BTreeMap<String, Parameters>,
}

fn unique_map<'de, D, T>(deserializer: D) -> std::result::Result<BTreeMap<String, T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Visitor<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Visitor<T> {
        type Value = BTreeMap<String, T>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an object with unique keys")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut result = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, T>()? {
                if result.insert(key.clone(), value).is_some() {
                    return Err(serde::de::Error::custom(format!("duplicate key {key}")));
                }
            }
            Ok(result)
        }
    }
    deserializer.deserialize_map(Visitor(std::marker::PhantomData))
}
impl Combination {
    /// Manager ownership follows market and frequency, independently of the
    /// configuration filename and the factor rules inside the strategy.
    pub fn manager_strategy_name(&self) -> String {
        match self.reference_venue {
            Some(reference) => format!(
                "cta-spread-{}-{}-{}-pos",
                self.venue.short_name(),
                reference.short_name(),
                self.interval.name()
            ),
            None => format!(
                "cta-prep-{}-{}-pos",
                self.venue.short_name(),
                self.interval.name()
            ),
        }
    }

    pub fn from_symbol_factors(
        name: String,
        venue: Market,
        interval: Interval,
        table: BTreeMap<String, BTreeMap<String, f64>>,
        include_nq: bool,
    ) -> Result<Self> {
        let mut factor_rules = Vec::new();
        let mut seen = BTreeSet::new();
        for (symbol, row) in table {
            for (factor, weight) in row {
                let factor = factor.trim().to_ascii_lowercase();
                ensure!(
                    !(factor.ends_with("_positive") && weight < 0.0)
                        && !(factor.ends_with("_negative") && weight > 0.0),
                    "old sleeve sign disagrees with its weight"
                );
                let factor = factor
                    .strip_suffix("_positive")
                    .or_else(|| factor.strip_suffix("_negative"))
                    .unwrap_or(&factor);
                ensure!(
                    seen.insert((symbol.clone(), factor.to_string())),
                    "multiple old sleeves map to one factor; import them as separate combinations"
                );
                factor_rules.push(FactorRule {
                    symbol: symbol.clone(),
                    factor: factor.into(),
                    weight,
                    nq: if include_nq { "0.50" } else { "关闭" }.into(),
                    venue: None,
                    interval: None,
                    reference_venue: None,
                });
            }
        }
        let combination = Self {
            name,
            venue,
            interval,
            reference_venue: None,
            factor_rules,
            signal: None,
            parameters: BTreeMap::new(),
        };
        combination.validate()?;
        Ok(combination)
    }
    pub fn rule_id(&self, rule: &FactorRule) -> Result<FactorId> {
        let symbol = rule.symbol.trim().to_ascii_lowercase();
        let symbol = if symbol.ends_with("usdt") {
            symbol
        } else {
            format!("{symbol}usdt")
        };
        let reference = if rule.factor.eq_ignore_ascii_case("spread") {
            rule.reference_venue.or(self.reference_venue)
        } else {
            None
        };
        let name = format!(
            "{}-{}{symbol}-{}-{}",
            rule.venue.unwrap_or(self.venue).name(),
            reference.map_or(String::new(), |m| format!("{}-", m.name())),
            rule.interval.unwrap_or(self.interval).name(),
            rule.factor.trim().to_ascii_lowercase()
        );
        FactorId::parse(&name)
    }
    pub fn factors(&self) -> Result<BTreeMap<String, f64>> {
        let mut factors = BTreeMap::new();
        for rule in &self.factor_rules {
            let id = self.rule_id(rule)?;
            if rule.weight == 0.0 {
                continue;
            }
            factors.entry(id.name()).or_insert(rule.weight);
            if rule.nq != "关闭" && id.factor != "nq" {
                let nq = FactorId {
                    factor: "nq".into(),
                    reference_market: None,
                    ..id
                };
                factors.entry(nq.name()).or_insert(1.0);
            }
            if self.signal.is_some()
                && self.reference_venue.is_some()
                && self.rule_id(rule)?.is_formula()
            {
                let source = self.rule_id(rule)?;
                let spread = FactorId {
                    market: self.venue,
                    reference_market: self.reference_venue,
                    symbol: source.symbol,
                    interval: self.interval,
                    factor: "spread".into(),
                };
                factors.entry(spread.name()).or_insert(1.0);
            }
        }
        Ok(factors)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.name.is_empty()
                && self.name.len() <= 96
                && self
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
            "invalid combination name"
        );
        ensure!(
            !self.factor_rules.is_empty() && self.factor_rules.len() <= 8192,
            "combination requires 1..=8192 factor rows"
        );
        let mut seen = BTreeSet::new();
        for rule in &self.factor_rules {
            ensure!(rule.weight.is_finite(), "factor sign/weight must be finite");
            let id = self.rule_id(rule)?;
            let gate = crate::cta_perp::config::FactorNqRule {
                symbol: id.symbol.clone(),
                factor: id.factor.clone(),
                sign: if rule.weight < 0.0 { -1 } else { 1 },
                nq: rule.nq.clone(),
            };
            gate.thresholds()?;
            ensure!(
                seen.insert((id.name(), rule.weight.to_bits(), gate.nq_key())),
                "duplicate factor/sign/NQ row"
            );
            ensure!(
                id.factor == "spread" || rule.reference_venue.is_none(),
                "only spread rows accept a reference venue"
            );
        }
        let factors = self.factors()?;
        ensure!(
            !factors.is_empty() && factors.len() <= 8192,
            "combination requires nonzero factor references"
        );
        for (id, parameters) in &self.parameters {
            ensure!(factors.contains_key(id), "unused parameters: {id}");
            parameters.validate(&FactorId::parse(id)?)?;
        }
        if self.signal.is_some() {
            self.signal_config()?.validate()?;
            if self.reference_venue.is_some() {
                self.spread_config()?.validate()?;
            }
        }
        Ok(())
    }
    pub fn load(path: &Path) -> Result<Self> {
        let combination: Self = serde_json::from_slice(&std::fs::read(path)?)
            .context("read unified CTA combination")?;
        combination.validate()?;
        ensure!(
            path.file_stem().and_then(|s| s.to_str()) == Some(&combination.name),
            "combination filename must match its name"
        );
        Ok(combination)
    }
    pub fn signal_config(&self) -> Result<crate::cta_perp::config::CtaPerpConfig> {
        use crate::cta_perp::{
            config::{CtaPerpConfig, FactorNqRule, PositionAlgorithm},
            interval::BarInterval,
        };
        let p = self
            .signal
            .as_ref()
            .context("combination has no signal parameters")?;
        let source_venue = self.reference_venue.unwrap_or(self.venue);
        if self.reference_venue.is_some() {
            ensure!(
                self.venue == Market::OkexFutures
                    && self.reference_venue == Some(Market::BinanceFutures)
                    && self.interval == Interval::Minute
                    && p.position_algorithm == PositionAlgorithm::V0,
                "Spread positions require OKX/Binance futures, 1min and independent factor rules"
            );
        } else {
            ensure!(
                self.venue == Market::BinanceFutures && p.spread.is_none(),
                "Prep positions require Binance futures without spread filters"
            );
        }
        let mut rules = Vec::new();
        for rule in self.factor_rules.iter().filter(|r| r.weight != 0.0) {
            let id = self.rule_id(rule)?;
            if self.reference_venue.is_some() && id.factor == "spread" {
                continue;
            }
            ensure!(
                id.market == source_venue
                    && id.interval == self.interval
                    && id.reference_market.is_none()
                    && id.factor != "nq",
                "signal rows require ordinary factors at the combination venue/frequency"
            );
            rules.push(FactorNqRule {
                symbol: id.symbol,
                factor: id.factor,
                sign: if rule.weight > 0.0 { 1 } else { -1 },
                nq: rule.nq.clone(),
            });
        }
        ensure!(!rules.is_empty(), "signal requires nonzero factor rows");
        if p.position_algorithm == PositionAlgorithm::V1 {
            let expected = if p.entry.nq_change_enabled {
                Some((p.entry.nq_long_quantile, p.entry.nq_short_quantile))
            } else {
                None
            };
            ensure!(
                rules
                    .iter()
                    .all(|r| r.thresholds().is_ok_and(|t| t == expected)),
                "symbol mean requires one common NQ gate for its factor rows"
            );
            rules.clear();
        }
        Ok(CtaPerpConfig {
            bar_interval: match self.interval {
                Interval::Minute => BarInterval::OneMinute,
                Interval::FiveMinutes => BarInterval::FiveMinutes,
                Interval::FifteenMinutes => BarInterval::FifteenMinutes,
            },
            enabled: p.enabled,
            max_pos_u: p.max_pos_u,
            position_algorithm: p.position_algorithm,
            symbol_overrides: p.symbol_overrides.clone(),
            factor_rules: rules,
            strict_nq: false,
            linear_nq: true,
            entry: p.entry.clone(),
            execution: p.execution.clone(),
        })
    }
    pub fn spread_config(&self) -> Result<crate::cta_spread::config::CtaSpreadConfig> {
        use crate::cta_spread::config::{
            CtaSpreadConfig, EntryConfig, ExecutionConfig, SpreadMode,
        };
        ensure!(
            self.reference_venue.is_some(),
            "not a spread position table"
        );
        let p = self.signal.as_ref().context("missing signal parameters")?;
        let filters = p
            .spread
            .as_ref()
            .context("Spread positions require explicit spread filters")?;
        let c = CtaSpreadConfig {
            mode: SpreadMode::OkexBinance,
            enabled: p.enabled,
            nq_change_enabled: p.entry.nq_change_enabled,
            max_pos_u: p.max_pos_u,
            symbol_overrides: BTreeMap::new(),
            entry: EntryConfig {
                factor_long_quantile: p.entry.factor_long_quantile,
                factor_short_quantile: p.entry.factor_short_quantile,
                spread_long_quantile: filters.long_quantile,
                spread_short_quantile: filters.short_quantile,
                nq_long_quantile: p.entry.nq_long_quantile,
                nq_short_quantile: p.entry.nq_short_quantile,
            },
            execution: ExecutionConfig {
                factor_exit_quantile_long: p.execution.factor_exit_quantile_long,
                factor_exit_quantile_short: p.execution.factor_exit_quantile_short,
            },
        };
        c.validate()?;
        Ok(c)
    }
    pub fn signal_table(&self) -> Result<crate::cta_perp::factors::SymbolFactorTable> {
        use crate::cta_perp::factors::{SymbolFactorTable, SymbolFactors};
        let config = self.signal_config()?;
        if !config.factor_rules.is_empty() {
            return Ok(config.rule_table());
        }
        let mut table = SymbolFactorTable::new();
        for rule in self.factor_rules.iter().filter(|r| r.weight != 0.0) {
            let id = self.rule_id(rule)?;
            let row = table.entry(id.symbol).or_insert_with(|| SymbolFactors {
                factors: vec![],
                weights: vec![],
            });
            ensure!(
                !row.factors.contains(&id.factor),
                "symbol mean cannot duplicate a factor row"
            );
            row.factors.push(id.factor);
            row.weights.push(rule.weight);
        }
        Ok(table)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Calculation {
    pub id: FactorId,
    pub parameters: Parameters,
    pub combinations: BTreeSet<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Plan {
    pub combinations: BTreeMap<String, Combination>,
    pub calculations: BTreeMap<String, Calculation>,
}
impl Plan {
    pub fn merge(combinations: impl IntoIterator<Item = Combination>) -> Result<Self> {
        let mut plan = Self::default();
        let mut signal_owners = BTreeSet::new();
        for combination in combinations {
            combination.validate()?;
            if combination.signal.is_some() {
                ensure!(
                    signal_owners.insert(combination.manager_strategy_name()),
                    "only one signal combination may own each market/frequency strategy"
                );
            }
            ensure!(
                !plan.combinations.contains_key(&combination.name),
                "duplicate combination {}",
                combination.name
            );
            for (name, weight) in &combination.factors()? {
                if *weight == 0.0 {
                    continue;
                }
                let id = FactorId::parse(name)?;
                let parameters = combination
                    .parameters
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| Parameters::for_id(&id));
                parameters.validate(&id)?;
                let entry = plan
                    .calculations
                    .entry(name.clone())
                    .or_insert_with(|| Calculation {
                        id,
                        parameters: parameters.clone(),
                        combinations: BTreeSet::new(),
                    });
                ensure!(entry.parameters == parameters, "conflicting parameters for shared factor {name}; one ID must have one definition");
                entry.combinations.insert(combination.name.clone());
            }
            plan.combinations
                .insert(combination.name.clone(), combination);
        }
        ensure!(
            plan.calculations.len() <= 8192,
            "too many factor calculations"
        );
        Ok(plan)
    }
    pub fn topics(&self) -> Vec<String> {
        self.calculations
            .values()
            .flat_map(|c| c.id.markets())
            .map(|v| v.topic().to_owned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    pub fn load(directory: &Path) -> Result<Self> {
        let mut files = std::fs::read_dir(directory)
            .with_context(|| format!("read {}", directory.display()))?
            .map(|v| v.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        files.retain(|p| p.extension().is_some_and(|v| v == "json"));
        files.sort();
        let combinations = files
            .into_iter()
            .map(|path| -> Result<Combination> {
                let combination: Combination = serde_json::from_slice(&std::fs::read(&path)?)
                    .with_context(|| format!("read combination {}", path.display()))?;
                ensure!(
                    path.file_stem().and_then(|s| s.to_str()) == Some(&combination.name),
                    "combination filename must match its name: {}",
                    path.display()
                );
                Ok(combination)
            })
            .collect::<Result<Vec<_>>>()?;
        Self::merge(combinations)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Worker {
    #[serde(rename = "cta-prep-binance-futures")]
    Perp,
    #[serde(rename = "cta-factor-spread")]
    Spread,
}
impl Worker {
    pub fn name(self) -> &'static str {
        match self {
            Self::Perp => "cta-prep-binance-futures",
            Self::Spread => "cta-factor-spread",
        }
    }
    pub fn default_config(self) -> &'static str {
        match self {
            Self::Perp => "config/cta_prep_binance_futures.toml",
            Self::Spread => "config/cta_factor_spread.toml",
        }
    }
    pub fn validate_plan(self, plan: &Plan) -> Result<()> {
        if self == Self::Spread {
            ensure!(
                plan.combinations
                    .values()
                    .all(|c| c.signal.is_none() || c.reference_venue.is_some()),
                "Spread worker position tables require an explicit market pair"
            );
        }
        if self == Self::Perp {
            ensure!(
                plan.calculations
                    .values()
                    .all(|c| c.id.market == Market::BinanceFutures
                        && c.id.reference_market.is_none()),
                "cta-prep-binance-futures accepts only Binance futures factor inputs"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub combinations_dir: PathBuf,
    pub history_dir: PathBuf,
    pub lookback_secs: u64,
    pub catchup_timeout_secs: u64,
    pub kafka: KafkaConsumerConfig,
    #[serde(default)]
    pub amount_thresholds: BTreeMap<String, [f64; 2]>,
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let mut config: Self = toml::from_str(&std::fs::read_to_string(path)?)
            .context("parse factor worker config")?;
        let parent = path.parent().unwrap_or(Path::new("."));
        if config.combinations_dir.is_relative() {
            config.combinations_dir = parent.join(&config.combinations_dir);
        }
        if config.history_dir.is_relative() {
            config.history_dir = parent.join(&config.history_dir);
        }
        ensure!(
            config.lookback_secs > 0 && config.catchup_timeout_secs > 0,
            "positive replay bounds required"
        );
        ensure!(
            !config.kafka.brokers.trim().is_empty(),
            "Kafka brokers are required"
        );
        for (key, [medium, large]) in &config.amount_thresholds {
            ensure!(
                key.split_once(':').is_some()
                    && crate::common::amount_threshold::is_online_amount_threshold(*medium, *large),
                "invalid amount thresholds for {key}"
            );
        }
        Ok(config)
    }
}
