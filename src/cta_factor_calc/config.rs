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

/// Time slots retain missing observations; valid samples preserve the existing
/// multi-minute NQ convention. The choice belongs to the factor definition.
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
            missing_policy: if id.factor == "nq" && id.interval != Interval::Minute {
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
pub struct Combination {
    pub name: String,
    #[serde(deserialize_with = "unique_map")]
    pub factors: BTreeMap<String, f64>,
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
    pub fn from_symbol_factors(
        name: String,
        market: Market,
        interval: Interval,
        table: BTreeMap<String, BTreeMap<String, f64>>,
        include_nq: bool,
    ) -> Result<Self> {
        let mut factors = BTreeMap::new();
        for (symbol, row) in table {
            let symbol = symbol.trim().to_ascii_lowercase();
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
                let id = format!("{}-{symbol}-{}-{factor}", market.name(), interval.name());
                ensure!(
                    factors.insert(id.clone(), weight).is_none(),
                    "multiple old sleeves map to {id}; import them as separate combinations"
                );
            }
            if include_nq {
                factors
                    .entry(format!("{}-{symbol}-{}-nq", market.name(), interval.name()))
                    .or_insert(1.0);
            }
        }
        let combination = Self {
            name,
            factors,
            parameters: BTreeMap::new(),
        };
        combination.validate()?;
        Ok(combination)
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
            !self.factors.is_empty() && self.factors.len() <= 4096,
            "combination requires 1..=4096 factor references"
        );
        ensure!(
            self.factors.values().all(|v| v.is_finite())
                && self.factors.values().any(|v| *v != 0.0),
            "combination requires finite weights and a nonzero term"
        );
        for id in self.factors.keys() {
            FactorId::parse(id).with_context(|| format!("combination {}", self.name))?;
        }
        for (id, parameters) in &self.parameters {
            ensure!(self.factors.contains_key(id), "unused parameters: {id}");
            parameters.validate(&FactorId::parse(id)?)?;
        }
        Ok(())
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
        for combination in combinations {
            combination.validate()?;
            ensure!(
                !plan.combinations.contains_key(&combination.name),
                "duplicate combination {}",
                combination.name
            );
            for (name, weight) in &combination.factors {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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
