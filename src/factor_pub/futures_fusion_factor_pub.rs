//! China-futures features with a native five-level order book.
//!
//! Input, state, registry, formulas, replay, and output are independent from
//! crypto. Every formula is registered here against the native five-level book.

// 引擎实现放在私有子模块，由专用 bin / replay 驱动；编其他 bin 时 rustc
// 会把这些项标成 dead_code。
#![allow(dead_code)]

#[path = "cn_features/app.rs"]
mod app;
#[path = "cn_features/baselines.rs"]
mod baselines;
#[path = "cn_features/cfg.rs"]
mod cfg;
#[path = "cn_features/factor_enum.rs"]
mod factor_enum;
#[path = "cn_features/intermediates.rs"]
mod intermediates;
#[path = "cn_features/math.rs"]
mod math;
#[path = "cn_features/opv_factors.rs"]
mod opv_factors;
#[path = "cn_features/plain_factors.rs"]
mod plain_factors;
#[path = "cn_features/plan.rs"]
mod plan;
#[path = "cn_features/publisher.rs"]
mod publisher;
#[cfg(test)]
#[path = "cn_features/review_manifest.rs"]
mod review_manifest;
#[path = "cn_features/view.rs"]
mod view;
#[path = "cn_features/zscore.rs"]
mod zscore;

use anyhow::{bail, Context, Result};
use std::collections::{HashSet, VecDeque};

use self::app::{CnDepthLevel, CnDepthSnapshot5, CnReplayState};
use self::factor_enum::{CnFactorId, CN_FACTOR_COUNT};
use self::plan::CnFormulaPlan;

pub const FUTURES_DEPTH_LEVELS: usize = 5;
pub const FUTURES_TRADE_FIELD_COUNT: usize = 32;
pub const MAX_FUTURES_HISTORY: usize = 4096;
pub const QUALITY_SEGMENT_BREAK: u32 = 1 << 9;

pub const FUTURES_TRADE_FIELD_NAMES: [&str; FUTURES_TRADE_FIELD_COUNT] = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "avg_amount",
    "count",
    "buy_count",
    "sell_count",
    "buy_amount",
    "sell_amount",
    "buy_volume",
    "sell_volume",
    "large_order",
    "medium_order",
    "small_order",
    "large_buy",
    "large_sell",
    "medium_buy",
    "medium_sell",
    "small_buy",
    "small_sell",
    "vwap",
    "buy_vwap",
    "sell_vwap",
    "net_buy_amount",
    "net_buy_volume",
    "net_buy_pct",
    "net_buy_large",
    "net_buy_medium",
    "net_buy_small",
];

pub const SUPPORTED_FUTURES_FACTOR_COUNT: usize = CN_FACTOR_COUNT;
pub const CN_ALL_FACTORS: &str = "cn_features_all";

#[derive(Debug, Clone, PartialEq)]
pub struct FuturesDepth5 {
    pub bid_prices: [f64; FUTURES_DEPTH_LEVELS],
    pub bid_amounts: [f64; FUTURES_DEPTH_LEVELS],
    pub ask_prices: [f64; FUTURES_DEPTH_LEVELS],
    pub ask_amounts: [f64; FUTURES_DEPTH_LEVELS],
}

impl FuturesDepth5 {
    pub fn from_slices(
        bid_prices: &[f64],
        bid_amounts: &[f64],
        ask_prices: &[f64],
        ask_amounts: &[f64],
    ) -> Result<Self> {
        let depth = Self {
            bid_prices: exact_depth_array("bid_prices", bid_prices)?,
            bid_amounts: exact_depth_array("bid_amounts", bid_amounts)?,
            ask_prices: exact_depth_array("ask_prices", ask_prices)?,
            ask_amounts: exact_depth_array("ask_amounts", ask_amounts)?,
        };
        depth.validate_levels()?;
        Ok(depth)
    }

    fn validate_levels(&self) -> Result<()> {
        for (name, values) in [
            ("bid_prices", &self.bid_prices),
            ("ask_prices", &self.ask_prices),
        ] {
            for (index, value) in values.iter().copied().enumerate() {
                if !value.is_nan() && !positive_finite(value) {
                    bail!("{name}[{index}] must be NaN or finite and positive, got {value}");
                }
            }
        }
        for (name, values) in [
            ("bid_amounts", &self.bid_amounts),
            ("ask_amounts", &self.ask_amounts),
        ] {
            for (index, value) in values.iter().copied().enumerate() {
                if !value.is_nan() && !nonnegative_finite(value) {
                    bail!("{name}[{index}] must be NaN or finite and non-negative, got {value}");
                }
            }
        }

        let best_bid = self.bid_prices[0];
        let best_ask = self.ask_prices[0];
        if best_bid.is_finite() && best_ask.is_finite() && best_bid > best_ask {
            bail!("crossed best book: bid={best_bid} ask={best_ask}");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FuturesTradeBar {
    pub values: [f64; FUTURES_TRADE_FIELD_COUNT],
}

impl FuturesTradeBar {
    pub fn from_slice(values: &[f64]) -> Result<Self> {
        let values = values.try_into().map_err(|_| {
            anyhow::anyhow!(
                "futures trade input must contain exactly {FUTURES_TRADE_FIELD_COUNT} fields, got {}",
                values.len()
            )
        })?;
        Ok(Self { values })
    }

    pub fn value(&self, name: &str) -> Option<f64> {
        FUTURES_TRADE_FIELD_NAMES
            .iter()
            .position(|field| *field == name)
            .map(|index| self.values[index])
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FuturesFusionInput {
    pub ts_ms: i64,
    pub symbol: String,
    pub trading_day: u32,
    pub trade: FuturesTradeBar,
    pub depth: Option<FuturesDepth5>,
    pub quality_flags: u32,
    pub volume_multiple: f64,
    pub volume_multiple_verified: bool,
}

impl FuturesFusionInput {
    pub fn validate(&self) -> Result<()> {
        if self.symbol.is_empty()
            || !self
                .symbol
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            bail!("invalid futures symbol: {}", self.symbol);
        }
        if self.trading_day == 0 {
            bail!("trading_day must be non-zero");
        }
        if !positive_finite(self.volume_multiple) {
            bail!(
                "volume_multiple must be finite and positive, got {}",
                self.volume_multiple
            );
        }
        let Some(depth) = self.depth.as_ref() else {
            bail!(
                "cn_features replay rejects rows without a native five-level book: symbol={} ts_ms={} trading_day={}",
                self.symbol,
                self.ts_ms,
                self.trading_day
            );
        };
        depth.validate_levels()?;
        Ok(())
    }
}

const CN_FACTOR_PREFIX: &str = "cn_features_";

#[derive(Debug, Clone)]
pub struct FuturesFactorPlan {
    names: Vec<String>,
    formula_plan: CnFormulaPlan,
}

impl FuturesFactorPlan {
    pub fn from_factor_names(raw_names: Vec<String>) -> Result<Self> {
        if raw_names.is_empty() {
            bail!("at least one cn_features factor is required");
        }
        let requested = if raw_names.len() == 1 && raw_names[0].trim() == CN_ALL_FACTORS {
            CnFactorId::ALL
                .iter()
                .copied()
                .map(canonical_cn_name)
                .collect()
        } else {
            raw_names
        };

        let mut seen = HashSet::with_capacity(requested.len());
        let mut names = Vec::with_capacity(requested.len());
        let mut source_names = Vec::with_capacity(requested.len());
        for raw_name in requested {
            let name = raw_name.trim();
            if name.is_empty() {
                bail!("cn_features factor name must not be empty");
            }
            let id = parse_cn_factor_name(name)?;
            let canonical = canonical_cn_name(id);
            if !seen.insert(canonical.clone()) {
                bail!("duplicate cn_features factor: {canonical}");
            }
            names.push(canonical);
            source_names.push(id.as_name().to_string());
        }

        let formula_plan = CnFormulaPlan::from_factor_names("cn_features", source_names)?;
        Ok(Self {
            names,
            formula_plan,
        })
    }

    pub fn factor_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.names.iter().map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

fn canonical_cn_name(id: CnFactorId) -> String {
    format!("{CN_FACTOR_PREFIX}{}", id.as_name().to_ascii_lowercase())
}

fn parse_cn_factor_name(name: &str) -> Result<CnFactorId> {
    let suffix = name.strip_prefix(CN_FACTOR_PREFIX).ok_or_else(|| {
        anyhow::anyhow!(
            "unsupported cn_features factor {name}; expected {CN_FACTOR_PREFIX}<legacy_factor_name>"
        )
    })?;
    CnFactorId::ALL
        .iter()
        .copied()
        .find(|id| id.as_name().eq_ignore_ascii_case(suffix))
        .ok_or_else(|| anyhow::anyhow!("unsupported cn_features factor {name}"))
}

#[derive(Default)]
pub struct FuturesFusionState {
    symbol: Option<String>,
    trading_day: Option<u32>,
    last_ts_ms: Option<i64>,
    history: VecDeque<FuturesFusionInput>,
    formula_state: CnReplayState,
}

impl FuturesFusionState {
    pub fn push(&mut self, input: FuturesFusionInput) -> Result<()> {
        input.validate()?;
        if let Some(symbol) = self.symbol.as_deref() {
            if symbol != input.symbol {
                bail!(
                    "futures state symbol mismatch: state={symbol} input={}",
                    input.symbol
                );
            }
        } else {
            self.symbol = Some(input.symbol.clone());
        }
        if let Some(previous) = self.last_ts_ms {
            if input.ts_ms <= previous {
                bail!(
                    "futures timestamps must be strictly increasing: previous={previous} current={}",
                    input.ts_ms
                );
            }
        }

        let reset = input.quality_flags & QUALITY_SEGMENT_BREAK != 0
            || self
                .trading_day
                .is_some_and(|trading_day| trading_day != input.trading_day);
        if reset {
            self.history.clear();
            self.formula_state = CnReplayState::default();
        }

        let formula_values = domestic_formula_values(&input);
        let depth = input.depth.as_ref().map(native_depth_snapshot);
        self.formula_state
            .push_native(input.ts_ms, &formula_values, depth)
            .context("push domestic-futures formula row")?;

        self.trading_day = Some(input.trading_day);
        self.last_ts_ms = Some(input.ts_ms);
        self.history.push_back(input);
        if self.history.len() > MAX_FUTURES_HISTORY {
            self.history.pop_front();
        }
        Ok(())
    }

    pub fn factor_values(&mut self, plan: &FuturesFactorPlan) -> Result<Vec<Option<f64>>> {
        self.history
            .back()
            .context("cannot compute futures factors before an input row is pushed")?;
        let values = self.formula_state.factor_values(&plan.formula_plan);
        Ok(values
            .into_iter()
            .map(|value| value.is_finite().then_some(value))
            .collect())
    }

    pub fn history_len(&self) -> usize {
        self.history.len()
    }
}

fn domestic_formula_values(input: &FuturesFusionInput) -> [f64; FUTURES_TRADE_FIELD_COUNT] {
    const VWAP: usize = 23;
    const BUY_VWAP: usize = 24;
    const SELL_VWAP: usize = 25;

    let mut values = input.trade.values;
    if !input.volume_multiple_verified {
        values[VWAP] = f64::NAN;
        values[BUY_VWAP] = f64::NAN;
        values[SELL_VWAP] = f64::NAN;
    }
    values
}

fn native_depth_snapshot(depth: &FuturesDepth5) -> CnDepthSnapshot5 {
    let bids = std::array::from_fn(|index| CnDepthLevel {
        price: depth.bid_prices[index],
        amount: depth.bid_amounts[index],
    });
    let asks = std::array::from_fn(|index| CnDepthLevel {
        price: depth.ask_prices[index],
        amount: depth.ask_amounts[index],
    });
    CnDepthSnapshot5 { bids, asks }
}

fn exact_depth_array(name: &str, values: &[f64]) -> Result<[f64; FUTURES_DEPTH_LEVELS]> {
    values.try_into().map_err(|_| {
        anyhow::anyhow!(
            "{name} must contain exactly {FUTURES_DEPTH_LEVELS} native levels, got {}",
            values.len()
        )
    })
}

fn positive_finite(value: f64) -> bool {
    value.is_finite() && value > 0.0
}

fn nonnegative_finite(value: f64) -> bool {
    value.is_finite() && value >= 0.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn depth() -> FuturesDepth5 {
        FuturesDepth5::from_slices(
            &[100.0, 99.0, 98.0, 97.0, 96.0],
            &[10.0, 20.0, 30.0, 40.0, 50.0],
            &[101.0, 102.0, 103.0, 104.0, 105.0],
            &[5.0, 5.0, 5.0, 5.0, 5.0],
        )
        .unwrap()
    }

    fn input(ts_ms: i64, trading_day: u32, quality_flags: u32, close: f64) -> FuturesFusionInput {
        let mut values = [0.0; FUTURES_TRADE_FIELD_COUNT];
        values[0] = close;
        values[1] = close + 1.0;
        values[2] = close - 1.0;
        values[3] = close;
        values[4] = 3.0;
        values[5] = close * 30.0;
        values[10] = close * 20.0;
        values[11] = close * 10.0;
        values[12] = 2.0;
        values[13] = 1.0;
        values[23] = close;
        values[24] = close;
        values[25] = close;
        values[26] = close * 10.0;
        values[27] = 1.0;
        values[28] = 1.0 / 3.0;
        FuturesFusionInput {
            ts_ms,
            symbol: "AP2601".to_string(),
            trading_day,
            trade: FuturesTradeBar::from_slice(&values).unwrap(),
            depth: Some(depth()),
            quality_flags,
            volume_multiple: 10.0,
            volume_multiple_verified: true,
        }
    }

    fn varied_input(index: usize) -> FuturesFusionInput {
        let phase = index as f64;
        let close = 100.0 + phase * 0.002 + (phase * 0.17).sin() * 0.3;
        let open = close - (phase * 0.11).cos() * 0.08;
        let high = open.max(close) + 0.2 + (index % 7) as f64 * 0.01;
        let low = open.min(close) - 0.2 - (index % 5) as f64 * 0.01;
        let volume = 20.0 + (index % 13) as f64;
        let buy_volume = volume * (0.42 + (index % 5) as f64 * 0.025);
        let sell_volume = volume - buy_volume;
        let amount = close * volume * 10.0;
        let buy_amount = close * buy_volume * 10.0 * 1.001;
        let sell_amount = amount - buy_amount;
        let count = 5.0 + (index % 9) as f64;
        let buy_count = 2.0 + (index % 4) as f64;
        let sell_count = count - buy_count;

        let mut values = [0.0; FUTURES_TRADE_FIELD_COUNT];
        values[0] = open;
        values[1] = high;
        values[2] = low;
        values[3] = close;
        values[4] = volume;
        values[5] = amount;
        values[6] = amount / count;
        values[7] = count;
        values[8] = buy_count;
        values[9] = sell_count;
        values[10] = buy_amount;
        values[11] = sell_amount;
        values[12] = buy_volume;
        values[13] = sell_volume;
        values[14] = amount * 0.45;
        values[15] = amount * 0.35;
        values[16] = amount * 0.20;
        values[17] = buy_amount * 0.45;
        values[18] = sell_amount * 0.45;
        values[19] = buy_amount * 0.35;
        values[20] = sell_amount * 0.35;
        values[21] = buy_amount * 0.20;
        values[22] = sell_amount * 0.20;
        values[23] = amount / volume / 10.0;
        values[24] = buy_amount / buy_volume / 10.0;
        values[25] = sell_amount / sell_volume / 10.0;
        values[26] = buy_amount - sell_amount;
        values[27] = buy_volume - sell_volume;
        values[28] = (buy_volume - sell_volume) / volume;
        values[29] = values[17] - values[18];
        values[30] = values[19] - values[20];
        values[31] = values[21] - values[22];

        let bid_prices = std::array::from_fn(|level| {
            close - 0.5 - level as f64 * (0.08 + (index % 3) as f64 * 0.005)
        });
        let bid_amounts = std::array::from_fn(|level| {
            10.0 + level as f64 * 3.0 + (index % (level + 3)) as f64 * 0.7
        });
        let ask_prices = std::array::from_fn(|level| {
            close + 0.5 + level as f64 * (0.09 + (index % 4) as f64 * 0.004)
        });
        let ask_amounts = std::array::from_fn(|level| {
            8.0 + level as f64 * 2.0 + (index % (level + 4)) as f64 * 0.6
        });

        FuturesFusionInput {
            ts_ms: index as i64 * 1_000 + 1_000,
            symbol: "AP2601".to_string(),
            trading_day: 20251103,
            trade: FuturesTradeBar { values },
            depth: Some(FuturesDepth5 {
                bid_prices,
                bid_amounts,
                ask_prices,
                ask_amounts,
            }),
            quality_flags: 0,
            volume_multiple: 10.0,
            volume_multiple_verified: true,
        }
    }

    #[test]
    fn rejects_any_non_five_depth_shape() {
        for levels in [4, 6, 20] {
            let error = FuturesDepth5::from_slices(
                &vec![100.0; levels],
                &vec![1.0; levels],
                &vec![101.0; levels],
                &vec![1.0; levels],
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("exactly 5 native levels"),
                "levels={levels} error={error:#}"
            );
        }
    }

    #[test]
    fn all_selector_expands_to_every_migrated_factor() {
        let plan = FuturesFactorPlan::from_factor_names(vec![CN_ALL_FACTORS.to_string()]).unwrap();
        assert_eq!(plan.len(), SUPPORTED_FUTURES_FACTOR_COUNT);
        assert_eq!(plan.len(), 632);
        let names: Vec<&str> = plan.factor_names().collect();
        let unique: HashSet<&str> = names.iter().copied().collect();
        assert_eq!(unique.len(), names.len());
        assert!(names.iter().all(|name| name.starts_with(CN_FACTOR_PREFIX)));
    }

    #[test]
    fn known_formula_repairs_are_computable() {
        let plan = FuturesFactorPlan::from_factor_names(vec![
            "cn_features_factor_166".to_string(),
            "cn_features_baseline_159".to_string(),
            "cn_features_baseline_160".to_string(),
        ])
        .unwrap();
        let mut state = FuturesFusionState::default();
        state.push(input(1_000, 20251103, 0, 100.0)).unwrap();
        let _ = state.factor_values(&plan).unwrap();
        state.push(input(2_000, 20251103, 0, 101.0)).unwrap();
        let values = state.factor_values(&plan).unwrap();

        assert!((values[0].unwrap() - 125.0 / 175.0).abs() < 1e-12);
        let log_return = (101.0_f64 / 100.0).ln();
        assert!((values[1].unwrap() - log_return.sin()).abs() < 1e-12);
        assert!((values[2].unwrap() - log_return.cos()).abs() < 1e-12);
    }

    #[test]
    fn rejects_rows_without_a_native_five_level_book() {
        let mut state = FuturesFusionState::default();
        state.push(input(1_000, 20251103, 0, 100.0)).unwrap();

        let mut row = input(2_000, 20251103, 0, 101.0);
        row.depth = None;
        let error = state.push(row).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("rejects rows without a native five-level book"),
            "{error:#}"
        );
        assert_eq!(state.history_len(), 1);
    }

    #[test]
    fn missing_outer_level_only_nulls_factors_that_use_it() {
        let plan = FuturesFactorPlan::from_factor_names(vec![
            "cn_features_factor_009".to_string(),
            "cn_features_factor_013".to_string(),
            "cn_features_factor_002".to_string(),
            "cn_features_factor_017".to_string(),
            "cn_features_factor_054".to_string(),
            "cn_features_factor_062".to_string(),
            "cn_features_factor_139".to_string(),
            "cn_features_factor_144".to_string(),
            "cn_features_factor_072".to_string(),
        ])
        .unwrap();
        let mut row = input(1_000, 20251103, 0, 100.0);
        let depth = row.depth.as_mut().unwrap();
        depth.bid_prices[4] = f64::NAN;
        depth.bid_amounts[4] = f64::NAN;
        let mut state = FuturesFusionState::default();
        state.push(row).unwrap();
        let values = state.factor_values(&plan).unwrap();

        assert!(values[0].is_some(), "BBO-only factor must remain available");
        assert_eq!(values[1], None, "full-book concentration needs level 5");
        assert_eq!(values[2], None, "full-book ratio needs level 5");
        for (index, name) in [
            "factor_017",
            "factor_054",
            "factor_062",
            "factor_139",
            "factor_144",
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(values[index + 3], None, "{name} must not skip level 5");
        }
        assert!(values[8].is_some(), "ask-only factor must remain available");
    }

    #[test]
    fn missing_best_bid_only_nulls_factors_that_read_it() {
        let plan = FuturesFactorPlan::from_factor_names(vec![
            "cn_features_factor_071".to_string(),
            "cn_features_factor_072".to_string(),
            "cn_features_factor_166".to_string(),
            "cn_features_baseline_159".to_string(),
        ])
        .unwrap();
        let mut state = FuturesFusionState::default();
        state.push(input(1_000, 20251103, 0, 100.0)).unwrap();
        let _ = state.factor_values(&plan).unwrap();

        let mut row = input(2_000, 20251103, 0, 101.0);
        let depth = row.depth.as_mut().unwrap();
        depth.bid_prices[0] = f64::NAN;
        depth.bid_amounts[0] = f64::NAN;
        state.push(row).unwrap();
        let values = state.factor_values(&plan).unwrap();

        assert_eq!(values[0], None, "bid-side factor needs the missing bid");
        assert!(values[1].is_some(), "ask-only factor must remain available");
        assert_eq!(values[2], None, "two-sided imbalance needs the missing bid");
        assert!(
            values[3].is_some(),
            "trade-only factor must remain available"
        );
    }

    #[test]
    fn five_level_adaptations_use_only_native_observations() {
        let plan = FuturesFactorPlan::from_factor_names(vec![
            "cn_features_factor_013".to_string(),
            "cn_features_factor_014".to_string(),
            "cn_features_factor_025".to_string(),
            "cn_features_factor_026".to_string(),
            "cn_features_factor_096".to_string(),
            "cn_features_factor_102".to_string(),
        ])
        .unwrap();
        let mut state = FuturesFusionState::default();
        state.push(input(1_000, 20251103, 0, 100.0)).unwrap();
        let values = state.factor_values(&plan).unwrap();

        assert!((values[0].unwrap() - 60.0 / 150.0).abs() < 1e-12);
        assert!((values[1].unwrap() - 15.0 / 25.0).abs() < 1e-12);
        assert!((values[2].unwrap() - 60.0 / 90.0).abs() < 1e-12);
        assert!((values[3].unwrap() - 15.0 / 10.0).abs() < 1e-12);
        assert_eq!(values[4], Some(5.0));
        let harmonic_bid = 5.0 / (1.0 / 10.0 + 1.0 / 20.0 + 1.0 / 30.0 + 1.0 / 40.0 + 1.0 / 50.0);
        assert!((values[5].unwrap() - harmonic_bid).abs() < 1e-12);
    }

    #[test]
    fn domestic_formula_values_preserve_source_trade_semantics() {
        let mut row = input(1_000, 20251103, 0, 100.0);
        let original = row.trade.values;
        let values = domestic_formula_values(&row);
        assert_eq!(values, original);

        for index in 6..=9 {
            row.trade.values[index] = f64::NAN;
        }
        for index in 14..=22 {
            row.trade.values[index] = f64::NAN;
        }
        for index in 29..=31 {
            row.trade.values[index] = f64::NAN;
        }
        let values = domestic_formula_values(&row);
        for index in [6, 7, 8, 9, 14, 15, 16, 17, 18, 19, 20, 21, 22, 29, 30, 31] {
            assert!(
                values[index].is_nan(),
                "field {} must not be proxied",
                FUTURES_TRADE_FIELD_NAMES[index]
            );
        }
    }

    #[test]
    fn crypto_factor_name_is_not_accepted() {
        let error =
            FuturesFactorPlan::from_factor_names(vec!["baseline_118".to_string()]).unwrap_err();
        assert!(error.to_string().contains("unsupported cn_features factor"));
    }

    #[test]
    fn segment_and_trading_day_changes_reset_formula_history() {
        let plan =
            FuturesFactorPlan::from_factor_names(vec!["cn_features_baseline_159".to_string()])
                .unwrap();
        let mut state = FuturesFusionState::default();
        state.push(input(1_000, 20251103, 0, 100.0)).unwrap();
        assert_eq!(state.factor_values(&plan).unwrap()[0], None);
        state.push(input(2_000, 20251103, 0, 101.0)).unwrap();
        assert!(state.factor_values(&plan).unwrap()[0].is_some());

        state.push(input(3_000, 20251104, 0, 102.0)).unwrap();
        assert_eq!(state.history_len(), 1);
        assert_eq!(state.factor_values(&plan).unwrap()[0], None);

        state.push(input(4_000, 20251104, 0, 103.0)).unwrap();
        assert!(state.factor_values(&plan).unwrap()[0].is_some());
        state
            .push(input(5_000, 20251104, QUALITY_SEGMENT_BREAK, 104.0))
            .unwrap();
        assert_eq!(state.history_len(), 1);
        assert_eq!(state.factor_values(&plan).unwrap()[0], None);
    }

    #[test]
    fn unverified_multiplier_hides_vwap_family_inputs() {
        let mut row = input(1_000, 20251103, 0, 100.0);
        row.volume_multiple_verified = false;
        let values = domestic_formula_values(&row);
        assert!(values[23].is_nan());
        assert!(values[24].is_nan());
        assert!(values[25].is_nan());
    }

    #[test]
    fn verified_zero_volume_preserves_compatibility_filled_vwap_inputs() {
        let mut row = input(1_000, 20251103, 0, 100.0);
        row.trade.values[4] = 0.0;
        row.trade.values[5] = 0.0;
        row.trade.values[23] = 99.5;
        row.trade.values[24] = 99.25;
        row.trade.values[25] = 99.75;

        let values = domestic_formula_values(&row);
        assert_eq!(values[23], 99.5);
        assert_eq!(values[24], 99.25);
        assert_eq!(values[25], 99.75);
    }
    #[test]
    fn every_cn_factor_has_a_computation_path() {
        let missing: Vec<&str> = CnFactorId::ALL
            .iter()
            .copied()
            .filter(|id| {
                let plan =
                    CnFormulaPlan::from_factor_names("coverage", vec![id.as_name().to_string()])
                        .unwrap();
                CnReplayState::validate_factor_plan(&plan).is_err()
            })
            .map(CnFactorId::as_name)
            .collect();
        assert!(missing.is_empty(), "missing CN computations: {missing:?}");
    }

    #[test]
    fn repaired_baselines_compute_directly_after_warmup() {
        let ids = [
            CnFactorId::Baseline023,
            CnFactorId::Baseline030,
            CnFactorId::Baseline033,
            CnFactorId::Baseline037,
            CnFactorId::Baseline041,
            CnFactorId::Baseline044,
            CnFactorId::Baseline048,
            CnFactorId::Baseline050,
            CnFactorId::Baseline056,
            CnFactorId::Baseline064,
            CnFactorId::Baseline075,
            CnFactorId::Baseline078,
            CnFactorId::Baseline084,
            CnFactorId::Baseline089,
            CnFactorId::Baseline094,
            CnFactorId::Baseline095,
            CnFactorId::Baseline097,
            CnFactorId::Baseline102,
            CnFactorId::Baseline106,
            CnFactorId::Baseline108,
            CnFactorId::Baseline130,
            CnFactorId::Baseline142,
            CnFactorId::Baseline144,
            CnFactorId::Baseline147,
            CnFactorId::Baseline150,
            CnFactorId::Baseline155,
            CnFactorId::Baseline165,
            CnFactorId::Baseline176,
            CnFactorId::Baseline183,
            CnFactorId::Baseline186,
            CnFactorId::Baseline197,
        ];
        let plan = FuturesFactorPlan::from_factor_names(
            ids.iter().copied().map(canonical_cn_name).collect(),
        )
        .unwrap();
        let mut state = FuturesFusionState::default();
        for index in 0..320 {
            state.push(varied_input(index)).unwrap();
        }
        let values = state.factor_values(&plan).unwrap();
        let missing: Vec<&str> = ids
            .iter()
            .zip(values.iter())
            .filter_map(|(id, value)| value.is_none().then_some(id.as_name()))
            .collect();
        assert!(
            missing.is_empty(),
            "warm factors returned NULL: {missing:?}"
        );
    }
}
