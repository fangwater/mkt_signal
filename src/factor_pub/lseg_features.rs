//! LSEG futures features with a native ten-level order book.
//!
//! Input, state, registry, formulas, replay, and output are independent from
//! crypto. Every formula is registered here against the native ten-level book.

// 引擎实现放在私有子模块，由专用 bin / replay 驱动；编其他 bin 时 rustc
// 会把这些项标成 dead_code。
#![allow(dead_code)]

#[path = "lseg_features/app.rs"]
mod app;
#[path = "lseg_features/baselines.rs"]
mod baselines;
#[path = "lseg_features/cfg.rs"]
mod cfg;
#[path = "lseg_features/factor_enum.rs"]
mod factor_enum;
#[path = "lseg_features/intermediates.rs"]
mod intermediates;
#[path = "lseg_features/math.rs"]
mod math;
#[path = "lseg_features/opv_factors.rs"]
mod opv_factors;
#[path = "lseg_features/plain_factors.rs"]
mod plain_factors;
#[path = "lseg_features/plan.rs"]
mod plan;
#[path = "lseg_features/publisher.rs"]
mod publisher;
#[path = "lseg_features/view.rs"]
mod view;
#[path = "lseg_features/zscore.rs"]
mod zscore;

use anyhow::{bail, Context, Result};
use std::collections::{HashSet, VecDeque};

use self::app::{LsegDepthLevel, LsegDepthSnapshot10, LsegReplayState};
use self::factor_enum::{LsegFactorId, LSEG_FACTOR_COUNT};
use self::plan::LsegFormulaPlan;

pub const LSEG_DEPTH_LEVELS: usize = 10;
pub const LSEG_TRADE_FIELD_COUNT: usize = 32;
pub const MAX_LSEG_HISTORY: usize = 4096;

pub const LSEG_TRADE_FIELD_NAMES: [&str; LSEG_TRADE_FIELD_COUNT] = [
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

pub const LSEG_FEATURE_FACTOR_COUNT: usize = LSEG_FACTOR_COUNT;
pub const LSEG_ALL_FACTORS: &str = "lseg_features_all";

#[derive(Debug, Clone, PartialEq)]
pub struct LsegDepth10 {
    pub bid_prices: [f64; LSEG_DEPTH_LEVELS],
    pub bid_amounts: [f64; LSEG_DEPTH_LEVELS],
    pub ask_prices: [f64; LSEG_DEPTH_LEVELS],
    pub ask_amounts: [f64; LSEG_DEPTH_LEVELS],
}

impl LsegDepth10 {
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
                if !value.is_nan() && !nonnegative_finite(value) {
                    bail!("{name}[{index}] must be NaN or finite and non-negative, got {value}");
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
pub struct LsegTradeBar {
    pub values: [f64; LSEG_TRADE_FIELD_COUNT],
}

impl LsegTradeBar {
    pub fn from_slice(values: &[f64]) -> Result<Self> {
        let values = values.try_into().map_err(|_| {
            anyhow::anyhow!(
                "futures trade input must contain exactly {LSEG_TRADE_FIELD_COUNT} fields, got {}",
                values.len()
            )
        })?;
        Ok(Self { values })
    }

    pub fn value(&self, name: &str) -> Option<f64> {
        LSEG_TRADE_FIELD_NAMES
            .iter()
            .position(|field| *field == name)
            .map(|index| self.values[index])
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LsegFusionInput {
    pub ts_ms: i64,
    pub symbol: String,
    pub trade: LsegTradeBar,
    pub depth: LsegDepth10,
    /// Reset before this row when the CME session has a non-contiguous minute.
    pub segment_break: bool,
}

impl LsegFusionInput {
    pub fn validate(&self) -> Result<()> {
        if self.symbol.is_empty()
            || !self
                .symbol
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'-'))
        {
            bail!("invalid futures symbol: {}", self.symbol);
        }
        self.depth.validate_levels()?;
        Ok(())
    }
}

const LSEG_FACTOR_PREFIX: &str = "lseg_features_";

#[derive(Debug, Clone)]
pub struct LsegFactorPlan {
    names: Vec<String>,
    formula_plan: LsegFormulaPlan,
}

impl LsegFactorPlan {
    pub fn from_factor_names(raw_names: Vec<String>) -> Result<Self> {
        if raw_names.is_empty() {
            bail!("at least one lseg_features factor is required");
        }
        let requested = if raw_names.len() == 1 && raw_names[0].trim() == LSEG_ALL_FACTORS {
            LsegFactorId::ALL
                .iter()
                .copied()
                .map(canonical_lseg_name)
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
                bail!("lseg_features factor name must not be empty");
            }
            let id = parse_lseg_factor_name(name)?;
            let canonical = canonical_lseg_name(id);
            if !seen.insert(canonical.clone()) {
                bail!("duplicate lseg_features factor: {canonical}");
            }
            names.push(canonical);
            source_names.push(id.as_name().to_string());
        }

        let formula_plan = LsegFormulaPlan::from_factor_names("lseg_features", source_names)?;
        LsegReplayState::validate_factor_plan(&formula_plan)
            .context("validate LSEG feature formula coverage")?;
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

fn canonical_lseg_name(id: LsegFactorId) -> String {
    format!("{LSEG_FACTOR_PREFIX}{}", id.as_name().to_ascii_lowercase())
}

fn parse_lseg_factor_name(name: &str) -> Result<LsegFactorId> {
    let suffix = name.strip_prefix(LSEG_FACTOR_PREFIX).ok_or_else(|| {
        anyhow::anyhow!(
            "unsupported lseg_features factor {name}; expected {LSEG_FACTOR_PREFIX}<legacy_factor_name>"
        )
    })?;
    LsegFactorId::ALL
        .iter()
        .copied()
        .find(|id| id.as_name().eq_ignore_ascii_case(suffix))
        .ok_or_else(|| anyhow::anyhow!("unsupported lseg_features factor {name}"))
}

#[derive(Default)]
pub struct LsegFeatureState {
    symbol: Option<String>,
    last_ts_ms: Option<i64>,
    history: VecDeque<LsegFusionInput>,
    formula_state: LsegReplayState,
}

impl LsegFeatureState {
    pub fn push(&mut self, input: LsegFusionInput) -> Result<()> {
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

        if input.segment_break {
            self.history.clear();
            self.formula_state = LsegReplayState::default();
        }

        let formula_values = lseg_formula_values(&input);
        let depth = native_depth_snapshot(&input.depth);
        self.formula_state
            .push_native(input.ts_ms, &formula_values, Some(depth))
            .context("push LSEG futures formula row")?;

        self.last_ts_ms = Some(input.ts_ms);
        self.history.push_back(input);
        if self.history.len() > MAX_LSEG_HISTORY {
            self.history.pop_front();
        }
        Ok(())
    }

    pub fn factor_values(&mut self, plan: &LsegFactorPlan) -> Result<Vec<Option<f64>>> {
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

fn lseg_formula_values(input: &LsegFusionInput) -> [f64; LSEG_TRADE_FIELD_COUNT] {
    input.trade.values
}

fn native_depth_snapshot(depth: &LsegDepth10) -> LsegDepthSnapshot10 {
    let bids = std::array::from_fn(|index| LsegDepthLevel {
        price: depth.bid_prices[index],
        amount: depth.bid_amounts[index],
    });
    let asks = std::array::from_fn(|index| LsegDepthLevel {
        price: depth.ask_prices[index],
        amount: depth.ask_amounts[index],
    });
    LsegDepthSnapshot10 { bids, asks }
}

fn exact_depth_array(name: &str, values: &[f64]) -> Result<[f64; LSEG_DEPTH_LEVELS]> {
    values.try_into().map_err(|_| {
        anyhow::anyhow!(
            "{name} must contain exactly {LSEG_DEPTH_LEVELS} native levels, got {}",
            values.len()
        )
    })
}

fn nonnegative_finite(value: f64) -> bool {
    value.is_finite() && value >= 0.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn depth() -> LsegDepth10 {
        let bid_prices: [f64; LSEG_DEPTH_LEVELS] =
            std::array::from_fn(|level| 100.0 - level as f64 * 0.1);
        let bid_amounts: [f64; LSEG_DEPTH_LEVELS] =
            std::array::from_fn(|level| 10.0 + level as f64);
        let ask_prices: [f64; LSEG_DEPTH_LEVELS] =
            std::array::from_fn(|level| 100.1 + level as f64 * 0.1);
        let ask_amounts: [f64; LSEG_DEPTH_LEVELS] =
            std::array::from_fn(|level| 12.0 + level as f64);
        LsegDepth10::from_slices(&bid_prices, &bid_amounts, &ask_prices, &ask_amounts).unwrap()
    }

    fn input(ts_ms: i64, segment_break: bool) -> LsegFusionInput {
        let mut values = [1.0; LSEG_TRADE_FIELD_COUNT];
        values[0] = 100.0;
        values[1] = 100.2;
        values[2] = 99.8;
        values[3] = 100.1;
        values[4] = 10.0;
        values[5] = 1_001.0;
        values[6] = 100.1;
        values[7] = 10.0;
        values[10] = 600.0;
        values[11] = 401.0;
        values[12] = 6.0;
        values[13] = 4.0;
        values[23] = 100.1;
        values[24] = 100.0;
        values[25] = 100.25;
        LsegFusionInput {
            ts_ms,
            symbol: "CME:ES:2024-03".to_string(),
            trade: LsegTradeBar { values },
            depth: depth(),
            segment_break,
        }
    }

    #[test]
    fn all_selector_expands_to_the_full_lseg_registry() {
        let plan = LsegFactorPlan::from_factor_names(vec![LSEG_ALL_FACTORS.to_string()]).unwrap();
        assert_eq!(plan.len(), LSEG_FACTOR_COUNT);
        assert_eq!(plan.len(), 632);
        assert!(plan
            .factor_names()
            .all(|name| name.starts_with(LSEG_FACTOR_PREFIX)));
    }

    #[test]
    fn ten_level_contract_id_and_full_factor_plan_are_accepted() {
        let plan = LsegFactorPlan::from_factor_names(vec![LSEG_ALL_FACTORS.to_string()]).unwrap();
        let mut state = LsegFeatureState::default();
        state.push(input(1_000, false)).unwrap();
        let values = state.factor_values(&plan).unwrap();
        assert_eq!(values.len(), 632);
    }

    #[test]
    fn native_ten_level_adaptation_keeps_top_five_concentration() {
        let plan = LsegFactorPlan::from_factor_names(vec!["lseg_features_factor_013".to_string()])
            .unwrap();
        let mut state = LsegFeatureState::default();
        state.push(input(1_000, false)).unwrap();
        let value = state.factor_values(&plan).unwrap()[0].unwrap();
        let top_five = 10.0 + 11.0 + 12.0 + 13.0 + 14.0;
        let full_ten = 145.0;
        assert!((value - top_five / full_ten).abs() < 1e-12);
    }

    #[test]
    fn session_gap_resets_lseg_formula_history() {
        let plan =
            LsegFactorPlan::from_factor_names(vec!["lseg_features_baseline_159".to_string()])
                .unwrap();
        let mut state = LsegFeatureState::default();
        state.push(input(1_000, false)).unwrap();
        assert_eq!(state.factor_values(&plan).unwrap()[0], None);
        state.push(input(61_000, false)).unwrap();
        assert!(state.factor_values(&plan).unwrap()[0].is_some());
        state.push(input(121_000, true)).unwrap();
        assert_eq!(state.history_len(), 1);
        assert_eq!(state.factor_values(&plan).unwrap()[0], None);
    }

    #[test]
    fn special_only_minute_remains_in_lseg_state() {
        let plan = LsegFactorPlan::from_factor_names(vec![LSEG_ALL_FACTORS.to_string()]).unwrap();
        let mut special_only = input(1_000, false);
        for index in [0, 1, 2, 3, 23, 24, 25, 28] {
            special_only.trade.values[index] = f64::NAN;
        }
        let mut state = LsegFeatureState::default();
        state.push(special_only).unwrap();
        assert_eq!(state.history_len(), 1);
        assert_eq!(state.factor_values(&plan).unwrap().len(), 632);
    }
}
