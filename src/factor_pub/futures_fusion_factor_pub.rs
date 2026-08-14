//! China-futures features with a native five-level order book.
//!
//! This module intentionally does not reuse the crypto fusion message, state,
//! factor identifiers, or formulas. New cn_features factors must be registered and
//! reviewed here even when a similarly named crypto factor exists.

use anyhow::{bail, Context, Result};
use std::collections::{HashSet, VecDeque};

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

pub const SUPPORTED_FUTURES_FACTORS: [&str; 3] = [
    "cn_features_book_mid_price",
    "cn_features_book_spread",
    "cn_features_book_imbalance_5",
];

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
        depth.validate_best_book()?;
        Ok(depth)
    }

    fn validate_best_book(&self) -> Result<()> {
        let best_bid = self.bid_prices[0];
        let best_ask = self.ask_prices[0];
        let best_bid_amount = self.bid_amounts[0];
        let best_ask_amount = self.ask_amounts[0];
        if !positive_finite(best_bid) {
            bail!("bid_prices[0] must be finite and positive, got {best_bid}");
        }
        if !positive_finite(best_ask) {
            bail!("ask_prices[0] must be finite and positive, got {best_ask}");
        }
        if !nonnegative_finite(best_bid_amount) {
            bail!("bid_amounts[0] must be finite and non-negative, got {best_bid_amount}");
        }
        if !nonnegative_finite(best_ask_amount) {
            bail!("ask_amounts[0] must be finite and non-negative, got {best_ask_amount}");
        }
        if best_bid > best_ask {
            bail!("crossed best book: bid={best_bid} ask={best_ask}");
        }
        Ok(())
    }

    fn mid_price(&self) -> Option<f64> {
        finite_value((self.bid_prices[0] + self.ask_prices[0]) / 2.0)
    }

    fn spread(&self) -> Option<f64> {
        finite_value(self.ask_prices[0] - self.bid_prices[0])
    }

    fn imbalance_5(&self) -> Option<f64> {
        let bid = complete_amount_sum(&self.bid_amounts)?;
        let ask = complete_amount_sum(&self.ask_amounts)?;
        let total = bid + ask;
        if total <= 0.0 {
            return None;
        }
        finite_value((bid - ask) / total)
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
    pub depth: FuturesDepth5,
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
        self.depth.validate_best_book()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FuturesFactorId {
    BookMidPrice,
    BookSpread,
    BookImbalance5,
}

impl FuturesFactorId {
    fn parse(name: &str) -> Result<Self> {
        match name {
            "cn_features_book_mid_price" => Ok(Self::BookMidPrice),
            "cn_features_book_spread" => Ok(Self::BookSpread),
            "cn_features_book_imbalance_5" => Ok(Self::BookImbalance5),
            _ => bail!(
                "unsupported cn_features factor {name}; supported factors: {}",
                SUPPORTED_FUTURES_FACTORS.join(", ")
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FuturesFactorPlan {
    names: Vec<String>,
    ids: Vec<FuturesFactorId>,
}

impl FuturesFactorPlan {
    pub fn from_factor_names(names: Vec<String>) -> Result<Self> {
        if names.is_empty() {
            bail!("at least one cn_features factor is required");
        }
        let mut seen = HashSet::with_capacity(names.len());
        let mut normalized = Vec::with_capacity(names.len());
        let mut ids = Vec::with_capacity(names.len());
        for raw_name in names {
            let name = raw_name.trim().to_string();
            if name.is_empty() {
                bail!("cn_features factor name must not be empty");
            }
            if !seen.insert(name.clone()) {
                bail!("duplicate cn_features factor: {name}");
            }
            ids.push(FuturesFactorId::parse(&name)?);
            normalized.push(name);
        }
        Ok(Self {
            names: normalized,
            ids,
        })
    }

    pub fn factor_names(&self) -> impl Iterator<Item = &str> {
        self.names.iter().map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }
}

#[derive(Debug, Default)]
pub struct FuturesFusionState {
    symbol: Option<String>,
    last_ts_ms: Option<i64>,
    history: VecDeque<FuturesFusionInput>,
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
        if input.quality_flags & QUALITY_SEGMENT_BREAK != 0 {
            self.history.clear();
        }
        self.last_ts_ms = Some(input.ts_ms);
        self.history.push_back(input);
        if self.history.len() > MAX_FUTURES_HISTORY {
            self.history.pop_front();
        }
        Ok(())
    }

    pub fn factor_values(&self, plan: &FuturesFactorPlan) -> Result<Vec<Option<f64>>> {
        let latest = self
            .history
            .back()
            .context("cannot compute futures factors before an input row is pushed")?;
        Ok(plan
            .ids
            .iter()
            .map(|factor| match factor {
                FuturesFactorId::BookMidPrice => latest.depth.mid_price(),
                FuturesFactorId::BookSpread => latest.depth.spread(),
                FuturesFactorId::BookImbalance5 => latest.depth.imbalance_5(),
            })
            .collect())
    }

    pub fn history_len(&self) -> usize {
        self.history.len()
    }
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

fn complete_amount_sum(values: &[f64; FUTURES_DEPTH_LEVELS]) -> Option<f64> {
    values
        .iter()
        .try_fold(0.0, |sum, value| {
            nonnegative_finite(*value).then_some(sum + value)
        })
        .and_then(finite_value)
}

fn finite_value(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
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

    fn input(ts_ms: i64, quality_flags: u32) -> FuturesFusionInput {
        FuturesFusionInput {
            ts_ms,
            symbol: "AP2601".to_string(),
            trading_day: 20251103,
            trade: FuturesTradeBar::from_slice(&[0.0; FUTURES_TRADE_FIELD_COUNT]).unwrap(),
            depth: depth(),
            quality_flags,
            volume_multiple: 1.0,
            volume_multiple_verified: false,
        }
    }

    #[test]
    fn rejects_any_non_five_depth_shape() {
        let error =
            FuturesDepth5::from_slices(&[100.0; 4], &[1.0; 4], &[101.0; 4], &[1.0; 4]).unwrap_err();
        assert!(error.to_string().contains("exactly 5 native levels"));

        let error =
            FuturesDepth5::from_slices(&[100.0; 6], &[1.0; 6], &[101.0; 6], &[1.0; 6]).unwrap_err();
        assert!(error.to_string().contains("exactly 5 native levels"));
    }

    #[test]
    fn computes_only_explicit_native_five_level_factors() {
        let plan = FuturesFactorPlan::from_factor_names(
            SUPPORTED_FUTURES_FACTORS
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        )
        .unwrap();
        let mut state = FuturesFusionState::default();
        state.push(input(1_000, 0)).unwrap();
        let values = state.factor_values(&plan).unwrap();
        assert_eq!(values[0], Some(100.5));
        assert_eq!(values[1], Some(1.0));
        assert!((values[2].unwrap() - 125.0 / 175.0).abs() < 1e-12);
    }

    #[test]
    fn missing_native_level_does_not_affect_level_one_factors() {
        let mut depth = depth();
        depth.bid_amounts[4] = f64::NAN;
        let mut row = input(1_000, 0);
        row.depth = depth;
        let plan = FuturesFactorPlan::from_factor_names(
            SUPPORTED_FUTURES_FACTORS
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        )
        .unwrap();
        let mut state = FuturesFusionState::default();
        state.push(row).unwrap();
        let values = state.factor_values(&plan).unwrap();
        assert_eq!(values[0], Some(100.5));
        assert_eq!(values[1], Some(1.0));
        assert_eq!(values[2], None);
    }

    #[test]
    fn crypto_factor_name_is_not_accepted() {
        let error =
            FuturesFactorPlan::from_factor_names(vec!["baseline_118".to_string()]).unwrap_err();
        assert!(error.to_string().contains("unsupported cn_features factor"));
    }

    #[test]
    fn segment_break_clears_only_futures_history() {
        let mut state = FuturesFusionState::default();
        state.push(input(1_000, 0)).unwrap();
        state.push(input(2_000, 0)).unwrap();
        assert_eq!(state.history_len(), 2);
        state.push(input(3_000, QUALITY_SEGMENT_BREAK)).unwrap();
        assert_eq!(state.history_len(), 1);
    }
}
