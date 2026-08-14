//! Fusion Factor 数据通路应用
//!
//! 订阅 trade_flow_feature（包含 trade + depth）并触发融合因子计算。
//! 当前先落地 factor_118，后续在同一框架上扩展全部因子。

#![allow(clippy::needless_range_loop)]

use super::baselines as baseline_engine;
use super::math::{
    pct_change_last, rolling_corr_last, rolling_kurt_last, rolling_mean_at_from_series,
    rolling_mean_last, rolling_mean_last_opt_from_series, rolling_mean_last_with_min_periods,
    rolling_mean_series, rolling_mean_series_opt, rolling_rank_last, rolling_skew_last,
    rolling_std_last, rolling_sum_at_from_series, rolling_sum_at_opt_from_series,
    rolling_sum_last_from_series, rolling_sum_last_opt_from_series,
    rolling_sum_last_with_min_periods, rolling_sum_series, rolling_sum_series_opt,
    tail_skew_last_opt,
};
use super::view::{CnSeries, F64SeriesView, OptF64SeriesView, SplitSlice};
use anyhow::{bail, Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use reqwest::Client;
use serde::Deserialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use super::cfg::{CnFactorEngineConfig, CnFactorVersion, TlenServerConfig};
use super::factor_enum::CnFactorId;
pub(crate) use super::plan::{
    load_symbol_factor_plans_from_tlen_server, CnExtraFactorId, CnFactorBinding, CnFormulaPlan,
};
use super::publisher::CnFactorPublisher;
use super::publisher::CN_FACTOR_PAYLOAD_MAX_BYTES;
use super::zscore::{
    load_optional_zscore_config_from_tlen_server, load_zscore_config_from_tlen_server,
    normalize_feature_values, SymbolNormState, ZscoreRuntimeConfig,
};
use crate::common::amount_threshold::is_online_amount_threshold;
use crate::common::msg_parser::parse_trade_flow_feature;
use crate::common::rolling_welford::RollingWelfordCovariance;
use mkt_parsers::msg::mkt_msg::FeatureMsg;
use mkt_parsers::msg::trade_flow_feature_msg::{
    TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_HISTORY_SIZE,
    TRADE_FLOW_FEATURE_MAX_BYTES, TRADE_FLOW_FEATURE_MSG_TYPE,
};
use order_common::TradingVenue;
use runtime_common::symbol_util::normalize_symbol_for_venue;

const IDLE_SLEEP_MICROS: u64 = 200;
const STATS_LOG_INTERVAL_SECS: u64 = 60;
const TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE: usize = 8192;
const TRADE_FLOW_MAX_SUBSCRIBERS: usize = 10;
pub const MAX_CN_HISTORY: usize = 4096;
pub const CN_DEPTH_LEVELS: usize = 5;
pub const CN_INNER_DEPTH_LEVELS: usize = 3;
const APPENDED_DEPTH_VALUES: usize = CN_DEPTH_LEVELS * 4;
const FACTOR_118_WINDOW: usize = 120;
const FACTOR_118_VWAP_LEVELS: usize = 5;
const SYMBOL_RELOAD_WARN_INTERVAL_SECS: u64 = 60;
const ROLLING_CORR_CLOSE_VOLUME_14_WINDOW: usize = 14;
const ROLLING_CORR_OPEN_VOLUME_300_WINDOW: usize = 300;
const ROLLING_CORR_MID_MIDVOL_300_WINDOW: usize = 300;
const FIELD_OPEN: usize = 0;
const FIELD_HIGH: usize = 1;
const FIELD_LOW: usize = 2;
const FIELD_CLOSE: usize = 3;
const FIELD_VOLUME: usize = 4;
const FIELD_AMOUNT: usize = 5;
const FIELD_AVG_AMOUNT: usize = 6;
const FIELD_COUNT: usize = 7;
const FIELD_BUY_COUNT: usize = 8;
const FIELD_SELL_COUNT: usize = 9;
const FIELD_BUY_AMOUNT: usize = 10;
const FIELD_SELL_AMOUNT: usize = 11;
const FIELD_BUY_VOLUME: usize = 12;
const FIELD_SELL_VOLUME: usize = 13;
const FIELD_LARGE_ORDER: usize = 14;
const FIELD_MEDIUM_ORDER: usize = 15;
const FIELD_SMALL_ORDER: usize = 16;
const FIELD_LARGE_BUY: usize = 17;
const FIELD_LARGE_SELL: usize = 18;
const FIELD_MEDIUM_BUY: usize = 19;
const FIELD_MEDIUM_SELL: usize = 20;
const FIELD_SMALL_BUY: usize = 21;
const FIELD_SMALL_SELL: usize = 22;
const FIELD_VWAP: usize = 23;
const FIELD_BUY_VWAP: usize = 24;
const FIELD_SELL_VWAP: usize = 25;
const FIELD_NET_BUY_AMOUNT: usize = 26;
const FIELD_NET_BUY_VOLUME: usize = 27;
const FIELD_NET_BUY_PCT: usize = 28;
const FIELD_NET_BUY_LARGE: usize = 29;
const FIELD_NET_BUY_MEDIUM: usize = 30;
const FIELD_NET_BUY_SMALL: usize = 31;
const FACTOR_160_LEVELS: [usize; CN_DEPTH_LEVELS] = [0, 1, 2, 3, 4];

#[derive(Debug, Clone, Copy)]
pub struct CnDepthLevel {
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Clone)]
pub struct CnDepthSnapshot5 {
    pub bids: [CnDepthLevel; CN_DEPTH_LEVELS],
    pub asks: [CnDepthLevel; CN_DEPTH_LEVELS],
}

#[derive(Debug, Clone)]
pub struct CnDepthStats5 {
    pub bids: [CnDepthLevel; CN_DEPTH_LEVELS],
    pub asks: [CnDepthLevel; CN_DEPTH_LEVELS],
    bid_amount_prefix: [f64; CN_DEPTH_LEVELS + 1],
    ask_amount_prefix: [f64; CN_DEPTH_LEVELS + 1],
    bid_price_prefix: [f64; CN_DEPTH_LEVELS + 1],
    ask_price_prefix: [f64; CN_DEPTH_LEVELS + 1],
    bid_pxv_prefix: [f64; CN_DEPTH_LEVELS + 1],
    ask_pxv_prefix: [f64; CN_DEPTH_LEVELS + 1],
}

impl CnDepthStats5 {
    pub fn from_snapshot(depth: &CnDepthSnapshot5) -> Self {
        let mut out = Self {
            bids: depth.bids,
            asks: depth.asks,
            bid_amount_prefix: [0.0; CN_DEPTH_LEVELS + 1],
            ask_amount_prefix: [0.0; CN_DEPTH_LEVELS + 1],
            bid_price_prefix: [0.0; CN_DEPTH_LEVELS + 1],
            ask_price_prefix: [0.0; CN_DEPTH_LEVELS + 1],
            bid_pxv_prefix: [0.0; CN_DEPTH_LEVELS + 1],
            ask_pxv_prefix: [0.0; CN_DEPTH_LEVELS + 1],
        };
        for i in 0..CN_DEPTH_LEVELS {
            let b = out.bids[i];
            let a = out.asks[i];
            out.bid_amount_prefix[i + 1] = strict_prefix_add(out.bid_amount_prefix[i], b.amount);
            out.ask_amount_prefix[i + 1] = strict_prefix_add(out.ask_amount_prefix[i], a.amount);
            out.bid_price_prefix[i + 1] = strict_prefix_add(out.bid_price_prefix[i], b.price);
            out.ask_price_prefix[i + 1] = strict_prefix_add(out.ask_price_prefix[i], a.price);
            out.bid_pxv_prefix[i + 1] =
                strict_prefix_add(out.bid_pxv_prefix[i], b.price * b.amount);
            out.ask_pxv_prefix[i + 1] =
                strict_prefix_add(out.ask_pxv_prefix[i], a.price * a.amount);
        }
        out
    }

    #[inline]
    fn clamp_limit(&self, limit: usize) -> usize {
        assert!(
            limit <= CN_DEPTH_LEVELS,
            "CN formula requested {limit} levels from a native {CN_DEPTH_LEVELS}-level book"
        );
        limit
    }

    #[inline]
    pub fn bid_amount(&self, idx: usize) -> f64 {
        self.bids.get(idx).map(|l| l.amount).unwrap_or(f64::NAN)
    }

    #[inline]
    pub fn ask_amount(&self, idx: usize) -> f64 {
        self.asks.get(idx).map(|l| l.amount).unwrap_or(f64::NAN)
    }

    #[inline]
    pub fn bid_price(&self, idx: usize) -> f64 {
        self.bids.get(idx).map(|l| l.price).unwrap_or(f64::NAN)
    }

    #[inline]
    pub fn ask_price(&self, idx: usize) -> f64 {
        self.asks.get(idx).map(|l| l.price).unwrap_or(f64::NAN)
    }

    #[inline]
    pub fn sum_bid_amount(&self, limit: usize) -> f64 {
        self.bid_amount_prefix[self.clamp_limit(limit)]
    }

    #[inline]
    pub fn sum_ask_amount(&self, limit: usize) -> f64 {
        self.ask_amount_prefix[self.clamp_limit(limit)]
    }

    #[inline]
    pub fn sum_bid_price(&self, limit: usize) -> f64 {
        self.bid_price_prefix[self.clamp_limit(limit)]
    }

    #[inline]
    pub fn sum_ask_price(&self, limit: usize) -> f64 {
        self.ask_price_prefix[self.clamp_limit(limit)]
    }

    #[inline]
    pub fn mean_bid_amount(&self, limit: usize) -> f64 {
        let l = self.clamp_limit(limit);
        if l == 0 {
            return f64::NAN;
        }
        self.sum_bid_amount(l) / l as f64
    }

    #[inline]
    pub fn mean_ask_amount(&self, limit: usize) -> f64 {
        let l = self.clamp_limit(limit);
        if l == 0 {
            return f64::NAN;
        }
        self.sum_ask_amount(l) / l as f64
    }

    #[inline]
    pub fn mean_bid_price(&self, limit: usize) -> f64 {
        let l = self.clamp_limit(limit);
        if l == 0 {
            return f64::NAN;
        }
        self.sum_bid_price(l) / l as f64
    }

    #[inline]
    pub fn mean_ask_price(&self, limit: usize) -> f64 {
        let l = self.clamp_limit(limit);
        if l == 0 {
            return f64::NAN;
        }
        self.sum_ask_price(l) / l as f64
    }

    #[inline]
    pub fn bid_vwap(&self, limit: usize) -> Option<f64> {
        let l = self.clamp_limit(limit);
        if l == 0 {
            return None;
        }
        let den = self.bid_amount_prefix[l];
        let num = self.bid_pxv_prefix[l];
        if !den.is_finite() || !num.is_finite() {
            return None;
        }
        if den.abs() <= 1e-12 {
            return None;
        }
        finite_opt(Some(num / den))
    }

    #[inline]
    pub fn ask_vwap(&self, limit: usize) -> Option<f64> {
        let l = self.clamp_limit(limit);
        if l == 0 {
            return None;
        }
        let den = self.ask_amount_prefix[l];
        let num = self.ask_pxv_prefix[l];
        if !den.is_finite() || !num.is_finite() {
            return None;
        }
        if den.abs() <= 1e-12 {
            return None;
        }
        finite_opt(Some(num / den))
    }

    #[inline]
    pub fn best_bid(&self) -> (f64, f64) {
        (self.bid_price(0), self.bid_amount(0))
    }

    #[inline]
    pub fn best_ask(&self) -> (f64, f64) {
        (self.ask_price(0), self.ask_amount(0))
    }
}

#[derive(Clone, Default)]
pub struct CnCalcState {
    pub factor_118_mid_price_diffs: VecDeque<f64>,
    pub open: VecDeque<f64>,
    pub high: VecDeque<f64>,
    pub low: VecDeque<f64>,
    pub close: VecDeque<f64>,
    pub volume: VecDeque<f64>,
    pub amount: VecDeque<f64>,
    pub avg_amount: VecDeque<f64>,
    pub count: VecDeque<f64>,
    pub trade_time: VecDeque<f64>,
    pub buy_count: VecDeque<f64>,
    pub sell_count: VecDeque<f64>,
    pub buy_amount: VecDeque<f64>,
    pub sell_amount: VecDeque<f64>,
    pub buy_volume: VecDeque<f64>,
    pub sell_volume: VecDeque<f64>,
    pub large_order: VecDeque<f64>,
    pub medium_order: VecDeque<f64>,
    pub large_buy: VecDeque<f64>,
    pub large_sell: VecDeque<f64>,
    pub medium_buy: VecDeque<f64>,
    pub medium_sell: VecDeque<f64>,
    pub small_order: VecDeque<f64>,
    pub small_buy: VecDeque<f64>,
    pub small_sell: VecDeque<f64>,
    pub vwap: VecDeque<f64>,
    pub buy_vwap: VecDeque<f64>,
    pub sell_vwap: VecDeque<f64>,
    pub net_buy_volume: VecDeque<f64>,
    pub net_buy_pct: VecDeque<f64>,
    pub net_buy_large: VecDeque<f64>,
    pub net_buy_medium: VecDeque<f64>,
    pub net_buy_small: VecDeque<f64>,
    pub net_buy_amount: VecDeque<f64>,
    pub bid0v: VecDeque<f64>,
    pub mid_price: VecDeque<f64>,
    pub spread: VecDeque<f64>,
    pub relative_spread: VecDeque<f64>,
    pub bid_vwap5: VecDeque<f64>,
    pub total_bid5: VecDeque<f64>,
    pub total_ask5: VecDeque<f64>,
    pub top3_bid_volume: VecDeque<f64>,
    pub top3_ask_volume: VecDeque<f64>,
    pub top3_bid_mean: VecDeque<f64>,
    pub top3_ask_mean: VecDeque<f64>,
    pub outer_bid_amount: VecDeque<f64>,
    pub outer_ask_amount: VecDeque<f64>,
    pub outer_bid_price: VecDeque<f64>,
    pub outer_ask_price: VecDeque<f64>,
    pub outer_level_volume: VecDeque<f64>,
    pub mean_bid_amount5: VecDeque<f64>,
    pub mean_bid_price3: VecDeque<f64>,
    pub mean_bid_price5: VecDeque<f64>,
    pub avg_ask_price3: VecDeque<f64>,
    pub mean_ask_price5: VecDeque<f64>,
    pub ask_pv5_mean: VecDeque<f64>,
    pub bid_pv5_mean: VecDeque<f64>,
    pub factor_031_ratio: VecDeque<f64>,
    pub factor_119_mid_minus_ask_vwap3: VecDeque<f64>,
    pub total_volume5_sum: VecDeque<f64>,
    pub median_all_price10: VecDeque<f64>,
    pub factor_152_prev_price_diff5: Option<[f64; CN_DEPTH_LEVELS]>,
    pub factor_152_pct_mean: VecDeque<Option<f64>>,
    pub ask_vwap5: VecDeque<f64>,
    pub ask_vwap_diff_3_5: VecDeque<f64>,
    pub ask_mean_amount5: VecDeque<f64>,
    pub ask0v: VecDeque<f64>,
    pub factor_113_bid_price_pct_hmean: VecDeque<Option<f64>>,
    pub factor_114_ask_price_pct_mean: VecDeque<Option<f64>>,
    pub factor_127_bid_price_kurt: VecDeque<f64>,
    pub factor_128_ask_price_sum: VecDeque<f64>,
    pub factor_128_diff_sum: VecDeque<Option<f64>>,
    pub factor_128_skew: VecDeque<Option<f64>>,
    pub factor_131_ask_price_kurt: VecDeque<f64>,
    pub factor_157_bid_ask_diff_std: VecDeque<Option<f64>>,
    pub factor_160_prev_ratios: Option<[f64; CN_DEPTH_LEVELS]>,
    pub factor_160_pct_change_mean: VecDeque<Option<f64>>,
    pub factor_157_prev_bid_amounts5: Option<[f64; CN_DEPTH_LEVELS]>,
    pub factor_157_prev_ask_amounts5: Option<[f64; CN_DEPTH_LEVELS]>,
    pub bid_price_level_hist5: [VecDeque<f64>; CN_DEPTH_LEVELS],
    pub ask_price_level_hist5: [VecDeque<f64>; CN_DEPTH_LEVELS],
    pub corr_close_volume_14_last: Option<f64>,
    pub corr_open_volume_300_last: Option<f64>,
    pub corr_mid_midvol_300_last: Option<f64>,
}

struct CnRollingStats {
    corr_close_volume_14: RollingWelfordCovariance,
    corr_open_volume_300: RollingWelfordCovariance,
    corr_mid_midvol_300: RollingWelfordCovariance,
}

impl Default for CnRollingStats {
    fn default() -> Self {
        Self {
            corr_close_volume_14: RollingWelfordCovariance::new(
                ROLLING_CORR_CLOSE_VOLUME_14_WINDOW,
            ),
            corr_open_volume_300: RollingWelfordCovariance::new(
                ROLLING_CORR_OPEN_VOLUME_300_WINDOW,
            ),
            corr_mid_midvol_300: RollingWelfordCovariance::new(ROLLING_CORR_MID_MIDVOL_300_WINDOW),
        }
    }
}

/// Stateful evaluator for the domestic-futures replay path.
#[derive(Default)]
pub struct CnReplayState {
    state: CnCalcState,
    rolling: CnRollingStats,
    latest_depth: Option<CnDepthStats5>,
}

impl CnReplayState {
    pub fn validate_factor_plan(plan: &CnFormulaPlan) -> Result<()> {
        let mut state = CnCalcState::default();
        let series = CnFactorEngine::build_symbol_series_from_state(&mut state);
        for binding in &plan.ordered_factors {
            if CnFactorEngine::compute_supported_factor(binding, None, None, Some(&series))
                .is_none()
            {
                bail!("factor has no fusion computation: {}", binding.name);
            }
        }
        Ok(())
    }

    pub fn push_native(
        &mut self,
        ts_ms: i64,
        values: &[f64; TRADE_FLOW_FEATURE_DIM],
        depth: Option<CnDepthSnapshot5>,
    ) -> Result<()> {
        let depth = depth.map(|snapshot| CnDepthStats5::from_snapshot(&snapshot));
        self.state.push_native_bar(ts_ms, values);
        self.state.push_optional_depth_stats(depth.as_ref());
        self.update_native_rolling(values, depth.as_ref());
        self.latest_depth = depth;
        Ok(())
    }

    fn update_native_rolling(
        &mut self,
        values: &[f64; TRADE_FLOW_FEATURE_DIM],
        depth: Option<&CnDepthStats5>,
    ) {
        let open = values[FIELD_OPEN];
        let close = values[FIELD_CLOSE];
        let volume = values[FIELD_VOLUME];
        self.rolling.corr_open_volume_300.push(open, volume);
        self.rolling.corr_close_volume_14.push(close, volume);
        self.state.corr_open_volume_300_last =
            finite_opt(self.rolling.corr_open_volume_300.corr_strict());
        self.state.corr_close_volume_14_last =
            finite_opt(self.rolling.corr_close_volume_14.corr_strict());

        if let Some(depth) = depth {
            let (bid0p, bid0v) = depth.best_bid();
            let (ask0p, ask0v) = depth.best_ask();
            self.rolling
                .corr_mid_midvol_300
                .push((ask0p + bid0p) / 2.0, (bid0v + ask0v) / 2.0);
            self.state.corr_mid_midvol_300_last =
                finite_opt(self.rolling.corr_mid_midvol_300.corr_strict());
        } else {
            self.rolling.corr_mid_midvol_300.push(f64::NAN, f64::NAN);
            self.state.corr_mid_midvol_300_last = None;
        }
    }

    pub fn baseline_values(&mut self, names: &[String]) -> Vec<f64> {
        let series = CnFactorEngine::build_symbol_series_from_state(&mut self.state);
        names
            .iter()
            .map(|name| baseline_engine::compute_baseline(name, &series).unwrap_or(f64::NAN))
            .collect()
    }

    /// Evaluate raw factor values with the same bindings and readiness rules as
    /// the live fusion publisher. Call exactly once after every pushed row,
    /// including warm-up rows, so stateful factors advance identically.
    pub fn factor_values(&mut self, plan: &CnFormulaPlan) -> Vec<f64> {
        let needs_factor_118 = plan
            .ordered_factors
            .iter()
            .any(|binding| binding.factor_id == Some(CnFactorId::Factor118));
        let factor_118_result = if needs_factor_118 {
            self.latest_depth.as_ref().and_then(|depth| {
                CnFactorEngine::compute_factor_118_with_state(&mut self.state, depth)
            })
        } else {
            None
        };
        let series = CnFactorEngine::build_symbol_series_from_state(&mut self.state);
        plan.ordered_factors
            .iter()
            .map(|binding| {
                match CnFactorEngine::compute_supported_factor(
                    binding,
                    factor_118_result,
                    self.latest_depth.as_ref(),
                    Some(&series),
                ) {
                    Some((value, true, _)) => value,
                    Some(_) | None => f64::NAN,
                }
            })
            .collect()
    }
}

impl CnCalcState {
    pub fn push_mid_price_diff(&mut self, value: f64) {
        self.factor_118_mid_price_diffs.push_back(value);
        if self.factor_118_mid_price_diffs.len() > MAX_CN_HISTORY {
            self.factor_118_mid_price_diffs.pop_front();
        }
    }

    pub fn push_trade_flow(&mut self, msg: &TradeFlowFeatureMsg) {
        let values: &[f64; TRADE_FLOW_FEATURE_DIM] = msg
            .values
            .get(..TRADE_FLOW_FEATURE_DIM)
            .expect("validated CN trade-flow width")
            .try_into()
            .expect("CN trade-flow prefix width");
        self.push_native_bar(msg.ts, values);
    }

    pub fn push_native_bar(&mut self, ts_ms: i64, values: &[f64; TRADE_FLOW_FEATURE_DIM]) {
        push_with_limit(&mut self.open, values[FIELD_OPEN]);
        push_with_limit(&mut self.high, values[FIELD_HIGH]);
        push_with_limit(&mut self.low, values[FIELD_LOW]);
        push_with_limit(&mut self.close, values[FIELD_CLOSE]);
        push_with_limit(&mut self.volume, values[FIELD_VOLUME]);
        push_with_limit(&mut self.amount, values[FIELD_AMOUNT]);
        push_with_limit(&mut self.avg_amount, values[FIELD_AVG_AMOUNT]);
        push_with_limit(&mut self.count, values[FIELD_COUNT]);
        push_with_limit(&mut self.trade_time, ts_ms as f64);
        push_with_limit(&mut self.buy_count, values[FIELD_BUY_COUNT]);
        push_with_limit(&mut self.sell_count, values[FIELD_SELL_COUNT]);
        push_with_limit(&mut self.buy_amount, values[FIELD_BUY_AMOUNT]);
        push_with_limit(&mut self.sell_amount, values[FIELD_SELL_AMOUNT]);
        push_with_limit(&mut self.buy_volume, values[FIELD_BUY_VOLUME]);
        push_with_limit(&mut self.sell_volume, values[FIELD_SELL_VOLUME]);
        push_with_limit(&mut self.large_order, values[FIELD_LARGE_ORDER]);
        push_with_limit(&mut self.medium_order, values[FIELD_MEDIUM_ORDER]);
        push_with_limit(&mut self.large_buy, values[FIELD_LARGE_BUY]);
        push_with_limit(&mut self.large_sell, values[FIELD_LARGE_SELL]);
        push_with_limit(&mut self.medium_buy, values[FIELD_MEDIUM_BUY]);
        push_with_limit(&mut self.medium_sell, values[FIELD_MEDIUM_SELL]);
        push_with_limit(&mut self.small_order, values[FIELD_SMALL_ORDER]);
        push_with_limit(&mut self.small_buy, values[FIELD_SMALL_BUY]);
        push_with_limit(&mut self.small_sell, values[FIELD_SMALL_SELL]);
        push_with_limit(&mut self.vwap, values[FIELD_VWAP]);
        push_with_limit(&mut self.buy_vwap, values[FIELD_BUY_VWAP]);
        push_with_limit(&mut self.sell_vwap, values[FIELD_SELL_VWAP]);
        push_with_limit(&mut self.net_buy_volume, values[FIELD_NET_BUY_VOLUME]);
        push_with_limit(&mut self.net_buy_pct, values[FIELD_NET_BUY_PCT]);
        push_with_limit(&mut self.net_buy_large, values[FIELD_NET_BUY_LARGE]);
        push_with_limit(&mut self.net_buy_medium, values[FIELD_NET_BUY_MEDIUM]);
        push_with_limit(&mut self.net_buy_small, values[FIELD_NET_BUY_SMALL]);
        push_with_limit(&mut self.net_buy_amount, values[FIELD_NET_BUY_AMOUNT]);
    }

    pub fn push_depth_metrics(&mut self, depth: &CnDepthSnapshot5) {
        let stats = CnDepthStats5::from_snapshot(depth);
        self.push_depth_stats(&stats);
    }

    fn push_optional_depth_stats(&mut self, depth: Option<&CnDepthStats5>) {
        match depth {
            Some(depth) => self.push_depth_stats(depth),
            None => self.push_missing_depth_metrics(),
        }
    }

    pub fn push_missing_depth_metrics(&mut self) {
        for values in [
            &mut self.bid0v,
            &mut self.mid_price,
            &mut self.spread,
            &mut self.relative_spread,
            &mut self.bid_vwap5,
            &mut self.total_bid5,
            &mut self.total_ask5,
            &mut self.top3_bid_volume,
            &mut self.top3_ask_volume,
            &mut self.top3_bid_mean,
            &mut self.top3_ask_mean,
            &mut self.outer_bid_amount,
            &mut self.outer_ask_amount,
            &mut self.outer_bid_price,
            &mut self.outer_ask_price,
            &mut self.outer_level_volume,
            &mut self.mean_bid_amount5,
            &mut self.mean_bid_price3,
            &mut self.mean_bid_price5,
            &mut self.avg_ask_price3,
            &mut self.mean_ask_price5,
            &mut self.ask_pv5_mean,
            &mut self.bid_pv5_mean,
            &mut self.factor_031_ratio,
            &mut self.factor_119_mid_minus_ask_vwap3,
            &mut self.total_volume5_sum,
            &mut self.median_all_price10,
            &mut self.ask_vwap5,
            &mut self.ask_vwap_diff_3_5,
            &mut self.ask_mean_amount5,
            &mut self.ask0v,
            &mut self.factor_127_bid_price_kurt,
            &mut self.factor_128_ask_price_sum,
            &mut self.factor_131_ask_price_kurt,
        ] {
            push_with_limit(values, f64::NAN);
        }
        for values in [
            &mut self.factor_152_pct_mean,
            &mut self.factor_113_bid_price_pct_hmean,
            &mut self.factor_114_ask_price_pct_mean,
            &mut self.factor_128_diff_sum,
            &mut self.factor_128_skew,
            &mut self.factor_157_bid_ask_diff_std,
            &mut self.factor_160_pct_change_mean,
        ] {
            push_opt_with_limit(values, Some(f64::NAN));
        }
        for values in &mut self.bid_price_level_hist5 {
            push_with_limit(values, f64::NAN);
        }
        for values in &mut self.ask_price_level_hist5 {
            push_with_limit(values, f64::NAN);
        }

        self.factor_118_mid_price_diffs.clear();
        self.factor_152_prev_price_diff5 = None;
        self.factor_157_prev_bid_amounts5 = None;
        self.factor_157_prev_ask_amounts5 = None;
        self.factor_160_prev_ratios = None;
    }

    fn push_depth_stats(&mut self, depth: &CnDepthStats5) {
        let (bid0p, bid0v) = depth.best_bid();
        let (ask0p, ask0v) = depth.best_ask();

        let spread = ask0p - bid0p;
        let mid = (ask0p + bid0p) / 2.0;
        push_with_limit(&mut self.bid0v, bid0v);
        push_with_limit(&mut self.mid_price, mid);
        push_with_limit(&mut self.spread, spread);
        let rel_spread = if mid.abs() > 1e-12 {
            spread / mid
        } else {
            f64::NAN
        };
        push_with_limit(&mut self.relative_spread, rel_spread);

        let top3_bid = depth.sum_bid_amount(3);
        let top3_ask = depth.sum_ask_amount(3);
        let total_bid5 = depth.sum_bid_amount(CN_DEPTH_LEVELS);
        let total_ask5 = depth.sum_ask_amount(CN_DEPTH_LEVELS);
        push_with_limit(&mut self.total_bid5, total_bid5);
        push_with_limit(&mut self.total_ask5, total_ask5);
        push_with_limit(&mut self.total_volume5_sum, total_bid5 + total_ask5);
        push_with_limit(&mut self.top3_bid_volume, top3_bid);
        push_with_limit(&mut self.top3_ask_volume, top3_ask);
        push_with_limit(&mut self.top3_bid_mean, top3_bid / 3.0);
        push_with_limit(&mut self.top3_ask_mean, top3_ask / 3.0);
        let outer = CN_DEPTH_LEVELS - 1;
        push_with_limit(&mut self.outer_bid_amount, depth.bid_amount(outer));
        push_with_limit(&mut self.outer_ask_amount, depth.ask_amount(outer));
        push_with_limit(&mut self.outer_bid_price, depth.bid_price(outer));
        push_with_limit(&mut self.outer_ask_price, depth.ask_price(outer));
        push_with_limit(
            &mut self.outer_level_volume,
            depth.bid_amount(outer) + depth.ask_amount(outer),
        );
        push_with_limit(
            &mut self.mean_bid_amount5,
            depth.mean_bid_amount(CN_DEPTH_LEVELS),
        );
        push_with_limit(&mut self.mean_bid_price3, depth.mean_bid_price(3));
        push_with_limit(
            &mut self.mean_bid_price5,
            depth.mean_bid_price(CN_DEPTH_LEVELS),
        );
        push_with_limit(&mut self.avg_ask_price3, depth.mean_ask_price(3));
        push_with_limit(
            &mut self.mean_ask_price5,
            depth.mean_ask_price(CN_DEPTH_LEVELS),
        );
        push_with_limit(
            &mut self.ask_pv5_mean,
            depth_mean_pxv(&depth.asks, CN_DEPTH_LEVELS),
        );
        push_with_limit(
            &mut self.bid_pv5_mean,
            depth_mean_pxv(&depth.bids, CN_DEPTH_LEVELS),
        );
        let ratio_031 = {
            let num = depth.mean_bid_price(CN_DEPTH_LEVELS);
            if mid.abs() > 1e-12 {
                num / mid
            } else {
                f64::NAN
            }
        };
        push_with_limit(&mut self.factor_031_ratio, ratio_031);
        push_with_limit(
            &mut self.median_all_price10,
            median_from_iter(
                depth
                    .bids
                    .iter()
                    .map(|level| level.price)
                    .chain(depth.asks.iter().map(|level| level.price)),
            )
            .unwrap_or(f64::NAN),
        );

        let ask_vwap3 = depth.ask_vwap(3);
        let ask_vwap5 = depth.ask_vwap(CN_DEPTH_LEVELS);
        let bid_vwap5 = depth.bid_vwap(CN_DEPTH_LEVELS);
        push_with_limit(&mut self.bid_vwap5, bid_vwap5.unwrap_or(f64::NAN));
        let ask_vwap_diff = match (ask_vwap3, ask_vwap5) {
            (Some(a), Some(b)) => a - b,
            _ => f64::NAN,
        };
        push_with_limit(&mut self.ask_vwap_diff_3_5, ask_vwap_diff);
        push_with_limit(&mut self.ask_vwap5, ask_vwap5.unwrap_or(f64::NAN));
        let factor_119_diff = ask_vwap3
            .map(|v| mid - v)
            .filter(|v| v.is_finite())
            .unwrap_or(f64::NAN);
        push_with_limit(&mut self.factor_119_mid_minus_ask_vwap3, factor_119_diff);

        let ask_mean5 = depth.mean_ask_amount(CN_DEPTH_LEVELS);
        push_with_limit(&mut self.ask_mean_amount5, ask_mean5);

        push_with_limit(&mut self.ask0v, ask0v);

        let bid_prices5: Vec<f64> = (0..CN_DEPTH_LEVELS).map(|i| depth.bid_price(i)).collect();
        let ask_prices5: Vec<f64> = (0..CN_DEPTH_LEVELS).map(|i| depth.ask_price(i)).collect();
        push_with_limit(
            &mut self.factor_127_bid_price_kurt,
            cross_sectional_kurtosis(&bid_prices5, true, false).unwrap_or(f64::NAN),
        );
        push_with_limit(
            &mut self.factor_131_ask_price_kurt,
            cross_sectional_kurtosis(&ask_prices5, true, false).unwrap_or(f64::NAN),
        );

        let curr_bid_amounts5: [f64; CN_DEPTH_LEVELS] =
            std::array::from_fn(|i| depth.bid_amount(i));
        let curr_ask_amounts5: [f64; CN_DEPTH_LEVELS] =
            std::array::from_fn(|i| depth.ask_amount(i));
        let factor_157_value = match (
            self.factor_157_prev_bid_amounts5,
            self.factor_157_prev_ask_amounts5,
        ) {
            (Some(prev_bid), Some(prev_ask)) => {
                let bid_diffs: Vec<f64> = curr_bid_amounts5
                    .iter()
                    .zip(prev_bid.iter())
                    .map(|(curr, prev)| curr - prev)
                    .collect();
                let ask_diffs: Vec<f64> = curr_ask_amounts5
                    .iter()
                    .zip(prev_ask.iter())
                    .map(|(curr, prev)| curr - prev)
                    .collect();
                match (
                    sample_std_last(&bid_diffs, bid_diffs.len(), 2),
                    sample_std_last(&ask_diffs, ask_diffs.len(), 2),
                ) {
                    (Some(b), Some(a)) => finite_opt(Some(b - a)),
                    _ => None,
                }
            }
            _ => None,
        };
        push_opt_with_limit(&mut self.factor_157_bid_ask_diff_std, factor_157_value);
        self.factor_157_prev_bid_amounts5 = Some(curr_bid_amounts5);
        self.factor_157_prev_ask_amounts5 = Some(curr_ask_amounts5);

        for i in 0..CN_DEPTH_LEVELS {
            push_with_limit(&mut self.bid_price_level_hist5[i], depth.bid_price(i));
            push_with_limit(&mut self.ask_price_level_hist5[i], depth.ask_price(i));
        }

        let bid_pct_hmean = {
            let mut pct = Vec::with_capacity(CN_DEPTH_LEVELS);
            for hist in &self.bid_price_level_hist5 {
                if hist.len() <= 30 {
                    pct.clear();
                    break;
                }
                let prev = hist[hist.len() - 31];
                let curr = hist[hist.len() - 1];
                if !prev.is_finite() || !curr.is_finite() || prev.abs() <= 1e-12 {
                    pct.clear();
                    break;
                }
                let value = (curr - prev) / prev;
                if !value.is_finite() || value.abs() <= 1e-12 {
                    pct.clear();
                    break;
                }
                pct.push(value);
            }
            harmonic_mean_nonzero(&pct)
        };
        push_opt_with_limit(&mut self.factor_113_bid_price_pct_hmean, bid_pct_hmean);

        let ask_pct_mean = {
            let mut pct = Vec::with_capacity(CN_DEPTH_LEVELS);
            for hist in &self.ask_price_level_hist5 {
                if hist.len() <= 30 {
                    pct.clear();
                    break;
                }
                let prev = hist[hist.len() - 31];
                let curr = hist[hist.len() - 1];
                if !prev.is_finite() || !curr.is_finite() || prev.abs() <= 1e-12 {
                    pct.clear();
                    break;
                }
                let value = (curr - prev) / prev;
                if !value.is_finite() {
                    pct.clear();
                    break;
                }
                pct.push(value);
            }
            if pct.len() == CN_DEPTH_LEVELS {
                Some(pct.iter().sum::<f64>() / pct.len() as f64)
            } else {
                None
            }
        };
        push_opt_with_limit(&mut self.factor_114_ask_price_pct_mean, ask_pct_mean);

        let ask_price_sum5 = depth.sum_ask_price(CN_DEPTH_LEVELS);
        let prev_sum = self.factor_128_ask_price_sum.back().copied();
        push_with_limit(&mut self.factor_128_ask_price_sum, ask_price_sum5);
        let curr_diff = prev_sum.map(|prev| ask_price_sum5 - prev);
        push_opt_with_limit(&mut self.factor_128_diff_sum, curr_diff);

        let diff_vec: Vec<Option<f64>> = self.factor_128_diff_sum.iter().copied().collect();
        let skew = tail_skew_last_opt(&diff_vec, 90, 30, false).ok().flatten();
        push_opt_with_limit(&mut self.factor_128_skew, skew);

        let mut curr_ratios = [f64::NAN; CN_DEPTH_LEVELS];
        for (k, level_idx) in FACTOR_160_LEVELS.iter().enumerate() {
            let bid = depth.bid_amount(*level_idx);
            let ask = depth.ask_amount(*level_idx);
            let den = bid + ask;
            curr_ratios[k] = if den.abs() > 1e-12 {
                bid / den
            } else {
                f64::NAN
            };
        }
        let pct_change_mean = self.factor_160_prev_ratios.and_then(|prev| {
            let mut vals = Vec::with_capacity(CN_DEPTH_LEVELS);
            for i in 0..CN_DEPTH_LEVELS {
                let p = prev[i];
                let c = curr_ratios[i];
                if !p.is_finite() || !c.is_finite() || p.abs() <= 1e-12 {
                    return None;
                }
                let v = (c - p) / p;
                vals.push(finite_opt(Some(v))?);
            }
            finite_opt(Some(vals.iter().sum::<f64>() / CN_DEPTH_LEVELS as f64))
        });
        self.factor_160_prev_ratios = Some(curr_ratios);
        push_opt_with_limit(&mut self.factor_160_pct_change_mean, pct_change_mean);

        let mut curr_diff = [f64::NAN; CN_DEPTH_LEVELS];
        for (i, val) in curr_diff.iter_mut().enumerate() {
            let bidp = depth.bid_price(i);
            let askp = depth.ask_price(i);
            *val = bidp - askp;
        }
        let pct_mean = self.factor_152_prev_price_diff5.and_then(|prev| {
            let mut vals = Vec::with_capacity(CN_DEPTH_LEVELS);
            for i in 0..CN_DEPTH_LEVELS {
                let p = prev[i];
                let c = curr_diff[i];
                if !p.is_finite() || !c.is_finite() || p.abs() <= 1e-12 {
                    return None;
                }
                let v = (c - p) / p;
                vals.push(finite_opt(Some(v))?);
            }
            finite_opt(Some(vals.iter().sum::<f64>() / CN_DEPTH_LEVELS as f64))
        });
        self.factor_152_prev_price_diff5 = Some(curr_diff);
        push_opt_with_limit(&mut self.factor_152_pct_mean, pct_mean);
    }
}

#[derive(Default)]
struct OrderedEvalStats {
    factor_plan_count: u64,
    factor_evaluated_count: u64,
    factor_ready_count: u64,
    factor_warming_up_count: u64,
    factor_invalid_value_count: u64,
    factor_missing_depth_count: u64,
    factor_unsupported_count: u64,
    factor118_ready_count: u64,
    factor_nan_fill_count: u64,
}

struct OrderedEvalResult {
    stats: OrderedEvalStats,
    factor_issues: Vec<String>,
    factor_values: Vec<f64>,
    status: u8,
}

#[derive(Debug, Clone)]
struct ReplayEvalSummary;

impl ReplayEvalSummary {
    fn from_eval(_eval_result: &OrderedEvalResult, _mark_reload: bool) -> Self {
        Self
    }
}
pub struct CnFactorEngine {
    venue_slug: String,
    venue: TradingVenue,
    version: CnFactorVersion,
    trade_flow_subscriber: Subscriber<ipc::Service, [u8; TRADE_FLOW_FEATURE_MAX_BYTES], ()>,
    tlen_server: Option<TlenServerConfig>,
    publisher: Option<CnFactorPublisher>,
    zscore_config: ZscoreRuntimeConfig,
    allowed_symbols: HashSet<String>,
    symbol_all_ready_seen: HashSet<String>,
    symbol_factor_plans: HashMap<String, CnFormulaPlan>,
    symbol_states: HashMap<String, CnCalcState>,
    symbol_rolling_stats: HashMap<String, CnRollingStats>,
    symbol_norm_states: HashMap<String, SymbolNormState>,
    depth_attached_count: u64,
    trade_flow_raw_count: u64,
    trade_flow_count: u64,
    trigger_count: u64,
    missing_depth_count: u64,
    factor_118_ready_count: u64,
    trade_flow_dropped_symbol_count: u64,
    trade_flow_dropped_symbol_samples: Vec<String>,
    trade_flow_decode_error_count: u64,
    trade_flow_decode_error_last: Option<String>,
    factor_plan_count: u64,
    factor_evaluated_count: u64,
    factor_ready_count: u64,
    factor_warming_up_count: u64,
    factor_invalid_value_count: u64,
    factor_missing_depth_count: u64,
    factor_unsupported_count: u64,
    published_count: u64,
    publish_failed_count: u64,
    last_symbol_reload: Instant,
    symbol_reload_interval: Option<Duration>,
    last_symbol_reload_warn: Instant,
    last_stats_log: Instant,
}

impl CnFactorEngine {
    pub async fn new(config_path: &str, venue: TradingVenue) -> Result<Self> {
        Self::new_for_version(config_path, venue, CnFactorVersion::Default)
            .await?
            .context("default fusion factor pipeline must be configured")
    }

    pub async fn new_one_minute(config_path: &str, venue: TradingVenue) -> Result<Self> {
        Self::try_new_one_minute(config_path, venue)
            .await?
            .context("1-minute fusion factor configs are incomplete")
    }

    pub async fn try_new_one_minute(
        config_path: &str,
        venue: TradingVenue,
    ) -> Result<Option<Self>> {
        Self::new_for_version(config_path, venue, CnFactorVersion::OneMinute).await
    }

    async fn new_for_version(
        config_path: &str,
        venue: TradingVenue,
        version: CnFactorVersion,
    ) -> Result<Option<Self>> {
        let cfg = CnFactorEngineConfig::load(config_path)?;
        let venue_slug = venue.data_pub_slug().to_string();
        let tlen_server = cfg.tlen_server.clone();
        let allowed_symbols: Vec<String> = load_online_symbols_from_tlen_server(
            &cfg.tlen_server,
            venue,
            &venue_slug,
            version.amount_threshold_config_type(),
        )
        .await
        .with_context(|| {
            format!(
                "load online symbols from tlen_server failed: venue={} version={}",
                venue_slug,
                version.label()
            )
        })?
        .into_iter()
        .collect();
        if allowed_symbols.is_empty() {
            if version == CnFactorVersion::OneMinute {
                info!(
                    "CnFactorEngine[{}:1m] disabled: config_type={} has no online symbols",
                    venue_slug,
                    version.amount_threshold_config_type()
                );
                return Ok(None);
            }
            anyhow::bail!(
                "tlen_server returned no online symbols: venue={} version={} config_type={}",
                venue_slug,
                version.label(),
                version.amount_threshold_config_type()
            );
        }

        let symbol_factor_plans = load_symbol_factor_plans_from_tlen_server(
            &cfg.tlen_server,
            &venue_slug,
            version.factor_plan_config_type(),
        )
        .await
        .with_context(|| {
            format!(
                "load symbol factor plans from tlen_server failed: venue={} version={}",
                venue_slug,
                version.label()
            )
        })?;
        if version == CnFactorVersion::OneMinute && symbol_factor_plans.is_empty() {
            info!(
                "CnFactorEngine[{}:1m] disabled: config_type={} is empty",
                venue_slug,
                version.factor_plan_config_type()
            );
            return Ok(None);
        }
        validate_symbol_factor_plan_payload_limits(&symbol_factor_plans)?;

        let zscore_config = if version == CnFactorVersion::OneMinute {
            match load_optional_zscore_config_from_tlen_server(
                &cfg.tlen_server,
                &venue_slug,
                version.zscore_config_type(),
            )
            .await
            .with_context(|| {
                format!(
                    "load zscore config from tlen_server failed: venue={} version={}",
                    venue_slug,
                    version.label()
                )
            })? {
                Some(config) => config,
                None => {
                    info!(
                        "CnFactorEngine[{}:1m] disabled: config_type={} is empty",
                        venue_slug,
                        version.zscore_config_type()
                    );
                    return Ok(None);
                }
            }
        } else {
            load_zscore_config_from_tlen_server(
                &cfg.tlen_server,
                &venue_slug,
                version.zscore_config_type(),
            )
            .await
            .with_context(|| {
                format!(
                    "load zscore config from tlen_server failed: venue={} version={}",
                    venue_slug,
                    version.label()
                )
            })?
        };

        let trade_flow_subscriber = Self::create_trade_flow_subscriber(&venue_slug, version)?;
        let output_service_path = version.output_service_path(&venue_slug);
        let publisher_node_name = format!(
            "fusion_pub_{}{}",
            venue_slug.replace('-', "_"),
            version.node_suffix()
        );
        let publisher = CnFactorPublisher::new(&publisher_node_name, &output_service_path)
            .with_context(|| {
                format!(
                    "create fusion factor publisher failed: service_path={}",
                    output_service_path
                )
            })?;

        info!(
            "CnFactorEngine created: venue={} version={} symbols={} sample={} factor_plan_config={} zscore_config={} symbol_config={} trade_flow_channel=factor_pub/{}/{} output_service={}",
            venue_slug,
            version.label(),
            allowed_symbols.len(),
            format_symbol_sample(&allowed_symbols),
            version.factor_plan_config_type(),
            version.zscore_config_type(),
            version.amount_threshold_config_type(),
            venue_slug,
            version.trade_flow_channel(),
            output_service_path,
        );
        info!(
            "CnFactorEngine[{}:{}] symbol factor plans loaded: symbols={}",
            venue_slug,
            version.label(),
            symbol_factor_plans.len(),
        );
        info!(
            "CnFactorEngine[{}:{}] zscore config: window_size={} min_samples={} zscore_cap={}",
            venue_slug,
            version.label(),
            zscore_config.window_size,
            zscore_config.min_samples,
            zscore_config.zscore_cap,
        );

        Ok(Some(Self {
            venue_slug,
            venue,
            version,
            trade_flow_subscriber,
            tlen_server: Some(tlen_server.clone()),
            publisher: Some(publisher),
            zscore_config,
            allowed_symbols: allowed_symbols.into_iter().collect(),
            symbol_all_ready_seen: HashSet::new(),
            symbol_factor_plans,
            symbol_states: HashMap::new(),
            symbol_rolling_stats: HashMap::new(),
            symbol_norm_states: HashMap::new(),
            depth_attached_count: 0,
            trade_flow_raw_count: 0,
            trade_flow_count: 0,
            trigger_count: 0,
            missing_depth_count: 0,
            factor_118_ready_count: 0,
            trade_flow_dropped_symbol_count: 0,
            trade_flow_dropped_symbol_samples: Vec::new(),
            trade_flow_decode_error_count: 0,
            trade_flow_decode_error_last: None,
            factor_plan_count: 0,
            factor_evaluated_count: 0,
            factor_ready_count: 0,
            factor_warming_up_count: 0,
            factor_invalid_value_count: 0,
            factor_missing_depth_count: 0,
            factor_unsupported_count: 0,
            published_count: 0,
            publish_failed_count: 0,
            last_symbol_reload: Instant::now(),
            symbol_reload_interval: Some(Duration::from_secs(tlen_server.symbol_reload_secs)),
            last_symbol_reload_warn: Instant::now()
                - Duration::from_secs(SYMBOL_RELOAD_WARN_INTERVAL_SECS),
            last_stats_log: Instant::now(),
        }))
    }

    fn create_trade_flow_subscriber(
        venue: &str,
        version: CnFactorVersion,
    ) -> Result<Subscriber<ipc::Service, [u8; TRADE_FLOW_FEATURE_MAX_BYTES], ()>> {
        let node_name = format!(
            "fusion_sub_{}_trade_flow{}",
            venue.replace('-', "_"),
            version.node_suffix()
        );
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_name = format!("factor_pub/{}/{}", venue, version.trade_flow_channel());
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; TRADE_FLOW_FEATURE_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(TRADE_FLOW_MAX_SUBSCRIBERS)
            .subscriber_max_buffer_size(TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE)
            .history_size(TRADE_FLOW_FEATURE_HISTORY_SIZE)
            .open_or_create()?;
        let service_max_buffer = service.static_config().subscriber_max_buffer_size();
        let service_history = service.static_config().history_size();
        if service_max_buffer < TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE {
            bail!(
                "trade_flow service buffer is too small: service={} service_subscriber_max_buffer_size={} required_min_buffer_size={} history_size={} hint=restart producer with larger subscriber_max_buffer_size and clean stale iceoryx service",
                service_name,
                service_max_buffer,
                TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE,
                service_history
            );
        }
        let requested_buffer = TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE;
        let subscriber = service
            .subscriber_builder()
            .buffer_size(requested_buffer)
            .create()?;

        info!(
            "Subscribed to trade_flow channel: {} subscriber_buffer_size={} service_subscriber_max_buffer_size={} service_history_size={}",
            service_name,
            requested_buffer,
            service_max_buffer,
            service_history
        );
        Ok(subscriber)
    }

    async fn maybe_reload_symbols(&mut self) {
        let Some(interval) = self.symbol_reload_interval else {
            return;
        };
        if self.last_symbol_reload.elapsed() < interval {
            return;
        }
        self.last_symbol_reload = Instant::now();

        let Some(tlen_server) = self.tlen_server.clone() else {
            return;
        };
        match load_zscore_config_from_tlen_server(
            &tlen_server,
            &self.venue_slug,
            self.version.zscore_config_type(),
        )
        .await
        {
            Ok(zscore_config) => {
                self.apply_zscore_config_update(zscore_config);
            }
            Err(err) => {
                self.warn_symbol_reload_throttled(&format!(
                    "fusion zscore config reload failed: venue={} err={:#}",
                    self.venue_slug, err
                ));
            }
        }
        match load_online_symbols_from_tlen_server(
            &tlen_server,
            self.venue,
            &self.venue_slug,
            self.version.amount_threshold_config_type(),
        )
        .await
        {
            Ok(symbols) => {
                if let Err(err) = self.apply_allowed_symbol_update(symbols).await {
                    self.warn_symbol_reload_throttled(&format!(
                        "fusion online symbol reload apply failed: venue={} err={:#}",
                        self.venue_slug, err
                    ));
                }
            }
            Err(err) => {
                self.warn_symbol_reload_throttled(&format!(
                    "fusion online symbol reload failed: venue={} err={:#}",
                    self.venue_slug, err
                ));
            }
        }
    }

    fn apply_zscore_config_update(&mut self, new_config: ZscoreRuntimeConfig) {
        if self.zscore_config == new_config {
            return;
        }

        let old_config = self.zscore_config.clone();
        let cleared_symbols = self.symbol_norm_states.len();
        self.zscore_config = new_config.clone();
        self.symbol_norm_states.clear();

        info!(
            "CnFactorEngine[{}] zscore config reloaded: old={{window_size:{} min_samples:{} zscore_cap:{}}} new={{window_size:{} min_samples:{} zscore_cap:{}}} cleared_norm_symbols={}",
            self.venue_slug,
            old_config.window_size,
            old_config.min_samples,
            old_config.zscore_cap,
            new_config.window_size,
            new_config.min_samples,
            new_config.zscore_cap,
            cleared_symbols,
        );
    }

    async fn apply_allowed_symbol_update(&mut self, new_allowed: HashSet<String>) -> Result<()> {
        if new_allowed.is_empty() {
            anyhow::bail!("online symbol reload produced empty set");
        }
        if new_allowed == self.allowed_symbols {
            return Ok(());
        }

        let retired: HashSet<String> = self
            .allowed_symbols
            .difference(&new_allowed)
            .cloned()
            .collect();

        self.allowed_symbols = new_allowed;
        self.symbol_states
            .retain(|symbol, _| self.allowed_symbols.contains(symbol));
        self.symbol_rolling_stats
            .retain(|symbol, _| self.allowed_symbols.contains(symbol));
        self.symbol_norm_states
            .retain(|symbol, _| self.allowed_symbols.contains(symbol));
        self.symbol_all_ready_seen
            .retain(|symbol| self.allowed_symbols.contains(symbol));

        info!(
            "CnFactorEngine[{}] online symbols reloaded: online_symbols={} sample={} retired_symbols={} retired_sample={}",
            self.venue_slug,
            self.allowed_symbols.len(),
            format_symbol_sample_set(&self.allowed_symbols),
            retired.len(),
            format_symbol_sample_set(&retired),
        );
        Ok(())
    }

    fn warn_symbol_reload_throttled(&mut self, msg: &str) {
        if self.last_symbol_reload_warn.elapsed()
            >= Duration::from_secs(SYMBOL_RELOAD_WARN_INTERVAL_SECS)
        {
            warn!("{}", msg);
            self.last_symbol_reload_warn = Instant::now();
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        self.prepare_run()?;

        loop {
            self.maybe_reload_symbols().await;
            let has_message = self.poll_trade_flow()?;
            self.maybe_log_stats();

            if !has_message {
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }
        }
    }

    fn prepare_run(&mut self) -> Result<()> {
        let mut drained = 0u64;
        while self.trade_flow_subscriber.receive()?.is_some() {
            drained += 1;
        }
        info!(
            "CnFactorEngine[{}:{}] started: symbols={} sample={} drained_stale={}",
            self.venue_slug,
            self.version.label(),
            self.allowed_symbols.len(),
            format_symbol_sample_set(&self.allowed_symbols),
            drained,
        );
        Ok(())
    }

    fn poll_trade_flow(&mut self) -> Result<bool> {
        let mut has_message = false;
        while let Some(sample) = self.trade_flow_subscriber.receive()? {
            has_message = true;
            self.trade_flow_raw_count = self.trade_flow_raw_count.saturating_add(1);
            let payload = sample.payload();
            let symbol_raw = match parse_trade_flow_symbol(payload) {
                Ok(v) => v,
                Err(err) => {
                    self.trade_flow_decode_error_count =
                        self.trade_flow_decode_error_count.saturating_add(1);
                    self.trade_flow_decode_error_last =
                        Some(format!("symbol pre-parse failed: {}", err));
                    warn!(
                        "trade_flow pre-parse failed: venue={} version={} err={}",
                        self.venue_slug,
                        self.version.label(),
                        err
                    );
                    continue;
                }
            };
            let symbol = normalize_symbol_for_venue(symbol_raw, self.venue);
            if !self.allowed_symbols.contains(&symbol) {
                self.trade_flow_dropped_symbol_count =
                    self.trade_flow_dropped_symbol_count.saturating_add(1);
                self.record_dropped_symbol_sample(&symbol);
                continue;
            }

            match parse_trade_flow_feature(payload) {
                Ok(msg) => {
                    self.on_trade_flow(symbol, msg);
                }
                Err(err) => {
                    self.trade_flow_decode_error_count =
                        self.trade_flow_decode_error_count.saturating_add(1);
                    self.trade_flow_decode_error_last = Some(err.to_string());
                    warn!(
                        "trade_flow decode failed: venue={} version={} symbol={} err={}",
                        self.venue_slug,
                        self.version.label(),
                        symbol,
                        err
                    );
                }
            }
        }
        Ok(has_message)
    }

    fn maybe_log_stats(&mut self) {
        if self.last_stats_log.elapsed() < Duration::from_secs(STATS_LOG_INTERVAL_SECS) {
            return;
        }

        let (pub_total, pub_dropped) = self
            .publisher
            .as_ref()
            .map(|publisher| (publisher.published_count(), publisher.dropped_count()))
            .unwrap_or((0, 0));
        info!(
            "CnFactorEngine[{}:{}] stats: raw_msgs={} trade_flow_msgs={} triggers={} decode_errors={} depth_attached={} missing_depth={} factor118_ready={} trade_drop_symbols={} drop_symbol_samples={:?} calc_symbols={} factor_plan={} factor_eval={} factor_ready={} factor_warming_up={} factor_invalid={} factor_missing_depth={} published={} publish_failed={} pub_total={} pub_dropped={} last_decode_error={}",
            self.venue_slug,
            self.version.label(),
            self.trade_flow_raw_count,
            self.trade_flow_count,
            self.trigger_count,
            self.trade_flow_decode_error_count,
            self.depth_attached_count,
            self.missing_depth_count,
            self.factor_118_ready_count,
            self.trade_flow_dropped_symbol_count,
            self.trade_flow_dropped_symbol_samples,
            self.symbol_states.len(),
            self.factor_plan_count,
            self.factor_evaluated_count,
            self.factor_ready_count,
            self.factor_warming_up_count,
            self.factor_invalid_value_count,
            self.factor_missing_depth_count,
            self.published_count,
            self.publish_failed_count,
            pub_total,
            pub_dropped,
            self.trade_flow_decode_error_last.as_deref().unwrap_or("-"),
        );
        self.trade_flow_raw_count = 0;
        self.trade_flow_count = 0;
        self.trigger_count = 0;
        self.trade_flow_decode_error_count = 0;
        self.trade_flow_decode_error_last = None;
        self.depth_attached_count = 0;
        self.missing_depth_count = 0;
        self.factor_118_ready_count = 0;
        self.trade_flow_dropped_symbol_count = 0;
        self.trade_flow_dropped_symbol_samples.clear();
        self.factor_plan_count = 0;
        self.factor_evaluated_count = 0;
        self.factor_ready_count = 0;
        self.factor_warming_up_count = 0;
        self.factor_invalid_value_count = 0;
        self.factor_missing_depth_count = 0;
        self.factor_unsupported_count = 0;
        self.published_count = 0;
        self.publish_failed_count = 0;
        self.last_stats_log = Instant::now();
    }

    fn validate_trade_flow_shape(
        venue_slug: &str,
        symbol: &str,
        msg: &TradeFlowFeatureMsg,
    ) -> Result<(), String> {
        let width = msg.values.len();
        if width != TRADE_FLOW_FEATURE_DIM
            && width != TRADE_FLOW_FEATURE_DIM + APPENDED_DEPTH_VALUES
        {
            return Err(format!(
                "cn_features input rejected: venue={venue_slug} symbol={symbol} ts={} expected {TRADE_FLOW_FEATURE_DIM} trade fields with either no book or exactly {CN_DEPTH_LEVELS} native levels, got {width} values",
                msg.ts
            ));
        }
        if let Some(depth) = parse_embedded_depth(msg) {
            validate_native_depth(&depth)?;
        }
        Ok(())
    }

    fn update_symbol_rolling_stats(
        state: &mut CnCalcState,
        rolling: &mut CnRollingStats,
        msg: &TradeFlowFeatureMsg,
        depth: Option<&CnDepthStats5>,
    ) {
        let open = msg.values[FIELD_OPEN];
        let close = msg.values[FIELD_CLOSE];
        let volume = msg.values[FIELD_VOLUME];

        rolling.corr_open_volume_300.push(open, volume);
        rolling.corr_close_volume_14.push(close, volume);
        state.corr_open_volume_300_last = finite_opt(rolling.corr_open_volume_300.corr_strict());
        state.corr_close_volume_14_last = finite_opt(rolling.corr_close_volume_14.corr_strict());

        if let Some(depth) = depth {
            let (bid0p, bid0v) = depth.best_bid();
            let (ask0p, ask0v) = depth.best_ask();
            let mid = (ask0p + bid0p) / 2.0;
            let mid_vol = (bid0v + ask0v) / 2.0;
            rolling.corr_mid_midvol_300.push(mid, mid_vol);
            state.corr_mid_midvol_300_last = finite_opt(rolling.corr_mid_midvol_300.corr_strict());
        } else {
            rolling.corr_mid_midvol_300.push(f64::NAN, f64::NAN);
            state.corr_mid_midvol_300_last = None;
        }
    }

    fn on_trade_flow(
        &mut self,
        symbol: String,
        msg: mkt_parsers::msg::trade_flow_feature_msg::TradeFlowFeatureMsg,
    ) {
        let _ = self.apply_trade_flow_msg(symbol, msg, true);
    }

    fn apply_trade_flow_msg(
        &mut self,
        symbol: String,
        msg: mkt_parsers::msg::trade_flow_feature_msg::TradeFlowFeatureMsg,
        emit_output: bool,
    ) -> Option<ReplayEvalSummary> {
        match Self::validate_trade_flow_shape(&self.venue_slug, &symbol, &msg) {
            Ok(()) => {}
            Err(err) => {
                warn!("{err}");
                return None;
            }
        }

        if emit_output {
            self.trade_flow_count = self.trade_flow_count.saturating_add(1);
            self.trigger_count = self.trigger_count.saturating_add(1);
        }

        let depth_stats = parse_embedded_depth(&msg)
            .as_ref()
            .map(CnDepthStats5::from_snapshot);
        let depth_opt = depth_stats.as_ref();
        {
            let (symbol_states, symbol_rolling_stats) =
                (&mut self.symbol_states, &mut self.symbol_rolling_stats);
            let state = symbol_states.entry(symbol.clone()).or_default();
            let rolling = symbol_rolling_stats.entry(symbol.clone()).or_default();
            state.push_trade_flow(&msg);
            state.push_optional_depth_stats(depth_opt);
            Self::update_symbol_rolling_stats(state, rolling, &msg, depth_opt);
        }

        if emit_output {
            if depth_opt.is_none() {
                self.missing_depth_count = self.missing_depth_count.saturating_add(1);
            } else {
                self.depth_attached_count = self.depth_attached_count.saturating_add(1);
            }
        }

        let eval_started = Instant::now();
        let Some(eval_result) = self.evaluate_ordered_factors(&symbol, depth_opt) else {
            if emit_output {
                debug!(
                    "fusion-trigger: venue={} symbol={} trade_ts={} reason=missing_symbol_factor_plan",
                    self.venue_slug, symbol, msg.ts
                );
            }
            return None;
        };

        if eval_result.status == 0 {
            self.symbol_all_ready_seen.insert(symbol.clone());
        } else if eval_result.status == 1 && self.symbol_all_ready_seen.contains(&symbol) {
            let warming_factors: Vec<&str> = eval_result
                .factor_issues
                .iter()
                .filter(|issue| issue.contains(":warming_up"))
                .map(|s| s.as_str())
                .collect();
            debug!(
                "cn_features factor regressed to warming_up after all_ready venue={} symbol={} trade_ts={} warming_factors=[{}]",
                self.venue_slug,
                symbol,
                msg.ts,
                warming_factors.join(",")
            );
        }

        let replay_summary = ReplayEvalSummary::from_eval(&eval_result, false);
        if !emit_output {
            return Some(replay_summary);
        }

        let eval_elapsed_us = eval_started.elapsed().as_micros();

        self.factor_plan_count = self
            .factor_plan_count
            .saturating_add(eval_result.stats.factor_plan_count);
        self.factor_evaluated_count = self
            .factor_evaluated_count
            .saturating_add(eval_result.stats.factor_evaluated_count);
        self.factor_ready_count = self
            .factor_ready_count
            .saturating_add(eval_result.stats.factor_ready_count);
        self.factor_warming_up_count = self
            .factor_warming_up_count
            .saturating_add(eval_result.stats.factor_warming_up_count);
        self.factor_invalid_value_count = self
            .factor_invalid_value_count
            .saturating_add(eval_result.stats.factor_invalid_value_count);
        self.factor_missing_depth_count = self
            .factor_missing_depth_count
            .saturating_add(eval_result.stats.factor_missing_depth_count);
        self.factor_unsupported_count = self
            .factor_unsupported_count
            .saturating_add(eval_result.stats.factor_unsupported_count);
        self.factor_118_ready_count = self
            .factor_118_ready_count
            .saturating_add(eval_result.stats.factor118_ready_count);

        self.log_factor_summary(&symbol, eval_elapsed_us, &eval_result);

        let normalized = {
            let norm_state = self
                .symbol_norm_states
                .entry(symbol.clone())
                .or_insert_with(|| SymbolNormState::new(self.zscore_config.window_size, 0));
            normalize_feature_values(norm_state, &eval_result.factor_values, &self.zscore_config)
        };
        let Some(normalized) = normalized else {
            return Some(replay_summary);
        };

        // Publish FeatureMsg via iceoryx2
        let ts_ms = msg.ts / 1000; // ts is in microseconds, convert to ms
        let feature_msg = FeatureMsg::create(symbol, ts_ms, eval_result.status, normalized);
        match feature_msg.to_bytes() {
            Ok(bytes) => {
                if self
                    .publisher
                    .as_mut()
                    .map(|publisher| publisher.publish(&bytes))
                    .unwrap_or(false)
                {
                    self.published_count = self.published_count.saturating_add(1);
                    if self.published_count % 500 == 1 {
                        info!(
                            "CnFactorEngine[{}] published: symbol={} ts_ms={} bytes={} total={}",
                            self.venue_slug,
                            feature_msg.symbol,
                            ts_ms,
                            bytes.len(),
                            self.published_count
                        );
                    }
                } else {
                    self.publish_failed_count = self.publish_failed_count.saturating_add(1);
                    warn!(
                        "CnFactorEngine[{}] publish failed: symbol={} ts_ms={} total_failed={}",
                        self.venue_slug, feature_msg.symbol, ts_ms, self.publish_failed_count
                    );
                }
            }
            Err(e) => {
                self.publish_failed_count = self.publish_failed_count.saturating_add(1);
                warn!(
                    "CnFactorEngine[{}] FeatureMsg serialize failed: {}",
                    self.venue_slug, e
                );
            }
        }
        Some(replay_summary)
    }

    fn evaluate_ordered_factors(
        &mut self,
        symbol: &str,
        depth: Option<&CnDepthStats5>,
    ) -> Option<OrderedEvalResult> {
        let needs_factor_118 = {
            let symbol_factor_plan = self.symbol_factor_plans.get(symbol)?;
            symbol_factor_plan
                .ordered_factors
                .iter()
                .any(|factor| factor.factor_id == Some(CnFactorId::Factor118))
        };
        let factor_118_result = if needs_factor_118 {
            depth.and_then(|d| self.compute_factor_118(symbol, d))
        } else {
            None
        };
        let series = {
            let state = self.symbol_states.get_mut(symbol)?;
            Self::build_symbol_series_from_state(state)
        };
        let plan = self.symbol_factor_plans.get(symbol)?;
        Some(Self::evaluate_ordered_factors_with_plan(
            plan,
            factor_118_result,
            depth,
            Some(&series),
        ))
    }

    fn evaluate_ordered_factors_with_plan(
        plan: &CnFormulaPlan,
        factor_118_result: Option<(f64, bool, usize)>,
        depth: Option<&CnDepthStats5>,
        series: Option<&CnSeries<'_>>,
    ) -> OrderedEvalResult {
        let mut result = OrderedEvalResult {
            stats: OrderedEvalStats::default(),
            factor_issues: Vec::with_capacity(plan.ordered_factors.len()),
            factor_values: Vec::with_capacity(plan.ordered_factors.len()),
            status: 0,
        };
        result.stats.factor_plan_count = plan.ordered_factors.len() as u64;

        let mut has_warming_up = false;
        let mut has_missing_depth = false;

        for binding in &plan.ordered_factors {
            match Self::compute_supported_factor(binding, factor_118_result, depth, series) {
                Some((value, ready, status)) => {
                    result.stats.factor_evaluated_count =
                        result.stats.factor_evaluated_count.saturating_add(1);
                    if ready {
                        result.stats.factor_ready_count =
                            result.stats.factor_ready_count.saturating_add(1);
                        result.factor_values.push(value);
                        if value.is_nan() {
                            result.stats.factor_nan_fill_count =
                                result.stats.factor_nan_fill_count.saturating_add(1);
                            result
                                .factor_issues
                                .push(format!("{}:nan_fill", binding.name));
                        }
                    } else {
                        result.factor_values.push(f64::NAN);
                        let reason = if value.is_nan() {
                            format!("{}:{}(nan)", binding.name, status)
                        } else {
                            format!("{}:{}", binding.name, status)
                        };
                        result.factor_issues.push(reason);
                        match status {
                            "warming_up" => {
                                has_warming_up = true;
                                result.stats.factor_warming_up_count =
                                    result.stats.factor_warming_up_count.saturating_add(1);
                            }
                            "invalid_value" => {
                                result.stats.factor_invalid_value_count =
                                    result.stats.factor_invalid_value_count.saturating_add(1);
                            }
                            "missing_depth" => {
                                has_missing_depth = true;
                                result.stats.factor_missing_depth_count =
                                    result.stats.factor_missing_depth_count.saturating_add(1);
                            }
                            _ => {}
                        }
                    }
                    if ready && binding.factor_id == Some(CnFactorId::Factor118) {
                        result.stats.factor118_ready_count =
                            result.stats.factor118_ready_count.saturating_add(1);
                    }
                }
                None => {
                    result.factor_values.push(f64::NAN);
                    result.stats.factor_unsupported_count =
                        result.stats.factor_unsupported_count.saturating_add(1);
                }
            }
        }

        result.status = if has_missing_depth {
            2
        } else if has_warming_up {
            1
        } else {
            0
        };
        result
    }

    fn record_dropped_symbol_sample(&mut self, symbol: &str) {
        if self.trade_flow_dropped_symbol_samples.len() >= 5 {
            return;
        }
        if !self
            .trade_flow_dropped_symbol_samples
            .iter()
            .any(|s| s == symbol)
        {
            self.trade_flow_dropped_symbol_samples
                .push(symbol.to_string());
        }
    }

    fn log_factor_summary(
        &self,
        symbol: &str,
        eval_cost_us: u128,
        eval_result: &OrderedEvalResult,
    ) {
        if eval_cost_us < 1_000 {
            return;
        }

        let s = &eval_result.stats;
        let good = s.factor_ready_count.saturating_sub(s.factor_nan_fill_count);
        let bad = s.factor_warming_up_count
            + s.factor_invalid_value_count
            + s.factor_missing_depth_count
            + s.factor_nan_fill_count;

        // 非 warming_up 的异常因子
        let non_warming_issues: Vec<&str> = eval_result
            .factor_issues
            .iter()
            .filter(|s| !s.contains(":warming_up"))
            .map(|s| s.as_str())
            .collect();

        info!(
            "CnFactorEngine[{}] factor-summary: symbol={} plan={} good={} bad={} eval_cost_us={} (warming_up={} nan_fill={} invalid={} missing_depth={}) non_warming=[{}]",
            self.venue_slug,
            symbol,
            s.factor_plan_count,
            good,
            bad,
            eval_cost_us,
            s.factor_warming_up_count,
            s.factor_nan_fill_count,
            s.factor_invalid_value_count,
            s.factor_missing_depth_count,
            non_warming_issues.join(","),
        );
    }

    pub fn build_symbol_series_from_state(state: &mut CnCalcState) -> CnSeries<'_> {
        CnSeries {
            open: SplitSlice::from_parts(state.open.as_slices()),
            high: SplitSlice::from_parts(state.high.as_slices()),
            low: SplitSlice::from_parts(state.low.as_slices()),
            close: SplitSlice::from_parts(state.close.as_slices()),
            volume: SplitSlice::from_parts(state.volume.as_slices()),
            amount: SplitSlice::from_parts(state.amount.as_slices()),
            avg_amount: SplitSlice::from_parts(state.avg_amount.as_slices()),
            count: SplitSlice::from_parts(state.count.as_slices()),
            trade_time: SplitSlice::from_parts(state.trade_time.as_slices()),
            buy_count: SplitSlice::from_parts(state.buy_count.as_slices()),
            sell_count: SplitSlice::from_parts(state.sell_count.as_slices()),
            buy_amount: SplitSlice::from_parts(state.buy_amount.as_slices()),
            sell_amount: SplitSlice::from_parts(state.sell_amount.as_slices()),
            buy_volume: SplitSlice::from_parts(state.buy_volume.as_slices()),
            sell_volume: SplitSlice::from_parts(state.sell_volume.as_slices()),
            large_order: SplitSlice::from_parts(state.large_order.as_slices()),
            medium_order: SplitSlice::from_parts(state.medium_order.as_slices()),
            large_buy: SplitSlice::from_parts(state.large_buy.as_slices()),
            large_sell: SplitSlice::from_parts(state.large_sell.as_slices()),
            medium_buy: SplitSlice::from_parts(state.medium_buy.as_slices()),
            medium_sell: SplitSlice::from_parts(state.medium_sell.as_slices()),
            small_order: SplitSlice::from_parts(state.small_order.as_slices()),
            small_buy: SplitSlice::from_parts(state.small_buy.as_slices()),
            small_sell: SplitSlice::from_parts(state.small_sell.as_slices()),
            vwap: SplitSlice::from_parts(state.vwap.as_slices()),
            buy_vwap: SplitSlice::from_parts(state.buy_vwap.as_slices()),
            sell_vwap: SplitSlice::from_parts(state.sell_vwap.as_slices()),
            net_buy_volume: SplitSlice::from_parts(state.net_buy_volume.as_slices()),
            net_buy_pct: SplitSlice::from_parts(state.net_buy_pct.as_slices()),
            net_buy_large: SplitSlice::from_parts(state.net_buy_large.as_slices()),
            net_buy_medium: SplitSlice::from_parts(state.net_buy_medium.as_slices()),
            net_buy_small: SplitSlice::from_parts(state.net_buy_small.as_slices()),
            net_buy_amount: SplitSlice::from_parts(state.net_buy_amount.as_slices()),
            bid0v: SplitSlice::from_parts(state.bid0v.as_slices()),
            mid_price: SplitSlice::from_parts(state.mid_price.as_slices()),
            spread: SplitSlice::from_parts(state.spread.as_slices()),
            relative_spread: SplitSlice::from_parts(state.relative_spread.as_slices()),
            bid_vwap5: SplitSlice::from_parts(state.bid_vwap5.as_slices()),
            total_bid5: SplitSlice::from_parts(state.total_bid5.as_slices()),
            total_ask5: SplitSlice::from_parts(state.total_ask5.as_slices()),
            top3_bid_volume: SplitSlice::from_parts(state.top3_bid_volume.as_slices()),
            top3_ask_volume: SplitSlice::from_parts(state.top3_ask_volume.as_slices()),
            top3_bid_mean: SplitSlice::from_parts(state.top3_bid_mean.as_slices()),
            top3_ask_mean: SplitSlice::from_parts(state.top3_ask_mean.as_slices()),
            outer_bid_amount: SplitSlice::from_parts(state.outer_bid_amount.as_slices()),
            outer_ask_amount: SplitSlice::from_parts(state.outer_ask_amount.as_slices()),
            outer_bid_price: SplitSlice::from_parts(state.outer_bid_price.as_slices()),
            outer_ask_price: SplitSlice::from_parts(state.outer_ask_price.as_slices()),
            outer_level_volume: SplitSlice::from_parts(state.outer_level_volume.as_slices()),
            mean_bid_amount5: SplitSlice::from_parts(state.mean_bid_amount5.as_slices()),
            mean_bid_price3: SplitSlice::from_parts(state.mean_bid_price3.as_slices()),
            mean_bid_price5: SplitSlice::from_parts(state.mean_bid_price5.as_slices()),
            avg_ask_price3: SplitSlice::from_parts(state.avg_ask_price3.as_slices()),
            mean_ask_price5: SplitSlice::from_parts(state.mean_ask_price5.as_slices()),
            ask_pv5_mean: SplitSlice::from_parts(state.ask_pv5_mean.as_slices()),
            bid_pv5_mean: SplitSlice::from_parts(state.bid_pv5_mean.as_slices()),
            factor_031_ratio: SplitSlice::from_parts(state.factor_031_ratio.as_slices()),
            factor_119_mid_minus_ask_vwap3: SplitSlice::from_parts(
                state.factor_119_mid_minus_ask_vwap3.as_slices(),
            ),
            total_volume5_sum: SplitSlice::from_parts(state.total_volume5_sum.as_slices()),
            median_all_price10: SplitSlice::from_parts(state.median_all_price10.as_slices()),
            factor_152_pct_mean: SplitSlice::from_parts(state.factor_152_pct_mean.as_slices()),
            ask_vwap_diff_3_5: SplitSlice::from_parts(state.ask_vwap_diff_3_5.as_slices()),
            ask_mean_amount5: SplitSlice::from_parts(state.ask_mean_amount5.as_slices()),
            ask0v: SplitSlice::from_parts(state.ask0v.as_slices()),
            ask_vwap5: SplitSlice::from_parts(state.ask_vwap5.as_slices()),
            factor_113_bid_price_pct_hmean: SplitSlice::from_parts(
                state.factor_113_bid_price_pct_hmean.as_slices(),
            ),
            factor_114_ask_price_pct_mean: SplitSlice::from_parts(
                state.factor_114_ask_price_pct_mean.as_slices(),
            ),
            factor_127_bid_price_kurt: SplitSlice::from_parts(
                state.factor_127_bid_price_kurt.as_slices(),
            ),
            factor_128_skew: SplitSlice::from_parts(state.factor_128_skew.as_slices()),
            factor_131_ask_price_kurt: SplitSlice::from_parts(
                state.factor_131_ask_price_kurt.as_slices(),
            ),
            factor_157_bid_ask_diff_std: SplitSlice::from_parts(
                state.factor_157_bid_ask_diff_std.as_slices(),
            ),
            factor_160_pct_change_mean: SplitSlice::from_parts(
                state.factor_160_pct_change_mean.as_slices(),
            ),
            corr_close_volume_14_last: state.corr_close_volume_14_last,
            corr_open_volume_300_last: state.corr_open_volume_300_last,
            corr_mid_midvol_300_last: state.corr_mid_midvol_300_last,
        }
    }

    pub(crate) fn compute_supported_factor(
        binding: &CnFactorBinding,
        factor_118_result: Option<(f64, bool, usize)>,
        depth: Option<&CnDepthStats5>,
        series: Option<&CnSeries<'_>>,
    ) -> Option<(f64, bool, &'static str)> {
        if baseline_engine::is_supported_baseline(&binding.name) {
            return Some(Self::wrap_factor_value(
                series.and_then(|s| baseline_engine::compute_baseline(&binding.name, s)),
            ));
        }

        if let Some(out) = super::opv_factors::compute_supported_opv_factor(binding, series) {
            return Some(out);
        }
        if let Some(out) =
            super::plain_factors::compute_supported_plain_factor(binding, depth, series)
        {
            return Some(out);
        }

        match binding.factor_id {
            Some(CnFactorId::Factor118) => {
                let out = match factor_118_result {
                    Some((value, ready, _samples)) => {
                        let status = if ready { "ready" } else { "warming_up" };
                        (value, ready, status)
                    }
                    None => (f64::NAN, false, "missing_depth"),
                };
                return Some(out);
            }
            Some(CnFactorId::FactorTrades001) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_001),
                ));
            }
            Some(CnFactorId::FactorTrades002) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_002),
                ));
            }
            Some(CnFactorId::FactorTrades003) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_003),
                ));
            }
            Some(CnFactorId::FactorTrades004) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_004),
                ));
            }
            Some(CnFactorId::FactorTrades005) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_005),
                ));
            }
            Some(CnFactorId::FactorTrades006) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_006),
                ));
            }
            Some(CnFactorId::FactorTrades007) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_007),
                ));
            }
            Some(CnFactorId::FactorTrades008) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_008),
                ));
            }
            Some(CnFactorId::FactorTrades009) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_009),
                ));
            }
            Some(CnFactorId::FactorTrades010) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_010),
                ));
            }
            Some(CnFactorId::FactorTrades011) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_011),
                ));
            }
            Some(CnFactorId::FactorTrades012) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_012),
                ));
            }
            Some(CnFactorId::FactorTrades013) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_013),
                ));
            }
            Some(CnFactorId::FactorTrades014) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_014),
                ));
            }
            Some(CnFactorId::FactorTrades015) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_015),
                ));
            }
            Some(CnFactorId::FactorTrades016) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_016),
                ));
            }
            Some(CnFactorId::FactorTrades017) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_017),
                ));
            }
            Some(CnFactorId::FactorTrades018) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_018),
                ));
            }
            Some(CnFactorId::FactorTrades019) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_019),
                ));
            }
            Some(CnFactorId::FactorTrades020) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_020),
                ));
            }
            Some(CnFactorId::FactorTrades021) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_021),
                ));
            }
            Some(CnFactorId::FactorTrades022) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_022),
                ));
            }
            Some(CnFactorId::FactorTrades023) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_023),
                ));
            }
            Some(CnFactorId::FactorTrades024) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_024),
                ));
            }
            Some(CnFactorId::FactorTrades025) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_025),
                ));
            }
            Some(CnFactorId::FactorTrades026) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_026),
                ));
            }
            Some(CnFactorId::FactorTrades027) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_027),
                ));
            }
            Some(CnFactorId::FactorTrades028) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_028),
                ));
            }
            Some(CnFactorId::FactorTrades029) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_029),
                ));
            }
            Some(CnFactorId::FactorTrades030) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_030),
                ));
            }
            Some(CnFactorId::FactorTrades031) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_031),
                ));
            }
            Some(CnFactorId::FactorTrades032) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_032),
                ));
            }
            Some(CnFactorId::FactorTrades033) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_033),
                ));
            }
            Some(CnFactorId::FactorTrades034) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_034),
                ));
            }
            Some(CnFactorId::FactorTrades035) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_035),
                ));
            }
            Some(CnFactorId::FactorTrades036) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_036),
                ));
            }
            Some(CnFactorId::FactorTrades037) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_037),
                ));
            }
            Some(CnFactorId::FactorTrades038) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_038),
                ));
            }
            Some(CnFactorId::FactorTrades039) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_039),
                ));
            }
            Some(CnFactorId::FactorTrades040) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_040),
                ));
            }
            Some(CnFactorId::FactorTrades041) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_041),
                ));
            }
            Some(CnFactorId::FactorTrades042) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_042),
                ));
            }
            Some(CnFactorId::FactorTrades043) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_043),
                ));
            }
            Some(CnFactorId::FactorTrades044) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_044),
                ));
            }
            Some(CnFactorId::FactorTrades045) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_045),
                ));
            }
            Some(CnFactorId::FactorTrades046) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_046),
                ));
            }
            Some(CnFactorId::FactorTrades047) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_047),
                ));
            }
            Some(CnFactorId::FactorTrades048) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_048),
                ));
            }
            Some(CnFactorId::FactorTrades049) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_049),
                ));
            }
            Some(CnFactorId::FactorTrades050) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_050),
                ));
            }
            Some(CnFactorId::TdTi010) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_010),
                ));
            }
            Some(CnFactorId::TdTi015) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_015),
                ));
            }
            Some(CnFactorId::TdTi026) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_026),
                ));
            }
            Some(CnFactorId::TdTi031) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_031),
                ));
            }
            Some(CnFactorId::TdTi033) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_033),
                ));
            }
            Some(CnFactorId::TdTi034) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_034),
                ));
            }
            Some(CnFactorId::TdTi036) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_036),
                ));
            }
            Some(CnFactorId::TdTi037) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_037),
                ));
            }
            Some(CnFactorId::TdMt003) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_mt_003),
                ));
            }
            Some(CnFactorId::TdMt008) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_mt_008),
                ));
            }
            Some(CnFactorId::TdMt014) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_mt_014),
                ));
            }
            Some(CnFactorId::TdMt015) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_mt_015),
                ));
            }
            Some(CnFactorId::TdMt039) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_mt_039),
                ));
            }
            Some(CnFactorId::TpVpi004) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_004),
                ));
            }
            Some(CnFactorId::TpVpi006) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_006),
                ));
            }
            Some(CnFactorId::TpVpi001) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_001),
                ));
            }
            Some(CnFactorId::TpVpi002) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_002),
                ));
            }
            Some(CnFactorId::TpVpi005) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_005),
                ));
            }
            Some(CnFactorId::TpVpi015) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_015),
                ));
            }
            Some(CnFactorId::TpVpi014) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_014),
                ));
            }
            Some(CnFactorId::TpVpi017) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_017),
                ));
            }
            Some(CnFactorId::TdPt001) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_001),
                ));
            }
            Some(CnFactorId::TdPt002) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_002),
                ));
            }
            Some(CnFactorId::TdPt003) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_003),
                ));
            }
            Some(CnFactorId::TdPt004) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_004),
                ));
            }
            Some(CnFactorId::TdPt010) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_010),
                ));
            }
            Some(CnFactorId::TdPt027) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_027),
                ));
            }
            Some(CnFactorId::TdVi010) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_vi_010),
                ));
            }
            Some(CnFactorId::TdVi011) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_vi_011),
                ));
            }
            Some(CnFactorId::TdVi025) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_vi_025),
                ));
            }
            Some(CnFactorId::TdVi026) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_vi_026),
                ));
            }
            Some(CnFactorId::TdVi028) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_vi_028),
                ));
            }
            Some(CnFactorId::TdCi007) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ci_007),
                ));
            }
            Some(CnFactorId::TdCi003) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ci_003),
                ));
            }
            Some(CnFactorId::TdCi008) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ci_008),
                ));
            }
            Some(CnFactorId::TdPr001) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_001),
                ));
            }
            Some(CnFactorId::TdPr002) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_002),
                ));
            }
            Some(CnFactorId::TdPr005) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_005),
                ));
            }
            Some(CnFactorId::TdPr006) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_006),
                ));
            }
            Some(CnFactorId::TdPr007) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_007),
                ));
            }
            Some(CnFactorId::TdPr015) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_015),
                ));
            }
            Some(CnFactorId::TdPr016) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_016),
                ));
            }
            Some(CnFactorId::TdPr017) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_017),
                ));
            }
            Some(CnFactorId::Factor001) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_001),
                ));
            }
            Some(CnFactorId::Factor002) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_002),
                ));
            }
            Some(CnFactorId::Factor003) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_003),
                ));
            }
            Some(CnFactorId::Factor004) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(|s| Self::compute_factor_004(s, depth)),
                ));
            }
            Some(CnFactorId::Factor006) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_006),
                ));
            }
            Some(CnFactorId::Factor008) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_008),
                ));
            }
            Some(CnFactorId::Factor009) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_009),
                ));
            }
            Some(CnFactorId::Factor010) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_010),
                ));
            }
            Some(CnFactorId::Factor011) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_011),
                ));
            }
            Some(CnFactorId::Factor012) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_012),
                ));
            }
            Some(CnFactorId::Factor014) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_014),
                ));
            }
            Some(CnFactorId::Factor016) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_016),
                ));
            }
            Some(CnFactorId::Factor017) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_017),
                ));
            }
            Some(CnFactorId::Factor018) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_018),
                ));
            }
            Some(CnFactorId::Factor019) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_019),
                ));
            }
            Some(CnFactorId::Factor020) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_020),
                ));
            }
            Some(CnFactorId::Factor021) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_021),
                ));
            }
            Some(CnFactorId::Factor022) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_022),
                ));
            }
            Some(CnFactorId::Factor023) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_023),
                ));
            }
            Some(CnFactorId::Factor024) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_024),
                ));
            }
            Some(CnFactorId::Factor025) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_025),
                ));
            }
            Some(CnFactorId::Factor026) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_026),
                ));
            }
            Some(CnFactorId::Factor027) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_027),
                ));
            }
            Some(CnFactorId::Factor028) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_028),
                ));
            }
            Some(CnFactorId::Factor029) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_029),
                ));
            }
            Some(CnFactorId::Factor030) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_030),
                ));
            }
            Some(CnFactorId::Factor031) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_031),
                ));
            }
            Some(CnFactorId::Factor032) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_032),
                ));
            }
            Some(CnFactorId::Factor033) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_033),
                ));
            }
            Some(CnFactorId::Factor035) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_035),
                ));
            }
            Some(CnFactorId::Factor036) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_036),
                ));
            }
            Some(CnFactorId::Factor037) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_037),
                ));
            }
            Some(CnFactorId::Factor038) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_038),
                ));
            }
            Some(CnFactorId::Factor040) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_040),
                ));
            }
            Some(CnFactorId::Factor041) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_041),
                ));
            }
            Some(CnFactorId::Factor042) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_042),
                ));
            }
            Some(CnFactorId::Factor043) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_043),
                ));
            }
            Some(CnFactorId::Factor045) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_045),
                ));
            }
            Some(CnFactorId::Factor046) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_046),
                ));
            }
            Some(CnFactorId::Factor047) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_047),
                ));
            }
            Some(CnFactorId::Factor048) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_048),
                ));
            }
            Some(CnFactorId::Factor049) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_049),
                ));
            }
            Some(CnFactorId::Factor051) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_051),
                ));
            }
            Some(CnFactorId::Factor052) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_052),
                ));
            }
            Some(CnFactorId::Factor053) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_053),
                ));
            }
            Some(CnFactorId::Factor054) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_054),
                ));
            }
            Some(CnFactorId::Factor055) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_055),
                ));
            }
            Some(CnFactorId::Factor056) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_056),
                ));
            }
            Some(CnFactorId::Factor057) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_057),
                ));
            }
            Some(CnFactorId::Factor058) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_058),
                ));
            }
            Some(CnFactorId::Factor059) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_059),
                ));
            }
            Some(CnFactorId::Factor060) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_060),
                ));
            }
            Some(CnFactorId::Factor061) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_061),
                ));
            }
            Some(CnFactorId::Factor062) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_062),
                ));
            }
            Some(CnFactorId::Factor063) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_063),
                ));
            }
            Some(CnFactorId::Factor064) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_064),
                ));
            }
            Some(CnFactorId::Factor065) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_065),
                ));
            }
            Some(CnFactorId::Factor066) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_066),
                ));
            }
            Some(CnFactorId::Factor067) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_067),
                ));
            }
            Some(CnFactorId::Factor068) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_068),
                ));
            }
            Some(CnFactorId::Factor069) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_069),
                ));
            }
            Some(CnFactorId::Factor070) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_070),
                ));
            }
            Some(CnFactorId::Factor073) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_073),
                ));
            }
            Some(CnFactorId::Factor074) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_074),
                ));
            }
            Some(CnFactorId::Factor075) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_075),
                ));
            }
            Some(CnFactorId::Factor076) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_076),
                ));
            }
            Some(CnFactorId::Factor077) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_077),
                ));
            }
            Some(CnFactorId::Factor079) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_079),
                ));
            }
            Some(CnFactorId::Factor080) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_080),
                ));
            }
            Some(CnFactorId::Factor085) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_085),
                ));
            }
            Some(CnFactorId::Factor086) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_086),
                ));
            }
            Some(CnFactorId::Factor087) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_087),
                ));
            }
            Some(CnFactorId::Factor088) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_088),
                ));
            }
            Some(CnFactorId::Factor089) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_089),
                ));
            }
            Some(CnFactorId::Factor091) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_091),
                ));
            }
            Some(CnFactorId::Factor093) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_093),
                ));
            }
            Some(CnFactorId::Factor094) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_094),
                ));
            }
            Some(CnFactorId::Factor095) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_095),
                ));
            }
            Some(CnFactorId::Factor096) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_096),
                ));
            }
            Some(CnFactorId::Factor097) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_097),
                ));
            }
            Some(CnFactorId::Factor102) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_102),
                ));
            }
            Some(CnFactorId::Factor103) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_103),
                ));
            }
            Some(CnFactorId::Factor107) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_107),
                ));
            }
            Some(CnFactorId::Factor108) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_108),
                ));
            }
            Some(CnFactorId::Factor110) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_110),
                ));
            }
            Some(CnFactorId::Factor111) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_111),
                ));
            }
            Some(CnFactorId::Factor113) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_113),
                ));
            }
            Some(CnFactorId::Factor114) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_114),
                ));
            }
            Some(CnFactorId::Factor115) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_115),
                ));
            }
            Some(CnFactorId::Factor116) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_116),
                ));
            }
            Some(CnFactorId::Factor119) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_119),
                ));
            }
            Some(CnFactorId::Factor120) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_120),
                ));
            }
            Some(CnFactorId::Factor121) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_121),
                ));
            }
            Some(CnFactorId::Factor122) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_122),
                ));
            }
            Some(CnFactorId::Factor123) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_123),
                ));
            }
            Some(CnFactorId::Factor124) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_124),
                ));
            }
            Some(CnFactorId::Factor125) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_125),
                ));
            }
            Some(CnFactorId::Factor126) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_126),
                ));
            }
            Some(CnFactorId::Factor128) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_128),
                ));
            }
            Some(CnFactorId::Factor129) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_129),
                ));
            }
            Some(CnFactorId::Factor130) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_130),
                ));
            }
            Some(CnFactorId::Factor133) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_133),
                ));
            }
            Some(CnFactorId::Factor134) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_134),
                ));
            }
            Some(CnFactorId::Factor139) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_139),
                ));
            }
            Some(CnFactorId::Factor144) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_144),
                ));
            }
            Some(CnFactorId::Factor151) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_151),
                ));
            }
            Some(CnFactorId::Factor152) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_152),
                ));
            }
            Some(CnFactorId::Factor156) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_156),
                ));
            }
            Some(CnFactorId::Factor159) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_159),
                ));
            }
            Some(CnFactorId::Factor160) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_160),
                ));
            }
            Some(CnFactorId::Factor165) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_165),
                ));
            }
            Some(CnFactorId::Factor166) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_166),
                ));
            }
            Some(CnFactorId::Factor168) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_168),
                ));
            }
            Some(CnFactorId::Factor170) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_170),
                ));
            }
            Some(CnFactorId::Factor175) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_175),
                ));
            }
            Some(CnFactorId::Factor176) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_176),
                ));
            }
            Some(CnFactorId::Factor164) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_164),
                ));
            }
            Some(_) | None => {}
        }
        if let Some(extra_factor_id) = binding.extra_factor_id {
            return Some(Self::wrap_factor_value(Self::compute_extra_factor(
                series,
                depth,
                extra_factor_id,
            )));
        }
        None
    }

    fn compute_extra_factor(
        series: Option<&CnSeries<'_>>,
        depth: Option<&CnDepthStats5>,
        extra_factor_id: CnExtraFactorId,
    ) -> Option<f64> {
        match extra_factor_id {
            CnExtraFactorId::Bid0Price1 => {
                return depth.and_then(|value| finite_opt(Some(depth_best_bid(value).0)));
            }
            CnExtraFactorId::Bid0Volume1 => {
                return depth.and_then(|value| finite_opt(Some(depth_best_bid(value).1)));
            }
            CnExtraFactorId::Ask0Price1 => {
                return depth.and_then(|value| finite_opt(Some(depth_best_ask(value).0)));
            }
            CnExtraFactorId::Ask0Volume1 => {
                return depth.and_then(|value| finite_opt(Some(depth_best_ask(value).1)));
            }
            _ => {}
        }

        let series = series?;
        match extra_factor_id {
            CnExtraFactorId::AvgPrice => rolling_kurt_last(&series.vwap, 250, true, false)
                .ok()
                .flatten(),
            CnExtraFactorId::BuyAvgPrice => rolling_skew_last(&series.buy_vwap, 500, false)
                .ok()
                .flatten(),
            CnExtraFactorId::SellAvgPrice => {
                let n = series.vwap.len().min(series.sell_vwap.len());
                if n < 360 {
                    return None;
                }
                let diff: Vec<f64> = (0..n)
                    .map(|i| series.vwap[i] - series.sell_vwap[i])
                    .collect();
                rolling_mean_last(&diff, 360).ok().flatten()
            }
            CnExtraFactorId::SmallBuy => finite_opt(series.small_buy.last().copied()),
            CnExtraFactorId::SmallSell => finite_opt(series.small_sell.last().copied()),
            CnExtraFactorId::NetBuyLarge => finite_opt(series.net_buy_large.last().copied()),
            CnExtraFactorId::Open => finite_opt(series.open.last().copied()),
            CnExtraFactorId::High => finite_opt(series.high.last().copied()),
            CnExtraFactorId::Low => finite_opt(series.low.last().copied()),
            CnExtraFactorId::Close => finite_opt(series.close.last().copied()),
            CnExtraFactorId::Volume => finite_opt(series.volume.last().copied()),
            CnExtraFactorId::Amount => finite_opt(series.amount.last().copied()),
            CnExtraFactorId::AvgAmount => finite_opt(series.avg_amount.last().copied()),
            CnExtraFactorId::Count => finite_opt(series.count.last().copied()),
            CnExtraFactorId::BuyCount => finite_opt(series.buy_count.last().copied()),
            CnExtraFactorId::SellCount => finite_opt(series.sell_count.last().copied()),
            CnExtraFactorId::BuyAmount => finite_opt(series.buy_amount.last().copied()),
            CnExtraFactorId::SellAmount => finite_opt(series.sell_amount.last().copied()),
            CnExtraFactorId::BuyVolume => finite_opt(series.buy_volume.last().copied()),
            CnExtraFactorId::SellVolume => finite_opt(series.sell_volume.last().copied()),
            CnExtraFactorId::LargeOrder => finite_opt(series.large_order.last().copied()),
            CnExtraFactorId::MediumOrder => finite_opt(series.medium_order.last().copied()),
            CnExtraFactorId::SmallOrder => finite_opt(series.small_order.last().copied()),
            CnExtraFactorId::LargeBuy => finite_opt(series.large_buy.last().copied()),
            CnExtraFactorId::LargeSell => finite_opt(series.large_sell.last().copied()),
            CnExtraFactorId::MediumBuy => finite_opt(series.medium_buy.last().copied()),
            CnExtraFactorId::MediumSell => finite_opt(series.medium_sell.last().copied()),
            CnExtraFactorId::Vwap => finite_opt(series.vwap.last().copied()),
            CnExtraFactorId::BuyVwap => finite_opt(series.buy_vwap.last().copied()),
            CnExtraFactorId::SellVwap => finite_opt(series.sell_vwap.last().copied()),
            CnExtraFactorId::NetBuyAmount => finite_opt(series.net_buy_amount.last().copied()),
            CnExtraFactorId::NetBuyVolume => finite_opt(series.net_buy_volume.last().copied()),
            CnExtraFactorId::NetBuyPct => finite_opt(series.net_buy_pct.last().copied()),
            CnExtraFactorId::NetBuyMedium => finite_opt(series.net_buy_medium.last().copied()),
            CnExtraFactorId::NetBuySmall => finite_opt(series.net_buy_small.last().copied()),
            CnExtraFactorId::Bid0Price1
            | CnExtraFactorId::Bid0Volume1
            | CnExtraFactorId::Ask0Price1
            | CnExtraFactorId::Ask0Volume1 => unreachable!("handled before series lookup"),
        }
    }

    pub fn wrap_factor_value(value: Option<f64>) -> (f64, bool, &'static str) {
        match value {
            Some(v) if v.is_finite() => (v, true, "ready"),
            Some(v) => (v, false, "invalid_value"),
            None => (f64::NAN, false, "warming_up"),
        }
    }

    fn compute_factor_trades_001(series: &CnSeries<'_>) -> Option<f64> {
        let amount = series.amount.last().copied()?;
        finite_opt(Some((amount + 1e-6).ln()))
    }

    fn compute_factor_trades_002(series: &CnSeries<'_>) -> Option<f64> {
        let volume = series.volume.last().copied()?;
        finite_opt(Some((volume + 1e-6).ln()))
    }

    fn compute_factor_trades_003(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.amount.len().min(series.buy_amount.len());
        if n == 0 {
            return None;
        }
        let amount = series.amount[n - 1];
        if amount.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.buy_amount[n - 1] / amount))
    }

    fn compute_factor_trades_004(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.amount.len().min(series.sell_amount.len());
        if n == 0 {
            return None;
        }
        let amount = series.amount[n - 1];
        if amount.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.sell_amount[n - 1] / amount))
    }

    fn compute_factor_trades_005(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.volume.len().min(series.buy_volume.len());
        if n == 0 {
            return None;
        }
        let volume = series.volume[n - 1];
        if volume.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.buy_volume[n - 1] / volume))
    }

    fn compute_factor_trades_006(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.volume.len().min(series.sell_volume.len());
        if n == 0 {
            return None;
        }
        let volume = series.volume[n - 1];
        if volume.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.sell_volume[n - 1] / volume))
    }

    fn compute_factor_trades_007(series: &CnSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_amount.last().copied())
    }

    fn compute_factor_trades_008(series: &CnSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_volume.last().copied())
    }

    fn compute_factor_trades_009(series: &CnSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_pct.last().copied())
    }

    fn compute_factor_trades_010(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.buy_vwap.len().min(series.sell_vwap.len());
        if n == 0 {
            return None;
        }
        finite_opt(Some(series.buy_vwap[n - 1] - series.sell_vwap[n - 1]))
    }

    fn compute_factor_trades_011(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.buy_vwap.len().min(series.sell_vwap.len());
        if n == 0 {
            return None;
        }
        let sell_vwap = series.sell_vwap[n - 1];
        if sell_vwap.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.buy_vwap[n - 1] / sell_vwap))
    }

    fn compute_factor_trades_012(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.count.len().min(series.buy_count.len());
        if n == 0 {
            return None;
        }
        let count = series.count[n - 1];
        if count.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.buy_count[n - 1] / count))
    }

    fn compute_factor_trades_013(series: &CnSeries<'_>) -> Option<f64> {
        finite_opt(series.avg_amount.last().copied())
    }

    fn compute_factor_trades_014(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.buy_amount.len().min(series.buy_count.len());
        if n == 0 {
            return None;
        }
        let buy_count = series.buy_count[n - 1];
        if buy_count.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.buy_amount[n - 1] / buy_count))
    }

    fn compute_factor_trades_015(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.sell_amount.len().min(series.sell_count.len());
        if n == 0 {
            return None;
        }
        let sell_count = series.sell_count[n - 1];
        if sell_count.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.sell_amount[n - 1] / sell_count))
    }

    fn compute_factor_trades_016(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.amount.len().min(series.large_order.len());
        if n == 0 {
            return None;
        }
        let amount = series.amount[n - 1];
        if amount.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.large_order[n - 1] / amount))
    }

    fn compute_factor_trades_017(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.amount.len().min(series.medium_order.len());
        if n == 0 {
            return None;
        }
        let amount = series.amount[n - 1];
        if amount.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.medium_order[n - 1] / amount))
    }

    fn compute_factor_trades_018(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.amount.len().min(series.small_order.len());
        if n == 0 {
            return None;
        }
        let amount = series.amount[n - 1];
        if amount.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.small_order[n - 1] / amount))
    }

    fn compute_factor_trades_019(series: &CnSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_large.last().copied())
    }

    fn compute_factor_trades_020(series: &CnSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_medium.last().copied())
    }

    fn compute_factor_trades_021(series: &CnSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_small.last().copied())
    }

    fn compute_factor_trades_022(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.large_order.len().min(series.net_buy_large.len());
        if n == 0 {
            return None;
        }
        let large_order = series.large_order[n - 1];
        if large_order.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.net_buy_large[n - 1] / large_order))
    }

    fn compute_factor_trades_023(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.medium_order.len().min(series.net_buy_medium.len());
        if n == 0 {
            return None;
        }
        let medium_order = series.medium_order[n - 1];
        if medium_order.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.net_buy_medium[n - 1] / medium_order))
    }

    fn compute_factor_trades_024(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.small_order.len().min(series.net_buy_small.len());
        if n == 0 {
            return None;
        }
        let small_order = series.small_order[n - 1];
        if small_order.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.net_buy_small[n - 1] / small_order))
    }

    fn compute_factor_trades_025(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len().min(series.vwap.len());
        if n == 0 {
            return None;
        }
        let vwap = series.vwap[n - 1];
        if vwap.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((series.close[n - 1] - vwap) / vwap))
    }

    fn compute_factor_trades_026(series: &CnSeries<'_>) -> Option<f64> {
        let amount = series.amount.last().copied()?;
        let cumsum_amount = series.amount.iter().copied().sum::<f64>();
        if cumsum_amount.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(amount / cumsum_amount))
    }

    fn compute_factor_trades_027(series: &CnSeries<'_>) -> Option<f64> {
        let volume = series.volume.last().copied()?;
        let cumsum_volume = series.volume.iter().copied().sum::<f64>();
        if cumsum_volume.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(volume / cumsum_volume))
    }

    fn compute_factor_trades_028(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.buy_amount.len().min(series.buy_count.len());
        if n == 0 {
            return None;
        }
        finite_opt(Some(series.buy_amount[n - 1] * series.buy_count[n - 1]))
    }

    fn compute_factor_trades_029(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.sell_amount.len().min(series.sell_count.len());
        if n == 0 {
            return None;
        }
        finite_opt(Some(series.sell_amount[n - 1] * series.sell_count[n - 1]))
    }

    fn compute_factor_trades_030(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .buy_amount
            .len()
            .min(series.buy_count.len())
            .min(series.sell_amount.len())
            .min(series.sell_count.len());
        if n == 0 {
            return None;
        }
        let buy_strength = series.buy_amount[n - 1] * series.buy_count[n - 1];
        let sell_strength = series.sell_amount[n - 1] * series.sell_count[n - 1];
        if sell_strength.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(buy_strength / sell_strength))
    }

    fn compute_factor_trades_031(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.open.len().min(series.close.len());
        if n == 0 {
            return None;
        }
        let open = series.open[n - 1];
        if open.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((series.close[n - 1] - open) / open))
    }

    fn compute_factor_trades_032(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.open.len().min(series.close.len());
        if n == 0 {
            return None;
        }
        finite_opt(Some(
            (series.close[n - 1] / (series.open[n - 1] + 1e-6)).ln(),
        ))
    }

    fn compute_factor_trades_033(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.open.len());
        if n == 0 {
            return None;
        }
        let open = series.open[n - 1];
        if open.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((series.high[n - 1] - series.low[n - 1]) / open))
    }

    fn compute_factor_trades_034(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.open.len())
            .min(series.close.len());
        if n == 0 {
            return None;
        }
        let high = series.high[n - 1];
        let low = series.low[n - 1];
        let open = series.open[n - 1];
        let close = series.close[n - 1];
        if !high.is_finite() || !low.is_finite() || !open.is_finite() || !close.is_finite() {
            return None;
        }
        let upper_shadow = high - open.max(close);
        let range_hl = high - low;
        if range_hl.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(upper_shadow / range_hl))
    }

    fn compute_factor_trades_035(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.open.len())
            .min(series.close.len());
        if n == 0 {
            return None;
        }
        let high = series.high[n - 1];
        let low = series.low[n - 1];
        let open = series.open[n - 1];
        let close = series.close[n - 1];
        if !high.is_finite() || !low.is_finite() || !open.is_finite() || !close.is_finite() {
            return None;
        }
        let lower_shadow = open.min(close) - low;
        let range_hl = high - low;
        if range_hl.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(lower_shadow / range_hl))
    }

    fn compute_factor_trades_036(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.open.len())
            .min(series.close.len());
        if n == 0 {
            return None;
        }
        let range_hl = series.high[n - 1] - series.low[n - 1];
        if range_hl.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(
            (series.close[n - 1] - series.open[n - 1]).abs() / range_hl,
        ))
    }

    fn compute_factor_trades_037(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len());
        if n == 0 {
            return None;
        }
        let range_hl = series.high[n - 1] - series.low[n - 1];
        if range_hl.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((series.close[n - 1] - series.low[n - 1]) / range_hl))
    }

    fn compute_factor_trades_038(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len();
        if n <= 5 {
            return None;
        }
        finite_opt(Some(series.close[n - 1] - series.close[n - 6]))
    }

    fn compute_factor_trades_039(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len();
        if n <= 10 {
            return None;
        }
        finite_opt(Some(series.close[n - 1] - series.close[n - 11]))
    }

    fn compute_factor_trades_040(series: &CnSeries<'_>) -> Option<f64> {
        Self::compute_ema_last(&series.close, 5)
    }

    fn compute_factor_trades_041(series: &CnSeries<'_>) -> Option<f64> {
        Self::compute_ema_last(&series.close, 10)
    }

    fn compute_factor_trades_042(series: &CnSeries<'_>) -> Option<f64> {
        rolling_mean_last_with_min_periods(&series.volume, 5, 1)
            .ok()
            .flatten()
    }

    fn compute_factor_trades_043(series: &CnSeries<'_>) -> Option<f64> {
        if series.volume.is_empty() {
            return None;
        }
        sample_std_last(&series.volume, 5, 1)
    }

    fn compute_factor_trades_044(series: &CnSeries<'_>) -> Option<f64> {
        if series.close.is_empty() {
            return None;
        }
        sample_std_last(&series.close, 10, 1)
    }

    fn compute_factor_trades_045(series: &CnSeries<'_>) -> Option<f64> {
        let close = series.close.last().copied()?;
        let ma5 = rolling_mean_last_with_min_periods(&series.close, 5, 1)
            .ok()
            .flatten()?;
        if ma5.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((close - ma5) / ma5))
    }

    fn compute_factor_trades_046(series: &CnSeries<'_>) -> Option<f64> {
        let volume = series.volume.last().copied()?;
        let ma_vol = rolling_mean_last_with_min_periods(&series.volume, 5, 1)
            .ok()
            .flatten()?;
        if ma_vol.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(volume / ma_vol))
    }

    fn compute_factor_trades_047(series: &CnSeries<'_>) -> Option<f64> {
        let pct_change = Self::build_pct_change_series(&series.close);
        rolling_mean_last_opt_from_series(&pct_change, 5, 1)
            .ok()
            .flatten()
    }

    fn compute_factor_trades_048(series: &CnSeries<'_>) -> Option<f64> {
        let pct_change = Self::build_pct_change_series(&series.close);
        tail_skew_last_opt(&pct_change, 10, 3, false).ok().flatten()
    }

    fn compute_factor_trades_049(series: &CnSeries<'_>) -> Option<f64> {
        let pct_change = Self::build_pct_change_series(&series.close);
        Self::tail_kurt_last_opt(&pct_change, 10, 4, true, false)
    }

    fn compute_factor_trades_050(series: &CnSeries<'_>) -> Option<f64> {
        rolling_mean_last_with_min_periods(&series.net_buy_amount, 5, 1)
            .ok()
            .flatten()
    }

    fn compute_ema_last(values: &(impl F64SeriesView + ?Sized), span: usize) -> Option<f64> {
        if span == 0 || values.len() == 0 {
            return None;
        }
        let alpha = 2.0 / (span as f64 + 1.0);
        let mut ema = values.value_at(0);
        if !ema.is_finite() {
            return None;
        }
        for i in 1..values.len() {
            let value = values.value_at(i);
            if !value.is_finite() {
                return None;
            }
            ema = alpha * value + (1.0 - alpha) * ema;
        }
        finite_opt(Some(ema))
    }

    fn build_pct_change_series(values: &(impl F64SeriesView + ?Sized)) -> Vec<Option<f64>> {
        let n = values.len();
        if n == 0 {
            return Vec::new();
        }

        let mut pct = Vec::with_capacity(n);
        pct.push(None);
        for i in 1..n {
            let prev = values.value_at(i - 1);
            let curr = values.value_at(i);
            if !prev.is_finite() || !curr.is_finite() || prev.abs() <= 1e-12 {
                pct.push(Some(f64::NAN));
            } else {
                pct.push(Some((curr - prev) / prev));
            }
        }
        pct
    }

    fn tail_kurt_last_opt(
        values: &(impl OptF64SeriesView + ?Sized),
        window: usize,
        min_periods: usize,
        fisher: bool,
        bias: bool,
    ) -> Option<f64> {
        if window == 0 || min_periods == 0 || values.len() == 0 {
            return None;
        }

        let start = values.len().saturating_sub(window);
        let mut valid = Vec::with_capacity(values.len() - start);
        for i in start..values.len() {
            if let Some(value) = values.value_at(i) {
                if !value.is_finite() {
                    return Some(f64::NAN);
                }
                valid.push(value);
            }
        }

        if valid.len() < min_periods {
            return None;
        }

        let n = valid.len() as f64;
        let mean = valid.iter().sum::<f64>() / n;
        let mut m2 = 0.0;
        let mut m4 = 0.0;
        for value in &valid {
            let d = *value - mean;
            let d2 = d * d;
            m2 += d2;
            m4 += d2 * d2;
        }
        let m2 = m2 / n;
        if m2.abs() <= 1e-12 {
            return Some(if fisher { -3.0 } else { 0.0 });
        }
        let m4 = m4 / n;
        let g2 = m4 / (m2 * m2) - 3.0;

        let mut out = if bias {
            g2
        } else {
            if valid.len() < 4 {
                return None;
            }
            ((n - 1.0) / ((n - 2.0) * (n - 3.0))) * ((n + 1.0) * g2 + 6.0)
        };
        if !fisher {
            out += 3.0;
        }
        finite_opt(Some(out))
    }

    fn compute_factor_001(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.bid_vwap5.len();
        if n < 3 {
            return None;
        }
        let d1 = series.bid_vwap5[n - 1] - series.bid_vwap5[n - 2];
        let d0 = series.bid_vwap5[n - 2] - series.bid_vwap5[n - 3];
        if !d1.is_finite() || !d0.is_finite() || d0.abs() <= 1e-12 {
            return Some(f64::NAN);
        }
        let value = (d1 - d0) / d0;
        finite_opt(Some(value))
    }

    fn compute_factor_002(depth: &CnDepthStats5) -> Option<f64> {
        let bid = depth.sum_bid_amount(CN_DEPTH_LEVELS);
        let ask = depth.sum_ask_amount(CN_DEPTH_LEVELS);
        if bid.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(ask / bid))
    }

    fn compute_factor_003(depth: &CnDepthStats5) -> Option<f64> {
        let bid = depth_sum_price(&depth.bids, CN_DEPTH_LEVELS);
        let ask = depth_sum_price(&depth.asks, CN_DEPTH_LEVELS);
        let den = bid + ask;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid - ask) / den))
    }

    fn compute_factor_004(series: &CnSeries<'_>, depth: Option<&CnDepthStats5>) -> Option<f64> {
        let depth = depth?;
        let buy_vwap = *series.buy_vwap.last()?;
        let (bid0p, _) = depth_best_bid(depth);
        let (ask0p, _) = depth_best_ask(depth);
        let mid = (bid0p + ask0p) / 2.0;
        finite_opt(Some(mid - buy_vwap))
    }

    fn compute_factor_006(depth: &CnDepthStats5) -> Option<f64> {
        let mut ask_strength = 0.0;
        let mut bid_strength = 0.0;
        for i in 0..CN_DEPTH_LEVELS {
            ask_strength += depth_level_price(&depth.asks, i) * depth_level_amount(&depth.asks, i);
            bid_strength += depth_level_price(&depth.bids, i) * depth_level_amount(&depth.bids, i);
        }
        let den = bid_strength + ask_strength;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid_strength - ask_strength) / den))
    }

    fn compute_factor_008(depth: &CnDepthStats5) -> Option<f64> {
        let diffs: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| {
                depth_level_price(&depth.bids, i) * depth_level_amount(&depth.bids, i)
                    - depth_level_price(&depth.asks, i) * depth_level_amount(&depth.asks, i)
            })
            .collect();
        std_pop(&diffs)
    }

    fn compute_factor_009(depth: &CnDepthStats5) -> Option<f64> {
        let (bid0p, _) = depth_best_bid(depth);
        let (ask0p, _) = depth_best_ask(depth);
        if !bid0p.is_finite() || !ask0p.is_finite() || bid0p <= 0.0 || ask0p <= 0.0 {
            return None;
        }
        finite_opt(Some(ask0p.ln() - bid0p.ln()))
    }

    fn compute_factor_010(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.bid_vwap5.len();
        if n < 2 {
            return None;
        }
        finite_opt(Some(series.bid_vwap5[n - 1] - series.bid_vwap5[n - 2]))
    }

    fn compute_factor_011(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.ask_vwap5.len();
        if n < 2 {
            return None;
        }
        finite_opt(Some(series.ask_vwap5[n - 1] - series.ask_vwap5[n - 2]))
    }

    fn compute_factor_012(depth: &CnDepthStats5) -> Option<f64> {
        let bid = depth.bid_pxv_prefix[CN_DEPTH_LEVELS];
        let ask = depth.ask_pxv_prefix[CN_DEPTH_LEVELS];
        let den = bid + ask;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid - ask) / den))
    }

    fn compute_factor_014(depth: &CnDepthStats5) -> Option<f64> {
        let inner = depth_sum_amount(&depth.asks, CN_INNER_DEPTH_LEVELS);
        let total = depth_sum_amount(&depth.asks, CN_DEPTH_LEVELS);
        if total.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(inner / total))
    }

    fn compute_factor_016(depth: &CnDepthStats5) -> Option<f64> {
        let total_ask_price = depth_sum_price(&depth.asks, CN_DEPTH_LEVELS);
        if !total_ask_price.is_finite() {
            return None;
        }
        if (total_ask_price + 1e-6).abs() <= 1e-12 {
            return Some(0.0);
        }
        let mut weighted_depth = 0.0;
        for i in 0..CN_DEPTH_LEVELS {
            let askv = depth_level_amount(&depth.asks, i);
            let askp = depth_level_price(&depth.asks, i);
            if !askv.is_finite() || !askp.is_finite() {
                return None;
            }
            weighted_depth += askv * askp / (total_ask_price + 1e-6);
        }
        finite_opt(Some(weighted_depth))
    }

    fn compute_factor_017(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        if bids.iter().any(|value| !value.is_finite()) {
            return None;
        }
        let mean = bids.iter().sum::<f64>() / bids.len() as f64;
        let den = mean + 1e-6;
        let norm: Vec<f64> = bids.into_iter().map(|v| v / den).collect();
        std_pop(&norm)
    }

    fn compute_factor_018(depth: &CnDepthStats5) -> Option<f64> {
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        let mean = asks.iter().sum::<f64>() / asks.len() as f64;
        let denom = mean + 1e-6;
        let norm: Vec<f64> = asks.into_iter().map(|v| v / denom).collect();
        std_pop(&norm)
    }

    fn compute_factor_019(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.bid_vwap5.len();
        if n < 2 {
            return None;
        }
        let curr = rolling_mean_at_from_series(&series.bid_vwap5, n, 5, 1)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))?;
        let prev = rolling_mean_at_from_series(&series.bid_vwap5, n - 1, 5, 1)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))?;
        finite_opt(Some(curr - prev))
    }

    fn compute_factor_020(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.ask_vwap5.len();
        if n < 2 {
            return None;
        }
        let curr = rolling_mean_at_from_series(&series.ask_vwap5, n, 5, 1)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))?;
        let prev = rolling_mean_at_from_series(&series.ask_vwap5, n - 1, 5, 1)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))?;
        finite_opt(Some(curr - prev))
    }

    fn compute_factor_021(series: &CnSeries<'_>) -> Option<f64> {
        let bid_std = sample_std_last(&series.total_bid5, 10, 1)?;
        let ask_std = sample_std_last(&series.total_ask5, 10, 1)?;
        if ask_std.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid_std / ask_std))
    }

    fn compute_factor_022(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.relative_spread.len();
        if n < 2 {
            return None;
        }
        finite_opt(Some(
            series.relative_spread[n - 1] - series.relative_spread[n - 2],
        ))
    }

    fn compute_factor_023(series: &CnSeries<'_>) -> Option<f64> {
        let values = &series.top3_bid_volume;
        let last = *values.last()?;
        let ma = rolling_mean_last_with_min_periods(values, 5, 1)
            .ok()
            .flatten()?;
        finite_opt(Some(last - ma))
    }

    fn compute_factor_024(series: &CnSeries<'_>) -> Option<f64> {
        let values = &series.top3_ask_volume;
        let last = *values.last()?;
        let ma = rolling_mean_last_with_min_periods(values, 5, 1)
            .ok()
            .flatten()?;
        finite_opt(Some(last - ma))
    }

    fn compute_factor_025(depth: &CnDepthStats5) -> Option<f64> {
        let inner = depth_sum_amount(&depth.bids, CN_INNER_DEPTH_LEVELS);
        let outer = depth_sum_amount(&depth.bids, CN_DEPTH_LEVELS) - inner;
        if outer.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(inner / outer))
    }

    fn compute_factor_026(depth: &CnDepthStats5) -> Option<f64> {
        let top3 = depth.sum_ask_amount(3);
        let bottom17 = depth.sum_ask_amount(CN_DEPTH_LEVELS) - top3;
        if bottom17.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(top3 / bottom17))
    }

    fn compute_factor_027(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        cross_sectional_skew(&bids, false)
    }

    fn compute_factor_028(depth: &CnDepthStats5) -> Option<f64> {
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        cross_sectional_skew(&asks, false)
    }

    fn compute_factor_029(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        cross_sectional_kurtosis(&bids, true, false)
    }

    fn compute_factor_030(depth: &CnDepthStats5) -> Option<f64> {
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        cross_sectional_kurtosis(&asks, true, false)
    }

    fn compute_factor_031(series: &CnSeries<'_>) -> Option<f64> {
        rolling_rank_last(&series.factor_031_ratio, 500)
            .ok()
            .flatten()
    }

    fn compute_factor_032(series: &CnSeries<'_>) -> Option<f64> {
        rolling_mean_last(&series.ask_vwap_diff_3_5, 300)
            .ok()
            .flatten()
    }

    fn compute_factor_033(series: &CnSeries<'_>) -> Option<f64> {
        let last = *series.top3_bid_volume.last()?;
        let ma = rolling_mean_last(&series.top3_bid_volume, 30)
            .ok()
            .flatten()?;
        if ma.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(last / ma))
    }

    fn compute_factor_035(depth: &CnDepthStats5) -> Option<f64> {
        let bid = depth_vwap(&depth.bids, CN_DEPTH_LEVELS)?;
        let ask = depth_vwap(&depth.asks, CN_DEPTH_LEVELS)?;
        if ask.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid / ask))
    }

    fn compute_factor_036(series: &CnSeries<'_>) -> Option<f64> {
        let last = *series.mean_ask_price5.last()?;
        let mean = rolling_mean_last(&series.mean_ask_price5, 300)
            .ok()
            .flatten()?;
        let std = sample_std_last(&series.mean_ask_price5, 300, 300)?;
        if std.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((last - mean) / std))
    }

    fn compute_factor_037(depth: &CnDepthStats5) -> Option<f64> {
        let inner = depth_vwap(&depth.bids, CN_INNER_DEPTH_LEVELS)?;
        let full = depth_vwap(&depth.bids, CN_DEPTH_LEVELS)?;
        finite_opt(Some(inner - full))
    }

    fn compute_factor_038(depth: &CnDepthStats5) -> Option<f64> {
        let inner = depth_vwap(&depth.asks, CN_INNER_DEPTH_LEVELS)?;
        let full = depth_vwap(&depth.asks, CN_DEPTH_LEVELS)?;
        finite_opt(Some(inner - full))
    }

    fn compute_factor_040(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        std_pop(&bids)
    }

    fn compute_factor_041(depth: &CnDepthStats5) -> Option<f64> {
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        std_pop(&asks)
    }

    fn compute_factor_042(depth: &CnDepthStats5) -> Option<f64> {
        let bid = depth.sum_bid_amount(CN_DEPTH_LEVELS);
        let ask = depth.sum_ask_amount(CN_DEPTH_LEVELS);
        if ask.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid / ask))
    }

    fn compute_factor_043(depth: &CnDepthStats5) -> Option<f64> {
        let (_, bid0v) = depth.best_bid();
        let (_, ask0v) = depth.best_ask();
        let total = depth.sum_bid_amount(CN_DEPTH_LEVELS) + depth.sum_ask_amount(CN_DEPTH_LEVELS);
        if total.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid0v + ask0v) / total))
    }

    fn compute_factor_045(series: &CnSeries<'_>) -> Option<f64> {
        rolling_kurt_last(&series.top3_bid_mean, 30, true, false)
            .ok()
            .flatten()
    }

    fn compute_factor_046(series: &CnSeries<'_>) -> Option<f64> {
        rolling_kurt_last(&series.top3_ask_mean, 30, true, false)
            .ok()
            .flatten()
    }

    fn compute_factor_047(series: &CnSeries<'_>) -> Option<f64> {
        rolling_skew_last(&series.ask_pv5_mean, 60, false)
            .ok()
            .flatten()
    }

    fn compute_factor_048(series: &CnSeries<'_>) -> Option<f64> {
        rolling_skew_last(&series.bid_pv5_mean, 60, false)
            .ok()
            .flatten()
    }

    fn compute_factor_049(series: &CnSeries<'_>) -> Option<f64> {
        rolling_std_last(&series.mean_bid_price3, 30).ok().flatten()
    }

    fn compute_factor_051(series: &CnSeries<'_>) -> Option<f64> {
        let last = *series.avg_ask_price3.last()?;
        let ma = rolling_mean_last(&series.avg_ask_price3, 300)
            .ok()
            .flatten()?;
        if ma.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(last / ma))
    }

    fn compute_factor_052(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.avg_ask_price3.len();
        if n < 300 {
            return None;
        }
        let mut neg_diff = Vec::with_capacity(n);
        neg_diff.push(0.0);
        for i in 1..n {
            let d = series.avg_ask_price3[i] - series.avg_ask_price3[i - 1];
            neg_diff.push(if d < 0.0 { d } else { 0.0 });
        }
        let ma30 = rolling_mean_last(&neg_diff, 30).ok().flatten()?;
        let ma300 = rolling_mean_last(&neg_diff, 300).ok().flatten()?;
        if ma300.abs() <= 1e-12 {
            return Some(f64::NAN);
        }
        finite_opt(Some(ma30 / ma300))
    }

    fn compute_factor_053(depth: &CnDepthStats5) -> Option<f64> {
        let (_, bid0v) = depth.best_bid();
        let (_, ask0v) = depth.best_ask();
        let top5 = depth.sum_bid_amount(CN_DEPTH_LEVELS) + depth.sum_ask_amount(CN_DEPTH_LEVELS);
        if top5.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid0v + ask0v) / top5))
    }

    fn compute_factor_054(depth: &CnDepthStats5) -> Option<f64> {
        let mut num = 0.0;
        let mut den = 0.0;
        for i in 0..CN_DEPTH_LEVELS {
            let bidp = depth_level_price(&depth.bids, i);
            let bidv = depth_level_amount(&depth.bids, i);
            let askp = depth_level_price(&depth.asks, i);
            let askv = depth_level_amount(&depth.asks, i);
            num += bidp * bidv + askp * askv;
            den += bidv + askv;
        }
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        let vwap = num / den;
        let (bid0p, _) = depth_best_bid(depth);
        let (ask0p, _) = depth_best_ask(depth);
        let mid = (bid0p + ask0p) / 2.0;
        finite_opt(Some(mid - vwap))
    }

    fn compute_factor_055(series: &CnSeries<'_>) -> Option<f64> {
        rolling_sum_last_with_min_periods(&series.outer_bid_amount, 3, 3)
            .ok()
            .flatten()
    }

    fn compute_factor_056(series: &CnSeries<'_>) -> Option<f64> {
        rolling_sum_last_with_min_periods(&series.outer_ask_amount, 3, 3)
            .ok()
            .flatten()
    }

    fn compute_factor_057(series: &CnSeries<'_>) -> Option<f64> {
        finite_opt(series.total_volume5_sum.last().copied())
    }

    fn compute_factor_058(depth: &CnDepthStats5) -> Option<f64> {
        finite_opt(Some(depth.sum_bid_amount(CN_DEPTH_LEVELS)))
    }

    fn compute_factor_059(depth: &CnDepthStats5) -> Option<f64> {
        let ask = depth.sum_ask_amount(CN_DEPTH_LEVELS);
        let total = ask + depth.sum_bid_amount(CN_DEPTH_LEVELS);
        if total.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(ask / total))
    }

    fn compute_factor_060(series: &CnSeries<'_>) -> Option<f64> {
        pct_change_last(&series.mean_bid_amount5, 10)
    }

    fn compute_factor_061(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.ask_mean_amount5.len();
        if n <= 10 {
            return None;
        }
        let curr = series.ask_mean_amount5[n - 1];
        let prev = series.ask_mean_amount5[n - 11];
        if !curr.is_finite() || !prev.is_finite() {
            return None;
        }
        if prev.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((curr - prev) / prev))
    }

    fn compute_factor_062(depth: &CnDepthStats5) -> Option<f64> {
        let mut weighted_sum = 0.0;
        let mut weight_sum = 0.0;
        for i in 0..CN_DEPTH_LEVELS {
            let weight = depth_level_amount(&depth.bids, i) + depth_level_amount(&depth.asks, i);
            let spread = depth_level_price(&depth.asks, i) - depth_level_price(&depth.bids, i);
            if !weight.is_finite() || !spread.is_finite() {
                return None;
            }
            weighted_sum += spread * weight;
            weight_sum += weight;
        }
        if weight_sum.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(weighted_sum / weight_sum))
    }

    fn compute_factor_063(depth: &CnDepthStats5) -> Option<f64> {
        (0..CN_DEPTH_LEVELS)
            .map(|i| finite_opt(Some(depth_level_amount(&depth.bids, i))))
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .max_by(|a, b| a.total_cmp(b))
    }

    fn compute_factor_064(depth: &CnDepthStats5) -> Option<f64> {
        (0..CN_DEPTH_LEVELS)
            .map(|i| finite_opt(Some(depth_level_amount(&depth.asks, i))))
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .max_by(|a, b| a.total_cmp(b))
    }

    fn compute_factor_065(depth: &CnDepthStats5) -> Option<f64> {
        (0..CN_DEPTH_LEVELS)
            .map(|i| finite_opt(Some(depth_level_amount(&depth.bids, i))))
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .min_by(|a, b| a.total_cmp(b))
    }

    fn compute_factor_066(depth: &CnDepthStats5) -> Option<f64> {
        (0..CN_DEPTH_LEVELS)
            .map(|i| finite_opt(Some(depth_level_amount(&depth.asks, i))))
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .min_by(|a, b| a.total_cmp(b))
    }

    fn compute_factor_067(depth: &CnDepthStats5) -> Option<f64> {
        finite_opt(Some(
            depth.sum_bid_amount(CN_DEPTH_LEVELS) - depth.sum_ask_amount(CN_DEPTH_LEVELS),
        ))
    }

    fn compute_factor_068(depth: &CnDepthStats5) -> Option<f64> {
        let bid = depth.sum_bid_amount(CN_DEPTH_LEVELS);
        let ask = depth.sum_ask_amount(CN_DEPTH_LEVELS);
        if ask.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid / ask))
    }

    fn compute_factor_069(depth: &CnDepthStats5) -> Option<f64> {
        let value = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i).sqrt())
            .sum::<f64>();
        finite_opt(Some(value))
    }

    fn compute_factor_070(depth: &CnDepthStats5) -> Option<f64> {
        let value = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i).sqrt())
            .sum::<f64>();
        finite_opt(Some(value))
    }

    fn compute_factor_073(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        std_pop(&bids)
    }

    fn compute_factor_074(depth: &CnDepthStats5) -> Option<f64> {
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        std_pop(&asks)
    }

    fn compute_factor_075(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let mean = bids.iter().sum::<f64>() / bids.len() as f64;
        if mean.abs() <= 1e-12 {
            return Some(0.0);
        }
        let std = std_pop(&bids)?;
        finite_opt(Some(std / mean))
    }

    fn compute_factor_076(depth: &CnDepthStats5) -> Option<f64> {
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        let mean = asks.iter().sum::<f64>() / asks.len() as f64;
        if mean.abs() <= 1e-12 {
            return Some(0.0);
        }
        let std = std_pop(&asks)?;
        finite_opt(Some(std / mean))
    }

    fn compute_factor_077(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        strict_corr_last_slices(&bids, &asks, CN_DEPTH_LEVELS)
    }

    fn compute_factor_079(depth: &CnDepthStats5) -> Option<f64> {
        let ask = depth.sum_ask_amount(CN_DEPTH_LEVELS);
        let total = ask + depth.sum_bid_amount(CN_DEPTH_LEVELS);
        if total.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(ask / total))
    }

    fn compute_factor_080(series: &CnSeries<'_>) -> Option<f64> {
        sample_std_last(&series.outer_level_volume, 3, 3)
    }

    fn compute_factor_085(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        sample_cov(&bids, &asks)
    }

    fn compute_factor_086(depth: &CnDepthStats5) -> Option<f64> {
        let bid = depth_sum_amount(&depth.bids, CN_DEPTH_LEVELS);
        let ask = depth_sum_amount(&depth.asks, CN_DEPTH_LEVELS);
        finite_opt(Some(bid - ask))
    }

    fn compute_factor_087(depth: &CnDepthStats5) -> Option<f64> {
        let bid = depth_sum_amount(&depth.bids, CN_DEPTH_LEVELS);
        let ask = depth_sum_amount(&depth.asks, CN_DEPTH_LEVELS);
        if ask.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid / ask))
    }

    fn compute_factor_088(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let std = std_pop(&bids)?;
        finite_opt(Some(std * std))
    }

    fn compute_factor_089(depth: &CnDepthStats5) -> Option<f64> {
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        let std = std_pop(&asks)?;
        finite_opt(Some(std * std))
    }

    fn compute_factor_091(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        let bid_std = std_pop(&bids)?;
        let ask_std = std_pop(&asks)?;
        if ask_std.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid_std / ask_std))
    }

    fn compute_factor_093(series: &CnSeries<'_>) -> Option<f64> {
        tail_quantile_last(&series.avg_ask_price3, 360, 0.5)
    }

    fn compute_factor_094(series: &CnSeries<'_>) -> Option<f64> {
        tail_quantile_last(&series.avg_ask_price3, 100, 0.1)
    }

    fn compute_factor_095(depth: &CnDepthStats5) -> Option<f64> {
        median_from_iter((0..CN_DEPTH_LEVELS).map(|i| depth_level_amount(&depth.bids, i)))
    }

    fn compute_factor_096(depth: &CnDepthStats5) -> Option<f64> {
        median_from_iter((0..CN_DEPTH_LEVELS).map(|i| depth_level_amount(&depth.asks, i)))
    }

    fn compute_factor_097(depth: &CnDepthStats5) -> Option<f64> {
        let mut vals = Vec::with_capacity(CN_DEPTH_LEVELS);
        for i in 0..CN_DEPTH_LEVELS {
            let b = depth_level_amount(&depth.bids, i);
            let a = depth_level_amount(&depth.asks, i);
            if !b.is_finite() || !a.is_finite() || a.abs() <= 1e-12 {
                return None;
            }
            vals.push(b / a);
        }
        if vals.is_empty() {
            Some(0.0)
        } else {
            Some(vals.iter().sum::<f64>() / vals.len() as f64)
        }
    }

    fn compute_factor_102(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        if bids.iter().any(|v| !v.is_finite() || *v <= 0.0) {
            return None;
        }
        let den = bids.iter().map(|v| 1.0 / v).sum::<f64>();
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(CN_DEPTH_LEVELS as f64 / den))
    }

    fn compute_factor_103(depth: &CnDepthStats5) -> Option<f64> {
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        harmonic_mean(&asks)
    }

    fn compute_factor_107(depth: &CnDepthStats5) -> Option<f64> {
        let prices: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_price(&depth.bids, i))
            .collect();
        let vols: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        strict_corr_last_slices(&prices, &vols, CN_DEPTH_LEVELS)
    }

    fn compute_factor_108(depth: &CnDepthStats5) -> Option<f64> {
        let prices: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_price(&depth.asks, i))
            .collect();
        let vols: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        strict_corr_last_slices(&prices, &vols, CN_DEPTH_LEVELS)
    }

    fn compute_factor_110(depth: &CnDepthStats5) -> Option<f64> {
        let (bid0p, _) = depth_best_bid(depth);
        let (ask0p, _) = depth_best_ask(depth);
        let mid = (bid0p + ask0p) / 2.0;
        let mut sum = 0.0;
        for i in 0..CN_DEPTH_LEVELS {
            let v = depth_level_price(&depth.asks, i) - mid;
            if !v.is_finite() {
                return None;
            }
            sum += v;
        }
        finite_opt(Some(sum / CN_DEPTH_LEVELS as f64))
    }

    fn compute_factor_111(series: &CnSeries<'_>) -> Option<f64> {
        let std = sample_std_last(&series.outer_bid_price, 3, 3)?;
        finite_opt(Some(std * std))
    }

    fn compute_factor_113(series: &CnSeries<'_>) -> Option<f64> {
        last_opt(&series.factor_113_bid_price_pct_hmean)
    }

    fn compute_factor_114(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.factor_114_ask_price_pct_mean.len();
        if n < 2 {
            return None;
        }
        let curr =
            rolling_sum_at_opt_from_series(&series.factor_114_ask_price_pct_mean, n, 600, 600)
                .ok()
                .flatten()
                .and_then(|v| finite_opt(Some(v)))?;
        let prev =
            rolling_sum_at_opt_from_series(&series.factor_114_ask_price_pct_mean, n - 1, 600, 600)
                .ok()
                .flatten()
                .and_then(|v| finite_opt(Some(v)))?;
        finite_opt(Some(curr - prev))
    }

    fn compute_factor_115(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.bid_vwap5.len().min(series.ask_vwap5.len());
        if n < 2 {
            return None;
        }
        let mut pct = Vec::with_capacity(n);
        pct.push(None);
        for i in 1..n {
            let prev = series.bid_vwap5[i - 1] - series.ask_vwap5[i - 1];
            let curr = series.bid_vwap5[i] - series.ask_vwap5[i];
            if !prev.is_finite() || !curr.is_finite() || prev.abs() <= 1e-12 {
                pct.push(None);
                continue;
            }
            pct.push(finite_opt(Some((curr - prev) / prev)));
        }
        rolling_mean_last_opt_from_series(&pct, 60, 60)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_factor_116(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.mean_bid_price5.len();
        if n < 50 {
            return None;
        }
        let mut diff3 = Vec::with_capacity(n);
        for i in 0..n {
            if i >= 3 {
                diff3.push(series.mean_bid_price5[i] - series.mean_bid_price5[i - 3]);
            } else {
                diff3.push(f64::NAN);
            }
        }
        let tail_len = n.min(300);
        rank_last_average(&diff3[n - tail_len..], 50)
    }

    fn compute_factor_119(series: &CnSeries<'_>) -> Option<f64> {
        let last = *series.factor_119_mid_minus_ask_vwap3.last()?;
        let ma = rolling_mean_last(&series.factor_119_mid_minus_ask_vwap3, 120)
            .ok()
            .flatten()?;
        if ma.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(last / ma))
    }

    fn compute_factor_120(depth: &CnDepthStats5) -> Option<f64> {
        median_from_iter((0..CN_DEPTH_LEVELS).map(|i| depth_level_amount(&depth.bids, i)))
    }

    fn compute_factor_121(depth: &CnDepthStats5) -> Option<f64> {
        median_from_iter((0..CN_DEPTH_LEVELS).map(|i| depth_level_amount(&depth.asks, i)))
    }

    fn compute_factor_122(depth: &CnDepthStats5) -> Option<f64> {
        let top3 = depth.sum_ask_amount(3);
        let total5 = depth.sum_ask_amount(CN_DEPTH_LEVELS);
        if total5.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(top3 / total5))
    }

    fn compute_factor_123(depth: &CnDepthStats5) -> Option<f64> {
        let top3 = depth_sum_amount(&depth.bids, 3);
        let total5 = depth_sum_amount(&depth.bids, CN_DEPTH_LEVELS);
        if total5.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(top3 / total5))
    }

    fn compute_factor_124(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        let bid_var = std_pop(&bids)?;
        let ask_var = std_pop(&asks)?;
        let bid_var = bid_var * bid_var;
        let ask_var = ask_var * ask_var;
        if ask_var.abs() <= 1e-12 {
            return Some(f64::NAN);
        }
        finite_opt(Some(bid_var / ask_var))
    }

    fn compute_factor_125(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.median_all_price10.len();
        if n < 21 {
            return None;
        }
        let last = series.median_all_price10[n - 1];
        let prev15 = series.median_all_price10[n - 16];
        if !last.is_finite() || !prev15.is_finite() {
            return None;
        }
        if prev15.abs() <= 1e-12 {
            return Some(0.0);
        }
        let pct15 = (last - prev15) / prev15;
        let period = if pct15 < 0.0 { 5 } else { 20 };
        let prev = series.median_all_price10[n - 1 - period];
        if !prev.is_finite() {
            return None;
        }
        if prev.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((last - prev) / prev))
    }

    fn compute_factor_126(depth: &CnDepthStats5) -> Option<f64> {
        let mut vals = Vec::with_capacity(CN_DEPTH_LEVELS);
        for i in 0..CN_DEPTH_LEVELS {
            vals.push(depth_level_amount(&depth.bids, i) + depth_level_amount(&depth.asks, i));
        }
        let mean = vals.iter().sum::<f64>() / vals.len() as f64;
        if mean.abs() <= 1e-12 {
            return Some(0.0);
        }
        let std = std_pop(&vals)?;
        finite_opt(Some(std / mean))
    }

    fn compute_factor_128(series: &CnSeries<'_>) -> Option<f64> {
        rolling_mean_last_opt_from_series(&series.factor_128_skew, 300, 50)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_factor_129(depth: &CnDepthStats5) -> Option<f64> {
        let bids: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        weighted_harmonic_with_index_weights(&bids)
    }

    fn compute_factor_130(depth: &CnDepthStats5) -> Option<f64> {
        let asks: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        weighted_harmonic_with_index_weights(&asks)
    }

    fn compute_factor_133(series: &CnSeries<'_>) -> Option<f64> {
        let bid = *series.total_bid5.last()?;
        let ask = *series.total_ask5.last()?;
        let den = bid + ask;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid - ask) / den))
    }

    fn compute_factor_134(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .mid_price
            .len()
            .min(series.bid0v.len())
            .min(series.ask0v.len());
        if n < 300 {
            return None;
        }
        let mid_price: Vec<f64> = (0..n).map(|i| series.mid_price[i]).collect();
        let mid_vol: Vec<f64> = (0..n)
            .map(|i| (series.bid0v[i] + series.ask0v[i]) / 2.0)
            .collect();
        strict_corr_last_slices(&mid_price, &mid_vol, 300)
    }

    fn compute_factor_139(depth: &CnDepthStats5) -> Option<f64> {
        let mut sum = 0.0;
        for i in 0..CN_DEPTH_LEVELS {
            let v = depth_level_price(&depth.bids, i) - depth_level_price(&depth.asks, i);
            if !v.is_finite() {
                return None;
            }
            sum += v;
        }
        finite_opt(Some(sum / CN_DEPTH_LEVELS as f64))
    }

    fn compute_factor_144(depth: &CnDepthStats5) -> Option<f64> {
        let diffs: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_price(&depth.bids, i) - depth_level_price(&depth.asks, i))
            .collect();
        let mut sum = 0.0;
        for i in 1..diffs.len() {
            let v = diffs[i] - diffs[i - 1];
            if !v.is_finite() {
                return None;
            }
            sum += v;
        }
        finite_opt(Some(sum / (CN_DEPTH_LEVELS - 1) as f64))
    }

    fn compute_factor_151(series: &CnSeries<'_>) -> Option<f64> {
        rolling_skew_last(&series.total_volume5_sum, 45, false)
            .ok()
            .flatten()
    }

    fn compute_factor_152(series: &CnSeries<'_>) -> Option<f64> {
        last_opt(&series.factor_152_pct_mean)
    }

    fn compute_factor_156(depth: &CnDepthStats5) -> Option<f64> {
        let bid_prices: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_price(&depth.bids, i))
            .collect();
        let ask_prices: Vec<f64> = (0..CN_DEPTH_LEVELS)
            .map(|i| depth_level_price(&depth.asks, i))
            .collect();
        let bid_std = std_pop(&bid_prices)?;
        let ask_std = std_pop(&ask_prices)?;
        finite_opt(Some(bid_std - ask_std))
    }

    fn compute_factor_159(depth: &CnDepthStats5) -> Option<f64> {
        let bid = depth.mean_bid_price(CN_DEPTH_LEVELS);
        let ask = depth.mean_ask_price(CN_DEPTH_LEVELS);
        finite_opt(Some(bid - ask))
    }

    fn compute_factor_160(series: &CnSeries<'_>) -> Option<f64> {
        last_opt(&series.factor_160_pct_change_mean)
    }

    fn compute_factor_164(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.bid0v.len();
        if n < 2 {
            return None;
        }
        finite_opt(Some(series.bid0v[n - 1] - series.bid0v[n - 2]))
    }

    fn compute_factor_165(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.ask0v.len();
        if n < 2 {
            return None;
        }
        finite_opt(Some(series.ask0v[n - 1] - series.ask0v[n - 2]))
    }

    fn compute_factor_166(depth: &CnDepthStats5) -> Option<f64> {
        let bid = depth.sum_bid_amount(CN_DEPTH_LEVELS);
        let ask = depth.sum_ask_amount(CN_DEPTH_LEVELS);
        if !bid.is_finite() || !ask.is_finite() {
            return None;
        }
        let total = bid + ask;
        if total <= 1e-12 {
            return None;
        }
        finite_opt(Some((bid - ask) / total))
    }

    fn compute_factor_168(series: &CnSeries<'_>) -> Option<f64> {
        let spread = *series.spread.last()?;
        let mid = *series.mid_price.last()?;
        if mid.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(spread / mid))
    }

    fn compute_factor_170(series: &CnSeries<'_>) -> Option<f64> {
        pct_change_last(&series.mid_price, 120)
    }

    fn compute_factor_175(series: &CnSeries<'_>) -> Option<f64> {
        sample_std_last(&series.spread, 5, 1)
    }

    fn compute_factor_176(depth: &CnDepthStats5) -> Option<f64> {
        let (_, bid0v) = depth_best_bid(depth);
        let (_, ask0v) = depth_best_ask(depth);
        if !bid0v.is_finite() || !ask0v.is_finite() {
            return None;
        }
        if ask0v.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid0v / ask0v))
    }

    fn compute_td_ti_010(series: &CnSeries<'_>) -> Option<f64> {
        let ema1 = rolling_mean_series(&series.close, 30, 30).ok()?;
        let ema2 = rolling_mean_series_opt(&ema1, 30, 30).ok()?;
        rolling_mean_last_opt_from_series(&ema2, 30, 30)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_td_ti_015(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.open.len())
            .min(series.high.len())
            .min(series.low.len());
        if n < 39 {
            return None;
        }
        let ratio1: Vec<f64> = (0..n)
            .map(|i| {
                let den = series.open[i];
                if den.abs() > 1e-12 {
                    series.close[i] / den
                } else {
                    f64::NAN
                }
            })
            .collect();
        let ratio2: Vec<f64> = (0..n)
            .map(|i| {
                let den = series.low[i];
                if den.abs() > 1e-12 {
                    series.high[i] / den
                } else {
                    f64::NAN
                }
            })
            .collect();
        let mut sum = 0.0;
        for j in n - 20..n {
            let mb = sample_std_last(&ratio1[..=j], 20, 20)?;
            let sd = sample_std_last(&ratio2[..=j], 20, 20)?;
            sum += mb - sd;
        }
        finite_opt(Some(sum))
    }

    fn compute_td_ti_031(series: &CnSeries<'_>) -> Option<f64> {
        corr_last_with_min_periods(&series.large_order, &series.small_order, 60, 10)
    }

    fn compute_td_ti_036(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.high.len();
        let period = 14usize;
        if n < period + 1 {
            return None;
        }
        let start = n - (period + 1);
        let mut argmax = 0usize;
        let mut best = f64::NEG_INFINITY;
        for i in start..n {
            let v = series.high[i];
            if v > best {
                best = v;
                argmax = i - start;
            }
        }
        finite_opt(Some(100.0 * argmax as f64 / period as f64))
    }

    fn compute_td_ti_037(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.low.len();
        let period = 14usize;
        if n < period + 1 {
            return None;
        }
        let start = n - (period + 1);
        let mut argmin = 0usize;
        let mut best = f64::INFINITY;
        for i in start..n {
            let v = series.low[i];
            if v < best {
                best = v;
                argmin = i - start;
            }
        }
        finite_opt(Some(100.0 * argmin as f64 / period as f64))
    }

    fn compute_td_mt_003(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len();
        if n < 90 {
            return None;
        }
        let mut sum = 0.0;
        for i in n - 30..n {
            sum += series.close[i] - series.close[i - 60];
        }
        finite_opt(Some(sum / 30.0))
    }

    fn compute_td_mt_008(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len());
        if n < 14 {
            return None;
        }
        let mut plus_dm = vec![0.0; n];
        let mut minus_dm = vec![0.0; n];
        let mut tr = vec![f64::NAN; n];
        for i in 1..n {
            let high = series.high[i];
            let previous_high = series.high[i - 1];
            let low = series.low[i];
            let previous_low = series.low[i - 1];
            let previous_close = series.close[i - 1];
            if !high.is_finite()
                || !previous_high.is_finite()
                || !low.is_finite()
                || !previous_low.is_finite()
                || !previous_close.is_finite()
            {
                plus_dm[i] = f64::NAN;
                minus_dm[i] = f64::NAN;
                tr[i] = f64::NAN;
                continue;
            }
            let hd = high - previous_high;
            let ld = low - previous_low;
            plus_dm[i] = if hd > ld { hd } else { 0.0 };
            minus_dm[i] = if ld > hd { ld } else { 0.0 };
            tr[i] = (high - low)
                .max((high - previous_close).abs())
                .max((low - previous_close).abs());
        }
        let plus_ma = rolling_mean_last(&plus_dm, 14).ok().flatten()?;
        let minus_ma = rolling_mean_last(&minus_dm, 14).ok().flatten()?;
        let tr_ma = rolling_mean_last_with_min_periods(&tr, 14, 14)
            .ok()
            .flatten()?;
        if tr_ma.abs() <= 1e-12 {
            return Some(0.0);
        }
        let plus_di = 100.0 * plus_ma / tr_ma;
        let minus_di = 100.0 * minus_ma / tr_ma;
        let den = plus_di + minus_di;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((plus_di - minus_di).abs() / den * 100.0))
    }

    fn compute_td_mt_014(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len();
        if n == 0 {
            return None;
        }
        let fast = rolling_mean_series(&series.close, 12, 1).ok()?;
        let slow = rolling_mean_series(&series.close, 26, 1).ok()?;
        let macd: Vec<Option<f64>> = (0..n)
            .map(|i| match (fast[i], slow[i]) {
                (Some(f), Some(s)) => finite_opt(Some(f - s)),
                _ => Some(0.0),
            })
            .collect();
        let signal = rolling_mean_series_opt(&macd, 9, 1).ok()?;
        let m = last_opt(&macd)?;
        let s = last_opt(&signal)?;
        finite_opt(Some(m - s))
    }

    fn compute_td_mt_015(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len());
        if n < 15 {
            return None;
        }
        let mut minus_dm = vec![0.0; n];
        let mut tr = vec![f64::NAN; n];
        for i in 1..n {
            let high = series.high[i];
            let previous_high = series.high[i - 1];
            let low = series.low[i];
            let previous_low = series.low[i - 1];
            let previous_close = series.close[i - 1];
            if !high.is_finite()
                || !previous_high.is_finite()
                || !low.is_finite()
                || !previous_low.is_finite()
                || !previous_close.is_finite()
            {
                minus_dm[i] = f64::NAN;
                tr[i] = f64::NAN;
                continue;
            }
            let hd = high - previous_high;
            let ld = low - previous_low;
            minus_dm[i] = if ld > hd { ld } else { 0.0 };
            tr[i] = (high - low)
                .max((high - previous_close).abs())
                .max((low - previous_close).abs());
        }
        let Some(m) = rolling_mean_last(&minus_dm, 14).ok().flatten() else {
            return Some(f64::NAN);
        };
        let Some(t) = rolling_mean_last_with_min_periods(&tr, 14, 14)
            .ok()
            .flatten()
        else {
            return Some(f64::NAN);
        };
        if t.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(100.0 * m / t))
    }

    fn compute_td_mt_039(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len().min(series.low.len());
        if n < 10 {
            return None;
        }
        let mut sum = 0.0;
        for i in n - 10..n {
            sum += series.close[i] - series.low[i];
        }
        finite_opt(Some(sum))
    }

    fn compute_td_vi_010(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.high.len().min(series.low.len());
        if n < 2 {
            return None;
        }
        if !series.high[n - 1].is_finite()
            || !series.high[n - 2].is_finite()
            || !series.low[n - 1].is_finite()
            || !series.low[n - 2].is_finite()
        {
            return None;
        }
        let hd = series.high[n - 1] - series.high[n - 2];
        let ld = series.low[n - 1] - series.low[n - 2];
        finite_opt(Some(if hd > ld { hd } else { 0.0 }))
    }

    fn compute_td_vi_011(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.high.len().min(series.low.len());
        if n < 2 {
            return None;
        }
        let mut plus_dm = vec![0.0; n];
        for (i, v) in plus_dm.iter_mut().enumerate().take(n).skip(1) {
            if !series.high[i].is_finite()
                || !series.high[i - 1].is_finite()
                || !series.low[i].is_finite()
                || !series.low[i - 1].is_finite()
            {
                *v = f64::NAN;
                continue;
            }
            let hd = series.high[i] - series.high[i - 1];
            let ld = series.low[i] - series.low[i - 1];
            *v = if hd > ld { hd } else { 0.0 };
        }
        rolling_sum_last_with_min_periods(&plus_dm, 14, 1)
            .ok()
            .flatten()
    }

    fn compute_td_vi_025(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len());
        if n < 2 {
            return None;
        }
        let mut bp = vec![0.0; n];
        let mut tr = vec![0.0; n];
        for i in 1..n {
            bp[i] = series.close[i] - series.low[i].min(series.close[i - 1]);
            tr[i] = (series.high[i] - series.low[i])
                .max((series.high[i] - series.close[i - 1]).abs())
                .max((series.low[i] - series.close[i - 1]).abs());
        }
        let avg = |w: usize| -> Option<f64> {
            let b = rolling_sum_last_with_min_periods(&bp, w, 1)
                .ok()
                .flatten()?;
            let t = rolling_sum_last_with_min_periods(&tr, w, 1)
                .ok()
                .flatten()?;
            if t.abs() <= 1e-12 {
                Some(0.0)
            } else {
                Some(b / t)
            }
        };
        let a7 = avg(7)?;
        let a14 = avg(14)?;
        let a28 = avg(28)?;
        finite_opt(Some((4.0 * a7 + 2.0 * a14 + a28) / 7.0))
    }

    fn compute_td_vi_026(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len())
            .min(series.open.len());
        if n == 0 {
            return None;
        }
        let mut x = Vec::with_capacity(n);
        for i in 0..n {
            if !series.high[i].is_finite()
                || !series.low[i].is_finite()
                || !series.close[i].is_finite()
                || !series.open[i].is_finite()
            {
                x.push(f64::NAN);
                continue;
            }
            let upper = series.high[i] - series.close[i].max(series.open[i]);
            let lower = series.close[i].min(series.open[i]) - series.low[i];
            let body = (series.close[i] - series.open[i]).abs();
            x.push(upper - lower - body);
        }
        rolling_sum_last_with_min_periods(&x, 14, 1).ok().flatten()
    }

    fn compute_td_vi_028(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.close.len())
            .min(series.open.len());
        if n == 0 {
            return None;
        }
        let x: Vec<f64> = (0..n)
            .map(|i| {
                if !series.high[i].is_finite()
                    || !series.close[i].is_finite()
                    || !series.open[i].is_finite()
                {
                    f64::NAN
                } else {
                    series.high[i] - series.close[i].max(series.open[i])
                }
            })
            .collect();
        rolling_sum_last_with_min_periods(&x, 14, 1).ok().flatten()
    }

    fn compute_td_pt_001(series: &CnSeries<'_>) -> Option<f64> {
        finite_opt(series.close.last().copied().map(f64::sin))
    }

    fn compute_td_pt_002(series: &CnSeries<'_>) -> Option<f64> {
        finite_opt(series.close.last().copied().map(f64::cos))
    }

    fn compute_td_pt_010(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.high.len())
            .min(series.low.len());
        if n < 15 {
            return None;
        }
        let mid = (series.close[n - 1] + series.high[n - 1] + series.low[n - 1]) / 3.0;
        let mut llv_low = f64::INFINITY;
        let mut llv_high = f64::INFINITY;
        for i in n - 15..n {
            llv_low = f64::min(llv_low, series.low[i]);
            llv_high = f64::min(llv_high, series.high[i]);
        }
        let zc2 = mid - llv_high + llv_low;
        finite_opt(Some(series.close[n - 1] - zc2))
    }

    fn compute_td_pt_027(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len();
        if n < 14 {
            return None;
        }
        let start = n - 14;
        let mut idx = 0usize;
        let mut best = f64::INFINITY;
        for i in start..n {
            let v = series.close[i];
            if v < best {
                best = v;
                idx = i - start;
            }
        }
        Some(idx as f64)
    }

    fn compute_td_ci_007(series: &CnSeries<'_>) -> Option<f64> {
        let ma = rolling_mean_last_with_min_periods(&series.close, 30, 1)
            .ok()
            .flatten()?;
        finite_opt(Some(ma.sin()))
    }

    fn compute_td_ci_008(series: &CnSeries<'_>) -> Option<f64> {
        let ma = rolling_mean_last_with_min_periods(&series.close, 30, 1)
            .ok()
            .flatten()?;
        finite_opt(Some(ma.cos()))
    }

    fn compute_td_pr_001(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.open.len())
            .min(series.high.len())
            .min(series.low.len());
        if n == 0 {
            return None;
        }
        let i = n - 1;
        if !series.close[i].is_finite()
            || !series.open[i].is_finite()
            || !series.high[i].is_finite()
            || !series.low[i].is_finite()
        {
            return None;
        }
        let body = (series.close[i] - series.open[i]).abs();
        let upper = series.high[i] - series.close[i].max(series.open[i]);
        let lower = series.close[i].min(series.open[i]) - series.low[i];
        let inv_hammer = (upper > 2.0 * body) && (lower < 0.1 * body);
        Some(if inv_hammer { 1.0 } else { 0.0 })
    }

    fn compute_td_pr_002(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.open.len())
            .min(series.high.len())
            .min(series.low.len());
        if n == 0 {
            return None;
        }
        let i = n - 1;
        if !series.close[i].is_finite()
            || !series.open[i].is_finite()
            || !series.high[i].is_finite()
            || !series.low[i].is_finite()
        {
            return None;
        }
        let body = (series.close[i] - series.open[i]).abs();
        let upper = series.high[i] - series.close[i].max(series.open[i]);
        let lower = series.close[i].min(series.open[i]) - series.low[i];
        let hammer = (lower > 2.0 * body) && (upper < 0.1 * body);
        Some(if hammer { 1.0 } else { 0.0 })
    }

    fn compute_td_pr_007(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len().min(series.open.len());
        if n < 2 {
            return None;
        }
        let i = n - 1;
        if !series.close[i - 1].is_finite()
            || !series.open[i - 1].is_finite()
            || !series.close[i].is_finite()
            || !series.open[i].is_finite()
        {
            return None;
        }
        let prev_bull = series.close[i - 1] > series.open[i - 1];
        let open_gap = series.open[i] > series.close[i - 1];
        let prev_mid = series.open[i - 1] + (series.close[i - 1] - series.open[i - 1]) / 2.0;
        let close_below_mid = series.close[i] < prev_mid;
        let curr_bear = series.close[i] < series.open[i];
        Some(if prev_bull && open_gap && close_below_mid && curr_bear {
            1.0
        } else {
            0.0
        })
    }

    fn compute_td_pr_015(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .open
            .len()
            .min(series.low.len())
            .min(series.high.len());
        if n == 0 {
            return None;
        }
        let i = n - 1;
        if !series.open[i].is_finite() || !series.low[i].is_finite() || !series.high[i].is_finite()
        {
            return None;
        }
        Some(
            if series.open[i] == series.low[i] || series.open[i] == series.high[i] {
                100.0
            } else {
                0.0
            },
        )
    }

    fn compute_td_pr_016(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .open
            .len()
            .min(series.close.len())
            .min(series.volume.len());
        if n < 90 {
            return None;
        }
        let i = n - 1;
        let oc = series.open[i] - series.close[i];
        let var1 = if oc > 0.0 {
            -1.0
        } else if oc < 0.0 {
            1.0
        } else {
            0.0
        };
        let oc_ma = rolling_mean_last(
            &(0..n)
                .map(|k| series.open[k] - series.close[k])
                .collect::<Vec<_>>(),
            90,
        )
        .ok()
        .flatten()?;
        let vol_ma = rolling_mean_last(&series.volume, 90).ok().flatten()?;
        if oc_ma.abs() <= 1e-12 || vol_ma.abs() <= 1e-12 {
            return Some(0.0);
        }
        let var2 = oc / oc_ma;
        let var3 = series.volume[i] / vol_ma;
        finite_opt(Some(var1 * var2 * var3))
    }

    fn compute_td_pr_017(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .open
            .len()
            .min(series.close.len())
            .min(series.low.len())
            .min(series.high.len());
        if n == 0 {
            return None;
        }
        let i = n - 1;
        if !series.open[i].is_finite()
            || !series.close[i].is_finite()
            || !series.low[i].is_finite()
            || !series.high[i].is_finite()
        {
            return None;
        }
        let range = series.high[i] - series.low[i];
        let takuri = (series.open[i] == series.close[i])
            && (series.low[i] < series.open[i])
            && (series.close[i] - series.low[i] > 0.5 * range);
        Some(if takuri { 100.0 } else { 0.0 })
    }

    fn compute_tp_vpi_006(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len().min(series.volume.len());
        if n < 373 {
            return None;
        }
        let mut obv_delta = Vec::with_capacity(n);
        obv_delta.push(0.0);
        for i in 1..n {
            let d = series.close[i] - series.close[i - 1];
            let volume = series.volume[i];
            let v = if !d.is_finite() || !volume.is_finite() {
                f64::NAN
            } else if d > 0.0 {
                volume
            } else if d < 0.0 {
                -volume
            } else {
                0.0
            };
            obv_delta.push(v);
        }
        let start = n - 14;
        let mut obv_tail = Vec::with_capacity(14);
        for end in start + 1..=n {
            obv_tail.push(
                rolling_sum_at_from_series(&obv_delta, end, 360, 360)
                    .ok()
                    .flatten(),
            );
        }
        rolling_mean_last_opt_from_series(&obv_tail, 14, 14)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_tp_vpi_014(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.volume.len();
        if n < 39 {
            return None;
        }
        let start = n - 20;
        let mut ratio_tail = Vec::with_capacity(20);
        for end in start + 1..=n {
            let ratio = rolling_mean_at_from_series(&series.volume, end, 20, 20)
                .ok()
                .flatten()
                .and_then(|v| finite_opt(Some(v)))
                .and_then(|m| {
                    if m.abs() <= 1e-12 {
                        return Some(0.0);
                    }
                    finite_opt(Some(series.volume[end - 1] / m))
                });
            ratio_tail.push(ratio);
        }
        rolling_mean_last_opt_from_series(&ratio_tail, 20, 20)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_tp_vpi_017(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len())
            .min(series.volume.len());
        if n < 14 {
            return None;
        }
        let mut pos = vec![0.0; n];
        let mut neg = vec![0.0; n];
        let mut tp = Vec::with_capacity(n);
        for i in 0..n {
            tp.push((series.high[i] + series.low[i] + series.close[i]) / 3.0);
        }
        for i in 1..n {
            if !tp[i].is_finite()
                || !tp[i - 1].is_finite()
                || !series.close[i].is_finite()
                || !series.close[i - 1].is_finite()
                || !series.volume[i].is_finite()
            {
                pos[i] = f64::NAN;
                neg[i] = f64::NAN;
                continue;
            }
            let money_flow = tp[i] * series.volume[i];
            if series.close[i] - series.close[i - 1] > 0.0 {
                pos[i] = money_flow;
            } else if series.close[i] - series.close[i - 1] < 0.0 {
                neg[i] = money_flow;
            }
        }
        let pos_sum = rolling_sum_last_with_min_periods(&pos, 14, 14)
            .ok()
            .flatten()?;
        let neg_sum = rolling_sum_last_with_min_periods(&neg, 14, 14)
            .ok()
            .flatten()?;
        finite_opt(Some(pos_sum - neg_sum))
    }

    fn compute_td_ti_026(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.high.len())
            .min(series.low.len());
        if n == 0 {
            return None;
        }
        let close_last = series.close[n - 1];
        let close_prev = if n >= 2 {
            series.close[n - 2]
        } else {
            f64::NAN
        };
        let sar = if close_prev < close_last {
            series.high[n - 1]
        } else {
            series.low[n - 1]
        };
        let value = close_last - sar;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_td_ti_033(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.open.len().min(series.volume.len());
        if n < 100 {
            return None;
        }
        finite_opt(series.corr_open_volume_300_last).or_else(|| {
            let window = n.min(300);
            rolling_corr_last(&series.open, &series.volume, window, 1)
                .ok()
                .flatten()
        })
    }

    fn compute_td_ti_034(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.net_buy_large.len().min(series.net_buy_small.len());
        if n < 2 {
            return None;
        }
        let high_diff = series.net_buy_large[n - 1] - series.net_buy_large[n - 2];
        let low_diff = series.net_buy_small[n - 1] - series.net_buy_small[n - 2];
        let value = high_diff / low_diff;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_tp_vpi_004(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.high.len())
            .min(series.low.len())
            .min(series.volume.len());
        if n == 0 {
            return None;
        }

        let mut clv_x = Vec::with_capacity(n);
        for i in 0..n {
            let high = series.high[i];
            let low = series.low[i];
            let close = series.close[i];
            let volume = series.volume[i];
            if !high.is_finite() || !low.is_finite() || !close.is_finite() || !volume.is_finite() {
                clv_x.push(f64::NAN);
                continue;
            }
            let den = high - low;
            let clv = if den.abs() <= 1e-12 {
                0.0
            } else {
                ((close - low) - (high - close)) / den
            };
            clv_x.push(clv * volume);
        }
        let clv_x_opt: Vec<Option<f64>> = clv_x.into_iter().map(Some).collect();
        let ad = rolling_sum_series_opt(&clv_x_opt, 360, 360).ok()?;
        let fast = rolling_mean_series_opt(&ad, 12, 1).ok()?;
        let slow = rolling_mean_series_opt(&ad, 26, 1).ok()?;
        finite_opt(Some(last_opt(&fast)? - last_opt(&slow)?))
    }

    fn compute_tp_vpi_001(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.high.len())
            .min(series.low.len())
            .min(series.volume.len());
        if n == 0 {
            return None;
        }

        let mut clv_x = Vec::with_capacity(n);
        for i in 0..n {
            let den = series.high[i] - series.low[i];
            let raw = if den.abs() <= 1e-12 {
                0.0
            } else {
                ((series.close[i] - series.low[i]) - (series.high[i] - series.close[i])) / den
            };
            clv_x.push(if raw.is_finite() {
                raw * series.volume[i]
            } else {
                f64::NAN
            });
        }
        let clv_x_opt: Vec<Option<f64>> = clv_x
            .into_iter()
            .map(|v| if v.is_finite() { Some(v) } else { None })
            .collect();
        rolling_sum_last_opt_from_series(&clv_x_opt, 360, 360)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_tp_vpi_002(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.high.len())
            .min(series.low.len())
            .min(series.volume.len());
        if n == 0 {
            return None;
        }

        let mut clv_x = Vec::with_capacity(n);
        for i in 0..n {
            let den = series.high[i] - series.low[i];
            let raw = if den.abs() <= 1e-12 {
                0.0
            } else {
                ((series.close[i] - series.low[i]) - (series.high[i] - series.close[i])) / den
            };
            clv_x.push(if raw.is_finite() {
                raw * series.volume[i]
            } else {
                f64::NAN
            });
        }
        let clv_x_opt: Vec<Option<f64>> = clv_x
            .into_iter()
            .map(|v| if v.is_finite() { Some(v) } else { None })
            .collect();
        let start = n.saturating_sub(14);
        let mut ad_tail = Vec::with_capacity(n - start);
        for end in start + 1..=n {
            ad_tail.push(
                rolling_sum_at_opt_from_series(&clv_x_opt, end, 360, 360)
                    .ok()
                    .flatten(),
            );
        }
        rolling_mean_last_opt_from_series(&ad_tail, 14, 14)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_tp_vpi_005(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len().min(series.volume.len());
        if n == 0 {
            return None;
        }

        let mut obv_delta = Vec::with_capacity(n);
        obv_delta.push(0.0);
        for i in 1..n {
            let price_diff = series.close[i] - series.close[i - 1];
            let delta = if price_diff > 0.0 {
                series.volume[i]
            } else if price_diff < 0.0 {
                -series.volume[i]
            } else {
                0.0
            };
            obv_delta.push(delta);
        }
        rolling_sum_last_from_series(&obv_delta, 360, 360)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_tp_vpi_015(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.open.len())
            .min(series.volume.len());
        if n == 0 {
            return None;
        }
        let value = (series.close[n - 1] - series.open[n - 1]) / series.volume[n - 1];
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_td_pt_003(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.amount.len().min(series.volume.len());
        if n == 0 {
            return None;
        }

        let vwap: Vec<Option<f64>> = (0..n)
            .map(|i| {
                let vol = series.volume[i];
                if vol.abs() > 1e-12 {
                    let value = series.amount[i] / vol;
                    finite_opt(Some(value))
                } else {
                    Some(0.0)
                }
            })
            .collect();
        let numerator = rolling_mean_series_opt(&vwap, 6, 6).ok()?;
        let amount_roll = rolling_sum_series(&series.amount, 6, 6).ok()?;
        let volume_roll = rolling_sum_series(&series.volume, 6, 6).ok()?;

        let denominator: Vec<Option<f64>> = amount_roll
            .iter()
            .zip(volume_roll.iter())
            .map(|(a, v)| {
                let a = finite_opt(*a)?;
                let v = finite_opt(*v)?;
                if v.abs() > 1e-12 {
                    finite_opt(Some(a / v))
                } else {
                    Some(0.0)
                }
            })
            .collect();

        let apb: Vec<Option<f64>> = numerator
            .iter()
            .zip(denominator.iter())
            .map(|(num, den)| {
                let num = finite_opt(*num)?;
                let den = finite_opt(*den)?;
                if den.abs() <= 1e-12 {
                    return Some(0.0);
                }
                let ratio = num / den;
                if ratio > 0.0 {
                    finite_opt(Some(ratio.ln()))
                } else {
                    Some(0.0)
                }
            })
            .collect();

        rolling_mean_last_opt_from_series(&apb, 18, 18)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_td_pt_004(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.open.len())
            .min(series.high.len())
            .min(series.low.len())
            .min(series.volume.len());
        if n == 0 {
            return None;
        }

        let vol_ma = rolling_mean_series(&series.volume, 120, 120).ok()?;
        let var1: Vec<Option<f64>> = (0..n)
            .map(|i| {
                if !series.close[i].is_finite()
                    || !series.open[i].is_finite()
                    || !series.high[i].is_finite()
                    || !series.low[i].is_finite()
                    || !series.volume[i].is_finite()
                {
                    return Some(f64::NAN);
                }
                let hl = series.high[i] - series.low[i];
                let vol_ma_i = match vol_ma[i] {
                    Some(value) if value.is_finite() => value,
                    Some(_) => return Some(f64::NAN),
                    None => return None,
                };
                if hl.abs() <= 1e-12 || vol_ma_i.abs() <= 1e-12 {
                    return Some(0.0);
                }
                let value = (series.close[i] - series.open[i]) / hl * (series.volume[i] / vol_ma_i);
                finite_opt(Some(value))
            })
            .collect();

        let accum = rolling_sum_series_opt(&var1, 60, 60).ok()?;
        let sma_fast = rolling_mean_series_opt(&accum, 9, 1).ok()?;
        let sma_slow = rolling_mean_series_opt(&accum, 25, 1).ok()?;
        let fast = last_opt(&sma_fast)?;
        let slow = last_opt(&sma_slow)?;
        finite_opt(Some(fast - slow))
    }

    fn compute_td_ci_003(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len();
        if n == 0 {
            return None;
        }

        let mut close_diff: Vec<Option<f64>> = Vec::with_capacity(n);
        close_diff.push(None);
        for i in 1..n {
            close_diff.push(finite_opt(Some(series.close[i] - series.close[i - 1])));
        }

        let diff_sma = rolling_mean_series_opt(&close_diff, 30, 1).ok()?;
        let close_sma = rolling_mean_series(&series.close, 30, 1).ok()?;
        let phase: Vec<Option<f64>> = diff_sma
            .iter()
            .zip(close_sma.iter())
            .map(|(d, c)| {
                let d = finite_opt(*d)?;
                let c = finite_opt(*c)?;
                if c.abs() <= 1e-12 {
                    return Some(0.0);
                }
                finite_opt(Some((d / c).atan()))
            })
            .collect();
        let inv_phase: Vec<Option<f64>> = phase
            .iter()
            .map(|p| {
                let p = finite_opt(*p)?;
                if p.abs() <= 1e-12 {
                    return Some(0.0);
                }
                finite_opt(Some((2.0 * std::f64::consts::PI) / p))
            })
            .collect();
        rolling_mean_last_opt_from_series(&inv_phase, 30, 1)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_td_pr_005(series: &CnSeries<'_>) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.open.len())
            .min(series.high.len())
            .min(series.low.len());
        if n < 3 {
            return None;
        }

        let i = n - 1;
        if !series.close[i - 2].is_finite()
            || !series.open[i - 2].is_finite()
            || !series.close[i - 1].is_finite()
            || !series.open[i - 1].is_finite()
            || !series.high[i - 1].is_finite()
            || !series.low[i - 1].is_finite()
            || !series.close[i].is_finite()
            || !series.open[i].is_finite()
        {
            return None;
        }
        let small_body = (series.close[i - 1] - series.open[i - 1]).abs()
            < (series.high[i - 1] - series.low[i - 1]) * 0.1;
        let first_down = series.close[i - 2] < series.open[i - 2];
        let third_up = series.close[i] > series.open[i];
        let morning_star = first_down && small_body && third_up;
        Some(if morning_star { 1.0 } else { 0.0 })
    }

    fn compute_td_pr_006(series: &CnSeries<'_>) -> Option<f64> {
        let n = series.close.len().min(series.open.len());
        if n < 3 {
            return None;
        }
        let i = n - 1;
        if (i - 2..=i)
            .any(|index| !series.close[index].is_finite() || !series.open[index].is_finite())
        {
            return None;
        }
        let three_black_crows = (series.close[i - 2] < series.open[i - 2])
            && (series.close[i - 1] < series.open[i - 1])
            && (series.close[i] < series.open[i]);
        Some(if three_black_crows { 1.0 } else { 0.0 })
    }

    fn compute_factor_118(
        &mut self,
        symbol: &str,
        depth: &CnDepthStats5,
    ) -> Option<(f64, bool, usize)> {
        let state = self.symbol_states.entry(symbol.to_string()).or_default();
        Self::compute_factor_118_with_state(state, depth)
    }

    pub(crate) fn compute_factor_118_with_state(
        state: &mut CnCalcState,
        depth: &CnDepthStats5,
    ) -> Option<(f64, bool, usize)> {
        let Some(mid_price_diff) = compute_mid_price_minus_bid_vwap(depth) else {
            state.factor_118_mid_price_diffs.clear();
            return None;
        };
        state.push_mid_price_diff(mid_price_diff);

        let series: Vec<f64> = state.factor_118_mid_price_diffs.iter().copied().collect();
        let samples = series.len();
        let denom = rolling_mean_last(&series, FACTOR_118_WINDOW).ok().flatten();

        match denom {
            Some(ma) if ma.abs() > 1e-12 && ma.is_finite() => {
                let value = mid_price_diff / ma;
                if value.is_finite() {
                    Some((value, true, samples))
                } else {
                    Some((0.0, false, samples))
                }
            }
            _ => Some((0.0, false, samples)),
        }
    }
}

fn max_feature_msg_bytes(symbol: &str, feature_dim: usize) -> usize {
    4 + 4 + symbol.len() + 8 + 1 + 2 + feature_dim * 8
}

fn validate_symbol_factor_plan_payload_limits(
    symbol_factor_plans: &HashMap<String, CnFormulaPlan>,
) -> Result<()> {
    for (symbol, plan) in symbol_factor_plans {
        let bytes = max_feature_msg_bytes(symbol, plan.ordered_factors.len());
        if bytes > CN_FACTOR_PAYLOAD_MAX_BYTES {
            bail!(
                "symbol factor plan exceeds fusion IPC payload limit: symbol={} factors={} bytes={} limit={}",
                symbol,
                plan.ordered_factors.len(),
                bytes,
                CN_FACTOR_PAYLOAD_MAX_BYTES
            );
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct AmountThresholdResp {
    thresholds: HashMap<String, AmountThresholdItem>,
}

#[derive(Debug, Deserialize)]
struct AmountThresholdItem {
    medium_notional_threshold: f64,
    large_notional_threshold: f64,
}

async fn load_online_symbols_from_tlen_server(
    tlen: &TlenServerConfig,
    venue: TradingVenue,
    venue_slug: &str,
    config_type: &str,
) -> Result<HashSet<String>> {
    let base_url = tlen.base_url.trim_end_matches('/');
    let client = Client::builder()
        .timeout(Duration::from_millis(tlen.request_timeout_ms))
        .build()
        .context("build reqwest client for tlen_server failed")?;

    let url = format!("{}/api/thresholds", base_url);
    let resp = client
        .get(&url)
        .query(&[("venue", venue_slug), ("config_type", config_type)])
        .send()
        .await
        .with_context(|| format!("GET {} failed", url))?
        .error_for_status()
        .with_context(|| format!("GET {} returned error status", url))?;

    let payload: AmountThresholdResp = resp
        .json()
        .await
        .with_context(|| format!("decode amount threshold response failed: {}", url))?;

    let mut out = HashSet::with_capacity(payload.thresholds.len());
    for (raw_symbol, item) in payload.thresholds {
        let symbol = normalize_symbol_for_venue(&raw_symbol, venue);
        if !symbol.is_empty()
            && is_online_amount_threshold(
                item.medium_notional_threshold,
                item.large_notional_threshold,
            )
        {
            out.insert(symbol);
        }
    }
    Ok(out)
}

fn compute_mid_price_minus_bid_vwap(depth: &CnDepthStats5) -> Option<f64> {
    let bid1 = depth.bids.first()?;
    let ask1 = depth.asks.first()?;
    if !bid1.price.is_finite() || !ask1.price.is_finite() || bid1.price <= 0.0 || ask1.price <= 0.0
    {
        return None;
    }

    let mid_price = (bid1.price + ask1.price) / 2.0;
    if !mid_price.is_finite() || mid_price <= 0.0 {
        return None;
    }

    let mut sum_pxv = 0.0;
    let mut sum_v = 0.0;
    for level in depth.bids.iter().take(FACTOR_118_VWAP_LEVELS) {
        if !level.price.is_finite()
            || !level.amount.is_finite()
            || level.price <= 0.0
            || level.amount < 0.0
        {
            return None;
        }
        if level.amount > 0.0 {
            sum_pxv += level.price * level.amount;
            sum_v += level.amount;
        }
    }
    if sum_v <= 0.0 {
        return None;
    }

    let bid_vwap = sum_pxv / sum_v;
    let diff = mid_price - bid_vwap;
    if diff.is_finite() {
        Some(diff)
    } else {
        None
    }
}

pub fn depth_best_bid(depth: &CnDepthStats5) -> (f64, f64) {
    match depth.bids.first() {
        Some(level) => (level.price, level.amount),
        None => (f64::NAN, f64::NAN),
    }
}

pub fn depth_best_ask(depth: &CnDepthStats5) -> (f64, f64) {
    match depth.asks.first() {
        Some(level) => (level.price, level.amount),
        None => (f64::NAN, f64::NAN),
    }
}

pub fn depth_level_amount(levels: &[CnDepthLevel], idx: usize) -> f64 {
    levels
        .get(idx)
        .map(|level| level.amount)
        .unwrap_or(f64::NAN)
}

pub fn depth_level_price(levels: &[CnDepthLevel], idx: usize) -> f64 {
    levels.get(idx).map(|level| level.price).unwrap_or(f64::NAN)
}

pub fn depth_sum_amount(levels: &[CnDepthLevel], limit: usize) -> f64 {
    assert_native_depth_limit(levels, limit);
    strict_depth_sum(levels.iter().take(limit).map(|level| level.amount))
}

pub fn depth_mean_amount(levels: &[CnDepthLevel], limit: usize) -> f64 {
    assert_native_depth_limit(levels, limit);
    let mut sum = 0.0;
    let levels = &levels[..limit];
    if levels.is_empty() {
        return f64::NAN;
    }
    for level in levels {
        if !level.amount.is_finite() {
            return f64::NAN;
        }
        sum += level.amount;
    }
    sum / levels.len() as f64
}

pub fn depth_mean_price(levels: &[CnDepthLevel], limit: usize) -> f64 {
    assert_native_depth_limit(levels, limit);
    let mut sum = 0.0;
    let levels = &levels[..limit];
    if levels.is_empty() {
        return f64::NAN;
    }
    for level in levels {
        if !level.price.is_finite() {
            return f64::NAN;
        }
        sum += level.price;
    }
    sum / levels.len() as f64
}

pub fn depth_mean_pxv(levels: &[CnDepthLevel], limit: usize) -> f64 {
    assert_native_depth_limit(levels, limit);
    let mut sum = 0.0;
    let levels = &levels[..limit];
    if levels.is_empty() {
        return f64::NAN;
    }
    for level in levels {
        let v = level.price * level.amount;
        if !v.is_finite() {
            return f64::NAN;
        }
        sum += v;
    }
    sum / levels.len() as f64
}

pub fn depth_sum_price(levels: &[CnDepthLevel], limit: usize) -> f64 {
    assert_native_depth_limit(levels, limit);
    strict_depth_sum(levels.iter().take(limit).map(|level| level.price))
}

pub fn depth_vwap(levels: &[CnDepthLevel], limit: usize) -> Option<f64> {
    assert_native_depth_limit(levels, limit);
    let mut sum_pxv = 0.0;
    let mut sum_v = 0.0;
    for level in levels.iter().take(limit) {
        if !level.price.is_finite() || !level.amount.is_finite() {
            return None;
        }
        sum_pxv += level.price * level.amount;
        sum_v += level.amount;
    }
    if sum_v.abs() <= 1e-12 {
        return None;
    }
    let value = sum_pxv / sum_v;
    if value.is_finite() {
        Some(value)
    } else {
        None
    }
}

fn strict_prefix_add(prefix: f64, value: f64) -> f64 {
    if prefix.is_finite() && value.is_finite() {
        prefix + value
    } else {
        f64::NAN
    }
}

fn assert_native_depth_limit(levels: &[CnDepthLevel], limit: usize) {
    assert_eq!(
        levels.len(),
        CN_DEPTH_LEVELS,
        "CN formula received a non-native depth slice"
    );
    assert!(
        limit <= CN_DEPTH_LEVELS,
        "CN formula requested {limit} levels from a native {CN_DEPTH_LEVELS}-level book"
    );
}

fn strict_depth_sum(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut sum = 0.0;
    let mut count = 0usize;
    for value in values {
        if !value.is_finite() {
            return f64::NAN;
        }
        sum += value;
        count += 1;
    }
    if count == 0 {
        f64::NAN
    } else {
        sum
    }
}

fn tail_quantile_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    q: f64,
) -> Option<f64> {
    if window == 0 || values.len() < window || !(0.0..=1.0).contains(&q) {
        return None;
    }
    let start = values.len() - window;
    let mut tail = Vec::with_capacity(window);
    for i in start..values.len() {
        let v = values.value_at(i);
        if !v.is_finite() {
            return None;
        }
        tail.push(v);
    }
    if tail.is_empty() {
        return None;
    }
    tail.sort_by(|a, b| a.total_cmp(b));
    let n = tail.len();
    // linear interpolation (matches pandas default)
    let pos = (n - 1) as f64 * q;
    let lo = pos.floor() as usize;
    let hi = (lo + 1).min(n - 1);
    let frac = pos - lo as f64;
    let value = tail[lo] * (1.0 - frac) + tail[hi] * frac;
    if value.is_finite() {
        Some(value)
    } else {
        None
    }
}

fn sample_std_last(
    values: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Option<f64> {
    if window == 0 || min_periods == 0 || values.len() < min_periods {
        return None;
    }
    let start = values.len().saturating_sub(window);
    let mut tail = Vec::with_capacity(values.len() - start);
    for i in start..values.len() {
        let v = values.value_at(i);
        if !v.is_finite() {
            return None;
        }
        tail.push(v);
    }
    if tail.len() < min_periods || tail.len() < 2 {
        return None;
    }
    let mean = tail.iter().sum::<f64>() / tail.len() as f64;
    let var = tail
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / (tail.len() as f64 - 1.0);
    let out = var.sqrt();
    if out.is_finite() {
        Some(out)
    } else {
        None
    }
}

fn rank_last_average(values: &[f64], min_periods: usize) -> Option<f64> {
    if values.len() < min_periods {
        return None;
    }
    let last = *values.last()?;
    if !last.is_finite() {
        return None;
    }
    let mut lt = 0usize;
    let mut eq = 0usize;
    for v in values {
        if !v.is_finite() {
            return None;
        }
        if *v < last {
            lt += 1;
        } else if (*v - last).abs() <= 1e-12 {
            eq += 1;
        }
    }
    if eq == 0 {
        return None;
    }
    Some(lt as f64 + (eq as f64 + 1.0) / 2.0)
}

fn corr_last_with_min_periods(
    xs: &(impl F64SeriesView + ?Sized),
    ys: &(impl F64SeriesView + ?Sized),
    window: usize,
    min_periods: usize,
) -> Option<f64> {
    if window == 0 || min_periods == 0 {
        return None;
    }
    let n = xs.len().min(ys.len());
    if n < min_periods {
        return None;
    }
    let start = n.saturating_sub(window);
    let mut x = Vec::new();
    let mut y = Vec::new();
    for i in start..n {
        let xv = xs.value_at(i);
        let yv = ys.value_at(i);
        if !xv.is_finite() || !yv.is_finite() {
            return None;
        }
        x.push(xv);
        y.push(yv);
    }
    if x.len() < min_periods {
        return None;
    }
    let mean_x = x.iter().sum::<f64>() / x.len() as f64;
    let mean_y = y.iter().sum::<f64>() / y.len() as f64;
    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for i in 0..x.len() {
        let dx = x[i] - mean_x;
        let dy = y[i] - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }
    if var_x.abs() <= 1e-12 || var_y.abs() <= 1e-12 {
        return Some(0.0);
    }
    let out = cov / (var_x.sqrt() * var_y.sqrt());
    finite_opt(Some(out))
}

fn strict_corr_last_slices(xs: &[f64], ys: &[f64], window: usize) -> Option<f64> {
    let n = xs.len().min(ys.len());
    if window == 0 || n < window {
        return None;
    }

    let start = n - window;
    let mut x = Vec::with_capacity(window);
    let mut y = Vec::with_capacity(window);
    for i in start..n {
        let xv = xs[i];
        let yv = ys[i];
        if !xv.is_finite() || !yv.is_finite() {
            return Some(f64::NAN);
        }
        x.push(xv);
        y.push(yv);
    }
    let mean_x = x.iter().sum::<f64>() / x.len() as f64;
    let mean_y = y.iter().sum::<f64>() / y.len() as f64;
    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for i in 0..x.len() {
        let dx = x[i] - mean_x;
        let dy = y[i] - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }
    if var_x.abs() <= 1e-12 || var_y.abs() <= 1e-12 {
        return Some(f64::NAN);
    }
    finite_opt(Some(cov / (var_x.sqrt() * var_y.sqrt()))).or(Some(f64::NAN))
}

fn std_pop(values: &[f64]) -> Option<f64> {
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let var_sum = values
        .iter()
        .map(|value| (*value - mean).powi(2))
        .sum::<f64>();
    finite_opt(Some((var_sum / values.len() as f64).sqrt()))
}

fn sample_cov(xs: &[f64], ys: &[f64]) -> Option<f64> {
    let n = xs.len().min(ys.len());
    if n < 2 {
        return None;
    }
    if xs[..n].iter().any(|value| !value.is_finite())
        || ys[..n].iter().any(|value| !value.is_finite())
    {
        return None;
    }
    let mean_x = xs[..n].iter().sum::<f64>() / n as f64;
    let mean_y = ys[..n].iter().sum::<f64>() / n as f64;
    let cov_sum = (0..n)
        .map(|index| (xs[index] - mean_x) * (ys[index] - mean_y))
        .sum::<f64>();
    finite_opt(Some(cov_sum / (n - 1) as f64))
}

fn harmonic_mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut denom = 0.0;
    for value in values {
        if !value.is_finite() || *value <= 0.0 {
            return None;
        }
        denom += 1.0 / *value;
    }
    if denom.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(values.len() as f64 / denom))
}

fn harmonic_mean_nonzero(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut denom = 0.0;
    for value in values {
        if !value.is_finite() {
            return Some(f64::NAN);
        }
        if value.abs() <= 1e-12 {
            return Some(0.0);
        }
        denom += 1.0 / *value;
    }
    if denom.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(values.len() as f64 / denom))
}

fn weighted_harmonic_with_index_weights(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut denom = 0.0;
    for (idx, value) in values.iter().enumerate() {
        if !value.is_finite() || *value <= 0.0 {
            return None;
        }
        denom += (idx + 1) as f64 / *value;
    }
    if denom.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(values.len() as f64 / denom))
}

fn cross_sectional_kurtosis(values: &[f64], fisher: bool, bias: bool) -> Option<f64> {
    if values.len() < 4 || values.iter().any(|v| !v.is_finite()) {
        return None;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let mut m2 = 0.0;
    let mut m4 = 0.0;
    for value in values {
        let d = *value - mean;
        let d2 = d * d;
        m2 += d2;
        m4 += d2 * d2;
    }
    let m2 = m2 / n;
    if m2.abs() <= 1e-12 {
        return Some(0.0);
    }
    let m4 = m4 / n;
    let g2 = m4 / (m2 * m2) - 3.0;
    let mut out = if bias {
        g2
    } else {
        if values.len() < 4 {
            return None;
        }
        ((n - 1.0) / ((n - 2.0) * (n - 3.0))) * ((n + 1.0) * g2 + 6.0)
    };
    if !fisher {
        out += 3.0;
    }
    finite_opt(Some(out))
}

fn cross_sectional_skew(values: &[f64], bias: bool) -> Option<f64> {
    if values.len() < 3 || values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let m2 = values
        .iter()
        .map(|value| (*value - mean).powi(2))
        .sum::<f64>()
        / n;
    if m2.abs() <= 1e-12 {
        return Some(0.0);
    }
    let m3 = values
        .iter()
        .map(|value| (*value - mean).powi(3))
        .sum::<f64>()
        / n;
    let mut skew = m3 / m2.powf(1.5);
    if !bias {
        skew *= (n * (n - 1.0)).sqrt() / (n - 2.0);
    }
    finite_opt(Some(skew))
}

fn median_from_iter<I>(iter: I) -> Option<f64>
where
    I: IntoIterator<Item = f64>,
{
    let mut values = Vec::new();
    for value in iter {
        if !value.is_finite() {
            return None;
        }
        values.push(value);
    }
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        finite_opt(Some((values[mid - 1] + values[mid]) / 2.0))
    } else {
        finite_opt(Some(values[mid]))
    }
}

pub fn push_with_limit(buf: &mut VecDeque<f64>, value: f64) {
    buf.push_back(value);
    if buf.len() > MAX_CN_HISTORY {
        buf.pop_front();
    }
}

pub fn push_opt_with_limit(buf: &mut VecDeque<Option<f64>>, value: Option<f64>) {
    buf.push_back(value);
    if buf.len() > MAX_CN_HISTORY {
        buf.pop_front();
    }
}

pub fn finite_opt(value: Option<f64>) -> Option<f64> {
    match value {
        Some(v) if v.is_finite() => Some(v),
        Some(_) | None => None,
    }
}

fn last_opt(values: &(impl OptF64SeriesView + ?Sized)) -> Option<f64> {
    if values.len() == 0 {
        return None;
    }
    finite_opt(values.value_at(values.len() - 1))
}

fn format_symbol_sample(symbols: &[String]) -> String {
    let sample: Vec<&str> = symbols.iter().take(5).map(|s| s.as_str()).collect();
    if sample.is_empty() {
        "[]".to_string()
    } else {
        format!("[{}]", sample.join(","))
    }
}

fn format_symbol_sample_set(symbols: &HashSet<String>) -> String {
    let mut sorted: Vec<String> = symbols.iter().cloned().collect();
    sorted.sort_unstable();
    format_symbol_sample(&sorted)
}

fn parse_trade_flow_symbol(data: &[u8]) -> Result<&str> {
    if data.len() < 8 {
        anyhow::bail!("trade_flow payload too short for header: {}", data.len());
    }

    let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if msg_type != TRADE_FLOW_FEATURE_MSG_TYPE {
        anyhow::bail!(
            "invalid trade_flow msg_type={}, expected={}",
            msg_type,
            TRADE_FLOW_FEATURE_MSG_TYPE
        );
    }

    let symbol_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let symbol_end = 8usize
        .checked_add(symbol_len)
        .context("trade_flow symbol end overflow")?;
    if symbol_end > data.len() {
        anyhow::bail!(
            "trade_flow symbol out of bounds: symbol_len={} payload_len={}",
            symbol_len,
            data.len()
        );
    }

    let symbol = std::str::from_utf8(&data[8..symbol_end]).context("trade_flow symbol utf8")?;
    if symbol.is_empty() {
        anyhow::bail!("trade_flow symbol is empty");
    }
    Ok(symbol)
}

fn parse_embedded_depth(msg: &TradeFlowFeatureMsg) -> Option<CnDepthSnapshot5> {
    if msg.values.len() != TRADE_FLOW_FEATURE_DIM + APPENDED_DEPTH_VALUES {
        return None;
    }

    let mut bids = [CnDepthLevel {
        price: 0.0,
        amount: 0.0,
    }; CN_DEPTH_LEVELS];
    let mut asks = [CnDepthLevel {
        price: 0.0,
        amount: 0.0,
    }; CN_DEPTH_LEVELS];
    let mut offset = TRADE_FLOW_FEATURE_DIM;
    for idx in 0..CN_DEPTH_LEVELS {
        let price = msg.values[offset];
        let amount = msg.values[offset + 1];
        offset += 2;
        bids[idx] = CnDepthLevel { price, amount };
    }

    for idx in 0..CN_DEPTH_LEVELS {
        let price = msg.values[offset];
        let amount = msg.values[offset + 1];
        offset += 2;
        asks[idx] = CnDepthLevel { price, amount };
    }

    Some(CnDepthSnapshot5 { bids, asks })
}

fn validate_native_depth(depth: &CnDepthSnapshot5) -> Result<(), String> {
    for (side, levels) in [("bid", &depth.bids), ("ask", &depth.asks)] {
        for (index, level) in levels.iter().enumerate() {
            if !level.price.is_nan() && (!level.price.is_finite() || level.price <= 0.0) {
                return Err(format!(
                    "cn_features {side}_price_{} must be NaN or finite and positive, got {}",
                    index + 1,
                    level.price
                ));
            }
            if !level.amount.is_nan() && (!level.amount.is_finite() || level.amount < 0.0) {
                return Err(format!(
                    "cn_features {side}_amount_{} must be NaN or finite and non-negative, got {}",
                    index + 1,
                    level.amount
                ));
            }
        }
    }

    let best_bid = depth.bids[0].price;
    let best_ask = depth.asks[0].price;
    if best_bid.is_finite() && best_ask.is_finite() && best_bid > best_ask {
        return Err(format!(
            "cn_features crossed best book: bid={best_bid} ask={best_ask}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const STRICT_MISSING_HISTORY: usize = 700;
    const STRICT_MISSING_RECOVERY_CHECKPOINTS: &[usize] = &[
        1, 2, 3, 4, 5, 6, 9, 13, 14, 19, 29, 59, 89, 119, 139, 179, 249, 299, 300, 359, 372, 373,
        499, 599, 600, 601,
    ];

    fn dummy_depth() -> CnDepthStats5 {
        let bids = std::array::from_fn(|i| CnDepthLevel {
            price: 100.0 - i as f64 * 0.1,
            amount: 10.0 + i as f64,
        });
        let asks = std::array::from_fn(|i| CnDepthLevel {
            price: 100.1 + i as f64 * 0.1,
            amount: 11.0 + i as f64,
        });
        CnDepthStats5::from_snapshot(&CnDepthSnapshot5 { bids, asks })
    }

    fn flat_depth() -> CnDepthStats5 {
        let bids = std::array::from_fn(|i| CnDepthLevel {
            price: 100.0 - i as f64 * 0.1,
            amount: 10.0,
        });
        let asks = std::array::from_fn(|i| CnDepthLevel {
            price: 100.1 + i as f64 * 0.1,
            amount: 10.0,
        });
        CnDepthStats5::from_snapshot(&CnDepthSnapshot5 { bids, asks })
    }

    fn varied_native_row(index: usize) -> ([f64; TRADE_FLOW_FEATURE_DIM], CnDepthSnapshot5) {
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

        let mut values = [0.0; TRADE_FLOW_FEATURE_DIM];
        values[FIELD_OPEN] = open;
        values[FIELD_HIGH] = high;
        values[FIELD_LOW] = low;
        values[FIELD_CLOSE] = close;
        values[FIELD_VOLUME] = volume;
        values[FIELD_AMOUNT] = amount;
        values[FIELD_AVG_AMOUNT] = amount / count;
        values[FIELD_COUNT] = count;
        values[FIELD_BUY_COUNT] = buy_count;
        values[FIELD_SELL_COUNT] = sell_count;
        values[FIELD_BUY_AMOUNT] = buy_amount;
        values[FIELD_SELL_AMOUNT] = sell_amount;
        values[FIELD_BUY_VOLUME] = buy_volume;
        values[FIELD_SELL_VOLUME] = sell_volume;
        values[FIELD_LARGE_ORDER] = amount * 0.45;
        values[FIELD_MEDIUM_ORDER] = amount * 0.35;
        values[FIELD_SMALL_ORDER] = amount * 0.20;
        values[FIELD_LARGE_BUY] = buy_amount * 0.45;
        values[FIELD_LARGE_SELL] = sell_amount * 0.45;
        values[FIELD_MEDIUM_BUY] = buy_amount * 0.35;
        values[FIELD_MEDIUM_SELL] = sell_amount * 0.35;
        values[FIELD_SMALL_BUY] = buy_amount * 0.20;
        values[FIELD_SMALL_SELL] = sell_amount * 0.20;
        values[FIELD_VWAP] = amount / volume / 10.0;
        values[FIELD_BUY_VWAP] = buy_amount / buy_volume / 10.0;
        values[FIELD_SELL_VWAP] = sell_amount / sell_volume / 10.0;
        values[FIELD_NET_BUY_AMOUNT] = buy_amount - sell_amount;
        values[FIELD_NET_BUY_VOLUME] = buy_volume - sell_volume;
        values[FIELD_NET_BUY_PCT] = (buy_volume - sell_volume) / volume;
        values[FIELD_NET_BUY_LARGE] = values[FIELD_LARGE_BUY] - values[FIELD_LARGE_SELL];
        values[FIELD_NET_BUY_MEDIUM] = values[FIELD_MEDIUM_BUY] - values[FIELD_MEDIUM_SELL];
        values[FIELD_NET_BUY_SMALL] = values[FIELD_SMALL_BUY] - values[FIELD_SMALL_SELL];

        let bids = std::array::from_fn(|level| CnDepthLevel {
            price: close - 0.5 - level as f64 * (0.08 + (index % 3) as f64 * 0.005),
            amount: 10.0 + level as f64 * 3.0 + (index % (level + 3)) as f64 * 0.7,
        });
        let asks = std::array::from_fn(|level| CnDepthLevel {
            price: close + 0.5 + level as f64 * (0.09 + (index % 4) as f64 * 0.004),
            amount: 8.0 + level as f64 * 2.0 + (index % (level + 4)) as f64 * 0.6,
        });
        (values, CnDepthSnapshot5 { bids, asks })
    }

    fn replay_branch_from(state: &CnReplayState) -> CnReplayState {
        CnReplayState {
            state: state.state.clone(),
            rolling: CnRollingStats::default(),
            latest_depth: state.latest_depth.clone(),
        }
    }

    fn assert_finite_missing_outputs_match(
        plan: &CnFormulaPlan,
        clean_values: Vec<f64>,
        missing_values: Vec<f64>,
        context: &str,
    ) {
        for ((binding, clean_value), missing_value) in plan
            .ordered_factors
            .iter()
            .zip(clean_values)
            .zip(missing_values)
        {
            if !missing_value.is_finite() {
                continue;
            }
            assert!(
                clean_value.is_finite(),
                "{} became finite only after {context}",
                binding.name
            );
            let tolerance = 1e-10 * clean_value.abs().max(1.0);
            assert!(
                (missing_value - clean_value).abs() <= tolerance,
                "{} consumed or skipped {context}: clean={clean_value} missing={missing_value}",
                binding.name
            );
        }
    }

    #[test]
    fn native_depth_aggregates_do_not_skip_missing_levels() {
        let mut depth = flat_depth();
        depth.bids[CN_DEPTH_LEVELS - 1].amount = f64::NAN;
        let snapshot = CnDepthSnapshot5 {
            bids: depth.bids,
            asks: depth.asks,
        };
        let depth = CnDepthStats5::from_snapshot(&snapshot);

        assert!(depth.sum_bid_amount(CN_INNER_DEPTH_LEVELS).is_finite());
        assert!(depth.sum_bid_amount(CN_DEPTH_LEVELS).is_nan());
        assert!(depth_level_amount(&depth.bids, CN_DEPTH_LEVELS).is_nan());
        assert!(compute_mid_price_minus_bid_vwap(&depth).is_none());

        let mut replay = CnReplayState::default();
        replay
            .push_native(1_000, &[0.0; TRADE_FLOW_FEATURE_DIM], Some(snapshot))
            .unwrap();
        let plan =
            CnFormulaPlan::from_factor_names("missing-level", vec!["factor_013".to_string()])
                .unwrap();
        assert!(replay.factor_values(&plan)[0].is_nan());
    }

    #[test]
    fn missing_book_advances_depth_history_without_blanketing_trade_history() {
        let (first_values, first_depth) = varied_native_row(0);
        let (second_values, _) = varied_native_row(1);
        let first_stats = CnDepthStats5::from_snapshot(&first_depth);
        let mut state = CnCalcState::default();

        state.push_native_bar(0, &first_values);
        state.push_optional_depth_stats(Some(&first_stats));
        state.push_native_bar(1_000, &second_values);
        state.push_optional_depth_stats(None);

        assert_eq!(state.close.len(), 2);
        assert!(state.close.back().unwrap().is_finite());
        assert_eq!(state.mid_price.len(), 2);
        assert!(state.mid_price.back().unwrap().is_nan());
        assert_eq!(state.factor_113_bid_price_pct_hmean.len(), 2);
        assert!(state
            .factor_113_bid_price_pct_hmean
            .back()
            .unwrap()
            .unwrap()
            .is_nan());
    }

    #[test]
    fn every_finite_factor_is_invariant_to_an_unread_missing_depth_field() {
        let factor_names: Vec<String> = CnFactorId::ALL
            .iter()
            .map(|factor| factor.as_name().to_string())
            .collect();
        let all_plan = CnFormulaPlan::from_factor_names("strict-missing", factor_names).unwrap();
        let factor_118_plan =
            CnFormulaPlan::from_factor_names("strict-missing", vec!["factor_118".to_string()])
                .unwrap();
        let mut warmed = CnReplayState::default();
        for index in 0..STRICT_MISSING_HISTORY {
            let (values, depth) = varied_native_row(index);
            warmed
                .push_native(index as i64 * 1_000, &values, Some(depth))
                .unwrap();
            let _ = warmed.factor_values(&factor_118_plan);
        }

        let final_index = STRICT_MISSING_HISTORY;
        for side in ["bid", "ask"] {
            for field in ["price", "amount"] {
                for level in 0..CN_DEPTH_LEVELS {
                    let (values, clean_depth) = varied_native_row(final_index);
                    let mut missing_depth = clean_depth.clone();
                    let target = match (side, field) {
                        ("bid", "price") => &mut missing_depth.bids[level].price,
                        ("bid", "amount") => &mut missing_depth.bids[level].amount,
                        ("ask", "price") => &mut missing_depth.asks[level].price,
                        ("ask", "amount") => &mut missing_depth.asks[level].amount,
                        _ => unreachable!(),
                    };
                    *target = f64::NAN;

                    let mut clean = replay_branch_from(&warmed);
                    clean
                        .push_native(
                            final_index as i64 * 1_000,
                            &values,
                            Some(clean_depth.clone()),
                        )
                        .unwrap();
                    let clean_values = clean.factor_values(&all_plan);

                    let mut missing = replay_branch_from(&warmed);
                    missing
                        .push_native(
                            final_index as i64 * 1_000,
                            &values,
                            Some(missing_depth.clone()),
                        )
                        .unwrap();
                    let missing_values = missing.factor_values(&all_plan);

                    for ((binding, clean_value), missing_value) in all_plan
                        .ordered_factors
                        .iter()
                        .zip(clean_values)
                        .zip(missing_values)
                    {
                        if !missing_value.is_finite() {
                            continue;
                        }
                        assert!(
                            clean_value.is_finite(),
                            "{} became finite only after {side}_{field}_{} was missing",
                            binding.name,
                            level + 1
                        );
                        let tolerance = 1e-10 * clean_value.abs().max(1.0);
                        assert!(
                            (missing_value - clean_value).abs() <= tolerance,
                            "{} consumed a partial book after {side}_{field}_{} was missing: clean={clean_value} missing={missing_value}",
                            binding.name,
                            level + 1
                        );
                    }

                    let mut clean_history = replay_branch_from(&warmed);
                    clean_history
                        .push_native(
                            final_index as i64 * 1_000,
                            &values,
                            Some(clean_depth.clone()),
                        )
                        .unwrap();
                    let _ = clean_history.factor_values(&factor_118_plan);

                    let mut missing_history = replay_branch_from(&warmed);
                    missing_history
                        .push_native(
                            final_index as i64 * 1_000,
                            &values,
                            Some(missing_depth.clone()),
                        )
                        .unwrap();
                    let _ = missing_history.factor_values(&factor_118_plan);

                    let final_recovery = *STRICT_MISSING_RECOVERY_CHECKPOINTS
                        .last()
                        .expect("strict missing recovery checkpoints");
                    for recovery in 1..=final_recovery {
                        let (next_values, next_depth) = varied_native_row(final_index + recovery);
                        clean_history
                            .push_native(
                                (final_index + recovery) as i64 * 1_000,
                                &next_values,
                                Some(next_depth.clone()),
                            )
                            .unwrap();
                        missing_history
                            .push_native(
                                (final_index + recovery) as i64 * 1_000,
                                &next_values,
                                Some(next_depth),
                            )
                            .unwrap();
                        if STRICT_MISSING_RECOVERY_CHECKPOINTS.contains(&recovery) {
                            let clean_values = clean_history.factor_values(&all_plan);
                            let missing_values = missing_history.factor_values(&all_plan);
                            assert_finite_missing_outputs_match(
                                &all_plan,
                                clean_values,
                                missing_values,
                                &format!(
                                    "{side}_{field}_{} missing {recovery} rows ago",
                                    level + 1
                                ),
                            );
                        } else {
                            let _ = clean_history.factor_values(&factor_118_plan);
                            let _ = missing_history.factor_values(&factor_118_plan);
                        }
                    }
                }
            }
        }

        let mut clean_history = replay_branch_from(&warmed);
        let mut missing_history = replay_branch_from(&warmed);
        let final_recovery = *STRICT_MISSING_RECOVERY_CHECKPOINTS
            .last()
            .expect("strict missing recovery checkpoints");
        for recovery in 0..=final_recovery {
            let index = final_index + recovery;
            let (values, depth) = varied_native_row(index);
            clean_history
                .push_native(index as i64 * 1_000, &values, Some(depth.clone()))
                .unwrap();
            missing_history
                .push_native(
                    index as i64 * 1_000,
                    &values,
                    (recovery != 0).then_some(depth),
                )
                .unwrap();
            if recovery == 0 || STRICT_MISSING_RECOVERY_CHECKPOINTS.contains(&recovery) {
                let clean_values = clean_history.factor_values(&all_plan);
                let missing_values = missing_history.factor_values(&all_plan);
                assert_finite_missing_outputs_match(
                    &all_plan,
                    clean_values,
                    missing_values,
                    &format!("whole book missing {recovery} rows ago"),
                );
            } else {
                let _ = clean_history.factor_values(&factor_118_plan);
                let _ = missing_history.factor_values(&factor_118_plan);
            }
        }
    }

    #[test]
    fn every_finite_factor_is_invariant_to_an_unread_missing_trade_field() {
        let factor_names: Vec<String> = CnFactorId::ALL
            .iter()
            .map(|factor| factor.as_name().to_string())
            .collect();
        let all_plan = CnFormulaPlan::from_factor_names("strict-missing", factor_names).unwrap();
        let factor_118_plan =
            CnFormulaPlan::from_factor_names("strict-missing", vec!["factor_118".to_string()])
                .unwrap();
        let mut warmed = CnReplayState::default();
        for index in 0..STRICT_MISSING_HISTORY {
            let (values, depth) = varied_native_row(index);
            warmed
                .push_native(index as i64 * 1_000, &values, Some(depth))
                .unwrap();
            let _ = warmed.factor_values(&factor_118_plan);
        }

        let missing_index = STRICT_MISSING_HISTORY;
        for field in 0..TRADE_FLOW_FEATURE_DIM {
            let (clean_row, depth) = varied_native_row(missing_index);
            let mut missing_row = clean_row;
            missing_row[field] = f64::NAN;
            let field_name = match field {
                FIELD_OPEN => "open",
                FIELD_HIGH => "high",
                FIELD_LOW => "low",
                FIELD_CLOSE => "close",
                FIELD_VOLUME => "volume",
                FIELD_AMOUNT => "amount",
                FIELD_AVG_AMOUNT => "avg_amount",
                FIELD_COUNT => "count",
                FIELD_BUY_COUNT => "buy_count",
                FIELD_SELL_COUNT => "sell_count",
                FIELD_BUY_AMOUNT => "buy_amount",
                FIELD_SELL_AMOUNT => "sell_amount",
                FIELD_BUY_VOLUME => "buy_volume",
                FIELD_SELL_VOLUME => "sell_volume",
                FIELD_LARGE_ORDER => "large_order",
                FIELD_MEDIUM_ORDER => "medium_order",
                FIELD_SMALL_ORDER => "small_order",
                FIELD_LARGE_BUY => "large_buy",
                FIELD_LARGE_SELL => "large_sell",
                FIELD_MEDIUM_BUY => "medium_buy",
                FIELD_MEDIUM_SELL => "medium_sell",
                FIELD_SMALL_BUY => "small_buy",
                FIELD_SMALL_SELL => "small_sell",
                FIELD_VWAP => "vwap",
                FIELD_BUY_VWAP => "buy_vwap",
                FIELD_SELL_VWAP => "sell_vwap",
                FIELD_NET_BUY_AMOUNT => "net_buy_amount",
                FIELD_NET_BUY_VOLUME => "net_buy_volume",
                FIELD_NET_BUY_PCT => "net_buy_pct",
                FIELD_NET_BUY_LARGE => "net_buy_large",
                FIELD_NET_BUY_MEDIUM => "net_buy_medium",
                FIELD_NET_BUY_SMALL => "net_buy_small",
                _ => unreachable!(),
            };

            let mut clean = replay_branch_from(&warmed);
            clean
                .push_native(
                    missing_index as i64 * 1_000,
                    &clean_row,
                    Some(depth.clone()),
                )
                .unwrap();
            let clean_values = clean.factor_values(&all_plan);
            let mut missing = replay_branch_from(&warmed);
            missing
                .push_native(missing_index as i64 * 1_000, &missing_row, Some(depth))
                .unwrap();
            let missing_values = missing.factor_values(&all_plan);
            assert_finite_missing_outputs_match(
                &all_plan,
                clean_values,
                missing_values,
                &format!("missing trade field {field_name}"),
            );

            let final_recovery = *STRICT_MISSING_RECOVERY_CHECKPOINTS
                .last()
                .expect("strict missing recovery checkpoints");
            for recovery in 1..=final_recovery {
                let index = missing_index + recovery;
                let (values, depth) = varied_native_row(index);
                clean
                    .push_native(index as i64 * 1_000, &values, Some(depth.clone()))
                    .unwrap();
                missing
                    .push_native(index as i64 * 1_000, &values, Some(depth))
                    .unwrap();
                if STRICT_MISSING_RECOVERY_CHECKPOINTS.contains(&recovery) {
                    let clean_values = clean.factor_values(&all_plan);
                    let missing_values = missing.factor_values(&all_plan);
                    assert_finite_missing_outputs_match(
                        &all_plan,
                        clean_values,
                        missing_values,
                        &format!("trade field {field_name} missing {recovery} rows ago"),
                    );
                } else {
                    let _ = clean.factor_values(&factor_118_plan);
                    let _ = missing.factor_values(&factor_118_plan);
                }
            }
        }
    }

    fn replay_message(ts: i64) -> TradeFlowFeatureMsg {
        let mut values = vec![1.0; TRADE_FLOW_FEATURE_DIM];
        for index in 0..CN_DEPTH_LEVELS {
            values.push(100.0 - index as f64 * 0.1);
            values.push(10.0 + index as f64);
        }
        for index in 0..CN_DEPTH_LEVELS {
            values.push(100.1 + index as f64 * 0.1);
            values.push(11.0 + index as f64);
        }
        TradeFlowFeatureMsg::from_indexed_values("BTCUSDT".to_string(), 1, ts, &values)
            .expect("replay message")
    }

    fn zero_activity_replay_message(ts: i64) -> TradeFlowFeatureMsg {
        let mut values = vec![0.0; TRADE_FLOW_FEATURE_DIM];
        for name in [
            "open",
            "high",
            "low",
            "close",
            "vwap",
            "buy_vwap",
            "sell_vwap",
        ] {
            let index = mkt_parsers::msg::trade_flow_feature_msg::TRADE_FLOW_FEATURE_FIELD_NAMES
                .iter()
                .position(|candidate| *candidate == name)
                .expect("trade-flow price field");
            values[index] = 100.0;
        }
        for index in 0..CN_DEPTH_LEVELS {
            values.push(100.0 - index as f64 * 0.1);
            values.push(10.0);
        }
        for index in 0..CN_DEPTH_LEVELS {
            values.push(100.1 + index as f64 * 0.1);
            values.push(10.0);
        }
        TradeFlowFeatureMsg::from_indexed_values("BTCUSDT".to_string(), 1, ts, &values)
            .expect("zero-activity replay message")
    }

    fn push_replay_message(state: &mut CnReplayState, msg: TradeFlowFeatureMsg) -> Result<()> {
        let values: [f64; TRADE_FLOW_FEATURE_DIM] = msg.values[..TRADE_FLOW_FEATURE_DIM]
            .try_into()
            .expect("trade-flow values");
        let depth = parse_embedded_depth(&msg);
        state.push_native(msg.ts, &values, depth)
    }

    fn message_with_depth_levels(levels: usize) -> TradeFlowFeatureMsg {
        let mut values = vec![1.0; TRADE_FLOW_FEATURE_DIM];
        for level in 0..levels {
            values.push(100.0 - level as f64 * 0.1);
            values.push(10.0 + level as f64);
        }
        for level in 0..levels {
            values.push(100.1 + level as f64 * 0.1);
            values.push(11.0 + level as f64);
        }
        TradeFlowFeatureMsg::from_indexed_values("AP2601".to_string(), 1, 1_000, &values)
            .expect("CN input message")
    }

    #[test]
    fn live_input_accepts_only_no_book_or_native_five_level_book() {
        assert!(CnFactorEngine::validate_trade_flow_shape(
            "xzce",
            "AP2601",
            &message_with_depth_levels(0),
        )
        .is_ok());
        assert!(CnFactorEngine::validate_trade_flow_shape(
            "xzce",
            "AP2601",
            &message_with_depth_levels(5),
        )
        .is_ok());
        for levels in [4, 6, 20] {
            let error = CnFactorEngine::validate_trade_flow_shape(
                "xzce",
                "AP2601",
                &message_with_depth_levels(levels),
            )
            .unwrap_err();
            assert!(error.contains("exactly 5 native levels"), "{error}");
        }
    }

    #[test]
    fn live_input_preserves_zero_and_missing_trade_fields() {
        let mut msg = zero_activity_replay_message(1_000);
        msg.values[FIELD_BUY_VWAP] = f64::NAN;
        assert!(CnFactorEngine::validate_trade_flow_shape("xzce", "AP2601", &msg).is_ok());

        let mut state = CnCalcState::default();
        state.push_trade_flow(&msg);
        assert_eq!(state.volume.back(), Some(&0.0));
        assert_eq!(state.amount.back(), Some(&0.0));
        assert_eq!(state.buy_volume.back(), Some(&0.0));
        assert!(state.buy_vwap.back().unwrap().is_nan());
    }

    #[test]
    fn live_input_rejects_invalid_finite_book_values_but_allows_nan() {
        let mut missing = message_with_depth_levels(5);
        missing.values[TRADE_FLOW_FEATURE_DIM] = f64::NAN;
        assert!(CnFactorEngine::validate_trade_flow_shape("xzce", "AP2601", &missing).is_ok());

        let mut infinite = message_with_depth_levels(5);
        infinite.values[TRADE_FLOW_FEATURE_DIM] = f64::INFINITY;
        assert!(CnFactorEngine::validate_trade_flow_shape("xzce", "AP2601", &infinite).is_err());
    }

    #[test]
    fn replay_factor_plan_returns_raw_values_and_advances_stateful_factor_118() {
        let plan = CnFormulaPlan::from_factor_names(
            "BTCUSDT",
            vec!["close".to_string(), "factor_118".to_string()],
        )
        .expect("factor plan");
        let mut state = CnReplayState::default();

        for index in 0..119 {
            push_replay_message(&mut state, replay_message(index * 5_000))
                .expect("push warm-up row");
            let values = state.factor_values(&plan);
            assert_eq!(values[0], 1.0);
            assert!(values[1].is_nan());
        }

        push_replay_message(&mut state, replay_message(119 * 5_000)).expect("push ready row");
        let values = state.factor_values(&plan);
        assert_eq!(values[0], 1.0);
        assert!(values[1].is_finite());
    }

    #[test]
    fn zero_activity_and_flat_bars_produce_finite_ready_factors() {
        let factor_names = vec![
            "TP_VPI_001".to_string(),
            "TP_VPI_002".to_string(),
            "TD_TI_034".to_string(),
            "TD_TI_035".to_string(),
            "TD_MT_008".to_string(),
            "TD_PR_016".to_string(),
            "TP_VPI_014".to_string(),
            "factor_021".to_string(),
            "baseline_149".to_string(),
        ];
        let plan = CnFormulaPlan::from_factor_names("BTCUSDT", factor_names.clone()).expect("plan");
        let mut state = CnReplayState::default();
        let mut values = Vec::new();

        for index in 0..500 {
            push_replay_message(&mut state, zero_activity_replay_message(index * 5_000))
                .expect("push flat row");
            values = state.factor_values(&plan);
        }

        for (name, value) in factor_names.iter().zip(values) {
            assert!(value.is_finite(), "{name} must be finite, got {value}");
        }
    }

    #[test]
    fn normalize_feature_values_waits_for_min_samples() {
        let cfg = ZscoreRuntimeConfig {
            window_size: 8,
            min_samples: 3,
            zscore_cap: 3.0,
        };
        let mut state = SymbolNormState::new(cfg.window_size, 0);

        assert!(normalize_feature_values(&mut state, &[1.0, 10.0], &cfg).is_none());
        assert!(normalize_feature_values(&mut state, &[2.0, 20.0], &cfg).is_none());

        let normalized = normalize_feature_values(&mut state, &[3.0, 30.0], &cfg).unwrap();
        assert_eq!(normalized.len(), 2);
        assert!(normalized.iter().all(|value| value.is_finite()));
        assert!(normalized[0] > 0.0);
        assert!(normalized[1] > 0.0);
    }

    #[test]
    fn normalize_feature_values_resets_on_dim_change() {
        let cfg = ZscoreRuntimeConfig {
            window_size: 8,
            min_samples: 2,
            zscore_cap: 3.0,
        };
        let mut state = SymbolNormState::new(cfg.window_size, 0);

        assert!(normalize_feature_values(&mut state, &[1.0, 2.0], &cfg).is_none());
        assert!(normalize_feature_values(&mut state, &[3.0], &cfg).is_none());

        let normalized = normalize_feature_values(&mut state, &[5.0], &cfg).unwrap();
        assert_eq!(normalized.len(), 1);
        assert!(normalized[0].is_finite());
        assert_eq!(state.welford_vec.len(), 1);
        assert_eq!(state.valid_sample_counts, vec![2]);
        assert_eq!(state.last_valid_values, vec![Some(5.0)]);
    }

    #[test]
    fn normalize_feature_values_skips_nan_and_reuses_last_valid() {
        let cfg = ZscoreRuntimeConfig {
            window_size: 8,
            min_samples: 2,
            zscore_cap: 3.0,
        };
        let mut state = SymbolNormState::new(cfg.window_size, 0);

        assert!(normalize_feature_values(&mut state, &[1.0, 10.0], &cfg).is_none());

        let normalized = normalize_feature_values(&mut state, &[2.0, 20.0], &cfg).unwrap();
        let second_before = normalized[1];

        let normalized = normalize_feature_values(&mut state, &[3.0, f64::NAN], &cfg).unwrap();
        assert_eq!(state.valid_sample_counts, vec![3, 2]);
        assert_eq!(state.welford_vec[0].len(), 3);
        assert_eq!(state.welford_vec[1].len(), 2);
        assert_eq!(state.last_valid_values, vec![Some(3.0), Some(20.0)]);
        assert!((normalized[1] - second_before).abs() < 1e-12);
        assert!(normalized.iter().all(|value| value.is_finite()));
    }

    #[test]
    fn factor_nan_paths_preserve_nan_except_explicit_neutral_cases() {
        let mut state = CnCalcState::default();
        let depth = flat_depth();

        state.bid_vwap5.extend([100.0, 100.0, 100.0]);
        state.total_bid5.extend([10.0; 10]);
        state.total_ask5.extend([10.0; 10]);
        state.avg_ask_price3.extend([100.0; 300]);
        state.bid0v.extend([10.0; 300]);
        state.ask0v.extend([10.0; 300]);
        state.mid_price.extend([100.0; 300]);

        let series = CnFactorEngine::build_symbol_series_from_state(&mut state);

        assert!(CnFactorEngine::compute_factor_001(&series)
            .unwrap()
            .is_nan());
        assert_eq!(CnFactorEngine::compute_factor_021(&series), Some(0.0));
        assert!(CnFactorEngine::compute_factor_052(&series)
            .unwrap()
            .is_nan());
        assert!(CnFactorEngine::compute_factor_077(&depth).unwrap().is_nan());
        assert!(CnFactorEngine::compute_factor_107(&depth).unwrap().is_nan());
        assert!(CnFactorEngine::compute_factor_108(&depth).unwrap().is_nan());
        assert!(CnFactorEngine::compute_factor_124(&depth).unwrap().is_nan());
        assert!(CnFactorEngine::compute_factor_134(&series)
            .unwrap()
            .is_nan());
    }

    #[test]
    fn td_mt_015_propagates_missing_ohlc_in_its_window() {
        let plan = CnFormulaPlan::from_factor_names("missing-td-mt-015", vec!["TD_MT_015".into()])
            .unwrap();
        let mut clean = CnReplayState::default();
        let mut missing = CnReplayState::default();

        for index in 0..20 {
            let (values, depth) = varied_native_row(index);
            let mut missing_values = values;
            if index == 19 {
                missing_values[FIELD_HIGH] = f64::NAN;
            }
            clean
                .push_native(index as i64 * 1_000, &values, Some(depth.clone()))
                .unwrap();
            missing
                .push_native(index as i64 * 1_000, &missing_values, Some(depth))
                .unwrap();
        }

        assert!(clean.factor_values(&plan)[0].is_finite());
        assert!(missing.factor_values(&plan)[0].is_nan());
    }

    #[test]
    fn harmonic_mean_nonzero_does_not_turn_missing_into_zero() {
        assert!(harmonic_mean_nonzero(&[1.0, f64::NAN]).unwrap().is_nan());
        assert_eq!(harmonic_mean_nonzero(&[1.0, 0.0]), Some(0.0));
    }

    #[test]
    fn level_one_depth_extra_factors_are_wired() {
        let depth = flat_depth();
        let expected = [
            ("bid0p_1", CnExtraFactorId::Bid0Price1, 100.0),
            ("bid0v_1", CnExtraFactorId::Bid0Volume1, 10.0),
            ("ask0p_1", CnExtraFactorId::Ask0Price1, 100.1),
            ("ask0v_1", CnExtraFactorId::Ask0Volume1, 10.0),
        ];

        for (name, extra_factor_id, expected_value) in expected {
            let binding = CnFactorBinding {
                name: name.to_string(),
                factor_id: None,
                extra_factor_id: Some(extra_factor_id),
            };
            let (value, ready, status) =
                CnFactorEngine::compute_supported_factor(&binding, None, Some(&depth), None)
                    .expect("level-one depth factor should be supported");
            assert!(ready, "{name} status={status}");
            assert!((value - expected_value).abs() < 1e-12, "{name}");
        }
    }

    #[test]
    fn newly_added_reference_factors_are_wired() {
        let expected = [
            CnFactorId::Factor002,
            CnFactorId::Factor008,
            CnFactorId::Factor012,
            CnFactorId::Factor020,
            CnFactorId::Factor026,
            CnFactorId::Factor028,
            CnFactorId::Factor036,
            CnFactorId::Factor040,
            CnFactorId::Factor042,
            CnFactorId::Factor043,
            CnFactorId::Factor049,
            CnFactorId::Factor053,
            CnFactorId::Factor058,
            CnFactorId::Factor059,
            CnFactorId::Factor062,
            CnFactorId::Factor064,
            CnFactorId::Factor067,
            CnFactorId::Factor068,
            CnFactorId::Factor069,
            CnFactorId::Factor070,
            CnFactorId::Factor073,
            CnFactorId::Factor079,
            CnFactorId::Factor080,
            CnFactorId::Factor085,
            CnFactorId::Factor091,
            CnFactorId::Factor093,
            CnFactorId::Factor095,
            CnFactorId::Factor103,
            CnFactorId::Factor111,
            CnFactorId::Factor113,
            CnFactorId::Factor114,
            CnFactorId::Factor115,
            CnFactorId::Factor120,
            CnFactorId::Factor122,
            CnFactorId::Factor125,
            CnFactorId::Factor129,
            CnFactorId::Factor130,
            CnFactorId::Factor159,
        ];

        let mut state = CnCalcState::default();
        let series = CnFactorEngine::build_symbol_series_from_state(&mut state);
        let depth = dummy_depth();

        for factor_id in expected {
            let binding = CnFactorBinding {
                name: factor_id.as_name().to_string(),
                factor_id: Some(factor_id),
                extra_factor_id: None,
            };
            assert!(
                CnFactorEngine::compute_supported_factor(
                    &binding,
                    None,
                    Some(&depth),
                    Some(&series),
                )
                .is_some(),
                "missing support for {}",
                factor_id.as_name()
            );
        }
    }

    #[test]
    fn requested_baselines_are_all_wired() {
        const REQUESTED: &str = "
            baseline_001, baseline_002, baseline_003, baseline_004, baseline_005,
            baseline_006, baseline_007, baseline_008, baseline_009, baseline_010,
            baseline_011, baseline_012, baseline_013, baseline_014, baseline_015,
            baseline_016, baseline_017, baseline_018, baseline_019, baseline_020,
            baseline_021, baseline_022, baseline_024, baseline_025,
            baseline_026, baseline_027, baseline_028, baseline_029,
            baseline_031, baseline_032, baseline_034, baseline_035,
            baseline_036, baseline_038, baseline_039, baseline_040,
            baseline_042, baseline_043, baseline_045,
            baseline_046, baseline_047, baseline_048, baseline_049,
            baseline_051, baseline_052, baseline_053, baseline_054, baseline_055,
            baseline_057, baseline_058, baseline_059, baseline_060,
            baseline_061, baseline_062, baseline_063, baseline_065,
            baseline_066, baseline_067, baseline_068, baseline_069, baseline_070,
            baseline_071, baseline_072, baseline_073, baseline_074, baseline_075,
            baseline_076, baseline_077, baseline_078, baseline_079, baseline_080,
            baseline_081, baseline_082, baseline_083, baseline_085,
            baseline_086, baseline_087, baseline_088, baseline_090,
            baseline_091, baseline_092, baseline_093, baseline_094, baseline_095,
            baseline_096, baseline_098, baseline_099, baseline_100,
            baseline_101, baseline_102, baseline_103, baseline_104, baseline_105,
            baseline_107, baseline_109, baseline_110,
            baseline_111, baseline_112, baseline_113, baseline_114, baseline_115,
            baseline_116, baseline_117, baseline_118, baseline_119, baseline_120,
            baseline_121, baseline_122, baseline_123, baseline_124, baseline_125,
            baseline_126, baseline_127, baseline_128, baseline_129,
            baseline_131, baseline_132, baseline_133, baseline_134, baseline_135,
            baseline_136, baseline_137, baseline_138, baseline_139, baseline_140,
            baseline_141, baseline_143, baseline_145,
            baseline_146, baseline_148, baseline_149,
            baseline_151, baseline_152, baseline_153, baseline_154, baseline_155,
            baseline_156, baseline_157, baseline_158, baseline_159, baseline_160,
            baseline_161, baseline_162, baseline_163, baseline_164,
            baseline_166, baseline_167, baseline_168, baseline_169, baseline_170,
            baseline_171, baseline_172, baseline_173, baseline_174, baseline_175,
            baseline_177, baseline_178, baseline_179, baseline_180,
            baseline_181, baseline_182, baseline_184, baseline_185,
            baseline_187, baseline_188, baseline_189, baseline_190,
            baseline_191, baseline_192, baseline_193, baseline_194, baseline_195,
            baseline_196, baseline_198, baseline_199, baseline_200
        ";
        let mut state = CnCalcState::default();
        let series = CnFactorEngine::build_symbol_series_from_state(&mut state);

        for name in REQUESTED
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            let binding = CnFactorBinding {
                name: name.to_string(),
                factor_id: CnFactorId::from_name(name),
                extra_factor_id: None,
            };
            let supported =
                CnFactorEngine::compute_supported_factor(&binding, None, None, Some(&series))
                    .is_some();
            assert!(supported, "missing baseline wiring: {name}");
        }
    }
}
