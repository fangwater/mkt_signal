//! Fusion Factor 数据通路应用
//!
//! 订阅 trade_flow_feature（包含 trade + depth）并触发融合因子计算。
//! 当前先落地 factor_118，后续在同一框架上扩展全部因子。

use anyhow::{bail, Context, Result};
use factor_engine::baseline as baseline_engine;
use factor_engine::math::pct_change_last;
use factor_engine::view::{SplitSlice, SymbolSeries};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use reqwest::Client;
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, DB};
use serde::Deserialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::cfg::{
    ClipRange as FusionClipRange, FusionFactorPubConfig, RlFactorConfig as FusionRlFactorConfig,
    TlenServerConfig,
};
use super::factor_enum::FusionFactorId;
pub(crate) use super::plan::{
    load_venue_factor_plan_from_tlen_server, ExtraFactorId, FactorBinding, SymbolFactorPlan,
};
use super::publisher::FusionFactorPublisher;
use super::window_primitives::{
    rolling_corr_last, rolling_kurt_last, rolling_mean_at_from_series, rolling_mean_last,
    rolling_mean_last_opt_from_series, rolling_mean_last_with_min_periods, rolling_mean_series,
    rolling_mean_series_opt, rolling_rank_last, rolling_skew_last, rolling_std_last,
    rolling_sum_at_from_series, rolling_sum_at_opt_from_series, rolling_sum_last_from_series,
    rolling_sum_last_opt_from_series, rolling_sum_last_with_min_periods, rolling_sum_series,
    rolling_sum_series_opt, tail_skew_last_opt, F64SeriesView, OptF64SeriesView,
};
use super::zscore::{
    load_zscore_config_from_tlen_server, normalize_feature_values, SymbolNormState,
    ZscoreRuntimeConfig,
};
use crate::common::amount_threshold::is_online_amount_threshold;
use crate::common::mkt_msg::{FactorValueMsg, FeatureMsg, FeatureStatus};
use crate::common::msg_parser::parse_trade_flow_feature;
use crate::common::rolling_welford::RollingWelfordCovariance;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::trade_flow_feature_msg::{
    TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_MSG_TYPE,
};
use crate::factor_pub::factor_index::factor_name_to_index;
use crate::factor_pub::kline_factor_pub::app::compute_rl_return_volatility;
use crate::signal::common::TradingVenue;

const TRADE_FLOW_MAX_BYTES: usize = 1024;
const IDLE_SLEEP_MICROS: u64 = 200;
const STATS_LOG_INTERVAL_SECS: u64 = 60;
const TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE: usize = 8192;
const TRADE_FLOW_SERVICE_HISTORY_SIZE: usize = 128;
const TRADE_FLOW_MAX_SUBSCRIBERS: usize = 10;
const TRADE_FLOW_FEATURE_CF_SUFFIX: &str = "trade_flow:feature";
const ROCKSDB_BOOTSTRAP_LOG_EVERY: usize = 12 * 60 * 4;
const MAX_SYMBOL_HISTORY: usize = 4096;
const MAX_DEPTH_LEVELS_CACHE: usize = 20;
const APPENDED_DEPTH_VALUES: usize = MAX_DEPTH_LEVELS_CACHE * 4;
const FACTOR_118_WINDOW: usize = 120;
const FACTOR_118_VWAP_LEVELS: usize = 5;
const RL_FACTOR_NAME: &str = "rl_return_volatility";
const RL_FACTOR_PAYLOAD_MAX_BYTES: usize = 256;
const RL_FACTOR_WARN_INTERVAL_SECS: u64 = 5;
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
const FACTOR_160_RANDOM_LEVELS: [usize; 10] = [18, 1, 19, 8, 10, 17, 6, 13, 4, 2];

#[derive(Debug, Clone)]
struct RlReturnVolatilityRuntimeConfig {
    pct_change_period: usize,
    rolling_window: usize,
    scale_factor: f64,
    clip: Option<FusionClipRange>,
}

impl RlReturnVolatilityRuntimeConfig {
    fn from_fusion_config(cfg: &FusionRlFactorConfig) -> Result<Self> {
        let pct_change_period = cfg.pct_change_period;
        let rolling_window = cfg.rolling_window;
        let required = pct_change_period + rolling_window;
        if required > MAX_SYMBOL_HISTORY {
            bail!(
                "rl required history {} exceeds MAX_SYMBOL_HISTORY {}",
                required,
                MAX_SYMBOL_HISTORY
            );
        }

        Ok(Self {
            pct_change_period: cfg.pct_change_period,
            rolling_window: cfg.rolling_window,
            scale_factor: cfg.scale_factor,
            clip: cfg.clip.clone(),
        })
    }

    fn apply_scale_and_clip(&self, raw: f64) -> f64 {
        let mut value = raw * self.scale_factor;
        if let Some(clip) = &self.clip {
            value = value.clamp(clip.min, clip.max);
        }
        value
    }
}

struct RlFactorPublisher {
    publisher: Publisher<ipc::Service, [u8; RL_FACTOR_PAYLOAD_MAX_BYTES], ()>,
    factor_index: u16,
    published_count: u64,
    dropped_count: u64,
    last_warn: Instant,
}

impl RlFactorPublisher {
    fn new(venue_slug: &str) -> Result<Self> {
        let Some(factor_index) = factor_name_to_index(RL_FACTOR_NAME) else {
            bail!("missing factor index for '{}'", RL_FACTOR_NAME);
        };

        let node_name = format!("fusion_rl_pub_{}", venue_slug.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_path = format!("factor_pub/{}/{}", venue_slug, RL_FACTOR_NAME);
        let service = node
            .service_builder(&ServiceName::new(&service_path)?)
            .publish_subscribe::<[u8; RL_FACTOR_PAYLOAD_MAX_BYTES]>()
            .open_or_create()?;
        let publisher = service.publisher_builder().create()?;

        info!(
            "RlFactorPublisher created: service={} factor_index={}",
            service_path, factor_index
        );

        Ok(Self {
            publisher,
            factor_index,
            published_count: 0,
            dropped_count: 0,
            last_warn: Instant::now() - Duration::from_secs(RL_FACTOR_WARN_INTERVAL_SECS),
        })
    }

    fn publish(&mut self, symbol: &str, value: f64, timestamp_ms: i64, ready: bool) -> bool {
        let msg = FactorValueMsg::create_with_factor_index(
            symbol.to_string(),
            value,
            timestamp_ms,
            ready,
            self.factor_index,
        );
        let bytes = msg.to_bytes();
        if bytes.len() > RL_FACTOR_PAYLOAD_MAX_BYTES {
            self.warn_throttled(
                "payload_too_large",
                &format!(
                    "len={} max={} symbol={}",
                    bytes.len(),
                    RL_FACTOR_PAYLOAD_MAX_BYTES,
                    symbol
                ),
            );
            self.dropped_count = self.dropped_count.saturating_add(1);
            return false;
        }

        let mut buffer = [0u8; RL_FACTOR_PAYLOAD_MAX_BYTES];
        buffer[..bytes.len()].copy_from_slice(&bytes);

        match self.publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                if sample.send().is_ok() {
                    self.published_count = self.published_count.saturating_add(1);
                    true
                } else {
                    self.warn_throttled("send", &symbol);
                    self.dropped_count = self.dropped_count.saturating_add(1);
                    false
                }
            }
            Err(err) => {
                self.warn_throttled("loan_uninit", &err);
                self.dropped_count = self.dropped_count.saturating_add(1);
                false
            }
        }
    }

    fn published_count(&self) -> u64 {
        self.published_count
    }

    fn dropped_count(&self) -> u64 {
        self.dropped_count
    }

    fn warn_throttled(&mut self, action: &str, detail: &dyn std::fmt::Debug) {
        if self.last_warn.elapsed() >= Duration::from_secs(RL_FACTOR_WARN_INTERVAL_SECS) {
            warn!("rl factor publish {} failed: {:?}", action, detail);
            self.last_warn = Instant::now();
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DepthLevel {
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Clone)]
pub struct DepthSnapshot {
    pub bids: [DepthLevel; MAX_DEPTH_LEVELS_CACHE],
    pub asks: [DepthLevel; MAX_DEPTH_LEVELS_CACHE],
}

#[derive(Debug, Clone)]
pub struct DepthDerived {
    pub bids: [DepthLevel; MAX_DEPTH_LEVELS_CACHE],
    pub asks: [DepthLevel; MAX_DEPTH_LEVELS_CACHE],
    bid_amount_prefix: [f64; MAX_DEPTH_LEVELS_CACHE + 1],
    ask_amount_prefix: [f64; MAX_DEPTH_LEVELS_CACHE + 1],
    bid_price_prefix: [f64; MAX_DEPTH_LEVELS_CACHE + 1],
    ask_price_prefix: [f64; MAX_DEPTH_LEVELS_CACHE + 1],
    bid_pxv_prefix: [f64; MAX_DEPTH_LEVELS_CACHE + 1],
    ask_pxv_prefix: [f64; MAX_DEPTH_LEVELS_CACHE + 1],
}

impl DepthDerived {
    pub fn from_snapshot(depth: &DepthSnapshot) -> Self {
        let mut out = Self {
            bids: depth.bids,
            asks: depth.asks,
            bid_amount_prefix: [0.0; MAX_DEPTH_LEVELS_CACHE + 1],
            ask_amount_prefix: [0.0; MAX_DEPTH_LEVELS_CACHE + 1],
            bid_price_prefix: [0.0; MAX_DEPTH_LEVELS_CACHE + 1],
            ask_price_prefix: [0.0; MAX_DEPTH_LEVELS_CACHE + 1],
            bid_pxv_prefix: [0.0; MAX_DEPTH_LEVELS_CACHE + 1],
            ask_pxv_prefix: [0.0; MAX_DEPTH_LEVELS_CACHE + 1],
        };
        for i in 0..MAX_DEPTH_LEVELS_CACHE {
            let b = out.bids[i];
            let a = out.asks[i];
            out.bid_amount_prefix[i + 1] =
                out.bid_amount_prefix[i] + if b.amount.is_finite() { b.amount } else { 0.0 };
            out.ask_amount_prefix[i + 1] =
                out.ask_amount_prefix[i] + if a.amount.is_finite() { a.amount } else { 0.0 };
            out.bid_price_prefix[i + 1] =
                out.bid_price_prefix[i] + if b.price.is_finite() { b.price } else { 0.0 };
            out.ask_price_prefix[i + 1] =
                out.ask_price_prefix[i] + if a.price.is_finite() { a.price } else { 0.0 };
            out.bid_pxv_prefix[i + 1] = out.bid_pxv_prefix[i]
                + if b.price.is_finite() && b.amount.is_finite() {
                    b.price * b.amount
                } else {
                    0.0
                };
            out.ask_pxv_prefix[i + 1] = out.ask_pxv_prefix[i]
                + if a.price.is_finite() && a.amount.is_finite() {
                    a.price * a.amount
                } else {
                    0.0
                };
        }
        out
    }

    #[inline]
    fn clamp_limit(limit: usize) -> usize {
        limit.min(MAX_DEPTH_LEVELS_CACHE)
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
        self.bid_amount_prefix[Self::clamp_limit(limit)]
    }

    #[inline]
    pub fn sum_ask_amount(&self, limit: usize) -> f64 {
        self.ask_amount_prefix[Self::clamp_limit(limit)]
    }

    #[inline]
    pub fn sum_bid_price(&self, limit: usize) -> f64 {
        self.bid_price_prefix[Self::clamp_limit(limit)]
    }

    #[inline]
    pub fn sum_ask_price(&self, limit: usize) -> f64 {
        self.ask_price_prefix[Self::clamp_limit(limit)]
    }

    #[inline]
    pub fn mean_bid_amount(&self, limit: usize) -> f64 {
        let l = Self::clamp_limit(limit);
        if l == 0 {
            return f64::NAN;
        }
        self.sum_bid_amount(l) / l as f64
    }

    #[inline]
    pub fn mean_ask_amount(&self, limit: usize) -> f64 {
        let l = Self::clamp_limit(limit);
        if l == 0 {
            return f64::NAN;
        }
        self.sum_ask_amount(l) / l as f64
    }

    #[inline]
    pub fn mean_bid_price(&self, limit: usize) -> f64 {
        let l = Self::clamp_limit(limit);
        if l == 0 {
            return f64::NAN;
        }
        self.sum_bid_price(l) / l as f64
    }

    #[inline]
    pub fn mean_ask_price(&self, limit: usize) -> f64 {
        let l = Self::clamp_limit(limit);
        if l == 0 {
            return f64::NAN;
        }
        self.sum_ask_price(l) / l as f64
    }

    #[inline]
    pub fn bid_vwap(&self, limit: usize) -> Option<f64> {
        let l = Self::clamp_limit(limit);
        if l == 0 {
            return None;
        }
        let den = self.bid_amount_prefix[l];
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(self.bid_pxv_prefix[l] / den)).or(Some(0.0))
    }

    #[inline]
    pub fn ask_vwap(&self, limit: usize) -> Option<f64> {
        let l = Self::clamp_limit(limit);
        if l == 0 {
            return None;
        }
        let den = self.ask_amount_prefix[l];
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(self.ask_pxv_prefix[l] / den)).or(Some(0.0))
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

#[derive(Default)]
pub struct SymbolCalcState {
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
    pub bid_vwap20: VecDeque<f64>,
    pub total_bid20: VecDeque<f64>,
    pub total_ask20: VecDeque<f64>,
    pub top10_bid_volume: VecDeque<f64>,
    pub top10_ask_volume: VecDeque<f64>,
    pub top10_bid_mean: VecDeque<f64>,
    pub top10_ask_mean: VecDeque<f64>,
    pub bid9v: VecDeque<f64>,
    pub ask9v: VecDeque<f64>,
    pub bid9p: VecDeque<f64>,
    pub ask9p: VecDeque<f64>,
    pub combined_level9_volume: VecDeque<f64>,
    pub mean_bid_vol20: VecDeque<f64>,
    pub mean_bid_price5: VecDeque<f64>,
    pub mean_bid_price20: VecDeque<f64>,
    pub avg_ask_price5: VecDeque<f64>,
    pub mean_ask_price15: VecDeque<f64>,
    pub ask_pv15_mean: VecDeque<f64>,
    pub bid_pv15_mean: VecDeque<f64>,
    pub factor_031_ratio: VecDeque<f64>,
    pub factor_119_mid_minus_ask_vwap5: VecDeque<f64>,
    pub total_volume20_sum: VecDeque<f64>,
    pub median_all_price40: VecDeque<f64>,
    pub factor_152_prev_price_diff10: Option<[f64; 10]>,
    pub factor_152_pct_mean: VecDeque<Option<f64>>,
    pub ask_vwap20: VecDeque<f64>,
    pub ask_vwap_diff_5_20: VecDeque<f64>,
    pub ask_mean_volume_20: VecDeque<f64>,
    pub ask0v: VecDeque<f64>,
    pub factor_113_bid_price_pct_hmean: VecDeque<Option<f64>>,
    pub factor_114_ask_price_pct_mean: VecDeque<Option<f64>>,
    pub factor_127_bid_price_kurt: VecDeque<f64>,
    pub factor_128_ask_price_sum: VecDeque<f64>,
    pub factor_128_diff_sum: VecDeque<Option<f64>>,
    pub factor_128_skew: VecDeque<Option<f64>>,
    pub factor_131_ask_price_kurt: VecDeque<f64>,
    pub factor_157_bid_ask_diff_std: VecDeque<Option<f64>>,
    pub factor_160_prev_ratios: Option<[f64; 10]>,
    pub factor_160_pct_change_mean: VecDeque<Option<f64>>,
    pub factor_157_prev_bid_amounts20: Option<[f64; 20]>,
    pub factor_157_prev_ask_amounts20: Option<[f64; 20]>,
    pub bid_price_level_hist10: [VecDeque<f64>; 10],
    pub ask_price_level_hist10: [VecDeque<f64>; 10],
    pub corr_close_volume_14_last: Option<f64>,
    pub corr_open_volume_300_last: Option<f64>,
    pub corr_mid_midvol_300_last: Option<f64>,
}

struct SymbolRollingStats {
    corr_close_volume_14: RollingWelfordCovariance,
    corr_open_volume_300: RollingWelfordCovariance,
    corr_mid_midvol_300: RollingWelfordCovariance,
}

impl Default for SymbolRollingStats {
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

impl SymbolCalcState {
    pub fn push_mid_price_diff(&mut self, value: f64) {
        self.factor_118_mid_price_diffs.push_back(value);
        if self.factor_118_mid_price_diffs.len() > MAX_SYMBOL_HISTORY {
            self.factor_118_mid_price_diffs.pop_front();
        }
    }

    pub fn push_trade_flow(&mut self, msg: &TradeFlowFeatureMsg) {
        push_with_limit(&mut self.open, msg.values[FIELD_OPEN]);
        push_with_limit(&mut self.high, msg.values[FIELD_HIGH]);
        push_with_limit(&mut self.low, msg.values[FIELD_LOW]);
        push_with_limit(&mut self.close, msg.values[FIELD_CLOSE]);
        push_with_limit(&mut self.volume, msg.values[FIELD_VOLUME]);
        push_with_limit(&mut self.amount, msg.values[FIELD_AMOUNT]);
        push_with_limit(&mut self.avg_amount, msg.values[FIELD_AVG_AMOUNT]);
        push_with_limit(&mut self.count, msg.values[FIELD_COUNT]);
        push_with_limit(&mut self.trade_time, msg.ts as f64);
        push_with_limit(&mut self.buy_count, msg.values[FIELD_BUY_COUNT]);
        push_with_limit(&mut self.sell_count, msg.values[FIELD_SELL_COUNT]);
        push_with_limit(&mut self.buy_amount, msg.values[FIELD_BUY_AMOUNT]);
        push_with_limit(&mut self.sell_amount, msg.values[FIELD_SELL_AMOUNT]);
        push_with_limit(&mut self.buy_volume, msg.values[FIELD_BUY_VOLUME]);
        push_with_limit(&mut self.sell_volume, msg.values[FIELD_SELL_VOLUME]);
        push_with_limit(&mut self.large_order, msg.values[FIELD_LARGE_ORDER]);
        push_with_limit(&mut self.medium_order, msg.values[FIELD_MEDIUM_ORDER]);
        push_with_limit(&mut self.large_buy, msg.values[FIELD_LARGE_BUY]);
        push_with_limit(&mut self.large_sell, msg.values[FIELD_LARGE_SELL]);
        push_with_limit(&mut self.medium_buy, msg.values[FIELD_MEDIUM_BUY]);
        push_with_limit(&mut self.medium_sell, msg.values[FIELD_MEDIUM_SELL]);
        push_with_limit(&mut self.small_order, msg.values[FIELD_SMALL_ORDER]);
        push_with_limit(&mut self.small_buy, msg.values[FIELD_SMALL_BUY]);
        push_with_limit(&mut self.small_sell, msg.values[FIELD_SMALL_SELL]);
        push_with_limit(&mut self.vwap, msg.values[FIELD_VWAP]);
        push_with_limit(&mut self.buy_vwap, msg.values[FIELD_BUY_VWAP]);
        push_with_limit(&mut self.sell_vwap, msg.values[FIELD_SELL_VWAP]);
        push_with_limit(&mut self.net_buy_volume, msg.values[FIELD_NET_BUY_VOLUME]);
        push_with_limit(&mut self.net_buy_pct, msg.values[FIELD_NET_BUY_PCT]);
        push_with_limit(&mut self.net_buy_large, msg.values[FIELD_NET_BUY_LARGE]);
        push_with_limit(&mut self.net_buy_medium, msg.values[FIELD_NET_BUY_MEDIUM]);
        push_with_limit(&mut self.net_buy_small, msg.values[FIELD_NET_BUY_SMALL]);
        push_with_limit(&mut self.net_buy_amount, msg.values[FIELD_NET_BUY_AMOUNT]);
    }

    pub fn push_depth_metrics(&mut self, depth: &DepthSnapshot) {
        let derived = DepthDerived::from_snapshot(depth);
        self.push_depth_metrics_derived(&derived);
    }

    pub fn push_depth_metrics_derived(&mut self, depth: &DepthDerived) {
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

        let top10_bid = depth.sum_bid_amount(10);
        let top10_ask = depth.sum_ask_amount(10);
        let total_bid20 = depth.sum_bid_amount(20);
        let total_ask20 = depth.sum_ask_amount(20);
        push_with_limit(&mut self.total_bid20, total_bid20);
        push_with_limit(&mut self.total_ask20, total_ask20);
        push_with_limit(&mut self.total_volume20_sum, total_bid20 + total_ask20);
        push_with_limit(&mut self.top10_bid_volume, top10_bid);
        push_with_limit(&mut self.top10_ask_volume, top10_ask);
        push_with_limit(&mut self.top10_bid_mean, top10_bid / 10.0);
        push_with_limit(&mut self.top10_ask_mean, top10_ask / 10.0);
        push_with_limit(&mut self.bid9v, depth.bid_amount(9));
        push_with_limit(&mut self.ask9v, depth.ask_amount(9));
        push_with_limit(&mut self.bid9p, depth.bid_price(9));
        push_with_limit(&mut self.ask9p, depth.ask_price(9));
        push_with_limit(
            &mut self.combined_level9_volume,
            depth.bid_amount(9) + depth.ask_amount(9),
        );
        push_with_limit(&mut self.mean_bid_vol20, depth.mean_bid_amount(20));
        push_with_limit(&mut self.mean_bid_price5, depth.mean_bid_price(5));
        push_with_limit(&mut self.mean_bid_price20, depth.mean_bid_price(20));
        push_with_limit(&mut self.avg_ask_price5, depth.mean_ask_price(5));
        push_with_limit(&mut self.mean_ask_price15, depth.mean_ask_price(15));
        push_with_limit(&mut self.ask_pv15_mean, depth_mean_pxv(&depth.asks, 15));
        push_with_limit(&mut self.bid_pv15_mean, depth_mean_pxv(&depth.bids, 15));
        let ratio_031 = {
            let num = depth.mean_bid_price(15);
            if mid.abs() > 1e-12 {
                num / mid
            } else {
                f64::NAN
            }
        };
        push_with_limit(&mut self.factor_031_ratio, ratio_031);
        push_with_limit(
            &mut self.median_all_price40,
            median_from_iter(
                depth
                    .bids
                    .iter()
                    .map(|level| level.price)
                    .chain(depth.asks.iter().map(|level| level.price)),
            )
            .unwrap_or(f64::NAN),
        );

        let ask_vwap5 = depth.ask_vwap(5);
        let ask_vwap20 = depth.ask_vwap(20);
        let bid_vwap20 = depth.bid_vwap(20);
        push_with_limit(&mut self.bid_vwap20, bid_vwap20.unwrap_or(f64::NAN));
        let ask_vwap_diff = match (ask_vwap5, ask_vwap20) {
            (Some(a), Some(b)) => a - b,
            _ => f64::NAN,
        };
        push_with_limit(&mut self.ask_vwap_diff_5_20, ask_vwap_diff);
        push_with_limit(&mut self.ask_vwap20, ask_vwap20.unwrap_or(f64::NAN));
        let factor_119_diff = ask_vwap5
            .map(|v| mid - v)
            .filter(|v| v.is_finite())
            .unwrap_or(f64::NAN);
        push_with_limit(&mut self.factor_119_mid_minus_ask_vwap5, factor_119_diff);

        let ask_mean20 = depth.mean_ask_amount(20);
        push_with_limit(&mut self.ask_mean_volume_20, ask_mean20);

        push_with_limit(&mut self.ask0v, ask0v);

        let bid_prices10: Vec<f64> = (0..10).map(|i| depth.bid_price(i)).collect();
        let ask_prices20: Vec<f64> = (0..20).map(|i| depth.ask_price(i)).collect();
        push_with_limit(
            &mut self.factor_127_bid_price_kurt,
            cross_sectional_kurtosis(&bid_prices10, true, false).unwrap_or(f64::NAN),
        );
        push_with_limit(
            &mut self.factor_131_ask_price_kurt,
            cross_sectional_kurtosis(&ask_prices20, true, false).unwrap_or(f64::NAN),
        );

        let curr_bid_amounts20: [f64; 20] = std::array::from_fn(|i| depth.bid_amount(i));
        let curr_ask_amounts20: [f64; 20] = std::array::from_fn(|i| depth.ask_amount(i));
        let factor_157_value = match (
            self.factor_157_prev_bid_amounts20,
            self.factor_157_prev_ask_amounts20,
        ) {
            (Some(prev_bid), Some(prev_ask)) => {
                let bid_diffs: Vec<f64> = curr_bid_amounts20
                    .iter()
                    .zip(prev_bid.iter())
                    .map(|(curr, prev)| curr - prev)
                    .collect();
                let ask_diffs: Vec<f64> = curr_ask_amounts20
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
        self.factor_157_prev_bid_amounts20 = Some(curr_bid_amounts20);
        self.factor_157_prev_ask_amounts20 = Some(curr_ask_amounts20);

        for i in 0..10 {
            push_with_limit(&mut self.bid_price_level_hist10[i], depth.bid_price(i));
            push_with_limit(&mut self.ask_price_level_hist10[i], depth.ask_price(i));
        }

        let bid_pct_hmean = {
            let mut pct = Vec::with_capacity(10);
            for hist in &self.bid_price_level_hist10 {
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
            let mut pct = Vec::with_capacity(10);
            for hist in &self.ask_price_level_hist10 {
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
            if pct.len() == 10 {
                Some(pct.iter().sum::<f64>() / pct.len() as f64)
            } else {
                None
            }
        };
        push_opt_with_limit(&mut self.factor_114_ask_price_pct_mean, ask_pct_mean);

        let ask_price_sum20 = depth.sum_ask_price(20);
        let prev_sum = self.factor_128_ask_price_sum.back().copied();
        push_with_limit(&mut self.factor_128_ask_price_sum, ask_price_sum20);
        let curr_diff = prev_sum.map(|prev| ask_price_sum20 - prev).or(Some(0.0));
        push_opt_with_limit(&mut self.factor_128_diff_sum, curr_diff);

        let diff_vec: Vec<Option<f64>> = self.factor_128_diff_sum.iter().copied().collect();
        let skew = tail_skew_last_opt(&diff_vec, 90, 30, false).ok().flatten();
        push_opt_with_limit(&mut self.factor_128_skew, skew);

        let mut curr_ratios = [f64::NAN; 10];
        for (k, level_idx) in FACTOR_160_RANDOM_LEVELS.iter().enumerate() {
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
            let mut vals = Vec::with_capacity(10);
            for i in 0..10 {
                let p = prev[i];
                let c = curr_ratios[i];
                if !p.is_finite() || !c.is_finite() || p.abs() <= 1e-12 {
                    continue;
                }
                let v = (c - p) / p;
                if v.is_finite() {
                    vals.push(v);
                }
            }
            if vals.is_empty() {
                None
            } else {
                Some(vals.iter().sum::<f64>() / vals.len() as f64)
            }
        });
        self.factor_160_prev_ratios = Some(curr_ratios);
        push_opt_with_limit(&mut self.factor_160_pct_change_mean, pct_change_mean);

        let mut curr_diff = [f64::NAN; 10];
        for (i, val) in curr_diff.iter_mut().enumerate() {
            let bidp = depth.bid_price(i);
            let askp = depth.ask_price(i);
            *val = bidp - askp;
        }
        let pct_mean = self.factor_152_prev_price_diff10.and_then(|prev| {
            let mut vals = Vec::with_capacity(10);
            for i in 0..10 {
                let p = prev[i];
                let c = curr_diff[i];
                if !p.is_finite() || !c.is_finite() || p.abs() <= 1e-12 {
                    continue;
                }
                let v = (c - p) / p;
                if v.is_finite() {
                    vals.push(v);
                }
            }
            if vals.is_empty() {
                None
            } else {
                Some(vals.iter().sum::<f64>() / vals.len() as f64)
            }
        });
        self.factor_152_prev_price_diff10 = Some(curr_diff);
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
struct ReplayEvalSummary {
    status: u8,
}

struct BootstrapSymbolReplayResult {
    symbol: String,
    loaded: usize,
    calculated: u64,
    reload: u64,
    all_ready_seen: bool,
    state: SymbolCalcState,
    rolling_stats: SymbolRollingStats,
    norm_state: SymbolNormState,
    bootstrap_published: u64,
}

struct BootstrapSymbolRecords {
    symbol: String,
    records: Vec<TradeFlowFeatureMsg>,
}

impl ReplayEvalSummary {
    fn from_eval(eval_result: &OrderedEvalResult, mark_reload: bool) -> Self {
        let status = if mark_reload && eval_result.status == FeatureStatus::AllReady as u8 {
            FeatureStatus::Reload as u8
        } else {
            eval_result.status
        };
        Self { status }
    }

    fn missing_plan() -> Self {
        Self { status: u8::MAX }
    }
}

impl F64SeriesView for SplitSlice<'_, f64> {
    fn len(&self) -> usize {
        SplitSlice::len(self)
    }

    fn value_at(&self, idx: usize) -> f64 {
        self[idx]
    }
}

impl OptF64SeriesView for SplitSlice<'_, Option<f64>> {
    fn len(&self) -> usize {
        SplitSlice::len(self)
    }

    fn value_at(&self, idx: usize) -> Option<f64> {
        self[idx]
    }
}

pub struct FusionFactorPubApp {
    venue_slug: String,
    venue: TradingVenue,
    trade_flow_feature_rocksdb_path: String,
    trade_flow_subscriber: Subscriber<ipc::Service, [u8; TRADE_FLOW_MAX_BYTES], ()>,
    tlen_server: Option<TlenServerConfig>,
    publisher: Option<FusionFactorPublisher>,
    zscore_config: ZscoreRuntimeConfig,
    rl_config: RlReturnVolatilityRuntimeConfig,
    rl_publisher: RlFactorPublisher,
    allowed_symbols: HashSet<String>,
    symbol_all_ready_seen: HashSet<String>,
    venue_factor_plan: Option<SymbolFactorPlan>,
    symbol_states: HashMap<String, SymbolCalcState>,
    symbol_rolling_stats: HashMap<String, SymbolRollingStats>,
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
    rl_published_count: u64,
    rl_publish_failed_count: u64,
    last_symbol_reload: Instant,
    symbol_reload_interval: Option<Duration>,
    last_symbol_reload_warn: Instant,
    last_stats_log: Instant,
}

impl FusionFactorPubApp {
    pub async fn new(config_path: &str, venue: TradingVenue) -> Result<Self> {
        let cfg = FusionFactorPubConfig::load(config_path)?;
        let venue_slug = venue.data_pub_slug().to_string();
        let trade_flow_feature_rocksdb_path = resolve_trade_flow_feature_rocksdb_path(&venue_slug)?;
        let tlen_server = cfg.tlen_server.clone();
        let zscore_config = load_zscore_config_from_tlen_server(&cfg.tlen_server, &venue_slug)
            .await
            .with_context(|| {
                format!(
                    "load zscore config from tlen_server failed: venue={}",
                    venue_slug
                )
            })?;
        let venue_factor_plan =
            load_venue_factor_plan_from_tlen_server(&cfg.tlen_server, &venue_slug)
                .await
                .with_context(|| {
                    format!(
                        "load venue factor plan from tlen_server failed: venue={}",
                        venue_slug
                    )
                })?;
        let allowed_symbols: Vec<String> =
            load_online_symbols_from_tlen_server(&cfg.tlen_server, venue, &venue_slug)
                .await
                .with_context(|| {
                    format!(
                        "load online symbols from tlen_server failed: venue={}",
                        venue_slug
                    )
                })?
                .into_iter()
                .collect();
        if allowed_symbols.is_empty() {
            anyhow::bail!(
                "tlen_server returned no online amount_threshold symbols for venue={}",
                venue_slug
            );
        }

        let trade_flow_subscriber =
            Self::create_trade_flow_subscriber(&venue_slug, cfg.trade_flow_channel())?;
        let rl_config = RlReturnVolatilityRuntimeConfig::from_fusion_config(&cfg.rl_factor)?;
        let rl_publisher = RlFactorPublisher::new(&venue_slug)?;
        let output_service_path = cfg.output_service_path(&venue_slug);
        let publisher_node_name = format!("fusion_pub_{}", venue_slug.replace('-', "_"));
        let publisher = FusionFactorPublisher::new(&publisher_node_name, &output_service_path)
            .with_context(|| {
                format!(
                    "create fusion factor publisher failed: service_path={}",
                    output_service_path
                )
            })?;

        info!(
            "FusionFactorPubApp created: venue={} mode={} symbol_source={} symbols={} sample={} trade_flow_channel=factor_pub/{}/{} rocksdb_path={} output_service={}",
            venue_slug,
            "fusion+rl",
            "tlen_server.amount_thresholds+factor_plan[__shared__]",
            allowed_symbols.len(),
            format_symbol_sample(&allowed_symbols),
            venue_slug,
            cfg.trade_flow_channel(),
            trade_flow_feature_rocksdb_path,
            output_service_path,
        );
        info!(
            "FusionFactorPubApp[{}] venue factor plan loaded: factors={}",
            venue_slug,
            venue_factor_plan.ordered_factors.len(),
        );
        info!(
            "FusionFactorPubApp[{}] zscore config: source=tlen_server.zscore window_size={} min_samples={} zscore_cap={}",
            venue_slug,
            zscore_config.window_size,
            zscore_config.min_samples,
            zscore_config.zscore_cap,
        );
        info!(
            "FusionFactorPubApp[{}] rl config: factor={} source={} pct_change_period={} rolling_window={} scale_factor={} clip={:?}",
            venue_slug,
            RL_FACTOR_NAME,
            "fusion_factor_pub.toml[rl_factor]",
            rl_config.pct_change_period,
            rl_config.rolling_window,
            rl_config.scale_factor,
            rl_config.clip
        );

        Ok(Self {
            venue_slug,
            venue,
            trade_flow_feature_rocksdb_path,
            trade_flow_subscriber,
            tlen_server: Some(tlen_server.clone()),
            publisher: Some(publisher),
            zscore_config,
            rl_config,
            rl_publisher,
            allowed_symbols: allowed_symbols.into_iter().collect(),
            symbol_all_ready_seen: HashSet::new(),
            venue_factor_plan: Some(venue_factor_plan),
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
            rl_published_count: 0,
            rl_publish_failed_count: 0,
            last_symbol_reload: Instant::now(),
            symbol_reload_interval: Some(Duration::from_secs(tlen_server.symbol_reload_secs)),
            last_symbol_reload_warn: Instant::now()
                - Duration::from_secs(SYMBOL_RELOAD_WARN_INTERVAL_SECS),
            last_stats_log: Instant::now(),
        })
    }

    fn create_trade_flow_subscriber(
        venue: &str,
        channel: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; TRADE_FLOW_MAX_BYTES], ()>> {
        let node_name = format!("fusion_sub_{}_trade_flow", venue.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_name = format!("factor_pub/{}/{}", venue, channel);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; TRADE_FLOW_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(TRADE_FLOW_MAX_SUBSCRIBERS)
            .subscriber_max_buffer_size(TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE)
            .history_size(TRADE_FLOW_SERVICE_HISTORY_SIZE)
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
        match load_zscore_config_from_tlen_server(&tlen_server, &self.venue_slug).await {
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
        match load_online_symbols_from_tlen_server(&tlen_server, self.venue, &self.venue_slug).await
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
            "FusionFactorPubApp[{}] zscore config reloaded: old={{window_size:{} min_samples:{} zscore_cap:{}}} new={{window_size:{} min_samples:{} zscore_cap:{}}} cleared_norm_symbols={}",
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
            "FusionFactorPubApp[{}] online symbols reloaded: online_symbols={} sample={} retired_symbols={} retired_sample={}",
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
        self.bootstrap_from_rocksdb()?;

        // Drain stale IPC messages buffered before this process started.
        let mut drained = 0u64;
        while self.trade_flow_subscriber.receive()?.is_some() {
            drained += 1;
        }
        info!(
            "FusionFactorPubApp[{}] started: symbols={} sample={} trade_flow=single_stream drained_stale={}",
            self.venue_slug,
            self.allowed_symbols.len(),
            format_symbol_sample_set(&self.allowed_symbols),
            drained,
        );

        loop {
            self.maybe_reload_symbols().await;
            let mut has_message = false;

            // trade_flow 是计算触发源，优先消费可降低触发延迟。
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
                            "trade_flow pre-parse failed: venue={} err={}",
                            self.venue_slug, err
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
                            "trade_flow decode failed: venue={} symbol={} err={}",
                            self.venue_slug, symbol, err
                        );
                    }
                }
            }

            if !has_message {
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }

            if self.last_stats_log.elapsed() >= Duration::from_secs(STATS_LOG_INTERVAL_SECS) {
                let (pub_total, pub_dropped) = self
                    .publisher
                    .as_ref()
                    .map(|publisher| (publisher.published_count(), publisher.dropped_count()))
                    .unwrap_or((0, 0));
                info!(
                    "FusionFactorPubApp[{}] stats: raw_msgs={} trade_flow_msgs={} triggers={} decode_errors={} depth_attached={} missing_depth={} factor118_ready={} trade_drop_symbols={} drop_symbol_samples={:?} calc_symbols={} factor_plan={} factor_eval={} factor_ready={} factor_warming_up={} factor_invalid={} factor_missing_depth={} published={} publish_failed={} pub_total={} pub_dropped={} rl_published={} rl_publish_failed={} rl_pub_total={} rl_pub_dropped={} last_decode_error={}",
                    self.venue_slug,
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
                    self.rl_published_count,
                    self.rl_publish_failed_count,
                    self.rl_publisher.published_count(),
                    self.rl_publisher.dropped_count(),
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
                self.rl_published_count = 0;
                self.rl_publish_failed_count = 0;
                self.last_stats_log = Instant::now();
            }
        }
    }

    fn bootstrap_from_rocksdb(&mut self) -> Result<()> {
        let started = Instant::now();
        let mut symbol_list: Vec<String> = self.allowed_symbols.iter().cloned().collect();
        symbol_list.sort_unstable_by(|a, b| {
            let a_pri = if a == "BTCUSDT" { 0 } else { 1 };
            let b_pri = if b == "BTCUSDT" { 0 } else { 1 };
            a_pri.cmp(&b_pri).then_with(|| a.cmp(b))
        });

        let mut db_opts = Options::default();
        db_opts.create_if_missing(false);
        db_opts.create_missing_column_families(false);

        let cf_names = match DB::list_cf(&db_opts, &self.trade_flow_feature_rocksdb_path) {
            Ok(names) => names,
            Err(err) => {
                panic!(
                    "fusion bootstrap list rocksdb cfs failed: path={} err={}",
                    self.trade_flow_feature_rocksdb_path, err
                );
            }
        };

        let mut sorted_cf_names = cf_names;
        sorted_cf_names.sort_unstable();
        let existing_cf_set: HashSet<String> = sorted_cf_names.iter().cloned().collect();
        let mut expected_symbol_cfs: Vec<String> = symbol_list
            .iter()
            .map(|symbol| trade_flow_feature_cf_name(&self.venue_slug, symbol))
            .collect();
        expected_symbol_cfs.sort_unstable();
        let mut existing_symbol_cfs = Vec::new();
        let mut missing_symbol_cfs = Vec::new();
        for cf in &expected_symbol_cfs {
            if existing_cf_set.contains(cf) {
                existing_symbol_cfs.push(cf.clone());
            } else {
                missing_symbol_cfs.push(cf.clone());
            }
        }

        info!(
            "rocksdb cf-diagnose: total_cf={} expected={} existing={} missing={}",
            sorted_cf_names.len(),
            expected_symbol_cfs.len(),
            existing_symbol_cfs.len(),
            missing_symbol_cfs.len()
        );
        if !missing_symbol_cfs.is_empty() {
            warn!(
                "rocksdb cf-diagnose: missing_cf=[{}]",
                missing_symbol_cfs.join(",")
            );
        }

        let cf_opts = Options::default();
        let descriptors: Vec<ColumnFamilyDescriptor> = sorted_cf_names
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name.clone(), cf_opts.clone()))
            .collect();
        let db = match DB::open_cf_descriptors_read_only(
            &db_opts,
            &self.trade_flow_feature_rocksdb_path,
            descriptors,
            false,
        ) {
            Ok(db) => db,
            Err(err) => {
                panic!(
                    "fusion bootstrap open rocksdb failed: path={} err={}",
                    self.trade_flow_feature_rocksdb_path, err
                );
            }
        };

        info!(
            "FusionFactorPubApp[{}] rocksdb bootstrap start: path={} symbols={}",
            self.venue_slug,
            self.trade_flow_feature_rocksdb_path,
            symbol_list.len()
        );

        let mut symbol_records = Vec::with_capacity(symbol_list.len());
        for symbol in symbol_list {
            let cf_name = trade_flow_feature_cf_name(&self.venue_slug, &symbol);
            let Some(cf) = db.cf_handle(&cf_name) else {
                warn!(
                    "fusion bootstrap missing rocksdb history: venue={} symbol={} cf={} path={}",
                    self.venue_slug, symbol, cf_name, self.trade_flow_feature_rocksdb_path
                );
                continue;
            };

            let mut records = Vec::new();
            let iter = db.iterator_cf(cf, IteratorMode::Start);
            for item in iter {
                let (_, value) = item.with_context(|| {
                    format!(
                        "rocksdb iterator failed: venue={} symbol={} cf={} path={}",
                        self.venue_slug, symbol, cf_name, self.trade_flow_feature_rocksdb_path
                    )
                })?;
                let msg = parse_trade_flow_feature(value.as_ref()).with_context(|| {
                    format!(
                        "decode trade_flow_feature from rocksdb failed: venue={} symbol={} cf={} path={}",
                        self.venue_slug, symbol, cf_name, self.trade_flow_feature_rocksdb_path
                    )
                })?;
                let msg_symbol = normalize_symbol_for_venue(&msg.symbol, self.venue);
                if msg_symbol != symbol {
                    warn!(
                        "FusionFactorPubApp[{}] rocksdb bootstrap skipped mismatched symbol: cf_symbol={} msg_symbol={} ts={} cf={}",
                        self.venue_slug,
                        symbol,
                        msg_symbol,
                        msg.ts,
                        cf_name
                    );
                    continue;
                }

                records.push(msg);
            }

            if records.is_empty() {
                warn!(
                    "fusion bootstrap missing rocksdb history rows: venue={} symbol={} cf={} path={}",
                    self.venue_slug, symbol, cf_name, self.trade_flow_feature_rocksdb_path
                );
                continue;
            }

            // 裁掉尾部连续的全零 volume/amount bar（rocksdb 写入了未完成的空槽位）
            let before_trim = records.len();
            while let Some(last) = records.last() {
                if last.values[FIELD_VOLUME] == 0.0 && last.values[FIELD_AMOUNT] == 0.0 {
                    records.pop();
                } else {
                    break;
                }
            }
            let trimmed = before_trim - records.len();
            if trimmed > 0 {
                info!(
                    "bootstrap tail trim: symbol={} trimmed={} remaining={}",
                    symbol,
                    trimmed,
                    records.len()
                );
            }

            symbol_records.push(BootstrapSymbolRecords { symbol, records });
        }

        info!(
            "FusionFactorPubApp[{}] rocksdb bootstrap replay start: symbols={} single-threaded",
            self.venue_slug,
            symbol_records.len(),
        );
        let venue_slug = self.venue_slug.clone();
        let venue_factor_plan = self.venue_factor_plan.clone();
        let zscore_config = self.zscore_config.clone();
        let mut publisher = self.publisher.take();

        let mut total_loaded = 0usize;
        let mut total_calculated = 0u64;
        let mut total_reload = 0u64;
        let mut total_bootstrap_published = 0u64;

        let mut symbol_records_vec: Vec<_> = symbol_records;
        symbol_records_vec.sort_unstable_by(|a, b| a.symbol.cmp(&b.symbol));

        for sr in symbol_records_vec {
            let symbol_result = Self::replay_symbol_records(
                &venue_slug,
                venue_factor_plan.as_ref(),
                sr,
                &zscore_config,
                publisher.as_mut(),
            );
            let BootstrapSymbolReplayResult {
                symbol,
                loaded,
                calculated,
                reload,
                all_ready_seen,
                state,
                rolling_stats,
                norm_state,
                bootstrap_published,
            } = symbol_result;
            total_loaded = total_loaded.saturating_add(loaded);
            total_calculated = total_calculated.saturating_add(calculated);
            total_reload = total_reload.saturating_add(reload);
            if all_ready_seen {
                self.symbol_all_ready_seen.insert(symbol.clone());
            }
            total_bootstrap_published =
                total_bootstrap_published.saturating_add(bootstrap_published);
            self.symbol_states.insert(symbol.clone(), state);
            self.symbol_rolling_stats
                .insert(symbol.clone(), rolling_stats);
            self.symbol_norm_states.insert(symbol.clone(), norm_state);
            info!(
                "FusionFactorPubApp[{}] rocksdb bootstrap symbol done: symbol={} loaded={} calculated={} reload={} published={} total_loaded={}",
                self.venue_slug,
                symbol,
                loaded,
                calculated,
                reload,
                bootstrap_published,
                total_loaded
            );
        }

        info!(
            "FusionFactorPubApp[{}] rocksdb bootstrap done: symbols={} total_loaded={} total_calculated={} total_reload={} total_published={} elapsed_ms={}",
            self.venue_slug,
            self.allowed_symbols.len(),
            total_loaded,
            total_calculated,
            total_reload,
            total_bootstrap_published,
            started.elapsed().as_millis()
        );
        self.publisher = publisher;
        Ok(())
    }

    fn replay_symbol_records(
        venue_slug: &str,
        venue_factor_plan: Option<&SymbolFactorPlan>,
        symbol_records: BootstrapSymbolRecords,
        zscore_config: &ZscoreRuntimeConfig,
        mut publisher: Option<&mut FusionFactorPublisher>,
    ) -> BootstrapSymbolReplayResult {
        let BootstrapSymbolRecords {
            symbol,
            mut records,
        } = symbol_records;
        let mut state = SymbolCalcState::default();
        let mut loaded = 0usize;
        let mut calculated = 0u64;
        let mut reload = 0u64;
        let mut all_ready_seen = false;
        let mut bootstrap_published = 0u64;
        let mut interval_loaded = 0u64;
        let mut interval_calculated = 0u64;
        let mut interval_reload = 0u64;
        let mut interval_warming_up = 0u64;
        let mut interval_other_status = 0u64;
        let mut rolling_stats = SymbolRollingStats::default();
        let mut norm_state = SymbolNormState::new(zscore_config.window_size, 0);
        let plan = venue_factor_plan;
        let needs_factor_118 = plan
            .map(|plan| {
                plan.ordered_factors
                    .iter()
                    .any(|factor| factor.factor_id == Some(FusionFactorId::Factor118))
            })
            .unwrap_or(false);

        let record_len = records.len();
        for i in 0..record_len {
            let corrected = Self::validate_and_fix_trade_flow(venue_slug, &symbol, &mut records[i]);
            let _ = corrected;
            let msg = &records[i];
            let depth_derived = parse_embedded_depth(msg)
                .as_ref()
                .map(DepthDerived::from_snapshot);
            let depth_opt = depth_derived.as_ref();
            state.push_trade_flow(msg);
            if let Some(depth) = depth_opt {
                state.push_depth_metrics_derived(depth);
            }
            Self::update_symbol_rolling_stats(&mut state, &mut rolling_stats, msg, depth_opt);
            let latest_eval = match plan {
                Some(plan) => {
                    let factor_118_result = if needs_factor_118 {
                        depth_opt.and_then(|depth| {
                            Self::compute_factor_118_with_state(&mut state, depth)
                        })
                    } else {
                        None
                    };
                    let series = Self::build_symbol_series_from_state(&mut state);
                    let eval_result = Self::evaluate_ordered_factors_with_plan(
                        plan,
                        factor_118_result,
                        depth_opt,
                        Some(&series),
                    );
                    if eval_result.status == 0 {
                        all_ready_seen = true;
                        let normalized = normalize_feature_values(
                            &mut norm_state,
                            &eval_result.factor_values,
                            zscore_config,
                        );
                        if let (Some(normalized), Some(publisher)) =
                            (normalized, publisher.as_deref_mut())
                        {
                            let ts_ms = msg.ts / 1000;
                            let feature_msg = FeatureMsg::create(
                                symbol.clone(),
                                ts_ms,
                                FeatureStatus::Reload as u8,
                                normalized,
                            );
                            if let Ok(bytes) = feature_msg.to_bytes() {
                                publisher.publish(&bytes);
                                bootstrap_published = bootstrap_published.saturating_add(1);
                            }
                        }
                    } else if eval_result.status == 1 && all_ready_seen {
                        let warming_factors: Vec<&str> = eval_result
                            .factor_issues
                            .iter()
                            .filter(|issue| issue.contains(":warming_up"))
                            .map(|issue| issue.as_str())
                            .collect();
                        panic!(
                            "fusion factor regressed to warming_up after all_ready: venue={} symbol={} trade_ts={} warming_factors=[{}]",
                            venue_slug,
                            symbol,
                            msg.ts,
                            warming_factors.join(",")
                        );
                    }
                    ReplayEvalSummary::from_eval(&eval_result, true)
                }
                None => ReplayEvalSummary::missing_plan(),
            };
            loaded = loaded.saturating_add(1);
            interval_loaded = interval_loaded.saturating_add(1);
            calculated = calculated.saturating_add(1);
            interval_calculated = interval_calculated.saturating_add(1);
            match latest_eval.status {
                x if x == FeatureStatus::Reload as u8 => {
                    reload = reload.saturating_add(1);
                    interval_reload = interval_reload.saturating_add(1);
                }
                x if x == FeatureStatus::WarmingUp as u8 => {
                    interval_warming_up = interval_warming_up.saturating_add(1);
                }
                _ => interval_other_status = interval_other_status.saturating_add(1),
            }

            if loaded % ROCKSDB_BOOTSTRAP_LOG_EVERY == 0 {
                info!(
                    "FusionFactorPubApp[{}] rocksdb bootstrap progress: symbol={} interval_loaded={} interval_calculated={} interval_reload={} interval_warming_up={} interval_other={}",
                    venue_slug,
                    symbol,
                    interval_loaded,
                    interval_calculated,
                    interval_reload,
                    interval_warming_up,
                    interval_other_status
                );
                interval_loaded = 0;
                interval_calculated = 0;
                interval_reload = 0;
                interval_warming_up = 0;
                interval_other_status = 0;
            }
        }

        BootstrapSymbolReplayResult {
            symbol,
            loaded,
            calculated,
            reload,
            all_ready_seen,
            state,
            rolling_stats,
            norm_state,
            bootstrap_published,
        }
    }

    fn validate_and_fix_trade_flow(
        venue_slug: &str,
        symbol: &str,
        msg: &mut TradeFlowFeatureMsg,
    ) -> bool {
        const PRICE_FIELDS: &[(usize, &str)] = &[
            (FIELD_OPEN, "open"),
            (FIELD_HIGH, "high"),
            (FIELD_LOW, "low"),
            (FIELD_CLOSE, "close"),
            (FIELD_VWAP, "vwap"),
            (FIELD_BUY_VWAP, "buy_vwap"),
            (FIELD_SELL_VWAP, "sell_vwap"),
        ];
        for &(idx, name) in PRICE_FIELDS {
            let v = msg.values[idx];
            if v == 0.0 || !v.is_finite() {
                panic!(
                    "fusion input price is zero or non-finite: venue={} symbol={} ts={} field={} value={}",
                    venue_slug, symbol, msg.ts, name, v
                );
            }
        }

        const VOLUME_EPS: f64 = 1e-12;
        // buy/sell 分项在单边行情时为 0 是正常的，静默修正
        for idx in [
            FIELD_BUY_AMOUNT,
            FIELD_SELL_AMOUNT,
            FIELD_BUY_VOLUME,
            FIELD_SELL_VOLUME,
        ] {
            if msg.values[idx] == 0.0 {
                msg.values[idx] = VOLUME_EPS;
            }
        }
        // 总量 volume/amount 为 0 才是异常
        let mut corrected = false;
        for idx in [FIELD_VOLUME, FIELD_AMOUNT] {
            if msg.values[idx] == 0.0 {
                msg.values[idx] = VOLUME_EPS;
                corrected = true;
            }
        }
        corrected
    }

    fn update_symbol_rolling_stats(
        state: &mut SymbolCalcState,
        rolling: &mut SymbolRollingStats,
        msg: &TradeFlowFeatureMsg,
        depth: Option<&DepthDerived>,
    ) {
        let open = msg.values[FIELD_OPEN];
        let close = msg.values[FIELD_CLOSE];
        let volume = msg.values[FIELD_VOLUME];

        rolling.corr_open_volume_300.push(open, volume);
        rolling.corr_close_volume_14.push(close, volume);
        state.corr_open_volume_300_last = finite_opt(rolling.corr_open_volume_300.corr());
        state.corr_close_volume_14_last = finite_opt(rolling.corr_close_volume_14.corr());

        if let Some(depth) = depth {
            let (bid0p, bid0v) = depth.best_bid();
            let (ask0p, ask0v) = depth.best_ask();
            let mid = (ask0p + bid0p) / 2.0;
            let mid_vol = (bid0v + ask0v) / 2.0;
            rolling.corr_mid_midvol_300.push(mid, mid_vol);
            state.corr_mid_midvol_300_last = finite_opt(rolling.corr_mid_midvol_300.corr());
        }
    }

    fn publish_rl_return_volatility(&mut self, symbol: &str, timestamp_ms: i64) {
        let Some(state) = self.symbol_states.get(symbol) else {
            self.rl_publish_failed_count = self.rl_publish_failed_count.saturating_add(1);
            warn!(
                "rl publish skipped: missing symbol state, venue={} symbol={}",
                self.venue_slug, symbol
            );
            return;
        };

        let (value, ready) = match compute_rl_return_volatility(
            &state.close,
            self.rl_config.pct_change_period,
            self.rl_config.rolling_window,
        ) {
            Ok(Some(raw)) => {
                let scaled = self.rl_config.apply_scale_and_clip(raw);
                if scaled.is_finite() {
                    (scaled, true)
                } else {
                    (0.0, false)
                }
            }
            Ok(None) => (0.0, false),
            Err(err) => {
                self.rl_publish_failed_count = self.rl_publish_failed_count.saturating_add(1);
                warn!(
                    "rl compute failed: venue={} symbol={} err={}",
                    self.venue_slug, symbol, err
                );
                return;
            }
        };

        if self
            .rl_publisher
            .publish(symbol, value, timestamp_ms, ready)
        {
            self.rl_published_count = self.rl_published_count.saturating_add(1);
        } else {
            self.rl_publish_failed_count = self.rl_publish_failed_count.saturating_add(1);
        }
    }

    fn on_trade_flow(
        &mut self,
        symbol: String,
        msg: crate::common::trade_flow_feature_msg::TradeFlowFeatureMsg,
    ) {
        let _ = self.apply_trade_flow_msg(symbol, msg, true);
    }

    fn apply_trade_flow_msg(
        &mut self,
        symbol: String,
        mut msg: crate::common::trade_flow_feature_msg::TradeFlowFeatureMsg,
        emit_output: bool,
    ) -> Option<ReplayEvalSummary> {
        let corrected = Self::validate_and_fix_trade_flow(&self.venue_slug, &symbol, &mut msg);

        if corrected && symbol == "BTCUSDT" {
            if let Some(state) = self.symbol_states.get(&symbol) {
                let n = state.volume.len();
                let prev2 = if n >= 2 {
                    format!(
                        "ts_offset=-2 volume={} amount={} buy_amount={} sell_amount={} buy_volume={} sell_volume={}",
                        state.volume[n - 2], state.amount[n - 2],
                        state.buy_amount[n - 2], state.sell_amount[n - 2],
                        state.buy_volume[n - 2], state.sell_volume[n - 2],
                    )
                } else {
                    "ts_offset=-2 N/A".to_string()
                };
                let prev1 = if n >= 1 {
                    format!(
                        "ts_offset=-1 volume={} amount={} buy_amount={} sell_amount={} buy_volume={} sell_volume={}",
                        state.volume[n - 1], state.amount[n - 1],
                        state.buy_amount[n - 1], state.sell_amount[n - 1],
                        state.buy_volume[n - 1], state.sell_volume[n - 1],
                    )
                } else {
                    "ts_offset=-1 N/A".to_string()
                };
                warn!(
                    "zero volume/amount context: symbol={} current_ts={} current volume={} amount={} buy_amount={} sell_amount={} buy_volume={} sell_volume={} | {} | {}",
                    symbol, msg.ts,
                    msg.values[FIELD_VOLUME], msg.values[FIELD_AMOUNT],
                    msg.values[FIELD_BUY_AMOUNT], msg.values[FIELD_SELL_AMOUNT],
                    msg.values[FIELD_BUY_VOLUME], msg.values[FIELD_SELL_VOLUME],
                    prev2, prev1,
                );
            }
        }

        if emit_output {
            self.trade_flow_count = self.trade_flow_count.saturating_add(1);
            self.trigger_count = self.trigger_count.saturating_add(1);
        }

        let depth_derived = parse_embedded_depth(&msg)
            .as_ref()
            .map(DepthDerived::from_snapshot);
        let depth_opt = depth_derived.as_ref();
        {
            let (symbol_states, symbol_rolling_stats) =
                (&mut self.symbol_states, &mut self.symbol_rolling_stats);
            let state = symbol_states.entry(symbol.clone()).or_default();
            let rolling = symbol_rolling_stats.entry(symbol.clone()).or_default();
            state.push_trade_flow(&msg);
            if let Some(depth) = depth_opt {
                state.push_depth_metrics_derived(depth);
            }
            Self::update_symbol_rolling_stats(state, rolling, &msg, depth_opt);
        }

        if emit_output {
            // 与历史 kline_factor_pub 对齐: 继续发布单因子 rl_return_volatility 到旧 topic。
            self.publish_rl_return_volatility(&symbol, msg.ts);
        }

        if emit_output {
            if depth_opt.is_none() {
                self.missing_depth_count = self.missing_depth_count.saturating_add(1);
            } else {
                self.depth_attached_count = self.depth_attached_count.saturating_add(1);
            }
        }

        if self.venue_factor_plan.is_none() {
            return Some(ReplayEvalSummary::missing_plan());
        }

        let eval_started = Instant::now();
        let Some(eval_result) = self.evaluate_ordered_factors(&symbol, depth_opt) else {
            if emit_output {
                warn!(
                    "fusion-trigger: venue={} symbol={} trade_ts={} reason=missing_factor_plan",
                    self.venue_slug, symbol, msg.ts
                );
            }
            return Some(ReplayEvalSummary::missing_plan());
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
            panic!(
                "fusion factor regressed to warming_up after all_ready: venue={} symbol={} trade_ts={} warming_factors=[{}]",
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
                            "FusionFactorPubApp[{}] published: symbol={} ts_ms={} bytes={} total={}",
                            self.venue_slug, feature_msg.symbol, ts_ms, bytes.len(), self.published_count
                        );
                    }
                } else {
                    self.publish_failed_count = self.publish_failed_count.saturating_add(1);
                    warn!(
                        "FusionFactorPubApp[{}] publish failed: symbol={} ts_ms={} total_failed={}",
                        self.venue_slug, feature_msg.symbol, ts_ms, self.publish_failed_count
                    );
                }
            }
            Err(e) => {
                self.publish_failed_count = self.publish_failed_count.saturating_add(1);
                warn!(
                    "FusionFactorPubApp[{}] FeatureMsg serialize failed: {}",
                    self.venue_slug, e
                );
            }
        }
        Some(replay_summary)
    }

    fn evaluate_ordered_factors(
        &mut self,
        symbol: &str,
        depth: Option<&DepthDerived>,
    ) -> Option<OrderedEvalResult> {
        let needs_factor_118 = {
            let venue_factor_plan = self.venue_factor_plan.as_ref()?;
            venue_factor_plan
                .ordered_factors
                .iter()
                .any(|factor| factor.factor_id == Some(FusionFactorId::Factor118))
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
        let plan = self.venue_factor_plan.as_ref()?;
        Some(Self::evaluate_ordered_factors_with_plan(
            plan,
            factor_118_result,
            depth,
            Some(&series),
        ))
    }

    fn evaluate_ordered_factors_with_plan(
        plan: &SymbolFactorPlan,
        factor_118_result: Option<(f64, bool, usize)>,
        depth: Option<&DepthDerived>,
        series: Option<&SymbolSeries<'_>>,
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
                    if ready && binding.factor_id == Some(FusionFactorId::Factor118) {
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
            "FusionFactorPubApp[{}] factor-summary: symbol={} plan={} good={} bad={} eval_cost_us={} (warming_up={} nan_fill={} invalid={} missing_depth={}) non_warming=[{}]",
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

    pub fn build_symbol_series_from_state(state: &mut SymbolCalcState) -> SymbolSeries<'_> {
        SymbolSeries {
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
            bid_vwap20: SplitSlice::from_parts(state.bid_vwap20.as_slices()),
            total_bid20: SplitSlice::from_parts(state.total_bid20.as_slices()),
            total_ask20: SplitSlice::from_parts(state.total_ask20.as_slices()),
            top10_bid_volume: SplitSlice::from_parts(state.top10_bid_volume.as_slices()),
            top10_ask_volume: SplitSlice::from_parts(state.top10_ask_volume.as_slices()),
            top10_bid_mean: SplitSlice::from_parts(state.top10_bid_mean.as_slices()),
            top10_ask_mean: SplitSlice::from_parts(state.top10_ask_mean.as_slices()),
            bid9v: SplitSlice::from_parts(state.bid9v.as_slices()),
            ask9v: SplitSlice::from_parts(state.ask9v.as_slices()),
            bid9p: SplitSlice::from_parts(state.bid9p.as_slices()),
            ask9p: SplitSlice::from_parts(state.ask9p.as_slices()),
            combined_level9_volume: SplitSlice::from_parts(
                state.combined_level9_volume.as_slices(),
            ),
            mean_bid_vol20: SplitSlice::from_parts(state.mean_bid_vol20.as_slices()),
            mean_bid_price5: SplitSlice::from_parts(state.mean_bid_price5.as_slices()),
            mean_bid_price20: SplitSlice::from_parts(state.mean_bid_price20.as_slices()),
            avg_ask_price5: SplitSlice::from_parts(state.avg_ask_price5.as_slices()),
            mean_ask_price15: SplitSlice::from_parts(state.mean_ask_price15.as_slices()),
            ask_pv15_mean: SplitSlice::from_parts(state.ask_pv15_mean.as_slices()),
            bid_pv15_mean: SplitSlice::from_parts(state.bid_pv15_mean.as_slices()),
            factor_031_ratio: SplitSlice::from_parts(state.factor_031_ratio.as_slices()),
            factor_119_mid_minus_ask_vwap5: SplitSlice::from_parts(
                state.factor_119_mid_minus_ask_vwap5.as_slices(),
            ),
            total_volume20_sum: SplitSlice::from_parts(state.total_volume20_sum.as_slices()),
            median_all_price40: SplitSlice::from_parts(state.median_all_price40.as_slices()),
            factor_152_pct_mean: SplitSlice::from_parts(state.factor_152_pct_mean.as_slices()),
            ask_vwap_diff_5_20: SplitSlice::from_parts(state.ask_vwap_diff_5_20.as_slices()),
            ask_mean_volume_20: SplitSlice::from_parts(state.ask_mean_volume_20.as_slices()),
            ask0v: SplitSlice::from_parts(state.ask0v.as_slices()),
            ask_vwap20: SplitSlice::from_parts(state.ask_vwap20.as_slices()),
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
        binding: &FactorBinding,
        factor_118_result: Option<(f64, bool, usize)>,
        depth: Option<&DepthDerived>,
        series: Option<&SymbolSeries<'_>>,
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
            Some(FusionFactorId::Factor118) => {
                let out = match factor_118_result {
                    Some((value, ready, _samples)) => {
                        let status = if ready { "ready" } else { "warming_up" };
                        (value, ready, status)
                    }
                    None => (f64::NAN, false, "missing_depth"),
                };
                return Some(out);
            }
            Some(FusionFactorId::FactorTrades001) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_001),
                ));
            }
            Some(FusionFactorId::FactorTrades002) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_002),
                ));
            }
            Some(FusionFactorId::FactorTrades003) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_003),
                ));
            }
            Some(FusionFactorId::FactorTrades004) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_004),
                ));
            }
            Some(FusionFactorId::FactorTrades005) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_005),
                ));
            }
            Some(FusionFactorId::FactorTrades006) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_006),
                ));
            }
            Some(FusionFactorId::FactorTrades007) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_007),
                ));
            }
            Some(FusionFactorId::FactorTrades008) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_008),
                ));
            }
            Some(FusionFactorId::FactorTrades009) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_009),
                ));
            }
            Some(FusionFactorId::FactorTrades010) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_010),
                ));
            }
            Some(FusionFactorId::FactorTrades011) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_011),
                ));
            }
            Some(FusionFactorId::FactorTrades012) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_012),
                ));
            }
            Some(FusionFactorId::FactorTrades013) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_013),
                ));
            }
            Some(FusionFactorId::FactorTrades014) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_014),
                ));
            }
            Some(FusionFactorId::FactorTrades015) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_015),
                ));
            }
            Some(FusionFactorId::FactorTrades016) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_016),
                ));
            }
            Some(FusionFactorId::FactorTrades017) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_017),
                ));
            }
            Some(FusionFactorId::FactorTrades018) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_018),
                ));
            }
            Some(FusionFactorId::FactorTrades019) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_019),
                ));
            }
            Some(FusionFactorId::FactorTrades020) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_020),
                ));
            }
            Some(FusionFactorId::FactorTrades021) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_021),
                ));
            }
            Some(FusionFactorId::FactorTrades022) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_022),
                ));
            }
            Some(FusionFactorId::FactorTrades023) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_023),
                ));
            }
            Some(FusionFactorId::FactorTrades024) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_024),
                ));
            }
            Some(FusionFactorId::FactorTrades025) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_025),
                ));
            }
            Some(FusionFactorId::FactorTrades026) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_026),
                ));
            }
            Some(FusionFactorId::FactorTrades027) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_027),
                ));
            }
            Some(FusionFactorId::FactorTrades028) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_028),
                ));
            }
            Some(FusionFactorId::FactorTrades029) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_029),
                ));
            }
            Some(FusionFactorId::FactorTrades030) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_030),
                ));
            }
            Some(FusionFactorId::FactorTrades031) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_031),
                ));
            }
            Some(FusionFactorId::FactorTrades032) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_032),
                ));
            }
            Some(FusionFactorId::FactorTrades033) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_033),
                ));
            }
            Some(FusionFactorId::FactorTrades034) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_034),
                ));
            }
            Some(FusionFactorId::FactorTrades035) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_035),
                ));
            }
            Some(FusionFactorId::FactorTrades036) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_036),
                ));
            }
            Some(FusionFactorId::FactorTrades037) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_037),
                ));
            }
            Some(FusionFactorId::FactorTrades038) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_038),
                ));
            }
            Some(FusionFactorId::FactorTrades039) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_039),
                ));
            }
            Some(FusionFactorId::FactorTrades040) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_040),
                ));
            }
            Some(FusionFactorId::FactorTrades041) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_041),
                ));
            }
            Some(FusionFactorId::FactorTrades042) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_042),
                ));
            }
            Some(FusionFactorId::FactorTrades043) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_043),
                ));
            }
            Some(FusionFactorId::FactorTrades044) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_044),
                ));
            }
            Some(FusionFactorId::FactorTrades045) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_045),
                ));
            }
            Some(FusionFactorId::FactorTrades046) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_046),
                ));
            }
            Some(FusionFactorId::FactorTrades047) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_047),
                ));
            }
            Some(FusionFactorId::FactorTrades048) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_048),
                ));
            }
            Some(FusionFactorId::FactorTrades049) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_049),
                ));
            }
            Some(FusionFactorId::FactorTrades050) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_050),
                ));
            }
            Some(FusionFactorId::TdTi010) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_010),
                ));
            }
            Some(FusionFactorId::TdTi015) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_015),
                ));
            }
            Some(FusionFactorId::TdTi026) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_026),
                ));
            }
            Some(FusionFactorId::TdTi031) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_031),
                ));
            }
            Some(FusionFactorId::TdTi033) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_033),
                ));
            }
            Some(FusionFactorId::TdTi034) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_034),
                ));
            }
            Some(FusionFactorId::TdTi036) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_036),
                ));
            }
            Some(FusionFactorId::TdTi037) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ti_037),
                ));
            }
            Some(FusionFactorId::TdMt003) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_mt_003),
                ));
            }
            Some(FusionFactorId::TdMt008) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_mt_008),
                ));
            }
            Some(FusionFactorId::TdMt014) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_mt_014),
                ));
            }
            Some(FusionFactorId::TdMt015) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_mt_015),
                ));
            }
            Some(FusionFactorId::TdMt039) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_mt_039),
                ));
            }
            Some(FusionFactorId::TpVpi004) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_004),
                ));
            }
            Some(FusionFactorId::TpVpi006) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_006),
                ));
            }
            Some(FusionFactorId::TpVpi001) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_001),
                ));
            }
            Some(FusionFactorId::TpVpi002) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_002),
                ));
            }
            Some(FusionFactorId::TpVpi005) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_005),
                ));
            }
            Some(FusionFactorId::TpVpi015) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_015),
                ));
            }
            Some(FusionFactorId::TpVpi014) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_014),
                ));
            }
            Some(FusionFactorId::TpVpi017) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_tp_vpi_017),
                ));
            }
            Some(FusionFactorId::TdPt001) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_001),
                ));
            }
            Some(FusionFactorId::TdPt002) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_002),
                ));
            }
            Some(FusionFactorId::TdPt003) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_003),
                ));
            }
            Some(FusionFactorId::TdPt004) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_004),
                ));
            }
            Some(FusionFactorId::TdPt010) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_010),
                ));
            }
            Some(FusionFactorId::TdPt027) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pt_027),
                ));
            }
            Some(FusionFactorId::TdVi010) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_vi_010),
                ));
            }
            Some(FusionFactorId::TdVi011) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_vi_011),
                ));
            }
            Some(FusionFactorId::TdVi025) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_vi_025),
                ));
            }
            Some(FusionFactorId::TdVi026) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_vi_026),
                ));
            }
            Some(FusionFactorId::TdVi028) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_vi_028),
                ));
            }
            Some(FusionFactorId::TdCi007) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ci_007),
                ));
            }
            Some(FusionFactorId::TdCi003) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ci_003),
                ));
            }
            Some(FusionFactorId::TdCi008) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_ci_008),
                ));
            }
            Some(FusionFactorId::TdPr001) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_001),
                ));
            }
            Some(FusionFactorId::TdPr002) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_002),
                ));
            }
            Some(FusionFactorId::TdPr005) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_005),
                ));
            }
            Some(FusionFactorId::TdPr006) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_006),
                ));
            }
            Some(FusionFactorId::TdPr007) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_007),
                ));
            }
            Some(FusionFactorId::TdPr015) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_015),
                ));
            }
            Some(FusionFactorId::TdPr016) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_016),
                ));
            }
            Some(FusionFactorId::TdPr017) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_td_pr_017),
                ));
            }
            Some(FusionFactorId::Factor001) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_001),
                ));
            }
            Some(FusionFactorId::Factor002) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_002),
                ));
            }
            Some(FusionFactorId::Factor003) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_003),
                ));
            }
            Some(FusionFactorId::Factor004) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(|s| Self::compute_factor_004(s, depth)),
                ));
            }
            Some(FusionFactorId::Factor006) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_006),
                ));
            }
            Some(FusionFactorId::Factor008) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_008),
                ));
            }
            Some(FusionFactorId::Factor009) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_009),
                ));
            }
            Some(FusionFactorId::Factor010) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_010),
                ));
            }
            Some(FusionFactorId::Factor011) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_011),
                ));
            }
            Some(FusionFactorId::Factor012) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_012),
                ));
            }
            Some(FusionFactorId::Factor014) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_014),
                ));
            }
            Some(FusionFactorId::Factor016) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_016),
                ));
            }
            Some(FusionFactorId::Factor017) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_017),
                ));
            }
            Some(FusionFactorId::Factor018) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_018),
                ));
            }
            Some(FusionFactorId::Factor019) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_019),
                ));
            }
            Some(FusionFactorId::Factor020) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_020),
                ));
            }
            Some(FusionFactorId::Factor021) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_021),
                ));
            }
            Some(FusionFactorId::Factor022) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_022),
                ));
            }
            Some(FusionFactorId::Factor023) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_023),
                ));
            }
            Some(FusionFactorId::Factor024) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_024),
                ));
            }
            Some(FusionFactorId::Factor025) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_025),
                ));
            }
            Some(FusionFactorId::Factor026) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_026),
                ));
            }
            Some(FusionFactorId::Factor027) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_027),
                ));
            }
            Some(FusionFactorId::Factor028) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_028),
                ));
            }
            Some(FusionFactorId::Factor029) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_029),
                ));
            }
            Some(FusionFactorId::Factor030) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_030),
                ));
            }
            Some(FusionFactorId::Factor031) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_031),
                ));
            }
            Some(FusionFactorId::Factor032) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_032),
                ));
            }
            Some(FusionFactorId::Factor033) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_033),
                ));
            }
            Some(FusionFactorId::Factor035) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_035),
                ));
            }
            Some(FusionFactorId::Factor036) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_036),
                ));
            }
            Some(FusionFactorId::Factor037) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_037),
                ));
            }
            Some(FusionFactorId::Factor038) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_038),
                ));
            }
            Some(FusionFactorId::Factor040) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_040),
                ));
            }
            Some(FusionFactorId::Factor041) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_041),
                ));
            }
            Some(FusionFactorId::Factor042) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_042),
                ));
            }
            Some(FusionFactorId::Factor043) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_043),
                ));
            }
            Some(FusionFactorId::Factor045) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_045),
                ));
            }
            Some(FusionFactorId::Factor046) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_046),
                ));
            }
            Some(FusionFactorId::Factor047) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_047),
                ));
            }
            Some(FusionFactorId::Factor048) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_048),
                ));
            }
            Some(FusionFactorId::Factor049) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_049),
                ));
            }
            Some(FusionFactorId::Factor051) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_051),
                ));
            }
            Some(FusionFactorId::Factor052) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_052),
                ));
            }
            Some(FusionFactorId::Factor053) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_053),
                ));
            }
            Some(FusionFactorId::Factor054) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_054),
                ));
            }
            Some(FusionFactorId::Factor055) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_055),
                ));
            }
            Some(FusionFactorId::Factor056) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_056),
                ));
            }
            Some(FusionFactorId::Factor057) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_057),
                ));
            }
            Some(FusionFactorId::Factor058) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_058),
                ));
            }
            Some(FusionFactorId::Factor059) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_059),
                ));
            }
            Some(FusionFactorId::Factor060) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_060),
                ));
            }
            Some(FusionFactorId::Factor061) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_061),
                ));
            }
            Some(FusionFactorId::Factor062) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_062),
                ));
            }
            Some(FusionFactorId::Factor063) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_063),
                ));
            }
            Some(FusionFactorId::Factor064) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_064),
                ));
            }
            Some(FusionFactorId::Factor065) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_065),
                ));
            }
            Some(FusionFactorId::Factor066) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_066),
                ));
            }
            Some(FusionFactorId::Factor067) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_067),
                ));
            }
            Some(FusionFactorId::Factor068) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_068),
                ));
            }
            Some(FusionFactorId::Factor069) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_069),
                ));
            }
            Some(FusionFactorId::Factor070) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_070),
                ));
            }
            Some(FusionFactorId::Factor073) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_073),
                ));
            }
            Some(FusionFactorId::Factor074) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_074),
                ));
            }
            Some(FusionFactorId::Factor075) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_075),
                ));
            }
            Some(FusionFactorId::Factor076) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_076),
                ));
            }
            Some(FusionFactorId::Factor077) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_077),
                ));
            }
            Some(FusionFactorId::Factor079) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_079),
                ));
            }
            Some(FusionFactorId::Factor080) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_080),
                ));
            }
            Some(FusionFactorId::Factor085) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_085),
                ));
            }
            Some(FusionFactorId::Factor086) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_086),
                ));
            }
            Some(FusionFactorId::Factor087) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_087),
                ));
            }
            Some(FusionFactorId::Factor088) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_088),
                ));
            }
            Some(FusionFactorId::Factor089) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_089),
                ));
            }
            Some(FusionFactorId::Factor091) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_091),
                ));
            }
            Some(FusionFactorId::Factor093) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_093),
                ));
            }
            Some(FusionFactorId::Factor094) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_094),
                ));
            }
            Some(FusionFactorId::Factor095) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_095),
                ));
            }
            Some(FusionFactorId::Factor096) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_096),
                ));
            }
            Some(FusionFactorId::Factor097) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_097),
                ));
            }
            Some(FusionFactorId::Factor102) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_102),
                ));
            }
            Some(FusionFactorId::Factor103) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_103),
                ));
            }
            Some(FusionFactorId::Factor107) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_107),
                ));
            }
            Some(FusionFactorId::Factor108) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_108),
                ));
            }
            Some(FusionFactorId::Factor110) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_110),
                ));
            }
            Some(FusionFactorId::Factor111) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_111),
                ));
            }
            Some(FusionFactorId::Factor113) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_113),
                ));
            }
            Some(FusionFactorId::Factor114) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_114),
                ));
            }
            Some(FusionFactorId::Factor115) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_115),
                ));
            }
            Some(FusionFactorId::Factor116) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_116),
                ));
            }
            Some(FusionFactorId::Factor119) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_119),
                ));
            }
            Some(FusionFactorId::Factor120) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_120),
                ));
            }
            Some(FusionFactorId::Factor121) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_121),
                ));
            }
            Some(FusionFactorId::Factor122) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_122),
                ));
            }
            Some(FusionFactorId::Factor123) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_123),
                ));
            }
            Some(FusionFactorId::Factor124) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_124),
                ));
            }
            Some(FusionFactorId::Factor125) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_125),
                ));
            }
            Some(FusionFactorId::Factor126) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_126),
                ));
            }
            Some(FusionFactorId::Factor128) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_128),
                ));
            }
            Some(FusionFactorId::Factor129) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_129),
                ));
            }
            Some(FusionFactorId::Factor130) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_130),
                ));
            }
            Some(FusionFactorId::Factor133) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_133),
                ));
            }
            Some(FusionFactorId::Factor134) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_134),
                ));
            }
            Some(FusionFactorId::Factor139) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_139),
                ));
            }
            Some(FusionFactorId::Factor144) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_144),
                ));
            }
            Some(FusionFactorId::Factor151) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_151),
                ));
            }
            Some(FusionFactorId::Factor152) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_152),
                ));
            }
            Some(FusionFactorId::Factor156) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_156),
                ));
            }
            Some(FusionFactorId::Factor159) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_159),
                ));
            }
            Some(FusionFactorId::Factor160) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_160),
                ));
            }
            Some(FusionFactorId::Factor165) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_165),
                ));
            }
            Some(FusionFactorId::Factor166) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_166),
                ));
            }
            Some(FusionFactorId::Factor168) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_168),
                ));
            }
            Some(FusionFactorId::Factor170) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_170),
                ));
            }
            Some(FusionFactorId::Factor175) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_175),
                ));
            }
            Some(FusionFactorId::Factor176) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_176),
                ));
            }
            Some(FusionFactorId::Factor164) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_164),
                ));
            }
            Some(_) | None => {}
        }
        if let Some(extra_factor_id) = binding.extra_factor_id {
            return Some(Self::wrap_factor_value(Self::compute_extra_factor(
                series,
                extra_factor_id,
            )));
        }
        None
    }

    fn compute_extra_factor(
        series: Option<&SymbolSeries<'_>>,
        extra_factor_id: ExtraFactorId,
    ) -> Option<f64> {
        let series = series?;
        match extra_factor_id {
            ExtraFactorId::AvgPrice => rolling_kurt_last(&series.vwap, 250, true, false)
                .ok()
                .flatten(),
            ExtraFactorId::BuyAvgPrice => rolling_skew_last(&series.buy_vwap, 500, false)
                .ok()
                .flatten(),
            ExtraFactorId::SellAvgPrice => {
                let n = series.vwap.len().min(series.sell_vwap.len());
                if n < 360 {
                    return None;
                }
                let diff: Vec<f64> = (0..n)
                    .map(|i| series.vwap[i] - series.sell_vwap[i])
                    .collect();
                rolling_mean_last(&diff, 360).ok().flatten()
            }
            ExtraFactorId::SmallBuy => finite_opt(series.small_buy.last().copied()),
            ExtraFactorId::SmallSell => finite_opt(series.small_sell.last().copied()),
            ExtraFactorId::NetBuyLarge => finite_opt(series.net_buy_large.last().copied()),
        }
    }

    pub fn wrap_factor_value(value: Option<f64>) -> (f64, bool, &'static str) {
        match value {
            Some(v) if v.is_finite() => (v, true, "ready"),
            Some(v) => (v, false, "invalid_value"),
            None => (f64::NAN, false, "warming_up"),
        }
    }

    fn compute_factor_trades_001(series: &SymbolSeries<'_>) -> Option<f64> {
        let amount = series.amount.last().copied()?;
        finite_opt(Some((amount + 1e-6).ln()))
    }

    fn compute_factor_trades_002(series: &SymbolSeries<'_>) -> Option<f64> {
        let volume = series.volume.last().copied()?;
        finite_opt(Some((volume + 1e-6).ln()))
    }

    fn compute_factor_trades_003(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_004(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_005(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_006(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_007(series: &SymbolSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_amount.last().copied())
    }

    fn compute_factor_trades_008(series: &SymbolSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_volume.last().copied())
    }

    fn compute_factor_trades_009(series: &SymbolSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_pct.last().copied())
    }

    fn compute_factor_trades_010(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.buy_vwap.len().min(series.sell_vwap.len());
        if n == 0 {
            return None;
        }
        finite_opt(Some(series.buy_vwap[n - 1] - series.sell_vwap[n - 1]))
    }

    fn compute_factor_trades_011(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_012(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_013(series: &SymbolSeries<'_>) -> Option<f64> {
        finite_opt(series.avg_amount.last().copied())
    }

    fn compute_factor_trades_014(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_015(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.sell_amount.len().min(series.sell_count.len());
        if n == 0 {
            return None;
        }
        let sell_count = series.sell_count[n - 1];
        if sell_count.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(series.sell_amount[n - 1] / sell_count)).or(Some(0.0))
    }

    fn compute_factor_trades_016(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_017(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_018(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_019(series: &SymbolSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_large.last().copied())
    }

    fn compute_factor_trades_020(series: &SymbolSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_medium.last().copied())
    }

    fn compute_factor_trades_021(series: &SymbolSeries<'_>) -> Option<f64> {
        finite_opt(series.net_buy_small.last().copied())
    }

    fn compute_factor_trades_022(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_023(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_024(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_025(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_026(series: &SymbolSeries<'_>) -> Option<f64> {
        let amount = series.amount.last().copied()?;
        let cumsum_amount = series.amount.iter().copied().sum::<f64>();
        if cumsum_amount.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(amount / cumsum_amount))
    }

    fn compute_factor_trades_027(series: &SymbolSeries<'_>) -> Option<f64> {
        let volume = series.volume.last().copied()?;
        let cumsum_volume = series.volume.iter().copied().sum::<f64>();
        if cumsum_volume.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(volume / cumsum_volume))
    }

    fn compute_factor_trades_028(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.buy_amount.len().min(series.buy_count.len());
        if n == 0 {
            return None;
        }
        finite_opt(Some(series.buy_amount[n - 1] * series.buy_count[n - 1]))
    }

    fn compute_factor_trades_029(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.sell_amount.len().min(series.sell_count.len());
        if n == 0 {
            return None;
        }
        finite_opt(Some(series.sell_amount[n - 1] * series.sell_count[n - 1])).or(Some(0.0))
    }

    fn compute_factor_trades_030(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_031(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_032(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.open.len().min(series.close.len());
        if n == 0 {
            return None;
        }
        finite_opt(Some(
            (series.close[n - 1] / (series.open[n - 1] + 1e-6)).ln(),
        ))
    }

    fn compute_factor_trades_033(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_034(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.open.len())
            .min(series.close.len());
        if n == 0 {
            return None;
        }
        let upper_shadow = series.high[n - 1] - series.open[n - 1].max(series.close[n - 1]);
        let range_hl = series.high[n - 1] - series.low[n - 1];
        if range_hl.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(upper_shadow / range_hl))
    }

    fn compute_factor_trades_035(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.open.len())
            .min(series.close.len());
        if n == 0 {
            return None;
        }
        let lower_shadow = series.open[n - 1].min(series.close[n - 1]) - series.low[n - 1];
        let range_hl = series.high[n - 1] - series.low[n - 1];
        if range_hl.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(lower_shadow / range_hl))
    }

    fn compute_factor_trades_036(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_037(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_trades_038(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.close.len();
        if n <= 5 {
            return None;
        }
        finite_opt(Some(series.close[n - 1] - series.close[n - 6]))
    }

    fn compute_factor_trades_039(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.close.len();
        if n <= 10 {
            return None;
        }
        finite_opt(Some(series.close[n - 1] - series.close[n - 11]))
    }

    fn compute_factor_trades_040(series: &SymbolSeries<'_>) -> Option<f64> {
        Self::compute_ema_last(&series.close, 5)
    }

    fn compute_factor_trades_041(series: &SymbolSeries<'_>) -> Option<f64> {
        Self::compute_ema_last(&series.close, 10)
    }

    fn compute_factor_trades_042(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_mean_last_with_min_periods(&series.volume, 5, 1)
            .ok()
            .flatten()
            .or(Some(0.0))
    }

    fn compute_factor_trades_043(series: &SymbolSeries<'_>) -> Option<f64> {
        if series.volume.is_empty() {
            return None;
        }
        sample_std_last(&series.volume, 5, 1).or(Some(0.0))
    }

    fn compute_factor_trades_044(series: &SymbolSeries<'_>) -> Option<f64> {
        if series.close.is_empty() {
            return None;
        }
        sample_std_last(&series.close, 10, 1).or(Some(0.0))
    }

    fn compute_factor_trades_045(series: &SymbolSeries<'_>) -> Option<f64> {
        let close = series.close.last().copied()?;
        let ma5 = rolling_mean_last_with_min_periods(&series.close, 5, 1)
            .ok()
            .flatten()?;
        if ma5.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((close - ma5) / ma5))
    }

    fn compute_factor_trades_046(series: &SymbolSeries<'_>) -> Option<f64> {
        let volume = series.volume.last().copied()?;
        let ma_vol = rolling_mean_last_with_min_periods(&series.volume, 5, 1)
            .ok()
            .flatten()?;
        if ma_vol.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(volume / ma_vol))
    }

    fn compute_factor_trades_047(series: &SymbolSeries<'_>) -> Option<f64> {
        let pct_change = Self::build_pct_change_series(&series.close);
        rolling_mean_last_opt_from_series(&pct_change, 5, 1)
            .ok()
            .flatten()
            .or(Some(0.0))
    }

    fn compute_factor_trades_048(series: &SymbolSeries<'_>) -> Option<f64> {
        let pct_change = Self::build_pct_change_series(&series.close);
        tail_skew_last_opt(&pct_change, 10, 3, false)
            .ok()
            .flatten()
            .or(Some(0.0))
    }

    fn compute_factor_trades_049(series: &SymbolSeries<'_>) -> Option<f64> {
        let pct_change = Self::build_pct_change_series(&series.close);
        Self::tail_kurt_last_opt(&pct_change, 10, 4, true, false).or(Some(0.0))
    }

    fn compute_factor_trades_050(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_mean_last_with_min_periods(&series.net_buy_amount, 5, 1)
            .ok()
            .flatten()
            .or(Some(0.0))
    }

    fn compute_ema_last(values: &(impl F64SeriesView + ?Sized), span: usize) -> Option<f64> {
        if span == 0 || values.len() == 0 {
            return None;
        }
        let alpha = 2.0 / (span as f64 + 1.0);
        let mut ema = values.value_at(0);
        if !ema.is_finite() {
            return Some(0.0);
        }
        for i in 1..values.len() {
            let value = values.value_at(i);
            if !value.is_finite() {
                return Some(0.0);
            }
            ema = alpha * value + (1.0 - alpha) * ema;
        }
        finite_opt(Some(ema)).or(Some(0.0))
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
                pct.push(None);
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
                    return Some(0.0);
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
        finite_opt(Some(out)).or(Some(0.0))
    }

    fn compute_factor_001(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.bid_vwap20.len();
        if n < 3 {
            return None;
        }
        let d1 = series.bid_vwap20[n - 1] - series.bid_vwap20[n - 2];
        let d0 = series.bid_vwap20[n - 2] - series.bid_vwap20[n - 3];
        if !d1.is_finite() || !d0.is_finite() || d0.abs() <= 1e-12 {
            return Some(0.0);
        }
        let value = (d1 - d0) / d0;
        finite_opt(Some(value))
    }

    fn compute_factor_002(depth: &DepthDerived) -> Option<f64> {
        let bid = depth.sum_bid_amount(20);
        let ask = depth.sum_ask_amount(20);
        if bid.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(ask / bid))
    }

    fn compute_factor_003(depth: &DepthDerived) -> Option<f64> {
        let bid = depth_sum_price(&depth.bids, 15);
        let ask = depth_sum_price(&depth.asks, 15);
        let den = bid + ask;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid - ask) / den))
    }

    fn compute_factor_004(series: &SymbolSeries<'_>, depth: Option<&DepthDerived>) -> Option<f64> {
        let depth = depth?;
        let buy_vwap = *series.buy_vwap.last()?;
        let (bid0p, _) = depth_best_bid(depth);
        let (ask0p, _) = depth_best_ask(depth);
        let mid = (bid0p + ask0p) / 2.0;
        let value = mid - buy_vwap;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_006(depth: &DepthDerived) -> Option<f64> {
        let mut ask_strength = 0.0;
        let mut bid_strength = 0.0;
        for i in 0..5 {
            ask_strength += depth_level_price(&depth.asks, i) * depth_level_amount(&depth.asks, i);
            bid_strength += depth_level_price(&depth.bids, i) * depth_level_amount(&depth.bids, i);
        }
        let den = bid_strength + ask_strength;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid_strength - ask_strength) / den))
    }

    fn compute_factor_008(depth: &DepthDerived) -> Option<f64> {
        let diffs: Vec<f64> = (0..10)
            .map(|i| {
                depth_level_price(&depth.bids, i) * depth_level_amount(&depth.bids, i)
                    - depth_level_price(&depth.asks, i) * depth_level_amount(&depth.asks, i)
            })
            .collect();
        std_pop(&diffs)
    }

    fn compute_factor_009(depth: &DepthDerived) -> Option<f64> {
        let (bid0p, _) = depth_best_bid(depth);
        let (ask0p, _) = depth_best_ask(depth);
        if bid0p <= 0.0 || ask0p <= 0.0 {
            return Some(0.0);
        }
        let value = ask0p.ln() - bid0p.ln();
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_010(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.bid_vwap20.len();
        if n < 2 {
            return None;
        }
        finite_opt(Some(series.bid_vwap20[n - 1] - series.bid_vwap20[n - 2]))
    }

    fn compute_factor_011(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.ask_vwap20.len();
        if n < 2 {
            return None;
        }
        finite_opt(Some(series.ask_vwap20[n - 1] - series.ask_vwap20[n - 2]))
    }

    fn compute_factor_012(depth: &DepthDerived) -> Option<f64> {
        let bid = depth.bid_pxv_prefix[20];
        let ask = depth.ask_pxv_prefix[20];
        let den = bid + ask;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid - ask) / den))
    }

    fn compute_factor_014(depth: &DepthDerived) -> Option<f64> {
        let top5 = depth_sum_amount(&depth.asks, 5);
        let total = depth_sum_amount(&depth.asks, 20);
        if total.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(top5 / total))
    }

    fn compute_factor_016(depth: &DepthDerived) -> Option<f64> {
        let total_ask_price = depth_sum_price(&depth.asks, 20);
        if (total_ask_price + 1e-6).abs() <= 1e-12 {
            return Some(0.0);
        }
        let mut weighted_depth = 0.0;
        for i in 0..20 {
            let askv = depth_level_amount(&depth.asks, i);
            let askp = depth_level_price(&depth.asks, i);
            if askv.is_finite() && askp.is_finite() {
                weighted_depth += askv * askp / (total_ask_price + 1e-6);
            }
        }
        finite_opt(Some(weighted_depth))
    }

    fn compute_factor_017(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let mean = bids.iter().filter(|v| v.is_finite()).sum::<f64>() / bids.len() as f64;
        let den = mean + 1e-6;
        let norm: Vec<f64> = bids.into_iter().map(|v| v / den).collect();
        std_pop(&norm)
    }

    fn compute_factor_018(depth: &DepthDerived) -> Option<f64> {
        let asks: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        let mean = asks.iter().sum::<f64>() / asks.len() as f64;
        let denom = mean + 1e-6;
        let norm: Vec<f64> = asks.into_iter().map(|v| v / denom).collect();
        std_pop(&norm)
    }

    fn compute_factor_019(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.bid_vwap20.len();
        if n < 2 {
            return None;
        }
        let curr = rolling_mean_at_from_series(&series.bid_vwap20, n, 5, 1)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))?;
        let prev = rolling_mean_at_from_series(&series.bid_vwap20, n - 1, 5, 1)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))?;
        finite_opt(Some(curr - prev))
    }

    fn compute_factor_020(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.ask_vwap20.len();
        if n < 2 {
            return None;
        }
        let curr = rolling_mean_at_from_series(&series.ask_vwap20, n, 5, 1)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))?;
        let prev = rolling_mean_at_from_series(&series.ask_vwap20, n - 1, 5, 1)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))?;
        finite_opt(Some(curr - prev))
    }

    fn compute_factor_021(series: &SymbolSeries<'_>) -> Option<f64> {
        let bid_std = sample_std_last(&series.total_bid20, 10, 1)?;
        let ask_std = sample_std_last(&series.total_ask20, 10, 1)?;
        if ask_std.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid_std / ask_std))
    }

    fn compute_factor_022(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.relative_spread.len();
        if n < 2 {
            return None;
        }
        let value = series.relative_spread[n - 1] - series.relative_spread[n - 2];
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_023(series: &SymbolSeries<'_>) -> Option<f64> {
        let values = &series.top10_bid_volume;
        let last = *values.last()?;
        let ma = rolling_mean_last_with_min_periods(values, 5, 1)
            .ok()
            .flatten()?;
        let value = last - ma;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_024(series: &SymbolSeries<'_>) -> Option<f64> {
        let values = &series.top10_ask_volume;
        let last = *values.last()?;
        let ma = rolling_mean_last_with_min_periods(values, 5, 1)
            .ok()
            .flatten()?;
        let value = last - ma;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_025(depth: &DepthDerived) -> Option<f64> {
        let top3 = depth_sum_amount(&depth.bids, 3);
        let bottom17 = (3..20)
            .map(|i| depth_level_amount(&depth.bids, i))
            .filter(|v| v.is_finite())
            .sum::<f64>();
        if bottom17.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(top3 / bottom17))
    }

    fn compute_factor_026(depth: &DepthDerived) -> Option<f64> {
        let top3 = depth.sum_ask_amount(3);
        let bottom17 = depth.sum_ask_amount(20) - top3;
        if bottom17.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(top3 / bottom17))
    }

    fn compute_factor_027(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        rolling_skew_last(&bids, 20, false).ok().flatten()
    }

    fn compute_factor_028(depth: &DepthDerived) -> Option<f64> {
        let asks: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        rolling_skew_last(&asks, 20, false).ok().flatten()
    }

    fn compute_factor_029(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        cross_sectional_kurtosis(&bids, true, false)
    }

    fn compute_factor_030(depth: &DepthDerived) -> Option<f64> {
        let asks: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        cross_sectional_kurtosis(&asks, true, false)
    }

    fn compute_factor_031(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_rank_last(&series.factor_031_ratio, 500)
            .ok()
            .flatten()
    }

    fn compute_factor_032(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_mean_last(&series.ask_vwap_diff_5_20, 300)
            .ok()
            .flatten()
    }

    fn compute_factor_033(series: &SymbolSeries<'_>) -> Option<f64> {
        let last = *series.top10_bid_volume.last()?;
        let ma = rolling_mean_last(&series.top10_bid_volume, 30)
            .ok()
            .flatten()?;
        if ma.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(last / ma))
    }

    fn compute_factor_035(depth: &DepthDerived) -> Option<f64> {
        let bid = depth_vwap(&depth.bids, 5)?;
        let ask = depth_vwap(&depth.asks, 5)?;
        if ask.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid / ask))
    }

    fn compute_factor_036(series: &SymbolSeries<'_>) -> Option<f64> {
        let last = *series.mean_ask_price15.last()?;
        let mean = rolling_mean_last(&series.mean_ask_price15, 300)
            .ok()
            .flatten()?;
        let std = sample_std_last(&series.mean_ask_price15, 300, 300)?;
        if std.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((last - mean) / std))
    }

    fn compute_factor_037(depth: &DepthDerived) -> Option<f64> {
        let bid5 = depth_vwap(&depth.bids, 5)?;
        let bid15 = depth_vwap(&depth.bids, 15)?;
        finite_opt(Some(bid5 - bid15))
    }

    fn compute_factor_038(depth: &DepthDerived) -> Option<f64> {
        let vwap5 = depth_vwap(&depth.asks, 5)?;
        let vwap15 = depth_vwap(&depth.asks, 15)?;
        let value = vwap5 - vwap15;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_040(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        std_pop(&bids)
    }

    fn compute_factor_041(depth: &DepthDerived) -> Option<f64> {
        let asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        std_pop(&asks)
    }

    fn compute_factor_042(depth: &DepthDerived) -> Option<f64> {
        let bid = depth.sum_bid_amount(5);
        let ask = depth.sum_ask_amount(5);
        if ask.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid / ask))
    }

    fn compute_factor_043(depth: &DepthDerived) -> Option<f64> {
        let (_, bid0v) = depth.best_bid();
        let (_, ask0v) = depth.best_ask();
        let total = depth.sum_bid_amount(20) + depth.sum_ask_amount(20);
        if total.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid0v + ask0v) / total))
    }

    fn compute_factor_045(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_kurt_last(&series.top10_bid_mean, 30, true, false)
            .ok()
            .flatten()
    }

    fn compute_factor_046(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_kurt_last(&series.top10_ask_mean, 30, true, false)
            .ok()
            .flatten()
    }

    fn compute_factor_047(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_skew_last(&series.ask_pv15_mean, 60, false)
            .ok()
            .flatten()
    }

    fn compute_factor_048(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_skew_last(&series.bid_pv15_mean, 60, false)
            .ok()
            .flatten()
    }

    fn compute_factor_049(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_std_last(&series.mean_bid_price5, 30).ok().flatten()
    }

    fn compute_factor_051(series: &SymbolSeries<'_>) -> Option<f64> {
        let last = *series.avg_ask_price5.last()?;
        let ma = rolling_mean_last(&series.avg_ask_price5, 300)
            .ok()
            .flatten()?;
        if ma.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(last / ma))
    }

    fn compute_factor_052(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.avg_ask_price5.len();
        if n < 300 {
            return None;
        }
        let mut neg_diff = Vec::with_capacity(n);
        neg_diff.push(0.0);
        for i in 1..n {
            let d = series.avg_ask_price5[i] - series.avg_ask_price5[i - 1];
            neg_diff.push(if d < 0.0 { d } else { 0.0 });
        }
        let ma30 = rolling_mean_last(&neg_diff, 30).ok().flatten()?;
        let ma300 = rolling_mean_last(&neg_diff, 300).ok().flatten()?;
        if ma300.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(ma30 / ma300))
    }

    fn compute_factor_053(depth: &DepthDerived) -> Option<f64> {
        let (_, bid0v) = depth.best_bid();
        let (_, ask0v) = depth.best_ask();
        let top5 = depth.sum_bid_amount(5) + depth.sum_ask_amount(5);
        if top5.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid0v + ask0v) / top5))
    }

    fn compute_factor_054(depth: &DepthDerived) -> Option<f64> {
        let mut num = 0.0;
        let mut den = 0.0;
        for i in 0..20 {
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
        let (bid0p, bid0v) = depth_best_bid(depth);
        let mid = (bid0p + bid0v) / 2.0;
        finite_opt(Some(mid - vwap))
    }

    fn compute_factor_055(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_sum_last_with_min_periods(&series.bid9v, 3, 3)
            .ok()
            .flatten()
    }

    fn compute_factor_056(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_sum_last_with_min_periods(&series.ask9v, 3, 3)
            .ok()
            .flatten()
    }

    fn compute_factor_057(series: &SymbolSeries<'_>) -> Option<f64> {
        finite_opt(series.total_volume20_sum.last().copied())
    }

    fn compute_factor_058(depth: &DepthDerived) -> Option<f64> {
        finite_opt(Some(depth.sum_bid_amount(5)))
    }

    fn compute_factor_059(depth: &DepthDerived) -> Option<f64> {
        let ask = depth.sum_ask_amount(5);
        let total = ask + depth.sum_bid_amount(5);
        if total.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(ask / total))
    }

    fn compute_factor_060(series: &SymbolSeries<'_>) -> Option<f64> {
        pct_change_last(&series.mean_bid_vol20, 10)
    }

    fn compute_factor_061(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.ask_mean_volume_20.len();
        if n <= 10 {
            return None;
        }
        let curr = series.ask_mean_volume_20[n - 1];
        let prev = series.ask_mean_volume_20[n - 11];
        if prev.abs() <= 1e-12 {
            return Some(0.0);
        }
        let value = (curr - prev) / prev;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_062(depth: &DepthDerived) -> Option<f64> {
        let mut weighted_sum = 0.0;
        let mut weight_sum = 0.0;
        for i in 0..5 {
            let weight = depth_level_amount(&depth.bids, i) + depth_level_amount(&depth.asks, i);
            let spread = depth_level_price(&depth.asks, i) - depth_level_price(&depth.bids, i);
            if weight.is_finite() && spread.is_finite() {
                weighted_sum += spread * weight;
                weight_sum += weight;
            }
        }
        if weight_sum.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(weighted_sum / weight_sum))
    }

    fn compute_factor_063(depth: &DepthDerived) -> Option<f64> {
        let v = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .filter(|x| x.is_finite())
            .fold(f64::NEG_INFINITY, f64::max);
        finite_opt(Some(v))
    }

    fn compute_factor_064(depth: &DepthDerived) -> Option<f64> {
        let v = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .filter(|x| x.is_finite())
            .fold(f64::NEG_INFINITY, f64::max);
        finite_opt(Some(v))
    }

    fn compute_factor_065(depth: &DepthDerived) -> Option<f64> {
        let v = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .filter(|x| x.is_finite())
            .fold(f64::INFINITY, f64::min);
        finite_opt(Some(v))
    }

    fn compute_factor_066(depth: &DepthDerived) -> Option<f64> {
        let v = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .filter(|x| x.is_finite())
            .fold(f64::INFINITY, f64::min);
        finite_opt(Some(v))
    }

    fn compute_factor_067(depth: &DepthDerived) -> Option<f64> {
        finite_opt(Some(depth.sum_bid_amount(10) - depth.sum_ask_amount(10)))
    }

    fn compute_factor_068(depth: &DepthDerived) -> Option<f64> {
        let bid = depth.sum_bid_amount(10);
        let ask = depth.sum_ask_amount(10);
        if ask.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid / ask))
    }

    fn compute_factor_069(depth: &DepthDerived) -> Option<f64> {
        let value = (0..5)
            .map(|i| depth_level_amount(&depth.bids, i).sqrt())
            .filter(|v| v.is_finite())
            .sum::<f64>();
        finite_opt(Some(value))
    }

    fn compute_factor_070(depth: &DepthDerived) -> Option<f64> {
        let value = (0..5)
            .map(|i| depth_level_amount(&depth.asks, i).sqrt())
            .filter(|v| v.is_finite())
            .sum::<f64>();
        finite_opt(Some(value))
    }

    fn compute_factor_073(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..5).map(|i| depth_level_amount(&depth.bids, i)).collect();
        std_pop(&bids)
    }

    fn compute_factor_074(depth: &DepthDerived) -> Option<f64> {
        let asks: Vec<f64> = (0..5).map(|i| depth_level_amount(&depth.asks, i)).collect();
        std_pop(&asks)
    }

    fn compute_factor_075(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..5).map(|i| depth_level_amount(&depth.bids, i)).collect();
        let mean = bids.iter().sum::<f64>() / bids.len() as f64;
        if mean.abs() <= 1e-12 {
            return Some(0.0);
        }
        let std = std_pop(&bids)?;
        finite_opt(Some(std / mean))
    }

    fn compute_factor_076(depth: &DepthDerived) -> Option<f64> {
        let asks: Vec<f64> = (0..5).map(|i| depth_level_amount(&depth.asks, i)).collect();
        let mean = asks.iter().sum::<f64>() / asks.len() as f64;
        if mean.abs() <= 1e-12 {
            return Some(0.0);
        }
        let std = std_pop(&asks)?;
        let value = std / mean;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_077(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        rolling_corr_last(&bids, &asks, 10, 1).ok().flatten()
    }

    fn compute_factor_079(depth: &DepthDerived) -> Option<f64> {
        let ask = depth.sum_ask_amount(20);
        let total = ask + depth.sum_bid_amount(20);
        if total.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(ask / total))
    }

    fn compute_factor_080(series: &SymbolSeries<'_>) -> Option<f64> {
        sample_std_last(&series.combined_level9_volume, 3, 3)
    }

    fn compute_factor_085(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        sample_cov(&bids, &asks)
    }

    fn compute_factor_086(depth: &DepthDerived) -> Option<f64> {
        let bid = depth_sum_amount(&depth.bids, 15);
        let ask = depth_sum_amount(&depth.asks, 15);
        finite_opt(Some(bid - ask))
    }

    fn compute_factor_087(depth: &DepthDerived) -> Option<f64> {
        let bid = depth_sum_amount(&depth.bids, 20);
        let ask = depth_sum_amount(&depth.asks, 20);
        if ask.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid / ask))
    }

    fn compute_factor_088(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let std = std_pop(&bids)?;
        finite_opt(Some(std * std))
    }

    fn compute_factor_089(depth: &DepthDerived) -> Option<f64> {
        let asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        let std = std_pop(&asks)?;
        finite_opt(Some(std * std))
    }

    fn compute_factor_091(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        let bid_std = std_pop(&bids)?;
        let ask_std = std_pop(&asks)?;
        if ask_std.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid_std / ask_std))
    }

    fn compute_factor_093(series: &SymbolSeries<'_>) -> Option<f64> {
        tail_quantile_last(&series.avg_ask_price5, 360, 0.5)
    }

    fn compute_factor_094(series: &SymbolSeries<'_>) -> Option<f64> {
        tail_quantile_last(&series.avg_ask_price5, 100, 0.1)
    }

    fn compute_factor_095(depth: &DepthDerived) -> Option<f64> {
        median_from_iter((0..20).map(|i| depth_level_amount(&depth.bids, i)))
    }

    fn compute_factor_096(depth: &DepthDerived) -> Option<f64> {
        let mut asks: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        asks.sort_by(|a, b| a.total_cmp(b));
        let mid = asks.len() / 2;
        let value = (asks[mid - 1] + asks[mid]) / 2.0;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_097(depth: &DepthDerived) -> Option<f64> {
        let mut vals = Vec::with_capacity(10);
        for i in 0..10 {
            let b = depth_level_amount(&depth.bids, i);
            let a = depth_level_amount(&depth.asks, i);
            if a.abs() > 1e-12 {
                let r = b / a;
                if r.is_finite() {
                    vals.push(r);
                }
            }
        }
        if vals.is_empty() {
            Some(0.0)
        } else {
            Some(vals.iter().sum::<f64>() / vals.len() as f64)
        }
    }

    fn compute_factor_102(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        if bids.iter().any(|v| !v.is_finite() || *v <= 0.0) {
            return Some(0.0);
        }
        let den = bids.iter().map(|v| 1.0 / v).sum::<f64>();
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(10.0 / den))
    }

    fn compute_factor_103(depth: &DepthDerived) -> Option<f64> {
        let asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        harmonic_mean(&asks)
    }

    fn compute_factor_107(depth: &DepthDerived) -> Option<f64> {
        let prices: Vec<f64> = (0..10).map(|i| depth_level_price(&depth.bids, i)).collect();
        let vols: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        Some(depth_corr_or_panic(
            "factor_107",
            "bids",
            depth,
            &prices,
            &vols,
            rolling_corr_last(&prices, &vols, 10, 1),
        ))
    }

    fn compute_factor_108(depth: &DepthDerived) -> Option<f64> {
        let prices: Vec<f64> = (0..10).map(|i| depth_level_price(&depth.asks, i)).collect();
        let vols: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        Some(depth_corr_or_panic(
            "factor_108",
            "asks",
            depth,
            &prices,
            &vols,
            rolling_corr_last(&prices, &vols, 10, 1),
        ))
    }

    fn compute_factor_110(depth: &DepthDerived) -> Option<f64> {
        let (bid0p, _) = depth_best_bid(depth);
        let (ask0p, _) = depth_best_ask(depth);
        let mid = (bid0p + ask0p) / 2.0;
        let mut sum = 0.0;
        let mut cnt = 0usize;
        for i in 0..5 {
            let v = depth_level_price(&depth.asks, i) - mid;
            if v.is_finite() {
                sum += v;
                cnt += 1;
            }
        }
        if cnt == 0 {
            Some(0.0)
        } else {
            finite_opt(Some(sum / cnt as f64))
        }
    }

    fn compute_factor_111(series: &SymbolSeries<'_>) -> Option<f64> {
        let std = sample_std_last(&series.bid9p, 3, 3)?;
        finite_opt(Some(std * std))
    }

    fn compute_factor_113(series: &SymbolSeries<'_>) -> Option<f64> {
        last_opt(&series.factor_113_bid_price_pct_hmean)
    }

    fn compute_factor_114(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_factor_115(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.bid_vwap20.len().min(series.ask_vwap20.len());
        if n < 2 {
            return None;
        }
        let mut pct = Vec::with_capacity(n);
        pct.push(None);
        for i in 1..n {
            let prev = series.bid_vwap20[i - 1] - series.ask_vwap20[i - 1];
            let curr = series.bid_vwap20[i] - series.ask_vwap20[i];
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

    fn compute_factor_116(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.mean_bid_price20.len();
        if n < 50 {
            return None;
        }
        let mut diff3 = Vec::with_capacity(n);
        for i in 0..n {
            if i >= 3 {
                diff3.push(series.mean_bid_price20[i] - series.mean_bid_price20[i - 3]);
            } else {
                diff3.push(f64::NAN);
            }
        }
        let tail_len = n.min(300);
        rank_last_average(&diff3[n - tail_len..], 50)
    }

    fn compute_factor_119(series: &SymbolSeries<'_>) -> Option<f64> {
        let last = *series.factor_119_mid_minus_ask_vwap5.last()?;
        let ma = rolling_mean_last(&series.factor_119_mid_minus_ask_vwap5, 120)
            .ok()
            .flatten()?;
        if ma.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(last / ma))
    }

    fn compute_factor_120(depth: &DepthDerived) -> Option<f64> {
        median_from_iter((0..10).map(|i| depth_level_amount(&depth.bids, i)))
    }

    fn compute_factor_121(depth: &DepthDerived) -> Option<f64> {
        let mut asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        asks.sort_by(|a, b| a.total_cmp(b));
        let mid = asks.len() / 2;
        finite_opt(Some((asks[mid - 1] + asks[mid]) / 2.0))
    }

    fn compute_factor_122(depth: &DepthDerived) -> Option<f64> {
        let top3 = depth.sum_ask_amount(3);
        let total15 = depth.sum_ask_amount(15);
        if total15.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(top3 / total15))
    }

    fn compute_factor_123(depth: &DepthDerived) -> Option<f64> {
        let top3 = depth_sum_amount(&depth.bids, 3);
        let total15 = depth_sum_amount(&depth.bids, 15);
        if total15.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(top3 / total15))
    }

    fn compute_factor_124(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        let bid_var = std_pop(&bids)?;
        let ask_var = std_pop(&asks)?;
        let bid_var = bid_var * bid_var;
        let ask_var = ask_var * ask_var;
        if ask_var.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid_var / ask_var))
    }

    fn compute_factor_125(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.median_all_price40.len();
        if n < 21 {
            return None;
        }
        let last = series.median_all_price40[n - 1];
        let prev15 = series.median_all_price40[n - 16];
        if !last.is_finite() || !prev15.is_finite() || prev15.abs() <= 1e-12 {
            return Some(0.0);
        }
        let pct15 = (last - prev15) / prev15;
        let period = if pct15 < 0.0 { 5 } else { 20 };
        let prev = series.median_all_price40[n - 1 - period];
        if !prev.is_finite() || prev.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((last - prev) / prev))
    }

    fn compute_factor_126(depth: &DepthDerived) -> Option<f64> {
        let mut vals = Vec::with_capacity(10);
        for i in 0..10 {
            vals.push(depth_level_amount(&depth.bids, i) + depth_level_amount(&depth.asks, i));
        }
        let mean = vals.iter().sum::<f64>() / vals.len() as f64;
        if mean.abs() <= 1e-12 {
            return Some(0.0);
        }
        let std = std_pop(&vals)?;
        finite_opt(Some(std / mean))
    }

    fn compute_factor_128(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_mean_last_opt_from_series(&series.factor_128_skew, 300, 50)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_factor_129(depth: &DepthDerived) -> Option<f64> {
        let bids: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        weighted_harmonic_with_index_weights(&bids)
    }

    fn compute_factor_130(depth: &DepthDerived) -> Option<f64> {
        let asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        weighted_harmonic_with_index_weights(&asks)
    }

    fn compute_factor_133(series: &SymbolSeries<'_>) -> Option<f64> {
        let bid = *series.total_bid20.last()?;
        let ask = *series.total_ask20.last()?;
        let den = bid + ask;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid - ask) / den))
    }

    fn compute_factor_134(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series
            .mid_price
            .len()
            .min(series.bid0v.len())
            .min(series.ask0v.len());
        if n < 300 {
            return None;
        }
        if let Some(cached) = finite_opt(series.corr_mid_midvol_300_last) {
            return Some(cached);
        }
        let mid_vol: Vec<f64> = (0..n)
            .map(|i| (series.bid0v[i] + series.ask0v[i]) / 2.0)
            .collect();
        rolling_corr_last(&series.mid_price, &mid_vol, 300, 1)
            .ok()
            .flatten()
    }

    fn compute_factor_139(depth: &DepthDerived) -> Option<f64> {
        let mut sum = 0.0;
        let mut cnt = 0usize;
        for i in 0..5 {
            let v = depth_level_price(&depth.bids, i) - depth_level_price(&depth.asks, i);
            if v.is_finite() {
                sum += v;
                cnt += 1;
            }
        }
        if cnt == 0 {
            Some(0.0)
        } else {
            finite_opt(Some(sum / cnt as f64))
        }
    }

    fn compute_factor_144(depth: &DepthDerived) -> Option<f64> {
        let diffs: Vec<f64> = (0..15)
            .map(|i| depth_level_price(&depth.bids, i) - depth_level_price(&depth.asks, i))
            .collect();
        let mut sum = 0.0;
        let mut cnt = 0usize;
        for i in 1..diffs.len() {
            let v = diffs[i] - diffs[i - 1];
            if v.is_finite() {
                sum += v;
                cnt += 1;
            }
        }
        if cnt == 0 {
            Some(0.0)
        } else {
            finite_opt(Some(sum / cnt as f64))
        }
    }

    fn compute_factor_151(series: &SymbolSeries<'_>) -> Option<f64> {
        rolling_skew_last(&series.total_volume20_sum, 45, false)
            .ok()
            .flatten()
    }

    fn compute_factor_152(series: &SymbolSeries<'_>) -> Option<f64> {
        last_opt(&series.factor_152_pct_mean)
    }

    fn compute_factor_156(depth: &DepthDerived) -> Option<f64> {
        let bid_prices: Vec<f64> = (0..10).map(|i| depth_level_price(&depth.bids, i)).collect();
        let ask_prices: Vec<f64> = (0..10).map(|i| depth_level_price(&depth.asks, i)).collect();
        let bid_std = std_pop(&bid_prices)?;
        let ask_std = std_pop(&ask_prices)?;
        let value = bid_std - ask_std;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_159(depth: &DepthDerived) -> Option<f64> {
        let bid = depth.mean_bid_price(10);
        let ask = depth.mean_ask_price(10);
        finite_opt(Some(bid - ask))
    }

    fn compute_factor_160(series: &SymbolSeries<'_>) -> Option<f64> {
        last_opt(&series.factor_160_pct_change_mean)
    }

    fn compute_factor_164(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.bid0v.len();
        if n < 2 {
            return None;
        }
        finite_opt(Some(series.bid0v[n - 1] - series.bid0v[n - 2]))
    }

    fn compute_factor_165(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.ask0v.len();
        if n < 2 {
            return None;
        }
        let value = series.ask0v[n - 1] - series.ask0v[n - 2];
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_166(depth: &DepthDerived) -> Option<f64> {
        let (_, bid0v) = depth_best_bid(depth);
        let (ask0p, ask0v) = depth_best_ask(depth);
        let depth_mean = (bid0v + ask0v) / 2.0;
        if depth_mean.abs() <= 1e-12 {
            return Some(0.0);
        }
        let value = (ask0p - ask0v) / depth_mean;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_168(series: &SymbolSeries<'_>) -> Option<f64> {
        let spread = *series.spread.last()?;
        let mid = *series.mid_price.last()?;
        if mid.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(spread / mid))
    }

    fn compute_factor_170(series: &SymbolSeries<'_>) -> Option<f64> {
        pct_change_last(&series.mid_price, 120)
    }

    fn compute_factor_175(series: &SymbolSeries<'_>) -> Option<f64> {
        sample_std_last(&series.spread, 5, 1)
    }

    fn compute_factor_176(depth: &DepthDerived) -> Option<f64> {
        let (_, bid0v) = depth_best_bid(depth);
        let (_, ask0v) = depth_best_ask(depth);
        if ask0v.abs() <= 1e-12 {
            return Some(0.0);
        }
        let value = bid0v / ask0v;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_td_ti_010(series: &SymbolSeries<'_>) -> Option<f64> {
        let ema1 = rolling_mean_series(&series.close, 30, 30).ok()?;
        let ema2 = rolling_mean_series_opt(&ema1, 30, 30).ok()?;
        rolling_mean_last_opt_from_series(&ema2, 30, 30)
            .ok()
            .flatten()
            .and_then(|v| finite_opt(Some(v)))
    }

    fn compute_td_ti_015(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_ti_031(series: &SymbolSeries<'_>) -> Option<f64> {
        corr_last_with_min_periods(&series.large_order, &series.small_order, 60, 10)
    }

    fn compute_td_ti_036(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_ti_037(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_mt_003(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_mt_008(series: &SymbolSeries<'_>) -> Option<f64> {
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
            let hd = series.high[i] - series.high[i - 1];
            let ld = series.low[i] - series.low[i - 1];
            plus_dm[i] = if hd > ld { hd } else { 0.0 };
            minus_dm[i] = if ld > hd { ld } else { 0.0 };
            tr[i] = (series.high[i] - series.low[i])
                .max((series.high[i] - series.close[i - 1]).abs())
                .max((series.low[i] - series.close[i - 1]).abs());
        }
        let plus_ma = rolling_mean_last(&plus_dm, 14).ok().flatten()?;
        let minus_ma = rolling_mean_last(&minus_dm, 14).ok().flatten()?;
        let tr_ma = rolling_mean_last_with_min_periods(
            &tr.iter()
                .map(|v| if v.is_finite() { *v } else { f64::NAN })
                .collect::<Vec<_>>(),
            14,
            14,
        )
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

    fn compute_td_mt_014(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_mt_015(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len());
        if n < 14 {
            return None;
        }
        let mut minus_dm = vec![0.0; n];
        let mut tr = vec![f64::NAN; n];
        for i in 1..n {
            let hd = series.high[i] - series.high[i - 1];
            let ld = series.low[i] - series.low[i - 1];
            minus_dm[i] = if ld > hd { ld } else { 0.0 };
            tr[i] = (series.high[i] - series.low[i])
                .max((series.high[i] - series.close[i - 1]).abs())
                .max((series.low[i] - series.close[i - 1]).abs());
        }
        let m = rolling_mean_last(&minus_dm, 14).ok().flatten()?;
        let t = rolling_mean_last_with_min_periods(
            &tr.iter()
                .map(|v| if v.is_finite() { *v } else { f64::NAN })
                .collect::<Vec<_>>(),
            14,
            14,
        )
        .ok()
        .flatten()?;
        if t.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(100.0 * m / t))
    }

    fn compute_td_mt_039(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_vi_010(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.high.len().min(series.low.len());
        if n < 2 {
            return None;
        }
        let hd = series.high[n - 1] - series.high[n - 2];
        let ld = series.low[n - 1] - series.low[n - 2];
        finite_opt(Some(if hd > ld { hd } else { 0.0 }))
    }

    fn compute_td_vi_011(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.high.len().min(series.low.len());
        if n < 2 {
            return None;
        }
        let mut plus_dm = vec![0.0; n];
        for (i, v) in plus_dm.iter_mut().enumerate().take(n).skip(1) {
            let hd = series.high[i] - series.high[i - 1];
            let ld = series.low[i] - series.low[i - 1];
            *v = if hd > ld { hd } else { 0.0 };
        }
        rolling_sum_last_with_min_periods(&plus_dm, 14, 1)
            .ok()
            .flatten()
    }

    fn compute_td_vi_025(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_vi_026(series: &SymbolSeries<'_>) -> Option<f64> {
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
            let upper = series.high[i] - series.close[i].max(series.open[i]);
            let lower = series.close[i].min(series.open[i]) - series.low[i];
            let body = (series.close[i] - series.open[i]).abs();
            x.push(upper - lower - body);
        }
        rolling_sum_last_with_min_periods(&x, 14, 1).ok().flatten()
    }

    fn compute_td_vi_028(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.close.len())
            .min(series.open.len());
        if n == 0 {
            return None;
        }
        let x: Vec<f64> = (0..n)
            .map(|i| series.high[i] - series.close[i].max(series.open[i]))
            .collect();
        rolling_sum_last_with_min_periods(&x, 14, 1).ok().flatten()
    }

    fn compute_td_pt_001(series: &SymbolSeries<'_>) -> Option<f64> {
        finite_opt(series.close.last().copied().map(f64::sin))
    }

    fn compute_td_pt_002(series: &SymbolSeries<'_>) -> Option<f64> {
        finite_opt(series.close.last().copied().map(f64::cos))
    }

    fn compute_td_pt_010(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_pt_027(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_ci_007(series: &SymbolSeries<'_>) -> Option<f64> {
        let ma = rolling_mean_last_with_min_periods(&series.close, 30, 1)
            .ok()
            .flatten()?;
        finite_opt(Some(ma.sin()))
    }

    fn compute_td_ci_008(series: &SymbolSeries<'_>) -> Option<f64> {
        let ma = rolling_mean_last_with_min_periods(&series.close, 30, 1)
            .ok()
            .flatten()?;
        finite_opt(Some(ma.cos()))
    }

    fn compute_td_pr_001(series: &SymbolSeries<'_>) -> Option<f64> {
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
        let body = (series.close[i] - series.open[i]).abs();
        let upper = series.high[i] - series.close[i].max(series.open[i]);
        let lower = series.close[i].min(series.open[i]) - series.low[i];
        let inv_hammer = (upper > 2.0 * body) && (lower < 0.1 * body);
        Some(if inv_hammer { 1.0 } else { 0.0 })
    }

    fn compute_td_pr_002(series: &SymbolSeries<'_>) -> Option<f64> {
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
        let body = (series.close[i] - series.open[i]).abs();
        let upper = series.high[i] - series.close[i].max(series.open[i]);
        let lower = series.close[i].min(series.open[i]) - series.low[i];
        let hammer = (lower > 2.0 * body) && (upper < 0.1 * body);
        Some(if hammer { 1.0 } else { 0.0 })
    }

    fn compute_td_pr_007(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.close.len().min(series.open.len());
        if n < 2 {
            return None;
        }
        let i = n - 1;
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

    fn compute_td_pr_015(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series
            .open
            .len()
            .min(series.low.len())
            .min(series.high.len());
        if n == 0 {
            return None;
        }
        let i = n - 1;
        Some(
            if series.open[i] == series.low[i] || series.open[i] == series.high[i] {
                100.0
            } else {
                0.0
            },
        )
    }

    fn compute_td_pr_016(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_pr_017(series: &SymbolSeries<'_>) -> Option<f64> {
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
        let range = series.high[i] - series.low[i];
        let takuri = (series.open[i] == series.close[i])
            && (series.low[i] < series.open[i])
            && (series.close[i] - series.low[i] > 0.5 * range);
        Some(if takuri { 100.0 } else { 0.0 })
    }

    fn compute_tp_vpi_006(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.close.len().min(series.volume.len());
        if n < 373 {
            return None;
        }
        let mut obv_delta = Vec::with_capacity(n);
        obv_delta.push(0.0);
        for i in 1..n {
            let d = series.close[i] - series.close[i - 1];
            let v = if d > 0.0 {
                series.volume[i]
            } else if d < 0.0 {
                -series.volume[i]
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

    fn compute_tp_vpi_014(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_tp_vpi_017(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_ti_026(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_ti_033(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_ti_034(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_tp_vpi_004(series: &SymbolSeries<'_>) -> Option<f64> {
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
            let raw =
                ((series.close[i] - series.low[i]) - (series.high[i] - series.close[i])) / den;
            let clv = if raw.is_finite() { raw } else { 0.0 };
            clv_x.push(clv * series.volume[i]);
        }
        let clv_x_opt: Vec<Option<f64>> = clv_x
            .into_iter()
            .map(|v| if v.is_finite() { Some(v) } else { None })
            .collect();
        let ad = rolling_sum_series_opt(&clv_x_opt, 360, 360).ok()?;
        let fast = rolling_mean_series_opt(&ad, 12, 1).ok()?;
        let slow = rolling_mean_series_opt(&ad, 26, 1).ok()?;
        let value = last_opt(&fast)? - last_opt(&slow)?;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_tp_vpi_001(series: &SymbolSeries<'_>) -> Option<f64> {
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
            let raw =
                ((series.close[i] - series.low[i]) - (series.high[i] - series.close[i])) / den;
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

    fn compute_tp_vpi_002(series: &SymbolSeries<'_>) -> Option<f64> {
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
            let raw =
                ((series.close[i] - series.low[i]) - (series.high[i] - series.close[i])) / den;
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

    fn compute_tp_vpi_005(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_tp_vpi_015(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_pt_003(series: &SymbolSeries<'_>) -> Option<f64> {
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
            .or(Some(0.0))
    }

    fn compute_td_pt_004(series: &SymbolSeries<'_>) -> Option<f64> {
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
                let hl = series.high[i] - series.low[i];
                let vol_ma_i = finite_opt(vol_ma[i])?;
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
        let raw = match (last_opt(&sma_fast), last_opt(&sma_slow)) {
            (Some(f), Some(s)) => f - s,
            _ => 0.0,
        };
        if raw.is_finite() {
            Some(raw)
        } else if raw.is_sign_positive() {
            Some(f64::MAX)
        } else if raw.is_sign_negative() {
            Some(f64::MIN)
        } else {
            Some(0.0)
        }
    }

    fn compute_td_ci_003(series: &SymbolSeries<'_>) -> Option<f64> {
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

    fn compute_td_pr_005(series: &SymbolSeries<'_>) -> Option<f64> {
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
        let small_body = if i >= 1 {
            (series.close[i - 1] - series.open[i - 1]).abs()
                < (series.high[i - 1] - series.low[i - 1]) * 0.1
        } else {
            false
        };
        let first_down = if i >= 2 {
            series.close[i - 2] < series.open[i - 2]
        } else {
            false
        };
        let third_up = series.close[i] > series.open[i];
        let morning_star = first_down && small_body && third_up;
        Some(if morning_star { 1.0 } else { 0.0 })
    }

    fn compute_td_pr_006(series: &SymbolSeries<'_>) -> Option<f64> {
        let n = series.close.len().min(series.open.len());
        if n < 3 {
            return None;
        }
        let i = n - 1;
        let three_black_crows = (series.close[i - 2] < series.open[i - 2])
            && (series.close[i - 1] < series.open[i - 1])
            && (series.close[i] < series.open[i]);
        Some(if three_black_crows { 1.0 } else { 0.0 })
    }

    fn compute_factor_118(
        &mut self,
        symbol: &str,
        depth: &DepthDerived,
    ) -> Option<(f64, bool, usize)> {
        let state = self.symbol_states.entry(symbol.to_string()).or_default();
        Self::compute_factor_118_with_state(state, depth)
    }

    pub(crate) fn compute_factor_118_with_state(
        state: &mut SymbolCalcState,
        depth: &DepthDerived,
    ) -> Option<(f64, bool, usize)> {
        let mid_price_diff = compute_mid_price_minus_bid_vwap(depth)?;
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
) -> Result<HashSet<String>> {
    let base_url = tlen.base_url.trim_end_matches('/');
    let client = Client::builder()
        .timeout(Duration::from_millis(tlen.request_timeout_ms))
        .build()
        .context("build reqwest client for tlen_server failed")?;

    let url = format!("{}/api/thresholds", base_url);
    let resp = client
        .get(&url)
        .query(&[("venue", venue_slug), ("config_type", "amount_thresholds")])
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

fn compute_mid_price_minus_bid_vwap(depth: &DepthDerived) -> Option<f64> {
    let bid1 = depth.bids.first()?;
    let ask1 = depth.asks.first()?;
    if bid1.price <= 0.0 || ask1.price <= 0.0 {
        return Some(0.0);
    }

    let mid_price = (bid1.price + ask1.price) / 2.0;
    if !mid_price.is_finite() || mid_price <= 0.0 {
        return Some(0.0);
    }

    let mut sum_pxv = 0.0;
    let mut sum_v = 0.0;
    for level in depth.bids.iter().take(FACTOR_118_VWAP_LEVELS) {
        if level.price > 0.0 && level.amount > 0.0 {
            sum_pxv += level.price * level.amount;
            sum_v += level.amount;
        }
    }
    if sum_v <= 0.0 {
        return Some(0.0);
    }

    let bid_vwap = sum_pxv / sum_v;
    let diff = mid_price - bid_vwap;
    if diff.is_finite() {
        Some(diff)
    } else {
        Some(0.0)
    }
}

pub fn depth_best_bid(depth: &DepthDerived) -> (f64, f64) {
    match depth.bids.first() {
        Some(level) => (level.price, level.amount),
        None => (f64::NAN, f64::NAN),
    }
}

pub fn depth_best_ask(depth: &DepthDerived) -> (f64, f64) {
    match depth.asks.first() {
        Some(level) => (level.price, level.amount),
        None => (f64::NAN, f64::NAN),
    }
}

pub fn depth_level_amount(levels: &[DepthLevel], idx: usize) -> f64 {
    levels
        .get(idx)
        .map(|level| level.amount)
        .unwrap_or(f64::NAN)
}

pub fn depth_level_price(levels: &[DepthLevel], idx: usize) -> f64 {
    levels.get(idx).map(|level| level.price).unwrap_or(f64::NAN)
}

pub fn depth_sum_amount(levels: &[DepthLevel], limit: usize) -> f64 {
    levels
        .iter()
        .take(limit)
        .map(|level| level.amount)
        .filter(|v| v.is_finite())
        .sum()
}

pub fn depth_mean_amount(levels: &[DepthLevel], limit: usize) -> f64 {
    let mut sum = 0.0;
    let mut cnt = 0usize;
    for level in levels.iter().take(limit) {
        if level.amount.is_finite() {
            sum += level.amount;
            cnt += 1;
        }
    }
    if cnt == 0 {
        f64::NAN
    } else {
        sum / cnt as f64
    }
}

pub fn depth_mean_price(levels: &[DepthLevel], limit: usize) -> f64 {
    let mut sum = 0.0;
    let mut cnt = 0usize;
    for level in levels.iter().take(limit) {
        if level.price.is_finite() {
            sum += level.price;
            cnt += 1;
        }
    }
    if cnt == 0 {
        f64::NAN
    } else {
        sum / cnt as f64
    }
}

pub fn depth_mean_pxv(levels: &[DepthLevel], limit: usize) -> f64 {
    let mut sum = 0.0;
    let mut cnt = 0usize;
    for level in levels.iter().take(limit) {
        let v = level.price * level.amount;
        if v.is_finite() {
            sum += v;
            cnt += 1;
        }
    }
    if cnt == 0 {
        f64::NAN
    } else {
        sum / cnt as f64
    }
}

pub fn depth_sum_price(levels: &[DepthLevel], limit: usize) -> f64 {
    levels
        .iter()
        .take(limit)
        .map(|level| level.price)
        .filter(|v| v.is_finite())
        .sum()
}

pub fn depth_vwap(levels: &[DepthLevel], limit: usize) -> Option<f64> {
    let mut sum_pxv = 0.0;
    let mut sum_v = 0.0;
    for level in levels.iter().take(limit) {
        if !level.price.is_finite() || !level.amount.is_finite() {
            continue;
        }
        sum_pxv += level.price * level.amount;
        sum_v += level.amount;
    }
    if sum_v.abs() <= 1e-12 {
        return Some(0.0);
    }
    let value = sum_pxv / sum_v;
    if value.is_finite() {
        Some(value)
    } else {
        Some(0.0)
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
        if v.is_finite() {
            tail.push(v);
        }
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
        if v.is_finite() {
            tail.push(v);
        }
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
        return Some(0.0);
    }
    let mut lt = 0usize;
    let mut eq = 0usize;
    let mut valid = 0usize;
    for v in values {
        if !v.is_finite() {
            continue;
        }
        valid += 1;
        if *v < last {
            lt += 1;
        } else if (*v - last).abs() <= 1e-12 {
            eq += 1;
        }
    }
    if valid < min_periods || eq == 0 {
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
        if xv.is_finite() && yv.is_finite() {
            x.push(xv);
            y.push(yv);
        }
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

fn depth_corr_or_panic(
    factor_name: &str,
    side: &str,
    depth: &DepthDerived,
    prices: &[f64],
    vols: &[f64],
    corr_result: Result<Option<f64>>,
) -> f64 {
    match corr_result {
        Ok(Some(v)) if v.is_finite() => v,
        Ok(Some(v)) => {
            if corr_inputs_degenerate(prices, vols) {
                return 0.0;
            }
            panic!(
                "{} correlation non-finite: side={} corr={} prices={:?} vols={:?} depth={:?}",
                factor_name, side, v, prices, vols, depth
            );
        }
        Ok(None) => {
            if corr_inputs_degenerate(prices, vols) {
                return 0.0;
            }
            panic!(
                "{} correlation unavailable: side={} reason=none prices={:?} vols={:?} depth={:?}",
                factor_name, side, prices, vols, depth
            );
        }
        Err(err) => {
            panic!(
                "{} correlation failed: side={} err={} prices={:?} vols={:?} depth={:?}",
                factor_name, side, err, prices, vols, depth
            );
        }
    }
}

fn corr_inputs_degenerate(xs: &[f64], ys: &[f64]) -> bool {
    let n = xs.len().min(ys.len());
    if n < 2 {
        return false;
    }

    let mut x = Vec::with_capacity(n);
    let mut y = Vec::with_capacity(n);
    for i in 0..n {
        let xv = xs[i];
        let yv = ys[i];
        if xv.is_finite() && yv.is_finite() {
            x.push(xv);
            y.push(yv);
        }
    }
    if x.len() < 2 {
        return false;
    }

    let mean_x = x.iter().sum::<f64>() / x.len() as f64;
    let mean_y = y.iter().sum::<f64>() / y.len() as f64;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for i in 0..x.len() {
        let dx = x[i] - mean_x;
        let dy = y[i] - mean_y;
        var_x += dx * dx;
        var_y += dy * dy;
    }
    var_x.abs() <= 1e-12 || var_y.abs() <= 1e-12
}

fn std_pop(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sum = 0.0;
    let mut cnt = 0usize;
    for v in values {
        if v.is_finite() {
            sum += *v;
            cnt += 1;
        }
    }
    if cnt == 0 {
        return Some(0.0);
    }
    let mean = sum / cnt as f64;
    let mut var_sum = 0.0;
    for v in values {
        if v.is_finite() {
            let d = *v - mean;
            var_sum += d * d;
        }
    }
    let value = (var_sum / cnt as f64).sqrt();
    if value.is_finite() {
        Some(value)
    } else {
        Some(0.0)
    }
}

fn sample_cov(xs: &[f64], ys: &[f64]) -> Option<f64> {
    let n = xs.len().min(ys.len());
    if n < 2 {
        return None;
    }
    let mut pairs = Vec::with_capacity(n);
    for i in 0..n {
        let x = xs[i];
        let y = ys[i];
        if x.is_finite() && y.is_finite() {
            pairs.push((x, y));
        }
    }
    if pairs.len() < 2 {
        return Some(0.0);
    }
    let mean_x = pairs.iter().map(|(x, _)| *x).sum::<f64>() / pairs.len() as f64;
    let mean_y = pairs.iter().map(|(_, y)| *y).sum::<f64>() / pairs.len() as f64;
    let cov_sum = pairs
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>();
    finite_opt(Some(cov_sum / (pairs.len() - 1) as f64)).or(Some(0.0))
}

fn harmonic_mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut denom = 0.0;
    for value in values {
        if !value.is_finite() || *value <= 0.0 {
            return Some(0.0);
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
        if !value.is_finite() || value.abs() <= 1e-12 {
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
            return Some(0.0);
        }
        denom += (idx + 1) as f64 / *value;
    }
    if denom.abs() <= 1e-12 {
        return Some(0.0);
    }
    finite_opt(Some(values.len() as f64 / denom))
}

fn cross_sectional_kurtosis(values: &[f64], fisher: bool, bias: bool) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.iter().any(|v| !v.is_finite()) {
        return Some(0.0);
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
    finite_opt(Some(out)).or(Some(0.0))
}

fn median_from_iter<I>(iter: I) -> Option<f64>
where
    I: IntoIterator<Item = f64>,
{
    let mut values: Vec<f64> = iter.into_iter().filter(|v| v.is_finite()).collect();
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        finite_opt(Some((values[mid - 1] + values[mid]) / 2.0))
    } else {
        finite_opt(Some(values[mid]))
    }
}

pub fn push_with_limit(buf: &mut VecDeque<f64>, value: f64) {
    buf.push_back(value);
    if buf.len() > MAX_SYMBOL_HISTORY {
        buf.pop_front();
    }
}

pub fn push_opt_with_limit(buf: &mut VecDeque<Option<f64>>, value: Option<f64>) {
    buf.push_back(finite_opt(value));
    if buf.len() > MAX_SYMBOL_HISTORY {
        buf.pop_front();
    }
}

pub fn finite_opt(value: Option<f64>) -> Option<f64> {
    match value {
        Some(v) if v.is_finite() => Some(v),
        Some(_) => Some(0.0),
        None => None,
    }
}

fn last_opt(values: &(impl OptF64SeriesView + ?Sized)) -> Option<f64> {
    if values.len() == 0 {
        return Some(0.0);
    }
    finite_opt(values.value_at(values.len() - 1)).or(Some(0.0))
}

fn trade_flow_feature_cf_name(venue_slug: &str, symbol: &str) -> String {
    format!(
        "{}:{}:{}",
        venue_slug,
        symbol.to_uppercase(),
        TRADE_FLOW_FEATURE_CF_SUFFIX
    )
}

fn resolve_trade_flow_feature_rocksdb_path(venue_slug: &str) -> Result<String> {
    let home = std::env::var("HOME")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            let user = std::env::var("USER")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())?;
            Some(format!("/home/{}", user))
        })
        .context("resolve rocksdb path failed: both HOME and USER are empty")?;

    let mut path = PathBuf::from(home);
    path.push("trade_flow_feature");
    path.push(venue_slug);
    path.push("data");
    path.push("trade_flow_feature_pub_rocksdb");
    Ok(path.to_string_lossy().to_string())
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

fn parse_embedded_depth(msg: &TradeFlowFeatureMsg) -> Option<DepthSnapshot> {
    if msg.values.len() < TRADE_FLOW_FEATURE_DIM + APPENDED_DEPTH_VALUES {
        return None;
    }

    let mut bids = [DepthLevel {
        price: 0.0,
        amount: 0.0,
    }; MAX_DEPTH_LEVELS_CACHE];
    let mut asks = [DepthLevel {
        price: 0.0,
        amount: 0.0,
    }; MAX_DEPTH_LEVELS_CACHE];
    let mut best_bid_ok = false;
    let mut best_ask_ok = false;

    let mut offset = TRADE_FLOW_FEATURE_DIM;
    for idx in 0..MAX_DEPTH_LEVELS_CACHE {
        let price_raw = msg.values[offset];
        let amount_raw = msg.values[offset + 1];
        offset += 2;

        let price = if price_raw.is_finite() {
            price_raw
        } else {
            0.0
        };
        let amount = if amount_raw.is_finite() {
            amount_raw
        } else {
            0.0
        };
        if idx == 0 && price > 0.0 {
            best_bid_ok = true;
        }
        bids[idx] = DepthLevel { price, amount };
    }

    for idx in 0..MAX_DEPTH_LEVELS_CACHE {
        let price_raw = msg.values[offset];
        let amount_raw = msg.values[offset + 1];
        offset += 2;

        let price = if price_raw.is_finite() {
            price_raw
        } else {
            0.0
        };
        let amount = if amount_raw.is_finite() {
            amount_raw
        } else {
            0.0
        };
        if idx == 0 && price > 0.0 {
            best_ask_ok = true;
        }
        asks[idx] = DepthLevel { price, amount };
    }

    if !best_bid_ok || !best_ask_ok {
        return None;
    }

    Some(DepthSnapshot { bids, asks })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_depth() -> DepthDerived {
        let bids = std::array::from_fn(|i| DepthLevel {
            price: 100.0 - i as f64 * 0.1,
            amount: 10.0 + i as f64,
        });
        let asks = std::array::from_fn(|i| DepthLevel {
            price: 100.1 + i as f64 * 0.1,
            amount: 11.0 + i as f64,
        });
        DepthDerived::from_snapshot(&DepthSnapshot { bids, asks })
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
        assert_eq!(state.sample_count, 2);
        assert_eq!(state.welford_vec.len(), 1);
    }

    #[test]
    fn newly_added_reference_factors_are_wired() {
        let expected = [
            FusionFactorId::Factor002,
            FusionFactorId::Factor008,
            FusionFactorId::Factor012,
            FusionFactorId::Factor020,
            FusionFactorId::Factor026,
            FusionFactorId::Factor028,
            FusionFactorId::Factor036,
            FusionFactorId::Factor040,
            FusionFactorId::Factor042,
            FusionFactorId::Factor043,
            FusionFactorId::Factor049,
            FusionFactorId::Factor053,
            FusionFactorId::Factor058,
            FusionFactorId::Factor059,
            FusionFactorId::Factor062,
            FusionFactorId::Factor064,
            FusionFactorId::Factor067,
            FusionFactorId::Factor068,
            FusionFactorId::Factor069,
            FusionFactorId::Factor070,
            FusionFactorId::Factor073,
            FusionFactorId::Factor079,
            FusionFactorId::Factor080,
            FusionFactorId::Factor085,
            FusionFactorId::Factor091,
            FusionFactorId::Factor093,
            FusionFactorId::Factor095,
            FusionFactorId::Factor103,
            FusionFactorId::Factor111,
            FusionFactorId::Factor113,
            FusionFactorId::Factor114,
            FusionFactorId::Factor115,
            FusionFactorId::Factor120,
            FusionFactorId::Factor122,
            FusionFactorId::Factor125,
            FusionFactorId::Factor129,
            FusionFactorId::Factor130,
            FusionFactorId::Factor159,
        ];

        let mut state = SymbolCalcState::default();
        let series = FusionFactorPubApp::build_symbol_series_from_state(&mut state);
        let depth = dummy_depth();

        for factor_id in expected {
            let binding = FactorBinding {
                name: factor_id.as_name().to_string(),
                factor_id: Some(factor_id),
                extra_factor_id: None,
            };
            assert!(
                FusionFactorPubApp::compute_supported_factor(
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
    fn requested_baselines_are_wired_except_blocked_inputs() {
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
        const BLOCKED: &[&str] = &[
            "baseline_048",
            "baseline_075",
            "baseline_078",
            "baseline_094",
            "baseline_095",
            "baseline_102",
            "baseline_155",
        ];

        let mut state = SymbolCalcState::default();
        let series = FusionFactorPubApp::build_symbol_series_from_state(&mut state);

        for name in REQUESTED
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            let binding = FactorBinding {
                name: name.to_string(),
                factor_id: FusionFactorId::from_name(name),
                extra_factor_id: None,
            };
            let supported =
                FusionFactorPubApp::compute_supported_factor(&binding, None, None, Some(&series))
                    .is_some();
            if BLOCKED.contains(&name) {
                assert!(!supported, "blocked baseline unexpectedly wired: {name}");
            } else {
                assert!(supported, "missing baseline wiring: {name}");
            }
        }
    }
}
