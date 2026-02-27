//! Fusion Factor 数据通路应用
//!
//! 订阅 trade_flow_feature（包含 trade + depth）并触发融合因子计算。
//! 当前先落地 factor_118，后续在同一框架上扩展全部因子。

use anyhow::{bail, Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use reqwest::Client;
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use super::cfg::{FusionFactorPubConfig, ModelManagerConfig};
use super::factor_enum::FusionFactorId;
use super::publisher::FusionFactorPublisher;
use super::window_primitives::{
    rolling_corr_last, rolling_kurt_last, rolling_mean_last, rolling_mean_last_with_min_periods,
    rolling_mean_series, rolling_mean_series_opt, rolling_rank_last, rolling_skew_last,
    rolling_sum_last_with_min_periods, rolling_sum_series, rolling_sum_series_opt,
    tail_skew_last_opt,
};
use crate::common::mkt_msg::{FeatureMsg, FeatureStatus};
use crate::common::msg_parser::parse_trade_flow_feature;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::trade_flow_feature_msg::{
    TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM, TRADE_FLOW_FEATURE_MSG_TYPE,
};
use crate::signal::common::TradingVenue;

const TRADE_FLOW_MAX_BYTES: usize = 1024;
const IDLE_SLEEP_MICROS: u64 = 200;
const STATS_LOG_INTERVAL_SECS: u64 = 60;
const TRADE_FLOW_SUBSCRIBER_BUFFER_SIZE: usize = 8192;
const TRADE_FLOW_FEATURE_CF_SUFFIX: &str = "trade_flow:feature";
const ROCKSDB_BOOTSTRAP_LOG_EVERY: usize = 12 * 60 * 4;
const MAX_SYMBOL_HISTORY: usize = 4096;
const MAX_DEPTH_LEVELS_CACHE: usize = 20;
const APPENDED_DEPTH_VALUES: usize = MAX_DEPTH_LEVELS_CACHE * 4;
const FACTOR_118_WINDOW: usize = 120;
const FACTOR_118_VWAP_LEVELS: usize = 5;
const FIELD_OPEN: usize = 0;
const FIELD_HIGH: usize = 1;
const FIELD_LOW: usize = 2;
const FIELD_CLOSE: usize = 3;
const FIELD_VOLUME: usize = 4;
const FIELD_AMOUNT: usize = 5;
const FIELD_BUY_COUNT: usize = 8;
const FIELD_SELL_COUNT: usize = 9;
const FIELD_BUY_AMOUNT: usize = 10;
const FIELD_SELL_AMOUNT: usize = 11;
const FIELD_BUY_VOLUME: usize = 12;
const FIELD_SELL_VOLUME: usize = 13;
const FIELD_LARGE_ORDER: usize = 14;
const FIELD_SMALL_ORDER: usize = 16;
const FIELD_LARGE_BUY: usize = 17;
const FIELD_LARGE_SELL: usize = 18;
const FIELD_SMALL_BUY: usize = 21;
const FIELD_SMALL_SELL: usize = 22;
const FIELD_VWAP: usize = 23;
const FIELD_BUY_VWAP: usize = 24;
const FIELD_SELL_VWAP: usize = 25;
const FIELD_NET_BUY_AMOUNT: usize = 26;
const FIELD_NET_BUY_LARGE: usize = 29;
const FIELD_NET_BUY_SMALL: usize = 31;
const FACTOR_160_RANDOM_LEVELS: [usize; 10] = [18, 1, 19, 8, 10, 17, 6, 13, 4, 2];

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

#[derive(Default)]
pub struct SymbolCalcState {
    pub factor_118_mid_price_diffs: VecDeque<f64>,
    pub open: VecDeque<f64>,
    pub high: VecDeque<f64>,
    pub low: VecDeque<f64>,
    pub close: VecDeque<f64>,
    pub volume: VecDeque<f64>,
    pub amount: VecDeque<f64>,
    pub buy_count: VecDeque<f64>,
    pub sell_count: VecDeque<f64>,
    pub buy_amount: VecDeque<f64>,
    pub sell_amount: VecDeque<f64>,
    pub buy_volume: VecDeque<f64>,
    pub sell_volume: VecDeque<f64>,
    pub large_order: VecDeque<f64>,
    pub large_buy: VecDeque<f64>,
    pub large_sell: VecDeque<f64>,
    pub small_order: VecDeque<f64>,
    pub small_buy: VecDeque<f64>,
    pub small_sell: VecDeque<f64>,
    pub vwap: VecDeque<f64>,
    pub buy_vwap: VecDeque<f64>,
    pub sell_vwap: VecDeque<f64>,
    pub net_buy_large: VecDeque<f64>,
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
    pub mean_bid_vol20: VecDeque<f64>,
    pub mean_bid_price20: VecDeque<f64>,
    pub avg_ask_price5: VecDeque<f64>,
    pub ask_pv15_mean: VecDeque<f64>,
    pub bid_pv15_mean: VecDeque<f64>,
    pub factor_031_ratio: VecDeque<f64>,
    pub factor_119_mid_minus_ask_vwap5: VecDeque<f64>,
    pub total_volume20_sum: VecDeque<f64>,
    pub factor_152_prev_price_diff10: Option<[f64; 10]>,
    pub factor_152_pct_mean: VecDeque<Option<f64>>,
    pub ask_vwap20: VecDeque<f64>,
    pub ask_vwap_diff_5_20: VecDeque<f64>,
    pub ask_mean_volume_20: VecDeque<f64>,
    pub ask0v: VecDeque<f64>,
    pub factor_128_ask_price_sum: VecDeque<f64>,
    pub factor_128_diff_sum: VecDeque<Option<f64>>,
    pub factor_128_skew: VecDeque<Option<f64>>,
    pub factor_160_prev_ratios: Option<[f64; 10]>,
    pub factor_160_pct_change_mean: VecDeque<Option<f64>>,
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
        push_with_limit(&mut self.buy_count, msg.values[FIELD_BUY_COUNT]);
        push_with_limit(&mut self.sell_count, msg.values[FIELD_SELL_COUNT]);
        push_with_limit(&mut self.buy_amount, msg.values[FIELD_BUY_AMOUNT]);
        push_with_limit(&mut self.sell_amount, msg.values[FIELD_SELL_AMOUNT]);
        push_with_limit(&mut self.buy_volume, msg.values[FIELD_BUY_VOLUME]);
        push_with_limit(&mut self.sell_volume, msg.values[FIELD_SELL_VOLUME]);
        push_with_limit(&mut self.large_order, msg.values[FIELD_LARGE_ORDER]);
        push_with_limit(&mut self.large_buy, msg.values[FIELD_LARGE_BUY]);
        push_with_limit(&mut self.large_sell, msg.values[FIELD_LARGE_SELL]);
        push_with_limit(&mut self.small_order, msg.values[FIELD_SMALL_ORDER]);
        push_with_limit(&mut self.small_buy, msg.values[FIELD_SMALL_BUY]);
        push_with_limit(&mut self.small_sell, msg.values[FIELD_SMALL_SELL]);
        push_with_limit(&mut self.vwap, msg.values[FIELD_VWAP]);
        push_with_limit(&mut self.buy_vwap, msg.values[FIELD_BUY_VWAP]);
        push_with_limit(&mut self.sell_vwap, msg.values[FIELD_SELL_VWAP]);
        push_with_limit(&mut self.net_buy_large, msg.values[FIELD_NET_BUY_LARGE]);
        push_with_limit(&mut self.net_buy_small, msg.values[FIELD_NET_BUY_SMALL]);
        push_with_limit(&mut self.net_buy_amount, msg.values[FIELD_NET_BUY_AMOUNT]);
    }

    pub fn push_depth_metrics(&mut self, depth: &DepthSnapshot) {
        let (bid0p, bid0v) = depth_best_bid(depth);
        let (ask0p, ask0v) = depth_best_ask(depth);

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

        let top10_bid = depth_sum_amount(&depth.bids, 10);
        let top10_ask = depth_sum_amount(&depth.asks, 10);
        let total_bid20 = depth_sum_amount(&depth.bids, 20);
        let total_ask20 = depth_sum_amount(&depth.asks, 20);
        push_with_limit(&mut self.total_bid20, total_bid20);
        push_with_limit(&mut self.total_ask20, total_ask20);
        push_with_limit(&mut self.total_volume20_sum, total_bid20 + total_ask20);
        push_with_limit(&mut self.top10_bid_volume, top10_bid);
        push_with_limit(&mut self.top10_ask_volume, top10_ask);
        push_with_limit(&mut self.top10_bid_mean, top10_bid / 10.0);
        push_with_limit(&mut self.top10_ask_mean, top10_ask / 10.0);
        push_with_limit(&mut self.bid9v, depth_level_amount(&depth.bids, 9));
        push_with_limit(&mut self.ask9v, depth_level_amount(&depth.asks, 9));
        push_with_limit(&mut self.mean_bid_vol20, depth_mean_amount(&depth.bids, 20));
        push_with_limit(
            &mut self.mean_bid_price20,
            depth_mean_price(&depth.bids, 20),
        );
        push_with_limit(&mut self.avg_ask_price5, depth_mean_price(&depth.asks, 5));
        push_with_limit(&mut self.ask_pv15_mean, depth_mean_pxv(&depth.asks, 15));
        push_with_limit(&mut self.bid_pv15_mean, depth_mean_pxv(&depth.bids, 15));
        let ratio_031 = {
            let num = depth_mean_price(&depth.bids, 15);
            if mid.abs() > 1e-12 {
                num / mid
            } else {
                f64::NAN
            }
        };
        push_with_limit(&mut self.factor_031_ratio, ratio_031);

        let ask_vwap5 = depth_vwap(&depth.asks, 5);
        let ask_vwap20 = depth_vwap(&depth.asks, 20);
        let bid_vwap20 = depth_vwap(&depth.bids, 20);
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

        let ask_mean20 = depth_mean_amount(&depth.asks, 20);
        push_with_limit(&mut self.ask_mean_volume_20, ask_mean20);

        push_with_limit(&mut self.ask0v, ask0v);

        let ask_price_sum20 = depth_sum_price(&depth.asks, 20);
        let prev_sum = self.factor_128_ask_price_sum.back().copied();
        push_with_limit(&mut self.factor_128_ask_price_sum, ask_price_sum20);
        let curr_diff = prev_sum.map(|prev| ask_price_sum20 - prev).or(Some(0.0));
        push_opt_with_limit(&mut self.factor_128_diff_sum, curr_diff);

        let diff_vec: Vec<Option<f64>> = self.factor_128_diff_sum.iter().copied().collect();
        let skew = tail_skew_last_opt(&diff_vec, 90, 30, false).ok().flatten();
        push_opt_with_limit(&mut self.factor_128_skew, skew);

        let mut curr_ratios = [f64::NAN; 10];
        for (k, level_idx) in FACTOR_160_RANDOM_LEVELS.iter().enumerate() {
            let bid = depth_level_amount(&depth.bids, *level_idx);
            let ask = depth_level_amount(&depth.asks, *level_idx);
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
            let bidp = depth_level_price(&depth.bids, i);
            let askp = depth_level_price(&depth.asks, i);
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

#[derive(Debug, Clone)]
struct SymbolFactorPlan {
    ordered_factors: Vec<FactorBinding>,
}

#[derive(Debug, Clone)]
pub struct FactorBinding {
    pub name: String,
    pub factor_id: Option<FusionFactorId>,
    pub extra_factor_id: Option<ExtraFactorId>,
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

pub struct SymbolSeries {
    pub open: Vec<f64>,
    pub high: Vec<f64>,
    pub low: Vec<f64>,
    pub close: Vec<f64>,
    pub volume: Vec<f64>,
    pub amount: Vec<f64>,
    pub buy_count: Vec<f64>,
    pub sell_count: Vec<f64>,
    pub buy_amount: Vec<f64>,
    pub sell_amount: Vec<f64>,
    pub buy_volume: Vec<f64>,
    pub sell_volume: Vec<f64>,
    pub large_order: Vec<f64>,
    pub large_buy: Vec<f64>,
    pub large_sell: Vec<f64>,
    pub small_order: Vec<f64>,
    pub small_buy: Vec<f64>,
    pub small_sell: Vec<f64>,
    pub vwap: Vec<f64>,
    pub buy_vwap: Vec<f64>,
    pub sell_vwap: Vec<f64>,
    pub net_buy_large: Vec<f64>,
    pub net_buy_small: Vec<f64>,
    pub net_buy_amount: Vec<f64>,
    pub bid0v: Vec<f64>,
    pub mid_price: Vec<f64>,
    pub spread: Vec<f64>,
    pub relative_spread: Vec<f64>,
    pub bid_vwap20: Vec<f64>,
    pub total_bid20: Vec<f64>,
    pub total_ask20: Vec<f64>,
    pub top10_bid_volume: Vec<f64>,
    pub top10_ask_volume: Vec<f64>,
    pub top10_bid_mean: Vec<f64>,
    pub top10_ask_mean: Vec<f64>,
    pub bid9v: Vec<f64>,
    pub ask9v: Vec<f64>,
    pub mean_bid_vol20: Vec<f64>,
    pub mean_bid_price20: Vec<f64>,
    pub avg_ask_price5: Vec<f64>,
    pub ask_pv15_mean: Vec<f64>,
    pub bid_pv15_mean: Vec<f64>,
    pub factor_031_ratio: Vec<f64>,
    pub factor_119_mid_minus_ask_vwap5: Vec<f64>,
    pub total_volume20_sum: Vec<f64>,
    pub factor_152_pct_mean: Vec<Option<f64>>,
    pub ask_vwap_diff_5_20: Vec<f64>,
    pub ask_mean_volume_20: Vec<f64>,
    pub ask0v: Vec<f64>,
    pub ask_vwap20: Vec<f64>,
    pub factor_128_skew: Vec<Option<f64>>,
    pub factor_160_pct_change_mean: Vec<Option<f64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtraFactorId {
    AvgPrice,
    BuyAvgPrice,
    SellAvgPrice,
    SmallBuy,
    SmallSell,
    NetBuyLarge,
}

impl ExtraFactorId {
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "avg_price" => Some(Self::AvgPrice),
            "buy_avg_price" => Some(Self::BuyAvgPrice),
            "sell_avg_price" => Some(Self::SellAvgPrice),
            "small_buy" => Some(Self::SmallBuy),
            "small_sell" => Some(Self::SmallSell),
            "net_buy_large" => Some(Self::NetBuyLarge),
            _ => None,
        }
    }

    pub const fn as_index(self) -> u16 {
        const EXTRA_FACTOR_BASE: u16 = 1000;
        match self {
            Self::AvgPrice => EXTRA_FACTOR_BASE,
            Self::BuyAvgPrice => EXTRA_FACTOR_BASE + 1,
            Self::SellAvgPrice => EXTRA_FACTOR_BASE + 2,
            Self::SmallBuy => EXTRA_FACTOR_BASE + 3,
            Self::SmallSell => EXTRA_FACTOR_BASE + 4,
            Self::NetBuyLarge => EXTRA_FACTOR_BASE + 5,
        }
    }

    pub fn from_index(index: u16) -> Option<Self> {
        const EXTRA_FACTOR_BASE: u16 = 1000;
        match index {
            x if x == EXTRA_FACTOR_BASE => Some(Self::AvgPrice),
            x if x == EXTRA_FACTOR_BASE + 1 => Some(Self::BuyAvgPrice),
            x if x == EXTRA_FACTOR_BASE + 2 => Some(Self::SellAvgPrice),
            x if x == EXTRA_FACTOR_BASE + 3 => Some(Self::SmallBuy),
            x if x == EXTRA_FACTOR_BASE + 4 => Some(Self::SmallSell),
            x if x == EXTRA_FACTOR_BASE + 5 => Some(Self::NetBuyLarge),
            _ => None,
        }
    }

    pub fn index_to_name(index: u16) -> Option<&'static str> {
        Self::from_index(index).map(|id| match id {
            Self::AvgPrice => "avg_price",
            Self::BuyAvgPrice => "buy_avg_price",
            Self::SellAvgPrice => "sell_avg_price",
            Self::SmallBuy => "small_buy",
            Self::SmallSell => "small_sell",
            Self::NetBuyLarge => "net_buy_large",
        })
    }
}

pub struct FusionFactorPubApp {
    venue_slug: String,
    venue: TradingVenue,
    trade_flow_feature_rocksdb_path: String,
    trade_flow_subscriber: Subscriber<ipc::Service, [u8; TRADE_FLOW_MAX_BYTES], ()>,
    publisher: FusionFactorPublisher,
    allowed_symbols: HashSet<String>,
    symbol_all_ready_seen: HashSet<String>,
    symbol_factor_plans: HashMap<String, SymbolFactorPlan>,
    symbol_states: HashMap<String, SymbolCalcState>,
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
    last_stats_log: Instant,
}

impl FusionFactorPubApp {
    pub async fn new(config_path: &str, venue: TradingVenue) -> Result<Self> {
        let cfg = FusionFactorPubConfig::load(config_path)?;
        let venue_slug = venue.data_pub_slug().to_string();
        let trade_flow_feature_rocksdb_path = resolve_trade_flow_feature_rocksdb_path(&venue_slug)?;

        let allowed_symbols: Vec<String> = cfg
            .symbols
            .iter()
            .map(|s| normalize_symbol_for_venue(s, venue))
            .filter(|s| !s.is_empty())
            .collect();
        if allowed_symbols.is_empty() {
            anyhow::bail!(
                "no valid symbols after normalization for venue={}",
                venue_slug
            );
        }

        let symbol_factor_plans = load_symbol_factor_plans(&cfg.model_manager, &allowed_symbols)
            .await
            .with_context(|| {
                format!(
                    "load symbol factor plans failed: model={} symbols={}",
                    cfg.model_manager.model_name,
                    allowed_symbols.len()
                )
            })?;
        let trade_flow_subscriber =
            Self::create_trade_flow_subscriber(&venue_slug, cfg.trade_flow_channel())?;

        let output_service_path = cfg.output_service_path();
        let publisher_node_name = format!("fusion_pub_{}", venue_slug.replace('-', "_"));
        let publisher = FusionFactorPublisher::new(&publisher_node_name, &output_service_path)
            .with_context(|| {
                format!(
                    "create fusion factor publisher failed: service_path={}",
                    output_service_path
                )
            })?;

        info!(
            "FusionFactorPubApp created: venue={} symbols={} trade_flow_channel=factor_pub/{}/{} rocksdb_path={} output_service={}",
            venue_slug,
            allowed_symbols.len(),
            venue_slug,
            cfg.trade_flow_channel(),
            trade_flow_feature_rocksdb_path,
            output_service_path,
        );

        Ok(Self {
            venue_slug,
            venue,
            trade_flow_feature_rocksdb_path,
            trade_flow_subscriber,
            publisher,
            allowed_symbols: allowed_symbols.into_iter().collect(),
            symbol_all_ready_seen: HashSet::new(),
            symbol_factor_plans,
            symbol_states: HashMap::new(),
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
            .open()?;
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

    pub fn run(&mut self) -> Result<()> {
        self.bootstrap_from_rocksdb()?;

        // Drain stale IPC messages buffered before this process started.
        let mut drained = 0u64;
        while self.trade_flow_subscriber.receive()?.is_some() {
            drained += 1;
        }
        info!(
            "FusionFactorPubApp[{}] started: symbols={} trade_flow=single_stream drained_stale={}",
            self.venue_slug,
            self.allowed_symbols.len(),
            drained,
        );

        loop {
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
                info!(
                    "FusionFactorPubApp[{}] stats: raw_msgs={} trade_flow_msgs={} triggers={} decode_errors={} depth_attached={} missing_depth={} factor118_ready={} trade_drop_symbols={} drop_symbol_samples={:?} calc_symbols={} factor_plan={} factor_eval={} factor_ready={} factor_warming_up={} factor_invalid={} factor_missing_depth={} published={} publish_failed={} pub_total={} pub_dropped={} last_decode_error={}",
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
                    self.publisher.published_count(),
                    self.publisher.dropped_count(),
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
                panic!(
                    "fusion bootstrap missing rocksdb history: venue={} symbol={} cf={} path={}",
                    self.venue_slug, symbol, cf_name, self.trade_flow_feature_rocksdb_path
                );
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
                panic!(
                    "fusion bootstrap missing rocksdb history rows: venue={} symbol={} cf={} path={}",
                    self.venue_slug, symbol, cf_name, self.trade_flow_feature_rocksdb_path
                );
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
        let symbol_factor_plans = self.symbol_factor_plans.clone();

        let mut total_loaded = 0usize;
        let mut total_calculated = 0u64;
        let mut total_reload = 0u64;
        let mut total_bootstrap_published = 0u64;

        let mut symbol_records_vec: Vec<_> = symbol_records;
        symbol_records_vec.sort_unstable_by(|a, b| a.symbol.cmp(&b.symbol));

        for sr in symbol_records_vec {
            let symbol_result = Self::replay_symbol_records(
                &venue_slug,
                &symbol_factor_plans,
                sr,
                &mut self.publisher,
            );
            total_loaded = total_loaded.saturating_add(symbol_result.loaded);
            total_calculated = total_calculated.saturating_add(symbol_result.calculated);
            total_reload = total_reload.saturating_add(symbol_result.reload);
            if symbol_result.all_ready_seen {
                self.symbol_all_ready_seen
                    .insert(symbol_result.symbol.clone());
            }
            total_bootstrap_published =
                total_bootstrap_published.saturating_add(symbol_result.bootstrap_published);
            self.symbol_states
                .insert(symbol_result.symbol.clone(), symbol_result.state);
            info!(
                "FusionFactorPubApp[{}] rocksdb bootstrap symbol done: symbol={} loaded={} calculated={} reload={} published={} total_loaded={}",
                self.venue_slug,
                symbol_result.symbol,
                symbol_result.loaded,
                symbol_result.calculated,
                symbol_result.reload,
                symbol_result.bootstrap_published,
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
        Ok(())
    }

    fn replay_symbol_records(
        venue_slug: &str,
        symbol_factor_plans: &HashMap<String, SymbolFactorPlan>,
        symbol_records: BootstrapSymbolRecords,
        publisher: &mut FusionFactorPublisher,
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
        let plan = symbol_factor_plans.get(&symbol);
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
            let depth_snapshot = parse_embedded_depth(msg);
            let depth_opt = depth_snapshot.as_ref();
            state.push_trade_flow(msg);
            if let Some(depth) = depth_opt {
                state.push_depth_metrics(depth);
            }
            let latest_eval = match plan {
                Some(plan) => {
                    let factor_118_result = if needs_factor_118 {
                        depth_opt.and_then(|depth| {
                            Self::compute_factor_118_with_state(&mut state, depth)
                        })
                    } else {
                        None
                    };
                    let series = Self::build_symbol_series_from_state(&state);
                    let eval_result = Self::evaluate_ordered_factors_with_plan(
                        plan,
                        factor_118_result,
                        depth_opt,
                        Some(&series),
                    );
                    if eval_result.status == 0 {
                        all_ready_seen = true;
                        let ts_ms = msg.ts / 1000;
                        let feature_msg = FeatureMsg::create(
                            symbol.clone(),
                            ts_ms,
                            FeatureStatus::Reload as u8,
                            eval_result.factor_values.clone(),
                        );
                        if let Ok(bytes) = feature_msg.to_bytes() {
                            publisher.publish(&bytes);
                            bootstrap_published = bootstrap_published.saturating_add(1);
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

        let depth_snapshot = parse_embedded_depth(&msg);
        let depth_opt = depth_snapshot.as_ref();
        {
            let state = self.symbol_states.entry(symbol.clone()).or_default();
            state.push_trade_flow(&msg);
            if let Some(depth) = depth_opt {
                state.push_depth_metrics(depth);
            }
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

        // Publish FeatureMsg via iceoryx2
        let ts_ms = msg.ts / 1000; // ts is in microseconds, convert to ms
        let feature_msg =
            FeatureMsg::create(symbol, ts_ms, eval_result.status, eval_result.factor_values);
        match feature_msg.to_bytes() {
            Ok(bytes) => {
                if self.publisher.publish(&bytes) {
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
        depth: Option<&DepthSnapshot>,
    ) -> Option<OrderedEvalResult> {
        let needs_factor_118 = self
            .symbol_factor_plans
            .get(symbol)?
            .ordered_factors
            .iter()
            .any(|factor| factor.factor_id == Some(FusionFactorId::Factor118));
        let factor_118_result = if needs_factor_118 {
            depth.and_then(|d| self.compute_factor_118(symbol, d))
        } else {
            None
        };
        let series = self.build_symbol_series(symbol);
        let plan = self.symbol_factor_plans.get(symbol)?;
        Some(Self::evaluate_ordered_factors_with_plan(
            plan,
            factor_118_result,
            depth,
            series.as_ref(),
        ))
    }

    fn evaluate_ordered_factors_with_plan(
        plan: &SymbolFactorPlan,
        factor_118_result: Option<(f64, bool, usize)>,
        depth: Option<&DepthSnapshot>,
        series: Option<&SymbolSeries>,
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

    fn build_symbol_series(&self, symbol: &str) -> Option<SymbolSeries> {
        let state = self.symbol_states.get(symbol)?;
        Some(Self::build_symbol_series_from_state(state))
    }

    pub fn build_symbol_series_from_state(state: &SymbolCalcState) -> SymbolSeries {
        SymbolSeries {
            open: state.open.iter().copied().collect(),
            high: state.high.iter().copied().collect(),
            low: state.low.iter().copied().collect(),
            close: state.close.iter().copied().collect(),
            volume: state.volume.iter().copied().collect(),
            amount: state.amount.iter().copied().collect(),
            buy_count: state.buy_count.iter().copied().collect(),
            sell_count: state.sell_count.iter().copied().collect(),
            buy_amount: state.buy_amount.iter().copied().collect(),
            sell_amount: state.sell_amount.iter().copied().collect(),
            buy_volume: state.buy_volume.iter().copied().collect(),
            sell_volume: state.sell_volume.iter().copied().collect(),
            large_order: state.large_order.iter().copied().collect(),
            large_buy: state.large_buy.iter().copied().collect(),
            large_sell: state.large_sell.iter().copied().collect(),
            small_order: state.small_order.iter().copied().collect(),
            small_buy: state.small_buy.iter().copied().collect(),
            small_sell: state.small_sell.iter().copied().collect(),
            vwap: state.vwap.iter().copied().collect(),
            buy_vwap: state.buy_vwap.iter().copied().collect(),
            sell_vwap: state.sell_vwap.iter().copied().collect(),
            net_buy_large: state.net_buy_large.iter().copied().collect(),
            net_buy_small: state.net_buy_small.iter().copied().collect(),
            net_buy_amount: state.net_buy_amount.iter().copied().collect(),
            bid0v: state.bid0v.iter().copied().collect(),
            mid_price: state.mid_price.iter().copied().collect(),
            spread: state.spread.iter().copied().collect(),
            relative_spread: state.relative_spread.iter().copied().collect(),
            bid_vwap20: state.bid_vwap20.iter().copied().collect(),
            total_bid20: state.total_bid20.iter().copied().collect(),
            total_ask20: state.total_ask20.iter().copied().collect(),
            top10_bid_volume: state.top10_bid_volume.iter().copied().collect(),
            top10_ask_volume: state.top10_ask_volume.iter().copied().collect(),
            top10_bid_mean: state.top10_bid_mean.iter().copied().collect(),
            top10_ask_mean: state.top10_ask_mean.iter().copied().collect(),
            bid9v: state.bid9v.iter().copied().collect(),
            ask9v: state.ask9v.iter().copied().collect(),
            mean_bid_vol20: state.mean_bid_vol20.iter().copied().collect(),
            mean_bid_price20: state.mean_bid_price20.iter().copied().collect(),
            avg_ask_price5: state.avg_ask_price5.iter().copied().collect(),
            ask_pv15_mean: state.ask_pv15_mean.iter().copied().collect(),
            bid_pv15_mean: state.bid_pv15_mean.iter().copied().collect(),
            factor_031_ratio: state.factor_031_ratio.iter().copied().collect(),
            factor_119_mid_minus_ask_vwap5: state
                .factor_119_mid_minus_ask_vwap5
                .iter()
                .copied()
                .collect(),
            total_volume20_sum: state.total_volume20_sum.iter().copied().collect(),
            factor_152_pct_mean: state.factor_152_pct_mean.iter().copied().collect(),
            ask_vwap_diff_5_20: state.ask_vwap_diff_5_20.iter().copied().collect(),
            ask_mean_volume_20: state.ask_mean_volume_20.iter().copied().collect(),
            ask0v: state.ask0v.iter().copied().collect(),
            ask_vwap20: state.ask_vwap20.iter().copied().collect(),
            factor_128_skew: state.factor_128_skew.iter().copied().collect(),
            factor_160_pct_change_mean: state.factor_160_pct_change_mean.iter().copied().collect(),
        }
    }

    pub fn compute_supported_factor(
        binding: &FactorBinding,
        factor_118_result: Option<(f64, bool, usize)>,
        depth: Option<&DepthSnapshot>,
        series: Option<&SymbolSeries>,
    ) -> Option<(f64, bool, &'static str)> {
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
            Some(FusionFactorId::FactorTrades015) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_015),
                ));
            }
            Some(FusionFactorId::FactorTrades014) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_014),
                ));
            }
            Some(FusionFactorId::FactorTrades018) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_018),
                ));
            }
            Some(FusionFactorId::FactorTrades025) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_025),
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
            Some(FusionFactorId::FactorTrades048) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_trades_048),
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
            Some(FusionFactorId::Factor027) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_027),
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
            Some(FusionFactorId::Factor041) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_041),
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
            Some(FusionFactorId::Factor063) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_063),
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
            Some(FusionFactorId::Factor094) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_factor_094),
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
            Some(FusionFactorId::Factor121) => {
                return Some(Self::wrap_factor_value(
                    depth.and_then(Self::compute_factor_121),
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
            Some(FusionFactorId::Baseline018) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_018),
                ));
            }
            Some(FusionFactorId::Baseline004) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_004),
                ));
            }
            Some(FusionFactorId::Baseline007) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_007),
                ));
            }
            Some(FusionFactorId::Baseline008) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_008),
                ));
            }
            Some(FusionFactorId::Baseline016) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_016),
                ));
            }
            Some(FusionFactorId::Baseline028) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_028),
                ));
            }
            Some(FusionFactorId::Baseline034) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_034),
                ));
            }
            Some(FusionFactorId::Baseline040) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_040),
                ));
            }
            Some(FusionFactorId::Baseline045) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_045),
                ));
            }
            Some(FusionFactorId::Baseline046) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_046),
                ));
            }
            Some(FusionFactorId::Baseline047) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_047),
                ));
            }
            Some(FusionFactorId::Baseline073) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_073),
                ));
            }
            Some(FusionFactorId::Baseline076) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_076),
                ));
            }
            Some(FusionFactorId::Baseline079) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_079),
                ));
            }
            Some(FusionFactorId::Baseline080) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_080),
                ));
            }
            Some(FusionFactorId::Baseline082) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_082),
                ));
            }
            Some(FusionFactorId::Baseline135) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_135),
                ));
            }
            Some(FusionFactorId::Baseline100) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_100),
                ));
            }
            Some(FusionFactorId::Baseline113) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_113),
                ));
            }
            Some(FusionFactorId::Baseline116) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_116),
                ));
            }
            Some(FusionFactorId::Baseline117) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_117),
                ));
            }
            Some(FusionFactorId::Baseline120) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_120),
                ));
            }
            Some(FusionFactorId::Baseline134) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_134),
                ));
            }
            Some(FusionFactorId::Baseline137) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_137),
                ));
            }
            Some(FusionFactorId::Baseline140) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_140),
                ));
            }
            Some(FusionFactorId::Baseline141) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_141),
                ));
            }
            Some(FusionFactorId::Baseline163) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_163),
                ));
            }
            Some(FusionFactorId::Baseline164) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_164),
                ));
            }
            Some(FusionFactorId::Baseline172) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_172),
                ));
            }
            Some(FusionFactorId::Baseline177) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_177),
                ));
            }
            Some(FusionFactorId::Baseline178) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_178),
                ));
            }
            Some(FusionFactorId::Baseline189) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_189),
                ));
            }
            Some(FusionFactorId::Baseline191) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_191),
                ));
            }
            Some(FusionFactorId::Baseline193) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_193),
                ));
            }
            Some(FusionFactorId::Baseline196) => {
                return Some(Self::wrap_factor_value(
                    series.and_then(Self::compute_baseline_196),
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
        series: Option<&SymbolSeries>,
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

    fn compute_factor_trades_015(series: &SymbolSeries) -> Option<f64> {
        let n = series.sell_amount.len().min(series.sell_count.len());
        if n == 0 {
            return None;
        }
        let sell_count = series.sell_count[n - 1];
        if sell_count.abs() <= 1e-12 {
            return Some(0.0);
        }
        let value = series.sell_amount[n - 1] / sell_count;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_trades_014(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_trades_018(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_trades_025(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_trades_028(series: &SymbolSeries) -> Option<f64> {
        let n = series.buy_amount.len().min(series.buy_count.len());
        if n == 0 {
            return None;
        }
        finite_opt(Some(series.buy_amount[n - 1] * series.buy_count[n - 1]))
    }

    fn compute_factor_trades_029(series: &SymbolSeries) -> Option<f64> {
        let n = series.sell_amount.len().min(series.sell_count.len());
        if n == 0 {
            return None;
        }
        let value = series.sell_amount[n - 1] * series.sell_count[n - 1];
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_trades_034(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_trades_035(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_trades_048(series: &SymbolSeries) -> Option<f64> {
        let n = series.close.len();
        if n < 10 {
            return None;
        }
        let mut pct = Vec::with_capacity(n);
        pct.push(f64::NAN);
        for i in 1..n {
            let prev = series.close[i - 1];
            let curr = series.close[i];
            if prev.abs() <= 1e-12 {
                pct.push(f64::NAN);
            } else {
                pct.push((curr - prev) / prev);
            }
        }
        let tail = &pct[n - 10..n];
        let valid: Vec<Option<f64>> = tail
            .iter()
            .map(|v| if v.is_finite() { Some(*v) } else { None })
            .collect();
        tail_skew_last_opt(&valid, 10, 3, false)
            .ok()
            .flatten()
            .or(Some(0.0))
    }

    fn compute_factor_001(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_003(depth: &DepthSnapshot) -> Option<f64> {
        let bid = depth_sum_price(&depth.bids, 15);
        let ask = depth_sum_price(&depth.asks, 15);
        let den = bid + ask;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid - ask) / den))
    }

    fn compute_factor_006(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_010(series: &SymbolSeries) -> Option<f64> {
        let n = series.bid_vwap20.len();
        if n < 2 {
            return None;
        }
        finite_opt(Some(series.bid_vwap20[n - 1] - series.bid_vwap20[n - 2]))
    }

    fn compute_factor_011(series: &SymbolSeries) -> Option<f64> {
        let n = series.ask_vwap20.len();
        if n < 2 {
            return None;
        }
        finite_opt(Some(series.ask_vwap20[n - 1] - series.ask_vwap20[n - 2]))
    }

    fn compute_factor_014(depth: &DepthSnapshot) -> Option<f64> {
        let top5 = depth_sum_amount(&depth.asks, 5);
        let total = depth_sum_amount(&depth.asks, 20);
        if total.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(top5 / total))
    }

    fn compute_factor_016(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_017(depth: &DepthSnapshot) -> Option<f64> {
        let bids: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let mean = bids.iter().filter(|v| v.is_finite()).sum::<f64>() / bids.len() as f64;
        let den = mean + 1e-6;
        let norm: Vec<f64> = bids.into_iter().map(|v| v / den).collect();
        std_pop(&norm)
    }

    fn compute_factor_019(series: &SymbolSeries) -> Option<f64> {
        let n = series.bid_vwap20.len();
        if n < 2 {
            return None;
        }
        let ma = rolling_mean_series(&series.bid_vwap20, 5, 1).ok()?;
        let curr = finite_opt(ma[n - 1])?;
        let prev = finite_opt(ma[n - 2])?;
        finite_opt(Some(curr - prev))
    }

    fn compute_factor_021(series: &SymbolSeries) -> Option<f64> {
        let bid_std = sample_std_last(&series.total_bid20, 10, 1)?;
        let ask_std = sample_std_last(&series.total_ask20, 10, 1)?;
        if ask_std.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid_std / ask_std))
    }

    fn compute_factor_025(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_027(depth: &DepthSnapshot) -> Option<f64> {
        let bids: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        rolling_skew_last(&bids, 20, false).ok().flatten()
    }

    fn compute_factor_030(depth: &DepthSnapshot) -> Option<f64> {
        let asks: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        rolling_kurt_last(&asks, 20, true, false).ok().flatten()
    }

    fn compute_factor_031(series: &SymbolSeries) -> Option<f64> {
        rolling_rank_last(&series.factor_031_ratio, 500)
            .ok()
            .flatten()
    }

    fn compute_factor_033(series: &SymbolSeries) -> Option<f64> {
        let last = *series.top10_bid_volume.last()?;
        let ma = rolling_mean_last(&series.top10_bid_volume, 30)
            .ok()
            .flatten()?;
        if ma.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(last / ma))
    }

    fn compute_factor_035(depth: &DepthSnapshot) -> Option<f64> {
        let bid = depth_vwap(&depth.bids, 5)?;
        let ask = depth_vwap(&depth.asks, 5)?;
        if ask.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid / ask))
    }

    fn compute_factor_037(depth: &DepthSnapshot) -> Option<f64> {
        let bid5 = depth_vwap(&depth.bids, 5)?;
        let bid15 = depth_vwap(&depth.bids, 15)?;
        finite_opt(Some(bid5 - bid15))
    }

    fn compute_factor_041(depth: &DepthSnapshot) -> Option<f64> {
        let asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        std_pop(&asks)
    }

    fn compute_factor_045(series: &SymbolSeries) -> Option<f64> {
        rolling_kurt_last(&series.top10_bid_mean, 30, true, false)
            .ok()
            .flatten()
    }

    fn compute_factor_046(series: &SymbolSeries) -> Option<f64> {
        rolling_kurt_last(&series.top10_ask_mean, 30, true, false)
            .ok()
            .flatten()
    }

    fn compute_factor_047(series: &SymbolSeries) -> Option<f64> {
        rolling_skew_last(&series.ask_pv15_mean, 60, false)
            .ok()
            .flatten()
    }

    fn compute_factor_048(series: &SymbolSeries) -> Option<f64> {
        rolling_skew_last(&series.bid_pv15_mean, 60, false)
            .ok()
            .flatten()
    }

    fn compute_factor_051(series: &SymbolSeries) -> Option<f64> {
        let last = *series.avg_ask_price5.last()?;
        let ma = rolling_mean_last(&series.avg_ask_price5, 300)
            .ok()
            .flatten()?;
        if ma.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(last / ma))
    }

    fn compute_factor_052(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_054(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_055(series: &SymbolSeries) -> Option<f64> {
        rolling_sum_last_with_min_periods(&series.bid9v, 3, 3)
            .ok()
            .flatten()
    }

    fn compute_factor_056(series: &SymbolSeries) -> Option<f64> {
        rolling_sum_last_with_min_periods(&series.ask9v, 3, 3)
            .ok()
            .flatten()
    }

    fn compute_factor_057(series: &SymbolSeries) -> Option<f64> {
        finite_opt(series.total_volume20_sum.last().copied())
    }

    fn compute_factor_060(series: &SymbolSeries) -> Option<f64> {
        pct_change_last(&series.mean_bid_vol20, 10)
    }

    fn compute_factor_063(depth: &DepthSnapshot) -> Option<f64> {
        let v = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .filter(|x| x.is_finite())
            .fold(f64::NEG_INFINITY, f64::max);
        finite_opt(Some(v))
    }

    fn compute_factor_065(depth: &DepthSnapshot) -> Option<f64> {
        let v = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .filter(|x| x.is_finite())
            .fold(f64::INFINITY, f64::min);
        finite_opt(Some(v))
    }

    fn compute_factor_066(depth: &DepthSnapshot) -> Option<f64> {
        let v = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .filter(|x| x.is_finite())
            .fold(f64::INFINITY, f64::min);
        finite_opt(Some(v))
    }

    fn compute_factor_074(depth: &DepthSnapshot) -> Option<f64> {
        let asks: Vec<f64> = (0..5).map(|i| depth_level_amount(&depth.asks, i)).collect();
        std_pop(&asks)
    }

    fn compute_factor_075(depth: &DepthSnapshot) -> Option<f64> {
        let bids: Vec<f64> = (0..5).map(|i| depth_level_amount(&depth.bids, i)).collect();
        let mean = bids.iter().sum::<f64>() / bids.len() as f64;
        if mean.abs() <= 1e-12 {
            return Some(0.0);
        }
        let std = std_pop(&bids)?;
        finite_opt(Some(std / mean))
    }

    fn compute_factor_077(depth: &DepthSnapshot) -> Option<f64> {
        let bids: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        rolling_corr_last(&bids, &asks, 10, 1).ok().flatten()
    }

    fn compute_factor_086(depth: &DepthSnapshot) -> Option<f64> {
        let bid = depth_sum_amount(&depth.bids, 15);
        let ask = depth_sum_amount(&depth.asks, 15);
        finite_opt(Some(bid - ask))
    }

    fn compute_factor_087(depth: &DepthSnapshot) -> Option<f64> {
        let bid = depth_sum_amount(&depth.bids, 20);
        let ask = depth_sum_amount(&depth.asks, 20);
        if ask.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(bid / ask))
    }

    fn compute_factor_088(depth: &DepthSnapshot) -> Option<f64> {
        let bids: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        let std = std_pop(&bids)?;
        finite_opt(Some(std * std))
    }

    fn compute_factor_089(depth: &DepthSnapshot) -> Option<f64> {
        let asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        let std = std_pop(&asks)?;
        finite_opt(Some(std * std))
    }

    fn compute_factor_094(series: &SymbolSeries) -> Option<f64> {
        tail_quantile_last(&series.avg_ask_price5, 100, 0.1)
    }

    fn compute_factor_097(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_102(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_107(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_108(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_110(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_116(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_119(series: &SymbolSeries) -> Option<f64> {
        let last = *series.factor_119_mid_minus_ask_vwap5.last()?;
        let ma = rolling_mean_last(&series.factor_119_mid_minus_ask_vwap5, 120)
            .ok()
            .flatten()?;
        if ma.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(last / ma))
    }

    fn compute_factor_121(depth: &DepthSnapshot) -> Option<f64> {
        let mut asks: Vec<f64> = (0..10)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        asks.sort_by(|a, b| a.total_cmp(b));
        let mid = asks.len() / 2;
        finite_opt(Some((asks[mid - 1] + asks[mid]) / 2.0))
    }

    fn compute_factor_123(depth: &DepthSnapshot) -> Option<f64> {
        let top3 = depth_sum_amount(&depth.bids, 3);
        let total15 = depth_sum_amount(&depth.bids, 15);
        if total15.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(top3 / total15))
    }

    fn compute_factor_124(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_126(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_133(series: &SymbolSeries) -> Option<f64> {
        let bid = *series.total_bid20.last()?;
        let ask = *series.total_ask20.last()?;
        let den = bid + ask;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((bid - ask) / den))
    }

    fn compute_factor_134(series: &SymbolSeries) -> Option<f64> {
        let n = series
            .mid_price
            .len()
            .min(series.bid0v.len())
            .min(series.ask0v.len());
        if n < 300 {
            return None;
        }
        let mid = &series.mid_price[..n];
        let mid_vol: Vec<f64> = (0..n)
            .map(|i| (series.bid0v[i] + series.ask0v[i]) / 2.0)
            .collect();
        rolling_corr_last(mid, &mid_vol, 300, 1).ok().flatten()
    }

    fn compute_factor_139(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_144(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_151(series: &SymbolSeries) -> Option<f64> {
        rolling_skew_last(&series.total_volume20_sum, 45, false)
            .ok()
            .flatten()
    }

    fn compute_factor_152(series: &SymbolSeries) -> Option<f64> {
        last_opt(&series.factor_152_pct_mean)
    }

    fn compute_factor_164(series: &SymbolSeries) -> Option<f64> {
        let n = series.bid0v.len();
        if n < 2 {
            return None;
        }
        finite_opt(Some(series.bid0v[n - 1] - series.bid0v[n - 2]))
    }

    fn compute_factor_168(series: &SymbolSeries) -> Option<f64> {
        let spread = *series.spread.last()?;
        let mid = *series.mid_price.last()?;
        if mid.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(spread / mid))
    }

    fn compute_factor_170(series: &SymbolSeries) -> Option<f64> {
        pct_change_last(&series.mid_price, 120)
    }

    fn compute_factor_175(series: &SymbolSeries) -> Option<f64> {
        sample_std_last(&series.spread, 5, 1)
    }

    fn compute_factor_004(series: &SymbolSeries, depth: Option<&DepthSnapshot>) -> Option<f64> {
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

    fn compute_factor_009(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_018(depth: &DepthSnapshot) -> Option<f64> {
        let asks: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.asks, i))
            .collect();
        let mean = asks.iter().sum::<f64>() / asks.len() as f64;
        let denom = mean + 1e-6;
        let norm: Vec<f64> = asks.into_iter().map(|v| v / denom).collect();
        std_pop(&norm)
    }

    fn compute_factor_022(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_023(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_024(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_029(depth: &DepthSnapshot) -> Option<f64> {
        let bids: Vec<f64> = (0..20)
            .map(|i| depth_level_amount(&depth.bids, i))
            .collect();
        rolling_kurt_last(&bids, 20, true, false).ok().flatten()
    }

    fn compute_factor_032(series: &SymbolSeries) -> Option<f64> {
        rolling_mean_last(&series.ask_vwap_diff_5_20, 300)
            .ok()
            .flatten()
    }

    fn compute_factor_038(depth: &DepthSnapshot) -> Option<f64> {
        let vwap5 = depth_vwap(&depth.asks, 5)?;
        let vwap15 = depth_vwap(&depth.asks, 15)?;
        let value = vwap5 - vwap15;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_factor_061(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_076(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_096(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_128(series: &SymbolSeries) -> Option<f64> {
        let mean_series = rolling_mean_series_opt(&series.factor_128_skew, 300, 50).ok()?;
        last_opt(&mean_series)
    }

    fn compute_factor_156(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_160(series: &SymbolSeries) -> Option<f64> {
        last_opt(&series.factor_160_pct_change_mean)
    }

    fn compute_factor_165(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_factor_166(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_factor_176(depth: &DepthSnapshot) -> Option<f64> {
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

    fn compute_baseline_018(series: &SymbolSeries) -> Option<f64> {
        let n = series.buy_volume.len().min(series.sell_volume.len());
        if n <= 60 {
            return None;
        }
        let mut ratio = Vec::with_capacity(n);
        for i in 0..n {
            let buy = series.buy_volume[i];
            let sell = series.sell_volume[i];
            ratio.push(buy / sell);
        }
        let curr = *ratio.last()?;
        let prev = ratio[n - 1 - 60];
        if !curr.is_finite() || !prev.is_finite() {
            return Some(0.0);
        }
        let value = (curr - prev) / prev;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_baseline_034(series: &SymbolSeries) -> Option<f64> {
        let n = series.close.len();
        if n == 0 {
            return None;
        }
        let fast = rolling_mean_series(&series.close, 12, 12).ok()?;
        let slow = rolling_mean_series(&series.close, 26, 26).ok()?;
        let macd: Vec<Option<f64>> = (0..n)
            .map(|i| match (fast[i], slow[i]) {
                (Some(f), Some(s)) => finite_opt(Some(f - s)),
                _ => Some(0.0),
            })
            .collect();
        let signal = rolling_mean_series_opt(&macd, 9, 9).ok()?;
        let smmao: Vec<Option<f64>> = macd
            .iter()
            .zip(signal.iter())
            .map(|(m, s)| {
                let m = finite_opt(*m)?;
                let s = finite_opt(*s)?;
                finite_opt(Some(m - 2.0 * s))
            })
            .collect();
        last_opt(&smmao)
    }

    fn compute_baseline_045(series: &SymbolSeries) -> Option<f64> {
        let n = series.buy_amount.len().min(series.sell_amount.len());
        let window = 90;
        if n < window {
            return None;
        }
        let tail: Vec<f64> = (n - window..n)
            .map(|i| series.buy_amount[i] - series.sell_amount[i])
            .collect();
        linear_regression_intercept(&tail)
    }

    fn compute_baseline_047(series: &SymbolSeries) -> Option<f64> {
        let n = series.small_buy.len().min(series.small_sell.len());
        if n < 300 {
            return None;
        }
        let diff: Vec<f64> = (0..n)
            .map(|i| series.small_buy[i] - series.small_sell[i])
            .collect();
        rolling_mean_last(&diff, 300).ok().flatten()
    }

    fn compute_baseline_080(series: &SymbolSeries) -> Option<f64> {
        let n = series.high.len().min(series.low.len());
        let window = 10usize;
        if n < window {
            return None;
        }
        let highs = &series.high[n - window..n];
        let lows = &series.low[n - window..n];
        if highs.iter().any(|v| !v.is_finite()) || lows.iter().any(|v| !v.is_finite()) {
            return Some(0.0);
        }

        let mean_high = highs.iter().sum::<f64>() / window as f64;
        let mean_low = lows.iter().sum::<f64>() / window as f64;
        let mut cov = 0.0;
        let mut var_low = 0.0;
        for i in 0..window {
            let dh = highs[i] - mean_high;
            let dl = lows[i] - mean_low;
            cov += dh * dl;
            var_low += dl * dl;
        }
        if var_low.abs() <= 1e-12 {
            return Some(0.0);
        }
        let beta = cov / var_low;
        if beta.is_finite() {
            Some(beta)
        } else {
            Some(0.0)
        }
    }

    fn compute_baseline_082(series: &SymbolSeries) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.low.len())
            .min(series.buy_amount.len())
            .min(series.sell_amount.len());
        if n == 0 {
            return None;
        }
        let den = series.sell_amount[n - 1] - series.buy_amount[n - 1];
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        let value = (series.close[n - 1] - series.low[n - 1]) / den;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_baseline_135(series: &SymbolSeries) -> Option<f64> {
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
        let range = high - low;
        let is_hangingman =
            (range > 3.0 * (open - close).abs()) && ((close - high).abs() / (0.0001 + range) > 0.6);
        Some(if is_hangingman { 1.0 } else { 0.0 })
    }

    fn compute_baseline_193(series: &SymbolSeries) -> Option<f64> {
        let n = series.buy_amount.len().min(series.sell_amount.len());
        if n == 0 {
            return None;
        }
        let sell = series.sell_amount[n - 1];
        let value = series.buy_amount[n - 1] / sell;
        if value.is_finite() {
            Some(value)
        } else {
            Some(0.0)
        }
    }

    fn compute_baseline_004(series: &SymbolSeries) -> Option<f64> {
        let s1 = rolling_mean_series(&series.close, 30, 30).ok()?;
        let s2 = rolling_mean_series_opt(&s1, 30, 30).ok()?;
        let s3 = rolling_mean_series_opt(&s2, 30, 30).ok()?;
        let s4 = rolling_mean_series_opt(&s3, 30, 30).ok()?;
        let s5 = rolling_mean_series_opt(&s4, 30, 30).ok()?;
        let s6 = rolling_mean_series_opt(&s5, 30, 30).ok()?;
        let v1 = last_opt(&s1)?;
        let v2 = last_opt(&s2)?;
        let v3 = last_opt(&s3)?;
        let v4 = last_opt(&s4)?;
        let v5 = last_opt(&s5)?;
        let v6 = last_opt(&s6)?;
        finite_opt(Some(
            v1 - 2.0 * v2 + 3.0 * v3 - 4.0 * v4 + 5.0 * v5 - 6.0 * v6,
        ))
    }

    fn compute_baseline_007(series: &SymbolSeries) -> Option<f64> {
        let fast = rolling_mean_series(&series.net_buy_amount, 12, 12).ok()?;
        let slow = rolling_mean_series(&series.net_buy_amount, 26, 26).ok()?;
        let macd: Vec<Option<f64>> = (0..series.net_buy_amount.len())
            .map(|i| match (fast[i], slow[i]) {
                (Some(f), Some(s)) => finite_opt(Some(f - s)),
                _ => Some(0.0),
            })
            .collect();
        let signal = rolling_mean_series_opt(&macd, 9, 9).ok()?;
        last_opt(&signal)
    }

    fn compute_baseline_008(series: &SymbolSeries) -> Option<f64> {
        let fast = rolling_mean_series(&series.large_buy, 12, 12).ok()?;
        let slow = rolling_mean_series(&series.large_buy, 26, 26).ok()?;
        let macd: Vec<Option<f64>> = (0..series.large_buy.len())
            .map(|i| match (fast[i], slow[i]) {
                (Some(f), Some(s)) => finite_opt(Some(f - s)),
                _ => Some(0.0),
            })
            .collect();
        let signal = rolling_mean_series_opt(&macd, 9, 9).ok()?;
        let hist: Vec<Option<f64>> = macd
            .iter()
            .zip(signal.iter())
            .map(|(m, s)| {
                let m = finite_opt(*m)?;
                let s = finite_opt(*s)?;
                finite_opt(Some(m - s))
            })
            .collect();
        last_opt(&hist)
    }

    fn compute_baseline_016(series: &SymbolSeries) -> Option<f64> {
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
                .max(series.high[i] - series.close[i - 1])
                .max(series.low[i] - series.close[i - 1]);
        }
        let plus_ma = rolling_mean_series(&plus_dm, 14, 14).ok()?;
        let minus_ma = rolling_mean_series(&minus_dm, 14, 14).ok()?;
        let tr_ma = rolling_mean_series_opt(
            &tr.iter()
                .map(|v| if v.is_finite() { Some(*v) } else { None })
                .collect::<Vec<_>>(),
            14,
            14,
        )
        .ok()?;
        let dx: Vec<Option<f64>> = (0..n)
            .map(|i| {
                let p = finite_opt(plus_ma[i])?;
                let m = finite_opt(minus_ma[i])?;
                let t = finite_opt(tr_ma[i])?;
                if t.abs() <= 1e-12 {
                    return Some(0.0);
                }
                let plus_di = 100.0 * p / t;
                let minus_di = 100.0 * m / t;
                let den = plus_di + minus_di;
                if den.abs() <= 1e-12 {
                    return Some(0.0);
                }
                finite_opt(Some((plus_di - minus_di).abs() / den * 100.0))
            })
            .collect();
        let adx = rolling_mean_series_opt(&dx, 14, 14).ok()?;
        last_opt(&adx)
    }

    fn compute_baseline_028(series: &SymbolSeries) -> Option<f64> {
        let n = series.close.len();
        if n <= 30 {
            return None;
        }
        let change = series.close[n - 1] - series.close[n - 1 - 30];
        let mut abs_diff = Vec::with_capacity(n);
        abs_diff.push(f64::NAN);
        for i in 1..n {
            abs_diff.push((series.close[i] - series.close[i - 1]).abs());
        }
        let volatility = tail_quantile_last(&abs_diff, 30, 0.3)?;
        if volatility.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(change / volatility))
    }

    fn compute_baseline_040(series: &SymbolSeries) -> Option<f64> {
        let n = series.close.len().min(series.volume.len());
        if n < 360 {
            return None;
        }
        let mut x = Vec::with_capacity(n);
        x.push(0.0);
        for i in 1..n {
            let prev = series.close[i - 1];
            let delta = series.close[i] - series.close[i - 1];
            let pct = if prev.abs() > 1e-12 {
                delta / prev
            } else {
                0.0
            };
            x.push(pct * series.volume[i]);
        }
        rolling_sum_last_with_min_periods(&x, 360, 360)
            .ok()
            .flatten()
    }

    fn compute_baseline_046(series: &SymbolSeries) -> Option<f64> {
        let n = series.large_buy.len().min(series.large_sell.len());
        if n < 30 {
            return None;
        }
        let tail: Vec<f64> = (n - 30..n)
            .map(|i| series.large_buy[i] - series.large_sell[i])
            .collect();
        linear_regression_predict_last(&tail)
    }

    fn compute_baseline_073(series: &SymbolSeries) -> Option<f64> {
        let std = sample_std_last(&series.close, 5, 5)?;
        finite_opt(Some(std * std))
    }

    fn compute_baseline_076(series: &SymbolSeries) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len());
        if n < 14 {
            return None;
        }
        let mut tr = Vec::with_capacity(n);
        for i in 0..n {
            let prev_close = if i == 0 {
                series.close[n - 1]
            } else {
                series.close[i - 1]
            };
            let mut v = (series.high[i] - series.low[i]).max(0.0);
            v = v.max((series.high[i] - prev_close).abs());
            v = v.max((series.low[i] - prev_close).abs());
            tr.push(v);
        }
        let atr = rolling_mean_last(&tr, 14).ok().flatten()?;
        let close = series.close[n - 1];
        if close.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((atr / close) * 100.0))
    }

    fn compute_baseline_079(series: &SymbolSeries) -> Option<f64> {
        rolling_corr_last(&series.close, &series.volume, 14, 1)
            .ok()
            .flatten()
    }

    fn compute_baseline_100(series: &SymbolSeries) -> Option<f64> {
        rolling_mean_last(&series.close, 180).ok().flatten()
    }

    fn compute_baseline_113(series: &SymbolSeries) -> Option<f64> {
        let n = series.high.len().min(series.low.len());
        if n < 90 {
            return None;
        }
        let mid_log: Vec<f64> = (0..n)
            .map(|i| ((series.high[i] + series.low[i]) / 2.0).ln())
            .collect();
        tail_quantile_last(&mid_log, 90, 0.2)
    }

    fn compute_baseline_116(series: &SymbolSeries) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len())
            .min(series.volume.len());
        if n < 14 {
            return None;
        }
        let mut tp = Vec::with_capacity(n);
        for i in 0..n {
            tp.push((series.high[i] + series.low[i] + series.close[i]) / 3.0);
        }
        let mut pos = vec![0.0; n];
        let mut neg = vec![0.0; n];
        for i in 1..n {
            let dt = tp[i] - tp[i - 1];
            pos[i] = dt.max(0.0) * series.volume[i];
            neg[i] = (-dt).max(0.0) * series.volume[i];
        }
        let pos_sum = rolling_sum_last_with_min_periods(&pos, 14, 14)
            .ok()
            .flatten()?;
        let neg_sum = rolling_sum_last_with_min_periods(&neg, 14, 14)
            .ok()
            .flatten()?;
        finite_opt(Some(pos_sum - neg_sum))
    }

    fn compute_baseline_117(series: &SymbolSeries) -> Option<f64> {
        let n = series.close.len();
        if n < 26 {
            return None;
        }
        let mut pos = vec![0.0; n];
        let mut neg = vec![0.0; n];
        for i in 1..n {
            let d = series.close[i] - series.close[i - 1];
            if d > 0.0 {
                pos[i] = d;
            } else if d < 0.0 {
                neg[i] = -d;
            }
        }
        let pos_avg = rolling_mean_last(&pos, 12).ok().flatten()?;
        let neg_avg = rolling_mean_last(&neg, 26).ok().flatten()?;
        if neg_avg.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some(pos_avg / neg_avg))
    }

    fn compute_baseline_120(series: &SymbolSeries) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len());
        if n < 350 {
            return None;
        }
        let mut wcl = Vec::with_capacity(n);
        for i in 0..n {
            wcl.push((series.high[i] + series.low[i] + 2.0 * series.close[i]) / 4.0);
        }
        let mut pct = vec![f64::NAN; n];
        for i in 50..n {
            let prev = wcl[i - 50];
            if prev.abs() > 1e-12 {
                pct[i] = (wcl[i] - prev) / prev;
            }
        }
        let tail: Vec<f64> = pct[n - 300..n].to_vec();
        if tail.iter().filter(|v| v.is_finite()).count() < 300 {
            return None;
        }
        let mean = tail.iter().sum::<f64>() / 300.0;
        finite_opt(Some(mean))
    }

    fn compute_baseline_134(series: &SymbolSeries) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.open.len())
            .min(series.close.len());
        if n == 0 {
            return None;
        }
        let h = series.high[n - 1];
        let l = series.low[n - 1];
        let o = series.open[n - 1];
        let c = series.close[n - 1];
        let range = h - l;
        let is_hammer = (range > 3.0 * (o - c))
            && ((c - l) / (0.001 + range) > 0.6)
            && ((o - l) / (0.001 + range) > 0.6);
        Some(if is_hammer { 1.0 } else { 0.0 })
    }

    fn compute_baseline_137(series: &SymbolSeries) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.open.len())
            .min(series.volume.len());
        if n == 0 {
            return None;
        }
        let den = series.volume[n - 1];
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((series.close[n - 1] - series.open[n - 1]) / den))
    }

    fn compute_baseline_140(series: &SymbolSeries) -> Option<f64> {
        let n = series.low.len().min(series.close.len());
        if n < 60 {
            return None;
        }
        let low_min = series.low[n - 60..n]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        finite_opt(Some(low_min - series.close[n - 1]))
    }

    fn compute_baseline_141(series: &SymbolSeries) -> Option<f64> {
        let n = series.high.len().min(series.close.len());
        if n < 60 {
            return None;
        }
        let high_max = series.high[n - 60..n]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        finite_opt(Some(high_max - series.close[n - 1]))
    }

    fn compute_baseline_163(series: &SymbolSeries) -> Option<f64> {
        let n = series.close.len();
        if n < 30 {
            return None;
        }
        let min_close = series.close[n - 30..n]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let den = series.close[n - 1];
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((min_close - den) / den))
    }

    fn compute_baseline_164(series: &SymbolSeries) -> Option<f64> {
        let n = series.close.len();
        if n < 30 {
            return None;
        }
        let max_close = series.close[n - 30..n]
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let den = series.close[n - 1];
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((max_close - den) / den))
    }

    fn compute_baseline_172(series: &SymbolSeries) -> Option<f64> {
        // Same ADX-style approximation as baseline_172 python formula.
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
            plus_dm[i] = (series.high[i] - series.high[i - 1]).max(0.0);
            minus_dm[i] = (series.low[i - 1] - series.low[i]).max(0.0);
            tr[i] = (series.high[i] - series.low[i])
                .max((series.high[i] - series.close[i - 1]).abs())
                .max((series.low[i] - series.close[i - 1]).abs());
        }
        let plus_ma = rolling_mean_series(&plus_dm, 14, 14).ok()?;
        let minus_ma = rolling_mean_series(&minus_dm, 14, 14).ok()?;
        let tr_ma = rolling_mean_series_opt(
            &tr.iter()
                .map(|v| if v.is_finite() { Some(*v) } else { None })
                .collect::<Vec<_>>(),
            14,
            14,
        )
        .ok()?;
        let dx: Vec<Option<f64>> = (0..n)
            .map(|i| {
                let p = finite_opt(plus_ma[i])?;
                let m = finite_opt(minus_ma[i])?;
                let t = finite_opt(tr_ma[i])?;
                if t.abs() <= 1e-12 {
                    return Some(0.0);
                }
                let plus_di = 100.0 * p / t;
                let minus_di = 100.0 * m / t;
                let den = plus_di + minus_di;
                if den.abs() <= 1e-12 {
                    return Some(0.0);
                }
                finite_opt(Some((plus_di - minus_di).abs() / den * 100.0))
            })
            .collect();
        let adx = rolling_mean_series_opt(&dx, 14, 14).ok()?;
        last_opt(&adx)
    }

    fn compute_baseline_177(series: &SymbolSeries) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len());
        if n < 125 {
            return None;
        }
        let hl: Vec<f64> = (0..n).map(|i| series.high[i] + series.low[i]).collect();
        let mut diff5 = vec![f64::NAN; n];
        for i in 5..n {
            diff5[i] = hl[i] - hl[i - 5];
        }
        let tail: Vec<f64> = diff5[n - 120..n].to_vec();
        if tail.iter().filter(|v| v.is_finite()).count() < 120 {
            return None;
        }
        let den = tail.iter().sum::<f64>() / 120.0;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((series.high[n - 1] - series.close[n - 1]) / den))
    }

    fn compute_baseline_178(series: &SymbolSeries) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len());
        if n < 33 {
            return None;
        }
        let hl: Vec<f64> = (0..n).map(|i| series.high[i] + series.low[i]).collect();
        let mut diff3 = vec![f64::NAN; n];
        for i in 3..n {
            diff3[i] = hl[i] - hl[i - 3];
        }
        let tail: Vec<f64> = diff3[n - 30..n].to_vec();
        if tail.iter().filter(|v| v.is_finite()).count() < 30 {
            return None;
        }
        let den = tail.iter().sum::<f64>() / 30.0;
        if den.abs() <= 1e-12 {
            return Some(0.0);
        }
        finite_opt(Some((series.close[n - 1] - series.low[n - 1]) / den))
    }

    fn compute_baseline_189(series: &SymbolSeries) -> Option<f64> {
        let short = rolling_mean_series(&series.close, 20, 20).ok()?;
        let long = rolling_mean_series(&series.close, 90, 90).ok()?;
        let x: Vec<f64> = (0..series.close.len())
            .map(|i| match (short[i], long[i]) {
                (Some(s), Some(l)) => s - l,
                _ => f64::NAN,
            })
            .collect();
        let y: Vec<f64> = long.iter().map(|v| v.unwrap_or(f64::NAN)).collect();
        corr_last_with_min_periods(&x, &y, 100, 30)
    }

    fn compute_baseline_191(series: &SymbolSeries) -> Option<f64> {
        let n = series
            .high
            .len()
            .min(series.low.len())
            .min(series.close.len());
        if n < 16 {
            return None;
        }
        let mut fastk = vec![f64::NAN; n];
        for i in 13..n {
            let low_min = series.low[i + 1 - 14..i + 1]
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min);
            let high_max = series.high[i + 1 - 14..i + 1]
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            let den = high_max - low_min;
            if den.abs() > 1e-12 {
                fastk[i] = 100.0 * (series.close[i] - low_min) / den;
            }
        }
        let slowk = rolling_mean_series_opt(
            &fastk
                .iter()
                .map(|v| if v.is_finite() { Some(*v) } else { None })
                .collect::<Vec<_>>(),
            3,
            3,
        )
        .ok()?;
        let slowd = rolling_mean_series_opt(&slowk, 3, 3).ok()?;
        let k = last_opt(&slowk)?;
        let d = last_opt(&slowd)?;
        finite_opt(Some(k - d))
    }

    fn compute_baseline_196(series: &SymbolSeries) -> Option<f64> {
        let periods = 10usize;
        if series.large_buy.len() <= periods {
            return None;
        }
        let curr = *series.large_buy.last()?;
        let prev = series.large_buy[series.large_buy.len() - 1 - periods];
        if !curr.is_finite() || !prev.is_finite() {
            return Some(0.0);
        }
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

    fn compute_td_ti_010(series: &SymbolSeries) -> Option<f64> {
        let ema1 = rolling_mean_series(&series.close, 30, 30).ok()?;
        let ema2 = rolling_mean_series_opt(&ema1, 30, 30).ok()?;
        let ema3 = rolling_mean_series_opt(&ema2, 30, 30).ok()?;
        last_opt(&ema3)
    }

    fn compute_td_ti_015(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_ti_031(series: &SymbolSeries) -> Option<f64> {
        corr_last_with_min_periods(&series.large_order, &series.small_order, 60, 10)
    }

    fn compute_td_ti_036(series: &SymbolSeries) -> Option<f64> {
        let n = series.high.len();
        let period = 14usize;
        if n < period + 1 {
            return None;
        }
        let tail = &series.high[n - (period + 1)..n];
        let mut argmax = 0usize;
        let mut best = f64::NEG_INFINITY;
        for (i, v) in tail.iter().enumerate() {
            if *v > best {
                best = *v;
                argmax = i;
            }
        }
        finite_opt(Some(100.0 * argmax as f64 / period as f64))
    }

    fn compute_td_ti_037(series: &SymbolSeries) -> Option<f64> {
        let n = series.low.len();
        let period = 14usize;
        if n < period + 1 {
            return None;
        }
        let tail = &series.low[n - (period + 1)..n];
        let mut argmin = 0usize;
        let mut best = f64::INFINITY;
        for (i, v) in tail.iter().enumerate() {
            if *v < best {
                best = *v;
                argmin = i;
            }
        }
        finite_opt(Some(100.0 * argmin as f64 / period as f64))
    }

    fn compute_td_mt_003(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_mt_008(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_mt_014(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_mt_015(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_mt_039(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_vi_010(series: &SymbolSeries) -> Option<f64> {
        let n = series.high.len().min(series.low.len());
        if n < 2 {
            return None;
        }
        let hd = series.high[n - 1] - series.high[n - 2];
        let ld = series.low[n - 1] - series.low[n - 2];
        finite_opt(Some(if hd > ld { hd } else { 0.0 }))
    }

    fn compute_td_vi_011(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_vi_025(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_vi_026(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_vi_028(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_pt_001(series: &SymbolSeries) -> Option<f64> {
        finite_opt(series.close.last().copied().map(f64::sin))
    }

    fn compute_td_pt_002(series: &SymbolSeries) -> Option<f64> {
        finite_opt(series.close.last().copied().map(f64::cos))
    }

    fn compute_td_pt_010(series: &SymbolSeries) -> Option<f64> {
        let n = series
            .close
            .len()
            .min(series.high.len())
            .min(series.low.len());
        if n < 15 {
            return None;
        }
        let mid = (series.close[n - 1] + series.high[n - 1] + series.low[n - 1]) / 3.0;
        let llv_low = series.low[n - 15..n]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let llv_high = series.high[n - 15..n]
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        let zc2 = mid - llv_high + llv_low;
        finite_opt(Some(series.close[n - 1] - zc2))
    }

    fn compute_td_pt_027(series: &SymbolSeries) -> Option<f64> {
        let n = series.close.len();
        if n < 14 {
            return None;
        }
        let tail = &series.close[n - 14..n];
        let mut idx = 0usize;
        let mut best = f64::INFINITY;
        for (i, v) in tail.iter().enumerate() {
            if *v < best {
                best = *v;
                idx = i;
            }
        }
        Some(idx as f64)
    }

    fn compute_td_ci_007(series: &SymbolSeries) -> Option<f64> {
        let ma = rolling_mean_last_with_min_periods(&series.close, 30, 1)
            .ok()
            .flatten()?;
        finite_opt(Some(ma.sin()))
    }

    fn compute_td_ci_008(series: &SymbolSeries) -> Option<f64> {
        let ma = rolling_mean_last_with_min_periods(&series.close, 30, 1)
            .ok()
            .flatten()?;
        finite_opt(Some(ma.cos()))
    }

    fn compute_td_pr_001(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_pr_002(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_pr_007(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_pr_015(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_pr_016(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_pr_017(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_tp_vpi_006(series: &SymbolSeries) -> Option<f64> {
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
        let obv = rolling_sum_series(&obv_delta, 360, 360).ok()?;
        let ma = rolling_mean_series_opt(&obv, 14, 14).ok()?;
        last_opt(&ma)
    }

    fn compute_tp_vpi_014(series: &SymbolSeries) -> Option<f64> {
        let n = series.volume.len();
        if n < 39 {
            return None;
        }
        let vol_ma = rolling_mean_series(&series.volume, 20, 20).ok()?;
        let ratio: Vec<Option<f64>> = (0..n)
            .map(|i| {
                let m = finite_opt(vol_ma[i])?;
                if m.abs() <= 1e-12 {
                    return Some(0.0);
                }
                finite_opt(Some(series.volume[i] / m))
            })
            .collect();
        let ratio_ma = rolling_mean_series_opt(&ratio, 20, 20).ok()?;
        last_opt(&ratio_ma)
    }

    fn compute_tp_vpi_017(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_ti_026(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_ti_033(series: &SymbolSeries) -> Option<f64> {
        let n = series.open.len().min(series.volume.len());
        if n < 100 {
            return None;
        }
        let window = n.min(300);
        rolling_corr_last(&series.open[..n], &series.volume[..n], window, 1)
            .ok()
            .flatten()
    }

    fn compute_td_ti_034(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_tp_vpi_004(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_tp_vpi_001(series: &SymbolSeries) -> Option<f64> {
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
        let ad = rolling_sum_series_opt(&clv_x_opt, 360, 360).ok()?;
        last_opt(&ad)
    }

    fn compute_tp_vpi_002(series: &SymbolSeries) -> Option<f64> {
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
        let ad = rolling_sum_series_opt(&clv_x_opt, 360, 360).ok()?;
        let ad_ma = rolling_mean_series_opt(&ad, 14, 14).ok()?;
        last_opt(&ad_ma)
    }

    fn compute_tp_vpi_005(series: &SymbolSeries) -> Option<f64> {
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
        let obv = rolling_sum_series(&obv_delta, 360, 360).ok()?;
        last_opt(&obv)
    }

    fn compute_tp_vpi_015(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_pt_003(series: &SymbolSeries) -> Option<f64> {
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
        let amount_roll = rolling_sum_series(&series.amount[..n], 6, 6).ok()?;
        let volume_roll = rolling_sum_series(&series.volume[..n], 6, 6).ok()?;

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

        let apb_roll = rolling_mean_series_opt(&apb, 18, 18).ok()?;
        last_opt(&apb_roll).or(Some(0.0))
    }

    fn compute_td_pt_004(series: &SymbolSeries) -> Option<f64> {
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

        let vol_ma = rolling_mean_series(&series.volume[..n], 120, 120).ok()?;
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

    fn compute_td_ci_003(series: &SymbolSeries) -> Option<f64> {
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
        let result = rolling_mean_series_opt(&inv_phase, 30, 1).ok()?;
        last_opt(&result)
    }

    fn compute_td_pr_005(series: &SymbolSeries) -> Option<f64> {
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

    fn compute_td_pr_006(series: &SymbolSeries) -> Option<f64> {
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
        depth: &DepthSnapshot,
    ) -> Option<(f64, bool, usize)> {
        let state = self.symbol_states.entry(symbol.to_string()).or_default();
        Self::compute_factor_118_with_state(state, depth)
    }

    fn compute_factor_118_with_state(
        state: &mut SymbolCalcState,
        depth: &DepthSnapshot,
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
struct SymbolDetailResp {
    factors: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct LoginResp {
    token: String,
}

#[derive(Debug, Serialize)]
struct LoginReq<'a> {
    password: &'a str,
}

async fn load_symbol_factor_plans(
    mm: &ModelManagerConfig,
    symbols: &[String],
) -> Result<HashMap<String, SymbolFactorPlan>> {
    let base_url = mm.base_url.trim_end_matches('/');
    let client = Client::builder()
        .timeout(Duration::from_millis(mm.request_timeout_ms))
        .build()
        .context("build reqwest client failed")?;

    let token = authenticate_model_manager(&client, mm, base_url).await?;

    let mut out = HashMap::with_capacity(symbols.len());
    for symbol in symbols {
        let url = format!(
            "{}/api/models/{}/symbols/{}",
            base_url,
            urlencoding::encode(mm.model_name.as_str()),
            urlencoding::encode(symbol)
        );

        let mut req = client.get(&url);
        if let Some(token) = token.as_deref() {
            req = req.bearer_auth(token);
        }

        let resp = req
            .send()
            .await
            .with_context(|| format!("GET {} failed", url))?
            .error_for_status()
            .with_context(|| format!("GET {} returned error status", url))?;

        let detail: SymbolDetailResp = resp
            .json()
            .await
            .with_context(|| format!("decode symbol detail failed: {}", url))?;

        let mut ordered_factors = Vec::with_capacity(detail.factors.len());
        for name in detail.factors {
            let factor_id = FusionFactorId::from_name(&name);
            let extra_factor_id = ExtraFactorId::from_name(&name);
            if factor_id.is_none() && extra_factor_id.is_none() {
                warn!(
                    "model_manager factor name is not mapped yet: symbol={} factor={}",
                    symbol, name
                );
            }
            ordered_factors.push(FactorBinding {
                name,
                factor_id,
                extra_factor_id,
            });
        }

        let unknown_names: Vec<&str> = ordered_factors
            .iter()
            .filter(|f| f.factor_id.is_none() && f.extra_factor_id.is_none())
            .map(|f| f.name.as_str())
            .collect();
        if !unknown_names.is_empty() {
            warn!(
                "factor-plan: symbol={} unknown_factors=[{}]",
                symbol,
                unknown_names.join(",")
            );
        }

        out.insert(symbol.clone(), SymbolFactorPlan { ordered_factors });
    }

    Ok(out)
}

async fn authenticate_model_manager(
    client: &Client,
    mm: &ModelManagerConfig,
    base_url: &str,
) -> Result<Option<String>> {
    if let Some(token) = mm
        .bearer_token
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        return Ok(Some(token));
    }

    let Some(password) = mm
        .password
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    else {
        return Ok(None);
    };

    let login_url = format!("{}/api/auth/login", base_url);
    let resp = client
        .post(&login_url)
        .json(&LoginReq {
            password: password.as_str(),
        })
        .send()
        .await
        .with_context(|| format!("POST {} failed", login_url))?
        .error_for_status()
        .with_context(|| format!("POST {} returned error status", login_url))?;

    let payload: LoginResp = resp
        .json()
        .await
        .with_context(|| format!("decode login response failed: {}", login_url))?;

    Ok(Some(payload.token))
}

fn compute_mid_price_minus_bid_vwap(depth: &DepthSnapshot) -> Option<f64> {
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

pub fn depth_best_bid(depth: &DepthSnapshot) -> (f64, f64) {
    match depth.bids.first() {
        Some(level) => (level.price, level.amount),
        None => (f64::NAN, f64::NAN),
    }
}

pub fn depth_best_ask(depth: &DepthSnapshot) -> (f64, f64) {
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

fn tail_quantile_last(values: &[f64], window: usize, q: f64) -> Option<f64> {
    if window == 0 || values.len() < window || !(0.0..=1.0).contains(&q) {
        return None;
    }
    let mut tail: Vec<f64> = values[values.len() - window..]
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .collect();
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

fn sample_std_last(values: &[f64], window: usize, min_periods: usize) -> Option<f64> {
    if window == 0 || min_periods == 0 || values.len() < min_periods {
        return None;
    }
    let start = values.len().saturating_sub(window);
    let tail: Vec<f64> = values[start..]
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .collect();
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
    xs: &[f64],
    ys: &[f64],
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
        let xv = xs[i];
        let yv = ys[i];
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
    depth: &DepthSnapshot,
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

fn linear_regression_predict_last(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let n = values.len() as f64;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xx = 0.0;
    let mut sum_xy = 0.0;
    for (i, y) in values.iter().enumerate() {
        if !y.is_finite() {
            return Some(0.0);
        }
        let x = i as f64;
        sum_x += x;
        sum_y += *y;
        sum_xx += x * x;
        sum_xy += x * *y;
    }
    let denom = n * sum_xx - sum_x * sum_x;
    if denom.abs() <= 1e-12 {
        return Some(0.0);
    }
    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n;
    let pred = slope * (n - 1.0) + intercept;
    finite_opt(Some(pred))
}

fn pct_change_last(values: &[f64], periods: usize) -> Option<f64> {
    if periods == 0 || values.len() <= periods {
        return None;
    }
    let curr = *values.last()?;
    let prev = values[values.len() - 1 - periods];
    if !curr.is_finite() || !prev.is_finite() || prev.abs() <= 1e-12 {
        return Some(0.0);
    }
    let value = (curr - prev) / prev;
    if value.is_finite() {
        Some(value)
    } else {
        None
    }
}

fn linear_regression_intercept(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let n = values.len() as f64;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xx = 0.0;
    let mut sum_xy = 0.0;

    for (i, y) in values.iter().enumerate() {
        if !y.is_finite() {
            return Some(0.0);
        }
        let x = i as f64;
        sum_x += x;
        sum_y += *y;
        sum_xx += x * x;
        sum_xy += x * *y;
    }

    let denom = n * sum_xx - sum_x * sum_x;
    if denom.abs() <= 1e-12 {
        return Some(0.0);
    }
    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n;
    if intercept.is_finite() {
        Some(intercept)
    } else {
        None
    }
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

fn last_opt(values: &[Option<f64>]) -> Option<f64> {
    finite_opt(values.last().copied().flatten()).or(Some(0.0))
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
