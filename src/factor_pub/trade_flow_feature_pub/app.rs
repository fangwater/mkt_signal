//! Trade flow 特征应用主模块
//!
//! 订阅 trade + depth 数据，按 bar 聚合，并输出附带深度尾部的 TradeFlowFeatureMsg

use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use redis::Commands;
use rocksdb::{
    ColumnFamilyDescriptor, DBCompressionType, Direction, IteratorMode, Options, WriteOptions, DB,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use super::cfg::{PersistenceConfig, RuntimeConfig, TradeFlowFeaturePubConfig};
use super::publisher::TradeFlowFeaturePublisher;
use crate::common::mkt_msg::MktMsgType;
use crate::common::redis_client::RedisSettings;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::trade_flow_feature_msg::{TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM};
use crate::depth_pub::depth_msg::{DepthMsgType, DEPTH25_MAX_BYTES, DEPTH50_MAX_BYTES};
use crate::signal::common::TradingVenue;

const TRADE_MAX_BYTES: usize = 128;
const IDLE_SLEEP_MICROS: u64 = 200;
const TIMER_CHECK_INTERVAL_MICROS: u64 = 500;
const AMOUNT_THRESHOLD_REDIS_KEY_SUFFIX: &str = "amount-thresholds";
const REDIS_WARN_INTERVAL_SECS: u64 = 60;
const ROCKSDB_WARN_INTERVAL_SECS: u64 = 60;
const MISSING_DEPTH_WARN_INTERVAL_SECS: u64 = 60;
const PUBLISH_OUTCOME_LOG_INTERVAL_SECS: u64 = 10;
const PERSISTENCE_CLEANUP_INTERVAL_SECS: u64 = 7_200;
const TRADE_FLOW_FEATURE_CF_SUFFIX: &str = "trade_flow:feature";
const FIXED_TRADE_CHANNEL: &str = "trade";
const FIXED_REDIS_HOST: &str = "127.0.0.1";
const FIXED_REDIS_PORT: u16 = 6379;
const FIXED_REDIS_DB: i64 = 0;
const TRADE_DEDUP_WINDOW_MS: i64 = 10_000;
const MAX_DEPTH_LEVELS_CACHE: usize = 20;
const APPENDED_DEPTH_DIM: usize = MAX_DEPTH_LEVELS_CACHE * 4;

#[derive(Debug, Clone, Copy)]
enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug)]
struct TradeTick {
    symbol: String,
    trade_id: i64,
    timestamp_ms: i64,
    side: TradeSide,
    price: f64,
    amount: f64,
}

#[derive(Debug, Clone, Copy)]
enum DepthChannel {
    Depth25,
    Depth50,
}

impl DepthChannel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Depth25 => "depth25",
            Self::Depth50 => "depth50",
        }
    }

    fn from_cfg(value: &str) -> Result<Self> {
        match value {
            "depth25" => Ok(Self::Depth25),
            "depth50" => Ok(Self::Depth50),
            other => anyhow::bail!("unsupported depth channel: {}", other),
        }
    }

    fn expected_msg_type(self) -> u32 {
        match self {
            Self::Depth25 => DepthMsgType::Depth25 as u32,
            Self::Depth50 => DepthMsgType::Depth50 as u32,
        }
    }

    fn level_count(self) -> usize {
        match self {
            Self::Depth25 => 25,
            Self::Depth50 => 50,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DepthLevel {
    price: f64,
    amount: f64,
}

#[derive(Debug, Clone)]
struct DepthSnapshot {
    bids: Vec<DepthLevel>,
    asks: Vec<DepthLevel>,
}

enum DepthSubscriber {
    Depth25(Subscriber<ipc::Service, [u8; DEPTH25_MAX_BYTES], ()>),
    Depth50(Subscriber<ipc::Service, [u8; DEPTH50_MAX_BYTES], ()>),
}

impl DepthSubscriber {
    fn receive_snapshot(&self, venue: TradingVenue) -> Result<Option<(String, DepthSnapshot)>> {
        match self {
            Self::Depth25(sub) => Ok(sub.receive()?.and_then(|sample| {
                parse_depth_snapshot(sample.payload(), DepthChannel::Depth25, venue)
            })),
            Self::Depth50(sub) => Ok(sub.receive()?.and_then(|sample| {
                parse_depth_snapshot(sample.payload(), DepthChannel::Depth50, venue)
            })),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AmountThreshold {
    medium_notional_threshold: f64,
    large_notional_threshold: f64,
}

#[derive(Debug, Clone)]
struct TradeBar {
    start_ms: i64,
    has_trade: bool,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    amount: f64,
    avg_amount: f64,
    count: u64,
    buy_count: u64,
    sell_count: u64,
    buy_amount: f64,
    sell_amount: f64,
    buy_volume: f64,
    sell_volume: f64,
    large_order: f64,
    medium_order: f64,
    small_order: f64,
    large_buy: f64,
    large_sell: f64,
    medium_buy: f64,
    medium_sell: f64,
    small_buy: f64,
    small_sell: f64,
    vwap: f64,
    buy_vwap: f64,
    sell_vwap: f64,
    net_buy_amount: f64,
    net_buy_volume: f64,
    net_buy_pct: f64,
    net_buy_large: f64,
    net_buy_medium: f64,
    net_buy_small: f64,
}

impl TradeBar {
    fn new(start_ms: i64) -> Self {
        Self {
            start_ms,
            has_trade: false,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            amount: 0.0,
            avg_amount: 0.0,
            count: 0,
            buy_count: 0,
            sell_count: 0,
            buy_amount: 0.0,
            sell_amount: 0.0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            large_order: 0.0,
            medium_order: 0.0,
            small_order: 0.0,
            large_buy: 0.0,
            large_sell: 0.0,
            medium_buy: 0.0,
            medium_sell: 0.0,
            small_buy: 0.0,
            small_sell: 0.0,
            vwap: 0.0,
            buy_vwap: 0.0,
            sell_vwap: 0.0,
            net_buy_amount: 0.0,
            net_buy_volume: 0.0,
            net_buy_pct: 0.0,
            net_buy_large: 0.0,
            net_buy_medium: 0.0,
            net_buy_small: 0.0,
        }
    }

    fn empty(start_ms: i64) -> Self {
        Self::new(start_ms)
    }

    fn update(&mut self, trade: &TradeTick, threshold: AmountThreshold) {
        let notional = trade.price * trade.amount;
        if !notional.is_finite() || notional <= 0.0 {
            return;
        }

        if !self.has_trade {
            self.has_trade = true;
            self.open = trade.price;
            self.high = trade.price;
            self.low = trade.price;
            self.close = trade.price;
        } else {
            self.high = self.high.max(trade.price);
            self.low = self.low.min(trade.price);
            self.close = trade.price;
        }

        self.volume += trade.amount;
        self.amount += notional;
        self.count += 1;

        match trade.side {
            TradeSide::Buy => {
                self.buy_count += 1;
                self.buy_amount += notional;
                self.buy_volume += trade.amount;
            }
            TradeSide::Sell => {
                self.sell_count += 1;
                self.sell_amount += notional;
                self.sell_volume += trade.amount;
            }
        }

        if notional >= threshold.large_notional_threshold {
            self.large_order += notional;
            match trade.side {
                TradeSide::Buy => self.large_buy += notional,
                TradeSide::Sell => self.large_sell += notional,
            }
        } else if notional >= threshold.medium_notional_threshold {
            self.medium_order += notional;
            match trade.side {
                TradeSide::Buy => self.medium_buy += notional,
                TradeSide::Sell => self.medium_sell += notional,
            }
        } else {
            self.small_order += notional;
            match trade.side {
                TradeSide::Buy => self.small_buy += notional,
                TradeSide::Sell => self.small_sell += notional,
            }
        }
    }

    fn finalize(
        mut self,
        last_close: Option<f64>,
        last_vwap: Option<f64>,
        last_buy_vwap: Option<f64>,
        last_sell_vwap: Option<f64>,
    ) -> Self {
        if !self.has_trade {
            let px = last_close.unwrap_or(0.0);
            self.open = px;
            self.high = px;
            self.low = px;
            self.close = px;
        }

        self.avg_amount = if self.count > 0 {
            self.amount / self.count as f64
        } else {
            0.0
        };

        self.vwap = if self.volume > 0.0 {
            self.amount / self.volume
        } else {
            last_vwap.unwrap_or(0.0)
        };

        self.buy_vwap = if self.buy_volume > 0.0 {
            self.buy_amount / self.buy_volume
        } else {
            last_buy_vwap.unwrap_or(0.0)
        };

        self.sell_vwap = if self.sell_volume > 0.0 {
            self.sell_amount / self.sell_volume
        } else {
            last_sell_vwap.unwrap_or(0.0)
        };

        self.net_buy_amount = self.buy_amount - self.sell_amount;
        self.net_buy_volume = self.buy_volume - self.sell_volume;
        let total_orders = self.buy_amount + self.sell_amount;
        self.net_buy_pct = self.net_buy_amount / (total_orders + 1e-6);
        self.net_buy_large = self.large_buy - self.large_sell;
        self.net_buy_medium = self.medium_buy - self.medium_sell;
        self.net_buy_small = self.small_buy - self.small_sell;

        self
    }

    fn has_valid_ffill_fields(&self) -> bool {
        [
            self.open,
            self.high,
            self.low,
            self.close,
            self.vwap,
            self.buy_vwap,
            self.sell_vwap,
        ]
        .iter()
        .all(|v| v.is_finite() && *v > 0.0)
    }

    fn to_feature_values(&self) -> [f64; TRADE_FLOW_FEATURE_DIM] {
        [
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.amount,
            self.avg_amount,
            self.count as f64,
            self.buy_count as f64,
            self.sell_count as f64,
            self.buy_amount,
            self.sell_amount,
            self.buy_volume,
            self.sell_volume,
            self.large_order,
            self.medium_order,
            self.small_order,
            self.large_buy,
            self.large_sell,
            self.medium_buy,
            self.medium_sell,
            self.small_buy,
            self.small_sell,
            self.vwap,
            self.buy_vwap,
            self.sell_vwap,
            self.net_buy_amount,
            self.net_buy_volume,
            self.net_buy_pct,
            self.net_buy_large,
            self.net_buy_medium,
            self.net_buy_small,
        ]
    }
}

struct SymbolState {
    bar: Option<TradeBar>,
    last_bar_start_ms: Option<i64>,
    last_close: Option<f64>,
    last_vwap: Option<f64>,
    last_buy_vwap: Option<f64>,
    last_sell_vwap: Option<f64>,
    closed_bars: Vec<TradeBar>,
}

impl SymbolState {
    fn new() -> Self {
        Self {
            bar: None,
            last_bar_start_ms: None,
            last_close: None,
            last_vwap: None,
            last_buy_vwap: None,
            last_sell_vwap: None,
            closed_bars: Vec::new(),
        }
    }

    fn apply_trade(
        &mut self,
        trade: &TradeTick,
        runtime: &RuntimeConfig,
        threshold: AmountThreshold,
    ) -> bool {
        let mut late_trade = false;
        let trade_bar_start_ms = align_to_period(trade.timestamp_ms, runtime.bar_ms);

        match self.bar.as_mut() {
            None => {
                if let Some(last_start) = self.last_bar_start_ms {
                    if trade_bar_start_ms <= last_start {
                        return true;
                    }
                }
                self.fill_empty_until(trade_bar_start_ms, runtime.bar_ms);
                let mut bar = TradeBar::new(trade_bar_start_ms);
                bar.update(trade, threshold);
                self.bar = Some(bar);
            }
            Some(bar) => {
                if trade_bar_start_ms == bar.start_ms {
                    bar.update(trade, threshold);
                } else if trade_bar_start_ms > bar.start_ms {
                    if let Some(closed_bar) = self.bar.take().map(|b| self.finalize_bar(b)) {
                        self.closed_bars.push(closed_bar);
                    }
                    self.fill_empty_until(trade_bar_start_ms, runtime.bar_ms);
                    let mut next_bar = TradeBar::new(trade_bar_start_ms);
                    next_bar.update(trade, threshold);
                    self.bar = Some(next_bar);
                } else {
                    late_trade = true;
                }
            }
        }

        late_trade
    }

    fn finalize_bar(&mut self, bar: TradeBar) -> TradeBar {
        let bar = bar.finalize(
            self.last_close,
            self.last_vwap,
            self.last_buy_vwap,
            self.last_sell_vwap,
        );
        self.last_bar_start_ms = Some(bar.start_ms);
        if bar.close.is_finite() && bar.close > 0.0 {
            self.last_close = Some(bar.close);
        }
        if bar.vwap.is_finite() && bar.vwap > 0.0 {
            self.last_vwap = Some(bar.vwap);
        }
        if bar.buy_vwap.is_finite() && bar.buy_vwap > 0.0 {
            self.last_buy_vwap = Some(bar.buy_vwap);
        }
        if bar.sell_vwap.is_finite() && bar.sell_vwap > 0.0 {
            self.last_sell_vwap = Some(bar.sell_vwap);
        }
        bar
    }

    fn close_due_bars(&mut self, now_ms: i64, period_ms: i64) {
        if let Some(bar) = self.bar.take() {
            let close_at_ms = bar.start_ms + period_ms;
            if now_ms >= close_at_ms {
                let finalized = self.finalize_bar(bar);
                self.closed_bars.push(finalized);
            } else {
                self.bar = Some(bar);
            }
        }

        if self.bar.is_none() {
            let mut next_start = match self.last_bar_start_ms {
                Some(start) => start + period_ms,
                None => return,
            };

            while next_start + period_ms <= now_ms {
                let finalized = self.finalize_bar(TradeBar::empty(next_start));
                self.closed_bars.push(finalized);
                next_start += period_ms;
            }
        }
    }

    fn fill_empty_until(&mut self, target_start_ms: i64, period_ms: i64) {
        let Some(last_start) = self.last_bar_start_ms else {
            return;
        };
        let mut next_start = last_start + period_ms;
        while next_start < target_start_ms {
            let finalized = self.finalize_bar(TradeBar::empty(next_start));
            self.closed_bars.push(finalized);
            next_start += period_ms;
        }
    }

    fn take_closed_bars(&mut self) -> Vec<TradeBar> {
        std::mem::take(&mut self.closed_bars)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TradeDedupKey {
    symbol: String,
    trade_id: i64,
}

struct TradeDedupLru {
    window_ms: i64,
    latest_ts_ms: i64,
    entries: HashMap<TradeDedupKey, i64>,
    order: VecDeque<(TradeDedupKey, i64)>,
}

impl TradeDedupLru {
    fn new(window_ms: i64) -> Self {
        Self {
            window_ms: window_ms.max(1),
            latest_ts_ms: 0,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn is_duplicate_and_track(&mut self, symbol: &str, trade_id: i64, ts_ms: i64) -> bool {
        if trade_id == 0 {
            return false;
        }

        let ts_ms = ts_ms.max(0);
        if ts_ms > self.latest_ts_ms {
            self.latest_ts_ms = ts_ms;
        }
        self.evict_expired();

        let key = TradeDedupKey {
            symbol: symbol.to_string(),
            trade_id,
        };

        if let Some(prev_ts) = self.entries.get(&key).copied() {
            if ts_ms > prev_ts {
                self.entries.insert(key.clone(), ts_ms);
                self.order.push_back((key, ts_ms));
            }
            return true;
        }

        self.entries.insert(key.clone(), ts_ms);
        self.order.push_back((key, ts_ms));
        false
    }

    fn evict_expired(&mut self) {
        let cutoff = self.latest_ts_ms.saturating_sub(self.window_ms);
        while let Some((key, seen_ts)) = self.order.front() {
            if *seen_ts > cutoff {
                break;
            }
            let key = key.clone();
            let seen_ts = *seen_ts;
            self.order.pop_front();
            if self.entries.get(&key).copied() == Some(seen_ts) {
                self.entries.remove(&key);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct AmountThresholdJsonEntry {
    symbol: Option<String>,
    medium_notional_threshold: f64,
    large_notional_threshold: f64,
}

struct AmountThresholdRedisStore {
    settings: RedisSettings,
    client: redis::Client,
    conn: Option<redis::Connection>,
    last_warn: Instant,
}

impl AmountThresholdRedisStore {
    fn new(settings: RedisSettings) -> Result<Self> {
        let url = settings.connection_url();
        let client = redis::Client::open(url)?;
        Ok(Self {
            settings,
            client,
            conn: None,
            last_warn: Instant::now() - Duration::from_secs(REDIS_WARN_INTERVAL_SECS),
        })
    }

    fn load_thresholds_for_venue(
        &mut self,
        venue_slug: &str,
        venue: TradingVenue,
    ) -> Result<HashMap<String, AmountThreshold>> {
        let prefix_owned = self.settings.prefix.clone().filter(|p| !p.is_empty());
        let prefix = prefix_owned.as_deref();
        let redis_key = amount_threshold_hash_key(venue_slug);
        let full_key = with_prefix(prefix, &redis_key);

        self.ensure_connected()?;
        let Some(conn) = self.conn.as_mut() else {
            anyhow::bail!("redis connection unavailable");
        };

        let mut out = HashMap::new();
        let hash_map: HashMap<String, String> = conn
            .hgetall(full_key.clone())
            .with_context(|| format!("redis HGETALL failed for key={}", full_key))?;

        for (raw_symbol, raw_json) in hash_map {
            let symbol = normalize_symbol_for_venue(raw_symbol.trim(), venue);
            if symbol.is_empty() {
                continue;
            }

            let parsed_entries = parse_threshold_entries(&raw_json)
                .with_context(|| format!("parse threshold json failed: key={} field={}", redis_key, raw_symbol))?;

            for entry in parsed_entries {
                let entry_symbol = entry
                    .symbol
                    .as_ref()
                    .map(|s| normalize_symbol_for_venue(s.trim(), venue))
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| symbol.clone());

                if !entry.medium_notional_threshold.is_finite()
                    || !entry.large_notional_threshold.is_finite()
                {
                    continue;
                }
                if entry.medium_notional_threshold <= 0.0
                    || entry.large_notional_threshold <= 0.0
                    || entry.medium_notional_threshold > entry.large_notional_threshold
                {
                    continue;
                }

                out.insert(
                    entry_symbol,
                    AmountThreshold {
                        medium_notional_threshold: entry.medium_notional_threshold,
                        large_notional_threshold: entry.large_notional_threshold,
                    },
                );
            }
        }

        Ok(out)
    }

    fn ensure_connected(&mut self) -> Result<()> {
        if self.conn.is_some() {
            return Ok(());
        }
        let conn = self.client.get_connection()?;
        self.conn = Some(conn);
        Ok(())
    }

    fn warn_throttled(&mut self, msg: &str) {
        if self.last_warn.elapsed() >= Duration::from_secs(REDIS_WARN_INTERVAL_SECS) {
            warn!("{}", msg);
            self.last_warn = Instant::now();
        }
    }
}

#[derive(Debug, Clone)]
struct PersistenceRuntime {
    rocksdb_path: String,
    retention_hours: u64,
}

impl PersistenceRuntime {
    fn from_config(cfg: &PersistenceConfig) -> Self {
        Self {
            rocksdb_path: cfg.rocksdb_path.trim().to_string(),
            retention_hours: cfg.retention_hours,
        }
    }

    fn enabled(&self) -> bool {
        self.retention_hours > 0
    }
}

struct TradeFlowFeatureRocksDbStore {
    db: DB,
    cf_opts: Options,
    known_cf_names: HashSet<String>,
    sync_writes: bool,
}

impl TradeFlowFeatureRocksDbStore {
    fn open(path: &str, venue_slug: &str, symbols: &HashSet<String>) -> Result<Self> {
        let path_ref = Path::new(path);
        if let Some(parent) = path_ref.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("create rocksdb parent dir failed: {}", parent.display())
                })?;
            }
        }

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_compression_type(DBCompressionType::Lz4);

        let mut cf_opts = Options::default();
        cf_opts.set_compression_type(DBCompressionType::Lz4);

        let mut cf_names: HashSet<String> = HashSet::new();
        cf_names.insert("default".to_string());
        if path_ref.exists() {
            match DB::list_cf(&db_opts, path_ref) {
                Ok(existing) => {
                    for name in existing {
                        cf_names.insert(name);
                    }
                }
                Err(err) => {
                    warn!(
                        "list rocksdb cfs failed (continue with requested only): path={} err={}",
                        path_ref.display(),
                        err
                    );
                }
            }
        }
        for symbol in symbols {
            cf_names.insert(cf_name_for_symbol(venue_slug, symbol));
        }

        let mut cf_names_sorted: Vec<String> = cf_names.into_iter().collect();
        cf_names_sorted.sort_unstable();
        let descriptors: Vec<ColumnFamilyDescriptor> = cf_names_sorted
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name.clone(), cf_opts.clone()))
            .collect();

        let db = DB::open_cf_descriptors(&db_opts, path_ref, descriptors)
            .with_context(|| format!("open rocksdb failed: {}", path_ref.display()))?;

        let mut known_cf_names = HashSet::new();
        for name in cf_names_sorted {
            known_cf_names.insert(name);
        }

        Ok(Self {
            db,
            cf_opts,
            known_cf_names,
            sync_writes: false,
        })
    }

    fn ensure_symbol_cfs(&mut self, venue_slug: &str, symbols: &HashSet<String>) -> Result<()> {
        for symbol in symbols {
            let cf_name = cf_name_for_symbol(venue_slug, symbol);
            self.ensure_cf(&cf_name)?;
        }
        Ok(())
    }

    fn put_feature(
        &mut self,
        venue_slug: &str,
        symbol: &str,
        ts_ms: i64,
        payload: &[u8],
    ) -> Result<()> {
        let cf_name = cf_name_for_symbol(venue_slug, symbol);
        self.ensure_cf(&cf_name)?;

        let Some(cf) = self.db.cf_handle(&cf_name) else {
            anyhow::bail!("rocksdb cf missing after ensure: {}", cf_name);
        };
        let key = encode_ts_key(ts_ms);
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(cf, key, payload, &write_opts)
            .with_context(|| {
                format!(
                    "rocksdb put failed: cf={} symbol={} ts_ms={}",
                    cf_name, symbol, ts_ms
                )
            })?;
        Ok(())
    }

    fn cleanup_symbols_for_venue(
        &mut self,
        venue_slug: &str,
        symbols: &HashSet<String>,
    ) -> Result<usize> {
        if symbols.is_empty() {
            return Ok(0);
        }

        let mut dropped_cfs = 0usize;

        let mut sorted_symbols: Vec<String> = symbols.iter().cloned().collect();
        sorted_symbols.sort_unstable();

        for symbol in sorted_symbols {
            let cf_name = cf_name_for_symbol(venue_slug, &symbol);
            if !self.known_cf_names.contains(&cf_name) || self.db.cf_handle(&cf_name).is_none() {
                continue;
            }

            self.db.drop_cf(&cf_name).with_context(|| {
                format!(
                    "rocksdb retired symbol drop cf failed: cf={} symbol={}",
                    cf_name, symbol
                )
            })?;
            self.known_cf_names.remove(&cf_name);
            dropped_cfs += 1;
        }

        Ok(dropped_cfs)
    }

    fn cleanup_before_for_venue(&self, venue_slug: &str, cutoff_ms: i64) -> Result<(usize, usize)> {
        if cutoff_ms <= 0 {
            return Ok((0, 0));
        }

        let start_key = [0u8; 8];
        let end_key = encode_ts_key(cutoff_ms);
        if end_key <= start_key {
            return Ok((0, 0));
        }

        let mut touched_cfs = 0usize;
        let mut deleted_ranges = 0usize;
        let mut cf_names: Vec<String> = self.known_cf_names.iter().cloned().collect();
        cf_names.sort_unstable();

        for cf_name in cf_names {
            if cf_name == "default" || !is_trade_flow_feature_cf_for_venue(&cf_name, venue_slug) {
                continue;
            }
            let Some(cf) = self.db.cf_handle(&cf_name) else {
                continue;
            };

            let mut iter = self
                .db
                .iterator_cf(cf, IteratorMode::From(&start_key, Direction::Forward));
            let Some(first_item) = iter.next() else {
                continue;
            };
            let (first_key, _) = first_item
                .with_context(|| format!("rocksdb iterator failed while cleanup cf={}", cf_name))?;
            if first_key.as_ref() >= end_key.as_slice() {
                continue;
            }

            self.db
                .delete_range_cf(cf, &start_key, &end_key)
                .with_context(|| {
                    format!(
                        "rocksdb delete_range failed: cf={} cutoff_ms={}",
                        cf_name, cutoff_ms
                    )
                })?;
            touched_cfs += 1;
            deleted_ranges += 1;
        }

        Ok((touched_cfs, deleted_ranges))
    }

    fn ensure_cf(&mut self, cf_name: &str) -> Result<()> {
        if self.known_cf_names.contains(cf_name) {
            return Ok(());
        }
        self.db
            .create_cf(cf_name, &self.cf_opts)
            .with_context(|| format!("create rocksdb cf failed: {}", cf_name))?;
        self.known_cf_names.insert(cf_name.to_string());
        Ok(())
    }
}

fn with_prefix(prefix: Option<&str>, key: &str) -> String {
    match prefix {
        Some(prefix) => format!("{}{}", prefix, key),
        None => key.to_string(),
    }
}

pub struct TradeFlowFeaturePubApp {
    venue_slug: String,
    venue_u8: u8,
    venue: TradingVenue,
    heartbeat_symbol: String,
    config_path: String,
    config: TradeFlowFeaturePubConfig,
    depth_channel: DepthChannel,
    trade_subscriber: Subscriber<ipc::Service, [u8; TRADE_MAX_BYTES], ()>,
    depth_subscriber: DepthSubscriber,
    publisher: TradeFlowFeaturePublisher,
    symbols: HashMap<String, SymbolState>,
    latest_depth_by_symbol: HashMap<String, DepthSnapshot>,
    thresholds: HashMap<String, AmountThreshold>,
    online_symbols: HashSet<String>,
    threshold_store: AmountThresholdRedisStore,
    persistence: PersistenceRuntime,
    rocksdb_store: Option<TradeFlowFeatureRocksDbStore>,
    rocksdb_open_path: Option<String>,
    last_rocksdb_warn: Instant,
    last_missing_depth_warn: Instant,
    missing_depth_drop_count: u64,
    recv_trade_raw_count: u64,
    recv_trade_parse_ok_count: u64,
    recv_trade_parse_fail_count: u64,
    trade_filtered_offline_count: u64,
    trade_threshold_miss_count: u64,
    trade_dedup_drop_count: u64,
    trade_late_count: u64,
    publish_success_count: u64,
    publish_fail_invalid_count: u64,
    publish_fail_missing_depth_count: u64,
    publish_fail_send_count: u64,
    last_publish_outcome_log: Instant,
    last_threshold_reload: Instant,
    threshold_reload_interval: Duration,
    cleanup_interval: Duration,
    last_cleanup: Instant,
    last_retired_symbols: usize,
    trade_dedup_lru: TradeDedupLru,
    timer_check_interval: Duration,
    last_timer_check: Instant,
}

impl TradeFlowFeaturePubApp {
    pub fn new(
        config_path: &str,
        venue_slug: &str,
        venue_u8: u8,
        venue: TradingVenue,
    ) -> Result<Self> {
        let config = TradeFlowFeaturePubConfig::load(config_path)?;
        let depth_channel = DepthChannel::from_cfg(config.data_source.depth_channel.as_str())?;

        let trade_subscriber = Self::create_trade_subscriber(venue_slug)?;
        let depth_subscriber = Self::create_depth_subscriber(venue_slug, depth_channel)?;
        let publisher = TradeFlowFeaturePublisher::new(venue_slug)?;
        let threshold_store = AmountThresholdRedisStore::new(fixed_redis_settings())?;
        let threshold_reload_interval = Duration::from_secs(config.runtime.threshold_reload_secs);
        let cleanup_interval = Duration::from_secs(PERSISTENCE_CLEANUP_INTERVAL_SECS);
        let persistence = PersistenceRuntime::from_config(&config.persistence);
        let heartbeat_symbol = normalize_symbol_for_venue("BTCUSDT", venue);

        let mut app = Self {
            venue_slug: venue_slug.to_string(),
            venue_u8,
            venue,
            heartbeat_symbol,
            config_path: config_path.to_string(),
            config,
            depth_channel,
            trade_subscriber,
            depth_subscriber,
            publisher,
            symbols: HashMap::new(),
            latest_depth_by_symbol: HashMap::new(),
            thresholds: HashMap::new(),
            online_symbols: HashSet::new(),
            threshold_store,
            persistence,
            rocksdb_store: None,
            rocksdb_open_path: None,
            last_rocksdb_warn: Instant::now() - Duration::from_secs(ROCKSDB_WARN_INTERVAL_SECS),
            last_missing_depth_warn: Instant::now()
                - Duration::from_secs(MISSING_DEPTH_WARN_INTERVAL_SECS),
            missing_depth_drop_count: 0,
            recv_trade_raw_count: 0,
            recv_trade_parse_ok_count: 0,
            recv_trade_parse_fail_count: 0,
            trade_filtered_offline_count: 0,
            trade_threshold_miss_count: 0,
            trade_dedup_drop_count: 0,
            trade_late_count: 0,
            publish_success_count: 0,
            publish_fail_invalid_count: 0,
            publish_fail_missing_depth_count: 0,
            publish_fail_send_count: 0,
            last_publish_outcome_log: Instant::now(),
            last_threshold_reload: Instant::now() - threshold_reload_interval,
            threshold_reload_interval,
            cleanup_interval,
            // Avoid heavy cleanup right after startup; run it on its own cadence.
            last_cleanup: Instant::now(),
            last_retired_symbols: 0,
            trade_dedup_lru: TradeDedupLru::new(TRADE_DEDUP_WINDOW_MS),
            timer_check_interval: Duration::from_micros(TIMER_CHECK_INTERVAL_MICROS),
            last_timer_check: Instant::now(),
        };
        app.ensure_persistence_ready(true);
        app.reload_thresholds(true);
        Ok(app)
    }

    fn create_trade_subscriber(
        venue: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; TRADE_MAX_BYTES], ()>> {
        let node_name = format!(
            "factor_sub_{}_trade_flow_feature_trade",
            venue.replace('-', "_")
        );
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_name = format!("dat_pbs/{}/{}", venue, FIXED_TRADE_CHANNEL);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; TRADE_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        info!("Subscribed to trade channel: {}", service_name);
        Ok(subscriber)
    }

    fn create_depth_subscriber(venue: &str, channel: DepthChannel) -> Result<DepthSubscriber> {
        let node_name = format!(
            "factor_sub_{}_trade_flow_feature_{}",
            venue.replace('-', "_"),
            channel.as_str()
        );
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_name = format!("depth_pubs/{}/{}", venue, channel.as_str());
        let depth_subscriber = match channel {
            DepthChannel::Depth25 => {
                let service = node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; DEPTH25_MAX_BYTES]>()
                    .open()?;
                DepthSubscriber::Depth25(service.subscriber_builder().create()?)
            }
            DepthChannel::Depth50 => {
                let service = node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; DEPTH50_MAX_BYTES]>()
                    .open()?;
                DepthSubscriber::Depth50(service.subscriber_builder().create()?)
            }
        };

        info!("Subscribed to depth channel: {}", service_name);
        Ok(depth_subscriber)
    }

    pub fn run(&mut self) -> Result<()> {
        info!(
            "TradeFlowFeaturePubApp[{}] started: trade_channel={} depth_channel={} bar_ms={} threshold_reload_secs={} redis={}:{} db={} persist_path={} persist_hours={} persist_symbols={}",
            self.venue_slug,
            FIXED_TRADE_CHANNEL,
            self.depth_channel.as_str(),
            self.config.runtime.bar_ms,
            self.config.runtime.threshold_reload_secs,
            self.threshold_store.settings.host,
            self.threshold_store.settings.port,
            self.threshold_store.settings.db,
            self.persistence.rocksdb_path,
            self.persistence.retention_hours,
            self.online_symbols.len()
        );

        loop {
            self.maybe_reload_runtime();
            let mut has_message = false;

            while let Some((symbol, depth)) = self.depth_subscriber.receive_snapshot(self.venue)? {
                has_message = true;
                if !self.online_symbols.contains(&symbol) {
                    continue;
                }
                self.latest_depth_by_symbol.insert(symbol, depth);
            }

            while let Some(sample) = self.trade_subscriber.receive()? {
                has_message = true;
                self.recv_trade_raw_count = self.recv_trade_raw_count.saturating_add(1);
                if let Some(trade) = parse_trade(sample.payload(), self.venue) {
                    self.recv_trade_parse_ok_count =
                        self.recv_trade_parse_ok_count.saturating_add(1);
                    self.handle_trade(trade);
                } else {
                    self.recv_trade_parse_fail_count =
                        self.recv_trade_parse_fail_count.saturating_add(1);
                }
            }

            self.maybe_close_due_bars()?;
            if !has_message {
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }
        }
    }

    fn handle_trade(&mut self, trade: TradeTick) {
        if !self.online_symbols.contains(&trade.symbol) {
            self.trade_filtered_offline_count = self.trade_filtered_offline_count.saturating_add(1);
            return;
        }
        let Some(threshold) = self.thresholds.get(&trade.symbol).copied() else {
            self.trade_threshold_miss_count = self.trade_threshold_miss_count.saturating_add(1);
            return;
        };

        if self.trade_dedup_lru.is_duplicate_and_track(
            &trade.symbol,
            trade.trade_id,
            trade.timestamp_ms,
        ) {
            self.trade_dedup_drop_count = self.trade_dedup_drop_count.saturating_add(1);
            return;
        }
        let state = self
            .symbols
            .entry(trade.symbol.clone())
            .or_insert_with(SymbolState::new);
        let late_trade = state.apply_trade(&trade, &self.config.runtime, threshold);
        if late_trade {
            self.trade_late_count = self.trade_late_count.saturating_add(1);
        }
    }

    fn maybe_close_due_bars(&mut self) -> Result<()> {
        self.log_publish_outcome_10s();
        if self.last_timer_check.elapsed() < self.timer_check_interval {
            return Ok(());
        }

        self.last_timer_check = Instant::now();
        self.close_due_bars(now_millis())
    }

    fn close_due_bars(&mut self, now_ms: i64) -> Result<()> {
        let mut to_publish = Vec::new();
        for (symbol, state) in self.symbols.iter_mut() {
            state.close_due_bars(now_ms, self.config.runtime.bar_ms);
            let closed = state.take_closed_bars();
            if !closed.is_empty() {
                to_publish.push((symbol.clone(), closed));
            }
        }

        for (symbol, bars) in to_publish {
            self.process_closed_bars(&symbol, bars)?;
        }
        Ok(())
    }

    fn process_closed_bars(&mut self, symbol: &str, bars: Vec<TradeBar>) -> Result<()> {
        for bar in bars {
            if !bar.has_valid_ffill_fields() {
                self.publish_fail_invalid_count = self.publish_fail_invalid_count.saturating_add(1);
                continue;
            }
            let symbol_norm = normalize_symbol_for_venue(symbol, self.venue);
            let mut feature_values =
                Vec::with_capacity(TRADE_FLOW_FEATURE_DIM + APPENDED_DEPTH_DIM);
            feature_values.extend_from_slice(&bar.to_feature_values());
            let Some(depth_values) = self.appended_depth_values(&symbol_norm) else {
                self.publish_fail_missing_depth_count =
                    self.publish_fail_missing_depth_count.saturating_add(1);
                self.warn_missing_depth_throttled(&symbol_norm, bar.start_ms);
                continue;
            };
            feature_values.extend_from_slice(&depth_values);
            let msg = TradeFlowFeatureMsg::from_indexed_values(
                symbol_norm.clone(),
                self.venue_u8,
                bar.start_ms,
                &feature_values,
            )?;
            let payload = msg.to_bytes()?;

            self.maybe_persist_feature_payload(&symbol_norm, bar.start_ms, payload.as_ref());

            if !self.publisher.publish(payload.as_ref(), &symbol_norm) {
                self.publish_fail_send_count = self.publish_fail_send_count.saturating_add(1);
                warn!(
                    "failed to publish trade_flow_feature: venue={} symbol={} ts={}",
                    self.venue_slug, symbol_norm, bar.start_ms
                );
            } else {
                self.publish_success_count = self.publish_success_count.saturating_add(1);
            }
        }
        Ok(())
    }

    fn log_publish_outcome_10s(&mut self) {
        if self.last_publish_outcome_log.elapsed()
            < Duration::from_secs(PUBLISH_OUTCOME_LOG_INTERVAL_SECS)
        {
            return;
        }

        let fail_total = self
            .publish_fail_invalid_count
            .saturating_add(self.publish_fail_missing_depth_count)
            .saturating_add(self.publish_fail_send_count);
        info!(
            "TradeFlowFeaturePubApp[{}] publish_outcome_10s: raw_trade_in={} trade_parse_ok={} trade_parse_fail={} trade_filtered_offline={} trade_threshold_miss={} trade_dedup_drop={} trade_late={} success={} fail_total={} fail_invalid={} fail_missing_depth={} fail_send={}",
            self.venue_slug,
            self.recv_trade_raw_count,
            self.recv_trade_parse_ok_count,
            self.recv_trade_parse_fail_count,
            self.trade_filtered_offline_count,
            self.trade_threshold_miss_count,
            self.trade_dedup_drop_count,
            self.trade_late_count,
            self.publish_success_count,
            fail_total,
            self.publish_fail_invalid_count,
            self.publish_fail_missing_depth_count,
            self.publish_fail_send_count
        );

        self.last_publish_outcome_log = Instant::now();
        self.recv_trade_raw_count = 0;
        self.recv_trade_parse_ok_count = 0;
        self.recv_trade_parse_fail_count = 0;
        self.trade_filtered_offline_count = 0;
        self.trade_threshold_miss_count = 0;
        self.trade_dedup_drop_count = 0;
        self.trade_late_count = 0;
        self.publish_success_count = 0;
        self.publish_fail_invalid_count = 0;
        self.publish_fail_missing_depth_count = 0;
        self.publish_fail_send_count = 0;
    }

    fn appended_depth_values(&self, symbol: &str) -> Option<[f64; APPENDED_DEPTH_DIM]> {
        let depth = self.latest_depth_by_symbol.get(symbol)?;
        if depth.bids.first().map(|level| level.price).unwrap_or(0.0) <= 0.0
            || depth.asks.first().map(|level| level.price).unwrap_or(0.0) <= 0.0
        {
            return None;
        }

        let mut out = [0.0f64; APPENDED_DEPTH_DIM];

        let mut offset = 0usize;
        for level in depth.bids.iter().take(MAX_DEPTH_LEVELS_CACHE) {
            out[offset] = level.price;
            out[offset + 1] = level.amount;
            offset += 2;
        }

        offset = MAX_DEPTH_LEVELS_CACHE * 2;
        for level in depth.asks.iter().take(MAX_DEPTH_LEVELS_CACHE) {
            out[offset] = level.price;
            out[offset + 1] = level.amount;
            offset += 2;
        }

        Some(out)
    }

    fn maybe_reload_runtime(&mut self) {
        if self.last_threshold_reload.elapsed() < self.threshold_reload_interval {
            return;
        }
        self.last_threshold_reload = Instant::now();
        self.reload_runtime_config();
        self.reload_thresholds(false);
        self.maybe_cleanup_persistence();
    }

    fn reload_runtime_config(&mut self) {
        let loaded = match TradeFlowFeaturePubConfig::load(&self.config_path) {
            Ok(cfg) => cfg,
            Err(err) => {
                warn!("trade_flow_feature config reload failed: {}", err);
                return;
            }
        };

        if loaded.runtime.bar_ms != self.config.runtime.bar_ms {
            warn!(
                "trade_flow_feature config change ignored: bar_ms '{}' -> '{}' (requires restart)",
                self.config.runtime.bar_ms, loaded.runtime.bar_ms
            );
        }

        if loaded.runtime.threshold_reload_secs != self.config.runtime.threshold_reload_secs {
            self.config.runtime.threshold_reload_secs = loaded.runtime.threshold_reload_secs;
            self.threshold_reload_interval =
                Duration::from_secs(self.config.runtime.threshold_reload_secs);
            info!(
                "trade_flow_feature threshold reload interval updated: {}s",
                self.config.runtime.threshold_reload_secs
            );
        }

        if loaded.data_source.depth_channel != self.config.data_source.depth_channel {
            warn!(
                "trade_flow_feature config change ignored: depth_channel '{}' -> '{}' (requires restart)",
                self.config.data_source.depth_channel, loaded.data_source.depth_channel
            );
        }

        self.apply_persistence_config(&loaded.persistence, false);
    }

    fn apply_persistence_config(&mut self, cfg: &PersistenceConfig, init: bool) {
        let next = PersistenceRuntime::from_config(cfg);
        let changed = init
            || self.persistence.rocksdb_path != next.rocksdb_path
            || self.persistence.retention_hours != next.retention_hours;

        self.config.persistence = cfg.clone();
        if !changed {
            return;
        }

        self.persistence = next;
        self.ensure_persistence_ready(init);
    }

    fn ensure_persistence_ready(&mut self, init: bool) {
        if !self.persistence.enabled() {
            if self.rocksdb_store.take().is_some() {
                self.rocksdb_open_path = None;
                info!(
                    "trade_flow_feature persistence disabled: venue={} retention_hours={} symbols={}",
                    self.venue_slug,
                    self.persistence.retention_hours,
                    self.online_symbols.len()
                );
            } else if init {
                info!(
                    "trade_flow_feature persistence disabled on startup: venue={} retention_hours={} symbols={}",
                    self.venue_slug,
                    self.persistence.retention_hours,
                    self.online_symbols.len()
                );
            }
            return;
        }

        let target_path = self.persistence.rocksdb_path.clone();
        let need_open = self.rocksdb_store.is_none()
            || self.rocksdb_open_path.as_deref() != Some(target_path.as_str());
        if need_open {
            match TradeFlowFeatureRocksDbStore::open(
                &target_path,
                &self.venue_slug,
                &self.online_symbols,
            ) {
                Ok(store) => {
                    self.rocksdb_store = Some(store);
                    self.rocksdb_open_path = Some(target_path.clone());
                    info!(
                        "trade_flow_feature rocksdb {}opened: venue={} path={} retention_hours={} symbols={}",
                        if init { "" } else { "re" },
                        self.venue_slug,
                        target_path,
                        self.persistence.retention_hours,
                        self.online_symbols.len()
                    );
                }
                Err(err) => {
                    self.warn_rocksdb_throttled(&format!(
                        "trade_flow_feature rocksdb open failed: venue={} path={} err={:#}",
                        self.venue_slug, target_path, err
                    ));
                    self.rocksdb_store = None;
                    self.rocksdb_open_path = None;
                    return;
                }
            }
        }

        if let Some(store) = self.rocksdb_store.as_mut() {
            if let Err(err) = store.ensure_symbol_cfs(&self.venue_slug, &self.online_symbols) {
                self.warn_rocksdb_throttled(&format!(
                    "trade_flow_feature rocksdb ensure cfs failed: venue={} path={} err={:#}",
                    self.venue_slug, self.persistence.rocksdb_path, err
                ));
            }
        }
    }

    fn maybe_persist_feature_payload(&mut self, symbol: &str, ts_ms: i64, payload: &[u8]) {
        if !self.persistence.enabled() || !self.online_symbols.contains(symbol) {
            return;
        }
        if self.rocksdb_store.is_none() {
            self.ensure_persistence_ready(false);
        }
        let Some(store) = self.rocksdb_store.as_mut() else {
            return;
        };

        if let Err(err) = store.put_feature(&self.venue_slug, symbol, ts_ms, payload) {
            self.warn_rocksdb_throttled(&format!(
                "trade_flow_feature rocksdb put failed: venue={} symbol={} ts={} err={:#}",
                self.venue_slug, symbol, ts_ms, err
            ));
        }
    }

    fn maybe_cleanup_persistence(&mut self) {
        if self.last_cleanup.elapsed() < self.cleanup_interval {
            return;
        }
        self.last_cleanup = Instant::now();

        if !self.persistence.enabled() {
            return;
        }
        let Some(store) = self.rocksdb_store.as_ref() else {
            return;
        };

        let retention_ms = (self.persistence.retention_hours as i128) * 3_600_000i128;
        if retention_ms <= 0 {
            return;
        }
        let retention_ms = retention_ms.min(i64::MAX as i128) as i64;
        let cutoff_ms = now_millis().saturating_sub(retention_ms);
        if cutoff_ms <= 0 {
            return;
        }

        let started = Instant::now();
        match store.cleanup_before_for_venue(&self.venue_slug, cutoff_ms) {
            Ok((touched_cfs, deleted_ranges)) => {
                info!(
                    "trade_flow_feature rocksdb rolling cleanup: venue={} cutoff_ms={} retention_hours={} elapsed_ms={} cfs={} ranges={}",
                    self.venue_slug,
                    cutoff_ms,
                    self.persistence.retention_hours,
                    started.elapsed().as_millis(),
                    touched_cfs,
                    deleted_ranges
                );
            }
            Err(err) => {
                self.warn_rocksdb_throttled(&format!(
                    "trade_flow_feature rocksdb cleanup failed: venue={} cutoff_ms={} elapsed_ms={} err={:#}",
                    self.venue_slug,
                    cutoff_ms,
                    started.elapsed().as_millis(),
                    err
                ));
            }
        }
    }

    fn maybe_cleanup_retired_symbols(&mut self, retired_symbols: &HashSet<String>) {
        if retired_symbols.is_empty() || !self.persistence.enabled() {
            return;
        }

        if self.rocksdb_store.is_none() {
            self.ensure_persistence_ready(false);
        }

        let result = match self.rocksdb_store.as_mut() {
            Some(store) => store.cleanup_symbols_for_venue(&self.venue_slug, retired_symbols),
            None => return,
        };

        match result {
            Ok(dropped_cfs) => {
                if dropped_cfs > 0 {
                    info!(
                        "trade_flow_feature retired symbol cleanup: venue={} retired_symbols={} dropped_cfs={}",
                        self.venue_slug,
                        retired_symbols.len(),
                        dropped_cfs
                    );
                }
            }
            Err(err) => {
                self.warn_rocksdb_throttled(&format!(
                    "trade_flow_feature retired symbol cleanup failed: venue={} retired_symbols={} err={:#}",
                    self.venue_slug,
                    retired_symbols.len(),
                    err
                ));
            }
        }
    }

    fn reload_thresholds(&mut self, init: bool) {
        match self
            .threshold_store
            .load_thresholds_for_venue(&self.venue_slug, self.venue)
        {
            Ok(new_map) => {
                if !new_map.contains_key(&self.heartbeat_symbol) {
                    panic!(
                        "trade_flow_feature heartbeat threshold missing: venue={} symbol={} key='{}'",
                        self.venue_slug,
                        self.heartbeat_symbol,
                        amount_threshold_hash_key(&self.venue_slug)
                    );
                }

                let mut symbols = HashSet::with_capacity(new_map.len());
                for symbol in new_map.keys() {
                    symbols.insert(symbol.clone());
                }

                let retired_symbols: HashSet<String> =
                    self.online_symbols.difference(&symbols).cloned().collect();
                self.last_retired_symbols = retired_symbols.len();
                self.thresholds = new_map;
                self.online_symbols = symbols;
                self.symbols
                    .retain(|symbol, _| self.online_symbols.contains(symbol));
                self.latest_depth_by_symbol
                    .retain(|symbol, _| self.online_symbols.contains(symbol));

                info!(
                    "trade_flow_feature symbols {}loaded: venue={} online_symbols={} retired_symbols={}",
                    if init { "" } else { "re" },
                    self.venue_slug,
                    self.online_symbols.len(),
                    self.last_retired_symbols
                );

                self.maybe_cleanup_retired_symbols(&retired_symbols);
                self.ensure_persistence_ready(false);
            }
            Err(err) => {
                self.threshold_store.warn_throttled(&format!(
                    "trade_flow_feature threshold reload failed: venue={} err={:#}",
                    self.venue_slug, err
                ));
            }
        }
    }

    fn warn_rocksdb_throttled(&mut self, msg: &str) {
        if self.last_rocksdb_warn.elapsed() >= Duration::from_secs(ROCKSDB_WARN_INTERVAL_SECS) {
            warn!("{}", msg);
            self.last_rocksdb_warn = Instant::now();
        }
    }

    fn warn_missing_depth_throttled(&mut self, symbol: &str, ts_ms: i64) {
        self.missing_depth_drop_count = self.missing_depth_drop_count.saturating_add(1);
        if self.last_missing_depth_warn.elapsed()
            >= Duration::from_secs(MISSING_DEPTH_WARN_INTERVAL_SECS)
        {
            warn!(
                "trade_flow_feature skip bar due to missing depth: venue={} symbol={} ts={} dropped_since_last_warn={}",
                self.venue_slug, symbol, ts_ms, self.missing_depth_drop_count
            );
            self.last_missing_depth_warn = Instant::now();
            self.missing_depth_drop_count = 0;
        }
    }
}

fn parse_threshold_entries(raw: &str) -> Result<Vec<AmountThresholdJsonEntry>> {
    let value: Value = serde_json::from_str(raw)?;
    let mut out = Vec::new();
    collect_threshold_entries(&value, &mut out);
    Ok(out)
}

fn collect_threshold_entries(value: &Value, out: &mut Vec<AmountThresholdJsonEntry>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_threshold_entries(item, out);
            }
        }
        Value::Object(map) => {
            let medium_notional_threshold =
                map.get("medium_notional_threshold").and_then(json_as_f64);
            let large_notional_threshold =
                map.get("large_notional_threshold").and_then(json_as_f64);
            if let (Some(medium_notional_threshold), Some(large_notional_threshold)) =
                (medium_notional_threshold, large_notional_threshold)
            {
                let symbol = map
                    .get("symbol")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                out.push(AmountThresholdJsonEntry {
                    symbol,
                    medium_notional_threshold,
                    large_notional_threshold,
                });
            }

            for child in map.values() {
                if child.is_array() || child.is_object() {
                    collect_threshold_entries(child, out);
                }
            }
        }
        _ => {}
    }
}

fn json_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(num) => num.as_f64(),
        Value::String(text) => text.parse::<f64>().ok(),
        _ => None,
    }
}

fn amount_threshold_hash_key(venue_slug: &str) -> String {
    format!("{}:{}", venue_slug, AMOUNT_THRESHOLD_REDIS_KEY_SUFFIX)
}

fn fixed_redis_settings() -> RedisSettings {
    RedisSettings {
        host: FIXED_REDIS_HOST.to_string(),
        port: FIXED_REDIS_PORT,
        db: FIXED_REDIS_DB,
        username: None,
        password: None,
        prefix: None,
    }
}

fn cf_name_for_symbol(venue_slug: &str, symbol: &str) -> String {
    format!(
        "{}:{}:{}",
        venue_slug,
        symbol.to_uppercase(),
        TRADE_FLOW_FEATURE_CF_SUFFIX
    )
}

fn is_trade_flow_feature_cf_for_venue(cf_name: &str, venue_slug: &str) -> bool {
    let prefix = format!("{}:", venue_slug);
    cf_name.starts_with(&prefix) && cf_name.ends_with(TRADE_FLOW_FEATURE_CF_SUFFIX)
}

fn encode_ts_key(ts_ms: i64) -> [u8; 8] {
    let ts_u64 = if ts_ms <= 0 { 0u64 } else { ts_ms as u64 };
    ts_u64.to_be_bytes()
}

fn parse_trade(data: &[u8], venue: TradingVenue) -> Option<TradeTick> {
    if data.len() < 8 {
        return None;
    }

    let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if msg_type != MktMsgType::TradeInfo as u32 {
        return None;
    }

    let symbol_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let min_len = 8 + symbol_len + 8 + 8 + 8 + 8;
    if data.len() < min_len {
        return None;
    }

    let symbol_raw = std::str::from_utf8(&data[8..8 + symbol_len])
        .ok()?
        .to_string();
    let symbol = normalize_symbol_for_venue(&symbol_raw, venue);
    let mut offset = 8 + symbol_len;

    let trade_id = i64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);
    offset += 8;
    let timestamp_ms = i64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);
    offset += 8;

    let side = match data[offset] as char {
        'B' | 'b' => TradeSide::Buy,
        'S' | 's' => TradeSide::Sell,
        _ => return None,
    };
    offset += 8; // side + padding

    let price = f64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);
    offset += 8;

    let amount = f64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]);

    if !price.is_finite()
        || !amount.is_finite()
        || price <= 0.0
        || amount <= 0.0
        || timestamp_ms <= 0
    {
        return None;
    }

    Some(TradeTick {
        symbol,
        trade_id,
        timestamp_ms,
        side,
        price,
        amount,
    })
}

fn parse_depth_snapshot(
    data: &[u8],
    channel: DepthChannel,
    venue: TradingVenue,
) -> Option<(String, DepthSnapshot)> {
    if data.len() < 8 {
        return None;
    }

    let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if msg_type != channel.expected_msg_type() {
        return None;
    }

    let levels = channel.level_count();
    let symbol_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let min_len = 8 + symbol_len + 8 + levels * 16 * 2;
    if data.len() < min_len {
        return None;
    }

    let symbol_raw = std::str::from_utf8(&data[8..8 + symbol_len]).ok()?;
    let symbol = normalize_symbol_for_venue(symbol_raw, venue);
    if symbol.is_empty() {
        return None;
    }

    let mut bids = Vec::with_capacity(MAX_DEPTH_LEVELS_CACHE);
    let mut asks = Vec::with_capacity(MAX_DEPTH_LEVELS_CACHE);

    let mut offset = 8 + symbol_len + 8;
    for idx in 0..levels {
        let price = read_f64_at(data, offset)?;
        let amount = read_f64_at(data, offset + 8)?;
        offset += 16;
        if idx < MAX_DEPTH_LEVELS_CACHE {
            bids.push(DepthLevel { price, amount });
        }
    }

    for idx in 0..levels {
        let price = read_f64_at(data, offset)?;
        let amount = read_f64_at(data, offset + 8)?;
        offset += 16;
        if idx < MAX_DEPTH_LEVELS_CACHE {
            asks.push(DepthLevel { price, amount });
        }
    }

    Some((symbol, DepthSnapshot { bids, asks }))
}

fn read_f64_at(data: &[u8], offset: usize) -> Option<f64> {
    if data.len() < offset + 8 {
        return None;
    }

    Some(f64::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
        data[offset + 4],
        data[offset + 5],
        data[offset + 6],
        data[offset + 7],
    ]))
}

fn align_to_period(ts_ms: i64, period_ms: i64) -> i64 {
    if period_ms <= 0 {
        return ts_ms;
    }
    ts_ms - (ts_ms % period_ms)
}

fn now_millis() -> i64 {
    let Ok(duration) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return 0;
    };
    let millis = duration.as_millis();
    if millis > i64::MAX as u128 {
        i64::MAX
    } else {
        millis as i64
    }
}
