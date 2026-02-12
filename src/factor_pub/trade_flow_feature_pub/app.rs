//! Trade flow 特征应用主模块
//!
//! 订阅 trade 数据，按 bar 聚合，并输出 TradeFlowFeatureMsg

use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use redis::Commands;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use super::cfg::{RuntimeConfig, TradeFlowFeaturePubConfig};
use super::publisher::TradeFlowFeaturePublisher;
use crate::common::mkt_msg::MktMsgType;
use crate::common::redis_client::RedisSettings;
use crate::common::symbol_util::{extract_assets_from_symbol, normalize_symbol_for_venue};
use crate::common::trade_flow_feature_msg::{TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_DIM};
use crate::signal::common::TradingVenue;

const TRADE_MAX_BYTES: usize = 64;
const IDLE_SLEEP_MICROS: u64 = 200;
const TIMER_CHECK_INTERVAL_MICROS: u64 = 500;
const LOG_BASE_SYMBOLS: [&str; 3] = ["BTC", "ETH", "SOL"];
const QUANTILE_REDIS_KEY_SUFFIX: &str = "amount-quantile";
const REDIS_WARN_INTERVAL_SECS: u64 = 60;

#[derive(Debug, Clone, Copy)]
enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug)]
struct TradeTick {
    symbol: String,
    timestamp_ms: i64,
    side: TradeSide,
    price: f64,
    amount: f64,
}

#[derive(Debug, Clone, Copy)]
struct QuantileThreshold {
    low: f64,
    high: f64,
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

    fn update(&mut self, trade: &TradeTick, threshold: QuantileThreshold) {
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

        if notional >= threshold.high {
            self.large_order += notional;
            match trade.side {
                TradeSide::Buy => self.large_buy += notional,
                TradeSide::Sell => self.large_sell += notional,
            }
        } else if notional >= threshold.low {
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
        threshold: QuantileThreshold,
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

#[derive(Debug, Clone)]
struct QuantileJsonEntry {
    symbol: Option<String>,
    low: f64,
    high: f64,
}

struct QuantileRedisStore {
    settings: RedisSettings,
    client: redis::Client,
    conn: Option<redis::Connection>,
    last_warn: Instant,
}

impl QuantileRedisStore {
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

    fn load_quantiles_for_venue(
        &mut self,
        venue_slug: &str,
        venue: TradingVenue,
    ) -> Result<HashMap<String, QuantileThreshold>> {
        let prefix_owned = self.settings.prefix.clone().filter(|p| !p.is_empty());
        let prefix = prefix_owned.as_deref();
        let key_pattern = format!("{}:*:{}", venue_slug, QUANTILE_REDIS_KEY_SUFFIX);
        let full_pattern = with_prefix(prefix, &key_pattern);

        self.ensure_connected()?;
        let Some(conn) = self.conn.as_mut() else {
            anyhow::bail!("redis connection unavailable");
        };

        let keys: Vec<String> = conn
            .keys(full_pattern.clone())
            .with_context(|| format!("redis KEYS failed for pattern={}", full_pattern))?;

        let mut out = HashMap::new();
        for full_key in keys {
            let logical_key = strip_prefix_with(prefix, &full_key).to_string();
            let symbol_from_key = parse_symbol_from_quantile_key(&logical_key, venue_slug, venue);
            let raw: Option<String> = conn
                .get(full_key.clone())
                .with_context(|| format!("redis GET failed for key={}", full_key))?;
            let Some(raw_json) = raw else {
                continue;
            };

            let parsed_entries = parse_quantile_entries(&raw_json)
                .with_context(|| format!("parse quantile json failed: key={}", logical_key))?;

            for entry in parsed_entries {
                let symbol = entry
                    .symbol
                    .as_ref()
                    .map(|s| normalize_symbol_for_venue(s.trim(), venue))
                    .filter(|s| !s.is_empty())
                    .or_else(|| symbol_from_key.clone())
                    .unwrap_or_default();

                if symbol.is_empty() {
                    continue;
                }
                if !entry.low.is_finite() || !entry.high.is_finite() {
                    continue;
                }
                if entry.low <= 0.0 || entry.high <= 0.0 || entry.low > entry.high {
                    continue;
                }

                out.insert(
                    symbol,
                    QuantileThreshold {
                        low: entry.low,
                        high: entry.high,
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

fn with_prefix(prefix: Option<&str>, key: &str) -> String {
    match prefix {
        Some(prefix) => format!("{}{}", prefix, key),
        None => key.to_string(),
    }
}

fn strip_prefix_with<'a>(prefix: Option<&str>, full_key: &'a str) -> &'a str {
    match prefix {
        Some(prefix) if full_key.starts_with(prefix) => &full_key[prefix.len()..],
        _ => full_key,
    }
}

pub struct TradeFlowFeaturePubApp {
    venue_slug: String,
    venue_u8: u8,
    venue: TradingVenue,
    config: TradeFlowFeaturePubConfig,
    subscriber: Subscriber<ipc::Service, [u8; TRADE_MAX_BYTES], ()>,
    publisher: TradeFlowFeaturePublisher,
    symbols: HashMap<String, SymbolState>,
    quantiles: HashMap<String, QuantileThreshold>,
    online_symbols: HashSet<String>,
    quantile_store: QuantileRedisStore,
    last_quantile_reload: Instant,
    quantile_reload_interval: Duration,
    trade_count: u64,
    late_trade_count: u64,
    last_log_stats: Instant,
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

        let subscriber = Self::create_subscriber(venue_slug, &config.data_source.trade_channel)?;
        let publisher = TradeFlowFeaturePublisher::new(venue_slug)?;
        let quantile_store = QuantileRedisStore::new(config.redis.clone())?;
        let quantile_reload_interval = Duration::from_secs(config.runtime.quantile_reload_secs);

        let mut app = Self {
            venue_slug: venue_slug.to_string(),
            venue_u8,
            venue,
            config,
            subscriber,
            publisher,
            symbols: HashMap::new(),
            quantiles: HashMap::new(),
            online_symbols: HashSet::new(),
            quantile_store,
            last_quantile_reload: Instant::now() - quantile_reload_interval,
            quantile_reload_interval,
            trade_count: 0,
            late_trade_count: 0,
            last_log_stats: Instant::now(),
            timer_check_interval: Duration::from_micros(TIMER_CHECK_INTERVAL_MICROS),
            last_timer_check: Instant::now(),
        };
        app.reload_quantiles(true);
        Ok(app)
    }

    fn create_subscriber(
        venue: &str,
        channel: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; TRADE_MAX_BYTES], ()>> {
        let node_name = format!("factor_sub_{}_trade_flow_feature", venue.replace('-', "_"));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service_name = format!("dat_pbs/{}/{}", venue, channel);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; TRADE_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        info!("Subscribed to trade channel: {}", service_name);
        Ok(subscriber)
    }

    pub fn run(&mut self) -> Result<()> {
        info!(
            "TradeFlowFeaturePubApp[{}] started: channel={} bar_ms={} quantile_reload_secs={} redis={}:{} db={}",
            self.venue_slug,
            self.config.data_source.trade_channel,
            self.config.runtime.bar_ms,
            self.config.runtime.quantile_reload_secs,
            self.config.redis.host,
            self.config.redis.port,
            self.config.redis.db
        );

        loop {
            self.maybe_reload_quantiles();
            let mut has_message = false;

            while let Some(sample) = self.subscriber.receive()? {
                has_message = true;
                let data = sample.payload().to_vec();
                if let Some(trade) = parse_trade(&data, self.venue) {
                    self.handle_trade(trade);
                    self.maybe_close_due_bars()?;
                }
            }

            if !has_message {
                self.maybe_close_due_bars()?;
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }

            if self.last_log_stats.elapsed() >= Duration::from_secs(60) {
                info!(
                    "TradeFlowFeaturePubApp[{}] stats: online_symbols={} trades={} late_trades={}",
                    self.venue_slug,
                    self.online_symbols.len(),
                    self.trade_count,
                    self.late_trade_count
                );
                self.publisher.log_stats();
                self.trade_count = 0;
                self.late_trade_count = 0;
                self.last_log_stats = Instant::now();
            }
        }
    }

    fn handle_trade(&mut self, trade: TradeTick) {
        if !self.online_symbols.contains(&trade.symbol) {
            return;
        }
        let Some(threshold) = self.quantiles.get(&trade.symbol).copied() else {
            return;
        };

        self.trade_count = self.trade_count.saturating_add(1);

        let late_trade = {
            let state = self
                .symbols
                .entry(trade.symbol.clone())
                .or_insert_with(SymbolState::new);
            state.apply_trade(&trade, &self.config.runtime, threshold)
        };

        if late_trade {
            self.late_trade_count = self.late_trade_count.saturating_add(1);
        }
    }

    fn maybe_close_due_bars(&mut self) -> Result<()> {
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
                continue;
            }
            let symbol_norm = normalize_symbol_for_venue(symbol, self.venue);
            let msg = TradeFlowFeatureMsg::from_indexed_values(
                symbol_norm.clone(),
                self.venue_u8,
                bar.start_ms,
                &bar.to_feature_values(),
            )?;

            if should_log_symbol(&symbol_norm) {
                info!(
                    "trade-flow-feature: venue={} symbol={} ts={} close={} amount={} net_buy_amount={} net_buy_pct={}",
                    self.venue_slug,
                    symbol_norm,
                    bar.start_ms,
                    bar.close,
                    bar.amount,
                    bar.net_buy_amount,
                    bar.net_buy_pct
                );
            }

            if !self.publisher.publish(&msg) {
                warn!(
                    "failed to publish trade_flow_feature: venue={} symbol={} ts={}",
                    self.venue_slug, symbol_norm, bar.start_ms
                );
            }
        }
        Ok(())
    }

    fn maybe_reload_quantiles(&mut self) {
        if self.last_quantile_reload.elapsed() < self.quantile_reload_interval {
            return;
        }
        self.reload_quantiles(false);
    }

    fn reload_quantiles(&mut self, init: bool) {
        self.last_quantile_reload = Instant::now();
        match self
            .quantile_store
            .load_quantiles_for_venue(&self.venue_slug, self.venue)
        {
            Ok(new_map) => {
                let mut symbols = HashSet::with_capacity(new_map.len());
                for symbol in new_map.keys() {
                    symbols.insert(symbol.clone());
                }

                self.quantiles = new_map;
                self.online_symbols = symbols;
                self.symbols
                    .retain(|symbol, _| self.online_symbols.contains(symbol));

                info!(
                    "trade_flow_feature quantiles {}loaded: venue={} online_symbols={} key_pattern='{}:*:{}'",
                    if init { "" } else { "re" },
                    self.venue_slug,
                    self.online_symbols.len(),
                    self.venue_slug,
                    QUANTILE_REDIS_KEY_SUFFIX
                );
            }
            Err(err) => {
                self.quantile_store.warn_throttled(&format!(
                    "trade_flow_feature quantile reload failed: venue={} err={:#}",
                    self.venue_slug, err
                ));
            }
        }
    }
}

fn parse_quantile_entries(raw: &str) -> Result<Vec<QuantileJsonEntry>> {
    let value: Value = serde_json::from_str(raw)?;
    let mut out = Vec::new();
    collect_quantile_entries(&value, &mut out);
    Ok(out)
}

fn collect_quantile_entries(value: &Value, out: &mut Vec<QuantileJsonEntry>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_quantile_entries(item, out);
            }
        }
        Value::Object(map) => {
            let low = map.get("low").and_then(json_as_f64);
            let high = map.get("high").and_then(json_as_f64);
            if let (Some(low), Some(high)) = (low, high) {
                let symbol = map
                    .get("symbol")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                out.push(QuantileJsonEntry { symbol, low, high });
            }

            for child in map.values() {
                if child.is_array() || child.is_object() {
                    collect_quantile_entries(child, out);
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

fn parse_symbol_from_quantile_key(
    key: &str,
    venue_slug: &str,
    venue_cfg: TradingVenue,
) -> Option<String> {
    let mut parts = key.split(':');
    let key_venue = parts.next()?;
    let symbol = parts.next()?;
    let suffix = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    if !key_venue.eq_ignore_ascii_case(venue_slug) || suffix != QUANTILE_REDIS_KEY_SUFFIX {
        return None;
    }
    let symbol = normalize_symbol_for_venue(symbol.trim(), venue_cfg);
    if symbol.is_empty() {
        None
    } else {
        Some(symbol)
    }
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

    offset += 8; // trade_id
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
        timestamp_ms,
        side,
        price,
        amount,
    })
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

fn should_log_symbol(symbol: &str) -> bool {
    let (base, _) = extract_assets_from_symbol(symbol);
    LOG_BASE_SYMBOLS
        .iter()
        .any(|candidate| base.eq_ignore_ascii_case(candidate))
}
