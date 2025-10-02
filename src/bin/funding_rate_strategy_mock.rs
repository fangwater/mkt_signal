use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use chrono::Utc;
use log::{debug, error, info, warn};
use serde::Deserialize;
use tokio::signal;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use mkt_signal::common::iceoryx_publisher::SignalPublisher;
use mkt_signal::common::iceoryx_subscriber::{
    ChannelType, MultiChannelSubscriber, SubscribeParams,
};
use mkt_signal::common::min_qty_table::MinQtyTable;
use mkt_signal::common::redis_client::RedisSettings;
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::mkt_msg::{self, AskBidSpreadMsg, FundingRateMsg, MktMsgType};
use mkt_signal::pre_trade::order_manager::{OrderType, Side};
use mkt_signal::signal::binance_forward_arb::{
    BinSingleForwardArbCloseMarginCtx, BinSingleForwardArbOpenCtx,
};
use mkt_signal::signal::trade_signal::{SignalType, TradeSignal};
use mkt_signal::signal::resample::{compute_askbid_sr, compute_bidask_sr};

const SIGNAL_CHANNEL_MT_ARBITRAGE: &str = "mt_arbitrage";
const NODE_FUNDING_STRATEGY_SUB: &str = "funding_rate_strategy";
const DEFAULT_CFG_PATH: &str = "config/funding_rate_strategy.toml";
const DEFAULT_REDIS_HASH_KEY: &str = "binance_arb_price_spread_threshold";

// 已移除本地 JSON 模式，仅支持 Redis

#[derive(Debug, Clone, Deserialize)]
struct OrderConfig {
    #[serde(default = "default_open_range")]
    open_range: f64,
    #[serde(default = "default_close_range")]
    close_range: f64,
    #[serde(default = "default_order_amount")]
    amount_u: f64,
    #[serde(default = "default_max_open_keep")]
    max_open_order_keep_s: u64,
    #[serde(default = "default_max_close_keep")]
    max_close_order_keep_s: u64,
}

const fn default_open_range() -> f64 {
    0.0002
}

const fn default_close_range() -> f64 {
    0.0002
}

const fn default_order_amount() -> f64 {
    50.0
}

const fn default_max_open_keep() -> u64 {
    5
}

const fn default_max_close_keep() -> u64 {
    5
}

impl Default for OrderConfig {
    fn default() -> Self {
        Self {
            open_range: default_open_range(),
            close_range: default_close_range(),
            amount_u: default_order_amount(),
            max_open_order_keep_s: default_max_open_keep(),
            max_close_order_keep_s: default_max_close_keep(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct SignalConfig {
    #[serde(default = "default_signal_interval_ms")]
    min_interval_ms: u64,
}

const fn default_signal_interval_ms() -> u64 {
    1_000
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            min_interval_ms: default_signal_interval_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ReloadConfig {
    #[serde(default = "default_reload_interval")]
    interval_secs: u64,
}

const fn default_reload_interval() -> u64 {
    60
}

impl Default for ReloadConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_reload_interval(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct StrategyParams {
    #[serde(default = "default_interval")] 
    interval: usize,
    #[serde(default)]
    predict_num: usize,
    /// 重算滚动预测频率（秒）
    #[serde(default = "default_compute_secs")] 
    refresh_secs: u64,
    /// 拉取历史频率（秒）
    #[serde(default = "default_fetch_secs")] 
    fetch_secs: u64,
    /// 拉取对齐偏移（秒）
    #[serde(default = "default_fetch_offset_secs")] 
    fetch_offset_secs: u64,
    /// 单次拉取的最大记录条数
    #[serde(default = "default_fetch_limit")] 
    history_limit: usize,
    /// funding rate 滚动均值窗口大小（条数）
    #[serde(default = "default_funding_ma_size")] 
    funding_ma_size: usize,
    /// 结算偏移（秒）：基于 UTC 准点（4h 周期）增加的偏移量
    #[serde(default = "default_settlement_offset_secs")] 
    settlement_offset_secs: i64,
}

const fn default_interval() -> usize { 6 }
const fn default_compute_secs() -> u64 { 30 }
const fn default_fetch_secs() -> u64 { 7200 }
const fn default_fetch_offset_secs() -> u64 { 120 }
const fn default_fetch_limit() -> usize { 100 }
const fn default_funding_ma_size() -> usize { 60 }
const fn default_settlement_offset_secs() -> i64 { 0 }

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ParamsSnapshot {
    interval: u64,
    predict_num: u64,
    refresh_secs: u64,
    fetch_secs: u64,
    fetch_offset_secs: u64,
    history_limit: u64,
    funding_ma_size: u64,
    settlement_offset_secs: i64,
}

impl From<&StrategyParams> for ParamsSnapshot {
    fn from(p: &StrategyParams) -> Self {
        Self {
            interval: p.interval as u64,
            predict_num: p.predict_num as u64,
            refresh_secs: p.refresh_secs,
            fetch_secs: p.fetch_secs,
            fetch_offset_secs: p.fetch_offset_secs,
            history_limit: p.history_limit as u64,
            funding_ma_size: p.funding_ma_size as u64,
            settlement_offset_secs: p.settlement_offset_secs,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct StrategyConfig {
    #[allow(dead_code)]
    #[serde(default)]
    redis: Option<RedisSettings>,
    #[serde(default)]
    redis_key: Option<String>,
    #[serde(default)]
    order: OrderConfig,
    #[serde(default)]
    signal: SignalConfig,
    #[serde(default)]
    reload: ReloadConfig,
    #[serde(default)]
    strategy: StrategyParams,
}

impl StrategyConfig {
    fn load() -> Result<Self> {
        let cfg_path = std::env::var("FUNDING_RATE_CFG").unwrap_or_else(|_| DEFAULT_CFG_PATH.to_string());
        let cfg: StrategyConfig = match std::fs::read_to_string(&cfg_path) {
            Ok(content) => {
                let mut cfg: StrategyConfig = toml::from_str(&content)
                    .with_context(|| format!("解析配置文件失败: {cfg_path}"))?;
                if cfg.redis.is_none() { cfg.redis = Some(mkt_signal::common::redis_client::RedisSettings::default()); }
                cfg
            }
            Err(err) => {
                warn!("mock: 未找到配置文件({err}); 使用默认配置并从 Redis 读取参数");
                StrategyConfig {
                    redis: Some(mkt_signal::common::redis_client::RedisSettings::default()),
                    redis_key: None,
                    order: OrderConfig::default(),
                    signal: SignalConfig::default(),
                    reload: ReloadConfig::default(),
                    strategy: StrategyParams::default(),
                }
            }
        };
        if let Some(redis_cfg) = cfg.redis.as_ref() {
            info!(
                "Redis 数据源配置: host={} port={} db={} prefix={:?}",
                redis_cfg.host, redis_cfg.port, redis_cfg.db, redis_cfg.prefix
            );
        }
        info!(
            "mock 策略配置加载完成: reload_interval={}s strategy: interval={} predict_num={} refresh_secs={}s fetch_secs={}s fetch_offset={}s history_limit={} settlement_offset_secs={}",
            cfg.reload.interval_secs,
            cfg.strategy.interval, cfg.strategy.predict_num, cfg.strategy.refresh_secs,
            cfg.strategy.fetch_secs, cfg.strategy.fetch_offset_secs, cfg.strategy.history_limit,
            cfg.strategy.settlement_offset_secs
        );
        // funding_ma_size 固定为 60，忽略外部配置覆写
        let mut cfg = cfg;
        cfg.strategy.funding_ma_size = 60;
        Ok(cfg)
    }

    fn max_open_keep_us(&self) -> i64 {
        (self.order.max_open_order_keep_s.max(1) * 1_000_000) as i64
    }

    fn max_close_keep_us(&self) -> i64 {
        (self.order.max_close_order_keep_s.max(1) * 1_000_000) as i64
    }
}

#[derive(Debug, Clone)]
struct SymbolThreshold {
    spot_symbol: String,
    futures_symbol: String,
    // BidAskSR 阈值
    open_threshold: f64,
    close_threshold: f64,
    // AskBidSR 阈值
    askbid_open_threshold: f64,
    askbid_close_threshold: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct Quote {
    bid: f64,
    ask: f64,
    ts: i64,
}

impl Quote {
    fn update(&mut self, bid: f64, ask: f64, ts: i64) {
        self.bid = bid;
        self.ask = ask;
        self.ts = ts;
    }

    fn is_ready(&self) -> bool {
        self.bid > 0.0 && self.ask > 0.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PositionState {
    Flat,
    Opened,
}

impl Default for PositionState {
    fn default() -> Self {
        PositionState::Flat
    }
}

// note: removed label() as we no longer print Pos column

#[derive(Debug, Clone)]
struct SymbolState {
    spot_symbol: String,
    futures_symbol: String,
    open_threshold: f64,
    close_threshold: f64,
    askbid_open_threshold: f64,
    askbid_close_threshold: f64,
    spot_quote: Quote,
    futures_quote: Quote,
    position: PositionState,
    last_ratio: Option<f64>,
    last_open_ts: Option<i64>,
    last_close_ts: Option<i64>,
    last_signal_ts: Option<i64>,
    funding_rate: f64,
    predicted_rate: f64,
    loan_rate: f64,
    funding_ts: i64,
    next_funding_time: i64,
}

impl SymbolState {
    fn new(threshold: SymbolThreshold) -> Self {
        Self {
            spot_symbol: threshold.spot_symbol,
            futures_symbol: threshold.futures_symbol,
            open_threshold: threshold.open_threshold,
            close_threshold: threshold.close_threshold,
            askbid_open_threshold: threshold.askbid_open_threshold,
            askbid_close_threshold: threshold.askbid_close_threshold,
            spot_quote: Quote::default(),
            futures_quote: Quote::default(),
            position: PositionState::Flat,
            last_ratio: None,
            last_open_ts: None,
            last_close_ts: None,
            last_signal_ts: None,
            funding_rate: 0.0,
            predicted_rate: 0.0,
            loan_rate: 0.0,
            funding_ts: 0,
            next_funding_time: 0,
        }
    }

    fn update_threshold(&mut self, threshold: SymbolThreshold) {
        self.open_threshold = threshold.open_threshold;
        self.close_threshold = threshold.close_threshold;
        self.futures_symbol = threshold.futures_symbol;
        self.askbid_open_threshold = threshold.askbid_open_threshold;
        self.askbid_close_threshold = threshold.askbid_close_threshold;
    }

    /// bidask_sr = (spot_bid - futures_ask) / spot_bid
    fn calc_ratio(&self) -> Option<f64> {
        if self.spot_quote.bid <= 0.0 || self.futures_quote.ask <= 0.0 {
            return None;
        }
        Some((self.spot_quote.bid - self.futures_quote.ask) / self.spot_quote.bid)
    }

    fn mark_signal(&mut self, now_us: i64) {
        self.last_signal_ts = Some(now_us);
    }
}

#[derive(Debug, Clone)]
struct QtyStepInfo {
    spot_min: f64,
    futures_min: f64,
    step: f64,
}

use reqwest::blocking::Client;
use anyhow::Result as AnyResult;

struct MockController {
    cfg: StrategyConfig,
    publisher: SignalPublisher,
    symbols: HashMap<String, SymbolState>,
    futures_index: HashMap<String, String>,
    min_qty: MinQtyTable,
    qty_step_cache: RefCell<HashMap<String, QtyStepInfo>>,
    history_map: HashMap<String, Vec<f64>>,
    predicted_map: HashMap<String, f64>,
    next_compute_refresh: std::time::Instant,
    next_fetch_refresh: std::time::Instant,
    next_threshold_reload: std::time::Instant,
    http: Client,
    // 新增：内存维护 funding 频率与参数快照
    funding_frequency: HashMap<String, String>, // fut_symbol -> "4h" | "8h"
    last_params: Option<ParamsSnapshot>,
    last_settlement_marker_ms: Option<i64>,
    th_4h: RateThresholds,
    th_8h: RateThresholds,
    warmup_done: bool,
    next_params_reload: std::time::Instant,
}

impl MockController {
    async fn new(cfg: StrategyConfig, publisher: SignalPublisher) -> Result<Self> {
        let mut min_qty = MinQtyTable::new();
        if let Err(err) = min_qty.refresh_binance().await {
            warn!("刷新最小下单量失败: {err:?}");
        }
        let mut controller = Self {
            cfg,
            publisher,
            symbols: HashMap::new(),
            futures_index: HashMap::new(),
            min_qty,
            qty_step_cache: RefCell::new(HashMap::new()),
            history_map: HashMap::new(),
            predicted_map: HashMap::new(),
            next_compute_refresh: std::time::Instant::now(),
            next_fetch_refresh: std::time::Instant::now(),
            next_threshold_reload: std::time::Instant::now(),
            http: Client::new(),
            funding_frequency: HashMap::new(),
            last_params: None,
            last_settlement_marker_ms: None,
            th_4h: RateThresholds::default(),
            th_8h: RateThresholds::default(),
            warmup_done: false,
            next_params_reload: std::time::Instant::now(),
        };
        // 启动时先加载参数与符号
        let _ = controller.reload_params_if_changed();
        controller.reload_symbols()?;
        controller.warmup_done = controller.is_warmup_complete();
        Ok(controller)
    }

    fn poll_market(&mut self, subscriber: &mut MultiChannelSubscriber) {
        for msg in subscriber.poll_channel("binance", &ChannelType::AskBidSpread, Some(64)) {
            if mkt_msg::get_msg_type(&msg) == MktMsgType::AskBidSpread {
                self.handle_spot_quote(&msg);
            }
        }
        for msg in subscriber.poll_channel("binance-futures", &ChannelType::AskBidSpread, Some(64))
        {
            if mkt_msg::get_msg_type(&msg) == MktMsgType::AskBidSpread {
                self.handle_futures_quote(&msg);
            }
        }
        for msg in subscriber.poll_channel("binance-futures", &ChannelType::Derivatives, Some(64)) {
            if mkt_msg::get_msg_type(&msg) == MktMsgType::FundingRate {
                self.handle_funding_rate(&msg);
            }
        }
    }

    fn on_tick(&mut self, subscriber: &mut MultiChannelSubscriber) {
        self.poll_market(subscriber);
        // 到点拉取历史
        let now = std::time::Instant::now();
        // 阈值定时从 Redis 刷新
        if now >= self.next_threshold_reload {
            let gap = std::time::Duration::from_secs(self.cfg.reload.interval_secs.max(5));
            self.next_threshold_reload = now + gap;
            let before = self.symbols.len();
            if let Err(err) = self.reload_symbols() { warn!("mock 刷新追踪列表失败: {err:?}"); }
            let after = self.symbols.len();
            debug!("mock refresh: symbols_before={} symbols_after={}", before, after);
            if after != before {
                // 新增/移除符号：为新增符号推断频率并拉取历史，随后仅重算（不打印行情快照）
                if let Err(err) = self.update_added_symbols_history_and_freq() { warn!("mock 新增符号拉取失败: {err:?}"); }
                self.compute_predictions();
                self.recompute_and_print("符号增加或移除");
            }
        }
        // 结算点触发：更新频率并重算预测
        if self.is_settlement_trigger() {
            debug!("mock 结算点触发: 更新频率并重算");
            if let Err(err) = self.refresh_frequency_all() { debug!("mock 刷新频率失败: {err:?}"); }
            self.compute_predictions();
            self.recompute_and_print("结算点触发");
        }
        if now >= self.next_fetch_refresh {
            self.next_fetch_refresh = self.next_fetch_instant();
            if let Err(err) = self.fetch_histories() { warn!("mock 拉取历史失败: {err:?}"); }
        }
        // 按 refresh_secs 仅在“参数变更”时重算
        if now >= self.next_params_reload {
            self.next_params_reload = now + std::time::Duration::from_secs(self.cfg.strategy.refresh_secs.max(5));
            if self.reload_params_if_changed() {
                self.compute_predictions();
                self.recompute_and_print("参数修改");
            }
        }
    }

    fn next_fetch_instant(&self) -> std::time::Instant {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let fetch = self.cfg.strategy.fetch_secs.max(600);
        let offset = self.cfg.strategy.fetch_offset_secs.min(fetch - 1);
        let next_slot = ((now / fetch) + 1) * fetch + offset;
        let dur = next_slot.saturating_sub(now);
        std::time::Instant::now() + std::time::Duration::from_secs(dur)
    }

    fn fetch_histories(&mut self) -> AnyResult<()> {
        debug!("mock 开始拉取 funding 历史");
        let mut fut_syms: Vec<String> = self
            .symbols
            .values()
            .map(|s| s.futures_symbol.clone())
            .collect();
        fut_syms.sort();
        fut_syms.dedup();
        let mut new_history = HashMap::new();
        let limit = self.cfg.strategy.history_limit;
        let now = Utc::now().timestamp_millis();
        for sym in fut_syms {
            // 确认频率；若未知则推断
            let freq = self
                .funding_frequency
                .get(&sym.to_uppercase())
                .cloned()
                .or_else(|| infer_binance_funding_frequency_blocking(&self.http, &sym).ok().flatten())
                .unwrap_or_else(|| "8h".to_string());
            let hours = if freq.eq_ignore_ascii_case("4h") { 4 } else { 8 };
            let window_ms = (hours as i64) * 3600 * 1000 * (limit as i64 + 2);
            let start_time = now.saturating_sub(window_ms);
            let rates = fetch_binance_funding_history_range_blocking(&self.http, &sym, start_time, now, limit)?;
            new_history.insert(sym, rates);
        }
        self.history_map = new_history;
        debug!("mock 历史更新完成, symbols={}", self.history_map.len());
        // warmup 检查：首次完成时，立即计算并打印
        let was = self.warmup_done;
        self.warmup_done = self.is_warmup_complete();
        if !was && self.warmup_done {
            debug!("mock warmup 完成: 所有符号资金费率均值达到窗口");
            self.compute_predictions();
            self.print_funding_overview_table();
            self.print_symbol_snapshot();
            // 避免进入事件循环后立刻再次计算，推迟下一次预测
            self.next_compute_refresh = std::time::Instant::now()
                + std::time::Duration::from_secs(self.cfg.strategy.refresh_secs.max(5));
        } else if !self.warmup_done {
            self.print_warmup_progress_table();
        }
        Ok(())
    }

    fn refresh_frequency_all(&mut self) -> AnyResult<()> {
        let futs: Vec<String> = self
            .symbols
            .values()
            .map(|s| s.futures_symbol.to_uppercase())
            .collect();
        for fut in futs {
            if let Some(freq) = infer_binance_funding_frequency_blocking(&self.http, &fut)? {
                debug!("mock 频率刷新: {} -> {}", fut, freq);
                self.funding_frequency.insert(fut, freq);
            }
        }
        Ok(())
    }

    fn update_added_symbols_history_and_freq(&mut self) -> AnyResult<()> {
        // 为所有跟踪符号拉取 freq（频率）并确保新增符号有历史
        let limit = self.cfg.strategy.history_limit;
        for (_spot, state) in self.symbols.iter() {
            let fut = state.futures_symbol.to_uppercase();
            if let Some(freq) = infer_binance_funding_frequency_blocking(&self.http, &fut)? {
                debug!("mock 新增符号频率: {} -> {}", fut, freq);
                self.funding_frequency.insert(fut.clone(), freq);
            }
            if !self.history_map.contains_key(&fut) {
                let now = Utc::now().timestamp_millis();
                let freq = self
                    .funding_frequency
                    .get(&fut)
                    .cloned()
                    .unwrap_or_else(|| "8h".to_string());
                let hours = if freq.eq_ignore_ascii_case("4h") { 4 } else { 8 };
                let window_ms = (hours as i64) * 3600 * 1000 * (limit as i64 + 2);
                let start_time = now.saturating_sub(window_ms);
                let rates = fetch_binance_funding_history_range_blocking(&self.http, &fut, start_time, now, limit)?;
                self.history_map.insert(fut, rates);
            }
        }
        // warmup 检查：首次完成时，立即计算并打印
        let was = self.warmup_done;
        self.warmup_done = self.is_warmup_complete();
        if !was && self.warmup_done {
            debug!("mock warmup 完成(新增符号): 打印总览与价差");
            self.compute_predictions();
            self.print_funding_overview_table();
            self.print_symbol_snapshot();
            // 推迟下一次周期性预测，避免立即重复计算
            self.next_compute_refresh = std::time::Instant::now()
                + std::time::Duration::from_secs(self.cfg.strategy.refresh_secs.max(5));
        } else if !self.warmup_done {
            self.print_warmup_progress_table();
        }
        Ok(())
    }

    fn recompute_and_print(&mut self, reason: &str) {
        debug!("mock 开始重算资金费率阈值: reason='{}'", reason);
        // 仅重算阈值并打印三线表（不改变其它状态）
        if self.is_warmup_complete() {
            self.print_funding_overview_table();
        } else {
            debug!("mock 跳过总览打印: warmup 未完成");
        }
    }

    fn thresholds_for_frequency(&self, freq_4h_or_8h: &str) -> (f64, f64, f64, f64) {
        match freq_4h_or_8h {
            "4h" | "4H" => (
                self.th_4h.open_upper,
                self.th_4h.open_lower,
                self.th_4h.close_lower,
                self.th_4h.close_upper,
            ),
            "8h" | "8H" => (
                self.th_8h.open_upper,
                self.th_8h.open_lower,
                self.th_8h.close_lower,
                self.th_8h.close_upper,
            ),
            other => panic!("Unsupported funding frequency: {}", other),
        }
    }

    fn reload_params_if_changed(&mut self) -> bool {
        use redis::Commands;
        let Some(settings) = self.cfg.redis.clone() else { return false; };
        let url = settings.connection_url();
        let Ok(client) = redis::Client::open(url.clone()) else { return false; };
        let Ok(mut con) = client.get_connection() else { return false; };
        let key = match &settings.prefix { Some(p) if !p.is_empty() => format!("{}{}", p, "binance_forward_arb_params"), _ => "binance_forward_arb_params".to_string() };
        let Ok(map) = con.hgetall::<_, std::collections::HashMap<String, String>>(key) else { return false; };
        debug!("mock 参数读取: {:?}", map);
        // 必须包含 8 个阈值键
        let required = [
            "fr_4h_open_upper_threshold",
            "fr_4h_open_lower_threshold",
            "fr_4h_close_lower_threshold",
            "fr_4h_close_upper_threshold",
            "fr_8h_open_upper_threshold",
            "fr_8h_open_lower_threshold",
            "fr_8h_close_lower_threshold",
            "fr_8h_close_upper_threshold",
        ];
        let mut missing = Vec::new();
        for k in required.iter() { if !map.contains_key(*k) { missing.push(k.to_string()); } }
        if !missing.is_empty() { panic!("mock 缺少资金费率阈值参数: {:?}", missing); }
        let parse_u64 = |k: &str| -> Option<u64> { map.get(k).and_then(|v| v.parse::<u64>().ok()) };
        let parse_i64 = |k: &str| -> Option<i64> { map.get(k).and_then(|v| v.parse::<i64>().ok()) };
        let parse_f64 = |k: &str| -> Option<f64> { map.get(k).and_then(|v| v.parse::<f64>().ok()) };
        let mut changed = false;
        if let Some(v) = parse_u64("interval") { if self.cfg.strategy.interval as u64 != v { self.cfg.strategy.interval = v as usize; changed = true; } }
        if let Some(v) = parse_u64("predict_num") { if self.cfg.strategy.predict_num as u64 != v { self.cfg.strategy.predict_num = v as usize; changed = true; } }
        if let Some(v) = parse_u64("refresh_secs") { if self.cfg.strategy.refresh_secs != v { self.cfg.strategy.refresh_secs = v; changed = true; } }
        if let Some(v) = parse_u64("fetch_secs") { if self.cfg.strategy.fetch_secs != v { self.cfg.strategy.fetch_secs = v; changed = true; } }
        if let Some(v) = parse_u64("fetch_offset_secs") { if self.cfg.strategy.fetch_offset_secs != v { self.cfg.strategy.fetch_offset_secs = v; changed = true; } }
        if let Some(v) = parse_u64("history_limit") { if self.cfg.strategy.history_limit as u64 != v { self.cfg.strategy.history_limit = v as usize; changed = true; } }
        if let Some(v) = parse_i64("settlement_offset_secs") { if self.cfg.strategy.settlement_offset_secs != v { self.cfg.strategy.settlement_offset_secs = v; changed = true; } }
        if let Some(v) = parse_u64("signal_min_interval_ms") { if self.cfg.signal.min_interval_ms != v { self.cfg.signal.min_interval_ms = v; changed = true; } }
        if let Some(v) = parse_u64("reload_interval_secs") { if self.cfg.reload.interval_secs != v { self.cfg.reload.interval_secs = v; changed = true; } }
        // 阈值（4h/8h）
        let mut th_changed = false;
        if let Some(v) = parse_f64("fr_4h_open_upper_threshold") { if !approx_equal(self.th_4h.open_upper, v) { self.th_4h.open_upper = v; th_changed = true; } }
        if let Some(v) = parse_f64("fr_4h_open_lower_threshold") { if !approx_equal(self.th_4h.open_lower, v) { self.th_4h.open_lower = v; th_changed = true; } }
        if let Some(v) = parse_f64("fr_4h_close_lower_threshold") { if !approx_equal(self.th_4h.close_lower, v) { self.th_4h.close_lower = v; th_changed = true; } }
        if let Some(v) = parse_f64("fr_4h_close_upper_threshold") { if !approx_equal(self.th_4h.close_upper, v) { self.th_4h.close_upper = v; th_changed = true; } }
        if let Some(v) = parse_f64("fr_8h_open_upper_threshold") { if !approx_equal(self.th_8h.open_upper, v) { self.th_8h.open_upper = v; th_changed = true; } }
        if let Some(v) = parse_f64("fr_8h_open_lower_threshold") { if !approx_equal(self.th_8h.open_lower, v) { self.th_8h.open_lower = v; th_changed = true; } }
        if let Some(v) = parse_f64("fr_8h_close_lower_threshold") { if !approx_equal(self.th_8h.close_lower, v) { self.th_8h.close_lower = v; th_changed = true; } }
        if let Some(v) = parse_f64("fr_8h_close_upper_threshold") { if !approx_equal(self.th_8h.close_upper, v) { self.th_8h.close_upper = v; th_changed = true; } }
        if th_changed { changed = true; debug!("mock 阈值参数变更: 4h={:?} 8h={:?}", self.th_4h, self.th_8h); }
        let current = ParamsSnapshot::from(&self.cfg.strategy);
        if self.last_params.as_ref() != Some(&current) { self.last_params = Some(current); changed = true; }
        debug!("mock 参数变更检测: changed={}", changed);
        changed
    }

    fn is_settlement_trigger(&mut self) -> bool {
        // 基于 UTC 准点 + 偏移（秒）判断 4h 周期的结算点
        let now_ms = Utc::now().timestamp_millis();
        let offset_ms = self.cfg.strategy.settlement_offset_secs.saturating_mul(1000);
        let period_ms: i64 = 4 * 3600 * 1000; // 4h schedule
        let adj = now_ms.saturating_sub(offset_ms);
        if adj < 0 { return false; }
        let slot = adj / period_ms;
        let slot_ms = slot.saturating_mul(period_ms).saturating_add(offset_ms);
        if self.last_settlement_marker_ms != Some(slot_ms) {
            self.last_settlement_marker_ms = Some(slot_ms);
            debug!("mock 结算点触发: slot_ms={} (now_ms={} offset_s={})", slot_ms, now_ms, self.cfg.strategy.settlement_offset_secs);
            return true;
        }
        false
    }

    fn compute_predictions(&mut self) {
        if !self.is_warmup_complete() {
            debug!("mock 预测计算跳过: warmup 未完成");
            return;
        }
        let interval = self.cfg.strategy.interval.max(1);
        let predict_num = self.cfg.strategy.predict_num;
        let mut map = HashMap::new();
        for (sym, rates) in &self.history_map {
            let n = rates.len();
            let pred = if n == 0 {
                debug!("pred calc: {sym} len=0 => 0");
                0.0
            } else if interval == 0 {
                debug!("pred calc: {sym} interval=0 => 0");
                0.0
            } else {
                // 与 compute_predict_local 一致的窗口: 以 n-1-predict_num 为窗口尾
                if n - 1 < predict_num {
                    debug!(
                        "pred calc: {sym} len={} predict_num={} => 不足以回溯 (n-1<predict_num) => 0",
                        n, predict_num
                    );
                    0.0
                } else {
                    let end = n - 1 - predict_num;
                    if end + 1 < interval {
                        debug!(
                            "pred calc: {sym} len={} interval={} end={} => 窗口不足 (end+1<interval) => 0",
                            n, interval, end
                        );
                        0.0
                    } else {
                        let start = end + 1 - interval;
                        let slice = &rates[start..=end];
                        let sum: f64 = slice.iter().copied().sum();
                        let mean = sum / (interval as f64);
                        debug!(
                            "pred calc: {sym} len={} interval={} predict_num={} start={} end={} mean={:.6}",
                            n, interval, predict_num, start, end, mean
                        );
                        mean
                    }
                }
            };
            map.insert(sym.clone(), pred);
        }
        self.predicted_map = map;
        if !self.predicted_map.is_empty() {
            let mut sample: Vec<(&String, &f64)> = self.predicted_map.iter().take(5).collect();
            sample.sort_by(|a, b| a.0.cmp(b.0));
            debug!("mock 预测更新完成: {} 项, 示例: {:?}", self.predicted_map.len(), sample);
        } else {
            debug!("mock 预测更新完成: 空");
        }
    }

    fn get_predicted_for(&self, fut_symbol: &str) -> f64 {
        self.predicted_map
            .get(&fut_symbol.to_uppercase())
            .copied()
            .unwrap_or(0.0)
    }

    async fn handle_command(&mut self, line: &str) -> Result<bool> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(false);
        }
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() == 1 && parts[0].chars().all(|c| c.is_ascii_digit()) {
            let idx = parse_index(parts[0])?;
            self.force_open(idx)?;
            self.print_symbol_snapshot();
            return Ok(false);
        }
        let cmd = parts[0].to_ascii_lowercase();
        match cmd.as_str() {
            "list" | "ls" | "l" => {
                self.print_symbol_snapshot();
            }
            "open" | "o" => {
                let idx_str = parts
                    .get(1)
                    .copied()
                    .ok_or_else(|| anyhow!("缺少索引参数"))?;
                let idx = parse_index(idx_str)?;
                self.force_open(idx)?;
                self.print_symbol_snapshot();
            }
            "close" | "c" => {
                let idx_str = parts
                    .get(1)
                    .copied()
                    .ok_or_else(|| anyhow!("缺少索引参数"))?;
                let idx = parse_index(idx_str)?;
                self.force_close(idx)?;
                self.print_symbol_snapshot();
            }
            "reload" => {
                self.reload_symbols()?;
                self.print_symbol_snapshot();
            }
            "refresh" => {
                self.refresh_min_qty().await?;
                // 也打印一次价差表，保持“每次操作之后打印两表”的一致性
                self.print_symbol_snapshot();
            }
            "help" | "h" => {
                self.print_help();
            }
            "quit" | "exit" => {
                info!("收到退出命令");
                return Ok(true);
            }
            other => {
                warn!("未知指令: {other}");
                self.print_help();
            }
        }
        // 操作后维护：根据参数/符号变化与结算点决定是否重算或拉取历史
        self.post_operation_maintenance();
        // 每次操作后打印资金费率总览（三线表）
        self.print_funding_overview_table();
        Ok(false)
    }

    fn print_help(&self) {
        info!("命令: list | open <idx> | close <idx> | reload | refresh | help | quit");
        info!("直接输入索引 (例如 `2`) 等同于 open <idx>，索引从 1 开始");
    }

    fn print_funding_overview_table(&self) {
        if self.symbols.is_empty() { return; }
        if !self.is_warmup_complete() { return; }
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut keys: Vec<String> = self.symbols.keys().cloned().collect();
        keys.sort();
        for key in keys {
            let Some(state) = self.symbols.get(&key) else { continue; };
            let fut = state.futures_symbol.to_uppercase();
            let freq = self
                .funding_frequency
                .get(&fut)
                .cloned()
                .unwrap_or_else(|| "8h".to_string());
            let (ou, ol, cl, cu) = self.thresholds_for_frequency(&freq);
            let pred = self.get_predicted_for(&fut);
            // 使用已拉取的历史计算均值；若无历史则退化为当前 funding_rate
            let fr_mean = self
                .history_map
                .get(&fut)
                .and_then(|v| if v.is_empty() { None } else { Some(v.iter().copied().sum::<f64>() / (v.len() as f64)) })
                .or_else(|| if state.funding_rate != 0.0 { Some(state.funding_rate) } else { None })
                .map(|v| format!("{:.6}", v))
                .unwrap_or_else(|| "-".to_string());
            rows.push(vec![
                key.clone(),
                freq,
                fr_mean,
                format!("{:.6}", pred),
                format!("{:.6}", ou),
                format!("{:.6}", ol),
                format!("{:.6}", cl),
                format!("{:.6}", cu),
            ]);
        }
        if rows.is_empty() { return; }
        let table = render_three_line_table(
            &["Symbol", "Freq", "FR_Mean", "Pred", "OpenU", "OpenL", "CloseL", "CloseU"],
            &rows,
        );
        info!("资金费率总览\n{}", table);
    }

    fn print_warmup_progress_table(&self) {
        if self.symbols.is_empty() { return; }
        let need = self.cfg.strategy.funding_ma_size.max(1);
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut keys: Vec<String> = self.sorted_symbol_keys();
        for key in keys.drain(..) {
            if let Some(state) = self.symbols.get(&key) {
                let fut = state.futures_symbol.to_uppercase();
                let cnt = self.history_map.get(&fut).map(|v| v.len()).unwrap_or(0);
                let freq = self
                    .funding_frequency
                    .get(&fut)
                    .cloned()
                    .unwrap_or_else(|| "8h".to_string());
                rows.push(vec![
                    key.clone(),
                    fut,
                    cnt.to_string(),
                    need.to_string(),
                    freq,
                ]);
            }
        }
        if rows.is_empty() { return; }
        let table = render_three_line_table(&["Symbol", "Futures", "Count", "Need", "Freq"], &rows);
        info!("Warmup 进度\n{}", table);
    }

    fn post_operation_maintenance(&mut self) {
        let params_changed = self.reload_params_if_changed();
        // settlement trigger
        let settlement_triggered = self.is_settlement_trigger();
        if params_changed {
            // 仅重算
            self.compute_predictions();
            self.recompute_and_print("参数修改");
            self.next_compute_refresh = std::time::Instant::now()
                + std::time::Duration::from_secs(self.cfg.strategy.refresh_secs.max(5));
            return;
        }
        if settlement_triggered {
            self.compute_predictions();
            self.recompute_and_print("结算点触发");
            self.next_compute_refresh = std::time::Instant::now()
                + std::time::Duration::from_secs(self.cfg.strategy.refresh_secs.max(5));
            return;
        }
        // 无参数/结算变化，不额外动作
    }

    fn print_symbol_snapshot(&self) {
        if self.symbols.is_empty() {
            info!("当前未追踪任何交易对");
            return;
        }
        if !self.is_warmup_complete() { return; }
        let mut rows = Vec::new();
        for (idx, key) in self.sorted_symbol_keys().iter().enumerate() {
            if let Some(state) = self.symbols.get(key) {
                // 两个价差因子
                let bidask_sr = compute_bidask_sr(
                    Some(state.spot_quote.bid),
                    Some(state.futures_quote.ask),
                )
                .unwrap_or(0.0);
                let askbid_sr = compute_askbid_sr(
                    Some(state.spot_quote.ask),
                    Some(state.futures_quote.bid),
                )
                .unwrap_or(0.0);
                rows.push(vec![
                    format!("{:>3}", idx + 1),
                    state.spot_symbol.clone(),
                    state.futures_symbol.clone(),
                    format_price(state.spot_quote.bid),
                    format_price(state.spot_quote.ask),
                    format_price(state.futures_quote.bid),
                    format_price(state.futures_quote.ask),
                    format!("{:.6}", bidask_sr),
                    format!("{:.6}", askbid_sr),
                    format!("{:.6}", state.open_threshold),
                    format!("{:.6}", state.close_threshold),
                    format!("{:.6}", state.askbid_open_threshold),
                    format!("{:.6}", state.askbid_close_threshold),
                ]);
            }
        }
        let table = render_three_line_table(
            &[
                "Idx", "Spot", "Futures", "SpotBid", "SpotAsk", "FutBid", "FutAsk",
                "BidAskSR", "AskBidSR", "BA_OpenTh", "BA_CloseTh", "AB_OpenTh", "AB_CloseTh",
            ],
            &rows,
        );
        info!("\n{}", table);
    }

    fn sorted_symbol_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.symbols.keys().cloned().collect();
        keys.sort();
        keys
    }

    fn is_warmup_complete(&self) -> bool {
        if self.symbols.is_empty() { return false; }
        let need = self.cfg.strategy.funding_ma_size.max(1);
        for s in self.symbols.values() {
            let fut = s.futures_symbol.to_uppercase();
            match self.history_map.get(&fut) {
                Some(v) if v.len() >= need => {}
                _ => return false,
            }
        }
        true
    }

    fn symbol_key_by_index(&self, index: usize) -> Result<String> {
        let keys = self.sorted_symbol_keys();
        keys.get(index)
            .cloned()
            .ok_or_else(|| anyhow!("索引 {} 超出范围", index + 1))
    }

    fn force_open(&mut self, index: usize) -> Result<()> {
        let key = self.symbol_key_by_index(index)?;
        let (ctx, limit_price, adjusted_qty, spot_symbol) = {
            let state = self
                .symbols
                .get(&key)
                .ok_or_else(|| anyhow!("未找到交易对 {key}"))?;
            if !state.spot_quote.is_ready() || !state.futures_quote.is_ready() {
                anyhow::bail!("{} 行情尚未就绪，无法开仓", state.spot_symbol);
            }
            let spot_symbol = state.spot_symbol.clone();
            let futures_symbol = state.futures_symbol.clone();
            let mut limit_price = state.spot_quote.bid * (1.0 - self.cfg.order.open_range);
            if limit_price <= 0.0 {
                anyhow::bail!("{} 开仓价格非法: {:.6}", spot_symbol, limit_price);
            }
            let price_tick = self
                .min_qty
                .spot_price_tick_by_symbol(&spot_symbol)
                .unwrap_or(0.0);
            let raw_limit_price = limit_price;
            if price_tick > 0.0 {
                limit_price = align_price_floor(limit_price, price_tick);
                debug!(
                    "{} mock 开仓价格对齐: raw={:.8} tick={:.8} aligned={:.8}",
                    spot_symbol, raw_limit_price, price_tick, limit_price
                );
                if limit_price <= 0.0 {
                    anyhow::bail!(
                        "{} 开仓价格对齐失败: raw={:.8} tick={:.8}",
                        spot_symbol,
                        raw_limit_price,
                        price_tick
                    );
                }
            }
            let base_qty = if limit_price > 0.0 {
                self.cfg.order.amount_u / limit_price
            } else {
                0.0
            };
            let spot_min = self
                .min_qty
                .spot_min_qty_by_symbol(&spot_symbol)
                .unwrap_or(0.0);
            let futures_min = self
                .min_qty
                .futures_um_min_qty_by_symbol(&futures_symbol)
                .unwrap_or(0.0);
            let mut adjusted_qty = base_qty.max(spot_min).max(futures_min);
            if adjusted_qty <= 0.0 || !adjusted_qty.is_finite() {
                anyhow::bail!(
                    "{} 计算得到的下单数量非法: 基准={:.6} spot_min={:.6} futures_min={:.6}",
                    spot_symbol,
                    base_qty,
                    spot_min,
                    futures_min
                );
            }
            let qty_step = self.get_qty_step(&spot_symbol, spot_min, futures_min);
            if qty_step > 0.0 && qty_step.is_finite() {
                adjusted_qty = (adjusted_qty / qty_step).ceil() * qty_step;
            }
            let qty = adjusted_qty as f32;
            if qty <= 0.0 || !qty.is_finite() {
                anyhow::bail!(
                    "{} 下单数量非法: qty={qty} base={:.6}",
                    spot_symbol,
                    base_qty
                );
            }
            let ctx = BinSingleForwardArbOpenCtx {
                spot_symbol: spot_symbol.clone(),
                amount: qty,
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: limit_price,
                price_tick,
                exp_time: self.cfg.max_open_keep_us(),
            }
            .to_bytes();
            (ctx, limit_price, adjusted_qty, spot_symbol)
        };
        self.publish_signal(SignalType::BinSingleForwardArbOpen, ctx)?;
        let now = get_timestamp_us();
        if let Some(state) = self.symbols.get_mut(&key) {
            state.position = PositionState::Opened;
            state.last_ratio = state.calc_ratio();
            state.last_open_ts = Some(now);
            state.mark_signal(now);
            info!(
                "{} mock 强制开仓: qty={:.6} price={:.8}",
                spot_symbol, adjusted_qty, limit_price
            );
            Ok(())
        } else {
            anyhow::bail!("未找到交易对 {key}");
        }
    }

    fn force_close(&mut self, index: usize) -> Result<()> {
        let key = self.symbol_key_by_index(index)?;
        let (ctx, limit_price, spot_symbol) = {
            let state = self
                .symbols
                .get(&key)
                .ok_or_else(|| anyhow!("未找到交易对 {key}"))?;
            if !state.spot_quote.is_ready() {
                anyhow::bail!("{} 行情尚未就绪，无法平仓", state.spot_symbol);
            }
            let spot_symbol = state.spot_symbol.clone();
            let mut limit_price = state.spot_quote.ask * (1.0 + self.cfg.order.close_range);
            if limit_price <= 0.0 {
                anyhow::bail!("{} 平仓价格非法: {:.6}", spot_symbol, limit_price);
            }
            let price_tick = self
                .min_qty
                .spot_price_tick_by_symbol(&spot_symbol)
                .unwrap_or(0.0);
            let raw_limit_price = limit_price;
            if price_tick > 0.0 {
                limit_price = align_price_ceil(limit_price, price_tick);
                debug!(
                    "{} mock 平仓价格对齐: raw={:.8} tick={:.8} aligned={:.8}",
                    spot_symbol, raw_limit_price, price_tick, limit_price
                );
                if limit_price <= 0.0 {
                    anyhow::bail!(
                        "{} 平仓价格对齐失败: raw={:.8} tick={:.8}",
                        spot_symbol,
                        raw_limit_price,
                        price_tick
                    );
                }
            }
            let ctx = BinSingleForwardArbCloseMarginCtx {
                spot_symbol: spot_symbol.clone(),
                limit_price,
                price_tick,
                exp_time: self.cfg.max_close_keep_us(),
            }
            .to_bytes();
            (ctx, limit_price, spot_symbol)
        };
        self.publish_signal(SignalType::BinSingleForwardArbCloseMargin, ctx)?;
        let now = get_timestamp_us();
        if let Some(state) = self.symbols.get_mut(&key) {
            state.position = PositionState::Flat;
            state.last_ratio = state.calc_ratio();
            state.last_close_ts = Some(now);
            state.mark_signal(now);
            info!("{} mock 强制平仓: price={:.8}", spot_symbol, limit_price);
            Ok(())
        } else {
            anyhow::bail!("未找到交易对 {key}");
        }
    }

    fn publish_signal(&self, signal_type: SignalType, context: Bytes) -> Result<()> {
        // 预热未完成则忽略任何信号发布
        if !self.is_warmup_complete() {
            warn!("mock 跳过信号发布: warmup 未完成");
            return Ok(());
        }
        let now = get_timestamp_us();
        let signal = TradeSignal::create(signal_type.clone(), now, 0.0, context);
        self.publisher
            .publish(&signal.to_bytes())
            .with_context(|| format!("发布信号 {:?} 失败", signal_type))
    }

    async fn refresh_min_qty(&mut self) -> Result<()> {
        self.min_qty.refresh_binance().await?;
        self.qty_step_cache.borrow_mut().clear();
        info!("已刷新币安最小下单量表");
        Ok(())
    }

    fn reload_symbols(&mut self) -> Result<()> {
        let entries = self.load_thresholds()?;
        debug!("mock 刷新符号: entries={}", entries.len());
        let mut new_symbols = HashMap::new();
        let mut new_futures_index = HashMap::new();
        let mut added: Vec<(String, String)> = Vec::new();
        for entry in entries {
            let key = entry.spot_symbol.clone();
            let fut_key = entry.futures_symbol.clone();
            if let Some(mut state) = self.symbols.remove(&key) {
                state.update_threshold(entry.clone());
                new_futures_index.insert(fut_key, key.clone());
                new_symbols.insert(key, state);
            } else {
                let state = SymbolState::new(entry.clone());
                new_futures_index.insert(fut_key, key.clone());
                new_symbols.insert(key, state);
                added.push((entry.spot_symbol.clone(), entry.futures_symbol.clone()));
            }
        }
        self.symbols = new_symbols;
        self.futures_index = new_futures_index;
        self.qty_step_cache
            .borrow_mut()
            .retain(|symbol, _| self.symbols.contains_key(symbol));
        info!("已加载追踪交易对数量: {}", self.symbols.len());
        if !added.is_empty() { debug!("mock 新增交易对: {} 个", added.len()); }
        if !added.is_empty() {
            // 为新增符号推断频率并拉取历史，然后立刻计算预测
            if let Err(err) = self.update_added_symbols_history_and_freq() { warn!("mock 初次加载新增符号失败: {err:?}"); }
            self.compute_predictions();
        }
        Ok(())
    }

    fn load_thresholds(&self) -> Result<Vec<SymbolThreshold>> {
        self.load_from_redis()
    }

    fn load_from_redis(&self) -> Result<Vec<SymbolThreshold>> {
        use redis::Commands;
        let settings = self
            .cfg
            .redis
            .clone()
            .ok_or_else(|| anyhow!("Redis 模式需要配置 redis 设置"))?;
        let key = self
            .cfg
            .redis_key
            .clone()
            .unwrap_or_else(|| DEFAULT_REDIS_HASH_KEY.to_string());
        let url = settings.connection_url();
        let client = redis::Client::open(url.clone())?;
        let mut con = client.get_connection()?;
        let full_key = match &settings.prefix {
            Some(p) if !p.is_empty() => format!("{}{}", p, key),
            _ => key,
        };
        let map: std::collections::HashMap<String, String> = con.hgetall(full_key)?;
        let mut result = Vec::new();
        for (sym, raw) in map {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
                let spot_symbol = sym.to_uppercase();
                let futures_symbol = v
                    .get("futures_symbol")
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_uppercase())
                    .unwrap_or_else(|| spot_symbol.clone());
                let open_threshold = v
                    .get("bidask_sr_open_threshold")
                    .and_then(|x| x.as_f64())
                    .unwrap_or(0.0);
                let close_threshold = v
                    .get("bidask_sr_close_threshold")
                    .and_then(|x| x.as_f64())
                    .unwrap_or(open_threshold);
                let askbid_open_threshold = v
                    .get("askbid_sr_open_threshold")
                    .and_then(|x| x.as_f64())
                    .unwrap_or(open_threshold);
                let askbid_close_threshold = v
                    .get("askbid_sr_close_threshold")
                    .and_then(|x| x.as_f64())
                    .unwrap_or(close_threshold);
                result.push(SymbolThreshold { spot_symbol, futures_symbol, open_threshold, close_threshold, askbid_open_threshold, askbid_close_threshold });
            }
        }
        if result.is_empty() { anyhow::bail!("Redis Hash 未解析到有效的 binance 交易对阈值"); }
        Ok(result)
    }

    // 已弃用：不再从本地 JSON 解析阈值

    fn handle_spot_quote(&mut self, msg: &[u8]) {
        let symbol = AskBidSpreadMsg::get_symbol(msg).to_uppercase();
        let Some(state) = self.symbols.get_mut(&symbol) else {
            return;
        };
        let timestamp = AskBidSpreadMsg::get_timestamp(msg);
        let bid_price = AskBidSpreadMsg::get_bid_price(msg);
        let ask_price = AskBidSpreadMsg::get_ask_price(msg);
        if bid_price <= 0.0 || ask_price <= 0.0 {
            return;
        }
        state.spot_quote.update(bid_price, ask_price, timestamp);
    }

    fn handle_futures_quote(&mut self, msg: &[u8]) {
        let fut_symbol = AskBidSpreadMsg::get_symbol(msg).to_uppercase();
        let Some(spot_key) = self.futures_index.get(&fut_symbol).cloned() else {
            return;
        };
        if let Some(state) = self.symbols.get_mut(&spot_key) {
            let timestamp = AskBidSpreadMsg::get_timestamp(msg);
            let bid_price = AskBidSpreadMsg::get_bid_price(msg);
            let ask_price = AskBidSpreadMsg::get_ask_price(msg);
            if bid_price <= 0.0 || ask_price <= 0.0 {
                return;
            }
            state.futures_quote.update(bid_price, ask_price, timestamp);
        }
    }

    fn handle_funding_rate(&mut self, msg: &[u8]) {
        let symbol = FundingRateMsg::get_symbol(msg).to_uppercase();
        let fut_symbol = self
            .symbols
            .get(&symbol)
            .map(|s| s.futures_symbol.clone())
            .unwrap_or_default();
        let predicted = self.get_predicted_for(&fut_symbol);
        if let Some(state) = self.symbols.get_mut(&symbol) {
            let funding = FundingRateMsg::get_funding_rate(msg);
            let next_funding_time = FundingRateMsg::get_next_funding_time(msg);
            let timestamp = FundingRateMsg::get_timestamp(msg);
            state.last_ratio = state.calc_ratio();
            state.funding_rate = funding;
            state.predicted_rate = predicted;
            state.loan_rate = 0.0;
            state.funding_ts = timestamp;
            state.next_funding_time = next_funding_time;
            debug!(
                "mock Funding 更新: {} funding={:.6} pred={:.6} next={} ts={}",
                symbol, funding, predicted, next_funding_time, timestamp
            );
        }
    }

    fn get_qty_step(&self, symbol: &str, spot_min: f64, futures_min: f64) -> f64 {
        let mut cache = self.qty_step_cache.borrow_mut();
        let entry = cache
            .entry(symbol.to_string())
            .or_insert_with(|| QtyStepInfo {
                spot_min,
                futures_min,
                step: compute_step(spot_min, futures_min),
            });
        if !approx_equal(entry.spot_min, spot_min) || !approx_equal(entry.futures_min, futures_min)
        {
            entry.spot_min = spot_min;
            entry.futures_min = futures_min;
            entry.step = compute_step(spot_min, futures_min);
        }
        if approx_zero(entry.step) {
            entry.step = spot_min.max(futures_min);
        }
        entry.step
    }
}

fn parse_index(token: &str) -> Result<usize> {
    let idx: usize = token
        .parse()
        .with_context(|| format!("无法解析索引: {token}"))?;
    if idx == 0 {
        anyhow::bail!("索引从 1 开始");
    }
    Ok(idx - 1)
}

fn format_price(value: f64) -> String {
    if value > 0.0 {
        format!("{:.6}", value)
    } else {
        "-".to_string()
    }
}

// note: removed format_ratio as we now print SR values directly

#[derive(Debug, Deserialize)]
struct BinanceFundingHistItem {
    #[serde(rename = "fundingRate")] funding_rate: String,
    #[serde(rename = "fundingTime")] funding_time: Option<i64>,
}

fn fetch_binance_funding_history_range_blocking(
    client: &Client,
    symbol: &str,
    start_time: i64,
    end_time: i64,
    limit: usize,
) -> AnyResult<Vec<f64>> {
    let url = "https://fapi.binance.com/fapi/v1/fundingRate";
    let end = end_time.max(0);
    let start = start_time.min(end).max(0);
    let limit_s = limit.max(1).min(1000).to_string();
    let resp = client
        .get(url)
        .query(&[
            ("symbol", symbol),
            ("startTime", &start.to_string()),
            ("endTime", &end.to_string()),
            ("limit", &limit_s),
        ])
        .send()?;
    if !resp.status().is_success() { return Ok(vec![]); }
    let mut items: Vec<BinanceFundingHistItem> = resp.json().unwrap_or_default();
    items.sort_by_key(|it| it.funding_time.unwrap_or_default());
    let mut out = Vec::with_capacity(items.len());
    for it in items { if let Ok(v) = it.funding_rate.parse::<f64>() { out.push(v); } }
    if out.len() > limit { let drop_n = out.len() - limit; let _ = out.drain(0..drop_n); }
    Ok(out)
}

fn fetch_binance_funding_items_blocking(client: &Client, symbol: &str, limit: usize) -> AnyResult<Vec<BinanceFundingHistItem>> {
    let url = "https://fapi.binance.com/fapi/v1/fundingRate";
    let end_time = Utc::now().timestamp_millis();
    let start_time = end_time - 3 * 24 * 3600 * 1000;
    let limit_s = limit.max(1).min(1000).to_string();
    let resp = client
        .get(url)
        .query(&[
            ("symbol", symbol),
            ("startTime", &start_time.to_string()),
            ("endTime", &end_time.to_string()),
            ("limit", &limit_s),
        ])
        .send()?;
    if !resp.status().is_success() { return Ok(vec![]); }
    let mut items: Vec<BinanceFundingHistItem> = resp.json().unwrap_or_default();
    items.sort_by_key(|it| it.funding_time.unwrap_or_default());
    Ok(items)
}

fn infer_binance_funding_frequency_blocking(client: &Client, symbol: &str) -> AnyResult<Option<String>> {
    let items = fetch_binance_funding_items_blocking(client, symbol, 40)?;
    let mut times: Vec<i64> = items.into_iter().filter_map(|it| it.funding_time).collect();
    if times.len() < 3 { return Ok(Some("8h".to_string())); }
    times.sort_unstable();
    let mut diffs: Vec<i64> = Vec::with_capacity(times.len().saturating_sub(1));
    for w in times.windows(2) { if let [a, b] = w { diffs.push(b - a); } }
    if diffs.is_empty() { return Ok(Some("8h".to_string())); }
    diffs.sort_unstable();
    let median = diffs[diffs.len()/2];
    let six_hours_ms = 6 * 3600 * 1000;
    if median <= six_hours_ms { Ok(Some("4h".to_string())) } else { Ok(Some("8h".to_string())) }
}

// note: removed compute_predict_local; compute_predictions now inlines calculation with debug logs

#[derive(Debug, Clone, Default)]
struct RateThresholds {
    open_upper: f64,
    open_lower: f64,
    close_lower: f64,
    close_upper: f64,
}

fn lcm_nonzero(a: f64, b: f64) -> f64 {
    let a_pos = if a > 0.0 { a } else { 0.0 };
    let b_pos = if b > 0.0 { b } else { 0.0 };
    if approx_zero(a_pos) {
        return b_pos;
    }
    if approx_zero(b_pos) {
        return a_pos;
    }
    match (to_fraction(a_pos), to_fraction(b_pos)) {
        (Some((num_a, den_a)), Some((num_b, den_b))) => {
            let scale = lcm_i64(den_a, den_b);
            if scale == 0 {
                return a_pos.max(b_pos);
            }
            let scaled_a = num_a * (scale / den_a);
            let scaled_b = num_b * (scale / den_b);
            let lcm_int = lcm_i64(scaled_a.abs(), scaled_b.abs());
            if lcm_int == 0 {
                a_pos.max(b_pos)
            } else {
                (lcm_int as f64) / (scale as f64)
            }
        }
        _ => a_pos.max(b_pos),
    }
}

fn compute_step(spot_min: f64, futures_min: f64) -> f64 {
    let step = lcm_nonzero(spot_min, futures_min);
    if approx_zero(step) {
        spot_min.max(futures_min)
    } else {
        step
    }
}

fn align_price_floor(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return price;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((price * tick_den as f64) + 1e-9).floor() as i128;
        let aligned_units = (units / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((price / tick) + 1e-9).floor();
    scaled * tick
}

fn align_price_ceil(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return price;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((price * tick_den as f64) - 1e-9).ceil() as i128;
        let aligned_units = ((units + tick_num - 1) / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((price / tick) - 1e-9).ceil();
    scaled * tick
}

fn to_fraction(value: f64) -> Option<(i64, i64)> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    let mut denom: i64 = 1;
    let mut scaled = value;
    for _ in 0..9 {
        let rounded = scaled.round();
        if (scaled - rounded).abs() < 1e-9 {
            return Some((rounded as i64, denom));
        }
        scaled *= 10.0;
        denom = denom.saturating_mul(10);
    }
    None
}

fn gcd_i64(mut a: i64, mut b: i64) -> i64 {
    while b != 0 {
        let tmp = a % b;
        a = b;
        b = tmp;
    }
    a.abs()
}

fn lcm_i64(a: i64, b: i64) -> i64 {
    if a == 0 || b == 0 {
        return 0;
    }
    (a / gcd_i64(a, b)).saturating_mul(b).abs()
}

fn approx_zero(x: f64) -> bool {
    x.abs() < 1e-12
}

fn approx_equal(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-12
}

fn render_three_line_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let widths = compute_widths(headers, rows);
    let mut out = String::new();
    out.push_str(&build_separator(&widths, '-'));
    out.push('\n');
    out.push_str(&build_row(
        headers
            .iter()
            .map(|h| h.to_string())
            .collect::<Vec<String>>(),
        &widths,
    ));
    out.push('\n');
    out.push_str(&build_separator(&widths, '='));
    if rows.is_empty() {
        out.push('\n');
        out.push_str(&build_separator(&widths, '-'));
        return out;
    }
    for row in rows {
        out.push('\n');
        out.push_str(&build_row(row.clone(), &widths));
    }
    out.push('\n');
    out.push_str(&build_separator(&widths, '-'));
    out
}

// 阈值由 MockController 内部维护（从 Redis 参数读取），见 thresholds_for_frequency 方法

fn compute_widths(headers: &[&str], rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths = headers.iter().map(|h| h.len()).collect::<Vec<_>>();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }
    widths
}

fn build_separator(widths: &[usize], sep: char) -> String {
    let mut line = String::new();
    line.push('+');
    for width in widths {
        line.push_str(&sep.to_string().repeat(width + 2));
        line.push('+');
    }
    line
}

fn build_row(cells: Vec<String>, widths: &[usize]) -> String {
    let mut row = String::new();
    row.push('|');
    for (cell, width) in cells.iter().zip(widths.iter()) {
        row.push(' ');
        row.push_str(&format!("{:<width$}", cell, width = *width));
        row.push(' ');
        row.push('|');
    }
    row
}

fn spawn_command_reader(tx: mpsc::UnboundedSender<String>) {
    thread::spawn(move || {
        let stdin = io::stdin();
        let mut handle = stdin.lock();
        loop {
            let mut line = String::new();
            match handle.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let cmd = line.trim().to_string();
                    if cmd.is_empty() {
                        continue;
                    }
                    if tx.send(cmd).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    eprintln!("读取命令失败: {err}");
                    break;
                }
            }
        }
    });
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let default_filter = "info,funding_rate_strategy_mock=debug,mkt_signal=info,hyper=off,hyper_util=off,h2=off,reqwest=warn";
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_filter)).init();

    info!("启动 Funding Rate Mock 控制台");

    let cfg = StrategyConfig::load()?;
    let publisher = SignalPublisher::new(SIGNAL_CHANNEL_MT_ARBITRAGE)?;
    let mut controller = MockController::new(cfg.clone(), publisher).await?;
    controller.print_help();
    controller.print_symbol_snapshot();
    controller.print_funding_overview_table();

    let mut subscriber = MultiChannelSubscriber::new(NODE_FUNDING_STRATEGY_SUB)?;
    subscriber.subscribe_channels(vec![
        SubscribeParams {
            exchange: "binance".to_string(),
            channel: ChannelType::AskBidSpread,
        },
        SubscribeParams {
            exchange: "binance-futures".to_string(),
            channel: ChannelType::AskBidSpread,
        },
        SubscribeParams {
            exchange: "binance-futures".to_string(),
            channel: ChannelType::Derivatives,
        },
    ])?;
    info!("已订阅 Binance 现货/合约行情与资金费率频道");

    let shutdown = CancellationToken::new();
    setup_signal_handlers(&shutdown)?;

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
    spawn_command_reader(cmd_tx);

    info!("输入 help 查看指令，或者直接输入索引选择交易对开仓");

    let shutdown_listener = shutdown.clone();
    let mut ticker = interval(Duration::from_millis(100));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown_listener.cancelled() => {
                info!("收到退出信号，mock 控制台关闭");
                break;
            }
            maybe_cmd = cmd_rx.recv() => {
                match maybe_cmd {
                    Some(cmd) => {
                        match controller.handle_command(&cmd).await {
                            Ok(true) => {
                                shutdown.cancel();
                                break;
                            }
                            Ok(false) => {}
                            Err(err) => error!("处理指令失败: {err:?}"),
                        }
                    }
                    None => {
                        info!("命令通道结束");
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                controller.on_tick(&mut subscriber);
            }
        }
    }

    info!("mock 进程结束");
    Ok(())
}

fn setup_signal_handlers(token: &CancellationToken) -> Result<()> {
    let ctrl_c = token.clone();
    tokio::spawn(async move {
        if let Err(e) = signal::ctrl_c().await {
            error!("监听 Ctrl+C 失败: {}", e);
            return;
        }
        info!("接收到 Ctrl+C 信号");
        ctrl_c.cancel();
    });

    #[cfg(unix)]
    {
        let terminate = token.clone();
        tokio::spawn(async move {
            match unix_signal(SignalKind::terminate()) {
                Ok(mut stream) => {
                    if stream.recv().await.is_some() {
                        info!("接收到 SIGTERM 信号");
                        terminate.cancel();
                    }
                }
                Err(e) => error!("监听 SIGTERM 失败: {}", e),
            }
        });
    }

    Ok(())
}
