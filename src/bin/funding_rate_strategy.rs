use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use reqwest::Client;
use serde::Deserialize;
use tokio::signal;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio::task::yield_now;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use mkt_signal::common::iceoryx_publisher::SignalPublisher;
use mkt_signal::common::iceoryx_subscriber::{
    ChannelType, MultiChannelSubscriber, SubscribeParams,
};
use mkt_signal::common::min_qty_table::MinQtyTable;
use mkt_signal::common::redis_client::{RedisClient, RedisSettings};
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::mkt_msg::{self, AskBidSpreadMsg, FundingRateMsg, MktMsgType};
use mkt_signal::pre_trade::order_manager::{OrderType, Side};
use mkt_signal::signal::binance_forward_arb::{
    BinSingleForwardArbCloseMarginCtx, BinSingleForwardArbOpenCtx,
};
use mkt_signal::signal::resample::{
    compute_askbid_sr, compute_bidask_sr, ResampleBatch, ResampleItem, FR_RESAMPLE_CHANNEL,
    FR_RESAMPLE_MSG_CHANNEL,
};
use mkt_signal::signal::trade_signal::{SignalType, TradeSignal};

const SIGNAL_CHANNEL_MT_ARBITRAGE: &str = "mt_arbitrage";
const NODE_FUNDING_STRATEGY_SUB: &str = "funding_rate_strategy";
const DEFAULT_CFG_PATH: &str = "config/funding_rate_strategy.toml";
const DEFAULT_REDIS_HASH_KEY: &str = "binance_arb_price_spread_threshold";

// 本策略已移除本地 tracking_symbol.json 模式，仅支持 Redis 阈值

/// 下单相关配置
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

/// 信号节流配置
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

/// 阈值热更新配置
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
    #[serde(default = "default_resample_ms")]
    resample_ms: u64,
    /// funding rate 滚动均值窗口大小（条数）
    #[serde(default = "default_funding_ma_size")]
    funding_ma_size: usize,
    /// 结算偏移（秒）：基于 UTC 准点（4h 周期）增加的偏移量
    #[serde(default = "default_settlement_offset_secs")]
    settlement_offset_secs: i64,
}

const fn default_interval() -> usize {
    6
}
const fn default_compute_secs() -> u64 {
    30
}
const fn default_fetch_secs() -> u64 {
    7200
}
const fn default_fetch_offset_secs() -> u64 {
    120
}
const fn default_fetch_limit() -> usize {
    100
}
const fn default_resample_ms() -> u64 {
    3000
}
const fn default_funding_ma_size() -> usize {
    60
}
const fn default_settlement_offset_secs() -> i64 {
    0
}

/// 策略总体配置
#[derive(Debug, Clone, Deserialize)]
struct StrategyConfig {
    #[allow(dead_code)]
    #[serde(default)]
    redis: Option<RedisSettings>,
    /// Redis key 存储追踪的价差阈值（无需本地 tracking JSON）
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
    #[serde(default)]
    loan: LoanConfig,
}

impl StrategyConfig {
    fn load() -> Result<Self> {
        let cfg_path =
            std::env::var("FUNDING_RATE_CFG").unwrap_or_else(|_| DEFAULT_CFG_PATH.to_string());
        let cfg: StrategyConfig = match std::fs::read_to_string(&cfg_path) {
            Ok(content) => {
                let mut cfg: StrategyConfig = toml::from_str(&content)
                    .with_context(|| format!("解析配置文件失败: {}", cfg_path))?;
                if cfg.redis.is_none() {
                    cfg.redis = Some(RedisSettings::default());
                }
                cfg
            }
            Err(err) => {
                warn!("未找到配置文件或读取失败({err}); 将使用默认配置并从 Redis 读取参数");
                StrategyConfig {
                    redis: Some(RedisSettings::default()),
                    redis_key: None,
                    order: OrderConfig::default(),
                    signal: SignalConfig::default(),
                    reload: ReloadConfig::default(),
                    strategy: StrategyParams {
                        interval: default_interval(),
                        predict_num: 0,
                        refresh_secs: default_compute_secs(),
                        fetch_secs: default_fetch_secs(),
                        fetch_offset_secs: default_fetch_offset_secs(),
                        history_limit: default_fetch_limit(),
                        resample_ms: default_resample_ms(),
                        funding_ma_size: default_funding_ma_size(),
                        settlement_offset_secs: default_settlement_offset_secs(),
                    },
                    loan: LoanConfig::default(),
                }
            }
        };
        // funding_ma_size 固定为 60；结算偏移与 fetch_offset_secs 对齐
        let mut cfg = cfg;
        cfg.strategy.funding_ma_size = 60;
        cfg.strategy.settlement_offset_secs = cfg.strategy.fetch_offset_secs as i64;
        if let Some(redis_cfg) = cfg.redis.as_ref() {
            info!(
                "Redis 数据源配置: host={} port={} db={} prefix={:?}",
                redis_cfg.host, redis_cfg.port, redis_cfg.db, redis_cfg.prefix
            );
        }
        info!(
            "策略配置加载完成: reload_interval={}s strategy: interval={} predict_num={} refresh_secs={}s fetch_secs={}s fetch_offset={}s history_limit={} settlement_offset_secs={}",
            cfg.reload.interval_secs,
            cfg.strategy.interval, cfg.strategy.predict_num, cfg.strategy.refresh_secs,
            cfg.strategy.fetch_secs, cfg.strategy.fetch_offset_secs, cfg.strategy.history_limit,
            cfg.strategy.settlement_offset_secs
        );
        Ok(cfg)
    }

    // reload_interval() helper was unused; next_reload scheduling uses cfg.reload.interval_secs directly

    fn max_open_keep_us(&self) -> i64 {
        (self.order.max_open_order_keep_s.max(1) * 1_000_000) as i64
    }

    fn max_close_keep_us(&self) -> i64 {
        (self.order.max_close_order_keep_s.max(1) * 1_000_000) as i64
    }

    fn min_signal_gap_us(&self) -> i64 {
        (self.signal.min_interval_ms * 1_000) as i64
    }
}

/// 价差阈值配置
#[derive(Debug, Clone)]
struct SymbolThreshold {
    spot_symbol: String,
    futures_symbol: String,
    // BidAskSR 阈值（开/关）: (spot_bid - fut_ask) / spot_bid
    open_threshold: f64,
    close_threshold: f64,
    // AskBidSR 阈值（开/关）: (spot_ask - fut_bid) / spot_ask
    askbid_open_threshold: f64,
    askbid_close_threshold: f64,
}

/// 行情报价
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

/// 单个交易对的运行时状态
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
    funding_ma: Option<f64>,
    predicted_signal: i32,
    ma_signal: i32,
    final_signal_value: i32,
    latest_bidask_sr: Option<f64>,
    latest_askbid_sr: Option<f64>,
    price_open_bidask: bool,
    price_open_askbid: bool,
    price_close_bidask: bool,
    price_close_askbid: bool,
    price_open_ready: bool,
    price_close_ready: bool,
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
            funding_ma: None,
            predicted_signal: 0,
            ma_signal: 0,
            final_signal_value: 0,
            latest_bidask_sr: None,
            latest_askbid_sr: None,
            price_open_bidask: false,
            price_open_askbid: false,
            price_close_bidask: false,
            price_close_askbid: false,
            price_open_ready: false,
            price_close_ready: false,
        }
    }

    fn update_threshold(&mut self, threshold: SymbolThreshold) {
        self.open_threshold = threshold.open_threshold;
        self.close_threshold = threshold.close_threshold;
        self.futures_symbol = threshold.futures_symbol;
        self.askbid_open_threshold = threshold.askbid_open_threshold;
        self.askbid_close_threshold = threshold.askbid_close_threshold;
    }

    fn ready_for_eval(&self) -> bool {
        self.spot_quote.is_ready() && self.futures_quote.is_ready()
    }

    /// bidask_sr = (spot_bid - futures_ask) / spot_bid
    fn calc_ratio(&self) -> Option<f64> {
        if self.spot_quote.bid <= 0.0 || self.futures_quote.ask <= 0.0 {
            return None;
        }
        Some((self.spot_quote.bid - self.futures_quote.ask) / self.spot_quote.bid)
    }

    fn can_emit_signal(&self, now_us: i64, min_gap_us: i64) -> bool {
        if min_gap_us <= 0 {
            return true;
        }
        match self.last_signal_ts {
            Some(prev) => now_us.saturating_sub(prev) >= min_gap_us,
            None => true,
        }
    }

    fn mark_signal(&mut self, now_us: i64) {
        self.last_signal_ts = Some(now_us);
    }
}

/// 统计信息
#[derive(Debug, Default)]
struct EngineStats {
    open_signals: u64,
    close_signals: u64,
}

#[derive(Debug, Clone)]
struct EvaluateDecision {
    symbol_key: String,
    final_signal: i32,
    price_open_ready: bool,
    price_close_ready: bool,
    price_open_bidask: bool,
    price_open_askbid: bool,
    price_close_bidask: bool,
    price_close_askbid: bool,
    can_emit: bool,
    position: PositionState,
    ratio: f64,
}

#[derive(Debug, Clone)]
struct QtyStepInfo {
    spot_min: f64,
    futures_min: f64,
    step: f64,
}

/// 主策略执行引擎
struct StrategyEngine {
    cfg: StrategyConfig,
    publisher: SignalPublisher,
    symbols: HashMap<String, SymbolState>,
    futures_index: HashMap<String, String>,
    next_reload: Instant,
    stats: EngineStats,
    min_qty: MinQtyTable,
    qty_step_cache: RefCell<HashMap<String, QtyStepInfo>>,
    history_map: HashMap<String, Vec<f64>>,
    predicted_map: HashMap<String, f64>,
    next_compute_refresh: Instant,
    next_fetch_refresh: Instant,
    http: Client,
    // resample
    resample_interval: Duration,
    next_resample: Instant,
    resample_pub: SignalPublisher,
    resample_msg_pub: SignalPublisher,
    funding_series: HashMap<String, Vec<f64>>, // per symbol recent funding rates
    loan_map: HashMap<String, f64>,            // per symbol loan rate 8h
    next_loan_refresh: Instant,
    // funding thresholds maintained in program only
    funding_thresholds: HashMap<String, FundingThresholdEntry>,
    funding_frequency: HashMap<String, String>, // fut_symbol -> "4h" | "8h"
    last_params: Option<ParamsSnapshot>,        // last loaded params from Redis
    last_settlement_marker_ms: Option<i64>,     // last settlement time handled
    th_4h: RateThresholds,
    th_8h: RateThresholds,
    // warmup: 是否所有符号都达到资金费率均值所需的历史点数
    warmup_done: bool,
}

impl StrategyEngine {
    fn next_fetch_instant(&self) -> Instant {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let fetch = self.cfg.strategy.fetch_secs.max(600);
        let offset = self.cfg.strategy.fetch_offset_secs.min(fetch - 1);
        let next_slot = ((now / fetch) + 1) * fetch + offset;
        let dur = next_slot.saturating_sub(now);
        Instant::now() + Duration::from_secs(dur)
    }

    fn next_fetch_epoch_secs(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let fetch = self.cfg.strategy.fetch_secs.max(600);
        let offset = self.cfg.strategy.fetch_offset_secs.min(fetch - 1);
        ((now / fetch) + 1) * fetch + offset
    }

    async fn fetch_histories(&mut self) -> Result<()> {
        debug!("开始拉取 funding 历史");
        let mut fut_syms: Vec<String> = self
            .symbols
            .values()
            .map(|s| s.futures_symbol.clone())
            .collect();
        fut_syms.sort();
        fut_syms.dedup();
        let client = self.http.clone();
        let limit = self.cfg.strategy.history_limit;
        let mut tasks = Vec::new();
        for sym in fut_syms {
            let s = sym.clone();
            let c = client.clone();
            // 根据已知频率推断时间窗口；未知默认 8h
            let freq = self
                .funding_frequency
                .get(&s.to_uppercase())
                .cloned()
                .unwrap_or_else(|| "8h".to_string());
            let end_time = Utc::now().timestamp_millis();
            let hours = if freq.eq_ignore_ascii_case("4h") {
                4
            } else {
                8
            };
            let window_ms = (hours as i64) * 3600 * 1000 * (limit as i64 + 2);
            let start_time = end_time.saturating_sub(window_ms);
            tasks.push(tokio::spawn(async move {
                let rates =
                    fetch_binance_funding_history_range(&c, &s, start_time, end_time, limit)
                        .await
                        .unwrap_or_default();
                (s, rates)
            }));
        }
        let mut new_history = HashMap::new();
        for t in tasks {
            if let Ok((s, rates)) = t.await {
                // no per-symbol count logging to avoid noise
                new_history.insert(s, rates);
            }
        }
        self.history_map = new_history;
        debug!("历史更新完成, symbols={}", self.history_map.len());
        // warmup 检查：若已完成，则每次 fetch 都重算预测并打印资金费率总览；未完成则打印进度
        let was = self.warmup_done;
        self.warmup_done = self.is_warmup_complete();
        if self.warmup_done {
            if !was {
                debug!("warmup 完成: 所有符号资金费率均值达到窗口");
            }
            self.compute_predictions();
            self.print_funding_overview_table();
        } else {
            self.print_warmup_progress_table();
        }
        Ok(())
    }

    fn compute_predictions(&mut self) {
        // 预热未完成则不计算预测
        if !self.is_warmup_complete() {
            debug!("预测计算跳过: warmup 未完成");
            return;
        }
        let interval = self.cfg.strategy.interval.max(1);
        let predict_num = self.cfg.strategy.predict_num;
        let mut out = HashMap::new();
        for (sym, rates) in &self.history_map {
            let n = rates.len();
            let pred = if n == 0 {
                debug!("pred calc: {sym} len=0 => 0");
                0.0
            } else if interval == 0 {
                debug!("pred calc: {sym} interval=0 => 0");
                0.0
            } else if n - 1 < predict_num {
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
            };
            out.insert(sym.clone(), pred);
        }
        self.predicted_map = out;
        if !self.predicted_map.is_empty() {
            let mut sample: Vec<(&String, &f64)> = self.predicted_map.iter().take(5).collect();
            sample.sort_by(|a, b| a.0.cmp(b.0));
            debug!(
                "预测更新完成: {} 项, 示例: {:?}",
                self.predicted_map.len(),
                sample
            );
        } else {
            debug!("预测更新完成: 空");
        }
    }

    fn get_predicted_for(&self, fut_symbol: &str) -> f64 {
        self.predicted_map
            .get(&fut_symbol.to_uppercase())
            .copied()
            .unwrap_or(0.0)
    }
    async fn new(cfg: StrategyConfig, publisher: SignalPublisher) -> Result<Self> {
        let resample_ms = cfg.strategy.resample_ms.max(3000);
        let mut engine = Self {
            next_reload: Instant::now(),
            cfg,
            publisher,
            symbols: HashMap::new(),
            futures_index: HashMap::new(),
            stats: EngineStats::default(),
            min_qty: MinQtyTable::new(),
            qty_step_cache: RefCell::new(HashMap::new()),
            history_map: HashMap::new(),
            predicted_map: HashMap::new(),
            next_compute_refresh: Instant::now(),
            next_fetch_refresh: Instant::now(),
            http: Client::new(),
            resample_interval: Duration::from_millis(resample_ms),
            next_resample: Instant::now(),
            resample_pub: SignalPublisher::new(FR_RESAMPLE_CHANNEL)?,
            resample_msg_pub: SignalPublisher::new(FR_RESAMPLE_MSG_CHANNEL)?,
            funding_series: HashMap::new(),
            loan_map: HashMap::new(),
            next_loan_refresh: Instant::now(),
            funding_thresholds: HashMap::new(),
            funding_frequency: HashMap::new(),
            last_params: None,
            last_settlement_marker_ms: None,
            th_4h: RateThresholds::for_4h(),
            th_8h: RateThresholds::for_8h(),
            warmup_done: false,
        };
        engine.min_qty.refresh_binance().await?;
        // 首次读取参数（来自 Redis），然后加载符号列表
        let _ = engine.reload_params_if_changed().await;
        let _ = engine.reload_thresholds().await?;
        engine.next_fetch_refresh = engine.next_fetch_instant();
        // 打印下次拉取历史的 UTC 对齐时刻（fetch_secs + fetch_offset_secs）
        let next_epoch = engine.next_fetch_epoch_secs() as i64;
        if let Some(dt) = DateTime::<Utc>::from_timestamp(next_epoch, 0) {
            info!("下次拉取历史(UTC): {}", dt.format("%Y-%m-%d %H:%M:%S"));
        } else {
            info!("下次拉取历史 epoch: {}", next_epoch);
        }
        // 初始 warmup 检查
        engine.warmup_done = engine.is_warmup_complete();
        Ok(engine)
    }

    async fn maybe_reload(&mut self) {
        // 按 refresh_secs 刷新：读取参数 -> 刷新符号 -> 重算阈值（并打印三线表）
        if Instant::now() >= self.next_reload {
            if let Err(err) = self.min_qty.refresh_binance().await {
                warn!("刷新最小下单量失败: {err:?}");
            }
            let params_changed = self.reload_params_if_changed().await.unwrap_or(false);
            let symbols_changed = match self.reload_thresholds().await {
                Ok(changed) => changed,
                Err(err) => {
                    error!("刷新追踪列表失败: {err:?}");
                    false
                }
            };
            let settlement_triggered = self.is_settlement_trigger();
            let mut reasons: Vec<&str> = Vec::new();
            if symbols_changed {
                reasons.push("符号增加或移除");
            }
            if params_changed {
                reasons.push("参数修改");
            }
            if settlement_triggered {
                reasons.push("结算点触发");
            }
            debug!(
                "refresh 触发: params_changed={} symbols_changed={} settlement_triggered={}",
                params_changed, symbols_changed, settlement_triggered
            );
            if !reasons.is_empty() {
                // 结算点触发: 仅更新频率与阈值表，不重算预测（预测仅在 fetch 对齐点或参数变更时重算）
                if settlement_triggered {
                    if let Err(err) = self.refresh_frequency_all().await {
                        debug!("刷新频率推断失败: {err:#}");
                    }
                }
                self.recompute_and_log(&reasons.join("、"), symbols_changed)
                    .await;
            }
        }
        // 到点拉取历史（fetch_secs 边界）并在 warmup 完成后重算预测
        if Instant::now() >= self.next_fetch_refresh {
            if let Err(err) = self.fetch_histories().await {
                warn!("拉取资金费率历史失败: {err:?}");
            }
            self.next_fetch_refresh = self.next_fetch_instant();
        }
        // 按 refresh_secs 仅检测参数变更，变更时才重算预测
        if Instant::now() >= self.next_compute_refresh {
            let params_changed = self.reload_params_if_changed().await.unwrap_or(false);
            if params_changed && self.is_warmup_complete() {
                self.compute_predictions();
                self.print_funding_overview_table();
            }
            self.next_compute_refresh =
                Instant::now() + Duration::from_secs(self.cfg.strategy.refresh_secs.max(5));
        }
        // 到点刷新借贷利率（Redis）
        if Instant::now() >= self.next_loan_refresh {
            if let Err(err) = self.reload_loan_rates().await {
                warn!("刷新借贷利率失败: {err:#}");
            }
            let gap = self.cfg.loan.refresh_secs.max(5);
            self.next_loan_refresh = Instant::now() + Duration::from_secs(gap);
        }
    }

    async fn refresh_frequency_all(&mut self) -> Result<()> {
        if self.symbols.is_empty() {
            return Ok(());
        }
        let client = self.http.clone();
        let mut tasks = Vec::new();
        let futs: Vec<String> = self
            .symbols
            .values()
            .map(|s| s.futures_symbol.to_uppercase())
            .collect();
        for fut in futs {
            let c = client.clone();
            let s = fut.clone();
            tasks.push(tokio::spawn(async move {
                (s.clone(), infer_binance_funding_frequency(&c, &s).await)
            }));
        }
        for t in tasks {
            if let Ok((fut, freq)) = t.await {
                if let Some(f) = freq {
                    debug!("频率刷新: {} -> {}", fut, f);
                    self.funding_frequency.insert(fut, f);
                }
            }
        }
        Ok(())
    }

    async fn reload_thresholds(&mut self) -> Result<bool> {
        // Redis-only: refresh thresholds by strategy.refresh_secs
        let reload_dur = Duration::from_secs(self.cfg.reload.interval_secs.max(5));
        self.next_reload = Instant::now() + reload_dur;
        debug!("开始刷新追踪交易对(阈值表) ...");
        let entries = self.load_thresholds().await?;
        debug!("阈值表条目数: {}", entries.len());
        let mut new_symbols: HashMap<String, SymbolState> = HashMap::new();
        let mut new_futures_index: HashMap<String, String> = HashMap::new();
        let mut added: Vec<(String, String)> = Vec::new(); // (spot, fut)

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

        let removed = self.symbols.len();
        self.symbols = new_symbols;
        self.futures_index = new_futures_index;
        self.qty_step_cache
            .borrow_mut()
            .retain(|symbol, _| self.symbols.contains_key(symbol));
        if removed > 0 {
            info!("移除 {} 个不再跟踪的交易对", removed);
        }
        info!("本次加载追踪交易对数量: {}", self.symbols.len());
        if !added.is_empty() {
            debug!("新增交易对: {} 个", added.len());
        }
        self.log_min_qty_table();
        // 对新增的符号：推断 freq，并拉取历史，更新预测
        if !added.is_empty() {
            let client = self.http.clone();
            let limit = self.cfg.strategy.history_limit;
            let mut tasks = Vec::new();
            for (_spot, fut) in &added {
                let s = fut.clone();
                let c = client.clone();
                tasks.push(tokio::spawn(async move {
                    let freq = infer_binance_funding_frequency(&c, &s)
                        .await
                        .unwrap_or_else(|| "8h".to_string());
                    let end_time = Utc::now().timestamp_millis();
                    let hours = if freq.eq_ignore_ascii_case("4h") {
                        4
                    } else {
                        8
                    };
                    let window_ms = (hours as i64) * 3600 * 1000 * (limit as i64 + 2);
                    let start_time = end_time.saturating_sub(window_ms);
                    let rates =
                        fetch_binance_funding_history_range(&c, &s, start_time, end_time, limit)
                            .await
                            .unwrap_or_default();
                    (s, freq, rates)
                }));
            }
            for t in tasks {
                if let Ok((fut, freq, rates)) = t.await {
                    debug!(
                        "新增符号初始化: {} freq={} history={}条",
                        fut,
                        freq,
                        rates.len()
                    );
                    self.funding_frequency.insert(fut.to_uppercase(), freq);
                    if !rates.is_empty() {
                        self.history_map.insert(fut.to_uppercase(), rates);
                    }
                }
            }
            self.compute_predictions();
        }
        Ok(!added.is_empty() || removed > 0)
    }

    async fn load_thresholds(&self) -> Result<Vec<SymbolThreshold>> {
        self.load_from_redis().await
    }

    async fn load_from_redis(&self) -> Result<Vec<SymbolThreshold>> {
        let settings = self
            .cfg
            .redis
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Redis 模式需要配置 redis 设置"))?;
        let key = self
            .cfg
            .redis_key
            .clone()
            .unwrap_or_else(|| DEFAULT_REDIS_HASH_KEY.to_string());
        let mut client = RedisClient::connect(settings.clone()).await?;
        // HGETALL to fetch per-symbol thresholds JSON
        let map = client.hgetall_map(&key).await?;
        let mut result = Vec::new();
        for (sym, raw) in map {
            // Expect payload like { symbol, update_tp, bidask_sr_open_threshold, bidask_sr_close_threshold, ... }
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
                result.push(SymbolThreshold {
                    spot_symbol,
                    futures_symbol,
                    open_threshold,
                    close_threshold,
                    askbid_open_threshold,
                    askbid_close_threshold,
                });
            }
        }
        if result.is_empty() {
            anyhow::bail!("Redis Hash 未解析到任何有效的 binance 交易对阈值")
        }
        Ok(result)
    }

    fn log_min_qty_table(&self) {
        if self.symbols.is_empty() {
            info!("未追踪任何交易对，跳过最小下单量日志");
            return;
        }
        let mut keys: Vec<String> = self.symbols.keys().cloned().collect();
        keys.sort();
        let mut rows: Vec<Vec<String>> = Vec::new();
        for key in keys {
            let Some(state) = self.symbols.get(&key) else {
                continue;
            };
            let spot_min = self
                .min_qty
                .spot_min_qty_by_symbol(&state.spot_symbol)
                .unwrap_or(0.0);
            let futures_min = self
                .min_qty
                .futures_um_min_qty_by_symbol(&state.futures_symbol)
                .unwrap_or(0.0);
            rows.push(vec![
                key.clone(),
                state.spot_symbol.clone(),
                state.futures_symbol.clone(),
                format!("{:.8}", spot_min),
                format!("{:.8}", futures_min),
            ]);
        }
        if rows.is_empty() {
            info!("最小下单量: 未找到匹配条目");
            return;
        }
        let table = render_three_line_table(
            &[
                "Key",
                "SpotSymbol",
                "FuturesSymbol",
                "SpotMinQty",
                "UMMinQty",
            ],
            &rows,
        );
        info!("最小下单量快照\n{}", table);
    }

    fn log_symbol_snapshot(&self) {
        let mut keys: Vec<String> = self.symbols.keys().cloned().collect();
        keys.sort();
        let mut rows: Vec<Vec<String>> = Vec::new();
        for key in keys {
            let Some(state) = self.symbols.get(&key) else {
                continue;
            };
            // 两个价差因子（若盘口未准备好，则显示为0）
            let bidask_sr =
                compute_bidask_sr(Some(state.spot_quote.bid), Some(state.futures_quote.ask))
                    .unwrap_or(0.0);
            let askbid_sr =
                compute_askbid_sr(Some(state.spot_quote.ask), Some(state.futures_quote.bid))
                    .unwrap_or(0.0);
            rows.push(vec![
                key.clone(),
                format!("{:.6}", state.spot_quote.bid),
                format!("{:.6}", state.spot_quote.ask),
                format!("{:.6}", state.futures_quote.bid),
                format!("{:.6}", state.futures_quote.ask),
                format!("{:.6}", bidask_sr),
                format!("{:.6}", askbid_sr),
                format!("{:.6}", state.open_threshold),
                format!("{:.6}", state.close_threshold),
                format!("{:.6}", state.askbid_open_threshold),
                format!("{:.6}", state.askbid_close_threshold),
            ]);
        }

        if rows.is_empty() {
            info!("snapshot: 当前无可用盘口/价差信息");
            return;
        }

        let table = render_three_line_table(
            &[
                "Symbol",
                "SpotBid",
                "SpotAsk",
                "UMBid",
                "UMAsk",
                "BidAskSR",
                "AskBidSR",
                "BA_OpenTh",
                "BA_CloseTh",
                "AB_OpenTh",
                "AB_CloseTh",
            ],
            &rows,
        );
        info!("盘口/价差快照\n{}", table);
    }

    // 本策略不再从本地 JSON 解析阈值

    fn handle_spot_quote(&mut self, msg: &[u8]) {
        let symbol = AskBidSpreadMsg::get_symbol(msg).to_uppercase();
        let Some(state) = self.symbols.get_mut(&symbol) else {
            return;
        };
        let timestamp = AskBidSpreadMsg::get_timestamp(msg);
        let bid_price = AskBidSpreadMsg::get_bid_price(msg);
        let ask_price = AskBidSpreadMsg::get_ask_price(msg);
        if bid_price <= 0.0 || ask_price <= 0.0 {
            warn!(
                "Spot盘口异常: symbol={} bid={} ask={} ts={}",
                symbol, bid_price, ask_price, timestamp
            );
            return;
        }
        state.spot_quote.update(bid_price, ask_price, timestamp);
        self.evaluate(&symbol);
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
                warn!(
                    "Futures盘口异常: symbol={} bid={} ask={} ts={}",
                    fut_symbol, bid_price, ask_price, timestamp
                );
                return;
            }
            state.futures_quote.update(bid_price, ask_price, timestamp);
        }
        self.evaluate(&spot_key);
    }

    fn handle_funding_rate(&mut self, msg: &[u8]) {
        let fut_symbol = FundingRateMsg::get_symbol(msg).to_uppercase();
        let Some(spot_key) = self.futures_index.get(&fut_symbol).cloned() else {
            return;
        };

        let predicted = self.get_predicted_for(&fut_symbol);
        let funding = FundingRateMsg::get_funding_rate(msg);
        let next_funding_time = FundingRateMsg::get_next_funding_time(msg);
        let timestamp = FundingRateMsg::get_timestamp(msg);

        if let Some(state) = self.symbols.get_mut(&spot_key) {
            state.last_ratio = state.calc_ratio();
            state.funding_rate = funding;
            state.predicted_rate = predicted;
            state.loan_rate = 0.0;
            state.funding_ts = timestamp;
            state.next_funding_time = next_funding_time;
        } else {
            debug!(
                "Funding 更新: 现货 symbol={} 未在 symbols 中注册 (futures={})",
                spot_key, fut_symbol
            );
            return;
        }

        let entry_len = {
            let entry = self.funding_series.entry(spot_key.clone()).or_default();
            entry.push(funding);
            let max_keep = self.cfg.strategy.funding_ma_size.max(1) * 4; // 留存多点以便滑窗
            if entry.len() > max_keep {
                let drop_n = entry.len() - max_keep;
                entry.drain(0..drop_n);
            }
            entry.len()
        };

        let ma = self.calc_funding_ma(&spot_key);
        if let Some(state) = self.symbols.get_mut(&spot_key) {
            state.funding_ma = ma;
        }

        if log::log_enabled!(log::Level::Debug) {
            log::debug!(
                "Funding 更新: spot={} futures={} funding={:.6} pred={:.6} ma={:?} next={} ts={} len={}",
                spot_key,
                fut_symbol,
                funding,
                predicted,
                ma,
                next_funding_time,
                timestamp,
                entry_len
            );
        }
    }

    fn evaluate(&mut self, symbol: &str) {
        // 预热未完成则不触发任何开/平仓信号
        if !self.is_warmup_complete() {
            return;
        }
        let now_us = get_timestamp_us();
        let min_gap_us = self.cfg.min_signal_gap_us();

        let (open_upper, open_lower, close_lower, close_upper, _freq_value) =
            if let Some(entry) = self.funding_thresholds.get(symbol) {
                (
                    entry.open_upper_threshold,
                    entry.open_lower_threshold,
                    entry.close_lower_threshold,
                    entry.close_upper_threshold,
                    entry.funding_frequency.clone(),
                )
            } else {
                let fut_symbol_upper = self
                    .symbols
                    .get(symbol)
                    .map(|s| s.futures_symbol.to_uppercase())
                    .unwrap_or_default();
                let freq = self
                    .funding_frequency
                    .get(&fut_symbol_upper)
                    .cloned()
                    .unwrap_or_else(|| "8h".to_string());
                let thresholds = if freq.eq_ignore_ascii_case("4h") {
                    self.th_4h
                } else {
                    self.th_8h
                };
                (
                    thresholds.open_upper,
                    thresholds.open_lower,
                    thresholds.close_lower,
                    thresholds.close_upper,
                    freq,
                )
            };

        let predicted_now = if let Some(s) = self.symbols.get(symbol) {
            self.get_predicted_for(&s.futures_symbol)
        } else {
            return;
        };

        let decision = {
            let Some(state) = self.symbols.get_mut(symbol) else {
                return;
            };
            if !state.ready_for_eval() {
                return;
            }
            let Some(ratio) = state.calc_ratio() else {
                return;
            };

            state.predicted_rate = predicted_now;
            let askbid_sr =
                compute_askbid_sr(Some(state.spot_quote.ask), Some(state.futures_quote.bid));

            state.latest_bidask_sr = Some(ratio);
            state.latest_askbid_sr = askbid_sr;

            let funding_ma = state.funding_ma;

            let predicted_signal = if predicted_now >= open_upper {
                -1
            } else if predicted_now <= open_lower {
                1
            } else {
                0
            };
            let ma_signal = match funding_ma {
                Some(ma) if ma > close_upper => -2,
                Some(ma) if ma < close_lower => 2,
                _ => 0,
            };
            let final_signal = if ma_signal != 0 {
                ma_signal
            } else {
                predicted_signal
            };

            let price_open_bidask = ratio <= state.open_threshold;
            let price_open_askbid = if !state.askbid_open_threshold.is_finite()
                || state.askbid_open_threshold.abs() <= f64::EPSILON
            {
                true
            } else {
                askbid_sr.map_or(false, |v| v >= state.askbid_open_threshold)
            };
            let price_close_bidask = ratio >= state.close_threshold;
            let price_close_askbid = if !state.askbid_close_threshold.is_finite()
                || state.askbid_close_threshold.abs() <= f64::EPSILON
            {
                true
            } else {
                askbid_sr.map_or(false, |v| v <= state.askbid_close_threshold)
            };
            let price_open_ready = price_open_bidask && price_open_askbid;
            let price_close_ready = price_close_bidask && price_close_askbid;

            state.predicted_signal = predicted_signal;
            state.ma_signal = ma_signal;
            state.final_signal_value = final_signal;
            state.price_open_bidask = price_open_bidask;
            state.price_open_askbid = price_open_askbid;
            state.price_close_bidask = price_close_bidask;
            state.price_close_askbid = price_close_askbid;
            state.price_open_ready = price_open_ready;
            state.price_close_ready = price_close_ready;
            state.last_ratio = Some(ratio);

            EvaluateDecision {
                symbol_key: symbol.to_string(),
                final_signal,
                price_open_ready,
                price_close_ready,
                price_open_bidask,
                price_open_askbid,
                price_close_bidask,
                price_close_askbid,
                can_emit: state.can_emit_signal(now_us, min_gap_us),
                position: state.position,
                ratio,
            }
        };

        let mut request: Option<SignalRequest> = None;

        match decision.final_signal {
            -1 => {
                if decision.position != PositionState::Flat {
                    debug!(
                        "{} funding信号(-1)忽略: 当前持仓 {:?}",
                        decision.symbol_key, decision.position
                    );
                } else if !decision.price_open_ready {
                    debug!(
                        "{} funding信号(-1)忽略: 价差信号未满足 (bidask_ok={} askbid_ok={})",
                        decision.symbol_key, decision.price_open_bidask, decision.price_open_askbid
                    );
                } else if !decision.can_emit {
                    debug!(
                        "{} funding信号(-1)忽略: 触发节流 min_gap={}us",
                        decision.symbol_key, min_gap_us
                    );
                } else if let Some(state) = self.symbols.get(&decision.symbol_key) {
                    if let Some(req) = self.build_open_request(state, decision.ratio, now_us) {
                        request = Some(req);
                    } else {
                        debug!(
                            "{} funding信号(-1)构建开仓请求失败 (ratio={:.6})",
                            decision.symbol_key, decision.ratio
                        );
                    }
                }
            }
            -2 => {
                if decision.position != PositionState::Opened {
                    debug!(
                        "{} funding信号(-2)忽略: 当前持仓 {:?}",
                        decision.symbol_key, decision.position
                    );
                } else if !decision.price_close_ready {
                    debug!(
                        "{} funding信号(-2)忽略: 价差信号未满足 (bidask_ok={} askbid_ok={})",
                        decision.symbol_key,
                        decision.price_close_bidask,
                        decision.price_close_askbid
                    );
                } else if !decision.can_emit {
                    debug!(
                        "{} funding信号(-2)忽略: 触发节流 min_gap={}us",
                        decision.symbol_key, min_gap_us
                    );
                } else if let Some(state) = self.symbols.get(&decision.symbol_key) {
                    if let Some(req) = self.build_close_request(state, decision.ratio, now_us) {
                        request = Some(req);
                    } else {
                        debug!(
                            "{} funding信号(-2)构建平仓请求失败 (ratio={:.6})",
                            decision.symbol_key, decision.ratio
                        );
                    }
                }
            }
            1 | 2 => {
                debug!(
                    "{} funding信号({}) 仅记录 (未实现反向操作)",
                    decision.symbol_key, decision.final_signal
                );
            }
            _ => {}
        }

        if let Some(req) = request {
            match req {
                SignalRequest::Open {
                    symbol_key,
                    ctx,
                    ratio,
                    price,
                    emit_ts,
                } => {
                    match BinSingleForwardArbOpenCtx::from_bytes(ctx.clone()) {
                        Ok(open_ctx) => debug!(
                            "发送开仓信号: symbol={} amount={} side={:?} order_type={:?} price={:.8} tick={:.8} exp_time={}",
                            open_ctx.spot_symbol,
                            open_ctx.amount,
                            open_ctx.side,
                            open_ctx.order_type,
                            open_ctx.price,
                            open_ctx.price_tick,
                            open_ctx.exp_time
                        ),
                        Err(e) => debug!(
                            "发送开仓信号: 解析上下文失败 symbol={} err={}",
                            symbol_key, e
                        ),
                    }
                    if let Err(err) = self.publish_signal(SignalType::BinSingleForwardArbOpen, ctx)
                    {
                        error!("发送开仓信号失败 {}: {err:?}", symbol_key);
                        return;
                    }
                    if let Some(state) = self.symbols.get_mut(&symbol_key) {
                        state.position = PositionState::Opened;
                        state.last_ratio = Some(ratio);
                        state.last_open_ts = Some(emit_ts);
                        state.mark_signal(emit_ts);
                    }
                    self.stats.open_signals += 1;
                    info!(
                        "{} 触发开仓信号, spread_ratio={:.6}, 目标价格={:.6}",
                        symbol_key, ratio, price
                    );
                }
                SignalRequest::Close {
                    symbol_key,
                    ctx,
                    ratio,
                    price,
                    emit_ts,
                } => {
                    match BinSingleForwardArbCloseMarginCtx::from_bytes(ctx.clone()) {
                        Ok(close_ctx) => debug!(
                            "发送平仓信号: symbol={} limit_price={:.8} tick={:.8} exp_time={}",
                            close_ctx.spot_symbol,
                            close_ctx.limit_price,
                            close_ctx.price_tick,
                            close_ctx.exp_time
                        ),
                        Err(e) => debug!(
                            "发送平仓信号: 解析上下文失败 symbol={} err={}",
                            symbol_key, e
                        ),
                    }
                    if let Err(err) =
                        self.publish_signal(SignalType::BinSingleForwardArbCloseMargin, ctx)
                    {
                        error!("发送平仓信号失败 {}: {err:?}", symbol_key);
                        return;
                    }
                    if let Some(state) = self.symbols.get_mut(&symbol_key) {
                        state.position = PositionState::Flat;
                        state.last_ratio = Some(ratio);
                        state.last_close_ts = Some(emit_ts);
                        state.mark_signal(emit_ts);
                    }
                    self.stats.close_signals += 1;
                    info!(
                        "{} 触发平仓信号, spread_ratio={:.6}, 目标价格={:.6}",
                        symbol_key, ratio, price
                    );
                }
            }
        }
    }

    fn resample_and_publish(&mut self) {
        let ts_ms = (get_timestamp_us() / 1000) as i64;
        let mut batch_items: Vec<ResampleItem> = Vec::new();
        let mut price_rows: Vec<Vec<String>> = Vec::new();
        let mut funding_pred_rows: Vec<Vec<String>> = Vec::new();
        let mut funding_ma_rows: Vec<Vec<String>> = Vec::new();
        // 组装每个 symbol 的切片
        let mut keys: Vec<String> = self.symbols.keys().cloned().collect();
        keys.sort();
        for key in keys {
            let state = match self.symbols.get(&key) {
                Some(s) => s,
                None => continue,
            };
            let spot_bid = if state.spot_quote.bid > 0.0 {
                Some(state.spot_quote.bid)
            } else {
                None
            };
            let spot_ask = if state.spot_quote.ask > 0.0 {
                Some(state.spot_quote.ask)
            } else {
                None
            };
            let fut_bid = if state.futures_quote.bid > 0.0 {
                Some(state.futures_quote.bid)
            } else {
                None
            };
            let fut_ask = if state.futures_quote.ask > 0.0 {
                Some(state.futures_quote.ask)
            } else {
                None
            };
            let bidask_sr = compute_bidask_sr(spot_bid, fut_ask);
            let askbid_sr = compute_askbid_sr(spot_ask, fut_bid);
            let funding_rate = if state.funding_rate != 0.0 {
                Some(state.funding_rate)
            } else {
                None
            };
            let funding_rate_ma = self.calc_funding_ma(&key);
            let predicted_rate = Some(self.get_predicted_for(&state.futures_symbol));
            let loan_rate_8h = self.loan_map.get(&key).copied().or_else(|| {
                if state.loan_rate != 0.0 {
                    Some(state.loan_rate)
                } else {
                    None
                }
            });

            let item = ResampleItem {
                symbol: key.clone(),
                ts_ms,
                spot_bid,
                spot_ask,
                fut_bid,
                fut_ask,
                bidask_sr,
                askbid_sr,
                funding_rate,
                funding_rate_ma,
                predicted_rate,
                loan_rate_8h,
            };
            // 逐条发送到 msg 通道
            if let Ok(bytes) = item.to_bytes() {
                let mut buf = Vec::with_capacity(bytes.len() + 4);
                let len = bytes.len() as u32;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(&bytes);
                if buf.len() <= 1024 {
                    let _ = self.resample_msg_pub.publish(&buf);
                }
            }
            batch_items.push(item);

            let (open_upper, open_lower, close_lower, close_upper, freq) =
                if let Some(entry) = self.funding_thresholds.get(&key) {
                    (
                        entry.open_upper_threshold,
                        entry.open_lower_threshold,
                        entry.close_lower_threshold,
                        entry.close_upper_threshold,
                        entry.funding_frequency.clone(),
                    )
                } else {
                    let fut_key = state.futures_symbol.to_uppercase();
                    let freq = self
                        .funding_frequency
                        .get(&fut_key)
                        .cloned()
                        .unwrap_or_else(|| "8h".to_string());
                    let (ou, ol, cl, cu) = self.thresholds_for_frequency(&freq);
                    (ou, ol, cl, cu, freq)
                };

            let bidask_sr_val = bidask_sr.unwrap_or(0.0);
            let price_open_bidask = if state.price_open_bidask { "Y" } else { "N" };
            let price_close_bidask = if state.price_close_bidask { "Y" } else { "N" };
            let open_action = if state.price_open_ready {
                "现货做多 / 合约做空"
            } else {
                "-"
            };
            let close_action = if state.price_close_ready {
                "平仓现货多 / 合约空"
            } else {
                "-"
            };
            price_rows.push(vec![
                key.clone(),
                fmt_decimal(bidask_sr_val),
                price_open_bidask.to_string(),
                price_close_bidask.to_string(),
                open_action.to_string(),
                close_action.to_string(),
            ]);

            let predicted_value = state.predicted_rate;
            let pred_message = if state.predicted_signal == -1 {
                format!(
                    "满足 {:.6} >= {:.6} → 期货做空 / 现货做多",
                    predicted_value, open_upper
                )
            } else if state.predicted_signal == 1 {
                format!(
                    "满足 {:.6} <= {:.6} → 反向信号 (忽略)",
                    predicted_value, open_lower
                )
            } else {
                format!(
                    "未触发 {:.6} ∈ [{:.6}, {:.6}]",
                    predicted_value, open_lower, open_upper
                )
            };
            funding_pred_rows.push(vec![
                key.clone(),
                freq.clone(),
                fmt_decimal(predicted_value),
                fmt_decimal(open_upper),
                fmt_decimal(open_lower),
                pred_message,
            ]);

            let ma_value_opt = funding_rate_ma.or(state.funding_ma);
            let ma_value_str = ma_value_opt
                .map(fmt_decimal)
                .unwrap_or_else(|| "N/A".to_string());
            let ma_value = ma_value_opt.unwrap_or(0.0);
            let ma_message = if state.ma_signal == -2 {
                format!(
                    "满足 {:.6} > {:.6} → 平仓现货多 / 合约空",
                    ma_value, close_upper
                )
            } else if state.ma_signal == 2 {
                format!(
                    "满足 {:.6} < {:.6} → 反向信号 (忽略)",
                    ma_value, close_lower
                )
            } else {
                format!(
                    "未触发 {:.6} ∈ [{:.6}, {:.6}]",
                    ma_value, close_lower, close_upper
                )
            };
            funding_ma_rows.push(vec![
                key.clone(),
                ma_value_str,
                fmt_decimal(close_upper),
                fmt_decimal(close_lower),
                ma_message,
            ]);
        }

        // 批量发送（控制最大 1024 字节）
        let batch = ResampleBatch {
            ts_ms,
            items: batch_items,
        };
        if let Ok(mut bytes) = batch.to_bytes() {
            // Prepend length header
            let len = bytes.len() as u32;
            let mut buf = Vec::with_capacity(bytes.len() + 4);
            buf.extend_from_slice(&len.to_le_bytes());
            buf.append(&mut bytes);
            if buf.len() > 1024 {
                buf.truncate(1024);
            }
            let _ = self.resample_pub.publish(&buf);
        }

        if !price_rows.is_empty() {
            info!(
                "Resample价差信号\n{}",
                render_three_line_table(
                    &[
                        "Symbol",
                        "BidAskSR",
                        "BidAsk<=Open",
                        "BidAsk>=Close",
                        "开仓动作",
                        "平仓动作",
                    ],
                    &price_rows,
                )
            );
        }
        if !funding_pred_rows.is_empty() {
            info!(
                "Resample资金-预测信号\n{}",
                render_three_line_table(
                    &[
                        "Symbol",
                        "Freq",
                        "Predict",
                        "OpenUpper",
                        "OpenLower",
                        "解读",
                    ],
                    &funding_pred_rows,
                )
            );
        }
        if !funding_ma_rows.is_empty() {
            info!(
                "Resample资金-MA信号\n{}",
                render_three_line_table(
                    &["Symbol", "MA60", "CloseUpper", "CloseLower", "解读",],
                    &funding_ma_rows,
                )
            );
        }
    }

    fn calc_funding_ma(&self, symbol: &str) -> Option<f64> {
        let win = self.cfg.strategy.funding_ma_size.max(1);
        let values = self.funding_series.get(symbol)?;
        if values.is_empty() {
            return None;
        }
        let start = values.len().saturating_sub(win);
        let slice = &values[start..];
        if slice.is_empty() {
            return None;
        }
        let sum: f64 = slice.iter().copied().sum();
        let mean = sum / (slice.len() as f64);
        Some(mean)
    }

    async fn reload_loan_rates(&mut self) -> Result<()> {
        // 从 Redis HASH 读取借贷利率，字段为 symbol，值可为数字字符串或 JSON { loan_rate_8h, ts }
        let settings = self
            .cfg
            .redis
            .clone()
            .ok_or_else(|| anyhow::anyhow!("缺少 Redis 配置"))?;
        let key = self
            .cfg
            .loan
            .redis_key
            .clone()
            .unwrap_or_else(|| "binance_loan_rate_8h".to_string());
        let mut client = RedisClient::connect(settings).await?;
        let map = client.hgetall_map(&key).await?;
        let mut out = HashMap::new();
        for (sym, raw) in map {
            let sym_up = sym.to_uppercase();
            let val = match raw.parse::<f64>() {
                Ok(v) => Some(v),
                Err(_) => {
                    let v: Option<serde_json::Value> = serde_json::from_str(&raw).ok();
                    v.and_then(|o| o.get("loan_rate_8h").and_then(|x| x.as_f64()))
                }
            };
            if let Some(v) = val {
                out.insert(sym_up, v);
            }
        }
        self.loan_map = out;
        Ok(())
    }

    fn build_open_request(
        &self,
        state: &SymbolState,
        ratio: f64,
        emit_ts: i64,
    ) -> Option<SignalRequest> {
        if state.spot_quote.bid <= 0.0 {
            return None;
        }
        let mut limit_price = state.spot_quote.bid * (1.0 - self.cfg.order.open_range);
        if limit_price <= 0.0 {
            warn!(
                "{} 计算得到的开仓价格非法: {:.6}",
                state.spot_symbol, limit_price
            );
            return None;
        }
        let price_tick = self
            .min_qty
            .spot_price_tick_by_symbol(&state.spot_symbol)
            .unwrap_or(0.0);
        let raw_limit_price = limit_price;
        if price_tick > 0.0 {
            limit_price = align_price_floor(limit_price, price_tick);
            debug!(
                "{} 开仓价格对齐: raw={:.8} tick={:.8} aligned={:.8}",
                state.spot_symbol, raw_limit_price, price_tick, limit_price
            );
            if limit_price <= 0.0 {
                warn!(
                    "{} price tick 对齐后开仓价格非法: raw={:.8} tick={:.8}",
                    state.spot_symbol, raw_limit_price, price_tick
                );
                return None;
            }
        }
        let base_qty = if limit_price > 0.0 {
            self.cfg.order.amount_u / limit_price
        } else {
            0.0
        };

        let spot_min = self
            .min_qty
            .spot_min_qty_by_symbol(&state.spot_symbol)
            .unwrap_or(0.0);
        let futures_min = self
            .min_qty
            .futures_um_min_qty_by_symbol(&state.futures_symbol)
            .unwrap_or(0.0);

        let mut adjusted_qty = base_qty.max(spot_min).max(futures_min);
        if adjusted_qty <= 0.0 {
            warn!(
                "{} 计算得到的下单数量非法: 基准={:.6}, spot_min={:.6}, futures_min={:.6}",
                state.spot_symbol, base_qty, spot_min, futures_min
            );
            return None;
        }

        let qty_step = self.get_qty_step(&state.spot_symbol, spot_min, futures_min);
        if qty_step > 0.0 {
            adjusted_qty = (adjusted_qty / qty_step).ceil() * qty_step;
        }

        info!(
            "{} 下单量调整: 基准={:.6} spot_min={:.6} fut_min={:.6} 最终={:.6}",
            state.spot_symbol, base_qty, spot_min, futures_min, adjusted_qty
        );

        let qty = adjusted_qty as f32;
        let ctx = BinSingleForwardArbOpenCtx {
            spot_symbol: state.spot_symbol.clone(),
            amount: qty,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: limit_price,
            price_tick,
            exp_time: self.cfg.max_open_keep_us(),
        }
        .to_bytes();
        Some(SignalRequest::Open {
            symbol_key: state.spot_symbol.clone(),
            ctx,
            ratio,
            price: limit_price,
            emit_ts,
        })
    }

    fn build_close_request(
        &self,
        state: &SymbolState,
        ratio: f64,
        emit_ts: i64,
    ) -> Option<SignalRequest> {
        if state.spot_quote.ask <= 0.0 {
            return None;
        }
        let mut limit_price = state.spot_quote.ask * (1.0 + self.cfg.order.close_range);
        if limit_price <= 0.0 {
            warn!(
                "{} 计算得到的平仓价格非法: {:.6}",
                state.spot_symbol, limit_price
            );
            return None;
        }
        let price_tick = self
            .min_qty
            .spot_price_tick_by_symbol(&state.spot_symbol)
            .unwrap_or(0.0);
        let raw_limit_price = limit_price;
        if price_tick > 0.0 {
            limit_price = align_price_ceil(limit_price, price_tick);
            debug!(
                "{} 平仓价格对齐: raw={:.8} tick={:.8} aligned={:.8}",
                state.spot_symbol, raw_limit_price, price_tick, limit_price
            );
            if limit_price <= 0.0 {
                warn!(
                    "{} price tick 对齐后平仓价格非法: raw={:.8} tick={:.8}",
                    state.spot_symbol, raw_limit_price, price_tick
                );
                return None;
            }
        }
        let ctx = BinSingleForwardArbCloseMarginCtx {
            spot_symbol: state.spot_symbol.clone(),
            limit_price,
            price_tick,
            exp_time: self.cfg.max_close_keep_us(),
        }
        .to_bytes();
        Some(SignalRequest::Close {
            symbol_key: state.spot_symbol.clone(),
            ctx,
            ratio,
            price: limit_price,
            emit_ts,
        })
    }

    fn publish_signal(&self, signal_type: SignalType, context: Bytes) -> Result<()> {
        let now_us = get_timestamp_us();
        let signal = TradeSignal::create(signal_type.clone(), now_us, 0.0, context);
        let frame = signal.to_bytes();
        self.publisher
            .publish(&frame)
            .with_context(|| format!("发布信号 {:?} 失败", signal_type))
    }

    fn print_stats(&self) {
        info!(
            "当前追踪交易对: {} | 开仓信号累计 {} | 平仓信号累计 {}",
            self.symbols.len(),
            self.stats.open_signals,
            self.stats.close_signals
        );
    }
}

// 阈值获取逻辑改为读取 self.th_4h/self.th_8h，参见 StrategyEngine::thresholds_for_frequency

#[derive(Debug, Clone, Default)]
struct FundingThresholdEntry {
    symbol: String,
    predict_funding_rate: f64,
    lorn_rate: f64,
    funding_frequency: String, // "4h" | "8h"
    open_upper_threshold: f64,
    open_lower_threshold: f64,
    close_lower_threshold: f64,
    close_upper_threshold: f64,
}

#[derive(Debug, Clone, Copy)]
struct RateThresholds {
    open_upper: f64,
    open_lower: f64,
    close_lower: f64,
    close_upper: f64,
}

impl RateThresholds {
    const fn for_8h() -> Self {
        Self {
            open_upper: 0.00008,
            open_lower: -0.00008,
            close_lower: -0.001,
            close_upper: 0.001,
        }
    }

    const fn for_4h() -> Self {
        Self {
            open_upper: 0.00004,
            open_lower: -0.00004,
            close_lower: -0.0008,
            close_upper: 0.0008,
        }
    }
}

impl Default for RateThresholds {
    fn default() -> Self {
        Self::for_8h()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ParamsSnapshot {
    interval: u64,
    predict_num: u64,
    refresh_secs: u64,
    fetch_secs: u64,
    fetch_offset_secs: u64,
    history_limit: u64,
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
            settlement_offset_secs: p.settlement_offset_secs,
        }
    }
}

impl StrategyEngine {
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
    async fn reload_params_if_changed(&mut self) -> Result<bool> {
        // 从 Redis HASH 读取 binance_forward_arb_params
        let Some(redis_cfg) = self.cfg.redis.clone() else {
            return Ok(false);
        };
        let mut client = mkt_signal::common::redis_client::RedisClient::connect(redis_cfg).await?;
        let map = client
            .hgetall_map("binance_forward_arb_params")
            .await
            .unwrap_or_default();
        debug!("参数读取: {:?}", map);
        // 必须包含 4h/8h 四个阈值共8个键
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
        for k in required.iter() {
            if !map.contains_key(*k) {
                missing.push(k.to_string());
            }
        }
        if !missing.is_empty() {
            panic!(
                "缺少资金费率阈值参数: {:?}，请写入 Redis HASH binance_forward_arb_params 再启动",
                missing
            );
        }
        let mut changed = false;
        let parse_u64 = |k: &str| -> Option<u64> { map.get(k).and_then(|v| v.parse::<u64>().ok()) };
        let parse_f64 = |k: &str| -> Option<f64> { map.get(k).and_then(|v| v.parse::<f64>().ok()) };
        if let Some(v) = parse_u64("interval") {
            if self.cfg.strategy.interval as u64 != v {
                self.cfg.strategy.interval = v as usize;
                changed = true;
            }
        }
        if let Some(v) = parse_u64("predict_num") {
            if self.cfg.strategy.predict_num as u64 != v {
                self.cfg.strategy.predict_num = v as usize;
                changed = true;
            }
        }
        if let Some(v) = parse_u64("refresh_secs") {
            if self.cfg.strategy.refresh_secs != v {
                self.cfg.strategy.refresh_secs = v;
                changed = true;
            }
        }
        if let Some(v) = parse_u64("reload_interval_secs") {
            if self.cfg.reload.interval_secs != v {
                self.cfg.reload.interval_secs = v;
                changed = true;
            }
        }
        if let Some(v) = parse_u64("signal_min_interval_ms") {
            if self.cfg.signal.min_interval_ms != v {
                self.cfg.signal.min_interval_ms = v;
                changed = true;
            }
        }
        if let Some(v) = parse_u64("fetch_secs") {
            if self.cfg.strategy.fetch_secs != v {
                self.cfg.strategy.fetch_secs = v;
                changed = true;
            }
        }
        if let Some(v) = parse_u64("fetch_offset_secs") {
            if self.cfg.strategy.fetch_offset_secs != v {
                self.cfg.strategy.fetch_offset_secs = v;
                changed = true;
            }
        }
        if let Some(v) = parse_u64("history_limit") {
            if self.cfg.strategy.history_limit as u64 != v {
                self.cfg.strategy.history_limit = v as usize;
                changed = true;
            }
        }
        // 结算偏移与 fetch_offset_secs 对齐
        let new_settle = self.cfg.strategy.fetch_offset_secs as i64;
        if self.cfg.strategy.settlement_offset_secs != new_settle {
            self.cfg.strategy.settlement_offset_secs = new_settle;
            changed = true;
        }

        // 加载阈值（4h/8h）
        let mut th_changed = false;
        if let Some(v) = parse_f64("fr_4h_open_upper_threshold") {
            if !approx_equal(self.th_4h.open_upper, v) {
                self.th_4h.open_upper = v;
                th_changed = true;
            }
        }
        if let Some(v) = parse_f64("fr_4h_open_lower_threshold") {
            if !approx_equal(self.th_4h.open_lower, v) {
                self.th_4h.open_lower = v;
                th_changed = true;
            }
        }
        if let Some(v) = parse_f64("fr_4h_close_lower_threshold") {
            if !approx_equal(self.th_4h.close_lower, v) {
                self.th_4h.close_lower = v;
                th_changed = true;
            }
        }
        if let Some(v) = parse_f64("fr_4h_close_upper_threshold") {
            if !approx_equal(self.th_4h.close_upper, v) {
                self.th_4h.close_upper = v;
                th_changed = true;
            }
        }
        if let Some(v) = parse_f64("fr_8h_open_upper_threshold") {
            if !approx_equal(self.th_8h.open_upper, v) {
                self.th_8h.open_upper = v;
                th_changed = true;
            }
        }
        if let Some(v) = parse_f64("fr_8h_open_lower_threshold") {
            if !approx_equal(self.th_8h.open_lower, v) {
                self.th_8h.open_lower = v;
                th_changed = true;
            }
        }
        if let Some(v) = parse_f64("fr_8h_close_lower_threshold") {
            if !approx_equal(self.th_8h.close_lower, v) {
                self.th_8h.close_lower = v;
                th_changed = true;
            }
        }
        if let Some(v) = parse_f64("fr_8h_close_upper_threshold") {
            if !approx_equal(self.th_8h.close_upper, v) {
                self.th_8h.close_upper = v;
                th_changed = true;
            }
        }
        if th_changed {
            changed = true;
            debug!("阈值参数变更: 4h={:?} 8h={:?}", self.th_4h, self.th_8h);
        }

        // 订单参数
        if let Some(v) = parse_f64("order_open_range") {
            if !approx_equal(self.cfg.order.open_range, v) {
                self.cfg.order.open_range = v;
                changed = true;
            }
        }
        if let Some(v) = parse_f64("order_close_range") {
            if !approx_equal(self.cfg.order.close_range, v) {
                self.cfg.order.close_range = v;
                changed = true;
            }
        }
        if let Some(v) = parse_f64("order_amount_u") {
            if !approx_equal(self.cfg.order.amount_u, v) {
                self.cfg.order.amount_u = v;
                changed = true;
            }
        }
        if let Some(v) = parse_u64("order_max_open_order_keep_s") {
            if self.cfg.order.max_open_order_keep_s != v {
                self.cfg.order.max_open_order_keep_s = v;
                changed = true;
            }
        }
        if let Some(v) = parse_u64("order_max_close_order_keep_s") {
            if self.cfg.order.max_close_order_keep_s != v {
                self.cfg.order.max_close_order_keep_s = v;
                changed = true;
            }
        }

        let current = ParamsSnapshot::from(&self.cfg.strategy);
        if self.last_params.as_ref() != Some(&current) {
            changed = true;
            self.last_params = Some(current);
            // 重要参数变更时，重设调度时间点（阈值刷新周期按 reload.interval_secs）
            self.next_reload =
                Instant::now() + Duration::from_secs(self.cfg.reload.interval_secs.max(5));
            self.next_fetch_refresh = self.next_fetch_instant();
        }
        debug!("参数变更检测: changed={}", changed);
        Ok(changed)
    }

    fn is_settlement_trigger(&mut self) -> bool {
        // 基于 UTC 准点 + 偏移（秒）判断 settlement 周期；周期大小来自 fetch_secs
        let now_ms = Utc::now().timestamp_millis();
        let offset_ms = self
            .cfg
            .strategy
            .settlement_offset_secs
            .saturating_mul(1000);
        let period_s = self.cfg.strategy.fetch_secs.max(60) as i64;
        let period_ms = period_s.saturating_mul(1000);
        let adj = now_ms.saturating_sub(offset_ms);
        if adj < 0 {
            return false;
        }
        let slot = adj / period_ms;
        let slot_ms = slot.saturating_mul(period_ms).saturating_add(offset_ms);
        if self.last_settlement_marker_ms != Some(slot_ms) {
            self.last_settlement_marker_ms = Some(slot_ms);
            debug!(
                "结算点触发: slot_ms={} (now_ms={} offset_s={} period_s={})",
                slot_ms, now_ms, self.cfg.strategy.settlement_offset_secs, period_s
            );
            return true;
        }
        false
    }

    async fn recompute_and_log(&mut self, reason: &str, symbols_changed: bool) {
        // 重算每个符号对应的阈值条目
        debug!(
            "开始重算资金费率阈值: reason='{}' symbols_changed={}",
            reason, symbols_changed
        );
        for (key, state) in &self.symbols {
            let fut = state.futures_symbol.to_uppercase();
            let freq = self
                .funding_frequency
                .get(&fut)
                .cloned()
                .unwrap_or_else(|| "8h".to_string());
            let (ou, ol, cl, cu) = self.thresholds_for_frequency(&freq);
            let pred = self.get_predicted_for(&fut);
            let entry = FundingThresholdEntry {
                symbol: key.clone(),
                predict_funding_rate: pred,
                lorn_rate: 0.0,
                funding_frequency: freq,
                open_upper_threshold: ou,
                open_lower_threshold: ol,
                close_lower_threshold: cl,
                close_upper_threshold: cu,
            };
            self.funding_thresholds.insert(key.clone(), entry);
        }
        // 刷新后打印资金费率总览（三线表）（仅在 warmup 完成时）
        if self.is_warmup_complete() {
            self.print_funding_overview_table();
        } else {
            debug!("跳过总览打印: warmup 未完成");
        }
    }

    fn print_funding_overview_table(&self) {
        if self.symbols.is_empty() {
            return;
        }
        if !self.is_warmup_complete() {
            return;
        }
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut keys: Vec<String> = self.symbols.keys().cloned().collect();
        keys.sort();
        for key in keys {
            let Some(state) = self.symbols.get(&key) else {
                continue;
            };
            let fut = state.futures_symbol.to_uppercase();
            // 优先使用已重算的阈值表条目，避免触发未使用字段警告
            let (symbol, freq, ou, ol, cl, cu, pred) =
                if let Some(e) = self.funding_thresholds.get(&key) {
                    (
                        e.symbol.clone(),
                        e.funding_frequency.clone(),
                        e.open_upper_threshold,
                        e.open_lower_threshold,
                        e.close_lower_threshold,
                        e.close_upper_threshold,
                        e.predict_funding_rate,
                    )
                } else {
                    let freq = self
                        .funding_frequency
                        .get(&fut)
                        .cloned()
                        .unwrap_or_else(|| "8h".to_string());
                    let (ou, ol, cl, cu) = self.thresholds_for_frequency(&freq);
                    let pred = self.get_predicted_for(&fut);
                    (key.clone(), freq, ou, ol, cl, cu, pred)
                };
            // 使用 REST 历史的最近 funding_ma_size 条计算均值
            let fr_ma = self
                .history_map
                .get(&fut)
                .and_then(|v| {
                    let win = self.cfg.strategy.funding_ma_size.max(1);
                    if v.is_empty() {
                        return None;
                    }
                    let start = v.len().saturating_sub(win);
                    let slice = &v[start..];
                    if slice.is_empty() {
                        None
                    } else {
                        Some(slice.iter().copied().sum::<f64>() / (slice.len() as f64))
                    }
                })
                .map(|v| format!("{:.6}", v))
                .unwrap_or_else(|| "-".to_string());
            rows.push(vec![
                symbol,
                freq,
                fr_ma,
                format!("{:.6}", pred),
                format!("{:.6}", ou),
                format!("{:.6}", ol),
                format!("{:.6}", cl),
                format!("{:.6}", cu),
            ]);
            // 读取未使用字段避免 dead_code 警告（例如 lorn_rate）
            if let Some(e) = self.funding_thresholds.get(&key) {
                let _ = e.lorn_rate;
            }
        }
        if rows.is_empty() {
            return;
        }
        let table = render_three_line_table(
            &[
                "Symbol", "Freq", "FR_Mean", "Pred", "OpenU", "OpenL", "CloseL", "CloseU",
            ],
            &rows,
        );
        info!("资金费率总览\n{}", table);
    }

    fn print_warmup_progress_table(&self) {
        if self.symbols.is_empty() {
            return;
        }
        let need = self.cfg.strategy.funding_ma_size.max(1);
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut keys: Vec<String> = self.symbols.keys().cloned().collect();
        keys.sort();
        for key in keys {
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
        if rows.is_empty() {
            return;
        }
        let table = render_three_line_table(&["Symbol", "Futures", "Count", "Need", "Freq"], &rows);
        info!("Warmup 进度\n{}", table);
    }
}

#[derive(Debug)]
enum SignalRequest {
    Open {
        symbol_key: String,
        ctx: Bytes,
        ratio: f64,
        price: f64,
        emit_ts: i64,
    },
    Close {
        symbol_key: String,
        ctx: Bytes,
        ratio: f64,
        price: f64,
        emit_ts: i64,
    },
}

fn fmt_decimal(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if !value.is_finite() {
        return if value.is_sign_positive() {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }
    if value == 0.0 {
        return "0".to_string();
    }
    let mut s = format!("{value:.6}");
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
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

impl StrategyEngine {
    fn is_warmup_complete(&self) -> bool {
        if self.symbols.is_empty() {
            return false;
        }
        let need = self.cfg.strategy.funding_ma_size.max(1);
        for state in self.symbols.values() {
            let fut = state.futures_symbol.to_uppercase();
            match self.history_map.get(&fut) {
                Some(v) if v.len() >= need => {}
                _ => return false,
            }
        }
        true
    }
}

fn compute_widths(headers: &[&str], rows: &[Vec<String>]) -> Vec<usize> {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx >= widths.len() {
                continue;
            }
            widths[idx] = widths[idx].max(cell.len());
        }
    }
    widths
}

fn build_separator(widths: &[usize], fill: char) -> String {
    let mut line = String::new();
    line.push('+');
    for width in widths {
        line.push_str(&fill.to_string().repeat(width + 2));
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // 更保守的默认日志过滤，避免输出依赖库的 DEBUG 噪声
    let default_filter = "info,funding_rate_strategy=debug,mkt_signal=info,hyper=warn,hyper_util=warn,h2=warn,reqwest=warn";
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_filter))
        .init();

    info!("启动 Binance Forward Arb 信号策略");

    let cfg = StrategyConfig::load()?;
    let publisher = SignalPublisher::new(SIGNAL_CHANNEL_MT_ARBITRAGE)?;
    let mut engine = StrategyEngine::new(cfg.clone(), publisher).await?;
    // 初始化阶段：等待 warmup 完成后再打印任何表格
    if engine.is_warmup_complete() {
        engine.print_funding_overview_table();
        engine.log_symbol_snapshot();
    } else {
        info!(
            "等待资金费率均值预热: 需要每个符号至少 {} 条历史",
            cfg.strategy.funding_ma_size.max(1)
        );
    }

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

    let shutdown = CancellationToken::new();
    setup_signal_handlers(&shutdown)?;

    let mut next_stat_time = Instant::now() + Duration::from_secs(30);
    let mut next_snapshot = Instant::now() + Duration::from_secs(3);
    engine.next_resample = Instant::now();

    loop {
        if shutdown.is_cancelled() {
            info!("收到退出信号，准备关闭");
            break;
        }

        engine.maybe_reload().await;

        for msg in subscriber.poll_channel("binance", &ChannelType::AskBidSpread, Some(32)) {
            let msg_type = mkt_msg::get_msg_type(&msg);
            if msg_type == MktMsgType::AskBidSpread {
                engine.handle_spot_quote(&msg);
            }
        }

        for msg in subscriber.poll_channel("binance-futures", &ChannelType::AskBidSpread, Some(32))
        {
            let msg_type = mkt_msg::get_msg_type(&msg);
            if msg_type == MktMsgType::AskBidSpread {
                engine.handle_futures_quote(&msg);
            }
        }

        for msg in subscriber.poll_channel("binance-futures", &ChannelType::Derivatives, Some(32)) {
            match mkt_msg::get_msg_type(&msg) {
                MktMsgType::FundingRate => engine.handle_funding_rate(&msg),
                _ => (),
            }
        }

        if Instant::now() >= next_stat_time {
            engine.print_stats();
            next_stat_time += Duration::from_secs(30);
        }
        if Instant::now() >= next_snapshot {
            if engine.is_warmup_complete() {
                engine.log_symbol_snapshot();
            }
            next_snapshot += Duration::from_secs(3);
        }
        if Instant::now() >= engine.next_resample {
            engine.resample_and_publish();
            engine.next_resample += engine.resample_interval;
        }

        yield_now().await;
    }

    engine.print_stats();
    info!("策略进程结束");
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
        let term_token = token.clone();
        tokio::spawn(async move {
            match unix_signal(SignalKind::terminate()) {
                Ok(mut sigterm) => {
                    if sigterm.recv().await.is_some() {
                        info!("接收到 SIGTERM 信号");
                        term_token.cancel();
                    }
                }
                Err(e) => error!("监听 SIGTERM 失败: {}", e),
            }
        });

        let quit_token = token.clone();
        tokio::spawn(async move {
            match unix_signal(SignalKind::quit()) {
                Ok(mut sigquit) => {
                    if sigquit.recv().await.is_some() {
                        info!("接收到 SIGQUIT 信号");
                        quit_token.cancel();
                    }
                }
                Err(e) => error!("监听 SIGQUIT 失败: {}", e),
            }
        });
    }

    Ok(())
}

// removed: format_timestamp (no longer used in snapshot output)

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

impl StrategyEngine {
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

// ------- Funding prediction helpers (strategy-side) -------

#[derive(Debug, Deserialize)]
struct BinanceFundingHistItem {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "fundingTime")]
    funding_time: Option<i64>,
}

// removed: fetch_binance_funding_history (superseded by range variant)

async fn fetch_binance_funding_items(
    client: &Client,
    symbol: &str,
    limit: usize,
) -> Result<Vec<BinanceFundingHistItem>> {
    let url = "https://fapi.binance.com/fapi/v1/fundingRate";
    let end_time = Utc::now().timestamp_millis();
    let start_time = end_time - 3 * 24 * 3600 * 1000; // 3d window
    let limit_s = limit.max(1).min(1000).to_string();
    let params = [
        ("symbol", symbol),
        ("startTime", &start_time.to_string()),
        ("endTime", &end_time.to_string()),
        ("limit", &limit_s),
    ];
    let resp = client.get(url).query(&params).send().await?;
    if !resp.status().is_success() {
        return Ok(vec![]);
    }
    let mut items: Vec<BinanceFundingHistItem> = resp.json().await.unwrap_or_default();
    items.sort_by_key(|it| it.funding_time.unwrap_or_default());
    Ok(items)
}

async fn fetch_binance_funding_history_range(
    client: &Client,
    symbol: &str,
    start_time: i64,
    end_time: i64,
    limit: usize,
) -> Result<Vec<f64>> {
    let url = "https://fapi.binance.com/fapi/v1/fundingRate";
    let end = end_time.max(0);
    let start = start_time.min(end).max(0);
    let limit_s = limit.max(1).min(1000).to_string();
    let params = [
        ("symbol", symbol),
        ("startTime", &start.to_string()),
        ("endTime", &end.to_string()),
        ("limit", &limit_s),
    ];
    let resp = client.get(url).query(&params).send().await?;
    if !resp.status().is_success() {
        return Ok(vec![]);
    }
    let mut items: Vec<BinanceFundingHistItem> = resp.json().await.unwrap_or_default();
    items.sort_by_key(|it| it.funding_time.unwrap_or_default());
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        if let Ok(v) = it.funding_rate.parse::<f64>() {
            out.push(v);
        }
    }
    if out.len() > limit {
        let drop_n = out.len() - limit;
        out.drain(0..drop_n);
    }
    Ok(out)
}

async fn infer_binance_funding_frequency(client: &Client, symbol: &str) -> Option<String> {
    let items = fetch_binance_funding_items(client, symbol, 40).await.ok()?;
    let mut times: Vec<i64> = items.iter().filter_map(|it| it.funding_time).collect();
    if times.len() < 3 {
        return Some("8h".to_string());
    }
    times.sort_unstable();
    let mut diffs: Vec<i64> = Vec::with_capacity(times.len().saturating_sub(1));
    for w in times.windows(2) {
        if let [a, b] = w {
            diffs.push(b - a);
        }
    }
    if diffs.is_empty() {
        return Some("8h".to_string());
    }
    diffs.sort_unstable();
    let median = diffs[diffs.len() / 2];
    // 阈值 6 小时分界
    let six_hours_ms = 6 * 3600 * 1000;
    let freq = if median <= six_hours_ms { "4h" } else { "8h" };
    debug!("频率推断: {} median={}ms => {}", symbol, median, freq);
    Some(freq.to_string())
}

// removed: compute_predict (compute_predictions inlines the logic with debug logs)
#[derive(Debug, Clone, Deserialize)]
struct LoanConfig {
    #[serde(default)]
    redis_key: Option<String>,
    #[serde(default = "default_loan_refresh_secs")]
    refresh_secs: u64,
}

impl Default for LoanConfig {
    fn default() -> Self {
        Self {
            redis_key: None,
            refresh_secs: default_loan_refresh_secs(),
        }
    }
}

const fn default_loan_refresh_secs() -> u64 {
    60
}
