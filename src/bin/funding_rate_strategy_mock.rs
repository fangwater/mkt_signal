use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use serde::Deserialize;
use tokio::signal;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio::sync::mpsc;
use tokio::time::{interval, Instant, MissedTickBehavior};
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

const SIGNAL_CHANNEL_MT_ARBITRAGE: &str = "mt_arbitrage";
const NODE_FUNDING_STRATEGY_SUB: &str = "funding_rate_strategy_mock";
const DEFAULT_CFG_PATH: &str = "config/funding_rate_strategy.toml";
const DEFAULT_TRACKING_PATH: &str = "config/tracking_symbol.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DataMode {
    LocalJson,
    Redis,
}

impl Default for DataMode {
    fn default() -> Self {
        DataMode::LocalJson
    }
}

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

#[derive(Debug, Clone, Deserialize)]
struct StrategyConfig {
    #[serde(default)]
    mode: DataMode,
    #[serde(default = "default_tracking_path")]
    tracking_path: String,
    #[allow(dead_code)]
    #[serde(default)]
    redis: Option<RedisSettings>,
    #[serde(default)]
    order: OrderConfig,
    #[serde(default)]
    signal: SignalConfig,
    #[serde(default)]
    reload: ReloadConfig,
}

fn default_tracking_path() -> String {
    DEFAULT_TRACKING_PATH.to_string()
}

impl StrategyConfig {
    fn load() -> Result<Self> {
        let cfg_path =
            std::env::var("FUNDING_RATE_CFG").unwrap_or_else(|_| DEFAULT_CFG_PATH.to_string());
        let content = fs::read_to_string(&cfg_path)
            .with_context(|| format!("读取配置文件失败: {cfg_path}"))?;
        let mut cfg: StrategyConfig =
            toml::from_str(&content).with_context(|| format!("解析配置文件失败: {cfg_path}"))?;
        if cfg.tracking_path.trim().is_empty() {
            cfg.tracking_path = DEFAULT_TRACKING_PATH.to_string();
        }
        if cfg.mode == DataMode::Redis {
            if let Some(redis_cfg) = cfg.redis.as_ref() {
                info!(
                    "Redis 数据源配置: host={} port={} db={} prefix={:?}",
                    redis_cfg.host, redis_cfg.port, redis_cfg.db, redis_cfg.prefix
                );
            } else {
                warn!("已选择 Redis 模式，但未提供 redis 配置，暂时回退为本地 JSON");
            }
            warn!("当前 Redis 模式仅预留实现，mock 仍会使用本地 JSON");
        }
        info!(
            "mock 策略配置加载完成: mode={:?} tracking_path={} reload_interval={}s",
            cfg.mode, cfg.tracking_path, cfg.reload.interval_secs
        );
        Ok(cfg)
    }

    fn tracking_path(&self) -> PathBuf {
        PathBuf::from(&self.tracking_path)
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
    open_threshold: f64,
    close_threshold: f64,
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

impl PositionState {
    fn label(&self) -> &'static str {
        match self {
            PositionState::Flat => "FLAT",
            PositionState::Opened => "OPEN",
        }
    }
}

#[derive(Debug, Clone)]
struct SymbolState {
    spot_symbol: String,
    futures_symbol: String,
    open_threshold: f64,
    close_threshold: f64,
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
}

impl SymbolState {
    fn new(threshold: SymbolThreshold) -> Self {
        Self {
            spot_symbol: threshold.spot_symbol,
            futures_symbol: threshold.futures_symbol,
            open_threshold: threshold.open_threshold,
            close_threshold: threshold.close_threshold,
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
        }
    }

    fn update_threshold(&mut self, threshold: SymbolThreshold) {
        self.open_threshold = threshold.open_threshold;
        self.close_threshold = threshold.close_threshold;
        self.futures_symbol = threshold.futures_symbol;
    }

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

struct MockController {
    cfg: StrategyConfig,
    publisher: SignalPublisher,
    symbols: HashMap<String, SymbolState>,
    futures_index: HashMap<String, String>,
    min_qty: MinQtyTable,
    qty_step_cache: RefCell<HashMap<String, QtyStepInfo>>,
    next_snapshot: Instant,
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
            next_snapshot: Instant::now() + Duration::from_secs(5),
        };
        controller.reload_symbols()?;
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
        if Instant::now() >= self.next_snapshot {
            self.print_symbol_snapshot();
            self.next_snapshot = Instant::now() + Duration::from_secs(5);
        }
    }

    async fn handle_command(&mut self, line: &str) -> Result<bool> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(false);
        }
        let mut parts: Vec<&str> = trimmed.split_whitespace().collect();
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
        Ok(false)
    }

    fn print_help(&self) {
        info!("命令: list | open <idx> | close <idx> | reload | refresh | help | quit");
        info!("直接输入索引 (例如 `2`) 等同于 open <idx>，索引从 1 开始");
    }

    fn print_symbol_snapshot(&self) {
        if self.symbols.is_empty() {
            info!("当前未追踪任何交易对");
            return;
        }
        let mut rows = Vec::new();
        for (idx, key) in self.sorted_symbol_keys().iter().enumerate() {
            if let Some(state) = self.symbols.get(key) {
                let ratio = state.calc_ratio();
                rows.push(vec![
                    format!("{:>3}", idx + 1),
                    state.spot_symbol.clone(),
                    state.futures_symbol.clone(),
                    format_price(state.spot_quote.bid),
                    format_price(state.spot_quote.ask),
                    format_price(state.futures_quote.bid),
                    format_price(state.futures_quote.ask),
                    format_ratio(ratio),
                    format!("{:.6}", state.funding_rate),
                    format!("{:.6}", state.predicted_rate),
                    state.position.label().to_string(),
                ]);
            }
        }
        let table = render_three_line_table(
            &[
                "Idx", "Spot", "Futures", "SpotBid", "SpotAsk", "FutBid", "FutAsk", "Spread",
                "Funding", "Pred", "Pos",
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
            let limit_price = state.spot_quote.bid * (1.0 - self.cfg.order.open_range);
            if limit_price <= 0.0 {
                anyhow::bail!("{} 开仓价格非法: {:.6}", spot_symbol, limit_price);
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
                price: limit_price as f32,
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
                "{} mock 强制开仓: qty={:.6} price={:.6}",
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
            let limit_price = state.spot_quote.ask * (1.0 + self.cfg.order.close_range);
            if limit_price <= 0.0 {
                anyhow::bail!("{} 平仓价格非法: {:.6}", spot_symbol, limit_price);
            }
            let ctx = BinSingleForwardArbCloseMarginCtx {
                spot_symbol: spot_symbol.clone(),
                limit_price: limit_price as f32,
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
            info!("{} mock 强制平仓: price={:.6}", spot_symbol, limit_price);
            Ok(())
        } else {
            anyhow::bail!("未找到交易对 {key}");
        }
    }

    fn publish_signal(&self, signal_type: SignalType, context: Bytes) -> Result<()> {
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
        let mut new_symbols = HashMap::new();
        let mut new_futures_index = HashMap::new();
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
            }
        }
        self.symbols = new_symbols;
        self.futures_index = new_futures_index;
        self.qty_step_cache
            .borrow_mut()
            .retain(|symbol, _| self.symbols.contains_key(symbol));
        info!("已加载追踪交易对数量: {}", self.symbols.len());
        Ok(())
    }

    fn load_thresholds(&self) -> Result<Vec<SymbolThreshold>> {
        match self.cfg.mode {
            DataMode::LocalJson => self.load_from_json(self.cfg.tracking_path()),
            DataMode::Redis => anyhow::bail!("Redis 模式尚未实现"),
        }
    }

    fn load_from_json(&self, path: PathBuf) -> Result<Vec<SymbolThreshold>> {
        let data = fs::read_to_string(&path)
            .with_context(|| format!("读取 tracking_symbol.json 失败: {}", path.display()))?;
        let value: serde_json::Value = serde_json::from_str(&data)
            .with_context(|| format!("解析 tracking_symbol.json 失败: {}", path.display()))?;
        let binance = value
            .get("binance")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("tracking_symbol.json 缺少 binance 数组"))?;
        let mut result = Vec::new();
        for entry in binance {
            if let Some(threshold) = Self::parse_symbol_entry(entry) {
                result.push(threshold);
            }
        }
        if result.is_empty() {
            anyhow::bail!("tracking_symbol.json 未解析到有效的 binance 交易对");
        }
        Ok(result)
    }

    fn parse_symbol_entry(entry: &serde_json::Value) -> Option<SymbolThreshold> {
        match entry {
            serde_json::Value::String(symbol) => {
                let spot = symbol.to_uppercase();
                Some(SymbolThreshold {
                    futures_symbol: spot.clone(),
                    spot_symbol: spot,
                    open_threshold: 0.0,
                    close_threshold: 0.0,
                })
            }
            serde_json::Value::Object(map) => {
                let spot = map.get("spot_symbol")?.as_str()?.to_uppercase();
                let fut = map
                    .get("futures_symbol")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&spot)
                    .to_uppercase();
                let open_threshold = map.get("open_threshold")?.as_f64()?;
                let close_threshold = map
                    .get("close_threshold")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(open_threshold);
                Some(SymbolThreshold {
                    spot_symbol: spot,
                    futures_symbol: fut,
                    open_threshold,
                    close_threshold,
                })
            }
            _ => None,
        }
    }

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
        if let Some(state) = self.symbols.get_mut(&symbol) {
            let funding = FundingRateMsg::get_funding_rate(msg);
            let predicted = FundingRateMsg::get_predicted_funding_rate(msg);
            let timestamp = FundingRateMsg::get_timestamp(msg);
            let loan_rate = FundingRateMsg::get_loan_rate_8h(msg);
            state.last_ratio = state.calc_ratio();
            state.funding_rate = funding;
            state.predicted_rate = predicted;
            state.loan_rate = loan_rate;
            state.funding_ts = timestamp;
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

fn format_ratio(ratio: Option<f64>) -> String {
    match ratio {
        Some(value) => format!("{:.6}", value),
        None => "-".to_string(),
    }
}

fn format_timestamp(ts: Option<i64>) -> String {
    match ts {
        Some(value) if value > 0 => {
            let secs = value / 1_000_000;
            let nanos = ((value % 1_000_000) * 1_000) as u32;
            if let Some(dt) = DateTime::<Utc>::from_timestamp(secs, nanos) {
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                "-".to_string()
            }
        }
        _ => "-".to_string(),
    }
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
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    info!("启动 Funding Rate Mock 控制台");

    let cfg = StrategyConfig::load()?;
    let publisher = SignalPublisher::new(SIGNAL_CHANNEL_MT_ARBITRAGE)?;
    let mut controller = MockController::new(cfg.clone(), publisher).await?;
    controller.print_help();
    controller.print_symbol_snapshot();

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
