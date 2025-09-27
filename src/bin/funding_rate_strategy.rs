use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
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
use mkt_signal::common::redis_client::RedisSettings;
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::mkt_msg::{self, AskBidSpreadMsg, FundingRateMsg, MktMsgType};
use mkt_signal::pre_trade::order_manager::{OrderType, Side};
use mkt_signal::signal::binance_forward_arb::{
    BinSingleForwardArbCloseMarginCtx, BinSingleForwardArbOpenCtx,
};
use mkt_signal::signal::trade_signal::{SignalType, TradeSignal};

const SIGNAL_CHANNEL_MT_ARBITRAGE: &str = "mt_arbitrage";
const NODE_FUNDING_STRATEGY_SUB: &str = "funding_rate_strategy";
const DEFAULT_CFG_PATH: &str = "config/funding_rate_strategy.toml";
const DEFAULT_TRACKING_PATH: &str = "config/tracking_symbol.json";

/// 数据源类型
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

/// 策略总体配置
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
            .with_context(|| format!("读取配置文件失败: {}", cfg_path))?;
        let mut cfg: StrategyConfig =
            toml::from_str(&content).with_context(|| format!("解析配置文件失败: {}", cfg_path))?;
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
            warn!("当前 Redis 模式仅预留实现，策略仍会使用本地 JSON");
        }
        info!(
            "策略配置加载完成: mode={:?} tracking_path={} reload_interval={}s",
            cfg.mode, cfg.tracking_path, cfg.reload.interval_secs
        );
        Ok(cfg)
    }

    fn tracking_path(&self) -> PathBuf {
        PathBuf::from(&self.tracking_path)
    }

    fn reload_interval(&self) -> Duration {
        Duration::from_secs(self.reload.interval_secs.max(5))
    }

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
    open_threshold: f64,
    close_threshold: f64,
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

    fn ready_for_eval(&self) -> bool {
        self.spot_quote.is_ready() && self.futures_quote.is_ready()
    }

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

/// 主策略执行引擎
struct StrategyEngine {
    cfg: StrategyConfig,
    publisher: SignalPublisher,
    symbols: HashMap<String, SymbolState>,
    futures_index: HashMap<String, String>,
    next_reload: Instant,
    stats: EngineStats,
    min_qty: MinQtyTable,
}

impl StrategyEngine {
    async fn new(cfg: StrategyConfig, publisher: SignalPublisher) -> Result<Self> {
        let mut engine = Self {
            next_reload: Instant::now(),
            cfg,
            publisher,
            symbols: HashMap::new(),
            futures_index: HashMap::new(),
            stats: EngineStats::default(),
            min_qty: MinQtyTable::new(),
        };
        engine.min_qty.refresh_binance().await?;
        engine.reload_thresholds()?;
        Ok(engine)
    }

    async fn maybe_reload(&mut self) {
        if Instant::now() < self.next_reload {
            return;
        }
        if let Err(err) = self.min_qty.refresh_binance().await {
            warn!("刷新最小下单量失败: {err:?}");
        }
        if let Err(err) = self.reload_thresholds() {
            error!("刷新追踪列表失败: {err:?}");
        }
    }

    fn reload_thresholds(&mut self) -> Result<()> {
        self.next_reload = Instant::now() + self.cfg.reload_interval();
        let entries = self.load_thresholds()?;
        let mut new_symbols: HashMap<String, SymbolState> = HashMap::new();
        let mut new_futures_index: HashMap<String, String> = HashMap::new();

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

        let removed = self.symbols.len();
        self.symbols = new_symbols;
        self.futures_index = new_futures_index;
        if removed > 0 {
            info!("移除 {} 个不再跟踪的交易对", removed);
        }
        info!("本次加载追踪交易对数量: {}", self.symbols.len());
        self.log_min_qty_table();
        Ok(())
    }

    fn load_thresholds(&self) -> Result<Vec<SymbolThreshold>> {
        match self.cfg.mode {
            DataMode::LocalJson => self.load_from_json(self.cfg.tracking_path()),
            DataMode::Redis => {
                anyhow::bail!("Redis 模式尚未实现")
            }
        }
    }

    fn load_from_json(&self, path: PathBuf) -> Result<Vec<SymbolThreshold>> {
        let data = fs::read_to_string(&path)
            .with_context(|| format!("读取 tracking_symbol.json 失败: {}", path.display()))?;
        let value: serde_json::Value = serde_json::from_str(&data)
            .with_context(|| format!("解析 tracking_symbol.json 失败: {}", path.display()))?;
        let mut result = Vec::new();
        let binance = value
            .get("binance")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow::anyhow!("tracking_symbol.json 缺少 binance 数组"))?;

        for entry in binance {
            if let Some(threshold) = Self::parse_symbol_entry(entry) {
                result.push(threshold);
            }
        }

        if result.is_empty() {
            anyhow::bail!("tracking_symbol.json 未解析到任何有效的 binance 交易对");
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
            if !(state.spot_quote.is_ready() && state.futures_quote.is_ready()) {
                continue;
            }
            let ratio = state.calc_ratio().unwrap_or(0.0);
            rows.push(vec![
                key.clone(),
                format!("{:.6}", state.spot_quote.bid),
                format!("{:.6}", state.spot_quote.ask),
                format!("{:.6}", state.futures_quote.bid),
                format!("{:.6}", state.futures_quote.ask),
                format!("{:.6}", ratio),
                format!("{:.6}", state.open_threshold),
                format!("{:.6}", state.close_threshold),
                format!("{:.6}", state.funding_rate),
                format!("{:.6}", state.predicted_rate),
                format!("{:.6}", state.loan_rate),
                format_timestamp(state.last_open_ts),
            ]);
        }

        if rows.is_empty() {
            info!("snapshot: 当前无可用价差信息");
            return;
        }

        let table = render_three_line_table(
            &[
                "Symbol",
                "SpotBid",
                "SpotAsk",
                "UMBid",
                "UMAsk",
                "Ratio",
                "OpenTh",
                "CloseTh",
                "Funding",
                "Predicted",
                "Loan",
                "LastOpen",
            ],
            &rows,
        );
        info!("价差/资金费率快照\n{}", table);
    }

    fn parse_symbol_entry(value: &serde_json::Value) -> Option<SymbolThreshold> {
        match value {
            serde_json::Value::Array(items) => {
                if items.len() < 3 {
                    return None;
                }
                let spot = items.get(0)?.as_str()?.to_uppercase();
                let futures = items.get(1)?.as_str()?.to_uppercase();
                let open_threshold = items.get(2)?.as_f64()?;
                let close_threshold = items
                    .get(3)
                    .and_then(|v| v.as_f64())
                    .unwrap_or(open_threshold);
                Some(SymbolThreshold {
                    spot_symbol: spot,
                    futures_symbol: futures,
                    open_threshold,
                    close_threshold,
                })
            }
            serde_json::Value::Object(map) => {
                let spot = map.get("spot_symbol")?.as_str()?.to_uppercase();
                let futures = map
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
                    futures_symbol: futures,
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
                return;
            }
            state.futures_quote.update(bid_price, ask_price, timestamp);
        }
        self.evaluate(&spot_key);
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

    fn evaluate(&mut self, symbol: &str) {
        let now_us = get_timestamp_us();
        let min_gap_us = self.cfg.min_signal_gap_us();

        let request = {
            let Some(state) = self.symbols.get(symbol) else {
                return;
            };
            if !state.ready_for_eval() {
                return;
            }
            let Some(ratio) = state.calc_ratio() else {
                return;
            };
            match state.position {
                PositionState::Flat if ratio <= state.open_threshold => {
                    if !state.can_emit_signal(now_us, min_gap_us) {
                        return;
                    }
                    self.build_open_request(state, ratio, now_us)
                }
                PositionState::Opened if ratio >= state.close_threshold => {
                    if !state.can_emit_signal(now_us, min_gap_us) {
                        return;
                    }
                    self.build_close_request(state, ratio, now_us)
                }
                _ => None,
            }
        };

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
                            "发送开仓信号: symbol={} amount={} side={:?} order_type={:?} price={:.6} exp_time={}",
                            open_ctx.spot_symbol,
                            open_ctx.amount,
                            open_ctx.side,
                            open_ctx.order_type,
                            open_ctx.price,
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
                            "发送平仓信号: symbol={} limit_price={:.6} exp_time={}",
                            close_ctx.spot_symbol, close_ctx.limit_price, close_ctx.exp_time
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

    fn build_open_request(
        &self,
        state: &SymbolState,
        ratio: f64,
        emit_ts: i64,
    ) -> Option<SignalRequest> {
        if state.spot_quote.bid <= 0.0 {
            return None;
        }
        let limit_price = state.spot_quote.bid * (1.0 - self.cfg.order.open_range);
        if limit_price <= 0.0 {
            warn!(
                "{} 计算得到的开仓价格非法: {:.6}",
                state.spot_symbol, limit_price
            );
            return None;
        }
        let raw_qty = if limit_price > 0.0 {
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

        let mut adjusted_qty = raw_qty.max(spot_min).max(futures_min);
        if adjusted_qty <= 0.0 {
            warn!(
                "{} 计算得到的下单数量非法: desired={:.6}, spot_min={:.6}, futures_min={:.6}",
                state.spot_symbol, raw_qty, spot_min, futures_min
            );
            return None;
        }

        let qty_step = lcm_nonzero(spot_min, futures_min);
        if qty_step > 0.0 {
            adjusted_qty = (adjusted_qty / qty_step).ceil() * qty_step;
        }

        info!(
            "{} 下单量整形: raw={:.6} spot_min={:.6} fut_min={:.6} step={:.6} final={:.6}",
            state.spot_symbol, raw_qty, spot_min, futures_min, qty_step, adjusted_qty
        );

        let qty = adjusted_qty as f32;
        let ctx = BinSingleForwardArbOpenCtx {
            spot_symbol: state.spot_symbol.clone(),
            amount: qty,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: limit_price as f32,
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
        let limit_price = state.spot_quote.ask * (1.0 + self.cfg.order.close_range);
        if limit_price <= 0.0 {
            warn!(
                "{} 计算得到的平仓价格非法: {:.6}",
                state.spot_symbol, limit_price
            );
            return None;
        }
        let ctx = BinSingleForwardArbCloseMarginCtx {
            spot_symbol: state.spot_symbol.clone(),
            limit_price: limit_price as f32,
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
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }
    env_logger::init();

    info!("启动 Binance Forward Arb 信号策略");

    let cfg = StrategyConfig::load()?;
    let publisher = SignalPublisher::new(SIGNAL_CHANNEL_MT_ARBITRAGE)?;
    let mut engine = StrategyEngine::new(cfg.clone(), publisher).await?;
    engine.log_symbol_snapshot();

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
            engine.log_symbol_snapshot();
            next_snapshot += Duration::from_secs(3);
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

fn format_timestamp(ts: Option<i64>) -> String {
    let Some(us) = ts else {
        return "-".to_string();
    };
    if us <= 0 {
        return "-".to_string();
    }
    let secs = us / 1_000_000;
    let nanos = ((us % 1_000_000).abs() as u32) * 1_000;
    if let Some(dt) = DateTime::<Utc>::from_timestamp(secs, nanos) {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        "-".to_string()
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
