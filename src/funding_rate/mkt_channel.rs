//! 行情频道模块 - 单例访问模式
//!
//! 订阅 open/hedge 两端的行情，维护价差与资金费率数据
//!
//! 数据结构：TradingVenue -> HashMap<Symbol, Value>

use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::{Duration, Instant};

use super::common::{FundingRateData, Quote};
use super::symbol_list::SymbolList;
use crate::common::mkt_msg::{
    get_msg_type, AskBidSpreadMsg, FundingRateMsg, IndexPriceMsg, MarkPriceMsg, MktMsgType,
};
use crate::common::time_util::get_timestamp_us;
use crate::rolling_metrics::kll_quantile::segmented_quantiles_linear;
use crate::signal::common::TradingVenue;
use crate::symbol_match::{normalize_symbol_for_premium_pair, normalize_symbol_for_whitelist};

// 常量定义
const ASKBID_PAYLOAD: usize = 128;
const DERIVATIVES_PAYLOAD: usize = 128;
const DERIVATIVES_HISTORY_SIZE: usize = 50;
const DERIVATIVES_MAX_SUBSCRIBERS: usize = 10;
const DERIVATIVES_SUBSCRIBER_MAX_BUFFER: usize = 8192;
const DERIVATIVES_DRAIN_BUDGET: usize = 1024;
const DECISION_QUOTE_AGE_KLL_CAPACITY: usize = 10_000;
const DECISION_QUOTE_AGE_KLL_MAX_WINDOW: Duration = Duration::from_secs(60);

// Thread-local 单例存储
thread_local! {
    static MKT_CHANNEL: RefCell<Option<MktChannelInner>> = const { RefCell::new(None) };
}

fn build_node_name(slug: &str, suffix: &str) -> String {
    format!("trade_signal_{}_{}", slug.replace('-', "_"), suffix)
}

fn is_futures(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures
            | TradingVenue::OkexFutures
            | TradingVenue::BybitFutures
            | TradingVenue::BitgetFutures
            | TradingVenue::GateFutures
    )
}

fn normalize_symbol_key(symbol: &str) -> String {
    // Keep consistent with rolling_metrics/symbol_list: uppercase, remove '-'/'_', strip trailing "SWAP".
    normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
}

fn build_market_service(slug: &str, channel: &str) -> String {
    // bridge 用于 derivatives（funding/mark/index）；askbid 走 askbid_service_root
    format!("bridge/{}/{}", slug, channel)
}

fn should_trigger_decision(symbol: &str) -> bool {
    let symbol_list = SymbolList::instance();
    symbol_list.is_in_dump_list(symbol)
        || symbol_list.is_in_fwd_trade_list(symbol)
        || symbol_list.is_in_bwd_trade_list(symbol)
}

fn mark_dirty_symbol(
    symbol: &str,
    dirty_symbols: &mut Vec<String>,
    dirty_set: &mut HashSet<String>,
) {
    if dirty_set.insert(symbol.to_string()) {
        dirty_symbols.push(symbol.to_string());
    }
}

struct DecisionQuoteAgeKll {
    label: String,
    buffer: Vec<f64>,
    window_start: Instant,
}

impl DecisionQuoteAgeKll {
    fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            buffer: Vec::with_capacity(DECISION_QUOTE_AGE_KLL_CAPACITY),
            window_start: Instant::now(),
        }
    }

    fn push(&mut self, age_us: i64) {
        self.maybe_flush();
        self.buffer.push(age_us as f64);
        if self.buffer.len() >= DECISION_QUOTE_AGE_KLL_CAPACITY {
            self.flush();
        }
    }

    fn maybe_flush(&mut self) {
        if self.window_start.elapsed() >= DECISION_QUOTE_AGE_KLL_MAX_WINDOW {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() {
            self.window_start = Instant::now();
            return;
        }

        let qs = [0.50_f32, 0.90, 0.95, 0.99];
        let (n, results) = segmented_quantiles_linear(
            self.buffer.iter().copied(),
            DECISION_QUOTE_AGE_KLL_CAPACITY,
            &qs,
        );
        let p50 = results.first().and_then(|v| *v).unwrap_or(f64::NAN);
        let p90 = results.get(1).and_then(|v| *v).unwrap_or(f64::NAN);
        let p95 = results.get(2).and_then(|v| *v).unwrap_or(f64::NAN);
        let p99 = results.get(3).and_then(|v| *v).unwrap_or(f64::NAN);
        log::info!(
            "mkt_channel[{}] decision_quote_age_us n={} p50={:.0} p90={:.0} p95={:.0} p99={:.0}",
            self.label,
            n,
            p50,
            p90,
            p95,
            p99
        );
        self.buffer.clear();
        self.window_start = Instant::now();
    }
}

fn flush_askbid_dirty_symbols(
    dirty_symbols: &mut Vec<String>,
    dirty_set: &mut HashSet<String>,
    trigger_decisions: bool,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    quotes: &Rc<RefCell<HashMap<TradingVenue, HashMap<String, Quote>>>>,
    stats_triggerable_msgs: &mut u64,
    stats_unlisted_sample: &mut HashSet<String>,
    decision_quote_age: &mut DecisionQuoteAgeKll,
) {
    if dirty_symbols.is_empty() {
        decision_quote_age.maybe_flush();
        return;
    }

    if open_venue == hedge_venue {
        dirty_symbols.clear();
        dirty_set.clear();
        return;
    }

    for sym in dirty_symbols.iter() {
        let decision_quote_ts = {
            use super::spread_factor::SpreadFactor;

            let quotes_map = quotes.borrow();
            let open_quote = quotes_map
                .get(&open_venue)
                .and_then(|m| m.get(sym))
                .copied();
            let hedge_quote = quotes_map
                .get(&hedge_venue)
                .and_then(|m| m.get(sym))
                .copied();
            if let (Some(open_q), Some(hedge_q)) = (open_quote, hedge_quote) {
                if open_q.is_valid() && hedge_q.is_valid() {
                    let spread_factor = SpreadFactor::instance();
                    spread_factor.update(
                        open_venue,
                        sym,
                        hedge_venue,
                        sym,
                        open_q.bid,
                        open_q.ask,
                        hedge_q.bid,
                        hedge_q.ask,
                    );
                    Some(open_q.ts.max(hedge_q.ts))
                } else {
                    None
                }
            } else {
                None
            }
        };

        if trigger_decisions {
            let can_trigger = should_trigger_decision(sym);
            if can_trigger {
                *stats_triggerable_msgs += 1;
                if let Some(quote_ts) = decision_quote_ts.filter(|ts| *ts > 0) {
                    decision_quote_age.push(get_timestamp_us().saturating_sub(quote_ts));
                }
                super::decision_router::trigger_decision(sym, sym, open_venue, hedge_venue);
            } else if stats_unlisted_sample.len() < 10 {
                stats_unlisted_sample.insert(sym.clone());
            }
        }
    }

    decision_quote_age.maybe_flush();

    dirty_symbols.clear();
    dirty_set.clear();
}

fn flush_derivatives_dirty_symbols(
    dirty_symbols: &mut Vec<String>,
    dirty_set: &mut HashSet<String>,
    trigger_decisions: bool,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    stats_triggerable_msgs: &mut u64,
    stats_unlisted_sample: &mut HashSet<String>,
) {
    if dirty_symbols.is_empty() {
        return;
    }

    if open_venue == hedge_venue {
        dirty_symbols.clear();
        dirty_set.clear();
        return;
    }

    if trigger_decisions {
        for sym in dirty_symbols.iter() {
            let can_trigger = should_trigger_decision(sym);
            if can_trigger {
                *stats_triggerable_msgs += 1;
                super::decision_router::trigger_decision(sym, sym, open_venue, hedge_venue);
            } else if stats_unlisted_sample.len() < 10 {
                stats_unlisted_sample.insert(sym.clone());
            }
        }
    }

    dirty_symbols.clear();
    dirty_set.clear();
}

fn process_askbid_payload(
    payload: &[u8],
    feed_venue: TradingVenue,
    quotes: &Rc<RefCell<HashMap<TradingVenue, HashMap<String, Quote>>>>,
    dirty_symbols: &mut Vec<String>,
    dirty_set: &mut HashSet<String>,
    stats_total_msgs: &mut u64,
    stats_unique_symbols: &mut HashSet<String>,
    stats_last_symbol: &mut String,
) {
    if payload.is_empty() {
        return;
    }

    let msg_type = get_msg_type(payload);
    if msg_type != MktMsgType::AskBidSpread {
        return;
    }

    // 零拷贝解析
    let symbol_raw = AskBidSpreadMsg::get_symbol(payload);
    let symbol = normalize_symbol_key(symbol_raw);
    let bid_price = AskBidSpreadMsg::get_bid_price(payload);
    let ask_price = AskBidSpreadMsg::get_ask_price(payload);
    let timestamp = AskBidSpreadMsg::get_timestamp(payload);

    let symbol_for_decision = {
        let mut quotes_map = quotes.borrow_mut();
        if let Some(venue_quotes) = quotes_map.get_mut(&feed_venue) {
            let quote = venue_quotes
                .entry(symbol.clone())
                .or_insert(Quote::default());
            quote.update(bid_price, ask_price, timestamp);
            Some(symbol)
        } else {
            None
        }
    };

    if let Some(sym) = symbol_for_decision {
        *stats_total_msgs += 1;
        *stats_last_symbol = sym.clone();
        stats_unique_symbols.insert(sym.clone());
        mark_dirty_symbol(&sym, dirty_symbols, dirty_set);
    }
}

fn maybe_log_askbid_stats(
    stats_label: &str,
    stats_window_start: &mut Instant,
    stats_total_msgs: &mut u64,
    stats_triggerable_msgs: &mut u64,
    stats_unique_symbols: &mut HashSet<String>,
    stats_unlisted_sample: &mut HashSet<String>,
    stats_last_symbol: &str,
) {
    if stats_window_start.elapsed() < Duration::from_secs(10) {
        return;
    }

    let mut sample: Vec<String> = stats_unlisted_sample.iter().cloned().collect();
    sample.sort();
    debug!(
        "askbid stats: listener={} total_msgs={} triggerable_msgs={} unique_symbols={} last_symbol={} unlisted_sample={}",
        stats_label,
        *stats_total_msgs,
        *stats_triggerable_msgs,
        stats_unique_symbols.len(),
        stats_last_symbol,
        sample.join(",")
    );
    *stats_window_start = Instant::now();
    *stats_total_msgs = 0;
    *stats_triggerable_msgs = 0;
    stats_unique_symbols.clear();
    stats_unlisted_sample.clear();
}

/// MktChannel 单例访问器（零大小类型）
pub struct MktChannel;

/// MktChannel 内部实现，包含所有状态
struct MktChannelInner {
    /// 盘口数据：TradingVenue -> HashMap<Symbol, Quote>
    quotes: Rc<RefCell<HashMap<TradingVenue, HashMap<String, Quote>>>>,

    /// Funding Rate 数据：TradingVenue -> HashMap<Symbol, FundingRateData>
    funding_rates: Rc<RefCell<HashMap<TradingVenue, HashMap<String, FundingRateData>>>>,

    /// Mark Price 数据：TradingVenue -> HashMap<Symbol, f64> (只存最新值)
    mark_prices: Rc<RefCell<HashMap<TradingVenue, HashMap<String, f64>>>>,

    /// Index Price 数据：TradingVenue -> HashMap<Symbol, f64> (只存最新值)
    index_prices: Rc<RefCell<HashMap<TradingVenue, HashMap<String, f64>>>>,
}

impl MktChannel {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        MktChannel
    }

    pub fn is_initialized() -> bool {
        MKT_CHANNEL.with(|mc| mc.borrow().is_some())
    }

    /// 访问内部状态的辅助方法（内部使用）
    fn with_inner<F, R>(f: F) -> R
    where
        F: FnOnce(&MktChannelInner) -> R,
    {
        MKT_CHANNEL.with(|mc| {
            let mc_ref = mc.borrow();
            let inner = mc_ref.as_ref().expect("MktChannel not initialized");
            f(inner)
        })
    }

    /// 初始化单例并启动订阅任务
    ///
    /// open/hedge 由调用方传入；每个 venue 都订阅 ask_bid_spread。
    /// 若某个 venue 是 futures，则额外订阅 funding/mark/index price 衍生品频道。
    pub fn init_singleton(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()> {
        Self::init_singleton_with_mode(open_venue, hedge_venue, true)
    }

    /// 初始化只读单例并启动订阅任务，但不触发任何决策逻辑。
    ///
    /// 供 dashboard / 监控类服务复用实时行情与资费缓存，避免误发交易信号。
    pub fn init_singleton_readonly(
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<()> {
        Self::init_singleton_with_mode(open_venue, hedge_venue, false)
    }

    fn init_singleton_with_mode(
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        trigger_decisions: bool,
    ) -> Result<()> {
        let open_slug = open_venue.data_pub_slug();
        let hedge_slug = hedge_venue.data_pub_slug();

        // ask_bid_spread 全部走独立的 spread_pbs 通道（MM/Arb/Dashboard 一致）
        let open_service = format!("spread_pbs/{}/ask_bid_spread", open_slug);
        let hedge_service = format!("spread_pbs/{}/ask_bid_spread", hedge_slug);

        let open_node = build_node_name(open_slug, "askbid");
        let hedge_node = build_node_name(hedge_slug, "askbid");

        let quotes = Rc::new(RefCell::new(HashMap::new()));
        let funding_rates = Rc::new(RefCell::new(HashMap::new()));
        let mark_prices = Rc::new(RefCell::new(HashMap::new()));
        let index_prices = Rc::new(RefCell::new(HashMap::new()));

        // 初始化 HashMap（为每个 TradingVenue 创建子 HashMap）
        {
            let mut quotes_map = quotes.borrow_mut();
            quotes_map.entry(open_venue).or_default();
            quotes_map.entry(hedge_venue).or_default();
        }
        for venue in [open_venue, hedge_venue] {
            if is_futures(venue) {
                funding_rates.borrow_mut().entry(venue).or_default();
                mark_prices.borrow_mut().entry(venue).or_default();
                index_prices.borrow_mut().entry(venue).or_default();
            }
        }

        info!(
            "MktChannel 初始化完成: askbid_root=spread_pbs derivatives_root=bridge trigger_decisions={}",
            trigger_decisions
        );

        // Publish the singleton before listeners start. IPC backlog can deliver a
        // market message immediately, and decision code reads MktChannel during
        // that callback.
        let inner = MktChannelInner {
            quotes: quotes.clone(),
            funding_rates: funding_rates.clone(),
            mark_prices: mark_prices.clone(),
            index_prices: index_prices.clone(),
        };

        MKT_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });

        // 启动订阅任务。open/hedge 相同（例如 MM）时只订阅一次维护盘口；
        // open/hedge 不同时用单个 paired listener 同步 drain 两路后再判断价差。
        if hedge_venue == open_venue {
            Self::spawn_askbid_listener(
                open_node,
                open_service,
                trigger_decisions,
                open_venue,
                open_venue,
                hedge_venue,
                quotes.clone(),
            );
            info!(
                "MktChannel askbid paired listener skipped for same venue={:?}",
                open_venue
            );
        } else {
            Self::spawn_paired_askbid_listener(
                open_node,
                open_service,
                hedge_node,
                hedge_service,
                trigger_decisions,
                open_venue,
                hedge_venue,
                quotes.clone(),
            );
        }

        if is_futures(open_venue) {
            let derivatives_service = build_market_service(open_slug, "derivatives");
            let derivatives_node = build_node_name(open_slug, "derivatives");
            Self::spawn_derivatives_listener(
                derivatives_node,
                derivatives_service,
                trigger_decisions,
                open_venue,
                open_venue,
                hedge_venue,
                funding_rates.clone(),
                mark_prices.clone(),
                index_prices.clone(),
            );
        }
        if hedge_venue != open_venue && is_futures(hedge_venue) {
            let derivatives_service = build_market_service(hedge_slug, "derivatives");
            let derivatives_node = build_node_name(hedge_slug, "derivatives");
            Self::spawn_derivatives_listener(
                derivatives_node,
                derivatives_service,
                trigger_decisions,
                hedge_venue,
                open_venue,
                hedge_venue,
                funding_rates.clone(),
                mark_prices.clone(),
                index_prices.clone(),
            );
        } else if hedge_venue == open_venue && is_futures(open_venue) {
            info!(
                "MktChannel derivatives listener deduped for venue={:?}",
                open_venue
            );
        }

        Ok(())
    }

    // ==================== 查询接口 ====================

    /// 查询指定 symbol + tradingVenue 的盘口
    ///
    /// # 参数
    /// - `symbol`: 交易对符号
    /// - `venue`: 交易场所
    ///
    /// # 返回
    /// - Some(Quote): 盘口数据
    /// - None: 未找到或数据无效
    pub fn get_quote(&self, symbol: &str, venue: TradingVenue) -> Option<Quote> {
        let symbol_upper = normalize_symbol_key(symbol);

        Self::with_inner(|inner| {
            let quotes_map = inner.quotes.borrow();
            let venue_quotes = quotes_map.get(&venue)?;
            let quote = venue_quotes.get(&symbol_upper)?;

            if quote.is_valid() {
                Some(*quote)
            } else {
                None
            }
        })
    }

    /// 查询 Mark Price（只返回最新值）
    ///
    /// # 参数
    /// - `symbol`: 交易对符号
    /// - `venue`: 交易场所
    pub fn get_mark_price(&self, symbol: &str, venue: TradingVenue) -> Option<f64> {
        // premium 专用规范化（USD≡USDT，只看前缀），存取双方一致
        let symbol_key = normalize_symbol_for_premium_pair(symbol);

        Self::with_inner(|inner| {
            let mark_prices_map = inner.mark_prices.borrow();
            let venue_prices = mark_prices_map.get(&venue)?;
            venue_prices.get(&symbol_key).copied()
        })
    }

    /// 查询 Index Price（只返回最新值）
    pub fn get_index_price(&self, symbol: &str, venue: TradingVenue) -> Option<f64> {
        let symbol_key = normalize_symbol_for_premium_pair(symbol);

        Self::with_inner(|inner| {
            let index_prices_map = inner.index_prices.borrow();
            let venue_prices = index_prices_map.get(&venue)?;
            venue_prices.get(&symbol_key).copied()
        })
    }

    /// 查询 Premium Rate = (mark_price - index_price) / index_price
    pub fn get_premium_rate(&self, symbol: &str, venue: TradingVenue) -> Option<f64> {
        let mark_price = self.get_mark_price(symbol, venue)?;
        let index_price = self.get_index_price(symbol, venue)?;
        (index_price > 0.0)
            .then_some((mark_price - index_price) / index_price)
            .filter(|value| value.is_finite())
    }

    /// 查询 Funding Rate 均值
    ///
    /// # 参数
    /// - `symbol`: 交易对符号
    /// - `venue`: 交易场所
    pub fn get_funding_rate_mean(&self, symbol: &str, venue: TradingVenue) -> Option<f64> {
        let symbol_upper = normalize_symbol_key(symbol);

        Self::with_inner(|inner| {
            let funding_rates_map = inner.funding_rates.borrow();
            let venue_rates = funding_rates_map.get(&venue)?;
            let rate_data = venue_rates.get(&symbol_upper)?;
            rate_data.get_mean()
        })
    }

    /// REST 补丁注入：把外部拉到的 funding rate 当作一条样本推进 rolling buffer。
    /// 与 WS 路径共用同一个 push，WS 后续到达会通过 rolling window 自然顶替这条样本。
    /// 目前仅 Gate venue 调用：futures.tickers 是事件驱动，冷门 symbol 长时间无推送，
    /// 用 `/futures/usdt/contracts` 的 funding_rate（与 WS 同字段）每分钟兜底。
    /// 返回是否真的写入（false=venue 未初始化或值非有限）。
    pub fn seed_funding_rate(&self, symbol: &str, venue: TradingVenue, rate: f64) -> bool {
        if !rate.is_finite() {
            return false;
        }
        let symbol_upper = normalize_symbol_key(symbol);
        Self::with_inner(|inner| {
            let mut map = inner.funding_rates.borrow_mut();
            if let Some(venue_rates) = map.get_mut(&venue) {
                let rate_data = venue_rates
                    .entry(symbol_upper)
                    .or_insert_with(FundingRateData::new);
                rate_data.push(rate);
                true
            } else {
                false
            }
        })
    }

    /// 查询最新 Funding Rate
    ///
    /// # 参数
    /// - `symbol`: 交易对符号
    /// - `venue`: 交易场所
    pub fn get_latest_funding_rate(&self, symbol: &str, venue: TradingVenue) -> Option<f64> {
        let symbol_upper = normalize_symbol_key(symbol);

        Self::with_inner(|inner| {
            let funding_rates_map = inner.funding_rates.borrow();
            let venue_rates = funding_rates_map.get(&venue)?;
            let rate_data = venue_rates.get(&symbol_upper)?;
            rate_data.get_latest()
        })
    }

    // ==================== 内部辅助方法 ====================

    /// 启动 ask_bid_spread 监听任务
    fn spawn_askbid_listener(
        node_name: String,
        service_name: String,
        trigger_decisions: bool,
        this_venue: TradingVenue,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        quotes: Rc<RefCell<HashMap<TradingVenue, HashMap<String, Quote>>>>,
    ) {
        tokio::task::spawn_local(async move {
            let result: Result<()> = async move {
                let node = NodeBuilder::new()
                    .name(&NodeName::new(&node_name)?)
                    .create::<ipc::Service>()?;

                let service = node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; ASKBID_PAYLOAD]>()
                    .open_or_create()?;

                let subscriber: Subscriber<ipc::Service, [u8; ASKBID_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!("订阅盘口: {} ({:?})", service_name, this_venue);

                let mut stats_window_start = Instant::now();
                let mut stats_total_msgs: u64 = 0;
                let mut stats_triggerable_msgs: u64 = 0;
                let mut stats_unique_symbols: HashSet<String> = HashSet::new();
                let mut stats_unlisted_sample: HashSet<String> = HashSet::new();
                let mut stats_last_symbol: String = String::new();
                let mut dirty_symbols: Vec<String> = Vec::new();
                let mut dirty_set: HashSet<String> = HashSet::new();
                let mut decision_quote_age = DecisionQuoteAgeKll::new(this_venue.data_pub_slug());

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            process_askbid_payload(
                                sample.payload(),
                                this_venue,
                                &quotes,
                                &mut dirty_symbols,
                                &mut dirty_set,
                                &mut stats_total_msgs,
                                &mut stats_unique_symbols,
                                &mut stats_last_symbol,
                            );
                            maybe_log_askbid_stats(
                                this_venue.data_pub_slug(),
                                &mut stats_window_start,
                                &mut stats_total_msgs,
                                &mut stats_triggerable_msgs,
                                &mut stats_unique_symbols,
                                &mut stats_unlisted_sample,
                                &stats_last_symbol,
                            );
                        }
                        Ok(None) => {
                            flush_askbid_dirty_symbols(
                                &mut dirty_symbols,
                                &mut dirty_set,
                                trigger_decisions,
                                open_venue,
                                hedge_venue,
                                &quotes,
                                &mut stats_triggerable_msgs,
                                &mut stats_unlisted_sample,
                                &mut decision_quote_age,
                            );
                            tokio::task::yield_now().await;
                        }
                        Err(err) => {
                            flush_askbid_dirty_symbols(
                                &mut dirty_symbols,
                                &mut dirty_set,
                                trigger_decisions,
                                open_venue,
                                hedge_venue,
                                &quotes,
                                &mut stats_triggerable_msgs,
                                &mut stats_unlisted_sample,
                                &mut decision_quote_age,
                            );
                            warn!("现货盘口接收错误: {}", err);
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            }
            .await;

            if let Err(err) = result {
                warn!("现货盘口监听退出: {:?}", err);
            }
        });
    }

    /// 启动 open/hedge ask_bid_spread 成对监听任务。
    ///
    /// 两路在同一个 LocalSet task 内同步 drain：只有当 open 和 hedge 两边
    /// 当前都读到 None 后，才用最新 quote 刷新价差并触发决策。
    fn spawn_paired_askbid_listener(
        open_node_name: String,
        open_service_name: String,
        hedge_node_name: String,
        hedge_service_name: String,
        trigger_decisions: bool,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        quotes: Rc<RefCell<HashMap<TradingVenue, HashMap<String, Quote>>>>,
    ) {
        tokio::task::spawn_local(async move {
            let result: Result<()> = async move {
                let open_node = NodeBuilder::new()
                    .name(&NodeName::new(&open_node_name)?)
                    .create::<ipc::Service>()?;
                let hedge_node = NodeBuilder::new()
                    .name(&NodeName::new(&hedge_node_name)?)
                    .create::<ipc::Service>()?;

                let open_service = open_node
                    .service_builder(&ServiceName::new(&open_service_name)?)
                    .publish_subscribe::<[u8; ASKBID_PAYLOAD]>()
                    .open_or_create()?;
                let hedge_service = hedge_node
                    .service_builder(&ServiceName::new(&hedge_service_name)?)
                    .publish_subscribe::<[u8; ASKBID_PAYLOAD]>()
                    .open_or_create()?;

                let open_subscriber: Subscriber<ipc::Service, [u8; ASKBID_PAYLOAD], ()> =
                    open_service.subscriber_builder().create()?;
                let hedge_subscriber: Subscriber<ipc::Service, [u8; ASKBID_PAYLOAD], ()> =
                    hedge_service.subscriber_builder().create()?;

                info!(
                    "订阅成对盘口: open={} ({:?}) hedge={} ({:?})",
                    open_service_name, open_venue, hedge_service_name, hedge_venue
                );

                let mut stats_window_start = Instant::now();
                let mut stats_total_msgs: u64 = 0;
                let mut stats_triggerable_msgs: u64 = 0;
                let mut stats_unique_symbols: HashSet<String> = HashSet::new();
                let mut stats_unlisted_sample: HashSet<String> = HashSet::new();
                let mut stats_last_symbol: String = String::new();
                let mut dirty_symbols: Vec<String> = Vec::new();
                let mut dirty_set: HashSet<String> = HashSet::new();
                let stats_label = format!(
                    "{}<->{}",
                    open_venue.data_pub_slug(),
                    hedge_venue.data_pub_slug()
                );
                let mut decision_quote_age = DecisionQuoteAgeKll::new(stats_label.clone());

                loop {
                    loop {
                        match open_subscriber.receive() {
                            Ok(Some(sample)) => {
                                process_askbid_payload(
                                    sample.payload(),
                                    open_venue,
                                    &quotes,
                                    &mut dirty_symbols,
                                    &mut dirty_set,
                                    &mut stats_total_msgs,
                                    &mut stats_unique_symbols,
                                    &mut stats_last_symbol,
                                );
                            }
                            Ok(None) => break,
                            Err(err) => {
                                flush_askbid_dirty_symbols(
                                    &mut dirty_symbols,
                                    &mut dirty_set,
                                    trigger_decisions,
                                    open_venue,
                                    hedge_venue,
                                    &quotes,
                                    &mut stats_triggerable_msgs,
                                    &mut stats_unlisted_sample,
                                    &mut decision_quote_age,
                                );
                                warn!("open 盘口接收错误: {}", err);
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                break;
                            }
                        }
                    }

                    loop {
                        match hedge_subscriber.receive() {
                            Ok(Some(sample)) => {
                                process_askbid_payload(
                                    sample.payload(),
                                    hedge_venue,
                                    &quotes,
                                    &mut dirty_symbols,
                                    &mut dirty_set,
                                    &mut stats_total_msgs,
                                    &mut stats_unique_symbols,
                                    &mut stats_last_symbol,
                                );
                            }
                            Ok(None) => break,
                            Err(err) => {
                                flush_askbid_dirty_symbols(
                                    &mut dirty_symbols,
                                    &mut dirty_set,
                                    trigger_decisions,
                                    open_venue,
                                    hedge_venue,
                                    &quotes,
                                    &mut stats_triggerable_msgs,
                                    &mut stats_unlisted_sample,
                                    &mut decision_quote_age,
                                );
                                warn!("hedge 盘口接收错误: {}", err);
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                break;
                            }
                        }
                    }

                    maybe_log_askbid_stats(
                        &stats_label,
                        &mut stats_window_start,
                        &mut stats_total_msgs,
                        &mut stats_triggerable_msgs,
                        &mut stats_unique_symbols,
                        &mut stats_unlisted_sample,
                        &stats_last_symbol,
                    );

                    flush_askbid_dirty_symbols(
                        &mut dirty_symbols,
                        &mut dirty_set,
                        trigger_decisions,
                        open_venue,
                        hedge_venue,
                        &quotes,
                        &mut stats_triggerable_msgs,
                        &mut stats_unlisted_sample,
                        &mut decision_quote_age,
                    );
                    tokio::task::yield_now().await;
                }
            }
            .await;

            if let Err(err) = result {
                warn!("成对盘口监听退出: {:?}", err);
            }
        });
    }

    /// 启动衍生品（资金费率 + mark/index price）监听任务
    fn spawn_derivatives_listener(
        node_name: String,
        service_name: String,
        trigger_decisions: bool,
        feed_venue: TradingVenue,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        funding_rates: Rc<RefCell<HashMap<TradingVenue, HashMap<String, FundingRateData>>>>,
        mark_prices: Rc<RefCell<HashMap<TradingVenue, HashMap<String, f64>>>>,
        index_prices: Rc<RefCell<HashMap<TradingVenue, HashMap<String, f64>>>>,
    ) {
        tokio::task::spawn_local(async move {
            let result: Result<()> = async move {
                let node = NodeBuilder::new()
                    .name(&NodeName::new(&node_name)?)
                    .create::<ipc::Service>()?;

                let service = node
                    .service_builder(&ServiceName::new(&service_name)?)
                    .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD]>()
                    .max_publishers(1)
                    .max_subscribers(DERIVATIVES_MAX_SUBSCRIBERS)
                    .history_size(DERIVATIVES_HISTORY_SIZE)
                    .subscriber_max_buffer_size(DERIVATIVES_SUBSCRIBER_MAX_BUFFER)
                    .open_or_create()?;

                let subscriber: Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!("订阅衍生品数据: {}", service_name);

                let mut stats_window_start = Instant::now();
                let mut stats_funding_msgs: u64 = 0;
                let mut stats_mark_msgs: u64 = 0;
                let mut stats_triggerable_msgs: u64 = 0;
                let mut stats_unique_symbols: HashSet<String> = HashSet::new();
                let mut stats_unlisted_sample: HashSet<String> = HashSet::new();
                let mut stats_last_symbol: String = String::new();
                let mut dirty_funding_symbols: Vec<String> = Vec::new();
                let mut dirty_funding_set: HashSet<String> = HashSet::new();
                let mut drain_count: usize = 0;

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            drain_count += 1;
                            let payload = sample.payload();
                            if payload.is_empty() {
                                if drain_count >= DERIVATIVES_DRAIN_BUDGET {
                                    flush_derivatives_dirty_symbols(
                                        &mut dirty_funding_symbols,
                                        &mut dirty_funding_set,
                                        trigger_decisions,
                                        open_venue,
                                        hedge_venue,
                                        &mut stats_triggerable_msgs,
                                        &mut stats_unlisted_sample,
                                    );
                                    drain_count = 0;
                                    tokio::task::yield_now().await;
                                }
                                continue;
                            }

                            let msg_type = get_msg_type(payload);
                            match msg_type {
                                MktMsgType::FundingRate => {
                                    // 零拷贝解析
                                    let symbol_raw = FundingRateMsg::get_symbol(payload);
                                    let symbol = normalize_symbol_key(symbol_raw);
                                    let funding_rate = FundingRateMsg::get_funding_rate(payload);
                                    stats_funding_msgs += 1;
                                    stats_last_symbol = symbol.clone();
                                    stats_unique_symbols.insert(symbol.clone());

                                    let symbol_for_decision = {
                                        let mut funding_rates_map = funding_rates.borrow_mut();
                                        if let Some(venue_rates) =
                                            funding_rates_map.get_mut(&feed_venue)
                                        {
                                            let rate_data = venue_rates
                                                .entry(symbol.clone())
                                                .or_insert_with(FundingRateData::new);

                                            // 立刻更新均值
                                            rate_data.push(funding_rate);
                                            Some(symbol.clone())
                                        } else {
                                            None
                                        }
                                    };

                                    if let Some(sym) = symbol_for_decision {
                                        mark_dirty_symbol(
                                            &sym,
                                            &mut dirty_funding_symbols,
                                            &mut dirty_funding_set,
                                        );
                                    }
                                }
                                MktMsgType::MarkPrice => {
                                    // 零拷贝解析
                                    let symbol_raw = MarkPriceMsg::get_symbol(payload);
                                    // premium 配对：USD≡USDT，统一前缀，让 BTCUSDT/BTCUSD 落到同一 key
                                    let symbol = normalize_symbol_for_premium_pair(symbol_raw);
                                    let mark_price = MarkPriceMsg::get_mark_price(payload);
                                    stats_mark_msgs += 1;

                                    let mut mark_prices_map = mark_prices.borrow_mut();
                                    if let Some(venue_prices) = mark_prices_map.get_mut(&feed_venue)
                                    {
                                        venue_prices.insert(symbol, mark_price);
                                    }
                                }
                                MktMsgType::IndexPrice => {
                                    let symbol_raw = IndexPriceMsg::get_symbol(payload);
                                    let symbol = normalize_symbol_for_premium_pair(symbol_raw);
                                    let index_price = IndexPriceMsg::get_index_price(payload);

                                    let mut index_prices_map = index_prices.borrow_mut();
                                    if let Some(venue_prices) =
                                        index_prices_map.get_mut(&feed_venue)
                                    {
                                        venue_prices.insert(symbol, index_price);
                                    }
                                }
                                _ => {}
                            }

                            if stats_window_start.elapsed() >= Duration::from_secs(10) {
                                let mut sample: Vec<String> =
                                    stats_unlisted_sample.iter().cloned().collect();
                                sample.sort();
                                debug!(
                                    "derivatives stats: venue={:?} funding_msgs={} mark_msgs={} triggerable_msgs={} unique_symbols={} last_symbol={} unlisted_sample={}",
                                    feed_venue,
                                    stats_funding_msgs,
                                    stats_mark_msgs,
                                    stats_triggerable_msgs,
                                    stats_unique_symbols.len(),
                                    stats_last_symbol,
                                    sample.join(",")
                                );
                                stats_window_start = Instant::now();
                                stats_funding_msgs = 0;
                                stats_mark_msgs = 0;
                                stats_triggerable_msgs = 0;
                                stats_unique_symbols.clear();
                                stats_unlisted_sample.clear();
                            }

                            if drain_count >= DERIVATIVES_DRAIN_BUDGET {
                                flush_derivatives_dirty_symbols(
                                    &mut dirty_funding_symbols,
                                    &mut dirty_funding_set,
                                    trigger_decisions,
                                    open_venue,
                                    hedge_venue,
                                    &mut stats_triggerable_msgs,
                                    &mut stats_unlisted_sample,
                                );
                                drain_count = 0;
                                tokio::task::yield_now().await;
                            }
                        }
                        Ok(None) => {
                            flush_derivatives_dirty_symbols(
                                &mut dirty_funding_symbols,
                                &mut dirty_funding_set,
                                trigger_decisions,
                                open_venue,
                                hedge_venue,
                                &mut stats_triggerable_msgs,
                                &mut stats_unlisted_sample,
                            );
                            drain_count = 0;
                            tokio::task::yield_now().await;
                        }
                        Err(err) => {
                            flush_derivatives_dirty_symbols(
                                &mut dirty_funding_symbols,
                                &mut dirty_funding_set,
                                trigger_decisions,
                                open_venue,
                                hedge_venue,
                                &mut stats_triggerable_msgs,
                                &mut stats_unlisted_sample,
                            );
                            drain_count = 0;
                            warn!("衍生品数据接收错误: {}", err);
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            }
            .await;

            if let Err(err) = result {
                warn!("衍生品数据监听退出: {:?}", err);
            }
        });
    }
}
