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
    get_msg_type, AskBidSpreadMsg, FundingRateMsg, MarkPriceMsg, MktMsgType,
};
use crate::signal::common::TradingVenue;
use crate::symbol_match::normalize_symbol_for_whitelist;

// 常量定义
const ASKBID_PAYLOAD: usize = 128;
const DERIVATIVES_PAYLOAD: usize = 128;
const DERIVATIVES_HISTORY_SIZE: usize = 50;
const DERIVATIVES_MAX_SUBSCRIBERS: usize = 10;
const DERIVATIVES_SUBSCRIBER_MAX_BUFFER: usize = 8192;

// Thread-local 单例存储
thread_local! {
    static MKT_CHANNEL: RefCell<Option<MktChannelInner>> = RefCell::new(None);
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

fn should_trigger_decision(symbol: &str) -> bool {
    let symbol_list = SymbolList::instance();
    symbol_list.is_in_dump_list(symbol)
        || symbol_list.is_in_fwd_trade_list(symbol)
        || symbol_list.is_in_bwd_trade_list(symbol)
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
}

impl MktChannel {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        MktChannel
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
    /// 若某个 venue 是 futures，则额外订阅 funding/mark price 衍生品频道。
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

        let open_service = format!("dat_pbs/{}/ask_bid_spread", open_slug);
        let hedge_service = format!("dat_pbs/{}/ask_bid_spread", hedge_slug);

        let open_node = build_node_name(open_slug, "askbid");
        let hedge_node = build_node_name(hedge_slug, "askbid");

        let quotes = Rc::new(RefCell::new(HashMap::new()));
        let funding_rates = Rc::new(RefCell::new(HashMap::new()));
        let mark_prices = Rc::new(RefCell::new(HashMap::new()));

        // 初始化 HashMap（为每个 TradingVenue 创建子 HashMap）
        {
            quotes.borrow_mut().insert(open_venue, HashMap::new());
            quotes.borrow_mut().insert(hedge_venue, HashMap::new());

            if is_futures(open_venue) {
                funding_rates
                    .borrow_mut()
                    .insert(open_venue, HashMap::new());
                mark_prices.borrow_mut().insert(open_venue, HashMap::new());
            }
            if is_futures(hedge_venue) {
                funding_rates
                    .borrow_mut()
                    .insert(hedge_venue, HashMap::new());
                mark_prices.borrow_mut().insert(hedge_venue, HashMap::new());
            }
        }

        info!("MktChannel 初始化完成");

        // 启动订阅任务
        Self::spawn_askbid_listener(
            open_node,
            open_service,
            trigger_decisions,
            open_venue,
            open_venue,
            hedge_venue,
            quotes.clone(),
        );
        Self::spawn_askbid_listener(
            hedge_node,
            hedge_service,
            trigger_decisions,
            hedge_venue,
            open_venue,
            hedge_venue,
            quotes.clone(),
        );
        if is_futures(open_venue) {
            let derivatives_service = format!("dat_pbs/{}/derivatives", open_slug);
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
            );
        }
        if is_futures(hedge_venue) {
            let derivatives_service = format!("dat_pbs/{}/derivatives", hedge_slug);
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
            );
        }

        // 保存到 thread-local
        let inner = MktChannelInner {
            quotes,
            funding_rates,
            mark_prices,
        };

        MKT_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });

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
        let symbol_upper = normalize_symbol_key(symbol);

        Self::with_inner(|inner| {
            let mark_prices_map = inner.mark_prices.borrow();
            let venue_prices = mark_prices_map.get(&venue)?;
            venue_prices.get(&symbol_upper).copied()
        })
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

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let payload = sample.payload();
                            if payload.is_empty() {
                                continue;
                            }

                            let msg_type = get_msg_type(payload);
                            if msg_type == MktMsgType::AskBidSpread {
                                // 零拷贝解析
                                let symbol_raw = AskBidSpreadMsg::get_symbol(payload);
                                let symbol = normalize_symbol_key(symbol_raw);
                                let bid_price = AskBidSpreadMsg::get_bid_price(payload);
                                let ask_price = AskBidSpreadMsg::get_ask_price(payload);
                                let timestamp = AskBidSpreadMsg::get_timestamp(payload);

                                let symbol_for_decision = {
                                    let mut quotes_map = quotes.borrow_mut();
                                    if let Some(venue_quotes) = quotes_map.get_mut(&this_venue) {
                                        let quote = venue_quotes
                                            .entry(symbol.clone())
                                            .or_insert(Quote::default());
                                        quote.update(bid_price, ask_price, timestamp);

                                        // debug!(
                                        //     "现货盘口更新: {} bid={:.6} ask={:.6}",
                                        //     symbol, bid_price, ask_price
                                        // );
                                        Some(symbol.clone())
                                    } else {
                                        None
                                    }
                                };

                                // 更新价差因子
                                if let Some(sym) = &symbol_for_decision {
                                    use super::spread_factor::SpreadFactor;

                                    // 获取 open/hedge 盘口并更新价差因子（保持 open->hedge 方向）
                                    let quotes_map = quotes.borrow();
                                    let open_quote = quotes_map
                                        .get(&open_venue)
                                        .and_then(|m| m.get(sym))
                                        .copied();
                                    let hedge_quote = quotes_map
                                        .get(&hedge_venue)
                                        .and_then(|m| m.get(sym))
                                        .copied();
                                    if let (Some(open_q), Some(hedge_q)) = (open_quote, hedge_quote)
                                    {
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
                                        }
                                    }
                                }

                                // 盘口更新后触发决策（事件驱动）
                                if let Some(sym) = symbol_for_decision {
                                    stats_total_msgs += 1;
                                    stats_last_symbol = sym.clone();
                                    stats_unique_symbols.insert(sym.clone());

                                    if trigger_decisions {
                                        let can_trigger = should_trigger_decision(&sym);
                                        if can_trigger {
                                            stats_triggerable_msgs += 1;
                                            super::decision_router::trigger_decision(
                                                &sym,
                                                &sym,
                                                open_venue,
                                                hedge_venue,
                                            );
                                        } else if stats_unlisted_sample.len() < 10 {
                                            stats_unlisted_sample.insert(sym.clone());
                                        }
                                    }
                                }

                                if stats_window_start.elapsed() >= Duration::from_secs(10) {
                                    let mut sample: Vec<String> =
                                        stats_unlisted_sample.iter().cloned().collect();
                                    sample.sort();
                                    debug!(
                                        "askbid stats: venue={:?} total_msgs={} triggerable_msgs={} unique_symbols={} last_symbol={} unlisted_sample={}",
                                        this_venue,
                                        stats_total_msgs,
                                        stats_triggerable_msgs,
                                        stats_unique_symbols.len(),
                                        stats_last_symbol,
                                        sample.join(",")
                                    );
                                    stats_window_start = Instant::now();
                                    stats_total_msgs = 0;
                                    stats_triggerable_msgs = 0;
                                    stats_unique_symbols.clear();
                                    stats_unlisted_sample.clear();
                                }
                            }
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(err) => {
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

    /// 启动衍生品（资金费率 + mark price）监听任务
    fn spawn_derivatives_listener(
        node_name: String,
        service_name: String,
        trigger_decisions: bool,
        feed_venue: TradingVenue,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        funding_rates: Rc<RefCell<HashMap<TradingVenue, HashMap<String, FundingRateData>>>>,
        mark_prices: Rc<RefCell<HashMap<TradingVenue, HashMap<String, f64>>>>,
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

                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let payload = sample.payload();
                            if payload.is_empty() {
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

                                    // Funding Rate MA 重算后触发决策（事件驱动）
                                    if let Some(sym) = symbol_for_decision {
                                        if trigger_decisions {
                                            let can_trigger = should_trigger_decision(&sym);
                                            if can_trigger {
                                                stats_triggerable_msgs += 1;
                                                super::decision_router::trigger_decision(
                                                    &sym,
                                                    &sym,
                                                    open_venue,
                                                    hedge_venue,
                                                );
                                            } else if stats_unlisted_sample.len() < 10 {
                                                stats_unlisted_sample.insert(sym.clone());
                                            }
                                        }
                                    }
                                }
                                MktMsgType::MarkPrice => {
                                    // 零拷贝解析
                                    let symbol_raw = MarkPriceMsg::get_symbol(payload);
                                    let symbol = normalize_symbol_key(symbol_raw);
                                    let mark_price = MarkPriceMsg::get_mark_price(payload);
                                    stats_mark_msgs += 1;

                                    let mut mark_prices_map = mark_prices.borrow_mut();
                                    if let Some(venue_prices) = mark_prices_map.get_mut(&feed_venue)
                                    {
                                        // 只存最新值
                                        venue_prices.insert(symbol.clone(), mark_price);

                                        // debug!(
                                        //     "Mark Price 更新: {} price={:.6}",
                                        //     symbol, mark_price
                                        // );
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
                        }
                        Ok(None) => tokio::task::yield_now().await,
                        Err(err) => {
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
