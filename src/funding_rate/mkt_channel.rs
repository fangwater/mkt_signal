//! 行情频道模块 - 单例访问模式
//!
//! 订阅 open/hedge 两端的行情，维护价差与资金费率数据
//!
//! 数据结构：TradingVenue -> HashMap<Symbol, Value>

use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use super::common::{FundingRateData, Quote};
use crate::common::mkt_msg::{
    get_msg_type, AskBidSpreadMsg, FundingRateMsg, MarkPriceMsg, MktMsgType,
};
use crate::signal::common::TradingVenue;

// 常量定义
const ASKBID_PAYLOAD: usize = 64;
const DERIVATIVES_PAYLOAD: usize = 128;

// Thread-local 单例存储
thread_local! {
    static MKT_CHANNEL: RefCell<Option<MktChannelInner>> = RefCell::new(None);
}

fn build_node_name(slug: &str, suffix: &str) -> String {
    format!("fr_signal_{}_{}", slug.replace('-', "_"), suffix)
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
        let open_slug = open_venue.data_pub_slug();
        let hedge_slug = hedge_venue.data_pub_slug();

        let open_service = format!("data_pubs/{}/ask_bid_spread", open_slug);
        let hedge_service = format!("data_pubs/{}/ask_bid_spread", hedge_slug);

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
            open_venue,
            open_venue,
            hedge_venue,
            quotes.clone(),
        );
        Self::spawn_askbid_listener(
            hedge_node,
            hedge_service,
            hedge_venue,
            open_venue,
            hedge_venue,
            quotes.clone(),
        );
        if is_futures(open_venue) {
            let derivatives_service = format!("data_pubs/{}/derivatives", open_slug);
            let derivatives_node = build_node_name(open_slug, "derivatives");
            Self::spawn_derivatives_listener(
                derivatives_node,
                derivatives_service,
                open_venue,
                open_venue,
                hedge_venue,
                funding_rates.clone(),
                mark_prices.clone(),
            );
        }
        if is_futures(hedge_venue) {
            let derivatives_service = format!("data_pubs/{}/derivatives", hedge_slug);
            let derivatives_node = build_node_name(hedge_slug, "derivatives");
            Self::spawn_derivatives_listener(
                derivatives_node,
                derivatives_service,
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
        let symbol_upper = symbol.to_uppercase();

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
        let symbol_upper = symbol.to_uppercase();

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
        let symbol_upper = symbol.to_uppercase();

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
        let symbol_upper = symbol.to_uppercase();

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
                                let symbol = AskBidSpreadMsg::get_symbol(payload).to_uppercase();
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
                                    use super::decision::FrDecision;
                                    FrDecision::with_mut(|decision| {
                                        let _ = decision.make_combined_decision(
                                            &sym,
                                            &sym,
                                            open_venue,
                                            hedge_venue,
                                        );
                                    });
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
                    .open_or_create()?;

                let subscriber: Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()> =
                    service.subscriber_builder().create()?;

                info!("订阅衍生品数据: {}", service_name);

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
                                    let symbol = FundingRateMsg::get_symbol(payload).to_uppercase();
                                    let funding_rate = FundingRateMsg::get_funding_rate(payload);

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
                                        use super::decision::FrDecision;
                                        FrDecision::with_mut(|decision| {
                                            let _ = decision.make_combined_decision(
                                                &sym,
                                                &sym,
                                                open_venue,
                                                hedge_venue,
                                            );
                                        });
                                    }
                                }
                                MktMsgType::MarkPrice => {
                                    // 零拷贝解析
                                    let symbol = MarkPriceMsg::get_symbol(payload).to_uppercase();
                                    let mark_price = MarkPriceMsg::get_mark_price(payload);

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
