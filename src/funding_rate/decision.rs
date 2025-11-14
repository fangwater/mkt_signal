//! Funding Rate 套利决策模块
//!
//! 纯决策逻辑，不维护状态。接收状态作为参数，返回决策结果并发布信号。

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;
use tokio::runtime::Builder;
use tokio::time::{interval, Duration};

use crate::common::iceoryx_publisher::{SignalPublisher, SIGNAL_PAYLOAD};
use crate::common::iceoryx_subscriber::GenericSignalSubscriber;
use crate::common::min_qty_table::MinQtyTable;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{SignalBytes, TradingLeg, TradingVenue};
use crate::signal::hedge_signal::{ArbHedgeCtx, ArbHedgeSignalQueryMsg};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};

use super::funding_rate_factor::FundingRateFactor;
use super::mkt_channel::MktChannel;
use super::rate_fetcher::RateFetcher;
use super::spread_factor::SpreadFactor;
use super::state::Quote;

// ========== 线程本地单例 ==========

thread_local! {
    static FR_DECISION: OnceCell<RefCell<FrDecision>> = OnceCell::new();
}

// ========== 配置常量 ==========

/// 默认信号发布频道名称（发往 pre_trade）
/// 对应 pre_trade/signal_channel.rs 中的订阅频道
pub const DEFAULT_SIGNAL_CHANNEL: &str = "funding_rate_signal";

/// 默认反向订阅频道名称（来自 pre_trade 的查询反馈）
/// 对应 pre_trade/signal_channel.rs 中的 DEFAULT_BACKWARD_CHANNEL
pub const DEFAULT_BACKWARD_CHANNEL: &str = "signal_query";

// ========== 无状态设计 ==========
// FrDecision 不维护任何状态，所有状态由外部（如 Engine）维护

// ========== 资费信号类型 ==========

/// 资费信号类型（内部使用）
#[derive(Debug, Clone, Copy)]
enum FrSignal {
    ForwardOpen,   // 正套开仓
    ForwardClose,  // 正套平仓
    BackwardOpen,  // 反套开仓
    BackwardClose, // 反套平仓
}

// ========== 核心决策类 ==========

/// FrDecision - 套利决策单例（无状态）
///
/// 负责：
/// 1. 接收外部调用，根据传入的 SymbolState 进行决策
/// 2. 订阅来自 pre_trade 的反向查询（backward channel）
/// 3. 发布交易信号到 pre_trade（arb_open/arb_close/arb_cancel）
///
/// # 设计原则
/// - 无状态：不维护任何交易对状态，状态由外部管理
/// - 纯决策：只负责根据输入做决策并发送信号
///
/// # 设计对齐
/// - 与 pre_trade/signal_channel.rs 保持命名和结构一致
/// - signal_pub: 向下游发送信号（对应 pre_trade 的订阅）
/// - backward_sub: 接收下游查询（对应 pre_trade 的 backward_pub）
pub struct FrDecision {
    /// 信号发布器（发往 pre_trade）
    /// 对应 pre_trade/signal_channel.rs 订阅的频道
    signal_pub: SignalPublisher,

    /// 反向订阅器（来自 pre_trade 的查询反馈）
    /// 对应 pre_trade/signal_channel.rs 的 backward_pub
    backward_sub: GenericSignalSubscriber,

    /// 频道名称（用于日志）
    channel_name: String,

    /// IceOryx Node（用于创建服务）
    _node: Node<ipc::Service>,

    /// 挂单价格偏移列表（用于 open 信号的多档位挂单）
    /// 默认：[0.0002, 0.0004, 0.0006, 0.0008, 0.001]
    price_offsets: Vec<f64>,

    /// Binance 交易对过滤器，用于 price_tick / min_qty 查询
    min_qty_table: MinQtyTable,

    /// 单笔下单数量（单位：base asset）
    order_amount: f32,

    /// 开仓挂单最长挂单时间（微秒）
    open_order_ttl_us: i64,

    /// 对冲挂单（MM 模式）最长挂单时间（微秒）
    hedge_timeout_mm_us: i64,

    /// 对冲限价偏移（例如 0.0003 表示万分之 3）
    hedge_price_offset: f64,

    /// 需要检查的 symbol 列表（币安现货和期货共同的 symbol）
    /// 使用 Rc<RefCell<>> 以便在 spawn_local 任务中共享
    check_symbols: Rc<RefCell<HashSet<String>>>,
}

impl FrDecision {
    /// 访问线程本地单例（只读）
    ///
    /// # 使用示例
    /// ```ignore
    /// FrDecision::with(|decision| {
    ///     decision.get_symbol_state("BTCUSDT");
    /// });
    /// ```
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&FrDecision) -> R,
    {
        FR_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("FrDecision not initialized. Call init_singleton() first");
            f(&decision_ref.borrow())
        })
    }

    /// 可变访问线程本地单例
    ///
    /// # 使用示例
    /// ```ignore
    /// FrDecision::with_mut(|decision| {
    ///     decision.on_funding_rate_change("BTCUSDT", 0.0001);
    /// });
    /// ```
    pub fn with_mut<F, R>(f: F) -> R
    where
        F: FnOnce(&mut FrDecision) -> R,
    {
        FR_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("FrDecision not initialized. Call init_singleton() first");
            f(&mut decision_ref.borrow_mut())
        })
    }

    /// 初始化单例（必须在 LocalSet 中调用）
    ///
    /// 自动启动：
    /// - spawn_decision_loop: 10ms 决策循环
    /// - spawn_backward_listener: 100ms backward 监听
    pub fn init_singleton() -> Result<()> {
        let result: Result<()> = FR_DECISION.with(|cell| {
            if cell.get().is_some() {
                return Ok(());
            }
            let decision = Self::new()?;
            cell.set(RefCell::new(decision))
                .map_err(|_| anyhow::anyhow!("Failed to initialize FrDecision singleton"))?;
            info!("FrDecision singleton initialized");
            Ok(())
        });
        result?;

        // 启动决策循环和 backward 监听任务
        Self::spawn_decision_loop();
        Self::spawn_backward_listener();
        info!("FrDecision spawn tasks started");

        Ok(())
    }

    /// 创建新实例（私有）
    fn new() -> Result<Self> {
        let node_name = NodeName::new("fr_decision")?;
        let node = NodeBuilder::new()
            .name(&node_name)
            .create::<ipc::Service>()?;

        // 1. 创建信号发布器（发往 pre_trade）
        let signal_pub = SignalPublisher::new(DEFAULT_SIGNAL_CHANNEL)?;
        info!(
            "FrDecision: signal publisher created on '{}'",
            DEFAULT_SIGNAL_CHANNEL
        );

        // 2. 订阅反向频道（来自 pre_trade 的查询反馈）
        let backward_sub = Self::create_subscriber(&node, DEFAULT_BACKWARD_CHANNEL)?;
        info!(
            "FrDecision: backward subscriber created on '{}'",
            DEFAULT_BACKWARD_CHANNEL
        );

        // 默认挂单偏移：万2 到 千1，共5档
        let price_offsets = vec![0.0002, 0.0004, 0.0006, 0.0008, 0.001];

        let mut min_qty_table = MinQtyTable::new();
        if let Err(err) = Self::refresh_min_qty_blocking(&mut min_qty_table) {
            warn!(
                "FrDecision: failed to refresh Binance exchange filters, price_tick may be zero: {err:#}"
            );
        }

        Ok(Self {
            signal_pub,
            backward_sub,
            channel_name: DEFAULT_SIGNAL_CHANNEL.to_string(),
            _node: node,
            price_offsets,
            min_qty_table,
            order_amount: 100.0,
            open_order_ttl_us: 120_000_000,
            hedge_timeout_mm_us: 30_000_000,
            hedge_price_offset: 0.0003,
            check_symbols: Rc::new(RefCell::new(HashSet::new())),
        })
    }

    fn refresh_min_qty_blocking(table: &mut MinQtyTable) -> Result<()> {
        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(table.refresh_binance())
    }

    /// 创建订阅器（helper）
    fn create_subscriber(
        node: &Node<ipc::Service>,
        channel_name: &str,
    ) -> Result<GenericSignalSubscriber> {
        let service_name = format!("signal_pubs/{}", channel_name);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
            .open_or_create()?;

        let subscriber = service.subscriber_builder().create()?;
        Ok(GenericSignalSubscriber::Size4K(subscriber))
    }

    // ========== 外部主动触发接口 ==========

    /// 联合信号决策：同时检查资费和价差因子
    ///
    /// # 决策流程
    /// 1. 优先检查 cancel 信号（只和价差有关）
    /// 2. 获取资费信号
    /// 3. 如果资费没有信号，返回 None
    /// 4. 如果资费有信号，验证对应的价差 satisfy
    /// 5. 只有资费和价差同时满足时才发出信号
    ///
    /// # 参数
    /// - `spot_symbol`: 现货交易对
    /// - `futures_symbol`: 合约交易对
    /// - `spot_venue`: 现货交易所
    /// - `futures_venue`: 合约交易所
    ///
    /// # 返回
    /// 如果需要发送信号，返回 Ok(Some(signal_type))；否则返回 Ok(None)
    pub fn make_combined_decision(
        &mut self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
    ) -> Result<Option<SignalType>> {
        let spread_factor = SpreadFactor::instance();

        // 步骤1: 优先检查 cancel 信号（只和价差有关，不需要资费）
        if spread_factor.satisfy_forward_cancel(
            spot_venue,
            spot_symbol,
            futures_venue,
            futures_symbol,
        ) {
            self.emit_signals(
                spot_symbol,
                futures_symbol,
                spot_venue,
                futures_venue,
                SignalType::ArbCancel,
                None,
            )?;
            return Ok(Some(SignalType::ArbCancel));
        }
        if spread_factor.satisfy_backward_cancel(
            spot_venue,
            spot_symbol,
            futures_venue,
            futures_symbol,
        ) {
            self.emit_signals(
                spot_symbol,
                futures_symbol,
                spot_venue,
                futures_venue,
                SignalType::ArbCancel,
                None,
            )?;
            return Ok(Some(SignalType::ArbCancel));
        }

        // 步骤2: 获取资费信号
        let fr_signal = self.get_funding_rate_signal(spot_symbol, futures_symbol, futures_venue)?;

        // 步骤3: 如果资费没有信号，返回 None
        let fr_signal = match fr_signal {
            Some(s) => s,
            None => return Ok(None),
        };

        // 步骤4: 根据资费信号验证对应的价差 satisfy
        let final_signal = match fr_signal {
            FrSignal::ForwardOpen => {
                if spread_factor.satisfy_forward_open(
                    spot_venue,
                    spot_symbol,
                    futures_venue,
                    futures_symbol,
                ) {
                    Some(SignalType::ArbOpen)
                } else {
                    None
                }
            }
            FrSignal::ForwardClose => {
                if spread_factor.satisfy_forward_close(
                    spot_venue,
                    spot_symbol,
                    futures_venue,
                    futures_symbol,
                ) {
                    Some(SignalType::ArbClose)
                } else {
                    None
                }
            }
            FrSignal::BackwardOpen => {
                if spread_factor.satisfy_backward_open(
                    spot_venue,
                    spot_symbol,
                    futures_venue,
                    futures_symbol,
                ) {
                    Some(SignalType::ArbOpen)
                } else {
                    None
                }
            }
            FrSignal::BackwardClose => {
                if spread_factor.satisfy_backward_close(
                    spot_venue,
                    spot_symbol,
                    futures_venue,
                    futures_symbol,
                ) {
                    Some(SignalType::ArbClose)
                } else {
                    None
                }
            }
        };

        // 步骤5: 如果价差不满足，返回 None
        let final_signal = match final_signal {
            Some(s) => s,
            None => return Ok(None),
        };

        let signal_side = Self::side_from_fr_signal(fr_signal);

        // 步骤6: 发送信号
        self.emit_signals(
            spot_symbol,
            futures_symbol,
            spot_venue,
            futures_venue,
            final_signal.clone(),
            Some(signal_side),
        )?;

        info!(
            "Combined signal: fr={:?} final={:?} spot={} futures={}",
            fr_signal, final_signal, spot_symbol, futures_symbol
        );

        Ok(Some(final_signal))
    }

    /// 获取资费因子信号
    ///
    /// 通过查询 FundingRateFactor 的 satisfy 方法判断资费信号
    fn get_funding_rate_signal(
        &self,
        _spot_symbol: &str,
        futures_symbol: &str,
        futures_venue: TradingVenue,
    ) -> Result<Option<FrSignal>> {
        let fr_factor = FundingRateFactor::instance();
        let rate_fetcher = RateFetcher::instance();

        // 从 RateFetcher 获取该 symbol 的周期
        let period = rate_fetcher.get_period(futures_symbol, futures_venue);

        // 按优先级检查资费信号：close > open
        // 优先级1: 平仓
        if fr_factor.satisfy_forward_close(futures_symbol, period, futures_venue) {
            return Ok(Some(FrSignal::ForwardClose));
        }
        if fr_factor.satisfy_backward_close(futures_symbol, period, futures_venue) {
            return Ok(Some(FrSignal::BackwardClose));
        }

        // 优先级2: 开仓
        if fr_factor.satisfy_forward_open(futures_symbol, period) {
            return Ok(Some(FrSignal::ForwardOpen));
        }
        if fr_factor.satisfy_backward_open(futures_symbol, period) {
            return Ok(Some(FrSignal::BackwardOpen));
        }

        // 无资费信号
        Ok(None)
    }

    /// 处理反向查询（来自 pre_trade 的 backward channel）
    fn handle_backward_query(&mut self, data: Bytes) {
        let query = match ArbHedgeSignalQueryMsg::from_bytes(data) {
            Ok(q) => q,
            Err(err) => {
                warn!("FrDecision: 解析 hedge query 失败: {err}");
                return;
            }
        };

        let Some(side) = query.get_side() else {
            warn!("FrDecision: hedge query side 无效: {}", query.hedge_side);
            return;
        };

        let Some(venue) = TradingVenue::from_u8(query.venue) else {
            warn!("FrDecision: hedge query venue 无效: {}", query.venue);
            return;
        };

        let hedge_symbol = query.get_hedging_symbol();
        if hedge_symbol.is_empty() {
            warn!("FrDecision: hedge query 未携带对冲 symbol");
            return;
        }

        let qty = query.hedge_qty;
        if qty <= 0.0 {
            warn!(
                "FrDecision: hedge query quantity <= 0 strategy_id={} qty={:.8}",
                query.strategy_id, qty
            );
            return;
        }

        let mkt_channel = MktChannel::instance();
        let Some(fut_quote) = mkt_channel.get_quote(&hedge_symbol, venue) else {
            warn!(
                "FrDecision: hedge query 无行情 strategy_id={} symbol={} venue={:?}",
                query.strategy_id, hedge_symbol, venue
            );
            return;
        };

        let price_tick = self
            .min_qty_table
            .futures_um_price_tick_by_symbol(&hedge_symbol)
            .unwrap_or(0.0);

        let offset = if query.request_seq < 6 {
            self.hedge_price_offset.abs()
        } else {
            0.0
        };

        let now = get_timestamp_us();
        let aggressive = query.request_seq >= 6;

        let ctx = if aggressive {
            let price = match side {
                Side::Buy => fut_quote.ask,
                Side::Sell => fut_quote.bid,
            };
            if price <= 0.0 {
                warn!(
                    "FrDecision: hedge query aggressive price 无效 strategy_id={} price={:.8}",
                    query.strategy_id, price
                );
                return;
            }
            let mut ctx =
                ArbHedgeCtx::new_taker(query.strategy_id, query.client_order_id, qty, side.to_u8());
            ctx.hedging_leg = TradingLeg::new(venue, fut_quote.bid, fut_quote.ask);
            ctx.set_hedging_symbol(&hedge_symbol);
            ctx.market_ts = now;
            ctx
        } else {
            let base_price = match side {
                Side::Buy => fut_quote.bid,
                Side::Sell => fut_quote.ask,
            };
            // 对对冲单定价
            // ask是卖价，用于sell挂单
            // bid是买价，用于buy挂单
            let limit_price = if base_price > 0.0 {
                match side {
                    Side::Buy => base_price * (1.0 - offset),
                    Side::Sell => base_price * (1.0 + offset),
                }
            } else {
                0.0
            };

            if limit_price <= 0.0 {
                warn!(
                    "FrDecision: hedge query limit_price 无效 strategy_id={} price={:.8}",
                    query.strategy_id, limit_price
                );
                return;
            }

            let mut ctx = ArbHedgeCtx::new_maker(
                query.strategy_id,
                query.client_order_id,
                qty,
                side.to_u8(),
                limit_price,
                price_tick,
                false,
                now + self.hedge_timeout_mm_us,
            );
            ctx.hedging_leg = TradingLeg::new(venue, fut_quote.bid, fut_quote.ask);
            ctx.set_hedging_symbol(&hedge_symbol);
            ctx.market_ts = now;
            ctx
        };

        let signal = TradeSignal::create(SignalType::ArbHedge, now, 0.0, ctx.to_bytes());

        if let Err(err) = self.signal_pub.publish(&signal.to_bytes()) {
            warn!(
                "FrDecision: 发送 hedge 信号失败 strategy_id={} err={:?}",
                query.strategy_id, err
            );
            return;
        }

        debug!(
            "FrDecision: 回复 hedge query strategy_id={} symbol={} qty={:.6} seq={}",
            query.strategy_id, hedge_symbol, qty, query.request_seq
        );
    }


    // ========== 信号发布 ==========

    /// 发布交易信号到 pre_trade（支持多信号发送）
    ///
    /// # 参数
    /// - `spot_symbol`: 现货交易对
    /// - `futures_symbol`: 合约交易对
    /// - `spot_venue`: 现货交易所
    /// - `futures_venue`: 合约交易所
    /// - `signal_type`: 信号类型
    ///
    /// # 行为
    /// - ArbOpen: 根据 price_offsets 发送多个信号（每个偏移一个）
    /// - ArbClose / ArbCancel: 发送单个信号
    ///
    /// # 设计对齐
    /// 与 pre_trade/signal_channel.rs 的 publish_backward 对应
    fn emit_signals(
        &mut self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
        signal_type: SignalType,
        side: Option<Side>,
    ) -> Result<()> {
        let now = get_timestamp_us();
        let mkt_channel = MktChannel::instance();

        // 获取盘口数据
        let spot_quote = mkt_channel.get_quote(spot_symbol, spot_venue);
        let futures_quote = mkt_channel.get_quote(futures_symbol, futures_venue);

        // 如果盘口无效，记录警告并返回
        if spot_quote.is_none() || futures_quote.is_none() {
            warn!(
                "FrDecision: quote unavailable spot={} ({:?}) futures={} ({:?})",
                spot_symbol,
                spot_quote.is_some(),
                futures_symbol,
                futures_quote.is_some()
            );
            return Ok(());
        }

        let spot_quote = spot_quote.unwrap();
        let futures_quote = futures_quote.unwrap();

        // 检查盘口数据的有效性：bid（买价）必须小于 ask（卖价）
        if spot_quote.bid >= spot_quote.ask {
            warn!(
                "FrDecision: invalid spot quote bid={:.8} >= ask={:.8} for {}",
                spot_quote.bid, spot_quote.ask, spot_symbol
            );
            return Ok(());
        }
        if futures_quote.bid >= futures_quote.ask {
            warn!(
                "FrDecision: invalid futures quote bid={:.8} >= ask={:.8} for {}",
                futures_quote.bid, futures_quote.ask, futures_symbol
            );
            return Ok(());
        }

        // 根据信号类型决定发送策略
        match signal_type {
            SignalType::ArbOpen | SignalType::ArbClose => {
                let side = match side {
                    Some(sig) => sig,
                    None => {
                        warn!("FrDecision: {:?} signal missing side info", signal_type);
                        return Ok(());
                    }
                };
                // Open / Close 信号：使用 ArbOpenCtx
                // 注意：ArbClose 和 ArbOpen 使用相同的 context 结构

                let count = if matches!(signal_type, SignalType::ArbOpen) {
                    // Open 信号：根据 price_offsets 发送多个信号
                    for offset in &self.price_offsets {
                        let ctx = self.build_open_context(
                            spot_symbol,
                            futures_symbol,
                            spot_venue,
                            futures_venue,
                            &spot_quote,
                            &futures_quote,
                            *offset,
                            now,
                            side,
                        );

                        let context = ctx.to_bytes();
                        let signal = TradeSignal::create(signal_type.clone(), now, 0.0, context);
                        let signal_bytes = signal.to_bytes();
                        self.signal_pub.publish(&signal_bytes)?;

                        debug!(
                            "FrDecision: emitted ArbOpen signal offset={:.4} spot={} futures={}",
                            offset, spot_symbol, futures_symbol
                        );
                    }
                    self.price_offsets.len()
                } else {
                    // Close 信号：发送单个信号，offset = 0.0
                    let ctx = self.build_open_context(
                        spot_symbol,
                        futures_symbol,
                        spot_venue,
                        futures_venue,
                        &spot_quote,
                        &futures_quote,
                        0.0,
                        now,
                        side,
                    );

                    let context = ctx.to_bytes();
                    let signal = TradeSignal::create(signal_type.clone(), now, 0.0, context);
                    let signal_bytes = signal.to_bytes();
                    self.signal_pub.publish(&signal_bytes)?;
                    1
                };

                info!(
                    "FrDecision: emitted {} {:?} signal(s) to '{}' spot={} futures={}",
                    count, signal_type, self.channel_name, spot_symbol, futures_symbol
                );
            }

            SignalType::ArbCancel => {
                // Cancel 信号：使用 ArbCancelCtx
                let ctx = self.build_cancel_context(
                    spot_symbol,
                    futures_symbol,
                    spot_venue,
                    futures_venue,
                    &spot_quote,
                    &futures_quote,
                    now,
                );

                let context = ctx.to_bytes();
                let signal = TradeSignal::create(signal_type.clone(), now, 0.0, context);
                let signal_bytes = signal.to_bytes();
                self.signal_pub.publish(&signal_bytes)?;

                info!(
                    "FrDecision: emitted ArbCancel signal to '{}' spot={} futures={}",
                    self.channel_name, spot_symbol, futures_symbol
                );
            }

            _ => {
                warn!("FrDecision: unsupported signal type: {:?}", signal_type);
            }
        }

        Ok(())
    }

    /// 构造 ArbOpen / ArbClose 信号上下文
    ///
    /// # 资费套利策略设计
    /// - opening_leg: 现货（主动腿，使用 margin 开仓）
    /// - hedging_leg: 合约（对冲腿，UM 永续）
    ///
    /// # 参数
    /// - `spot_symbol`: 现货交易对
    /// - `futures_symbol`: 合约交易对
    /// - `spot_venue`: 现货交易所
    /// - `futures_venue`: 合约交易所
    /// - `spot_quote`: 现货盘口
    /// - `futures_quote`: 合约盘口
    /// - `price_offset`: 价格偏移（用于挂单）
    /// - `now`: 当前时间戳（微秒）
    fn build_open_context(
        &self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
        spot_quote: &Quote,
        futures_quote: &Quote,
        price_offset: f64,
        now: i64,
        side: Side,
    ) -> ArbOpenCtx {
        let mut ctx = ArbOpenCtx::new();
        let mkt_channel = MktChannel::instance();

        // opening_leg: 现货（主动腿，使用 margin 开仓）
        ctx.opening_leg = TradingLeg::new(spot_venue, spot_quote.bid, spot_quote.ask);
        ctx.set_opening_symbol(spot_symbol);

        // hedging_leg: 合约（对冲腿，UM 永续）
        ctx.hedging_leg = TradingLeg::new(futures_venue, futures_quote.bid, futures_quote.ask);
        ctx.set_hedging_symbol(futures_symbol);

        // 交易参数
        ctx.amount = self.order_amount;
        ctx.set_side(side);
        ctx.set_order_type(OrderType::Limit);

        // 价格 = 最优盘口 + 偏移，根据信号方向调整挂单基准
        // ask 卖价用于 sell 挂单
        // bid 买价用于 buy 挂单
        let base_price = match side {
            Side::Buy => spot_quote.bid,
            Side::Sell => spot_quote.ask,
        };
        ctx.price = if base_price > 0.0 {
            match side {
                // 逐渐原理盘口，买价减小，卖价升高
                Side::Buy => base_price * (1.0 - price_offset),
                Side::Sell => base_price * (1.0 + price_offset),
            }
        } else {
            0.0
        };
        ctx.price_tick = self
            .min_qty_table
            .margin_price_tick_by_symbol(spot_symbol)
            .or_else(|| self.min_qty_table.spot_price_tick_by_symbol(spot_symbol))
            .unwrap_or(0.0);

        ctx.exp_time = now + self.open_order_ttl_us;
        ctx.create_ts = now;
        ctx.open_threshold = 0.0; // TODO: 从 SpreadFactor 获取

        // hedge_timeout_us 根据 SpreadFactor 的 mode 决定
        let spread_factor = SpreadFactor::instance();
        let mode = spread_factor.get_mode();
        ctx.hedge_timeout_us = match mode {
            super::common::FactorMode::MT => 0,
            super::common::FactorMode::MM => self.hedge_timeout_mm_us,
        };

        // 资费相关字段
        let rate_fetcher = RateFetcher::instance();

        // funding_ma 从 MktChannel 获取
        ctx.funding_ma = mkt_channel
            .get_funding_rate_mean(futures_symbol, futures_venue)
            .unwrap_or(0.0);

        // predicted_funding_rate 从 RateFetcher 获取（内部根据 symbol 获取 period）
        ctx.predicted_funding_rate = rate_fetcher
            .get_predicted_funding_rate(futures_symbol, futures_venue)
            .map(|(_, v)| v)
            .unwrap_or(0.0);

        // loan_rate 使用 RateFetcher 的预测借贷利率
        ctx.loan_rate = rate_fetcher
            .get_predict_loan_rate(futures_symbol, futures_venue)
            .map(|(_, v)| v)
            .unwrap_or(0.0);

        ctx
    }

    fn side_from_fr_signal(fr_signal: FrSignal) -> Side {
        match fr_signal {
            FrSignal::ForwardOpen => Side::Buy,
            FrSignal::BackwardOpen => Side::Sell,
            FrSignal::ForwardClose => Side::Sell,
            FrSignal::BackwardClose => Side::Buy,
        }
    }

    /// 更新挂单档位
    pub fn update_price_offsets(&mut self, offsets: Vec<f64>) {
        if offsets.is_empty() {
            warn!("FrDecision: 忽略空的 price_offsets 更新请求");
            return;
        }
        self.price_offsets = offsets;
        info!(
            "FrDecision: price_offsets 已更新，总档位 {}",
            self.price_offsets.len()
        );
    }

    /// 更新开仓挂单超时时间（秒）
    pub fn update_open_order_timeout(&mut self, open_secs: u64) {
        if open_secs == 0 {
            warn!("FrDecision: open_secs=0 无效，忽略更新");
            return;
        }
        let ttl = open_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.open_order_ttl_us = ttl as i64;
        info!("FrDecision: open_order_ttl 更新为 {}s", open_secs);
    }

    /// 更新对冲挂单超时时间（秒）
    pub fn update_hedge_timeout(&mut self, hedge_secs: u64) {
        if hedge_secs == 0 {
            warn!("FrDecision: hedge_secs=0 无效，忽略更新");
            return;
        }
        let ttl = hedge_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.hedge_timeout_mm_us = ttl as i64;
        info!("FrDecision: hedge_timeout_mm 更新为 {}s", hedge_secs);
    }

    /// 更新单笔下单数量
    pub fn update_order_amount(&mut self, amount: f32) {
        if amount <= 0.0 {
            warn!("FrDecision: amount <= 0 无效，忽略更新");
            return;
        }
        self.order_amount = amount;
        info!("FrDecision: order_amount 更新为 {:.4}", self.order_amount);
    }

    /// 更新对冲价格偏移（例：0.0003 表示万分之3）
    pub fn update_hedge_price_offset(&mut self, offset: f64) {
        if offset <= 0.0 {
            warn!("FrDecision: hedge offset <= 0 无效，忽略更新");
            return;
        }
        self.hedge_price_offset = offset;
        info!("FrDecision: hedge_price_offset 更新为 {:.6}", offset);
    }

    /// 更新需要检查的 symbol 列表
    ///
    /// # 参数
    /// - `symbols`: symbol 列表
    pub fn update_check_symbols(&mut self, symbols: Vec<String>) {
        let mut check_symbols = self.check_symbols.borrow_mut();
        check_symbols.clear();
        for symbol in symbols {
            check_symbols.insert(symbol.to_uppercase());
        }
        info!(
            "FrDecision: check_symbols 已更新，总数 {}",
            check_symbols.len()
        );
    }

    /// 添加单个 symbol 到检查列表
    pub fn add_check_symbol(&mut self, symbol: &str) {
        let mut check_symbols = self.check_symbols.borrow_mut();
        let symbol_upper = symbol.to_uppercase();
        if check_symbols.insert(symbol_upper.clone()) {
            info!("FrDecision: 添加 symbol 到检查列表: {}", symbol_upper);
        }
    }

    /// 移除单个 symbol 从检查列表
    pub fn remove_check_symbol(&mut self, symbol: &str) {
        let mut check_symbols = self.check_symbols.borrow_mut();
        let symbol_upper = symbol.to_uppercase();
        if check_symbols.remove(&symbol_upper) {
            info!("FrDecision: 从检查列表移除 symbol: {}", symbol_upper);
        }
    }

    /// 构造 ArbCancel 信号上下文
    ///
    /// # 资费套利策略设计
    /// - opening_leg: 现货（主动腿）
    /// - hedging_leg: 合约（对冲腿）
    ///
    /// # 参数
    /// - `spot_symbol`: 现货交易对
    /// - `futures_symbol`: 合约交易对
    /// - `spot_venue`: 现货交易所
    /// - `futures_venue`: 合约交易所
    /// - `spot_quote`: 现货盘口
    /// - `futures_quote`: 合约盘口
    /// - `now`: 当前时间戳（微秒）
    fn build_cancel_context(
        &self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
        spot_quote: &Quote,
        futures_quote: &Quote,
        now: i64,
    ) -> ArbCancelCtx {
        let mut ctx = ArbCancelCtx::new();

        // opening_leg: 现货（主动腿）
        ctx.opening_leg = TradingLeg::new(spot_venue, spot_quote.bid, spot_quote.ask);
        ctx.set_opening_symbol(spot_symbol);

        // hedging_leg: 合约（对冲腿）
        ctx.hedging_leg = TradingLeg::new(futures_venue, futures_quote.bid, futures_quote.ask);
        ctx.set_hedging_symbol(futures_symbol);

        ctx.trigger_ts = now;

        ctx
    }

    // ========== 事件循环 ==========

    /// 启动决策循环任务（使用 tokio::spawn_local）
    ///
    /// 每 10ms 遍历一次 check_symbols，对每个 symbol 执行 make_combined_decision
    /// 资产类型：BinanceMargin（现货）+ BinanceUm（期货）
    pub fn spawn_decision_loop() {
        tokio::task::spawn_local(async move {
            info!("FrDecision 决策循环任务启动");

            let mut tick_interval = interval(Duration::from_millis(10));

            loop {
                tick_interval.tick().await;

                // 获取需要检查的 symbol 列表
                let symbols = FR_DECISION.with(|cell| {
                    let decision_ref = cell.get();
                    if decision_ref.is_none() {
                        return Vec::new();
                    }
                    let decision = decision_ref.unwrap().borrow();
                    let check_symbols = decision.check_symbols.borrow();
                    check_symbols.iter().cloned().collect::<Vec<String>>()
                });

                if symbols.is_empty() {
                    continue;
                }

                // 遍历所有 symbol，执行决策
                for symbol in symbols {
                    let result = FR_DECISION.with(|cell| {
                        let decision_ref = cell.get();
                        if decision_ref.is_none() {
                            return Ok(None);
                        }
                        let mut decision = decision_ref.unwrap().borrow_mut();
                        decision.make_combined_decision(
                            &symbol,                         // spot_symbol (BinanceMargin)
                            &symbol,                         // futures_symbol (BinanceUm)
                            TradingVenue::BinanceMargin,     // spot_venue
                            TradingVenue::BinanceUm,         // futures_venue
                        )
                    });

                    if let Err(err) = result {
                        warn!(
                            "FrDecision: 决策失败 symbol={} err={:?}",
                            symbol, err
                        );
                    }
                }

                // 让出控制权
                tokio::task::yield_now().await;
            }
        });
    }

    /// 启动 backward 订阅监听任务（使用 tokio::spawn_local）
    ///
    /// 持续轮询 backward_sub，处理来自 pre_trade 的查询反馈
    /// 策略：有消息时立即处理，无消息时 yield_now() 让出 CPU
    pub fn spawn_backward_listener() {
        tokio::task::spawn_local(async move {
            info!("FrDecision backward 监听任务启动");

            loop {
                let has_message = FR_DECISION.with(|cell| {
                    let decision_ref = cell.get();
                    if decision_ref.is_none() {
                        return false;
                    }
                    let mut decision = decision_ref.unwrap().borrow_mut();

                    // 尝试接收一条消息
                    match decision.backward_sub.receive_msg() {
                        Ok(Some(data)) => {
                            decision.handle_backward_query(data);
                            true  // 有消息，继续轮询
                        }
                        Ok(None) => {
                            false  // 无消息，让出 CPU
                        }
                        Err(err) => {
                            warn!("FrDecision: backward_sub 接收错误: {}", err);
                            false
                        }
                    }
                });

                // 如果没有消息，让出控制权（但可以快速恢复）
                if !has_message {
                    tokio::task::yield_now().await;
                }
            }
        });
    }

}
