//! Funding Rate 套利决策模块
//!
//! 纯决策逻辑，不维护状态。接收状态作为参数，返回决策结果并发布信号。

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::common::iceoryx_publisher::{SignalPublisher, SIGNAL_PAYLOAD};
use crate::common::iceoryx_subscriber::GenericSignalSubscriber;
use crate::common::ipc_service_name::build_service_name;
use crate::common::min_qty_table::MinQtyTable;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{SignalBytes, TradingLeg, TradingVenue};
use crate::signal::hedge_signal::{ArbHedgeCtx, ArbHedgeSignalQueryMsg};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};

use super::common::{Quote, ThresholdKey};
use super::funding_rate_factor::FundingRateFactor;
use super::mkt_channel::MktChannel;
use super::rate_fetcher::RateFetcher;
use super::spread_factor::SpreadFactor;
use super::symbol_list::SymbolList;

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

    /// 单笔下单名义金额（单位：USDT）
    order_amount: f32,

    /// 开仓挂单最长挂单时间（微秒）
    open_order_ttl_us: i64,

    /// 对冲挂单（MM 模式）最长挂单时间（微秒）
    hedge_timeout_mm_us: i64,

    /// 对冲限价偏移（例如 0.0003 表示万分之 3）
    hedge_price_offset: f64,

    /// 需要检查的交易对白名单（完整的 4 元组）
    /// key: (spot_venue, spot_symbol, futures_venue, futures_symbol)
    /// 使用 Rc<RefCell<>> 以便在 spawn_local 任务中共享
    check_symbols: Rc<RefCell<HashSet<ThresholdKey>>>,

    /// 信号冷却时间（微秒），默认 5 秒
    /// 每种信号类型（ArbOpen/ArbClose/ArbCancel）有独立的冷却时间
    /// 防止因多个事件同时触发（现货/期货盘口同时更新）导致重复发送相同类型的信号
    signal_cooldown_us: i64,

    /// 最后触发 ArbOpen 的时间戳（微秒）
    /// key: (spot_venue, spot_symbol, futures_venue, futures_symbol)
    last_open_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,

    /// 最后触发 ArbClose 的时间戳（微秒）
    /// key: (spot_venue, spot_symbol, futures_venue, futures_symbol)
    last_close_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,

    /// 最后触发 ArbCancel 的时间戳（微秒）
    /// key: (spot_venue, spot_symbol, futures_venue, futures_symbol)
    last_cancel_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
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
    /// - spawn_backward_listener: 事件驱动的 backward 监听
    ///
    /// # 事件驱动架构
    /// 决策逻辑采用事件驱动模式，不使用定时轮询。
    /// 调用方应在市场数据更新时主动调用 `make_combined_decision()`。
    pub async fn init_singleton() -> Result<()> {
        let result: Result<()> = FR_DECISION.with(|cell| {
            if cell.get().is_some() {
                return Ok(());
            }
            let decision = Self::new_sync()?;
            cell.set(RefCell::new(decision))
                .map_err(|_| anyhow::anyhow!("Failed to initialize FrDecision singleton"))?;
            info!("FrDecision singleton initialized");
            Ok(())
        });
        result?;

        // 异步加载 min_qty_table
        Self::refresh_min_qty_async().await;

        // 启动 backward 监听任务（处理来自 pre_trade 的查询）
        Self::spawn_backward_listener();
        info!("FrDecision backward listener started");

        Ok(())
    }

    /// 创建新实例（私有，同步版本）
    fn new_sync() -> Result<Self> {
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

        // min_qty_table 将在 init_singleton 中异步加载
        let min_qty_table = MinQtyTable::new();

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
            signal_cooldown_us: 5_000_000, // 默认 5 秒
            last_open_ts: Rc::new(RefCell::new(HashMap::new())),
            last_close_ts: Rc::new(RefCell::new(HashMap::new())),
            last_cancel_ts: Rc::new(RefCell::new(HashMap::new())),
        })
    }

    /// 异步刷新 min_qty_table
    async fn refresh_min_qty_async() {
        let mut table = MinQtyTable::new();
        match table.refresh_binance().await {
            Ok(_) => {
                Self::with_mut(|decision| {
                    decision.min_qty_table = table;
                });
                info!("FrDecision: min_qty_table loaded successfully");
            }
            Err(err) => {
                warn!(
                    "FrDecision: failed to refresh Binance exchange filters, price_tick may be zero: {err:#}"
                );
            }
        }
    }

    /// 创建订阅器（helper）
    fn create_subscriber(
        node: &Node<ipc::Service>,
        channel_name: &str,
    ) -> Result<GenericSignalSubscriber> {
        let service_name = build_service_name(&format!("signal_pubs/{}", channel_name));
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .open_or_create()?;

        let subscriber = service.subscriber_builder().create()?;
        Ok(GenericSignalSubscriber::Size4K(subscriber))
    }

    // ========== 外部主动触发接口 ==========

    /// 联合信号决策：同时检查资费和价差因子
    ///
    /// # 决策流程
    /// 0. 检查 symbol 是否在 check_symbols 白名单中
    /// 1. 优先检查 cancel 信号（只和价差有关）
    ///    - 如果满足 cancel 条件，检查 cancel 信号冷却
    ///    - 如果在冷却期内，跳过 cancel 信号
    ///    - 否则发送 cancel 信号并更新 cancel 冷却时间戳
    /// 2. 获取资费信号
    /// 3. 如果资费没有信号，返回 None
    /// 4. 如果资费有信号，验证对应的价差 satisfy
    /// 5. 只有资费和价差同时满足时才确定最终信号（Open/Close）
    /// 6. 检查对应信号类型的冷却
    /// 7. 发送信号并更新对应类型的冷却时间戳
    ///
    /// # 冷却机制
    /// - ArbOpen、ArbClose、ArbCancel 三种信号有**独立的**冷却时间
    /// - 每种信号发送后 5 秒内不能再发送同类型信号
    /// - 但不同类型信号之间互不影响
    ///
    /// # 参数
    /// - `spot_symbol`: 现货交易对
    /// - `futures_symbol`: 合约交易对
    /// - `spot_venue`: 现货交易所
    /// - `futures_venue`: 合约交易所
    ///
    /// # 返回
    /// 如果需要发送信号，返回 Ok(Some(signal_type))；否则返回 Ok(None)
    ///
    /// # 事件驱动架构
    /// 该方法应在市场数据更新时被主动调用（事件驱动），而非定时轮询。
    /// 触发时机：MktChannel 收到盘口更新、RateFetcher 更新资费率等
    pub fn make_combined_decision(
        &mut self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
    ) -> Result<Option<SignalType>> {
        // 步骤0: 检查交易对 4 元组是否在白名单中
        // 早期返回优化：不在白名单中的交易对直接跳过，提升效率
        let key = (
            spot_venue,
            spot_symbol.to_uppercase(),
            futures_venue,
            futures_symbol.to_uppercase(),
        );

        let check_symbols = self.check_symbols.borrow();
        if !check_symbols.contains(&key) {
            return Ok(None);
        }
        drop(check_symbols); // 释放借用

        let spread_factor = SpreadFactor::instance();
        let now = get_timestamp_us();

        // 步骤1: 优先检查 cancel 信号（只和价差有关，不需要资费）
        if spread_factor.satisfy_forward_cancel(
            spot_venue,
            spot_symbol,
            futures_venue,
            futures_symbol,
        ) || spread_factor.satisfy_backward_cancel(
            spot_venue,
            spot_symbol,
            futures_venue,
            futures_symbol,
        ) {
            // 检查 cancel 信号冷却
            if self.check_signal_cooldown(
                spot_symbol,
                futures_symbol,
                spot_venue,
                futures_venue,
                now,
                &SignalType::ArbCancel,
            ) {
                // 在冷却期内，不返回，继续检查 open/close 信号
            } else {
                // 发送 cancel 信号
                self.emit_signals(
                    spot_symbol,
                    futures_symbol,
                    spot_venue,
                    futures_venue,
                    SignalType::ArbCancel,
                    None,
                )?;
                // 更新 cancel 冷却时间戳
                self.update_last_signal_ts(
                    spot_symbol,
                    futures_symbol,
                    spot_venue,
                    futures_venue,
                    now,
                    &SignalType::ArbCancel,
                );
                return Ok(Some(SignalType::ArbCancel));
            }
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
                let symbol_list = SymbolList::instance();
                if spread_factor.satisfy_forward_open(
                    spot_venue,
                    spot_symbol,
                    futures_venue,
                    futures_symbol,
                ) && symbol_list.is_in_fwd_trade_list(futures_symbol, futures_venue)
                {
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
                let symbol_list = SymbolList::instance();
                if spread_factor.satisfy_backward_open(
                    spot_venue,
                    spot_symbol,
                    futures_venue,
                    futures_symbol,
                ) && symbol_list.is_in_bwd_trade_list(futures_symbol, futures_venue)
                {
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

        // 步骤6: 检查对应信号类型的冷却
        if self.check_signal_cooldown(
            spot_symbol,
            futures_symbol,
            spot_venue,
            futures_venue,
            now,
            &final_signal,
        ) {
            return Ok(None);
        }

        // 步骤7: 发送信号
        self.emit_signals(
            spot_symbol,
            futures_symbol,
            spot_venue,
            futures_venue,
            final_signal.clone(),
            Some(signal_side),
        )?;

        // 步骤8: 更新冷却时间戳
        self.update_last_signal_ts(
            spot_symbol,
            futures_symbol,
            spot_venue,
            futures_venue,
            now,
            &final_signal,
        );

        Ok(Some(final_signal))
    }

    /// 获取资费因子信号
    ///
    /// 通过查询 FundingRateFactor 的 satisfy 方法判断资费信号
    ///
    /// # 优先级规则
    /// 1. 如果同时满足 forward_close 和 backward_open，选择 backward_open
    /// 2. 如果同时满足 backward_close 和 forward_open，选择 forward_open
    /// 3. 否则按优先级：close > open
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

        // 检查所有条件
        let forward_open = fr_factor.satisfy_forward_open(futures_symbol, period);
        let forward_close = fr_factor.satisfy_forward_close(futures_symbol, period, futures_venue);
        let backward_open = fr_factor.satisfy_backward_open(futures_symbol, period);
        let backward_close =
            fr_factor.satisfy_backward_close(futures_symbol, period, futures_venue);

        // 优先级规则1: forward_close 和 backward_open 冲突时，选择 backward_open
        if forward_close && backward_open {
            return Ok(Some(FrSignal::BackwardOpen));
        }

        // 优先级规则2: backward_close 和 forward_open 冲突时，选择 forward_open
        if backward_close && forward_open {
            return Ok(Some(FrSignal::ForwardOpen));
        }

        // 默认优先级: close > open
        if forward_close {
            return Ok(Some(FrSignal::ForwardClose));
        }
        if backward_close {
            return Ok(Some(FrSignal::BackwardClose));
        }

        // 开仓信号
        if forward_open {
            return Ok(Some(FrSignal::ForwardOpen));
        }
        if backward_open {
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
            ctx.price_offset = 0.0; // aggressive taker order, no offset
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
            ctx.price_offset = offset; // maker order with price offset
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

                // Open/Close 信号：都使用多档位发送
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

                    let signal = TradeSignal::create(signal_type.clone(), now, 0.0, ctx.to_bytes());
                    self.signal_pub.publish(&signal.to_bytes())?;
                }

                info!(
                    "FrDecision: emitted {} {:?} signal(s) to '{}' spot={} futures={}",
                    self.price_offsets.len(), signal_type, self.channel_name, spot_symbol, futures_symbol
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

    /// 构造 ArbOpen/ArbClose 信号上下文
    ///
    /// opening_leg: 现货（主动腿），hedging_leg: 合约（对冲腿）
    /// 定价逻辑：Buy用bid降价，Sell用ask加价
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

        // opening_leg: 现货（主动腿）
        ctx.opening_leg = TradingLeg::new(spot_venue, spot_quote.bid, spot_quote.ask);
        ctx.set_opening_symbol(spot_symbol);

        // hedging_leg: 合约（对冲腿）
        ctx.hedging_leg = TradingLeg::new(futures_venue, futures_quote.bid, futures_quote.ask);
        ctx.set_hedging_symbol(futures_symbol);

        // 交易参数
        ctx.set_side(side);
        ctx.set_order_type(OrderType::Limit);

        // 定价：Buy基于bid降价，Sell基于ask加价
        let base_price = match side {
            Side::Buy => spot_quote.bid,
            Side::Sell => spot_quote.ask,
        };
        ctx.price = if base_price > 0.0 {
            match side {
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
        let qty = self.convert_order_amount_to_qty(spot_symbol, ctx.price);
        ctx.amount = qty as f32;

        ctx.exp_time = now + self.open_order_ttl_us;
        ctx.create_ts = now;
        ctx.price_offset = price_offset;

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

    /// 将 USDT 名义金额转换为下单数量，并对齐最小下单量/步长
    fn convert_order_amount_to_qty(&self, symbol: &str, price: f64) -> f64 {
        if !(self.order_amount > 0.0) {
            warn!(
                "FrDecision: order_amount <= 0 when building signal for {}, skip",
                symbol
            );
            return 0.0;
        }
        if price <= 0.0 {
            warn!(
                "FrDecision: price for {} <= 0 when converting order amount, fallback to 0",
                symbol
            );
            return 0.0;
        }

        let min_qty = self
            .min_qty_table
            .margin_min_qty_by_symbol(symbol)
            .or_else(|| self.min_qty_table.spot_min_qty_by_symbol(symbol));
        let step = self
            .min_qty_table
            .margin_step_by_symbol(symbol)
            .or_else(|| self.min_qty_table.spot_step_by_symbol(symbol));

        convert_usdt_amount_to_qty(self.order_amount as f64, price, min_qty, step)
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

    /// 更新需要检查的交易对白名单（批量）
    ///
    /// # 参数
    /// - `pairs`: 交易对列表，每个元素是 (spot_venue, spot_symbol, futures_venue, futures_symbol)
    pub fn update_check_symbols(&mut self, pairs: Vec<ThresholdKey>) {
        let mut check_symbols = self.check_symbols.borrow_mut();
        check_symbols.clear();
        for (spot_venue, spot_symbol, futures_venue, futures_symbol) in pairs {
            check_symbols.insert((
                spot_venue,
                spot_symbol.to_uppercase(),
                futures_venue,
                futures_symbol.to_uppercase(),
            ));
        }
        info!(
            "FrDecision: check_symbols 已更新，总数 {}",
            check_symbols.len()
        );
    }

    /// 添加单个交易对到检查列表
    ///
    /// # 参数
    /// - `spot_symbol`: 现货交易对
    /// - `futures_symbol`: 合约交易对
    /// - `spot_venue`: 现货交易所
    /// - `futures_venue`: 合约交易所
    pub fn add_check_symbol(
        &mut self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
    ) {
        let mut check_symbols = self.check_symbols.borrow_mut();
        let key = (
            spot_venue,
            spot_symbol.to_uppercase(),
            futures_venue,
            futures_symbol.to_uppercase(),
        );
        if check_symbols.insert(key.clone()) {
            info!(
                "FrDecision: 添加交易对到检查列表: {:?}/{} <-> {:?}/{}",
                spot_venue, spot_symbol, futures_venue, futures_symbol
            );
        }
    }

    /// 移除单个交易对从检查列表
    ///
    /// # 参数
    /// - `spot_symbol`: 现货交易对
    /// - `futures_symbol`: 合约交易对
    /// - `spot_venue`: 现货交易所
    /// - `futures_venue`: 合约交易所
    pub fn remove_check_symbol(
        &mut self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
    ) {
        let mut check_symbols = self.check_symbols.borrow_mut();
        let key = (
            spot_venue,
            spot_symbol.to_uppercase(),
            futures_venue,
            futures_symbol.to_uppercase(),
        );
        if check_symbols.remove(&key) {
            info!(
                "FrDecision: 从检查列表移除交易对: {:?}/{} <-> {:?}/{}",
                spot_venue, spot_symbol, futures_venue, futures_symbol
            );
        }
    }

    /// 更新信号冷却时间（秒）
    ///
    /// # 参数
    /// - `cooldown_secs`: 冷却时间（秒），默认 5 秒
    pub fn update_signal_cooldown(&mut self, cooldown_secs: u64) {
        if cooldown_secs == 0 {
            warn!("FrDecision: cooldown_secs=0 无效，忽略更新");
            return;
        }
        let cooldown_us = cooldown_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.signal_cooldown_us = cooldown_us as i64;
        info!(
            "FrDecision: signal_cooldown 更新为 {}s ({}us)",
            cooldown_secs, self.signal_cooldown_us
        );
    }

    /// 检查交易对是否在冷却期内
    ///
    /// # 参数
    /// - `spot_symbol`: 现货交易对
    /// - `futures_symbol`: 合约交易对
    /// - `spot_venue`: 现货交易所
    /// - `futures_venue`: 合约交易所
    /// - `now`: 当前时间戳（微秒）
    /// - `signal_type`: 信号类型（ArbOpen/ArbClose/ArbCancel）
    ///
    /// # 返回
    /// - true: 该类型信号在冷却期内，不应发出
    /// - false: 该类型信号不在冷却期内，可以发出
    fn check_signal_cooldown(
        &self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
        now: i64,
        signal_type: &SignalType,
    ) -> bool {
        let key = (
            spot_venue,
            spot_symbol.to_uppercase(),
            futures_venue,
            futures_symbol.to_uppercase(),
        );

        // 根据信号类型选择对应的 last_ts HashMap
        let last_ts_map = match signal_type {
            SignalType::ArbOpen => self.last_open_ts.borrow(),
            SignalType::ArbClose => self.last_close_ts.borrow(),
            SignalType::ArbCancel => self.last_cancel_ts.borrow(),
            _ => {
                warn!("FrDecision: 不支持的信号类型 {:?}", signal_type);
                return false;
            }
        };

        if let Some(&last_ts) = last_ts_map.get(&key) {
            let elapsed = now - last_ts;
            if elapsed < self.signal_cooldown_us {
                return true;
            }
        }
        false
    }

    /// 更新交易对的最后信号时间戳
    ///
    /// # 参数
    /// - `spot_symbol`: 现货交易对
    /// - `futures_symbol`: 合约交易对
    /// - `spot_venue`: 现货交易所
    /// - `futures_venue`: 合约交易所
    /// - `now`: 当前时间戳（微秒）
    /// - `signal_type`: 信号类型（ArbOpen/ArbClose/ArbCancel）
    fn update_last_signal_ts(
        &self,
        spot_symbol: &str,
        futures_symbol: &str,
        spot_venue: TradingVenue,
        futures_venue: TradingVenue,
        now: i64,
        signal_type: &SignalType,
    ) {
        let key = (
            spot_venue,
            spot_symbol.to_uppercase(),
            futures_venue,
            futures_symbol.to_uppercase(),
        );

        // 根据信号类型选择对应的 last_ts HashMap
        match signal_type {
            SignalType::ArbOpen => {
                let mut last_ts = self.last_open_ts.borrow_mut();
                last_ts.insert(key, now);
            }
            SignalType::ArbClose => {
                let mut last_ts = self.last_close_ts.borrow_mut();
                last_ts.insert(key, now);
            }
            SignalType::ArbCancel => {
                let mut last_ts = self.last_cancel_ts.borrow_mut();
                last_ts.insert(key, now);
            }
            _ => {
                warn!("FrDecision: 不支持的信号类型 {:?}", signal_type);
            }
        }
    }

    /// 构造 ArbCancel 信号上下文
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

        ctx.opening_leg = TradingLeg::new(spot_venue, spot_quote.bid, spot_quote.ask);
        ctx.set_opening_symbol(spot_symbol);

        ctx.hedging_leg = TradingLeg::new(futures_venue, futures_quote.bid, futures_quote.ask);
        ctx.set_hedging_symbol(futures_symbol);

        ctx.trigger_ts = now;

        ctx
    }

    // ========== 事件循环 ==========

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
                            true // 有消息，继续轮询
                        }
                        Ok(None) => {
                            false // 无消息，让出 CPU
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

fn convert_usdt_amount_to_qty(
    order_amount_u: f64,
    price: f64,
    min_qty: Option<f64>,
    step: Option<f64>,
) -> f64 {
    if order_amount_u <= 0.0 || price <= 0.0 {
        return 0.0;
    }
    let mut qty = order_amount_u / price;
    if let Some(step) = step {
        qty = align_step_ceil(qty, step);
    }

    if let Some(min_qty) = min_qty {
        if min_qty > 0.0 && qty < min_qty {
            qty = if let Some(step) = step {
                align_step_ceil(min_qty, step)
            } else {
                min_qty
            };
        }
    }

    qty
}

fn align_step_ceil(value: f64, step: f64) -> f64 {
    if value <= 0.0 || step <= 0.0 {
        return value;
    }
    let units = (value / step).ceil();
    units * step
}

#[cfg(test)]
mod tests {
    use super::{align_step_ceil, convert_usdt_amount_to_qty};

    #[test]
    fn test_convert_usdt_amount_basic() {
        let qty = convert_usdt_amount_to_qty(100.0, 20.0, None, None);
        assert!((qty - 5.0).abs() < 1e-12);
    }

    #[test]
    fn test_convert_usdt_amount_aligns_step() {
        let qty = convert_usdt_amount_to_qty(100.0, 3.0, None, Some(0.05));
        assert!((qty - 33.35).abs() < 1e-12);
    }

    #[test]
    fn test_convert_usdt_amount_hits_min_qty() {
        let qty = convert_usdt_amount_to_qty(10.0, 100.0, Some(0.5), Some(0.1));
        assert!((qty - 0.5).abs() < 1e-12);
    }

    #[test]
    fn test_align_step_handles_invalid() {
        assert_eq!(align_step_ceil(0.0, 0.1), 0.0);
        assert!((align_step_ceil(1.01, 0.1) - 1.1).abs() < 1e-12);
    }
}
