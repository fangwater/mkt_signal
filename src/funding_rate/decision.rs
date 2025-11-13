//! Funding Rate 套利决策模块
//!
//! 纯决策逻辑，不维护状态。接收状态作为参数，返回决策结果并发布信号。

use anyhow::Result;
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::cell::{OnceCell, RefCell};
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;

use crate::common::iceoryx_publisher::{SignalPublisher, SIGNAL_PAYLOAD};
use crate::common::iceoryx_subscriber::GenericSignalSubscriber;
use crate::common::time_util::get_timestamp_us;
use crate::signal::common::{SignalBytes, TradingLeg, TradingVenue};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::pre_trade::order_manager::{OrderType, Side};

use super::spread_factor::SpreadFactor;
use super::funding_rate_factor::FundingRateFactor;
use super::rate_fetcher::RateFetcher;
use super::mkt_channel::MktChannel;
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
    pub fn init_singleton() -> Result<()> {
        FR_DECISION.with(|cell| {
            if cell.get().is_some() {
                return Ok(());
            }
            let decision = Self::new()?;
            cell.set(RefCell::new(decision))
                .map_err(|_| anyhow::anyhow!("Failed to initialize FrDecision singleton"))?;
            info!("FrDecision singleton initialized");
            Ok(())
        })
    }

    /// 创建新实例（私有）
    fn new() -> Result<Self> {
        let node_name = NodeName::new("fr_decision")?;
        let node = NodeBuilder::new()
            .name(&node_name)
            .create::<ipc::Service>()?;

        // 1. 创建信号发布器（发往 pre_trade）
        let signal_pub = SignalPublisher::new(DEFAULT_SIGNAL_CHANNEL)?;
        info!("FrDecision: signal publisher created on '{}'", DEFAULT_SIGNAL_CHANNEL);

        // 2. 订阅反向频道（来自 pre_trade 的查询反馈）
        let backward_sub = Self::create_subscriber(&node, DEFAULT_BACKWARD_CHANNEL)?;
        info!("FrDecision: backward subscriber created on '{}'", DEFAULT_BACKWARD_CHANNEL);

        // 默认挂单偏移：万2 到 千1，共5档
        let price_offsets = vec![0.0002, 0.0004, 0.0006, 0.0008, 0.001];

        Ok(Self {
            signal_pub,
            backward_sub,
            channel_name: DEFAULT_SIGNAL_CHANNEL.to_string(),
            _node: node,
            price_offsets,
        })
    }

    /// 创建订阅器（helper）
    fn create_subscriber(node: &Node<ipc::Service>, channel_name: &str) -> Result<GenericSignalSubscriber> {
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
        if spread_factor.satisfy_forward_cancel(spot_venue, spot_symbol, futures_venue, futures_symbol) {
            self.emit_signals(spot_symbol, futures_symbol, spot_venue, futures_venue, SignalType::ArbCancel)?;
            return Ok(Some(SignalType::ArbCancel));
        }
        if spread_factor.satisfy_backward_cancel(spot_venue, spot_symbol, futures_venue, futures_symbol) {
            self.emit_signals(spot_symbol, futures_symbol, spot_venue, futures_venue, SignalType::ArbCancel)?;
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
                if spread_factor.satisfy_forward_open(spot_venue, spot_symbol, futures_venue, futures_symbol) {
                    Some(SignalType::ArbOpen)
                } else {
                    None
                }
            }
            FrSignal::ForwardClose => {
                if spread_factor.satisfy_forward_close(spot_venue, spot_symbol, futures_venue, futures_symbol) {
                    Some(SignalType::ArbClose)
                } else {
                    None
                }
            }
            FrSignal::BackwardOpen => {
                if spread_factor.satisfy_backward_open(spot_venue, spot_symbol, futures_venue, futures_symbol) {
                    Some(SignalType::ArbOpen)
                } else {
                    None
                }
            }
            FrSignal::BackwardClose => {
                if spread_factor.satisfy_backward_close(spot_venue, spot_symbol, futures_venue, futures_symbol) {
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

        // 步骤6: 发送信号
        self.emit_signals(spot_symbol, futures_symbol, spot_venue, futures_venue, final_signal.clone())?;

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
    ///
    /// # TODO
    /// 需要根据实际业务逻辑实现
    fn handle_backward_query(&mut self, data: Bytes) {
        debug!("Received backward query, size={}", data.len());
        // TODO: 解析查询请求，返回当前决策状态
        // 例如：查询某个 symbol 的最新信号、持仓状态等
        let _ = data; // 避免未使用警告
    }

    // ========== 决策逻辑 ==========
    // 决策逻辑已委托给 SymbolState::evaluate_signal，不需要在这里重复实现

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
                spot_symbol, spot_quote.is_some(), futures_symbol, futures_quote.is_some()
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
    /// - opening_leg: 期货（主动腿，关注资费）
    /// - hedging_leg: 现货（对冲腿）
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
    ) -> ArbOpenCtx {
        let mut ctx = ArbOpenCtx::new();
        let mkt_channel = MktChannel::instance();

        // opening_leg: 期货（主动腿）
        ctx.opening_leg = TradingLeg::new(futures_venue, futures_quote.bid, futures_quote.ask);
        ctx.set_opening_symbol(futures_symbol);

        // hedging_leg: 现货（对冲腿）
        ctx.hedging_leg = TradingLeg::new(spot_venue, spot_quote.bid, spot_quote.ask);
        ctx.set_hedging_symbol(spot_symbol);

        // 交易参数
        ctx.amount = 0.0; // TODO: 从配置或参数获取
        ctx.set_side(Side::Buy); // TODO: 根据正套/反套确定方向
        ctx.set_order_type(OrderType::Limit);

        // 价格 = 最优盘口 + 偏移
        // TODO: 根据 side 确定是用 bid 还是 ask
        ctx.price = futures_quote.bid * (1.0 + price_offset);
        ctx.price_tick = 0.01; // TODO: 从配置获取

        ctx.exp_time = now + 120_000_000; // 120 秒过期
        ctx.create_ts = now;
        ctx.open_threshold = 0.0; // TODO: 从 SpreadFactor 获取

        // hedge_timeout_us 根据 SpreadFactor 的 mode 决定
        let spread_factor = SpreadFactor::instance();
        let mode = spread_factor.get_mode();
        ctx.hedge_timeout_us = match mode {
            super::common::FactorMode::MT => 0,              // MT 模式：立即对冲
            super::common::FactorMode::MM => 30_000_000,     // MM 模式：30 秒对冲超时
        };

        // 资费相关字段
        let rate_fetcher = RateFetcher::instance();

        // funding_ma 从 MktChannel 获取
        ctx.funding_ma = mkt_channel
            .get_funding_rate_mean(futures_symbol, futures_venue)
            .unwrap_or(0.0);

        // predicted_funding_rate 从 RateFetcher 获取（内部根据 symbol 获取 period）
        ctx.predicted_funding_rate = rate_fetcher
            .get_binance_predicted_funding_rate(futures_symbol)
            .unwrap_or(0.0);

        ctx.loan_rate = 0.0; // 现货套利不涉及借贷

        ctx
    }

    /// 构造 ArbCancel 信号上下文
    ///
    /// # 资费套利策略设计
    /// - opening_leg: 期货（主动腿）
    /// - hedging_leg: 现货（对冲腿）
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

        // opening_leg: 期货（主动腿）
        ctx.opening_leg = TradingLeg::new(futures_venue, futures_quote.bid, futures_quote.ask);
        ctx.set_opening_symbol(futures_symbol);

        // hedging_leg: 现货（对冲腿）
        ctx.hedging_leg = TradingLeg::new(spot_venue, spot_quote.bid, spot_quote.ask);
        ctx.set_hedging_symbol(spot_symbol);

        ctx.trigger_ts = now;

        ctx
    }

    // ========== 事件循环 ==========

    /// 主事件循环（tokio select）
    ///
    /// # 参数
    /// - `token`: 取消令牌，用于优雅退出
    pub async fn run_event_loop(&mut self, token: CancellationToken) -> Result<()> {
        info!("FrDecision event loop starting");

        let mut tick_interval = interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    info!("FrDecision event loop cancelled");
                    break;
                }

                _ = tick_interval.tick() => {
                    // 每个 tick 轮询订阅器
                    self.poll_subscriptions();
                }
            }
        }

        info!("FrDecision event loop exited");
        Ok(())
    }

    /// 轮询订阅器（只处理 backward channel）
    fn poll_subscriptions(&mut self) {
        // 轮询反向频道（来自 pre_trade 的查询反馈）
        while let Ok(Some(data)) = self.backward_sub.receive_msg() {
            self.handle_backward_query(data);
        }
    }

}
