use crate::common::iceoryx_publisher::{SignalPublisher, SIGNAL_PAYLOAD};
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::Side;
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{SignalBytes, TradingVenue};
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::hedge_arb_strategy::HedgeArbStrategy;
use crate::strategy::{Strategy, StrategyManager};
use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::cell::{OnceCell, RefCell};
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

thread_local! {
    static SIGNAL_CHANNEL: OnceCell<SignalChannel> = OnceCell::new();
}

/// 默认信号频道名称
pub const DEFAULT_SIGNAL_CHANNEL: &str = "pre_trade";

/// 默认反向信号频道名称
pub const DEFAULT_BACKWARD_CHANNEL: &str = "signal_query";

/// 信号频道 - 负责信号进程和 pre-trade 之间的双向通讯
///
/// 采用线程本地单例模式，通过 `SignalChannel::with()` 访问
///
/// # 使用示例
/// ```ignore
/// use crate::pre_trade::signal_channel::SignalChannel;
///
/// // 初始化（通常在 main 或入口处调用一次）
/// SignalChannel::initialize("my_channel", Some("backward_channel"))?;
///
/// // 在任何地方访问
/// SignalChannel::with(|ch| {
///     if let Some(rx) = ch.take_receiver() {
///         // 使用 receiver
///     }
/// });
///
/// // 发送反向信号
/// SignalChannel::with(|ch| ch.publish_backward(&data));
/// ```
pub struct SignalChannel {
    /// 信号接收器（可以 take 走一次）
    signal_rx: RefCell<Option<UnboundedReceiver<TradeSignal>>>,
    /// 信号发送器（用于内部克隆）
    signal_tx: UnboundedSender<TradeSignal>,
    /// 反向发布器：用于向上游信号进程发送查询或反馈
    backward_pub: Option<SignalPublisher>,
    /// 频道名称（用于日志）
    channel_name: String,
}

impl SignalChannel {
    /// 在当前线程的 SignalChannel 单例上执行操作
    ///
    /// 第一次调用时会自动初始化默认频道，后续调用直接使用已初始化的实例
    ///
    /// # 使用示例
    /// ```ignore
    /// // 获取接收器（只能 take 一次）
    /// let rx = SignalChannel::with(|ch| ch.take_receiver());
    ///
    /// // 发送反向信号
    /// SignalChannel::with(|ch| ch.publish_backward(&data));
    /// ```
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&SignalChannel) -> R,
    {
        SIGNAL_CHANNEL.with(|cell| {
            let channel = cell.get_or_init(|| {
                info!("Initializing thread-local SignalChannel singleton with default config");
                SignalChannel::new(DEFAULT_SIGNAL_CHANNEL, Some(DEFAULT_BACKWARD_CHANNEL))
                    .expect("Failed to initialize default SignalChannel")
            });
            f(channel)
        })
    }

    /// 显式初始化信号频道（可选）
    ///
    /// 如果在首次调用 `with()` 之前调用此方法，可以自定义频道名称
    ///
    /// # 参数
    /// * `channel_name` - 要订阅的信号频道名称
    /// * `backward_channel` - 反向通道名称（可选）
    ///
    /// # 错误
    /// - 如果已经初始化，返回错误
    /// - 如果 IceOryx 初始化失败，返回错误
    pub fn initialize(channel_name: &str, backward_channel: Option<&str>) -> Result<()> {
        SIGNAL_CHANNEL.with(|cell| {
            if cell.get().is_some() {
                return Err(anyhow::anyhow!("SignalChannel already initialized"));
            }
            cell.set(SignalChannel::new(channel_name, backward_channel)?)
                .map_err(|_| anyhow::anyhow!("Failed to set SignalChannel (race condition)"))
        })
    }

    /// 创建信号频道并自动启动监听器
    ///
    /// # 参数
    /// * `channel_name` - 要订阅的信号频道名称
    /// * `backward_channel` - 反向通道名称（可选）
    fn new(channel_name: &str, backward_channel: Option<&str>) -> Result<Self> {
        // 创建消息队列
        let (signal_tx, signal_rx) = mpsc::unbounded_channel();

        // 创建反向发布器
        let backward_pub = if let Some(backward_ch) = backward_channel {
            match SignalPublisher::new(backward_ch) {
                Ok(p) => {
                    info!(
                        "SignalChannel: backward publisher created on '{}'",
                        backward_ch
                    );
                    Some(p)
                }
                Err(err) => {
                    warn!(
                        "SignalChannel: failed to create backward publisher on '{}': {err:#}",
                        backward_ch
                    );
                    None
                }
            }
        } else {
            None
        };

        // 启动监听任务（在这里 clone tx，避免 move 问题）
        let channel_name_owned = channel_name.to_string();
        tokio::task::spawn_local(async move {
            if let Err(err) = Self::run_listener(&channel_name_owned).await {
                warn!(
                    "signal listener exited (channel={}): {err:?}",
                    channel_name_owned
                );
            }
        });

        Ok(Self {
            signal_rx: RefCell::new(Some(signal_rx)),
            signal_tx,
            backward_pub,
            channel_name: channel_name.to_string(),
        })
    }

    /// 获取信号接收器，只能调用一次
    ///
    /// # 返回
    /// 如果 receiver 已经被 take 走，返回 None
    pub fn take_receiver(&self) -> Option<UnboundedReceiver<TradeSignal>> {
        self.signal_rx.borrow_mut().take()
    }

    /// 向上游发送反馈数据
    ///
    /// # 参数
    /// * `data` - 要发送的数据
    ///
    /// # 返回
    /// 如果没有配置反向发布器，返回 Ok(false)；成功发送返回 Ok(true)
    pub fn publish_backward(&self, data: &[u8]) -> Result<bool> {
        if let Some(publisher) = &self.backward_pub {
            publisher.publish(data)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// 检查反向发布器是否可用
    pub fn is_backward_publisher_available(&self) -> bool {
        self.backward_pub.is_some()
    }

    /// 获取频道名称
    pub fn channel_name(&self) -> &str {
        &self.channel_name
    }

    /// 监听器的核心逻辑
    async fn run_listener(channel_name: &str) -> Result<()> {
        let node_name = Self::signal_node_name(channel_name);
        let service_path = format!("signal_pubs/{}", channel_name);

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_path)?)
            .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .open_or_create()?;

        let subscriber: Subscriber<ipc::Service, [u8; SIGNAL_PAYLOAD], ()> =
            service.subscriber_builder().create()?;

        info!(
            "signal subscribed: node={} service={} channel={}",
            node_name,
            service.name(),
            channel_name
        );

        loop {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = Bytes::copy_from_slice(sample.payload());
                    if payload.is_empty() {
                        continue;
                    }
                    match TradeSignal::from_bytes(&payload) {
                        Ok(signal) => {
                            handle_trade_signal(signal);
                        }
                        Err(err) => warn!(
                            "failed to decode trade signal from channel {}: {}",
                            channel_name, err
                        ),
                    }
                }
                Ok(None) => tokio::task::yield_now().await,
                Err(err) => {
                    warn!("signal receive error (channel={}): {err}", channel_name);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    /// 生成信号节点名称
    fn signal_node_name(channel: &str) -> String {
        format!("pre_trade_signal_{}", channel)
    }
}


fn handle_trade_signal(signal: TradeSignal) {
    match signal.signal_type {
        SignalType::ArbOpen => match ArbOpenCtx::from_bytes(signal.context.clone()) {
            Ok(open_ctx) => {
                let symbol = open_ctx.get_opening_symbol().to_uppercase();
                // 检查限价挂单数量限制
                if let Err(e) = MonitorChannel::instance().check_pending_limit_order(&symbol) {
                    warn!("ArbOpen: {} 限价挂单数量超限: {}", symbol, e);
                    return;
                }
                let strategy_id = StrategyManager::generate_strategy_id();
                let mut strategy = HedgeArbStrategy::new(strategy_id, symbol.clone());
                strategy.handle_signal_with_record(&signal);
                if strategy.is_active() {
                    MonitorChannel::instance().strategy_mgr().borrow_mut().insert(Box::new(strategy));
                }
            }
            Err(err) => warn!("failed to decode ArbOpen context: {err}"),
        },
        SignalType::ArbClose => {
            match ArbOpenCtx::from_bytes(signal.context.clone()) {
                Ok(close_ctx) => {
                    let opening_symbol = close_ctx.get_opening_symbol();
                    let hedging_symbol = close_ctx.get_hedging_symbol();

                    // 获取平仓方向
                    let Some(close_side) = Side::from_u8(close_ctx.side) else {
                        warn!("ArbClose: invalid side {}", close_ctx.side);
                        return;
                    };

                    // 查询两条腿的持仓（带符号）
                    let Some(opening_venue) = TradingVenue::from_u8(close_ctx.opening_leg.venue) else {
                        warn!("ArbClose: invalid opening_venue {}", close_ctx.opening_leg.venue);
                        return;
                    };
                    let Some(hedging_venue) = TradingVenue::from_u8(close_ctx.hedging_leg.venue) else {
                        warn!("ArbClose: invalid hedging_venue {}", close_ctx.hedging_leg.venue);
                        return;
                    };

                    let opening_pos = MonitorChannel::instance().get_position_qty(&opening_symbol, opening_venue);
                    let hedging_pos = MonitorChannel::instance().get_position_qty(&hedging_symbol, hedging_venue);

                    // 检查opening leg方向是否匹配
                    // 如果close是Sell，持仓应该>0（多头）；如果close是Buy，持仓应该<0（空头）
                    let opening_direction_match = match close_side {
                        Side::Sell => opening_pos > 0.0,
                        Side::Buy => opening_pos < 0.0,
                    };

                    // 检查hedging leg方向是否匹配（方向相反）
                    // 如果opening close是Sell，hedging close应该是Buy，持仓应该<0（空头）
                    let hedging_direction_match = match close_side {
                        Side::Sell => hedging_pos < 0.0,
                        Side::Buy => hedging_pos > 0.0,
                    };

                    if !opening_direction_match || !hedging_direction_match {
                        warn!(
                            "ArbClose: position direction mismatch, close_side={:?} opening_symbol={} opening_pos={:.6} hedging_symbol={} hedging_pos={:.6}",
                            close_side, opening_symbol, opening_pos, hedging_symbol, hedging_pos
                        );
                        return;
                    }

                    // 两条腿方向都匹配，取绝对值的最小值
                    let closeable_qty = opening_pos.abs().min(hedging_pos.abs());

                    // 和信号中的amount对比，取较小值
                    let final_qty = closeable_qty.min(close_ctx.amount as f64);

                    if final_qty <= 0.0 {
                        warn!(
                            "ArbClose: final_qty <= 0, closeable_qty={:.6} signal_amount={:.6}",
                            closeable_qty, close_ctx.amount
                        );
                        return;
                    }

                    debug!(
                        "ArbClose: final_qty={:.6} (closeable={:.6} signal_amount={:.6}) opening_symbol={} opening_pos={:.6} hedging_symbol={} hedging_pos={:.6}",
                        final_qty, closeable_qty, close_ctx.amount, opening_symbol, opening_pos, hedging_symbol, hedging_pos
                    );

                    // 平仓本质就是反向开仓，复用 HedgeArbStrategy
                    let strategy_id = StrategyManager::generate_strategy_id();
                    let mut strategy =
                        HedgeArbStrategy::new(strategy_id, opening_symbol.clone());

                    strategy.handle_signal_with_record(&signal);

                    if strategy.is_active() {
                        MonitorChannel::instance().strategy_mgr().borrow_mut().insert(Box::new(strategy));
                    }
                }
                Err(err) => warn!("failed to decode ArbClose context: {err}"),
            }
        }
        SignalType::ArbCancel => match ArbCancelCtx::from_bytes(signal.context.clone()) {
            Ok(cancel_ctx) => {
                let symbol = cancel_ctx.get_opening_symbol().to_uppercase();
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let candidate_ids: Vec<i32> = strategy_mgr
                    .borrow()
                    .ids_for_symbol(&symbol)
                    .map(|set| set.iter().copied().collect())
                    .unwrap_or_default();

                if candidate_ids.is_empty() {
                    return;
                }
                for strategy_id in candidate_ids {
                    if !strategy_mgr.borrow().contains(strategy_id) {
                        return;
                    }
                    // 取出策略，处理信号，然后放回
                    if let Some(mut strategy) = strategy_mgr.borrow_mut().take(strategy_id) {
                        strategy.handle_signal_with_record(&signal);
                        if strategy.is_active() {
                            strategy_mgr.borrow_mut().insert(strategy);
                        }
                    }
                }
            }
            Err(err) => warn!("failed to decode ArbCancel context: {err}"),
        },
        SignalType::ArbHedge => match ArbHedgeCtx::from_bytes(signal.context.clone()) {
            Ok(hedge_ctx) => {
                let strategy_id = hedge_ctx.strategy_id;
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                if !strategy_mgr.borrow().contains(strategy_id) {
                    return;
                }
                // 取出策略，处理信号，然后放回
                if let Some(mut strategy) = strategy_mgr.borrow_mut().take(strategy_id) {
                    strategy.handle_signal_with_record(&signal);
                    if strategy.is_active() {
                        strategy_mgr.borrow_mut().insert(strategy);
                    }
                }
                drop(strategy_mgr);
            }
            Err(err) => warn!("failed to decode hedge context: {err}"),
        },
    }
}