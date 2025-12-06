use crate::common::iceoryx_publisher::{SignalPublisher, SIGNAL_PAYLOAD};
use crate::common::ipc_service_name::build_service_name;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::Side;
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{SignalBytes, TradingVenue};
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::hedge_arb_strategy::HedgeArbStrategy;
use crate::strategy::{ForceCloseControl, Strategy, StrategyManager};
use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::cell::OnceCell;
use std::time::Duration;

thread_local! {
    static SIGNAL_CHANNEL: OnceCell<SignalChannel> = OnceCell::new();
}

/// 默认信号频道名称（与 fr_signal 的发布频道一致）
pub const DEFAULT_SIGNAL_CHANNEL: &str = "funding_rate_signal";

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
/// // 发送反向信号
/// SignalChannel::with(|ch| ch.publish_backward(&data));
/// ```
pub struct SignalChannel {
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
        // 创建反向发布器
        let backward_pub = if let Some(backward_ch) = backward_channel {
            SignalPublisher::new(backward_ch)
                .map_err(|e| warn!("SignalChannel backward_pub failed: {e:#}"))
                .ok()
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
            backward_pub,
            channel_name: channel_name.to_string(),
        })
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
        let service_path = build_service_name(&format!("signal_pubs/{}", channel_name));

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

        // pre_trade 刚启动时可能存在大量历史信号，先主动清空队列
        let mut flushed = 0usize;
        loop {
            match subscriber.receive() {
                Ok(Some(_)) => flushed += 1,
                Ok(None) => break,
                Err(err) => {
                    warn!(
                        "signal flush failed (channel={}) err={:?}",
                        channel_name, err
                    );
                    break;
                }
            }
        }
        if flushed > 0 {
            info!(
                "signal channel {} flushed {} cached signals before processing new data",
                channel_name, flushed
            );
        }

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
                let hedging_symbol = open_ctx.get_hedging_symbol();
                let side = open_ctx.get_side();
                let opening_venue = TradingVenue::from_u8(open_ctx.opening_leg.venue)
                    .unwrap_or(TradingVenue::BinanceMargin);
                let hedging_venue = TradingVenue::from_u8(open_ctx.hedging_leg.venue)
                    .unwrap_or(TradingVenue::BinanceUm);

                // 检查限价挂单数量限制
                if let Err(e) = MonitorChannel::instance().check_pending_limit_order(&symbol) {
                    warn!("ArbOpen: {} 限价挂单数量超限: {}", symbol, e);
                    return;
                }
                let strategy_id = StrategyManager::generate_strategy_id();
                let mut strategy = HedgeArbStrategy::new(strategy_id, symbol.clone());
                strategy.handle_signal_with_record(&signal);
                if strategy.is_active() {
                    let hedge_mode = if open_ctx.hedge_timeout_us > 0 {
                        "MM"
                    } else {
                        "MT"
                    };
                    info!(
                        "🔔 收到 ArbOpen 信号({}): opening={} {:?} side={:?} price={:.6} hedging={} {:?} | amount={:.4} fr_ma={:.6} pred_fr={:.6} loan={:.6} hedge_timeout_us={}",
                        hedge_mode,
                        symbol, opening_venue, side, open_ctx.price,
                        hedging_symbol, hedging_venue,
                        open_ctx.amount, open_ctx.funding_ma, open_ctx.predicted_funding_rate, open_ctx.loan_rate, open_ctx.hedge_timeout_us
                    );
                    info!(
                        "✅ ArbOpen: strategy_id={} {} 已创建并激活",
                        strategy_id, symbol
                    );
                    MonitorChannel::instance()
                        .strategy_mgr()
                        .borrow_mut()
                        .insert(Box::new(strategy));
                } else {
                    warn!("⚠️ ArbOpen: strategy_id={} {} 未激活", strategy_id, symbol);
                }
            }
            Err(err) => warn!("failed to decode ArbOpen context: {err}"),
        },
        SignalType::ArbClose => {
            match ArbOpenCtx::from_bytes(signal.context.clone()) {
                Ok(mut close_ctx) => {
                    let opening_symbol = close_ctx.get_opening_symbol();
                    let hedging_symbol = close_ctx.get_hedging_symbol();

                    // 获取平仓方向
                    let Some(close_side) = Side::from_u8(close_ctx.side) else {
                        warn!("ArbClose: invalid side {}", close_ctx.side);
                        return;
                    };

                    // 查询两条腿的持仓（带符号）
                    let Some(opening_venue) = TradingVenue::from_u8(close_ctx.opening_leg.venue)
                    else {
                        warn!(
                            "ArbClose: invalid opening_venue {}",
                            close_ctx.opening_leg.venue
                        );
                        return;
                    };
                    let Some(hedging_venue) = TradingVenue::from_u8(close_ctx.hedging_leg.venue)
                    else {
                        warn!(
                            "ArbClose: invalid hedging_venue {}",
                            close_ctx.hedging_leg.venue
                        );
                        return;
                    };

                    let opening_pos =
                        MonitorChannel::instance().get_position_qty(&opening_symbol, opening_venue);
                    let hedging_pos =
                        MonitorChannel::instance().get_position_qty(&hedging_symbol, hedging_venue);

                    const SPOT_FLAT_THRESHOLD: f64 = 1e-5;
                    let is_spot_opening = matches!(
                        opening_venue,
                        TradingVenue::BinanceMargin | TradingVenue::BinanceSpot
                    );
                    if is_spot_opening && opening_pos.abs() <= SPOT_FLAT_THRESHOLD {
                        // 现货腿已经为 0，说明信号已失效，直接跳过不噪声打日志
                        return;
                    }

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
                        // ArbClose 可能在仓位已经被其他流程平掉后触发，这属于正常情况，只记录信息方便追踪
                        info!(
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

                    info!(
                        "ArbClose: final_qty={:.6} (closeable={:.6} signal_amount={:.6}) opening_symbol={} opening_pos={:.6} hedging_symbol={} hedging_pos={:.6}",
                        final_qty, closeable_qty, close_ctx.amount, opening_symbol, opening_pos, hedging_symbol, hedging_pos
                    );

                    // 使用最终可平仓数量覆盖原始 amount，并转换为 ArbOpen 信号
                    close_ctx.amount = final_qty as f32;
                    let converted_signal = TradeSignal::create(
                        SignalType::ArbOpen,
                        signal.generation_time,
                        signal.handle_time,
                        close_ctx.to_bytes(),
                    );

                    info!(
                        "ArbClose: converted to ArbOpen signal side={:?} qty={:.6} opening={} hedging={}",
                        close_side, final_qty, opening_symbol, hedging_symbol
                    );

                    // 平仓本质就是反向开仓，复用 HedgeArbStrategy
                    let strategy_id = StrategyManager::generate_strategy_id();
                    let mut strategy: HedgeArbStrategy =
                        HedgeArbStrategy::new(strategy_id, opening_symbol.clone());
                    strategy.set_force_close_mode(true);

                    strategy.handle_signal_with_record(&converted_signal);

                    if strategy.is_active() {
                        let hedge_mode = if close_ctx.hedge_timeout_us > 0 {
                            "MM"
                        } else {
                            "MT"
                        };
                        info!(
                            "🔔 收到 ArbClose 信号({}): opening={} {:?} hedging={} {:?} | side={:?} amount={:.4} price={:.6} hedge_timeout_us={}",
                            hedge_mode,
                            opening_symbol, opening_venue, hedging_symbol, hedging_venue,
                            close_side, close_ctx.amount, close_ctx.price, close_ctx.hedge_timeout_us
                        );

                        MonitorChannel::instance()
                            .strategy_mgr()
                            .borrow_mut()
                            .insert(Box::new(strategy));
                    }
                }
                Err(err) => warn!("failed to decode ArbClose context: {err}"),
            }
        }

        SignalType::ArbCancel => match ArbCancelCtx::from_bytes(signal.context.clone()) {
            Ok(cancel_ctx) => {
                let symbol = cancel_ctx.get_opening_symbol().to_uppercase();
                let hedging_symbol = cancel_ctx.get_hedging_symbol();
                let opening_venue = TradingVenue::from_u8(cancel_ctx.opening_leg.venue)
                    .unwrap_or(TradingVenue::BinanceMargin);
                let hedging_venue = TradingVenue::from_u8(cancel_ctx.hedging_leg.venue)
                    .unwrap_or(TradingVenue::BinanceUm);

                let strategy_mgr = MonitorChannel::instance().strategy_mgr();

                // 使用代码块限制借用作用域，确保在进入循环前释放
                let candidate_ids: Vec<i32> = {
                    strategy_mgr
                        .borrow()
                        .ids_for_symbol(&symbol)
                        .map(|set| set.iter().copied().collect())
                        .unwrap_or_default()
                };

                if candidate_ids.is_empty() {
                    return;
                }
                info!(
                    "ArbCancel: 找到 {} 个活跃策略 {:?}, opening={} {:?} hedging={} {:?}",
                    candidate_ids.len(),
                    candidate_ids,
                    symbol,
                    opening_venue,
                    hedging_symbol,
                    hedging_venue
                );
                for strategy_id in candidate_ids {
                    let exists = { strategy_mgr.borrow().contains(strategy_id) };
                    if !exists {
                        continue;
                    }
                    let strategy_opt = { strategy_mgr.borrow_mut().take(strategy_id) };
                    if let Some(mut strategy) = strategy_opt {
                        info!("ArbCancel: 处理策略 id={}", strategy_id);
                        strategy.handle_signal_with_record(&signal);
                        if strategy.is_active() {
                            strategy_mgr.borrow_mut().insert(strategy);
                        } else {
                            info!("ArbCancel: 策略 id={} 已不活跃，不再放回", strategy_id);
                        }
                    }
                }
                drop(strategy_mgr);
            }
            Err(err) => warn!("failed to decode ArbCancel context: {err}"),
        },
        SignalType::ArbHedge => match ArbHedgeCtx::from_bytes(signal.context.clone()) {
            Ok(hedge_ctx) => {
                let strategy_id = hedge_ctx.strategy_id;
                let hedging_symbol = hedge_ctx.get_hedging_symbol();
                let hedging_venue = TradingVenue::from_u8(hedge_ctx.hedging_leg.venue)
                    .unwrap_or(TradingVenue::BinanceUm);
                let hedge_side = hedge_ctx.get_side();
                let hedge_price = hedge_ctx.get_hedge_price();

                info!(
                    "🔔 收到 ArbHedge 信号: strategy_id={} hedging={} {:?} | side={:?} qty={:.4} price={:.6} is_maker={}",
                    strategy_id, hedging_symbol, hedging_venue, hedge_side, hedge_ctx.hedge_qty, hedge_price, hedge_ctx.is_maker()
                );

                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                if !strategy_mgr.borrow().contains(strategy_id) {
                    warn!("ArbHedge: 策略 id={} 不存在", strategy_id);
                    return;
                }
                // 取出策略，处理信号，然后放回
                let strategy_opt = { strategy_mgr.borrow_mut().take(strategy_id) };
                if let Some(mut strategy) = strategy_opt {
                    info!("ArbHedge: 处理策略 id={}", strategy_id);
                    strategy.handle_signal_with_record(&signal);
                    if strategy.is_active() {
                        strategy_mgr.borrow_mut().insert(strategy);
                    } else {
                        info!("ArbHedge: 策略 id={} 已不活跃，不再放回", strategy_id);
                    }
                }
                drop(strategy_mgr);
            }
            Err(err) => warn!("failed to decode hedge context: {err}"),
        },
    }
}
