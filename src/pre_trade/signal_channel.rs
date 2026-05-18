use crate::common::iceoryx_publisher::{SignalPublisher, SIGNAL_PAYLOAD};
use crate::common::ipc_service_name::build_service_name;
use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::log_throttle::log_pending_limit_summary;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::pre_trade::signal_throttle::check_signal_throttle;
use crate::signal::arb_signal::{
    ArbBackwardQueryMsg, ArbCancelCandidateEntry, ArbCancelCandidateQueryMsg, ArbCancelTriggerCtx,
};
use crate::signal::cancel_signal::{ArbCancelCtx, MmCancelCtx};
use crate::signal::common::{SignalBytes, TradingVenue};
use crate::signal::hedge_signal::{ArbHedgeCtx, MmHedgeCtx};
use crate::signal::mm_signal::{
    MmBackwardQueryMsg, MmCancelCandidateEntry, MmCancelCandidateQueryMsg, MmCancelTriggerCtx,
};
use crate::signal::open_signal::{ArbOpenCtx, MmOpenCtx};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::arb_close_strategy::ArbCloseStrategy;
use crate::strategy::arb_open_strategy::ArbOpenStrategy;
use crate::strategy::mm_open_strategy::MarketMakerOpenStrategy;
use crate::strategy::open_strategy_common::OpenStrategyCommon;
use crate::strategy::{Strategy, StrategyManager};
use anyhow::Result;
use bytes::Bytes;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

thread_local! {
    static SIGNAL_CHANNEL: OnceCell<SignalChannel> = const { OnceCell::new() };
    static SIGNAL_COUNTS: RefCell<HashMap<String, u64>> = RefCell::new(HashMap::new());
}

/// 默认信号频道名称（与 trade_signal 的发布频道一致）
pub const DEFAULT_SIGNAL_CHANNEL: &str = "trade_signal";

/// 默认反向信号频道名称
pub const DEFAULT_BACKWARD_CHANNEL: &str = "trade_query";

const ARB_CLOSE_MIN_NOTIONAL_U: f64 = 25.0;

fn arb_close_side_matches_open_position(close_side: Side, opening_pos: f64) -> bool {
    match close_side {
        Side::Sell => opening_pos > 0.0,
        Side::Buy => opening_pos < 0.0,
    }
}

fn arb_close_notional_meets_min(ctx: &ArbOpenCtx) -> bool {
    let notional = ctx.amount_value() * ctx.price_value();
    notional.is_finite() && notional >= ARB_CLOSE_MIN_NOTIONAL_U
}

fn should_drop_startup_buffered_signal(signal: &TradeSignal, listener_start_us: i64) -> bool {
    signal.generation_time > 0 && signal.generation_time < listener_start_us
}

fn should_suppress_arb_open_inactive_warning(reason: &str) -> bool {
    reason.starts_with("open order rate limit triggered:")
        || reason.starts_with("INTRA_NO_BORROW 余额不足")
        || reason.starts_with("STANDARD 余额不足")
}

pub fn take_signal_counts() -> HashMap<String, u64> {
    SIGNAL_COUNTS.with(|counts| std::mem::take(&mut *counts.borrow_mut()))
}

fn record_signal_count(signal_type: &SignalType) {
    SIGNAL_COUNTS.with(|counts| {
        let mut counts = counts.borrow_mut();
        *counts.entry(signal_type.as_str().to_string()).or_insert(0) += 1;
    });
}

fn is_arb_signal_type(signal_type: &SignalType) -> bool {
    matches!(
        signal_type,
        SignalType::ArbOpen
            | SignalType::ArbClose
            | SignalType::ArbCancel
            | SignalType::ArbCancelTrigger
            | SignalType::ArbHedge
    )
}

fn should_block_arb_signal_for_startup_net_gate(signal_type: &SignalType) -> bool {
    if !is_arb_signal_type(signal_type) {
        return false;
    }
    let status = MonitorChannel::instance().arb_startup_net_gate_status();
    if !status.enabled || status.ready {
        return false;
    }
    let status = MonitorChannel::instance().record_arb_startup_net_gate_signal_drop();
    debug!(
        "Arb signal blocked by startup net gate: type={} open_ready={} hedge_ready={} dropped_signals={}",
        signal_type.as_str(),
        status.open_ready,
        status.hedge_ready,
        status.dropped_signals
    );
    true
}

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
        let listener_start_us = get_timestamp_us();
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

        let mut dropped_startup_buffered = 0usize;

        loop {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    let payload = Bytes::copy_from_slice(sample.payload());
                    if payload.is_empty() {
                        continue;
                    }
                    match TradeSignal::from_bytes(&payload) {
                        Ok(signal) => {
                            if should_drop_startup_buffered_signal(&signal, listener_start_us) {
                                dropped_startup_buffered += 1;
                                if dropped_startup_buffered <= 5
                                    || dropped_startup_buffered.is_multiple_of(100)
                                {
                                    info!(
                                        "signal channel {} dropped startup-buffered signal count={} type={:?} generation_time={} listener_start_us={}",
                                        channel_name,
                                        dropped_startup_buffered,
                                        signal.signal_type,
                                        signal.generation_time,
                                        listener_start_us
                                    );
                                }
                                continue;
                            }
                            if dropped_startup_buffered > 0 {
                                info!(
                                    "signal channel {} finished dropping startup-buffered signals count={} first_live_generation_time={}",
                                    channel_name,
                                    dropped_startup_buffered,
                                    signal.generation_time
                                );
                                dropped_startup_buffered = 0;
                            }
                            record_signal_count(&signal.signal_type);
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

#[cfg(test)]
mod tests {
    use super::should_drop_startup_buffered_signal;
    use crate::signal::trade_signal::{SignalType, TradeSignal};
    use bytes::Bytes;

    #[test]
    fn startup_filter_drops_older_signals() {
        let signal = TradeSignal::create(SignalType::MMOpen, 999, 0.0, Bytes::new());
        assert!(should_drop_startup_buffered_signal(&signal, 1_000));
    }

    #[test]
    fn startup_filter_keeps_signals_from_current_generation_onward() {
        let fresh = TradeSignal::create(SignalType::MMOpen, 1_000, 0.0, Bytes::new());
        let newer = TradeSignal::create(SignalType::MMOpen, 1_001, 0.0, Bytes::new());
        let missing_ts = TradeSignal::create(SignalType::MMOpen, 0, 0.0, Bytes::new());
        assert!(!should_drop_startup_buffered_signal(&fresh, 1_000));
        assert!(!should_drop_startup_buffered_signal(&newer, 1_000));
        assert!(!should_drop_startup_buffered_signal(&missing_ts, 1_000));
    }
}

fn handle_trade_signal(signal: TradeSignal) {
    if should_block_arb_signal_for_startup_net_gate(&signal.signal_type) {
        return;
    }

    let is_mm_mode = {
        let monitor = MonitorChannel::instance();
        monitor.open_venue() == monitor.hedge_venue()
    };

    match signal.signal_type {
        SignalType::ArbOpen => match ArbOpenCtx::from_bytes(signal.context.clone()) {
            Ok(mut open_ctx) => {
                let symbol = normalize_symbol_for_internal(&open_ctx.get_opening_symbol());
                if symbol.is_empty() {
                    warn!("ArbOpen: empty symbol");
                    return;
                }
                let hedging_symbol = normalize_symbol_for_internal(&open_ctx.get_hedging_symbol());
                let Some(side) = open_ctx.get_side() else {
                    warn!("ArbOpen: invalid side {}", open_ctx.side);
                    return;
                };
                let from_key = String::from_utf8_lossy(&open_ctx.from_key).to_string();
                if let Some(hit) = check_signal_throttle(&symbol, side) {
                    debug!(
                        "ArbOpen: throttled by pre_trade block, symbol={} side={} remain_us={} last_code={} until_us={}, skip strategy construction",
                        symbol,
                        side.as_str(),
                        hit.remaining_us,
                        hit.last_error_code,
                        hit.until_us
                    );
                    return;
                }
                let opening_venue = TradingVenue::from_u8(open_ctx.opening_leg.venue)
                    .unwrap_or(TradingVenue::BinanceMargin);
                let hedging_venue = TradingVenue::from_u8(open_ctx.hedging_leg.venue)
                    .unwrap_or(TradingVenue::BinanceFutures);

                let configured_open_venue = MonitorChannel::instance().open_venue();
                let configured_hedge_venue = MonitorChannel::instance().hedge_venue();
                if opening_venue != configured_open_venue || hedging_venue != configured_hedge_venue
                {
                    warn!(
                        "ArbOpen: signal venue mismatch, configured_open={:?} configured_hedge={:?} but got open={:?} hedge={:?}, ignore",
                        configured_open_venue,
                        configured_hedge_venue,
                        opening_venue,
                        hedging_venue
                    );
                    return;
                }

                // 检查限价挂单数量限制
                if let Err(e) = MonitorChannel::instance().check_pending_limit_order(&symbol, side)
                {
                    log_pending_limit_summary("ArbOpen", None, &symbol, side, &e);
                    return;
                }
                open_ctx.set_opening_symbol(&symbol);
                open_ctx.set_hedging_symbol(&hedging_symbol);
                let normalized_signal = TradeSignal::create(
                    SignalType::ArbOpen,
                    signal.generation_time,
                    signal.handle_time,
                    open_ctx.to_bytes(),
                );
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let _ = strategy_mgr.borrow_mut().ensure_arb_hedge_strategy(&symbol);
                let strategy_id = StrategyManager::generate_strategy_id();
                let mut strategy = ArbOpenStrategy::new(strategy_id);
                strategy.handle_signal(&normalized_signal);
                if strategy.is_active() {
                    // hedge_timeout 已不再做 close_ts 延迟（强制 0），原本根据这个字段
                    // 推断 MM/MT 的标签也就失效，去掉。
                    let signal_price = open_ctx.price_value();
                    let signal_amount = open_ctx.amount_value();
                    info!(
                        "🔔 收到 ArbOpen 信号: opening={} {:?} side={:?} price={:.6} hedging={} {:?} | amount={:.4} spread_rate={:.6} from_key='{}'",
                        symbol, opening_venue, side, signal_price,
                        hedging_symbol, hedging_venue,
                        signal_amount,
                        open_ctx.spread_rate,
                        from_key
                    );
                    info!(
                        "✅ ArbOpenStrategy: strategy_id={} {} 已创建并激活",
                        strategy_id, symbol
                    );
                    strategy_mgr.borrow_mut().insert(Box::new(strategy));
                } else {
                    let reason = strategy
                        .open_strategy_inactive_reason()
                        .unwrap_or("unknown");
                    if !should_suppress_arb_open_inactive_warning(reason) {
                        warn!(
                            "⚠️ ArbOpen: strategy_id={} {} 未激活 reason={}",
                            strategy_id, symbol, reason
                        );
                    }
                }
            }
            Err(err) => warn!("failed to decode ArbOpen context: {err}"),
        },
        SignalType::ArbClose => {
            match ArbOpenCtx::from_bytes(signal.context.clone()) {
                Ok(mut close_ctx) => {
                    let opening_symbol =
                        normalize_symbol_for_internal(&close_ctx.get_opening_symbol());
                    let hedging_symbol =
                        normalize_symbol_for_internal(&close_ctx.get_hedging_symbol());
                    close_ctx.set_opening_symbol(&opening_symbol);
                    close_ctx.set_hedging_symbol(&hedging_symbol);

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

                    let configured_open_venue = MonitorChannel::instance().open_venue();
                    let configured_hedge_venue = MonitorChannel::instance().hedge_venue();
                    if opening_venue != configured_open_venue
                        || hedging_venue != configured_hedge_venue
                    {
                        warn!(
                            "ArbClose: signal venue mismatch, configured_open={:?} configured_hedge={:?} but got open={:?} hedge={:?}, ignore",
                            configured_open_venue,
                            configured_hedge_venue,
                            opening_venue,
                            hedging_venue
                        );
                        return;
                    }

                    if close_ctx.amount_value() <= 1e-12 || close_ctx.amount_count() <= 0 {
                        return;
                    }

                    let opening_pos =
                        MonitorChannel::instance().get_position_qty(&opening_symbol, opening_venue);
                    let hedging_pos =
                        MonitorChannel::instance().get_position_qty(&hedging_symbol, hedging_venue);
                    if !arb_close_side_matches_open_position(close_side, opening_pos) {
                        return;
                    }
                    if !arb_close_notional_meets_min(&close_ctx) {
                        return;
                    }
                    let strategy_mgr = MonitorChannel::instance().strategy_mgr();

                    let normalized_signal = TradeSignal::create(
                        SignalType::ArbClose,
                        signal.generation_time,
                        signal.handle_time,
                        close_ctx.to_bytes(),
                    );

                    {
                        let _ = strategy_mgr
                            .borrow_mut()
                            .ensure_arb_hedge_strategy(&opening_symbol);
                    }

                    let strategy_id = StrategyManager::generate_strategy_id();
                    let mut strategy = ArbCloseStrategy::new(strategy_id);
                    strategy.handle_signal(&normalized_signal);
                    if strategy.is_active() {
                        info!(
                            "🔔 收到 ArbClose 信号: opening={} {:?} hedging={} {:?} | side={:?} open_pos={:.4} hedge_pos={:.4} price={:.6}",
                            opening_symbol, opening_venue, hedging_symbol, hedging_venue,
                            close_side, opening_pos, hedging_pos, close_ctx.price_value()
                        );
                        strategy_mgr.borrow_mut().insert(Box::new(strategy));
                    }
                }
                Err(err) => warn!("failed to decode ArbClose context: {err}"),
            }
        }

        SignalType::ArbCancel => match ArbCancelCtx::from_bytes(signal.context.clone()) {
            Ok(mut cancel_ctx) => {
                let symbol = normalize_symbol_for_internal(&cancel_ctx.get_opening_symbol());
                let hedging_symbol =
                    normalize_symbol_for_internal(&cancel_ctx.get_hedging_symbol());
                let cancel_side = cancel_ctx.get_side();
                let cancel_reason = cancel_ctx.get_reason();
                let require_direction_match = matches!(
                    cancel_reason,
                    crate::signal::cancel_signal::ArbCancelReason::Spread
                ) && cancel_ctx.strategy_id <= 0;
                let opening_venue = TradingVenue::from_u8(cancel_ctx.opening_leg.venue)
                    .unwrap_or(TradingVenue::BinanceMargin);
                let hedging_venue = TradingVenue::from_u8(cancel_ctx.hedging_leg.venue)
                    .unwrap_or(TradingVenue::BinanceFutures);

                let configured_open_venue = MonitorChannel::instance().open_venue();
                let configured_hedge_venue = MonitorChannel::instance().hedge_venue();
                if opening_venue != configured_open_venue || hedging_venue != configured_hedge_venue
                {
                    warn!(
                        "ArbCancel: signal venue mismatch, configured_open={:?} configured_hedge={:?} but got open={:?} hedge={:?}, ignore",
                        configured_open_venue,
                        configured_hedge_venue,
                        opening_venue,
                        hedging_venue
                    );
                    return;
                }

                cancel_ctx.set_opening_symbol(&symbol);
                cancel_ctx.set_hedging_symbol(&hedging_symbol);
                let normalized_signal = TradeSignal::create(
                    SignalType::ArbCancel,
                    signal.generation_time,
                    signal.handle_time,
                    cancel_ctx.to_bytes(),
                );
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();

                if cancel_ctx.strategy_id > 0 {
                    let strategy_id = cancel_ctx.strategy_id;
                    let exists = { strategy_mgr.borrow().contains(strategy_id) };
                    if !exists {
                        debug!(
                            "ArbCancel: targeted strategy missing strategy_id={} symbol={} trigger_ts={}",
                            strategy_id,
                            symbol,
                            cancel_ctx.trigger_ts
                        );
                        return;
                    }
                    let strategy_opt = { strategy_mgr.borrow_mut().take(strategy_id) };
                    if let Some(mut strategy) = strategy_opt {
                        let should_handle = strategy
                            .as_any()
                            .downcast_ref::<ArbOpenStrategy>()
                            .is_some_and(|arb| {
                                !require_direction_match || arb.open_side() == Some(cancel_side)
                            });
                        if !should_handle {
                            strategy_mgr.borrow_mut().insert(strategy);
                            return;
                        }
                        strategy.handle_signal(&normalized_signal);
                        if strategy.is_active() {
                            strategy_mgr.borrow_mut().insert(strategy);
                        }
                    }
                    return;
                }

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
                for strategy_id in candidate_ids {
                    let exists = { strategy_mgr.borrow().contains(strategy_id) };
                    if !exists {
                        continue;
                    }
                    let strategy_opt = { strategy_mgr.borrow_mut().take(strategy_id) };
                    if let Some(mut strategy) = strategy_opt {
                        let should_handle = strategy
                            .as_any()
                            .downcast_ref::<ArbOpenStrategy>()
                            .is_some_and(|arb| {
                                !require_direction_match || arb.open_side() == Some(cancel_side)
                            });
                        if !should_handle {
                            strategy_mgr.borrow_mut().insert(strategy);
                            continue;
                        }
                        strategy.handle_signal(&normalized_signal);
                        if strategy.is_active() {
                            strategy_mgr.borrow_mut().insert(strategy);
                        }
                    }
                }
                drop(strategy_mgr);
            }
            Err(err) => warn!("failed to decode ArbCancel context: {err}"),
        },
        SignalType::ArbCancelTrigger => {
            match ArbCancelTriggerCtx::from_bytes(signal.context.clone()) {
                Ok(trigger_ctx) => {
                    if is_mm_mode {
                        debug!("ArbCancelTrigger ignored: pre_trade is in MM mode");
                        return;
                    }

                    let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                    let price_map_snapshot = strategy_mgr.borrow().arb_open_price_map_snapshot();
                    if price_map_snapshot.is_empty() {
                        return;
                    }

                    let mut symbol_counts: BTreeMap<String, usize> = BTreeMap::new();
                    let mut indexed_strategy_count = 0usize;
                    for (key, strategy_ids) in &price_map_snapshot {
                        *symbol_counts.entry(key.symbol.clone()).or_default() += strategy_ids.len();
                        indexed_strategy_count += strategy_ids.len();
                    }
                    let symbol_count = symbol_counts.len();
                    let symbol_sample = {
                        let preview: Vec<String> = symbol_counts
                            .iter()
                            .take(8)
                            .map(|(symbol, count)| format!("{symbol}:{count}"))
                            .collect();
                        if preview.is_empty() {
                            "-".to_string()
                        } else {
                            preview.join(",")
                        }
                    };
                    let strategy_sample = {
                        let preview: Vec<String> = price_map_snapshot
                            .iter()
                            .flat_map(|(key, strategy_ids)| {
                                strategy_ids.iter().map(move |strategy_id| {
                                    format!(
                                        "{}#{}@{}",
                                        key.symbol,
                                        strategy_id,
                                        key.price_qv.count()
                                    )
                                })
                            })
                            .take(12)
                            .collect();
                        if preview.is_empty() {
                            "-".to_string()
                        } else {
                            preview.join(",")
                        }
                    };

                    let mut chunk = ArbCancelCandidateQueryMsg::new(trigger_ctx.trigger_ts);
                    let mut published_chunks = 0usize;
                    let mut published_items = 0usize;

                    let flush_chunk =
                        |chunk: &mut ArbCancelCandidateQueryMsg,
                         published_chunks: &mut usize,
                         published_items: &mut usize| {
                            if chunk.is_empty() {
                                return;
                            }
                            let item_count = chunk
                                .groups
                                .iter()
                                .map(|group| group.items.len())
                                .sum::<usize>();
                            let payload =
                                ArbBackwardQueryMsg::CancelCandidates(chunk.clone()).to_bytes();
                            match SignalChannel::with(|ch| ch.publish_backward(&payload)) {
                                Ok(true) => {
                                    *published_chunks += 1;
                                    *published_items += item_count;
                                }
                                Ok(false) => {
                                    warn!("ArbCancelTrigger: backward publisher unavailable");
                                }
                                Err(err) => {
                                    warn!(
                                        "ArbCancelTrigger: publish backward failed err={:#}",
                                        err
                                    );
                                }
                            }
                            chunk.groups.clear();
                        };

                    for (key, strategy_ids) in price_map_snapshot {
                        let price_qv = key.price_qv.to_quantized_value();
                        for strategy_id in strategy_ids {
                            let entry = ArbCancelCandidateEntry::new(strategy_id, price_qv);
                            let next_len = 1 + chunk.next_encoded_len_with(&key.symbol, &entry);
                            if !chunk.is_empty() && next_len > SIGNAL_PAYLOAD {
                                flush_chunk(
                                    &mut chunk,
                                    &mut published_chunks,
                                    &mut published_items,
                                );
                            }
                            chunk.push_grouped(&key.symbol, entry);
                        }
                    }
                    flush_chunk(&mut chunk, &mut published_chunks, &mut published_items);
                    debug!(
                    "ArbCancelTrigger: dynamic index published chunks={} items={} indexed_strategies={} symbols={} sample={} strategies={} trigger_ts={} freq_ms={}",
                    published_chunks,
                    published_items,
                    indexed_strategy_count,
                    symbol_count,
                    symbol_sample,
                    strategy_sample,
                    trigger_ctx.trigger_ts,
                    trigger_ctx.freq_ms
                );
                }
                Err(err) => warn!("failed to decode ArbCancelTrigger context: {err}"),
            }
        }
        SignalType::ArbHedge => match ArbHedgeCtx::from_bytes(signal.context.clone()) {
            Ok(mut hedge_ctx) => {
                let strategy_id = hedge_ctx.strategy_id;
                let hedging_symbol = normalize_symbol_for_internal(&hedge_ctx.get_hedging_symbol());
                let hedging_venue = TradingVenue::from_u8(hedge_ctx.hedging_leg.venue)
                    .unwrap_or(TradingVenue::BinanceFutures);

                let configured_hedge_venue = MonitorChannel::instance().hedge_venue();
                if hedging_venue != configured_hedge_venue {
                    warn!(
                        "ArbHedge: signal venue mismatch, configured_hedge={:?} but got {:?}, ignore",
                        configured_hedge_venue, hedging_venue
                    );
                    return;
                }

                hedge_ctx.set_hedging_symbol(&hedging_symbol);
                let normalized_signal = TradeSignal::create(
                    SignalType::ArbHedge,
                    signal.generation_time,
                    signal.handle_time,
                    hedge_ctx.to_bytes(),
                );
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                if !strategy_mgr.borrow().contains(strategy_id) {
                    warn!("ArbHedge: 策略 id={} 不存在", strategy_id);
                    return;
                }
                let strategy_opt = { strategy_mgr.borrow_mut().take(strategy_id) };
                if let Some(mut strategy) = strategy_opt {
                    strategy.handle_signal(&normalized_signal);
                    if strategy.is_active() {
                        strategy_mgr.borrow_mut().insert(strategy);
                    }
                }
            }
            Err(err) => warn!("failed to decode ArbHedge context: {err}"),
        },
        SignalType::MMOpen => {
            if !is_mm_mode {
                debug!("MMOpen ignored: pre_trade is not in MM mode");
                return;
            }
            let Ok(mut open_ctx) = MmOpenCtx::from_bytes(signal.context.clone()) else {
                warn!("failed to decode MMOpen context");
                return;
            };
            let symbol = normalize_symbol_for_internal(&open_ctx.get_opening_symbol());
            if symbol.is_empty() {
                warn!("MMOpen: empty symbol");
                return;
            }
            let Some(side) = open_ctx.get_side() else {
                warn!("MMOpen: invalid side {}", open_ctx.side);
                return;
            };
            if let Some(order_type) = OrderType::from_u8(open_ctx.order_type) {
                if order_type.is_limit() {
                    if let Err(e) =
                        MonitorChannel::instance().check_pending_limit_order(&symbol, side)
                    {
                        log_pending_limit_summary("MMOpen", None, &symbol, side, &e);
                        return;
                    }
                }
            } else {
                warn!("MMOpen: invalid order_type {}", open_ctx.order_type);
                return;
            }

            open_ctx.set_opening_symbol(&symbol);
            let normalized_signal = TradeSignal::create(
                SignalType::MMOpen,
                signal.generation_time,
                signal.handle_time,
                open_ctx.to_bytes(),
            );
            let strategy_mgr = MonitorChannel::instance().strategy_mgr();
            let _ = strategy_mgr.borrow_mut().ensure_mm_hedge_strategy(&symbol);

            let strategy_id = StrategyManager::generate_strategy_id();
            let mut strategy = MarketMakerOpenStrategy::new(strategy_id);
            strategy.handle_signal(&normalized_signal);
            if strategy.is_active() {
                debug!("MMOpen: strategy activated id={}", strategy_id);
                strategy_mgr.borrow_mut().insert(Box::new(strategy));
            } else {
                info!("MMOpen: strategy_id={} 未激活", strategy_id);
            }
        }
        SignalType::MMCancelTrigger => match MmCancelTriggerCtx::from_bytes(signal.context.clone())
        {
            Ok(trigger_ctx) => {
                if !is_mm_mode {
                    debug!("MMCancelTrigger ignored: pre_trade is not in MM mode");
                    return;
                }

                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let price_map_snapshot = strategy_mgr.borrow().mm_open_price_map_snapshot();
                let mut symbol_counts: BTreeMap<String, usize> = BTreeMap::new();
                let mut indexed_strategy_count = 0usize;
                for (key, strategy_ids) in &price_map_snapshot {
                    *symbol_counts.entry(key.symbol.clone()).or_default() += strategy_ids.len();
                    indexed_strategy_count += strategy_ids.len();
                }
                let symbol_count = symbol_counts.len();
                let symbol_sample = {
                    let preview: Vec<String> = symbol_counts
                        .iter()
                        .take(8)
                        .map(|(symbol, count)| format!("{symbol}:{count}"))
                        .collect();
                    if preview.is_empty() {
                        "-".to_string()
                    } else {
                        preview.join(",")
                    }
                };
                let strategy_sample = {
                    let preview: Vec<String> = price_map_snapshot
                        .iter()
                        .flat_map(|(key, strategy_ids)| {
                            strategy_ids.iter().map(move |strategy_id| {
                                format!("{}#{}@{}", key.symbol, strategy_id, key.price_qv.count())
                            })
                        })
                        .take(12)
                        .collect();
                    if preview.is_empty() {
                        "-".to_string()
                    } else {
                        preview.join(",")
                    }
                };
                if price_map_snapshot.is_empty() {
                    return;
                }

                let mut chunk = MmCancelCandidateQueryMsg::new(trigger_ctx.trigger_ts);
                let mut published_chunks = 0usize;
                let mut published_items = 0usize;

                let flush_chunk = |chunk: &mut MmCancelCandidateQueryMsg,
                                   published_chunks: &mut usize,
                                   published_items: &mut usize| {
                    if chunk.is_empty() {
                        return;
                    }
                    let item_count = chunk
                        .groups
                        .iter()
                        .map(|group| group.items.len())
                        .sum::<usize>();
                    let payload = MmBackwardQueryMsg::CancelCandidates(chunk.clone()).to_bytes();
                    match SignalChannel::with(|ch| ch.publish_backward(&payload)) {
                        Ok(true) => {
                            *published_chunks += 1;
                            *published_items += item_count;
                        }
                        Ok(false) => {
                            warn!("MMCancelTrigger: backward publisher unavailable");
                        }
                        Err(err) => {
                            warn!("MMCancelTrigger: publish backward failed err={:#}", err);
                        }
                    }
                    chunk.groups.clear();
                };

                for (key, strategy_ids) in price_map_snapshot {
                    let price_qv = key.price_qv.to_quantized_value();
                    for strategy_id in strategy_ids {
                        let entry = MmCancelCandidateEntry::new(strategy_id, price_qv);
                        let next_len = 1 + chunk.next_encoded_len_with(&key.symbol, &entry);
                        if !chunk.is_empty() && next_len > SIGNAL_PAYLOAD {
                            flush_chunk(&mut chunk, &mut published_chunks, &mut published_items);
                        }
                        chunk.push_grouped(&key.symbol, entry);
                    }
                }
                flush_chunk(&mut chunk, &mut published_chunks, &mut published_items);
                debug!(
                    "MMCancelTrigger: dynamic index published chunks={} items={} indexed_strategies={} symbols={} sample={} strategies={} trigger_ts={} freq_ms={}",
                    published_chunks,
                    published_items,
                    indexed_strategy_count,
                    symbol_count,
                    symbol_sample,
                    strategy_sample,
                    trigger_ctx.trigger_ts,
                    trigger_ctx.freq_ms
                );
            }
            Err(err) => warn!("failed to decode MMCancelTrigger context: {err}"),
        },
        SignalType::MMCancel => match MmCancelCtx::from_bytes(signal.context.clone()) {
            Ok(mut cancel_ctx) => {
                if !is_mm_mode {
                    debug!("MMCancel ignored: pre_trade is not in MM mode");
                    return;
                }
                let symbol = normalize_symbol_for_internal(&cancel_ctx.get_opening_symbol());
                let opening_venue = TradingVenue::from_u8(cancel_ctx.opening_leg.venue)
                    .unwrap_or(TradingVenue::BinanceMargin);

                let configured_open_venue = MonitorChannel::instance().open_venue();
                if opening_venue != configured_open_venue {
                    info!(
                        "MMCancel: signal venue mismatch, configured_open={:?} but got {:?}, ignore",
                        configured_open_venue, opening_venue
                    );
                    return;
                }

                cancel_ctx.set_opening_symbol(&symbol);
                let normalized_signal = TradeSignal::create(
                    SignalType::MMCancel,
                    signal.generation_time,
                    signal.handle_time,
                    cancel_ctx.to_bytes(),
                );
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                if cancel_ctx.strategy_id > 0 {
                    let strategy_id = cancel_ctx.strategy_id;
                    let exists = { strategy_mgr.borrow().contains(strategy_id) };
                    if !exists {
                        debug!(
                            "MMCancel: targeted strategy missing strategy_id={} symbol={} trigger_ts={}",
                            strategy_id,
                            symbol,
                            cancel_ctx.trigger_ts
                        );
                        return;
                    }
                    let strategy_opt = { strategy_mgr.borrow_mut().take(strategy_id) };
                    if let Some(mut strategy) = strategy_opt {
                        strategy.handle_signal(&normalized_signal);
                        if strategy.is_active() {
                            strategy_mgr.borrow_mut().insert(strategy);
                        }
                    }
                    return;
                }

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
                for strategy_id in candidate_ids {
                    let exists = { strategy_mgr.borrow().contains(strategy_id) };
                    if !exists {
                        continue;
                    }
                    let strategy_opt = { strategy_mgr.borrow_mut().take(strategy_id) };
                    if let Some(mut strategy) = strategy_opt {
                        strategy.handle_signal(&normalized_signal);
                        if strategy.is_active() {
                            strategy_mgr.borrow_mut().insert(strategy);
                        }
                    }
                }
                drop(strategy_mgr);
            }
            Err(err) => warn!("failed to decode MMCancel context: {err}"),
        },
        SignalType::MMHedge => match MmHedgeCtx::from_bytes(signal.context.clone()) {
            Ok(mut hedge_ctx) => {
                if !is_mm_mode {
                    debug!("MMHedge ignored: pre_trade is not in MM mode");
                    return;
                }
                let symbol = normalize_symbol_for_internal(&hedge_ctx.get_opening_symbol());
                if symbol.is_empty() {
                    warn!("MMHedge: empty symbol");
                    return;
                }
                hedge_ctx.set_opening_symbol(&symbol);
                let normalized_signal = TradeSignal::create(
                    SignalType::MMHedge,
                    signal.generation_time,
                    signal.handle_time,
                    hedge_ctx.to_bytes(),
                );
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let strategy_id = strategy_mgr.borrow_mut().ensure_mm_hedge_strategy(&symbol);
                info!(
                    "MMHedge: received symbol={} strategy_id={} price_levels={} amount_levels={} next_query_ts={}",
                    symbol,
                    strategy_id,
                    hedge_ctx.price_qv_list.len(),
                    hedge_ctx.amount_qv_list.len(),
                    hedge_ctx.next_query_ts
                );
                let strategy_opt = { strategy_mgr.borrow_mut().take(strategy_id) };
                if let Some(mut strategy) = strategy_opt {
                    strategy.handle_signal(&normalized_signal);
                    if strategy.is_active() {
                        strategy_mgr.borrow_mut().insert(strategy);
                    } else {
                        debug!("MMHedge: strategy inactive id={}", strategy_id);
                    }
                }
            }
            Err(err) => warn!("failed to decode MMHedge context: {err}"),
        },
    }
}
