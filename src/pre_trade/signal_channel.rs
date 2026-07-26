use crate::pre_trade::account_open_block::{check_account_open_block, AccountOpenBlockReason};
use crate::pre_trade::binance_fr_position_limit_guard::BinanceFrPositionLimitGuard;
use crate::pre_trade::binance_std_um_margin_guard::BinanceStdUmMarginGuard;
use crate::pre_trade::bitget_position_tier_guard::BitgetPositionTierGuard;
use crate::pre_trade::fr_position_concentration_guard::FrPositionConcentrationGuard;
use crate::pre_trade::gate_fr_risk_limit_guard::GateFrRiskLimitGuard;
use crate::pre_trade::intra_unimmr_open_lock::IntraUnimmrOpenLock;
use crate::pre_trade::leverage_guard::LeverageGuard;
use crate::pre_trade::log_throttle::{log_pending_limit_summary, log_strategy_inactive_summary};
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::Side;
use crate::pre_trade::signal_throttle::{
    check_account_signal_throttle, check_signal_throttle,
    SIGNAL_THROTTLE_ERROR_CODE_BITGET_POSITION_TIER_LIMIT,
};
use crate::pre_trade::taker_decision_model::{
    PreTradeTakerDecisionModel, TakerDecisionOpenGateSnapshot,
};
use crate::strategy::arb_close_strategy::ArbCloseStrategy;
use crate::strategy::arb_hedge_strategy::ArbHedgeStrategy;
use crate::strategy::arb_open_strategy::ArbOpenStrategy;
use crate::strategy::mm_hedge_strategy::MarketMakerHedgeStrategy;
use crate::strategy::mm_open_strategy::MarketMakerOpenStrategy;
use crate::strategy::open_strategy_common::OpenStrategyCommon;
use crate::strategy::{Strategy, StrategyManager};
use anyhow::Result;
use bytes::BytesMut;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use ipc_common::iceoryx_publisher::{SignalPublisher, TradeSignalIpcPayload, SIGNAL_PAYLOAD};
use log::{debug, info, warn};
use order_common::TradingVenue;
use rolling_common::arb_open_latency::record_arb_open_latency;
use runtime_common::ipc_service_name::build_service_name;
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;
use signal_common::arb_signal::{
    ArbCancelCandidateEntry, ArbCancelCandidateQueryMsg, ArbCancelTriggerCtx,
};
use signal_common::cancel_signal::{ArbCancelCtx, MmCancelCtx};
use signal_common::common::bytes_helper;
use signal_common::hedge_signal::{ArbHedgeCtx, MmHedgeCtx};
use signal_common::mm_signal::{
    MmCancelCandidateEntry, MmCancelCandidateQueryMsg, MmCancelTriggerCtx,
};
use signal_common::open_signal::{ArbOpenCtxView, MmOpenCtxView};
use signal_common::trade_signal::{SignalType, TradeSignalView};
use std::borrow::Cow;
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;

thread_local! {
    static SIGNAL_CHANNEL: OnceCell<SignalChannel> = const { OnceCell::new() };
    static SIGNAL_COUNTS: RefCell<[u64; SIGNAL_COUNT_BUCKETS]> = const { RefCell::new([0; SIGNAL_COUNT_BUCKETS]) };
    static CANCEL_CANDIDATE_IDS: RefCell<Vec<i32>> = const { RefCell::new(Vec::new()) };
}

/// 默认信号频道名称（与 trade_signal 的发布频道一致）
pub const DEFAULT_SIGNAL_CHANNEL: &str = "trade_signal";

/// 默认反向信号频道名称
pub const DEFAULT_BACKWARD_CHANNEL: &str = "trade_query";

const ARB_CLOSE_MIN_NOTIONAL_U: f64 = 25.0;
const TAKER_DECISION_MODEL_OPEN_GATE_LOG_INTERVAL_US: i64 = 20_000_000;
const SIGNAL_COUNT_BUCKETS: usize = 14;

fn normalize_fixed_symbol_for_internal(symbol: &[u8; 32]) -> String {
    normalize_fixed_symbol_for_internal_cow(symbol).into_owned()
}

fn normalize_fixed_symbol_for_internal_cow(symbol: &[u8; 32]) -> Cow<'_, str> {
    let end = bytes_helper::fixed_bytes_len(symbol);
    let raw = &symbol[..end];
    if !raw.is_ascii() {
        return Cow::Owned(
            std::str::from_utf8(raw)
                .map(normalize_symbol_for_internal)
                .unwrap_or_default(),
        );
    }

    let start = raw
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(end);
    let stop = raw
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map(|idx| idx + 1)
        .unwrap_or(start);
    let raw = &raw[start..stop];
    if is_internal_symbol_bytes(raw) {
        return Cow::Borrowed(std::str::from_utf8(raw).unwrap_or_default());
    }

    let mut out = Vec::with_capacity(raw.len());
    for &byte in raw {
        match byte {
            b'-' | b'_' | b'/' => {}
            b'a'..=b'z' => out.push(byte - 32),
            _ => out.push(byte),
        }
    }
    if out.ends_with(b"SWAP") {
        out.truncate(out.len().saturating_sub(4));
    }
    Cow::Owned(String::from_utf8(out).unwrap_or_default())
}

fn is_internal_symbol_bytes(symbol: &[u8]) -> bool {
    !symbol.is_empty()
        && !symbol.ends_with(b"SWAP")
        && symbol
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

#[derive(Clone, Copy, Debug)]
pub struct OpenSignalDropReason {
    pub source: &'static str,
    pub elapsed_us: i64,
    pub threshold_us: i64,
}

#[derive(Debug, Clone)]
struct TakerDecisionOpenGateLogState {
    last_log_ts_us: i64,
    suppressed: usize,
    last_snapshot: TakerDecisionOpenGateSnapshot,
    last_side: Side,
    last_qty: f64,
}

thread_local! {
    static TAKER_DECISION_OPEN_GATE_LOGS: RefCell<HashMap<String, TakerDecisionOpenGateLogState>> =
        RefCell::new(HashMap::new());
}

fn arb_close_side_matches_open_position(close_side: Side, opening_pos: f64) -> bool {
    match close_side {
        Side::Sell => opening_pos > 0.0,
        Side::Buy => opening_pos < 0.0,
    }
}

fn arb_close_notional_meets_min_view(ctx: &ArbOpenCtxView<'_>) -> bool {
    let notional = ctx.amount_value() * ctx.price_value();
    notional.is_finite() && notional >= ARB_CLOSE_MIN_NOTIONAL_U
}

fn signed_qty_from_side(side: Side, qty: f64) -> f64 {
    match side {
        Side::Buy => qty.abs(),
        Side::Sell => -qty.abs(),
    }
}

fn is_position_reducing(current_qty: f64, add_qty: f64) -> bool {
    const EPS: f64 = 1e-12;
    if current_qty.abs() <= EPS || add_qty.abs() <= EPS {
        return false;
    }
    current_qty.signum() != add_qty.signum() && add_qty.abs() <= current_qty.abs() + EPS
}

fn arb_open_is_account_throttle_reducing(
    opening_symbol: &str,
    opening_venue: TradingVenue,
    hedging_symbol: &str,
    hedging_venue: TradingVenue,
    side: Side,
    qty: f64,
) -> bool {
    if !(qty.is_finite() && qty > 0.0) {
        return false;
    }

    let monitor = MonitorChannel::instance();
    let open_order_base_qty = monitor
        .qty_to_base(opening_venue, opening_symbol, qty)
        .abs();
    let hedge_order_base_qty = monitor
        .qty_to_base(hedging_venue, hedging_symbol, qty)
        .abs();
    if !(open_order_base_qty.is_finite()
        && open_order_base_qty > 0.0
        && hedge_order_base_qty.is_finite()
        && hedge_order_base_qty > 0.0)
    {
        return false;
    }

    let open_add_qty = signed_qty_from_side(side, open_order_base_qty);
    let hedge_side = match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    };
    let hedge_add_qty = signed_qty_from_side(hedge_side, hedge_order_base_qty);

    let open_pos = monitor.get_position_qty(opening_symbol, opening_venue);
    let hedge_pos = monitor.get_position_qty(hedging_symbol, hedging_venue);

    is_position_reducing(open_pos, open_add_qty) && is_position_reducing(hedge_pos, hedge_add_qty)
}

fn arb_open_is_bitget_margin_lock_spot_deleverage(
    opening_symbol: &str,
    opening_venue: TradingVenue,
    hedging_symbol: &str,
    hedging_venue: TradingVenue,
    side: Side,
    qty: f64,
) -> bool {
    opening_venue == TradingVenue::BitgetMargin
        && hedging_venue == TradingVenue::BitgetFutures
        && side == Side::Sell
        && arb_open_is_account_throttle_reducing(
            opening_symbol,
            opening_venue,
            hedging_symbol,
            hedging_venue,
            side,
            qty,
        )
}

fn arb_open_is_binance_std_um_margin_reducing(
    hedging_symbol: &str,
    hedging_venue: TradingVenue,
    open_side: Side,
    qty: f64,
) -> bool {
    if open_side != Side::Sell || !(qty.is_finite() && qty > 0.0) {
        return false;
    }

    let monitor = MonitorChannel::instance();
    let hedge_order_base_qty = monitor
        .qty_to_base(hedging_venue, hedging_symbol, qty)
        .abs();
    if !(hedge_order_base_qty.is_finite() && hedge_order_base_qty > 0.0) {
        return false;
    }

    let hedge_pos = monitor.get_position_qty(hedging_symbol, hedging_venue);
    is_position_reducing(
        hedge_pos,
        signed_qty_from_side(Side::Buy, hedge_order_base_qty),
    )
}

fn should_drop_startup_buffered_signal(generation_time: i64, listener_start_us: i64) -> bool {
    generation_time > 0 && generation_time < listener_start_us
}

fn is_open_signal_type(signal_type: &SignalType) -> bool {
    matches!(signal_type, SignalType::ArbOpen | SignalType::MMOpen)
}

fn should_drop_open_signal_for_slow_round(
    signal_type: &SignalType,
    reason: Option<OpenSignalDropReason>,
) -> bool {
    reason.is_some() && is_open_signal_type(signal_type)
}

fn log_taker_decision_open_gate_block(
    snapshot: TakerDecisionOpenGateSnapshot,
    side: Side,
    qty: f64,
) {
    let now_us = get_timestamp_us();
    let key = format!("{}|{}", snapshot.symbol, snapshot.note);
    TAKER_DECISION_OPEN_GATE_LOGS.with(|logs| {
        let mut logs = logs.borrow_mut();
        let state = logs
            .entry(key)
            .or_insert_with(|| TakerDecisionOpenGateLogState {
                last_log_ts_us: 0,
                suppressed: 0,
                last_snapshot: snapshot.clone(),
                last_side: side,
                last_qty: qty,
            });
        state.suppressed += 1;
        state.last_snapshot = snapshot;
        state.last_side = side;
        state.last_qty = qty;
        if state.last_log_ts_us == 0
            || now_us.saturating_sub(state.last_log_ts_us)
                >= TAKER_DECISION_MODEL_OPEN_GATE_LOG_INTERVAL_US
        {
            info!(
                "ArbOpen blocked by taker decision model gate: symbol={} side={} qty={:.8} note={} updates={} score={:?} q={:?} suppressed={}",
                state.last_snapshot.symbol,
                state.last_side.as_str(),
                state.last_qty,
                state.last_snapshot.note,
                state.last_snapshot.update_count,
                state.last_snapshot.score,
                state.last_snapshot.percentile,
                state.suppressed
            );
            state.last_log_ts_us = now_us;
            state.suppressed = 0;
        }
    });
}

fn should_suppress_arb_open_inactive_warning(reason: &str) -> bool {
    reason.starts_with("open order rate limit triggered:")
        || reason.starts_with("pending limit order risk failed:")
        || reason.starts_with("INTRA_NO_BORROW 余额不足")
        || reason.starts_with("STANDARD 余额不足")
}

pub fn take_signal_counts() -> HashMap<String, u64> {
    SIGNAL_COUNTS.with(|counts| {
        let mut counts = counts.borrow_mut();
        let mut snapshot = HashMap::new();
        for signal_type in [
            SignalType::ArbOpen,
            SignalType::ArbCancel,
            SignalType::ArbClose,
            SignalType::MMOpen,
            SignalType::MMCancel,
            SignalType::MMHedge,
            SignalType::MMCancelTrigger,
            SignalType::ArbCancelTrigger,
            SignalType::ArbHedge,
        ] {
            let idx = signal_count_index(&signal_type);
            let count = counts[idx];
            if count > 0 {
                snapshot.insert(signal_type.as_str().to_string(), count);
                counts[idx] = 0;
            }
        }
        snapshot
    })
}

fn record_signal_count(signal_type: &SignalType) {
    SIGNAL_COUNTS.with(|counts| {
        counts.borrow_mut()[signal_count_index(signal_type)] += 1;
    });
}

fn signal_count_index(signal_type: &SignalType) -> usize {
    match signal_type {
        SignalType::ArbOpen => 1,
        SignalType::ArbCancel => 3,
        SignalType::ArbClose => 4,
        SignalType::MMOpen => 5,
        SignalType::MMCancel => 6,
        SignalType::MMHedge => 7,
        SignalType::MMCancelTrigger => 8,
        SignalType::ArbCancelTrigger => 9,
        SignalType::ArbHedge => 10,
    }
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
    listener: RefCell<SignalListener>,
}

struct SignalListener {
    channel_name: String,
    listener_start_us: i64,
    dropped_startup_buffered: usize,
    dropped_slow_round_open: usize,
    _node: Node<ipc::Service>,
    subscriber: Subscriber<ipc::Service, TradeSignalIpcPayload, ()>,
}

impl SignalListener {
    fn new(channel_name: &str) -> Result<Self> {
        let listener_start_us = get_timestamp_us();
        let node_name = SignalChannel::signal_node_name(channel_name);
        let service_path = build_service_name(&format!("signal_pubs/{}", channel_name));

        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;

        let service = node
            .service_builder(&ServiceName::new(&service_path)?)
            .publish_subscribe::<TradeSignalIpcPayload>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .create()?;

        let subscriber: Subscriber<ipc::Service, TradeSignalIpcPayload, ()> =
            service.subscriber_builder().create()?;

        info!(
            "signal subscribed: node={} service={} channel={}",
            node_name,
            service.name(),
            channel_name
        );

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

        Ok(Self {
            channel_name: channel_name.to_string(),
            listener_start_us,
            dropped_startup_buffered: 0,
            dropped_slow_round_open: 0,
            _node: node,
            subscriber,
        })
    }

    fn drain_pending(&mut self, open_drop_reason: Option<OpenSignalDropReason>) -> bool {
        self.drain_pending_limit(open_drop_reason, usize::MAX).0
    }

    fn drain_pending_limit(
        &mut self,
        open_drop_reason: Option<OpenSignalDropReason>,
        max_messages: usize,
    ) -> (bool, bool) {
        let mut has_message = false;
        let mut received = 0usize;
        while received < max_messages {
            match self.subscriber.receive() {
                Ok(Some(sample)) => {
                    received += 1;
                    has_message = true;
                    let receive_us = get_timestamp_us();
                    let ipc_payload = sample.payload();
                    let Some(payload) = ipc_payload.as_signal_slice() else {
                        warn!(
                            "failed to decode trade signal from channel {}: invalid ipc payload",
                            self.channel_name
                        );
                        continue;
                    };
                    match TradeSignalView::from_exact_bytes(payload) {
                        Ok(signal) => {
                            if should_drop_startup_buffered_signal(
                                signal.generation_time,
                                self.listener_start_us,
                            ) {
                                self.dropped_startup_buffered += 1;
                                if self.dropped_startup_buffered <= 5
                                    || self.dropped_startup_buffered.is_multiple_of(100)
                                {
                                    info!(
                                        "signal channel {} dropped startup-buffered signal count={} type={:?} generation_time={} listener_start_us={}",
                                        self.channel_name,
                                        self.dropped_startup_buffered,
                                        signal.signal_type,
                                        signal.generation_time,
                                        self.listener_start_us
                                    );
                                }
                                continue;
                            }
                            if self.dropped_startup_buffered > 0 {
                                info!(
                                    "signal channel {} finished dropping startup-buffered signals count={} first_live_generation_time={}",
                                    self.channel_name,
                                    self.dropped_startup_buffered,
                                    signal.generation_time
                                );
                                self.dropped_startup_buffered = 0;
                            }
                            if should_drop_open_signal_for_slow_round(
                                &signal.signal_type,
                                open_drop_reason,
                            ) {
                                self.dropped_slow_round_open += 1;
                                if self.dropped_slow_round_open <= 5
                                    || self.dropped_slow_round_open.is_multiple_of(100)
                                {
                                    if let Some(reason) = open_drop_reason {
                                        warn!(
                                            "signal channel {} dropped slow-round open signal count={} type={} generation_time={} receive_lag_us={} source={} elapsed_us={} threshold_us={}",
                                            self.channel_name,
                                            self.dropped_slow_round_open,
                                            signal.signal_type.as_str(),
                                            signal.generation_time,
                                            receive_us.saturating_sub(signal.generation_time),
                                            reason.source,
                                            reason.elapsed_us,
                                            reason.threshold_us
                                        );
                                    }
                                }
                                continue;
                            }
                            record_signal_count(&signal.signal_type);
                            if matches!(signal.signal_type, SignalType::ArbOpen)
                                && signal.generation_time > 0
                            {
                                record_arb_open_latency(
                                    "pt_receive_minus_generation",
                                    receive_us.saturating_sub(signal.generation_time),
                                );
                            }
                            handle_trade_signal_view(signal, receive_us);
                        }
                        Err(err) => warn!(
                            "failed to decode trade signal from channel {}: {}",
                            self.channel_name, err
                        ),
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    warn!(
                        "signal receive error (channel={}): {err}",
                        self.channel_name
                    );
                    break;
                }
            }
        }
        (has_message, received >= max_messages)
    }
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

    /// 创建信号频道并订阅信号 IPC。
    ///
    /// # 参数
    /// * `channel_name` - 要订阅的信号频道名称
    /// * `backward_channel` - 反向通道名称（可选）
    fn new(channel_name: &str, backward_channel: Option<&str>) -> Result<Self> {
        // 创建反向发布器
        let backward_pub = if let Some(backward_ch) = backward_channel {
            Some(SignalPublisher::create(backward_ch)?)
        } else {
            None
        };

        let listener = RefCell::new(SignalListener::new(channel_name)?);

        Ok(Self {
            backward_pub,
            channel_name: channel_name.to_string(),
            listener,
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

    pub fn publish_backward_with<F>(&self, len: usize, write: F) -> Result<bool>
    where
        F: FnOnce(&mut [u8]),
    {
        if let Some(publisher) = &self.backward_pub {
            publisher.publish_with(len, write)?;
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

    pub fn drain_pending() -> bool {
        Self::drain_pending_with_open_drop(None)
    }

    pub fn drain_pending_with_open_drop(reason: Option<OpenSignalDropReason>) -> bool {
        Self::with(|ch| ch.listener.borrow_mut().drain_pending(reason))
    }

    pub fn drain_pending_with_open_drop_limit(
        reason: Option<OpenSignalDropReason>,
        max_messages: usize,
    ) -> (bool, bool) {
        Self::with(|ch| {
            ch.listener
                .borrow_mut()
                .drain_pending_limit(reason, max_messages)
        })
    }

    /// 生成信号节点名称
    fn signal_node_name(channel: &str) -> String {
        format!("pre_trade_signal_{}", channel)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        is_position_reducing, normalize_fixed_symbol_for_internal,
        normalize_fixed_symbol_for_internal_cow, should_drop_open_signal_for_slow_round,
        should_drop_startup_buffered_signal, should_suppress_arb_open_inactive_warning,
        OpenSignalDropReason,
    };
    use bytes::Bytes;
    use signal_common::trade_signal::{SignalType, TradeSignal};
    use std::borrow::Cow;

    fn fixed_symbol(value: &str) -> [u8; 32] {
        let mut out = [0u8; 32];
        let bytes = value.as_bytes();
        out[..bytes.len().min(32)].copy_from_slice(&bytes[..bytes.len().min(32)]);
        out
    }

    #[test]
    fn normalizes_fixed_ascii_symbols_without_general_string_path() {
        assert_eq!(
            normalize_fixed_symbol_for_internal(&fixed_symbol("BTCUSDT")),
            "BTCUSDT"
        );
        assert_eq!(
            normalize_fixed_symbol_for_internal(&fixed_symbol("btc-usdt-swap")),
            "BTCUSDT"
        );
        assert_eq!(
            normalize_fixed_symbol_for_internal(&fixed_symbol(" eth_usdt ")),
            "ETHUSDT"
        );
    }

    #[test]
    fn normalize_fixed_symbol_handles_empty_or_blank_values() {
        assert_eq!(normalize_fixed_symbol_for_internal(&[0u8; 32]), "");
        assert_eq!(
            normalize_fixed_symbol_for_internal(&fixed_symbol("   ")),
            ""
        );
    }

    #[test]
    fn normalize_fixed_symbol_borrows_internal_symbol() {
        let internal = fixed_symbol("BTCUSDT");
        let normalized = normalize_fixed_symbol_for_internal_cow(&internal);
        assert!(matches!(normalized, Cow::Borrowed("BTCUSDT")));

        let exchange_format = fixed_symbol("btc-usdt-swap");
        let normalized = normalize_fixed_symbol_for_internal_cow(&exchange_format);
        assert!(matches!(
            normalized,
            Cow::Owned(ref symbol) if symbol == "BTCUSDT"
        ));
    }

    #[test]
    fn position_reducing_allows_smaller_abs_position() {
        assert!(is_position_reducing(10.0, -2.0));
        assert!(is_position_reducing(-10.0, 2.0));
        assert!(is_position_reducing(10.0, -10.0));
    }

    #[test]
    fn position_reducing_rejects_larger_or_reversing_position() {
        assert!(!is_position_reducing(10.0, 2.0));
        assert!(!is_position_reducing(-10.0, -2.0));
        assert!(!is_position_reducing(1.0, -2.0));
        assert!(!is_position_reducing(0.0, 1.0));
    }

    #[test]
    fn startup_filter_drops_older_signals() {
        let signal = TradeSignal::create(SignalType::MMOpen, 999, 0.0, Bytes::new());
        assert!(should_drop_startup_buffered_signal(
            signal.generation_time,
            1_000
        ));
    }

    #[test]
    fn startup_filter_keeps_signals_from_current_generation_onward() {
        let fresh = TradeSignal::create(SignalType::MMOpen, 1_000, 0.0, Bytes::new());
        let newer = TradeSignal::create(SignalType::MMOpen, 1_001, 0.0, Bytes::new());
        let missing_ts = TradeSignal::create(SignalType::MMOpen, 0, 0.0, Bytes::new());
        assert!(!should_drop_startup_buffered_signal(
            fresh.generation_time,
            1_000
        ));
        assert!(!should_drop_startup_buffered_signal(
            newer.generation_time,
            1_000
        ));
        assert!(!should_drop_startup_buffered_signal(
            missing_ts.generation_time,
            1_000
        ));
    }

    #[test]
    fn slow_round_filter_only_drops_open_signals() {
        let reason = Some(OpenSignalDropReason {
            source: "test",
            elapsed_us: 2_000,
            threshold_us: 1_000,
        });
        assert!(should_drop_open_signal_for_slow_round(
            &SignalType::ArbOpen,
            reason
        ));
        assert!(should_drop_open_signal_for_slow_round(
            &SignalType::MMOpen,
            reason
        ));
        assert!(!should_drop_open_signal_for_slow_round(
            &SignalType::ArbClose,
            reason
        ));
        assert!(!should_drop_open_signal_for_slow_round(
            &SignalType::ArbCancel,
            None
        ));
    }

    #[test]
    fn suppresses_expected_arb_open_inactive_risk_noise() {
        assert!(should_suppress_arb_open_inactive_warning(
            "pending limit order risk failed: symbol=AVAUSDT side=BUY 当前限价挂单数=5，达到方向上限 5"
        ));
        assert!(should_suppress_arb_open_inactive_warning(
            "open order rate limit triggered: symbol=AVAUSDT"
        ));
        assert!(!should_suppress_arb_open_inactive_warning(
            "decode ArbOpen failed: broken payload"
        ));
    }
}

fn handle_trade_signal_view(signal: TradeSignalView<'_>, receive_us: i64) {
    match signal.signal_type {
        SignalType::ArbOpen => handle_arb_open_signal_view(signal, receive_us),
        SignalType::MMOpen => handle_mm_open_signal_view(signal, receive_us),
        _ => handle_trade_signal(signal, receive_us),
    }
}

fn handle_arb_open_signal_view(signal: TradeSignalView<'_>, receive_us: i64) {
    if should_block_arb_signal_for_startup_net_gate(&signal.signal_type) {
        return;
    }

    match ArbOpenCtxView::from_bytes(signal.context) {
        Ok(open_ctx) => {
            let handle_start_us = get_timestamp_us();
            let symbol = normalize_fixed_symbol_for_internal_cow(&open_ctx.opening_symbol);
            if symbol.is_empty() {
                warn!("ArbOpen: empty symbol");
                return;
            }
            let hedging_symbol = normalize_fixed_symbol_for_internal_cow(&open_ctx.hedging_symbol);
            let Some(side) = open_ctx.get_side() else {
                warn!("ArbOpen: invalid side {}", open_ctx.side);
                return;
            };
            let symbol_throttle_hit = check_signal_throttle(&symbol, side);
            let account_throttle_hit = check_account_signal_throttle();
            let account_open_block_hit = check_account_open_block();
            if let Some(hit) = symbol_throttle_hit.as_ref().filter(|hit| {
                hit.last_error_code == SIGNAL_THROTTLE_ERROR_CODE_BITGET_POSITION_TIER_LIMIT
            }) {
                debug!(
                    "ArbOpen: blocked by Bitget position tier symbol-side lock, symbol={} side={} remain_us={} last_code={} until_us={}, skip strategy construction",
                    symbol,
                    side.as_str(),
                    hit.remaining_us,
                    hit.last_error_code,
                    hit.until_us
                );
                return;
            }
            if account_throttle_hit.is_none() && account_open_block_hit.is_none() {
                if let Some(hit) = symbol_throttle_hit.as_ref() {
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
            }
            let opening_venue = TradingVenue::from_u8(open_ctx.opening_leg.venue)
                .unwrap_or(TradingVenue::BinanceMargin);
            let hedging_venue = TradingVenue::from_u8(open_ctx.hedging_leg.venue)
                .unwrap_or(TradingVenue::BinanceFutures);

            let configured_open_venue = MonitorChannel::instance().open_venue();
            let configured_hedge_venue = MonitorChannel::instance().hedge_venue();
            if opening_venue != configured_open_venue || hedging_venue != configured_hedge_venue {
                warn!(
                    "ArbOpen: signal venue mismatch, configured_open={:?} configured_hedge={:?} but got open={:?} hedge={:?}, ignore",
                    configured_open_venue, configured_hedge_venue, opening_venue, hedging_venue
                );
                return;
            }

            if IntraUnimmrOpenLock::is_locked()
                && !arb_open_is_account_throttle_reducing(
                    &symbol,
                    opening_venue,
                    &hedging_symbol,
                    hedging_venue,
                    side,
                    open_ctx.amount_value(),
                )
            {
                info!(
                    "ArbOpen blocked by intra UniMMR reduce-only lock: symbol={} side={} open_venue={:?} hedge_venue={:?} qty={:.8}",
                    symbol,
                    side.as_str(),
                    opening_venue,
                    hedging_venue,
                    open_ctx.amount_value()
                );
                return;
            }

            if BinanceStdUmMarginGuard::is_enabled() {
                let margin_guard_reducing = arb_open_is_binance_std_um_margin_reducing(
                    &hedging_symbol,
                    hedging_venue,
                    side,
                    open_ctx.amount_value(),
                );
                if BinanceStdUmMarginGuard::should_block_arb_open(
                    &symbol,
                    side,
                    margin_guard_reducing,
                ) {
                    return;
                }
            }

            let leverage_guard_reducing = arb_open_is_account_throttle_reducing(
                &symbol,
                opening_venue,
                &hedging_symbol,
                hedging_venue,
                side,
                open_ctx.amount_value(),
            );
            if FrPositionConcentrationGuard::should_block_arb_open(&symbol, leverage_guard_reducing)
            {
                return;
            }
            if !leverage_guard_reducing
                && LeverageGuard::should_block_arb_open(
                    &symbol,
                    opening_venue,
                    &hedging_symbol,
                    hedging_venue,
                )
            {
                return;
            }

            if GateFrRiskLimitGuard::should_block_arb_open(
                &symbol,
                opening_venue,
                &hedging_symbol,
                hedging_venue,
            ) {
                debug!(
                    "ArbOpen: blocked by Gate risk-limit snapshot guard, symbol={} hedge_symbol={} open_venue={:?} hedge_venue={:?}",
                    symbol, hedging_symbol, opening_venue, hedging_venue
                );
                return;
            }

            if BinanceFrPositionLimitGuard::should_block_arb_open(
                &symbol,
                opening_venue,
                &hedging_symbol,
                hedging_venue,
            ) {
                debug!(
                    "ArbOpen: blocked by Binance FR position-limit snapshot guard, symbol={} hedge_symbol={} open_venue={:?} hedge_venue={:?}",
                    symbol, hedging_symbol, opening_venue, hedging_venue
                );
                return;
            }

            if BitgetPositionTierGuard::should_block_arb_open(
                &symbol,
                opening_venue,
                &hedging_symbol,
                hedging_venue,
                leverage_guard_reducing,
            ) {
                debug!(
                    "ArbOpen: blocked by Bitget position-tier snapshot guard, symbol={} hedge_symbol={} open_venue={:?} hedge_venue={:?}",
                    symbol, hedging_symbol, opening_venue, hedging_venue
                );
                return;
            }

            let bitget_margin_lock_spot_deleverage =
                account_open_block_hit.as_ref().is_some_and(|hit| {
                    hit.reason == AccountOpenBlockReason::BitgetUnifiedInsufficientMargin
                        && arb_open_is_bitget_margin_lock_spot_deleverage(
                            &symbol,
                            opening_venue,
                            &hedging_symbol,
                            hedging_venue,
                            side,
                            open_ctx.amount_value(),
                        )
                });
            if let Some(hit) = account_open_block_hit.as_ref() {
                let reducing =
                    if hit.reason == AccountOpenBlockReason::BitgetUnifiedInsufficientMargin {
                        bitget_margin_lock_spot_deleverage
                    } else {
                        hit.allows_reducing_open()
                            && arb_open_is_account_throttle_reducing(
                                &symbol,
                                opening_venue,
                                &hedging_symbol,
                                hedging_venue,
                                side,
                                open_ctx.amount_value(),
                            )
                    };
                if !reducing {
                    debug!(
                        "ArbOpen: account-wide open block, reason={} symbol={} side={} open_venue={:?} hedge_venue={:?} qty={:.8} first_seen_us={} updated_at_us={} last_code={}, skip strategy construction",
                        hit.reason.as_str(),
                        symbol,
                        side.as_str(),
                        opening_venue,
                        hedging_venue,
                        open_ctx.amount_value(),
                        hit.first_seen_us,
                        hit.updated_at_us,
                        hit.last_error_code
                    );
                    return;
                }
                debug!(
                    "ArbOpen: account-wide open block active but signal is reducing, reason={} symbol={} side={} open_venue={:?} hedge_venue={:?} qty={:.8} last_code={}",
                    hit.reason.as_str(),
                    symbol,
                    side.as_str(),
                    opening_venue,
                    hedging_venue,
                    open_ctx.amount_value(),
                    hit.last_error_code
                );
            }

            if let Some(hit) = account_throttle_hit.as_ref() {
                let reducing = arb_open_is_account_throttle_reducing(
                    &symbol,
                    opening_venue,
                    &hedging_symbol,
                    hedging_venue,
                    side,
                    open_ctx.amount_value(),
                );
                if !reducing {
                    debug!(
                        "ArbOpen: account-wide reduce-only throttle, symbol={} side={} open_venue={:?} hedge_venue={:?} qty={:.8} remain_us={} last_code={} until_us={}, skip strategy construction",
                        symbol,
                        side.as_str(),
                        opening_venue,
                        hedging_venue,
                        open_ctx.amount_value(),
                        hit.remaining_us,
                        hit.last_error_code,
                        hit.until_us
                    );
                    return;
                }
                debug!(
                    "ArbOpen: account-wide throttle active but signal is reducing, symbol={} side={} open_venue={:?} hedge_venue={:?} qty={:.8} remain_us={} last_code={}",
                    symbol,
                    side.as_str(),
                    opening_venue,
                    hedging_venue,
                    open_ctx.amount_value(),
                    hit.remaining_us,
                    hit.last_error_code
                );
            }

            if let Some(gate) = PreTradeTakerDecisionModel::arb_open_gate_global(&symbol) {
                if !gate.allowed {
                    log_taker_decision_open_gate_block(gate, side, open_ctx.amount_value());
                    return;
                }
            }

            let mut pending_limit_prechecked = false;
            match open_ctx.get_order_type() {
                Some(order_type) => {
                    if order_type.is_limit() {
                        if let Err(e) = MonitorChannel::instance()
                            .check_pending_limit_order_for_arb(&symbol, side)
                        {
                            log_pending_limit_summary("ArbOpen", None, &symbol, side, &e);
                            return;
                        }
                        pending_limit_prechecked = true;
                    }
                }
                None => {
                    warn!("ArbOpen: invalid order_type {}", open_ctx.order_type);
                    return;
                }
            }
            let signal_price = open_ctx.price_value();
            let signal_amount = open_ctx.amount_value();
            let signal_spread_rate = open_ctx.spread_rate;
            let strategy_mgr = MonitorChannel::instance().strategy_mgr();
            {
                let mut mgr = strategy_mgr.borrow_mut();
                let _ = mgr.ensure_arb_hedge_strategy_for_normalized_symbol(&symbol);
            }
            let strategy_id = StrategyManager::generate_strategy_id();
            let mut strategy = ArbOpenStrategy::new(strategy_id);
            strategy.handle_arb_open_view_with_symbol(
                open_ctx,
                symbol,
                pending_limit_prechecked,
                bitget_margin_lock_spot_deleverage,
                receive_us,
                handle_start_us,
            );
            record_arb_open_latency(
                "pt_handle_strategy_total",
                get_timestamp_us().saturating_sub(handle_start_us),
            );
            if strategy.is_active() {
                let log_symbol = strategy.open_state().open_symbol.as_str();
                if log::log_enabled!(log::Level::Debug) {
                    let from_key = String::from_utf8_lossy(&strategy.open_state().from_key);
                    debug!(
                        "🔔 收到 ArbOpen 信号: opening={} {:?} side={:?} price={:.6} hedging={} {:?} | amount={:.4} spread_rate={:.6} from_key='{}'",
                        log_symbol,
                        opening_venue,
                        side,
                        signal_price,
                        hedging_symbol,
                        hedging_venue,
                        signal_amount,
                        signal_spread_rate,
                        from_key
                    );
                }
                debug!(
                    "✅ ArbOpenStrategy: strategy_id={} {} 已创建并激活",
                    strategy_id, log_symbol
                );
                strategy_mgr.borrow_mut().insert(Box::new(strategy));
            } else {
                let log_symbol = strategy.open_state().open_symbol.as_str();
                let log_symbol = if log_symbol.is_empty() {
                    "-"
                } else {
                    log_symbol
                };
                let reason = strategy
                    .open_strategy_inactive_reason()
                    .unwrap_or("unknown");
                if !should_suppress_arb_open_inactive_warning(reason) {
                    log_strategy_inactive_summary("ArbOpen", Some(strategy_id), log_symbol, reason);
                }
            }
        }
        Err(err) => warn!("failed to decode ArbOpen context: {err}"),
    }
}

fn handle_mm_open_signal_view(signal: TradeSignalView<'_>, _receive_us: i64) {
    if should_block_arb_signal_for_startup_net_gate(&signal.signal_type) {
        return;
    }

    let monitor = MonitorChannel::instance();
    if monitor.open_venue() != monitor.hedge_venue() {
        debug!("MMOpen ignored: pre_trade is not in MM mode");
        return;
    }

    let Ok(open_ctx) = MmOpenCtxView::from_bytes(signal.context) else {
        warn!("failed to decode MMOpen context");
        return;
    };
    let symbol = normalize_fixed_symbol_for_internal_cow(&open_ctx.opening_symbol);
    if symbol.is_empty() {
        warn!("MMOpen: empty symbol");
        return;
    }
    let Some(side) = open_ctx.get_side() else {
        warn!("MMOpen: invalid side {}", open_ctx.side);
        return;
    };
    if let Some(order_type) = open_ctx.get_order_type() {
        if order_type.is_limit() {
            if let Err(e) = MonitorChannel::instance().check_pending_limit_order(&symbol, side) {
                log_pending_limit_summary("MMOpen", None, &symbol, side, &e);
                return;
            }
        }
    } else {
        warn!("MMOpen: invalid order_type {}", open_ctx.order_type);
        return;
    }

    let strategy_mgr = MonitorChannel::instance().strategy_mgr();
    let _ = strategy_mgr
        .borrow_mut()
        .ensure_mm_hedge_strategy_for_normalized_symbol(&symbol);

    let strategy_id = StrategyManager::generate_strategy_id();
    let mut strategy = MarketMakerOpenStrategy::new(strategy_id);
    strategy.handle_mm_open_view_with_symbol(open_ctx, symbol);
    if strategy.is_active() {
        debug!("MMOpen: strategy activated id={}", strategy_id);
        strategy_mgr.borrow_mut().insert(Box::new(strategy));
    } else {
        info!("MMOpen: strategy_id={} 未激活", strategy_id);
    }
}

fn handle_trade_signal(signal: TradeSignalView<'_>, _receive_us: i64) {
    if should_block_arb_signal_for_startup_net_gate(&signal.signal_type) {
        return;
    }

    let current_is_mm_mode = || {
        let monitor = MonitorChannel::instance();
        monitor.open_venue() == monitor.hedge_venue()
    };

    match signal.signal_type {
        SignalType::ArbOpen | SignalType::MMOpen => {
            warn!(
                "{} reached generic signal handler; open signals must use view fast path",
                signal.signal_type.as_str()
            );
        }
        SignalType::ArbClose => {
            match ArbOpenCtxView::from_bytes(signal.context) {
                Ok(close_ctx_view) => {
                    let opening_symbol =
                        normalize_fixed_symbol_for_internal(&close_ctx_view.opening_symbol);
                    let hedging_symbol =
                        normalize_fixed_symbol_for_internal(&close_ctx_view.hedging_symbol);

                    // 获取平仓方向
                    let Some(close_side) = Side::from_u8(close_ctx_view.side) else {
                        warn!("ArbClose: invalid side {}", close_ctx_view.side);
                        return;
                    };

                    // 查询两条腿的持仓（带符号）
                    let Some(opening_venue) =
                        TradingVenue::from_u8(close_ctx_view.opening_leg.venue)
                    else {
                        warn!(
                            "ArbClose: invalid opening_venue {}",
                            close_ctx_view.opening_leg.venue
                        );
                        return;
                    };
                    let Some(hedging_venue) =
                        TradingVenue::from_u8(close_ctx_view.hedging_leg.venue)
                    else {
                        warn!(
                            "ArbClose: invalid hedging_venue {}",
                            close_ctx_view.hedging_leg.venue
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

                    if close_ctx_view.amount_value() <= 1e-12 || close_ctx_view.amount_count() <= 0
                    {
                        return;
                    }

                    let opening_pos =
                        MonitorChannel::instance().get_position_qty(&opening_symbol, opening_venue);
                    let hedging_pos =
                        MonitorChannel::instance().get_position_qty(&hedging_symbol, hedging_venue);
                    if !arb_close_side_matches_open_position(close_side, opening_pos) {
                        return;
                    }
                    if !arb_close_notional_meets_min_view(&close_ctx_view) {
                        return;
                    }
                    let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                    let close_price = close_ctx_view.price_value();

                    {
                        let _ = strategy_mgr
                            .borrow_mut()
                            .ensure_arb_hedge_strategy_for_normalized_symbol(&opening_symbol);
                    }

                    let strategy_id = StrategyManager::generate_strategy_id();
                    let mut strategy = ArbCloseStrategy::new(strategy_id);
                    strategy.handle_arb_close_view_with_symbols(
                        close_ctx_view,
                        opening_symbol.as_str(),
                        hedging_symbol.as_str(),
                    );
                    if strategy.is_active() {
                        debug!(
                            "🔔 收到 ArbClose 信号: opening={} {:?} hedging={} {:?} | side={:?} open_pos={:.4} hedge_pos={:.4} price={:.6}",
                            opening_symbol,
                            opening_venue,
                            hedging_symbol,
                            hedging_venue,
                            close_side,
                            opening_pos,
                            hedging_pos,
                            close_price
                        );
                        strategy_mgr.borrow_mut().insert(Box::new(strategy));
                    }
                }
                Err(err) => warn!("failed to decode ArbClose context: {err}"),
            }
        }

        SignalType::ArbCancel => match ArbCancelCtx::from_slice(signal.context) {
            Ok(mut cancel_ctx) => {
                let symbol = normalize_fixed_symbol_for_internal(&cancel_ctx.opening_symbol);
                let hedging_symbol =
                    normalize_fixed_symbol_for_internal(&cancel_ctx.hedging_symbol);
                let cancel_side = cancel_ctx.get_side();
                let cancel_reason = cancel_ctx.get_reason();
                let require_direction_match = matches!(
                    cancel_reason,
                    signal_common::cancel_signal::ArbCancelReason::Spread
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
                        configured_open_venue, configured_hedge_venue, opening_venue, hedging_venue
                    );
                    return;
                }

                cancel_ctx.set_opening_symbol(&symbol);
                cancel_ctx.set_hedging_symbol(&hedging_symbol);
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();

                if cancel_ctx.strategy_id > 0 {
                    let strategy_id = cancel_ctx.strategy_id;
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
                        if let Some(arb_open) =
                            strategy.as_any_mut().downcast_mut::<ArbOpenStrategy>()
                        {
                            arb_open.handle_arb_cancel_ctx(&cancel_ctx);
                        } else {
                            warn!(
                                "ArbCancel: target strategy type mismatch strategy_id={}",
                                strategy_id
                            );
                        }
                        if strategy.is_active() {
                            strategy_mgr.borrow_mut().insert(strategy);
                        }
                    } else {
                        debug!(
                            "ArbCancel: targeted strategy missing strategy_id={} symbol={} trigger_ts={}",
                            strategy_id, symbol, cancel_ctx.trigger_ts
                        );
                    }
                    return;
                }

                CANCEL_CANDIDATE_IDS.with(|ids| {
                    let mut ids = ids.borrow_mut();
                    strategy_mgr
                        .borrow()
                        .copy_ids_for_normalized_symbol_into(&symbol, &mut ids);
                    for strategy_id in ids.iter().copied() {
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
                            if let Some(arb_open) =
                                strategy.as_any_mut().downcast_mut::<ArbOpenStrategy>()
                            {
                                arb_open.handle_arb_cancel_ctx(&cancel_ctx);
                            } else {
                                warn!(
                                    "ArbCancel: target strategy type mismatch strategy_id={}",
                                    strategy_id
                                );
                            }
                            if strategy.is_active() {
                                strategy_mgr.borrow_mut().insert(strategy);
                            }
                        }
                    }
                });
                drop(strategy_mgr);
            }
            Err(err) => warn!("failed to decode ArbCancel context: {err}"),
        },
        SignalType::ArbCancelTrigger => match ArbCancelTriggerCtx::from_slice(signal.context) {
            Ok(trigger_ctx) => {
                if current_is_mm_mode() {
                    debug!("ArbCancelTrigger ignored: pre_trade is in MM mode");
                    return;
                }

                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let price_map_snapshot = strategy_mgr.borrow().arb_open_price_map_snapshot();
                if price_map_snapshot.is_empty() {
                    return;
                }

                let debug_stats = if log::log_enabled!(log::Level::Debug) {
                    let mut symbol_counts: HashMap<String, usize> = HashMap::new();
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
                    Some((
                        indexed_strategy_count,
                        symbol_count,
                        symbol_sample,
                        strategy_sample,
                    ))
                } else {
                    None
                };

                let mut chunk = ArbCancelCandidateQueryMsg::new(trigger_ctx.trigger_ts);
                let mut payload = BytesMut::with_capacity(SIGNAL_PAYLOAD);
                let mut published_chunks = 0usize;
                let mut published_items = 0usize;

                let flush_chunk = |chunk: &mut ArbCancelCandidateQueryMsg,
                                   payload: &mut BytesMut,
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
                    payload.clear();
                    chunk.write_backward_to(payload);
                    match SignalChannel::with(|ch| ch.publish_backward(payload.as_ref())) {
                        Ok(true) => {
                            *published_chunks += 1;
                            *published_items += item_count;
                        }
                        Ok(false) => {
                            warn!("ArbCancelTrigger: backward publisher unavailable");
                        }
                        Err(err) => {
                            warn!("ArbCancelTrigger: publish backward failed err={:#}", err);
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
                                &mut payload,
                                &mut published_chunks,
                                &mut published_items,
                            );
                        }
                        chunk.push_grouped(&key.symbol, entry);
                    }
                }
                flush_chunk(
                    &mut chunk,
                    &mut payload,
                    &mut published_chunks,
                    &mut published_items,
                );
                if let Some((
                    indexed_strategy_count,
                    symbol_count,
                    symbol_sample,
                    strategy_sample,
                )) = debug_stats
                {
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
            }
            Err(err) => warn!("failed to decode ArbCancelTrigger context: {err}"),
        },
        SignalType::ArbHedge => match ArbHedgeCtx::from_slice(signal.context) {
            Ok(mut hedge_ctx) => {
                let strategy_id = hedge_ctx.strategy_id;
                let hedging_symbol = normalize_fixed_symbol_for_internal(&hedge_ctx.hedging_symbol);
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
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let strategy_opt = { strategy_mgr.borrow_mut().take(strategy_id) };
                if let Some(mut strategy) = strategy_opt {
                    if let Some(arb_hedge) =
                        strategy.as_any_mut().downcast_mut::<ArbHedgeStrategy>()
                    {
                        arb_hedge
                            .handle_arb_hedge_ctx_with_symbol(hedge_ctx, hedging_symbol.clone());
                    } else {
                        warn!(
                            "ArbHedge: target strategy type mismatch strategy_id={}",
                            strategy_id
                        );
                    }
                    if strategy.is_active() {
                        strategy_mgr.borrow_mut().insert(strategy);
                    }
                } else {
                    warn!("ArbHedge: 策略 id={} 不存在", strategy_id);
                }
            }
            Err(err) => warn!("failed to decode ArbHedge context: {err}"),
        },
        SignalType::MMCancelTrigger => match MmCancelTriggerCtx::from_slice(signal.context) {
            Ok(trigger_ctx) => {
                if !current_is_mm_mode() {
                    debug!("MMCancelTrigger ignored: pre_trade is not in MM mode");
                    return;
                }

                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let price_map_snapshot = strategy_mgr.borrow().mm_open_price_map_snapshot();
                if price_map_snapshot.is_empty() {
                    return;
                }
                let debug_stats = if log::log_enabled!(log::Level::Debug) {
                    let mut symbol_counts: HashMap<String, usize> = HashMap::new();
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
                    Some((
                        indexed_strategy_count,
                        symbol_count,
                        symbol_sample,
                        strategy_sample,
                    ))
                } else {
                    None
                };

                let mut chunk = MmCancelCandidateQueryMsg::new(trigger_ctx.trigger_ts);
                let mut payload = BytesMut::with_capacity(SIGNAL_PAYLOAD);
                let mut published_chunks = 0usize;
                let mut published_items = 0usize;

                let flush_chunk = |chunk: &mut MmCancelCandidateQueryMsg,
                                   payload: &mut BytesMut,
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
                    payload.clear();
                    chunk.write_backward_to(payload);
                    match SignalChannel::with(|ch| ch.publish_backward(payload.as_ref())) {
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
                            flush_chunk(
                                &mut chunk,
                                &mut payload,
                                &mut published_chunks,
                                &mut published_items,
                            );
                        }
                        chunk.push_grouped(&key.symbol, entry);
                    }
                }
                flush_chunk(
                    &mut chunk,
                    &mut payload,
                    &mut published_chunks,
                    &mut published_items,
                );
                if let Some((
                    indexed_strategy_count,
                    symbol_count,
                    symbol_sample,
                    strategy_sample,
                )) = debug_stats
                {
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
            }
            Err(err) => warn!("failed to decode MMCancelTrigger context: {err}"),
        },
        SignalType::MMCancel => match MmCancelCtx::from_slice(signal.context) {
            Ok(mut cancel_ctx) => {
                if !current_is_mm_mode() {
                    debug!("MMCancel ignored: pre_trade is not in MM mode");
                    return;
                }
                let symbol = normalize_fixed_symbol_for_internal(&cancel_ctx.opening_symbol);
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
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                if cancel_ctx.strategy_id > 0 {
                    let strategy_id = cancel_ctx.strategy_id;
                    let strategy_opt = { strategy_mgr.borrow_mut().take(strategy_id) };
                    if let Some(mut strategy) = strategy_opt {
                        let handled = if let Some(mm_open) = strategy
                            .as_any_mut()
                            .downcast_mut::<MarketMakerOpenStrategy>(
                        ) {
                            mm_open.handle_mm_cancel_ctx(&cancel_ctx);
                            true
                        } else {
                            false
                        };
                        if handled && strategy.is_active() {
                            strategy_mgr.borrow_mut().insert(strategy);
                        } else if !handled {
                            strategy_mgr.borrow_mut().insert(strategy);
                        }
                    } else {
                        debug!(
                            "MMCancel: targeted strategy missing strategy_id={} symbol={} trigger_ts={}",
                            strategy_id, symbol, cancel_ctx.trigger_ts
                        );
                    }
                    return;
                }

                CANCEL_CANDIDATE_IDS.with(|ids| {
                    let mut ids = ids.borrow_mut();
                    strategy_mgr
                        .borrow()
                        .copy_ids_for_normalized_symbol_into(&symbol, &mut ids);
                    for strategy_id in ids.iter().copied() {
                        let strategy_opt = { strategy_mgr.borrow_mut().take(strategy_id) };
                        if let Some(mut strategy) = strategy_opt {
                            let handled = if let Some(mm_open) = strategy
                                .as_any_mut()
                                .downcast_mut::<MarketMakerOpenStrategy>(
                            ) {
                                mm_open.handle_mm_cancel_ctx(&cancel_ctx);
                                true
                            } else {
                                false
                            };
                            if handled && strategy.is_active() {
                                strategy_mgr.borrow_mut().insert(strategy);
                            } else if !handled {
                                strategy_mgr.borrow_mut().insert(strategy);
                            }
                        }
                    }
                });
                drop(strategy_mgr);
            }
            Err(err) => warn!("failed to decode MMCancel context: {err}"),
        },
        SignalType::MMHedge => match MmHedgeCtx::from_slice(signal.context) {
            Ok(mut hedge_ctx) => {
                if !current_is_mm_mode() {
                    debug!("MMHedge ignored: pre_trade is not in MM mode");
                    return;
                }
                let symbol = normalize_fixed_symbol_for_internal(&hedge_ctx.opening_symbol);
                if symbol.is_empty() {
                    warn!("MMHedge: empty symbol");
                    return;
                }
                hedge_ctx.set_opening_symbol(&symbol);
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let strategy_id = strategy_mgr
                    .borrow_mut()
                    .ensure_mm_hedge_strategy_for_normalized_symbol(&symbol);
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
                    if let Some(mm_hedge) = strategy
                        .as_any_mut()
                        .downcast_mut::<MarketMakerHedgeStrategy>()
                    {
                        mm_hedge.handle_mm_hedge_ctx_with_symbol(hedge_ctx, symbol.clone());
                    } else {
                        warn!(
                            "MMHedge: target strategy type mismatch strategy_id={}",
                            strategy_id
                        );
                    }
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
