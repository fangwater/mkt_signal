use crate::pre_trade::account_open_block::{
    check_account_open_block, register_account_open_block,
    register_bybit_internal_system_open_block, AccountOpenBlockReason,
    BYBIT_INTERNAL_SYSTEM_OPEN_BLOCK_TTL_US,
};
use crate::pre_trade::lazy_taker_action::publish_lazy_taker_action;
use crate::pre_trade::log_throttle::log_order_rate_limit_summary;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::{OrderRateBucket, OrderRateLimiter};
use crate::pre_trade::order_manager::PreTradeOrderRequestExt;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::runtime_flags::suppress_pre_submit_hot_path_logs;
use crate::pre_trade::signal_channel::SignalChannel;
use crate::pre_trade::signal_throttle::{
    register_binance_futures_margin_signal_throttle_for_mode, register_signal_throttle_for_mode,
    SIGNAL_THROTTLE_TTL_US,
};
use crate::pre_trade::taker_decision_model::{
    LazyHedgeDecision, LazyHedgeDecisionSnapshot, PreTradeTakerDecisionModel,
};
use crate::pre_trade::{PersistChannel, TradeEngHub};
use crate::strategy::hedge_order_reconcile::{HedgeOrderReconcileCommon, HedgeOrderReconcileState};
use crate::strategy::hedge_strategy_common::{
    mark_price_lookup_symbol, parse_return_qtl_from_from_key, signed_qty_from_side,
    CANCEL_RESEND_THROTTLE_US, TERMINAL_QTY_EPS,
};
use crate::strategy::manager::{
    OrderTerminalRecorder, OrphanHandoff, OrphanSourceKind, OrphanStrategyRole, Strategy,
};
use crate::strategy::net_qty_queue::{NetQtyQueue, TimedNetQtyLot, TimedNetQtyQueue};
use crate::strategy::order_reconcile::PendingOrderQueryReason;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update, signal_bbo_from_legs, UniformPublishCtx,
};
use log::{debug, error, info, warn};
use order_common::trade_error_code::gate;
use order_common::OrderUpdate;
use order_common::TradeEngineResponse;
use order_common::TradeUpdate;
use order_common::{
    Order, OrderExecutionStatus, OrderManager, OrderQuantizedValue, OrderType, Side,
};
use order_common::{OrderStatus, TradeRequestType, TradingVenue};
use runtime_common::exchange::Exchange;
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use runtime_common::symbol_util::{min_qty_symbol_key, normalize_symbol_for_internal};
use runtime_common::time_util::get_timestamp_us;
use signal_common::arb_signal::ArbBackwardQueryMsg;
use signal_common::common::{align_price_floor, TradingLeg};
use signal_common::hedge_signal::{ArbHedgeCtx, ArbHedgeSignalQueryMsg};
use signal_common::lazy_taker_action::LazyTakerAction;
use signal_common::tick_math::QuantizedValue;
use signal_common::trade_signal::{SignalType, TradeSignal};
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::OnceLock;

const ARB_HEDGE_QTY_EPS: f64 = 1e-12;
const ARB_HEDGE_PENDING_QUERY_MIN_USDT: f64 = 10.0;
const ARB_HEDGE_BORROW_SHORTFALL_MAX_USDT: f64 = 1.0;
const ARB_HEDGE_QUERY_INTERVAL_US: i64 = 1_000_000;
const ARB_HEDGE_QUERY_TIMEOUT_US: i64 = 3_000_000;
/// 保证金不足应急动作的冷却时间。同一 strategy 在窗口内的连续拒单
/// 只触发一次账户级 open block/撤单，避免一秒万次拒单情况下重复执行。
const ARB_HEDGE_INSUFFICIENT_MARGIN_COOLDOWN_US: i64 = 5_000_000;
static ARB_HEDGE_FORCE_TAKER: OnceLock<bool> = OnceLock::new();
static ARB_HEDGE_LAZY_TAKER: OnceLock<bool> = OnceLock::new();

fn env_flag_enabled(names: &[&str]) -> bool {
    names.iter().any(|name| {
        std::env::var(name)
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "True" | "on" | "ON"))
            .unwrap_or(false)
    })
}

fn arb_hedge_force_taker() -> bool {
    *ARB_HEDGE_FORCE_TAKER.get_or_init(|| {
        let force_taker = env_flag_enabled(&["ARB_HEDGE_FORCE_TAKER"]);
        let lazy_taker = env_flag_enabled(&["ARB_HEDGE_LAZY_TAKER", "ARB_HEDGE_lazy_TAKER"]);
        if force_taker && lazy_taker {
            panic!(
                "ARB_HEDGE_FORCE_TAKER 与 ARB_HEDGE_LAZY_TAKER/ARB_HEDGE_lazy_TAKER 互斥，只能有一个为 true"
            );
        }
        if force_taker {
            warn!(
                "ARB_HEDGE_FORCE_TAKER=on: pre_trade ArbHedge will bypass backward query and submit taker directly"
            );
        }
        force_taker
    })
}

fn arb_hedge_lazy_taker() -> bool {
    *ARB_HEDGE_LAZY_TAKER
        .get_or_init(|| env_flag_enabled(&["ARB_HEDGE_LAZY_TAKER", "ARB_HEDGE_lazy_TAKER"]))
}

fn publish_lazy_action(
    symbol: &str,
    venue: TradingVenue,
    local_tp_us: i64,
    due_hedge_qty: f64,
    action: LazyTakerAction,
) {
    let direction = if due_hedge_qty > ARB_HEDGE_QTY_EPS {
        1
    } else if due_hedge_qty < -ARB_HEDGE_QTY_EPS {
        -1
    } else {
        return;
    };
    let model_name = PreTradeTakerDecisionModel::evaluation_model_name_global()
        .unwrap_or_else(|| "unknown".to_string());
    let _ = publish_lazy_taker_action(
        local_tp_us,
        symbol,
        &model_name,
        venue.to_u8(),
        action,
        direction,
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DueHedgeRoute {
    Query,
    DirectTaker,
    Hold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InflightHedgeQuery {
    request_seq: u64,
    sent_ts_us: i64,
    deadline_ts_us: i64,
}

fn decide_due_hedge_route(
    force_taker: bool,
    lazy_taker: bool,
    snapshot: Option<&LazyHedgeDecisionSnapshot>,
) -> DueHedgeRoute {
    if force_taker {
        return DueHedgeRoute::DirectTaker;
    }
    if !lazy_taker {
        return DueHedgeRoute::Query;
    }
    if matches!(
        snapshot,
        Some(snapshot)
            if snapshot.ready && snapshot.decision != LazyHedgeDecision::Hedge
    ) {
        DueHedgeRoute::Hold
    } else {
        DueHedgeRoute::DirectTaker
    }
}

fn model_percentile_to_ret_qtl(percentile_pct: Option<f64>) -> Option<f64> {
    let percentile_pct = percentile_pct.filter(|value| value.is_finite())?;
    if !(0.0..=100.0).contains(&percentile_pct) {
        return None;
    }
    Some(percentile_pct / 100.0)
}

fn build_direct_taker_from_key(source: &str, now_ts: i64, ret_qtl: Option<f64>) -> Vec<u8> {
    let mut from_key = format!("arb_hedge_{}_direct|{}", source, now_ts);
    if let Some(ret_qtl) = ret_qtl.filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
    {
        from_key.push_str(&format!(":ret_qtl={ret_qtl:.8}"));
    }
    from_key.into_bytes()
}

fn is_direct_taker_from_key(from_key: &[u8]) -> bool {
    from_key.starts_with(b"arb_hedge_force_taker_direct|")
        || from_key.starts_with(b"arb_hedge_lazy_model_direct|")
}

fn order_qv_from_quantized_value(qv: QuantizedValue) -> OrderQuantizedValue {
    let (tick_i64, tick_exp) = qv.get_tick_parts();
    OrderQuantizedValue::new(tick_i64, tick_exp, qv.get_count())
}

/// Arb 对冲策略的只读状态快照。
///
/// 调用方可以通过它观察双 venue 合并后的净敞口和待对冲数量。
#[derive(Debug, Clone)]
pub struct ArbHedgeSnapshot {
    pub symbol: String,
    pub open_venue: TradingVenue,
    pub hedge_venue: TradingVenue,
    pub net_qty: f64,
    pub pending_hedge_qty: f64,
    pub due_hedge_qty: f64,
    pub hedge_ts_ms: Option<i64>,
    pub hedge_is_taker: Option<bool>,
    pub ret_qtl: Option<f64>,
    /// arb 永远 single-order hedge（drive_shared_arb_hedge_query 取 plan.levels.first()），
    /// 不存在 MM 那种多档拆单的 low/high，单一 price_offset 即可。
    pub offset: Option<f64>,
}

/// Arb 对冲状态策略。
///
/// 这一阶段只维护记录接口和队列状态，不负责生成对冲订单。
pub struct ArbHedgeStrategy {
    pub(super) strategy_id: i32,
    pub(super) symbol: String,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    /// open/hedge 两个 venue 合并后的实时净敞口，使用 base qty 口径互相冲销。
    pub(super) net_qty_queue: NetQtyQueue,
    /// 尚需由对冲腿覆盖的 opening-leg 成交队列，到期时间来自成交记录的 close_ts。
    pub(super) pending_hedge_queue: TimedNetQtyQueue,
    /// 启动时从账户快照带入的净敞口基线。这部分只作为状态展示，不反推出 hedge work。
    hedge_work_baseline_qv: f64,
    hedge_request_seq: u64,
    inflight_hedge_query: Option<InflightHedgeQuery>,
    last_hedge_ts_ms: Option<i64>,
    last_hedge_is_taker: Option<bool>,
    last_ret_qtl: Option<f64>,
    /// 最近一次 ArbHedge maker 单的 price_offset；taker 时为 0。供 dashboard 显示。
    last_hedge_offset: Option<f64>,
    next_query_ts_us: i64,
    order_seq: u32,
    hedge_order_meta: FastHashMap<i64, ArbHedgeOrderMeta>,
    orphaned_hedge_order_meta: FastHashMap<i64, ArbHedgeOrderMeta>,
    hedge_order_expiry_wheel: BTreeMap<i64, Vec<i64>>,
    order_reconcile_state: HedgeOrderReconcileState,
    alive_flag: bool,
    /// 上一次因 51008 触发应急动作的时间戳（us）。0 表示从未触发。
    last_insufficient_margin_action_ts: i64,
    bybit_oi_limit_block_until_us: i64,
    bybit_oi_limit_block_side: Option<Side>,
    bitget_position_tier_limit_block_until_us: i64,
    bitget_position_tier_limit_block_side: Option<Side>,
}

#[derive(Debug, Clone)]
struct ArbHedgeOrderMeta {
    signal_ts: i64,
    price_offset: f64,
    signal_bbo: Option<persist_common::SignalBbo>,
    borrowed_qv: f64,
    order_base_qty: f64,
    expire_ts: i64,
    next_expire_check_ts: i64,
    cancel_requested: bool,
    /// 主成分绑定的 open client_order_id —— borrow 时按吃量最大的 open lot 选取。
    /// release/cleanup/失败回写都使用这个 id 把未成交量送回原 open 身份；
    /// 借不到任何带 id 的 lot 时为 0（兜底，按用户约定）。
    bound_open_client_order_id: i64,
    /// 本对冲单收到的 rich from_key（开仓信号上下文），用于 uniform 发布时
    /// 以 "{id}|{rich}" 形式带上整条 from_key（保留 JOIN 前缀，同时携带信号字段）。
    from_key: Vec<u8>,
}

impl ArbHedgeStrategy {
    fn bitget_margin_lock_reduce_only_hedge(
        &self,
        venue: TradingVenue,
        side: Side,
        order_base_qty: f64,
    ) -> bool {
        const EPS: f64 = 1e-12;
        if self.open_venue != TradingVenue::BitgetMargin
            || self.hedge_venue != TradingVenue::BitgetFutures
            || venue != TradingVenue::BitgetFutures
            || side != Side::Buy
            || !(order_base_qty.is_finite() && order_base_qty > 0.0)
            || !check_account_open_block().is_some_and(|hit| {
                hit.reason == AccountOpenBlockReason::BitgetUnifiedInsufficientMargin
            })
        {
            return false;
        }

        let futures_net =
            MonitorChannel::instance().get_position_qty(&self.symbol, TradingVenue::BitgetFutures);
        futures_net < -EPS && order_base_qty <= -futures_net + EPS
    }

    fn is_gate_futures_open_ack_unknown(response: &dyn TradeEngineResponse) -> bool {
        response.status() == 504
            && response.req_type() == TradeRequestType::GateFuturesNewOrder as u32
    }

    fn should_retry_gate_market_forbidden_reduce_only(
        response: &dyn TradeEngineResponse,
        order: &Order,
    ) -> bool {
        response.exchange_enum() == Some(Exchange::Gate)
            && response.req_type() == TradeRequestType::GateFuturesNewOrder as u32
            && response.error_code() == gate::RISK_CHECK_MARKET_FORBIDDEN
            && order.venue == TradingVenue::GateFutures
            && order.order_type == OrderType::Market
            && !order.reduce_only
    }

    fn is_gate_market_forbidden_reduce_only_failure(
        response: &dyn TradeEngineResponse,
        order: &Order,
    ) -> bool {
        response.exchange_enum() == Some(Exchange::Gate)
            && response.req_type() == TradeRequestType::GateFuturesNewOrder as u32
            && response.error_code() == gate::RISK_CHECK_MARKET_FORBIDDEN
            && order.venue == TradingVenue::GateFutures
            && order.order_type == OrderType::Market
            && order.reduce_only
    }

    fn retry_gate_market_forbidden_reduce_only(
        &mut self,
        response: &dyn TradeEngineResponse,
        client_order_id: i64,
        order: &Order,
        now_ts: i64,
    ) -> bool {
        if !Self::should_retry_gate_market_forbidden_reduce_only(response, order) {
            return false;
        }
        let Some(meta) = self.hedge_order_meta.remove(&client_order_id) else {
            return false;
        };

        self.clear_order_query_state(client_order_id);
        let retry_client_order_id = self.next_order_id();
        let symbol = order.symbol.clone();
        let quantity_qv = order.quantity_qv;
        let price_qv = order.price_qv;
        let signal_ts = order.timestamp.signal_t;
        let signal_kind = order.timestamp.signal_kind;
        let mkt_ts = order.timestamp.mkt_t;
        let pre_trade_recv_ts = order.timestamp.pre_trade_recv_t;
        let pre_trade_handle_ts = order.timestamp.pre_trade_handle_t;
        let order_mgr = MonitorChannel::instance().order_manager();
        {
            let mut order_mgr = order_mgr.borrow_mut();
            let _ = order_mgr.remove(client_order_id);
            order_mgr.create_order_with_mut(
                order.venue,
                retry_client_order_id,
                order.order_type,
                symbol.clone(),
                order.side,
                order.quantity,
                order.price,
                true,
                order.qty_multiplier,
                order.count_pending_limit,
                |retry_order| {
                    if let Some(quantity_qv) = quantity_qv {
                        retry_order.set_quantity_qv(quantity_qv);
                    }
                    if let Some(price_qv) = price_qv {
                        retry_order.set_price_qv(price_qv);
                    }
                    retry_order.set_signal_meta(signal_ts, signal_kind);
                    retry_order.set_mkt_time(mkt_ts);
                    retry_order.set_pre_trade_open_trace(pre_trade_recv_ts, pre_trade_handle_ts);
                },
            );
        }
        self.hedge_order_meta.insert(retry_client_order_id, meta);

        warn!(
            "ArbHedgeStrategy: strategy_id={} Gate market order rejected by risk check; retry once reduce_only=true old_client_order_id={} retry_client_order_id={} symbol={} side={:?} qty={:.8}",
            self.strategy_id,
            client_order_id,
            retry_client_order_id,
            symbol,
            order.side,
            order.quantity
        );
        if let Err(err) = create_and_send_order(
            self.strategy_id,
            retry_client_order_id,
            "Gate reduce-only 状态对冲重试",
            &symbol,
        ) {
            self.cleanup_unsent_hedge_order_after_send_failure(
                retry_client_order_id,
                now_ts,
                order.price,
            );
            self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
            warn!(
                "ArbHedgeStrategy: strategy_id={} send Gate reduce-only hedge retry failed client_order_id={} symbol={} err={}",
                self.strategy_id, retry_client_order_id, symbol, err
            );
        } else {
            self.schedule_order_query_watchdog(
                retry_client_order_id,
                PendingOrderQueryReason::OrderWatchdog,
            );
        }
        true
    }

    fn handle_gate_futures_open_ack_unknown(&mut self, response: &dyn TradeEngineResponse) -> bool {
        if !Self::is_gate_futures_open_ack_unknown(response) {
            return false;
        }
        let client_order_id = response.client_order_id();
        if !self.hedge_order_meta.contains_key(&client_order_id) {
            return false;
        }
        warn!(
            "ArbHedgeStrategy: strategy_id={} GateFutures hedge open ack unknown: req_type={} status={} code={} client_order_id={} keep borrowed pending and wait for order/trade update or query reconcile {}",
            self.strategy_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            client_order_id,
            self.hedge_order_trace_snapshot(client_order_id)
        );
        true
    }

    pub fn new(
        strategy_id: i32,
        symbol: impl Into<String>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            open_venue,
            hedge_venue,
            net_qty_queue: NetQtyQueue::new(),
            pending_hedge_queue: TimedNetQtyQueue::new(),
            hedge_work_baseline_qv: 0.0,
            hedge_request_seq: 0,
            inflight_hedge_query: None,
            last_hedge_ts_ms: None,
            last_hedge_is_taker: None,
            last_ret_qtl: None,
            last_hedge_offset: None,
            next_query_ts_us: 0,
            order_seq: 0,
            hedge_order_meta: fast_hash_map(),
            orphaned_hedge_order_meta: fast_hash_map(),
            hedge_order_expiry_wheel: BTreeMap::new(),
            order_reconcile_state: HedgeOrderReconcileState::default(),
            alive_flag: true,
            last_insufficient_margin_action_ts: 0,
            bybit_oi_limit_block_until_us: 0,
            bybit_oi_limit_block_side: None,
            bitget_position_tier_limit_block_until_us: 0,
            bitget_position_tier_limit_block_side: None,
        }
    }

    pub fn snapshot(&self, now_ts: i64) -> ArbHedgeSnapshot {
        ArbHedgeSnapshot {
            symbol: self.symbol.clone(),
            open_venue: self.open_venue,
            hedge_venue: self.hedge_venue,
            net_qty: self.net_qty_queue.net_qty(),
            pending_hedge_qty: self.pending_hedge_queue.net_qty(),
            due_hedge_qty: self.pending_hedge_queue.due_qty(now_ts),
            hedge_ts_ms: self.last_hedge_ts_ms,
            hedge_is_taker: self.last_hedge_is_taker,
            ret_qtl: self.last_ret_qtl,
            offset: self.last_hedge_offset,
        }
    }

    pub fn net_qty(&self) -> f64 {
        self.net_qty_queue.net_qty()
    }

    pub fn seed_net_position(
        &mut self,
        seed_ts: i64,
        signed_base_qty: f64,
        price: f64,
        source: &'static str,
    ) -> bool {
        if signed_base_qty.abs() <= TERMINAL_QTY_EPS {
            return false;
        }
        let seed_price = if price.is_finite() && price > 0.0 {
            price
        } else {
            0.0
        };
        self.net_qty_queue
            .apply_fill(seed_ts, signed_base_qty, seed_price);
        self.hedge_work_baseline_qv = self.net_qty_queue.net_qty();
        if !suppress_pre_submit_hot_path_logs() {
            info!(
                "ArbHedgeSeed: strategy_id={} symbol={} source={} qv={:.8} price={:.8} seed_ts={} net={:.8} hedge_work_baseline={:.8}",
                self.strategy_id,
                self.symbol,
                source,
                signed_base_qty,
                seed_price,
                seed_ts,
                self.net_qty_queue.net_qty(),
                self.hedge_work_baseline_qv
            );
        }
        true
    }

    pub fn put_startup_stable_net_pending(
        &mut self,
        ready_ts: i64,
        signed_base_qty: f64,
        price: f64,
    ) -> bool {
        if signed_base_qty.abs() <= TERMINAL_QTY_EPS {
            return false;
        }
        let price = if price.is_finite() && price > 0.0 {
            price
        } else {
            0.0
        };
        self.net_qty_queue
            .apply_fill(ready_ts, signed_base_qty, price);
        self.pending_hedge_queue
            .put(ready_ts, 0, signed_base_qty, price);
        if !suppress_pre_submit_hot_path_logs() {
            info!(
                "ArbHedgeStartupNet: strategy_id={} symbol={} qv={:.8} price={:.8} ready_ts={} net={:.8} pending_hedge={:.8}",
                self.strategy_id,
                self.symbol,
                signed_base_qty,
                price,
                ready_ts,
                self.net_qty_queue.net_qty(),
                self.pending_hedge_queue.net_qty()
            );
        }
        true
    }

    // pending_hedge_qty 包含尚未借给 live hedge order 的对冲需求，无论是否已到期；
    pub fn pending_hedge_qty(&self) -> f64 {
        self.pending_hedge_queue.net_qty()
    }
    // due_hedge_qty 则只计算已到期的部分。
    pub fn due_hedge_qty(&self, now_ts: i64) -> f64 {
        self.pending_hedge_queue.due_qty(now_ts)
    }

    fn next_hedge_request_seq(&mut self) -> u64 {
        self.hedge_request_seq = self.hedge_request_seq.wrapping_add(1);
        if self.hedge_request_seq == 0 {
            self.hedge_request_seq = 1;
        }
        self.hedge_request_seq
    }

    fn begin_inflight_hedge_query(&mut self, request_seq: u64, now_ts: i64) {
        self.inflight_hedge_query = Some(InflightHedgeQuery {
            request_seq,
            sent_ts_us: now_ts,
            deadline_ts_us: now_ts.saturating_add(ARB_HEDGE_QUERY_TIMEOUT_US),
        });
    }

    fn retire_timed_out_hedge_query(&mut self, now_ts: i64) -> bool {
        let Some(inflight) = self.inflight_hedge_query else {
            return false;
        };
        if now_ts < inflight.deadline_ts_us {
            return false;
        }
        self.inflight_hedge_query = None;
        self.next_query_ts_us = 0;
        warn!(
            "ArbHedgeStrategy: strategy_id={} symbol={} hedge query timeout request_seq={} sent_ts_us={} deadline_ts_us={} now_ts={} pending_hedge_qty={:.8}",
            self.strategy_id,
            self.symbol,
            inflight.request_seq,
            inflight.sent_ts_us,
            inflight.deadline_ts_us,
            now_ts,
            self.pending_hedge_queue.net_qty()
        );
        true
    }

    fn coalesce_while_hedge_query_inflight(&mut self, now_ts: i64, reason: &str) -> bool {
        self.retire_timed_out_hedge_query(now_ts);
        let Some(inflight) = self.inflight_hedge_query else {
            return false;
        };
        debug!(
            "ArbHedgeStrategy: strategy_id={} symbol={} coalesce {} while hedge query inflight request_seq={} sent_ts_us={} deadline_ts_us={} pending_hedge_qty={:.8} due_hedge_qty={:.8}",
            self.strategy_id,
            self.symbol,
            reason,
            inflight.request_seq,
            inflight.sent_ts_us,
            inflight.deadline_ts_us,
            self.pending_hedge_queue.net_qty(),
            self.pending_hedge_queue.due_qty(now_ts)
        );
        true
    }

    fn mark_price(&self) -> Option<f64> {
        let monitor = MonitorChannel::instance();
        let mark_price_exchange = monitor.try_mark_price_exchange()?;
        let price_symbol = mark_price_lookup_symbol(&self.symbol, mark_price_exchange);
        monitor
            .try_price_table()?
            .borrow()
            .mark_price(&price_symbol)
            .filter(|price| price.is_finite() && *price > 0.0)
    }

    fn borrowed_hedge_qv(&self) -> f64 {
        self.hedge_order_meta
            .values()
            .chain(self.orphaned_hedge_order_meta.values())
            .map(|meta| meta.borrowed_qv)
            .sum()
    }

    fn outstanding_hedge_work_qv(&self) -> f64 {
        self.pending_hedge_queue.net_qty() + self.borrowed_hedge_qv()
    }

    fn target_hedge_work_qv(&self) -> f64 {
        self.net_qty_queue.net_qty() - self.hedge_work_baseline_qv
    }

    fn pending_hedge_usdt_with_mark_price(pending_hedge_qty: f64, mark_price: f64) -> f64 {
        pending_hedge_qty.abs() * mark_price.abs()
    }

    fn gate_futures_query_qty_below_min(
        due_hedge_qty: f64,
        min_qty_contracts: Option<f64>,
        step_contracts: Option<f64>,
        contract_multiplier: Option<f64>,
    ) -> Option<(f64, f64, f64)> {
        if !(due_hedge_qty.is_finite() && due_hedge_qty.abs() > ARB_HEDGE_QTY_EPS) {
            return None;
        }
        let min_qty_contracts = min_qty_contracts?;
        if !(min_qty_contracts.is_finite() && min_qty_contracts > 0.0) {
            return None;
        }
        let contract_multiplier = contract_multiplier?;
        if !(contract_multiplier.is_finite() && contract_multiplier > 0.0) {
            return None;
        }
        let raw_contracts = due_hedge_qty.abs() / contract_multiplier;
        if !(raw_contracts.is_finite() && raw_contracts > 0.0) {
            return None;
        }
        let aligned_contracts = match step_contracts {
            Some(step) if step.is_finite() && step > 0.0 => align_price_floor(raw_contracts, step),
            _ => raw_contracts,
        };
        if aligned_contracts + 1e-12 < min_qty_contracts {
            Some((raw_contracts, aligned_contracts, min_qty_contracts))
        } else {
            None
        }
    }

    fn gate_futures_query_due_qty_below_min(&self, due_hedge_qty: f64) -> Option<(f64, f64, f64)> {
        if self.hedge_venue != TradingVenue::GateFutures {
            return None;
        }
        let symbol_key = min_qty_symbol_key(self.hedge_venue, &self.symbol);
        let table = MonitorChannel::instance().try_venue_min_qty_table(self.hedge_venue)?;
        Self::gate_futures_query_qty_below_min(
            due_hedge_qty,
            table.min_qty(&symbol_key),
            table.step_size(&symbol_key),
            table.contract_multiplier_opt(&symbol_key),
        )
    }

    fn binance_futures_qty_below_min(
        due_hedge_qty: f64,
        min_qty: Option<f64>,
        step_size: Option<f64>,
    ) -> Option<(f64, f64, f64)> {
        if !(due_hedge_qty.is_finite() && due_hedge_qty.abs() > ARB_HEDGE_QTY_EPS) {
            return None;
        }
        let min_qty = min_qty?;
        if !(min_qty.is_finite() && min_qty > 0.0) {
            return None;
        }
        let raw_qty = due_hedge_qty.abs();
        let aligned_qty = match step_size {
            Some(step) if step.is_finite() && step > 0.0 => align_price_floor(raw_qty, step),
            _ => raw_qty,
        };
        if aligned_qty + 1e-12 < min_qty {
            Some((raw_qty, aligned_qty, min_qty))
        } else {
            None
        }
    }

    fn binance_futures_due_qty_below_min(&self, due_hedge_qty: f64) -> Option<(f64, f64, f64)> {
        if self.hedge_venue != TradingVenue::BinanceFutures {
            return None;
        }
        let symbol_key = min_qty_symbol_key(self.hedge_venue, &self.symbol);
        let table = MonitorChannel::instance().try_venue_min_qty_table(self.hedge_venue)?;
        Self::binance_futures_qty_below_min(
            due_hedge_qty,
            table.min_qty(&symbol_key),
            table.step_size(&symbol_key),
        )
    }

    fn hedge_leg_reference_price(price: f64, leg: TradingLeg) -> Option<f64> {
        if price.is_finite() && price > 0.0 {
            return Some(price);
        }
        let bid = leg.bid0;
        let ask = leg.ask0;
        if bid.is_finite() && bid > 0.0 && ask.is_finite() && ask > 0.0 {
            return Some((bid + ask) * 0.5);
        }
        if bid.is_finite() && bid > 0.0 {
            return Some(bid);
        }
        if ask.is_finite() && ask > 0.0 {
            return Some(ask);
        }
        None
    }

    fn borrow_shortfall_usdt(shortfall_qty: f64, price: f64, leg: TradingLeg) -> Option<f64> {
        if !(shortfall_qty.is_finite() && shortfall_qty > 0.0) {
            return Some(0.0);
        }
        Self::hedge_leg_reference_price(price, leg).map(|ref_price| shortfall_qty * ref_price)
    }

    fn borrow_shortfall_within_eps(shortfall_qty: f64, price: f64, leg: TradingLeg) -> bool {
        Self::borrow_shortfall_usdt(shortfall_qty, price, leg)
            .map(|shortfall_usdt| {
                shortfall_usdt <= ARB_HEDGE_BORROW_SHORTFALL_MAX_USDT + f64::EPSILON
            })
            .unwrap_or(false)
    }

    fn schedule_hedge_order_expiry_check(&mut self, client_order_id: i64, due_ts: i64) {
        if due_ts <= 0 {
            return;
        }
        if let Some(meta) = self.hedge_order_meta.get_mut(&client_order_id) {
            meta.next_expire_check_ts = due_ts;
        }
        self.hedge_order_expiry_wheel
            .entry(due_ts)
            .or_default()
            .push(client_order_id);
    }

    fn reschedule_expired_hedge_cancel_if_still_live(
        &mut self,
        client_order_id: i64,
        now_ts: i64,
        reason: &'static str,
    ) {
        {
            let Some(meta) = self.hedge_order_meta.get_mut(&client_order_id) else {
                return;
            };
            if meta.expire_ts <= 0 || !meta.cancel_requested {
                return;
            }
            meta.cancel_requested = false;
        }
        let retry_ts = now_ts.saturating_add(CANCEL_RESEND_THROTTLE_US);
        self.schedule_hedge_order_expiry_check(client_order_id, retry_ts);
        debug!(
            "ArbHedgeStrategy: strategy_id={} expired hedge order still live after cancel query, retry scheduled client_order_id={} reason={} retry_ts={}",
            self.strategy_id,
            client_order_id,
            reason,
            retry_ts
        );
    }

    fn cleanup_unsent_hedge_order_after_send_failure(
        &mut self,
        client_order_id: i64,
        now_ts: i64,
        release_price: f64,
    ) {
        if let Some(meta) = self.hedge_order_meta.remove(&client_order_id) {
            self.release_borrowed_with_bound_id(
                now_ts,
                meta.borrowed_qv,
                release_price,
                meta.bound_open_client_order_id,
            );
        }
        self.clear_order_query_state(client_order_id);
        if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let _ = order_mgr.borrow_mut().remove(client_order_id);
        }
    }

    fn release_borrowed_with_bound_id(
        &mut self,
        now_ts: i64,
        qv: f64,
        price: f64,
        bound_open_client_order_id: i64,
    ) -> f64 {
        if qv.abs() <= ARB_HEDGE_QTY_EPS {
            return 0.0;
        }
        if bound_open_client_order_id != 0 {
            self.pending_hedge_queue
                .release_with_id(now_ts, qv, price, bound_open_client_order_id)
        } else {
            self.pending_hedge_queue.release(now_ts, qv, price)
        }
    }

    fn trigger_hedge_query_after_pending_release(
        &mut self,
        terminal_ts: i64,
        reason: &'static str,
    ) -> bool {
        let triggered = self.trigger_hedge_query_after_opening_leg_terminal(terminal_ts);
        if triggered {
            debug!(
                "ArbHedgeStrategy: strategy_id={} symbol={} trigger hedge state query after pending release reason={}",
                self.strategy_id, self.symbol, reason
            );
        }
        triggered
    }

    fn clear_live_order_query_state(&mut self, client_order_id: i64) {
        // live update 只说明订单仍被交易所识别，不能说明撤单请求已经完成。
        // 对冲单过期撤单后可能收到重复 New/Partial，这里必须保留 CancelWatchdog，
        // 否则幂等 update 会把撤单后的 query/orphan 兜底清掉。
        let state = self.hedge_reconcile_state_mut();
        if state.pending_order_queries.get(&client_order_id).copied()
            == Some(PendingOrderQueryReason::OrderWatchdog)
        {
            state.pending_order_queries.remove(&client_order_id);
        }
        if state
            .order_query_watchdogs
            .get(&client_order_id)
            .map(|(_, reason)| *reason)
            == Some(PendingOrderQueryReason::OrderWatchdog)
        {
            state.order_query_watchdogs.remove(&client_order_id);
        }
    }

    fn send_force_taker_hedge_direct(&mut self, now_ts: i64, due_hedge_qty: f64) -> bool {
        self.send_taker_hedge_direct(now_ts, due_hedge_qty, "force_taker", None)
    }

    fn send_lazy_model_taker_hedge_direct(
        &mut self,
        now_ts: i64,
        due_hedge_qty: f64,
        model_percentile: Option<f64>,
    ) -> bool {
        self.send_taker_hedge_direct(
            now_ts,
            due_hedge_qty,
            "lazy_model",
            model_percentile_to_ret_qtl(model_percentile),
        )
    }

    fn send_taker_hedge_direct(
        &mut self,
        now_ts: i64,
        due_hedge_qty: f64,
        source: &str,
        ret_qtl: Option<f64>,
    ) -> bool {
        if self.coalesce_while_hedge_query_inflight(now_ts, source) {
            return false;
        }
        self.last_hedge_ts_ms = Some(now_ts / 1000);
        let Some(mark_price) = self.mark_price() else {
            if !suppress_pre_submit_hot_path_logs() {
                info!(
                    "ArbHedgeStrategy: strategy_id={} symbol={} skip {} direct hedge because mark_price missing due_hedge_qty={:.8}",
                    self.strategy_id, self.symbol, source, due_hedge_qty
                );
            }
            return false;
        };
        if !(mark_price.is_finite() && mark_price > 0.0) {
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} skip {} direct hedge because mark_price invalid mark_price={:.8} due_hedge_qty={:.8}",
                self.strategy_id, self.symbol, source, mark_price, due_hedge_qty
            );
            return false;
        }

        let hedge_side = if due_hedge_qty >= 0.0 {
            Side::Sell
        } else {
            Side::Buy
        };
        if self.is_bybit_oi_limit_blocked(hedge_side, now_ts) {
            self.next_query_ts_us = self.bybit_oi_limit_block_until_us;
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} skip {} direct hedge because Bybit OI/position-limit block is active hedge_side={} until_us={}",
                self.strategy_id,
                self.symbol,
                source,
                hedge_side.as_str(),
                self.bybit_oi_limit_block_until_us
            );
            return false;
        }
        if self.is_bitget_position_tier_limit_blocked(hedge_side, now_ts) {
            self.next_query_ts_us = self.bitget_position_tier_limit_block_until_us;
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} skip {} direct hedge because Bitget position-tier block is active hedge_side={} until_us={}",
                self.strategy_id,
                self.symbol,
                source,
                hedge_side.as_str(),
                self.bitget_position_tier_limit_block_until_us
            );
            return false;
        }
        let raw_base_qty = due_hedge_qty.abs();
        if let Some((raw_qty, aligned_qty, min_qty)) =
            self.binance_futures_due_qty_below_min(due_hedge_qty)
        {
            if !suppress_pre_submit_hot_path_logs() {
                info!(
                    "ArbHedgeStrategy: strategy_id={} symbol={} skip {} direct hedge because BinanceFutures qty below min due_hedge_qty={:.8} raw_qty={:.8} aligned_qty={:.8} min_qty={:.8}",
                    self.strategy_id,
                    self.symbol,
                    source,
                    due_hedge_qty,
                    raw_qty,
                    aligned_qty,
                    min_qty
                );
            }
            return false;
        }
        let (qty, _) = match MonitorChannel::instance().align_order_by_venue(
            self.hedge_venue,
            &self.symbol,
            raw_base_qty,
            mark_price,
        ) {
            Ok(aligned) => aligned,
            Err(err) => {
                warn!(
                    "ArbHedgeStrategy: strategy_id={} symbol={} {} direct align failed venue={:?} raw_base_qty={:.8} mark_price={:.8} err={}",
                    self.strategy_id, self.symbol, source, self.hedge_venue, raw_base_qty, mark_price, err
                );
                return false;
            }
        };
        if !(qty.is_finite() && qty > 0.0) {
            if !suppress_pre_submit_hot_path_logs() {
                info!(
                    "ArbHedgeStrategy: strategy_id={} symbol={} skip {} direct hedge because aligned qty zero raw_base_qty={:.8}",
                    self.strategy_id, self.symbol, source, raw_base_qty
                );
            }
            return false;
        }

        let qty_tick = MonitorChannel::instance()
            .try_venue_min_qty_table(self.hedge_venue)
            .and_then(|table| {
                let symbol_key =
                    runtime_common::symbol_util::min_qty_symbol_key(self.hedge_venue, &self.symbol);
                table.step_size(&symbol_key)
            })
            .unwrap_or(0.0);
        let Some(amount_qv) = QuantizedValue::encode_floor(qty, qty_tick) else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} {} direct amount qv invalid qty={:.8} qty_tick={:.8}",
                self.strategy_id, self.symbol, source, qty, qty_tick
            );
            return false;
        };
        if amount_qv.get_count() <= 0 {
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} {} direct amount qv non-positive qty={:.8} qty_tick={:.8} count={}",
                self.strategy_id,
                self.symbol,
                source,
                qty,
                qty_tick,
                amount_qv.get_count()
            );
            return false;
        }

        let request_seq = self.next_hedge_request_seq();
        self.begin_inflight_hedge_query(request_seq, now_ts);
        let mut ctx = ArbHedgeCtx::new();
        ctx.strategy_id = self.strategy_id;
        ctx.set_side(hedge_side);
        ctx.hedging_leg = TradingLeg::new(self.hedge_venue, mark_price, mark_price, now_ts);
        ctx.set_hedging_symbol(&self.symbol);
        ctx.price_qv = QuantizedValue::zero();
        ctx.amount_qv = amount_qv;
        ctx.price_offset = 0.0;
        ctx.signal_ts = now_ts;
        ctx.exp_time = 0;
        ctx.request_seq = request_seq;
        ctx.set_from_key(build_direct_taker_from_key(source, now_ts, ret_qtl));

        debug!(
            "ArbHedgeStrategy: strategy_id={} symbol={} {} direct hedge qty={:.8} side={:?} request_seq={}",
            self.strategy_id, self.symbol, source, qty, hedge_side, request_seq
        );
        debug!(
            "ArbHedgeStrategy: strategy_id={} symbol={} {} direct hedge detail due_hedge_qty={:.8} raw_base_qty={:.8} aligned_qty={:.8} mark_price={:.8} request_seq={}",
            self.strategy_id,
            self.symbol,
            source,
            due_hedge_qty,
            raw_base_qty,
            qty,
            mark_price,
            request_seq
        );
        self.handle_arb_hedge_ctx(ctx);
        true
    }

    fn send_hedge_query(&mut self, now_ts: i64, due_hedge_qty: f64) -> bool {
        self.last_hedge_ts_ms = Some(now_ts / 1000);
        let risk_loader = PreTradeParamsLoader::instance();
        let symbol_exposure_u = risk_loader
            .max_pos_u_for_symbol(self.open_venue, &self.symbol)
            .max(0.0)
            * risk_loader.max_symbol_exposure_ratio().max(0.0);
        let request_seq = self.next_hedge_request_seq();
        let query_msg = ArbHedgeSignalQueryMsg::new(
            self.strategy_id,
            &self.symbol,
            self.net_qty_queue.net_qty(),
            due_hedge_qty,
            self.pending_hedge_queue.net_qty(),
            symbol_exposure_u,
            self.net_qty_queue.weighted_avg_price().unwrap_or(0.0),
            request_seq,
        );
        let query = ArbBackwardQueryMsg::Hedge(query_msg);
        match SignalChannel::with(|ch| {
            ch.publish_backward_with(query.encoded_len(), |out| {
                query
                    .write_to_slice(out)
                    .expect("Arb hedge query encoded length must match writer");
            })
        }) {
            Ok(true) => {
                self.begin_inflight_hedge_query(request_seq, now_ts);
                self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
                if !suppress_pre_submit_hot_path_logs() {
                    info!(
                        "ArbHedgeStrategy: strategy_id={} symbol={} send hedge state query ok request_seq={} net_qty={:.8} due_hedge_qty={:.8} pending_hedge_qty={:.8} next_query_ts_us={}",
                        self.strategy_id,
                        self.symbol,
                        request_seq,
                        self.net_qty_queue.net_qty(),
                        due_hedge_qty,
                        self.pending_hedge_queue.net_qty(),
                        self.next_query_ts_us
                    );
                }
                true
            }
            Ok(false) => {
                self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
                warn!(
                    "ArbHedgeStrategy: backward publisher 未配置，无法发送对冲状态查询 strategy_id={} symbol={} request_seq={}",
                    self.strategy_id, self.symbol, request_seq
                );
                false
            }
            Err(err) => {
                self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
                warn!(
                    "ArbHedgeStrategy: 发送对冲状态查询失败 strategy_id={} symbol={} request_seq={} err={:#}",
                    self.strategy_id, self.symbol, request_seq, err
                );
                false
            }
        }
    }

    pub(super) fn trigger_hedge_query_after_opening_leg_terminal(
        &mut self,
        terminal_ts: i64,
    ) -> bool {
        let now_ts = if terminal_ts > 0 {
            terminal_ts
        } else {
            get_timestamp_us()
        };
        // Arb hedge 固定只有一套长期运行流程。这里是事件触发入口：
        // opening-leg terminal、hedge 撤单/终态释放后，如果已有 due 数量就立即补一次查询；
        // 如果 close_ts 还没到，固定由 period clock 的时间轮到期后重新拉起。
        self.try_send_due_hedge_query(now_ts, "trigger", false)
    }

    pub(super) fn trigger_lazy_taker_on_model_update(
        &mut self,
        now_ts: i64,
        model_percentile: Option<f64>,
    ) -> bool {
        if !arb_hedge_lazy_taker() {
            return false;
        }
        if !self.is_active() {
            return false;
        }
        if self.coalesce_while_hedge_query_inflight(now_ts, "lazy_model_update") {
            return false;
        }
        self.last_ret_qtl = model_percentile_to_ret_qtl(model_percentile);
        let pending_hedge_qty = self.pending_hedge_queue.net_qty();
        if pending_hedge_qty.abs() <= ARB_HEDGE_QTY_EPS {
            return false;
        }
        let due_hedge_qty = self.pending_hedge_queue.due_qty(now_ts);
        if due_hedge_qty.abs() <= ARB_HEDGE_QTY_EPS {
            return false;
        }
        let Some(mark_price) = self.mark_price() else {
            return false;
        };
        let due_hedge_usdt = Self::pending_hedge_usdt_with_mark_price(due_hedge_qty, mark_price);
        if due_hedge_usdt < ARB_HEDGE_PENDING_QUERY_MIN_USDT {
            return false;
        }
        let snapshot = PreTradeTakerDecisionModel::evaluate_global(&self.symbol, due_hedge_qty);
        match decide_due_hedge_route(false, true, snapshot.as_ref()) {
            DueHedgeRoute::Hold => {
                publish_lazy_action(
                    &self.symbol,
                    self.hedge_venue,
                    now_ts,
                    due_hedge_qty,
                    LazyTakerAction::Hold,
                );
                if let Some(snapshot) = snapshot.as_ref() {
                    debug!(
                        "ArbHedgeStrategy: strategy_id={} symbol={} lazy model update remains keep decision={:?} due_hedge_qty={:.8} due_hedge_usdt={:.8} q={:?}",
                        self.strategy_id,
                        self.symbol,
                        snapshot.decision,
                        due_hedge_qty,
                        due_hedge_usdt,
                        snapshot.percentile.or(model_percentile)
                    );
                }
                return false;
            }
            DueHedgeRoute::Query => return false,
            DueHedgeRoute::DirectTaker => {}
        }

        match snapshot.as_ref() {
            Some(snapshot) if snapshot.ready => {
                if !suppress_pre_submit_hot_path_logs() {
                    info!(
                        "ArbHedgeStrategy: strategy_id={} symbol={} lazy model update triggers taker hedge due_hedge_qty={:.8} due_hedge_usdt={:.8} q={:?} note={}",
                        self.strategy_id,
                        self.symbol,
                        due_hedge_qty,
                        due_hedge_usdt,
                        snapshot.percentile.or(model_percentile),
                        snapshot.note
                    );
                }
            }
            Some(snapshot) => {
                if !suppress_pre_submit_hot_path_logs() {
                    info!(
                        "ArbHedgeStrategy: strategy_id={} symbol={} lazy model update falls back to direct taker because model not ready due_hedge_qty={:.8} due_hedge_usdt={:.8} score={:?} q={:?} updates={} note={}",
                        self.strategy_id,
                        self.symbol,
                        due_hedge_qty,
                        due_hedge_usdt,
                        snapshot.score,
                        snapshot.percentile.or(model_percentile),
                        snapshot.update_count,
                        snapshot.note
                    );
                }
            }
            None => {
                if !suppress_pre_submit_hot_path_logs() {
                    info!(
                        "ArbHedgeStrategy: strategy_id={} symbol={} lazy model update falls back to direct taker because model state missing due_hedge_qty={:.8} due_hedge_usdt={:.8}",
                        self.strategy_id,
                        self.symbol,
                        due_hedge_qty,
                        due_hedge_usdt
                    );
                }
            }
        }
        let sent = self.send_lazy_model_taker_hedge_direct(
            now_ts,
            due_hedge_qty,
            snapshot.and_then(|snapshot| snapshot.percentile.or(model_percentile)),
        );
        if sent {
            publish_lazy_action(
                &self.symbol,
                self.hedge_venue,
                now_ts,
                due_hedge_qty,
                LazyTakerAction::Take,
            );
        }
        sent
    }

    fn try_send_due_hedge_query(
        &mut self,
        now_ts: i64,
        reason: &'static str,
        throttle_on_skip: bool,
    ) -> bool {
        if self.coalesce_while_hedge_query_inflight(now_ts, reason) {
            return false;
        }
        let pending_hedge_qty = self.pending_hedge_queue.net_qty();
        if pending_hedge_qty.abs() <= ARB_HEDGE_QTY_EPS {
            return false;
        }
        let due_hedge_qty = self.pending_hedge_queue.due_qty(now_ts);
        if due_hedge_qty.abs() <= ARB_HEDGE_QTY_EPS {
            debug!(
                "ArbHedgeStrategy: strategy_id={} symbol={} skip {} hedge query because pending hedge is not due yet pending_hedge_qty={:.8} due_hedge_qty={:.8} now_ts={}",
                self.strategy_id,
                self.symbol,
                reason,
                pending_hedge_qty,
                due_hedge_qty,
                now_ts
            );
            return false;
        }
        let Some(mark_price) = self.mark_price() else {
            if throttle_on_skip {
                self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
            }
            if !suppress_pre_submit_hot_path_logs() {
                info!(
                    "ArbHedgeStrategy: strategy_id={} symbol={} skip {} hedge query because mark_price missing pending_hedge_qty={:.8} due_hedge_qty={:.8} threshold_usdt={:.8} next_query_ts_us={}",
                    self.strategy_id,
                    self.symbol,
                    reason,
                    pending_hedge_qty,
                    due_hedge_qty,
                    ARB_HEDGE_PENDING_QUERY_MIN_USDT,
                    self.next_query_ts_us
                );
            }
            return false;
        };
        let pending_hedge_usdt =
            Self::pending_hedge_usdt_with_mark_price(pending_hedge_qty, mark_price);
        if pending_hedge_usdt < ARB_HEDGE_PENDING_QUERY_MIN_USDT {
            if throttle_on_skip {
                self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
            }
            debug!(
                "ArbHedgeStrategy: strategy_id={} symbol={} skip {} hedge query because pending hedge below threshold pending_hedge_qty={:.8} due_hedge_qty={:.8} mark_price={:.8} pending_hedge_usdt={:.8} threshold_usdt={:.8} next_query_ts_us={}",
                self.strategy_id,
                self.symbol,
                reason,
                pending_hedge_qty,
                due_hedge_qty,
                mark_price,
                pending_hedge_usdt,
                ARB_HEDGE_PENDING_QUERY_MIN_USDT,
                self.next_query_ts_us
            );
            return false;
        }
        let force_taker = arb_hedge_force_taker();
        let lazy_taker = arb_hedge_lazy_taker();
        let snapshot = if lazy_taker && !force_taker {
            PreTradeTakerDecisionModel::evaluate_global(&self.symbol, due_hedge_qty)
        } else {
            None
        };

        match decide_due_hedge_route(force_taker, lazy_taker, snapshot.as_ref()) {
            DueHedgeRoute::Hold => {
                publish_lazy_action(
                    &self.symbol,
                    self.hedge_venue,
                    now_ts,
                    due_hedge_qty,
                    LazyTakerAction::Hold,
                );
                if throttle_on_skip {
                    self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
                }
                if let Some(snapshot) = snapshot.as_ref() {
                    if !suppress_pre_submit_hot_path_logs() {
                        info!(
                            "ArbHedgeStrategy: strategy_id={} symbol={} lazy model keeps exposure decision={:?} due_hedge_qty={:.8} pending_hedge_usdt={:.8} score={:?} q={:?} updates={} note={}",
                            self.strategy_id,
                            self.symbol,
                            snapshot.decision,
                            due_hedge_qty,
                            pending_hedge_usdt,
                            snapshot.score,
                            snapshot.percentile,
                            snapshot.update_count,
                            snapshot.note
                        );
                    }
                }
                false
            }
            DueHedgeRoute::DirectTaker => {
                let sent = if force_taker {
                    self.send_force_taker_hedge_direct(now_ts, due_hedge_qty)
                } else {
                    match snapshot.as_ref() {
                        Some(snapshot) if snapshot.ready => {
                            if !suppress_pre_submit_hot_path_logs() {
                                info!(
                                    "ArbHedgeStrategy: strategy_id={} symbol={} lazy taker direct hedge due_hedge_qty={:.8} pending_hedge_usdt={:.8} q={:?} note={}",
                                    self.strategy_id,
                                    self.symbol,
                                    due_hedge_qty,
                                    pending_hedge_usdt,
                                    snapshot.percentile,
                                    snapshot.note
                                );
                            }
                        }
                        Some(snapshot) => {
                            if !suppress_pre_submit_hot_path_logs() {
                                info!(
                                    "ArbHedgeStrategy: strategy_id={} symbol={} lazy taker bypasses query because model not ready due_hedge_qty={:.8} pending_hedge_usdt={:.8} score={:?} q={:?} updates={} note={}",
                                    self.strategy_id,
                                    self.symbol,
                                    due_hedge_qty,
                                    pending_hedge_usdt,
                                    snapshot.score,
                                    snapshot.percentile,
                                    snapshot.update_count,
                                    snapshot.note
                                );
                            }
                        }
                        None => {
                            if !suppress_pre_submit_hot_path_logs() {
                                info!(
                                    "ArbHedgeStrategy: strategy_id={} symbol={} lazy taker bypasses query because model state missing due_hedge_qty={:.8} pending_hedge_usdt={:.8}",
                                    self.strategy_id,
                                    self.symbol,
                                    due_hedge_qty,
                                    pending_hedge_usdt
                                );
                            }
                        }
                    }
                    self.send_taker_hedge_direct(
                        now_ts,
                        due_hedge_qty,
                        "lazy_taker",
                        snapshot
                            .and_then(|snapshot| model_percentile_to_ret_qtl(snapshot.percentile)),
                    )
                };
                if sent {
                    if lazy_taker && !force_taker {
                        publish_lazy_action(
                            &self.symbol,
                            self.hedge_venue,
                            now_ts,
                            due_hedge_qty,
                            LazyTakerAction::Take,
                        );
                    }
                    return true;
                }
                if throttle_on_skip {
                    self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
                }
                false
            }
            DueHedgeRoute::Query => {
                if let Some((raw_qty, aligned_qty, min_qty)) =
                    self.binance_futures_due_qty_below_min(due_hedge_qty)
                {
                    if throttle_on_skip {
                        self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
                    }
                    if !suppress_pre_submit_hot_path_logs() {
                        info!(
                            "ArbHedgeStrategy: strategy_id={} symbol={} skip {} hedge query because BinanceFutures qty below min due_hedge_qty={:.8} raw_qty={:.8} aligned_qty={:.8} min_qty={:.8} next_query_ts_us={}",
                            self.strategy_id,
                            self.symbol,
                            reason,
                            due_hedge_qty,
                            raw_qty,
                            aligned_qty,
                            min_qty,
                            self.next_query_ts_us
                        );
                    }
                    return false;
                }
                if let Some((raw_contracts, aligned_contracts, min_qty_contracts)) =
                    self.gate_futures_query_due_qty_below_min(due_hedge_qty)
                {
                    if throttle_on_skip {
                        self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
                    }
                    if !suppress_pre_submit_hot_path_logs() {
                        info!(
                            "ArbHedgeStrategy: strategy_id={} symbol={} skip {} hedge query because GateFutures qty below min due_hedge_qty={:.8} raw_contracts={:.8} aligned_contracts={:.8} min_contracts={:.8} next_query_ts_us={}",
                            self.strategy_id,
                            self.symbol,
                            reason,
                            due_hedge_qty,
                            raw_contracts,
                            aligned_contracts,
                            min_qty_contracts,
                            self.next_query_ts_us
                        );
                    }
                    return false;
                }
                self.send_hedge_query(now_ts, due_hedge_qty)
            }
        }
    }

    fn compose_order_id(strategy_id: i32, seq: u32) -> i64 {
        ((strategy_id as i64) << 32) | seq as i64
    }

    fn extract_strategy_id(order_id: i64) -> i32 {
        (order_id >> 32) as i32
    }

    fn next_order_id(&mut self) -> i64 {
        self.order_seq = self.order_seq.wrapping_add(1);
        if self.order_seq == 0 {
            self.order_seq = 1;
        }
        Self::compose_order_id(self.strategy_id, self.order_seq)
    }

    fn uniform_hedge_publish_ctx(&self, client_order_id: i64) -> UniformPublishCtx {
        let meta = self.hedge_order_meta.get(&client_order_id);
        let signal_ts = meta.map(|m| m.signal_ts).unwrap_or(0);
        let price_offset = meta.map(|m| m.price_offset).unwrap_or(0.0);
        // arb hedge 的 from_key = "{绑定的 open client_order_id}|{rich from_key}"：
        // 前缀 id 与 open 腿一致（下游按第一个 '|' 切分取 id 做 JOIN），后缀带上整条 rich；
        // 没有绑定（borrow 全 None / 没拿到）按用户约定 id 写 "0" 兜底。
        let bound_open_client_order_id = meta.map(|m| m.bound_open_client_order_id).unwrap_or(0);
        let rich = meta
            .map(|m| String::from_utf8_lossy(&m.from_key).into_owned())
            .unwrap_or_default();
        UniformPublishCtx {
            signal_ts,
            from_key: format!("{bound_open_client_order_id}|{rich}").into_bytes(),
            signal_bbo: meta.and_then(|m| m.signal_bbo),
            price_offset,
        }
    }

    fn hedge_pending_qv_from_order(side: Side, base_qty: f64) -> f64 {
        match side {
            Side::Sell => base_qty.abs(),
            Side::Buy => -base_qty.abs(),
        }
    }

    pub fn handle_arb_hedge_ctx(&mut self, ctx: ArbHedgeCtx) {
        let symbol = normalize_symbol_for_internal(&ctx.get_hedging_symbol());
        self.handle_arb_hedge_ctx_with_symbol(ctx, symbol);
    }

    pub fn handle_arb_hedge_ctx_with_symbol(&mut self, ctx: ArbHedgeCtx, symbol: String) {
        let Some(inflight) = self.inflight_hedge_query else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} drop unexpected ArbHedge reply without pending query: symbol={} request_seq={}",
                self.strategy_id,
                symbol,
                ctx.request_seq
            );
            return;
        };
        if ctx.request_seq != inflight.request_seq {
            warn!(
                "ArbHedgeStrategy: strategy_id={} drop stale/duplicate ArbHedge reply: symbol={} request_seq={} expected_request_seq={}",
                self.strategy_id,
                symbol,
                ctx.request_seq,
                inflight.request_seq
            );
            return;
        }
        self.inflight_hedge_query = None;
        self.last_hedge_is_taker = Some(ctx.is_taker());
        self.last_ret_qtl = parse_return_qtl_from_from_key(&ctx.from_key);
        // taker 时 ctx.price_offset = 0；maker 时由 trade_signal 计算的偏移量
        self.last_hedge_offset = Some(ctx.price_offset);

        let Some(side) = ctx.get_side() else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedge invalid side={}",
                self.strategy_id, ctx.hedge_side
            );
            return;
        };
        if symbol.is_empty() {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedge empty symbol",
                self.strategy_id
            );
            return;
        }
        let Some(venue) = TradingVenue::from_u8(ctx.hedging_leg.venue) else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedge invalid venue={}",
                self.strategy_id, ctx.hedging_leg.venue
            );
            return;
        };
        let qty = ctx.amount_value();
        if qty <= 0.0 {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedge qty invalid symbol={} qty={:.8}",
                self.strategy_id, symbol, qty
            );
            return;
        }
        let is_taker = ctx.is_taker();
        let order_type = if is_taker {
            OrderType::Market
        } else {
            OrderType::Limit
        };
        let now_ts = get_timestamp_us();
        if self.is_bybit_oi_limit_blocked(side, now_ts) {
            self.next_query_ts_us = self.bybit_oi_limit_block_until_us;
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedge blocked by Bybit OI/position-limit throttle symbol={} side={} until_us={} request_seq={}",
                self.strategy_id,
                symbol,
                side.as_str(),
                self.bybit_oi_limit_block_until_us,
                ctx.request_seq
            );
            return;
        }
        if self.is_bitget_position_tier_limit_blocked(side, now_ts) {
            self.next_query_ts_us = self.bitget_position_tier_limit_block_until_us;
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedge blocked by Bitget position-tier throttle symbol={} side={} until_us={} request_seq={}",
                self.strategy_id,
                symbol,
                side.as_str(),
                self.bitget_position_tier_limit_block_until_us,
                ctx.request_seq
            );
            return;
        }
        let price = if is_taker { 0.0 } else { ctx.price_value() };
        if !is_taker && price <= 0.0 {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedge price invalid symbol={} price={:.8}",
                self.strategy_id, symbol, price
            );
            return;
        }

        let order_base_qty = MonitorChannel::instance().qty_to_base(venue, &symbol, qty);
        if order_base_qty <= 0.0 {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedge base qty invalid symbol={} venue={:?} qty={:.8}",
                self.strategy_id, symbol, venue, qty
            );
            return;
        }
        let signed_base_qty = signed_qty_from_side(side, order_base_qty);
        if !is_direct_taker_from_key(&ctx.from_key) {
            if let Err(err) = MonitorChannel::instance().check_arb_hedge_exposure_risk(
                &symbol,
                venue,
                signed_base_qty,
            ) {
                warn!(
                    "ArbHedgeStrategy: strategy_id={} ArbHedge exposure risk reject symbol={} venue={:?} side={:?} base_qty={:.8} err={}",
                    self.strategy_id,
                    symbol,
                    venue,
                    side,
                    order_base_qty,
                    err
                );
                return;
            }
        }
        let pending_qv = Self::hedge_pending_qv_from_order(side, order_base_qty);
        let borrowed = self.pending_hedge_queue.borrow(now_ts, pending_qv);
        // 主成分：borrowed.lots 中 qty 最大的那个对应的 open client_order_id；
        // 找不到（lot 全是 None / borrow 没拿到任何东西）时按约定用 0 兜底。
        let bound_open_client_order_id = pick_main_component_open_id(&borrowed.lots);
        if borrowed.qty + ARB_HEDGE_QTY_EPS < order_base_qty {
            let shortfall_qty = (order_base_qty - borrowed.qty).max(0.0);
            let shortfall_usdt = Self::borrow_shortfall_usdt(shortfall_qty, price, ctx.hedging_leg);
            let allow_shortfall = borrowed.qv.abs() > ARB_HEDGE_QTY_EPS
                && Self::borrow_shortfall_within_eps(shortfall_qty, price, ctx.hedging_leg);
            if !allow_shortfall {
                if borrowed.qv.abs() > ARB_HEDGE_QTY_EPS {
                    let release_price =
                        Self::hedge_leg_reference_price(price, ctx.hedging_leg).unwrap_or(0.0);
                    if bound_open_client_order_id != 0 {
                        self.pending_hedge_queue.release_with_id(
                            now_ts,
                            borrowed.qv,
                            release_price,
                            bound_open_client_order_id,
                        );
                    } else {
                        self.pending_hedge_queue
                            .release(now_ts, borrowed.qv, release_price);
                    }
                }
                warn!(
                    "ArbHedgeStrategy: strategy_id={} ArbHedge borrow insufficient symbol={} request_seq={} want_qv={:.8} borrowed_qv={:.8} shortfall_qty={:.8} shortfall_usdt={:.8} max_shortfall_usdt={:.8} pending_after={:.8}",
                    self.strategy_id,
                    symbol,
                    ctx.request_seq,
                    pending_qv,
                    borrowed.qv,
                    shortfall_qty,
                    shortfall_usdt.unwrap_or(f64::NAN),
                    ARB_HEDGE_BORROW_SHORTFALL_MAX_USDT,
                    self.pending_hedge_queue.net_qty()
                );
                return;
            }
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedge borrow shortfall tolerated symbol={} request_seq={} want_qv={:.8} borrowed_qv={:.8} shortfall_qty={:.8} shortfall_usdt={:.8} max_shortfall_usdt={:.8} pending_after={:.8}",
                self.strategy_id,
                symbol,
                ctx.request_seq,
                pending_qv,
                borrowed.qv,
                shortfall_qty,
                shortfall_usdt.unwrap_or(f64::NAN),
                ARB_HEDGE_BORROW_SHORTFALL_MAX_USDT,
                self.pending_hedge_queue.net_qty()
            );
        }

        let bitget_margin_lock_reduce_only =
            self.bitget_margin_lock_reduce_only_hedge(venue, side, order_base_qty);
        let client_order_id = self.next_order_id();
        let qty_multiplier = (order_base_qty / qty).max(1e-12);
        // egress 测度：建单即落 signal 元数据，覆盖本单后续 new/cancel 两次 egress（同归 ArbHedge 桶）。
        let quantity_qv = order_qv_from_quantized_value(ctx.amount_qv);
        let price_qv = if is_taker {
            None
        } else {
            Some(order_qv_from_quantized_value(ctx.price_qv))
        };
        MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .create_order_with_mut(
                venue,
                client_order_id,
                order_type,
                symbol.clone(),
                side,
                qty,
                price,
                bitget_margin_lock_reduce_only,
                qty_multiplier,
                false,
                |order| {
                    order.set_quantity_qv(quantity_qv);
                    if let Some(price_qv) = price_qv {
                        order.set_price_qv(price_qv);
                    }
                    order.set_signal_meta(ctx.signal_ts, SignalType::ArbHedge as u8);
                },
            );
        self.hedge_order_meta.insert(
            client_order_id,
            ArbHedgeOrderMeta {
                signal_ts: ctx.signal_ts,
                price_offset: ctx.price_offset,
                signal_bbo: signal_bbo_from_legs(None, Some(&ctx.hedging_leg)),
                borrowed_qv: borrowed.qv,
                order_base_qty,
                expire_ts: ctx.exp_time,
                next_expire_check_ts: ctx.exp_time,
                cancel_requested: false,
                bound_open_client_order_id,
                from_key: ctx.from_key.clone(),
            },
        );

        debug!(
            "ArbHedge订单已创建: strategy_id={} client_order_id={} symbol={} side={:?} type={:?} qty={:.8} mode={} request_seq={}",
            self.strategy_id,
            client_order_id,
            symbol,
            side,
            order_type,
            qty,
            if is_taker { "taker" } else { "maker" },
            ctx.request_seq
        );
        debug!(
            "ArbHedge订单已创建 detail: strategy_id={} client_order_id={} symbol={} venue={:?} side={:?} type={:?} qty={:.8} base_qty={:.8} price={:.8} mode={} request_seq={} expire_ts={}",
            self.strategy_id,
            client_order_id,
            symbol,
            venue,
            side,
            order_type,
            qty,
            order_base_qty,
            price,
            if is_taker { "taker" } else { "maker" },
            ctx.request_seq,
            ctx.exp_time
        );

        if let Err(err) =
            create_and_send_order(self.strategy_id, client_order_id, "状态对冲", &symbol)
        {
            self.cleanup_unsent_hedge_order_after_send_failure(
                client_order_id,
                now_ts,
                price.max(ctx.hedging_leg.bid0),
            );
            warn!(
                "ArbHedgeStrategy: strategy_id={} send ArbHedge order failed client_order_id={} symbol={} err={}",
                self.strategy_id, client_order_id, symbol, err
            );
            return;
        }
        if !is_taker && ctx.exp_time > 0 {
            // Arb hedge 的 maker 对冲单也有本地生命周期。这里登记到时间轮，
            // period clock 到期后会发 cancel；等撤单终态释放 pending 后，再触发
            // 下一轮状态查询，避免旧挂单无限占用 borrowed pending。
            self.schedule_hedge_order_expiry_check(client_order_id, ctx.exp_time);
        }
        if !suppress_pre_submit_hot_path_logs() {
            info!(
                "✅ ArbHedge订单已发送: strategy_id={} client_order_id={}",
                self.strategy_id, client_order_id
            );
        }
        self.schedule_order_query_watchdog(client_order_id, PendingOrderQueryReason::OrderWatchdog);
    }

    fn publish_uniform_new_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_hedge_publish_ctx(order.client_order_id);
        publish_uniform_new_order(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "ArbHedgeStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_terminal_order_with_ctx(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
        ctx: &UniformPublishCtx,
    ) {
        publish_uniform_terminal_order(
            order_update,
            order,
            prev_cumulative_filled_qty,
            ctx,
            "ArbHedgeStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_trade_order_from_order_update(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_hedge_publish_ctx(order.client_order_id);
        publish_uniform_trade_order_from_order_update(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "ArbHedgeStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_trade_order_from_order_update_with_ctx(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
        ctx: &UniformPublishCtx,
    ) {
        publish_uniform_trade_order_from_order_update(
            order_update,
            order,
            prev_cumulative_filled_qty,
            ctx,
            "ArbHedgeStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_trade_order_with_ctx(
        &self,
        trade: &dyn TradeUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
        status: OrderStatus,
        ctx: &UniformPublishCtx,
    ) {
        publish_uniform_trade_order(
            trade,
            order,
            prev_cumulative_filled_qty,
            status,
            ctx,
            "ArbHedgeStrategy",
            self.strategy_id,
        );
    }

    fn handle_expired_hedge_orders(&mut self, now_ts: i64) {
        if self.hedge_order_expiry_wheel.is_empty() {
            return;
        }

        let due_keys: Vec<i64> = self
            .hedge_order_expiry_wheel
            .keys()
            .copied()
            .take_while(|due_ts| *due_ts <= now_ts)
            .collect();
        if due_keys.is_empty() {
            return;
        }

        let mut due_order_ids = Vec::new();
        for due_ts in due_keys {
            if let Some(order_ids) = self.hedge_order_expiry_wheel.remove(&due_ts) {
                due_order_ids.extend(order_ids.into_iter().map(|order_id| (due_ts, order_id)));
            }
        }

        for (due_ts, client_order_id) in due_order_ids {
            let Some(meta) = self.hedge_order_meta.get(&client_order_id) else {
                continue;
            };
            if meta.expire_ts <= 0 || meta.next_expire_check_ts != due_ts || meta.cancel_requested {
                continue;
            }
            self.request_cancel_for_expired_hedge_order(client_order_id, now_ts);
        }
    }

    fn request_cancel_for_expired_hedge_order(&mut self, client_order_id: i64, now_ts: i64) {
        let order_snapshot = MonitorChannel::try_order_manager().and_then(|order_mgr| {
            order_mgr.borrow().get(client_order_id).map(|order| {
                (
                    order.status,
                    order.price,
                    order.symbol.clone(),
                    order.clone(),
                )
            })
        });
        let Some((status, price, symbol, order)) = order_snapshot else {
            let retry_ts = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
            if let Some(meta) = self.hedge_order_meta.get(&client_order_id) {
                warn!(
                    "ArbHedgeStrategy: strategy_id={} expired hedge order missing locally, keep borrowed pending and retry check client_order_id={} borrowed_qv={:.8} retry_ts={}",
                    self.strategy_id,
                    client_order_id,
                    meta.borrowed_qv,
                    retry_ts
                );
            }
            self.schedule_hedge_order_expiry_check(client_order_id, retry_ts);
            return;
        };

        if status.is_terminal() {
            self.record_terminal_hedge_order(client_order_id, now_ts, price);
            return;
        }

        let exchange = order.venue.trade_engine_exchange();
        match order.get_order_cancel_bytes() {
            Ok(req_bin) => {
                if let Err(err) =
                    TradeEngHub::publish_order_request_for(client_order_id, exchange, &req_bin)
                {
                    warn!(
                        "ArbHedgeStrategy: strategy_id={} expired hedge cancel publish failed, handoff orphan client_order_id={} symbol={} exchange={} err={}",
                        self.strategy_id,
                        client_order_id,
                        symbol,
                        exchange,
                        err
                    );
                    self.handoff_hedge_order_after_query_failure(
                        client_order_id,
                        "expired hedge cancel publish failed",
                    );
                    return;
                }
                if let Some(meta) = self.hedge_order_meta.get_mut(&client_order_id) {
                    meta.cancel_requested = true;
                }
                self.schedule_order_query_watchdog(
                    client_order_id,
                    PendingOrderQueryReason::CancelWatchdog,
                );
                debug!(
                    "ArbHedgeStrategy: strategy_id={} expired hedge cancel sent client_order_id={} symbol={} exchange={} expire_ts={} now_ts={}",
                    self.strategy_id,
                    client_order_id,
                    symbol,
                    exchange,
                    self.hedge_order_meta
                        .get(&client_order_id)
                        .map(|meta| meta.expire_ts)
                        .unwrap_or(0),
                    now_ts
                );
            }
            Err(err) => {
                warn!(
                    "ArbHedgeStrategy: strategy_id={} expired hedge cancel build failed, handoff orphan client_order_id={} symbol={} err={}",
                    self.strategy_id,
                    client_order_id,
                    symbol,
                    err
                );
                self.handoff_hedge_order_after_query_failure(
                    client_order_id,
                    "expired hedge cancel build failed",
                );
            }
        }
    }

    fn record_terminal_hedge_order(&mut self, client_order_id: i64, terminal_ts: i64, price: f64) {
        let order_snapshot = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id)
            .map(|order| {
                (
                    order.side,
                    order.cumulative_filled_quantity * order.qty_multiplier,
                    order.price,
                    order.status,
                )
            });
        let Some((side, filled_base_qty, order_price, status)) = order_snapshot else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} terminal hedge order missing locally, keep borrowed pending client_order_id={}",
                self.strategy_id,
                client_order_id
            );
            return;
        };
        let Some(meta) = self.hedge_order_meta.remove(&client_order_id) else {
            return;
        };
        let terminal_price = if price.is_finite() && price > 0.0 {
            price
        } else {
            order_price
        };
        self.record_hedge_order_terminal_with_borrowed(
            terminal_ts,
            side,
            meta.order_base_qty,
            filled_base_qty,
            terminal_price,
            meta.borrowed_qv,
            meta.bound_open_client_order_id,
        );
        if status != OrderExecutionStatus::Filled {
            self.trigger_hedge_query_after_pending_release(terminal_ts, "hedge_order_terminal");
        }
    }

    fn apply_order_update(&mut self, order_update: &dyn OrderUpdate) -> bool {
        let client_order_id = order_update.client_order_id();
        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let Some(current_order) = order_manager.get(client_order_id) else {
            return false;
        };
        if OrderManager::should_skip_idempotent_order_update(
            &current_order,
            order_update.status(),
            order_update.order_id(),
            order_update.cumulative_filled_quantity(),
            "ArbHedgeStrategy",
            self.strategy_id,
        )
        .is_some()
        {
            return false;
        }
        let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;
        let status = order_update.status();
        let protected_cumulative_fill =
            current_order.protected_cumulative_fill(order_update.cumulative_filled_quantity());
        let effective_cumulative_filled_qty = protected_cumulative_fill.effective_cum;
        let updated = order_manager.apply_remote_update(client_order_id, |order| match status {
            OrderStatus::New => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                if order.timestamp.create_t == 0 {
                    order.set_create_time(order_update.event_time());
                }
            }
            OrderStatus::Canceled => {
                order.status = OrderExecutionStatus::Cancelled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_end_time(order_update.event_time());
            }
            OrderStatus::Filled => {
                order.status = OrderExecutionStatus::Filled;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_end_time(order_update.event_time());
            }
            OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                order.status = OrderExecutionStatus::Rejected;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
                order.set_end_time(order_update.event_time());
            }
            OrderStatus::PartiallyFilled => {
                order.status = OrderExecutionStatus::Create;
                order.set_exchange_order_id(order_update.order_id());
                order.cumulative_filled_quantity = effective_cumulative_filled_qty;
            }
        });
        drop(order_manager);
        if !updated {
            return false;
        }
        let order_snapshot = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id)
            .map(|order| (order, self.uniform_hedge_publish_ctx(client_order_id)));
        if let Some((order, _)) = order_snapshot.as_ref() {
            if status == OrderStatus::New {
                self.publish_uniform_new_order(order_update, &order, prev_cumulative_filled_qty);
            } else if !status.is_finished()
                && matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled)
            {
                self.publish_uniform_trade_order_from_order_update(
                    order_update,
                    &order,
                    prev_cumulative_filled_qty,
                );
            }
        }
        if status.is_finished() {
            self.clear_order_query_state(client_order_id);
            self.record_terminal_hedge_order(
                client_order_id,
                order_update.event_time(),
                order_update.price(),
            );
        } else {
            self.clear_live_order_query_state(client_order_id);
            self.reschedule_expired_hedge_cancel_if_still_live(
                client_order_id,
                order_update.event_time(),
                "order_update_non_terminal",
            );
        }
        if let Some((order, uniform_ctx)) = order_snapshot.as_ref() {
            if matches!(
                status,
                OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::ExpiredInMatch
            ) {
                self.publish_uniform_terminal_order_with_ctx(
                    order_update,
                    &order,
                    prev_cumulative_filled_qty,
                    uniform_ctx,
                );
            } else if status == OrderStatus::Filled {
                self.publish_uniform_trade_order_from_order_update_with_ctx(
                    order_update,
                    &order,
                    prev_cumulative_filled_qty,
                    uniform_ctx,
                );
            }
        }
        true
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) -> bool {
        let client_order_id = trade.client_order_id();
        let Some(status) = trade.order_status() else {
            return false;
        };
        if !matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled) {
            return false;
        }
        let order_mgr = MonitorChannel::instance().order_manager();
        let mut order_manager = order_mgr.borrow_mut();
        let Some(current_order) = order_manager.get(client_order_id) else {
            return false;
        };
        let prev_cumulative_filled_qty = current_order.cumulative_filled_quantity;
        if OrderManager::should_skip_idempotent_trade_update(
            &current_order,
            status,
            trade.cumulative_filled_quantity(),
            trade.event_time(),
            "ArbHedgeStrategy",
            self.strategy_id,
        )
        .is_some()
        {
            return false;
        }
        let updated = order_manager.apply_remote_update(client_order_id, |order| {
            order.cumulative_filled_quantity = trade.cumulative_filled_quantity();
            order.set_exchange_order_id(trade.order_id());
            if trade.price() > 0.0 {
                order.price = trade.price();
            }
            order.status = if status == OrderStatus::Filled {
                OrderExecutionStatus::Filled
            } else {
                OrderExecutionStatus::Create
            };
            if status == OrderStatus::Filled {
                order.set_end_time(trade.event_time());
            }
        });
        if !updated {
            return false;
        }
        let order_snapshot = order_manager
            .get(client_order_id)
            .map(|order| (order, self.uniform_hedge_publish_ctx(client_order_id)));
        drop(order_manager);
        if status == OrderStatus::Filled {
            self.clear_order_query_state(client_order_id);
            self.record_terminal_hedge_order(client_order_id, trade.event_time(), trade.price());
        } else {
            self.clear_live_order_query_state(client_order_id);
            self.reschedule_expired_hedge_cancel_if_still_live(
                client_order_id,
                trade.event_time(),
                "trade_update_partial_fill",
            );
        }
        if let Some((order, uniform_ctx)) = order_snapshot.as_ref() {
            self.publish_uniform_trade_order_with_ctx(
                trade,
                &order,
                prev_cumulative_filled_qty,
                status,
                uniform_ctx,
            );
        }
        true
    }

    fn record_hedge_order_terminal_with_borrowed(
        &mut self,
        terminal_ts: i64,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
        borrowed_qv: f64,
        bound_open_client_order_id: i64,
    ) -> bool {
        let order_base_qty = order_base_qty.abs();
        let filled_base_qty = filled_base_qty.abs();
        let borrowed_base_qty = borrowed_qv.abs().min(order_base_qty);
        if order_base_qty <= TERMINAL_QTY_EPS && filled_base_qty <= TERMINAL_QTY_EPS {
            return false;
        }
        let signed_base_qty = signed_qty_from_side(side, filled_base_qty);
        let unfilled_base_qty = (order_base_qty - filled_base_qty).max(0.0);
        let uncovered_borrowed_qty =
            (borrowed_base_qty - filled_base_qty.min(borrowed_base_qty)).max(0.0);
        let pending_release_qv = if borrowed_qv < -TERMINAL_QTY_EPS {
            -uncovered_borrowed_qty
        } else if borrowed_qv > TERMINAL_QTY_EPS {
            uncovered_borrowed_qty
        } else {
            Self::hedge_pending_qv_from_order(side, uncovered_borrowed_qty)
        };
        let released_qv = self.release_borrowed_with_bound_id(
            terminal_ts,
            pending_release_qv,
            price,
            bound_open_client_order_id,
        );
        if filled_base_qty > TERMINAL_QTY_EPS {
            self.net_qty_queue
                .apply_fill(terminal_ts, signed_base_qty, price);
        }
        if !suppress_pre_submit_hot_path_logs() {
            info!(
                "ArbHedgeRecord: strategy_id={} symbol={} leg=hedge side={:?} order_base_qty={:.8} filled_base_qty={:.8} unfilled_base_qty={:.8} borrowed_qv={:.8} released_pending={:.8} qv={:.8} price={:.8} terminal_ts={} bound_open_co_id={} net={:.8} pending_hedge={:.8}",
                self.strategy_id,
                self.symbol,
                side,
                order_base_qty,
                filled_base_qty,
                unfilled_base_qty,
                borrowed_qv,
                released_qv,
                signed_base_qty,
                price,
                terminal_ts,
                bound_open_client_order_id,
                self.net_qty_queue.net_qty(),
                self.pending_hedge_queue.net_qty()
            );
        }
        true
    }

    /// 账户级保证金不足应急动作：锁住新增 ArbOpen，并撤掉当前全部 ArbOpen 挂单；
    /// 不再自动下调 max_leverage。
    /// 进入这里前调用方已经做了冷却节流（5s）。
    fn handle_insufficient_margin_emergency(
        &mut self,
        now_ts: i64,
        hedge_side: Option<Side>,
        error_code: i32,
    ) {
        let Some(hedge_side) = hedge_side else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} INSUFFICIENT_MARGIN emergency skipped: hedge_side unknown (order missing)",
                self.strategy_id, self.symbol
            );
            return;
        };
        // open 方向永远是 hedge 反向：long-spot/short-perp ↔ short-spot/long-perp 都自然成立。
        let open_side = match hedge_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let binance_direction_lock =
            self.register_binance_insufficient_margin_direction_lock(now_ts, open_side, error_code);

        let is_binance_pm_fr = self.open_venue == TradingVenue::BinanceMargin
            && self.hedge_venue == TradingVenue::BinanceFutures
            && MonitorChannel::try_order_manager()
                .is_some_and(|mgr| !mgr.borrow().binance_is_standard());
        let is_okex_unified_fr = self.open_venue == TradingVenue::OkexMargin
            && self.hedge_venue == TradingVenue::OkexFutures
            && error_code == 51008;
        let is_gate_unified_fr = self.open_venue == TradingVenue::GateMargin
            && self.hedge_venue == TradingVenue::GateFutures
            && matches!(
                error_code,
                gate::INITIAL_MARGIN_TOO_LOW
                    | gate::MARGIN_NOT_ENOUGH
                    | gate::POSITION_MARGIN_TOO_LOW
            );
        let is_bitget_unified_fr = self.open_venue == TradingVenue::BitgetMargin
            && self.hedge_venue == TradingVenue::BitgetFutures;
        let account_open_block = if is_binance_pm_fr {
            register_account_open_block(
                AccountOpenBlockReason::BinancePmInsufficientMargin,
                error_code,
            );
            self.cancel_all_arb_open_orders(now_ts, "insufficient_margin_account_open_block");
            "binance_pm_insufficient_margin"
        } else if is_okex_unified_fr {
            register_account_open_block(
                AccountOpenBlockReason::OkexUnifiedInsufficientMargin,
                error_code,
            );
            self.cancel_all_arb_open_orders(now_ts, "okex_insufficient_margin_account_open_block");
            "okex_unified_insufficient_margin"
        } else if is_gate_unified_fr {
            register_account_open_block(
                AccountOpenBlockReason::GateUnifiedInsufficientMargin,
                error_code,
            );
            self.cancel_all_arb_open_orders(now_ts, "gate_insufficient_margin_account_open_block");
            "gate_unified_insufficient_margin"
        } else if is_bitget_unified_fr {
            register_account_open_block(
                AccountOpenBlockReason::BitgetUnifiedInsufficientMargin,
                error_code,
            );
            self.cancel_all_arb_open_orders(
                now_ts,
                "bitget_insufficient_margin_account_open_block",
            );
            "bitget_unified_insufficient_margin"
        } else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} INSUFFICIENT_MARGIN emergency has no account block for open_venue={:?} hedge_venue={:?}",
                self.strategy_id, self.symbol, self.open_venue, self.hedge_venue
            );
            "none"
        };

        warn!(
            "ArbHedgeStrategy: strategy_id={} symbol={} INSUFFICIENT_MARGIN emergency triggered: hedge_side={:?} open_side={:?} code={} binance_direction_lock={} account_open_block={}",
            self.strategy_id,
            self.symbol,
            hedge_side,
            open_side,
            error_code,
            binance_direction_lock,
            account_open_block
        );
    }

    fn register_binance_insufficient_margin_direction_lock(
        &mut self,
        now_ts: i64,
        open_side: Side,
        error_code: i32,
    ) -> bool {
        if self.hedge_venue != TradingVenue::BinanceFutures {
            return false;
        }

        let registered = register_binance_futures_margin_signal_throttle_for_mode(
            &self.symbol,
            open_side,
            error_code,
            MonitorChannel::instance().arb_mode(),
        );
        if !registered {
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} Binance insufficient-margin direction lock rejected open_side={:?} code={}",
                self.strategy_id, self.symbol, open_side, error_code
            );
            return false;
        }

        let strategy_mgr_handle = MonitorChannel::instance().strategy_mgr();
        let ids: Vec<i32> = strategy_mgr_handle
            .borrow()
            .arb_open_strategy_ids_by_symbol_and_side(&self.symbol, open_side);
        for sid in &ids {
            let mut mgr = strategy_mgr_handle.borrow_mut();
            mgr.cancel_arb_open_by_id(
                *sid,
                open_side,
                "binance_insufficient_margin_direction_lock",
                now_ts,
            );
        }
        warn!(
            "ArbHedgeStrategy: strategy_id={} symbol={} Binance insufficient-margin direction lock registered open_side={:?} code={} block_for_s={} cancel_open_count={}",
            self.strategy_id,
            self.symbol,
            open_side,
            error_code,
            SIGNAL_THROTTLE_TTL_US / 1_000_000,
            ids.len()
        );
        true
    }

    fn cancel_all_arb_open_orders(&mut self, now_ts: i64, reason: &'static str) {
        let strategy_mgr_handle = MonitorChannel::instance().strategy_mgr();
        let ids_and_sides: Vec<(i32, Side)> = strategy_mgr_handle
            .borrow()
            .all_arb_open_strategy_ids_and_sides();
        if ids_and_sides.is_empty() {
            debug!(
                "ArbHedgeStrategy: strategy_id={} symbol={} no live ArbOpen to cancel for account open block",
                self.strategy_id, self.symbol
            );
            return;
        }
        for (sid, side) in &ids_and_sides {
            let mut mgr = strategy_mgr_handle.borrow_mut();
            mgr.cancel_arb_open_by_id(*sid, *side, reason, now_ts);
        }
        if !suppress_pre_submit_hot_path_logs() {
            info!(
                "ArbHedgeStrategy: strategy_id={} symbol={} INSUFFICIENT_MARGIN account open block cancel dispatched count={}",
                self.strategy_id,
                self.symbol,
                ids_and_sides.len()
            );
        }
    }

    fn is_bybit_oi_limit_blocked(&self, hedge_side: Side, now_ts: i64) -> bool {
        self.hedge_venue == TradingVenue::BybitFutures
            && self.bybit_oi_limit_block_side == Some(hedge_side)
            && now_ts < self.bybit_oi_limit_block_until_us
    }

    fn is_bitget_position_tier_limit_blocked(&self, hedge_side: Side, now_ts: i64) -> bool {
        self.hedge_venue == TradingVenue::BitgetFutures
            && self.bitget_position_tier_limit_block_side == Some(hedge_side)
            && now_ts < self.bitget_position_tier_limit_block_until_us
    }

    fn register_bybit_open_interest_position_limit_throttle(
        &mut self,
        now_ts: i64,
        hedge_side: Option<Side>,
        error_code: i32,
    ) {
        self.register_bybit_hedge_open_failure_throttle(
            now_ts,
            hedge_side,
            error_code,
            "Bybit OI/position-limit",
            "bybit_open_interest_position_limit",
        );
    }

    fn register_bybit_collateral_not_enabled_throttle(
        &mut self,
        now_ts: i64,
        hedge_side: Option<Side>,
        error_code: i32,
    ) {
        self.register_bybit_hedge_open_failure_throttle(
            now_ts,
            hedge_side,
            error_code,
            "Bybit collateral-not-enabled",
            "bybit_collateral_not_enabled",
        );
    }

    fn register_bybit_hedge_open_failure_throttle(
        &mut self,
        now_ts: i64,
        hedge_side: Option<Side>,
        error_code: i32,
        log_label: &'static str,
        cancel_reason: &'static str,
    ) {
        let Some(hedge_side) = hedge_side else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} {} throttle skipped: hedge_side unknown",
                self.strategy_id, self.symbol, log_label
            );
            return;
        };
        let open_side = match hedge_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        self.bybit_oi_limit_block_side = Some(hedge_side);
        self.bybit_oi_limit_block_until_us =
            now_ts.saturating_add(SIGNAL_THROTTLE_TTL_US.max(ARB_HEDGE_QUERY_INTERVAL_US));
        self.next_query_ts_us = self.bybit_oi_limit_block_until_us;

        let open_registered = register_signal_throttle_for_mode(
            &self.symbol,
            open_side,
            Some(Exchange::Bybit),
            error_code,
            MonitorChannel::instance().arb_mode(),
        );
        let strategy_mgr_handle = MonitorChannel::instance().strategy_mgr();
        let ids: Vec<i32> = strategy_mgr_handle
            .borrow()
            .arb_open_strategy_ids_by_symbol_and_side(&self.symbol, open_side);
        for sid in &ids {
            let mut mgr = strategy_mgr_handle.borrow_mut();
            mgr.cancel_arb_open_by_id(*sid, open_side, cancel_reason, now_ts);
        }
        warn!(
            "ArbHedgeStrategy: strategy_id={} symbol={} {} throttle registered hedge_side={:?} open_side={:?} code={} open_registered={} hedge_block_until_us={} cancel_open_count={}",
            self.strategy_id,
            self.symbol,
            log_label,
            hedge_side,
            open_side,
            error_code,
            open_registered,
            self.bybit_oi_limit_block_until_us,
            ids.len()
        );
    }

    fn register_bitget_position_tier_limit_throttle(
        &mut self,
        now_ts: i64,
        hedge_side: Option<Side>,
        error_code: i32,
    ) {
        let Some(hedge_side) = hedge_side else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} Bitget position-tier throttle skipped: hedge_side unknown",
                self.strategy_id, self.symbol
            );
            return;
        };
        let open_side = match hedge_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        self.bitget_position_tier_limit_block_side = Some(hedge_side);
        self.bitget_position_tier_limit_block_until_us =
            now_ts.saturating_add(SIGNAL_THROTTLE_TTL_US.max(ARB_HEDGE_QUERY_INTERVAL_US));
        self.next_query_ts_us = self.bitget_position_tier_limit_block_until_us;

        let open_registered = register_signal_throttle_for_mode(
            &self.symbol,
            open_side,
            Some(Exchange::Bitget),
            error_code,
            MonitorChannel::instance().arb_mode(),
        );
        let strategy_mgr_handle = MonitorChannel::instance().strategy_mgr();
        let ids: Vec<i32> = strategy_mgr_handle
            .borrow()
            .arb_open_strategy_ids_by_symbol_and_side(&self.symbol, open_side);
        for sid in &ids {
            let mut mgr = strategy_mgr_handle.borrow_mut();
            mgr.cancel_arb_open_by_id(*sid, open_side, "bitget_position_tier_limit", now_ts);
        }
        warn!(
            "ArbHedgeStrategy: strategy_id={} symbol={} Bitget position-tier throttle registered hedge_side={:?} open_side={:?} code={} open_registered={} hedge_block_until_us={} cancel_open_count={}",
            self.strategy_id,
            self.symbol,
            hedge_side,
            open_side,
            error_code,
            open_registered,
            self.bitget_position_tier_limit_block_until_us,
            ids.len()
        );
    }
}

impl HedgeOrderReconcileCommon for ArbHedgeStrategy {
    fn hedge_reconcile_strategy_name(&self) -> &'static str {
        "ArbHedge"
    }

    fn hedge_reconcile_strategy_id(&self) -> i32 {
        self.strategy_id
    }

    fn hedge_reconcile_state(&self) -> &HedgeOrderReconcileState {
        &self.order_reconcile_state
    }

    fn hedge_reconcile_state_mut(&mut self) -> &mut HedgeOrderReconcileState {
        &mut self.order_reconcile_state
    }

    fn is_hedge_order_tracked(&self, client_order_id: i64) -> bool {
        self.hedge_order_meta.contains_key(&client_order_id)
    }

    fn handoff_hedge_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        reason: &str,
    ) -> bool {
        warn!(
            "ArbHedgeReconcile: strategy_id={} orphan_handoff_start reason={} {}",
            self.strategy_id,
            reason,
            self.hedge_order_trace_snapshot(client_order_id)
        );
        let handoff = OrphanHandoff {
            client_order_id,
            source_strategy_id: self.strategy_id,
            source_kind: OrphanSourceKind::Hedge,
            uniform_ctx: self.uniform_hedge_publish_ctx(client_order_id),
            reason: reason.to_string(),
        };
        let Some(orphan_mgr) = MonitorChannel::try_orphan_strategy_mgr() else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} arb orphan manager unavailable client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
            return false;
        };
        let adopted = orphan_mgr
            .borrow_mut()
            .adopt_orphan_order_id(OrphanStrategyRole::Arb, &handoff);
        if !adopted {
            warn!(
                "ArbHedgeStrategy: strategy_id={} arb orphan handoff rejected client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
            return false;
        }
        self.clear_order_query_state(client_order_id);
        if let Some(meta) = self.hedge_order_meta.remove(&client_order_id) {
            self.orphaned_hedge_order_meta.insert(client_order_id, meta);
        } else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} orphan handoff adopted but hedge meta missing client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
        }
        warn!(
            "ArbHedgeStrategy: strategy_id={} handoff hedge order to arb orphan adopted: client_order_id={} reason={}",
            self.strategy_id, client_order_id, reason
        );
        true
    }

    fn handle_hedge_open_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        let now_ts = get_timestamp_us();
        // 同时取出 hedge 失败那笔单：Gate 风控降级和 51008 应急都依赖原订单字段。
        let order_snapshot = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id);
        if order_snapshot.as_ref().is_some_and(|order| {
            self.retry_gate_market_forbidden_reduce_only(response, client_order_id, order, now_ts)
        }) {
            return;
        }
        let is_gate_market_forbidden_reduce_only_failure =
            order_snapshot.as_ref().is_some_and(|order| {
                Self::is_gate_market_forbidden_reduce_only_failure(response, order)
            });
        let is_insufficient_margin = response.is_insufficient_margin();
        let is_bybit_open_interest_position_limit =
            response.is_bybit_open_interest_position_limit();
        let is_bybit_collateral_not_enabled = response.is_bybit_collateral_not_enabled();
        let is_bybit_internal_system_error = response.is_bybit_internal_system_error();
        let is_bitget_position_tier_limit = response.is_bitget_position_tier_limit_exceeded();
        let is_bitget_max_possible_leverage = response.is_bitget_max_possible_leverage_exceeded();
        let is_bitget_futures_open_limit =
            is_bitget_position_tier_limit || is_bitget_max_possible_leverage;
        if is_bybit_open_interest_position_limit {
            self.register_bybit_open_interest_position_limit_throttle(
                now_ts,
                order_snapshot.as_ref().map(|order| order.side),
                response.error_code(),
            );
        }
        if is_bybit_collateral_not_enabled {
            self.register_bybit_collateral_not_enabled_throttle(
                now_ts,
                order_snapshot.as_ref().map(|order| order.side),
                response.error_code(),
            );
        }
        if is_bybit_internal_system_error {
            register_bybit_internal_system_open_block(response.error_code());
            self.cancel_all_arb_open_orders(now_ts, "bybit_internal_system_account_open_block");
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} Bybit internal system error account-wide open block registered code={} block_for_s={}",
                self.strategy_id,
                self.symbol,
                response.error_code(),
                BYBIT_INTERNAL_SYSTEM_OPEN_BLOCK_TTL_US / 1_000_000
            );
        }
        if is_bitget_futures_open_limit {
            self.register_bitget_position_tier_limit_throttle(
                now_ts,
                order_snapshot.as_ref().map(|order| order.side),
                response.error_code(),
            );
        }
        if let Some(meta) = self.hedge_order_meta.remove(&client_order_id) {
            let release_price = order_snapshot
                .as_ref()
                .map(|order| order.price)
                .filter(|price| price.is_finite() && *price > 0.0)
                .unwrap_or(0.0);
            self.release_borrowed_with_bound_id(
                now_ts,
                meta.borrowed_qv,
                release_price,
                meta.bound_open_client_order_id,
            );
            // 保证金已经被打满时，立刻重发等同于扔进死循环；
            // Binance PM/FR 走账户级 open block + 撤全部 ArbOpen，并跳过普通 trigger 重发。
            // 1Hz period_clock 兜底保证 pending 不会永远卡住。
            if is_insufficient_margin {
                if now_ts.saturating_sub(self.last_insufficient_margin_action_ts)
                    >= ARB_HEDGE_INSUFFICIENT_MARGIN_COOLDOWN_US
                {
                    self.last_insufficient_margin_action_ts = now_ts;
                    let hedge_side = order_snapshot.as_ref().map(|order| order.side);
                    self.handle_insufficient_margin_emergency(
                        now_ts,
                        hedge_side,
                        response.error_code(),
                    );
                }
                self.next_query_ts_us =
                    now_ts.saturating_add(ARB_HEDGE_INSUFFICIENT_MARGIN_COOLDOWN_US);
            } else if is_bybit_open_interest_position_limit || is_bybit_collateral_not_enabled {
                self.next_query_ts_us = self.bybit_oi_limit_block_until_us;
            } else if is_bybit_internal_system_error {
                self.next_query_ts_us =
                    now_ts.saturating_add(BYBIT_INTERNAL_SYSTEM_OPEN_BLOCK_TTL_US);
            } else if is_bitget_futures_open_limit
                && self.bitget_position_tier_limit_block_until_us > now_ts
            {
                self.next_query_ts_us = self.bitget_position_tier_limit_block_until_us;
            } else if is_gate_market_forbidden_reduce_only_failure {
                self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
            } else {
                self.trigger_hedge_query_after_pending_release(now_ts, "hedge_open_failed");
            }
        }
        self.clear_order_query_state(client_order_id);
        if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let _ = order_mgr.borrow_mut().remove(client_order_id);
        }
        warn!(
            "ArbHedgeStrategy: strategy_id={} hedge open failed: req_type={} status={} code={}({}) client_order_id={} symbol={}{}",
            self.strategy_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id,
            order_snapshot
                .as_ref()
                .map(|order| order.symbol.as_str())
                .unwrap_or(""),
            if is_insufficient_margin {
                " [INSUFFICIENT_MARGIN]"
            } else if is_bybit_open_interest_position_limit {
                " [BYBIT_OPEN_INTEREST_POSITION_LIMIT]"
            } else if is_bybit_collateral_not_enabled {
                " [BYBIT_COLLATERAL_NOT_ENABLED]"
            } else if is_bybit_internal_system_error {
                " [BYBIT_INTERNAL_SYSTEM_OPEN_BLOCK]"
            } else if is_bitget_position_tier_limit {
                " [BITGET_POSITION_TIER_LIMIT]"
            } else if is_bitget_max_possible_leverage {
                " [BITGET_MAX_POSSIBLE_LEVERAGE]"
            } else if is_gate_market_forbidden_reduce_only_failure {
                " [GATE_REDUCE_ONLY_RETRY_FAILED]"
            } else {
                ""
            }
        );
    }
}

impl Strategy for ArbHedgeStrategy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.strategy_id
            && self.hedge_order_meta.contains_key(&order_id)
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match signal.signal_type {
            SignalType::ArbHedge => match ArbHedgeCtx::from_slice(signal.context.as_ref()) {
                Ok(ctx) => self.handle_arb_hedge_ctx(ctx),
                Err(err) => warn!(
                    "ArbHedgeStrategy: strategy_id={} decode ArbHedge failed err={}",
                    self.strategy_id, err
                ),
            },
            _ => {
                debug!(
                    "ArbHedgeStrategy: strategy_id={} ignore signal {:?}",
                    self.strategy_id, signal.signal_type
                );
            }
        }
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        let should_persist = ArbHedgeStrategy::apply_order_update(self, update);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_order_update(update));
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let should_persist = ArbHedgeStrategy::apply_trade_update(self, trade);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_trade_update(trade));
        }
    }

    fn apply_trade_engine_response(&mut self, response: &dyn TradeEngineResponse) {
        if self.handle_gate_futures_open_ack_unknown(response) {
            return;
        }
        self.apply_hedge_trade_engine_response_common(response);
    }

    fn handle_period_clock(&mut self, current_tp: i64) {
        let now_ts = if current_tp > 0 {
            current_tp
        } else {
            get_timestamp_us()
        };
        if !self.is_active() {
            return;
        }
        self.handle_order_query_watchdogs();
        self.handle_expired_hedge_orders(now_ts);
        // period clock 是 close_ts 的时间轮触发器：即使刚开仓时 trigger 因 due=0
        // 没有发 query，pending_hedge_queue 到期后也必须在这里重新触发状态查询。
        if self.next_query_ts_us > 0 && now_ts < self.next_query_ts_us {
            return;
        }
        self.try_send_due_hedge_query(now_ts, "period_clock", true);
    }

    fn is_active(&self) -> bool {
        self.alive_flag
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }

    fn has_order_terminal_recorder(&self) -> bool {
        true
    }

    fn order_terminal_recorder_mut(&mut self) -> Option<&mut dyn OrderTerminalRecorder> {
        Some(self)
    }
}

impl OrderTerminalRecorder for ArbHedgeStrategy {
    fn record_open_order_terminal(
        &mut self,
        terminal_ts: i64,
        side: Side,
        _order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
        close_ts: i64,
        open_client_order_id: i64,
    ) -> bool {
        let filled_base_qty = filled_base_qty.abs();
        if filled_base_qty <= TERMINAL_QTY_EPS {
            return false;
        }

        let outstanding_before = self.outstanding_hedge_work_qv();
        let signed_base_qty = signed_qty_from_side(side, filled_base_qty);
        self.net_qty_queue
            .apply_fill(terminal_ts, signed_base_qty, price);

        let target_outstanding = self.target_hedge_work_qv();
        let hedge_work_delta = target_outstanding - outstanding_before;
        if hedge_work_delta.abs() > TERMINAL_QTY_EPS {
            self.pending_hedge_queue.upsert_open_lot(
                terminal_ts,
                close_ts,
                hedge_work_delta,
                price,
                open_client_order_id,
            );
        }
        if log::log_enabled!(log::Level::Debug) {
            let pending_after = self.pending_hedge_queue.net_qty();
            let borrowed_after = self.borrowed_hedge_qv();
            debug!(
                "ArbHedgeRecord: strategy_id={} symbol={} leg=opening side={:?} filled_base_qty={:.8} hedge_work_delta={:.8} terminal_ts={} close_ts={} open_co_id={} pending_hedge={:.8} borrowed_hedge={:.8}",
                self.strategy_id,
                self.symbol,
                side,
                filled_base_qty,
                hedge_work_delta,
                terminal_ts,
                close_ts,
                open_client_order_id,
                pending_after,
                borrowed_after
            );
        }

        // opening-leg terminal 会尝试立即触发一次状态查询，但它只负责"已经 due 的量"。
        // 如果 close_ts 还没到，trigger 会跳过，后续由 ArbHedgeStrategy::handle_period_clock
        // 作为 close_ts 时间轮在到期后重新拉起 query。
        self.trigger_hedge_query_after_opening_leg_terminal(terminal_ts);
        true
    }

    fn record_hedge_order_terminal(
        &mut self,
        terminal_ts: i64,
        side: Side,
        order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
        bound_open_client_order_id: i64,
        hedge_client_order_id: i64,
    ) -> bool {
        if hedge_client_order_id > 0 {
            if let Some(meta) = self
                .orphaned_hedge_order_meta
                .remove(&hedge_client_order_id)
            {
                return self.record_hedge_order_terminal_with_borrowed(
                    terminal_ts,
                    side,
                    meta.order_base_qty,
                    filled_base_qty,
                    price,
                    meta.borrowed_qv,
                    meta.bound_open_client_order_id,
                );
            }
            if self.hedge_order_meta.contains_key(&hedge_client_order_id) {
                warn!(
                    "ArbHedgeStrategy: strategy_id={} hedge terminal recorder received live hedge order via orphan path client_order_id={}",
                    self.strategy_id, hedge_client_order_id
                );
            }
        }
        let borrowed_qv = Self::hedge_pending_qv_from_order(side, order_base_qty);
        self.record_hedge_order_terminal_with_borrowed(
            terminal_ts,
            side,
            order_base_qty,
            filled_base_qty,
            price,
            borrowed_qv,
            bound_open_client_order_id,
        )
    }
}

/// 在 borrow 返回的 lot 列表里挑"主成分"：按 qty 最大的那个 open_client_order_id 绑定；
/// qty 相等时取 close_ts 较早者；borrow 没拿到任何带 id 的 lot 时返回 0（按用户约定的兜底）。
fn pick_main_component_open_id(lots: &[TimedNetQtyLot]) -> i64 {
    lots.iter()
        .filter_map(|lot| {
            lot.open_client_order_id
                .map(|id| (id, lot.qty, lot.close_ts))
        })
        .max_by(|(_, qty_a, close_a), (_, qty_b, close_b)| {
            qty_a
                .partial_cmp(qty_b)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| close_b.cmp(close_a))
        })
        .map(|(id, _, _)| id)
        .unwrap_or(0)
}

fn create_and_send_order(
    strategy_id: i32,
    client_order_id: i64,
    order_type_str: &str,
    symbol: &str,
) -> Result<(), String> {
    let order = MonitorChannel::instance()
        .order_manager()
        .borrow_mut()
        .get(client_order_id);
    if let Some(order) = order.as_ref() {
        let exchange = order.venue.trade_engine_exchange();
        match order.get_order_request_bytes() {
            Ok(req_bin) => {
                let now_us = get_timestamp_us();
                let params = PreTradeParamsLoader::instance();
                if let Err(e) = OrderRateLimiter::check_limit(
                    OrderRateBucket::ArbHedge,
                    params.arb_hedge_order_rate_limit_per_min(),
                    params.arb_hedge_order_rate_limit_10s(),
                    now_us,
                ) {
                    log_order_rate_limit_summary(
                        "ArbHedgeStrategy",
                        Some(strategy_id),
                        OrderRateBucket::ArbHedge,
                        symbol,
                        &e,
                    );
                    return Err(format!("对冲下单频率风控触发: {}", e));
                }
                if let Err(e) =
                    TradeEngHub::publish_order_request_for(client_order_id, exchange, &req_bin)
                {
                    error!(
                        "ArbHedgeStrategy: strategy_id={} symbol={} exchange={} 推送{}订单失败: {}",
                        strategy_id, symbol, exchange, order_type_str, e
                    );
                    return Err(format!("推送{}订单失败: {}", order_type_str, e));
                }
                let stats =
                    OrderRateLimiter::record(OrderRateBucket::ArbHedge, client_order_id, now_us);
                debug!(
                    "ArbHedgeStrategy: strategy_id={} {} order action recorded client_order_id={} count_10s={} count_1m={}",
                    strategy_id, order_type_str, client_order_id, stats.count_10s, stats.count_1m
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "ArbHedgeStrategy: strategy_id={} symbol={} 获取{}订单请求字节失败: {}",
                    strategy_id, symbol, order_type_str, e
                );
                Err(format!("获取{}订单请求字节失败: {}", order_type_str, e))
            }
        }
    } else {
        error!(
            "ArbHedgeStrategy: strategy_id={} symbol={} 未找到创建的{}订单 client_order_id={}",
            strategy_id, symbol, order_type_str, client_order_id
        );
        Err(format!("未找到创建的{}订单", order_type_str))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_direct_taker_from_key, decide_due_hedge_route, model_percentile_to_ret_qtl,
        pick_main_component_open_id, ArbHedgeOrderMeta, ArbHedgeStrategy, DueHedgeRoute,
        ARB_HEDGE_QUERY_INTERVAL_US, ARB_HEDGE_QUERY_TIMEOUT_US,
    };
    use crate::strategy::manager::{OrderTerminalRecorder, Strategy};
    use crate::strategy::net_qty_queue::TimedNetQtyLot;
    use order_common::trade_error_code::gate;
    use order_common::TradingVenue;
    use order_common::{Order, OrderType, Side, TradeEngineResponseMessage, TradeRequestType};
    use runtime_common::exchange::Exchange;
    use signal_common::hedge_signal::ArbHedgeCtx;

    const OPEN_ID_A: i64 = 1001;
    const OPEN_ID_B: i64 = 1002;

    #[test]
    fn gate_futures_504_open_ack_is_unknown_for_arb_hedge() {
        let response = TradeEngineResponseMessage::new(
            504,
            TradeRequestType::GateFuturesNewOrder as u32,
            1,
            123,
            0,
        );

        assert!(ArbHedgeStrategy::is_gate_futures_open_ack_unknown(
            &response
        ));
    }

    #[test]
    fn only_gate_futures_504_open_ack_is_unknown_for_arb_hedge() {
        let gate_margin_504 = TradeEngineResponseMessage::new(
            504,
            TradeRequestType::GateUnifiedNewOrder as u32,
            1,
            123,
            0,
        );
        let gate_futures_400 = TradeEngineResponseMessage::new(
            400,
            TradeRequestType::GateFuturesNewOrder as u32,
            1,
            123,
            0,
        );

        assert!(!ArbHedgeStrategy::is_gate_futures_open_ack_unknown(
            &gate_margin_504
        ));
        assert!(!ArbHedgeStrategy::is_gate_futures_open_ack_unknown(
            &gate_futures_400
        ));
    }

    #[test]
    fn gate_market_forbidden_retries_only_non_reduce_only_futures_market_order() {
        let response = TradeEngineResponseMessage::new(
            403,
            TradeRequestType::GateFuturesNewOrder as u32,
            Exchange::Gate as u32,
            123,
            gate::RISK_CHECK_MARKET_FORBIDDEN,
        );
        let order = Order::new(
            TradingVenue::GateFutures,
            123,
            OrderType::Market,
            "BANKUSDT".to_string(),
            Side::Buy,
            4.0,
            0.0,
            false,
            100.0,
            None,
            false,
        );
        assert!(
            ArbHedgeStrategy::should_retry_gate_market_forbidden_reduce_only(&response, &order)
        );

        let mut reduce_only_order = order.clone();
        reduce_only_order.reduce_only = true;
        assert!(
            !ArbHedgeStrategy::should_retry_gate_market_forbidden_reduce_only(
                &response,
                &reduce_only_order,
            )
        );
        assert!(
            ArbHedgeStrategy::is_gate_market_forbidden_reduce_only_failure(
                &response,
                &reduce_only_order,
            )
        );
        assert!(
            !ArbHedgeStrategy::is_gate_market_forbidden_reduce_only_failure(&response, &order,)
        );

        let mut limit_order = order.clone();
        limit_order.order_type = OrderType::Limit;
        assert!(
            !ArbHedgeStrategy::should_retry_gate_market_forbidden_reduce_only(
                &response,
                &limit_order,
            )
        );

        let other_error = TradeEngineResponseMessage::new(
            403,
            TradeRequestType::GateFuturesNewOrder as u32,
            Exchange::Gate as u32,
            123,
            gate::MARGIN_NOT_ENOUGH,
        );
        assert!(
            !ArbHedgeStrategy::should_retry_gate_market_forbidden_reduce_only(&other_error, &order)
        );
    }

    #[test]
    fn direct_taker_from_key_carries_lazy_ret_qtl_in_zero_one_units() {
        let from_key =
            build_direct_taker_from_key("lazy_model", 123, model_percentile_to_ret_qtl(Some(42.5)));

        assert_eq!(
            String::from_utf8(from_key.clone()).expect("utf8 from_key"),
            "arb_hedge_lazy_model_direct|123:ret_qtl=0.42500000"
        );
        assert_eq!(
            super::parse_return_qtl_from_from_key(&from_key),
            Some(0.425)
        );
    }

    #[test]
    fn direct_taker_from_key_omits_force_ret_qtl_when_missing() {
        let from_key = build_direct_taker_from_key("force_taker", 123, None);

        assert_eq!(
            String::from_utf8(from_key.clone()).expect("utf8 from_key"),
            "arb_hedge_force_taker_direct|123"
        );
        assert_eq!(super::parse_return_qtl_from_from_key(&from_key), None);
        assert_eq!(model_percentile_to_ret_qtl(Some(0.0)), Some(0.0));
        assert_eq!(model_percentile_to_ret_qtl(Some(100.0)), Some(1.0));
        assert_eq!(model_percentile_to_ret_qtl(Some(101.0)), None);
    }

    #[test]
    fn open_fill_records_net_and_pending_hedge() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, Side::Buy, 2.0, 2.0, 100.0, 1_000, OPEN_ID_A);

        assert_eq!(strategy.net_qty(), 2.0);
        assert_eq!(strategy.pending_hedge_qty(), 2.0);
        assert_eq!(strategy.due_hedge_qty(999), 0.0);
        assert_eq!(strategy.due_hedge_qty(1_000), 2.0);
        // pending lot 必须带上 open id 作为绑定身份
        let lot = strategy
            .pending_hedge_queue
            .find_lot_by_open_id(OPEN_ID_A)
            .expect("lot bound to open id");
        assert_eq!(lot.qty, 2.0);
        assert_eq!(lot.open_client_order_id, Some(OPEN_ID_A));
    }

    #[test]
    fn hedge_fill_offsets_pending_and_base_net() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, Side::Buy, 2.0, 2.0, 100.0, 1_000, OPEN_ID_A);
        let borrowed = strategy.pending_hedge_queue.borrow(1_000, 2.0);
        assert_eq!(borrowed.qv, 2.0);
        // hedge terminal 把未成交 0.75 release 回 OPEN_ID_A 的身份下
        strategy.record_hedge_order_terminal(1_000, Side::Sell, 2.0, 1.25, 101.0, OPEN_ID_A, 0);

        assert_eq!(strategy.net_qty(), 0.75);
        assert_eq!(strategy.pending_hedge_qty(), 0.75);
        assert_eq!(strategy.due_hedge_qty(1_000), 0.75);
        let lot = strategy
            .pending_hedge_queue
            .find_lot_by_open_id(OPEN_ID_A)
            .expect("released lot still bound to OPEN_ID_A");
        assert_eq!(lot.qty, 0.75);
    }

    #[test]
    fn send_failure_cleanup_releases_pending_without_triggering_query() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "DOGEUSDT",
            TradingVenue::BybitMargin,
            TradingVenue::BybitFutures,
        );

        strategy
            .pending_hedge_queue
            .put_with_id(10, 0, -5.0, 0.1147, OPEN_ID_A);
        let borrowed = strategy.pending_hedge_queue.borrow(20, -2.0);
        assert_eq!(borrowed.qv, -2.0);
        assert_eq!(strategy.pending_hedge_qty(), -3.0);

        let client_order_id = 123;
        strategy.hedge_order_meta.insert(
            client_order_id,
            ArbHedgeOrderMeta {
                signal_ts: 10,
                price_offset: 0.0,
                signal_bbo: None,
                borrowed_qv: borrowed.qv,
                order_base_qty: borrowed.qty,
                expire_ts: 0,
                next_expire_check_ts: 0,
                cancel_requested: false,
                bound_open_client_order_id: OPEN_ID_A,
                from_key: Vec::new(),
            },
        );

        strategy.cleanup_unsent_hedge_order_after_send_failure(client_order_id, 30, 0.1147);

        assert_eq!(strategy.pending_hedge_qty(), -5.0);
        assert!(!strategy.hedge_order_meta.contains_key(&client_order_id));
        assert_eq!(strategy.next_query_ts_us, 0);
        // release 后所有量都回到 OPEN_ID_A 的身份
        let lot = strategy
            .pending_hedge_queue
            .find_lot_by_open_id(OPEN_ID_A)
            .expect("cleanup release goes back under bound id");
        assert_eq!(lot.qty, 5.0);
    }

    #[test]
    fn orphan_handoff_keeps_borrowed_work_in_outstanding() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "GENIUSUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );
        strategy.record_open_order_terminal(10, Side::Sell, 100.0, 100.0, 0.43, 0, OPEN_ID_A);
        let borrowed = strategy.pending_hedge_queue.borrow(20, -100.0);
        let client_order_id = 456;
        strategy.hedge_order_meta.insert(
            client_order_id,
            ArbHedgeOrderMeta {
                signal_ts: 10,
                price_offset: 0.0,
                signal_bbo: None,
                borrowed_qv: borrowed.qv,
                order_base_qty: borrowed.qty,
                expire_ts: 0,
                next_expire_check_ts: 0,
                cancel_requested: false,
                bound_open_client_order_id: OPEN_ID_A,
                from_key: Vec::new(),
            },
        );

        let meta = strategy
            .hedge_order_meta
            .remove(&client_order_id)
            .expect("live meta");
        strategy
            .orphaned_hedge_order_meta
            .insert(client_order_id, meta);
        strategy.record_open_order_terminal(30, Side::Sell, 10.0, 10.0, 0.431, 0, OPEN_ID_B);

        assert_eq!(strategy.pending_hedge_qty(), -10.0);
        assert_eq!(strategy.borrowed_hedge_qv(), -100.0);
        assert_eq!(strategy.outstanding_hedge_work_qv(), -110.0);
        let lot = strategy
            .pending_hedge_queue
            .find_lot_by_open_id(OPEN_ID_B)
            .expect("new open id only gets its own delta");
        assert_eq!(lot.qty, 10.0);
    }

    #[test]
    fn orphan_terminal_uses_original_borrowed_meta_and_bound_open_id() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "GENIUSUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );
        strategy.record_open_order_terminal(10, Side::Sell, 100.0, 100.0, 0.43, 0, OPEN_ID_A);
        let borrowed = strategy.pending_hedge_queue.borrow(20, -100.0);
        let client_order_id = 789;
        strategy.orphaned_hedge_order_meta.insert(
            client_order_id,
            ArbHedgeOrderMeta {
                signal_ts: 10,
                price_offset: 0.0,
                signal_bbo: None,
                borrowed_qv: borrowed.qv,
                order_base_qty: borrowed.qty,
                expire_ts: 0,
                next_expire_check_ts: 0,
                cancel_requested: false,
                bound_open_client_order_id: OPEN_ID_A,
                from_key: Vec::new(),
            },
        );

        strategy.record_hedge_order_terminal(30, Side::Buy, 100.0, 40.0, 0.429, 0, client_order_id);

        assert!(!strategy
            .orphaned_hedge_order_meta
            .contains_key(&client_order_id));
        assert_eq!(strategy.net_qty(), -60.0);
        assert_eq!(strategy.pending_hedge_qty(), -60.0);
        assert_eq!(strategy.borrowed_hedge_qv(), 0.0);
        let lot = strategy
            .pending_hedge_queue
            .find_lot_by_open_id(OPEN_ID_A)
            .expect("unfilled orphan release keeps original open id");
        assert_eq!(lot.qty, 60.0);
    }

    #[test]
    fn trigger_skips_pending_before_close_ts_and_period_clock_rechecks_after_due() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, Side::Buy, 2.0, 2.0, 100.0, 2_000, OPEN_ID_A);

        assert_eq!(strategy.pending_hedge_qty(), 2.0);
        assert_eq!(strategy.due_hedge_qty(1_000), 0.0);
        assert_eq!(strategy.hedge_request_seq, 0);
        assert_eq!(strategy.next_query_ts_us, 0);

        strategy.handle_period_clock(2_000);

        assert_eq!(strategy.due_hedge_qty(2_000), 2.0);
        assert_eq!(
            strategy.next_query_ts_us,
            2_000_i64.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US)
        );
    }

    #[test]
    fn inflight_hedge_query_coalesces_multiple_open_terminals() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "CKBUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );
        strategy.hedge_request_seq = 12;
        strategy.begin_inflight_hedge_query(12, 100);

        for (index, open_id) in [1001, 1002, 1003, 1004].into_iter().enumerate() {
            strategy.record_open_order_terminal(
                101 + index as i64,
                Side::Sell,
                100.0,
                100.0,
                0.001,
                0,
                open_id,
            );
        }

        assert_eq!(strategy.pending_hedge_qty(), -400.0);
        assert_eq!(strategy.hedge_request_seq, 12);
        assert_eq!(
            strategy
                .inflight_hedge_query
                .expect("original query remains inflight")
                .request_seq,
            12
        );
    }

    #[test]
    fn inflight_hedge_query_retires_only_at_deadline() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "CKBUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );
        strategy.begin_inflight_hedge_query(12, 100);
        strategy.next_query_ts_us = 999;

        assert!(strategy
            .coalesce_while_hedge_query_inflight(100 + ARB_HEDGE_QUERY_TIMEOUT_US - 1, "test"));
        assert_eq!(
            strategy
                .inflight_hedge_query
                .expect("query remains inflight before deadline")
                .request_seq,
            12
        );

        assert!(
            !strategy.coalesce_while_hedge_query_inflight(100 + ARB_HEDGE_QUERY_TIMEOUT_US, "test")
        );
        assert!(strategy.inflight_hedge_query.is_none());
        assert_eq!(strategy.next_query_ts_us, 0);
    }

    #[test]
    fn late_hedge_reply_cannot_clear_new_inflight_query() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "CKBUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );
        strategy.begin_inflight_hedge_query(12, 100);
        assert!(strategy.retire_timed_out_hedge_query(100 + ARB_HEDGE_QUERY_TIMEOUT_US));
        strategy.begin_inflight_hedge_query(13, 200);

        let mut stale_ctx = ArbHedgeCtx::new();
        stale_ctx.request_seq = 12;
        strategy.handle_arb_hedge_ctx_with_symbol(stale_ctx, "CKBUSDT".to_string());

        assert_eq!(
            strategy
                .inflight_hedge_query
                .expect("late reply must not clear current query")
                .request_seq,
            13
        );

        let mut current_ctx = ArbHedgeCtx::new();
        current_ctx.request_seq = 13;
        strategy.handle_arb_hedge_ctx_with_symbol(current_ctx, "CKBUSDT".to_string());
        assert!(strategy.inflight_hedge_query.is_none());
    }

    #[test]
    fn pending_hedge_query_threshold_uses_mark_price_usdt_value() {
        let below = ArbHedgeStrategy::pending_hedge_usdt_with_mark_price(0.04, 200.0);
        let above = ArbHedgeStrategy::pending_hedge_usdt_with_mark_price(-0.06, 200.0);

        assert!(below < super::ARB_HEDGE_PENDING_QUERY_MIN_USDT);
        assert!(above > super::ARB_HEDGE_PENDING_QUERY_MIN_USDT);
    }

    #[test]
    fn gate_futures_query_qty_gate_rejects_below_min_contracts() {
        let rejected = ArbHedgeStrategy::gate_futures_query_qty_below_min(
            -6.1,
            Some(0.1),
            Some(0.1),
            Some(100.0),
        )
        .expect("6.1 LAB is below 0.1 LAB_USDT contract");

        assert!((rejected.0 - 0.061).abs() < 1e-12);
        assert_eq!(rejected.1, 0.0);
        assert_eq!(rejected.2, 0.1);
    }

    #[test]
    fn gate_futures_query_qty_gate_allows_min_contracts() {
        let allowed = ArbHedgeStrategy::gate_futures_query_qty_below_min(
            -10.0,
            Some(0.1),
            Some(0.1),
            Some(100.0),
        );

        assert!(allowed.is_none());
    }

    #[test]
    fn binance_futures_qty_rejects_below_min_qty() {
        let rejected =
            ArbHedgeStrategy::binance_futures_qty_below_min(0.00096, Some(0.001), Some(0.001))
                .expect("0.00096 BTC is below Binance BTCUSDT min qty");

        assert!((rejected.0 - 0.00096).abs() < 1e-12);
        assert_eq!(rejected.1, 0.0);
        assert_eq!(rejected.2, 0.001);

        let allowed =
            ArbHedgeStrategy::binance_futures_qty_below_min(0.001, Some(0.001), Some(0.001));
        assert!(allowed.is_none());
    }

    #[test]
    fn borrow_shortfall_threshold_uses_one_usdt_eps() {
        let leg = signal_common::common::TradingLeg::new(
            TradingVenue::BinanceFutures,
            99_900.0,
            100_100.0,
            10,
        );

        assert!(ArbHedgeStrategy::borrow_shortfall_within_eps(
            0.000001, 0.0, leg
        ));
        assert!(!ArbHedgeStrategy::borrow_shortfall_within_eps(
            0.00002, 0.0, leg
        ));
    }

    #[test]
    fn hedge_terminal_release_uses_actual_borrowed_qty_for_shortfall_tolerance() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy
            .pending_hedge_queue
            .put_with_id(10, 0, 0.000999, 100_000.0, OPEN_ID_A);
        let borrowed = strategy.pending_hedge_queue.borrow(20, 0.001);
        assert!((borrowed.qv - 0.000999).abs() < 1e-12);
        assert_eq!(strategy.pending_hedge_qty(), 0.0);

        strategy.record_hedge_order_terminal_with_borrowed(
            30,
            Side::Sell,
            0.001,
            0.0005,
            100_000.0,
            borrowed.qv,
            OPEN_ID_A,
        );

        assert!((strategy.pending_hedge_qty() - 0.000499).abs() < 1e-12);
        let lot = strategy
            .pending_hedge_queue
            .find_lot_by_open_id(OPEN_ID_A)
            .expect("partial unfilled borrowed qty released under bound id");
        assert!((lot.qty - 0.000499).abs() < 1e-12);
    }

    fn lot_for(open_id: Option<i64>, qty: f64, close_ts: i64) -> TimedNetQtyLot {
        TimedNetQtyLot {
            ts: 0,
            close_ts,
            qv: qty,
            qty,
            price: 100.0,
            open_client_order_id: open_id,
        }
    }

    #[test]
    fn pick_main_component_picks_largest_qty() {
        let lots = vec![
            lot_for(Some(OPEN_ID_A), 1.5, 100),
            lot_for(Some(OPEN_ID_B), 3.0, 200),
        ];
        assert_eq!(pick_main_component_open_id(&lots), OPEN_ID_B);
    }

    #[test]
    fn pick_main_component_breaks_tie_by_earliest_close_ts() {
        let lots = vec![
            lot_for(Some(OPEN_ID_A), 2.0, 200),
            lot_for(Some(OPEN_ID_B), 2.0, 100),
        ];
        assert_eq!(pick_main_component_open_id(&lots), OPEN_ID_B);
    }

    #[test]
    fn pick_main_component_falls_back_to_zero_when_no_id() {
        let lots = vec![lot_for(None, 5.0, 100)];
        assert_eq!(pick_main_component_open_id(&lots), 0);
        assert_eq!(pick_main_component_open_id(&[]), 0);
    }

    #[test]
    fn upsert_open_lot_merges_partial_terminal_recall() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );
        // 同一个 open id 重复回调（partial-fill terminal 复发）应当合并到既有 lot
        strategy.record_open_order_terminal(10, Side::Buy, 1.0, 1.0, 100.0, 1_000, OPEN_ID_A);
        strategy.record_open_order_terminal(20, Side::Buy, 1.0, 0.5, 110.0, 1_000, OPEN_ID_A);

        let lot = strategy
            .pending_hedge_queue
            .find_lot_by_open_id(OPEN_ID_A)
            .expect("merged lot exists");
        assert!((lot.qty - 1.5).abs() < 1e-9);
        // 加权价 = (100*1 + 110*0.5) / 1.5 = 103.333...
        assert!((lot.price - (100.0 + 110.0 * 0.5) / 1.5).abs() < 1e-9);
        assert_eq!(strategy.pending_hedge_queue.len(), 1);
    }

    #[test]
    fn lazy_taker_without_model_routes_directly_to_taker() {
        assert_eq!(
            decide_due_hedge_route(false, true, None),
            DueHedgeRoute::DirectTaker
        );
    }
}
