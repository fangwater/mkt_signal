use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::log_throttle::log_order_rate_limit_summary;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::{OrderRateBucket, OrderRateLimiter};
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::signal_channel::SignalChannel;
use crate::pre_trade::{PersistChannel, TradeEngHub};
use crate::signal::arb_signal::ArbBackwardQueryMsg;
use crate::signal::common::{OrderStatus, SignalBytes, TradingVenue};
use crate::signal::exec_signal::{
    ExecCtx, ExecPositionTargetCtx, ExecRequestCtx, ExecSignalQueryMsg,
};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::hedge_order_reconcile::{HedgeOrderReconcileCommon, HedgeOrderReconcileState};
use crate::strategy::hedge_strategy_common::{
    mark_price_lookup_symbol, signed_qty_from_side, CANCEL_RESEND_THROTTLE_US,
};
use crate::strategy::manager::{OrphanHandoff, OrphanSourceKind, OrphanStrategyRole, Strategy};
use crate::strategy::net_qty_queue::TimedNetQtyQueue;
use crate::strategy::order_reconcile::PendingOrderQueryReason;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update, UniformPublishCtx,
};
use log::{debug, error, info, warn};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap};

const EXEC_QTY_EPS: f64 = 1e-12;
const EXEC_PENDING_QUERY_MIN_USDT: f64 = 25.0;
const EXEC_QUERY_INTERVAL_US: i64 = 1_000_000;
const EXEC_INSUFFICIENT_MARGIN_COOLDOWN_US: i64 = 5_000_000;
const EXEC_LEVERAGE_DAMPING: f64 = 0.8;

#[derive(Debug, Clone)]
pub struct ExecSnapshot {
    pub symbol: String,
    pub exec_venue: TradingVenue,
    pub pending_exec_qty: f64,
    pub due_exec_qty: f64,
    pub borrowed_exec_qty: f64,
    pub exec_ts_ms: Option<i64>,
    pub exec_is_taker: Option<bool>,
    pub offset: Option<f64>,
}

pub struct ExecStrategy {
    pub(super) strategy_id: i32,
    pub(super) symbol: String,
    exec_venue: TradingVenue,
    pub(super) pending_exec_queue: TimedNetQtyQueue,
    exec_request_seq: u64,
    pending_exec_request_seq: Option<u64>,
    last_exec_ts_ms: Option<i64>,
    last_exec_is_taker: Option<bool>,
    last_exec_offset: Option<f64>,
    next_query_ts_us: i64,
    order_seq: u32,
    exec_order_meta: HashMap<i64, ExecOrderMeta>,
    exec_order_expiry_wheel: BTreeMap<i64, Vec<i64>>,
    order_reconcile_state: HedgeOrderReconcileState,
    alive_flag: bool,
    last_insufficient_margin_action_ts: i64,
    position_target_state: Option<ExecPositionTargetState>,
}

#[derive(Debug, Clone)]
struct ExecPositionTargetState {
    target_qty: f64,
    generation_time: i64,
    from_key: Vec<u8>,
    cancel_order_ids: BTreeSet<i64>,
    next_cancel_ts_us: i64,
}

#[derive(Debug, Clone)]
struct ExecOrderMeta {
    signal_ts: i64,
    price_offset: f64,
    borrowed_qv: f64,
    order_base_qty: f64,
    expire_ts: i64,
    next_expire_check_ts: i64,
    cancel_requested: bool,
    from_key: Vec<u8>,
}

impl ExecStrategy {
    pub fn new(strategy_id: i32, symbol: impl Into<String>, exec_venue: TradingVenue) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            exec_venue,
            pending_exec_queue: TimedNetQtyQueue::new(),
            exec_request_seq: 0,
            pending_exec_request_seq: None,
            last_exec_ts_ms: None,
            last_exec_is_taker: None,
            last_exec_offset: None,
            next_query_ts_us: 0,
            order_seq: 0,
            exec_order_meta: HashMap::new(),
            exec_order_expiry_wheel: BTreeMap::new(),
            order_reconcile_state: HedgeOrderReconcileState::default(),
            alive_flag: true,
            last_insufficient_margin_action_ts: 0,
            position_target_state: None,
        }
    }

    pub fn exec_venue(&self) -> TradingVenue {
        self.exec_venue
    }

    pub fn snapshot(&self, now_ts: i64) -> ExecSnapshot {
        ExecSnapshot {
            symbol: self.symbol.clone(),
            exec_venue: self.exec_venue,
            pending_exec_qty: self.pending_exec_queue.net_qty(),
            due_exec_qty: self.pending_exec_queue.due_qty(now_ts),
            borrowed_exec_qty: self.borrowed_exec_qv(),
            exec_ts_ms: self.last_exec_ts_ms,
            exec_is_taker: self.last_exec_is_taker,
            offset: self.last_exec_offset,
        }
    }

    pub fn pending_exec_qty(&self) -> f64 {
        self.pending_exec_queue.net_qty()
    }

    pub fn due_exec_qty(&self, now_ts: i64) -> f64 {
        self.pending_exec_queue.due_qty(now_ts)
    }

    fn next_exec_request_seq(&mut self) -> u64 {
        self.exec_request_seq = self.exec_request_seq.wrapping_add(1);
        if self.exec_request_seq == 0 {
            self.exec_request_seq = 1;
        }
        self.exec_request_seq
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

    fn borrowed_exec_qv(&self) -> f64 {
        self.exec_order_meta
            .values()
            .map(|meta| meta.borrowed_qv)
            .sum()
    }

    fn pending_exec_usdt_with_mark_price(pending_exec_qty: f64, mark_price: f64) -> f64 {
        pending_exec_qty.abs() * mark_price.abs()
    }

    fn schedule_exec_order_expiry_check(&mut self, client_order_id: i64, due_ts: i64) {
        if due_ts <= 0 {
            return;
        }
        if let Some(meta) = self.exec_order_meta.get_mut(&client_order_id) {
            meta.next_expire_check_ts = due_ts;
        }
        self.exec_order_expiry_wheel
            .entry(due_ts)
            .or_default()
            .push(client_order_id);
    }

    fn reschedule_expired_exec_cancel_if_still_live(
        &mut self,
        client_order_id: i64,
        now_ts: i64,
        reason: &'static str,
    ) {
        {
            let Some(meta) = self.exec_order_meta.get_mut(&client_order_id) else {
                return;
            };
            if meta.expire_ts <= 0 || !meta.cancel_requested {
                return;
            }
            meta.cancel_requested = false;
        }
        let retry_ts = now_ts.saturating_add(CANCEL_RESEND_THROTTLE_US);
        self.schedule_exec_order_expiry_check(client_order_id, retry_ts);
        debug!(
            "ExecStrategy: strategy_id={} expired exec order still live after cancel query, retry scheduled client_order_id={} reason={} retry_ts={}",
            self.strategy_id, client_order_id, reason, retry_ts
        );
    }

    fn release_borrowed(&mut self, now_ts: i64, qv: f64, price: f64) -> f64 {
        if qv.abs() <= EXEC_QTY_EPS {
            return 0.0;
        }
        self.pending_exec_queue.release(now_ts, qv, price)
    }

    fn cleanup_unsent_exec_order_after_send_failure(
        &mut self,
        client_order_id: i64,
        now_ts: i64,
        release_price: f64,
    ) {
        if let Some(meta) = self.exec_order_meta.remove(&client_order_id) {
            self.release_borrowed(now_ts, meta.borrowed_qv, release_price);
        }
        self.clear_order_query_state(client_order_id);
        if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let _ = order_mgr.borrow_mut().remove(client_order_id);
        }
    }

    fn trigger_exec_query_after_pending_release(
        &mut self,
        terminal_ts: i64,
        reason: &'static str,
    ) -> bool {
        let triggered = self.trigger_exec_query(terminal_ts);
        if triggered {
            debug!(
                "ExecStrategy: strategy_id={} symbol={} trigger exec query after pending release reason={}",
                self.strategy_id, self.symbol, reason
            );
        }
        triggered
    }

    fn clear_live_order_query_state(&mut self, client_order_id: i64) {
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

    fn clear_pending_exec_for_position_target(&mut self) {
        let previous_pending = self.pending_exec_queue.net_qty();
        self.pending_exec_queue = TimedNetQtyQueue::new();
        self.pending_exec_request_seq = None;
        if previous_pending.abs() > EXEC_QTY_EPS {
            info!(
                "ExecPositionTarget: strategy_id={} symbol={} venue={:?} cleared previous pending_exec={:.8}",
                self.strategy_id, self.symbol, self.exec_venue, previous_pending
            );
        }
    }

    fn handle_exec_position_target_ctx(&mut self, ctx: ExecPositionTargetCtx) {
        let symbol = normalize_symbol_for_internal(&ctx.get_exec_symbol());
        if symbol.is_empty() {
            warn!(
                "ExecPositionTarget: strategy_id={} empty symbol",
                self.strategy_id
            );
            return;
        }
        if symbol != self.symbol {
            warn!(
                "ExecPositionTarget: strategy_id={} drop symbol mismatch strategy_symbol={} signal_symbol={}",
                self.strategy_id, self.symbol, symbol
            );
            return;
        }
        let Some(venue) = ctx.get_exec_venue() else {
            warn!(
                "ExecPositionTarget: strategy_id={} invalid venue={}",
                self.strategy_id, ctx.exec_venue
            );
            return;
        };
        if venue != self.exec_venue {
            warn!(
                "ExecPositionTarget: strategy_id={} drop venue mismatch configured={:?} got={:?}",
                self.strategy_id, self.exec_venue, venue
            );
            return;
        }
        if !ctx.target_qty.is_finite() {
            warn!(
                "ExecPositionTarget: strategy_id={} invalid target_qty symbol={} target_qty={:.8}",
                self.strategy_id, self.symbol, ctx.target_qty
            );
            return;
        }

        let now_ts = get_timestamp_us();
        let cancel_order_ids = self.collect_live_exec_order_ids_for_position_target();
        self.clear_pending_exec_for_position_target();
        self.position_target_state = Some(ExecPositionTargetState {
            target_qty: ctx.target_qty,
            generation_time: if ctx.generation_time > 0 {
                ctx.generation_time
            } else {
                now_ts
            },
            from_key: ctx.from_key.clone(),
            cancel_order_ids,
            next_cancel_ts_us: 0,
        });
        info!(
            "ExecPositionTarget: strategy_id={} symbol={} venue={:?} received target_qty={:.8} cancel_order_count={}",
            self.strategy_id,
            self.symbol,
            self.exec_venue,
            ctx.target_qty,
            self.position_target_state
                .as_ref()
                .map(|state| state.cancel_order_ids.len())
                .unwrap_or(0)
        );
        self.process_position_target_state(now_ts);
    }

    fn collect_live_exec_order_ids_for_position_target(&self) -> BTreeSet<i64> {
        self.exec_order_meta.keys().copied().collect()
    }

    fn request_cancel_for_position_target_order(
        &mut self,
        client_order_id: i64,
        now_ts: i64,
    ) -> bool {
        let Some(order) = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| order_mgr.borrow().get(client_order_id))
        else {
            return false;
        };
        if order.status.is_terminal() {
            return false;
        }
        let exchange = order.venue.trade_engine_exchange();
        match order.get_order_cancel_bytes() {
            Ok(req_bin) => {
                if let Err(err) =
                    TradeEngHub::publish_order_request_for(client_order_id, exchange, &req_bin)
                {
                    warn!(
                        "ExecPositionTarget: strategy_id={} symbol={} cancel publish failed client_order_id={} exchange={} err={:#}",
                        self.strategy_id, self.symbol, client_order_id, exchange, err
                    );
                    if self.exec_order_meta.contains_key(&client_order_id) {
                        self.handoff_hedge_order_after_query_failure(
                            client_order_id,
                            "position target cancel publish failed",
                        );
                    }
                    return false;
                }
                if let Some(meta) = self.exec_order_meta.get_mut(&client_order_id) {
                    meta.cancel_requested = true;
                }
                self.schedule_order_query_watchdog(
                    client_order_id,
                    PendingOrderQueryReason::CancelWatchdog,
                );
                debug!(
                    "ExecPositionTarget: strategy_id={} symbol={} cancel sent client_order_id={} now_ts={}",
                    self.strategy_id, self.symbol, client_order_id, now_ts
                );
                true
            }
            Err(err) => {
                warn!(
                    "ExecPositionTarget: strategy_id={} symbol={} cancel build failed client_order_id={} err={}",
                    self.strategy_id, self.symbol, client_order_id, err
                );
                if self.exec_order_meta.contains_key(&client_order_id) {
                    self.handoff_hedge_order_after_query_failure(
                        client_order_id,
                        "position target cancel build failed",
                    );
                }
                false
            }
        }
    }

    fn process_position_target_state(&mut self, now_ts: i64) {
        let Some(mut state) = self.position_target_state.take() else {
            return;
        };

        let tracked_ids: BTreeSet<i64> = self.exec_order_meta.keys().copied().collect();
        state.cancel_order_ids.retain(|client_order_id| {
            tracked_ids.contains(client_order_id)
                && MonitorChannel::try_order_manager()
                    .and_then(|order_mgr| order_mgr.borrow().get(*client_order_id))
                    .is_some_and(|order| !order.status.is_terminal())
        });

        if !state.cancel_order_ids.is_empty() {
            if state.next_cancel_ts_us <= 0 || now_ts >= state.next_cancel_ts_us {
                let order_ids: Vec<i64> = state.cancel_order_ids.iter().copied().collect();
                for client_order_id in order_ids {
                    self.request_cancel_for_position_target_order(client_order_id, now_ts);
                }
                state.next_cancel_ts_us = now_ts.saturating_add(CANCEL_RESEND_THROTTLE_US);
            }
            debug!(
                "ExecPositionTarget: strategy_id={} symbol={} waiting cancel live_order_count={} next_cancel_ts_us={}",
                self.strategy_id,
                self.symbol,
                state.cancel_order_ids.len(),
                state.next_cancel_ts_us
            );
            self.position_target_state = Some(state);
            return;
        }

        self.apply_position_target_after_cancels(now_ts, state);
    }

    fn apply_position_target_after_cancels(&mut self, now_ts: i64, state: ExecPositionTargetState) {
        let current_qty =
            MonitorChannel::instance().get_position_qty(&self.symbol, self.exec_venue);
        let delta_qty = state.target_qty - current_qty;
        self.clear_pending_exec_for_position_target();
        if delta_qty.abs() <= EXEC_QTY_EPS {
            info!(
                "ExecPositionTarget: strategy_id={} symbol={} venue={:?} already at target target_qty={:.8} current_qty={:.8} generation_time={} ready_ts={} from_key='{}'",
                self.strategy_id,
                self.symbol,
                self.exec_venue,
                state.target_qty,
                current_qty,
                state.generation_time,
                now_ts,
                String::from_utf8_lossy(&state.from_key)
            );
            return;
        }
        let price = self.mark_price().unwrap_or(0.0);
        self.pending_exec_queue.put(now_ts, 0, delta_qty, price);
        info!(
            "ExecPositionTarget: strategy_id={} symbol={} venue={:?} cancel barrier complete target_qty={:.8} current_qty={:.8} delta_qty={:.8} pending_exec={:.8} generation_time={} ready_ts={} from_key='{}'",
            self.strategy_id,
            self.symbol,
            self.exec_venue,
            state.target_qty,
            current_qty,
            delta_qty,
            self.pending_exec_queue.net_qty(),
            state.generation_time,
            now_ts,
            String::from_utf8_lossy(&state.from_key)
        );
        self.trigger_exec_query(now_ts);
    }

    pub fn handle_exec_request_ctx(&mut self, ctx: ExecRequestCtx) {
        let Some(side) = ctx.get_side() else {
            warn!(
                "ExecStrategy: strategy_id={} invalid ExecRequest side={}",
                self.strategy_id, ctx.side
            );
            return;
        };
        let symbol = normalize_symbol_for_internal(&ctx.get_exec_symbol());
        if symbol.is_empty() {
            warn!(
                "ExecStrategy: strategy_id={} ExecRequest empty symbol",
                self.strategy_id
            );
            return;
        }
        if symbol != self.symbol {
            warn!(
                "ExecStrategy: strategy_id={} drop ExecRequest symbol mismatch strategy_symbol={} signal_symbol={}",
                self.strategy_id, self.symbol, symbol
            );
            return;
        }
        let Some(venue) = TradingVenue::from_u8(ctx.exec_leg.venue) else {
            warn!(
                "ExecStrategy: strategy_id={} ExecRequest invalid venue={}",
                self.strategy_id, ctx.exec_leg.venue
            );
            return;
        };
        if venue != self.exec_venue {
            warn!(
                "ExecStrategy: strategy_id={} drop ExecRequest venue mismatch configured={:?} got={:?}",
                self.strategy_id, self.exec_venue, venue
            );
            return;
        }
        let qty = ctx.amount_value();
        if !(qty.is_finite() && qty > 0.0) {
            warn!(
                "ExecStrategy: strategy_id={} ExecRequest qty invalid symbol={} qty={:.8}",
                self.strategy_id, symbol, qty
            );
            return;
        }
        let order_base_qty = MonitorChannel::instance().qty_to_base(venue, &symbol, qty);
        if !(order_base_qty.is_finite() && order_base_qty > 0.0) {
            warn!(
                "ExecStrategy: strategy_id={} ExecRequest base qty invalid symbol={} venue={:?} qty={:.8}",
                self.strategy_id, symbol, venue, qty
            );
            return;
        }
        let signed_base_qty = signed_qty_from_side(side, order_base_qty);
        let now_ts = get_timestamp_us();
        let create_ts = if ctx.create_ts > 0 {
            ctx.create_ts
        } else {
            now_ts
        };
        let price = if ctx.price_hint.is_finite() && ctx.price_hint > 0.0 {
            ctx.price_hint
        } else {
            ((ctx.exec_leg.bid0 + ctx.exec_leg.ask0) * 0.5).max(0.0)
        };
        self.pending_exec_queue
            .put(create_ts, ctx.close_ts, signed_base_qty, price);
        info!(
            "ExecRequestRecord: strategy_id={} symbol={} venue={:?} side={:?} qty={:.8} base_qty={:.8} qv={:.8} price={:.8} create_ts={} close_ts={} pending_exec={:.8}",
            self.strategy_id,
            self.symbol,
            venue,
            side,
            qty,
            order_base_qty,
            signed_base_qty,
            price,
            create_ts,
            ctx.close_ts,
            self.pending_exec_queue.net_qty()
        );
        self.trigger_exec_query(now_ts);
    }

    fn send_exec_query(&mut self, now_ts: i64, due_exec_qty: f64) {
        self.last_exec_ts_ms = Some(now_ts / 1000);
        let risk_loader = PreTradeParamsLoader::instance();
        let symbol_exposure_u = risk_loader
            .max_pos_u_for_symbol(self.exec_venue, &self.symbol)
            .max(0.0)
            * risk_loader.max_symbol_exposure_ratio().max(0.0);
        let request_seq = self.next_exec_request_seq();
        self.pending_exec_request_seq = Some(request_seq);
        let query_msg = ExecSignalQueryMsg::new(
            self.strategy_id,
            self.exec_venue,
            &self.symbol,
            due_exec_qty,
            self.pending_exec_queue.net_qty(),
            symbol_exposure_u,
            self.pending_exec_queue.weighted_avg_price().unwrap_or(0.0),
            request_seq,
        );
        let payload = ArbBackwardQueryMsg::Exec(query_msg).to_bytes();
        match SignalChannel::with(|ch| ch.publish_backward(&payload)) {
            Ok(true) => {
                self.next_query_ts_us = now_ts.saturating_add(EXEC_QUERY_INTERVAL_US);
                info!(
                    "ExecStrategy: strategy_id={} symbol={} send exec query ok request_seq={} due_exec_qty={:.8} pending_exec_qty={:.8} next_query_ts_us={}",
                    self.strategy_id,
                    self.symbol,
                    request_seq,
                    due_exec_qty,
                    self.pending_exec_queue.net_qty(),
                    self.next_query_ts_us
                );
            }
            Ok(false) => warn!(
                "ExecStrategy: backward publisher not configured strategy_id={} symbol={} request_seq={}",
                self.strategy_id, self.symbol, request_seq
            ),
            Err(err) => warn!(
                "ExecStrategy: send exec query failed strategy_id={} symbol={} request_seq={} err={:#}",
                self.strategy_id, self.symbol, request_seq, err
            ),
        }
    }

    fn trigger_exec_query(&mut self, terminal_ts: i64) -> bool {
        let now_ts = if terminal_ts > 0 {
            terminal_ts
        } else {
            get_timestamp_us()
        };
        self.try_send_due_exec_query(now_ts, "trigger", false)
    }

    fn try_send_due_exec_query(
        &mut self,
        now_ts: i64,
        reason: &'static str,
        throttle_on_skip: bool,
    ) -> bool {
        let pending_exec_qty = self.pending_exec_queue.net_qty();
        if pending_exec_qty.abs() <= EXEC_QTY_EPS {
            return false;
        }
        let due_exec_qty = self.pending_exec_queue.due_qty(now_ts);
        if due_exec_qty.abs() <= EXEC_QTY_EPS {
            debug!(
                "ExecStrategy: strategy_id={} symbol={} skip {} exec query because pending is not due pending_exec_qty={:.8} due_exec_qty={:.8} now_ts={}",
                self.strategy_id, self.symbol, reason, pending_exec_qty, due_exec_qty, now_ts
            );
            return false;
        }
        let Some(mark_price) = self.mark_price() else {
            if throttle_on_skip {
                self.next_query_ts_us = now_ts.saturating_add(EXEC_QUERY_INTERVAL_US);
            }
            info!(
                "ExecStrategy: strategy_id={} symbol={} skip {} exec query because mark_price missing pending_exec_qty={:.8} due_exec_qty={:.8} threshold_usdt={:.8} next_query_ts_us={}",
                self.strategy_id,
                self.symbol,
                reason,
                pending_exec_qty,
                due_exec_qty,
                EXEC_PENDING_QUERY_MIN_USDT,
                self.next_query_ts_us
            );
            return false;
        };
        let pending_exec_usdt =
            Self::pending_exec_usdt_with_mark_price(pending_exec_qty, mark_price);
        if pending_exec_usdt < EXEC_PENDING_QUERY_MIN_USDT {
            if throttle_on_skip {
                self.next_query_ts_us = now_ts.saturating_add(EXEC_QUERY_INTERVAL_US);
            }
            debug!(
                "ExecStrategy: strategy_id={} symbol={} skip {} exec query because pending below threshold pending_exec_qty={:.8} due_exec_qty={:.8} mark_price={:.8} pending_exec_usdt={:.8} threshold_usdt={:.8} next_query_ts_us={}",
                self.strategy_id,
                self.symbol,
                reason,
                pending_exec_qty,
                due_exec_qty,
                mark_price,
                pending_exec_usdt,
                EXEC_PENDING_QUERY_MIN_USDT,
                self.next_query_ts_us
            );
            return false;
        }
        self.send_exec_query(now_ts, due_exec_qty);
        true
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

    fn uniform_exec_publish_ctx(&self, client_order_id: i64) -> UniformPublishCtx {
        let meta = self.exec_order_meta.get(&client_order_id);
        UniformPublishCtx {
            signal_ts: meta.map(|m| m.signal_ts).unwrap_or(0),
            from_key: meta.map(|m| m.from_key.clone()).unwrap_or_default(),
            price_offset: meta.map(|m| m.price_offset).unwrap_or(0.0),
        }
    }

    fn handle_exec_signal(&mut self, ctx: ExecCtx) {
        let Some(expected_request_seq) = self.pending_exec_request_seq else {
            warn!(
                "ExecStrategy: strategy_id={} drop unexpected Exec reply without pending query symbol={} request_seq={}",
                self.strategy_id,
                ctx.get_exec_symbol(),
                ctx.request_seq
            );
            return;
        };
        if ctx.request_seq != expected_request_seq {
            warn!(
                "ExecStrategy: strategy_id={} drop stale/duplicate Exec reply symbol={} request_seq={} expected_request_seq={}",
                self.strategy_id,
                ctx.get_exec_symbol(),
                ctx.request_seq,
                expected_request_seq
            );
            return;
        }
        self.pending_exec_request_seq = None;
        self.last_exec_is_taker = Some(ctx.is_taker());
        self.last_exec_offset = Some(ctx.price_offset);

        let Some(side) = ctx.get_side() else {
            warn!(
                "ExecStrategy: strategy_id={} Exec invalid side={}",
                self.strategy_id, ctx.exec_side
            );
            return;
        };
        let symbol = normalize_symbol_for_internal(&ctx.get_exec_symbol());
        if symbol != self.symbol {
            warn!(
                "ExecStrategy: strategy_id={} Exec symbol mismatch strategy_symbol={} signal_symbol={}",
                self.strategy_id, self.symbol, symbol
            );
            return;
        }
        let Some(venue) = TradingVenue::from_u8(ctx.exec_leg.venue) else {
            warn!(
                "ExecStrategy: strategy_id={} Exec invalid venue={}",
                self.strategy_id, ctx.exec_leg.venue
            );
            return;
        };
        if venue != self.exec_venue {
            warn!(
                "ExecStrategy: strategy_id={} Exec venue mismatch configured={:?} got={:?}",
                self.strategy_id, self.exec_venue, venue
            );
            return;
        }
        let qty = ctx.amount_value();
        if !(qty.is_finite() && qty > 0.0) {
            warn!(
                "ExecStrategy: strategy_id={} Exec qty invalid symbol={} qty={:.8}",
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
        let price = if is_taker { 0.0 } else { ctx.price_value() };
        if !is_taker && !(price.is_finite() && price > 0.0) {
            warn!(
                "ExecStrategy: strategy_id={} Exec price invalid symbol={} price={:.8}",
                self.strategy_id, symbol, price
            );
            return;
        }
        if order_type.is_limit() {
            if let Err(e) =
                MonitorChannel::instance().check_pending_limit_order_for_arb(&symbol, side)
            {
                warn!(
                    "ExecStrategy: strategy_id={} pending limit risk reject symbol={} side={:?} err={}",
                    self.strategy_id, symbol, side, e
                );
                return;
            }
        }

        let order_base_qty = MonitorChannel::instance().qty_to_base(venue, &symbol, qty);
        if !(order_base_qty.is_finite() && order_base_qty > 0.0) {
            warn!(
                "ExecStrategy: strategy_id={} Exec base qty invalid symbol={} venue={:?} qty={:.8}",
                self.strategy_id, symbol, venue, qty
            );
            return;
        }
        let signed_base_qty = signed_qty_from_side(side, order_base_qty);
        if let Err(err) = MonitorChannel::instance().check_arb_hedge_exposure_risk(
            &symbol,
            venue,
            signed_base_qty,
        ) {
            warn!(
                "ExecStrategy: strategy_id={} Exec exposure risk reject symbol={} venue={:?} side={:?} base_qty={:.8} err={}",
                self.strategy_id, symbol, venue, side, order_base_qty, err
            );
            return;
        }

        let now_ts = get_timestamp_us();
        let borrowed = self.pending_exec_queue.borrow(now_ts, signed_base_qty);
        if borrowed.qty + EXEC_QTY_EPS < order_base_qty {
            if borrowed.qv.abs() > EXEC_QTY_EPS {
                self.pending_exec_queue
                    .release(now_ts, borrowed.qv, price.max(ctx.exec_leg.bid0));
            }
            warn!(
                "ExecStrategy: strategy_id={} Exec borrow insufficient symbol={} request_seq={} want_qv={:.8} borrowed_qv={:.8} pending_after={:.8}",
                self.strategy_id,
                symbol,
                ctx.request_seq,
                signed_base_qty,
                borrowed.qv,
                self.pending_exec_queue.net_qty()
            );
            return;
        }

        let client_order_id = self.next_order_id();
        let qty_multiplier = (order_base_qty / qty).max(1e-12);
        MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .create_order_with_pending_limit_flag(
                venue,
                client_order_id,
                order_type,
                symbol.clone(),
                side,
                qty,
                price,
                false,
                qty_multiplier,
                false,
            );
        let _ = MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .update(client_order_id, |o| {
                o.set_signal_meta(ctx.signal_ts, SignalType::Exec as u8)
            });
        self.exec_order_meta.insert(
            client_order_id,
            ExecOrderMeta {
                signal_ts: ctx.signal_ts,
                price_offset: ctx.price_offset,
                borrowed_qv: borrowed.qv,
                order_base_qty,
                expire_ts: ctx.exp_time,
                next_expire_check_ts: ctx.exp_time,
                cancel_requested: false,
                from_key: ctx.from_key.clone(),
            },
        );

        info!(
            "Exec order created: strategy_id={} client_order_id={} symbol={} venue={:?} side={:?} type={:?} qty={:.8} base_qty={:.8} price={:.8} mode={} request_seq={} expire_ts={}",
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

        if let Err(err) = create_and_send_exec_order(self.strategy_id, client_order_id, &symbol) {
            self.cleanup_unsent_exec_order_after_send_failure(
                client_order_id,
                now_ts,
                price.max(ctx.exec_leg.bid0),
            );
            warn!(
                "ExecStrategy: strategy_id={} send Exec order failed client_order_id={} symbol={} err={}",
                self.strategy_id, client_order_id, symbol, err
            );
            return;
        }
        if !is_taker && ctx.exp_time > 0 {
            self.schedule_exec_order_expiry_check(client_order_id, ctx.exp_time);
        }
        self.schedule_order_query_watchdog(client_order_id, PendingOrderQueryReason::OrderWatchdog);
    }

    fn publish_uniform_new_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_exec_publish_ctx(order.client_order_id);
        publish_uniform_new_order(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "ExecStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_terminal_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_exec_publish_ctx(order.client_order_id);
        publish_uniform_terminal_order(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "ExecStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_trade_order_from_order_update(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_exec_publish_ctx(order.client_order_id);
        publish_uniform_trade_order_from_order_update(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
            "ExecStrategy",
            self.strategy_id,
        );
    }

    fn publish_uniform_trade_order(
        &self,
        trade: &dyn TradeUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
        status: OrderStatus,
    ) {
        let ctx = self.uniform_exec_publish_ctx(order.client_order_id);
        publish_uniform_trade_order(
            trade,
            order,
            prev_cumulative_filled_qty,
            status,
            &ctx,
            "ExecStrategy",
            self.strategy_id,
        );
    }

    fn handle_expired_exec_orders(&mut self, now_ts: i64) {
        if self.exec_order_expiry_wheel.is_empty() {
            return;
        }
        let due_keys: Vec<i64> = self
            .exec_order_expiry_wheel
            .keys()
            .copied()
            .take_while(|due_ts| *due_ts <= now_ts)
            .collect();
        if due_keys.is_empty() {
            return;
        }
        let mut due_order_ids = Vec::new();
        for due_ts in due_keys {
            if let Some(order_ids) = self.exec_order_expiry_wheel.remove(&due_ts) {
                due_order_ids.extend(order_ids.into_iter().map(|order_id| (due_ts, order_id)));
            }
        }
        for (due_ts, client_order_id) in due_order_ids {
            let Some(meta) = self.exec_order_meta.get(&client_order_id) else {
                continue;
            };
            if meta.expire_ts <= 0 || meta.next_expire_check_ts != due_ts || meta.cancel_requested {
                continue;
            }
            self.request_cancel_for_expired_exec_order(client_order_id, now_ts);
        }
    }

    fn request_cancel_for_expired_exec_order(&mut self, client_order_id: i64, now_ts: i64) {
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
            let retry_ts = now_ts.saturating_add(EXEC_QUERY_INTERVAL_US);
            if let Some(meta) = self.exec_order_meta.get(&client_order_id) {
                warn!(
                    "ExecStrategy: strategy_id={} expired exec order missing locally, keep borrowed pending client_order_id={} borrowed_qv={:.8} retry_ts={}",
                    self.strategy_id, client_order_id, meta.borrowed_qv, retry_ts
                );
            }
            self.schedule_exec_order_expiry_check(client_order_id, retry_ts);
            return;
        };
        if status.is_terminal() {
            self.record_terminal_exec_order(client_order_id, now_ts, price);
            return;
        }
        let exchange = order.venue.trade_engine_exchange();
        match order.get_order_cancel_bytes() {
            Ok(req_bin) => {
                if let Err(err) =
                    TradeEngHub::publish_order_request_for(client_order_id, exchange, &req_bin)
                {
                    warn!(
                        "ExecStrategy: strategy_id={} expired exec cancel publish failed, handoff orphan client_order_id={} symbol={} exchange={} err={}",
                        self.strategy_id, client_order_id, symbol, exchange, err
                    );
                    self.handoff_hedge_order_after_query_failure(
                        client_order_id,
                        "expired exec cancel publish failed",
                    );
                    return;
                }
                if let Some(meta) = self.exec_order_meta.get_mut(&client_order_id) {
                    meta.cancel_requested = true;
                }
                self.schedule_order_query_watchdog(
                    client_order_id,
                    PendingOrderQueryReason::CancelWatchdog,
                );
            }
            Err(err) => {
                warn!(
                    "ExecStrategy: strategy_id={} expired exec cancel build failed, handoff orphan client_order_id={} symbol={} err={}",
                    self.strategy_id, client_order_id, symbol, err
                );
                self.handoff_hedge_order_after_query_failure(
                    client_order_id,
                    "expired exec cancel build failed",
                );
            }
        }
    }

    fn record_terminal_exec_order(&mut self, client_order_id: i64, terminal_ts: i64, price: f64) {
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
                "ExecStrategy: strategy_id={} terminal exec order missing locally client_order_id={}",
                self.strategy_id, client_order_id
            );
            return;
        };
        let Some(meta) = self.exec_order_meta.remove(&client_order_id) else {
            return;
        };
        let terminal_price = if price.is_finite() && price > 0.0 {
            price
        } else {
            order_price
        };
        let filled_base_qty = filled_base_qty.abs();
        let unfilled_base_qty = (meta.order_base_qty - filled_base_qty).max(0.0);
        let release_qv = signed_qty_from_side(side, unfilled_base_qty);
        let released_qv = self.release_borrowed(terminal_ts, release_qv, terminal_price);
        info!(
            "ExecRecord: strategy_id={} symbol={} side={:?} order_base_qty={:.8} filled_base_qty={:.8} unfilled_base_qty={:.8} released_pending={:.8} terminal_ts={} pending_exec={:.8}",
            self.strategy_id,
            self.symbol,
            side,
            meta.order_base_qty,
            filled_base_qty,
            unfilled_base_qty,
            released_qv,
            terminal_ts,
            self.pending_exec_queue.net_qty()
        );
        if status != OrderExecutionStatus::Filled {
            self.trigger_exec_query_after_pending_release(terminal_ts, "exec_order_terminal");
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
            "ExecStrategy",
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
                order.set_create_time(order_update.event_time());
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
        if let Some(order) = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id)
        {
            if status == OrderStatus::New {
                self.publish_uniform_new_order(order_update, &order, prev_cumulative_filled_qty);
            } else if matches!(
                status,
                OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::ExpiredInMatch
            ) {
                self.publish_uniform_terminal_order(
                    order_update,
                    &order,
                    prev_cumulative_filled_qty,
                );
            } else if matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled) {
                self.publish_uniform_trade_order_from_order_update(
                    order_update,
                    &order,
                    prev_cumulative_filled_qty,
                );
            }
        }
        if status.is_finished() {
            self.clear_order_query_state(client_order_id);
            self.record_terminal_exec_order(
                client_order_id,
                order_update.event_time(),
                order_update.price(),
            );
        } else {
            self.clear_live_order_query_state(client_order_id);
            self.reschedule_expired_exec_cancel_if_still_live(
                client_order_id,
                order_update.event_time(),
                "order_update_non_terminal",
            );
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
            "ExecStrategy",
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
        if let Some(order) = order_manager.get(client_order_id) {
            self.publish_uniform_trade_order(trade, &order, prev_cumulative_filled_qty, status);
        }
        drop(order_manager);
        if status == OrderStatus::Filled {
            self.clear_order_query_state(client_order_id);
            self.record_terminal_exec_order(client_order_id, trade.event_time(), trade.price());
        } else {
            self.clear_live_order_query_state(client_order_id);
            self.reschedule_expired_exec_cancel_if_still_live(
                client_order_id,
                trade.event_time(),
                "trade_update_partial_fill",
            );
        }
        true
    }

    fn handle_insufficient_margin_emergency(&mut self, now_ts: i64, exec_side: Option<Side>) {
        let Some(exec_side) = exec_side else {
            warn!(
                "ExecStrategy: strategy_id={} symbol={} INSUFFICIENT_MARGIN emergency skipped: exec_side unknown",
                self.strategy_id, self.symbol
            );
            return;
        };
        let open_side = match exec_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        self.cancel_same_symbol_open_orders(open_side, now_ts);
        let loader = PreTradeParamsLoader::instance();
        let current = loader.max_leverage();
        let new_lev = ((current * EXEC_LEVERAGE_DAMPING) * 10.0).round() / 10.0;
        if new_lev > 0.0 && (new_lev - current).abs() > f64::EPSILON {
            loader.set_max_leverage_async(new_lev);
        }
        warn!(
            "ExecStrategy: strategy_id={} symbol={} INSUFFICIENT_MARGIN emergency triggered exec_side={:?} open_side={:?} leverage {:.2} -> {:.1}",
            self.strategy_id, self.symbol, exec_side, open_side, current, new_lev
        );
    }

    fn cancel_same_symbol_open_orders(&mut self, open_side: Side, now_ts: i64) {
        let strategy_mgr_handle = MonitorChannel::instance().strategy_mgr();
        let ids: Vec<i32> = strategy_mgr_handle
            .borrow()
            .arb_open_strategy_ids_by_symbol_and_side(&self.symbol, open_side);
        if ids.is_empty() {
            debug!(
                "ExecStrategy: strategy_id={} symbol={} no live ArbOpen with side={:?} to cancel",
                self.strategy_id, self.symbol, open_side
            );
            return;
        }
        for sid in &ids {
            let mut mgr = strategy_mgr_handle.borrow_mut();
            mgr.cancel_arb_open_by_id(
                *sid,
                open_side,
                "exec_insufficient_margin_emergency",
                now_ts,
            );
        }
        info!(
            "ExecStrategy: strategy_id={} symbol={} INSUFFICIENT_MARGIN cancel dispatched count={} side={:?}",
            self.strategy_id, self.symbol, ids.len(), open_side
        );
    }
}

impl HedgeOrderReconcileCommon for ExecStrategy {
    fn hedge_reconcile_strategy_name(&self) -> &'static str {
        "Exec"
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
        self.exec_order_meta.contains_key(&client_order_id)
    }

    fn handoff_hedge_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        reason: &str,
    ) -> bool {
        warn!(
            "ExecReconcile: strategy_id={} orphan_handoff_start reason={} {}",
            self.strategy_id,
            reason,
            self.hedge_order_trace_snapshot(client_order_id)
        );
        let handoff = OrphanHandoff {
            client_order_id,
            source_strategy_id: self.strategy_id,
            source_kind: OrphanSourceKind::Hedge,
            uniform_ctx: self.uniform_exec_publish_ctx(client_order_id),
            reason: reason.to_string(),
        };
        let Some(orphan_mgr) = MonitorChannel::try_orphan_strategy_mgr() else {
            warn!(
                "ExecStrategy: strategy_id={} orphan manager unavailable client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
            return false;
        };
        let adopted = orphan_mgr
            .borrow_mut()
            .adopt_orphan_order_id(OrphanStrategyRole::Exec, &handoff);
        if !adopted {
            warn!(
                "ExecStrategy: strategy_id={} orphan handoff rejected client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
            return false;
        }
        self.clear_order_query_state(client_order_id);
        self.exec_order_meta.remove(&client_order_id);
        true
    }

    fn handle_hedge_open_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        let now_ts = get_timestamp_us();
        let order_snapshot = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id)
            .map(|order| (order.side, order.price, order.symbol.clone()));
        let is_insufficient_margin = response.is_insufficient_margin();
        if let Some(meta) = self.exec_order_meta.remove(&client_order_id) {
            let release_price = order_snapshot
                .as_ref()
                .map(|(_, price, _)| *price)
                .filter(|price| price.is_finite() && *price > 0.0)
                .unwrap_or(0.0);
            self.release_borrowed(now_ts, meta.borrowed_qv, release_price);
            if is_insufficient_margin {
                if now_ts.saturating_sub(self.last_insufficient_margin_action_ts)
                    >= EXEC_INSUFFICIENT_MARGIN_COOLDOWN_US
                {
                    self.last_insufficient_margin_action_ts = now_ts;
                    let exec_side = order_snapshot.as_ref().map(|(side, _, _)| *side);
                    self.handle_insufficient_margin_emergency(now_ts, exec_side);
                }
            } else {
                self.trigger_exec_query_after_pending_release(now_ts, "exec_open_failed");
            }
        }
        self.clear_order_query_state(client_order_id);
        if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let _ = order_mgr.borrow_mut().remove(client_order_id);
        }
        warn!(
            "ExecStrategy: strategy_id={} exec open failed req_type={} status={} code={}({}) client_order_id={} symbol={}{}",
            self.strategy_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id,
            order_snapshot.as_ref().map(|(_, _, symbol)| symbol.as_str()).unwrap_or(""),
            if is_insufficient_margin { " [INSUFFICIENT_MARGIN]" } else { "" }
        );
    }
}

impl Strategy for ExecStrategy {
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
            && self.exec_order_meta.contains_key(&order_id)
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match signal.signal_type {
            SignalType::ExecRequest => match ExecRequestCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_exec_request_ctx(ctx),
                Err(err) => warn!(
                    "ExecStrategy: strategy_id={} decode ExecRequest failed err={}",
                    self.strategy_id, err
                ),
            },
            SignalType::Exec => match ExecCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_exec_signal(ctx),
                Err(err) => warn!(
                    "ExecStrategy: strategy_id={} decode Exec failed err={}",
                    self.strategy_id, err
                ),
            },
            SignalType::ExecPositionTarget => {
                match ExecPositionTargetCtx::from_bytes(signal.context.clone()) {
                    Ok(ctx) => self.handle_exec_position_target_ctx(ctx),
                    Err(err) => warn!(
                        "ExecStrategy: strategy_id={} decode ExecPositionTarget failed err={}",
                        self.strategy_id, err
                    ),
                }
            }
            _ => debug!(
                "ExecStrategy: strategy_id={} ignore signal {:?}",
                self.strategy_id, signal.signal_type
            ),
        }
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        let should_persist = ExecStrategy::apply_order_update(self, update);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_order_update(update));
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let should_persist = ExecStrategy::apply_trade_update(self, trade);
        if should_persist {
            PersistChannel::with(|ch| ch.publish_trade_update(trade));
        }
    }

    fn apply_trade_engine_response(&mut self, response: &dyn TradeEngineResponse) {
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
        self.handle_expired_exec_orders(now_ts);
        self.process_position_target_state(now_ts);
        if self.next_query_ts_us > 0 && now_ts < self.next_query_ts_us {
            return;
        }
        self.try_send_due_exec_query(now_ts, "period_clock", true);
    }

    fn is_active(&self) -> bool {
        self.alive_flag
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }
}

fn create_and_send_exec_order(
    strategy_id: i32,
    client_order_id: i64,
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
                        "ExecStrategy",
                        Some(strategy_id),
                        OrderRateBucket::ArbHedge,
                        symbol,
                        &e,
                    );
                    return Err(format!("exec order rate limit triggered: {}", e));
                }
                if let Err(e) =
                    TradeEngHub::publish_order_request_for(client_order_id, exchange, &req_bin)
                {
                    error!(
                        "ExecStrategy: strategy_id={} symbol={} exchange={} publish exec order failed: {}",
                        strategy_id, symbol, exchange, e
                    );
                    return Err(format!("publish exec order failed: {}", e));
                }
                let stats =
                    OrderRateLimiter::record(OrderRateBucket::ArbHedge, client_order_id, now_us);
                debug!(
                    "ExecStrategy: strategy_id={} exec order action recorded client_order_id={} count_10s={} count_1m={}",
                    strategy_id, client_order_id, stats.count_10s, stats.count_1m
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "ExecStrategy: strategy_id={} symbol={} build exec order bytes failed: {}",
                    strategy_id, symbol, e
                );
                Err(format!("build exec order bytes failed: {}", e))
            }
        }
    } else {
        error!(
            "ExecStrategy: strategy_id={} symbol={} missing local exec order client_order_id={}",
            strategy_id, symbol, client_order_id
        );
        Err("missing local exec order".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::ExecStrategy;
    use crate::pre_trade::order_manager::Side;
    use crate::signal::common::{TradingLeg, TradingVenue};
    use crate::signal::exec_signal::ExecRequestCtx;
    use crate::strategy::Strategy;

    #[test]
    fn exec_request_records_timed_pending_qty() {
        let mut strategy = ExecStrategy::new(1, "BTCUSDT", TradingVenue::BinanceFutures);
        let mut ctx = ExecRequestCtx::new();
        ctx.exec_leg = TradingLeg::new(TradingVenue::BinanceFutures, 100.0, 101.0, 1);
        ctx.set_exec_symbol("BTCUSDT");
        ctx.set_side(Side::Buy);
        ctx.amount_qv = crate::common::tick_math::QuantizedValue::from_parts(1, 0, 2);
        ctx.close_ts = 1_000;
        ctx.create_ts = 10;
        ctx.price_hint = 100.0;
        strategy.handle_exec_request_ctx(ctx);
        assert_eq!(strategy.pending_exec_qty(), 2.0);
        assert_eq!(strategy.due_exec_qty(999), 0.0);
        assert_eq!(strategy.due_exec_qty(1_000), 2.0);
        assert!(strategy.is_active());
    }
}
