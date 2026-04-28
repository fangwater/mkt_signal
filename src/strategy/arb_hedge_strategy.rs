use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::signal_channel::SignalChannel;
use crate::pre_trade::{PersistChannel, TradeEngHub};
use crate::signal::arb_signal::ArbBackwardQueryMsg;
use crate::signal::common::{OrderStatus, SignalBytes, TradingVenue};
use crate::signal::hedge_signal::{ArbHedgeStateCtx, ArbHedgeStateQueryMsg};
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::arb_helper::create_and_send_order;
use crate::strategy::hedge_order_reconcile::{HedgeOrderReconcileCommon, HedgeOrderReconcileState};
use crate::strategy::hedge_strategy_common::{
    mark_price_lookup_symbol, parse_return_qtl_from_from_key, CANCEL_RESEND_THROTTLE_US,
    HEDGE_QUERY_INTERVAL_US,
};
use crate::strategy::manager::{
    OrderTerminalRecorder, OrphanHandoff, OrphanSourceKind, OrphanStrategyRole, Strategy,
};
use crate::strategy::net_qty_queue::{NetQtyQueue, TimedNetQtyQueue};
use crate::strategy::order_reconcile::PendingOrderQueryReason;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update, UniformPublishCtx,
};
use log::{debug, info, warn};
use std::any::Any;
use std::collections::{BTreeMap, HashMap};

const ARB_HEDGE_QTY_EPS: f64 = 1e-12;
const ARB_HEDGE_PENDING_QUERY_MIN_USDT: f64 = 25.0;

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
    /// 尚需由对冲腿覆盖的开仓成交队列，到期时间来自开仓记录的 close_ts。
    pub(super) pending_hedge_queue: TimedNetQtyQueue,
    hedge_request_seq: u64,
    pending_hedge_request_seq: Option<u64>,
    last_hedge_ts_ms: Option<i64>,
    last_hedge_is_taker: Option<bool>,
    last_ret_qtl: Option<f64>,
    next_query_ts_us: i64,
    order_seq: u32,
    hedge_order_meta: HashMap<i64, ArbHedgeOrderMeta>,
    hedge_order_expiry_wheel: BTreeMap<i64, Vec<i64>>,
    order_reconcile_state: HedgeOrderReconcileState,
    alive_flag: bool,
}

#[derive(Debug, Clone)]
struct ArbHedgeOrderMeta {
    signal_ts: i64,
    from_key: Vec<u8>,
    price_offset: f64,
    borrowed_qv: f64,
    order_base_qty: f64,
    expire_ts: i64,
    next_expire_check_ts: i64,
    cancel_requested: bool,
}

impl ArbHedgeStrategy {
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
            hedge_request_seq: 0,
            pending_hedge_request_seq: None,
            last_hedge_ts_ms: None,
            last_hedge_is_taker: None,
            last_ret_qtl: None,
            next_query_ts_us: 0,
            order_seq: 0,
            hedge_order_meta: HashMap::new(),
            hedge_order_expiry_wheel: BTreeMap::new(),
            order_reconcile_state: HedgeOrderReconcileState::default(),
            alive_flag: true,
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
        }
    }

    pub fn net_qty(&self) -> f64 {
        self.net_qty_queue.net_qty()
    }

    // pending_hedge_qty 包含了所有开仓成交形成的对冲需求，无论是否已到期；
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

    fn pending_hedge_usdt_with_mark_price(pending_hedge_qty: f64, mark_price: f64) -> f64 {
        pending_hedge_qty.abs() * mark_price.abs()
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

    fn trigger_hedge_state_query_after_pending_release(
        &mut self,
        terminal_ts: i64,
        reason: &'static str,
    ) -> bool {
        let triggered = self.trigger_hedge_state_query_after_open_terminal(terminal_ts);
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

    fn send_hedge_state_query(&mut self, now_ts: i64, due_hedge_qty: f64) {
        self.last_hedge_ts_ms = Some(now_ts / 1000);
        let risk_loader = PreTradeParamsLoader::instance();
        let symbol_exposure_u = risk_loader
            .max_pos_u_for_symbol(self.open_venue, &self.symbol)
            .max(0.0)
            * risk_loader.max_symbol_exposure_ratio().max(0.0);
        let request_seq = self.next_hedge_request_seq();
        self.pending_hedge_request_seq = Some(request_seq);
        let query_msg = ArbHedgeStateQueryMsg::new(
            self.strategy_id,
            &self.symbol,
            self.net_qty_queue.net_qty(),
            due_hedge_qty,
            self.pending_hedge_queue.net_qty(),
            symbol_exposure_u,
            self.net_qty_queue.weighted_avg_price().unwrap_or(0.0),
            request_seq,
        );
        let payload = ArbBackwardQueryMsg::HedgeState(query_msg).to_bytes();
        match SignalChannel::with(|ch| ch.publish_backward(&payload)) {
            Ok(true) => {
                self.next_query_ts_us = now_ts.saturating_add(HEDGE_QUERY_INTERVAL_US);
                debug!(
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
            Ok(false) => {
                warn!(
                    "ArbHedgeStrategy: backward publisher 未配置，无法发送对冲状态查询 strategy_id={} symbol={} request_seq={}",
                    self.strategy_id, self.symbol, request_seq
                );
            }
            Err(err) => {
                warn!(
                    "ArbHedgeStrategy: 发送对冲状态查询失败 strategy_id={} symbol={} request_seq={} err={:#}",
                    self.strategy_id, self.symbol, request_seq, err
                );
            }
        }
    }

    pub(super) fn trigger_hedge_state_query_after_open_terminal(
        &mut self,
        terminal_ts: i64,
    ) -> bool {
        let now_ts = if terminal_ts > 0 {
            terminal_ts
        } else {
            get_timestamp_us()
        };
        // Arb hedge 固定只有一套长期运行流程。这里是事件触发入口：
        // open terminal、hedge 撤单/终态释放后，如果已有 due 数量就立即补一次查询；
        // 如果 close_ts 还没到，固定由 period clock 的时间轮到期后重新拉起。
        self.try_send_due_hedge_state_query(now_ts, "trigger", false)
    }

    fn try_send_due_hedge_state_query(
        &mut self,
        now_ts: i64,
        reason: &'static str,
        throttle_on_skip: bool,
    ) -> bool {
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
                self.next_query_ts_us = now_ts.saturating_add(HEDGE_QUERY_INTERVAL_US);
            }
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
            return false;
        };
        let pending_hedge_usdt =
            Self::pending_hedge_usdt_with_mark_price(pending_hedge_qty, mark_price);
        if pending_hedge_usdt < ARB_HEDGE_PENDING_QUERY_MIN_USDT {
            if throttle_on_skip {
                self.next_query_ts_us = now_ts.saturating_add(HEDGE_QUERY_INTERVAL_US);
            }
            info!(
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
        self.send_hedge_state_query(now_ts, due_hedge_qty);
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

    fn uniform_hedge_publish_ctx(&self, client_order_id: i64) -> UniformPublishCtx {
        let meta = self.hedge_order_meta.get(&client_order_id);
        let signal_ts = meta.map(|m| m.signal_ts).unwrap_or(0);
        let price_offset = meta.map(|m| m.price_offset).unwrap_or(0.0);
        let raw_from_key = meta.map(|m| m.from_key.clone()).unwrap_or_default();
        UniformPublishCtx {
            signal_ts,
            from_key: format!("hedge|{}", String::from_utf8_lossy(&raw_from_key)).into_bytes(),
            price_offset,
        }
    }

    fn hedge_pending_qv_from_order(side: Side, base_qty: f64) -> f64 {
        match side {
            Side::Sell => base_qty.abs(),
            Side::Buy => -base_qty.abs(),
        }
    }

    fn handle_arb_hedge_state_signal(&mut self, ctx: ArbHedgeStateCtx) {
        let Some(expected_request_seq) = self.pending_hedge_request_seq else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} drop unexpected ArbHedgeState reply without pending query: symbol={} request_seq={}",
                self.strategy_id,
                ctx.get_hedging_symbol(),
                ctx.request_seq
            );
            return;
        };
        if ctx.request_seq != expected_request_seq {
            warn!(
                "ArbHedgeStrategy: strategy_id={} drop stale/duplicate ArbHedgeState reply: symbol={} request_seq={} expected_request_seq={}",
                self.strategy_id,
                ctx.get_hedging_symbol(),
                ctx.request_seq,
                expected_request_seq
            );
            return;
        }
        self.pending_hedge_request_seq = None;
        self.last_hedge_is_taker = Some(ctx.is_taker());
        self.last_ret_qtl = parse_return_qtl_from_from_key(&ctx.from_key);

        let Some(side) = ctx.get_side() else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedgeState invalid side={}",
                self.strategy_id, ctx.hedge_side
            );
            return;
        };
        let symbol = normalize_symbol_for_internal(&ctx.get_hedging_symbol());
        if symbol.is_empty() {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedgeState empty symbol",
                self.strategy_id
            );
            return;
        }
        let Some(venue) = TradingVenue::from_u8(ctx.hedging_leg.venue) else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedgeState invalid venue={}",
                self.strategy_id, ctx.hedging_leg.venue
            );
            return;
        };
        let qty = ctx.amount_value();
        if qty <= 0.0 {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedgeState qty invalid symbol={} qty={:.8}",
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
        if !is_taker && price <= 0.0 {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedgeState price invalid symbol={} price={:.8}",
                self.strategy_id, symbol, price
            );
            return;
        }

        let order_base_qty = MonitorChannel::instance().qty_to_base(venue, &symbol, qty);
        if order_base_qty <= 0.0 {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedgeState base qty invalid symbol={} venue={:?} qty={:.8}",
                self.strategy_id, symbol, venue, qty
            );
            return;
        }
        let pending_qv = Self::hedge_pending_qv_from_order(side, order_base_qty);
        let now_ts = get_timestamp_us();
        let borrowed = self.pending_hedge_queue.borrow(now_ts, pending_qv);
        if borrowed.qty + ARB_HEDGE_QTY_EPS < order_base_qty {
            if borrowed.qv.abs() > ARB_HEDGE_QTY_EPS {
                self.pending_hedge_queue.release(
                    now_ts,
                    borrowed.qv,
                    price.max(ctx.hedging_leg.bid0),
                );
            }
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedgeState borrow insufficient symbol={} request_seq={} want_qv={:.8} borrowed_qv={:.8} pending_after={:.8}",
                self.strategy_id,
                symbol,
                ctx.request_seq,
                pending_qv,
                borrowed.qv,
                self.pending_hedge_queue.net_qty()
            );
            return;
        }

        let client_order_id = self.next_order_id();
        let qty_multiplier = (order_base_qty / qty).max(1e-12);
        MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .create_order(
                venue,
                client_order_id,
                order_type,
                symbol.clone(),
                side,
                qty,
                price,
                false,
                qty_multiplier,
                now_ts,
            );
        self.hedge_order_meta.insert(
            client_order_id,
            ArbHedgeOrderMeta {
                signal_ts: ctx.signal_ts,
                from_key: ctx.from_key.clone(),
                price_offset: ctx.price_offset,
                borrowed_qv: borrowed.qv,
                order_base_qty,
                expire_ts: ctx.exp_time,
                next_expire_check_ts: ctx.exp_time,
                cancel_requested: false,
            },
        );

        if let Err(err) =
            create_and_send_order(self.strategy_id, client_order_id, "状态对冲", &symbol)
        {
            if let Some(meta) = self.hedge_order_meta.remove(&client_order_id) {
                self.pending_hedge_queue.release(
                    now_ts,
                    meta.borrowed_qv,
                    price.max(ctx.hedging_leg.bid0),
                );
                self.trigger_hedge_state_query_after_pending_release(
                    now_ts,
                    "hedge_order_send_failed",
                );
            }
            warn!(
                "ArbHedgeStrategy: strategy_id={} send ArbHedgeState order failed client_order_id={} symbol={} err={}",
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
        debug!(
            "ArbHedgeStrategy: strategy_id={} ArbHedgeState order sent client_order_id={} symbol={} venue={:?} side={:?} type={:?} qty={:.8} base_qty={:.8} price={:.8} request_seq={}",
            self.strategy_id,
            client_order_id,
            symbol,
            venue,
            side,
            order_type,
            qty,
            order_base_qty,
            price,
            ctx.request_seq
        );
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

    fn publish_uniform_terminal_order(
        &self,
        order_update: &dyn OrderUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
    ) {
        let ctx = self.uniform_hedge_publish_ctx(order.client_order_id);
        publish_uniform_terminal_order(
            order_update,
            order,
            prev_cumulative_filled_qty,
            &ctx,
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

    fn publish_uniform_trade_order(
        &self,
        trade: &dyn TradeUpdate,
        order: &Order,
        prev_cumulative_filled_qty: f64,
        status: OrderStatus,
    ) {
        let ctx = self.uniform_hedge_publish_ctx(order.client_order_id);
        publish_uniform_trade_order(
            trade,
            order,
            prev_cumulative_filled_qty,
            status,
            &ctx,
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
            let retry_ts = now_ts.saturating_add(HEDGE_QUERY_INTERVAL_US);
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
                if let Err(err) = TradeEngHub::publish_order_request(exchange, &req_bin) {
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
        self.record_hedge_order_terminal(
            terminal_ts,
            side,
            meta.order_base_qty,
            filled_base_qty,
            terminal_price,
        );
        if status != OrderExecutionStatus::Filled {
            self.trigger_hedge_state_query_after_pending_release(
                terminal_ts,
                "hedge_order_terminal",
            );
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
        let updated = order_manager.update(client_order_id, |order| match status {
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
                order.set_filled_time(order_update.event_time());
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
                order.set_filled_time(order_update.event_time());
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
        let updated = order_manager.update(client_order_id, |order| {
            order.cumulative_filled_quantity = trade.cumulative_filled_quantity();
            order.set_filled_time(trade.trade_time());
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
            self.record_terminal_hedge_order(client_order_id, trade.event_time(), trade.price());
        } else {
            self.clear_live_order_query_state(client_order_id);
            self.reschedule_expired_hedge_cancel_if_still_live(
                client_order_id,
                trade.event_time(),
                "trade_update_partial_fill",
            );
        }
        true
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
        self.hedge_order_meta.remove(&client_order_id);
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
        let order_snapshot = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id)
            .map(|order| (order.price, order.symbol.clone()));
        if let Some(meta) = self.hedge_order_meta.remove(&client_order_id) {
            let release_price = order_snapshot
                .as_ref()
                .map(|(price, _)| *price)
                .filter(|price| price.is_finite() && *price > 0.0)
                .unwrap_or(0.0);
            self.pending_hedge_queue
                .release(now_ts, meta.borrowed_qv, release_price);
            self.trigger_hedge_state_query_after_pending_release(now_ts, "hedge_open_failed");
        }
        self.clear_order_query_state(client_order_id);
        if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let _ = order_mgr.borrow_mut().remove(client_order_id);
        }
        warn!(
            "ArbHedgeStrategy: strategy_id={} hedge open failed: req_type={} status={} code={}({}) client_order_id={} symbol={}",
            self.strategy_id,
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id,
            order_snapshot
                .as_ref()
                .map(|(_, symbol)| symbol.as_str())
                .unwrap_or("")
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
            SignalType::ArbHedgeState => match ArbHedgeStateCtx::from_bytes(signal.context.clone())
            {
                Ok(ctx) => self.handle_arb_hedge_state_signal(ctx),
                Err(err) => warn!(
                    "ArbHedgeStrategy: strategy_id={} decode ArbHedgeState failed err={}",
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
        self.try_send_due_hedge_state_query(now_ts, "period_clock", true);
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

#[cfg(test)]
mod tests {
    use super::{ArbHedgeStrategy, HEDGE_QUERY_INTERVAL_US};
    use crate::pre_trade::order_manager::Side;
    use crate::signal::common::TradingVenue;
    use crate::strategy::manager::{OrderTerminalRecorder, Strategy};

    #[test]
    fn open_fill_records_net_and_pending_hedge() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, Side::Buy, 2.0, 2.0, 100.0, 1_000);

        assert_eq!(strategy.net_qty(), 2.0);
        assert_eq!(strategy.pending_hedge_qty(), 2.0);
        assert_eq!(strategy.due_hedge_qty(999), 0.0);
        assert_eq!(strategy.due_hedge_qty(1_000), 2.0);
    }

    #[test]
    fn hedge_fill_offsets_pending_and_base_net() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, Side::Buy, 2.0, 2.0, 100.0, 1_000);
        let borrowed = strategy.pending_hedge_queue.borrow(1_000, 2.0);
        assert_eq!(borrowed.qv, 2.0);
        strategy.record_hedge_order_terminal(1_000, Side::Sell, 2.0, 1.25, 101.0);

        assert_eq!(strategy.net_qty(), 0.75);
        assert_eq!(strategy.pending_hedge_qty(), 0.75);
        assert_eq!(strategy.due_hedge_qty(1_000), 0.75);
    }

    #[test]
    fn trigger_skips_pending_before_close_ts_and_period_clock_rechecks_after_due() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, Side::Buy, 2.0, 2.0, 100.0, 2_000);

        assert_eq!(strategy.pending_hedge_qty(), 2.0);
        assert_eq!(strategy.due_hedge_qty(1_000), 0.0);
        assert_eq!(strategy.hedge_request_seq, 0);
        assert_eq!(strategy.next_query_ts_us, 0);

        strategy.handle_period_clock(2_000);

        assert_eq!(strategy.due_hedge_qty(2_000), 2.0);
        assert_eq!(
            strategy.next_query_ts_us,
            2_000_i64.saturating_add(HEDGE_QUERY_INTERVAL_US)
        );
    }

    #[test]
    fn pending_hedge_query_threshold_uses_mark_price_usdt_value() {
        let below = ArbHedgeStrategy::pending_hedge_usdt_with_mark_price(0.1, 200.0);
        let above = ArbHedgeStrategy::pending_hedge_usdt_with_mark_price(-0.2, 200.0);

        assert!(below < super::ARB_HEDGE_PENDING_QUERY_MIN_USDT);
        assert!(above > super::ARB_HEDGE_PENDING_QUERY_MIN_USDT);
    }
}
