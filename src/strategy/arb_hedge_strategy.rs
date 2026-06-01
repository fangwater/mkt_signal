use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::log_throttle::log_order_rate_limit_summary;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::{OrderRateBucket, OrderRateLimiter};
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderManager, OrderType, Side};
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::signal_channel::SignalChannel;
use crate::pre_trade::{PersistChannel, TradeEngHub};
use crate::signal::arb_signal::ArbBackwardQueryMsg;
use crate::signal::common::{OrderStatus, SignalBytes, TradingLeg, TradingVenue};
use crate::signal::hedge_signal::{ArbHedgeCtx, ArbHedgeSignalQueryMsg};
use crate::signal::trade_signal::{SignalType, TradeSignal};
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
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update, UniformPublishCtx,
};
use log::{debug, error, info, warn};
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

const ARB_HEDGE_QTY_EPS: f64 = 1e-12;
const ARB_HEDGE_PENDING_QUERY_MIN_USDT: f64 = 25.0;
const ARB_HEDGE_QUERY_INTERVAL_US: i64 = 1_000_000;
/// 51008（保证金不足）应急动作的冷却时间。同一 strategy 在窗口内的连续 51008
/// 只触发一次撤单 + 杠杆下调。避免一秒万次拒单情况下重复执行。
const ARB_HEDGE_INSUFFICIENT_MARGIN_COOLDOWN_US: i64 = 5_000_000;
/// 应急每次降杠杆的衰减系数：new = round(current * 0.8, 1)。
const ARB_HEDGE_LEVERAGE_DAMPING: f64 = 0.8;

static ARB_HEDGE_FORCE_TAKER: OnceLock<bool> = OnceLock::new();

fn arb_hedge_force_taker() -> bool {
    *ARB_HEDGE_FORCE_TAKER.get_or_init(|| {
        let enabled = std::env::var("ARB_HEDGE_FORCE_TAKER")
            .ok()
            .filter(|value| !value.is_empty())
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "True" | "on" | "ON"))
            .unwrap_or(false);
        if enabled {
            warn!(
                "ARB_HEDGE_FORCE_TAKER=on: pre_trade ArbHedge will bypass backward query and submit taker directly"
            );
        }
        enabled
    })
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
    pending_hedge_request_seq: Option<u64>,
    last_hedge_ts_ms: Option<i64>,
    last_hedge_is_taker: Option<bool>,
    last_ret_qtl: Option<f64>,
    /// 最近一次 ArbHedge maker 单的 price_offset；taker 时为 0。供 dashboard 显示。
    last_hedge_offset: Option<f64>,
    next_query_ts_us: i64,
    order_seq: u32,
    hedge_order_meta: HashMap<i64, ArbHedgeOrderMeta>,
    hedge_order_expiry_wheel: BTreeMap<i64, Vec<i64>>,
    order_reconcile_state: HedgeOrderReconcileState,
    alive_flag: bool,
    /// 上一次因 51008 触发应急动作的时间戳（us）。0 表示从未触发。
    last_insufficient_margin_action_ts: i64,
}

#[derive(Debug, Clone)]
struct ArbHedgeOrderMeta {
    signal_ts: i64,
    price_offset: f64,
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
            pending_hedge_request_seq: None,
            last_hedge_ts_ms: None,
            last_hedge_is_taker: None,
            last_ret_qtl: None,
            last_hedge_offset: None,
            next_query_ts_us: 0,
            order_seq: 0,
            hedge_order_meta: HashMap::new(),
            hedge_order_expiry_wheel: BTreeMap::new(),
            order_reconcile_state: HedgeOrderReconcileState::default(),
            alive_flag: true,
            last_insufficient_margin_action_ts: 0,
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
        self.last_hedge_ts_ms = Some(now_ts / 1000);
        let Some(mark_price) = self.mark_price() else {
            info!(
                "ArbHedgeStrategy: strategy_id={} symbol={} skip force-taker direct hedge because mark_price missing due_hedge_qty={:.8}",
                self.strategy_id, self.symbol, due_hedge_qty
            );
            return false;
        };
        if !(mark_price.is_finite() && mark_price > 0.0) {
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} skip force-taker direct hedge because mark_price invalid mark_price={:.8} due_hedge_qty={:.8}",
                self.strategy_id, self.symbol, mark_price, due_hedge_qty
            );
            return false;
        }

        let hedge_side = if due_hedge_qty >= 0.0 {
            Side::Sell
        } else {
            Side::Buy
        };
        let raw_base_qty = due_hedge_qty.abs();
        let (qty, _) = match MonitorChannel::instance().align_order_by_venue(
            self.hedge_venue,
            &self.symbol,
            raw_base_qty,
            mark_price,
        ) {
            Ok(aligned) => aligned,
            Err(err) => {
                warn!(
                    "ArbHedgeStrategy: strategy_id={} symbol={} force-taker direct align failed venue={:?} raw_base_qty={:.8} mark_price={:.8} err={}",
                    self.strategy_id, self.symbol, self.hedge_venue, raw_base_qty, mark_price, err
                );
                return false;
            }
        };
        if !(qty.is_finite() && qty > 0.0) {
            info!(
                "ArbHedgeStrategy: strategy_id={} symbol={} skip force-taker direct hedge because aligned qty zero raw_base_qty={:.8}",
                self.strategy_id, self.symbol, raw_base_qty
            );
            return false;
        }

        let qty_tick = MonitorChannel::instance()
            .try_venue_min_qty_table(self.hedge_venue)
            .and_then(|table| {
                let symbol_key =
                    crate::common::symbol_util::min_qty_symbol_key(self.hedge_venue, &self.symbol);
                table.step_size(&symbol_key)
            })
            .unwrap_or(0.0);
        let Some(amount_qv) = QuantizedValue::encode_floor(qty, qty_tick) else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} force-taker direct amount qv invalid qty={:.8} qty_tick={:.8}",
                self.strategy_id, self.symbol, qty, qty_tick
            );
            return false;
        };
        if amount_qv.get_count() <= 0 {
            warn!(
                "ArbHedgeStrategy: strategy_id={} symbol={} force-taker direct amount qv non-positive qty={:.8} qty_tick={:.8} count={}",
                self.strategy_id,
                self.symbol,
                qty,
                qty_tick,
                amount_qv.get_count()
            );
            return false;
        }

        let request_seq = self.next_hedge_request_seq();
        self.pending_hedge_request_seq = Some(request_seq);
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
        ctx.set_from_key(format!("arb_hedge_force_taker_direct|{}", now_ts).into_bytes());

        info!(
            "ArbHedgeStrategy: strategy_id={} symbol={} force-taker direct hedge due_hedge_qty={:.8} raw_base_qty={:.8} aligned_qty={:.8} side={:?} mark_price={:.8} request_seq={}",
            self.strategy_id,
            self.symbol,
            due_hedge_qty,
            raw_base_qty,
            qty,
            hedge_side,
            mark_price,
            request_seq
        );
        self.handle_arb_hedge_signal(ctx);
        true
    }

    fn send_hedge_query(&mut self, now_ts: i64, due_hedge_qty: f64) {
        self.last_hedge_ts_ms = Some(now_ts / 1000);
        let risk_loader = PreTradeParamsLoader::instance();
        let symbol_exposure_u = risk_loader
            .max_pos_u_for_symbol(self.open_venue, &self.symbol)
            .max(0.0)
            * risk_loader.max_symbol_exposure_ratio().max(0.0);
        let request_seq = self.next_hedge_request_seq();
        self.pending_hedge_request_seq = Some(request_seq);
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
        let payload = ArbBackwardQueryMsg::Hedge(query_msg).to_bytes();
        match SignalChannel::with(|ch| ch.publish_backward(&payload)) {
            Ok(true) => {
                self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
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

    fn try_send_due_hedge_query(
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
                self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
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
        if arb_hedge_force_taker() {
            if self.send_force_taker_hedge_direct(now_ts, due_hedge_qty) {
                return true;
            }
            if throttle_on_skip {
                self.next_query_ts_us = now_ts.saturating_add(ARB_HEDGE_QUERY_INTERVAL_US);
            }
            return false;
        }
        self.send_hedge_query(now_ts, due_hedge_qty);
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
            price_offset,
        }
    }

    fn hedge_pending_qv_from_order(side: Side, base_qty: f64) -> f64 {
        match side {
            Side::Sell => base_qty.abs(),
            Side::Buy => -base_qty.abs(),
        }
    }

    fn handle_arb_hedge_signal(&mut self, ctx: ArbHedgeCtx) {
        let Some(expected_request_seq) = self.pending_hedge_request_seq else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} drop unexpected ArbHedge reply without pending query: symbol={} request_seq={}",
                self.strategy_id,
                ctx.get_hedging_symbol(),
                ctx.request_seq
            );
            return;
        };
        if ctx.request_seq != expected_request_seq {
            warn!(
                "ArbHedgeStrategy: strategy_id={} drop stale/duplicate ArbHedge reply: symbol={} request_seq={} expected_request_seq={}",
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
        // taker 时 ctx.price_offset = 0；maker 时由 trade_signal 计算的偏移量
        self.last_hedge_offset = Some(ctx.price_offset);

        let Some(side) = ctx.get_side() else {
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedge invalid side={}",
                self.strategy_id, ctx.hedge_side
            );
            return;
        };
        let symbol = normalize_symbol_for_internal(&ctx.get_hedging_symbol());
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
        let pending_qv = Self::hedge_pending_qv_from_order(side, order_base_qty);
        let now_ts = get_timestamp_us();
        let borrowed = self.pending_hedge_queue.borrow(now_ts, pending_qv);
        // 主成分：borrowed.lots 中 qty 最大的那个对应的 open client_order_id；
        // 找不到（lot 全是 None / borrow 没拿到任何东西）时按约定用 0 兜底。
        let bound_open_client_order_id = pick_main_component_open_id(&borrowed.lots);
        if borrowed.qty + ARB_HEDGE_QTY_EPS < order_base_qty {
            if borrowed.qv.abs() > ARB_HEDGE_QTY_EPS {
                if bound_open_client_order_id != 0 {
                    self.pending_hedge_queue.release_with_id(
                        now_ts,
                        borrowed.qv,
                        price.max(ctx.hedging_leg.bid0),
                        bound_open_client_order_id,
                    );
                } else {
                    self.pending_hedge_queue.release(
                        now_ts,
                        borrowed.qv,
                        price.max(ctx.hedging_leg.bid0),
                    );
                }
            }
            warn!(
                "ArbHedgeStrategy: strategy_id={} ArbHedge borrow insufficient symbol={} request_seq={} want_qv={:.8} borrowed_qv={:.8} pending_after={:.8}",
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
        // egress 测度：建单即落 signal 元数据，覆盖本单后续 new/cancel 两次 egress（同归 ArbHedge 桶）。
        let _ = MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .update(client_order_id, |o| {
                o.set_signal_meta(ctx.signal_ts, SignalType::ArbHedge as u8)
            });
        self.hedge_order_meta.insert(
            client_order_id,
            ArbHedgeOrderMeta {
                signal_ts: ctx.signal_ts,
                price_offset: ctx.price_offset,
                borrowed_qv: borrowed.qv,
                order_base_qty,
                expire_ts: ctx.exp_time,
                next_expire_check_ts: ctx.exp_time,
                cancel_requested: false,
                bound_open_client_order_id,
                from_key: ctx.from_key.clone(),
            },
        );

        info!(
            "📤 ArbHedge订单已创建: strategy_id={} client_order_id={} symbol={} {:?} side={:?} type={:?} qty={:.8} base_qty={:.8} price={:.8} mode={} request_seq={} expire_ts={}",
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
        info!(
            "✅ ArbHedge订单已发送: strategy_id={} client_order_id={}",
            self.strategy_id, client_order_id
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
        self.record_hedge_order_terminal(
            terminal_ts,
            side,
            meta.order_base_qty,
            filled_base_qty,
            terminal_price,
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

    /// 51008 应急动作：撤同 symbol、对应 open 方向（=hedge 反向）的 open 挂单，
    /// 并把 max_leverage 下调一档（current * 0.8 取 1 位小数）写回 Redis。
    /// 进入这里前调用方已经做了冷却节流（5s）。
    fn handle_insufficient_margin_emergency(&mut self, now_ts: i64, hedge_side: Option<Side>) {
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

        // 1) 撤同 symbol、同 open 方向的 open 单（最小爆破：不影响相反方向 arb 的策略）。
        self.cancel_same_symbol_open_orders(open_side, now_ts);

        // 2) 杠杆下调：current * 0.8，取 1 位小数；不设下限。
        let loader = PreTradeParamsLoader::instance();
        let current = loader.max_leverage();
        let new_lev = ((current * ARB_HEDGE_LEVERAGE_DAMPING) * 10.0).round() / 10.0;
        if new_lev > 0.0 && (new_lev - current).abs() > f64::EPSILON {
            loader.set_max_leverage_async(new_lev);
        }

        warn!(
            "ArbHedgeStrategy: strategy_id={} symbol={} INSUFFICIENT_MARGIN emergency triggered: hedge_side={:?} open_side={:?} leverage {:.2} -> {:.1}",
            self.strategy_id, self.symbol, hedge_side, open_side, current, new_lev
        );
    }

    fn cancel_same_symbol_open_orders(&mut self, open_side: Side, now_ts: i64) {
        let strategy_mgr_handle = MonitorChannel::instance().strategy_mgr();
        let ids: Vec<i32> = strategy_mgr_handle
            .borrow()
            .arb_open_strategy_ids_by_symbol_and_side(&self.symbol, open_side);
        if ids.is_empty() {
            debug!(
                "ArbHedgeStrategy: strategy_id={} symbol={} no live ArbOpen with side={:?} to cancel",
                self.strategy_id, self.symbol, open_side
            );
            return;
        }
        for sid in &ids {
            let mut mgr = strategy_mgr_handle.borrow_mut();
            mgr.cancel_arb_open_by_id(*sid, open_side, "insufficient_margin_emergency", now_ts);
        }
        info!(
            "ArbHedgeStrategy: strategy_id={} symbol={} INSUFFICIENT_MARGIN cancel dispatched count={} side={:?}",
            self.strategy_id,
            self.symbol,
            ids.len(),
            open_side
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
        // 同时取出 hedge 失败那笔单的 side：51008 应急时需要据此推算 open 端方向。
        let order_snapshot = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id)
            .map(|order| (order.side, order.price, order.symbol.clone()));
        let is_insufficient_margin = response.is_insufficient_margin();
        if let Some(meta) = self.hedge_order_meta.remove(&client_order_id) {
            let release_price = order_snapshot
                .as_ref()
                .map(|(_, price, _)| *price)
                .filter(|price| price.is_finite() && *price > 0.0)
                .unwrap_or(0.0);
            self.release_borrowed_with_bound_id(
                now_ts,
                meta.borrowed_qv,
                release_price,
                meta.bound_open_client_order_id,
            );
            // 51008 / 51061：保证金已经被打满，立刻重发等同于扔进死循环；
            // 走应急路径（撤同方向 open 单 + 降杠杆），并跳过普通的 trigger 重发。
            // 1Hz period_clock 兜底保证 pending 不会永远卡住。
            if is_insufficient_margin {
                if now_ts.saturating_sub(self.last_insufficient_margin_action_ts)
                    >= ARB_HEDGE_INSUFFICIENT_MARGIN_COOLDOWN_US
                {
                    self.last_insufficient_margin_action_ts = now_ts;
                    let hedge_side = order_snapshot.as_ref().map(|(side, _, _)| *side);
                    self.handle_insufficient_margin_emergency(now_ts, hedge_side);
                }
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
                .map(|(_, _, symbol)| symbol.as_str())
                .unwrap_or(""),
            if is_insufficient_margin { " [INSUFFICIENT_MARGIN]" } else { "" }
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
            SignalType::ArbHedge => match ArbHedgeCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_arb_hedge_signal(ctx),
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
        order_base_qty: f64,
        filled_base_qty: f64,
        price: f64,
        close_ts: i64,
        open_client_order_id: i64,
    ) -> bool {
        let order_base_qty = order_base_qty.abs();
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
        let pending_after = self.pending_hedge_queue.net_qty();
        let borrowed_after = self.borrowed_hedge_qv();

        info!(
            "ArbHedgeRecord: strategy_id={} symbol={} leg=opening side={:?} order_base_qty={:.8} filled_base_qty={:.8} fill_qv={:.8} hedge_work_delta={:.8} price={:.8} terminal_ts={} close_ts={} open_co_id={} outstanding_before={:.8} outstanding_after={:.8} net={:.8} hedge_work_baseline={:.8} pending_hedge={:.8} borrowed_hedge={:.8}",
            self.strategy_id,
            self.symbol,
            side,
            order_base_qty,
            filled_base_qty,
            signed_base_qty,
            hedge_work_delta,
            price,
            terminal_ts,
            close_ts,
            open_client_order_id,
            outstanding_before,
            pending_after + borrowed_after,
            self.net_qty_queue.net_qty(),
            self.hedge_work_baseline_qv,
            pending_after,
            borrowed_after
        );

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
    ) -> bool {
        let order_base_qty = order_base_qty.abs();
        let filled_base_qty = filled_base_qty.abs();
        if order_base_qty <= TERMINAL_QTY_EPS && filled_base_qty <= TERMINAL_QTY_EPS {
            return false;
        }
        let signed_base_qty = signed_qty_from_side(side, filled_base_qty);
        let unfilled_base_qty = (order_base_qty - filled_base_qty).max(0.0);
        // arb下单时会按照挂单量borrow待对冲量，在terminal的时候根据实际成交量释放
        let pending_release_qv = match side {
            Side::Buy => -unfilled_base_qty,
            Side::Sell => unfilled_base_qty,
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
        info!(
            "ArbHedgeRecord: strategy_id={} symbol={} leg=hedge side={:?} order_base_qty={:.8} filled_base_qty={:.8} unfilled_base_qty={:.8} released_pending={:.8} qv={:.8} price={:.8} terminal_ts={} bound_open_co_id={} net={:.8} pending_hedge={:.8}",
            self.strategy_id,
            self.symbol,
            side,
            order_base_qty,
            filled_base_qty,
            unfilled_base_qty,
            released_qv,
            signed_base_qty,
            price,
            terminal_ts,
            bound_open_client_order_id,
            self.net_qty_queue.net_qty(),
            self.pending_hedge_queue.net_qty()
        );
        true
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
        pick_main_component_open_id, ArbHedgeOrderMeta, ArbHedgeStrategy,
        ARB_HEDGE_QUERY_INTERVAL_US,
    };
    use crate::pre_trade::order_manager::Side;
    use crate::signal::common::TradingVenue;
    use crate::strategy::manager::{OrderTerminalRecorder, Strategy};
    use crate::strategy::net_qty_queue::TimedNetQtyLot;

    const OPEN_ID_A: i64 = 1001;
    const OPEN_ID_B: i64 = 1002;

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
        strategy.record_hedge_order_terminal(1_000, Side::Sell, 2.0, 1.25, 101.0, OPEN_ID_A);

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
    fn pending_hedge_query_threshold_uses_mark_price_usdt_value() {
        let below = ArbHedgeStrategy::pending_hedge_usdt_with_mark_price(0.1, 200.0);
        let above = ArbHedgeStrategy::pending_hedge_usdt_with_mark_price(-0.2, 200.0);

        assert!(below < super::ARB_HEDGE_PENDING_QUERY_MIN_USDT);
        assert!(above > super::ARB_HEDGE_PENDING_QUERY_MIN_USDT);
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
}
