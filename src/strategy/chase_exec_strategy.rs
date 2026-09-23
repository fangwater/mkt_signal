use crate::pre_trade::log_throttle::log_order_rate_limit_summary;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::{
    OkexModifyRateLimiter, OrderRateBucket, OrderRateLimiter,
};
use crate::pre_trade::order_manager::PreTradeOrderRequestExt;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::{PersistChannel, TradeEngHub};
use crate::strategy::batch_exec_strategy::{validate_target_signal, BatchExecTarget};
use crate::strategy::chase_exec::{ChaseExecCompletionReason, ChaseExecConfig, ChaseExecSnapshot};
use crate::strategy::hedge_order_reconcile::{HedgeOrderReconcileCommon, HedgeOrderReconcileState};
use crate::strategy::hedge_strategy_common::{mark_price_lookup_symbol, signed_qty_from_side};
use crate::strategy::manager::{
    ExecOrphanTerminal, OrphanHandoff, OrphanSourceKind, OrphanStrategyRole, Strategy,
};
use crate::strategy::order_reconcile::PendingOrderQueryReason;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update, UniformPublishCtx,
};
use log::{debug, info, warn};
use order_common::{
    ExecutionType, OrderExecutionStatus, OrderManager, OrderStatus, OrderType, OrderUpdate, Side,
    TradeEngineResponse, TradeRequestKind, TradeUpdate, TradingVenue,
};
use persist_common::{
    SignalBbo, SignalBboLeg, UnifiedOrderRecord, UNIFORM_ORDER_TYPE_INTERNAL_CROSS,
};
use quote_plan::common::{align_price_ceil, align_price_floor, Quote};
use quote_plan::order_align::{align_final_order_qty, min_qty_symbol_key};
use runtime_common::exchange::Exchange as RuntimeExchange;
use runtime_common::execution_backend::ExecBackend;
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;
use signal_common::tick_math::QuantizedValue;
use signal_common::trade_signal::TradeSignal;
use std::any::Any;
use trade_signal::MktChannel;

const QTY_EPS: f64 = 1e-12;
const POSITION_RECONCILE_SETTLE_US: i64 = 5_000_000;
const OPEN_REJECT_BACKOFF_US: i64 = 1_000_000;
const RAPIDX_REPLACE_RATE_LIMIT_PER_MIN: i32 = 300;
/// Order signal metadata kind. Must not collide with `SignalType as u8`
/// (egress decodes via `SignalType::from_u32`; invalid values are skipped,
/// which is the desired "no signal" attribution for exec orders). 0 is
/// BatchExec; 12 is the first free value after MMOpenBatch=11.
const CHASE_EXEC_SIGNAL_KIND: u8 = 12;

#[derive(Debug, Clone)]
struct ChaseChildMeta {
    side: Side,
    order_base_qty: f64,
    accounted_fill_base_qty: f64,
    target_generation: i64,
    is_taker: bool,
    /// We cancelled this maker after `expires_at_us`; the confirmed remainder
    /// escalates to taker instead of reposting maker.
    maker_expired: bool,
    /// Cancelled because the target/generation no longer needs it; the
    /// remainder is released back, never reposted and never escalated.
    cancel_for_target: bool,
    /// Own-best anchor confirmed for the order's current price.
    anchor_price: f64,
    /// Own-best anchor and order price waiting for amend confirmation.
    pending_anchor_price: Option<f64>,
    pending_amend_price: Option<f64>,
    last_amend_ts_us: i64,
    amend_in_flight: bool,
    reprice_pending: bool,
    expires_at_us: i64,
    cancel_requested: bool,
    signal_ts: i64,
    signal_bbo: Option<SignalBbo>,
    price_offset: f64,
    from_key: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ActiveTarget {
    target: BatchExecTarget,
    generation_time: i64,
    from_key: Vec<u8>,
    /// Direction fixed at activation: the sign of `target - position` at that
    /// moment. Once the gap flips sign the strategy stops instead of trading
    /// the overshoot back (parity with BatchExec: residual is left to the
    /// position-allocation layer, not re-traded).
    release_side: Side,
    /// Batch notional frozen from the target generation's initial gap.
    effective_batch_usdt: Option<f64>,
}

#[derive(Debug, Clone)]
struct PendingTarget {
    target: BatchExecTarget,
    generation_time: i64,
    from_key: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct ChaseOrderLimits {
    price_tick: f64,
    qty_step: f64,
    min_qty: f64,
    min_notional: f64,
    qty_multiplier: f64,
    inverse_contract_size: Option<f64>,
}

impl ChaseOrderLimits {
    fn qty_multiplier_at(self, price: f64) -> Result<f64, String> {
        let multiplier = if let Some(contract_size) = self.inverse_contract_size {
            if !price.is_finite() || price <= 0.0 {
                return Err(format!(
                    "inverse contract requires positive price, got {price}"
                ));
            }
            contract_size / price
        } else {
            self.qty_multiplier
        };
        if multiplier.is_finite() && multiplier > 0.0 {
            Ok(multiplier)
        } else {
            Err(format!("invalid qty multiplier={multiplier}"))
        }
    }
}

/// Clamp a pending taker obligation to what the uncommitted gap can absorb.
/// Anything beyond is phantom overhang (the gap is already covered by live
/// orders) that can never execute without overshooting the target.
fn clamp_taker_pending_to_gap(taker_pending: f64, uncommitted_base: f64) -> f64 {
    taker_pending.min(uncommitted_base.max(0.0))
}

fn maker_release_usdt(
    effective_batch_usdt: f64,
    max_open_batches: u32,
    open_unfilled_usdt: f64,
    maker_capacity_usdt: f64,
) -> f64 {
    let water_level = effective_batch_usdt * f64::from(max_open_batches);
    effective_batch_usdt
        .min((water_level - open_unfilled_usdt).max(0.0))
        .min(maker_capacity_usdt.max(0.0))
}

fn align_child_qty_floor(raw_qty: f64, qty_step: f64) -> f64 {
    if !raw_qty.is_finite() || raw_qty <= 0.0 {
        return 0.0;
    }
    let adjusted_qty = if qty_step.is_finite() && qty_step > 0.0 {
        raw_qty + QTY_EPS
    } else {
        raw_qty
    };
    align_final_order_qty(adjusted_qty, qty_step, 0.0).0
}

fn align_child_qty_ceil(raw_qty: f64, qty_step: f64) -> f64 {
    if !raw_qty.is_finite() || raw_qty <= 0.0 {
        return 0.0;
    }
    if qty_step.is_finite() && qty_step > 0.0 {
        align_price_ceil((raw_qty - QTY_EPS).max(0.0), qty_step)
    } else {
        raw_qty
    }
}

fn minimum_executable_base_qty(price: f64, limits: ChaseOrderLimits) -> Result<f64, String> {
    if !price.is_finite() || price <= 0.0 {
        return Err(format!("invalid minimum-order price={price}"));
    }
    let qty_multiplier = limits.qty_multiplier_at(price)?;
    if !limits.min_qty.is_finite() || limits.min_qty < 0.0 {
        return Err(format!("invalid minimum-order min qty={}", limits.min_qty));
    }
    if !limits.min_notional.is_finite() || limits.min_notional < 0.0 {
        return Err(format!(
            "invalid minimum-order min notional={}",
            limits.min_notional
        ));
    }
    if !limits.qty_step.is_finite() || limits.qty_step < 0.0 {
        return Err(format!(
            "invalid minimum-order qty step={}",
            limits.qty_step
        ));
    }
    let notional_qty = if limits.min_notional > 0.0 {
        limits.min_notional / (price * qty_multiplier)
    } else {
        0.0
    };
    let required_venue_qty = limits.min_qty.max(notional_qty).max(limits.qty_step);
    Ok(align_child_qty_ceil(required_venue_qty, limits.qty_step) * qty_multiplier)
}

/// Level-0 maker price fixed at the same-side best, aligned away from crossing.
fn level0_maker_price(side: Side, bid: f64, ask: f64, price_tick: f64) -> Result<f64, String> {
    let start_price = match side {
        Side::Sell => ask,
        Side::Buy => bid,
    };
    let limit_price = match side {
        Side::Sell => align_price_ceil(start_price, price_tick),
        Side::Buy => align_price_floor(start_price, price_tick),
    };
    if !limit_price.is_finite() || limit_price <= 0.0 {
        return Err(format!(
            "invalid maker price side={side:?} price={limit_price}"
        ));
    }
    Ok(limit_price)
}

fn own_best_anchor(side: Side, bid: f64, ask: f64) -> f64 {
    match side {
        Side::Buy => bid,
        Side::Sell => ask,
    }
}

fn is_exec_rate_limit_error(error: &str) -> bool {
    error.starts_with("exec ") && error.contains("下单数") && error.contains("达到上限")
}

fn is_chase_strategy_rate_limit_error(error: &str) -> bool {
    error.starts_with("chase strategy=") && error.contains("下单数") && error.contains("达到上限")
}

fn amend_price_matches(actual: f64, expected: f64) -> bool {
    actual.is_finite()
        && expected.is_finite()
        && actual > 0.0
        && expected > 0.0
        && (actual - expected).abs() <= expected.abs().max(1.0) * 1e-10
}

fn effective_modify_rate_limit_per_min(backend: ExecBackend, configured: i32) -> i32 {
    if backend != ExecBackend::Ltp {
        return configured;
    }
    if configured <= 0 {
        RAPIDX_REPLACE_RATE_LIMIT_PER_MIN
    } else {
        configured.min(RAPIDX_REPLACE_RATE_LIMIT_PER_MIN)
    }
}

pub struct ChaseExecStrategy {
    strategy_id: i32,
    strategy_name: String,
    symbol: String,
    exec_venue: TradingVenue,
    config: ChaseExecConfig,
    source_updated_at_us: i64,
    virtual_position_qty: Option<f64>,
    position_allocation_ready: bool,
    last_position_fill_at_us: i64,
    active_target: Option<ActiveTarget>,
    pending_target: Option<PendingTarget>,
    children: FastHashMap<i64, ChaseChildMeta>,
    orphaned_children: FastHashMap<i64, ChaseChildMeta>,
    /// Base-qty remainder already escalated to taker but not yet (re)sent.
    /// Filled-water-level bookkeeping: it is the unfunded part of
    /// `unallocated`, reserved for market orders rather than maker quotes.
    taker_pending_base_qty: f64,
    order_seq: u32,
    /// Local exec rate-limit defer for new places and amends.
    submit_blocked_until_us: i64,
    completion_reason: Option<ChaseExecCompletionReason>,
    reconcile_state: HedgeOrderReconcileState,
    alive_flag: bool,
}

impl ChaseExecStrategy {
    pub fn new(
        strategy_id: i32,
        strategy_name: impl Into<String>,
        symbol: impl Into<String>,
        exec_venue: TradingVenue,
        config: ChaseExecConfig,
    ) -> Self {
        Self {
            strategy_id,
            strategy_name: strategy_name.into(),
            symbol: normalize_symbol_for_internal(&symbol.into()),
            exec_venue,
            config,
            source_updated_at_us: 0,
            virtual_position_qty: None,
            position_allocation_ready: false,
            last_position_fill_at_us: 0,
            active_target: None,
            pending_target: None,
            children: fast_hash_map(),
            orphaned_children: fast_hash_map(),
            taker_pending_base_qty: 0.0,
            order_seq: 0,
            submit_blocked_until_us: 0,
            completion_reason: None,
            reconcile_state: HedgeOrderReconcileState::default(),
            alive_flag: true,
        }
    }

    pub fn exec_venue(&self) -> TradingVenue {
        self.exec_venue
    }

    pub fn strategy_name(&self) -> &str {
        &self.strategy_name
    }

    pub fn set_source_updated_at_us(&mut self, updated_at_us: i64) {
        self.source_updated_at_us = updated_at_us.max(0);
    }

    pub fn exec_symbol(&self) -> &str {
        &self.symbol
    }

    pub fn target_qty(&self) -> Option<f64> {
        self.current_target().map(|target| target.qty)
    }

    fn current_target(&self) -> Option<BatchExecTarget> {
        self.pending_target
            .as_ref()
            .map(|target| target.target)
            .or_else(|| self.active_target.as_ref().map(|target| target.target))
    }

    pub fn virtual_position_qty(&self) -> Option<f64> {
        self.virtual_position_qty
    }

    pub fn position_allocation_ready(&self) -> bool {
        self.position_allocation_ready && self.virtual_position_qty.is_some()
    }

    pub fn has_execution_in_flight(&self) -> bool {
        !self.children.is_empty()
            || !self.orphaned_children.is_empty()
            || self.taker_pending_base_qty > QTY_EPS
    }

    pub fn pause_position_allocation(&mut self) {
        self.position_allocation_ready = false;
    }

    pub fn begin_position_reallocation(&mut self) {
        self.pause_position_allocation();
        // Taker obligations are committed toward the *current* ledger
        // position. Reallocation is about to move that position, so keeping
        // the obligation would pin `has_execution_in_flight` forever (the
        // paused strategy can no longer drain it) and block the removal /
        // reconcile path. The residual exposure is re-allocated through the
        // ledger instead.
        self.taker_pending_base_qty = 0.0;
        self.cancel_all_children_for_target();
    }

    pub fn position_reconciliation_settled(&self, now_ts: i64) -> bool {
        !self.has_execution_in_flight()
            && (self.last_position_fill_at_us == 0
                || now_ts.saturating_sub(self.last_position_fill_at_us)
                    >= POSITION_RECONCILE_SETTLE_US)
    }

    pub fn position_reconciliation_ready(&self, now_ts: i64) -> bool {
        self.position_allocation_ready() && self.position_reconciliation_settled(now_ts)
    }

    pub fn suspend_position_allocation(&mut self) -> Result<(), String> {
        if self.has_execution_in_flight() {
            return Err("cannot suspend position allocation with orders in flight".to_string());
        }
        self.pause_position_allocation();
        Ok(())
    }

    pub fn apply_position_allocation(
        &mut self,
        position_qty: f64,
        _now_ts: i64,
    ) -> Result<(), String> {
        if !position_qty.is_finite() {
            return Err("position allocation must be finite".to_string());
        }
        if self.has_execution_in_flight() {
            return Err("cannot apply position allocation with orders in flight".to_string());
        }
        let previous = self.virtual_position_qty;
        self.virtual_position_qty = Some(position_qty);
        self.position_allocation_ready = true;
        self.completion_reason = None;
        info!(
            "ChaseExecStrategy: strategy_id={} strategy_name={} symbol={} position allocation applied previous={:?} current={:.8}",
            self.strategy_id, self.strategy_name, self.symbol, previous, position_qty
        );
        self.process_pending_target();
        Ok(())
    }

    fn current_from_key(&self) -> Option<Vec<u8>> {
        self.pending_target
            .as_ref()
            .map(|target| target.from_key.clone())
            .or_else(|| {
                self.active_target
                    .as_ref()
                    .map(|target| target.from_key.clone())
            })
    }

    pub fn can_apply_internal_cross_fill(
        &self,
        signed_base_qty: f64,
        quote: &Quote,
    ) -> Result<(), String> {
        if !signed_base_qty.is_finite() || signed_base_qty.abs() <= QTY_EPS {
            return Err("internal cross qty must be finite and non-zero".to_string());
        }
        let mid = (quote.bid + quote.ask) / 2.0;
        if !quote.is_valid() || !mid.is_finite() || mid <= 0.0 {
            return Err("internal cross requires a valid quote".to_string());
        }
        if !self.position_allocation_ready()
            || self.current_from_key().is_none()
            || self.virtual_position_qty.is_none()
        {
            return Err("internal cross requires an applied target and position".to_string());
        }
        let symbol = crate::pre_trade::persist_channel::normalize_symbol_for_venue(
            self.exec_venue,
            &self.symbol,
        );
        let multiplier = crate::pre_trade::persist_channel::resolve_futures_qty_multiplier(
            self.exec_venue,
            &symbol,
            mid,
        );
        let venue_qty = signed_base_qty.abs() / multiplier;
        if !multiplier.is_finite()
            || multiplier <= 0.0
            || !venue_qty.is_finite()
            || venue_qty <= 0.0
        {
            return Err("internal cross invalid venue quantity".to_string());
        }
        Ok(())
    }

    /// Books an internal cross fill: `signed_base_qty` moves this strategy's
    /// ledger position without touching the shared account position. The
    /// shrinked gap stops further release; live children beyond the new
    /// remaining gap are cancelled by the next clock pass.
    pub fn apply_internal_cross_fill(
        &mut self,
        signed_base_qty: f64,
        quote: &Quote,
        now_ts: i64,
    ) -> Result<UnifiedOrderRecord, String> {
        if !signed_base_qty.is_finite() || signed_base_qty.abs() <= QTY_EPS {
            return Err(format!(
                "internal cross qty must be finite and non-zero: {signed_base_qty}"
            ));
        }
        let mid = (quote.bid + quote.ask) / 2.0;
        if !quote.is_valid() || !mid.is_finite() || mid <= 0.0 {
            return Err(format!(
                "internal cross requires a valid quote: bid={} ask={}",
                quote.bid, quote.ask
            ));
        }
        if !self.position_allocation_ready() {
            return Err("internal cross requires an applied position allocation".to_string());
        }
        let Some(from_key) = self.current_from_key() else {
            return Err("internal cross requires a current target".to_string());
        };
        let normalized_symbol = crate::pre_trade::persist_channel::normalize_symbol_for_venue(
            self.exec_venue,
            &self.symbol,
        );
        let qty_multiplier = crate::pre_trade::persist_channel::resolve_futures_qty_multiplier(
            self.exec_venue,
            &normalized_symbol,
            mid,
        );
        if !qty_multiplier.is_finite() || qty_multiplier <= 0.0 {
            return Err(format!(
                "internal cross invalid qty multiplier={qty_multiplier}"
            ));
        }
        let venue_qty = signed_base_qty.abs() / qty_multiplier;
        if !venue_qty.is_finite() || venue_qty <= 0.0 {
            return Err(format!("internal cross invalid venue qty={venue_qty}"));
        }
        let Some(virtual_position) = self.virtual_position_qty.as_mut() else {
            return Err("internal cross requires a virtual position".to_string());
        };
        *virtual_position += signed_base_qty;
        let virtual_position_qty = *virtual_position;
        self.last_position_fill_at_us = now_ts;
        self.completion_reason = None;
        let side = if signed_base_qty > 0.0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let signal_bbo = SignalBbo::new(
            SignalBboLeg::checked(
                self.exec_venue as u8,
                quote.ts,
                quote.bid,
                quote.bid_qty,
                quote.ask,
                quote.ask_qty,
            ),
            None,
        );
        let mut record = UnifiedOrderRecord {
            symbol_len: 0,
            symbol: self.symbol.as_bytes().to_vec(),
            create_ts: now_ts,
            update_ts: now_ts,
            signal_ts: now_ts,
            submit_ts: now_ts,
            local_ts: now_ts,
            mkt_ts: quote.ts,
            client_order_id: self.next_order_id(),
            venue: self.exec_venue as u8,
            ttype: UNIFORM_ORDER_TYPE_INTERNAL_CROSS,
            side: side.to_u8(),
            price: mid,
            price_offset: 0.0,
            amount_init: venue_qty,
            amount_update: venue_qty,
            status: OrderStatus::Filled.to_u8(),
            from_key_len: 0,
            from_key,
            signal_bbo,
        };
        record.refresh_lengths();
        info!(
            "ChaseExecStrategy: strategy_id={} strategy_name={} symbol={} internal cross applied signed_base_qty={:.8} mid={:.8} virtual_position_qty={:.8} client_order_id={}",
            self.strategy_id,
            self.strategy_name,
            self.symbol,
            signed_base_qty,
            mid,
            virtual_position_qty,
            record.client_order_id,
        );
        Ok(record)
    }

    fn next_order_id(&mut self) -> i64 {
        self.order_seq = self.order_seq.wrapping_add(1).max(1);
        ((self.strategy_id as i64) << 32) | self.order_seq as i64
    }

    fn extract_strategy_id(order_id: i64) -> i32 {
        (order_id >> 32) as i32
    }

    /// Signed unfilled qty reserved by live orders (children + handed-off
    /// orphans). A cancel in flight still counts; the confirmed terminal event
    /// releases it. This is the water-level ledger.
    fn live_order_signed_qty(&self) -> f64 {
        self.children
            .values()
            .chain(self.orphaned_children.values())
            .map(|meta| {
                let remaining = (meta.order_base_qty - meta.accounted_fill_base_qty).max(0.0);
                signed_qty_from_side(meta.side, remaining)
            })
            .sum()
    }

    /// Unsigned unfilled base qty reserved by live orders (children +
    /// orphans). Release direction is expressed separately via
    /// `ActiveTarget::release_side`.
    fn open_unfilled_base_qty(&self) -> f64 {
        self.children
            .values()
            .chain(self.orphaned_children.values())
            .map(|meta| (meta.order_base_qty - meta.accounted_fill_base_qty).max(0.0))
            .sum()
    }

    fn mark_price(&self) -> Option<f64> {
        let monitor = MonitorChannel::instance();
        let exchange = monitor.try_mark_price_exchange()?;
        let price_symbol = mark_price_lookup_symbol(&self.symbol, exchange);
        monitor
            .try_price_table()?
            .borrow()
            .mark_price(&price_symbol)
            .filter(|price| price.is_finite() && *price > 0.0)
    }

    fn effective_batch_usdt(&mut self, generation: i64, remaining_base: f64) -> Option<f64> {
        if let Some(value) = self
            .active_target
            .as_ref()
            .filter(|target| target.generation_time == generation)
            .and_then(|target| target.effective_batch_usdt)
        {
            return Some(value);
        }
        let mark_price = self.mark_price()?;
        let delta_usdt = remaining_base * mark_price;
        let value = self.config.effective_batch_usdt(delta_usdt);
        if let Some(target) = self
            .active_target
            .as_mut()
            .filter(|target| target.generation_time == generation)
        {
            target.effective_batch_usdt = Some(value);
        }
        info!(
            "ChaseExecStrategy: strategy_id={} strategy_name={} symbol={} target generation={} mark_price={:.8} delta_usdt={:.4} batch_floor_usdt={:.4} effective_batch_usdt={:.4} max_batch={} max_open_batches={}",
            self.strategy_id,
            self.strategy_name,
            self.symbol,
            generation,
            mark_price,
            delta_usdt,
            self.config.batch_floor_usdt,
            value,
            self.config.max_batch,
            self.config.max_open_batches,
        );
        Some(value)
    }

    fn latest_generation_time(&self) -> i64 {
        self.pending_target
            .as_ref()
            .map(|target| target.generation_time)
            .into_iter()
            .chain(
                self.active_target
                    .as_ref()
                    .map(|target| target.generation_time),
            )
            .max()
            .unwrap_or(0)
    }

    pub fn update_config(&mut self, config: ChaseExecConfig) -> Result<(), String> {
        config.validate()?;
        if self.config == config {
            return Ok(());
        }
        self.config = config;
        self.completion_reason = None;
        Ok(())
    }

    fn order_rate_retry_at_us(
        &self,
        account_limit_per_min: i32,
        account_limit_10s: i32,
        now_us: i64,
    ) -> i64 {
        OrderRateLimiter::next_available_at_us(
            OrderRateBucket::Exec,
            account_limit_per_min,
            account_limit_10s,
            now_us,
        )
        .max(OrderRateLimiter::strategy_next_available_at_us(
            &self.strategy_name,
            self.config.strategy_order_rate_limit_per_min,
            self.config.strategy_order_rate_limit_10s,
            now_us,
        ))
    }

    pub fn update_target(
        &mut self,
        target: BatchExecTarget,
        generation_time: i64,
        from_key: Vec<u8>,
    ) {
        if !target.qty.is_finite() {
            warn!(
                "ChaseExecStrategy: strategy_id={} invalid target_qty={}",
                self.strategy_id, target.qty
            );
            return;
        }
        if let Err(err) = validate_target_signal(target.signal) {
            warn!(
                "ChaseExecStrategy: strategy_id={} invalid target signal={}: {}",
                self.strategy_id, target.signal, err
            );
            return;
        }
        let generation_time = if generation_time > 0 {
            generation_time
        } else {
            get_timestamp_us()
        };
        if generation_time <= self.latest_generation_time() {
            debug!(
                "ChaseExecStrategy: strategy_id={} drop stale target generation={} latest={}",
                self.strategy_id,
                generation_time,
                self.latest_generation_time()
            );
            return;
        }

        self.pending_target = Some(PendingTarget {
            target,
            generation_time,
            from_key,
        });
        self.completion_reason = None;
        // A new generation invalidates leftover taker obligations from the old
        // one; released remainders re-enter through the unallocated ledger.
        self.taker_pending_base_qty = 0.0;
        self.cancel_all_children_for_target();
        self.process_pending_target();
    }

    fn process_pending_target(&mut self) {
        if self.pending_target.is_none() {
            return;
        }
        if !self.children.is_empty() || !self.orphaned_children.is_empty() {
            return;
        }
        if !MonitorChannel::instance().exec_position_snapshot_ready()
            || !self.position_allocation_ready()
        {
            return;
        }
        let pending = self.pending_target.take().expect("checked above");
        let position_qty = self.virtual_position_qty.expect("allocation checked above");
        self.taker_pending_base_qty = 0.0;
        info!(
            "ChaseExecStrategy: strategy_id={} strategy_name={} symbol={} target activated target_qty={:.8} signal={} allocated_position_qty={:.8} generation={}",
            self.strategy_id,
            self.strategy_name,
            self.symbol,
            pending.target.qty,
            pending.target.signal,
            position_qty,
            pending.generation_time
        );
        let release_side = if pending.target.qty >= position_qty {
            Side::Buy
        } else {
            Side::Sell
        };
        self.active_target = Some(ActiveTarget {
            target: pending.target,
            generation_time: pending.generation_time,
            from_key: pending.from_key,
            release_side,
            effective_batch_usdt: None,
        });
        self.completion_reason = None;
    }

    fn load_order_limits(&self, reference_price: f64) -> Result<ChaseOrderLimits, String> {
        let table = MonitorChannel::instance()
            .try_venue_min_qty_table(self.exec_venue)
            .ok_or_else(|| format!("missing min qty table venue={:?}", self.exec_venue))?;
        if table.snapshot_loaded() && !table.is_tradable_symbol(&self.symbol) {
            return Err(format!("symbol is not tradable symbol={}", self.symbol));
        }
        let symbol_key = min_qty_symbol_key(self.exec_venue, &self.symbol);
        let price_tick = table
            .price_tick(&symbol_key)
            .ok_or_else(|| format!("missing price tick symbol={}", self.symbol))?;
        let qty_multiplier = MonitorChannel::instance().qty_multiplier_for_venue_at_price(
            self.exec_venue,
            &self.symbol,
            reference_price,
        )?;
        if !qty_multiplier.is_finite() || qty_multiplier <= 0.0 {
            return Err(format!("invalid qty multiplier symbol={}", self.symbol));
        }
        let inverse_contract_size = if self.exec_venue.is_inverse_futures() {
            table.contract_multiplier_opt(&symbol_key)
        } else {
            None
        };
        Ok(ChaseOrderLimits {
            price_tick,
            qty_step: table.step_size(&symbol_key).unwrap_or(0.0),
            min_qty: table.min_qty(&symbol_key).unwrap_or(0.0),
            min_notional: table.min_notional(&symbol_key).unwrap_or(0.0),
            qty_multiplier,
            inverse_contract_size,
        })
    }

    fn symbol_is_tradable(&self) -> Option<bool> {
        let table = MonitorChannel::instance().try_venue_min_qty_table(self.exec_venue)?;
        table
            .snapshot_loaded()
            .then(|| table.is_tradable_symbol(&self.symbol))
    }

    fn cancel_all_children_for_target(&mut self) {
        let ids: Vec<i64> = self.children.keys().copied().collect();
        for client_order_id in ids {
            if let Some(meta) = self.children.get_mut(&client_order_id) {
                meta.cancel_for_target = true;
            }
            self.request_cancel(client_order_id);
        }
    }

    /// Cancel live children when the remaining target no longer needs their
    /// committed side/qty (fill overshoot, direction flip, or tolerance
    /// reached). Their released remainder simply returns to the ledger.
    fn cancel_children_when_target_no_longer_needs_them(&mut self) {
        if self.pending_target.is_some()
            || self.children.is_empty()
            || !self.position_allocation_ready()
        {
            return;
        }
        let Some(target_qty) = self.active_target.as_ref().map(|target| target.target.qty) else {
            return;
        };
        let position_qty = self.virtual_position_qty.expect("allocation checked above");
        let remaining_qty = target_qty - position_qty;
        let committed_qty = self.live_order_signed_qty();

        let direction_changed = committed_qty.abs() > QTY_EPS
            && (remaining_qty.abs() <= QTY_EPS || remaining_qty.signum() != committed_qty.signum());
        let committed_too_much = committed_qty.abs() > remaining_qty.abs() + QTY_EPS;

        if !direction_changed && !committed_too_much {
            let within_tolerance = MktChannel::instance()
                .get_quote(&self.symbol, self.exec_venue)
                .map(|quote| {
                    let reference_price = if remaining_qty >= 0.0 {
                        quote.bid
                    } else {
                        quote.ask
                    };
                    remaining_qty.abs() * reference_price <= self.config.target_tolerance_usdt
                })
                .unwrap_or(false);
            if !within_tolerance {
                return;
            }
        }
        self.cancel_all_children_for_target();
    }

    /// Maker timeout: each live child gets one maker lifetime. Expired children
    /// are cancelled; on the terminal event their remainder escalates to taker.
    fn handle_child_timeouts(&mut self, now_ts: i64) {
        let expired: Vec<i64> = self
            .children
            .iter()
            .filter_map(|(client_order_id, meta)| {
                (!meta.is_taker
                    && !meta.cancel_requested
                    && !meta.maker_expired
                    && meta.expires_at_us > 0
                    && now_ts >= meta.expires_at_us)
                    .then_some(*client_order_id)
            })
            .collect();
        for client_order_id in expired {
            if let Some(meta) = self.children.get_mut(&client_order_id) {
                meta.maker_expired = true;
            }
            self.request_cancel(client_order_id);
        }
    }

    /// Re-issue cancels that were requested but never confirmed sent (e.g. a
    /// publish failure inside `request_cancel`). Without this retry an
    /// expired maker child or a target-cancelled child could stay live on the
    /// venue forever, blocking generation activation and taker escalation.
    fn retry_unsent_cancels(&mut self) {
        let ids: Vec<i64> = self
            .children
            .iter()
            .filter_map(|(client_order_id, meta)| {
                (!meta.cancel_requested && (meta.cancel_for_target || meta.maker_expired))
                    .then_some(*client_order_id)
            })
            .collect();
        for client_order_id in ids {
            self.request_cancel(client_order_id);
        }
    }

    /// In-place amend of live maker children when the own-best anchor has
    /// moved at least `maker_recenter_trigger_bps` (or the aligned level-0
    /// price changed when the trigger is 0). One modify in flight per child;
    /// post-only crossing cancels surface as order updates and repost through
    /// the normal fill-water-level ledger.
    fn recenter_children(&mut self, now_ts: i64) {
        if self.pending_target.is_some()
            || !self.position_allocation_ready()
            || self.children.is_empty()
            || now_ts < self.submit_blocked_until_us
        {
            return;
        }
        let Some(generation) = self
            .active_target
            .as_ref()
            .map(|target| target.generation_time)
        else {
            return;
        };
        let Some(quote) = MktChannel::instance().get_quote(&self.symbol, self.exec_venue) else {
            return;
        };
        let mut ids: Vec<i64> = self
            .children
            .iter()
            .filter_map(|(client_order_id, meta)| {
                (!meta.is_taker
                    && !meta.cancel_requested
                    && !meta.maker_expired
                    && !meta.amend_in_flight
                    && meta.target_generation == generation
                    && now_ts.saturating_sub(meta.last_amend_ts_us)
                        >= i64::from(self.config.maker_amend_cooldown_ms).saturating_mul(1_000))
                .then_some(*client_order_id)
            })
            .collect();
        ids.sort_unstable();
        for client_order_id in ids {
            let Some(meta) = self.children.get(&client_order_id) else {
                continue;
            };
            let side = meta.side;
            let anchor = own_best_anchor(side, quote.bid, quote.ask);
            let trigger_met = if meta.reprice_pending {
                true
            } else if !anchor.is_finite() || anchor <= 0.0 || meta.anchor_price <= 0.0 {
                false
            } else if self.config.maker_recenter_trigger_bps <= 0.0 {
                true
            } else {
                ((anchor / meta.anchor_price) - 1.0).abs() * 1e4
                    >= self.config.maker_recenter_trigger_bps
            };
            if !trigger_met {
                continue;
            }
            let limits = match self.load_order_limits((quote.bid + quote.ask) * 0.5) {
                Ok(limits) => limits,
                Err(err) => {
                    debug!(
                        "ChaseExecStrategy: strategy_id={} symbol={} recenter skipped: {}",
                        self.strategy_id, self.symbol, err
                    );
                    return;
                }
            };
            let new_price = match level0_maker_price(side, quote.bid, quote.ask, limits.price_tick)
            {
                Ok(price) => price,
                Err(err) => {
                    debug!(
                        "ChaseExecStrategy: strategy_id={} symbol={} recenter price invalid: {}",
                        self.strategy_id, self.symbol, err
                    );
                    continue;
                }
            };
            let order_mgr = MonitorChannel::instance().order_manager();
            let Some(order) = order_mgr.borrow().get(client_order_id) else {
                continue;
            };
            if order.status.is_terminal() {
                continue;
            }
            if (order.price - new_price).abs() <= QTY_EPS {
                // Aligned price unchanged: refreshing the anchor avoids an
                // amend that would only burn queue priority and rate budget.
                if let Some(meta) = self.children.get_mut(&client_order_id) {
                    meta.anchor_price = anchor;
                    meta.pending_anchor_price = None;
                    meta.pending_amend_price = None;
                    meta.reprice_pending = false;
                }
                continue;
            }
            let Some(price_qv) = QuantizedValue::encode_floor(new_price, limits.price_tick) else {
                warn!(
                    "ChaseExecStrategy: strategy_id={} symbol={} cannot quantize amend price={:.8} order_id={}",
                    self.strategy_id, self.symbol, new_price, client_order_id
                );
                continue;
            };
            let req_bin = match order.get_order_modify_bytes(price_qv) {
                Ok(req_bin) => req_bin,
                Err(err) => {
                    debug!(
                        "ChaseExecStrategy: strategy_id={} build modify failed order_id={} err={}",
                        self.strategy_id, client_order_id, err
                    );
                    continue;
                }
            };
            drop(order);
            let params = PreTradeParamsLoader::instance();
            let modify_exchange = match self.exec_venue {
                TradingVenue::BinanceFutures => RuntimeExchange::Binance,
                TradingVenue::OkexFutures => RuntimeExchange::Okex,
                _ => {
                    warn!(
                        "ChaseExecStrategy: strategy_id={} unsupported modify venue {:?}",
                        self.strategy_id, self.exec_venue
                    );
                    return;
                }
            };
            let modify_backend = match ExecBackend::for_exchange(modify_exchange) {
                Ok(backend) => backend,
                Err(err) => {
                    warn!(
                        "ChaseExecStrategy: strategy_id={} cannot resolve execution backend for modify: {err:#}",
                        self.strategy_id
                    );
                    return;
                }
            };
            let modify_limit_per_min = effective_modify_rate_limit_per_min(
                modify_backend,
                params.exec_order_rate_limit_per_min(),
            );
            if self.exec_venue == TradingVenue::OkexFutures && modify_backend == ExecBackend::Native
            {
                if let Err(err) = OkexModifyRateLimiter::check_limit(&self.symbol, now_ts) {
                    warn!(
                        "ChaseExecStrategy: strategy_id={} {}",
                        self.strategy_id, err
                    );
                    if let Some(meta) = self.children.get_mut(&client_order_id) {
                        meta.reprice_pending = true;
                    }
                    self.submit_blocked_until_us =
                        OkexModifyRateLimiter::next_available_at_us(&self.symbol, now_ts).max(
                            self.order_rate_retry_at_us(
                                modify_limit_per_min,
                                params.exec_order_rate_limit_10s(),
                                now_ts,
                            ),
                        );
                    return;
                }
            }
            if let Err(err) = OrderRateLimiter::check_limit(
                OrderRateBucket::Exec,
                modify_limit_per_min,
                params.exec_order_rate_limit_10s(),
                now_ts,
            ) {
                log_order_rate_limit_summary(
                    "ChaseExecStrategy",
                    Some(self.strategy_id),
                    OrderRateBucket::Exec,
                    &self.symbol,
                    &err,
                );
                if let Some(meta) = self.children.get_mut(&client_order_id) {
                    meta.reprice_pending = true;
                }
                self.submit_blocked_until_us = self.order_rate_retry_at_us(
                    modify_limit_per_min,
                    params.exec_order_rate_limit_10s(),
                    now_ts,
                );
                return;
            }
            if let Err(err) = OrderRateLimiter::check_strategy_limit(
                &self.strategy_name,
                self.config.strategy_order_rate_limit_per_min,
                self.config.strategy_order_rate_limit_10s,
                now_ts,
            ) {
                warn!(
                    "ChaseExecStrategy: strategy_id={} symbol={} {}",
                    self.strategy_id, self.symbol, err
                );
                if let Some(meta) = self.children.get_mut(&client_order_id) {
                    meta.reprice_pending = true;
                }
                self.submit_blocked_until_us = self.order_rate_retry_at_us(
                    modify_limit_per_min,
                    params.exec_order_rate_limit_10s(),
                    now_ts,
                );
                return;
            }
            match TradeEngHub::publish_order_request_for(
                client_order_id,
                self.exec_venue.trade_engine_exchange(),
                &req_bin,
            ) {
                Ok(()) => {
                    OrderRateLimiter::record(OrderRateBucket::Exec, client_order_id, now_ts);
                    OrderRateLimiter::record_strategy(&self.strategy_name, client_order_id, now_ts);
                    if self.exec_venue == TradingVenue::OkexFutures
                        && modify_backend == ExecBackend::Native
                    {
                        OkexModifyRateLimiter::record(&self.symbol, now_ts);
                    }
                    if let Some(meta) = self.children.get_mut(&client_order_id) {
                        meta.last_amend_ts_us = now_ts;
                        meta.amend_in_flight = true;
                        meta.reprice_pending = false;
                        meta.pending_anchor_price = Some(anchor);
                        meta.pending_amend_price = Some(new_price);
                    }
                    self.schedule_order_query_watchdog(
                        client_order_id,
                        PendingOrderQueryReason::OrderWatchdog,
                    );
                }
                Err(err) => {
                    warn!(
                        "ChaseExecStrategy: strategy_id={} publish modify failed order_id={} err={}",
                        self.strategy_id, client_order_id, err
                    );
                    if let Some(meta) = self.children.get_mut(&client_order_id) {
                        meta.reprice_pending = true;
                    }
                }
            }
        }
    }

    /// Fill-water-level release. Taker-committed remainder drains first; maker
    /// batches then refill the configured number of open batch-equivalents.
    fn maybe_release(&mut self, now_ts: i64) {
        if self.pending_target.is_some()
            || !self.orphaned_children.is_empty()
            || !MonitorChannel::instance().exec_position_snapshot_ready()
            || !self.position_allocation_ready()
        {
            return;
        }
        let Some((target_qty, generation, from_key, release_side)) =
            self.active_target.as_ref().map(|t| {
                (
                    t.target.qty,
                    t.generation_time,
                    t.from_key.clone(),
                    t.release_side,
                )
            })
        else {
            return;
        };
        let taker_only = self
            .active_target
            .as_ref()
            .is_some_and(|t| t.target.uses_taker_only());

        if let Some(false) = self.symbol_is_tradable() {
            self.cancel_all_children_for_target();
            if self.children.is_empty() {
                if self.completion_reason != Some(ChaseExecCompletionReason::SymbolNotTradable) {
                    warn!(
                        "ChaseExecStrategy: strategy_id={} strategy_name={} symbol={} execution blocked because Manager market rules mark the symbol not tradable",
                        self.strategy_id, self.strategy_name, self.symbol
                    );
                }
                self.completion_reason = Some(ChaseExecCompletionReason::SymbolNotTradable);
            }
            return;
        }
        if self.completion_reason == Some(ChaseExecCompletionReason::SymbolNotTradable) {
            info!(
                "ChaseExecStrategy: strategy_id={} strategy_name={} symbol={} resumed after Manager market rules marked the symbol tradable",
                self.strategy_id, self.strategy_name, self.symbol
            );
            self.completion_reason = None;
        }
        let Some(quote) = MktChannel::instance().get_quote(&self.symbol, self.exec_venue) else {
            return;
        };

        let position_qty = self.virtual_position_qty.unwrap_or(0.0);
        let side_sign = signed_qty_from_side(release_side, 1.0);
        let remaining_base = (target_qty - position_qty) * side_sign;
        if remaining_base <= QTY_EPS {
            // Gap consumed or overshot: the strategy stops here rather than
            // trading the overshoot back toward the target. Any residual
            // taker obligation is unexecutable overhang; release it so it
            // cannot pin `has_execution_in_flight` forever.
            self.taker_pending_base_qty = 0.0;
            if self.children.is_empty() {
                self.completion_reason = Some(ChaseExecCompletionReason::TargetReached);
            }
            return;
        }
        let side = release_side;
        let reference_price = match side {
            Side::Buy => quote.bid,
            Side::Sell => quote.ask,
        };
        let limits = match self.load_order_limits(reference_price) {
            Ok(limits) => limits,
            Err(err) => {
                warn!(
                    "ChaseExecStrategy: strategy_id={} symbol={} cannot load order limits: {}",
                    self.strategy_id, self.symbol, err
                );
                return;
            }
        };
        let maker_price = match level0_maker_price(side, quote.bid, quote.ask, limits.price_tick) {
            Ok(price) => price,
            Err(err) => {
                warn!(
                    "ChaseExecStrategy: strategy_id={} symbol={} invalid maker price: {}",
                    self.strategy_id, self.symbol, err
                );
                return;
            }
        };
        let minimum_base_qty = match minimum_executable_base_qty(maker_price, limits) {
            Ok(qty) => qty,
            Err(err) => {
                warn!(
                    "ChaseExecStrategy: strategy_id={} symbol={} invalid order limits: {}",
                    self.strategy_id, self.symbol, err
                );
                return;
            }
        };
        if remaining_base * reference_price <= self.config.target_tolerance_usdt {
            // Within tolerance the target is declared done; abandon any
            // residual taker obligation so the strategy can complete.
            self.taker_pending_base_qty = 0.0;
            if self.children.is_empty() {
                self.completion_reason = Some(ChaseExecCompletionReason::TargetTolerance);
            }
            return;
        }
        if remaining_base + QTY_EPS < minimum_base_qty {
            // Below the venue minimum the gap can never execute; release the
            // obligation back instead of pinning it as taker-pending dust.
            self.taker_pending_base_qty = 0.0;
            if self.children.is_empty() {
                self.completion_reason = Some(ChaseExecCompletionReason::ExchangeMinimum);
            }
            return;
        }
        self.completion_reason = None;
        if now_ts < self.submit_blocked_until_us {
            return;
        }

        let open_unfilled_base = self.open_unfilled_base_qty();
        let uncommitted_base = (remaining_base - open_unfilled_base).max(0.0);
        // Taker obligations beyond the uncommitted gap are phantom overhang
        // that can never execute without overshooting the target; clamp them
        // back into the water level.
        self.taker_pending_base_qty =
            clamp_taker_pending_to_gap(self.taker_pending_base_qty, uncommitted_base);
        if uncommitted_base <= QTY_EPS {
            return;
        }

        // 1) Taker obligations drain first: escalated remainders and
        //    taker-only targets send market orders before any maker release.
        let mut taker_qty = self.taker_pending_base_qty.min(uncommitted_base);
        if taker_only {
            taker_qty = uncommitted_base;
        }
        if taker_qty > QTY_EPS {
            let qty_multiplier = match limits.qty_multiplier_at(reference_price) {
                Ok(multiplier) => multiplier,
                Err(err) => {
                    warn!(
                        "ChaseExecStrategy: strategy_id={} symbol={} invalid taker multiplier: {}",
                        self.strategy_id, self.symbol, err
                    );
                    return;
                }
            };
            let qty_venue = align_child_qty_floor(taker_qty / qty_multiplier, limits.qty_step);
            let qty_base = qty_venue * qty_multiplier;
            if qty_base + QTY_EPS >= minimum_base_qty {
                let sent = self.send_child(
                    side,
                    OrderType::Market,
                    qty_venue,
                    qty_base,
                    qty_multiplier,
                    0.0,
                    0.0,
                    generation,
                    &from_key,
                    &quote,
                    now_ts,
                    true,
                );
                if sent > QTY_EPS {
                    self.taker_pending_base_qty = (self.taker_pending_base_qty - sent).max(0.0);
                }
            } else {
                // The drainable remainder floors below the venue minimum and
                // can never execute; release it back to the uncommitted water
                // level so it cannot pin `has_execution_in_flight` forever.
                self.taker_pending_base_qty = (self.taker_pending_base_qty - taker_qty).max(0.0);
            }
        }
        if taker_only {
            return;
        }

        // 2) Maker top-up: release at most one batch per pass. Fills reduce the
        //    open water level and reopen capacity up to max_open_batches.
        //    Taker-committed remainders retain priority over maker capacity.
        let open_unfilled_base = self.open_unfilled_base_qty();
        let maker_capacity_base =
            (remaining_base - open_unfilled_base - self.taker_pending_base_qty).max(0.0);
        if maker_capacity_base <= QTY_EPS {
            return;
        }
        let Some(effective_batch_usdt) = self.effective_batch_usdt(generation, remaining_base)
        else {
            debug!(
                "ChaseExecStrategy: strategy_id={} symbol={} waiting for mark price before sizing target generation={}",
                self.strategy_id, self.symbol, generation
            );
            return;
        };
        let release_usdt = maker_release_usdt(
            effective_batch_usdt,
            self.config.max_open_batches,
            open_unfilled_base * reference_price,
            maker_capacity_base * reference_price,
        );
        if release_usdt <= 0.0 {
            return;
        }
        let qty_multiplier = match limits.qty_multiplier_at(maker_price) {
            Ok(multiplier) => multiplier,
            Err(err) => {
                warn!(
                    "ChaseExecStrategy: strategy_id={} symbol={} invalid maker multiplier: {}",
                    self.strategy_id, self.symbol, err
                );
                return;
            }
        };
        let qty_venue =
            align_child_qty_floor(release_usdt / maker_price / qty_multiplier, limits.qty_step);
        let qty_base = qty_venue * qty_multiplier;
        if qty_base + QTY_EPS < minimum_base_qty {
            return;
        }
        self.send_child(
            side,
            OrderType::Limit,
            qty_venue,
            qty_base,
            qty_multiplier,
            maker_price,
            own_best_anchor(side, quote.bid, quote.ask),
            generation,
            &from_key,
            &quote,
            now_ts,
            false,
        );
    }

    /// Creates the local order, registers the child, and publishes the request.
    /// Returns the committed base qty on success (0 on failure).
    #[allow(clippy::too_many_arguments)]
    fn send_child(
        &mut self,
        side: Side,
        order_type: OrderType,
        qty_venue: f64,
        qty_base: f64,
        qty_multiplier: f64,
        price: f64,
        anchor_price: f64,
        generation: i64,
        from_key: &[u8],
        quote: &Quote,
        now_ts: i64,
        is_taker: bool,
    ) -> f64 {
        MonitorChannel::instance().refresh_exec_risk_state();
        if order_type.is_limit()
            && MonitorChannel::instance()
                .check_pending_limit_order_for_exec(&self.symbol, side)
                .is_err()
        {
            return 0.0;
        }
        let client_order_id = self.next_order_id();
        let signal_bbo = SignalBbo::new(
            SignalBboLeg::checked(
                self.exec_venue.to_u8(),
                quote.ts,
                quote.bid,
                quote.bid_qty,
                quote.ask,
                quote.ask_qty,
            ),
            None,
        );
        MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .create_order_with_pending_limit_flag(
                self.exec_venue,
                client_order_id,
                order_type,
                self.symbol.clone(),
                side,
                qty_venue,
                price,
                false,
                qty_multiplier,
                order_type.is_limit(),
            );
        let _ = MonitorChannel::instance()
            .order_manager()
            .borrow_mut()
            .update(client_order_id, |order| {
                order.set_signal_meta(now_ts, CHASE_EXEC_SIGNAL_KIND);
                if quote.ts > 0 {
                    order.set_mkt_time(quote.ts);
                }
            });
        self.children.insert(
            client_order_id,
            ChaseChildMeta {
                side,
                order_base_qty: qty_base,
                accounted_fill_base_qty: 0.0,
                target_generation: generation,
                is_taker,
                maker_expired: false,
                cancel_for_target: false,
                anchor_price,
                pending_anchor_price: None,
                pending_amend_price: None,
                // The placement counts as the child's first pricing event so
                // the amend cooldown also gates the first amend.
                last_amend_ts_us: now_ts,
                amend_in_flight: false,
                reprice_pending: false,
                expires_at_us: if is_taker {
                    0
                } else {
                    now_ts.saturating_add(i64::from(self.config.maker_timeout_sec) * 1_000_000)
                },
                cancel_requested: false,
                signal_ts: now_ts,
                signal_bbo,
                price_offset: 0.0,
                from_key: from_key.to_vec(),
            },
        );
        match self.send_order(client_order_id) {
            Ok(()) => {
                self.schedule_order_query_watchdog(
                    client_order_id,
                    PendingOrderQueryReason::OrderWatchdog,
                );
                qty_base
            }
            Err(err) => {
                self.children.remove(&client_order_id);
                let _ = MonitorChannel::instance()
                    .order_manager()
                    .borrow_mut()
                    .remove(client_order_id);
                if is_exec_rate_limit_error(&err) || is_chase_strategy_rate_limit_error(&err) {
                    let params = PreTradeParamsLoader::instance();
                    self.submit_blocked_until_us =
                        self.submit_blocked_until_us
                            .max(self.order_rate_retry_at_us(
                                params.exec_order_rate_limit_per_min(),
                                params.exec_order_rate_limit_10s(),
                                now_ts,
                            ));
                    warn!(
                        "ChaseExecStrategy: strategy_id={} symbol={} deferred after local exec order-rate limit retry_at_us={}",
                        self.strategy_id, self.symbol, self.submit_blocked_until_us
                    );
                } else {
                    warn!(
                        "ChaseExecStrategy: strategy_id={} send child failed order_id={} err={}",
                        self.strategy_id, client_order_id, err
                    );
                }
                0.0
            }
        }
    }

    fn send_order(&self, client_order_id: i64) -> Result<(), String> {
        let order = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id)
            .ok_or_else(|| "missing local child order".to_string())?;
        let req_bin = order.get_order_request_bytes()?;
        let now_ts = get_timestamp_us();
        let params = PreTradeParamsLoader::instance();
        if let Err(err) = OrderRateLimiter::check_limit(
            OrderRateBucket::Exec,
            params.exec_order_rate_limit_per_min(),
            params.exec_order_rate_limit_10s(),
            now_ts,
        ) {
            log_order_rate_limit_summary(
                "ChaseExecStrategy",
                Some(self.strategy_id),
                OrderRateBucket::Exec,
                &self.symbol,
                &err,
            );
            return Err(err);
        }
        if let Err(err) = OrderRateLimiter::check_strategy_limit(
            &self.strategy_name,
            self.config.strategy_order_rate_limit_per_min,
            self.config.strategy_order_rate_limit_10s,
            now_ts,
        ) {
            warn!(
                "ChaseExecStrategy: strategy_id={} symbol={} {}",
                self.strategy_id, self.symbol, err
            );
            return Err(err);
        }
        TradeEngHub::publish_order_request_for(
            client_order_id,
            order.venue.trade_engine_exchange(),
            &req_bin,
        )
        .map_err(|err| err.to_string())?;
        OrderRateLimiter::record(OrderRateBucket::Exec, client_order_id, now_ts);
        OrderRateLimiter::record_strategy(&self.strategy_name, client_order_id, now_ts);
        Ok(())
    }

    fn request_cancel(&mut self, client_order_id: i64) {
        if self
            .children
            .get(&client_order_id)
            .is_some_and(|meta| meta.cancel_requested)
        {
            return;
        }
        let Some(order) = MonitorChannel::try_order_manager()
            .and_then(|manager| manager.borrow().get(client_order_id))
        else {
            self.finish_child_order(client_order_id);
            return;
        };
        if order.status.is_terminal() {
            self.finish_child_order(client_order_id);
            return;
        }
        let exchange = order.venue.trade_engine_exchange();
        match order.get_order_cancel_bytes() {
            Ok(req_bin) => {
                if TradeEngHub::publish_order_request_for(client_order_id, exchange, &req_bin)
                    .is_ok()
                {
                    if let Some(meta) = self.children.get_mut(&client_order_id) {
                        meta.cancel_requested = true;
                    }
                    self.schedule_order_query_watchdog(
                        client_order_id,
                        PendingOrderQueryReason::CancelWatchdog,
                    );
                }
            }
            Err(err) => warn!(
                "ChaseExecStrategy: strategy_id={} build cancel failed order_id={} err={}",
                self.strategy_id, client_order_id, err
            ),
        }
    }

    fn retry_cancel_after_live_update(&mut self, client_order_id: i64) {
        let should_retry = self
            .children
            .get(&client_order_id)
            .is_some_and(|meta| meta.cancel_requested);
        if !should_retry {
            return;
        }
        if let Some(meta) = self.children.get_mut(&client_order_id) {
            meta.cancel_requested = false;
        }
        self.request_cancel(client_order_id);
    }

    fn confirm_pending_amend(&mut self, client_order_id: i64, actual_price: f64) -> bool {
        let Some(meta) = self.children.get_mut(&client_order_id) else {
            return false;
        };
        let Some(expected_price) = meta.pending_amend_price else {
            return false;
        };
        if !amend_price_matches(actual_price, expected_price) {
            return false;
        }
        if let Some(anchor) = meta.pending_anchor_price.take() {
            meta.anchor_price = anchor;
        }
        meta.pending_amend_price = None;
        meta.amend_in_flight = false;
        meta.reprice_pending = false;
        true
    }

    fn reject_pending_amend(&mut self, client_order_id: i64) {
        if let Some(meta) = self.children.get_mut(&client_order_id) {
            meta.pending_anchor_price = None;
            meta.pending_amend_price = None;
            meta.amend_in_flight = false;
            meta.reprice_pending = true;
        }
    }

    fn account_fill_progress(
        &mut self,
        client_order_id: i64,
        previous_venue_qty: f64,
        cumulative_venue_qty: f64,
        fill_price: f64,
    ) {
        let delta_venue_qty = (cumulative_venue_qty - previous_venue_qty).max(0.0);
        let delta_base_at_fill = MonitorChannel::instance()
            .qty_to_base_at_price(self.exec_venue, &self.symbol, delta_venue_qty, fill_price)
            .unwrap_or(0.0);
        if !delta_base_at_fill.is_finite() || delta_base_at_fill < 0.0 {
            warn!(
                "ChaseExecStrategy: strategy_id={} invalid fill delta order_id={} delta_base_qty={}",
                self.strategy_id, client_order_id, delta_base_at_fill
            );
            return;
        }
        let Some((side, delta_base_qty)) = self.children.get_mut(&client_order_id).map(|meta| {
            let next_accounted =
                (meta.accounted_fill_base_qty + delta_base_at_fill).min(meta.order_base_qty);
            let delta_base_qty = next_accounted - meta.accounted_fill_base_qty;
            meta.accounted_fill_base_qty = next_accounted;
            (meta.side, delta_base_qty)
        }) else {
            return;
        };
        self.apply_fill_delta(client_order_id, side, delta_base_qty);
    }

    fn apply_fill_delta(&mut self, client_order_id: i64, side: Side, delta_base_qty: f64) {
        if delta_base_qty <= QTY_EPS {
            return;
        }
        let signed_fill_qty = signed_qty_from_side(side, delta_base_qty);
        if let Some(position_qty) = self.virtual_position_qty.as_mut() {
            *position_qty += signed_fill_qty;
            self.last_position_fill_at_us = get_timestamp_us();
        } else {
            warn!(
                "ChaseExecStrategy: strategy_id={} strategy_name={} symbol={} fill arrived before position allocation order_id={} signed_fill_qty={:.8}",
                self.strategy_id, self.strategy_name, self.symbol, client_order_id, signed_fill_qty
            );
        }
    }

    /// Removes a child on its terminal event and routes the confirmed unfilled
    /// remainder: stale/cancelled-for-target remainders are dropped back to the
    /// ledger, taker-committed remainders stay taker-committed, everything else
    /// (post-only/GTX cross cancel, unexpected exchange cancel) returns through
    /// `unallocated` and reposts as maker on the next release pass.
    fn finish_child_order(&mut self, client_order_id: i64) {
        self.clear_order_query_state(client_order_id);
        let Some(meta) = self.children.remove(&client_order_id) else {
            return;
        };
        let remainder = (meta.order_base_qty - meta.accounted_fill_base_qty).max(0.0);
        if remainder <= QTY_EPS {
            return;
        }
        let generation_active = self
            .active_target
            .as_ref()
            .is_some_and(|target| target.generation_time == meta.target_generation);
        if meta.cancel_for_target || !generation_active {
            return;
        }
        if meta.is_taker || meta.maker_expired {
            self.taker_pending_base_qty += remainder;
        }
    }

    fn uniform_ctx(&self, client_order_id: i64) -> UniformPublishCtx {
        self.children
            .get(&client_order_id)
            .map(|meta| UniformPublishCtx {
                signal_bbo: meta.signal_bbo,
                signal_ts: meta.signal_ts,
                from_key: meta.from_key.clone(),
                price_offset: meta.price_offset,
            })
            .unwrap_or_else(|| UniformPublishCtx {
                signal_bbo: None,
                signal_ts: 0,
                from_key: Vec::new(),
                price_offset: 0.0,
            })
    }

    fn apply_order_update_inner(&mut self, update: &dyn OrderUpdate) -> bool {
        let client_order_id = update.client_order_id();
        let order_mgr = MonitorChannel::instance().order_manager();
        let mut manager = order_mgr.borrow_mut();
        let Some(current) = manager.get(client_order_id) else {
            return false;
        };
        if OrderManager::should_skip_idempotent_order_update(
            &current,
            update.execution_type(),
            update.status(),
            update.order_id(),
            update.cumulative_filled_quantity(),
            "ChaseExecStrategy",
            self.strategy_id,
        )
        .is_some()
        {
            return false;
        }
        let previous_fill = current.cumulative_filled_quantity;
        let effective_fill = current
            .protected_cumulative_fill(update.cumulative_filled_quantity())
            .effective_cum;
        let status = update.status();
        let pending_amend_price = self
            .children
            .get(&client_order_id)
            .and_then(|meta| meta.pending_amend_price);
        let confirms_pending_amend = pending_amend_price
            .is_some_and(|expected| amend_price_matches(update.price(), expected));
        let changed = manager.apply_remote_update(client_order_id, |order| {
            order.apply_replacement_fields(update);
            if confirms_pending_amend {
                order.price = update.price();
                order.price_qv = None;
            }
            order.set_exchange_order_id(update.order_id());
            order.cumulative_filled_quantity = effective_fill;
            match status {
                OrderStatus::New | OrderStatus::PartiallyFilled => {
                    order.status = OrderExecutionStatus::Create;
                    if order.timestamp.create_t == 0 {
                        order.set_create_time(update.event_time());
                    }
                }
                OrderStatus::Canceled => {
                    order.status = OrderExecutionStatus::Cancelled;
                    order.set_end_time(update.event_time());
                }
                OrderStatus::Filled => {
                    order.status = OrderExecutionStatus::Filled;
                    order.set_end_time(update.event_time());
                }
                OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                    order.status = OrderExecutionStatus::Rejected;
                    order.set_end_time(update.event_time());
                }
            }
        });
        let snapshot = manager
            .get(client_order_id)
            .map(|order| (order, self.uniform_ctx(client_order_id)));
        drop(manager);
        if !changed {
            return false;
        }
        let fill_price = update.price().max(current.price);
        self.account_fill_progress(client_order_id, previous_fill, effective_fill, fill_price);
        let amend_confirmed = snapshot
            .as_ref()
            .is_some_and(|(order, _)| self.confirm_pending_amend(client_order_id, order.price));
        if !amend_confirmed
            && update.execution_type() == ExecutionType::Replaced
            && pending_amend_price.is_some()
        {
            self.reject_pending_amend(client_order_id);
        }
        if let Some((order, ctx)) = snapshot.as_ref() {
            if status == OrderStatus::New {
                publish_uniform_new_order(
                    update,
                    order,
                    previous_fill,
                    ctx,
                    "ChaseExecStrategy",
                    self.strategy_id,
                );
            } else if matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled) {
                publish_uniform_trade_order_from_order_update(
                    update,
                    order,
                    previous_fill,
                    ctx,
                    "ChaseExecStrategy",
                    self.strategy_id,
                );
            } else if status.is_finished() {
                publish_uniform_terminal_order(
                    update,
                    order,
                    previous_fill,
                    ctx,
                    "ChaseExecStrategy",
                    self.strategy_id,
                );
            }
        }
        if status.is_finished() {
            self.finish_child_order(client_order_id);
        } else {
            if !self
                .children
                .get(&client_order_id)
                .is_some_and(|meta| meta.amend_in_flight)
            {
                self.clear_live_order_query_state(client_order_id);
            }
            self.retry_cancel_after_live_update(client_order_id);
        }
        true
    }

    fn apply_trade_update_inner(&mut self, trade: &dyn TradeUpdate) -> bool {
        let Some(status) = trade.order_status() else {
            return false;
        };
        if !matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled) {
            return false;
        }
        let client_order_id = trade.client_order_id();
        let order_mgr = MonitorChannel::instance().order_manager();
        let mut manager = order_mgr.borrow_mut();
        let Some(current) = manager.get(client_order_id) else {
            return false;
        };
        if OrderManager::should_skip_idempotent_trade_update(
            &current,
            status,
            trade.cumulative_filled_quantity(),
            trade.event_time(),
            "ChaseExecStrategy",
            self.strategy_id,
        )
        .is_some()
        {
            return false;
        }
        let previous_fill = current.cumulative_filled_quantity;
        let cumulative_fill = trade.cumulative_filled_quantity();
        let changed = manager.apply_remote_update(client_order_id, |order| {
            order.cumulative_filled_quantity = cumulative_fill;
            order.set_exchange_order_id(trade.order_id());
            order.status = if status == OrderStatus::Filled {
                OrderExecutionStatus::Filled
            } else {
                OrderExecutionStatus::Create
            };
            if status == OrderStatus::Filled {
                order.set_end_time(trade.event_time());
            }
        });
        let snapshot = manager
            .get(client_order_id)
            .map(|order| (order, self.uniform_ctx(client_order_id)));
        drop(manager);
        if !changed {
            return false;
        }
        let fill_price = if trade.price() > 0.0 {
            trade.price()
        } else {
            current.price
        };
        self.account_fill_progress(client_order_id, previous_fill, cumulative_fill, fill_price);
        if let Some((order, ctx)) = snapshot.as_ref() {
            publish_uniform_trade_order(
                trade,
                order,
                previous_fill,
                status,
                ctx,
                "ChaseExecStrategy",
                self.strategy_id,
            );
        }
        if status == OrderStatus::Filled {
            self.finish_child_order(client_order_id);
        } else {
            if !self
                .children
                .get(&client_order_id)
                .is_some_and(|meta| meta.amend_in_flight)
            {
                self.clear_live_order_query_state(client_order_id);
            }
            self.retry_cancel_after_live_update(client_order_id);
        }
        true
    }

    /// Binance native responses can confirm the applied price immediately.
    /// OKX and RapidX acknowledgements are acceptance-only, so their zero-price
    /// responses leave the amend watchdog armed until an order update confirms it.
    fn apply_modify_response(&mut self, response: &dyn TradeEngineResponse) {
        let client_order_id = response.client_order_id();
        if let Some(price) = response.response_price().filter(|price| *price > 0.0) {
            let _ = MonitorChannel::instance()
                .order_manager()
                .borrow_mut()
                .update(client_order_id, |order| {
                    order.price = price;
                    if let Some(order_id) = response.order_id().filter(|id| *id > 0) {
                        order.set_exchange_order_id(order_id);
                    }
                });
            if self.confirm_pending_amend(client_order_id, price) {
                self.clear_live_order_query_state(client_order_id);
            }
        }
    }

    pub fn snapshot(&self, _now_ts: i64) -> ChaseExecSnapshot {
        let account_position_qty =
            MonitorChannel::instance().get_position_qty(&self.symbol, self.exec_venue);
        let position_qty = self.virtual_position_qty.unwrap_or(0.0);
        let live_order_qty = self.live_order_signed_qty();
        let target_qty = self.target_qty();
        let taker_pending_qty = self
            .active_target
            .as_ref()
            .map(|target| self.taker_pending_base_qty * (target.target.qty - position_qty).signum())
            .unwrap_or(self.taker_pending_base_qty);
        let execution_complete = self.completion_reason.is_some() && self.children.is_empty();
        ChaseExecSnapshot {
            algorithm: "chase_exec".to_string(),
            pov: None,
            strategy_name: self.strategy_name.clone(),
            source_updated_at_ms: self.source_updated_at_us / 1_000,
            symbol: self.symbol.clone(),
            exec_venue: self.exec_venue,
            account_position_qty,
            position_qty,
            effective_position_qty: position_qty,
            position_allocated: self.position_allocation_ready(),
            target_qty,
            pending_qty: taker_pending_qty,
            live_order_qty,
            live_children: self.children.len(),
            execution_complete,
            completion_reason: self
                .completion_reason
                .map(|reason| reason.as_str().to_string())
                .unwrap_or_default(),
        }
    }
}

impl HedgeOrderReconcileCommon for ChaseExecStrategy {
    fn hedge_reconcile_strategy_name(&self) -> &'static str {
        "ChaseExec"
    }

    fn hedge_reconcile_strategy_id(&self) -> i32 {
        self.strategy_id
    }

    fn hedge_reconcile_state(&self) -> &HedgeOrderReconcileState {
        &self.reconcile_state
    }

    fn hedge_reconcile_state_mut(&mut self) -> &mut HedgeOrderReconcileState {
        &mut self.reconcile_state
    }

    fn is_hedge_order_tracked(&self, client_order_id: i64) -> bool {
        self.children.contains_key(&client_order_id)
    }

    fn handoff_hedge_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        reason: &str,
    ) -> bool {
        if !self.children.contains_key(&client_order_id) {
            return false;
        }
        let handoff = OrphanHandoff {
            client_order_id,
            source_strategy_id: self.strategy_id,
            source_kind: OrphanSourceKind::Hedge,
            uniform_ctx: self.uniform_ctx(client_order_id),
            reason: reason.to_string(),
        };
        let Some(orphan_mgr) = MonitorChannel::try_orphan_strategy_mgr() else {
            return false;
        };
        if !orphan_mgr
            .borrow_mut()
            .adopt_orphan_order_id(OrphanStrategyRole::Exec, &handoff)
        {
            return false;
        }
        self.clear_order_query_state(client_order_id);
        let meta = self
            .children
            .remove(&client_order_id)
            .expect("child order checked before synchronous orphan adoption");
        self.orphaned_children.insert(client_order_id, meta);
        true
    }

    fn handle_hedge_open_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        // Post-only (GTX) rejects are transient: the book moved and the order
        // was refused instead of crossing. The remainder already flows back
        // through `unallocated`, so reposting at the next tick's fresh anchor
        // is the desired immediate retry. Other rejects (price/qty filters,
        // permissions) are likely deterministic; without a backoff the
        // release pass would spin a new open request every clock tick.
        let post_only_rejected = response.is_post_only_rejected();
        warn!(
            "ChaseExecStrategy: strategy_id={} child open failed order_id={} code={}({}) post_only_rejected={}",
            self.strategy_id,
            client_order_id,
            response.error_code(),
            code_desc,
            post_only_rejected
        );
        self.finish_child_order(client_order_id);
        if !post_only_rejected {
            self.submit_blocked_until_us = self
                .submit_blocked_until_us
                .max(get_timestamp_us().saturating_add(OPEN_REJECT_BACKOFF_US));
        }
        if let Some(manager) = MonitorChannel::try_order_manager() {
            let _ = manager.borrow_mut().remove(client_order_id);
        }
    }

    fn handle_hedge_other_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        if response.request_kind() == TradeRequestKind::Modify {
            warn!(
                "ChaseExecStrategy: strategy_id={} modify failed order_id={} code={}({}) {}",
                self.strategy_id,
                client_order_id,
                response.error_code(),
                code_desc,
                self.hedge_order_trace_snapshot(client_order_id)
            );
            // Keep the live order and retry the amend; if the exchange in fact
            // removed it, the terminal order update finishes the child.
            self.reject_pending_amend(client_order_id);
            return;
        }
        warn!(
            "{}: strategy_id={} hedge other failed: req_type={} status={} code={}({}) client_order_id={} {}",
            self.hedge_reconcile_strategy_name(),
            self.hedge_reconcile_strategy_id(),
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id,
            self.hedge_order_trace_snapshot(client_order_id)
        );
    }
}

impl Strategy for ChaseExecStrategy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn apply_exec_orphan_terminal(&mut self, terminal: &ExecOrphanTerminal) -> bool {
        if terminal.source_kind != OrphanSourceKind::Hedge {
            warn!(
                "ChaseExecStrategy: strategy_id={} reject orphan terminal order_id={} source_kind={:?}",
                self.strategy_id, terminal.client_order_id, terminal.source_kind
            );
            return false;
        }
        let Some(existing_meta) = self
            .orphaned_children
            .get(&terminal.client_order_id)
            .cloned()
        else {
            warn!(
                "ChaseExecStrategy: strategy_id={} missing orphan metadata order_id={}",
                self.strategy_id, terminal.client_order_id
            );
            return false;
        };
        if existing_meta.side != terminal.side {
            warn!(
                "ChaseExecStrategy: strategy_id={} reject orphan terminal side mismatch order_id={} expected={:?} actual={:?}",
                self.strategy_id, terminal.client_order_id, existing_meta.side, terminal.side
            );
            return false;
        }
        if !terminal.filled_base_qty.is_finite() || terminal.filled_base_qty < 0.0 {
            warn!(
                "ChaseExecStrategy: strategy_id={} reject invalid orphan terminal fill order_id={} filled_base_qty={}",
                self.strategy_id, terminal.client_order_id, terminal.filled_base_qty
            );
            return false;
        }

        let mut meta = self
            .orphaned_children
            .remove(&terminal.client_order_id)
            .expect("orphan metadata checked above");
        let previous_accounted = meta.accounted_fill_base_qty;
        let next_accounted = terminal
            .filled_base_qty
            .max(previous_accounted)
            .min(meta.order_base_qty);
        meta.accounted_fill_base_qty = next_accounted;
        let delta = next_accounted - previous_accounted;
        let side = meta.side;
        self.apply_fill_delta(terminal.client_order_id, side, delta);
        let remainder = (meta.order_base_qty - meta.accounted_fill_base_qty).max(0.0);
        let generation_active = self
            .active_target
            .as_ref()
            .is_some_and(|target| target.generation_time == meta.target_generation);
        if remainder > QTY_EPS
            && generation_active
            && !meta.cancel_for_target
            && (meta.is_taker || meta.maker_expired)
        {
            self.taker_pending_base_qty += remainder;
        }
        info!(
            "ChaseExecStrategy: strategy_id={} applied orphan terminal order_id={} terminal_ts={} side={:?} source_order_base_qty={:.8} tracked_order_base_qty={:.8} cumulative_filled_base_qty={:.8} newly_accounted_base_qty={:.8} price={:.8}",
            self.strategy_id,
            terminal.client_order_id,
            terminal.terminal_ts,
            terminal.side,
            terminal.order_base_qty,
            existing_meta.order_base_qty,
            terminal.filled_base_qty,
            delta,
            terminal.price
        );
        true
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.strategy_id
            && self.children.contains_key(&order_id)
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        debug!(
            "ChaseExecStrategy: strategy_id={} ignore signal {:?}; targets come from Redis",
            self.strategy_id, signal.signal_type
        );
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        if self.apply_order_update_inner(update) {
            PersistChannel::with(|channel| channel.publish_order_update(update));
        }
    }

    fn apply_order_amendment_result(&mut self, update: &dyn OrderUpdate) {
        let client_order_id = update.client_order_id();
        let amend_in_flight = self
            .children
            .get(&client_order_id)
            .is_some_and(|meta| meta.amend_in_flight);
        if !amend_in_flight {
            return;
        }
        match update.amendment_succeeded() {
            Some(true) if update.price().is_finite() && update.price() > 0.0 => {
                let price = update.price();
                let _ = MonitorChannel::instance()
                    .order_manager()
                    .borrow_mut()
                    .update(client_order_id, |order| {
                        order.price = price;
                        order.price_qv = None;
                    });
                if self.confirm_pending_amend(client_order_id, price) {
                    self.clear_order_query_state(client_order_id);
                }
            }
            Some(false) => {
                self.reject_pending_amend(client_order_id);
                self.clear_order_query_state(client_order_id);
            }
            _ => {}
        }
    }

    fn apply_live_order_query(
        &mut self,
        update: &dyn OrderUpdate,
        query_advanced_fill: bool,
    ) -> bool {
        let client_order_id = update.client_order_id();
        if !self.children.contains_key(&client_order_id) {
            return false;
        }
        let retry_cancel = self
            .order_query_reason(client_order_id)
            .is_some_and(HedgeOrderReconcileState::is_cancel_reconcile_reason);
        let amend_in_flight = self
            .children
            .get(&client_order_id)
            .is_some_and(|meta| meta.amend_in_flight);
        let cancel_already_retried = retry_cancel
            && query_advanced_fill
            && self
                .children
                .get(&client_order_id)
                .is_some_and(|meta| meta.cancel_requested);
        if retry_cancel {
            if let Some(meta) = self.children.get_mut(&client_order_id) {
                meta.cancel_requested = false;
            }
        }
        if cancel_already_retried {
            self.clear_pending_order_query(client_order_id);
        } else {
            self.clear_order_query_state(client_order_id);
        }
        if !query_advanced_fill {
            self.apply_order_update(update);
        }
        let order_price = update.price();
        if order_price.is_finite() && order_price > 0.0 {
            let _ = MonitorChannel::instance()
                .order_manager()
                .borrow_mut()
                .update(client_order_id, |order| {
                    order.price = order_price;
                    order.price_qv = None;
                });
        }
        if amend_in_flight
            && self
                .children
                .get(&client_order_id)
                .is_some_and(|meta| meta.amend_in_flight)
        {
            if !(order_price.is_finite()
                && order_price > 0.0
                && self.confirm_pending_amend(client_order_id, order_price))
            {
                self.reject_pending_amend(client_order_id);
            }
        }
        if !cancel_already_retried {
            self.clear_order_query_state(client_order_id);
        }
        if cancel_already_retried {
            if let Some(meta) = self.children.get_mut(&client_order_id) {
                meta.cancel_requested = true;
            }
        } else if retry_cancel {
            self.request_cancel(client_order_id);
        }
        true
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        if self.apply_trade_update_inner(trade) {
            PersistChannel::with(|channel| channel.publish_trade_update(trade));
        }
    }

    fn apply_trade_engine_response(&mut self, response: &dyn TradeEngineResponse) {
        if response.request_kind() == TradeRequestKind::Modify
            && response.is_request_success()
            && self.is_strategy_order(response.client_order_id())
        {
            self.apply_modify_response(response);
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
        self.handle_order_query_watchdogs();
        self.cancel_children_when_target_no_longer_needs_them();
        self.handle_child_timeouts(now_ts);
        self.retry_unsent_cancels();
        self.process_pending_target();
        self.recenter_children(now_ts);
        self.maybe_release(now_ts);
    }

    fn is_active(&self) -> bool {
        self.alive_flag
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use order_common::{TradeEngineResponseMessage, TradeRequestType};
    use symbol_utils::Exchange;

    fn config() -> ChaseExecConfig {
        ChaseExecConfig::default()
    }

    fn make_strategy() -> ChaseExecStrategy {
        ChaseExecStrategy::new(
            1,
            "chase_alpha",
            "BTCUSDT",
            TradingVenue::BinanceFutures,
            config(),
        )
    }

    fn maker_child_meta(
        side: Side,
        order_base_qty: f64,
        accounted_fill_base_qty: f64,
        generation: i64,
    ) -> ChaseChildMeta {
        ChaseChildMeta {
            side,
            order_base_qty,
            accounted_fill_base_qty,
            target_generation: generation,
            is_taker: false,
            maker_expired: false,
            cancel_for_target: false,
            anchor_price: 50_000.0,
            pending_anchor_price: None,
            pending_amend_price: None,
            last_amend_ts_us: 0,
            amend_in_flight: false,
            reprice_pending: false,
            expires_at_us: i64::MAX,
            cancel_requested: false,
            signal_ts: 1,
            signal_bbo: None,
            price_offset: 0.0,
            from_key: b"chase_alpha".to_vec(),
        }
    }

    fn active_target(qty: f64, signal: i32, generation: i64) -> ActiveTarget {
        let release_side = if qty >= 0.0 { Side::Buy } else { Side::Sell };
        ActiveTarget {
            target: BatchExecTarget { qty, signal },
            generation_time: generation,
            from_key: b"chase_alpha".to_vec(),
            release_side,
            effective_batch_usdt: None,
        }
    }

    #[test]
    fn maker_price_and_trigger_anchor_are_fixed_to_own_best() {
        assert_eq!(
            level0_maker_price(Side::Buy, 100.01, 100.03, 0.01),
            Ok(100.01)
        );
        assert_eq!(
            level0_maker_price(Side::Sell, 100.01, 100.03, 0.01),
            Ok(100.03)
        );
        assert_eq!(own_best_anchor(Side::Buy, 100.01, 100.03), 100.01);
        assert_eq!(own_best_anchor(Side::Sell, 100.01, 100.03), 100.03);
    }

    #[test]
    fn rapidx_modify_rate_is_capped_by_replace_order_contract() {
        assert_eq!(
            effective_modify_rate_limit_per_min(ExecBackend::Ltp, 0),
            300
        );
        assert_eq!(
            effective_modify_rate_limit_per_min(ExecBackend::Ltp, 400),
            300
        );
        assert_eq!(
            effective_modify_rate_limit_per_min(ExecBackend::Ltp, 200),
            200
        );
        assert_eq!(
            effective_modify_rate_limit_per_min(ExecBackend::Native, 400),
            400
        );
    }

    #[test]
    fn maker_child_finish_releases_remainder_without_taker_commitment() {
        let mut strategy = make_strategy();
        strategy.active_target = Some(active_target(1.0, 0, 7));
        let order_id = strategy.next_order_id();
        strategy
            .children
            .insert(order_id, maker_child_meta(Side::Buy, 1.0, 0.4, 7));

        strategy.finish_child_order(order_id);

        assert!(!strategy.children.contains_key(&order_id));
        // Plain maker remainder (post-only/GTX cancel or exchange cancel)
        // returns to the unallocated ledger for a fresh repost; it is not a
        // taker obligation.
        assert_eq!(strategy.taker_pending_base_qty, 0.0);
    }

    #[test]
    fn expired_or_taker_child_finish_escalates_remainder_to_taker() {
        let mut strategy = make_strategy();
        strategy.active_target = Some(active_target(1.0, 0, 7));
        let maker_id = strategy.next_order_id();
        let mut maker_meta = maker_child_meta(Side::Buy, 1.0, 0.3, 7);
        maker_meta.maker_expired = true;
        strategy.children.insert(maker_id, maker_meta);
        let taker_id = strategy.next_order_id();
        let mut taker_meta = maker_child_meta(Side::Buy, 0.5, 0.0, 7);
        taker_meta.is_taker = true;
        strategy.children.insert(taker_id, taker_meta);

        strategy.finish_child_order(maker_id);
        strategy.finish_child_order(taker_id);

        assert!((strategy.taker_pending_base_qty - (0.7 + 0.5)).abs() < QTY_EPS);
    }

    #[test]
    fn canceled_for_target_and_stale_generation_drops_remainder() {
        let mut strategy = make_strategy();
        strategy.active_target = Some(active_target(1.0, 0, 8));
        let for_target_id = strategy.next_order_id();
        let mut meta = maker_child_meta(Side::Buy, 1.0, 0.0, 8);
        meta.cancel_for_target = true;
        meta.maker_expired = true; // even escalated remainders drop
        strategy.children.insert(for_target_id, meta);
        let stale_id = strategy.next_order_id();
        let mut stale_meta = maker_child_meta(Side::Buy, 1.0, 0.0, 7);
        stale_meta.maker_expired = true;
        strategy.children.insert(stale_id, stale_meta);

        strategy.finish_child_order(for_target_id);
        strategy.finish_child_order(stale_id);

        assert_eq!(strategy.taker_pending_base_qty, 0.0);
    }

    #[test]
    fn fill_delta_advances_virtual_position_once() {
        let mut strategy = make_strategy();
        strategy.active_target = Some(active_target(1.0, 0, 7));
        strategy.virtual_position_qty = Some(0.2);
        let order_id = strategy.next_order_id();

        strategy.apply_fill_delta(order_id, Side::Buy, 0.3);
        strategy.apply_fill_delta(order_id, Side::Buy, 0.3);
        strategy.apply_fill_delta(order_id, Side::Sell, 0.1);

        assert!((strategy.virtual_position_qty.unwrap() - 0.7).abs() < QTY_EPS);
        assert!(strategy.last_position_fill_at_us > 0);
    }

    #[test]
    fn water_level_counts_unfilled_across_children_and_orphans() {
        let mut strategy = make_strategy();
        let id_a = strategy.next_order_id();
        let id_b = strategy.next_order_id();
        let id_c = strategy.next_order_id();
        strategy
            .children
            .insert(id_a, maker_child_meta(Side::Buy, 1.0, 0.25, 7));
        strategy
            .children
            .insert(id_b, maker_child_meta(Side::Sell, 0.5, 0.0, 7));
        strategy
            .orphaned_children
            .insert(id_c, maker_child_meta(Side::Buy, 0.4, 0.4, 7));

        assert!((strategy.open_unfilled_base_qty() - (0.75 + 0.5)).abs() < QTY_EPS);
        assert!((strategy.live_order_signed_qty() - (0.75 - 0.5)).abs() < QTY_EPS);
    }

    #[test]
    fn update_target_cancels_children_and_queues_new_generation() {
        let mut strategy = make_strategy();
        strategy.active_target = Some(active_target(1.0, 0, 7));
        let order_id = strategy.next_order_id();
        strategy
            .children
            .insert(order_id, maker_child_meta(Side::Buy, 1.0, 0.0, 7));

        strategy.update_target(
            BatchExecTarget {
                qty: 2.0,
                signal: 0,
            },
            8,
            b"chase_alpha".to_vec(),
        );

        // The old child is finished via the cancel path (its remainder is
        // dropped as cancel_for_target, not escalated to taker).
        assert!(strategy.children.is_empty());
        assert_eq!(strategy.taker_pending_base_qty, 0.0);
        // Position allocation is not ready in the test harness, so the new
        // generation stays pending.
        let pending = strategy.pending_target.expect("pending target");
        assert_eq!(pending.target.qty, 2.0);
        assert_eq!(pending.generation_time, 8);
    }

    #[test]
    fn update_target_drops_stale_generation_and_bad_signal() {
        let mut strategy = make_strategy();
        strategy.active_target = Some(active_target(1.0, 0, 7));

        strategy.update_target(
            BatchExecTarget {
                qty: 2.0,
                signal: 0,
            },
            7,
            b"chase_alpha".to_vec(),
        );
        assert!(strategy.pending_target.is_none());

        strategy.update_target(
            BatchExecTarget {
                qty: 2.0,
                signal: 3,
            },
            9,
            b"chase_alpha".to_vec(),
        );
        assert!(strategy.pending_target.is_none());
    }

    #[test]
    fn post_only_open_rejection_reposts_without_backoff() {
        let mut strategy = make_strategy();
        strategy.active_target = Some(active_target(1.0, 0, 7));
        let order_id = strategy.next_order_id();
        strategy
            .children
            .insert(order_id, maker_child_meta(Side::Buy, 1.0, 0.0, 7));

        let response = TradeEngineResponseMessage::new(
            400,
            TradeRequestType::BinanceWsNewUMOrder as u32,
            Exchange::Binance as u32,
            order_id,
            -5022,
        );
        strategy.handle_hedge_open_failed(&response, "Post Only rejected", order_id);

        assert!(!strategy.children.contains_key(&order_id));
        assert_eq!(strategy.taker_pending_base_qty, 0.0);
        assert_eq!(strategy.submit_blocked_until_us, 0);
    }

    #[test]
    fn deterministic_open_rejection_applies_backoff() {
        let mut strategy = make_strategy();
        strategy.active_target = Some(active_target(1.0, 0, 7));
        let order_id = strategy.next_order_id();
        strategy
            .children
            .insert(order_id, maker_child_meta(Side::Buy, 1.0, 0.0, 7));

        let response = TradeEngineResponseMessage::new(
            400,
            TradeRequestType::BinanceWsNewUMOrder as u32,
            Exchange::Binance as u32,
            order_id,
            -2019,
        );
        let before = get_timestamp_us();
        strategy.handle_hedge_open_failed(&response, "Margin insufficient", order_id);

        assert!(!strategy.children.contains_key(&order_id));
        assert!(strategy.submit_blocked_until_us >= before + OPEN_REJECT_BACKOFF_US);
    }

    #[test]
    fn acceptance_only_modify_ack_keeps_amend_in_flight() {
        let mut strategy = make_strategy();
        let order_id = strategy.next_order_id();
        let mut meta = maker_child_meta(Side::Buy, 1.0, 0.0, 7);
        meta.amend_in_flight = true;
        meta.pending_anchor_price = Some(50_100.0);
        meta.pending_amend_price = Some(50_100.0);
        strategy.children.insert(order_id, meta);

        let response = TradeEngineResponseMessage::new(
            200,
            TradeRequestType::BinanceWsModifyUMOrder as u32,
            Exchange::Binance as u32,
            order_id,
            0,
        );
        strategy.apply_trade_engine_response(&response);

        let meta = strategy.children.get(&order_id).unwrap();
        assert!(meta.amend_in_flight);
        assert!(!meta.reprice_pending);
    }

    #[test]
    fn matching_order_price_confirms_pending_amend() {
        let mut strategy = make_strategy();
        let order_id = strategy.next_order_id();
        let mut meta = maker_child_meta(Side::Buy, 1.0, 0.0, 7);
        meta.amend_in_flight = true;
        meta.pending_anchor_price = Some(50_100.0);
        meta.pending_amend_price = Some(50_100.0);
        strategy.children.insert(order_id, meta);

        assert!(strategy.confirm_pending_amend(order_id, 50_100.0));
        let meta = strategy.children.get(&order_id).unwrap();
        assert!(!meta.amend_in_flight);
        assert_eq!(meta.anchor_price, 50_100.0);
        assert!(meta.pending_anchor_price.is_none());
        assert!(meta.pending_amend_price.is_none());
    }

    #[test]
    fn modify_failure_marks_reprice_pending() {
        let mut strategy = make_strategy();
        let order_id = strategy.next_order_id();
        let mut meta = maker_child_meta(Side::Buy, 1.0, 0.0, 7);
        meta.amend_in_flight = true;
        strategy.children.insert(order_id, meta);

        let response = TradeEngineResponseMessage::new(
            400,
            TradeRequestType::BinanceWsModifyUMOrder as u32,
            Exchange::Binance as u32,
            order_id,
            -1005,
        );
        strategy.apply_trade_engine_response(&response);

        let meta = strategy.children.get(&order_id).unwrap();
        assert!(!meta.amend_in_flight);
        assert!(meta.reprice_pending);
    }

    #[test]
    fn begin_position_reallocation_clears_taker_pending() {
        let mut strategy = make_strategy();
        strategy.position_allocation_ready = true;
        strategy.taker_pending_base_qty = 0.5;

        strategy.begin_position_reallocation();

        assert!(!strategy.position_allocation_ready());
        assert_eq!(strategy.taker_pending_base_qty, 0.0);
    }

    #[test]
    fn orphan_terminal_accounts_fill_and_routes_remainder() {
        let mut strategy = make_strategy();
        strategy.active_target = Some(active_target(1.0, 0, 7));
        strategy.virtual_position_qty = Some(0.0);
        let order_id = strategy.next_order_id();
        let mut meta = maker_child_meta(Side::Buy, 1.0, 0.3, 7);
        meta.maker_expired = true;
        strategy.orphaned_children.insert(order_id, meta);

        let handled = strategy.apply_exec_orphan_terminal(&ExecOrphanTerminal {
            client_order_id: order_id,
            source_kind: OrphanSourceKind::Hedge,
            terminal_ts: 42,
            side: Side::Buy,
            order_base_qty: 1.0,
            filled_base_qty: 0.5,
            price: 50_000.0,
        });

        assert!(handled);
        assert!(strategy.orphaned_children.is_empty());
        // 0.2 newly accounted fill advances the virtual position.
        assert!((strategy.virtual_position_qty.unwrap() - 0.2).abs() < QTY_EPS);
        // Expired maker remainder (0.5 unfilled) is a taker obligation.
        assert!((strategy.taker_pending_base_qty - 0.5).abs() < QTY_EPS);
    }

    #[test]
    fn internal_cross_fill_moves_virtual_position_and_builds_fill_record() {
        let mut strategy = make_strategy();
        strategy.virtual_position_qty = Some(0.6);
        strategy.position_allocation_ready = true;
        strategy.active_target = Some(active_target(1.0, 0, 7));
        let quote = Quote {
            bid: 99.0,
            bid_qty: 2.0,
            ask: 101.0,
            ask_qty: 3.0,
            ts: 500,
        };
        let record = strategy
            .apply_internal_cross_fill(0.4, &quote, 1_000)
            .unwrap();
        assert_eq!(strategy.virtual_position_qty, Some(1.0));
        assert_eq!(strategy.last_position_fill_at_us, 1_000);
        assert_eq!(record.ttype, UNIFORM_ORDER_TYPE_INTERNAL_CROSS);
        assert_eq!(record.side, Side::Buy.to_u8());
        assert_eq!(record.status, OrderStatus::Filled.to_u8());
        assert_eq!(record.price, 100.0);
        assert_eq!(record.amount_init, 0.4);
        assert_eq!(record.amount_update, 0.4);
        assert_eq!(record.symbol, b"BTCUSDT".to_vec());
        assert_eq!(record.from_key, b"chase_alpha".to_vec());
        assert_eq!(record.mkt_ts, 500);
        assert!(record.signal_bbo.is_some());
        assert_eq!(record.symbol_len as usize, record.symbol.len());
        assert_eq!(record.from_key_len as usize, record.from_key.len());

        let record = strategy
            .apply_internal_cross_fill(-0.25, &quote, 1_001)
            .unwrap();
        assert_eq!(strategy.virtual_position_qty, Some(0.75));
        assert_eq!(record.side, Side::Sell.to_u8());
        assert_eq!(record.amount_update, 0.25);
    }

    #[test]
    fn internal_cross_fill_rejects_invalid_inputs_without_moving_position() {
        let mut strategy = make_strategy();
        let quote = Quote {
            bid: 99.0,
            bid_qty: 2.0,
            ask: 101.0,
            ask_qty: 3.0,
            ts: 500,
        };
        // No applied allocation yet.
        assert!(strategy
            .apply_internal_cross_fill(0.4, &quote, 1_000)
            .is_err());
        strategy.virtual_position_qty = Some(0.0);
        strategy.position_allocation_ready = true;
        // Allocation applied but no target exists to attribute the fill.
        assert!(strategy
            .apply_internal_cross_fill(0.4, &quote, 1_000)
            .is_err());
        strategy.active_target = Some(active_target(1.0, 0, 7));
        assert!(strategy
            .apply_internal_cross_fill(0.0, &quote, 1_000)
            .is_err());
        assert!(strategy
            .apply_internal_cross_fill(f64::NAN, &quote, 1_000)
            .is_err());
        let mut invalid = quote;
        invalid.bid = 0.0;
        assert!(strategy
            .apply_internal_cross_fill(0.4, &invalid, 1_000)
            .is_err());
        assert_eq!(strategy.virtual_position_qty, Some(0.0));
    }

    #[test]
    fn is_strategy_order_requires_child_registration() {
        let mut strategy = make_strategy();
        let order_id = strategy.next_order_id();
        assert!(!strategy.is_strategy_order(order_id));
        strategy
            .children
            .insert(order_id, maker_child_meta(Side::Buy, 1.0, 0.0, 7));
        assert!(strategy.is_strategy_order(order_id));
        assert!(!strategy.is_strategy_order(order_id + 1));
    }

    #[test]
    fn retry_unsent_cancels_finishes_children_whose_cancel_never_sent() {
        let mut strategy = make_strategy();
        strategy.active_target = Some(active_target(1.0, 0, 7));

        let target_cancelled = strategy.next_order_id();
        let mut meta = maker_child_meta(Side::Buy, 1.0, 0.0, 7);
        meta.cancel_for_target = true;
        strategy.children.insert(target_cancelled, meta);

        let expired = strategy.next_order_id();
        let mut meta = maker_child_meta(Side::Buy, 1.0, 0.0, 7);
        meta.maker_expired = true;
        strategy.children.insert(expired, meta);

        // A live child with no cancel intent is not retried.
        let live = strategy.next_order_id();
        strategy
            .children
            .insert(live, maker_child_meta(Side::Buy, 1.0, 0.0, 7));
        // A child whose cancel was already confirmed sent is left alone.
        let acked = strategy.next_order_id();
        let mut meta = maker_child_meta(Side::Buy, 1.0, 0.0, 7);
        meta.cancel_for_target = true;
        meta.cancel_requested = true;
        strategy.children.insert(acked, meta);

        // Without an order manager each stuck child resolves through the
        // missing-order finish path rather than staying live forever.
        strategy.retry_unsent_cancels();

        assert!(!strategy.children.contains_key(&target_cancelled));
        assert!(!strategy.children.contains_key(&expired));
        assert!(strategy.children.contains_key(&live));
        assert!(strategy.children.contains_key(&acked));
        // The expired child's remainder still escalates to taker; the
        // target-cancelled child's remainder drops back to the ledger.
        assert!((strategy.taker_pending_base_qty - 1.0).abs() < QTY_EPS);
    }

    #[test]
    fn taker_pending_clamps_to_uncommitted_gap() {
        assert_eq!(clamp_taker_pending_to_gap(5.0, 1.0), 1.0);
        assert_eq!(clamp_taker_pending_to_gap(0.5, 3.0), 0.5);
        assert_eq!(clamp_taker_pending_to_gap(2.0, -1.0), 0.0);
        assert_eq!(clamp_taker_pending_to_gap(0.0, 5.0), 0.0);
    }

    #[test]
    fn maker_release_uses_batch_equivalent_water_level() {
        assert_eq!(maker_release_usdt(2_500.0, 2, 0.0, 10_000.0), 2_500.0);
        assert_eq!(maker_release_usdt(2_500.0, 2, 2_500.0, 7_500.0), 2_500.0);
        assert_eq!(maker_release_usdt(2_500.0, 2, 5_000.0, 5_000.0), 0.0);
        assert_eq!(maker_release_usdt(2_500.0, 2, 4_500.0, 5_500.0), 500.0);
        assert_eq!(maker_release_usdt(100.0, 2, 0.0, 80.0), 80.0);
    }
}
