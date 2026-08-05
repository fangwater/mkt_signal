use crate::pre_trade::log_throttle::log_order_rate_limit_summary;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::open_order_rate_limiter::{OrderRateBucket, OrderRateLimiter};
use crate::pre_trade::order_manager::PreTradeOrderRequestExt;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::{PersistChannel, TradeEngHub};
use crate::strategy::hedge_order_reconcile::{HedgeOrderReconcileCommon, HedgeOrderReconcileState};
use crate::strategy::hedge_strategy_common::signed_qty_from_side;
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
    OrderExecutionStatus, OrderManager, OrderStatus, OrderType, OrderUpdate, Side,
    TradeEngineResponse, TradeUpdate, TradingVenue,
};
use persist_common::{SignalBbo, SignalBboLeg};
use quote_plan::common::{align_price_ceil, align_price_floor};
use quote_plan::order_align::{align_final_order_qty, min_qty_symbol_key};
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;
use serde::{Deserialize, Serialize};
use signal_common::trade_signal::TradeSignal;
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use trade_signal::MktChannel;

const QTY_EPS: f64 = 1e-12;
const BBO_MAX_AGE_US: i64 = 2_000_000;
const POSITION_RECONCILE_SETTLE_US: i64 = 5_000_000;
const BATCH_EXEC_SIGNAL_KIND: u8 = 0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchExecConfig {
    pub single_order_usdt: f64,
    pub orders_per_batch: u32,
    pub maker_price_anchor: MakerPriceAnchor,
    pub tick_spacing: u32,
    pub batch_interval_ms: u32,
    pub maker_timeout_ms: u32,
    pub max_maker_requotes: u32,
    pub target_tolerance_usdt: f64,
}

impl BatchExecConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.single_order_usdt.is_finite() || self.single_order_usdt <= 0.0 {
            return Err("single_order_usdt must be positive".to_string());
        }
        if self.orders_per_batch == 0 {
            return Err("orders_per_batch must be positive".to_string());
        }
        if self.maker_timeout_ms == 0 {
            return Err("maker_timeout_ms must be positive".to_string());
        }
        if !self.target_tolerance_usdt.is_finite() || self.target_tolerance_usdt < 0.0 {
            return Err("target_tolerance_usdt must be finite and non-negative".to_string());
        }
        Ok(())
    }

    fn max_batch_usdt(&self) -> f64 {
        self.single_order_usdt * f64::from(self.orders_per_batch)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MakerPriceAnchor {
    OwnBest,
    OppositeBestPlusOneTick,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchPhase {
    Live,
    CancellingForRequote,
    CancellingForTarget,
    ReadyToSubmit,
}

#[derive(Debug, Clone)]
struct BatchState {
    target_generation: i64,
    side: Side,
    remaining_base_qty: f64,
    maker_requotes: u32,
    use_taker: bool,
    phase: BatchPhase,
    child_order_ids: BTreeSet<i64>,
    remaining_qty_by_level: BTreeMap<u32, f64>,
    last_quote_ts: i64,
    require_new_quote: bool,
    expires_at_us: i64,
    from_key: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ChildOrderMeta {
    batch_seq: u64,
    level_index: u32,
    order_base_qty: f64,
    accounted_fill_base_qty: f64,
    signal_ts: i64,
    price_offset: f64,
    from_key: Vec<u8>,
    signal_bbo: Option<SignalBbo>,
    cancel_requested: bool,
}

#[derive(Debug, Clone)]
struct ActiveTarget {
    target_qty: f64,
    generation_time: i64,
    from_key: Vec<u8>,
}

#[derive(Debug, Clone)]
struct PendingTarget {
    target_qty: f64,
    generation_time: i64,
    from_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchExecSnapshot {
    pub strategy_name: String,
    pub symbol: String,
    pub exec_venue: TradingVenue,
    /// Physical net position shared by every BatchExec strategy on this symbol.
    pub account_position_qty: f64,
    /// Position allocated to this strategy in the internal ledger.
    pub position_qty: f64,
    pub effective_position_qty: f64,
    pub position_allocated: bool,
    pub target_qty: Option<f64>,
    pub pending_qty: f64,
    pub live_order_qty: f64,
    pub active_batches: usize,
}

#[derive(Debug, Clone, PartialEq)]
struct BatchChildOrderPlan {
    pub level_index: u32,
    pub side: Side,
    pub order_type: OrderType,
    pub price: f64,
    pub qty_venue: f64,
    pub qty_base: f64,
    pub price_offset: f64,
}

fn build_child_order_plans(
    config: &BatchExecConfig,
    side: Side,
    remaining_base_qty: f64,
    maker_requotes: u32,
    bid: f64,
    ask: f64,
    price_tick: f64,
    qty_step: f64,
    min_qty: f64,
    qty_multiplier: f64,
) -> Result<Vec<BatchChildOrderPlan>, String> {
    if !bid.is_finite()
        || !ask.is_finite()
        || bid <= 0.0
        || ask <= bid
        || !price_tick.is_finite()
        || price_tick <= 0.0
        || !qty_multiplier.is_finite()
        || qty_multiplier <= 0.0
    {
        return Err("invalid BBO, price tick, or qty multiplier".to_string());
    }
    let use_taker = maker_requotes > config.max_maker_requotes;
    let child_count = if use_taker {
        1
    } else {
        config.orders_per_batch as usize
    };
    let mut unplanned_base_qty = remaining_base_qty;
    let mut plans = Vec::with_capacity(child_count);
    for level in 0..child_count {
        if unplanned_base_qty <= QTY_EPS {
            break;
        }
        let level_ticks = level as i64 * i64::from(config.tick_spacing);
        let start_price = match (side, config.maker_price_anchor) {
            (Side::Sell, MakerPriceAnchor::OwnBest) => ask,
            (Side::Buy, MakerPriceAnchor::OwnBest) => bid,
            (Side::Sell, MakerPriceAnchor::OppositeBestPlusOneTick) => bid + price_tick,
            (Side::Buy, MakerPriceAnchor::OppositeBestPlusOneTick) => ask - price_tick,
        };
        let limit_price = match side {
            Side::Sell => {
                align_price_ceil(start_price + level_ticks as f64 * price_tick, price_tick)
            }
            Side::Buy => {
                align_price_floor(start_price - level_ticks as f64 * price_tick, price_tick)
            }
        };
        let sizing_price = if use_taker {
            match side {
                Side::Buy => ask,
                Side::Sell => bid,
            }
        } else {
            limit_price
        };
        if !sizing_price.is_finite() || sizing_price <= 0.0 {
            return Err("invalid sizing price".to_string());
        }
        let hand_usdt = if use_taker {
            unplanned_base_qty * sizing_price
        } else {
            config
                .single_order_usdt
                .min(unplanned_base_qty * sizing_price)
        };
        let raw_base_qty = (hand_usdt / sizing_price).min(unplanned_base_qty);
        let raw_venue_qty = raw_base_qty / qty_multiplier;
        let (qty_venue, _) = align_final_order_qty(raw_venue_qty, qty_step, min_qty);
        let qty_base = qty_venue * qty_multiplier;
        if qty_base <= QTY_EPS {
            break;
        }
        plans.push(BatchChildOrderPlan {
            level_index: level as u32,
            side,
            order_type: if use_taker {
                OrderType::Market
            } else {
                OrderType::Limit
            },
            price: if use_taker { 0.0 } else { limit_price },
            qty_venue,
            qty_base,
            price_offset: level_ticks as f64,
        });
        unplanned_base_qty = (unplanned_base_qty - qty_base).max(0.0);
    }
    Ok(plans)
}

#[allow(clippy::too_many_arguments)]
fn rebuild_child_order_plans_at_preserved_levels(
    config: &BatchExecConfig,
    side: Side,
    remaining_qty_by_level: &BTreeMap<u32, f64>,
    bid: f64,
    ask: f64,
    price_tick: f64,
    qty_step: f64,
    min_qty: f64,
    qty_multiplier: f64,
) -> Result<Vec<BatchChildOrderPlan>, String> {
    if !bid.is_finite()
        || !ask.is_finite()
        || bid <= 0.0
        || ask <= bid
        || !price_tick.is_finite()
        || price_tick <= 0.0
        || !qty_multiplier.is_finite()
        || qty_multiplier <= 0.0
    {
        return Err("invalid BBO, price tick, or qty multiplier".to_string());
    }

    let start_price = match (side, config.maker_price_anchor) {
        (Side::Sell, MakerPriceAnchor::OwnBest) => ask,
        (Side::Buy, MakerPriceAnchor::OwnBest) => bid,
        (Side::Sell, MakerPriceAnchor::OppositeBestPlusOneTick) => bid + price_tick,
        (Side::Buy, MakerPriceAnchor::OppositeBestPlusOneTick) => ask - price_tick,
    };
    let mut plans = Vec::with_capacity(remaining_qty_by_level.len());
    for (&level_index, &remaining_base_qty) in remaining_qty_by_level {
        if remaining_base_qty <= QTY_EPS {
            continue;
        }
        let level_ticks = i64::from(level_index) * i64::from(config.tick_spacing);
        let limit_price = match side {
            Side::Sell => {
                align_price_ceil(start_price + level_ticks as f64 * price_tick, price_tick)
            }
            Side::Buy => {
                align_price_floor(start_price - level_ticks as f64 * price_tick, price_tick)
            }
        };
        if !limit_price.is_finite() || limit_price <= 0.0 {
            return Err("invalid preserved-level price".to_string());
        }
        let raw_venue_qty = remaining_base_qty / qty_multiplier;
        let (qty_venue, _) = align_final_order_qty(raw_venue_qty, qty_step, min_qty);
        let qty_base = qty_venue * qty_multiplier;
        if qty_base <= QTY_EPS {
            continue;
        }
        plans.push(BatchChildOrderPlan {
            level_index,
            side,
            order_type: OrderType::Limit,
            price: limit_price,
            qty_venue,
            qty_base,
            price_offset: level_ticks as f64,
        });
    }
    Ok(plans)
}

pub struct BatchExecStrategy {
    strategy_id: i32,
    strategy_name: String,
    symbol: String,
    exec_venue: TradingVenue,
    config: BatchExecConfig,
    virtual_position_qty: Option<f64>,
    position_allocation_ready: bool,
    last_position_fill_at_us: i64,
    active_target: Option<ActiveTarget>,
    pending_target: Option<PendingTarget>,
    batches: BTreeMap<u64, BatchState>,
    child_orders: FastHashMap<i64, ChildOrderMeta>,
    orphaned_child_orders: FastHashMap<i64, ChildOrderMeta>,
    batch_seq: u64,
    order_seq: u32,
    next_batch_at_us: i64,
    reconcile_state: HedgeOrderReconcileState,
    alive_flag: bool,
}

impl BatchExecStrategy {
    pub fn new(
        strategy_id: i32,
        strategy_name: impl Into<String>,
        symbol: impl Into<String>,
        exec_venue: TradingVenue,
        config: BatchExecConfig,
    ) -> Self {
        Self {
            strategy_id,
            strategy_name: strategy_name.into(),
            symbol: normalize_symbol_for_internal(&symbol.into()),
            exec_venue,
            config,
            virtual_position_qty: None,
            position_allocation_ready: false,
            last_position_fill_at_us: 0,
            active_target: None,
            pending_target: None,
            batches: BTreeMap::new(),
            child_orders: fast_hash_map(),
            orphaned_child_orders: fast_hash_map(),
            batch_seq: 0,
            order_seq: 0,
            next_batch_at_us: 0,
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

    pub fn exec_symbol(&self) -> &str {
        &self.symbol
    }

    pub fn target_qty(&self) -> Option<f64> {
        self.pending_target
            .as_ref()
            .map(|target| target.target_qty)
            .or_else(|| self.active_target.as_ref().map(|target| target.target_qty))
    }

    pub fn virtual_position_qty(&self) -> Option<f64> {
        self.virtual_position_qty
    }

    pub fn position_allocation_ready(&self) -> bool {
        self.position_allocation_ready && self.virtual_position_qty.is_some()
    }

    pub fn has_execution_in_flight(&self) -> bool {
        !self.child_orders.is_empty()
            || !self.orphaned_child_orders.is_empty()
            || !self.batches.is_empty()
    }

    pub fn pause_position_allocation(&mut self) {
        self.position_allocation_ready = false;
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
        now_ts: i64,
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
        self.next_batch_at_us = now_ts;
        info!(
            "BatchExecStrategy: strategy_id={} strategy_name={} symbol={} position allocation applied previous={:?} current={:.8}",
            self.strategy_id,
            self.strategy_name,
            self.symbol,
            previous,
            position_qty
        );
        self.process_pending_target(now_ts);
        Ok(())
    }

    pub fn snapshot(&self, _now_ts: i64) -> BatchExecSnapshot {
        let account_position_qty =
            MonitorChannel::instance().get_position_qty(&self.symbol, self.exec_venue);
        let position_qty = self.virtual_position_qty.unwrap_or(0.0);
        let effective_position_qty = position_qty;
        let target_qty = self.target_qty();
        let live_order_qty = self.live_order_signed_qty();
        let pending_qty = target_qty
            .map(|target| target - effective_position_qty - live_order_qty)
            .unwrap_or(0.0);
        BatchExecSnapshot {
            strategy_name: self.strategy_name.clone(),
            symbol: self.symbol.clone(),
            exec_venue: self.exec_venue,
            account_position_qty,
            position_qty,
            effective_position_qty,
            position_allocated: self.position_allocation_ready(),
            target_qty,
            pending_qty,
            live_order_qty,
            active_batches: self.batches.len(),
        }
    }

    fn next_batch_seq(&mut self) -> u64 {
        self.batch_seq = self.batch_seq.wrapping_add(1).max(1);
        self.batch_seq
    }

    fn next_order_id(&mut self) -> i64 {
        self.order_seq = self.order_seq.wrapping_add(1).max(1);
        ((self.strategy_id as i64) << 32) | self.order_seq as i64
    }

    fn extract_strategy_id(order_id: i64) -> i32 {
        (order_id >> 32) as i32
    }

    fn live_order_signed_qty(&self) -> f64 {
        self.child_orders
            .values()
            .chain(self.orphaned_child_orders.values())
            .map(|meta| {
                let remaining = (meta.order_base_qty - meta.accounted_fill_base_qty).max(0.0);
                self.batches
                    .get(&meta.batch_seq)
                    .map(|batch| signed_qty_from_side(batch.side, remaining))
                    .unwrap_or(0.0)
            })
            .sum()
    }

    fn committed_batch_signed_qty(&self) -> f64 {
        self.batches
            .values()
            .map(|batch| signed_qty_from_side(batch.side, batch.remaining_base_qty))
            .sum()
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

    pub fn update_config(&mut self, config: BatchExecConfig) -> Result<(), String> {
        config.validate()?;
        if self.config == config {
            return Ok(());
        }
        self.config = config;
        if self.active_target.is_some() && self.batches.is_empty() {
            self.next_batch_at_us = get_timestamp_us();
        }
        Ok(())
    }

    pub fn update_target(&mut self, target_qty: f64, generation_time: i64, from_key: Vec<u8>) {
        if !target_qty.is_finite() {
            warn!(
                "BatchExecStrategy: strategy_id={} invalid target_qty={}",
                self.strategy_id, target_qty
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
                "BatchExecStrategy: strategy_id={} drop stale target generation={} latest={}",
                self.strategy_id,
                generation_time,
                self.latest_generation_time()
            );
            return;
        }

        self.pending_target = Some(PendingTarget {
            target_qty,
            generation_time,
            from_key,
        });
        let batch_ids: Vec<u64> = self.batches.keys().copied().collect();
        for batch_seq in batch_ids {
            self.begin_cancel_batch(batch_seq, BatchPhase::CancellingForTarget);
        }
        self.process_pending_target(get_timestamp_us());
    }

    fn process_pending_target(&mut self, now_ts: i64) {
        if self.pending_target.is_none() {
            return;
        }
        if !self.child_orders.is_empty() || !self.orphaned_child_orders.is_empty() {
            return;
        }
        if !MonitorChannel::instance().exec_position_snapshot_ready()
            || !self.position_allocation_ready()
        {
            return;
        }
        self.batches.clear();
        let pending = self.pending_target.take().expect("checked above");
        let position_qty = self.virtual_position_qty.expect("allocation checked above");
        info!(
            "BatchExecStrategy: strategy_id={} strategy_name={} symbol={} target activated target_qty={:.8} allocated_position_qty={:.8} generation={}",
            self.strategy_id,
            self.strategy_name,
            self.symbol,
            pending.target_qty,
            position_qty,
            pending.generation_time
        );
        self.active_target = Some(ActiveTarget {
            target_qty: pending.target_qty,
            generation_time: pending.generation_time,
            from_key: pending.from_key,
        });
        self.next_batch_at_us = now_ts;
    }

    fn maybe_start_or_requote_batch(&mut self, now_ts: i64) {
        if self.pending_target.is_some()
            || !self.orphaned_child_orders.is_empty()
            || !MonitorChannel::instance().exec_position_snapshot_ready()
            || !self.position_allocation_ready()
        {
            return;
        }

        let ready_batch = self.batches.iter().find_map(|(batch_seq, batch)| {
            (batch.phase == BatchPhase::ReadyToSubmit).then_some(*batch_seq)
        });
        if let Some(batch_seq) = ready_batch {
            self.submit_batch(batch_seq, now_ts);
            return;
        }

        if now_ts < self.next_batch_at_us {
            return;
        }
        let Some((target_qty, target_generation, target_from_key)) =
            self.active_target.as_ref().map(|target| {
                (
                    target.target_qty,
                    target.generation_time,
                    target.from_key.clone(),
                )
            })
        else {
            return;
        };
        let Some(quote) = MktChannel::instance().get_quote(&self.symbol, self.exec_venue) else {
            return;
        };
        if !Self::quote_is_fresh(quote.ts, now_ts) {
            return;
        }
        let position_qty = self.virtual_position_qty.expect("allocation checked above");
        let unallocated_qty = target_qty - position_qty - self.committed_batch_signed_qty();
        let side = if unallocated_qty > 0.0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let reference_price = match side {
            Side::Buy => quote.bid,
            Side::Sell => quote.ask,
        };
        if unallocated_qty.abs() * reference_price <= self.config.target_tolerance_usdt {
            return;
        }
        let batch_base_qty = unallocated_qty
            .abs()
            .min(self.config.max_batch_usdt() / reference_price);
        if batch_base_qty <= QTY_EPS {
            return;
        }
        let batch_seq = self.next_batch_seq();
        self.batches.insert(
            batch_seq,
            BatchState {
                target_generation,
                side,
                remaining_base_qty: batch_base_qty,
                maker_requotes: 0,
                use_taker: false,
                phase: BatchPhase::ReadyToSubmit,
                child_order_ids: BTreeSet::new(),
                remaining_qty_by_level: BTreeMap::new(),
                last_quote_ts: 0,
                require_new_quote: false,
                expires_at_us: 0,
                from_key: target_from_key,
            },
        );
        self.submit_batch(batch_seq, now_ts);
    }

    fn quote_is_fresh(quote_ts: i64, now_ts: i64) -> bool {
        if quote_ts <= 0 {
            return false;
        }
        let quote_ts_us = if quote_ts < 10_000_000_000_000 {
            quote_ts.saturating_mul(1_000)
        } else {
            quote_ts
        };
        now_ts.saturating_sub(quote_ts_us) <= BBO_MAX_AGE_US
    }

    fn build_child_orders(
        &self,
        batch: &BatchState,
        bid: f64,
        ask: f64,
    ) -> Result<(Vec<BatchChildOrderPlan>, f64), String> {
        let table = MonitorChannel::instance()
            .try_venue_min_qty_table(self.exec_venue)
            .ok_or_else(|| format!("missing min qty table venue={:?}", self.exec_venue))?;
        let symbol_key = min_qty_symbol_key(self.exec_venue, &self.symbol);
        let price_tick = table
            .price_tick(&symbol_key)
            .ok_or_else(|| format!("missing price tick symbol={}", self.symbol))?;
        let qty_step = table.step_size(&symbol_key).unwrap_or(0.0);
        let min_qty = table.min_qty(&symbol_key).unwrap_or(0.0);
        let qty_multiplier = MonitorChannel::instance()
            .qty_multiplier_for_venue(self.exec_venue, &self.symbol)
            .unwrap_or(1.0);
        if !qty_multiplier.is_finite() || qty_multiplier <= 0.0 {
            return Err(format!("invalid qty multiplier symbol={}", self.symbol));
        }

        let use_taker = batch.maker_requotes > self.config.max_maker_requotes;
        let plans = if !use_taker && !batch.remaining_qty_by_level.is_empty() {
            rebuild_child_order_plans_at_preserved_levels(
                &self.config,
                batch.side,
                &batch.remaining_qty_by_level,
                bid,
                ask,
                price_tick,
                qty_step,
                min_qty,
                qty_multiplier,
            )?
        } else {
            build_child_order_plans(
                &self.config,
                batch.side,
                batch.remaining_base_qty,
                batch.maker_requotes,
                bid,
                ask,
                price_tick,
                qty_step,
                min_qty,
                qty_multiplier,
            )?
        };
        Ok((plans, qty_multiplier))
    }

    fn submit_batch(&mut self, batch_seq: u64, now_ts: i64) {
        let Some(batch) = self.batches.get(&batch_seq).cloned() else {
            return;
        };
        if batch.remaining_base_qty <= QTY_EPS {
            self.batches.remove(&batch_seq);
            return;
        }
        let Some(quote) = MktChannel::instance().get_quote(&self.symbol, self.exec_venue) else {
            return;
        };
        if !Self::quote_is_fresh(quote.ts, now_ts) {
            return;
        }
        if batch.require_new_quote && quote.ts <= batch.last_quote_ts {
            return;
        }
        if self
            .active_target
            .as_ref()
            .is_none_or(|target| target.generation_time != batch.target_generation)
        {
            self.batches.remove(&batch_seq);
            return;
        }
        let use_taker = batch.maker_requotes > self.config.max_maker_requotes;
        let (plans, qty_multiplier) = match self.build_child_orders(&batch, quote.bid, quote.ask) {
            Ok(value) => value,
            Err(err) => {
                warn!(
                    "BatchExecStrategy: strategy_id={} build batch={} failed: {}",
                    self.strategy_id, batch_seq, err
                );
                return;
            }
        };
        if plans.is_empty() {
            self.batches.remove(&batch_seq);
            self.next_batch_at_us = i64::MAX;
            return;
        }
        let planned_base_qty: f64 = plans.iter().map(|plan| plan.qty_base).sum();
        if let Some(batch) = self.batches.get_mut(&batch_seq) {
            batch.remaining_base_qty = planned_base_qty;
            batch.remaining_qty_by_level = plans
                .iter()
                .map(|plan| (plan.level_index, plan.qty_base))
                .collect();
            batch.use_taker = use_taker;
            batch.last_quote_ts = quote.ts;
            batch.require_new_quote = false;
            batch.expires_at_us = if use_taker {
                0
            } else {
                now_ts.saturating_add(i64::from(self.config.maker_timeout_ms) * 1_000)
            };
        }
        self.next_batch_at_us =
            now_ts.saturating_add(i64::from(self.config.batch_interval_ms).saturating_mul(1_000));
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
        self.send_child_orders(
            batch_seq,
            quote.bid,
            quote.ask,
            quote.ts,
            signal_bbo,
            plans,
            qty_multiplier,
        );
    }

    fn send_child_orders(
        &mut self,
        batch_seq: u64,
        bid: f64,
        ask: f64,
        quote_ts: i64,
        signal_bbo: Option<SignalBbo>,
        plans: Vec<BatchChildOrderPlan>,
        qty_multiplier: f64,
    ) {
        let now_ts = get_timestamp_us();
        MonitorChannel::instance().refresh_exec_risk_state();
        let mut sent_ids = Vec::new();
        for plan in plans {
            let signed_base_qty = signed_qty_from_side(plan.side, plan.qty_base);
            let current_qty =
                MonitorChannel::instance().get_position_qty(&self.symbol, self.exec_venue);
            let price_hint = if plan.price > 0.0 {
                plan.price
            } else {
                match plan.side {
                    Side::Buy => ask,
                    Side::Sell => bid,
                }
            };
            if plan.order_type.is_limit()
                && MonitorChannel::instance()
                    .check_pending_limit_order_for_exec(&self.symbol, plan.side)
                    .is_err()
            {
                break;
            }
            if (current_qty + signed_base_qty).abs() > current_qty.abs() + QTY_EPS
                && MonitorChannel::instance().check_leverage().is_err()
            {
                break;
            }
            if MonitorChannel::instance()
                .check_exec_position_imbalance_risk(&self.symbol, self.exec_venue, signed_base_qty)
                .is_err()
            {
                break;
            }
            if MonitorChannel::instance()
                .ensure_max_pos_u_for_base_delta(
                    &self.symbol,
                    self.exec_venue,
                    current_qty,
                    signed_base_qty,
                    price_hint,
                    plan.qty_venue,
                    qty_multiplier,
                )
                .is_err()
            {
                break;
            }

            let client_order_id = self.next_order_id();
            let from_key = self
                .batches
                .get(&batch_seq)
                .map(|batch| batch.from_key.clone())
                .unwrap_or_default();
            let price_offset = plan.price_offset;
            MonitorChannel::instance()
                .order_manager()
                .borrow_mut()
                .create_order_with_pending_limit_flag(
                    self.exec_venue,
                    client_order_id,
                    plan.order_type,
                    self.symbol.clone(),
                    plan.side,
                    plan.qty_venue,
                    plan.price,
                    false,
                    qty_multiplier,
                    plan.order_type.is_limit(),
                );
            let _ = MonitorChannel::instance()
                .order_manager()
                .borrow_mut()
                .update(client_order_id, |order| {
                    order.set_signal_meta(now_ts, BATCH_EXEC_SIGNAL_KIND);
                    if quote_ts > 0 {
                        order.set_mkt_time(quote_ts);
                    }
                });
            self.child_orders.insert(
                client_order_id,
                ChildOrderMeta {
                    batch_seq,
                    level_index: plan.level_index,
                    order_base_qty: plan.qty_base,
                    accounted_fill_base_qty: 0.0,
                    signal_ts: now_ts,
                    signal_bbo,
                    price_offset,
                    from_key,
                    cancel_requested: false,
                },
            );
            if let Err(err) = self.send_order(client_order_id) {
                self.child_orders.remove(&client_order_id);
                let _ = MonitorChannel::instance()
                    .order_manager()
                    .borrow_mut()
                    .remove(client_order_id);
                warn!(
                    "BatchExecStrategy: strategy_id={} send child failed order_id={} err={}",
                    self.strategy_id, client_order_id, err
                );
                continue;
            }
            self.schedule_order_query_watchdog(
                client_order_id,
                PendingOrderQueryReason::OrderWatchdog,
            );
            sent_ids.push(client_order_id);
        }

        let Some(batch) = self.batches.get_mut(&batch_seq) else {
            return;
        };
        batch.child_order_ids.extend(sent_ids);
        if batch.child_order_ids.is_empty() {
            batch.phase = BatchPhase::ReadyToSubmit;
            return;
        }
        batch.phase = BatchPhase::Live;
        if !batch.use_taker && batch.expires_at_us <= now_ts {
            batch.expires_at_us = now_ts;
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
                "BatchExecStrategy",
                Some(self.strategy_id),
                OrderRateBucket::Exec,
                &self.symbol,
                &err,
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
        Ok(())
    }

    fn begin_cancel_batch(&mut self, batch_seq: u64, phase: BatchPhase) {
        let order_ids = match self.batches.get_mut(&batch_seq) {
            Some(batch) => {
                batch.phase = phase;
                batch.child_order_ids.iter().copied().collect::<Vec<_>>()
            }
            None => return,
        };
        for client_order_id in order_ids {
            self.request_cancel(client_order_id);
        }
    }

    fn request_cancel(&mut self, client_order_id: i64) {
        if self
            .child_orders
            .get(&client_order_id)
            .is_some_and(|meta| meta.cancel_requested)
        {
            return;
        }
        let Some(order) = MonitorChannel::instance()
            .order_manager()
            .borrow()
            .get(client_order_id)
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
                    if let Some(meta) = self.child_orders.get_mut(&client_order_id) {
                        meta.cancel_requested = true;
                    }
                    self.schedule_order_query_watchdog(
                        client_order_id,
                        PendingOrderQueryReason::CancelWatchdog,
                    );
                }
            }
            Err(err) => warn!(
                "BatchExecStrategy: strategy_id={} build cancel failed order_id={} err={}",
                self.strategy_id, client_order_id, err
            ),
        }
    }

    fn handle_batch_timeouts(&mut self, now_ts: i64) {
        let expired: Vec<u64> = self
            .batches
            .iter()
            .filter_map(|(seq, batch)| {
                (batch.phase == BatchPhase::Live
                    && !batch.use_taker
                    && batch.expires_at_us > 0
                    && now_ts >= batch.expires_at_us)
                    .then_some(*seq)
            })
            .collect();
        for batch_seq in expired {
            self.begin_cancel_batch(batch_seq, BatchPhase::CancellingForRequote);
        }
    }

    fn cancel_batches_when_target_no_longer_needs_them(&mut self) {
        if self.pending_target.is_some()
            || self.batches.is_empty()
            || !self.position_allocation_ready()
        {
            return;
        }
        let Some(target_qty) = self.active_target.as_ref().map(|target| target.target_qty) else {
            return;
        };
        let position_qty = self.virtual_position_qty.expect("allocation checked above");
        let remaining_qty = target_qty - position_qty;
        let committed_qty = self.committed_batch_signed_qty();

        let direction_changed = committed_qty.abs() > QTY_EPS
            && (remaining_qty.abs() <= QTY_EPS || remaining_qty.signum() != committed_qty.signum());
        let committed_too_much = committed_qty.abs() > remaining_qty.abs() + QTY_EPS;
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

        if !direction_changed && !committed_too_much && !within_tolerance {
            return;
        }
        let batch_ids: Vec<u64> = self.batches.keys().copied().collect();
        for batch_seq in batch_ids {
            self.begin_cancel_batch(batch_seq, BatchPhase::CancellingForTarget);
        }
    }

    fn account_fill_progress(&mut self, client_order_id: i64, cumulative_venue_qty: f64) {
        let cumulative_base_qty = MonitorChannel::instance().qty_to_base(
            self.exec_venue,
            &self.symbol,
            cumulative_venue_qty,
        );
        if !cumulative_base_qty.is_finite() || cumulative_base_qty < 0.0 {
            warn!(
                "BatchExecStrategy: strategy_id={} invalid cumulative fill order_id={} cumulative_base_qty={}",
                self.strategy_id, client_order_id, cumulative_base_qty
            );
            return;
        }
        let Some((batch_seq, level_index, delta_base_qty)) =
            self.child_orders.get_mut(&client_order_id).map(|meta| {
                let next_accounted = cumulative_base_qty
                    .max(meta.accounted_fill_base_qty)
                    .min(meta.order_base_qty);
                let delta_base_qty = next_accounted - meta.accounted_fill_base_qty;
                meta.accounted_fill_base_qty = next_accounted;
                (meta.batch_seq, meta.level_index, delta_base_qty)
            })
        else {
            return;
        };
        self.apply_fill_delta(client_order_id, batch_seq, level_index, delta_base_qty);
    }

    fn apply_fill_delta(
        &mut self,
        client_order_id: i64,
        batch_seq: u64,
        level_index: u32,
        delta_base_qty: f64,
    ) {
        if delta_base_qty <= QTY_EPS {
            return;
        }
        let Some(batch) = self.batches.get_mut(&batch_seq) else {
            return;
        };
        batch.remaining_base_qty = (batch.remaining_base_qty - delta_base_qty).max(0.0);
        if let Some(level_remaining) = batch.remaining_qty_by_level.get_mut(&level_index) {
            *level_remaining = (*level_remaining - delta_base_qty).max(0.0);
            if *level_remaining <= QTY_EPS {
                batch.remaining_qty_by_level.remove(&level_index);
            }
        }
        let signed_fill_qty = signed_qty_from_side(batch.side, delta_base_qty);
        if let Some(position_qty) = self.virtual_position_qty.as_mut() {
            *position_qty += signed_fill_qty;
            self.last_position_fill_at_us = get_timestamp_us();
        } else {
            warn!(
                "BatchExecStrategy: strategy_id={} strategy_name={} symbol={} fill arrived before position allocation order_id={} signed_fill_qty={:.8}",
                self.strategy_id,
                self.strategy_name,
                self.symbol,
                client_order_id,
                signed_fill_qty
            );
        }
    }

    fn finish_child_order(&mut self, client_order_id: i64) {
        self.clear_order_query_state(client_order_id);
        let Some(meta) = self.child_orders.remove(&client_order_id) else {
            return;
        };
        self.finish_child_order_meta(client_order_id, meta);
    }

    fn finish_child_order_meta(&mut self, client_order_id: i64, meta: ChildOrderMeta) {
        let batch_seq = meta.batch_seq;
        let has_orphaned_sibling = self
            .orphaned_child_orders
            .values()
            .any(|orphaned| orphaned.batch_seq == batch_seq);
        let mut remove_batch = false;
        let mut cancel_siblings_for_requote = false;
        if let Some(batch) = self.batches.get_mut(&batch_seq) {
            batch.child_order_ids.remove(&client_order_id);
            let unfilled_qty = (meta.order_base_qty - meta.accounted_fill_base_qty).max(0.0);
            if batch.phase == BatchPhase::Live && unfilled_qty > QTY_EPS {
                batch.phase = BatchPhase::CancellingForRequote;
                batch.require_new_quote = true;
                cancel_siblings_for_requote = !batch.child_order_ids.is_empty();
            }
            if batch.child_order_ids.is_empty() && !has_orphaned_sibling {
                if batch.phase == BatchPhase::CancellingForTarget
                    || batch.remaining_base_qty <= QTY_EPS
                {
                    remove_batch = true;
                } else {
                    if batch.phase == BatchPhase::CancellingForRequote && !batch.use_taker {
                        batch.maker_requotes = batch.maker_requotes.saturating_add(1);
                    }
                    batch.phase = BatchPhase::ReadyToSubmit;
                    batch.expires_at_us = 0;
                }
            }
        }
        if remove_batch {
            self.batches.remove(&batch_seq);
        } else if cancel_siblings_for_requote {
            self.begin_cancel_batch(batch_seq, BatchPhase::CancellingForRequote);
        }
    }

    fn uniform_ctx(&self, client_order_id: i64) -> UniformPublishCtx {
        self.child_orders
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
            update.status(),
            update.order_id(),
            update.cumulative_filled_quantity(),
            "BatchExecStrategy",
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
        let changed = manager.apply_remote_update(client_order_id, |order| {
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
        self.account_fill_progress(client_order_id, effective_fill);
        if let Some((order, ctx)) = snapshot.as_ref() {
            if status == OrderStatus::New {
                publish_uniform_new_order(
                    update,
                    order,
                    previous_fill,
                    ctx,
                    "BatchExecStrategy",
                    self.strategy_id,
                );
            } else if matches!(status, OrderStatus::PartiallyFilled | OrderStatus::Filled) {
                publish_uniform_trade_order_from_order_update(
                    update,
                    order,
                    previous_fill,
                    ctx,
                    "BatchExecStrategy",
                    self.strategy_id,
                );
            } else if status.is_finished() {
                publish_uniform_terminal_order(
                    update,
                    order,
                    previous_fill,
                    ctx,
                    "BatchExecStrategy",
                    self.strategy_id,
                );
            }
        }
        if status.is_finished() {
            self.finish_child_order(client_order_id);
        } else {
            self.clear_order_query_state(client_order_id);
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
            "BatchExecStrategy",
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
        let snapshot = manager
            .get(client_order_id)
            .map(|order| (order, self.uniform_ctx(client_order_id)));
        drop(manager);
        if !changed {
            return false;
        }
        self.account_fill_progress(client_order_id, cumulative_fill);
        if let Some((order, ctx)) = snapshot.as_ref() {
            publish_uniform_trade_order(
                trade,
                order,
                previous_fill,
                status,
                ctx,
                "BatchExecStrategy",
                self.strategy_id,
            );
        }
        if status == OrderStatus::Filled {
            self.finish_child_order(client_order_id);
        } else {
            self.clear_order_query_state(client_order_id);
        }
        true
    }
}

impl HedgeOrderReconcileCommon for BatchExecStrategy {
    fn hedge_reconcile_strategy_name(&self) -> &'static str {
        "BatchExec"
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
        self.child_orders.contains_key(&client_order_id)
    }

    fn handoff_hedge_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        reason: &str,
    ) -> bool {
        if !self.child_orders.contains_key(&client_order_id) {
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
            .child_orders
            .remove(&client_order_id)
            .expect("child order checked before synchronous orphan adoption");
        if let Some(batch) = self.batches.get_mut(&meta.batch_seq) {
            batch.child_order_ids.remove(&client_order_id);
        }
        self.orphaned_child_orders.insert(client_order_id, meta);
        true
    }

    fn handle_hedge_open_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        warn!(
            "BatchExecStrategy: strategy_id={} child open failed order_id={} code={}({})",
            self.strategy_id,
            client_order_id,
            response.error_code(),
            code_desc
        );
        self.finish_child_order(client_order_id);
        if let Some(manager) = MonitorChannel::try_order_manager() {
            let _ = manager.borrow_mut().remove(client_order_id);
        }
    }
}

impl Strategy for BatchExecStrategy {
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
                "BatchExecStrategy: strategy_id={} reject orphan terminal order_id={} source_kind={:?}",
                self.strategy_id, terminal.client_order_id, terminal.source_kind
            );
            return false;
        }
        let Some(existing_meta) = self
            .orphaned_child_orders
            .get(&terminal.client_order_id)
            .cloned()
        else {
            warn!(
                "BatchExecStrategy: strategy_id={} missing orphan metadata order_id={}",
                self.strategy_id, terminal.client_order_id
            );
            return false;
        };
        let Some(batch) = self.batches.get(&existing_meta.batch_seq) else {
            warn!(
                "BatchExecStrategy: strategy_id={} missing orphan batch order_id={} batch_seq={}",
                self.strategy_id, terminal.client_order_id, existing_meta.batch_seq
            );
            return false;
        };
        if batch.side != terminal.side {
            warn!(
                "BatchExecStrategy: strategy_id={} reject orphan terminal side mismatch order_id={} expected={:?} actual={:?}",
                self.strategy_id, terminal.client_order_id, batch.side, terminal.side
            );
            return false;
        }
        if !terminal.filled_base_qty.is_finite() || terminal.filled_base_qty < 0.0 {
            warn!(
                "BatchExecStrategy: strategy_id={} reject invalid orphan terminal fill order_id={} filled_base_qty={}",
                self.strategy_id, terminal.client_order_id, terminal.filled_base_qty
            );
            return false;
        }

        let mut meta = self
            .orphaned_child_orders
            .remove(&terminal.client_order_id)
            .expect("orphan metadata checked above");
        let previous_accounted = meta.accounted_fill_base_qty;
        let next_accounted = terminal
            .filled_base_qty
            .max(previous_accounted)
            .min(meta.order_base_qty);
        meta.accounted_fill_base_qty = next_accounted;
        self.apply_fill_delta(
            terminal.client_order_id,
            meta.batch_seq,
            meta.level_index,
            next_accounted - previous_accounted,
        );
        let batch_seq = meta.batch_seq;
        self.finish_child_order_meta(terminal.client_order_id, meta);
        info!(
            "BatchExecStrategy: strategy_id={} applied orphan terminal order_id={} batch_seq={} terminal_ts={} side={:?} source_order_base_qty={:.8} tracked_order_base_qty={:.8} cumulative_filled_base_qty={:.8} newly_accounted_base_qty={:.8} price={:.8}",
            self.strategy_id,
            terminal.client_order_id,
            batch_seq,
            terminal.terminal_ts,
            terminal.side,
            terminal.order_base_qty,
            existing_meta.order_base_qty,
            terminal.filled_base_qty,
            next_accounted - previous_accounted,
            terminal.price
        );
        true
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        Self::extract_strategy_id(order_id) == self.strategy_id
            && self.child_orders.contains_key(&order_id)
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        debug!(
            "BatchExecStrategy: strategy_id={} ignore signal {:?}; targets come from Redis",
            self.strategy_id, signal.signal_type
        );
    }

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        if self.apply_order_update_inner(update) {
            PersistChannel::with(|channel| channel.publish_order_update(update));
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        if self.apply_trade_update_inner(trade) {
            PersistChannel::with(|channel| channel.publish_trade_update(trade));
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
        self.handle_order_query_watchdogs();
        self.cancel_batches_when_target_no_longer_needs_them();
        self.handle_batch_timeouts(now_ts);
        self.process_pending_target(now_ts);
        self.maybe_start_or_requote_batch(now_ts);
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

    fn config() -> BatchExecConfig {
        BatchExecConfig {
            single_order_usdt: 100.0,
            orders_per_batch: 3,
            maker_price_anchor: MakerPriceAnchor::OwnBest,
            tick_spacing: 2,
            batch_interval_ms: 500,
            maker_timeout_ms: 1_000,
            max_maker_requotes: 2,
            target_tolerance_usdt: 10.0,
        }
    }

    fn strategy_with_orphan_batch(entries: &[(i64, u32, f64, f64)]) -> BatchExecStrategy {
        let mut strategy = BatchExecStrategy::new(
            1,
            "cta_alpha",
            "BTCUSDT",
            TradingVenue::BinanceFutures,
            config(),
        );
        let remaining_base_qty: f64 = entries
            .iter()
            .map(|(_, _, order_qty, accounted)| order_qty - accounted)
            .sum();
        let accounted_base_qty: f64 = entries.iter().map(|(_, _, _, accounted)| accounted).sum();
        let remaining_qty_by_level = entries
            .iter()
            .map(|(_, level, order_qty, accounted)| (*level, order_qty - accounted))
            .collect();
        strategy.virtual_position_qty = Some(accounted_base_qty);
        strategy.position_allocation_ready = true;
        strategy.batches.insert(
            1,
            BatchState {
                target_generation: 1,
                side: Side::Buy,
                remaining_base_qty,
                maker_requotes: 0,
                use_taker: false,
                phase: BatchPhase::Live,
                child_order_ids: BTreeSet::new(),
                remaining_qty_by_level,
                last_quote_ts: 10,
                require_new_quote: false,
                expires_at_us: 100,
                from_key: b"cta_alpha".to_vec(),
            },
        );
        for (client_order_id, level_index, order_base_qty, accounted_fill_base_qty) in entries {
            strategy.orphaned_child_orders.insert(
                *client_order_id,
                ChildOrderMeta {
                    batch_seq: 1,
                    level_index: *level_index,
                    order_base_qty: *order_base_qty,
                    accounted_fill_base_qty: *accounted_fill_base_qty,
                    signal_ts: 1,
                    signal_bbo: None,
                    price_offset: f64::from(*level_index),
                    from_key: b"cta_alpha".to_vec(),
                    cancel_requested: false,
                },
            );
        }
        strategy
    }

    fn orphan_terminal(client_order_id: i64, filled_base_qty: f64) -> ExecOrphanTerminal {
        ExecOrphanTerminal {
            client_order_id,
            source_kind: OrphanSourceKind::Hedge,
            terminal_ts: 1_000,
            side: Side::Buy,
            order_base_qty: 1.0,
            filled_base_qty,
            price: 100.0,
        }
    }

    #[test]
    fn orphan_partial_fill_accounts_only_delta_and_requotes() {
        let mut strategy = strategy_with_orphan_batch(&[(101, 0, 1.0, 0.25)]);

        assert!(strategy.apply_exec_orphan_terminal(&orphan_terminal(101, 0.6)));

        assert!((strategy.virtual_position_qty().unwrap() - 0.6).abs() < QTY_EPS);
        assert!(strategy.orphaned_child_orders.is_empty());
        let batch = strategy.batches.get(&1).unwrap();
        assert!((batch.remaining_base_qty - 0.4).abs() < QTY_EPS);
        assert_eq!(batch.phase, BatchPhase::ReadyToSubmit);
        assert_eq!(batch.maker_requotes, 1);
        assert!(batch.require_new_quote);

        assert!(!strategy.apply_exec_orphan_terminal(&orphan_terminal(101, 0.6)));
        assert!((strategy.virtual_position_qty().unwrap() - 0.6).abs() < QTY_EPS);
    }

    #[test]
    fn zero_fill_orphan_terminal_releases_batch_for_requote() {
        let mut strategy = strategy_with_orphan_batch(&[(102, 0, 1.0, 0.0)]);

        assert!(strategy.apply_exec_orphan_terminal(&orphan_terminal(102, 0.0)));

        let batch = strategy.batches.get(&1).unwrap();
        assert!((batch.remaining_base_qty - 1.0).abs() < QTY_EPS);
        assert_eq!(batch.phase, BatchPhase::ReadyToSubmit);
        assert_eq!(batch.maker_requotes, 1);
    }

    #[test]
    fn fully_filled_orphan_terminal_finishes_batch() {
        let mut strategy = strategy_with_orphan_batch(&[(106, 0, 1.0, 0.0)]);

        assert!(strategy.apply_exec_orphan_terminal(&orphan_terminal(106, 1.0)));

        assert!((strategy.virtual_position_qty().unwrap() - 1.0).abs() < QTY_EPS);
        assert!(strategy.batches.is_empty());
        assert!(!strategy.has_execution_in_flight());
    }

    #[test]
    fn batch_waits_for_every_orphan_terminal() {
        let mut strategy = strategy_with_orphan_batch(&[(103, 0, 0.5, 0.0), (104, 1, 0.5, 0.0)]);

        assert!(strategy.apply_exec_orphan_terminal(&orphan_terminal(103, 0.0)));
        assert_eq!(
            strategy.batches.get(&1).unwrap().phase,
            BatchPhase::CancellingForRequote
        );
        assert_eq!(strategy.batches.get(&1).unwrap().maker_requotes, 0);
        assert_eq!(strategy.orphaned_child_orders.len(), 1);

        assert!(strategy.apply_exec_orphan_terminal(&orphan_terminal(104, 0.0)));
        assert_eq!(
            strategy.batches.get(&1).unwrap().phase,
            BatchPhase::ReadyToSubmit
        );
        assert_eq!(strategy.batches.get(&1).unwrap().maker_requotes, 1);
    }

    #[test]
    fn pending_target_stays_blocked_while_orphan_is_unresolved() {
        let mut strategy = strategy_with_orphan_batch(&[(105, 0, 1.0, 0.0)]);
        strategy.pending_target = Some(PendingTarget {
            target_qty: 2.0,
            generation_time: 2,
            from_key: b"cta_alpha".to_vec(),
        });

        strategy.process_pending_target(2);

        assert!(strategy.pending_target.is_some());
        assert!(strategy.active_target.is_none());
        assert!(strategy.batches.contains_key(&1));
    }

    #[test]
    fn position_allocation_gates_execution_and_can_be_reapplied() {
        let mut strategy = BatchExecStrategy::new(
            1,
            "cta_alpha",
            "BTCUSDT",
            TradingVenue::BinanceFutures,
            config(),
        );
        assert!(!strategy.position_allocation_ready());
        assert_eq!(strategy.virtual_position_qty(), None);

        strategy.apply_position_allocation(0.6, 100).unwrap();
        assert!(strategy.position_allocation_ready());
        assert_eq!(strategy.virtual_position_qty(), Some(0.6));

        strategy.suspend_position_allocation().unwrap();
        assert!(!strategy.position_allocation_ready());
        assert_eq!(strategy.virtual_position_qty(), Some(0.6));
        strategy.last_position_fill_at_us = 1_000;
        assert!(!strategy.position_reconciliation_settled(1_000 + POSITION_RECONCILE_SETTLE_US - 1));
        assert!(strategy.position_reconciliation_settled(1_000 + POSITION_RECONCILE_SETTLE_US));
        assert!(!strategy.position_reconciliation_ready(1_000 + POSITION_RECONCILE_SETTLE_US));

        strategy.apply_position_allocation(-0.2, 200).unwrap();
        assert!(strategy.position_allocation_ready());
        assert_eq!(strategy.virtual_position_qty(), Some(-0.2));
    }

    #[test]
    fn maker_batch_uses_configured_tick_ladder() {
        let plans = build_child_order_plans(
            &config(),
            Side::Sell,
            3.0,
            0,
            99.0,
            100.0,
            1.0,
            0.01,
            0.01,
            1.0,
        )
        .unwrap();
        assert_eq!(plans.len(), 3);
        assert_eq!(plans[0].price, 100.0);
        assert_eq!(plans[1].price, 102.0);
        assert_eq!(plans[2].price, 104.0);
        assert!(plans.iter().all(|plan| plan.order_type == OrderType::Limit));
    }

    #[test]
    fn maker_batch_can_start_one_tick_inside_opposite_best() {
        let mut config = config();
        config.maker_price_anchor = MakerPriceAnchor::OppositeBestPlusOneTick;
        let sell = build_child_order_plans(
            &config,
            Side::Sell,
            3.0,
            0,
            95.0,
            100.0,
            1.0,
            0.01,
            0.01,
            1.0,
        )
        .unwrap();
        assert_eq!(sell[0].price, 96.0);
        assert_eq!(sell[1].price, 98.0);
        assert_eq!(sell[2].price, 100.0);

        let buy = build_child_order_plans(
            &config,
            Side::Buy,
            3.0,
            0,
            95.0,
            100.0,
            1.0,
            0.01,
            0.01,
            1.0,
        )
        .unwrap();
        assert_eq!(buy[0].price, 99.0);
        assert_eq!(buy[1].price, 97.0);
        assert_eq!(buy[2].price, 95.0);
    }

    #[test]
    fn requote_preserves_each_hands_level_index() {
        let mut remaining_qty_by_level = BTreeMap::new();
        remaining_qty_by_level.insert(1, 0.8);
        remaining_qty_by_level.insert(2, 0.7);
        let plans = rebuild_child_order_plans_at_preserved_levels(
            &config(),
            Side::Sell,
            &remaining_qty_by_level,
            101.0,
            102.0,
            1.0,
            0.01,
            0.01,
            1.0,
        )
        .unwrap();

        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].level_index, 1);
        assert_eq!(plans[0].price, 104.0);
        assert_eq!(plans[1].level_index, 2);
        assert_eq!(plans[1].price, 106.0);
    }

    #[test]
    fn final_hand_can_be_smaller_than_single_order_amount() {
        let plans = build_child_order_plans(
            &config(),
            Side::Buy,
            0.55,
            0,
            100.0,
            101.0,
            1.0,
            0.01,
            0.01,
            1.0,
        )
        .unwrap();
        assert_eq!(plans.len(), 1);
        assert!((plans[0].qty_base - 0.55).abs() < QTY_EPS);
    }

    #[test]
    fn switches_to_one_taker_order_after_requote_limit() {
        let plans = build_child_order_plans(
            &config(),
            Side::Buy,
            2.0,
            3,
            100.0,
            101.0,
            1.0,
            0.01,
            0.01,
            1.0,
        )
        .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].order_type, OrderType::Market);
        assert_eq!(plans[0].price, 0.0);
    }
}
