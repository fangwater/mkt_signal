use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::pre_trade::{QueryEngHub, TradeEngHub};
use crate::signal::common::{ExecutionType, OrderStatus};
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::manager::{ForceCloseControl, OrphanHandoff, OrphanSourceKind, Strategy};
use crate::strategy::order_query_builder::build_order_query_request;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update, UniformAmountSource, UniformPublishCtx,
};
use log::{info, warn};
use std::any::Any;
use std::collections::{HashMap, HashSet};

const HEDGE_ORPHAN_QUERY_BASE_TICKS: u32 = 25;
const HEDGE_ORPHAN_QUERY_MAX_TICKS: u32 = 3_200;

#[derive(Debug, Clone, PartialEq)]
pub struct HedgeOrphanOrderOwner {
    pub source_strategy_id: i32,
    pub source_kind: OrphanSourceKind,
    pub uniform_ctx: UniformPublishCtx,
    pub recorded_base_qty: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HedgeOrphanQueryState {
    query_count: u8,
    ticks_until_next_query: u32,
}

pub struct HedgeOrphanOrderStrategy {
    strategy_id: i32,
    symbol: String,
    order_ids: HashSet<i64>,
    order_owners: HashMap<i64, HedgeOrphanOrderOwner>,
    query_states: HashMap<i64, HedgeOrphanQueryState>,
    active: bool,
}

impl HedgeOrphanOrderStrategy {
    pub fn new(strategy_id: i32, symbol: impl Into<String>) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            order_ids: HashSet::new(),
            order_owners: HashMap::new(),
            query_states: HashMap::new(),
            active: true,
        }
    }

    fn initial_query_state() -> HedgeOrphanQueryState {
        HedgeOrphanQueryState {
            query_count: 0,
            ticks_until_next_query: HEDGE_ORPHAN_QUERY_BASE_TICKS,
        }
    }

    fn next_query_ticks(query_count: u8) -> u32 {
        let multiplier = 1_u32
            .checked_shl(query_count.min(31) as u32)
            .unwrap_or(u32::MAX);
        HEDGE_ORPHAN_QUERY_BASE_TICKS
            .saturating_mul(multiplier)
            .min(HEDGE_ORPHAN_QUERY_MAX_TICKS)
    }

    fn ensure_query_state(&mut self, client_order_id: i64) {
        self.query_states
            .entry(client_order_id)
            .or_insert_with(Self::initial_query_state);
    }

    fn track_order_id(&mut self, client_order_id: i64) {
        self.order_ids.insert(client_order_id);
        self.ensure_query_state(client_order_id);
    }

    fn adopt_order_id_inner(&mut self, handoff: &OrphanHandoff) -> bool {
        if handoff.client_order_id <= 0 {
            return false;
        }
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(handoff.client_order_id) else {
            warn!(
                "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} adopt missing local order client_order_id={} reason={}",
                self.strategy_id, handoff.client_order_id, handoff.reason
            );
            return false;
        };
        let symbol = normalize_symbol_for_internal(&order.symbol);
        let venue = order.venue;
        if symbol != self.symbol {
            return false;
        }

        self.track_order_id(handoff.client_order_id);
        self.order_owners.insert(
            handoff.client_order_id,
            HedgeOrphanOrderOwner {
                source_strategy_id: handoff.source_strategy_id,
                source_kind: handoff.source_kind,
                uniform_ctx: handoff.uniform_ctx.clone(),
                recorded_base_qty: handoff.recorded_base_qty.max(0.0),
            },
        );
        info!(
            "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} adopted order_id symbol={} client_order_id={} venue={:?} source_strategy_id={} source_kind={:?} reason={}",
            self.strategy_id,
            symbol,
            handoff.client_order_id,
            venue,
            handoff.source_strategy_id,
            handoff.source_kind,
            handoff.reason
        );
        true
    }

    fn forget_order_id(&mut self, client_order_id: i64, reason: &str) -> bool {
        let removed = self.order_ids.remove(&client_order_id);
        if removed {
            self.order_owners.remove(&client_order_id);
            self.query_states.remove(&client_order_id);
            info!(
                "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} forgot order_id client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
        }
        removed
    }

    fn finalize_terminal_order(&mut self, client_order_id: i64, event_time: i64, reason: &str) {
        let owner = self.order_owners.get(&client_order_id).cloned();
        if let (Some(order_mgr), Some(owner)) = (MonitorChannel::try_order_manager(), owner) {
            let snapshot = {
                let mgr = order_mgr.borrow();
                mgr.get(client_order_id).map(|order| {
                    (
                        order.symbol.clone(),
                        order.side,
                        order.cumulative_filled_quantity * order.qty_multiplier,
                        order.price,
                    )
                })
            };
            if let Some((symbol, side, cumulative_base_qty, price)) = snapshot {
                let terminal_base_qty = cumulative_base_qty - owner.recorded_base_qty;
                if terminal_base_qty > 1e-12 {
                    let signed_base_qty = match side {
                        crate::pre_trade::order_manager::Side::Buy => terminal_base_qty.abs(),
                        crate::pre_trade::order_manager::Side::Sell => -terminal_base_qty.abs(),
                    };
                    let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                    let mut strategy_mgr = strategy_mgr.borrow_mut();
                    let recorded = match owner.source_kind {
                        OrphanSourceKind::Open => strategy_mgr.record_open_order_terminal(
                            &symbol,
                            signed_base_qty,
                            event_time,
                            price,
                            0,
                        ),
                        OrphanSourceKind::Hedge => strategy_mgr.record_hedge_order_terminal(
                            &symbol,
                            signed_base_qty,
                            event_time,
                            price,
                        ),
                    };
                    if !recorded {
                        warn!(
                            "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} record order terminal failed client_order_id={} symbol={} source_kind={:?} terminal_base_qty={:.8} reason={}",
                            self.strategy_id,
                            client_order_id,
                            symbol,
                            owner.source_kind,
                            terminal_base_qty,
                            reason
                        );
                    }
                }
            }
            let _ = order_mgr.borrow_mut().remove(client_order_id);
        }
        self.forget_order_id(client_order_id, reason);
    }

    fn request_cancel_if_needed(&mut self, update: &dyn OrderUpdate) {
        if update.execution_type() == ExecutionType::Trade {
            return;
        }
        if !matches!(
            update.status(),
            OrderStatus::New | OrderStatus::PartiallyFilled
        ) {
            return;
        }

        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let Some(order) = order_mgr.borrow().get(update.client_order_id()) else {
            return;
        };
        let exchange = order.venue.trade_engine_exchange();
        let cancel_bytes = match order.get_order_cancel_bytes() {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(
                    "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} failed to build cancel client_order_id={} symbol={} venue={:?}: {}",
                    self.strategy_id,
                    order.client_order_id,
                    order.symbol,
                    order.venue,
                    err
                );
                return;
            }
        };
        match TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
            Ok(()) => warn!(
                "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} sent cancel client_order_id={} order_id={} symbol={} venue={:?} x={:?} X={:?}",
                self.strategy_id,
                update.client_order_id(),
                update.order_id(),
                update.symbol(),
                update.trading_venue(),
                update.execution_type(),
                update.status()
            ),
            Err(err) => warn!(
                "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} failed to send cancel client_order_id={} order_id={} symbol={} venue={:?}: {:#}",
                self.strategy_id,
                update.client_order_id(),
                update.order_id(),
                update.symbol(),
                update.trading_venue(),
                err
            ),
        }
    }

    fn send_order_query(&mut self, client_order_id: i64) -> bool {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} send_order_query but local order missing client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        };
        let request_query_id = client_order_id;
        match build_order_query_request(&order, request_query_id, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request(exchange.as_str(), &req_bytes)
                {
                    warn!(
                        "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} publish query failed client_order_id={} request_query_id={} err={:#}",
                        self.strategy_id, client_order_id, request_query_id, err
                    );
                    return false;
                }
                info!(
                    "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} query sent client_order_id={} request_query_id={}",
                    self.strategy_id, client_order_id, request_query_id
                );
                true
            }
            Err(err) => {
                warn!(
                    "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} build query failed client_order_id={} err={}",
                    self.strategy_id, client_order_id, err
                );
                false
            }
        }
    }
}

impl ForceCloseControl for HedgeOrphanOrderStrategy {
    fn set_force_close_mode(&mut self, _enabled: bool) {}

    fn is_force_close_mode(&self) -> bool {
        false
    }
}

impl Strategy for HedgeOrphanOrderStrategy {
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
        self.order_ids.contains(&order_id)
    }

    fn handle_signal(&mut self, _signal: &TradeSignal) {}

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        if normalize_symbol_for_internal(update.symbol()) != self.symbol {
            return;
        }
        let client_order_id = update.client_order_id();
        self.track_order_id(client_order_id);
        let uniform_ctx = self
            .order_owners
            .get(&client_order_id)
            .map(|owner| owner.uniform_ctx.clone());
        let prev_cumulative_filled_qty = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| {
                order_mgr
                    .borrow()
                    .get(client_order_id)
                    .map(|order| order.cumulative_filled_quantity)
            })
            .unwrap_or(0.0);

        if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let incoming_cum = update.cumulative_filled_quantity();
            let incoming_order_id = update.order_id();
            let event_time = update.event_time();
            let status = update.status();
            let _ = order_mgr.borrow_mut().update(client_order_id, |order| {
                if incoming_cum > order.cumulative_filled_quantity {
                    order.cumulative_filled_quantity = incoming_cum;
                }
                if incoming_order_id > 0 {
                    order.set_exchange_order_id(incoming_order_id);
                }
                match status {
                    OrderStatus::New | OrderStatus::PartiallyFilled => {
                        if !order.status.is_terminal() {
                            order.status = OrderExecutionStatus::Create;
                        }
                    }
                    OrderStatus::Canceled => {
                        order.status = OrderExecutionStatus::Cancelled;
                        order.set_end_time(event_time);
                    }
                    OrderStatus::Filled => {
                        order.status = OrderExecutionStatus::Filled;
                        order.set_end_time(event_time);
                    }
                    OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                        order.status = OrderExecutionStatus::Rejected;
                        order.set_end_time(event_time);
                    }
                }
            });
        }

        if let Some(ctx) = uniform_ctx.as_ref() {
            let updated_order = MonitorChannel::try_order_manager()
                .and_then(|order_mgr| order_mgr.borrow().get(client_order_id));
            if let Some(order) = updated_order {
                if update.status() == OrderStatus::New {
                    publish_uniform_new_order(
                        update,
                        &order,
                        prev_cumulative_filled_qty,
                        ctx,
                        "HedgeOrphanOrderStrategy",
                        self.strategy_id,
                        UniformAmountSource::OrderUpdate,
                    );
                }
                if matches!(
                    update.status(),
                    OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::ExpiredInMatch
                ) {
                    publish_uniform_terminal_order(
                        update,
                        &order,
                        prev_cumulative_filled_qty,
                        ctx,
                        "HedgeOrphanOrderStrategy",
                        self.strategy_id,
                        UniformAmountSource::OrderUpdate,
                    );
                }
                if matches!(
                    update.status(),
                    OrderStatus::PartiallyFilled | OrderStatus::Filled
                ) {
                    publish_uniform_trade_order_from_order_update(
                        update,
                        &order,
                        prev_cumulative_filled_qty,
                        ctx,
                        "HedgeOrphanOrderStrategy",
                        self.strategy_id,
                    );
                }
            }
        }

        if matches!(
            update.status(),
            OrderStatus::Canceled
                | OrderStatus::Filled
                | OrderStatus::Expired
                | OrderStatus::ExpiredInMatch
        ) {
            self.finalize_terminal_order(
                client_order_id,
                update.event_time(),
                "terminal order update",
            );
        } else {
            self.request_cancel_if_needed(update);
        }
        info!(
            "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} adopted order_update symbol={} client_order_id={} order_id={} x={:?} X={:?}",
            self.strategy_id,
            update.symbol(),
            update.client_order_id(),
            update.order_id(),
            update.execution_type(),
            update.status()
        );
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        if normalize_symbol_for_internal(trade.symbol()) != self.symbol {
            return;
        }
        let client_order_id = trade.client_order_id();
        self.track_order_id(client_order_id);
        let uniform_ctx = self
            .order_owners
            .get(&client_order_id)
            .map(|owner| owner.uniform_ctx.clone());
        let prev_cumulative_filled_qty = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| {
                order_mgr
                    .borrow()
                    .get(client_order_id)
                    .map(|order| order.cumulative_filled_quantity)
            })
            .unwrap_or(0.0);

        if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let cumulative_qty = trade.cumulative_filled_quantity();
            let trade_time = trade.trade_time();
            let event_time = trade.event_time();
            let order_id = trade.order_id();
            let price = trade.price();
            let terminal_status = trade.order_status();
            let _ = order_mgr.borrow_mut().update(client_order_id, |order| {
                if cumulative_qty > order.cumulative_filled_quantity {
                    order.cumulative_filled_quantity = cumulative_qty;
                }
                order.set_filled_time(trade_time);
                if order_id > 0 {
                    order.set_exchange_order_id(order_id);
                }
                if price > 0.0 {
                    order.price = price;
                }
                match terminal_status {
                    Some(OrderStatus::Filled) => {
                        order.status = OrderExecutionStatus::Filled;
                        order.set_end_time(event_time);
                    }
                    Some(OrderStatus::PartiallyFilled) => {
                        if !order.status.is_terminal() {
                            order.status = OrderExecutionStatus::Create;
                        }
                    }
                    Some(OrderStatus::Canceled) => {
                        order.status = OrderExecutionStatus::Cancelled;
                        order.set_end_time(event_time);
                    }
                    Some(OrderStatus::Expired | OrderStatus::ExpiredInMatch) => {
                        order.status = OrderExecutionStatus::Rejected;
                        order.set_end_time(event_time);
                    }
                    Some(OrderStatus::New) | None => {}
                }
            });
        }

        if let (Some(ctx), Some(status)) = (uniform_ctx.as_ref(), trade.order_status()) {
            let updated_order = MonitorChannel::try_order_manager()
                .and_then(|order_mgr| order_mgr.borrow().get(client_order_id));
            if let Some(order) = updated_order {
                publish_uniform_trade_order(
                    trade,
                    &order,
                    prev_cumulative_filled_qty,
                    status,
                    ctx,
                    "HedgeOrphanOrderStrategy",
                    self.strategy_id,
                );
            }
        }

        if trade.order_status().is_some_and(|status| {
            matches!(
                status,
                OrderStatus::Canceled
                    | OrderStatus::Filled
                    | OrderStatus::Expired
                    | OrderStatus::ExpiredInMatch
            )
        }) {
            self.finalize_terminal_order(
                client_order_id,
                trade.event_time(),
                "terminal trade update",
            );
        }
        info!(
            "HedgeOrphanOrderStrategy: strategy_role=hedge_orphan strategy_id={} adopted trade_update symbol={} client_order_id={} order_id={} cumulative_qty={:.8} status={:?}",
            self.strategy_id,
            trade.symbol(),
            trade.client_order_id(),
            trade.order_id(),
            trade.cumulative_filled_quantity(),
            trade.order_status()
        );
    }

    fn adopt_hedge_orphan_order_id(&mut self, handoff: &OrphanHandoff) -> bool {
        self.adopt_order_id_inner(handoff)
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        let tracked_order_ids: Vec<i64> = self.order_ids.iter().copied().collect();
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };

        for client_order_id in tracked_order_ids {
            let order_opt = order_mgr.borrow().get(client_order_id);
            let Some(order) = order_opt else {
                self.forget_order_id(client_order_id, "missing local order on period clock");
                continue;
            };
            if order.status.is_terminal() {
                self.finalize_terminal_order(
                    client_order_id,
                    get_timestamp_us(),
                    "terminal local order on period clock",
                );
                continue;
            }

            let Some(query_state) = self.query_states.get_mut(&client_order_id) else {
                continue;
            };
            if query_state.ticks_until_next_query > 0 {
                query_state.ticks_until_next_query -= 1;
                continue;
            }

            let next_query_count = query_state.query_count.saturating_add(1);
            query_state.query_count = next_query_count;
            query_state.ticks_until_next_query = Self::next_query_ticks(next_query_count);
            let _ = self.send_order_query(client_order_id);
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }
}
