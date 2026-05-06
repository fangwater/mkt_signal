use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::pre_trade::{QueryEngHub, TradeEngHub};
use crate::signal::common::{ExecutionType, OrderStatus};
use crate::strategy::manager::OrphanSourceKind;
use crate::strategy::order_query_builder::build_order_query_request;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update, UniformPublishCtx,
};
use log::{info, warn};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
pub struct OrphanOrderOwner {
    pub source_strategy_id: i32,
    pub source_kind: OrphanSourceKind,
    pub uniform_ctx: UniformPublishCtx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrphanQueryState {
    query_count: u8,
    ticks_until_next_query: u32,
}

pub struct OrphanOrderTracker {
    order_ids: HashSet<i64>,
    order_owners: HashMap<i64, OrphanOrderOwner>,
    query_states: HashMap<i64, OrphanQueryState>,
    initial_query_ticks: u32,
    query_base_ticks: u32,
    query_max_ticks: u32,
}

impl OrphanOrderTracker {
    pub fn new(initial_query_ticks: u32, query_base_ticks: u32, query_max_ticks: u32) -> Self {
        Self {
            order_ids: HashSet::new(),
            order_owners: HashMap::new(),
            query_states: HashMap::new(),
            initial_query_ticks,
            query_base_ticks,
            query_max_ticks,
        }
    }

    pub fn len(&self) -> usize {
        self.order_ids.len()
    }

    pub fn contains(&self, client_order_id: i64) -> bool {
        self.order_ids.contains(&client_order_id)
    }

    pub fn tracked_order_ids(&self) -> Vec<i64> {
        self.order_ids.iter().copied().collect()
    }

    pub fn owner(&self, client_order_id: i64) -> Option<OrphanOrderOwner> {
        self.order_owners.get(&client_order_id).cloned()
    }

    pub fn uniform_ctx(&self, client_order_id: i64) -> Option<UniformPublishCtx> {
        self.order_owners
            .get(&client_order_id)
            .map(|owner| owner.uniform_ctx.clone())
    }

    fn track_order_id(&mut self, client_order_id: i64) {
        if client_order_id <= 0 {
            return;
        }
        self.order_ids.insert(client_order_id);
        self.ensure_query_state(client_order_id);
    }

    pub fn adopt_order_owner(&mut self, client_order_id: i64, owner: OrphanOrderOwner) {
        if client_order_id <= 0 {
            return;
        }
        self.track_order_id(client_order_id);
        self.order_owners.insert(client_order_id, owner);
    }

    pub fn forget_order_id(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
        reason: &str,
    ) -> bool {
        let removed = self.order_ids.remove(&client_order_id);
        if removed {
            self.order_owners.remove(&client_order_id);
            self.query_states.remove(&client_order_id);
            info!(
                "{}: strategy_id={} forgot order_id client_order_id={} reason={}",
                strategy_role, strategy_id, client_order_id, reason
            );
        }
        removed
    }

    pub fn query_due_now(&mut self, client_order_id: i64) -> bool {
        let query_base_ticks = self.query_base_ticks;
        let query_max_ticks = self.query_max_ticks;
        let Some(query_state) = self.query_states.get_mut(&client_order_id) else {
            return false;
        };
        if query_state.ticks_until_next_query > 0 {
            query_state.ticks_until_next_query -= 1;
            return false;
        }
        let next_query_count = query_state.query_count.saturating_add(1);
        query_state.query_count = next_query_count;
        query_state.ticks_until_next_query =
            Self::next_query_ticks(query_base_ticks, query_max_ticks, next_query_count);
        true
    }

    pub fn send_order_query(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
    ) -> bool {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "{}: strategy_id={} send_order_query missing local order client_order_id={}",
                strategy_role, strategy_id, client_order_id
            );
            return false;
        };
        let request_query_id = client_order_id;
        match build_order_query_request(&order, request_query_id, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request(exchange.as_str(), &req_bytes)
                {
                    warn!(
                        "{}: strategy_id={} publish query failed client_order_id={} request_query_id={} err={:#}",
                        strategy_role, strategy_id, client_order_id, request_query_id, err
                    );
                    return false;
                }
                info!(
                    "{}: strategy_id={} query sent client_order_id={} request_query_id={}",
                    strategy_role, strategy_id, client_order_id, request_query_id
                );
                true
            }
            Err(err) => {
                warn!(
                    "{}: strategy_id={} build query failed client_order_id={} err={}",
                    strategy_role, strategy_id, client_order_id, err
                );
                false
            }
        }
    }

    pub fn apply_order_update(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        update: &dyn OrderUpdate,
    ) -> bool {
        let client_order_id = update.client_order_id();
        if !self.contains(client_order_id) {
            return false;
        }
        let Some(ctx) = self.uniform_ctx(client_order_id) else {
            return false;
        };
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
            let incoming_price = update.price();
            let event_time = update.event_time();
            let status = update.status();
            let _ = order_mgr.borrow_mut().update(client_order_id, |order| {
                if incoming_cum > order.cumulative_filled_quantity {
                    order.cumulative_filled_quantity = incoming_cum;
                }
                if incoming_order_id > 0 {
                    order.set_exchange_order_id(incoming_order_id);
                }
                if incoming_price > 0.0 {
                    order.price = incoming_price;
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

        let updated_order = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| order_mgr.borrow().get(client_order_id));
        if let Some(order) = updated_order {
            if update.status() == OrderStatus::New {
                publish_uniform_new_order(
                    update,
                    &order,
                    prev_cumulative_filled_qty,
                    &ctx,
                    strategy_role,
                    strategy_id,
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
                    &ctx,
                    strategy_role,
                    strategy_id,
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
                    &ctx,
                    strategy_role,
                    strategy_id,
                );
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
                strategy_role,
                strategy_id,
                client_order_id,
                update.event_time(),
                "terminal order update",
                0.0,
            );
        } else {
            let _ = self.request_cancel_from_order_update(strategy_role, strategy_id, update);
        }
        info!(
            "{}: strategy_id={} adopted order_update symbol={} client_order_id={} order_id={} venue={:?} x={:?} X={:?}",
            strategy_role,
            strategy_id,
            update.symbol(),
            update.client_order_id(),
            update.order_id(),
            update.trading_venue(),
            update.execution_type(),
            update.status()
        );
        true
    }

    pub fn apply_trade_update(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        trade: &dyn TradeUpdate,
    ) -> bool {
        let client_order_id = trade.client_order_id();
        if !self.contains(client_order_id) {
            return false;
        }
        let Some(ctx) = self.uniform_ctx(client_order_id) else {
            return false;
        };
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
            let event_time = trade.event_time();
            let order_id = trade.order_id();
            let price = trade.price();
            let terminal_status = trade.order_status();
            let _ = order_mgr.borrow_mut().update(client_order_id, |order| {
                if cumulative_qty > order.cumulative_filled_quantity {
                    order.cumulative_filled_quantity = cumulative_qty;
                }
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

        if let Some(status) = trade.order_status() {
            let updated_order = MonitorChannel::try_order_manager()
                .and_then(|order_mgr| order_mgr.borrow().get(client_order_id));
            if let Some(order) = updated_order {
                publish_uniform_trade_order(
                    trade,
                    &order,
                    prev_cumulative_filled_qty,
                    status,
                    &ctx,
                    strategy_role,
                    strategy_id,
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
                strategy_role,
                strategy_id,
                client_order_id,
                trade.event_time(),
                "terminal trade update",
                0.0,
            );
        }
        info!(
            "{}: strategy_id={} adopted trade_update symbol={} client_order_id={} order_id={} venue={:?} cumulative_qty={:.8} status={:?}",
            strategy_role,
            strategy_id,
            trade.symbol(),
            trade.client_order_id(),
            trade.order_id(),
            trade.trading_venue(),
            trade.cumulative_filled_quantity(),
            trade.order_status()
        );
        true
    }

    pub fn request_cancel_from_order_update(
        &self,
        strategy_role: &str,
        strategy_id: i32,
        update: &dyn OrderUpdate,
    ) -> bool {
        if update.execution_type() == ExecutionType::Trade {
            return false;
        }
        if !matches!(
            update.status(),
            OrderStatus::New | OrderStatus::PartiallyFilled
        ) {
            return false;
        }

        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(update.client_order_id()) else {
            return false;
        };
        if order.status.is_terminal() {
            return false;
        }

        let client_order_id = order.client_order_id;
        let symbol = order.symbol.clone();
        let venue = order.venue;
        let exchange = venue.trade_engine_exchange();
        let cancel_bytes = match order.get_order_cancel_bytes() {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(
                    "{}: strategy_id={} failed to build cancel client_order_id={} symbol={} venue={:?}: {}",
                    strategy_role, strategy_id, client_order_id, symbol, venue, err
                );
                return false;
            }
        };
        drop(order);

        match TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
            Ok(()) => {
                warn!(
                    "{}: strategy_id={} sent cancel client_order_id={} order_id={} symbol={} venue={:?} x={:?} X={:?}",
                    strategy_role,
                    strategy_id,
                    update.client_order_id(),
                    update.order_id(),
                    update.symbol(),
                    update.trading_venue(),
                    update.execution_type(),
                    update.status()
                );
                true
            }
            Err(err) => {
                warn!(
                    "{}: strategy_id={} failed to send cancel client_order_id={} order_id={} symbol={} venue={:?}: {:#}",
                    strategy_role,
                    strategy_id,
                    update.client_order_id(),
                    update.order_id(),
                    update.symbol(),
                    update.trading_venue(),
                    err
                );
                false
            }
        }
    }

    pub fn finalize_terminal_order(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
        event_time: i64,
        reason: &str,
        eps: f64,
    ) {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            self.forget_order_id(strategy_role, strategy_id, client_order_id, reason);
            return;
        };
        let snapshot = {
            let mgr = order_mgr.borrow();
            mgr.get(client_order_id).map(|order| {
                (
                    order.venue,
                    order.symbol.clone(),
                    order.side,
                    order.quantity,
                    order.cumulative_filled_quantity,
                    order.price,
                )
            })
        };
        let Some((venue, symbol, side, order_qty, cumulative_qty, price)) = snapshot else {
            self.forget_order_id(strategy_role, strategy_id, client_order_id, reason);
            return;
        };

        let had_owner = if let Some(owner) = self.owner(client_order_id) {
            let order_base_qty = MonitorChannel::instance().qty_to_base(venue, &symbol, order_qty);
            let cumulative_base_qty =
                MonitorChannel::instance().qty_to_base(venue, &symbol, cumulative_qty);
            let should_record = match owner.source_kind {
                OrphanSourceKind::Open => cumulative_base_qty > eps,
                OrphanSourceKind::Hedge => order_base_qty > eps || cumulative_base_qty > eps,
            };
            if should_record {
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let mut strategy_mgr = strategy_mgr.borrow_mut();
                let normalized_symbol = normalize_symbol_for_internal(&symbol);
                let recorded = match owner.source_kind {
                    OrphanSourceKind::Open => strategy_mgr.record_open_order_terminal(
                        &normalized_symbol,
                        side,
                        order_base_qty,
                        cumulative_base_qty,
                        event_time,
                        price,
                        0,
                    ),
                    OrphanSourceKind::Hedge => strategy_mgr.record_hedge_order_terminal(
                        &normalized_symbol,
                        side,
                        order_base_qty,
                        cumulative_base_qty,
                        event_time,
                        price,
                    ),
                };
                if !recorded {
                    warn!(
                        "{}: strategy_id={} record order terminal failed client_order_id={} symbol={} source_kind={:?} cumulative_base_qty={:.8} reason={}",
                        strategy_role,
                        strategy_id,
                        client_order_id,
                        normalized_symbol,
                        owner.source_kind,
                        cumulative_base_qty,
                        reason
                    );
                }
            }
            info!(
                "{}: strategy_id={} finalized order client_order_id={} source_kind={:?} symbol={} venue={:?} side={:?} order_qty={:.8} cumulative_qty={:.8} order_base_qty={:.8} cumulative_base_qty={:.8} reason={}",
                strategy_role,
                strategy_id,
                client_order_id,
                owner.source_kind,
                symbol,
                venue,
                side,
                order_qty,
                cumulative_qty,
                order_base_qty,
                cumulative_base_qty,
                reason
            );
            true
        } else {
            warn!(
                "{}: strategy_id={} finalize terminal order missing owner client_order_id={} reason={}",
                strategy_role, strategy_id, client_order_id, reason
            );
            false
        };

        if had_owner {
            let _ = order_mgr.borrow_mut().remove(client_order_id);
        }
        self.forget_order_id(strategy_role, strategy_id, client_order_id, reason);
    }

    pub fn handle_period_clock(&mut self, strategy_role: &str, strategy_id: i32) {
        let tracked_order_ids = self.tracked_order_ids();
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };

        for client_order_id in tracked_order_ids {
            let order_opt = order_mgr.borrow().get(client_order_id);
            let Some(order) = order_opt else {
                self.forget_order_id(
                    strategy_role,
                    strategy_id,
                    client_order_id,
                    "missing local order on period clock",
                );
                continue;
            };
            if order.status.is_terminal() {
                drop(order);
                self.finalize_terminal_order(
                    strategy_role,
                    strategy_id,
                    client_order_id,
                    get_timestamp_us(),
                    "terminal local order on period clock",
                    0.0,
                );
                continue;
            }

            drop(order);
            if self.query_due_now(client_order_id) {
                let _ = self.send_order_query(strategy_role, strategy_id, client_order_id);
            }
        }
    }

    fn ensure_query_state(&mut self, client_order_id: i64) {
        self.query_states
            .entry(client_order_id)
            .or_insert_with(|| OrphanQueryState {
                query_count: 0,
                ticks_until_next_query: self.initial_query_ticks,
            });
    }

    fn next_query_ticks(query_base_ticks: u32, query_max_ticks: u32, query_count: u8) -> u32 {
        let multiplier = 1_u32
            .checked_shl(query_count.min(31) as u32)
            .unwrap_or(u32::MAX);
        query_base_ticks
            .saturating_mul(multiplier)
            .min(query_max_ticks)
    }
}
