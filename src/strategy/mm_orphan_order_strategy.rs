use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::pre_trade::TradeEngHub;
use crate::signal::common::{ExecutionType, OrderStatus, TradingVenue};
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::manager::{OrphanHandoff, Strategy};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::orphan_order_common::{OrphanOrderOwner, OrphanOrderTracker};
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update,
};
use log::{debug, info, warn};
use std::any::Any;

const MM_ORPHAN_QUERY_BASE_TICKS: u32 = 25;
const MM_ORPHAN_QUERY_MAX_TICKS: u32 = 3_200;
const MM_ORPHAN_ROLE: &str = "MmOrphanOrderStrategy: strategy_role=mm_orphan";

pub type MmOrphanOrderOwner = OrphanOrderOwner;

pub struct MmOrphanOrderStrategy {
    strategy_id: i32,
    symbol: String,
    orders: OrphanOrderTracker,
    active: bool,
}

impl MmOrphanOrderStrategy {
    pub fn new(strategy_id: i32, symbol: impl Into<String>) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            orders: OrphanOrderTracker::new(
                MM_ORPHAN_QUERY_BASE_TICKS,
                MM_ORPHAN_QUERY_BASE_TICKS,
                MM_ORPHAN_QUERY_MAX_TICKS,
            ),
            active: true,
        }
    }

    pub fn len(&self) -> usize {
        self.orders.len()
    }

    pub(crate) fn adopt_orphan_order_id(&mut self, handoff: &OrphanHandoff) -> bool {
        if handoff.client_order_id <= 0 {
            return false;
        }
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(handoff.client_order_id) else {
            warn!(
                "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} adopt missing local order client_order_id={} reason={}",
                self.strategy_id, handoff.client_order_id, handoff.reason
            );
            return false;
        };
        let symbol = normalize_symbol_for_internal(&order.symbol);
        let venue = order.venue;
        if symbol != self.symbol {
            return false;
        }
        drop(order);
        self.orders.adopt_order_owner(
            handoff.client_order_id,
            OrphanOrderOwner {
                source_strategy_id: handoff.source_strategy_id,
                source_kind: handoff.source_kind,
                uniform_ctx: Some(handoff.uniform_ctx.clone()),
            },
        );
        info!(
            "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} adopted order_id symbol={} client_order_id={} venue={:?} source_strategy_id={} source_kind={:?} reason={}",
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

    pub fn forget_order_id(&mut self, client_order_id: i64, reason: &str) -> bool {
        self.orders
            .forget_order_id(MM_ORPHAN_ROLE, self.strategy_id, client_order_id, reason)
    }

    fn track_order_id(&mut self, client_order_id: i64) {
        self.orders.track_order_id(client_order_id);
    }

    pub fn order_owner(&self, client_order_id: i64) -> Option<MmOrphanOrderOwner> {
        self.orders.owner(client_order_id)
    }

    fn finalize_terminal_order(&mut self, client_order_id: i64, event_time: i64, reason: &str) {
        self.orders.finalize_terminal_order(
            MM_ORPHAN_ROLE,
            self.strategy_id,
            client_order_id,
            event_time,
            reason,
            0.0,
        );
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

        let venue = update.trading_venue();
        if !matches!(
            venue,
            TradingVenue::BinanceMargin | TradingVenue::BinanceFutures
        ) {
            debug!(
                "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} skip cancel unsupported venue={:?} symbol={} client_order_id={}",
                self.strategy_id,
                venue,
                update.symbol(),
                update.client_order_id()
            );
            return;
        }

        let client_order_id = update.client_order_id();
        let symbol = normalize_symbol_for_internal(update.symbol());
        let cancel_bytes = {
            let order_mgr = MonitorChannel::instance().order_manager();
            let mgr = order_mgr.borrow();
            match mgr.build_unmatched_cancel_bytes(venue, &symbol, client_order_id) {
                Ok(bytes) => bytes,
                Err(err) => {
                    warn!(
                        "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} failed to build cancel client_order_id={} symbol={} venue={:?}: {}",
                        self.strategy_id,
                        client_order_id,
                        symbol,
                        venue,
                        err
                    );
                    return;
                }
            }
        };

        let exchange = venue.trade_engine_exchange();
        match TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
            Ok(()) => {
                warn!(
                    "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} sent cancel client_order_id={} order_id={} symbol={} venue={:?} x={:?} X={:?}",
                    self.strategy_id,
                    client_order_id,
                    update.order_id(),
                    symbol,
                    venue,
                    update.execution_type(),
                    update.status()
                );
            }
            Err(err) => warn!(
                "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} failed to send cancel client_order_id={} order_id={} symbol={} venue={:?}: {:#}",
                self.strategy_id,
                client_order_id,
                update.order_id(),
                symbol,
                venue,
                err
            ),
        }
    }

    fn send_order_query(&mut self, client_order_id: i64) -> bool {
        self.orders
            .send_order_query(MM_ORPHAN_ROLE, self.strategy_id, client_order_id, false)
    }
}

impl Strategy for MmOrphanOrderStrategy {
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
        self.orders.contains(order_id)
    }

    fn handle_signal(&mut self, _signal: &TradeSignal) {}

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        if normalize_symbol_for_internal(update.symbol()) != self.symbol {
            return;
        }
        let client_order_id = update.client_order_id();
        self.track_order_id(client_order_id);
        let uniform_ctx = self.orders.uniform_ctx(client_order_id);
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
                        "MmOrphanOrderStrategy",
                        self.strategy_id,
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
                        "MmOrphanOrderStrategy",
                        self.strategy_id,
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
                        "MmOrphanOrderStrategy",
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
            "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} adopted order_update symbol={} client_order_id={} order_id={} venue={:?} x={:?} X={:?}",
            self.strategy_id,
            update.symbol(),
            update.client_order_id(),
            update.order_id(),
            update.trading_venue(),
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
        let uniform_ctx = self.orders.uniform_ctx(client_order_id);
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
                    "MmOrphanOrderStrategy",
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
            "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} adopted trade_update symbol={} client_order_id={} order_id={} venue={:?} cumulative_qty={:.8} status={:?}",
            self.strategy_id,
            trade.symbol(),
            trade.client_order_id(),
            trade.order_id(),
            trade.trading_venue(),
            trade.cumulative_filled_quantity(),
            trade.order_status()
        );
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        let tracked_order_ids = self.orders.tracked_order_ids();
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
                drop(order);
                self.finalize_terminal_order(
                    client_order_id,
                    get_timestamp_us(),
                    "terminal local order on period clock",
                );
                continue;
            }

            drop(order);
            if self.orders.query_due_now(client_order_id) {
                let _ = self.send_order_query(client_order_id);
            }
        }
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }
}
