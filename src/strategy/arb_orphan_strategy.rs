use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::signal::common::OrderStatus;
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::manager::{OrphanHandoff, Strategy};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::orphan_order_common::{OrphanOrderOwner, OrphanOrderTracker};
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update,
};
use log::info;
use std::any::Any;

const ARB_ORPHAN_EPS: f64 = 1e-12;
const ARB_ORPHAN_QUERY_BASE_TICKS: u32 = 25;
const ARB_ORPHAN_QUERY_MAX_TICKS: u32 = 3_200;
const ARB_ORPHAN_ROLE: &str = "ArbOrphanStrategy: strategy_role=arb_orphan";

#[derive(Debug, Clone)]
pub struct ArbOrphanSnapshot {
    pub symbol: String,
    pub tracked_orders: usize,
}

pub struct ArbOrphanStrategy {
    strategy_id: i32,
    symbol: String,
    active: bool,
    orders: OrphanOrderTracker,
}

impl ArbOrphanStrategy {
    pub fn new(strategy_id: i32, symbol: impl Into<String>) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            active: true,
            orders: OrphanOrderTracker::new(
                ARB_ORPHAN_QUERY_BASE_TICKS,
                ARB_ORPHAN_QUERY_BASE_TICKS,
                ARB_ORPHAN_QUERY_MAX_TICKS,
            ),
        }
    }

    pub(crate) fn adopt_orphan_order_id(&mut self, handoff: &OrphanHandoff) -> bool {
        if handoff.client_order_id <= 0 {
            return false;
        }
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(handoff.client_order_id) else {
            return false;
        };
        let symbol = normalize_symbol_for_internal(&order.symbol);
        let venue = order.venue;
        let status = order.status;
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
            "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} adopted order symbol={} client_order_id={} venue={:?} status={:?} source_kind={:?} source_strategy_id={} reason={}",
            self.strategy_id,
            symbol,
            handoff.client_order_id,
            venue,
            status,
            handoff.source_kind,
            handoff.source_strategy_id,
            handoff.reason
        );
        true
    }

    pub fn snapshot(&self) -> ArbOrphanSnapshot {
        ArbOrphanSnapshot {
            symbol: self.symbol.clone(),
            tracked_orders: self.orders.len(),
        }
    }

    fn forget_order_id(&mut self, client_order_id: i64, reason: &str) -> bool {
        self.orders
            .forget_order_id(ARB_ORPHAN_ROLE, self.strategy_id, client_order_id, reason)
    }

    fn finalize_terminal_order(&mut self, client_order_id: i64, event_time: i64, reason: &str) {
        self.orders.finalize_terminal_order(
            ARB_ORPHAN_ROLE,
            self.strategy_id,
            client_order_id,
            event_time,
            reason,
            ARB_ORPHAN_EPS,
        );
    }

    fn update_order_from_order_update(&mut self, update: &dyn OrderUpdate) {
        let client_order_id = update.client_order_id();
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let _ = order_mgr.borrow_mut().update(client_order_id, |order| {
            if update.cumulative_filled_quantity() > order.cumulative_filled_quantity {
                order.cumulative_filled_quantity = update.cumulative_filled_quantity();
            }
            if update.order_id() > 0 {
                order.set_exchange_order_id(update.order_id());
            }
            if update.price() > 0.0 {
                order.price = update.price();
            }
            match update.status() {
                OrderStatus::New | OrderStatus::PartiallyFilled => {
                    if !order.status.is_terminal() {
                        order.status = OrderExecutionStatus::Create;
                    }
                }
                OrderStatus::Filled => {
                    order.status = OrderExecutionStatus::Filled;
                    order.set_end_time(update.event_time());
                }
                OrderStatus::Canceled => {
                    order.status = OrderExecutionStatus::Cancelled;
                    order.set_end_time(update.event_time());
                }
                OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                    order.status = OrderExecutionStatus::Rejected;
                    order.set_end_time(update.event_time());
                }
            }
        });
    }

    fn update_order_from_trade_update(&mut self, trade: &dyn TradeUpdate) {
        let client_order_id = trade.client_order_id();
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let _ = order_mgr.borrow_mut().update(client_order_id, |order| {
            if trade.cumulative_filled_quantity() > order.cumulative_filled_quantity {
                order.cumulative_filled_quantity = trade.cumulative_filled_quantity();
            }
            order.set_filled_time(trade.trade_time());
            if trade.order_id() > 0 {
                order.set_exchange_order_id(trade.order_id());
            }
            if trade.price() > 0.0 {
                order.price = trade.price();
            }
            match trade.order_status() {
                Some(OrderStatus::Filled) => {
                    order.status = OrderExecutionStatus::Filled;
                    order.set_end_time(trade.event_time());
                }
                Some(OrderStatus::PartiallyFilled) => {
                    if !order.status.is_terminal() {
                        order.status = OrderExecutionStatus::Create;
                    }
                }
                Some(OrderStatus::Canceled) => {
                    order.status = OrderExecutionStatus::Cancelled;
                    order.set_end_time(trade.event_time());
                }
                Some(OrderStatus::Expired | OrderStatus::ExpiredInMatch) => {
                    order.status = OrderExecutionStatus::Rejected;
                    order.set_end_time(trade.event_time());
                }
                Some(OrderStatus::New) | None => {}
            }
        });
    }

    fn request_cancel_if_needed(&self, update: &dyn OrderUpdate) -> bool {
        self.orders
            .request_cancel_from_order_update(ARB_ORPHAN_ROLE, self.strategy_id, update)
    }

    fn send_order_query(&mut self, client_order_id: i64) -> bool {
        self.orders
            .send_order_query(ARB_ORPHAN_ROLE, self.strategy_id, client_order_id, true)
    }
}

impl Strategy for ArbOrphanStrategy {
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
        if !self.is_strategy_order(client_order_id) {
            return;
        }
        let uniform_ctx = self.orders.uniform_ctx(client_order_id);
        let prev_cumulative_filled_qty = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| {
                order_mgr
                    .borrow()
                    .get(client_order_id)
                    .map(|order| order.cumulative_filled_quantity)
            })
            .unwrap_or(0.0);
        self.update_order_from_order_update(update);
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
                        "ArbOrphanStrategy: strategy_role=arb_orphan",
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
                        "ArbOrphanStrategy: strategy_role=arb_orphan",
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
                        "ArbOrphanStrategy: strategy_role=arb_orphan",
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
            let _ = self.request_cancel_if_needed(update);
        }
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        if normalize_symbol_for_internal(trade.symbol()) != self.symbol {
            return;
        }
        let client_order_id = trade.client_order_id();
        if !self.is_strategy_order(client_order_id) {
            return;
        }
        let uniform_ctx = self.orders.uniform_ctx(client_order_id);
        let prev_cumulative_filled_qty = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| {
                order_mgr
                    .borrow()
                    .get(client_order_id)
                    .map(|order| order.cumulative_filled_quantity)
            })
            .unwrap_or(0.0);
        self.update_order_from_trade_update(trade);
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
                    "ArbOrphanStrategy: strategy_role=arb_orphan",
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
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        let tracked = self.orders.tracked_order_ids();
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        for client_order_id in tracked {
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
