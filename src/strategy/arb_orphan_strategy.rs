use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{OrderExecutionStatus, Side};
use crate::signal::common::OrderStatus;
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::manager::{ForceCloseControl, Strategy};
use crate::strategy::net_qty_queue::NetQtyQueue;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::query_engine_response::QueryEngineResponse;
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::strategy::trade_update::TradeUpdate;
use log::{debug, info};
use std::any::Any;
use std::collections::HashMap;

const ARB_ORPHAN_EPS: f64 = 1e-12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbOrphanLeg {
    Open,
    Hedge,
}

#[derive(Debug, Clone)]
pub struct ArbOrphanSnapshot {
    pub symbol: String,
    pub net_qty: f64,
    pub weighted_inventory_price: f64,
    pub open_buy_qty: f64,
    pub open_sell_qty: f64,
    pub hedge_buy_qty: f64,
    pub hedge_sell_qty: f64,
    pub tracked_orders: usize,
}

pub struct ArbOrphanStrategy {
    strategy_id: i32,
    symbol: String,
    active: bool,
    order_legs: HashMap<i64, ArbOrphanLeg>,
    net_qty_queue: NetQtyQueue,
    net_qty: f64,
    open_buy_qty: f64,
    open_sell_qty: f64,
    hedge_buy_qty: f64,
    hedge_sell_qty: f64,
}

impl ArbOrphanStrategy {
    pub fn new(strategy_id: i32, symbol: impl Into<String>) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            active: true,
            order_legs: HashMap::new(),
            net_qty_queue: NetQtyQueue::new(),
            net_qty: 0.0,
            open_buy_qty: 0.0,
            open_sell_qty: 0.0,
            hedge_buy_qty: 0.0,
            hedge_sell_qty: 0.0,
        }
    }

    pub fn track_open_order_id(&mut self, client_order_id: i64) {
        self.track_order_id(client_order_id, ArbOrphanLeg::Open);
    }

    pub fn track_hedge_order_id(&mut self, client_order_id: i64) {
        self.track_order_id(client_order_id, ArbOrphanLeg::Hedge);
    }

    pub fn track_order_id(&mut self, client_order_id: i64, leg: ArbOrphanLeg) {
        if client_order_id <= 0 {
            return;
        }
        self.order_legs.insert(client_order_id, leg);
        debug!(
            "ArbOrphanStrategy: strategy_id={} track order client_order_id={} leg={:?}",
            self.strategy_id, client_order_id, leg
        );
    }

    pub fn snapshot(&self) -> ArbOrphanSnapshot {
        ArbOrphanSnapshot {
            symbol: self.symbol.clone(),
            net_qty: self.net_qty,
            weighted_inventory_price: self.weighted_inventory_price(),
            open_buy_qty: self.open_buy_qty,
            open_sell_qty: self.open_sell_qty,
            hedge_buy_qty: self.hedge_buy_qty,
            hedge_sell_qty: self.hedge_sell_qty,
            tracked_orders: self.order_legs.len(),
        }
    }

    pub fn weighted_inventory_price(&self) -> f64 {
        self.net_qty_queue.weighted_avg_price().unwrap_or(0.0)
    }

    pub fn net_qty(&self) -> f64 {
        self.net_qty
    }

    fn apply_net_qty_fill(&mut self, fill_ts: i64, signed_qty: f64, price: f64, leg: ArbOrphanLeg) {
        if signed_qty.abs() <= ARB_ORPHAN_EPS {
            return;
        }
        let before = self.net_qty;
        let result = self.net_qty_queue.apply_fill(fill_ts, signed_qty, price);
        self.net_qty = result.net_qty;
        debug!(
            "ArbOrphanNetQueue: strategy_id={} symbol={} leg={:?} fill_ts={} signed_qty={:.8} price={:.8} matched_qty={:.8} appended_qty={:.8} net_before={:.8} net_after={:.8} lots={}",
            self.strategy_id,
            self.symbol,
            leg,
            fill_ts,
            signed_qty,
            price,
            result.matched_qty,
            result.appended_qty,
            before,
            self.net_qty,
            self.net_qty_queue.len()
        );
    }

    fn record_terminal_fill(
        &mut self,
        leg: ArbOrphanLeg,
        fill_ts: i64,
        side: Side,
        base_qty: f64,
        price: f64,
    ) {
        if base_qty <= ARB_ORPHAN_EPS {
            return;
        }
        match (leg, side) {
            (ArbOrphanLeg::Open, Side::Buy) => self.open_buy_qty += base_qty,
            (ArbOrphanLeg::Open, Side::Sell) => self.open_sell_qty += base_qty,
            (ArbOrphanLeg::Hedge, Side::Buy) => self.hedge_buy_qty += base_qty,
            (ArbOrphanLeg::Hedge, Side::Sell) => self.hedge_sell_qty += base_qty,
        }
        let signed_qty = match leg {
            ArbOrphanLeg::Open => base_qty,
            ArbOrphanLeg::Hedge => -base_qty,
        };
        self.apply_net_qty_fill(fill_ts, signed_qty, price, leg);
    }

    fn finalize_terminal_order(&mut self, client_order_id: i64, event_time: i64, reason: &str) {
        let Some(leg) = self.order_legs.remove(&client_order_id) else {
            return;
        };
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let snapshot = {
            let mgr = order_mgr.borrow();
            mgr.get(client_order_id).map(|order| {
                (
                    order.venue,
                    order.symbol.clone(),
                    order.side,
                    order.cumulative_filled_quantity,
                    order.price,
                )
            })
        };
        let Some((venue, symbol, side, cumulative_qty, price)) = snapshot else {
            return;
        };
        let base_qty = MonitorChannel::instance().qty_to_base(venue, &symbol, cumulative_qty);
        self.record_terminal_fill(leg, event_time, side, base_qty, price);
        info!(
            "ArbOrphanStrategy: strategy_id={} finalized order client_order_id={} leg={:?} symbol={} venue={:?} side={:?} cumulative_qty={:.8} base_qty={:.8} reason={}",
            self.strategy_id,
            client_order_id,
            leg,
            symbol,
            venue,
            side,
            cumulative_qty,
            base_qty,
            reason
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
}

impl ForceCloseControl for ArbOrphanStrategy {
    fn set_force_close_mode(&mut self, _enabled: bool) {}

    fn is_force_close_mode(&self) -> bool {
        false
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
        self.order_legs.contains_key(&order_id)
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
        self.update_order_from_order_update(update);
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
        self.update_order_from_trade_update(trade);
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

    fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse) {}

    fn apply_query_engine_response(&mut self, _response: &dyn QueryEngineResponse) {}

    fn handle_period_clock(&mut self, _current_tp: i64) {
        let tracked: Vec<i64> = self.order_legs.keys().copied().collect();
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        for client_order_id in tracked {
            let terminal = order_mgr
                .borrow()
                .get(client_order_id)
                .is_some_and(|order| order.status.is_terminal());
            if terminal {
                self.finalize_terminal_order(
                    client_order_id,
                    get_timestamp_us(),
                    "terminal local order on period clock",
                );
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
