use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{OrderExecutionStatus, Side};
use crate::pre_trade::{QueryEngHub, TradeEngHub};
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce};
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::manager::{ArbOrphanHandoff, ForceCloseControl, Strategy};
use crate::strategy::net_qty_queue::NetQtyQueue;
use crate::strategy::order_query_builder::build_order_query_request;
use crate::strategy::order_query_parser::parse_compact_order_query_resp;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::query_engine_response::QueryEngineResponse;
use crate::strategy::query_order_updates::{OrderQueryOrderUpdate, OrderQueryTradeUpdate};
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::strategy::trade_update::TradeUpdate;
use crate::trade_engine::query_parsers::compact_order::{
    is_order_query_not_found_marker, CompactOrderQueryResp,
};
use log::{debug, info, warn};
use std::any::Any;
use std::collections::HashMap;

const ARB_ORPHAN_EPS: f64 = 1e-12;
const ARB_ORPHAN_QUERY_BASE_TICKS: u32 = 25;
const ARB_ORPHAN_QUERY_MAX_TICKS: u32 = 3_200;

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
    cancel_intents: HashMap<i64, bool>,
    query_states: HashMap<i64, ArbOrphanQueryState>,
    pending_query_targets: HashMap<i64, i64>,
    max_query_attempts: HashMap<i64, u8>,
    query_seq: u32,
    net_qty_queue: NetQtyQueue,
    net_qty: f64,
    open_buy_qty: f64,
    open_sell_qty: f64,
    hedge_buy_qty: f64,
    hedge_sell_qty: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ArbOrphanQueryState {
    query_count: u8,
    ticks_until_next_query: u32,
}

impl ArbOrphanStrategy {
    pub fn new(strategy_id: i32, symbol: impl Into<String>) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            active: true,
            order_legs: HashMap::new(),
            cancel_intents: HashMap::new(),
            query_states: HashMap::new(),
            pending_query_targets: HashMap::new(),
            max_query_attempts: HashMap::new(),
            query_seq: 0,
            net_qty_queue: NetQtyQueue::new(),
            net_qty: 0.0,
            open_buy_qty: 0.0,
            open_sell_qty: 0.0,
            hedge_buy_qty: 0.0,
            hedge_sell_qty: 0.0,
        }
    }

    fn initial_query_state() -> ArbOrphanQueryState {
        ArbOrphanQueryState {
            query_count: 0,
            ticks_until_next_query: 0,
        }
    }

    fn next_query_ticks(query_count: u8) -> u32 {
        let multiplier = 1_u32
            .checked_shl(query_count.min(31) as u32)
            .unwrap_or(u32::MAX);
        ARB_ORPHAN_QUERY_BASE_TICKS
            .saturating_mul(multiplier)
            .min(ARB_ORPHAN_QUERY_MAX_TICKS)
    }

    fn ensure_query_state(&mut self, client_order_id: i64) {
        self.query_states
            .entry(client_order_id)
            .or_insert_with(Self::initial_query_state);
    }

    fn next_query_request_id(&mut self) -> i64 {
        self.query_seq = self.query_seq.wrapping_add(1);
        if self.query_seq == 0 {
            self.query_seq = 1;
        }
        ((self.strategy_id as i64) << 32) | self.query_seq as i64
    }

    pub fn adopt_order_id(&mut self, handoff: &ArbOrphanHandoff) -> bool {
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

        self.order_legs.insert(handoff.client_order_id, handoff.leg);
        self.cancel_intents
            .insert(handoff.client_order_id, handoff.cancel_intent);
        self.ensure_query_state(handoff.client_order_id);
        if let Some(max_attempts) = handoff.max_query_attempts {
            self.max_query_attempts
                .insert(handoff.client_order_id, max_attempts);
        }
        info!(
            "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} adopted order symbol={} client_order_id={} venue={:?} status={:?} leg={:?} cancel_intent={} source_strategy_id={} reason={}",
            self.strategy_id,
            symbol,
            handoff.client_order_id,
            venue,
            status,
            handoff.leg,
            handoff.cancel_intent,
            handoff.source_strategy_id,
            handoff.reason
        );
        true
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
        self.ensure_query_state(client_order_id);
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

    fn forget_order_id(&mut self, client_order_id: i64, reason: &str) -> bool {
        let removed = self.order_legs.remove(&client_order_id).is_some();
        if removed {
            self.cancel_intents.remove(&client_order_id);
            self.query_states.remove(&client_order_id);
            self.max_query_attempts.remove(&client_order_id);
            self.pending_query_targets
                .retain(|_, tracked_order_id| *tracked_order_id != client_order_id);
            info!(
                "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} forgot order client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
        }
        removed
    }

    fn drop_unconfirmed_order(&mut self, client_order_id: i64, reason: &str) {
        if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let _ = order_mgr.borrow_mut().remove(client_order_id);
        }
        self.forget_order_id(client_order_id, reason);
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
        let signed_qty = match side {
            Side::Buy => base_qty,
            Side::Sell => -base_qty,
        };
        self.apply_net_qty_fill(fill_ts, signed_qty, price, leg);
    }

    fn finalize_terminal_order(&mut self, client_order_id: i64, event_time: i64, reason: &str) {
        let Some(leg) = self.order_legs.get(&client_order_id).copied() else {
            return;
        };
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            self.forget_order_id(client_order_id, reason);
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
            self.forget_order_id(client_order_id, reason);
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
        let _ = order_mgr.borrow_mut().remove(client_order_id);
        self.forget_order_id(client_order_id, reason);
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

    fn request_cancel_for_order(&mut self, client_order_id: i64, reason: &str) -> bool {
        if !self
            .cancel_intents
            .get(&client_order_id)
            .copied()
            .unwrap_or(false)
        {
            return false;
        }
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            return false;
        };
        if order.status.is_terminal() || order.status == OrderExecutionStatus::Commit {
            return false;
        }
        let exchange = order.venue.trade_engine_exchange();
        let cancel_bytes = match order.get_order_cancel_bytes() {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(
                    "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} build cancel failed client_order_id={} reason={} err={}",
                    self.strategy_id, client_order_id, reason, err
                );
                return false;
            }
        };
        match TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
            Ok(()) => {
                info!(
                    "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} sent cancel client_order_id={} exchange={} reason={}",
                    self.strategy_id, client_order_id, exchange, reason
                );
                true
            }
            Err(err) => {
                warn!(
                    "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} send cancel failed client_order_id={} exchange={} reason={} err={:#}",
                    self.strategy_id, client_order_id, exchange, reason, err
                );
                false
            }
        }
    }

    fn send_order_query(&mut self, client_order_id: i64) -> bool {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            self.forget_order_id(client_order_id, "query missing local order");
            return false;
        };
        let request_query_id = self.next_query_request_id();
        match build_order_query_request(&order, request_query_id, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request(exchange.as_str(), &req_bytes) {
                    warn!(
                        "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} publish query failed client_order_id={} request_query_id={} err={:#}",
                        self.strategy_id, client_order_id, request_query_id, err
                    );
                    return false;
                }
                self.pending_query_targets
                    .insert(request_query_id, client_order_id);
                info!(
                    "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} query sent client_order_id={} request_query_id={}",
                    self.strategy_id, client_order_id, request_query_id
                );
                true
            }
            Err(err) => {
                warn!(
                    "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} build query failed client_order_id={} err={}",
                    self.strategy_id, client_order_id, err
                );
                false
            }
        }
    }

    fn apply_parsed_order_query_updates(
        &mut self,
        order: &crate::pre_trade::order_manager::Order,
        parsed: CompactOrderQueryResp,
    ) {
        let event_time_us = parsed.update_time_ms.saturating_mul(1_000);
        let event_time_us = if event_time_us > 0 {
            event_time_us
        } else {
            get_timestamp_us()
        };
        let order_id = if parsed.order_id > 0 {
            parsed.order_id
        } else {
            order.exchange_order_id.unwrap_or(order.client_order_id)
        };
        let tif = TimeInForce::from_u8(parsed.time_in_force_u8).unwrap_or(TimeInForce::GTC);

        if parsed.executed_qty > order.cumulative_filled_quantity + ARB_ORPHAN_EPS {
            let trade_status = if parsed.status_u8 == OrderExecutionStatus::Filled.to_u8() {
                Some(OrderStatus::Filled)
            } else {
                Some(OrderStatus::PartiallyFilled)
            };
            let trade = OrderQueryTradeUpdate::new(
                order,
                order_id,
                event_time_us,
                parsed.executed_qty,
                Some(parsed.response_price),
                trade_status,
                tif,
            );
            <Self as Strategy>::apply_trade_update(self, &trade);
        }

        let status_u8 = parsed.status_u8;
        if status_u8 == OrderExecutionStatus::Create.to_u8() {
            let update = OrderQueryOrderUpdate::new(
                order,
                order_id,
                event_time_us,
                OrderStatus::New,
                ExecutionType::New,
                parsed.executed_qty,
                tif,
            );
            <Self as Strategy>::apply_order_update(self, &update);
        } else if status_u8 == OrderExecutionStatus::Cancelled.to_u8() {
            let update = OrderQueryOrderUpdate::new(
                order,
                order_id,
                event_time_us,
                OrderStatus::Canceled,
                ExecutionType::Canceled,
                parsed.executed_qty,
                tif,
            );
            <Self as Strategy>::apply_order_update(self, &update);
        } else if status_u8 == OrderExecutionStatus::Filled.to_u8() {
            let update = OrderQueryOrderUpdate::new(
                order,
                order_id,
                event_time_us,
                OrderStatus::Filled,
                ExecutionType::Trade,
                parsed.executed_qty,
                tif,
            );
            <Self as Strategy>::apply_order_update(self, &update);
        } else if status_u8 == OrderExecutionStatus::Rejected.to_u8() {
            let update = OrderQueryOrderUpdate::new(
                order,
                order_id,
                event_time_us,
                OrderStatus::Expired,
                ExecutionType::Rejected,
                parsed.executed_qty,
                tif,
            );
            <Self as Strategy>::apply_order_update(self, &update);
        }
    }

    fn handle_query_not_found_or_error(&mut self, client_order_id: i64, marker: &str) {
        if let Some(max_attempts) = self.max_query_attempts.get(&client_order_id).copied() {
            let attempts = self
                .query_states
                .get(&client_order_id)
                .map(|state| state.query_count)
                .unwrap_or(0);
            if attempts >= max_attempts {
                warn!(
                    "ArbOrphanStrategy: strategy_role=arb_orphan strategy_id={} query {} reached max attempts {}, drop unconfirmed order client_order_id={}",
                    self.strategy_id, marker, max_attempts, client_order_id
                );
                self.drop_unconfirmed_order(client_order_id, "unconfirmed query max attempts");
            }
        }
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
        } else {
            self.request_cancel_for_order(client_order_id, "non-terminal order update");
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
        } else {
            self.request_cancel_for_order(client_order_id, "non-terminal trade update");
        }
    }

    fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse) {}

    fn apply_query_engine_response(&mut self, response: &dyn QueryEngineResponse) {
        let request_query_id = response.client_query_id();
        let Some(client_order_id) = self.pending_query_targets.remove(&request_query_id) else {
            return;
        };

        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            self.forget_order_id(client_order_id, "query response missing order manager");
            return;
        };
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            self.forget_order_id(client_order_id, "query response missing local order");
            return;
        };

        let body = response.body_bytes().as_ref();
        let has_any_byte = body.iter().any(|&b| b != 0);
        if !has_any_byte {
            return;
        }

        let actual_len = body
            .iter()
            .rposition(|&b| b != 0)
            .map(|pos| pos + 1)
            .unwrap_or(0);
        if is_order_query_not_found_marker(&body[..actual_len]) {
            drop(order);
            self.handle_query_not_found_or_error(client_order_id, "not found marker");
            return;
        }
        if actual_len == 1 && body[0] == b'E' {
            drop(order);
            self.handle_query_not_found_or_error(client_order_id, "error marker (E)");
            return;
        }

        if let Some(parsed) = parse_compact_order_query_resp(response.body_bytes()) {
            self.apply_parsed_order_query_updates(&order, parsed);
        }
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        let tracked: Vec<i64> = self.order_legs.keys().copied().collect();
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
            let should_cancel = order.status != OrderExecutionStatus::Commit;
            drop(order);
            if should_cancel {
                self.request_cancel_for_order(client_order_id, "period clock");
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
