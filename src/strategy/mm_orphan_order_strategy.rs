use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::pre_trade::{PersistChannel, QueryEngHub, TradeEngHub};
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::manager::{
    ForceCloseControl, MmOrphanHandoff, MmOrphanSourceKind, Strategy,
};
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
use std::collections::{HashMap, HashSet};

const MM_ORPHAN_QUERY_BASE_TICKS: u32 = 25;
const MM_ORPHAN_QUERY_MAX_TICKS: u32 = 3_200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MmOrphanOrderOwner {
    pub source_strategy_id: i32,
    pub source_kind: MmOrphanSourceKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MmOrphanQueryState {
    query_count: u8,
    ticks_until_next_query: u32,
}

pub struct MmOrphanOrderStrategy {
    strategy_id: i32,
    symbol: String,
    order_ids: HashSet<i64>,
    order_owners: HashMap<i64, MmOrphanOrderOwner>,
    query_states: HashMap<i64, MmOrphanQueryState>,
    pending_query_targets: HashMap<i64, i64>,
    query_seq: u32,
    active: bool,
}

impl MmOrphanOrderStrategy {
    pub fn new(strategy_id: i32, symbol: impl Into<String>) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            order_ids: HashSet::new(),
            order_owners: HashMap::new(),
            query_states: HashMap::new(),
            pending_query_targets: HashMap::new(),
            query_seq: 0,
            active: true,
        }
    }

    pub fn len(&self) -> usize {
        self.order_ids.len()
    }

    fn initial_query_state() -> MmOrphanQueryState {
        MmOrphanQueryState {
            query_count: 0,
            ticks_until_next_query: MM_ORPHAN_QUERY_BASE_TICKS,
        }
    }

    fn next_query_ticks(query_count: u8) -> u32 {
        let multiplier = 1_u32
            .checked_shl(query_count.min(31) as u32)
            .unwrap_or(u32::MAX);
        MM_ORPHAN_QUERY_BASE_TICKS
            .saturating_mul(multiplier)
            .min(MM_ORPHAN_QUERY_MAX_TICKS)
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

    fn adopt_order_id_inner(&mut self, handoff: &MmOrphanHandoff) -> bool {
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
        self.track_order_id(handoff.client_order_id);
        self.ensure_query_state(handoff.client_order_id);
        self.order_owners.insert(
            handoff.client_order_id,
            MmOrphanOrderOwner {
                source_strategy_id: handoff.source_strategy_id,
                source_kind: handoff.source_kind,
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
        let removed = self.order_ids.remove(&client_order_id);
        if removed {
            self.order_owners.remove(&client_order_id);
            self.query_states.remove(&client_order_id);
            self.pending_query_targets
                .retain(|_, tracked_order_id| *tracked_order_id != client_order_id);
            info!(
                "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} forgot order_id client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
        }
        removed
    }

    fn track_order_id(&mut self, client_order_id: i64) {
        self.order_ids.insert(client_order_id);
        self.ensure_query_state(client_order_id);
    }

    pub fn order_owner(&self, client_order_id: i64) -> Option<MmOrphanOrderOwner> {
        self.order_owners.get(&client_order_id).copied()
    }

    fn finalize_terminal_order(&mut self, client_order_id: i64, event_time: i64, reason: &str) {
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

        if let Some((venue, symbol, side, cumulative_qty, price)) = snapshot {
            if cumulative_qty > 0.0 {
                let base_qty = MonitorChannel::instance().qty_to_base(venue, &symbol, cumulative_qty);
                if base_qty > 0.0 {
                    let (signed_qty, buy_qty, sell_qty) = match side {
                        crate::pre_trade::order_manager::Side::Buy => (base_qty, base_qty, 0.0),
                        crate::pre_trade::order_manager::Side::Sell => (-base_qty, 0.0, base_qty),
                    };
                    let _ = MonitorChannel::instance()
                        .strategy_mgr()
                        .borrow_mut()
                        .record_mm_hedge_fill(
                            &normalize_symbol_for_internal(&symbol),
                            signed_qty,
                            buy_qty,
                            sell_qty,
                            event_time,
                            price,
                        );
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
        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} send_order_query but local order missing client_order_id={}",
                self.strategy_id, client_order_id
            );
            return false;
        };
        let request_query_id = self.next_query_request_id();
        match build_order_query_request(&order, request_query_id, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request(exchange.as_str(), &req_bytes)
                {
                    warn!(
                        "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} publish query failed client_order_id={} request_query_id={} err={:#}",
                        self.strategy_id, client_order_id, request_query_id, err
                    );
                    return false;
                }
                self.pending_query_targets
                    .insert(request_query_id, client_order_id);
                info!(
                    "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} query sent client_order_id={} request_query_id={}",
                    self.strategy_id, client_order_id, request_query_id
                );
                true
            }
            Err(err) => {
                warn!(
                    "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} build query failed client_order_id={} err={}",
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
        let order_id = if parsed.order_id > 0 {
            parsed.order_id
        } else {
            order.exchange_order_id.unwrap_or(order.client_order_id)
        };
        let tif = TimeInForce::from_u8(parsed.time_in_force_u8).unwrap_or(TimeInForce::GTC);

        if parsed.executed_qty > order.cumulative_filled_quantity + 1e-12 {
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

    fn persist_order_update(update: &dyn OrderUpdate) {
        PersistChannel::with(|ch| ch.publish_order_update_unmatched(update));
    }

    fn persist_trade_update(trade: &dyn TradeUpdate) {
        PersistChannel::with(|ch| ch.publish_trade_update_unmatched(trade));
    }
}

impl ForceCloseControl for MmOrphanOrderStrategy {
    fn set_force_close_mode(&mut self, _enabled: bool) {}

    fn is_force_close_mode(&self) -> bool {
        false
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
        self.order_ids.contains(&order_id)
    }

    fn handle_signal(&mut self, _signal: &TradeSignal) {}

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        if normalize_symbol_for_internal(update.symbol()) != self.symbol {
            return;
        }
        let client_order_id = update.client_order_id();
        self.track_order_id(client_order_id);
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
        Self::persist_order_update(update);
        if matches!(
            update.status(),
            OrderStatus::Canceled | OrderStatus::Filled | OrderStatus::Expired | OrderStatus::ExpiredInMatch
        ) {
            self.finalize_terminal_order(client_order_id, update.event_time(), "terminal order update");
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
        Self::persist_trade_update(trade);
        if trade.order_status().is_some_and(|status| {
            matches!(
                status,
                OrderStatus::Canceled
                    | OrderStatus::Filled
                    | OrderStatus::Expired
                    | OrderStatus::ExpiredInMatch
            )
        }) {
            self.finalize_terminal_order(client_order_id, trade.event_time(), "terminal trade update");
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

    fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse) {}

    fn apply_query_engine_response(&mut self, response: &dyn QueryEngineResponse) {
        let request_query_id = response.client_query_id();
        let Some(client_order_id) = self.pending_query_targets.remove(&request_query_id) else {
            return;
        };

        let order_mgr = MonitorChannel::instance().order_manager();
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
            return;
        }
        if actual_len == 1 && body[0] == b'E' {
            return;
        }

        let parsed = parse_compact_order_query_resp(response.body_bytes());
        if let Some(parsed) = parsed {
            self.apply_parsed_order_query_updates(&order, parsed);
        }
    }

    fn adopt_order_id(&mut self, handoff: &MmOrphanHandoff) -> bool {
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
                drop(order);
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
            drop(order);
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
