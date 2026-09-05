use log::{debug, warn};

use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus};
use crate::strategy::order_query_parser::parse_compact_order_query_resp;
use crate::strategy::ws_order_update::WsOrderUpdate;
use crate::strategy::Strategy;
use order_common::OrderUpdate;
use order_common::QueryEngineResponse;
use order_common::TradeEngineResponse;
use order_common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use order_common::{OrderQueryOrderUpdate, OrderQueryTradeUpdate};
use runtime_common::time_util::get_timestamp_us;
use std::cell::RefCell;
use std::collections::HashMap;
use trade_engine::query_parsers::compact_order::{
    is_order_query_not_found_marker, CompactOrderQueryResp,
};
use trade_engine::query_request::QueryRequestType;

const DEFAULT_FILL_EPSILON: f64 = 1e-12;

#[derive(Debug, Clone, Copy)]
struct DeferredHyperliquidTerminal {
    expected_cumulative_qty: f64,
    parsed: CompactOrderQueryResp,
    options: CompactOrderQueryApplyOptions,
}

thread_local! {
    static DEFERRED_HYPERLIQUID_TERMINALS: RefCell<HashMap<i64, DeferredHyperliquidTerminal>> =
        RefCell::new(HashMap::new());
}

#[derive(Debug, Clone, Copy)]
pub struct CompactOrderQueryApplyOptions {
    pub fallback_order_id: bool,
    pub fallback_event_time_to_now: bool,
    pub skip_live_create_update: bool,
    pub emit_filled_order_update: bool,
    pub emit_rejected_as_expired: bool,
    pub fill_epsilon: f64,
}

impl CompactOrderQueryApplyOptions {
    pub const fn open_reconcile() -> Self {
        Self {
            fallback_order_id: false,
            fallback_event_time_to_now: false,
            skip_live_create_update: false,
            emit_filled_order_update: false,
            emit_rejected_as_expired: false,
            fill_epsilon: DEFAULT_FILL_EPSILON,
        }
    }

    pub const fn orphan_reconcile(fill_epsilon: f64) -> Self {
        Self {
            fallback_order_id: true,
            fallback_event_time_to_now: true,
            skip_live_create_update: false,
            emit_filled_order_update: true,
            emit_rejected_as_expired: true,
            fill_epsilon,
        }
    }
}

pub(crate) fn is_hyperliquid_order_query(req_type: u32) -> bool {
    matches!(
        QueryRequestType::try_from(req_type),
        Ok(QueryRequestType::HyperliquidMarginQuery | QueryRequestType::HyperliquidUMQuery)
    )
}

fn suppress_hyperliquid_nonfactual_fill(
    req_type: u32,
    factual_cumulative_qty: f64,
    order_quantity: f64,
    parsed: &mut CompactOrderQueryResp,
    options: &mut CompactOrderQueryApplyOptions,
) -> Option<f64> {
    if is_hyperliquid_order_query(req_type) {
        let exchange_cumulative_qty = parsed.executed_qty;
        let terminal = matches!(
            parsed.status_u8,
            value if value == OrderExecutionStatus::Cancelled.to_u8()
                || value == OrderExecutionStatus::Filled.to_u8()
                || value == OrderExecutionStatus::Rejected.to_u8()
        );
        // orderStatus has no factual fill price. Retain the lifecycle/oid in
        // the compact response, but only userFills may advance fill quantity.
        parsed.executed_qty = factual_cumulative_qty;
        options.emit_filled_order_update = false;
        let epsilon = (order_quantity.abs() * 1.0e-9)
            .max(options.fill_epsilon)
            .max(DEFAULT_FILL_EPSILON);
        return (terminal && exchange_cumulative_qty > factual_cumulative_qty + epsilon)
            .then_some(exchange_cumulative_qty);
    }
    None
}

fn remember_deferred_hyperliquid_terminal(
    client_order_id: i64,
    expected_cumulative_qty: f64,
    parsed: CompactOrderQueryResp,
    options: CompactOrderQueryApplyOptions,
) {
    DEFERRED_HYPERLIQUID_TERMINALS.with(|cell| {
        let mut pending = cell.borrow_mut();
        let replace = pending
            .get(&client_order_id)
            .is_none_or(|current| parsed.update_time_ms >= current.parsed.update_time_ms);
        if replace {
            pending.insert(
                client_order_id,
                DeferredHyperliquidTerminal {
                    expected_cumulative_qty,
                    parsed,
                    options,
                },
            );
        }
    });
}

pub(crate) fn clear_deferred_hyperliquid_terminal(client_order_id: i64) {
    if client_order_id <= 0 {
        return;
    }
    DEFERRED_HYPERLIQUID_TERMINALS.with(|cell| {
        cell.borrow_mut().remove(&client_order_id);
    });
}

pub(crate) fn take_ready_deferred_hyperliquid_terminal(
    client_order_id: i64,
) -> Option<OrderQueryOrderUpdate> {
    let pending =
        DEFERRED_HYPERLIQUID_TERMINALS.with(|cell| cell.borrow().get(&client_order_id).copied())?;
    let Some(order_mgr) = MonitorChannel::try_order_manager() else {
        return None;
    };
    let Some(order) = order_mgr.borrow().get(client_order_id) else {
        clear_deferred_hyperliquid_terminal(client_order_id);
        return None;
    };
    let epsilon = (order.quantity.abs() * 1.0e-9)
        .max(pending.options.fill_epsilon)
        .max(DEFAULT_FILL_EPSILON);
    if order.cumulative_filled_quantity + epsilon < pending.expected_cumulative_qty {
        return None;
    }

    clear_deferred_hyperliquid_terminal(client_order_id);
    if order.status.is_terminal()
        || pending.parsed.status_u8 == OrderExecutionStatus::Filled.to_u8()
    {
        return None;
    }
    let (status, execution_type) =
        if pending.parsed.status_u8 == OrderExecutionStatus::Cancelled.to_u8() {
            (OrderStatus::Canceled, ExecutionType::Canceled)
        } else if pending.parsed.status_u8 == OrderExecutionStatus::Rejected.to_u8()
            && pending.options.emit_rejected_as_expired
        {
            (OrderStatus::Expired, ExecutionType::Rejected)
        } else {
            return None;
        };
    let event_time_us = pending.parsed.update_time_ms.saturating_mul(1_000);
    let event_time_us = if event_time_us > 0 {
        event_time_us
    } else if pending.options.fallback_event_time_to_now {
        get_timestamp_us()
    } else {
        event_time_us
    };
    let order_id = if pending.parsed.order_id > 0 || !pending.options.fallback_order_id {
        pending.parsed.order_id
    } else {
        order.exchange_order_id.unwrap_or(order.client_order_id)
    };
    let tif = TimeInForce::from_u8(pending.parsed.time_in_force_u8).unwrap_or(TimeInForce::GTC);
    Some(OrderQueryOrderUpdate::new(
        &order,
        order_id,
        event_time_us,
        status,
        execution_type,
        order.cumulative_filled_quantity,
        tif,
    ))
}

pub fn apply_trade_response_as_update(
    strategy: &mut dyn Strategy,
    response: &dyn TradeEngineResponse,
) -> bool {
    if !response.is_request_success()
        || !WsOrderUpdate::supports_trade_response_req_type(response.req_type())
    {
        return false;
    }

    let client_order_id = response.client_order_id();
    if !strategy.is_strategy_order(client_order_id) {
        return false;
    }

    let Some(order_mgr) = MonitorChannel::try_order_manager() else {
        return false;
    };
    let Some(order_snapshot) = order_mgr.borrow().get(client_order_id) else {
        return false;
    };

    let Some(update) = WsOrderUpdate::from_trade_response(response, &order_snapshot) else {
        return false;
    };

    if matches!(
        order_snapshot.venue,
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures
    ) {
        if matches!(update.status(), OrderStatus::New | OrderStatus::Canceled) {
            strategy.apply_order_update(&update);
        } else {
            debug!(
                "ResponseReconcile: strategy_id={} skip non-NEW/CANCELED binance ws response: venue={:?} client_order_id={} status={:?}",
                strategy.get_id(),
                order_snapshot.venue,
                client_order_id,
                update.status()
            );
        }
        return true;
    }

    if matches!(
        update.status(),
        OrderStatus::PartiallyFilled | OrderStatus::Filled
    ) {
        let trade = OrderQueryTradeUpdate::new(
            &order_snapshot,
            update.order_id(),
            update.event_time(),
            update.cumulative_filled_quantity(),
            response.response_price(),
            Some(update.status()),
            update.time_in_force(),
        );
        strategy.apply_trade_update(&trade);
    } else {
        strategy.apply_order_update(&update);
    }
    true
}

pub fn apply_query_response_as_updates(
    strategy: &mut dyn Strategy,
    response: &dyn QueryEngineResponse,
) -> bool {
    let client_order_id = response.client_query_id();
    if !strategy.is_strategy_order(client_order_id) {
        return false;
    }

    let body = response.body_bytes().as_ref();
    let actual_len = body
        .iter()
        .rposition(|&b| b != 0)
        .map(|pos| pos + 1)
        .unwrap_or(0);
    if actual_len == 0 {
        strategy.reset_order_query_not_found(client_order_id);
        return false;
    }
    if actual_len == 1 && body[0] == b'E' {
        strategy.reset_order_query_not_found(client_order_id);
        return false;
    }

    if is_order_query_not_found_marker(&body[..actual_len]) {
        strategy.record_order_query_not_found(client_order_id);
        debug!(
            "ResponseReconcile: strategy_id={} order query not found recorded client_order_id={}",
            strategy.get_id(),
            client_order_id
        );
        return true;
    }
    strategy.reset_order_query_not_found(client_order_id);

    let Some(order_mgr) = MonitorChannel::try_order_manager() else {
        return false;
    };
    let Some(order) = order_mgr.borrow().get(client_order_id) else {
        return false;
    };

    let mut options = if strategy
        .as_any()
        .is::<crate::strategy::orphan_order_strategy::OrphanOrderStrategy>()
        || strategy
            .as_any()
            .is::<crate::strategy::hedge_orphan_order_strategy::HedgeOrphanOrderStrategy>()
    {
        CompactOrderQueryApplyOptions::orphan_reconcile(DEFAULT_FILL_EPSILON)
    } else {
        CompactOrderQueryApplyOptions::open_reconcile()
    };

    let Some(mut parsed) = parse_compact_order_query_resp(response.body_bytes()) else {
        return false;
    };
    let terminal_waits_for_fills = suppress_hyperliquid_nonfactual_fill(
        response.req_type(),
        order.cumulative_filled_quantity,
        order.quantity,
        &mut parsed,
        &mut options,
    );
    if let Some(expected_cumulative_qty) = terminal_waits_for_fills {
        // Keep the local order and its query watchdog alive. The private
        // userFills stream owns factual quantities/prices; its deferred
        // orderUpdates terminal will close the lifecycle after catching up.
        // If that stream is unavailable, the existing watchdog hands the
        // order to orphan reconciliation instead of deleting it early.
        warn!(
            "ResponseReconcile: defer Hyperliquid terminal until factual userFills catch up: client_order_id={} local_cumulative={} expected_cumulative={}",
            client_order_id, order.cumulative_filled_quantity, expected_cumulative_qty
        );
        remember_deferred_hyperliquid_terminal(
            client_order_id,
            expected_cumulative_qty,
            parsed,
            options,
        );
        return true;
    }
    if is_hyperliquid_order_query(response.req_type()) {
        clear_deferred_hyperliquid_terminal(client_order_id);
    }

    apply_compact_order_query_updates(strategy, &order, parsed, options)
}

pub fn apply_compact_order_query_updates(
    strategy: &mut dyn Strategy,
    order: &Order,
    parsed: CompactOrderQueryResp,
    options: CompactOrderQueryApplyOptions,
) -> bool {
    let mut applied = false;
    let event_time_us = parsed.update_time_ms.saturating_mul(1_000);
    let event_time_us = if event_time_us > 0 {
        event_time_us
    } else if options.fallback_event_time_to_now {
        get_timestamp_us()
    } else {
        event_time_us
    };
    let order_id = if parsed.order_id > 0 || !options.fallback_order_id {
        parsed.order_id
    } else {
        order.exchange_order_id.unwrap_or(order.client_order_id)
    };
    let tif = TimeInForce::from_u8(parsed.time_in_force_u8).unwrap_or(TimeInForce::GTC);

    if parsed.executed_qty > order.cumulative_filled_quantity + options.fill_epsilon {
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
        strategy.apply_trade_update(&trade);
        applied = true;
    }

    let status_u8 = parsed.status_u8;
    if status_u8 == OrderExecutionStatus::Create.to_u8() {
        let already_live = order.status == OrderExecutionStatus::Create
            && order.exchange_order_id.is_some_and(|id| id == order_id);
        if !options.skip_live_create_update || !already_live {
            let update = OrderQueryOrderUpdate::new(
                order,
                order_id,
                event_time_us,
                OrderStatus::New,
                ExecutionType::New,
                parsed.executed_qty,
                tif,
            );
            strategy.apply_order_update(&update);
            applied = true;
        }
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
        strategy.apply_order_update(&update);
        applied = true;
    } else if status_u8 == OrderExecutionStatus::Filled.to_u8() && options.emit_filled_order_update
    {
        let update = OrderQueryOrderUpdate::new(
            order,
            order_id,
            event_time_us,
            OrderStatus::Filled,
            ExecutionType::Trade,
            parsed.executed_qty,
            tif,
        );
        strategy.apply_order_update(&update);
        applied = true;
    } else if status_u8 == OrderExecutionStatus::Rejected.to_u8()
        && options.emit_rejected_as_expired
    {
        let update = OrderQueryOrderUpdate::new(
            order,
            order_id,
            event_time_us,
            OrderStatus::Expired,
            ExecutionType::Rejected,
            parsed.executed_qty,
            tif,
        );
        strategy.apply_order_update(&update);
        applied = true;
    }

    applied
}

#[cfg(test)]
mod tests {
    use super::{
        apply_query_response_as_updates, suppress_hyperliquid_nonfactual_fill,
        CompactOrderQueryApplyOptions,
    };
    use crate::strategy::Strategy;
    use bytes::Bytes;
    use order_common::OrderUpdate;
    use order_common::QueryEngineResponseMessage;
    use order_common::TradeEngineResponse;
    use order_common::TradeUpdate;
    use signal_common::trade_signal::TradeSignal;
    use std::any::Any;
    use trade_engine::query_parsers::compact_order::{
        CompactOrderQueryResp, ORDER_QUERY_NOT_FOUND_MARKER,
    };
    use trade_engine::query_request::QueryRequestType;

    struct RecordingStrategy {
        strategy_id: i32,
        client_order_id: i64,
        order_updates: usize,
        trade_updates: usize,
        query_not_found: usize,
        query_not_found_resets: usize,
    }

    impl RecordingStrategy {
        fn new(strategy_id: i32, client_order_id: i64) -> Self {
            Self {
                strategy_id,
                client_order_id,
                order_updates: 0,
                trade_updates: 0,
                query_not_found: 0,
                query_not_found_resets: 0,
            }
        }
    }

    impl Strategy for RecordingStrategy {
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
            order_id == self.client_order_id
        }

        fn handle_signal(&mut self, _signal: &TradeSignal) {}

        fn apply_order_update(&mut self, _update: &dyn OrderUpdate) {
            self.order_updates += 1;
        }

        fn apply_trade_update(&mut self, _trade: &dyn TradeUpdate) {
            self.trade_updates += 1;
        }

        fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse) {}

        fn record_order_query_not_found(&mut self, client_order_id: i64) {
            assert_eq!(client_order_id, self.client_order_id);
            self.query_not_found += 1;
        }

        fn reset_order_query_not_found(&mut self, client_order_id: i64) {
            assert_eq!(client_order_id, self.client_order_id);
            self.query_not_found_resets += 1;
        }

        fn handle_period_clock(&mut self, _current_tp: i64) {}

        fn is_active(&self) -> bool {
            true
        }

        fn symbol(&self) -> Option<&str> {
            None
        }
    }

    #[test]
    fn query_not_found_marker_is_recorded_without_direct_terminal_update() {
        let client_order_id = 1987641311888408577;
        let mut strategy = RecordingStrategy::new(462783819, client_order_id);
        let response = QueryEngineResponseMessage::new(
            0,
            client_order_id,
            Bytes::from_static(ORDER_QUERY_NOT_FOUND_MARKER),
        );

        assert!(apply_query_response_as_updates(&mut strategy, &response));
        assert_eq!(strategy.order_updates, 0);
        assert_eq!(strategy.trade_updates, 0);
        assert_eq!(strategy.query_not_found, 1);
        assert_eq!(strategy.query_not_found_resets, 0);
    }

    #[test]
    fn query_error_resets_consecutive_not_found_evidence() {
        let client_order_id = 1987641311888408577;
        let mut strategy = RecordingStrategy::new(462783819, client_order_id);
        let response =
            QueryEngineResponseMessage::new(0, client_order_id, Bytes::from_static(b"E"));

        assert!(!apply_query_response_as_updates(&mut strategy, &response));
        assert_eq!(strategy.query_not_found, 0);
        assert_eq!(strategy.query_not_found_resets, 1);
    }

    #[test]
    fn hyperliquid_order_status_cannot_synthesize_a_fill_from_limit_price() {
        let mut parsed = CompactOrderQueryResp {
            executed_qty: 1.5,
            order_id: 99,
            status_u8: 3,
            update_time_ms: 123,
            time_in_force_u8: 1,
            response_price: 42_000.0,
        };
        let mut options = CompactOrderQueryApplyOptions::orphan_reconcile(1e-12);
        let waits_for_fills = suppress_hyperliquid_nonfactual_fill(
            QueryRequestType::HyperliquidUMQuery as u32,
            0.25,
            2.0,
            &mut parsed,
            &mut options,
        );
        assert_eq!(waits_for_fills, Some(1.5));
        assert_eq!(parsed.executed_qty, 0.25);
        assert!(!options.emit_filled_order_update);
        assert_eq!(parsed.response_price, 42_000.0);
    }

    #[test]
    fn hyperliquid_terminal_barrier_uses_order_relative_float_tolerance() {
        let expected = 1_215_380.683_655_f64;
        let factual = 939_179.340_687_f64 + 276_201.342_968_f64;
        assert!(expected > factual);
        assert!(expected - factual > 1.0e-12);
        let mut parsed = CompactOrderQueryResp {
            executed_qty: expected,
            order_id: 99,
            status_u8: 3,
            update_time_ms: 123,
            time_in_force_u8: 1,
            response_price: 42_000.0,
        };
        let mut options = CompactOrderQueryApplyOptions::orphan_reconcile(1.0e-12);
        assert_eq!(
            suppress_hyperliquid_nonfactual_fill(
                QueryRequestType::HyperliquidUMQuery as u32,
                factual,
                1_449_911.428_021,
                &mut parsed,
                &mut options,
            ),
            None
        );
        assert_eq!(parsed.executed_qty, factual);
    }
}
