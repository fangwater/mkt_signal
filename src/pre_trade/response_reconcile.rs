use log::debug;

use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus};
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use crate::strategy::order_query_parser::parse_compact_order_query_resp;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::query_engine_response::QueryEngineResponse;
use crate::strategy::query_order_updates::{OrderQueryOrderUpdate, OrderQueryTradeUpdate};
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::strategy::ws_order_update::WsOrderUpdate;
use crate::strategy::Strategy;
use crate::trade_engine::query_parsers::compact_order::CompactOrderQueryResp;

const DEFAULT_FILL_EPSILON: f64 = 1e-12;

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
    if !body.iter().any(|&b| b != 0) {
        return false;
    }
    let actual_len = body
        .iter()
        .rposition(|&b| b != 0)
        .map(|pos| pos + 1)
        .unwrap_or(0);
    if actual_len == 1 && matches!(body[0], b'E' | b'N') {
        return false;
    }

    let Some(parsed) = parse_compact_order_query_resp(response.body_bytes()) else {
        return false;
    };

    let Some(order_mgr) = MonitorChannel::try_order_manager() else {
        return false;
    };
    let Some(order) = order_mgr.borrow().get(client_order_id) else {
        return false;
    };

    let options = if strategy
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
