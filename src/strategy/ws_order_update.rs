use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderType, Side};
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use crate::strategy::manager::Strategy;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::trade_engine::trade_request::TradeRequestType;
use log::{info, warn};

#[derive(Debug, Clone)]
pub struct WsOrderUpdate {
    event_time: i64,
    symbol: String,
    order_id: i64,
    client_order_id: i64,
    side: Side,
    order_type: OrderType,
    time_in_force: TimeInForce,
    price: f64,
    quantity: f64,
    cumulative_filled_quantity: f64,
    status: OrderStatus,
    execution_type: ExecutionType,
    trading_venue: TradingVenue,
}

impl WsOrderUpdate {
    pub fn supports_trade_response_req_type(req_type: u32) -> bool {
        matches!(
            TradeRequestType::try_from(req_type),
            Ok(TradeRequestType::BinanceWsNewUMOrder
                | TradeRequestType::BinanceWsCancelUMOrder
                | TradeRequestType::BinanceWsNewMarginOrder
                | TradeRequestType::BinanceWsCancelMarginOrder
                | TradeRequestType::BybitNewMarginOrder
                | TradeRequestType::BybitNewUMOrder
                | TradeRequestType::OkexNewMarginOrder
                | TradeRequestType::OkexNewUMOrder
                | TradeRequestType::GateUnifiedNewOrder
                | TradeRequestType::GateFuturesNewOrder
                | TradeRequestType::BitgetNewMarginOrder
                | TradeRequestType::BitgetNewUMOrder
                | TradeRequestType::BitgetCancelMarginOrder
                | TradeRequestType::BitgetCancelUMOrder)
        )
    }

    pub fn new(
        event_time: i64,
        symbol: String,
        order_id: i64,
        client_order_id: i64,
        side: Side,
        order_type: OrderType,
        time_in_force: TimeInForce,
        price: f64,
        quantity: f64,
        cumulative_filled_quantity: f64,
        status: OrderStatus,
        trading_venue: TradingVenue,
    ) -> Self {
        let execution_type = execution_type_from_status(status);
        Self {
            event_time,
            symbol,
            order_id,
            client_order_id,
            side,
            order_type,
            time_in_force,
            price,
            quantity,
            cumulative_filled_quantity,
            status,
            execution_type,
            trading_venue,
        }
    }

    pub fn infer_time_in_force(venue: TradingVenue, order_type: OrderType) -> TimeInForce {
        if !order_type.is_limit() {
            return TimeInForce::GTC;
        }
        match venue {
            TradingVenue::BinanceFutures
            | TradingVenue::BybitMargin
            | TradingVenue::BybitFutures
            | TradingVenue::OkexMargin
            | TradingVenue::OkexFutures
            | TradingVenue::GateMargin
            | TradingVenue::GateFutures
            | TradingVenue::BitgetMargin
            | TradingVenue::BitgetFutures => TimeInForce::GTX,
            _ => TimeInForce::GTC,
        }
    }

    pub fn from_trade_response(response: &dyn TradeEngineResponse, order: &Order) -> Option<Self> {
        if !response.is_request_success() {
            return None;
        }
        if !Self::supports_trade_response_req_type(response.req_type()) {
            return None;
        }

        let status_u8 = response.order_status_u8()?;
        let status = OrderStatus::from_u8(status_u8)?;
        if is_duplicate_status(order.status, status) {
            return None;
        }

        let mut order_id = response.order_id().unwrap_or(0);
        if order_id <= 0 {
            if let Some(ex) = order.exchange_order_id {
                order_id = ex;
            }
        }

        let event_time = response
            .order_update_time()
            .and_then(|ts| ts.checked_mul(1_000))
            .filter(|ts| *ts > 0)
            .unwrap_or_else(get_timestamp_us);
        let executed_qty = response.executed_qty().unwrap_or(0.0);
        let time_in_force = Self::infer_time_in_force(order.venue, order.order_type);

        Some(Self::new(
            event_time,
            order.symbol.clone(),
            order_id,
            order.client_order_id,
            order.side,
            order.order_type,
            time_in_force,
            order.price,
            order.quantity,
            executed_qty,
            status,
            order.venue,
        ))
    }
}

fn execution_type_from_status(status: OrderStatus) -> ExecutionType {
    match status {
        OrderStatus::New => ExecutionType::New,
        OrderStatus::PartiallyFilled | OrderStatus::Filled => ExecutionType::Trade,
        OrderStatus::Canceled => ExecutionType::Canceled,
        OrderStatus::Expired | OrderStatus::ExpiredInMatch => ExecutionType::Expired,
    }
}

fn is_duplicate_status(current: OrderExecutionStatus, incoming: OrderStatus) -> bool {
    match incoming {
        OrderStatus::New => current == OrderExecutionStatus::Create,
        OrderStatus::Canceled => current == OrderExecutionStatus::Cancelled,
        OrderStatus::Filled => current == OrderExecutionStatus::Filled,
        OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
            current == OrderExecutionStatus::Rejected
        }
        OrderStatus::PartiallyFilled => false,
    }
}

impl OrderUpdate for WsOrderUpdate {
    fn event_time(&self) -> i64 {
        self.event_time
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn order_id(&self) -> i64 {
        self.order_id
    }

    fn client_order_id(&self) -> i64 {
        self.client_order_id
    }

    fn side(&self) -> Side {
        self.side
    }

    fn order_type(&self) -> OrderType {
        self.order_type
    }

    fn time_in_force(&self) -> TimeInForce {
        self.time_in_force
    }

    fn price(&self) -> f64 {
        self.price
    }

    fn quantity(&self) -> f64 {
        self.quantity
    }

    fn cumulative_filled_quantity(&self) -> f64 {
        self.cumulative_filled_quantity
    }

    fn status(&self) -> OrderStatus {
        self.status
    }

    fn execution_type(&self) -> ExecutionType {
        self.execution_type
    }

    fn trading_venue(&self) -> TradingVenue {
        self.trading_venue
    }
}

pub fn try_apply_ws_order_update_for_strategy<S: Strategy + ?Sized>(
    strategy: &mut S,
    response: &dyn TradeEngineResponse,
) -> bool {
    if !WsOrderUpdate::supports_trade_response_req_type(response.req_type()) {
        return false;
    }

    let client_order_id = response.client_order_id();
    if !strategy.is_strategy_order(client_order_id) {
        return false;
    }
    let order_mgr = MonitorChannel::instance().order_manager();
    let Some(order_snapshot) = order_mgr.borrow().get(client_order_id) else {
        warn!(
            "{}: strategy_id={} ws order update missing local order: client_order_id={}",
            ws_strategy_name::<S>(),
            strategy.get_id(),
            client_order_id
        );
        return false;
    };

    let Some(update) = WsOrderUpdate::from_trade_response(response, &order_snapshot) else {
        return false;
    };

    if matches!(update.status(), OrderStatus::New | OrderStatus::Canceled) {
        strategy.apply_order_update(&update);
    } else {
        info!(
            "{}: strategy_id={} skip non-NEW/CANCELED ws response: venue={:?} client_order_id={} status={:?}",
            ws_strategy_name::<S>(),
            strategy.get_id(),
            order_snapshot.venue,
            client_order_id,
            update.status()
        );
    }
    true
}

/// Shared prelude for trade-engine responses that still need strategy-specific failure handling.
///
/// Success responses may already carry enough order state to synthesize a websocket-style update;
/// if so, it is applied here. Plain success responses are intentionally ignored because normal
/// account/order streams or query reconciliation own the final state. The returned client order id
/// is therefore only for failed responses that belong to this strategy.
pub fn prepare_failed_trade_engine_response_for_strategy<S: Strategy + ?Sized>(
    strategy: &mut S,
    response: &dyn TradeEngineResponse,
) -> Option<i64> {
    if try_apply_ws_order_update_for_strategy(strategy, response) {
        return None;
    }
    if response.is_request_success() {
        return None;
    }

    let client_order_id = response.client_order_id();
    if !strategy.is_strategy_order(client_order_id) {
        return None;
    }

    Some(client_order_id)
}

fn ws_strategy_name<S: ?Sized>() -> &'static str {
    std::any::type_name::<S>()
        .rsplit("::")
        .next()
        .unwrap_or("Strategy")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bybit_cancel_response_does_not_drive_ws_order_update() {
        assert!(WsOrderUpdate::supports_trade_response_req_type(
            TradeRequestType::BybitNewUMOrder as u32
        ));
        assert!(WsOrderUpdate::supports_trade_response_req_type(
            TradeRequestType::BybitNewMarginOrder as u32
        ));

        assert!(!WsOrderUpdate::supports_trade_response_req_type(
            TradeRequestType::BybitCancelUMOrder as u32
        ));
        assert!(!WsOrderUpdate::supports_trade_response_req_type(
            TradeRequestType::BybitCancelMarginOrder as u32
        ));
    }

    #[test]
    fn binance_ws_cancel_response_still_drives_ws_order_update() {
        assert!(WsOrderUpdate::supports_trade_response_req_type(
            TradeRequestType::BinanceWsNewUMOrder as u32
        ));
        assert!(WsOrderUpdate::supports_trade_response_req_type(
            TradeRequestType::BinanceWsCancelUMOrder as u32
        ));
        assert!(WsOrderUpdate::supports_trade_response_req_type(
            TradeRequestType::BinanceWsNewMarginOrder as u32
        ));
        assert!(WsOrderUpdate::supports_trade_response_req_type(
            TradeRequestType::BinanceWsCancelMarginOrder as u32
        ));
    }
}
