use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::order_manager::{Order, OrderExecutionStatus, OrderType, Side};
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::trade_engine::trade_request::TradeRequestType;

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
            Ok(
                TradeRequestType::BinanceWsNewUMOrder
                    | TradeRequestType::BinanceWsCancelUMOrder
                    | TradeRequestType::BinanceWsNewMarginOrder
                    | TradeRequestType::BinanceWsCancelMarginOrder
            )
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
            TradingVenue::BinanceFutures => TimeInForce::GTX,
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
