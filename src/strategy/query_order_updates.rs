use crate::pre_trade::order_manager::{Order, OrderType, Side};
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;

pub struct OrderQueryOrderUpdate {
    event_time_us: i64,
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
    venue: TradingVenue,
}

impl OrderQueryOrderUpdate {
    pub fn new(
        order: &Order,
        order_id: i64,
        event_time_us: i64,
        status: OrderStatus,
        execution_type: ExecutionType,
        cumulative_filled_quantity: f64,
        time_in_force: TimeInForce,
    ) -> Self {
        Self {
            event_time_us,
            symbol: order.symbol.clone(),
            order_id,
            client_order_id: order.client_order_id,
            side: order.side,
            order_type: order.order_type,
            time_in_force,
            price: order.price,
            quantity: order.quantity,
            cumulative_filled_quantity,
            status,
            execution_type,
            venue: order.venue,
        }
    }
}

impl OrderUpdate for OrderQueryOrderUpdate {
    fn event_time(&self) -> i64 {
        self.event_time_us
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
        self.venue
    }
}

pub struct OrderQueryTradeUpdate {
    event_time_us: i64,
    trade_time_us: i64,
    symbol: String,
    trade_id: i64,
    order_id: i64,
    client_order_id: i64,
    side: Side,
    price: f64,
    cumulative_filled_quantity: f64,
    is_maker: bool,
    venue: TradingVenue,
    order_status: Option<OrderStatus>,
}

impl OrderQueryTradeUpdate {
    pub fn new(
        order: &Order,
        order_id: i64,
        trade_id: i64,
        event_time_us: i64,
        cumulative_filled_quantity: f64,
        order_status: Option<OrderStatus>,
        time_in_force: TimeInForce,
    ) -> Self {
        let is_maker = infer_query_is_maker(order, time_in_force);
        Self {
            event_time_us,
            trade_time_us: event_time_us,
            symbol: order.symbol.clone(),
            trade_id,
            order_id,
            client_order_id: order.client_order_id,
            side: order.side,
            price: order.price,
            cumulative_filled_quantity,
            is_maker,
            venue: order.venue,
            order_status,
        }
    }
}

fn infer_query_is_maker(order: &Order, time_in_force: TimeInForce) -> bool {
    match order.venue {
        TradingVenue::BinanceMargin
        | TradingVenue::BinanceFutures
        | TradingVenue::OkexMargin
        | TradingVenue::OkexFutures => match time_in_force {
            TimeInForce::GTX => true,
            TimeInForce::GTC => order.price > 0.0,
            _ => false,
        },
        _ => false,
    }
}

impl TradeUpdate for OrderQueryTradeUpdate {
    fn event_time(&self) -> i64 {
        self.event_time_us
    }

    fn trade_time(&self) -> i64 {
        self.trade_time_us
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn trade_id(&self) -> i64 {
        self.trade_id
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

    fn price(&self) -> f64 {
        self.price
    }

    fn is_maker(&self) -> bool {
        self.is_maker
    }

    fn trading_venue(&self) -> TradingVenue {
        self.venue
    }

    fn cumulative_filled_quantity(&self) -> f64 {
        self.cumulative_filled_quantity
    }

    fn order_status(&self) -> Option<OrderStatus> {
        self.order_status
    }
}
