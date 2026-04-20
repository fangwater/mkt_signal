//! 为 Binance basic 订单消息实现统一的 OrderUpdate / TradeUpdate / TradeUpdateLite 接口

use crate::common::basic_account_msg::{BinanceBasicOrderMsg, BinanceTradeLiteMsg};
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use crate::strategy::trade_update_lite::TradeUpdateLite;

fn map_execution_type(code: u8) -> ExecutionType {
    ExecutionType::from_u8(code).unwrap_or(ExecutionType::New)
}

fn map_order_status(code: u8) -> OrderStatus {
    OrderStatus::from_u8(code).unwrap_or(OrderStatus::New)
}

fn map_trading_venue(code: u8) -> TradingVenue {
    match code {
        BinanceBasicOrderMsg::VENUE_UM => TradingVenue::BinanceFutures,
        _ => TradingVenue::BinanceMargin,
    }
}

impl OrderUpdate for BinanceBasicOrderMsg {
    fn event_time(&self) -> i64 {
        self.event_time.saturating_mul(1_000)
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
        Side::from_u8(self.side).unwrap_or(Side::Buy)
    }

    fn order_type(&self) -> OrderType {
        OrderType::from_u8(self.order_type).unwrap_or(OrderType::Limit)
    }

    fn time_in_force(&self) -> TimeInForce {
        TimeInForce::from_u8(self.time_in_force).unwrap_or(TimeInForce::GTC)
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
        map_order_status(self.order_status)
    }

    fn execution_type(&self) -> ExecutionType {
        map_execution_type(self.execution_type)
    }

    fn trading_venue(&self) -> TradingVenue {
        map_trading_venue(self.venue)
    }
}

impl TradeUpdate for BinanceBasicOrderMsg {
    fn event_time(&self) -> i64 {
        self.event_time.saturating_mul(1_000)
    }

    fn trade_time(&self) -> i64 {
        self.trade_time.saturating_mul(1_000)
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
        Side::from_u8(self.side).unwrap_or(Side::Buy)
    }

    fn price(&self) -> f64 {
        self.last_executed_price
    }

    fn is_maker(&self) -> bool {
        self.is_maker != 0
    }

    fn trading_venue(&self) -> TradingVenue {
        map_trading_venue(self.venue)
    }

    fn cumulative_filled_quantity(&self) -> f64 {
        self.cumulative_filled_quantity
    }

    fn order_status(&self) -> Option<OrderStatus> {
        Some(map_order_status(self.order_status))
    }
}

impl TradeUpdateLite for BinanceTradeLiteMsg {
    fn event_time(&self) -> i64 {
        self.event_time.saturating_mul(1_000)
    }

    fn trade_time(&self) -> i64 {
        self.trade_time.saturating_mul(1_000)
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

    fn trade_id(&self) -> i64 {
        self.trade_id
    }

    fn side(&self) -> Side {
        Side::from_u8(self.side).unwrap_or(Side::Buy)
    }

    fn price(&self) -> f64 {
        self.last_executed_price
    }

    fn last_filled_quantity(&self) -> f64 {
        self.last_executed_quantity
    }

    fn is_maker(&self) -> bool {
        self.is_maker != 0
    }

    fn trading_venue(&self) -> TradingVenue {
        map_trading_venue(self.venue)
    }
}
