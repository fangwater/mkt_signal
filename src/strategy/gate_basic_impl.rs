//! 为 Gate basic 订单消息实现统一的 OrderUpdate / TradeUpdate 接口

use crate::common::basic_account_msg::GateBasicOrderMsg;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;

fn map_execution_type(code: u8) -> ExecutionType {
    ExecutionType::from_u8(code).unwrap_or(ExecutionType::New)
}

fn map_order_status(code: u8) -> OrderStatus {
    OrderStatus::from_u8(code).unwrap_or(OrderStatus::New)
}

fn map_trading_venue(code: u8) -> TradingVenue {
    match code {
        GateBasicOrderMsg::VENUE_FUTURES => TradingVenue::GateFutures,
        _ => TradingVenue::GateMargin,
    }
}

impl OrderUpdate for GateBasicOrderMsg {
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

    fn last_time_executed_qty(&self) -> f64 {
        // Gate 订单推送里没有 last_fill_qty，这里沿用 OKX 的处理：返回累计成交量
        self.cumulative_filled_quantity
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

    fn last_executed_price(&self) -> Option<f64> {
        if self.last_executed_price > 0.0 {
            Some(self.last_executed_price)
        } else {
            None
        }
    }
}

impl TradeUpdate for GateBasicOrderMsg {
    fn event_time(&self) -> i64 {
        self.event_time
    }

    fn trade_time(&self) -> i64 {
        // Gate 订单推送里没有独立的成交时间，这里使用 update time
        self.event_time
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn trade_id(&self) -> i64 {
        0
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
        // 仅当解析到成交价口径字段时才返回，否则返回 0（避免误用挂单价）
        if self.last_executed_price > 0.0 {
            self.last_executed_price
        } else {
            0.0
        }
    }

    fn quantity(&self) -> f64 {
        // Gate 订单推送里没有 last_fill_qty，这里返回累计成交量（与 OKX 保持一致）
        self.cumulative_filled_quantity
    }

    fn commission(&self) -> f64 {
        0.0
    }

    fn commission_asset(&self) -> &str {
        &self.commission_asset
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
