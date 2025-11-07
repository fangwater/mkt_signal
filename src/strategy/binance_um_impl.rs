// ============================================================================
// Trait 实现：为 OrderTradeUpdateMsg 实现 OrderUpdate 和 TradeUpdate
// 币安的杠杆下单， margin order的账号消息流推送
// ============================================================================

use crate::common::account_msg::OrderTradeUpdateMsg;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use crate::pre_trade::order_manager::{Side, OrderType};
use crate::signal::common::{TimeInForce, ExecutionType, OrderStatus, TradingVenue};

impl OrderUpdate for OrderTradeUpdateMsg {
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
        match self.side {
            'B' | 'b' => Side::Buy,
            'S' | 's' => Side::Sell,
            _ => Side::Buy,
        }
    }

    fn order_type(&self) -> OrderType {
        OrderType::from_str(&self.order_type).unwrap_or(OrderType::Limit)
    }

    fn time_in_force(&self) -> TimeInForce {
        TimeInForce::from_str(&self.time_in_force).unwrap_or(TimeInForce::GTC)
    }

    fn price(&self) -> f64 {
        self.price
    }

    fn quantity(&self) -> f64 {
        self.quantity
    }

    fn last_time_executed_qty(&self) -> f64 {
        self.last_executed_quantity
    }

    fn cumulative_qty(&self) -> f64 {
        self.cumulative_filled_quantity
    }
    

    fn status(&self) -> OrderStatus {
        OrderStatus::from_str(&self.order_status).unwrap_or(OrderStatus::New)
    }

    fn execution_type(&self) -> ExecutionType {
        ExecutionType::from_str(&self.execution_type).unwrap_or(ExecutionType::New)
    }

    fn trading_venue(&self) -> TradingVenue {
        // OrderTradeUpdate 消息类型对应 BinanceUm
        TradingVenue::BinanceUm
    }
}

impl TradeUpdate for OrderTradeUpdateMsg {
    fn event_time(&self) -> i64 {
        self.event_time
    }

    fn trade_time(&self) -> i64 {
        self.transaction_time
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
        match self.side {
            'B' | 'b' => Side::Buy,
            'S' | 's' => Side::Sell,
            _ => Side::Buy,
        }
    }

    fn price(&self) -> f64 {
        self.last_executed_price
    }

    fn quantity(&self) -> f64 {
        self.last_executed_quantity
    }

    fn commission(&self) -> f64 {
        self.commission_amount
    }

    fn commission_asset(&self) -> &str {
        &self.commission_asset
    }

    fn is_maker(&self) -> bool {
        self.is_maker
    }
    
    fn realized_pnl(&self) -> f64 {
        // 现货/杠杆没有已实现盈亏
        self.realized_profit
    }
    fn trading_venue(&self) -> TradingVenue {
        // OrderTradeUpdate 消息类型对应 BinanceUm
        TradingVenue::BinanceUm
    }
}

