//! 为 OKX basic 订单消息实现统一的 OrderUpdate / TradeUpdate 接口

use crate::common::basic_account_msg::OkexOrderMsg;
use crate::pre_trade::order_manager::OrderType;
use crate::pre_trade::order_manager::Side;
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;

/// OKX 订单状态 -> 通用 OrderStatus（Binance 语义）
/// - 1: canceled/mmp_canceled => CANCELED
/// - 3: partially_filled      => PARTIALLY_FILLED
/// - 4: filled                => FILLED
/// - others (live 等)         => NEW
fn map_order_status(state: u8) -> OrderStatus {
    match state {
        1 | 5 => OrderStatus::Canceled,
        3 => OrderStatus::PartiallyFilled,
        4 => OrderStatus::Filled,
        _ => OrderStatus::New,
    }
}

/// OKX 状态 -> 执行类型（Binance 语义）
/// - 1/5: canceled/mmp_canceled => CANCELED
/// - 3/4: partially_filled/filled => TRADE
/// - others (live/new) => NEW
fn map_execution_type(state: u8) -> ExecutionType {
    match state {
        1 | 5 => ExecutionType::Canceled,
        3 | 4 => ExecutionType::Trade,
        _ => ExecutionType::New,
    }
}

/// OKX instType -> 交易场所
/// - 2: 永续合约（SWAP） => TradingVenue::OkexFutures
/// - 3: 交割合约（FUTURES） => TradingVenue::OkexFutures
/// - 4: 期权合约（OPTION） => TradingVenue::OkexFutures
/// - 其余（默认 1: 现货保证金） => TradingVenue::OkexMargin
fn map_trading_venue(inst_type: u8) -> TradingVenue {
    match inst_type {
        2..=4 => TradingVenue::OkexFutures,
        _ => TradingVenue::OkexMargin,
    }
}

/// OKX ordType -> 通用 OrderType（只覆盖常见值）
/// - 0: 市价 => OrderType::Market
/// - 1: 限价 => OrderType::Limit
/// - 2: PostOnly => OrderType::Limit（无需单独映射）
/// - 其它值保持默认（仍按 Market 处理）
fn map_order_type(ord_type: u8) -> OrderType {
    match ord_type {
        1 => OrderType::Limit,
        2 => OrderType::Limit,
        0 => OrderType::Market,
        _ => OrderType::Market,
    }
}

/// OKX ordType -> TimeInForce（仅推断 IOC/FOK，其余视为 GTC）
fn map_time_in_force(ord_type: u8) -> TimeInForce {
    match ord_type {
        3 => TimeInForce::FOK,
        4 => TimeInForce::IOC,
        _ => TimeInForce::GTC,
    }
}

/// OKX side(u8) -> Side
fn map_side(side: u8) -> Side {
    match side {
        1 => Side::Buy,
        2 => Side::Sell,
        _ => Side::Buy,
    }
}

impl OrderUpdate for OkexOrderMsg {
    fn event_time(&self) -> i64 {
        // OKX 时间戳为毫秒，统一转换为微秒
        self.update_time.saturating_mul(1_000)
    }

    fn symbol(&self) -> &str {
        &self.inst_id
    }

    fn order_id(&self) -> i64 {
        self.ord_id
    }

    fn client_order_id(&self) -> i64 {
        self.cl_ord_id
    }

    fn side(&self) -> Side {
        map_side(self.side)
    }

    fn order_type(&self) -> OrderType {
        map_order_type(self.ord_type)
    }

    fn time_in_force(&self) -> TimeInForce {
        map_time_in_force(self.ord_type)
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
        map_order_status(self.state)
    }

    fn execution_type(&self) -> ExecutionType {
        map_execution_type(self.state)
    }

    fn trading_venue(&self) -> TradingVenue {
        map_trading_venue(self.inst_type)
    }
}

impl TradeUpdate for OkexOrderMsg {
    fn event_time(&self) -> i64 {
        self.update_time.saturating_mul(1_000)
    }

    fn trade_time(&self) -> i64 {
        self.fill_time.saturating_mul(1_000)
    }

    fn symbol(&self) -> &str {
        &self.inst_id
    }

    fn order_id(&self) -> i64 {
        self.ord_id
    }

    fn client_order_id(&self) -> i64 {
        self.cl_ord_id
    }

    fn side(&self) -> Side {
        map_side(self.side)
    }

    fn price(&self) -> f64 {
        self.price
    }

    fn is_maker(&self) -> bool {
        matches!(self.ord_type, 1 | 2)
    }

    fn trading_venue(&self) -> TradingVenue {
        map_trading_venue(self.inst_type)
    }

    fn cumulative_filled_quantity(&self) -> f64 {
        self.cumulative_filled_quantity
    }

    fn order_status(&self) -> Option<OrderStatus> {
        Some(map_order_status(self.state))
    }
}
