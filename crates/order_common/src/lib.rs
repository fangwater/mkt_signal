use ahash::RandomState;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use symbol_utils::symbol_util::normalize_symbol_for_internal;
use symbol_utils::time_util::get_timestamp_us;

pub use symbol_utils::TradingVenue;

pub mod binance_basic_impl;
pub mod bitget_basic_impl;
pub mod bybit_basic_impl;
pub mod gate_basic_impl;
pub mod okex_basic_impl;
pub mod order_update;
pub mod query_engine_response;
pub mod query_order_updates;
pub mod trade_engine_response;
pub mod trade_error_code;
pub mod trade_request_type;
pub mod trade_update;
pub mod trade_update_lite;

pub use order_update::OrderUpdate;
pub use query_engine_response::{QueryEngineResponse, QueryEngineResponseMessage};
pub use query_order_updates::{OrderQueryOrderUpdate, OrderQueryTradeUpdate};
pub use trade_engine_response::{
    TradeEngineResponse, TradeEngineResponseMessage, TradeRequestKind,
};
pub use trade_request_type::TradeRequestType;
pub use trade_update::TradeUpdate;
pub use trade_update_lite::TradeUpdateLite;

type FastHashMap<K, V> = HashMap<K, V, RandomState>;

#[inline]
fn fast_hash_map<K, V>() -> FastHashMap<K, V> {
    HashMap::with_hasher(RandomState::new())
}

const ARB_CLOSE_SIGNAL_KIND: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceAccountMode {
    Unified,
    Standard,
}

impl BinanceAccountMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unified => "UNIFIED",
            Self::Standard => "STANDARD",
        }
    }
}

pub fn gate_text_from_client_order_id(client_order_id: i64) -> String {
    format!("t-{client_order_id}")
}

#[derive(Debug, Clone, Copy)]
pub struct OrderSubmitSignalMeta {
    pub signal_t: i64,
    pub signal_kind: u8,
    pub pre_trade_recv_t: i64,
    pub pre_trade_handle_t: i64,
    pub mkt_t: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeInForce {
    GTC,
    IOC,
    FOK,
    GTX,
}

impl TimeInForce {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "GTC" => Some(TimeInForce::GTC),
            "IOC" => Some(TimeInForce::IOC),
            "FOK" => Some(TimeInForce::FOK),
            "GTX" => Some(TimeInForce::GTX),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TimeInForce::GTC => "GTC",
            TimeInForce::IOC => "IOC",
            TimeInForce::FOK => "FOK",
            TimeInForce::GTX => "GTX",
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(TimeInForce::GTC),
            1 => Some(TimeInForce::IOC),
            2 => Some(TimeInForce::FOK),
            3 => Some(TimeInForce::GTX),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            TimeInForce::GTC => 0,
            TimeInForce::IOC => 1,
            TimeInForce::FOK => 2,
            TimeInForce::GTX => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionType {
    New,
    Canceled,
    Replaced,
    Rejected,
    Trade,
    Expired,
    TradePrevention,
}

impl ExecutionType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NEW" => Some(ExecutionType::New),
            "CANCELED" | "CANCELLED" => Some(ExecutionType::Canceled),
            "REPLACED" => Some(ExecutionType::Replaced),
            "REJECTED" => Some(ExecutionType::Rejected),
            "TRADE" => Some(ExecutionType::Trade),
            "EXPIRED" => Some(ExecutionType::Expired),
            "TRADE_PREVENTION" => Some(ExecutionType::TradePrevention),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionType::New => "NEW",
            ExecutionType::Canceled => "CANCELED",
            ExecutionType::Replaced => "REPLACED",
            ExecutionType::Rejected => "REJECTED",
            ExecutionType::Trade => "TRADE",
            ExecutionType::Expired => "EXPIRED",
            ExecutionType::TradePrevention => "TRADE_PREVENTION",
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            ExecutionType::New => 1,
            ExecutionType::Canceled => 2,
            ExecutionType::Replaced => 3,
            ExecutionType::Rejected => 4,
            ExecutionType::Trade => 5,
            ExecutionType::Expired => 6,
            ExecutionType::TradePrevention => 7,
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(ExecutionType::New),
            2 => Some(ExecutionType::Canceled),
            3 => Some(ExecutionType::Replaced),
            4 => Some(ExecutionType::Rejected),
            5 => Some(ExecutionType::Trade),
            6 => Some(ExecutionType::Expired),
            7 => Some(ExecutionType::TradePrevention),
            _ => None,
        }
    }

    pub fn is_trade(&self) -> bool {
        matches!(self, ExecutionType::Trade)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ExecutionType::Canceled
                | ExecutionType::Rejected
                | ExecutionType::Expired
                | ExecutionType::TradePrevention
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Expired,
    ExpiredInMatch,
}

impl OrderStatus {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NEW" => Some(OrderStatus::New),
            "PARTIALLY_FILLED" => Some(OrderStatus::PartiallyFilled),
            "FILLED" => Some(OrderStatus::Filled),
            "CANCELED" | "CANCELLED" => Some(OrderStatus::Canceled),
            "EXPIRED" => Some(OrderStatus::Expired),
            "EXPIRED_IN_MATCH" => Some(OrderStatus::ExpiredInMatch),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::New => "NEW",
            OrderStatus::PartiallyFilled => "PARTIALLY_FILLED",
            OrderStatus::Filled => "FILLED",
            OrderStatus::Canceled => "CANCELED",
            OrderStatus::Expired => "EXPIRED",
            OrderStatus::ExpiredInMatch => "EXPIRED_IN_MATCH",
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            OrderStatus::New => 1,
            OrderStatus::PartiallyFilled => 2,
            OrderStatus::Filled => 3,
            OrderStatus::Canceled => 4,
            OrderStatus::Expired => 5,
            OrderStatus::ExpiredInMatch => 6,
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(OrderStatus::New),
            2 => Some(OrderStatus::PartiallyFilled),
            3 => Some(OrderStatus::Filled),
            4 => Some(OrderStatus::Canceled),
            5 => Some(OrderStatus::Expired),
            6 => Some(OrderStatus::ExpiredInMatch),
            _ => None,
        }
    }

    pub fn is_finished(&self) -> bool {
        matches!(
            self,
            OrderStatus::Filled
                | OrderStatus::Canceled
                | OrderStatus::Expired
                | OrderStatus::ExpiredInMatch
        )
    }

    pub fn is_partially_filled(&self) -> bool {
        matches!(self, OrderStatus::PartiallyFilled)
    }

    pub fn has_filled(&self) -> bool {
        matches!(self, OrderStatus::PartiallyFilled | OrderStatus::Filled)
    }

    pub fn is_active(&self) -> bool {
        matches!(self, OrderStatus::New | OrderStatus::PartiallyFilled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Side {
    Buy = 1,
    Sell = 2,
}

impl Side {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Buy),
            2 => Some(Self::Sell),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "buy" | "BUY" | "Buy" => Some(Self::Buy),
            "sell" | "SELL" | "Sell" => Some(Self::Sell),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }

    pub fn as_str_lower(&self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }

    pub fn is_buy(&self) -> bool {
        matches!(self, Self::Buy)
    }

    pub fn is_sell(&self) -> bool {
        matches!(self, Self::Sell)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderExecutionStatus {
    Commit = 1,
    Create = 2,
    Filled = 3,
    Cancelled = 4,
    Rejected = 5,
}

impl OrderExecutionStatus {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Commit),
            2 => Some(Self::Create),
            3 => Some(Self::Filled),
            4 => Some(Self::Cancelled),
            5 => Some(Self::Rejected),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "CREATE" => Some(Self::Create),
            "COMMIT" => Some(Self::Commit),
            "FILLED" => Some(Self::Filled),
            "CANCELLED" => Some(Self::Cancelled),
            "REJECTED" => Some(Self::Rejected),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Create => "CREATE",
            Self::Commit => "COMMIT",
            Self::Filled => "FILLED",
            Self::Cancelled => "CANCELLED",
            Self::Rejected => "REJECTED",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Filled | Self::Cancelled | Self::Rejected)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderType {
    Limit = 1,
    Market = 3,
    StopLoss = 4,
    StopLossLimit = 5,
    TakeProfit = 6,
    TakeProfitLimit = 7,
    StopMarket = 8,
    TakeProfitMarket = 9,
}

impl OrderType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Limit),
            3 => Some(Self::Market),
            4 => Some(Self::StopLoss),
            5 => Some(Self::StopLossLimit),
            6 => Some(Self::TakeProfit),
            7 => Some(Self::TakeProfitLimit),
            8 => Some(Self::StopMarket),
            9 => Some(Self::TakeProfitMarket),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            Self::Limit => 1,
            Self::Market => 3,
            Self::StopLoss => 4,
            Self::StopLossLimit => 5,
            Self::TakeProfit => 6,
            Self::TakeProfitLimit => 7,
            Self::StopMarket => 8,
            Self::TakeProfitMarket => 9,
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "LIMIT" => Some(Self::Limit),
            "MARKET" => Some(Self::Market),
            "STOP_LOSS" => Some(Self::StopLoss),
            "STOP_LOSS_LIMIT" => Some(Self::StopLossLimit),
            "TAKE_PROFIT" => Some(Self::TakeProfit),
            "TAKE_PROFIT_LIMIT" => Some(Self::TakeProfitLimit),
            "STOP_MARKET" => Some(Self::StopMarket),
            "TAKE_PROFIT_MARKET" => Some(Self::TakeProfitMarket),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Limit => "LIMIT",
            Self::Market => "MARKET",
            Self::StopLoss => "STOP_LOSS",
            Self::StopLossLimit => "STOP_LOSS_LIMIT",
            Self::TakeProfit => "TAKE_PROFIT",
            Self::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
            Self::StopMarket => "STOP_MARKET",
            Self::TakeProfitMarket => "TAKE_PROFIT_MARKET",
        }
    }

    pub fn is_limit(&self) -> bool {
        matches!(
            self,
            Self::Limit | Self::StopLossLimit | Self::TakeProfitLimit
        )
    }

    pub fn is_market(&self) -> bool {
        matches!(
            self,
            Self::Market | Self::StopMarket | Self::TakeProfitMarket
        )
    }

    pub fn is_conditional(&self) -> bool {
        !matches!(self, Self::Limit | Self::Market)
    }
}

fn format_order_value(value: f64) -> String {
    if !value.is_finite() {
        return value.to_string();
    }
    let mut out = format!("{value:.12}");
    while out.contains('.') && out.ends_with('0') {
        out.pop();
    }
    if out.ends_with('.') {
        out.pop();
    }
    if out == "-0" {
        "0".to_string()
    } else {
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderUpdateSkipReason {
    DuplicateStatus,
    TerminalToTerminal,
    StaleNewOnTerminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeUpdateSkipReason {
    DuplicateFilled,
    StaleOrDuplicatePartial,
}

pub const CUMULATIVE_FILL_ROLLBACK_EPS: f64 = 1e-9;
const TRADE_UPDATE_QTY_EPS: f64 = CUMULATIVE_FILL_ROLLBACK_EPS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderQuantizedValue {
    pub tick_i64: i64,
    pub tick_exp: i32,
    pub count: i64,
}

impl OrderQuantizedValue {
    pub fn new(tick_i64: i64, tick_exp: i32, count: i64) -> Self {
        Self {
            tick_i64,
            tick_exp,
            count,
        }
    }

    pub fn zero() -> Self {
        Self::new(0, 0, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProtectedCumulativeFill {
    pub effective_cum: f64,
    pub rollback_detected: bool,
}

/// 订单管理器
pub struct OrderManager {
    orders: FastHashMap<i64, Order>, //映射order id到order
    pending_limit_order_count: FastHashMap<String, i32>, //单个交易品种当前有多少待成交的maker单
    pending_limit_buy_order_count: FastHashMap<String, i32>,
    pending_limit_sell_order_count: FastHashMap<String, i32>,
    pending_arb_close_limit_order_count: FastHashMap<String, i32>,
    pending_arb_close_limit_buy_order_count: FastHashMap<String, i32>,
    pending_arb_close_limit_sell_order_count: FastHashMap<String, i32>,
    binance_account_mode: Option<BinanceAccountMode>,
}

impl OrderManager {
    pub fn new(binance_account_mode: Option<BinanceAccountMode>) -> Self {
        if let Some(mode) = binance_account_mode {
            info!(
                "OrderManager: BINANCE_ACCOUNT_MODE={} (Binance UM account mode)",
                mode.as_str()
            );
        }
        Self {
            orders: fast_hash_map(),
            pending_limit_order_count: fast_hash_map(),
            pending_limit_buy_order_count: fast_hash_map(),
            pending_limit_sell_order_count: fast_hash_map(),
            pending_arb_close_limit_order_count: fast_hash_map(),
            pending_arb_close_limit_buy_order_count: fast_hash_map(),
            pending_arb_close_limit_sell_order_count: fast_hash_map(),
            binance_account_mode,
        }
    }

    pub fn binance_is_standard(&self) -> bool {
        self.binance_account_mode == Some(BinanceAccountMode::Standard)
    }

    pub fn map_update_status(status: OrderStatus) -> Option<OrderExecutionStatus> {
        match status {
            OrderStatus::New => Some(OrderExecutionStatus::Create),
            OrderStatus::Canceled => Some(OrderExecutionStatus::Cancelled),
            OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                Some(OrderExecutionStatus::Rejected)
            }
            _ => None,
        }
    }

    fn validate_duplicate_order_update_fields(
        order: &Order,
        incoming_status: OrderStatus,
        incoming_order_id: i64,
        incoming_cum_qty: f64,
        log_owner: &str,
        strategy_id: i32,
    ) {
        if incoming_order_id > 0 {
            if let Some(existing_order_id) = order.exchange_order_id {
                if existing_order_id != incoming_order_id {
                    warn!(
                        "{}: strategy_id={} duplicate order update has mismatched exchange_order_id: client_order_id={} local={} incoming={}",
                        log_owner,
                        strategy_id,
                        order.client_order_id,
                        existing_order_id,
                        incoming_order_id
                    );
                }
            }
        }

        if matches!(
            incoming_status,
            OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::ExpiredInMatch
        ) && (order.cumulative_filled_quantity - incoming_cum_qty).abs() > 1e-8
        {
            warn!(
                "{}: strategy_id={} duplicate terminal update has mismatched cumulative qty: client_order_id={} local={:.8} incoming={:.8}",
                log_owner,
                strategy_id,
                order.client_order_id,
                order.cumulative_filled_quantity,
                incoming_cum_qty
            );
        }
    }

    pub fn should_skip_idempotent_order_update(
        order: &Order,
        incoming_status: OrderStatus,
        incoming_order_id: i64,
        incoming_cum_qty: f64,
        log_owner: &str,
        strategy_id: i32,
    ) -> Option<OrderUpdateSkipReason> {
        let incoming_exec_status = Self::map_update_status(incoming_status)?;

        if order.status == incoming_exec_status {
            Self::validate_duplicate_order_update_fields(
                order,
                incoming_status,
                incoming_order_id,
                incoming_cum_qty,
                log_owner,
                strategy_id,
            );
            debug!(
                "{}: strategy_id={} skip duplicate order update: client_order_id={} status={:?}",
                log_owner, strategy_id, order.client_order_id, incoming_status
            );
            return Some(OrderUpdateSkipReason::DuplicateStatus);
        }

        if order.status.is_terminal() && incoming_exec_status.is_terminal() {
            Self::validate_duplicate_order_update_fields(
                order,
                incoming_status,
                incoming_order_id,
                incoming_cum_qty,
                log_owner,
                strategy_id,
            );
            warn!(
                "{}: strategy_id={} skip terminal->terminal order update: client_order_id={} local={:?} incoming={:?}",
                log_owner,
                strategy_id,
                order.client_order_id,
                order.status,
                incoming_status
            );
            return Some(OrderUpdateSkipReason::TerminalToTerminal);
        }

        if order.status.is_terminal() && incoming_exec_status == OrderExecutionStatus::Create {
            Self::validate_duplicate_order_update_fields(
                order,
                incoming_status,
                incoming_order_id,
                incoming_cum_qty,
                log_owner,
                strategy_id,
            );
            warn!(
                "{}: strategy_id={} skip stale NEW update on terminal order: client_order_id={} local={:?}",
                log_owner, strategy_id, order.client_order_id, order.status
            );
            return Some(OrderUpdateSkipReason::StaleNewOnTerminal);
        }

        None
    }

    pub fn should_skip_idempotent_trade_update(
        order: &Order,
        incoming_status: OrderStatus,
        incoming_cum_qty: f64,
        _incoming_update_ts: i64,
        log_owner: &str,
        strategy_id: i32,
    ) -> Option<TradeUpdateSkipReason> {
        let prev_cum = order.cumulative_filled_quantity;
        let same_cum_qty = (incoming_cum_qty - prev_cum).abs() <= TRADE_UPDATE_QTY_EPS;

        if incoming_status == OrderStatus::Filled && order.status == OrderExecutionStatus::Filled {
            debug!(
                "{}: strategy_id={} skip duplicate filled trade update: client_order_id={} prev_cum={:.8} incoming_cum={:.8}",
                log_owner,
                strategy_id,
                order.client_order_id,
                prev_cum,
                incoming_cum_qty
            );
            return Some(TradeUpdateSkipReason::DuplicateFilled);
        }

        // Gate futures may emit a terminal FILLED update after a PARTIALLY_FILLED
        // update without increasing cumulative fill quantity. Allow that status
        // promotion so the local order can move to a terminal state.
        if incoming_status == OrderStatus::Filled
            && order.status != OrderExecutionStatus::Filled
            && same_cum_qty
        {
            debug!(
                "{}: strategy_id={} accept terminal filled trade update with unchanged cumulative qty: client_order_id={} prev_cum={:.8} incoming_cum={:.8} local_status={:?}",
                log_owner,
                strategy_id,
                order.client_order_id,
                prev_cum,
                incoming_cum_qty,
                order.status
            );
            return None;
        }

        if incoming_cum_qty < prev_cum - TRADE_UPDATE_QTY_EPS || same_cum_qty {
            debug!(
                "{}: strategy_id={} skip stale/duplicate trade update by cumulative qty: client_order_id={} prev_cum={:.8} incoming_cum={:.8} local_status={:?} incoming_status={:?}",
                log_owner,
                strategy_id,
                order.client_order_id,
                prev_cum,
                incoming_cum_qty,
                order.status,
                incoming_status
            );
            return Some(TradeUpdateSkipReason::StaleOrDuplicatePartial);
        }

        None
    }

    pub fn compute_uniform_amount_update_from_cumulative(
        prev_cumulative_filled_qty: f64,
        incoming_cum_qty: f64,
    ) -> Option<f64> {
        if incoming_cum_qty + TRADE_UPDATE_QTY_EPS >= prev_cumulative_filled_qty {
            Some(incoming_cum_qty - prev_cumulative_filled_qty)
        } else {
            None
        }
    }

    pub fn create_order(
        &mut self,
        venue: TradingVenue,
        id: i64,
        order_type: OrderType,
        symbol: String,
        side: Side,
        quantity: f64,
        price: f64,
        reduce_only: bool,
        qty_multiplier: f64,
    ) -> i64 {
        self.create_order_with_pending_limit_flag(
            venue,
            id,
            order_type,
            symbol,
            side,
            quantity,
            price,
            reduce_only,
            qty_multiplier,
            true,
        )
    }

    pub fn create_order_with_pending_limit_flag(
        &mut self,
        venue: TradingVenue,
        id: i64,
        order_type: OrderType,
        symbol: String,
        side: Side,
        quantity: f64,
        price: f64,
        reduce_only: bool,
        qty_multiplier: f64,
        count_pending_limit: bool,
    ) -> i64 {
        let qty_multiplier = if qty_multiplier.is_finite() && qty_multiplier > 0.0 {
            qty_multiplier
        } else {
            warn!(
                "OrderManager: invalid qty_multiplier={}, fallback to 1.0 client_order_id={} symbol={} venue={:?}",
                qty_multiplier,
                id,
                symbol,
                venue
            );
            1.0
        };
        let symbol = normalize_symbol_for_internal(&symbol);
        let order = Order::new(
            venue,
            id,
            order_type,
            symbol.clone(),
            side,
            quantity,
            price,
            reduce_only,
            qty_multiplier,
            self.binance_account_mode,
            count_pending_limit,
        );
        self.insert(order);
        id
    }

    pub fn create_order_with_mut<F, R>(
        &mut self,
        venue: TradingVenue,
        id: i64,
        order_type: OrderType,
        symbol: String,
        side: Side,
        quantity: f64,
        price: f64,
        reduce_only: bool,
        qty_multiplier: f64,
        count_pending_limit: bool,
        f: F,
    ) -> Option<R>
    where
        F: FnOnce(&mut Order) -> R,
    {
        let symbol = normalize_symbol_for_internal(&symbol);
        self.create_order_with_mut_normalized_symbol(
            venue,
            id,
            order_type,
            &symbol,
            side,
            quantity,
            price,
            reduce_only,
            qty_multiplier,
            count_pending_limit,
            f,
        )
    }

    pub fn create_order_with_mut_normalized_symbol<F, R>(
        &mut self,
        venue: TradingVenue,
        id: i64,
        order_type: OrderType,
        symbol: &str,
        side: Side,
        quantity: f64,
        price: f64,
        reduce_only: bool,
        qty_multiplier: f64,
        count_pending_limit: bool,
        f: F,
    ) -> Option<R>
    where
        F: FnOnce(&mut Order) -> R,
    {
        match self.try_create_order_with_mut_normalized_symbol(
            venue,
            id,
            order_type,
            symbol,
            side,
            quantity,
            price,
            reduce_only,
            qty_multiplier,
            count_pending_limit,
            |order| Ok::<R, std::convert::Infallible>(f(order)),
        ) {
            Ok(result) => Some(result),
            Err(never) => match never {},
        }
    }

    pub fn try_create_order_with_mut_normalized_symbol<F, R, E>(
        &mut self,
        venue: TradingVenue,
        id: i64,
        order_type: OrderType,
        symbol: &str,
        side: Side,
        quantity: f64,
        price: f64,
        reduce_only: bool,
        qty_multiplier: f64,
        count_pending_limit: bool,
        f: F,
    ) -> Result<R, E>
    where
        F: FnOnce(&mut Order) -> Result<R, E>,
    {
        let qty_multiplier = if qty_multiplier.is_finite() && qty_multiplier > 0.0 {
            qty_multiplier
        } else {
            warn!(
                "OrderManager: invalid qty_multiplier={}, fallback to 1.0 client_order_id={} symbol={} venue={:?}",
                qty_multiplier,
                id,
                symbol,
                venue
            );
            1.0
        };
        let mut order = Order::new(
            venue,
            id,
            order_type,
            symbol.to_string(),
            side,
            quantity,
            price,
            reduce_only,
            qty_multiplier,
            self.binance_account_mode,
            count_pending_limit,
        );
        match f(&mut order) {
            Ok(result) => {
                self.insert(order);
                Ok(result)
            }
            Err(err) => Err(err),
        }
    }

    pub fn get_symbol_pending_limit_order_count(&self, symbol: &str) -> i32 {
        let symbol = normalize_symbol_for_internal(symbol);
        self.get_symbol_pending_limit_order_count_normalized(&symbol)
    }

    pub fn get_symbol_pending_limit_order_count_normalized(&self, symbol: &str) -> i32 {
        self.pending_limit_order_count
            .get(symbol)
            .copied()
            .unwrap_or(0)
    }

    pub fn get_symbol_pending_limit_order_count_by_side(&self, symbol: &str, side: Side) -> i32 {
        let symbol = normalize_symbol_for_internal(symbol);
        self.get_symbol_pending_limit_order_count_by_side_normalized(&symbol, side)
    }

    pub fn get_symbol_pending_limit_order_count_by_side_normalized(
        &self,
        symbol: &str,
        side: Side,
    ) -> i32 {
        self.pending_limit_side_count_map(PendingLimitScope::Default, side)
            .get(symbol)
            .copied()
            .unwrap_or(0)
    }

    pub fn get_symbol_pending_arb_close_limit_order_count(&self, symbol: &str) -> i32 {
        let symbol = normalize_symbol_for_internal(symbol);
        self.get_symbol_pending_arb_close_limit_order_count_normalized(&symbol)
    }

    pub fn get_symbol_pending_arb_close_limit_order_count_normalized(&self, symbol: &str) -> i32 {
        self.pending_arb_close_limit_order_count
            .get(symbol)
            .copied()
            .unwrap_or(0)
    }

    pub fn get_symbol_pending_arb_close_limit_order_count_by_side(
        &self,
        symbol: &str,
        side: Side,
    ) -> i32 {
        let symbol = normalize_symbol_for_internal(symbol);
        self.get_symbol_pending_arb_close_limit_order_count_by_side_normalized(&symbol, side)
    }

    pub fn get_symbol_pending_arb_close_limit_order_count_by_side_normalized(
        &self,
        symbol: &str,
        side: Side,
    ) -> i32 {
        self.pending_limit_side_count_map(PendingLimitScope::ArbClose, side)
            .get(symbol)
            .copied()
            .unwrap_or(0)
    }

    /// 添加订单
    pub fn insert(&mut self, order: Order) {
        let pending_key =
            Self::pending_limit_key(&order).map(|(scope, symbol, side)| (scope, symbol, side));

        let order_id = order.client_order_id;
        let prev = self.orders.insert(order_id, order);

        if let Some(prev_order) = prev {
            if let Some((scope, symbol, side)) = Self::pending_limit_key(&prev_order) {
                self.decrement_pending_limit_count_normalized(scope, &symbol, side);
            }
        }

        if let Some((scope, symbol, side)) = pending_key {
            self.increment_pending_limit_count_normalized(scope, symbol, side);
        }

        // 持久化
    }

    /// 根据订单ID获取订单
    pub fn get(&self, order_id: i64) -> Option<Order> {
        self.orders.get(&order_id).cloned()
    }

    /// 获取订单数量乘数（venue qty -> base qty）
    pub fn get_qty_multiplier(&self, order_id: i64) -> Option<f64> {
        self.orders.get(&order_id).map(|order| order.qty_multiplier)
    }

    /// 基于订单记录的数量乘数，将 venue qty 转为 base qty
    pub fn venue_qty_to_base_by_order(&self, order_id: i64, venue_qty: f64) -> Option<f64> {
        self.get_qty_multiplier(order_id)
            .map(|qty_multiplier| venue_qty * qty_multiplier)
    }

    /// 打印订单详细信息的三线表日志
    pub fn log_order_details(&self, order: &Order, title: &str, strategy_id: i32) {
        warn!("═══════════════════════════════════════════════════════════════");
        warn!("{} - Strategy ID: {}", title, strategy_id);
        warn!("───────────────────────────────────────────────────────────────");
        warn!("订单ID:       {}", order.client_order_id);
        warn!("交易场所:     {:?}", order.venue);
        warn!("交易对:       {}", order.symbol);
        warn!("订单类型:     {:?}", order.order_type);
        warn!("方向:         {:?}", order.side);
        warn!("价格:         {}", format_order_value(order.price));
        warn!("数量:         {}", format_order_value(order.quantity));
        warn!("数量乘数:     {:.8}", order.qty_multiplier);
        warn!("只减仓:       {}", order.reduce_only);
        warn!("成交量:       {:.8}", order.cumulative_filled_quantity);
        warn!("订单状态:     {:?}", order.status);
        warn!("提交时间:     {}", order.timestamp.submit_t);
        warn!("创建时间:     {}", order.timestamp.create_t);
        warn!("结束时间:     {}", order.timestamp.end_t);
        warn!("本地更新:     {}", order.timestamp.local_t);
        warn!("═══════════════════════════════════════════════════════════════");
    }

    /// 根据订单ID获取订单的可变引用并执行操作
    pub fn update<F>(&mut self, order_id: i64, f: F) -> bool
    where
        F: FnOnce(&mut Order),
    {
        let Some((before_key, after_key)) = self.orders.get_mut(&order_id).map(|order| {
            let before_key = Self::pending_limit_key(order);
            f(order);
            let after_key = Self::pending_limit_key(order);
            (before_key, after_key)
        }) else {
            return false;
        };

        if before_key != after_key {
            if let Some((scope, symbol, side)) = before_key {
                self.decrement_pending_limit_count_normalized(scope, &symbol, side);
            }
            if let Some((scope, symbol, side)) = after_key {
                self.increment_pending_limit_count_normalized(scope, symbol, side);
            }
        }
        true
    }

    /// 应用一次远端来的订单更新（OrderUpdate / TradeUpdate / 查询回报），
    /// 在闭包执行之前先把 `Order.timestamp.local_t` 覆写为当前本地时间(µs)。
    ///
    /// 仅用于"实质性接受"的远端事件；本地内部状态调整（例如 cleanup 阶段
    /// 的 terminalize）请继续使用 [`OrderManager::update`]。
    pub fn apply_remote_update<F>(&mut self, order_id: i64, f: F) -> bool
    where
        F: FnOnce(&mut Order),
    {
        let now = get_timestamp_us();
        self.update(order_id, |order| {
            order.timestamp.local_t = now;
            f(order);
        })
    }

    pub fn set_submit_time_and_signal_meta(
        &mut self,
        order_id: i64,
        submit_time: i64,
        is_new_order_request: bool,
    ) -> Option<OrderSubmitSignalMeta> {
        self.orders.get_mut(&order_id).map(|order| {
            if is_new_order_request && order.timestamp.create_t == 0 {
                order.set_create_time(submit_time);
            }
            order.set_submit_time(submit_time);
            OrderSubmitSignalMeta {
                signal_t: order.timestamp.signal_t,
                signal_kind: order.timestamp.signal_kind,
                pre_trade_recv_t: order.timestamp.pre_trade_recv_t,
                pre_trade_handle_t: order.timestamp.pre_trade_handle_t,
                mkt_t: order.timestamp.mkt_t,
            }
        })
    }

    /// 移除订单
    pub fn remove(&mut self, order_id: i64) -> Option<Order> {
        let removed = self.orders.remove(&order_id);

        if let Some(ref order) = removed {
            // 如果是限价单，减少计数
            if let Some((scope, symbol, side)) = Self::pending_limit_key(order) {
                self.decrement_pending_limit_count_normalized(scope, &symbol, side);
            }
        }

        removed
    }

    /// 获取所有订单ID
    pub fn get_all_ids(&self) -> Vec<i64> {
        self.orders.keys().cloned().collect()
    }

    /// 获取订单数量
    pub fn count(&self) -> usize {
        self.orders.len()
    }

    /// 清空所有订单
    pub fn clear(&mut self) {
        self.orders.clear();
        self.pending_limit_order_count.clear();
        self.pending_limit_buy_order_count.clear();
        self.pending_limit_sell_order_count.clear();
        self.pending_arb_close_limit_order_count.clear();
        self.pending_arb_close_limit_buy_order_count.clear();
        self.pending_arb_close_limit_sell_order_count.clear();
    }

    fn pending_limit_key(order: &Order) -> Option<(PendingLimitScope, String, Side)> {
        (order.order_type.is_limit() && order.count_pending_limit && !order.status.is_terminal())
            .then(|| (order.pending_limit_scope, order.symbol.clone(), order.side))
    }

    fn pending_limit_total_count_map_mut(
        &mut self,
        scope: PendingLimitScope,
    ) -> &mut FastHashMap<String, i32> {
        match scope {
            PendingLimitScope::Default => &mut self.pending_limit_order_count,
            PendingLimitScope::ArbClose => &mut self.pending_arb_close_limit_order_count,
        }
    }

    fn pending_limit_side_count_map(
        &self,
        scope: PendingLimitScope,
        side: Side,
    ) -> &FastHashMap<String, i32> {
        match (scope, side) {
            (PendingLimitScope::Default, Side::Buy) => &self.pending_limit_buy_order_count,
            (PendingLimitScope::Default, Side::Sell) => &self.pending_limit_sell_order_count,
            (PendingLimitScope::ArbClose, Side::Buy) => {
                &self.pending_arb_close_limit_buy_order_count
            }
            (PendingLimitScope::ArbClose, Side::Sell) => {
                &self.pending_arb_close_limit_sell_order_count
            }
        }
    }

    fn pending_limit_side_count_map_mut(
        &mut self,
        scope: PendingLimitScope,
        side: Side,
    ) -> &mut FastHashMap<String, i32> {
        match (scope, side) {
            (PendingLimitScope::Default, Side::Buy) => &mut self.pending_limit_buy_order_count,
            (PendingLimitScope::Default, Side::Sell) => &mut self.pending_limit_sell_order_count,
            (PendingLimitScope::ArbClose, Side::Buy) => {
                &mut self.pending_arb_close_limit_buy_order_count
            }
            (PendingLimitScope::ArbClose, Side::Sell) => {
                &mut self.pending_arb_close_limit_sell_order_count
            }
        }
    }

    fn increment_pending_limit_count_normalized(
        &mut self,
        scope: PendingLimitScope,
        symbol: String,
        side: Side,
    ) {
        *self
            .pending_limit_total_count_map_mut(scope)
            .entry(symbol.clone())
            .or_insert(0) += 1;
        *self
            .pending_limit_side_count_map_mut(scope, side)
            .entry(symbol)
            .or_insert(0) += 1;
    }

    fn decrement_count(map: &mut FastHashMap<String, i32>, symbol: &str) -> i32 {
        let mut should_remove = false;
        let remaining = match map.get_mut(symbol) {
            Some(entry) if *entry > 1 => {
                *entry -= 1;
                *entry
            }
            Some(_) => {
                should_remove = true;
                0
            }
            None => 0,
        };
        if should_remove {
            map.remove(symbol);
        }
        remaining
    }

    fn decrement_pending_limit_count_normalized(
        &mut self,
        scope: PendingLimitScope,
        symbol: &str,
        side: Side,
    ) {
        let remaining_total =
            Self::decrement_count(self.pending_limit_total_count_map_mut(scope), symbol);
        let remaining_side =
            Self::decrement_count(self.pending_limit_side_count_map_mut(scope, side), symbol);

        debug!(
            "OrderManager: symbol={} side={} pending_limit_scope={} pending_limit_count dec -> total={} side={}",
            symbol,
            side.as_str(),
            scope.as_str(),
            remaining_total,
            remaining_side
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingLimitScope {
    Default,
    ArbClose,
}

impl PendingLimitScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::ArbClose => "arb_close",
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderTimeStamp {
    pub submit_t: i64, // 最近一次给 trade engine / query engine 发送请求的本地时间(µs)
    pub create_t: i64, // 新订单请求首次 publish 的本地时间；缺失时可由远端 NEW 事件时间兜底
    pub end_t: i64,    // 交易所时间(完全成交或者被撤单的时间)
    pub local_t: i64, // OrderUpdate/TradeUpdate/查询回报在本地最近一次被实质性接受的时间(µs)，每次覆写
    pub mkt_t: i64,   // 触发该订单动作（open/cancel/close）时所参考的最新盘口时间(µs)；
    // 套利场景=max(open_leg.ts, hedge_leg.ts)；MM/Hedge 等无概念路径保持 0
    pub signal_t: i64, // 触发该订单动作的信号在 trade_signal 进程的生成时间(µs)；
    // 用于 egress 单点测度 signal→submit 延迟；无信号上下文(orphan 兜底)保持 0
    pub signal_kind: u8, // 触发该订单动作的信号类型(SignalType as u8)，0=未知/不计入测度
    pub pre_trade_recv_t: i64, // pre_trade 从 signal IPC 收到该 open 信号的本地时间(µs)，0=未知
    pub pre_trade_handle_t: i64, // pre_trade 开始处理该 open 信号的本地时间(µs)，0=未知
}

impl OrderTimeStamp {
    fn new() -> Self {
        OrderTimeStamp {
            submit_t: 0,
            create_t: 0,
            end_t: 0,
            local_t: 0,
            mkt_t: 0,
            signal_t: 0,
            signal_kind: 0,
            pre_trade_recv_t: 0,
            pre_trade_handle_t: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Order {
    pub venue: TradingVenue,                      // 订单对应的交易标的
    pub client_order_id: i64,                     // 订单ID
    pub order_type: OrderType,                    // 订单类型
    pub symbol: String,                           // 交易对
    pub side: Side,                               // 买卖方向
    pub price: f64,                               // 限价单价格, 市价单没有意义
    pub quantity: f64,                            // 数量
    pub quantity_qv: Option<OrderQuantizedValue>, // 已对齐/量化的数量缓存
    pub price_qv: Option<OrderQuantizedValue>,    // 已对齐/量化的价格缓存
    pub qty_multiplier: f64,                      // 数量乘数（venue qty -> base qty）
    pub reduce_only: bool,                        // 是否只减仓
    pub bitget_spot_order: bool,                  // Bitget UTA order category=SPOT
    pub cumulative_filled_quantity: f64,          // 成交量
    pub exchange_order_id: Option<i64>,           // 交易所返回的 orderId
    pub status: OrderExecutionStatus,             // 订单执行状态
    pub timestamp: OrderTimeStamp,
    pub count_pending_limit: bool, // 是否计入 pending-limit 风控统计
    pub pending_limit_scope: PendingLimitScope,
    binance_account_mode: Option<BinanceAccountMode>,
}

impl Order {
    /// 获取策略ID - 策略ID是订单ID的前32位
    pub fn get_strategy_id(&self) -> i32 {
        (self.client_order_id >> 32) as i32
    }

    /// 创建新订单
    pub fn new(
        venue: TradingVenue,
        client_order_id: i64,
        order_type: OrderType,
        symbol: String,
        side: Side,
        quantity: f64,
        price: f64,
        reduce_only: bool,
        qty_multiplier: f64,
        binance_account_mode: Option<BinanceAccountMode>,
        count_pending_limit: bool,
    ) -> Self {
        Order {
            venue,
            client_order_id,
            order_type,
            symbol,
            side,
            price,
            quantity,
            quantity_qv: None,
            price_qv: None,
            qty_multiplier,
            reduce_only,
            bitget_spot_order: false,
            status: OrderExecutionStatus::Commit,
            cumulative_filled_quantity: 0.0,
            exchange_order_id: None,
            timestamp: OrderTimeStamp::new(),
            count_pending_limit,
            pending_limit_scope: PendingLimitScope::Default,
            binance_account_mode,
        }
    }

    pub fn require_binance_account_mode(&self) -> BinanceAccountMode {
        self.binance_account_mode.unwrap_or_else(|| {
            panic!("BINANCE_ACCOUNT_MODE must be set to 'UNIFIED' or 'STANDARD' when using binance-futures");
        })
    }

    pub fn set_quantity_qv(&mut self, qv: OrderQuantizedValue) {
        self.quantity_qv = Some(qv);
    }

    pub fn set_price_qv(&mut self, qv: OrderQuantizedValue) {
        self.price_qv = Some(qv);
    }

    pub fn set_quantized_values(
        &mut self,
        quantity_qv: OrderQuantizedValue,
        price_qv: Option<OrderQuantizedValue>,
    ) {
        self.set_quantity_qv(quantity_qv);
        if let Some(price_qv) = price_qv {
            self.set_price_qv(price_qv);
        }
    }

    pub fn set_bitget_spot_order(&mut self, enabled: bool) {
        self.bitget_spot_order = enabled;
    }

    /// 更新订单状态
    pub fn update_status(&mut self, status: OrderExecutionStatus) {
        // 增加订单状态检查
        if status == OrderExecutionStatus::Create && self.status != OrderExecutionStatus::Commit {
            //出现非正常的状态切换，打印日志
            warn!("unexpected OrderExecutionStatus");
        }
        self.status = status;
    }

    pub fn protected_cumulative_fill(&self, incoming_cum: f64) -> ProtectedCumulativeFill {
        Self::protect_cumulative_fill_value(self.cumulative_filled_quantity, incoming_cum)
    }

    pub fn protect_cumulative_fill_value(
        prev_cum: f64,
        incoming_cum: f64,
    ) -> ProtectedCumulativeFill {
        let rollback_detected = incoming_cum + CUMULATIVE_FILL_ROLLBACK_EPS < prev_cum;
        let effective_cum = if rollback_detected {
            prev_cum
        } else {
            incoming_cum
        };
        ProtectedCumulativeFill {
            effective_cum,
            rollback_detected,
        }
    }

    /// 设置最近一次给 trade engine / query engine 发送请求的时间（每次 send 都覆写）
    pub fn set_submit_time(&mut self, time: i64) {
        self.timestamp.submit_t = time;
    }

    /// 设置执行时间
    pub fn set_create_time(&mut self, time: i64) {
        self.timestamp.create_t = time;
    }

    /// 设置结束时间
    pub fn set_end_time(&mut self, time: i64) {
        self.timestamp.end_t = time;
    }

    /// 设置触发该订单动作时所参考的最新盘口时间（µs）。
    /// 套利策略在 ArbOpen / ArbCancel / ArbClose 信号到达时调用，传入两腿盘口 ts 的较新者。
    pub fn set_mkt_time(&mut self, time: i64) {
        self.timestamp.mkt_t = time;
    }

    /// 设置触发本次订单动作的信号元数据：signal_t（trade_signal 进程生成信号的时间，µs）
    /// 与 signal_kind（SignalType as u8）。必须在 egress 发送前调用，供 egress 单点测度
    /// signal→submit 延迟。无信号上下文（orphan 兜底）保持默认 0，egress 自动跳过。
    pub fn set_signal_meta(&mut self, signal_t: i64, signal_kind: u8) {
        self.timestamp.signal_t = signal_t;
        self.timestamp.signal_kind = signal_kind;
        if signal_kind == ARB_CLOSE_SIGNAL_KIND {
            self.pending_limit_scope = PendingLimitScope::ArbClose;
        }
    }

    pub fn set_pre_trade_open_trace(&mut self, receive_t: i64, handle_t: i64) {
        self.timestamp.pre_trade_recv_t = receive_t;
        self.timestamp.pre_trade_handle_t = handle_t;
    }

    pub fn set_exchange_order_id(&mut self, exchange_order_id: i64) {
        if exchange_order_id <= 0 {
            return;
        }

        match self.exchange_order_id {
            None => {
                self.exchange_order_id = Some(exchange_order_id);
            }
            Some(existing_order_id) if existing_order_id == exchange_order_id => {}
            Some(existing_order_id) => {
                warn!(
                    "ignore mismatched exchange_order_id update: client_order_id={} local={} incoming={}",
                    self.client_order_id, existing_order_id, exchange_order_id
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert_test_order(manager: &mut OrderManager, client_order_id: i64) {
        manager.create_order(
            TradingVenue::BinanceFutures,
            client_order_id,
            OrderType::Limit,
            "BTCUSDT".to_string(),
            Side::Buy,
            1.0,
            100.0,
            false,
            1.0,
        );
    }

    #[test]
    fn new_order_submit_initializes_create_time_once() {
        let mut manager = OrderManager::new(None);
        let client_order_id = 42;
        insert_test_order(&mut manager, client_order_id);

        let meta = manager.set_submit_time_and_signal_meta(client_order_id, 1_000, true);
        assert!(meta.is_some());
        let order = manager.get(client_order_id).expect("order exists");
        assert_eq!(order.timestamp.create_t, 1_000);
        assert_eq!(order.timestamp.submit_t, 1_000);

        manager.set_submit_time_and_signal_meta(client_order_id, 2_000, false);
        let order = manager.get(client_order_id).expect("order exists");
        assert_eq!(order.timestamp.create_t, 1_000);
        assert_eq!(order.timestamp.submit_t, 2_000);

        manager.set_submit_time_and_signal_meta(client_order_id, 3_000, true);
        let order = manager.get(client_order_id).expect("order exists");
        assert_eq!(order.timestamp.create_t, 1_000);
        assert_eq!(order.timestamp.submit_t, 3_000);
    }

    #[test]
    fn pending_limit_counts_use_stored_normalized_symbol() {
        let mut manager = OrderManager::new(None);
        manager.create_order(
            TradingVenue::OkexMargin,
            7,
            OrderType::Limit,
            "btc-usdt-swap".to_string(),
            Side::Buy,
            1.0,
            100.0,
            false,
            1.0,
        );

        let order = manager.get(7).expect("order");
        assert_eq!(order.symbol, "BTCUSDT");
        assert_eq!(manager.get_symbol_pending_limit_order_count("BTCUSDT"), 1);
        assert_eq!(
            manager.get_symbol_pending_limit_order_count_normalized("BTCUSDT"),
            1
        );
        assert_eq!(
            manager.get_symbol_pending_limit_order_count_by_side("BTCUSDT", Side::Buy),
            1
        );
        assert_eq!(
            manager.get_symbol_pending_limit_order_count_by_side_normalized("BTCUSDT", Side::Buy),
            1
        );
        assert_eq!(manager.get_symbol_pending_limit_order_count("BTC-USDT"), 1);
    }

    #[test]
    fn try_create_order_with_mut_does_not_insert_on_error() {
        let mut manager = OrderManager::new(None);
        let result = manager.try_create_order_with_mut_normalized_symbol(
            TradingVenue::OkexMargin,
            7,
            OrderType::Limit,
            "BTCUSDT",
            Side::Buy,
            1.0,
            100.0,
            false,
            1.0,
            true,
            |_order| Err::<(), _>("request build failed"),
        );

        assert_eq!(result, Err("request build failed"));
        assert!(manager.get(7).is_none());
        assert_eq!(manager.get_symbol_pending_limit_order_count("BTCUSDT"), 0);
        assert_eq!(
            manager.get_symbol_pending_limit_order_count_by_side("BTCUSDT", Side::Buy),
            0
        );
    }

    #[test]
    fn arb_close_pending_limit_counts_are_separate() {
        let mut manager = OrderManager::new(None);
        manager.create_order_with_mut(
            TradingVenue::OkexMargin,
            10,
            OrderType::Limit,
            "fil-usdt".to_string(),
            Side::Sell,
            1.0,
            100.0,
            true,
            1.0,
            true,
            |order| order.set_signal_meta(1_000, ARB_CLOSE_SIGNAL_KIND),
        );
        manager.create_order(
            TradingVenue::OkexMargin,
            11,
            OrderType::Limit,
            "FILUSDT".to_string(),
            Side::Sell,
            1.0,
            101.0,
            false,
            1.0,
        );

        assert_eq!(manager.get_symbol_pending_limit_order_count("FILUSDT"), 1);
        assert_eq!(
            manager.get_symbol_pending_limit_order_count_by_side("FILUSDT", Side::Sell),
            1
        );
        assert_eq!(
            manager.get_symbol_pending_arb_close_limit_order_count("FILUSDT"),
            1
        );
        assert_eq!(
            manager.get_symbol_pending_arb_close_limit_order_count_by_side("FILUSDT", Side::Sell),
            1
        );

        manager.update(10, |order| order.set_signal_meta(2_000, 3));
        assert_eq!(
            manager.get_symbol_pending_arb_close_limit_order_count_by_side("FILUSDT", Side::Sell),
            1
        );
        assert_eq!(
            manager.get_symbol_pending_limit_order_count_by_side("FILUSDT", Side::Sell),
            1
        );

        manager.update(10, |order| order.status = OrderExecutionStatus::Cancelled);
        assert_eq!(
            manager.get_symbol_pending_arb_close_limit_order_count("FILUSDT"),
            0
        );
        assert_eq!(manager.get_symbol_pending_limit_order_count("FILUSDT"), 1);
    }

    #[test]
    fn trade_request_type_classifies_new_order_requests() {
        assert!(TradeRequestType::BinanceNewUMOrder.is_new_order());
        assert!(TradeRequestType::GateFuturesNewOrder.is_new_order());
        assert!(TradeRequestType::BitgetNewUMOrder.is_new_order());

        assert!(!TradeRequestType::BinanceCancelUMOrder.is_new_order());
        assert!(!TradeRequestType::GateFuturesCancelOrder.is_new_order());
        assert!(!TradeRequestType::BinanceUMSetLeverage.is_new_order());
    }
}
