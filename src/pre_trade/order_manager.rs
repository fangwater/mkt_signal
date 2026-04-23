use crate::trade_engine::bybit::{
    BybitCancelOrderParams, BybitCancelOrderRequest, BybitNewOrderParams, BybitNewOrderRequest,
};
use crate::trade_engine::okex::{
    OkexCancelOrderParams, OkexCancelOrderRequest, OkexNewOrderParams, OkexNewOrderRequest,
    OkexOrderType,
};
use crate::trade_engine::trade_request::BinanceNewMarginOrderRequest;
use crate::trade_engine::trade_request::BinanceNewUMOrderRequest;
use crate::trade_engine::trade_request::{
    BinanceCancelMarginOrderRequest, BinanceCancelUMOrderRequest,
    BinanceWsCancelMarginOrderRequest, BinanceWsCancelUMOrderRequest,
    BinanceWsNewMarginOrderRequest, BinanceWsNewUMOrderRequest, BitgetMarginCancelOrderRequest,
    BitgetMarginNewOrderRequest, BitgetUmCancelOrderRequest, BitgetUmNewOrderRequest,
    GateFuturesCancelOrderRequest, GateFuturesNewOrderRequest, GateUnifiedCancelOrderRequest,
    GateUnifiedNewOrderRequest,
};
use crate::{
    common::symbol_util::normalize_symbol_for_internal,
    common::tick_math::QuantizedValue,
    common::time_util::get_timestamp_us,
    signal::common::{OrderStatus, TradingVenue},
};
use bytes::Bytes;
use log::{debug, info, warn};
use std::collections::HashMap;
fn format_decimal(value: f64) -> String {
    QuantizedValue::from_decimal(value)
        .map(|qv| qv.decimal_string())
        .unwrap_or_else(|| "0".to_string())
}

fn format_quantity(quantity: f64) -> String {
    format_decimal(quantity)
}

fn format_signed_quantity(quantity: f64) -> String {
    if quantity < 0.0 {
        let abs = format_decimal(-quantity);
        if abs == "0" {
            abs
        } else {
            format!("-{abs}")
        }
    } else {
        format_decimal(quantity)
    }
}

fn format_price(price: f64) -> String {
    format_decimal(price)
}

fn format_order_quantity(quantity: f64) -> String {
    QuantizedValue::from_decimal(quantity)
        .map(|qv| qv.decimal_string())
        .unwrap_or_else(|| format_quantity(quantity))
}

fn format_order_price(price: f64) -> String {
    QuantizedValue::from_decimal(price)
        .map(|qv| qv.decimal_string())
        .unwrap_or_else(|| format_price(price))
}

fn binance_ws_um_new_order_resp_type() -> &'static str {
    "RESULT"
}

/// 从交易对符号中提取 base asset 和 quote asset
/// 例如: "BTCUSDT" -> ("BTC", "USDT")
fn extract_assets_from_symbol(symbol: &str) -> (String, String) {
    let symbol_upper = symbol.to_uppercase();
    const QUOTE_ASSETS: [&str; 7] = ["USDT", "USDC", "BUSD", "FDUSD", "BIDR", "TRY", "USD"];

    for quote in QUOTE_ASSETS {
        if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
            let base = &symbol_upper[..symbol_upper.len() - quote.len()];
            return (base.to_string(), quote.to_string());
        }
    }

    // 如果没有匹配到已知的 quote asset，默认返回整个符号作为 base，USDT 作为 quote
    (symbol_upper, "USDT".to_string())
}

pub(crate) fn okex_inst_id_from_symbol(
    symbol: &str,
    venue: TradingVenue,
) -> Result<String, String> {
    let symbol_upper = symbol.to_uppercase();

    if symbol_upper.contains('-') {
        return match venue {
            TradingVenue::OkexMargin => Ok(symbol_upper.replace("-SWAP", "")),
            TradingVenue::OkexFutures => {
                if symbol_upper.ends_with("-SWAP") {
                    Ok(symbol_upper)
                } else {
                    Ok(format!("{symbol_upper}-SWAP"))
                }
            }
            _ => Err(format!("venue {:?} not okex", venue)),
        };
    }

    let (base, quote) = extract_assets_from_symbol(&symbol_upper);
    match venue {
        TradingVenue::OkexMargin => Ok(format!("{base}-{quote}")),
        TradingVenue::OkexFutures => Ok(format!("{base}-{quote}-SWAP")),
        _ => Err(format!("venue {:?} not okex", venue)),
    }
}

fn bybit_symbol_from_symbol(symbol: &str) -> String {
    normalize_symbol_for_internal(symbol)
}

pub fn gate_currency_pair_from_symbol(symbol: &str) -> String {
    let mut upper = symbol.to_ascii_uppercase();
    if upper.contains("-SWAP") {
        upper = upper.replace("-SWAP", "");
    }
    if upper.contains('_') {
        return upper;
    }
    if upper.contains('-') {
        return upper.replace('-', "_");
    }
    let (base, quote) = extract_assets_from_symbol(&upper);
    format!("{base}_{quote}")
}

pub fn gate_text_from_client_order_id(client_order_id: i64) -> String {
    format!("t-{client_order_id}")
}

fn okex_order_type_from_order_type(order_type: OrderType) -> Result<OkexOrderType, String> {
    match order_type {
        OrderType::Market => Ok(OkexOrderType::Market),
        OrderType::Limit => Ok(OkexOrderType::PostOnly),
        _ => Err(format!("unsupported okex order type: {:?}", order_type)),
    }
}

use crate::common::binance_account_mode::BinanceAccountMode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Side {
    Buy = 1,  // 买入
    Sell = 2, // 卖出
}

impl Side {
    /// 从 u8 转换为 Side
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Side::Buy),
            2 => Some(Side::Sell),
            _ => None,
        }
    }

    /// 转换为 u8
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// 从字符串解析
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "buy" | "BUY" | "Buy" => Some(Side::Buy),
            "sell" | "SELL" | "Sell" => Some(Side::Sell),
            _ => None,
        }
    }

    /// 转换为字符串（大写格式，常用于 API）
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }

    /// 转换为小写字符串
    pub fn as_str_lower(&self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }

    /// 是否是买入
    pub fn is_buy(&self) -> bool {
        matches!(self, Side::Buy)
    }

    /// 是否是卖出
    pub fn is_sell(&self) -> bool {
        matches!(self, Side::Sell)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderExecutionStatus {
    Commit = 1,    // 构造未提交
    Create = 2,    // 已确认，待执行
    Filled = 3,    // 完全成交
    Cancelled = 4, // 已取消
    Rejected = 5,  // 被拒绝
}

impl OrderExecutionStatus {
    /// 从 u8 转换为 OrderExecutionStatus
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(OrderExecutionStatus::Commit),
            2 => Some(OrderExecutionStatus::Create),
            3 => Some(OrderExecutionStatus::Filled),
            4 => Some(OrderExecutionStatus::Cancelled),
            5 => Some(OrderExecutionStatus::Rejected),
            _ => None,
        }
    }

    /// 转换为 u8
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// 从字符串解析
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "CREATE" => Some(OrderExecutionStatus::Create),
            "COMMIT" => Some(OrderExecutionStatus::Commit),
            "FILLED" => Some(OrderExecutionStatus::Filled),
            "CANCELLED" => Some(OrderExecutionStatus::Cancelled),
            "REJECTED" => Some(OrderExecutionStatus::Rejected),
            _ => None,
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderExecutionStatus::Create => "CREATE",
            OrderExecutionStatus::Commit => "COMMIT",
            OrderExecutionStatus::Filled => "FILLED",
            OrderExecutionStatus::Cancelled => "CANCELLED",
            OrderExecutionStatus::Rejected => "REJECTED",
        }
    }

    /// 是否是终态（不会再变化的状态）
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            OrderExecutionStatus::Filled
                | OrderExecutionStatus::Cancelled
                | OrderExecutionStatus::Rejected
        )
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

const TRADE_UPDATE_QTY_EPS: f64 = 1e-9;

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
    /// 从 u8 转换为 OrderType
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(OrderType::Limit),
            3 => Some(OrderType::Market),
            4 => Some(OrderType::StopLoss),
            5 => Some(OrderType::StopLossLimit),
            6 => Some(OrderType::TakeProfit),
            7 => Some(OrderType::TakeProfitLimit),
            8 => Some(OrderType::StopMarket),
            9 => Some(OrderType::TakeProfitMarket),
            _ => None,
        }
    }
    pub fn to_u8(self) -> u8 {
        match self {
            OrderType::Limit => 1,
            OrderType::Market => 3,
            OrderType::StopLoss => 4,
            OrderType::StopLossLimit => 5,
            OrderType::TakeProfit => 6,
            OrderType::TakeProfitLimit => 7,
            OrderType::StopMarket => 8,
            OrderType::TakeProfitMarket => 9,
        }
    }

    /// 从字符串解析（交易所 API 格式）
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "LIMIT" => Some(OrderType::Limit),
            "MARKET" => Some(OrderType::Market),
            "STOP_LOSS" => Some(OrderType::StopLoss),
            "STOP_LOSS_LIMIT" => Some(OrderType::StopLossLimit),
            "TAKE_PROFIT" => Some(OrderType::TakeProfit),
            "TAKE_PROFIT_LIMIT" => Some(OrderType::TakeProfitLimit),
            "STOP_MARKET" => Some(OrderType::StopMarket),
            "TAKE_PROFIT_MARKET" => Some(OrderType::TakeProfitMarket),
            _ => None,
        }
    }

    /// 转换为字符串（交易所 API 格式）
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderType::Limit => "LIMIT",
            OrderType::Market => "MARKET",
            OrderType::StopLoss => "STOP_LOSS",
            OrderType::StopLossLimit => "STOP_LOSS_LIMIT",
            OrderType::TakeProfit => "TAKE_PROFIT",
            OrderType::TakeProfitLimit => "TAKE_PROFIT_LIMIT",
            OrderType::StopMarket => "STOP_MARKET",
            OrderType::TakeProfitMarket => "TAKE_PROFIT_MARKET",
        }
    }

    /// 是否是限价单类型
    pub fn is_limit(&self) -> bool {
        matches!(
            self,
            OrderType::Limit | OrderType::StopLossLimit | OrderType::TakeProfitLimit
        )
    }

    /// 是否是市价单类型
    pub fn is_market(&self) -> bool {
        matches!(
            self,
            OrderType::Market | OrderType::StopMarket | OrderType::TakeProfitMarket
        )
    }

    /// 是否是条件单（止损/止盈）
    pub fn is_conditional(&self) -> bool {
        !matches!(self, OrderType::Limit | OrderType::Market)
    }
}

/// 订单管理器
pub struct OrderManager {
    orders: HashMap<i64, Order>,                     //映射order id到order
    pending_limit_order_count: HashMap<String, i32>, //单个交易品种当前有多少待成交的maker单
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
            orders: HashMap::new(),
            pending_limit_order_count: HashMap::new(),
            binance_account_mode,
        }
    }

    pub fn binance_is_standard(&self) -> bool {
        self.binance_account_mode == Some(BinanceAccountMode::Standard)
    }

    pub fn build_unmatched_cancel_bytes(
        &self,
        venue: TradingVenue,
        symbol: &str,
        client_order_id: i64,
    ) -> Result<Bytes, String> {
        if client_order_id <= 0 {
            return Err(format!(
                "invalid unmatched client_order_id for cancel: {}",
                client_order_id
            ));
        }

        let params = Bytes::from(format!(
            "symbol={}&origClientOrderId={}",
            symbol, client_order_id
        ));
        match venue {
            TradingVenue::BinanceMargin => {
                if self.binance_is_standard() {
                    let request = BinanceWsCancelMarginOrderRequest::create(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceCancelMarginOrderRequest::create(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                }
            }
            TradingVenue::BinanceFutures => {
                if self.binance_is_standard() {
                    let request = BinanceWsCancelUMOrderRequest::create(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceCancelUMOrderRequest::create(
                        get_timestamp_us(),
                        client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                }
            }
            _ => Err(format!(
                "unmatched cancel fallback not supported for venue {:?}",
                venue
            )),
        }
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
        let Some(incoming_exec_status) = Self::map_update_status(incoming_status) else {
            return None;
        };

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

        if incoming_cum_qty < prev_cum - TRADE_UPDATE_QTY_EPS
            || (incoming_cum_qty - prev_cum).abs() <= TRADE_UPDATE_QTY_EPS
        {
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
        sumbit_ts_local: i64,
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
            sumbit_ts_local,
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
        sumbit_ts_local: i64,
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
        let mut order = Order::new(
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
        order.set_submit_time(sumbit_ts_local);
        self.insert(order);
        id
    }

    pub fn get_symbol_pending_limit_order_count(&self, symbol: &String) -> i32 {
        let symbol = normalize_symbol_for_internal(symbol);
        let actual = self
            .orders
            .values()
            .filter(|order| {
                order.order_type.is_limit()
                    && order.count_pending_limit
                    && !order.status.is_terminal()
                    && order.symbol.eq_ignore_ascii_case(&symbol)
            })
            .count() as i32;

        if let Some(stored) = self.pending_limit_order_count.get(&symbol) {
            if *stored != actual {
                debug!(
                    "OrderManager: symbol={} pending_limit_count inconsistent cached={} actual={} (using actual)",
                    symbol,
                    stored,
                    actual
                );
            }
        }

        actual
    }

    pub fn get_symbol_pending_limit_order_count_by_side(&self, symbol: &str, side: Side) -> i32 {
        let symbol = normalize_symbol_for_internal(symbol);
        self.orders
            .values()
            .filter(|order| {
                order.order_type.is_limit()
                    && order.count_pending_limit
                    && !order.status.is_terminal()
                    && order.side == side
                    && order.symbol.eq_ignore_ascii_case(&symbol)
            })
            .count() as i32
    }

    /// 添加订单
    pub fn insert(&mut self, order: Order) {
        let is_limit = order.order_type.is_limit();
        let count_pending_limit = order.count_pending_limit;
        let symbol = if is_limit && count_pending_limit {
            Some(order.symbol.clone())
        } else {
            None
        };

        let order_id = order.client_order_id;
        let prev = self.orders.insert(order_id, order);

        if let Some(symbol) = symbol {
            self.increment_pending_limit_count(&symbol);
        }

        if let Some(prev_order) = prev {
            if prev_order.order_type.is_limit() && prev_order.count_pending_limit {
                self.decrement_pending_limit_count(&prev_order.symbol);
            }
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
        warn!("价格:         {}", format_order_price(order.price));
        warn!("数量:         {}", format_order_quantity(order.quantity));
        warn!("数量乘数:     {:.8}", order.qty_multiplier);
        warn!("只减仓:       {}", order.reduce_only);
        warn!("成交量:       {:.8}", order.cumulative_filled_quantity);
        warn!("订单状态:     {:?}", order.status);
        warn!("提交时间:     {}", order.timestamp.submit_t);
        warn!("创建时间:     {}", order.timestamp.create_t);
        warn!("成交时间:     {}", order.timestamp.filled_t);
        warn!("结束时间:     {}", order.timestamp.end_t);
        warn!("═══════════════════════════════════════════════════════════════");
    }

    /// 根据订单ID获取订单的可变引用并执行操作
    pub fn update<F>(&mut self, order_id: i64, f: F) -> bool
    where
        F: FnOnce(&mut Order),
    {
        if let Some(order) = self.orders.get_mut(&order_id) {
            // 直接修改订单
            f(order);
            true
        } else {
            false
        }
    }

    /// 移除订单
    pub fn remove(&mut self, order_id: i64) -> Option<Order> {
        let removed = self.orders.remove(&order_id);

        if let Some(ref order) = removed {
            // 如果是限价单，减少计数
            if order.order_type.is_limit() && order.count_pending_limit {
                self.decrement_pending_limit_count(&order.symbol);
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
    }

    fn increment_pending_limit_count(&mut self, symbol: &str) {
        let symbol = normalize_symbol_for_internal(symbol);
        let entry = self.pending_limit_order_count.entry(symbol).or_insert(0);
        *entry += 1;
    }

    fn decrement_pending_limit_count(&mut self, symbol: &str) {
        let symbol = normalize_symbol_for_internal(symbol);
        let mut should_remove = false;
        let mut remaining = None;

        if let Some(entry) = self.pending_limit_order_count.get_mut(&symbol) {
            if *entry > 1 {
                *entry -= 1;
                remaining = Some(*entry);
            } else {
                should_remove = true;
            }
        } else {
            return;
        }

        if should_remove {
            self.pending_limit_order_count.remove(&symbol);
        }

        info!(
            "OrderManager: symbol={} pending_limit_count dec -> {}",
            symbol,
            remaining.unwrap_or(0)
        );
    }
}

#[derive(Debug, Clone)]
pub struct OrderTimeStamp {
    pub submit_t: i64, // 订单提交时间(本地时间)
    pub create_t: i64, // 交易所订单创建时间(交易所时间)
    pub filled_t: i64, // 订单执行成功的时间（交易所时间，记录最后一次）
    pub end_t: i64,    // 交易所时间(完全成交或者被撤单的时间)
}

impl OrderTimeStamp {
    fn new() -> Self {
        OrderTimeStamp {
            submit_t: 0,
            create_t: 0,
            filled_t: 0,
            end_t: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Order {
    pub venue: TradingVenue,             // 订单对应的交易标的
    pub client_order_id: i64,            // 订单ID
    pub order_type: OrderType,           // 订单类型
    pub symbol: String,                  // 交易对
    pub side: Side,                      // 买卖方向
    pub price: f64,                      // 限价单价格, 市价单没有意义
    pub quantity: f64,                   // 数量
    pub qty_multiplier: f64,             // 数量乘数（venue qty -> base qty）
    pub reduce_only: bool,               // 是否只减仓
    pub cumulative_filled_quantity: f64, // 成交量
    pub exchange_order_id: Option<i64>,  // 交易所返回的 orderId
    pub status: OrderExecutionStatus,    // 订单执行状态
    pub timestamp: OrderTimeStamp,
    pub count_pending_limit: bool, // 是否计入 pending-limit 风控统计
    binance_account_mode: Option<BinanceAccountMode>,
}

impl Order {
    /// 获取策略ID - 策略ID是订单ID的前32位
    pub fn get_strategy_id(&self) -> i32 {
        (self.client_order_id >> 32) as i32
    }

    /// 创建新订单
    fn new(
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
            qty_multiplier,
            reduce_only,
            status: OrderExecutionStatus::Commit,
            cumulative_filled_quantity: 0.0,
            exchange_order_id: None,
            timestamp: OrderTimeStamp::new(),
            count_pending_limit,
            binance_account_mode,
        }
    }

    fn require_binance_account_mode(&self) -> BinanceAccountMode {
        self.binance_account_mode.unwrap_or_else(|| {
            panic!("BINANCE_ACCOUNT_MODE must be set to 'UNIFIED' or 'STANDARD' when using binance-futures");
        })
    }

    /// 更新订单状态
    pub fn update_status(&mut self, status: OrderExecutionStatus) {
        // 增加订单状态检查
        if status == OrderExecutionStatus::Create {
            if self.status != OrderExecutionStatus::Commit {
                //出现非正常的状态切换，打印日志
                warn!("unexpected OrderExecutionStatus");
            }
        }
        self.status = status;
    }

    /// 设置提交时间
    pub fn set_submit_time(&mut self, time: i64) {
        self.timestamp.submit_t = time;
    }

    /// 设置执行时间
    pub fn set_create_time(&mut self, time: i64) {
        self.timestamp.create_t = time;
    }

    /// 设置成交时间
    pub fn set_filled_time(&mut self, time: i64) {
        self.timestamp.filled_t = time;
    }

    /// 设置结束时间
    pub fn set_end_time(&mut self, time: i64) {
        self.timestamp.end_t = time;
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

    pub fn get_order_cancel_bytes(&self) -> Result<Bytes, String> {
        let now = get_timestamp_us();
        match self.venue {
            TradingVenue::BinanceMargin => {
                // 使用 origClientOrderId 以客户端订单ID撤单；当前未保存交易所 orderId
                let params = Bytes::from(format!(
                    "symbol={}&origClientOrderId={}",
                    self.symbol, self.client_order_id
                ));
                if self.require_binance_account_mode() == BinanceAccountMode::Standard {
                    let request: BinanceWsCancelMarginOrderRequest =
                        BinanceWsCancelMarginOrderRequest::create(
                            now,
                            self.client_order_id,
                            params,
                        );
                    return Ok(request.to_bytes());
                }
                let request: BinanceCancelMarginOrderRequest =
                    BinanceCancelMarginOrderRequest::create(now, self.client_order_id, params);
                return Ok(request.to_bytes());
            }
            TradingVenue::BinanceFutures => {
                let params = Bytes::from(format!(
                    "symbol={}&origClientOrderId={}",
                    self.symbol, self.client_order_id
                ));
                if self.require_binance_account_mode() == BinanceAccountMode::Standard {
                    let request: BinanceWsCancelUMOrderRequest =
                        BinanceWsCancelUMOrderRequest::create(now, self.client_order_id, params);
                    return Ok(request.to_bytes());
                }
                let request: BinanceCancelUMOrderRequest =
                    BinanceCancelUMOrderRequest::create(now, self.client_order_id, params);
                return Ok(request.to_bytes());
            }
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                let inst_id = okex_inst_id_from_symbol(&self.symbol, self.venue)?;
                // Use a distinct request id to avoid overwriting inflight mapping for the original order.
                let mut cancel_req_id = now;
                if cancel_req_id == self.client_order_id {
                    cancel_req_id = cancel_req_id.saturating_add(1);
                }
                let params = OkexCancelOrderParams {
                    ord_id: self.exchange_order_id.unwrap_or(0),
                    cl_ord_id: self.client_order_id,
                    inst_id,
                };
                let request = match self.venue {
                    TradingVenue::OkexMargin => {
                        OkexCancelOrderRequest::create_margin(now, cancel_req_id, params)
                    }
                    TradingVenue::OkexFutures => {
                        OkexCancelOrderRequest::create_um(now, cancel_req_id, params)
                    }
                    _ => None,
                }
                .ok_or_else(|| "failed to build okex cancel request".to_string())?;
                Ok(request.to_bytes())
            }
            TradingVenue::GateMargin => {
                let currency_pair = gate_currency_pair_from_symbol(&self.symbol);
                let order_id = self
                    .exchange_order_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| gate_text_from_client_order_id(self.client_order_id));
                let req_param = json!({
                    "order_id": order_id,
                    "currency_pair": currency_pair,
                    "account": "unified",
                });
                let params = Bytes::from(req_param.to_string());
                let request =
                    GateUnifiedCancelOrderRequest::create(now, self.client_order_id, params);
                Ok(request.to_bytes())
            }
            TradingVenue::GateFutures => {
                let contract = gate_currency_pair_from_symbol(&self.symbol);
                let order_id = self
                    .exchange_order_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| gate_text_from_client_order_id(self.client_order_id));
                let req_param = json!({
                    "order_id": order_id,
                    "contract": contract,
                });
                let params = Bytes::from(req_param.to_string());
                let request =
                    GateFuturesCancelOrderRequest::create(now, self.client_order_id, params);
                Ok(request.to_bytes())
            }
            TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
                let symbol = bybit_symbol_from_symbol(&self.symbol);
                let params = BybitCancelOrderParams {
                    symbol,
                    order_link_id: self.client_order_id,
                };
                let request = match self.venue {
                    TradingVenue::BybitMargin => {
                        BybitCancelOrderRequest::create_margin(now, self.client_order_id, params)
                    }
                    TradingVenue::BybitFutures => {
                        BybitCancelOrderRequest::create_um(now, self.client_order_id, params)
                    }
                    _ => None,
                }
                .ok_or_else(|| "failed to build bybit cancel request".to_string())?;
                Ok(request.to_bytes())
            }
            TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
                let mut req_param = serde_json::Map::new();
                if let Some(order_id) = self.exchange_order_id.filter(|&id| id > 0) {
                    req_param.insert("orderId".to_string(), json!(order_id.to_string()));
                }
                req_param.insert(
                    "clientOid".to_string(),
                    json!(self.client_order_id.to_string()),
                );
                let params = Bytes::from(Value::Object(req_param).to_string());
                match self.venue {
                    TradingVenue::BitgetMargin => {
                        let request = BitgetMarginCancelOrderRequest::create(
                            now,
                            self.client_order_id,
                            params,
                        );
                        Ok(request.to_bytes())
                    }
                    TradingVenue::BitgetFutures => {
                        let request =
                            BitgetUmCancelOrderRequest::create(now, self.client_order_id, params);
                        Ok(request.to_bytes())
                    }
                    _ => unreachable!(),
                }
            }
            _ => Err(format!("Unsupported trading venue: {:?}", self.venue)),
        }
    }

    pub fn get_order_request_bytes(&self) -> Result<Bytes, String> {
        if self.order_type.is_limit() && self.price <= 0.0 {
            return Err(format!(
                "invalid limit price: price={:.8} order_type={:?} symbol={} client_order_id={}",
                self.price, self.order_type, self.symbol, self.client_order_id
            ));
        }

        match self.venue {
            //币安的杠杆账户下单
            TradingVenue::BinanceMargin => {
                let use_binance_ws_margin =
                    self.require_binance_account_mode() == BinanceAccountMode::Standard;
                let binance_margin_type = if use_binance_ws_margin && self.order_type.is_limit() {
                    "LIMIT_MAKER"
                } else {
                    self.order_type.as_str()
                };

                let mut params_parts = vec![
                    format!("symbol={}", self.symbol),
                    format!("side={}", self.side.as_str()), //下单方向确定就可以
                    format!("type={}", binance_margin_type),
                    format!("quantity={}", format_quantity(self.quantity)),
                    format!("newClientOrderId={}", self.client_order_id),
                ];
                if use_binance_ws_margin {
                    params_parts.push("newOrderRespType=FULL".to_string());
                }
                let local_create_ts = get_timestamp_us();
                // ===== 余额检查和日志记录 =====
                // 提取 base asset 和 quote asset
                let (base_asset, quote_asset) = extract_assets_from_symbol(&self.symbol);

                // 根据 side 确定需要检查的资产和所需金额
                let (check_asset, required_amount) = match self.side {
                    Side::Buy => {
                        // BUY: 需要 quote asset (USDT) 的余额
                        let required = self.quantity * self.price;
                        (quote_asset, required)
                    }
                    Side::Sell => {
                        // SELL: 需要 base asset 的余额
                        (base_asset, self.quantity)
                    }
                };

                // 从 MonitorChannel 获取 basic margin 余额（当前实现以净余额作为可用余额近似）
                use crate::pre_trade::monitor_channel::MonitorChannel;
                let available_balance =
                    MonitorChannel::instance().balance_position_for_venue(self.venue, &check_asset);

                // 余额判断：记录余额不足场景。STANDARD 模式走 spot ws-api，不传 sideEffectType。
                if available_balance < required_amount {
                    let borrow_amount = required_amount - available_balance;
                    if !(use_binance_ws_margin && self.side == Side::Sell) {
                        warn!(
                            "💰 余额不足将借币: 资产={} 需要={:.8} 可用={:.8} 需借={:.8} symbol={} side={:?} qty={} price={}",
                            check_asset, required_amount, available_balance, borrow_amount,
                            self.symbol,
                            self.side,
                            format_order_quantity(self.quantity),
                            format_order_price(self.price)
                        );
                    }
                    if !use_binance_ws_margin {
                        params_parts.push("sideEffectType=MARGIN_BUY".to_string());
                    } else {
                        info!(
                            "BinanceMargin STANDARD mode: omit sideEffectType for symbol={} side={:?}",
                            self.symbol, self.side
                        );
                    }
                } else {
                    info!(
                        "✅ 余额充足: 资产={} 需要={:.8} 可用={:.8} symbol={} side={:?}",
                        check_asset, required_amount, available_balance, self.symbol, self.side
                    );
                    // 余额充足，不添加 sideEffectType（默认 NO_SIDE_EFFECT）
                }
                // ===== 余额检查结束 =====/

                // WS margin: LIMIT_MAKER 作为 post-only，不传 tif。
                // REST margin: 保持原逻辑（LIMIT + GTC）。
                if self.order_type.is_limit() {
                    if !use_binance_ws_margin {
                        params_parts.push("timeInForce=GTC".to_string());
                    }
                    params_parts.push(format!("price={}", format_price(self.price)));
                }
                //如果是市价单，不需要价格和tif参数
                let params_plain = params_parts.join("&");
                info!(
                    "OrderManager: venue={:?} client_order_id={} params={}",
                    self.venue, self.client_order_id, params_plain
                );
                let params = Bytes::from(params_plain);
                if use_binance_ws_margin {
                    let request = BinanceWsNewMarginOrderRequest::create(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceNewMarginOrderRequest::create(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                }
            }
            TradingVenue::BinanceFutures => {
                let use_binance_ws_um =
                    self.require_binance_account_mode() == BinanceAccountMode::Standard;
                let mut params_parts = vec![
                    format!("symbol={}", self.symbol),
                    format!("side={}", self.side.as_str()), //下单方向确定就可以
                    format!("type={}", self.order_type.as_str()),
                    format!("quantity={}", format_quantity(self.quantity)),
                    format!("reduceOnly={}", self.reduce_only),
                    format!("newClientOrderId={}", self.client_order_id),
                ];
                if use_binance_ws_um {
                    params_parts.push(format!(
                        "newOrderRespType={}",
                        binance_ws_um_new_order_resp_type()
                    ));
                }
                let local_create_ts = get_timestamp_us();
                //UM合约下单
                if self.order_type.is_limit() {
                    params_parts.push("timeInForce=GTX".to_string());
                    params_parts.push(format!("price={}", format_price(self.price)));
                }
                let params_plain = params_parts.join("&");
                info!(
                    "OrderManager: venue={:?} client_order_id={} params={}",
                    self.venue, self.client_order_id, params_plain
                );
                let params = Bytes::from(params_plain);
                if use_binance_ws_um {
                    let request = BinanceWsNewUMOrderRequest::create(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                } else {
                    let request = BinanceNewUMOrderRequest::create(
                        local_create_ts,
                        self.client_order_id,
                        params,
                    );
                    Ok(request.to_bytes())
                }
            }
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                let create_ts = get_timestamp_us();
                let inst_id = okex_inst_id_from_symbol(&self.symbol, self.venue)?;
                let okex_order_type = okex_order_type_from_order_type(self.order_type)?;
                let quantity_qv = QuantizedValue::from_decimal(self.quantity).ok_or_else(|| {
                    format!(
                        "failed to quantize okex quantity: qty={:.12} symbol={} client_order_id={}",
                        self.quantity, self.symbol, self.client_order_id
                    )
                })?;
                let price_qv = if self.order_type.is_limit() {
                    QuantizedValue::from_decimal(self.price).ok_or_else(|| {
                        format!(
                            "failed to quantize okex price: price={:.12} symbol={} client_order_id={}",
                            self.price, self.symbol, self.client_order_id
                        )
                    })?
                } else {
                    QuantizedValue::zero()
                };

                let params = OkexNewOrderParams {
                    side: self.side,
                    order_type: okex_order_type,
                    quantity_qv,
                    price_qv,
                    symbol: inst_id,
                    reduce_only: self.reduce_only,
                    client_order_id: self.client_order_id,
                };

                let request = match self.venue {
                    TradingVenue::OkexMargin => {
                        OkexNewOrderRequest::create_margin(create_ts, self.client_order_id, params)
                    }
                    TradingVenue::OkexFutures => {
                        OkexNewOrderRequest::create_um(create_ts, self.client_order_id, params)
                    }
                    _ => None,
                }
                .ok_or_else(|| "failed to build okex new order request".to_string())?;
                Ok(request.to_bytes())
            }
            TradingVenue::GateMargin => {
                let create_ts = get_timestamp_us();
                let currency_pair = gate_currency_pair_from_symbol(&self.symbol);
                let order_type = match self.order_type {
                    OrderType::Limit => "limit",
                    OrderType::Market => "market",
                    _ => {
                        return Err(format!(
                            "unsupported gate order type: {:?}",
                            self.order_type
                        ));
                    }
                };
                let time_in_force = match self.order_type {
                    OrderType::Limit => Some("poc"),
                    OrderType::Market => None,
                    _ => None,
                };

                let mut req_param = serde_json::Map::new();
                req_param.insert("text".to_string(), json!(self.client_order_id.to_string()));
                req_param.insert("currency_pair".to_string(), json!(currency_pair));
                req_param.insert("type".to_string(), json!(order_type));
                req_param.insert("account".to_string(), json!("unified"));
                req_param.insert("auto_borrow".to_string(), json!(true));
                req_param.insert("side".to_string(), json!(self.side.as_str_lower()));
                req_param.insert("amount".to_string(), json!(format_quantity(self.quantity)));
                if self.order_type.is_limit() {
                    req_param.insert("price".to_string(), json!(format_price(self.price)));
                }
                if let Some(tif) = time_in_force {
                    req_param.insert("time_in_force".to_string(), json!(tif));
                }

                let params = Bytes::from(Value::Object(req_param).to_string());
                let request =
                    GateUnifiedNewOrderRequest::create(create_ts, self.client_order_id, params);
                Ok(request.to_bytes())
            }
            TradingVenue::GateFutures => {
                let create_ts = get_timestamp_us();
                let contract = gate_currency_pair_from_symbol(&self.symbol);
                let time_in_force = match self.order_type {
                    OrderType::Limit => Some("poc"),
                    OrderType::Market => Some("ioc"),
                    _ => None,
                };

                let signed_size = match self.side {
                    Side::Buy => self.quantity,
                    Side::Sell => -self.quantity,
                };

                let mut req_param = serde_json::Map::new();
                req_param.insert("text".to_string(), json!(self.client_order_id.to_string()));
                req_param.insert("contract".to_string(), json!(contract));
                req_param.insert("account".to_string(), json!("unified"));
                req_param.insert(
                    "size".to_string(),
                    json!(format_signed_quantity(signed_size)),
                );

                if self.order_type.is_limit() {
                    req_param.insert("price".to_string(), json!(format_price(self.price)));
                } else {
                    req_param.insert("price".to_string(), json!("0"));
                }
                if let Some(tif) = time_in_force {
                    req_param.insert("tif".to_string(), json!(tif));
                }
                if self.reduce_only {
                    req_param.insert("reduce_only".to_string(), json!(true));
                }

                let params = Bytes::from(Value::Object(req_param).to_string());
                let request =
                    GateFuturesNewOrderRequest::create(create_ts, self.client_order_id, params);
                Ok(request.to_bytes())
            }
            TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
                let create_ts = get_timestamp_us();
                let symbol = bybit_symbol_from_symbol(&self.symbol);
                let quantity_qv = QuantizedValue::from_decimal(self.quantity).ok_or_else(|| {
                    format!(
                        "failed to quantize bybit quantity: qty={:.12} symbol={} client_order_id={}",
                        self.quantity, self.symbol, self.client_order_id
                    )
                })?;
                let price_qv = if self.order_type.is_limit() {
                    QuantizedValue::from_decimal(self.price).ok_or_else(|| {
                        format!(
                            "failed to quantize bybit price: price={:.12} symbol={} client_order_id={}",
                            self.price, self.symbol, self.client_order_id
                        )
                    })?
                } else {
                    QuantizedValue::zero()
                };
                let params = BybitNewOrderParams {
                    side: self.side,
                    order_type: self.order_type,
                    reduce_only: self.reduce_only,
                    is_leverage: matches!(self.venue, TradingVenue::BybitMargin),
                    quantity_qv,
                    price_qv,
                    symbol,
                };
                let request = match self.venue {
                    TradingVenue::BybitMargin => {
                        BybitNewOrderRequest::create_margin(create_ts, self.client_order_id, params)
                    }
                    TradingVenue::BybitFutures => {
                        BybitNewOrderRequest::create_um(create_ts, self.client_order_id, params)
                    }
                    _ => None,
                }
                .ok_or_else(|| "failed to build bybit new order request".to_string())?;
                Ok(request.to_bytes())
            }
            TradingVenue::BitgetMargin => {
                let create_ts = get_timestamp_us();
                let mut req_param = serde_json::Map::new();
                req_param.insert("category".to_string(), json!("spot"));
                req_param.insert(
                    "symbol".to_string(),
                    json!(self.symbol.to_ascii_uppercase()),
                );
                req_param.insert("side".to_string(), json!(self.side.as_str_lower()));
                req_param.insert(
                    "orderType".to_string(),
                    json!(if self.order_type.is_limit() {
                        "limit"
                    } else {
                        "market"
                    }),
                );
                req_param.insert("force".to_string(), json!("post_only"));
                req_param.insert("price".to_string(), json!(format_price(self.price)));
                req_param.insert("size".to_string(), json!(format_quantity(self.quantity)));
                req_param.insert(
                    "clientOid".to_string(),
                    json!(self.client_order_id.to_string()),
                );
                let params = Bytes::from(Value::Object(req_param).to_string());
                let request =
                    BitgetMarginNewOrderRequest::create(create_ts, self.client_order_id, params);
                Ok(request.to_bytes())
            }
            TradingVenue::BitgetFutures => {
                let create_ts = get_timestamp_us();
                let mut req_param = serde_json::Map::new();
                req_param.insert("category".to_string(), json!("usdt-futures"));
                req_param.insert(
                    "symbol".to_string(),
                    json!(self.symbol.to_ascii_uppercase()),
                );
                req_param.insert("side".to_string(), json!(self.side.as_str_lower()));
                req_param.insert(
                    "orderType".to_string(),
                    json!(if self.order_type.is_limit() {
                        "limit"
                    } else {
                        "market"
                    }),
                );
                req_param.insert("force".to_string(), json!("post_only"));
                req_param.insert("price".to_string(), json!(format_price(self.price)));
                req_param.insert("size".to_string(), json!(format_quantity(self.quantity)));
                req_param.insert(
                    "clientOid".to_string(),
                    json!(self.client_order_id.to_string()),
                );
                if self.reduce_only {
                    req_param.insert("reduceOnly".to_string(), json!("YES"));
                }
                let params = Bytes::from(Value::Object(req_param).to_string());
                let request =
                    BitgetUmNewOrderRequest::create(create_ts, self.client_order_id, params);
                Ok(request.to_bytes())
            }
            //之后在这支持别的类型下单，根据资产类型决定下单的request，统一序列化为bytes
            _ => Err(format!("Unsupported trading venue: {:?}", self.venue)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Order, OrderExecutionStatus, OrderManager, OrderStatus, OrderType, Side,
        TradeUpdateSkipReason,
    };
    use crate::signal::common::TradingVenue;
    use serde_json::Value;

    fn extract_request_json(bytes: &[u8]) -> Value {
        serde_json::from_slice(&bytes[24..]).expect("gate request params should be valid json")
    }

    #[test]
    fn duplicate_partial_trade_is_skipped_by_cumulative_qty_even_with_newer_ts() {
        let mut order = Order::new(
            TradingVenue::BybitFutures,
            42,
            OrderType::Limit,
            "ETHUSDT".to_string(),
            Side::Buy,
            0.04,
            2300.0,
            false,
            1.0,
            None,
            true,
        );
        order.status = OrderExecutionStatus::Create;
        order.cumulative_filled_quantity = 0.04;
        order.timestamp.filled_t = 1_000;

        let skip = OrderManager::should_skip_idempotent_trade_update(
            &order,
            OrderStatus::PartiallyFilled,
            0.04,
            9_999,
            "test",
            1,
        );

        assert_eq!(skip, Some(TradeUpdateSkipReason::StaleOrDuplicatePartial));
    }

    #[test]
    fn gate_futures_sell_order_serializes_negative_size() {
        let order = Order::new(
            TradingVenue::GateFutures,
            42,
            OrderType::Limit,
            "SOLUSDT".to_string(),
            Side::Sell,
            3.0,
            88.56,
            true,
            1.0,
            None,
            true,
        );

        let bytes = order
            .get_order_request_bytes()
            .expect("gate futures request should build");
        let payload = extract_request_json(bytes.as_ref());

        assert_eq!(
            payload.get("contract").and_then(Value::as_str),
            Some("SOL_USDT")
        );
        assert_eq!(payload.get("size").and_then(Value::as_str), Some("-3"));
        assert_eq!(payload.get("price").and_then(Value::as_str), Some("88.56"));
        assert_eq!(payload.get("tif").and_then(Value::as_str), Some("poc"));
        assert_eq!(
            payload.get("reduce_only").and_then(Value::as_bool),
            Some(true)
        );
    }
}
