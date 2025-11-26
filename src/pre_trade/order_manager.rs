use crate::trade_engine::trade_request::BinanceNewMarginOrderRequest;
use crate::trade_engine::trade_request::BinanceNewUMOrderRequest;
use crate::trade_engine::trade_request::{
    BinanceCancelMarginOrderRequest, BinanceCancelUMOrderRequest,
};
use crate::{common::time_util::get_timestamp_us, signal::common::TradingVenue};
use bytes::Bytes;
use log::{debug, info, warn};
use std::collections::HashMap;
fn format_decimal(value: f64) -> String {
    let mut s = format!("{:.8}", value);
    if let Some(dot_pos) = s.find('.') {
        while s.len() > dot_pos + 1 && s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() {
        "0".to_string()
    } else {
        s
    }
}

fn format_quantity(quantity: f64) -> String {
    format_decimal(quantity)
}

fn format_price(price: f64) -> String {
    format_decimal(price)
}

/// 从交易对符号中提取 base asset 和 quote asset
/// 例如: "BTCUSDT" -> ("BTC", "USDT")
fn extract_assets_from_symbol(symbol: &str) -> (String, String) {
    let symbol_upper = symbol.to_uppercase();
    const QUOTE_ASSETS: [&str; 6] = ["USDT", "USDC", "BUSD", "FDUSD", "BIDR", "TRY"];

    for quote in QUOTE_ASSETS {
        if symbol_upper.ends_with(quote) && symbol_upper.len() > quote.len() {
            let base = &symbol_upper[..symbol_upper.len() - quote.len()];
            return (base.to_string(), quote.to_string());
        }
    }

    // 如果没有匹配到已知的 quote asset，默认返回整个符号作为 base，USDT 作为 quote
    (symbol_upper, "USDT".to_string())
}

use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderType {
    Limit = 1,
    LimitMaker = 2,
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
            2 => Some(OrderType::LimitMaker),
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
            OrderType::LimitMaker => 2,
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
            "LIMIT_MAKER" => Some(OrderType::LimitMaker),
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
            OrderType::LimitMaker => "LIMIT_MAKER",
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
            OrderType::Limit
                | OrderType::LimitMaker
                | OrderType::StopLossLimit
                | OrderType::TakeProfitLimit
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
        !matches!(
            self,
            OrderType::Limit | OrderType::LimitMaker | OrderType::Market
        )
    }
}

/// 订单管理器
pub struct OrderManager {
    orders: HashMap<i64, Order>,                     //映射order id到order
    pending_limit_order_count: HashMap<String, i32>, //单个交易品种当前有多少待成交的maker单
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            pending_limit_order_count: HashMap::new(),
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
        sumbit_ts_local: i64,
    ) -> i64 {
        let mut order = Order::new(venue, id, order_type, symbol.clone(), side, quantity, price);
        order.set_submit_time(sumbit_ts_local);
        self.insert(order);
        id
    }

    pub fn get_symbol_pending_limit_order_count(&self, symbol: &String) -> i32 {
        let actual = self
            .orders
            .values()
            .filter(|order| {
                order.order_type.is_limit()
                    && !order.status.is_terminal()
                    && order.symbol.eq_ignore_ascii_case(symbol)
            })
            .count() as i32;

        if let Some(stored) = self.pending_limit_order_count.get(symbol) {
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

    /// 添加订单
    pub fn insert(&mut self, order: Order) {
        let is_limit = order.order_type.is_limit();
        let symbol = if is_limit {
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
            if prev_order.order_type.is_limit() {
                self.decrement_pending_limit_count(&prev_order.symbol);
            }
        }

        // 持久化
    }

    /// 根据订单ID获取订单
    pub fn get(&self, order_id: i64) -> Option<Order> {
        self.orders.get(&order_id).cloned()
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
        warn!("价格:         {:.8}", order.price);
        warn!("数量:         {:.8}", order.quantity);
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
            if order.order_type.is_limit() {
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
        let entry = self
            .pending_limit_order_count
            .entry(symbol.to_string())
            .or_insert(0);
        *entry += 1;
    }

    fn decrement_pending_limit_count(&mut self, symbol: &str) {
        let mut should_remove = false;
        let mut remaining = None;

        if let Some(entry) = self.pending_limit_order_count.get_mut(symbol) {
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
            self.pending_limit_order_count.remove(symbol);
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
    pub cumulative_filled_quantity: f64, // 成交量
    pub exchange_order_id: Option<i64>,  // 交易所返回的 orderId
    pub status: OrderExecutionStatus,    // 订单执行状态
    pub timestamp: OrderTimeStamp,
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
    ) -> Self {
        Order {
            venue,
            client_order_id,
            order_type,
            symbol,
            side,
            price,
            quantity,
            status: OrderExecutionStatus::Commit,
            cumulative_filled_quantity: 0.0,
            exchange_order_id: None,
            timestamp: OrderTimeStamp::new(),
        }
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
        if exchange_order_id > 0 {
            self.exchange_order_id = Some(exchange_order_id);
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
                let request: BinanceCancelMarginOrderRequest =
                    BinanceCancelMarginOrderRequest::create(now, self.client_order_id, params);
                return Ok(request.to_bytes());
            }
            TradingVenue::BinanceUm => {
                let params = Bytes::from(format!(
                    "symbol={}&origClientOrderId={}",
                    self.symbol, self.client_order_id
                ));
                let request: BinanceCancelUMOrderRequest =
                    BinanceCancelUMOrderRequest::create(now, self.client_order_id, params);
                return Ok(request.to_bytes());
            }
            _ => Err(format!("Unsupported trading venue: {:?}", self.venue)),
        }
    }

    pub fn get_order_request_bytes(&self) -> Result<Bytes, String> {
        match self.venue {
            //币安的杠杆账户下单
            TradingVenue::BinanceMargin | TradingVenue::BinanceUm => {
                let mut params_parts = vec![
                    format!("symbol={}", self.symbol),
                    format!("side={}", self.side.as_str()), //下单方向确定就可以
                    format!("type={}", self.order_type.as_str()),
                    format!("quantity={}", format_quantity(self.quantity)),
                    format!("newClientOrderId={}", self.client_order_id),
                ];
                let local_create_ts = get_timestamp_us();
                if self.venue == TradingVenue::BinanceMargin {
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

                    // 从 MonitorChannel 获取 spot_manager 并检查余额
                    use crate::pre_trade::monitor_channel::MonitorChannel;
                    let spot_mgr = MonitorChannel::instance().spot_manager();
                    let available_balance = {
                        let mgr = spot_mgr.borrow();
                        if let Some(snapshot) = mgr.snapshot() {
                            snapshot
                                .balances
                                .iter()
                                .find(|b| b.asset.eq_ignore_ascii_case(&check_asset))
                                .map(|b| b.cross_margin_free)
                                .unwrap_or(0.0)
                        } else {
                            0.0
                        }
                    };

                    // 余额判断：决定是否需要借币
                    if available_balance < required_amount {
                        let borrow_amount = required_amount - available_balance;
                        warn!(
                            "💰 余额不足将借币: 资产={} 需要={:.8} 可用={:.8} 需借={:.8} symbol={} side={:?} qty={:.4} price={:.6}",
                            check_asset, required_amount, available_balance, borrow_amount,
                            self.symbol, self.side, self.quantity, self.price
                        );
                        // 余额不足，使用 MARGIN_BUY（有额度就买）
                        params_parts.push("sideEffectType=MARGIN_BUY".to_string());
                    } else {
                        info!(
                            "✅ 余额充足: 资产={} 需要={:.8} 可用={:.8} symbol={} side={:?}",
                            check_asset, required_amount, available_balance, self.symbol, self.side
                        );
                        // 余额充足，不添加 sideEffectType（默认 NO_SIDE_EFFECT）
                    }
                    // ===== 余额检查结束 =====/

                    //margin下单不支持GTX模式，无论是否要作为maker，都是gtc
                    if self.order_type.is_limit() {
                        params_parts.push("timeInForce=GTC".to_string());
                        params_parts.push(format!("price={}", format_price(self.price)));
                    }
                    //如果是市价单，不需要价格和tif参数
                } else {
                    //UM合约下单
                    if self.order_type.is_limit() {
                        params_parts.push("timeInForce=GTX".to_string());
                        params_parts.push(format!("price={}", format_price(self.price)));
                    }
                }
                let params_plain = params_parts.join("&");
                info!(
                    "OrderManager: venue={:?} client_order_id={} params={}",
                    self.venue, self.client_order_id, params_plain
                );
                let params = Bytes::from(params_plain);
                if self.venue == TradingVenue::BinanceMargin {
                    let request = BinanceNewMarginOrderRequest::create(
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
            //之后在这支持别的类型下单，根据资产类型决定下单的request，统一序列化为bytes
            _ => Err(format!("Unsupported trading venue: {:?}", self.venue)),
        }
    }
}
