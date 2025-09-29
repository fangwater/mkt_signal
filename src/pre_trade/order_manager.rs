use std::collections::HashMap;

use log::{debug, warn};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PositionSide {
    BOTH = 1,
    SHORT = 2,
    LONG = 3,
}

impl PositionSide {
    /// 从 u8 转换为 PositionSide
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(PositionSide::BOTH),
            2 => Some(PositionSide::SHORT),
            3 => Some(PositionSide::LONG),
            _ => None,
        }
    }

    /// 从字符串解析（交易所 API 格式）
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "BOTH" => Some(PositionSide::BOTH),
            "SHORT" => Some(PositionSide::SHORT),
            "LONG" => Some(PositionSide::LONG),
            _ => None,
        }
    }

    /// 转换为字符串（交易所 API 格式）
    pub fn as_str(&self) -> &'static str {
        match self {
            PositionSide::BOTH => "BOTH",
            PositionSide::SHORT => "SHORT",
            PositionSide::LONG => "LONG",
        }
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

    pub fn get_symbol_pending_limit_order_count(&self, symbol: &String) -> i32 {
        self.pending_limit_order_count
            .get(symbol)
            .copied()
            .unwrap_or(0)
    }

    /// 添加订单
    pub fn insert(&mut self, order: Order) {
        let is_limit = order.order_type.is_limit();
        let symbol = if is_limit {
            Some(order.symbol.clone())
        } else {
            None
        };

        let prev = self.orders.insert(order.order_id, order);

        if let Some(symbol) = symbol {
            self.increment_pending_limit_count(&symbol);
        }

        if let Some(prev_order) = prev {
            if prev_order.order_type.is_limit() {
                self.decrement_pending_limit_count(&prev_order.symbol);
            }
        }
    }

    /// 根据订单ID获取订单
    pub fn get(&self, order_id: i64) -> Option<Order> {
        self.orders.get(&order_id).cloned()
    }

    /// 根据订单ID获取订单的可变引用并执行操作
    pub fn update<F>(&mut self, order_id: i64, f: F) -> bool
    where
        F: FnOnce(&mut Order),
    {
        if let Some(order) = self.orders.get_mut(&order_id) {
            let previous_type = order.order_type;
            let previous_symbol = order.symbol.clone();

            f(order);

            let current_type = order.order_type;
            let current_symbol = order.symbol.clone();

            if previous_type.is_limit()
                && (!current_type.is_limit() || previous_symbol != current_symbol)
            {
                self.decrement_pending_limit_count(&previous_symbol);
            }

            if current_type.is_limit()
                && (!previous_type.is_limit() || previous_symbol != current_symbol)
            {
                self.increment_pending_limit_count(&current_symbol);
            }

            true
        } else {
            false
        }
    }

    /// 移除订单
    pub fn remove(&mut self, order_id: i64) -> Option<Order> {
        let removed = self.orders.remove(&order_id);

        if let Some(ref order) = removed {
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
        let before = *entry;
        *entry += 1;
        debug!(
            "OrderManager: symbol={} pending_limit_count {} -> {} (inc)",
            symbol,
            before,
            *entry
        );
    }

    fn decrement_pending_limit_count(&mut self, symbol: &str) {
        if let Some(entry) = self.pending_limit_order_count.get_mut(symbol) {
            let before = *entry;
            if *entry > 1 {
                *entry -= 1;
                debug!(
                    "OrderManager: symbol={} pending_limit_count {} -> {} (dec)",
                    symbol,
                    before,
                    *entry
                );
            } else {
                self.pending_limit_order_count.remove(symbol);
                debug!(
                    "OrderManager: symbol={} pending_limit_count {} -> 0 (remove entry)",
                    symbol,
                    before
                );
            }
        } else {
            debug!(
                "OrderManager: symbol={} pending_limit_count not found when decrement",
                symbol
            );
        }
    }
}

impl Default for OrderManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct Order {
    pub order_id: i64,                   // 订单ID
    pub order_type: OrderType,           // 订单类型
    pub symbol: String,                  // 交易对
    pub side: Side,                      // 买卖方向
    pub price: f64,                      // 限价单价格, 市价单没有意义
    pub quantity: f64,                   // 数量
    pub cumulative_filled_quantity: f64, // 成交量
    pub hedged_quantily: f64,            // 未对冲量
    pub status: OrderExecutionStatus,    // 订单执行状态
    // 时间戳记录
    pub submit_time: i64, // 订单提交时间(本地时间)
    // "O": 1499405658657,            // Order creation time 对应币安杠杆下单
    pub create_time: i64, // 交易所订单创建时间(交易所时间)
    pub ack_time: i64,    // 订单收到交易所回报的时间(本地时间)
    pub filled_time: i64, // 订单执行成功的时间（交易所时间，记录最后一次）
    pub end_time: i64,    // 收到成交回报的时间（本地时间，记录最后一次）
}

impl Order {
    /// 获取策略ID - 策略ID是订单ID的前32位
    pub fn get_strategy_id(&self) -> i32 {
        (self.order_id >> 32) as i32
    }

    /// 创建新订单
    pub fn new(
        order_id: i64,
        order_type: OrderType,
        symbol: String,
        side: Side,
        quantity: f64,
        price: f64,
    ) -> Self {
        Order {
            order_id,
            order_type,
            symbol,
            side,
            price,
            quantity,
            status: OrderExecutionStatus::Commit,
            submit_time: 0,
            create_time: 0,
            ack_time: 0,
            filled_time: 0,
            end_time: 0,
            cumulative_filled_quantity: 0.0,
            hedged_quantily: 0.0,
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
    /// 更新订单累计成交量
    pub fn update_cumulative_filled_quantity(&mut self, qty: f64) {
        self.cumulative_filled_quantity = qty;
        self.hedged_quantily = self.quantity - self.cumulative_filled_quantity;
    }

    /// 设置提交时间
    pub fn set_submit_time(&mut self, time: i64) {
        self.submit_time = time;
    }

    /// 设置执行时间
    pub fn set_create_time(&mut self, time: i64) {
        self.create_time = time;
    }

    /// 设置确认时间
    pub fn set_ack_time(&mut self, time: i64) {
        self.ack_time = time;
    }

    /// 设置成交时间
    pub fn set_filled_time(&mut self, time: i64) {
        self.filled_time = time;
    }

    /// 设置结束时间
    pub fn set_end_time(&mut self, time: i64) {
        self.end_time = time;
    }
}
