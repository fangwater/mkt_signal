use std::collections::HashMap;

use log::{debug, info, warn};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

        let order_id = order.order_id;
        let prev = self.orders.insert(order_id, order);

        if let Some(symbol) = symbol {
            self.increment_pending_limit_count(&symbol);
            if let Some(o) = self.orders.get_mut(&order_id) {
                o.pending_counted = true;
            }
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

    /// 根据订单ID获取订单的可变引用并执行操作
    pub fn update<F>(&mut self, order_id: i64, f: F) -> bool
    where
        F: FnOnce(&mut Order),
    {
        // 先在受限作用域内完成对订单本身的修改，并计算需要对计数器执行的动作，
        // 避免在持有 self.orders 的可变借用时再次可变借用 self（触发 E0499）。
        let mut dec_prev = false;
        let mut inc_curr = false;
        let mut dec_on_fill = false;
        let mut prev_symbol = String::new();
        let mut curr_symbol = String::new();
        let mut existed = false;

        {
            if let Some(order) = self.orders.get_mut(&order_id) {
                existed = true;
                let previous_type = order.order_type;
                prev_symbol = order.symbol.clone();
                let previous_status = order.status;

                // 交给调用方修改订单
                f(order);

                let current_type = order.order_type;
                curr_symbol = order.symbol.clone();

                // 1) 由限价 -> 非限价 或者 symbol 变更时，减少旧 symbol 的 pending 计数
                if previous_type.is_limit()
                    && (!current_type.is_limit() || prev_symbol != curr_symbol)
                {
                    dec_prev = true;
                }

                // 2) 由非限价 -> 限价 或者 symbol 变更时，增加新 symbol 的 pending 计数
                if current_type.is_limit()
                    && (!previous_type.is_limit() || prev_symbol != curr_symbol)
                {
                    inc_curr = true;
                }

                // 计算本次状态迁移后（执行 1/2 后）是否应被计入 pending
                let mut will_be_counted = order.pending_counted;
                if dec_prev {
                    will_be_counted = false;
                }
                if inc_curr {
                    will_be_counted = true;
                }

                // 3) 限价单在转为 Filled 时释放计数（仅当当前处于计数状态时）
                if current_type.is_limit()
                    && previous_status != OrderExecutionStatus::Filled
                    && order.status == OrderExecutionStatus::Filled
                    && will_be_counted
                {
                    dec_on_fill = true;
                    will_be_counted = false;
                }

                // 最终同步到订单本身
                order.pending_counted = will_be_counted;
            }
        }

        if !existed {
            return false;
        }

        // 现在已不再持有对 self.orders 的可变借用，可以安全更新计数器
        if dec_prev {
            self.decrement_pending_limit_count(&prev_symbol);
        }
        if inc_curr {
            self.increment_pending_limit_count(&curr_symbol);
        }
        if dec_on_fill {
            self.decrement_pending_limit_count(&curr_symbol);
        }

        true
    }

    /// 移除订单
    pub fn remove(&mut self, order_id: i64) -> Option<Order> {
        let removed = self.orders.remove(&order_id);

        if let Some(ref order) = removed {
            if order.order_type.is_limit() && order.pending_counted {
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

impl Default for OrderManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: i64,                   // 订单ID
    pub order_type: OrderType,           // 订单类型
    pub symbol: String,                  // 交易对
    pub side: Side,                      // 买卖方向
    pub price: f64,                      // 限价单价格, 市价单没有意义
    pub quantity: f64,                   // 数量
    pub cumulative_filled_quantity: f64, // 成交量
    pub hedged_quantily: f64,            // 未对冲量
    #[serde(default)]
    pub hedged_filled_quantity: f64, // 已对冲到期货端的成交量
    #[serde(default)]
    pub exchange_order_id: Option<i64>, // 交易所返回的 orderId
    #[serde(default)]
    pub update_event_times: Vec<i64>, // 交易所成交/状态更新时间戳列表
    pub status: OrderExecutionStatus,    // 订单执行状态
    #[serde(default)]
    pub pending_counted: bool, // 是否已计入 pending 限价单 count（用于去重防二次递减）
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
            hedged_filled_quantity: 0.0,
            exchange_order_id: None,
            update_event_times: Vec::new(),
            pending_counted: order_type.is_limit(),
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

    /// 更新已对冲成交量（部分对冲使用）
    pub fn update_hedged_filled_quantity(&mut self, qty: f64) {
        self.hedged_filled_quantity = qty;
    }

    /// 设置提交时间
    pub fn set_submit_time(&mut self, time: i64) {
        self.submit_time = time;
    }

    /// 设置执行时间
    pub fn set_create_time(&mut self, time: i64) {
        self.record_exchange_create(time);
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

    pub fn set_exchange_order_id(&mut self, exchange_order_id: i64) {
        if exchange_order_id > 0 {
            self.exchange_order_id = Some(exchange_order_id);
        }
    }

    pub fn record_exchange_create(&mut self, time: i64) {
        if time > 0 && self.create_time == 0 {
            self.create_time = time;
        }
    }

    pub fn record_exchange_update(&mut self, time: i64) {
        if time <= 0 {
            return;
        }
        if self.update_event_times.last().copied() == Some(time) {
            return;
        }
        self.update_event_times.push(time);
    }
}
