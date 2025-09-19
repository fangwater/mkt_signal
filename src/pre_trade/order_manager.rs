use std::collections::HashMap;
use std::cell::RefCell;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Side {
    Buy = 1,   // 买入
    Sell = 2,  // 卖出
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
    Create = 0,    // 构造未提交
    Commit = 1,    // 已提交，待确认
    Acked = 2,     // 已确认，待执行
    Filled = 3,    // 完全成交
    Cancelled = 4, // 已取消
    Rejected = 5,  // 被拒绝
}

impl OrderExecutionStatus {
    /// 从 u8 转换为 OrderExecutionStatus
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(OrderExecutionStatus::Create),
            1 => Some(OrderExecutionStatus::Commit),
            2 => Some(OrderExecutionStatus::Acked),
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
            "ACKED" => Some(OrderExecutionStatus::Acked),
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
            OrderExecutionStatus::Acked => "ACKED",
            OrderExecutionStatus::Filled => "FILLED",
            OrderExecutionStatus::Cancelled => "CANCELLED",
            OrderExecutionStatus::Rejected => "REJECTED",
        }
    }

    /// 是否是终态（不会再变化的状态）
    pub fn is_terminal(&self) -> bool {
        matches!(self, 
            OrderExecutionStatus::Filled | 
            OrderExecutionStatus::Cancelled | 
            OrderExecutionStatus::Rejected
        )
    }

    /// 是否是活跃状态（可能还会变化）
    pub fn is_active(&self) -> bool {
        matches!(self, 
            OrderExecutionStatus::Commit | 
            OrderExecutionStatus::Acked
        )
    }

    /// 是否成功完成
    pub fn is_success(&self) -> bool {
        matches!(self, OrderExecutionStatus::Filled)
    }

    /// 是否失败
    pub fn is_failed(&self) -> bool {
        matches!(self, 
            OrderExecutionStatus::Cancelled | 
            OrderExecutionStatus::Rejected
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
        matches!(self, 
            OrderType::Limit | 
            OrderType::LimitMaker | 
            OrderType::StopLossLimit | 
            OrderType::TakeProfitLimit
        )
    }

    /// 是否是市价单类型
    pub fn is_market(&self) -> bool {
        matches!(self, 
            OrderType::Market | 
            OrderType::StopMarket | 
            OrderType::TakeProfitMarket
        )
    }

    /// 是否是条件单（止损/止盈）
    pub fn is_conditional(&self) -> bool {
        !matches!(self, OrderType::Limit | OrderType::LimitMaker | OrderType::Market)
    }
}


thread_local! {
    static ORDER_MAP: RefCell<HashMap<i64, Order>> = RefCell::new(HashMap::new());
}

/// 全局订单管理器
pub struct OrderManager;

impl OrderManager {
    /// 添加订单到全局map
    pub fn insert(order: Order) {
        ORDER_MAP.with(|map| {
            map.borrow_mut().insert(order.order_id, order);
        });
    }
    
    /// 根据订单ID获取订单
    pub fn get(order_id: i64) -> Option<Order> {
        ORDER_MAP.with(|map| {
            map.borrow().get(&order_id).cloned()
        })
    }
    
    /// 根据订单ID获取订单的可变引用并执行操作
    pub fn update<F>(order_id: i64, f: F) -> bool 
    where
        F: FnOnce(&mut Order)
    {
        ORDER_MAP.with(|map| {
            if let Some(order) = map.borrow_mut().get_mut(&order_id) {
                f(order);
                true
            } else {
                false
            }
        })
    }
    
    /// 移除订单
    pub fn remove(order_id: i64) -> Option<Order> {
        ORDER_MAP.with(|map| {
            map.borrow_mut().remove(&order_id)
        })
    }
    
    /// 获取所有订单ID
    pub fn get_all_ids() -> Vec<i64> {
        ORDER_MAP.with(|map| {
            map.borrow().keys().cloned().collect()
        })
    }
    
    /// 获取订单数量
    pub fn count() -> usize {
        ORDER_MAP.with(|map| {
            map.borrow().len()
        })
    }
    
    /// 清空所有订单
    pub fn clear() {
        ORDER_MAP.with(|map| {
            map.borrow_mut().clear();
        });
    }
}

#[derive(Debug, Clone)]
pub struct Order {
    pub order_id: i64,               // 订单ID
    pub order_type: OrderType,       // 订单类型
    pub symbol: String,              // 交易对
    pub side: Side,                  // 买卖方向
    pub price: Option<f64>,          // 限价单价格, 市价单没有意义
    pub quantity: f64,               // 数量
    pub status: OrderExecutionStatus, // 订单执行状态
    // 时间戳记录
    pub submit_time: i64,            // 订单提交时间(本地时间)
    pub exec_time: i64,              // trading-engine执行的时间
    pub ack_time: i64,               // 订单收到交易所回报的时间
    pub filled_time: i64,            // 订单执行成功的时间（交易所时间）
    pub end_time: i64,               // 收到成交回报的时间（本地时间）
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
        price: Option<f64>,
    ) -> Self {
        Order {
            order_id,
            order_type,
            symbol,
            side,
            price,
            quantity,
            status: OrderExecutionStatus::Create,
            submit_time: 0,
            exec_time: 0,
            ack_time: 0,
            filled_time: 0,
            end_time: 0,
        }
    }
    
    /// 更新订单状态
    pub fn update_status(&mut self, status: OrderExecutionStatus) {
        self.status = status;
    }
    
    /// 设置提交时间
    pub fn set_submit_time(&mut self, time: i64) {
        self.submit_time = time;
    }
    
    /// 设置执行时间
    pub fn set_exec_time(&mut self, time: i64) {
        self.exec_time = time;
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
