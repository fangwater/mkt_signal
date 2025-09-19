use crate::common::account_msg;
use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::pre_trade::order_manager::OrderType;
use crate::pre_trade::order_manager::Order;
use std::collections::HashMap;
use bytes::Bytes;

pub trait Strategy {
    fn get_id(&self) -> i32;
    fn handle_trade_response(&mut self, trade_msg_raws : &Bytes, success: bool);
    fn handle_account_event(&mut self, account_event_msg_raws : &Bytes);
    fn check_timeout(&mut self) -> Vec<Event>;
    fn get_orders(&self) -> &HashMap<i64, Order>;
    fn get_orders_mut(&mut self) -> &mut HashMap<i64, Order>;
}

#[derive(Debug, Clone)]
pub struct ForwardArbitrageConfig {
    pub open_range: f64,           // 基于价格设计的调节系数
    pub order_timeout_ms: i64,     // 保单时长限制（毫秒）
    pub max_position: f64,         // 最大持仓
}

impl Default for ForwardArbitrageConfig {
    fn default() -> Self {
        ForwardArbitrageConfig {
            open_range: 0.001,      // 0.1% 价差
            order_timeout_ms: 3000, // 3秒超时
            max_position: 1000.0,   // 最大持仓1000U
        }
    }
}

#[derive(Debug)]
pub struct ForwardArbitrage {
    pub strategy_id: i32,
    pub create_time: i64,
    pub spot_tick_time: i64,      // 现货盘口时间
    pub futures_tick_time: i64,   // 期货盘口时间
    pub config: ForwardArbitrageConfig,
    pub spot_orders: Vec<i64>,    // 现货订单列表
    pub futures_orders: Vec<i64>, // 合约订单列表
    pub orders: HashMap<i64, Order>,
    pub order_seq: i32,           // 订单序列号
}

impl ForwardArbitrage {
    pub fn new(
        strategy_id: i32,
        spot_tick_time: i64,
        futures_tick_time: i64,
        config: ForwardArbitrageConfig,
    ) -> Self {
        ForwardArbitrage {
            strategy_id,
            create_time: get_timestamp(),
            spot_tick_time,
            futures_tick_time,
            config,
            spot_orders: Vec::new(),
            futures_orders: Vec::new(),
            orders: HashMap::new(),
            order_seq: 0,
        }
    }

    pub fn create_spot_order(
        &mut self,
        symbol: String,
        price: f64,
        quantity: f64,
    ) -> Order {
        self.order_seq += 1;
        let order_id = generate_order_id(self.strategy_id, self.order_seq);
        
        let order = Order::new(
            order_id,
            self.strategy_id,
            OrderType::Limit,
            symbol,
            OrderSide::Buy,
            quantity,
            Some(price * (1.0 - self.config.open_range)), // 调节买入价格
        );
        
        self.spot_orders.push(order_id);
        self.orders.insert(order_id, order.clone());
        order
    }

    pub fn create_futures_hedge_order(
        &mut self,
        symbol: String,
        quantity: f64,
    ) -> Order {
        self.order_seq += 1;
        let order_id = generate_order_id(self.strategy_id, self.order_seq);
        
        let order = Order::new(
            order_id,
            self.strategy_id,
            OrderType::Market,  // 对冲单使用市价单
            symbol,
            OrderSide::Sell,    // 卖出期货
            quantity,
            None,
        );
        
        self.futures_orders.push(order_id);
        self.orders.insert(order_id, order.clone());
        order
    }
}

impl Strategy for ForwardArbitrage {
    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn handle_commit_reply(&mut self, order_id: i64, success: bool) -> Option<Event> {
        if let Some(order) = self.orders.get_mut(&order_id) {
            if success {
                order.update_status(crate::order::OrderExecutionStatus::Acked);
                println!("[策略{}] 订单{:x}提交成功", self.strategy_id, order_id);
            } else {
                order.update_status(crate::order::OrderExecutionStatus::Rejected);
                println!("[策略{}] 订单{:x}被拒绝", self.strategy_id, order_id);
            }
        }
        None
    }

    fn handle_execution_reply(&mut self, order_id: i64) -> Option<Event> {
        // 先更新订单状态
        let quantity = if let Some(order) = self.orders.get_mut(&order_id) {
            order.update_status(order::OrderExecutionStatus::Filled);
            println!("[策略{}] 订单{:x}成交", self.strategy_id, order_id);
            order.quantity
        } else {
            return None;
        };
        
        // 如果是现货订单成交，创建对冲的期货订单
        if self.spot_orders.contains(&order_id) {
            let futures_order = self.create_futures_hedge_order(
                "BTC-PERP".to_string(),
                quantity,
            );
            println!("[策略{}] 创建对冲订单{:x}", self.strategy_id, futures_order.order_id);
            return Some(Event::SubmitOrder(futures_order));
        }
        None
    }

    fn check_timeout(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        let current_time = get_timestamp();
        
        for order in self.orders.values() {
            // 检查未确认订单的超时
            if order.status == order::OrderExecutionStatus::Commit {
                if let Some(submit_time) = order.submit_time {
                    if current_time - submit_time > self.config.order_timeout_ms {
                        println!("[策略{}] 订单{:x}超时，发送撤单", self.strategy_id, order.order_id);
                        events.push(Event::CancelOrder(order.order_id));
                    }
                }
            }
            
            // 检查已确认但未成交订单的超时
            if order.status == crate::order::OrderExecutionStatus::Acked {
                if let Some(ack_time) = order.ws_ack_time {
                    if current_time - ack_time > self.config.order_timeout_ms {
                        println!("[策略{}] 订单{:x}长时间未成交，发送撤单", self.strategy_id, order.order_id);
                        events.push(Event::CancelOrder(order.order_id));
                    }
                }
            }
        }
        
        events
    }

    fn get_orders(&self) -> &HashMap<i64, Order> {
        &self.orders
    }

    fn get_orders_mut(&mut self) -> &mut HashMap<i64, Order> {
        &mut self.orders
    }
}