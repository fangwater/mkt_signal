use crate::common::account_msg::OrderTradeUpdateMsg;
use crate::signal::common::ExecutionType;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;

/// 策略处理器接口
/// 策略需要实现这个 trait 来处理订单和成交更新
pub trait StrategyHandler {
    /// 处理成交事件
    /// 当 execution_type 为 TRADE 时调用
    fn handle_trade_update(&mut self, trade: &dyn TradeUpdate);

    /// 处理订单更新事件
    /// 当 execution_type 为其他类型时调用（NEW, CANCELED, REJECTED, EXPIRED 等）
    fn handle_order_update(&mut self, order: &dyn OrderUpdate);
}

/// 消息分发器
/// 根据 execution_type 将消息分发到对应的处理方法
pub struct MessageDispatcher;

impl MessageDispatcher {
    /// 分发消息到策略
    /// 根据 execution_type 自动决定调用 handle_trade 还是 handle_order
    pub fn dispatch<S>(msg: &OrderTradeUpdateMsg, strategy: &mut S)
    where
        S: StrategyHandler,
    {
        let exec_type = ExecutionType::from_str(&msg.execution_type)
            .unwrap_or(ExecutionType::New);

        if exec_type.is_trade() {
            // TRADE 类型 -> 作为 TradeUpdate 处理
            strategy.handle_trade_update(msg);
        } else {
            // 其他类型 -> 作为 OrderUpdate 处理
            strategy.handle_order_update(msg);
        } 
    } 

    /// 判断是否为成交事件
    pub fn is_trade_event(msg: &OrderTradeUpdateMsg) -> bool {
        ExecutionType::from_str(&msg.execution_type)
            .map(|t| t.is_trade())
            .unwrap_or(false)
    }

    /// 获取执行类型
    pub fn get_execution_type(msg: &OrderTradeUpdateMsg) -> ExecutionType {
        ExecutionType::from_str(&msg.execution_type).unwrap_or(ExecutionType::New)
    }
}