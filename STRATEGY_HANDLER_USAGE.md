# StrategyHandler 使用指南

## 架构设计

```
OrderTradeUpdateMsg (原始消息)
    |
    | 实现了 OrderUpdate trait
    | 实现了 TradeUpdate trait
    |
    V
MessageDispatcher (分发器)
    |
    | 检查 execution_type
    |
    ├─> ExecutionType::Trade → handle_trade(&dyn TradeUpdate)
    └─> 其他类型 → handle_order(&dyn OrderUpdate)
    |
    V
Strategy (策略通过 dyn trait 操作，完全屏蔽底层消息)
```

## 快速开始

### 1. 策略实现 StrategyHandler trait

```rust
use crate::strategy::message_dispatcher::StrategyHandler;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use crate::signal::common::ExecutionType;

pub struct HedgeArbStrategy {
    pub strategy_id: i32,
    pub symbol: String,
    pub open_order_id: i64,
    pub cumulative_hedged_qty: f64,
    pub cumulative_open_qty: f64,
    // ... 其他字段
}

impl StrategyHandler for HedgeArbStrategy {
    /// 处理成交事件
    fn handle_trade(&mut self, trade: &dyn TradeUpdate) {
        // 检查是否有效成交
        if !trade.is_valid_trade() {
            return;
        }

        // 业务逻辑（留空，根据实际需求填充）
        // TODO: 更新持仓
        // TODO: 检查对冲机会
        // TODO: 计算盈亏
    }

    /// 处理订单更新事件
    fn handle_order(&mut self, order: &dyn OrderUpdate) {
        // 根据执行类型分支处理
        match order.execution_type() {
            ExecutionType::New => {
                // 新订单创建
                // TODO: 记录订单信息
            }
            ExecutionType::Canceled => {
                // 订单取消
                // TODO: 清理订单状态
            }
            ExecutionType::Rejected => {
                // 订单拒绝
                // TODO: 记录拒绝原因，触发报警
            }
            ExecutionType::Expired => {
                // 订单过期
                // TODO: 检查是否需要重新下单
            }
            _ => {
                // 其他类型
            }
        }
    }
}
```

### 2. 使用分发器处理消息

```rust
use crate::common::account_msg::OrderTradeUpdateMsg;
use crate::strategy::message_dispatcher::MessageDispatcher;

impl HedgeArbStrategy {
    /// 主入口：处理订单更新消息
    pub fn handle_order_trade_update(&mut self, msg: &OrderTradeUpdateMsg) {
        // 使用分发器自动路由
        MessageDispatcher::dispatch(msg, self);
    }
}

// 在你的主逻辑中调用
fn on_websocket_message(msg: OrderTradeUpdateMsg) {
    let mut strategy = HedgeArbStrategy::new(...);
    
    // 一行代码完成分发和处理
    strategy.handle_order_trade_update(&msg);
}
```

## 完整示例

```rust
use crate::strategy::message_dispatcher::StrategyHandler;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use crate::signal::common::ExecutionType;
use crate::pre_trade::order_manager::Side;

pub struct HedgeArbStrategy {
    strategy_id: i32,
    symbol: String,
    open_order_id: i64,
    hedge_order_ids: Vec<i64>,
    cumulative_hedged_qty: f64,
    cumulative_open_qty: f64,
}

impl HedgeArbStrategy {
    pub fn new(id: i32, symbol: String) -> Self {
        Self {
            strategy_id: id,
            symbol,
            open_order_id: 0,
            hedge_order_ids: Vec::new(),
            cumulative_hedged_qty: 0.0,
            cumulative_open_qty: 0.0,
        }
    }

    /// 主入口
    pub fn handle_order_trade_update(&mut self, msg: &OrderTradeUpdateMsg) {
        MessageDispatcher::dispatch(msg, self);
    }
}

impl StrategyHandler for HedgeArbStrategy {
    fn handle_trade(&mut self, trade: &dyn TradeUpdate) {
        if !trade.is_valid_trade() {
            return;
        }

        println!("[TRADE] symbol={} trade_id={} price={} qty={} maker={} commission={}",
            trade.symbol(),
            trade.trade_id(),
            trade.price(),
            trade.quantity(),
            trade.is_maker(),
            trade.commission()
        );

        // 业务逻辑（根据实际需求实现）
        match trade.side() {
            Side::Buy => {
                self.cumulative_open_qty += trade.quantity();
                // TODO: 更新买入持仓
            }
            Side::Sell => {
                self.cumulative_hedged_qty += trade.quantity();
                // TODO: 更新卖出持仓
            }
        }

        // TODO: 检查是否需要对冲
        // TODO: 计算已实现盈亏
        // TODO: 更新风险敞口
    }

    fn handle_order(&mut self, order: &dyn OrderUpdate) {
        println!("[ORDER] symbol={} order_id={} status={} exec_type={:?} filled={}/{}",
            order.symbol(),
            order.order_id(),
            order.status(),
            order.execution_type(),
            order.cumulative_qty(),
            order.quantity()
        );

        match order.execution_type() {
            ExecutionType::New => {
                // 新订单创建
                self.open_order_id = order.order_id();
                println!("  新订单创建成功");
                // TODO: 记录订单到数据库
            }
            
            ExecutionType::Canceled => {
                // 订单取消
                if self.open_order_id == order.order_id() {
                    self.open_order_id = 0;
                }
                println!("  订单已取消");
                // TODO: 检查是否需要重新下单
            }
            
            ExecutionType::Rejected => {
                // 订单拒绝
                println!("  订单被拒绝！");
                // TODO: 记录拒绝原因
                // TODO: 触发报警
            }
            
            ExecutionType::Expired => {
                // 订单过期
                println!("  订单过期");
                // TODO: 检查 Time In Force
                // TODO: 决定是否重新下单
            }
            
            _ => {
                println!("  其他类型: {:?}", order.execution_type());
            }
        }
    }
}
```

## 核心优势

1. **完全屏蔽底层细节**
   - 策略只看到 `&dyn TradeUpdate` 和 `&dyn OrderUpdate`
   - 不需要知道具体的消息类型

2. **自动路由**
   - `MessageDispatcher` 根据 `execution_type` 自动分发
   - 策略不需要手动判断

3. **类型安全**
   - 编译期检查所有方法调用
   - 不会调用错误的接口

4. **易于扩展**
   - 新增消息类型只需实现两个 trait
   - 策略代码无需修改

5. **业务逻辑集中**
   - 成交逻辑在 `handle_trade`
   - 订单逻辑在 `handle_order`
   - 清晰的职责划分

## 测试

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_handler() {
        let mut strategy = HedgeArbStrategy::new(1, "BTCUSDT".to_string());
        
        // 模拟成交消息
        let trade_msg = create_trade_message();
        strategy.handle_order_trade_update(&trade_msg);
        
        // 验证状态
        assert!(strategy.cumulative_hedged_qty > 0.0);
    }
}
```

## 注意事项

1. **dyn trait 的性能**
   - 有虚函数表开销，但在你的单线程场景下可以忽略
   - 相比 WebSocket 延迟，这个开销微不足道

2. **生命周期**
   - 所有 trait 方法都使用 `&self`，不涉及所有权转移
   - 消息本身是借用的，零拷贝

3. **错误处理**
   - `ExecutionType::from_str()` 返回 `Option`，需要处理 `None` 的情况
   - 默认值设置为 `ExecutionType::New`

4. **扩展其他交易所**
   - 为 `ExecutionReportMsg`（现货）实现两个 trait
   - 为 `OkexOrderMsg` 实现两个 trait
   - 分发器自动支持，策略代码不变

## 下一步

业务逻辑根据实际需求填充：
- [ ] `handle_trade` 中实现持仓更新逻辑
- [ ] `handle_trade` 中实现对冲检查逻辑
- [ ] `handle_order` 中实现订单状态管理
- [ ] 添加日志记录
- [ ] 添加性能监控
- [ ] 添加异常报警
