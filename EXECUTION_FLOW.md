# 执行流程设计

## 消息处理流程

```
ExecutionReport (原始消息)
    |
    ├─> 根据 venue 确定交易所
    |   ├─> BinanceMargin
    |   ├─> BinanceUm  
    |   ├─> OkexSwap
    |   └─> ...
    |
    └─> 根据 execution_type 决定处理方式
        ├─> TRADE → 作为 TradeUpdate 处理
        └─> 其他 → 作为 OrderUpdate 处理
```

## 实现方案

### 方案：使用统一的适配器枚举

```rust
// 文件: src/strategy/execution_adapter.rs

use crate::common::account_msg::{OrderTradeUpdateMsg, ExecutionReportMsg};
use crate::signal::common::{TradingVenue, ExecutionType};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;

/// 统一的执行报告适配器
/// 根据 venue 和 execution_type 自动路由到正确的处理逻辑
pub enum ExecutionAdapter<'a> {
    /// 币安期货订单更新
    BinanceFutures(&'a OrderTradeUpdateMsg),
    /// 币安现货执行报告
    BinanceSpot(&'a ExecutionReportMsg),
    /// OKEx 订单更新
    Okex(&'a SomeOkexMsg),
    // ... 其他交易所
}

impl<'a> ExecutionAdapter<'a> {
    /// 从 ExecutionReport 创建适配器（根据 venue）
    pub fn from_execution_report(
        msg: &'a ExecutionReportMsg,
        venue: TradingVenue,
    ) -> Self {
        match venue {
            TradingVenue::BinanceMargin | TradingVenue::BinanceSpot => {
                ExecutionAdapter::BinanceSpot(msg)
            }
            TradingVenue::BinanceUm => {
                // 如果是期货，需要转换为 OrderTradeUpdateMsg
                // 或者直接包装
                todo!()
            }
            _ => todo!()
        }
    }

    /// 从 OrderTradeUpdateMsg 创建适配器
    pub fn from_order_trade_update(msg: &'a OrderTradeUpdateMsg) -> Self {
        ExecutionAdapter::BinanceFutures(msg)
    }

    /// 获取执行类型
    pub fn execution_type(&self) -> ExecutionType {
        let type_str = match self {
            ExecutionAdapter::BinanceFutures(msg) => &msg.execution_type,
            ExecutionAdapter::BinanceSpot(msg) => &msg.execution_type,
            ExecutionAdapter::Okex(msg) => &msg.execution_type,
        };
        ExecutionType::from_str(type_str).unwrap_or(ExecutionType::New)
    }

    /// 是否为成交事件
    pub fn is_trade_event(&self) -> bool {
        self.execution_type().is_trade()
    }
}

// 实现 OrderUpdate trait
impl<'a> OrderUpdate for ExecutionAdapter<'a> {
    fn event_time(&self) -> i64 {
        match self {
            ExecutionAdapter::BinanceFutures(msg) => msg.event_time,
            ExecutionAdapter::BinanceSpot(msg) => msg.event_time,
            ExecutionAdapter::Okex(msg) => msg.event_time,
        }
    }

    fn symbol(&self) -> &str {
        match self {
            ExecutionAdapter::BinanceFutures(msg) => &msg.symbol,
            ExecutionAdapter::BinanceSpot(msg) => &msg.symbol,
            ExecutionAdapter::Okex(msg) => &msg.symbol,
        }
    }

    fn execution_type(&self) -> ExecutionType {
        ExecutionAdapter::execution_type(self)
    }

    // ... 实现其他方法
}

// 实现 TradeUpdate trait
impl<'a> TradeUpdate for ExecutionAdapter<'a> {
    fn trade_id(&self) -> i64 {
        match self {
            ExecutionAdapter::BinanceFutures(msg) => msg.trade_id,
            ExecutionAdapter::BinanceSpot(msg) => msg.trade_id,
            ExecutionAdapter::Okex(msg) => msg.trade_id,
        }
    }

    fn price(&self) -> f64 {
        match self {
            ExecutionAdapter::BinanceFutures(msg) => msg.last_executed_price,
            ExecutionAdapter::BinanceSpot(msg) => msg.last_executed_price,
            ExecutionAdapter::Okex(msg) => msg.price,
        }
    }

    // ... 实现其他方法
}
```

## 策略中使用

```rust
use crate::strategy::execution_adapter::ExecutionAdapter;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use crate::signal::common::TradingVenue;

pub struct MyStrategy {
    venue: TradingVenue,
}

impl MyStrategy {
    /// 处理任意执行报告
    pub fn handle_execution(&mut self, msg: &ExecutionReportMsg) {
        // 1. 创建适配器
        let adapter = ExecutionAdapter::from_execution_report(msg, self.venue);

        // 2. 根据 execution_type 决定处理方式
        if adapter.is_trade_event() {
            // 作为成交处理
            self.on_trade(&adapter);
        } else {
            // 作为订单更新处理
            self.on_order_update(&adapter);
        }
    }

    /// 处理成交
    fn on_trade(&mut self, trade: &impl TradeUpdate) {
        println!("成交: {} @ {}", trade.quantity(), trade.price());
        println!("手续费: {}", trade.commission());
        
        // 策略逻辑
        self.update_position(trade);
    }

    /// 处理订单更新
    fn on_order_update(&mut self, order: &impl OrderUpdate) {
        println!("订单更新: {} status={}", 
            order.order_id(), order.status());
        
        match order.execution_type() {
            ExecutionType::New => {
                println!("新订单创建");
            }
            ExecutionType::Canceled => {
                println!("订单取消");
                self.on_order_canceled(order);
            }
            ExecutionType::Rejected => {
                println!("订单拒绝");
            }
            _ => {}
        }
    }

    fn update_position(&mut self, trade: &impl TradeUpdate) {
        // 更新持仓逻辑
    }

    fn on_order_canceled(&mut self, order: &impl OrderUpdate) {
        // 订单取消逻辑
    }
}
```

## 关键点

1. **ExecutionAdapter** 是统一的枚举包装器
2. 同时实现 `OrderUpdate` 和 `TradeUpdate` trait
3. 策略根据 `execution_type` 决定调用哪个接口
4. 支持多交易所，只需添加新的枚举变体

这样设计的好处：
- ✅ 策略只操作 trait 接口
- ✅ 自动处理 venue 映射
- ✅ 根据 execution_type 灵活路由
- ✅ 易于扩展新交易所
