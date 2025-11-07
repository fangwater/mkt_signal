# OrderUpdate 和 TradeUpdate Trait 使用指南

## 概述

这套 trait 系统为订单和成交更新提供了统一的接口，支持多交易所扩展。

## 文件结构

```
src/strategy/
├── order_update.rs       # OrderUpdate trait 和订单处理器
├── trade_update.rs       # TradeUpdate trait
└── binance_adapter.rs    # 币安适配器实现

src/signal/common.rs      # TimeInForce 枚举定义
```

## 核心组件

### 1. OrderUpdate Trait

提供订单状态更新的统一接口。

```rust
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::binance_adapter::BinanceOrderUpdateAdapter;

fn handle_order_update(msg: &OrderTradeUpdateMsg) {
    let adapter = BinanceOrderUpdateAdapter::new(msg);
    
    // 访问订单基本信息
    println!("订单ID: {}", adapter.order_id());
    println!("交易对: {}", adapter.symbol());
    println!("方向: {:?}", adapter.side());
    println!("类型: {:?}", adapter.order_type());
    println!("TIF: {:?}", adapter.time_in_force());
    
    // 访问价格数量信息
    println!("价格: {}", adapter.price());
    println!("数量: {}", adapter.quantity());
    println!("已成交: {}", adapter.cumulative_qty());
    println!("剩余: {}", adapter.remaining_qty());
    
    // 检查订单状态
    if adapter.is_order_finished() {
        println!("订单已完成");
    } else if adapter.is_partially_filled() {
        println!("订单部分成交");
    }
}
```

### 2. TradeUpdate Trait

提供成交信息的统一接口。

```rust
use crate::strategy::trade_update::TradeUpdate;

fn handle_trade(msg: &OrderTradeUpdateMsg) {
    let adapter = BinanceOrderUpdateAdapter::new(msg);
    
    // 检查是否有成交
    if adapter.is_valid_trade() {
        println!("成交ID: {}", adapter.trade_id());
        println!("成交价格: {}", adapter.price());
        println!("成交数量: {}", adapter.quantity());
        println!("成交金额: {}", adapter.notional());
        println!("手续费: {} {}", adapter.commission(), adapter.commission_asset());
        println!("Maker/Taker: {}", if adapter.is_maker() { "Maker" } else { "Taker" });
        
        // 期货特有
        if adapter.realized_pnl() != 0.0 {
            println!("已实现盈亏: {}", adapter.realized_pnl());
        }
    }
}
```

### 3. OrderUpdateProcessor

跟踪和管理活跃订单。

```rust
use crate::strategy::order_update::{OrderUpdateProcessor, OrderUpdateEvent};

let mut processor = OrderUpdateProcessor::new();

fn process_update(processor: &mut OrderUpdateProcessor, msg: &OrderTradeUpdateMsg) {
    let adapter = BinanceOrderUpdateAdapter::new(msg);
    
    let event = processor.process_order_update(&adapter);
    match event {
        OrderUpdateEvent::OrderCreated(info) => {
            println!("新订单: {} {} {} @{}", 
                info.symbol, info.side, info.quantity, info.price);
        }
        OrderUpdateEvent::OrderPartiallyFilled(info) => {
            println!("部分成交: {}/{}", info.filled_qty, info.quantity);
        }
        OrderUpdateEvent::OrderFilled(info) => {
            println!("完全成交: {}", info.order_id);
        }
        OrderUpdateEvent::OrderCanceled(info) => {
            println!("订单取消: {}", info.order_id);
        }
        OrderUpdateEvent::OrderRejected(info) => {
            println!("订单拒绝: {}", info.order_id);
        }
        OrderUpdateEvent::UnknownOrder(order_id) => {
            println!("未知订单: {}", order_id);
        }
        _ => {}
    }
}

// 查询活跃订单
println!("活跃订单数: {}", processor.active_order_count());
if let Some(order) = processor.get_order(order_id) {
    println!("订单状态: {}", order.status);
}
```

## 完整示例：策略中使用

```rust
use crate::common::account_msg::OrderTradeUpdateMsg;
use crate::strategy::binance_adapter::BinanceOrderUpdateAdapter;
use crate::strategy::order_update::{OrderUpdate, OrderUpdateProcessor, OrderUpdateEvent};
use crate::strategy::trade_update::TradeUpdate;

pub struct MyStrategy {
    order_processor: OrderUpdateProcessor,
    // ... 其他字段
}

impl MyStrategy {
    pub fn handle_order_update(&mut self, msg: &OrderTradeUpdateMsg) {
        let adapter = BinanceOrderUpdateAdapter::new(&msg);
        
        // 1. 检查是否为成交事件
        if adapter.execution_type() == "TRADE" && adapter.is_valid_trade() {
            self.on_trade(&adapter);
        }
        
        // 2. 处理订单状态变化
        let event = self.order_processor.process_order_update(&adapter);
        match event {
            OrderUpdateEvent::OrderCreated(info) => {
                self.on_order_created(&info);
            }
            OrderUpdateEvent::OrderFilled(info) => {
                self.on_order_filled(&info);
            }
            OrderUpdateEvent::OrderCanceled(info) => {
                self.on_order_canceled(&info);
            }
            _ => {}
        }
    }
    
    fn on_trade(&mut self, trade: &impl TradeUpdate) {
        // 处理成交逻辑
        println!("成交: {} @ {}", trade.quantity(), trade.price());
    }
    
    fn on_order_created(&mut self, info: &OrderInfo) {
        // 订单创建逻辑
    }
    
    fn on_order_filled(&mut self, info: &OrderInfo) {
        // 订单完成逻辑
    }
    
    fn on_order_canceled(&mut self, info: &OrderInfo) {
        // 订单取消逻辑
    }
}
```

## 扩展到其他交易所

要支持其他交易所（如OKEx、Bybit），只需：

1. 创建新的适配器（如 `OkexOrderUpdateAdapter`）
2. 实现 `OrderUpdate` 和 `TradeUpdate` trait
3. 策略代码无需修改

```rust
// 示例：OKEx适配器
pub struct OkexOrderUpdateAdapter<'a> {
    msg: &'a OkexOrderMsg,
}

impl<'a> OrderUpdate for OkexOrderUpdateAdapter<'a> {
    fn event_time(&self) -> i64 {
        self.msg.timestamp
    }
    
    fn order_id(&self) -> i64 {
        self.msg.ord_id.parse().unwrap_or(0)
    }
    
    // ... 实现其他方法
}
```

## 设计优势

1. **解耦**: 策略不依赖具体交易所消息格式
2. **类型安全**: 使用枚举类型（Side, OrderType, TimeInForce）
3. **易扩展**: 新增交易所只需实现适配器
4. **统一接口**: 所有交易所使用相同的 trait
5. **单线程优化**: 避免不必要的 Send + Sync 约束

