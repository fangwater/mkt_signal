# pre_trade 模块与 strategy 模块的关联关系分析

## 目录
1. [模块概述](#模块概述)
2. [关键数据结构](#关键数据结构)
3. [核心调用关系](#核心调用关系)
4. [数据流动链路](#数据流动链路)
5. [strategy 对 pre_trade 的使用](#strategy-对-pre_trade-的使用)
6. [pre_trade 对 strategy 的管理](#pre_trade-对-strategy-的管理)
7. [代码示例](#代码示例)

---

## 模块概述

### pre_trade 模块（预交易处理）
位置：`/src/pre_trade/`

**职责：**
- 账户状态管理（UM/Spot/Margin）
- 风险敞口管理（ExposureManager）
- 订单管理（OrderManager）
- 价格表维护（PriceTable）
- 策略生命周期管理（通过 StrategyManager）
- 信号处理和分发

**主要组件：**
- `runner.rs` - PreTrade 主运行循环，管理整个预交易流程
- `order_manager.rs` - 订单创建、查询、更新
- `exposure_manager.rs` - 资产敞口计算与更新
- `price_table.rs` - 交易对标记价格维护
- `binance_pm_um_manager.rs` / `binance_pm_spot_manager.rs` - 币安账户管理

### strategy 模块（策略执行）
位置：`/src/strategy/`

**职责：**
- 策略核心逻辑实现
- 订单和成交处理
- 风险检查执行
- 信号处理

**主要组件：**
- `hedge_arb_strategy.rs` - 套利对冲策略实现
- `manager.rs` - 策略管理器（StrategyManager）
- `risk_checker.rs` - 风险检查器（RiskChecker）
- `order_update.rs` / `trade_update.rs` - 订单和成交的 trait 定义
- `binance_um_impl.rs` / `binance_margin_impl.rs` - 币安特定实现

---

## 关键数据结构

### 1. PreTradeEnv（关键桥接结构）
```rust
// 位置：src/strategy/risk_checker.rs 第 388-415 行

pub struct PreTradeEnv {
    pub min_qty_table: std::rc::Rc<MinQtyTable>,           // 最小下单量表
    pub signal_tx: Option<UnboundedSender<Bytes>>,         // 信号发送器
    pub signal_query_tx: Option<UnboundedSender<Bytes>>,   // 查询信号发送器
    pub trade_request_tx: UnboundedSender<Bytes>,          // 交易请求发送器
    pub risk_checker: RiskChecker,                          // 风险检查器
    pub hedge_residual_map: Rc<RefCell<HashMap<...>>>,     // 对冲残余量
}
```

**作用：**
- 为策略提供访问 pre_trade 资源的统一入口
- 封装订单和信号的发送接口
- 提供风险检查功能

### 2. RiskChecker（风控检查）
```rust
// 位置：src/strategy/risk_checker.rs 第 25-35 行

pub struct RiskChecker {
    pub exposure_manager: Rc<RefCell<ExposureManager>>,
    pub order_manager: Rc<RefCell<OrderManager>>,
    pub price_table: std::rc::Rc<std::cell::RefCell<PriceTable>>,
    max_symbol_exposure_ratio: f64,
    max_total_exposure_ratio: f64,
    max_pos_u: f64,
    max_leverage: f64,
    max_pending_limit_orders: Rc<Cell<i32>>,
}
```

**职责：**
- 检查杠杆率：`check_leverage()`
- 检查单品种敞口：`check_symbol_exposure()`
- 检查总敞口：`check_total_exposure()`
- 检查限价挂单数量：`check_pending_limit_order()`
- 检查仓位限制：`ensure_max_pos_u()`

### 3. HedgeArbStrategy（套利策略）
```rust
// 位置：src/strategy/hedge_arb_strategy.rs 第 15-30 行

pub struct HedgeArbStrategy {
    pub strategy_id: i32,
    pub symbol: String,                 // 开仓交易对
    pub open_order_id: i64,             // 开仓单ID
    pub hedge_order_ids: Vec<i64>,      // 对冲单ID列表
    pub open_timeout_us: Option<i64>,   // 开仓超时时间
    pub hedge_timeout_us: Option<i64>,  // 对冲超时时间
    pub order_seq: u32,                 // 订单序列号计数器
    pub pre_trade_env: Rc<PreTradeEnv>, // 预交易环境（关键依赖）
    pub cumulative_hedged_qty: f64,     // 累计对冲数量
    pub cumulative_open_qty: f64,       // 累计开仓数量
    pub alive_flag: bool,               // 策略是否活跃
    pub hedge_symbol: String,           // 对冲交易对
    pub hedge_venue: TradingVenue,      // 对冲场所
    pub hedge_side: Side,               // 对冲方向
}
```

---

## 核心调用关系

### 1. pre_trade → strategy 的调用链路

```
runner.rs::handle_trade_signal()
  ├─ 创建 HedgeArbStrategy 实例
  │   └─ strategy_id = StrategyManager::generate_strategy_id()
  │   └─ env = ctx.get_pre_trade_env()  ← 获取 PreTradeEnv
  │   └─ HedgeArbStrategy::new(strategy_id, symbol, env)
  │
  ├─ strategy.handle_signal(&signal)
  │   └─ HedgeArbStrategy::handle_arb_open_signal()
  │   └─ HedgeArbStrategy::handle_arb_hedge_signal()
  │   └─ HedgeArbStrategy::handle_arb_cancel_signal()
  │
  └─ ctx.insert_strategy(Box::new(strategy))
      └─ strategy_mgr.insert(strategy)
```

位置：`runner.rs` 第 994-1008 行

```rust
fn handle_trade_signal(ctx: &mut RuntimeContext, signal: TradeSignal) {
    match signal.signal_type {
        SignalType::ArbOpen => match ArbOpenCtx::from_bytes(signal.context.clone()) {
            Ok(open_ctx) => {
                let symbol = open_ctx.get_opening_symbol().to_uppercase();
                if !ctx.check_pending_limit_order_quota(&symbol) {
                    return;
                }
                // 第1步：创建策略 ID
                let strategy_id = StrategyManager::generate_strategy_id();
                // 第2步：获取预交易环境
                let env = ctx.get_pre_trade_env();
                // 第3步：创建策略实例
                let mut strategy = HedgeArbStrategy::new(strategy_id, symbol.clone(), env);
                // 第4步：分发信号
                strategy.handle_signal(&signal);
                // 第5步：注册策略
                if strategy.is_active() {
                    ctx.insert_strategy(Box::new(strategy));
                }
            }
            Err(err) => warn!("failed to decode ArbOpen context: {err}"),
        },
        // ... 其他信号类型 ...
    }
}
```

### 2. strategy → pre_trade 的调用链路

```
HedgeArbStrategy::handle_arb_open_signal()
  │
  ├─ 风险检查（通过 RiskChecker）
  │  ├─ risk_checker.check_leverage()
  │  ├─ risk_checker.check_symbol_exposure(&symbol)
  │  ├─ risk_checker.check_total_exposure()
  │  └─ risk_checker.check_pending_limit_order(&symbol)
  │
  ├─ 订单管理（通过 OrderManager）
  │  └─ order_manager.create_order(
  │      venue, order_id, order_type, symbol, side, qty, price, ts
  │    )
  │
  └─ 订单发送（通过 UnboundedSender）
     └─ trade_request_tx.send(order_request_bytes)

HedgeArbStrategy::handle_arb_hedge_signal()
  │
  ├─ 对齐订单量/价格
  │  └─ pre_trade_env.align_order_by_venue(...)
  │
  ├─ 检查最小交易要求
  │  └─ pre_trade_env.check_min_trading_requirements(...)
  │
  └─ 创建对冲订单
     └─ order_manager.create_order(...)
     └─ trade_request_tx.send(...)
```

位置：`hedge_arb_strategy.rs` 第 67-224 行

---

## 数据流动链路

### 完整的信号处理流程

```
外部信号
  │
  ├─ ArbOpen (开仓信号)
  │  │
  │  ├─ runner.rs::handle_trade_signal() 接收
  │  │
  │  ├─ 创建 HedgeArbStrategy 实例并关联 PreTradeEnv
  │  │
  │  ├─ strategy.handle_arb_open_signal()
  │  │  │
  │  │  ├─ 执行风险检查
  │  │  │  ├─ check_leverage()           [查询 exposure_manager]
  │  │  │  ├─ check_symbol_exposure()    [查询 exposure_manager + price_table]
  │  │  │  ├─ check_total_exposure()     [查询 exposure_manager + price_table]
  │  │  │  └─ check_pending_limit_order()[查询 order_manager]
  │  │  │
  │  │  ├─ 对齐订单量和价格
  │  │  │
  │  │  └─ 创建订单
  │  │     ├─ order_manager.create_order() [记录到 order_manager]
  │  │     └─ trade_request_tx.send()      [发送到交易引擎]
  │  │
  │  └─ ctx.insert_strategy() 注册策略
  │
  ├─ ArbHedge (对冲信号)
  │  │
  │  ├─ runner.rs::handle_trade_signal() 接收
  │  │
  │  ├─ 查询现有策略实例
  │  │
  │  └─ strategy.handle_arb_hedge_signal()
  │     │
  │     ├─ 对齐订单
  │     │  └─ align_order_by_venue()     [查询 min_qty_table]
  │     │
  │     ├─ 检查最小要求
  │     │  └─ check_min_trading_requirements() [查询 price_table]
  │     │
  │     └─ 创建对冲订单
  │        ├─ order_manager.create_order()
  │        └─ trade_request_tx.send()
  │
  ├─ ArbCancel (取消信号)
  │  │
  │  └─ strategy.handle_arb_cancel_signal()
  │     └─ order_manager.get() → get_order_cancel_bytes() → send()
  │
  └─ 账户更新事件
     │
     ├─ ExecutionReport (订单执行回报)
     │  │
     │  ├─ dispatch_execution_report()
     │  │
     │  └─ 查询策略：strategy_mgr.iter_ids()
     │     └─ strategy.is_strategy_order(order_id)
     │        ├─ strategy.apply_order_update(report)  [更新订单状态]
     │        └─ strategy.apply_trade_update(report)  [处理成交]
     │
     └─ OrderTradeUpdate (订单成交更新)
        │
        ├─ dispatch_order_trade_update()
        │
        └─ 查询策略并处理
```

### 订单管理的生命周期

```
1. 创建阶段
   ├─ HedgeArbStrategy 决定下单
   ├─ 调用 order_manager.create_order()
   └─ 返回 client_order_id
   
2. 提交阶段
   ├─ 获取订单序列化字节
   ├─ 通过 trade_request_tx 发送到交易引擎
   └─ 订单进入交易所

3. 执行阶段
   ├─ 币安返回 ExecutionReport（订单已确认）
   ├─ 通过 account monitor 接收事件
   ├─ dispatch_execution_report() 查询对应策略
   ├─ strategy.apply_order_update() 更新订单状态
   └─ order_manager 记录最新状态

4. 成交处理
   ├─ OrderTradeUpdate 消息到达
   ├─ dispatch_order_trade_update() 分发
   ├─ strategy.apply_trade_update() 处理成交
   │  ├─ 更新累计成交数量
   │  ├─ 更新策略状态
   │  └─ 可能触发对冲逻辑
   └─ order_manager 记录成交信息

5. 完成阶段
   ├─ 订单完全成交或被取消
   ├─ strategy.is_active() 返回 false
   └─ ctx.remove_strategy() 清理策略
```

---

## strategy 对 pre_trade 的使用

### 1. 风险检查接口调用

**检查杠杆率**
```rust
// hedge_arb_strategy.rs 第 71-78 行
if let Err(e) = self.pre_trade_env.risk_checker.check_leverage() {
    error!("HedgeArbStrategy: strategy_id={} 杠杆风控检查失败: {}，标记策略为不活跃", 
           self.strategy_id, e);
    self.alive_flag = false;
    return;
}
```

**检查单品种敞口**
```rust
// hedge_arb_strategy.rs 第 81-89 行
if let Err(e) = self.pre_trade_env.risk_checker.check_symbol_exposure(&self.symbol) {
    error!("HedgeArbStrategy: strategy_id={} symbol={} 单品种敞口风控检查失败: {}", 
           self.strategy_id, self.symbol, e);
    self.alive_flag = false;
    return;
}
```

**检查总敞口**
```rust
// hedge_arb_strategy.rs 第 92-99 行
if let Err(e) = self.pre_trade_env.risk_checker.check_total_exposure() {
    error!("HedgeArbStrategy: strategy_id={} 总敞口风控检查失败: {}，标记策略为不活跃", 
           self.strategy_id, e);
    self.alive_flag = false;
    return;
}
```

**检查限价挂单数量**
```rust
// hedge_arb_strategy.rs 第 103-113 行
let order_type = OrderType::from_u8(ctx.order_type);
if order_type == Some(OrderType::Limit) {
    if let Err(e) = self.pre_trade_env.risk_checker.check_pending_limit_order(&self.symbol) {
        error!("HedgeArbStrategy: strategy_id={} 限价挂单数量风控检查失败: {}", 
               self.strategy_id, e);
        self.alive_flag = false;
        return;
    }
}
```

**检查仓位限制**
```rust
// hedge_arb_strategy.rs 第 184-195 行
if let Err(e) = self.pre_trade_env.risk_checker.ensure_max_pos_u(
    &symbol, aligned_qty, aligned_price
) {
    error!("HedgeArbStrategy: strategy_id={} 仓位限制检查失败: {}，标记策略为不活跃", 
           self.strategy_id, e);
    self.alive_flag = false;
    return;
}
```

### 2. 订单管理接口调用

**创建订单**
```rust
// hedge_arb_strategy.rs 第 199-217 行
let client_order_id = self
    .pre_trade_env
    .risk_checker
    .order_manager
    .borrow_mut()
    .create_order(
        venue,
        order_id,
        OrderType::from_u8(ctx.order_type).unwrap(),
        symbol.clone(),
        Side::from_u8(ctx.side).unwrap(),
        aligned_qty,
        aligned_price,
        ts,
    );
```

**查询订单**
```rust
// hedge_arb_strategy.rs 第 355-360 行
let mut order = self
    .pre_trade_env
    .risk_checker
    .order_manager
    .borrow_mut()
    .get(client_order_id);
```

**获取订单请求字节**
```rust
// hedge_arb_strategy.rs 第 362-375 行
match order.get_order_request_bytes() {
    Ok(req_bin) => {
        if let Err(e) = self.pre_trade_env.trade_request_tx.send(req_bin) {
            error!("HedgeArbStrategy: strategy_id={} 推送{}订单失败: {}", 
                   self.strategy_id, order_type_str, e);
            self.alive_flag = false;
            return Err(format!("推送{}订单失败: {}", order_type_str, e));
        }
        Ok(())
    }
    Err(e) => {
        error!("HedgeArbStrategy: 获取{}订单请求字节失败: {}", order_type_str, e);
        self.alive_flag = false;
        Err(format!("获取{}订单请求字节失败: {}", order_type_str, e))
    }
}
```

### 3. 订单对齐和验证

**对齐订单量和价格**
```rust
// hedge_arb_strategy.rs 第 247-252 行
let (aligned_qty, aligned_price) = self.pre_trade_env.align_order_by_venue(
    hedge_venue,
    &hedge_symbol,
    target_qty,
    hedge_price,
)?;
```

**检查最小交易要求**
```rust
// hedge_arb_strategy.rs 第 255-266 行
if let Err(e) = self.pre_trade_env.check_min_trading_requirements(
    hedge_venue,
    &hedge_symbol,
    aligned_qty,
    Some(aligned_price),
) {
    debug!("HedgeArbStrategy: strategy_id={} 对冲订单不满足最小要求: {}，等待更多成交", 
           self.strategy_id, e);
    return Ok(());
}
```

---

## pre_trade 对 strategy 的管理

### 1. RuntimeContext 中的策略管理

```rust
// runner.rs 第 237-257 行

struct RuntimeContext {
    // ... 其他字段 ...
    strategy_mgr: StrategyManager,        // 策略管理器
    // ... 其他字段 ...
}
```

### 2. 策略的创建和注册

```rust
// runner.rs 第 373-375 行
fn insert_strategy(&mut self, strategy: Box<dyn Strategy>) {
    self.strategy_mgr.insert(strategy);
}
```

位置：`runner.rs` 第 1002-1008 行，创建流程：

```rust
let strategy_id = StrategyManager::generate_strategy_id();
let env = ctx.get_pre_trade_env();
let mut strategy = HedgeArbStrategy::new(strategy_id, symbol.clone(), env);
strategy.handle_signal(&signal);
if strategy.is_active() {
    ctx.insert_strategy(Box::new(strategy));
}
```

### 3. 策略的信号分发

**ArbOpen 信号 - 创建新策略**
```rust
// runner.rs 第 996-1008 行
SignalType::ArbOpen => match ArbOpenCtx::from_bytes(signal.context.clone()) {
    Ok(open_ctx) => {
        let symbol = open_ctx.get_opening_symbol().to_uppercase();
        if !ctx.check_pending_limit_order_quota(&symbol) {
            return;
        }
        let strategy_id = StrategyManager::generate_strategy_id();
        let env = ctx.get_pre_trade_env();
        let mut strategy = HedgeArbStrategy::new(strategy_id, symbol.clone(), env);
        strategy.handle_signal(&signal);
        if strategy.is_active() {
            ctx.insert_strategy(Box::new(strategy));
        }
    }
    Err(err) => warn!("failed to decode ArbOpen context: {err}"),
},
```

**ArbHedge 信号 - 查询现有策略**
```rust
// runner.rs 第 1120-1136 行
SignalType::ArbHedge => match ArbHedgeCtx::from_bytes(signal.context.clone()) {
    Ok(hedge_ctx) => {
        let strategy_id = hedge_ctx.strategy_id;
        // ... 发布信号记录 ...
        if !ctx.strategy_mgr.contains(strategy_id) {
            return;
        }
        ctx.with_strategy_mut(strategy_id, |strategy| {
            strategy.handle_signal(&signal);
        });
    }
    Err(err) => warn!("failed to decode hedge context: {err}"),
},
```

**ArbCancel 信号 - 查询同一 symbol 的策略**
```rust
// runner.rs 第 1089-1117 行
SignalType::ArbCancel => match ArbCancelCtx::from_bytes(signal.context.clone()) {
    Ok(cancel_ctx) => {
        let symbol = cancel_ctx.get_opening_symbol().to_uppercase();
        let candidate_ids: Vec<i32> = ctx
            .strategy_mgr
            .ids_for_symbol(&symbol)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();

        if candidate_ids.is_empty() {
            return;
        }
        for strategy_id in candidate_ids {
            ctx.publish_signal_record(&record);
            if !ctx.strategy_mgr.contains(strategy_id) {
                return;
            }
            ctx.with_strategy_mut(strategy_id, |strategy| {
                strategy.handle_signal(&signal);
            });
        }
    }
    Err(err) => warn!("failed to decode ArbCancel context: {err}"),
},
```

### 4. 订单执行回报分发

```rust
// runner.rs 第 1145-1196 行
fn dispatch_execution_report(ctx: &mut RuntimeContext, report: &ExecutionReportMsg) {
    let order_id = report.client_order_id;
    let strategy_ids: Vec<i32> = ctx.strategy_mgr.iter_ids().cloned().collect();
    let mut matched = false;
    for strategy_id in strategy_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            if strategy.is_strategy_order(order_id) {
                matched = true;
                match report.execution_type() {
                    ExecutionType::New | ExecutionType::Canceled => {
                        strategy.apply_order_update(report);
                    }
                    ExecutionType::Trade => {
                        strategy.apply_trade_update(report);
                    }
                    // ...
                }
            }
        });
    }
    if !matched {
        debug!("executionReport unmatched: sym={} cli_id={}", 
               report.symbol, report.client_order_id);
    }
    ctx.cleanup_inactive();
}
```

### 5. 策略的清理和移除

```rust
// runner.rs 第 390-405 行
fn with_strategy_mut<F>(&mut self, strategy_id: i32, mut f: F)
where
    F: FnMut(&mut dyn Strategy),
{
    if let Some(mut strategy) = self.strategy_mgr.take(strategy_id) {
        f(strategy.as_mut());
        if strategy.is_active() {
            self.strategy_mgr.insert(strategy);  // 策略仍活跃，重新插入
        } else {
            drop(strategy);
            self.remove_strategy(strategy_id);   // 策略已完成，移除
        }
    }
}
```

---

## 代码示例

### 完整的开仓流程

```rust
// 1. pre_trade 收到开仓信号
fn handle_trade_signal(ctx: &mut RuntimeContext, signal: TradeSignal) {
    match signal.signal_type {
        SignalType::ArbOpen => {
            // 2. 解析信号内容
            match ArbOpenCtx::from_bytes(signal.context.clone()) {
                Ok(open_ctx) => {
                    let symbol = open_ctx.get_opening_symbol().to_uppercase();
                    
                    // 3. 检查限价挂单配额
                    if !ctx.check_pending_limit_order_quota(&symbol) {
                        return;
                    }
                    
                    // 4. 生成策略 ID
                    let strategy_id = StrategyManager::generate_strategy_id();
                    
                    // 5. 获取预交易环境（包含风控和订单管理）
                    let env = ctx.get_pre_trade_env();
                    
                    // 6. 创建策略实例
                    let mut strategy = HedgeArbStrategy::new(
                        strategy_id, 
                        symbol.clone(), 
                        env
                    );
                    
                    // 7. 策略处理信号（会触发风险检查和订单创建）
                    strategy.handle_signal(&signal);
                    
                    // 8. 只注册活跃的策略
                    if strategy.is_active() {
                        ctx.insert_strategy(Box::new(strategy));
                    }
                }
                Err(err) => warn!("failed to decode ArbOpen context: {err}"),
            }
        }
        _ => {}
    }
}

// 在 HedgeArbStrategy 中的处理
impl HedgeArbStrategy {
    fn handle_arb_open_signal(&mut self, ctx: ArbOpenCtx) {
        // 风险检查
        if let Err(e) = self.pre_trade_env.risk_checker.check_leverage() {
            error!("杠杆风控检查失败: {}", e);
            self.alive_flag = false;
            return;
        }
        
        if let Err(e) = self.pre_trade_env.risk_checker
            .check_symbol_exposure(&self.symbol) {
            error!("单品种敞口风控检查失败: {}", e);
            self.alive_flag = false;
            return;
        }
        
        // ... 更多检查 ...
        
        // 创建订单
        let symbol = ctx.get_opening_symbol();
        let (aligned_qty, aligned_price) = TradingVenue::align_um_order(
            &symbol,
            raw_qty,
            raw_price,
            &self.pre_trade_env.min_qty_table,
        ).unwrap();
        
        // 记录订单到 order_manager
        let client_order_id = self
            .pre_trade_env
            .risk_checker
            .order_manager
            .borrow_mut()
            .create_order(
                venue,
                order_id,
                order_type,
                symbol.clone(),
                side,
                aligned_qty,
                aligned_price,
                ts,
            );
        
        // 发送订单到交易引擎
        let order = self.pre_trade_env.risk_checker
            .order_manager
            .borrow_mut()
            .get(client_order_id);
        
        if let Some(order) = order {
            match order.get_order_request_bytes() {
                Ok(req_bin) => {
                    let _ = self.pre_trade_env.trade_request_tx.send(req_bin);
                }
                Err(e) => {
                    error!("获取订单请求字节失败: {}", e);
                    self.alive_flag = false;
                }
            }
        }
    }
}
```

### 完整的成交处理流程

```rust
// 1. pre_trade 从币安账户监控接收成交更新
fn dispatch_order_trade_update(ctx: &mut RuntimeContext, update: &OrderTradeUpdateMsg) {
    let order_id: i64 = update.client_order_id;
    
    // 2. 查询所有策略，找到匹配的策略
    let strategy_ids: Vec<i32> = ctx.strategy_mgr.iter_ids().cloned().collect();
    
    for strategy_id in strategy_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            // 3. 检查订单是否属于该策略
            if strategy.is_strategy_order(order_id) {
                // 4. 策略处理成交更新
                strategy.apply_trade_update(update);
                
                // 策略可能返回 active=true（继续运行）或 active=false（完成）
            }
        });
    }
    
    ctx.cleanup_inactive();
}

// 在 HedgeArbStrategy 中实现的成交处理
// 这是通过 apply_trade_update 方法实现的
// (具体实现在 Strategy trait 的实现中)
```

### 对冲流程示例

```rust
// 1. pre_trade 收到对冲信号
SignalType::ArbHedge => match ArbHedgeCtx::from_bytes(signal.context.clone()) {
    Ok(hedge_ctx) => {
        let strategy_id = hedge_ctx.strategy_id;
        
        // 2. 检查策略是否存在
        if !ctx.strategy_mgr.contains(strategy_id) {
            warn!("Strategy not found: {}", strategy_id);
            return;
        }
        
        // 3. 获取策略的可变引用
        ctx.with_strategy_mut(strategy_id, |strategy| {
            // 4. 分发对冲信号给策略
            strategy.handle_signal(&signal);
        });
    }
    Err(err) => warn!("failed to decode hedge context: {err}"),
},

// 在 HedgeArbStrategy 中的处理
fn handle_arb_hedge_signal(&mut self, ctx: ArbHedgeCtx) -> Result<(), String> {
    // 1. 确定对冲数量
    let target_qty = ctx.hedge_qty as f64;
    
    // 2. 对齐订单量和价格（调用 pre_trade_env）
    let (aligned_qty, aligned_price) = self.pre_trade_env
        .align_order_by_venue(
            hedge_venue,
            &hedge_symbol,
            target_qty,
            hedge_price,
        )?;
    
    // 3. 检查最小交易要求
    self.pre_trade_env.check_min_trading_requirements(
        hedge_venue,
        &hedge_symbol,
        aligned_qty,
        Some(aligned_price),
    )?;
    
    // 4. 创建对冲订单
    let hedge_order_id = Self::compose_order_id(self.strategy_id, self.order_seq);
    
    let hedge_client_order_id = self
        .pre_trade_env
        .risk_checker
        .order_manager
        .borrow_mut()
        .create_order(
            hedge_venue,
            hedge_order_id,
            order_type,
            hedge_symbol.clone(),
            hedge_side,
            aligned_qty,
            aligned_price,
            ts,
        );
    
    // 5. 记录对冲订单 ID
    self.hedge_order_ids.push(hedge_order_id);
    
    // 6. 发送对冲订单到交易引擎
    self.create_and_send_order(hedge_client_order_id, "对冲", &hedge_symbol)?;
    
    Ok(())
}
```

---

## 关键接口和 Trait

### Strategy Trait
```rust
// src/strategy/manager.rs
pub trait Strategy {
    fn get_id(&self) -> i32;
    fn is_strategy_order(&self, order_id: i64) -> bool;
    fn handle_signal(&mut self, signal: &TradeSignal);
    fn apply_order_update(&mut self, update: &dyn OrderUpdate);
    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate);
    fn handle_period_clock(&mut self, current_tp: i64);
    fn is_active(&self) -> bool;
    fn symbol(&self) -> Option<&str>;
}
```

### OrderUpdate Trait
```rust
// src/strategy/order_update.rs
pub trait OrderUpdate {
    fn event_time(&self) -> i64;
    fn symbol(&self) -> &str;
    fn order_id(&self) -> i64;
    fn client_order_id(&self) -> i64;
    fn side(&self) -> Side;
    fn order_type(&self) -> OrderType;
    fn status(&self) -> OrderStatus;
    fn execution_type(&self) -> ExecutionType;
    fn trading_venue(&self) -> TradingVenue;
    // ... 更多方法 ...
}
```

### TradeUpdate Trait
```rust
// src/strategy/trade_update.rs
pub trait TradeUpdate {
    fn event_time(&self) -> i64;
    fn trade_time(&self) -> i64;
    fn symbol(&self) -> &str;
    fn trade_id(&self) -> i64;
    fn order_id(&self) -> i64;
    fn client_order_id(&self) -> i64;
    fn side(&self) -> Side;
    fn price(&self) -> f64;
    fn quantity(&self) -> f64;
    fn commission(&self) -> f64;
    fn commission_asset(&self) -> &str;
    fn is_maker(&self) -> bool;
    fn trading_venue(&self) -> TradingVenue;
}
```

---

## 总结

### pre_trade 与 strategy 的关系

**单向依赖：**
- strategy **强依赖** pre_trade（通过 PreTradeEnv 和 RiskChecker）
- pre_trade **管理** strategy（通过 StrategyManager）

**数据流向：**
1. pre_trade 创建 PreTradeEnv，包含：
   - ExposureManager：账户敞口
   - OrderManager：订单信息
   - PriceTable：标记价格
   - RiskChecker：风险检查器

2. strategy 通过 PreTradeEnv 访问 pre_trade 的核心资源

3. strategy 通过发送 Bytes（序列化数据）与外部交互：
   - trade_request_tx：发送订单到交易引擎
   - signal_tx：发送信号到其他模块

**关键交互点：**
- 信号分发：pre_trade 分发信号给 strategy
- 订单管理：strategy 通过 OrderManager 创建订单
- 风险检查：strategy 通过 RiskChecker 执行风险控制
- 回报处理：pre_trade 分发订单/成交回报给 strategy

