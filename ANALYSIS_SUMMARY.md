# pre_trade 与 strategy 模块关联关系分析 - 执行摘要

## 快速概览

### 模块定位

**pre_trade 模块** - 预交易处理的核心引擎
- 位置：`/src/pre_trade/`
- 核心类：`RuntimeContext`（包含 `StrategyManager`）
- 职责：账户管理、风险敞口、订单管理、策略生命周期、信号分发

**strategy 模块** - 交易策略执行引擎
- 位置：`/src/strategy/`
- 核心类：`HedgeArbStrategy`（套利对冲策略）
- 职责：策略逻辑、信号处理、订单/成交操作

### 关系性质

```
strategy ──强依赖──► pre_trade
                    ▲
pre_trade ──管理────┘
```

- **strategy 依赖** pre_trade 提供的资源（通过 `PreTradeEnv`）
- **pre_trade 管理** strategy 的生命周期（通过 `StrategyManager`）

---

## 核心数据结构

### 1. PreTradeEnv - 桥接结构

**位置：** `src/strategy/risk_checker.rs` 第 388-415 行

**作用：** 为 strategy 提供 pre_trade 资源的统一访问入口

```rust
pub struct PreTradeEnv {
    pub risk_checker: RiskChecker,                  // 风险检查
    pub trade_request_tx: UnboundedSender<Bytes>,  // 订单发送
    pub signal_tx: Option<UnboundedSender<Bytes>>, // 信号发送
    pub min_qty_table: Rc<MinQtyTable>,            // 最小量表
    pub hedge_residual_map: Rc<RefCell<HashMap>>,  // 对冲残余量
}
```

### 2. RiskChecker - 风控检查器

**关键字段：**
- `exposure_manager` - 敞口管理（引用 pre_trade）
- `order_manager` - 订单管理（引用 pre_trade）
- `price_table` - 价格表（引用 pre_trade）

**关键方法：**
```
check_leverage()              // 检查杠杆率
check_symbol_exposure()       // 检查单品种敞口
check_total_exposure()        // 检查总敞口
check_pending_limit_order()   // 检查限价挂单数量
ensure_max_pos_u()            // 检查仓位限制
```

### 3. HedgeArbStrategy - 套利策略

**关键字段：**
```rust
pub struct HedgeArbStrategy {
    pub strategy_id: i32,                 // 策略ID
    pub symbol: String,                   // 开仓交易对
    pub open_order_id: i64,               // 开仓单ID
    pub hedge_order_ids: Vec<i64>,        // 对冲单ID列表
    pub pre_trade_env: Rc<PreTradeEnv>,   // ★ 关键：访问 pre_trade 资源
    pub cumulative_hedged_qty: f64,       // 累计对冲数量
    pub cumulative_open_qty: f64,         // 累计开仓数量
    pub alive_flag: bool,                 // 策略是否活跃
}
```

---

## 调用关系详解

### 路径1：pre_trade → strategy（创建和初始化）

```
runner.rs::handle_trade_signal()
  ├─ 接收 ArbOpen 信号
  ├─ 创建 strategy_id
  ├─ 调用 get_pre_trade_env() ─────────► 创建 PreTradeEnv
  │                                      ├─ RiskChecker::new()
  │                                      └─ 包含 exposure_manager 引用
  │                                          包含 order_manager 引用
  │                                          包含 price_table 引用
  ├─ 创建 HedgeArbStrategy 并传入 env
  ├─ 调用 strategy.handle_signal()
  └─ 注册到 strategy_mgr
```

**关键代码：** `runner.rs` 第 994-1008 行

### 路径2：strategy → pre_trade（执行操作）

#### 2a. 风险检查

```
strategy.handle_arb_open_signal()
  ├─ risk_checker.check_leverage()           // 查询 exposure_manager
  ├─ risk_checker.check_symbol_exposure()    // 查询 exposure_manager + price_table
  ├─ risk_checker.check_total_exposure()     // 查询 exposure_manager + price_table
  ├─ risk_checker.check_pending_limit_order()// 查询 order_manager
  └─ risk_checker.ensure_max_pos_u()         // 查询 price_table
```

**实例代码：** `hedge_arb_strategy.rs` 第 71-113 行

#### 2b. 订单创建和发送

```
strategy.create_and_send_order()
  ├─ order_manager.create_order()            // 创建订单记录
  ├─ order_manager.get()                     // 查询订单
  ├─ order.get_order_request_bytes()         // 序列化订单
  └─ trade_request_tx.send()                 // 发送到交易引擎
```

**实例代码：** `hedge_arb_strategy.rs` 第 349-397 行

### 路径3：pre_trade → strategy（分发回报）

#### 3a. 订单执行回报

```
dispatch_execution_report()
  ├─ 获取订单 ID
  ├─ strategy_mgr.iter_ids()                 // 遍历所有策略
  └─ strategy.is_strategy_order()            // 匹配策略
      ├─ strategy.apply_order_update()       // 更新订单状态
      └─ strategy.apply_trade_update()       // 处理成交
```

**实例代码：** `runner.rs` 第 1145-1196 行

#### 3b. 周期性检查

```
runtime.tick()
  └─ strategy_mgr.handle_period_clock()      // 处理超时/定时任务
```

---

## 数据流动完整链路

### 开仓完整流程

```
1. 信号接收
   外部信号 ──► pre_trade::handle_trade_signal()
       ├─ 解析 ArbOpen 信号
       └─ 检查限价挂单配额

2. 策略创建和初始化
   ├─ 生成 strategy_id
   ├─ 创建 PreTradeEnv
   │  └─ RiskChecker::new(exposure_manager, order_manager, price_table, ...)
   └─ HedgeArbStrategy::new(strategy_id, symbol, env)

3. 风险检查
   strategy.handle_arb_open_signal()
   ├─ check_leverage()           ──► 查询 exposure_manager.total_equity()
   ├─ check_symbol_exposure()    ──► 查询 exposure_manager + price_table.mark_price()
   ├─ check_total_exposure()     ──► 查询 exposure_manager + price_table
   └─ 检查失败 ──► 标记 alive_flag = false

4. 订单对齐
   align_um_order() ──► 验证 min_qty_table

5. 订单创建
   order_manager.create_order()
   ├─ 在 order_manager 中记录订单
   └─ 返回 client_order_id

6. 订单发送
   ├─ order.get_order_request_bytes()  ──► 序列化为 Bytes
   └─ trade_request_tx.send()          ──► 发送到交易引擎

7. 策略注册
   ctx.insert_strategy(Box::new(strategy))  ──► StrategyManager
```

### 成交处理流程

```
1. 账户监控接收成交
   Binance ──ExecutionReport──► MonitorChannel

2. pre_trade 接收并分发
   dispatch_execution_report()
   ├─ 获取 order_id = report.client_order_id
   ├─ 遍历所有策略：strategy_ids = strategy_mgr.iter_ids()
   └─ for each strategy_id:
       ├─ with_strategy_mut()
       └─ if strategy.is_strategy_order(order_id)
           ├─ match report.execution_type()
           │  ├─ ExecutionType::New/Canceled ──► strategy.apply_order_update()
           │  └─ ExecutionType::Trade ──► strategy.apply_trade_update()
           └─ if !strategy.is_active() ──► remove_strategy()
```

---

## strategy 使用 pre_trade 的详细方式

### 1. 风险检查接口

**检查杠杆率**
```rust
if let Err(e) = self.pre_trade_env.risk_checker.check_leverage() {
    self.alive_flag = false;
    return;
}
```

**检查单品种敞口**
```rust
if let Err(e) = self.pre_trade_env.risk_checker.check_symbol_exposure(&self.symbol) {
    self.alive_flag = false;
    return;
}
```

**检查总敞口**
```rust
if let Err(e) = self.pre_trade_env.risk_checker.check_total_exposure() {
    self.alive_flag = false;
    return;
}
```

### 2. 订单管理接口

**创建订单**
```rust
let client_order_id = self
    .pre_trade_env
    .risk_checker
    .order_manager
    .borrow_mut()
    .create_order(venue, order_id, order_type, symbol, side, qty, price, ts);
```

**查询订单**
```rust
let order = self
    .pre_trade_env
    .risk_checker
    .order_manager
    .borrow()
    .get(client_order_id);
```

### 3. 订单对齐和验证

**对齐订单量和价格**
```rust
let (aligned_qty, aligned_price) = self.pre_trade_env.align_order_by_venue(
    venue, symbol, raw_qty, raw_price
)?;
```

**检查最小交易要求**
```rust
self.pre_trade_env.check_min_trading_requirements(
    venue, symbol, aligned_qty, Some(aligned_price)
)?;
```

### 4. 订单发送

**获取订单请求字节并发送**
```rust
match order.get_order_request_bytes() {
    Ok(req_bin) => {
        self.pre_trade_env.trade_request_tx.send(req_bin)?;
    }
    Err(e) => {
        self.alive_flag = false;
        return Err(format!("Failed: {}", e));
    }
}
```

---

## pre_trade 管理 strategy 的方式

### 1. 策略的创建和注册

```rust
// 创建新策略
let strategy_id = StrategyManager::generate_strategy_id();
let env = ctx.get_pre_trade_env();
let mut strategy = HedgeArbStrategy::new(strategy_id, symbol, env);

// 处理信号
strategy.handle_signal(&signal);

// 注册策略
if strategy.is_active() {
    ctx.insert_strategy(Box::new(strategy));
}
```

### 2. 策略的信号分发

**ArbOpen 信号 - 创建新策略**
```rust
SignalType::ArbOpen => {
    // 创建并注册新策略
    ctx.insert_strategy(Box::new(strategy));
}
```

**ArbHedge 信号 - 更新现有策略**
```rust
SignalType::ArbHedge => {
    let strategy_id = hedge_ctx.strategy_id;
    ctx.with_strategy_mut(strategy_id, |strategy| {
        strategy.handle_signal(&signal);
    });
}
```

**ArbCancel 信号 - 查询同一 symbol 的策略**
```rust
SignalType::ArbCancel => {
    let symbol = cancel_ctx.get_opening_symbol().to_uppercase();
    let candidate_ids = ctx.strategy_mgr.ids_for_symbol(&symbol);
    for strategy_id in candidate_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            strategy.handle_signal(&signal);
        });
    }
}
```

### 3. 策略的执行回报处理

```rust
fn dispatch_execution_report(ctx: &mut RuntimeContext, report: &ExecutionReportMsg) {
    let strategy_ids = ctx.strategy_mgr.iter_ids().cloned().collect();
    for strategy_id in strategy_ids {
        ctx.with_strategy_mut(strategy_id, |strategy| {
            if strategy.is_strategy_order(order_id) {
                match report.execution_type() {
                    ExecutionType::Trade => strategy.apply_trade_update(report),
                    _ => strategy.apply_order_update(report),
                }
            }
        });
    }
    ctx.cleanup_inactive();
}
```

### 4. 策略的清理

```rust
fn with_strategy_mut<F>(&mut self, strategy_id: i32, mut f: F)
where F: FnMut(&mut dyn Strategy)
{
    if let Some(mut strategy) = self.strategy_mgr.take(strategy_id) {
        f(strategy.as_mut());
        if strategy.is_active() {
            self.strategy_mgr.insert(strategy);  // 策略继续运行
        } else {
            // 策略完成，自动清理
            self.remove_strategy(strategy_id);
        }
    }
}
```

---

## 关键交互点总结

| 交互点 | 源 | 目标 | 方式 | 位置 |
|--------|-----|------|------|------|
| 信号分发 | pre_trade | strategy | strategy.handle_signal() | runner.rs:994 |
| 风险检查 | strategy | pre_trade | risk_checker.check_*() | hedge_arb_strategy.rs:71 |
| 订单创建 | strategy | pre_trade | order_manager.create_order() | hedge_arb_strategy.rs:199 |
| 订单发送 | strategy | pre_trade | trade_request_tx.send() | hedge_arb_strategy.rs:364 |
| 回报处理 | pre_trade | strategy | strategy.apply_order_update() | runner.rs:1155 |
| 成交处理 | pre_trade | strategy | strategy.apply_trade_update() | runner.rs:1158 |
| 策略注册 | pre_trade | StrategyManager | insert() | runner.rs:1007 |
| 策略清理 | pre_trade | StrategyManager | remove() | runner.rs:1404 |

---

## 生成的文档文件

本分析生成了以下文档：

1. **PRE_TRADE_STRATEGY_ANALYSIS.md**（本目录）
   - 详细的关联关系分析
   - 完整的代码示例
   - 数据结构详解

2. **MODULE_RELATIONSHIP.txt**（本目录）
   - 快速参考指南
   - ASCII 架构图
   - 调用链路图

3. **ANALYSIS_SUMMARY.md**（本文件）
   - 执行摘要
   - 快速导航
   - 关键要点

---

## 关键文件导航

### pre_trade 模块核心文件

| 文件 | 行数范围 | 关键内容 |
|------|---------|---------|
| runner.rs | 994-1008 | ArbOpen 信号处理，创建策略 |
| runner.rs | 1120-1136 | ArbHedge 信号处理，查询策略 |
| runner.rs | 1089-1117 | ArbCancel 信号处理 |
| runner.rs | 1145-1196 | 执行回报分发 |
| runner.rs | 417-436 | get_pre_trade_env() 创建 |
| order_manager.rs | 1-100 | Side/OrderType 定义 |

### strategy 模块核心文件

| 文件 | 行数范围 | 关键内容 |
|------|---------|---------|
| hedge_arb_strategy.rs | 15-30 | HedgeArbStrategy 结构定义 |
| hedge_arb_strategy.rs | 67-224 | handle_arb_open_signal() 实现 |
| hedge_arb_strategy.rs | 227-337 | handle_arb_hedge_signal() 实现 |
| hedge_arb_strategy.rs | 349-397 | create_and_send_order() 实现 |
| risk_checker.rs | 25-35 | RiskChecker 结构定义 |
| risk_checker.rs | 388-415 | PreTradeEnv 结构定义 |
| manager.rs | 7-16 | Strategy trait 定义 |
| binance_um_impl.rs | 12-100 | OrderTradeUpdateMsg 实现 |

---

## 常见问题

### Q1: strategy 如何访问 pre_trade 的资源？
通过 `PreTradeEnv` 结构体，它包含了对 `RiskChecker`、`OrderManager` 等的引用。

### Q2: pre_trade 如何管理 strategy？
通过 `StrategyManager`，使用 `HashMap<i32, Box<dyn Strategy>>` 存储策略实例。

### Q3: 订单的完整生命周期是什么？
创建 → 提交 → 确认 → 成交 → 清理

### Q4: 策略何时被移除？
当 `strategy.is_active()` 返回 `false` 时，会被自动清理。

### Q5: 如何添加新的风险检查？
在 `RiskChecker` 中添加新的 `check_*()` 方法，然后在策略的 `handle_arb_open_signal()` 中调用。

---

## 设计要点

1. **分离关注点**：strategy 专注逻辑，pre_trade 管理资源
2. **共享引用**：使用 `Rc<RefCell<T>>` 实现多个地方的共享访问
3. **trait 驱动**：使用 `Strategy` trait 实现通用管理
4. **事件驱动**：通过信号和回报实现异步通信
5. **生命周期管理**：通过 `is_active()` 标志实现优雅清理

---

**分析时间：** 2025-11-11
**分析工具：** Claude Code
**版本：** 1.0
