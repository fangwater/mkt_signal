# trade_engine IPC→WS 端到端延迟采样

**日期**: 2026-05-07
**目标**: 在 trade_engine 上加一组 KLL 延迟分位数采样，量化"IPC 收到下单/撤单消息 → WS 实际发出"的端到端延迟（μs），按 10000 样本一次 flush 输出 p50/p90/p95/p99，新建单和撤单分桶。为后续做热路径优化提供 baseline。

## 背景

trade_engine 当前没有端到端延迟 instrumentation。`spread_pbs` 已经实现了基于 `KllSketch` 的延迟统计 wrapper（`src/spread_pbs/latency.rs::LatencyKll`，10000 样本默认 capacity，30s 兜底窗口，输出 p90/p95/p99）。本次复用该基础设施。

## 数据流（采样点定位）

```
subscriber.receive()                          [engine.rs:2000]   IPC 收到 raw payload
  ↓ binary parse
TradeRequestMsg::parse()                      [engine.rs:2016]
  ↓ T0 打点（本设计新增）
req_tx.send(msg)                              [engine.rs:2024]   入 mpsc
  ↓
req_rx.recv()                                 [engine.rs:661]    req_worker 取出
  ↓ 路由
endpoints[idx].send(WsCommand::Send(msg))     [engine.rs:710]    入 WS client mpsc
  ↓
cmd_rx.recv()                                 [ws_client.rs:803] WS event_loop 取出
  ↓ handle_send → flush_pending → send_one
ws.send(Message::Text(payload))               [ws_client.rs:1035] T1 打点（本设计新增）
```

T0/T1 之差 = 一条消息从 IPC parse 完成到真正下到 WS 的完整内部延迟。

## 设计

### 1. `LatencyKll` 移到共享位置（破坏性变更）

- **新增**: `src/rolling_metrics/latency_kll.rs`，从 `src/spread_pbs/latency.rs` 迁移并增强：
  - 输出加 `p50`，格式：`[<label>] latency_us n=N p50=.. p90=.. p95=.. p99=..`
  - 日志前缀从硬编码 `spread_pbs[{}]` 改成 `[{}]`，由 caller 通过 `label` 提供完整上下文
- **删除**: `src/spread_pbs/latency.rs`
- **更新**: `src/rolling_metrics/mod.rs` 加 `pub mod latency_kll;`
- **更新 spread_pbs 调用方**: import 路径改为 `crate::rolling_metrics::latency_kll::LatencyKll`，label 自带 `spread_pbs:` 前缀（保持上下文）
  - 涉及文件（按当前 import 实际情况，命中再改）：`src/spread_pbs/{adapter,binance,bybit,gate,bitget,okex,app,mod}.rs`

**破坏性影响**: spread_pbs 现有日志格式从 `spread_pbs[net] latency_us n=... p90=...` 变成 `[spread_pbs:net] latency_us n=... p50=... p90=...`（新增 p50，前缀方括号内由 caller 决定）。已与用户确认接受。

### 2. 时间戳载体：扩展 `TradeRequestMsg`

`src/trade_engine/trade_request.rs`：

```rust
pub struct TradeRequestMsg {
    pub req_type: TradeRequestType,
    pub create_time: i64,
    pub client_order_id: i64,
    pub params: Bytes,
    pub ipc_recv: Option<std::time::Instant>,  // 新增
}
```

- `Instant` 是 `Copy`，与 `Clone` 派生兼容，零开销
- 只在进程内流转，不参与 IPC wire format 序列化（`parse()` 由二进制头部构造，新字段在 parse 后由 caller 填）
- `parse()` 返回 `Some(...)` 时把字段初始化为 `None`，由 engine 主循环立即覆盖为 `Some(Instant::now())`

### 3. 聚合 KLL 实例（跨 endpoint 共享）

`TradeEngine::run_with_shutdown` 早期（mpsc 构造附近，engine.rs 约 232 行）创建：

```rust
let lat_new = Arc::new(parking_lot::Mutex::new(
    LatencyKll::with_capacity(format!("trade_engine:{}:ws:new", exchange.as_str()), 10_000),
));
let lat_cancel = Arc::new(parking_lot::Mutex::new(
    LatencyKll::with_capacity(format!("trade_engine:{}:ws:cancel", exchange.as_str()), 10_000),
));
```

- 用 `parking_lot::Mutex`：sync 锁、无 await、无锁路径亚 μs，已是仓库依赖
- `Arc::clone` 进每个 WS client（构造 `TradeWsClient` 时通过参数传入，新增两个字段 `lat_new` / `lat_cancel`）

### 4. T0 打点

`src/trade_engine/engine.rs:2017`，parse 成功后：

```rust
match TradeRequestMsg::parse(&owned) {
    Some(mut msg) => {
        msg.ipc_recv = Some(std::time::Instant::now());
        // ... 现有 debug! 和 req_tx.send(msg)
    }
    None => { ... }
}
```

### 5. T1 采样

`src/trade_engine/ws_client.rs::send_one`，`ws.send(Message::Text(payload)).await` 之前（line 1035）：

```rust
if let Some(t0) = msg.ipc_recv {
    let us = t0.elapsed().as_micros() as f64;
    match classify_ws_action(msg.req_type) {
        WsAction::New    => self.lat_new.lock().push(us),
        WsAction::Cancel => self.lat_cancel.lock().push(us),
        WsAction::Other  => {}
    }
}
ws.send(Message::Text(payload)).await?;
```

- 锁范围最小，仅一次 `push`
- 失败/被拒消息（如 inflight 超限走 `notify_rejected` 提前 return）**不采样**

### 6. 新增分类 helper

`src/trade_engine/ws_client.rs` 内（或邻近 trade_type_mapping.rs）新增：

```rust
enum WsAction { New, Cancel, Other }

fn classify_ws_action(rt: TradeRequestType) -> WsAction {
    use TradeRequestType::*;
    match rt {
        BinanceWsNewUMOrder | BinanceWsNewMarginOrder
        | OkexNewMarginOrder | OkexNewUMOrder
        | GateUnifiedNewOrder | GateFuturesNewOrder
        | BybitNewMarginOrder | BybitNewUMOrder
        | BitgetNewMarginOrder | BitgetNewUMOrder => WsAction::New,

        BinanceWsCancelUMOrder | BinanceWsCancelMarginOrder
        | OkexCancelMarginOrder | OkexCancelUMOrder
        | GateUnifiedCancelOrder | GateFuturesCancelOrder
        | BybitCancelMarginOrder | BybitCancelUMOrder
        | BitgetCancelMarginOrder | BitgetCancelUMOrder => WsAction::Cancel,

        _ => WsAction::Other,  // 走 REST 的或非 WS 类型，理论上不应到达 send_one
    }
}
```

## 输出示例

稳态下每 10000 条 trade WS 消息会刷一次：

```
[trade_engine:binance:ws:new]    latency_us n=10000 p50=42  p90=78  p95=110 p99=350
[trade_engine:binance:ws:cancel] latency_us n=10000 p50=39  p90=72  p95=98  p99=280
```

flush 触发条件（沿用现有 `LatencyKll` 行为）：
- buffer 满 10000，或
- 距上次 flush 超过 30 秒（兜底，避免低流量时间窗口数据陈旧）

## 显式取舍 / Caveats

1. **包含 backoff 期间排队的消息**：WS 重连期间消息会停在 `self.pending` 里，重连后 `flush_pending` 一次发出，这些样本会拉高 p99。选 raw 测量是因为：(a) backoff 是异常态、不常见，(b) 与其加过滤逻辑、不如保留信号让用户结合其他日志识别。
2. **不覆盖 REST 路径**：本次只测 WS。REST 走 `dispatcher.lock().await` 路径完全不同，要单测的话另开。
3. **不覆盖 WS query 消息**：query 不是延迟敏感路径，跳过。
4. **不采样失败消息**：inflight 超限被拒等情况不进 KLL（避免污染稳态分布）。
5. **T0 在 binary parse 之后**：`subscriber.receive()` 到 parse 完毕之间还有 `Bytes::copy_from_slice` 和 parse 调用，亚 μs 级，对统计影响可忽略；放在 parse 后是为了拿到 `req_type` 之前不需要打点。

## 文件改动清单

新增：
- `src/rolling_metrics/latency_kll.rs`

删除：
- `src/spread_pbs/latency.rs`

修改：
- `src/rolling_metrics/mod.rs` — 导出新模块
- `src/spread_pbs/mod.rs` — 移除 latency mod，import 改路径，label 加 `spread_pbs:` 前缀以保留上下文
- `src/spread_pbs/{adapter,binance,bybit,gate,bitget,okex,app}.rs` — 仅命中处改 import / label
- `src/trade_engine/trade_request.rs` — 加 `ipc_recv: Option<Instant>` 字段
- `src/trade_engine/engine.rs` — 构造两个 `Arc<Mutex<LatencyKll>>`、stamp T0、传 Arc 进 WS client
- `src/trade_engine/ws_client.rs` — `TradeWsClient` 加 `lat_new` / `lat_cancel` 字段、构造函数参数、`send_one` 采样、新增 `classify_ws_action`

## 验证

构建期：
- `cargo build --release` 通过
- `cargo fmt --all` 通过
- `cargo clippy -- -D warnings` 通过

运行期（手测）：
- 启动 trade_engine binance，在有交易信号的环境下观察日志
- 预期看到稳态下两条独立的 `[trade_engine:binance:ws:new]` / `[trade_engine:binance:ws:cancel]` 输出
- 观察 spread_pbs 的 latency 日志格式从 `spread_pbs[X] ...` 变为 `[spread_pbs:X] ...`，且新增 p50 字段

## 非目标

- 不做热路径优化本身（已在前次审计列出，待 baseline 拿到后再决定）
- 不引入新的指标系统或导出（仅日志）
- 不改 `LatencyKll` 内部 KLL 算法
