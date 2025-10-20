# 订单记录广播方案

## 背景与目标
本方案用于支撑 `binance_funding_rate_arb` 策略相关的订单全链路可视化，要求整合 pre-trade 与 account monitor 两路数据，并通过 WebSocket 对外广播。目标包括：
- 清晰呈现挂单、对冲单（支持多个对冲腿）与成交消息的生命周期。
- 统一字段命名，使客户端可以通过 `client_order_id` 及相关上下文完成关联。
- 提供可扩展、可监控的广播进程，便于未来扩展至其他交易所或策略。

## 新增进程
- 进程名称：`binance_funding_rate_arb_broadcast`。
- 放置位置：`src/bin/binance_funding_rate_arb_broadcast.rs`，复用现有 runtime/connection 基础设施。
- 职责：
  - 订阅以下频道/数据源：
    1. pre-trade 推送（新增频道 `binance_funding_rate_arb_order`，使用 icexy2）。
    2. account monitor（icexy2 频道，订阅方式与 pre-trade 一致），接收订单状态、成交、撤单、拒单等事件。
  - 将多源事件规整为统一的中间模型后，聚合成完整订单包并广播。

## 数据流
1. **订阅层**：分别为 pre-trade、account monitor 创建连接，入队 `BrokerEvent`（字段包含来源、事件时间、client_order_id、原始 payload 等）。
2. **聚合层**：维护 `OrderCache`（键为 `client_order_id`），将事件映射为标准结构并驱动状态机更新：
   - 对冲腿允许存在多个（例如逐笔成交触发多次对冲），需使用 `hedge_orders: Vec<HedgeLeg>` 聚合。
   - 针对延迟到达的事件进行补齐（例如先收到成交，再收到确认）。
3. **广播层**：当订单状态有变化或达到终态时，将 `OrderRecordPack` 序列化为 JSON，通过 WebSocket 推送；新客户端建立连接后可要求发送最近 N 条快照。

## pre-trade 推送需求
- 频道：`binance_funding_rate_arb_order`（icexy2）。
- 现有 signal 程序已构造上下文（见 `BinSingleForwardArbOpenCtx`），后续需将字段明确落入以下结构，方便 broadcast 进程直接解析。
- 通用字段（所有事件均需携带）：
  - `event_type`：`signal_generated` / `order_committed` / `order_cancelled` / `order_rejected` 等；
  - `client_order_id`：原日志中的 `OrderId`；
  - `order_id`：交易所回报的 `ExchangeId`；
  - `strategy`：如 `binance_funding_rate_arb`；
  - `symbol`、`side`、`quantity`、`price`、`order_type`、`tif`；
  - `signal_ts`：ISO8601 UTC，signal 进程生成的时间；
  - `risk_flags`：数组，可为空；
  - `extra_tags`：记录 `MarginOpen` 等标签或模式信息。

### Signal Context 细化
- `context` 字段需包含以下键：
  - `strategy_id`、`symbol_key`、`emit_ts`（本地纳秒或毫秒时间戳）；
  - `amount`、`ratio`、`price_tick`、`exp_time`；
  - `funding_rate`、`sr_level`、`spread`；
  - 盘口快照：`book_snapshot_id`、`top_bid`、`top_ask`、`mid_price`、`depth_stats`（如 5 档聚合量）；
  - 风控/策略补充：`risk_flags`（与顶层字段一致，可冗余）以及 `extra`（键值对，可放置临时调试信息）。
- 解析失败时需记录告警，但仍推送原始 `ctx` 供后续排查。
- `signal_ctx_bytes`（新增在 `Order` 上）保留原始上下文：
  - Margin 开仓单：写入 `BinSingleForwardArbOpenCtx` + `OpenSignalMeta` 组成的 JSON，供 broadcast 还原信号细节；
  - UM 对冲单：写入触发成交的 `trade_id` 及 `spot_order_id`，用于在 UI 上串联开仓成交与对冲腿。

### 生命周期字段改造
- 原 pre-trade 订单日志字段映射为：
  - `submit_time` → `create_ts_local`（风控通过并向 trade engine 提交的本地时间）。
  - `create_time` → `create_ts_exchange`（交易所订单创建时间）。
  - `ack_time` 不再使用。
  - `filled_time`、`end_time` → 合并为 `filled_ts`（采用交易所成交时间；若需本地时间，可作为 `context.filled_local_ts` 追加）。
- 撤单相关：
  - 新增 `cancel_ts`（交易所时间）、`cancel_reason`；
  - 若因超时撤单，需额外写入 `extra_tags`（例如 `timeout_cancel`）。
- 拒单相关：
  - 输出 `reject_ts` 和 `reject_reason`；
  - 保留 `order_type`/`tif`，便于识别 GTX 穿价等场景。
- 以上字段在 pre-trade 推送中即可补齐，broadcast 进程只负责重组格式。

## WebSocket 服务
- 绑定地址：`0.0.0.0:15001`（常量，写死于配置或源码常量）。
- 协议：文本帧，JSON 格式。
- 消息类型：默认 `order_record_batch`（批量发送），另有 `heartbeat`（服务器每 15 秒一次，客户端 30 秒内需回 Pong）。
- 速率控制：对同一 `client_order_id` 变更设置 50ms 最小间隔；超过则批量合并。
- 安全性：初期内网开放，后续可在握手阶段校验来源或做白名单。

## 数据结构草案
### Rust 结构
```rust
#[derive(Serialize)]
struct OrderRecordPack {
    meta: RecordMeta,
    open_order: Option<OpenLeg>,
    hedge_orders: Vec<HedgeLeg>,
    fills: Vec<FillEvent>,
}

struct RecordMeta {
    client_order_id: String,
    strategy: String,
    symbol: String,
    base_ccy: String,
    quote_ccy: String,
    side: OrderSide,
    leverage: Option<f64>,
    position_mode: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

struct OpenLeg {
    signal: SignalContext,
    lifecycle: OrderLifecycle,
    quantity: Decimal,
    price: Decimal,
    order_type: OrderType,
    tif: Option<String>,
    extra_tags: Vec<String>,
}

struct HedgeLeg {
    hedge_id: String,
    signal_ts: DateTime<Utc>,
    source_trade_id: String,
    lifecycle: OrderLifecycle,
    quantity: Decimal,
    price: Decimal,
    order_type: OrderType,
    tif: Option<String>,
}

struct OrderLifecycle {
    signal_ts: Option<DateTime<Utc>>,
    commit_ts: Option<DateTime<Utc>>,
    exchange_ack_ts: Option<DateTime<Utc>>,
    end_state: Option<OrderEndState>,
}

enum OrderEndState {
    Filled { ts: DateTime<Utc>, fill_trade_ids: Vec<String> },
    Cancelled { ts: DateTime<Utc>, reason: CancelReason },
    Rejected { ts: DateTime<Utc>, reason_code: String, reason_msg: String },
}

struct SignalContext {
    signal_ts: DateTime<Utc>,
    strategy_id: Option<String>,
    symbol_key: Option<String>,
    emit_ts: Option<DateTime<Utc>>,
    amount: Option<Decimal>,
    ratio: Option<Decimal>,
    price_tick: Option<Decimal>,
    exp_time: Option<DateTime<Utc>>,
    funding_rate: Option<f64>,
    sr_level: Option<Decimal>,
    spread: Option<Decimal>,
    book_snapshot_id: Option<String>,
    top_bid: Option<Decimal>,
    top_ask: Option<Decimal>,
    mid_price: Option<Decimal>,
    depth_stats: Option<DepthStats>, // 自定义结构，存放 5 档等聚合信息
    risk_flags: Vec<String>,
    extra: HashMap<String, String>, // 需引入 use std::collections::HashMap;
}

struct DepthStats {
    bid_qty_5: Option<Decimal>,
    ask_qty_5: Option<Decimal>,
    imbalance: Option<Decimal>,
}

struct FillEvent {
    fill_id: String,
    order_source: FillSource, // "open" 或 "hedge"
    trade_id: String,
    exchange_ts: DateTime<Utc>,
    match_price: Decimal,
    match_qty: Decimal,
    fee: Decimal,
    fee_ccy: String,
    liquidity: String, // "maker" / "taker"
}
```

### JSON 对应字段
- 时间字段统一使用 ISO8601（UTC）字符串，如 `2024-05-30T12:34:56.789Z`。
- 金额与数量字段以字符串表示，避免精度损失。
- `hedge_orders` 数组用于表达多个对冲腿，数组元素和 `fills` 中的 `order_source` 帮助客户端建立关联。
- `meta.extra` 可扩展策略特定信息，例如风险标签、资金费率区间等（如需再补充）。

## 时间节点映射
- 挂单：
  - `signal_ts`：来自 signal 进程。
  - `commit_ts`=`create_ts_local`：pre-trade 风控通过后的下发时间（原 `submit_time`）。
  - `exchange_ack_ts`=`create_ts_exchange`：account monitor 推送的 `NEW` 事件时间（原 `create_time`）。
  - 结束：
    - `Filled`：使用 `filled_ts`（交易所成交时间）。
    - `Cancelled`：以 `cancel_ts`（交易所撤单时间）为准，并记录 `cancel_reason`。
    - `Rejected`：记录 `reject_ts` 与 `reject_reason`。
- 对冲单：
  - 指定对冲腿的 `signal_ts`：由交易所成交事件给出，视为信号。
  - 各对冲腿独立记录 `commit_ts`、`exchange_ack_ts`、`end_state`，并允许多个 `hedge_id`。
  - 市价单默认在成交时进入 `Filled`，若失败则落入 `Rejected`（携带 GTX 等拒单原因）。
- 成交消息：
  - 直接来自 account monitor，将 `FillEvent` 记录在 `fills`。
  - 若为开仓腿，则 `order_source = "open"`；若为某个对冲腿，则包含其 `hedge_id`。
