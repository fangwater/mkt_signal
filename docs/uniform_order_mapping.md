# 统一订单（Uniform Order）映射说明

本文档说明 `uniform_order_record` 的链路、字段语义和当前策略映射规则。

## 数据链路

- 发布：`PersistChannel::publish_uniform_order(...)`
  - 文件：`src/pre_trade/persist_channel.rs`
  - channel：`uniform_order_record`
- 持久化消费：`UniformOrderPersistor`
  - 文件：`src/persist_manager/uniform_order_persist.rs`
  - CF：`uniform_orders`

## 事件分派（switch）

统一事件类型：`UniformOrderEventKind`

文件：`src/strategy/uniform_order_helper.rs`

- `New`
  - `create_ts = event_ts`
  - `update_ts = event_ts`
- `Terminal`
  - `create_ts = order.timestamp.create_t`
  - `update_ts = event_ts`
- `Trade`
  - `create_ts = order.timestamp.create_t`
  - `update_ts = event_ts`

策略侧仅负责传入 `event_kind` 与业务字段（`signal_ts/from_key/price_offset/amount_update/status`），公共 helper 统一完成 record 构造与发布。

## 标准结构

结构体：`UnifiedOrderRecord`

文件：`src/persist_manager/unified_order.rs`

| 字段 | 含义注释 |
| --- | --- |
| `symbol` | 交易标的字节（UTF-8），例如 `BTCUSDT`。 |
| `create_ts` | 订单创建时间戳。 |
| `update_ts` | 本次状态更新时间戳。 |
| `signal_ts` | 触发该订单的信号时间戳。 |
| `client_order_id` | 客户端订单 ID（i64，仅算法单）。 |
| `venue` | 交易所编码（`u8`，对齐 `TradingVenue`）。 |
| `ttype` | 订单类型编码（`u8`，对齐 `OrderType`）。 |
| `side` | 买卖方向编码（`u8`，对齐 `Side`）。 |
| `price` | 下单价格。 |
| `price_offset` | 价格偏移（来自信号上下文，不做反推）。 |
| `amount_init` | 初始下单数量。 |
| `amount_update` | 本次增量数量（由累计成交量差分得到）。 |
| `status` | 订单状态编码（`u8`，对齐 `OrderStatus`）。 |
| `from_key` | 来源规则字节（尾部不定长 `u32 + bytes`）。 |

## 统一编码约束

- 不使用 `String` 作为持久化记录字段，文本用 bytes。
- `venue/ttype/side/status` 均为 `u8` 枚举编码。
- `from_key` 以 `from_key_len(u32) + from_key(bytes)` 存放在生产者记录尾部。
- `persist_manager` 可在入库前追加可选 `bbo_spread_len(u16) + bbo_spread(bytes)`。旧记录没有该尾部字段，导出时为空字符串。
- `bbo_spread` 是 10 个逗号分隔数字：`open_tp,open_bid,open_bid_qty,open_ask,open_ask_qty,hedge_tp,hedge_bid,hedge_bid_qty,hedge_ask,hedge_ask_qty`。
- `bbo_spread` 的查询索引使用 uniform order 的 `update_ts`；NEW 对应挂单回报时间，TRADE 对应成交更新时间。
- `price_offset` 必须来自信号上下文，禁止盘口反推。

## 当前策略映射

## HedgeArbStrategy

文件：`src/strategy/hedge_arb_strategy.rs`

- NEW（`OrderStatus::New`）
  - `create_ts = update_ts = order_update.event_time()`
  - `signal_ts`：按腿选择 `open_signal_ts` / `hedge_signal_ts`
  - `from_key`：按腿前缀 `open|...` / `hedge|...`
  - `price_offset`：按腿使用 `open_price_offset` / `hedge_price_offset`
- PARTIAL/FILLED（trade update）
  - `create_ts = order.timestamp.create_t`
  - `update_ts = trade.event_time()`
  - `status = trade.order_status`
- TERMINAL（`Canceled/Expired/ExpiredInMatch`）
  - 复用统一 terminal 发布函数
  - `create_ts = order.timestamp.create_t`
  - `update_ts = order_update.event_time()`

## MarketMakerOpenStrategy

文件：`src/strategy/mm_open_strategy.rs`

- NEW / PARTIAL / FILLED / TERMINAL 均已映射到 `UnifiedOrderRecord`
- `signal_ts = self.signal_ts`
- `from_key = open|{self.open_from_key}`
- `price_offset = self.open_price_offset`（仅信号字段，不反推）
- PARTIAL/FILLED（trade update）
  - `create_ts = order.timestamp.create_t`
  - `update_ts = trade.event_time()`
  - `status = trade.order_status`

## MarketMakerHedgeStrategy

文件：`src/strategy/mm_hedge_strategy.rs`

- NEW / PARTIAL / FILLED / TERMINAL 均已映射到 `UnifiedOrderRecord`
- `signal_ts = self.signal_ts`
- `from_key = hedge|{MmHedgeCtx.from_key}`
- `price_offset = 0.0`（当前 MMHedge 上下文未提供独立偏移字段）
- PARTIAL/FILLED（trade update）
  - `create_ts = order.timestamp.create_t`
  - `update_ts = trade.event_time()`
  - `status = trade.order_status`

## amount_update 规则

统一逻辑：

- `amount_update = incoming_cum_qty - prev_cum_qty`（当 `incoming >= prev`）
- 若出现回退（`incoming < prev`），记录警告并写 `0.0`

该规则用于 NEW / TERMINAL / PARTIAL / FILLED 的统一口径，保证“增量更新”语义一致。
