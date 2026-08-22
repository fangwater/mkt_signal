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
  - `create_ts = order.timestamp.create_t`（优先本地新订单请求首次 publish 时间；缺失时 fallback `event_ts`）
  - `update_ts = event_ts`
- `Terminal`
  - `create_ts = order.timestamp.create_t`（同上，缺失时 fallback `event_ts`）
  - `update_ts = event_ts`
- `Trade`
  - `create_ts = order.timestamp.create_t`（同上，缺失时 fallback `event_ts`）
  - `update_ts = event_ts`

策略侧仅负责传入 `event_kind` 与业务字段（`signal_ts/from_key/signal_bbo/price_offset/amount_update/status`），公共 helper 统一完成 record 构造与发布。

## 标准结构

结构体：`UnifiedOrderRecord`

规范定义：`crates/persist_common/src/unified_order.rs`

| 字段 | 含义注释 |
| --- | --- |
| `symbol` | 交易标的字节（UTF-8），例如 `BTCUSDT`。 |
| `create_ts` | 新订单请求首次 publish 的本地时间戳；缺失时由远端事件时间兜底。 |
| `update_ts` | 本次状态更新时间戳。 |
| `signal_ts` | 触发该订单的信号时间戳。 |
| `submit_ts` | 最近一次向 trade/query engine 发送请求的本地时间戳。 |
| `local_ts` | 最近一次在本地实质性接受订单/成交/查询回报的时间戳。 |
| `mkt_ts` | 决策盘口事件时间；双腿信号取两腿 `ts` 最大值。 |
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
| `signal_bbo` | 决策时冻结的结构化 BBO，可分别包含 open/hedge 腿。 |

## 统一编码约束

- 不使用 `String` 作为持久化记录字段，文本用 bytes。
- `venue/ttype/side/status` 均为 `u8` 枚举编码。
- `from_key` 以 `from_key_len(u32) + from_key(bytes)` 存放，不再包含 `open_bid/open_ask/hedge_bid/hedge_ask`。
- pre-trade IPC 在 `from_key` 后固定写入 83 字节 `signal_bbo`。
- `persist_manager` 入库时将事件盘口插到 `signal_bbo` 前，RocksDB 尾部布局为 `bbo_spread_len(u16) + bbo_spread(bytes) + signal_bbo(83 bytes)`。
- `signal_bbo` 使用固定二进制布局，不包含 version 或 magic：
  - `presence(u8)`：bit 0 表示 open，bit 1 表示 hedge。
  - open/hedge 各占 41 字节：`venue(u8) + ts(i64) + bid_price(f64) + bid_qty(f64) + ask_price(f64) + ask_qty(f64)`。
  - 整数和浮点数均使用 little-endian。
- 历史 RocksDB 记录若没有 83 字节尾部，`signal_bbo` 直接解析为 `None`；不会从旧 `from_key` 或 `bbo_spread` 重建。
- `bbo_spread` 是 10 个逗号分隔数字：`open_tp,open_bid,open_bid_qty,open_ask,open_ask_qty,hedge_tp,hedge_bid,hedge_bid_qty,hedge_ask,hedge_ask_qty`。
- `bbo_spread` 的查询索引使用 uniform order 的 `update_ts`；NEW 对应挂单回报时间，TRADE 对应成交更新时间。
- 因此 `signal_bbo` 表示决策时盘口，`bbo_spread` 表示订单事件时盘口，两者不互相替代。
- `price_offset` 必须来自信号上下文，禁止盘口反推。

## Parquet 映射

`signal_bbo` 导出为 12 个强类型可空列：

| 腿 | 列 |
| --- | --- |
| open | `signal_open_venue`, `signal_open_ts`, `signal_open_bid_price`, `signal_open_bid_qty`, `signal_open_ask_price`, `signal_open_ask_qty` |
| hedge | `signal_hedge_venue`, `signal_hedge_ts`, `signal_hedge_bid_price`, `signal_hedge_bid_qty`, `signal_hedge_ask_price`, `signal_hedge_ask_qty` |

旧 Parquet 完全没有这 12 列时，回灌保持历史记录布局；新 Parquet 必须完整包含 12 列。单腿的 6 列必须同时为 null 或同时有值。

## signal_bbo 槽位规则

| 模式/订单 | open 槽 | hedge 槽 |
| --- | --- | --- |
| intra arb open/close | 开仓腿 BBO | 对冲腿 BBO |
| intra arb hedge | null | 对冲决策 BBO |
| MM open | 报价腿 BBO | null |
| MM hedge | null | 对冲决策 BBO |
| exec | 执行腿 BBO | null |

## Intra 触发时间分类

对于 intra-arb 开仓，价差信号由两腿中本次更新较新的 BBO 触发。分析时使用
`signal_bbo` 中冻结的双腿时间，而不是可在订单生命周期中变化的 `mkt_ts`：

- `signal_open_ts > signal_hedge_ts`：现货/open 腿触发，
  `trigger_mkt_ts = signal_open_ts`。
- `signal_hedge_ts > signal_open_ts`：合约/hedge 腿触发，
  `trigger_mkt_ts = signal_hedge_ts`。
- 两者相等：标记为 `tie`，不得任选一腿。

新旧记录按以下规则处理：

- 两腿时间戳都大于 0：`new_signal_bbo`，可以精确分类触发腿。
- Parquet 完全没有 12 个 `signal_bbo` 列：`legacy_schema`，回退到 `mkt_ts`，
  触发腿标记为未知。
- 12 列存在但双腿均为空：`legacy_or_empty_signal_bbo`。Parquet 已丢失物理尾段
  是否存在的信息，不能进一步断言是旧记录还是新记录的空 presence mask。
- 只有一腿有效：`incomplete_signal_bbo`，不得据此推断双腿价差的触发源。

延迟口径：

- `signal_ts - trigger_mkt_ts` 是交易所 BBO 事件到本地信号生成的延迟。它包含
  交易所与本机时钟差、网络、解析、IPC 和决策计算，不是纯系统内部延迟。
- `create_ts - signal_ts` 才是信号生成到首次下单请求 publish 的系统内部延迟；
  可以按现货触发和合约触发分组，但两组使用相同的时间端点。

## 当前策略映射

## HedgeArbStrategy

文件：`src/strategy/hedge_arb_strategy.rs`

- NEW（`OrderStatus::New`）
  - `create_ts = order.timestamp.create_t`（优先本地新订单请求首次 publish 时间；缺失时 fallback `order_update.event_time()`）
  - `update_ts = order_update.event_time()`
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
- `signal_bbo.open = MmOpenCtx.opening_leg`
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
- `signal_bbo.hedge = MmHedgeCtx.opening_leg`（上下文字段名沿用 opening，持久化语义为 hedge）
- `price_offset = MmHedgeCtx.price_offsets` 对应订单档位的偏移
- PARTIAL/FILLED（trade update）
  - `create_ts = order.timestamp.create_t`
  - `update_ts = trade.event_time()`
  - `status = trade.order_status`

## amount_update 规则

统一逻辑：

- `amount_update = incoming_cum_qty - prev_cum_qty`（当 `incoming >= prev`）
- 若出现回退（`incoming < prev`），记录警告并写 `0.0`

该规则用于 NEW / TERMINAL / PARTIAL / FILLED 的统一口径，保证“增量更新”语义一致。
