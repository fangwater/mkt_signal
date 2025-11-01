# 持久化数据总览

本文汇总当前 `persist_manager` 负责落库的账号与策略类数据，方便排查对账与通道配置。

## 通道速览

| 场景 | 发布方 | Iceoryx 渠道 | 数据结构 | 默认 RocksDB 列族 |
| --- | --- | --- | --- | --- |
| Binance Margin `executionReport` | `account_monitor` | `account_execution/binance_margin` | `ExecutionRecordMessage` | `executions_binance_margin` |
| Binance UM `ORDER_TRADE_UPDATE` (`fs=UM`) | `account_monitor` | `account_execution/binance_um` | `OrderUpdateRecordMessage` | `executions_binance_um` |
| 策略信号记录 | `pre_trade` 等策略组件 | `pre_trade_signal_record` | `SignalRecordMessage` | 视 `SignalType` 而定（如 `signals_bin_single_forward_arb_open`） |

> 配置项位于 `config/persist_manager.toml`，可分别通过 `[execution]`、`[um_execution]`、`[signal]` 节点启停并修改通道名或回退间隔。

## 保证金 executionReport

- 来源：`account_monitor` 解析 Binance 账户事件后，将 `executionReport` 转换为 `ExecutionRecordMessage`，并在发布前按 `(order_id, trade_id, update_id, execution_type)` 做去重。
- 发布：Iceoryx 通道 `account_execution/binance_margin`，消息体为 `ExecutionRecordMessage::to_bytes()`。
- 持久化：`persist_manager` 的 `ExecutionPersistor` 订阅通道后写入 RocksDB 列族 `executions_binance_margin`，键格式 `recv_ts_us_order_id_update_id`。
- 主要字段：
  - `recv_ts_us`：`account_monitor` 收到并发布消息时的时间戳（µs）。
  - `event_time` / `transaction_time` / `order_creation_time` / `working_time`。
  - `order_id`、`trade_id`、`update_id`、`symbol`、`client_order_id` + 原始字符串。
  - `strategy_id = client_order_id >> 32`、`side`、`is_maker`、`is_working`。
  - 价格与数量：`price`、`quantity`、`last_executed_quantity`、`cumulative_filled_quantity`、`last_executed_price`。
  - 费用与额：`commission_amount`、`commission_asset`、`cumulative_quote`、`last_quote`、`quote_order_quantity`。
  - 订单元数据：`order_type`、`time_in_force`、`execution_type`、`order_status`。

## 统一账户 ORDER_TRADE_UPDATE

- 触发条件：账户事件的 `fs` 字段为 `"UM"`（USDS-M Futures）。
- 来源：`account_monitor` 将 `ORDER_TRADE_UPDATE` 解析为 `OrderTradeUpdateMsg` 后，基于 `(order_id, trade_id, execution_type, event_time)` 去重并构造 `OrderUpdateRecordMessage`。
- 发布：Iceoryx 通道 `account_execution/binance_um`，消息体为 `OrderUpdateRecordMessage::to_bytes()`。
- 持久化：`persist_manager` 的 `UmOrderUpdatePersistor` 监听通道并将原始字节写入 RocksDB 列族 `executions_binance_um`，键格式 `recv_ts_us_order_id_trade_id`。
- 关键字段：
  - `recv_ts_us`：写入前的本地时间戳；`account_recv_ts_us`：`account_monitor` 收到消息时的时间戳。
  - `event_time` / `transaction_time`、`business_unit`、`strategy_type`。
  - `order_id`、`trade_id`、`client_order_id`、`client_order_id_str`。
  - `strategy_id`（消息内原始值）、`derived_strategy_id`（通过 `client_order_id >> 32` 还原）。
  - 持仓与成交：`side`、`position_side`、`reduce_only`、`is_maker`。
  - 价格数量：`price`、`quantity`、`average_price`、`stop_price`、`last_executed_quantity`、`cumulative_filled_quantity`、`last_executed_price`。
  - 成交额与盈亏：`buy_notional`、`sell_notional`、`commission_amount`、`commission_asset`、`realized_profit`。
  - 订单元数据：`order_type`、`time_in_force`、`execution_type`、`order_status`。

## 策略信号记录

- 来源：`pre_trade` 及其他策略组件在生成触发信号时，构造 `SignalRecordMessage`，并根据配置决定是否发布。
- 通道：默认 `pre_trade_signal_record`，由 `SignalPublisher` 推送。
- 持久化：`persist_manager` 的 `SignalPersistor` 写入不同列族，目前主要使用 `signals_bin_single_forward_arb_open`。
- 内容：`SignalRecordMessage` 内含 `signal_type`、`strategy_id`、`context`（以策略自定义结构序列化），并记录生成时间戳。

## 配置与快速验证

- 配置文件：`config/persist_manager.toml` 中可独立启停三类持久化任务，示例：

```toml
[execution]
enabled = true
channel = "account_execution/binance_margin"

[um_execution]
enabled = true
channel = "account_execution/binance_um"

[signal]
enabled = true
channel = "pre_trade_signal_record"
```

- 快速验证：
  1. 启动 `persist_manager` 后检查日志，确认各通道订阅成功。
  2. 使用 `rocksdb` 工具或内部 HTTP 服务（如 `/signals/:kind`）读取列族，核对键格式与字段。
  3. 如需变更通道名称或禁用持久化，只需保持 `account_monitor` 与 `persist_manager` 的配置一致即可。
