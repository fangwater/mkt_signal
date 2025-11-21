# 持久化数据总览

`persist_manager` 现在只负责接收 `pre_trade` 的持久化通道，并把原始字节写入 RocksDB，同时通过 HTTP 暴露查询与 Parquet 导出功能。下表汇总三类数据源：

| 数据类型 | Iceoryx 渠道 | RocksDB 列族 | HTTP 列表 | HTTP 导出 |
| --- | --- | --- | --- | --- |
| 策略信号 `SignalRecordMessage` | `pre_trade_signal_record` | `signals_arb_open` / `signals_arb_close` / `signals_arb_cancel` / `signals_arb_hedge` | `GET /signals/:kind` | `GET /signals/:kind/export` |
| 成交更新（`TradeUpdate` 序列化） | `trade_update_record` | `trade_updates` | `GET /trade_updates` | `GET /trade_updates/export` |
| 订单更新（`OrderUpdate` 序列化） | `order_update_record` | `order_updates` | `GET /order_updates` | `GET /order_updates/export` |

> `persist_manager` 启动参数：`cargo run --bin persist_manager -- --port 8088`。列族路径固定为 `data/persist_manager`。

## 策略信号

- 发布：`PreTrade` 在生成 ArbOpen/ArbClose/ArbHedge/ArbCancel 信号后，通过 `PersistChannel::publish_signal_record` 写入 `pre_trade_signal_record`。
- 落库：`SignalPersistor` 根据 `SignalType` 写入不同列族（`signals_arb_open`、`signals_arb_close`、`signals_arb_cancel`、`signals_arb_hedge`）。
- HTTP：
  - `GET /signals/:kind?limit=100&direction=desc&start=<key>&start_ts=<us>&end_ts=<us>`
  - `GET /signals/:kind/:key`
  - `DELETE /signals/:kind` （body: `{"keys":["..."]}`） / `DELETE /signals/:kind/:key`
  - `GET /signals/:kind/export` 导出 Parquet，列包含上下文解析后的 JSON 字段。
- `kind` 取值：`signals_arb_open` / `signals_arb_close` / `signals_arb_cancel` / `signals_arb_hedge`。

## 成交更新（trade_updates）

- 发布：`HedgeArbStrategy` 在处理成交时调用 `PersistChannel::publish_trade_update`，内部以固定顺序序列化 `TradeUpdate` trait 的字段（事件时间、成交价量、手续费、`order_status` 等）。
- 落库：`TradeUpdatePersistor` 订阅 `trade_update_record`，以处理时间戳（20 位整数）作为键写入 `trade_updates` 列族。
- HTTP：
  - `GET /trade_updates?limit=200&direction=desc&start=<key>&start_ts=<us>&end_ts=<us>`
  - `GET /trade_updates/:key`
  - `DELETE /trade_updates`（批量） / `DELETE /trade_updates/:key`
  - `GET /trade_updates/export` 返回 Parquet，字段覆盖 price/qty/commission/maker/venue/order_status 等。

## 订单更新（order_updates）

- 发布：策略侧收到订单状态变化后调用 `PersistChannel::publish_order_update`，序列化 `OrderUpdate` trait（包含 `OrderType`、`TimeInForce`、累计成交量、执行类型、业务单元等）。
- 落库：`OrderUpdatePersistor` 订阅 `order_update_record`，按时间戳写入 `order_updates` 列族。
- HTTP：
  - `GET /order_updates?limit=200&direction=desc&start=<key>&start_ts=<us>&end_ts=<us>`
  - `GET /order_updates/:key`
  - `DELETE /order_updates` / `DELETE /order_updates/:key`
  - `GET /order_updates/export`，Parquet 中包含价量、`order_type`、`time_in_force`、`execution_type`、`average_price`、`business_unit` 等字段。

## 快速操作

1. **启动**：`cargo run --bin persist_manager -- --port 8088`（默认 8088）。
2. **连通性**：`curl http://127.0.0.1:8088/health`。
3. **按时间分页**：
   - `limit`：每页 1~1000（导出时上限 10,000）。
   - `direction=desc` 默认从最新数据倒序。
   - `start`：直接传 RocksDB key（20 位时间戳；信号类形如 `ts_strategyId`）。
   - `start_ts` / `end_ts`：微秒级过滤窗口。
4. **Parquet 导出**：所有接口返回 `Content-Type: application/vnd.apache.parquet`，文件名以数据类型 + 时间戳命名，可直接用 `polars` / `pandas` 打开，例如：
   ```bash
   curl "http://127.0.0.1:8088/trade_updates/export?limit=1000" -o trade_updates.parquet
   ```
5. **数据清理**：三个数据集均支持单条与批量删除，便于回收异常记录或做重处理。

## 配置提示

- Iceoryx 通道名称来自 `PersistChannel` 常量，如需自定义，可同时修改 `pre_trade` 与 `persist_manager` 的常量（`TRADE_UPDATE_RECORD_CHANNEL`、`ORDER_UPDATE_RECORD_CHANNEL`、`PRE_TRADE_SIGNAL_RECORD_CHANNEL`）。
- RocksDB 目录默认 `data/persist_manager`。线上可在启动脚本中通过符号链接将 `target/release/crypto_proxy` 指向最新二进制，再配合 `persist_manager --port <port>` 启动。
