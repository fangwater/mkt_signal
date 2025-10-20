# Binance Funding Rate Arb 订单时间戳来源

本文汇总 `binance_funding_rate_arb` 策略内部对 **Margin 订单** 与 **UM 订单** 的生命周期时间戳填充规则，便于排查订单状态与广播数据的来源。时间戳字段均来自 `pre_trade::Order` 结构：

- `create_ts_local`
- `create_ts_exchange`
- `filled_ts`
- `cancel_ts`
- `reject_ts`
- `signal_ctx_bytes`

除非特别说明，策略内部使用 `get_timestamp_us()` 取得的本地时间为微秒（Unix 时间），交易所推送的 `event_time` / `transaction_time` 等为 Binance WS 上报的毫秒时间。

## Margin 订单（开仓 / 平仓）

| 字段 | 触发消息 | 处理函数/位置 | 时间来源 | 说明 |
| --- | --- | --- | --- | --- |
| `create_ts_local` | 策略提交下单请求时 | `BinSingleForwardArbStrategy::submit_margin_open` / `close_margin_with_limit` | 本地 `get_timestamp_us()` | 调用 `Order::new` 后，立即写入提交给 Trade Engine 的时间。 |
| `create_ts_exchange` | Account Monitor `ExecutionReportMsg`，`order_status=NEW` | `handle_binance_margin_order_update` | `report.event_time` | 交易所确认订单创建时记录，仅首次写入。 |
| `filled_ts` | Account Monitor `ExecutionReportMsg`，`order_status=FILLED` | `handle_binance_margin_order_update` | `report.event_time` | 记录交易所层面的最终成交时间。 |
| `cancel_ts` | 1. Account Monitor `ExecutionReportMsg`，`order_status=CANCELED/EXPIRED`<br>2. 撤单 API 回执 `TradeExecOutcome`（状态 2xx）<br>3. 周期性超时撤单逻辑 | 1.`handle_binance_margin_order_update`<br>2.`handle_trade_response`（分支 `TradeRequestType::BinanceCancelMarginOrder`）<br>3.`hanle_period_clock` | 1.`report.event_time`<br>2.`get_timestamp_us()`（撤单回执本地时间）<br>3.`current_tp`（周期调度传入的本地微秒） | 优先使用交易所 `event_time`。若由撤单回执或超时逻辑触发，则写入本地时间。 |
| `reject_ts` | 1. Trade Engine 响应 `TradeExecOutcome` 非 2xx 状态<br>2. Account Monitor `ExecutionReportMsg`，`order_status=REJECTED/TRADE_PREVENTION` | 1.`handle_trade_response`<br>2.`handle_binance_margin_order_update` | 1.`get_timestamp_us()`<br>2.`report.event_time` | 记录拒单发生时间。若交易所直接拒单，使用其事件时间。 |
> 注：`signal_ctx_bytes` 在下单成功提交时写入，包含触发信号的上下文序列化结果（详见后文）。

额外说明：
- `cancel_ts`/`reject_ts` 会与 `OrderExecutionStatus` 联动，保持状态机一致。
- 当策略触发超时撤单时，会先尝试通过 API 下撤单请求，若立即返回成功，则写入第二种来源的本地时间；若后续收到 `ExecutionReport`，同一字段会使用交易所 `event_time` 覆盖。

## UM 订单（对冲 / 平仓）

UM 订单为市价单，默认没有人工撤单路径；若发生 `CANCELED` / `REJECTED` 状态属于异常场景。

| 字段 | 触发消息 | 处理函数/位置 | 时间来源 | 说明 |
| --- | --- | --- | --- | --- |
| `create_ts_local` | 策略提交 UM 对冲/平仓请求时 | `create_hedge_um_order_from_margin_order` / `close_um_with_market` | 本地 `get_timestamp_us()` | 在构造 `Order::new` 后立即写入。 |
| `create_ts_exchange` | 账户流 `OrderTradeUpdateMsg`，`execution_type=NEW` | `handle_binance_futures_order_update` → `apply_um_fill_state` | `event.event_time` | Binance UM 成交推送中的 `event_time`。 |
| `filled_ts` | 账户流 `OrderTradeUpdateMsg`，`order_status=FILLED` | 同上 | `event.event_time` | 市价单成交时更新。 |
| `cancel_ts` | **无常规路径** | — | — | 策略预期 UM 对冲单不会撤单。若收到 `CANCELED`/`EXPIRED`，代码会 `panic!` / `warn!`，未写入字段。 |
| `reject_ts` | Trade Engine 响应 `TradeExecOutcome` 非 2xx 状态 | `handle_trade_response` | `get_timestamp_us()` | 只在下单 API 直接失败时记录。Binance UM 推送若出现 `REJECTED` 当前未做专门处理（视为异常）。 |
> 注：`signal_ctx_bytes` 在下单时写入，包含触发对冲的 margin 成交通知 `trade_id` 等关联信息。

## `signal_ctx_bytes` 内容

- **Margin 开仓单**：创建订单时，策略会将 `BinSingleForwardArbOpenCtx` 与 `OpenSignalMeta`（含触发时间、资金费率、盘口信息等）封装为 JSON 并写入 `signal_ctx_bytes`。用于后续广播时快速恢复信号上下文。
- **UM 对冲单**：当 margin 单的成交事件触发对冲时，`signal_ctx_bytes` 保存 Binance `executionReport` 中的 `trade_id`（字段 `t`）及关联的 `spot_order_id`，以便将对冲腿与原始成交进行绑定。

## 查看/定位提示

- Margin 订单关键逻辑集中在 `src/signal/binance_forward_arb.rs` 的 `handle_trade_response`、`handle_binance_margin_order_update` 与 `hanle_period_clock`。  
- UM 订单的状态更新由 `handle_binance_futures_order_update` 和 `apply_um_fill_state` 维护。  
- 所有时间戳最终存放在 `pre_trade::Order`，广播或日志输出时可直接读取对应字段。  
- 若需要扩展额外消息来源，应确保在 `Order` 更新前后维持状态机一致，避免重复计数或覆盖非预期时间。
