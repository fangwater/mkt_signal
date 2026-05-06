# 订单链路延迟统计与测试方案

## 目标

统计从行情进入本地、数据聚合、信号产生、`pre_trade` 创建订单、`trade_engine`
提交订单、交易所确认订单、账户/成交事件从交易所返回本地 colo 的完整链路延迟。

结论必须满足两个约束：

- 不把缺失埋点的链路段用推测值补齐。
- 所有统计都保留样本数、负延迟数量、分位数和最大值，不能只看均值。

## 现有订单数据可证明的口径

`order_export` 导出的 `uniform_orders.parquet` 是当前最可靠的全策略订单口径：

- `signal_ts`：触发该订单的信号时间，本地微秒。
- `create_ts`：订单创建时间。NEW 事件等于订单更新事件时间；后续事件等于本地订单记录中的交易所创建时间。
- `update_ts`：本次订单状态更新时间，来自交易所事件时间，微秒。
- `recv_ts_us`：`pre_trade` 发布 uniform order 持久化消息时的本地时间，微秒。
- `ts_us`：RocksDB key 对应的本地落盘时间；当前等于 payload 头部时间。

可直接统计：

- `update_to_local_ms = (recv_ts_us - update_ts) / 1000`
- `trade_update_to_local_ms = (recv_ts_us - update_ts) / 1000`，仅取
  `status in {PARTIALLY_FILLED,FILLED}` 且 `amount_update > 0` 的成交更新行。
- `signal_to_update_ms = (update_ts - signal_ts) / 1000`
- `signal_to_local_ms = (recv_ts_us - signal_ts) / 1000`
- 订单首事件：
  - `signal_to_first_update_ms`
  - `first_update_to_local_ms`
  - `signal_to_first_local_ms`

`order_updates_unmatched.parquet` 和 `trade_updates_unmatched.parquet` 只覆盖未匹配到策略的旁路事件，
不能代表全量订单，只能用于异常调查和交叉验证。

## 当前不能无偏统计的链路段

以下链路段目前没有全量结构化落盘字段，不能从最近订单数据中精确拆分：

- 行情交易所事件时间到 `mkt_pub` 本地接收。
- `mkt_pub` 发布到 `depth_pub` / `trade_flow_feature_pub` / `rolling_metrics` / `model_pub` 的每段本地耗时。
- 聚合结果进入 `trade_signal` 到信号发布的内部耗时。
- `pre_trade` 接收信号到 `OrderManager::create_order`、构造 request bytes、Iceoryx publish 的细分耗时。
- `trade_engine` Iceoryx receive、路由、REST/WS payload 构造、socket send start/done 的细分耗时。
- REST/WS ack 本地收到时间到 `pre_trade` 监听到 `order_resps/*` 的耗时。

要对这些段下结论，必须新增统一 trace 埋点。

## 必需埋点

为每条订单链路附带同一个 `trace_id`，优先复用 `client_order_id`。所有时间戳用微秒。

推荐新增结构化事件 `latency_trace`：

- `trace_id`
- `env_name`
- `venue`
- `symbol`
- `stage`
- `local_ts_us`
- `exchange_ts_us`
- `source_seq` 或 `update_id`
- `client_order_id`
- `order_id`
- `status`

最低埋点点位：

- `mkt_pub`: raw ws frame received, parsed msg publish。
- `depth_pub` / `trade_flow_feature_pub` / `rolling_metrics`: input receive, aggregate publish。
- `trade_signal`: aggregate/model input receive, decision start, signal publish。
- `pre_trade`: signal receive, risk check start/done, order object created, request bytes built, order request publish。
- `trade_engine`: order request receive, route chosen, request send start, request send done, response receive, response publish。
- `account_monitor`: user stream frame receive, order/trade parsed, basic account event publish。
- `pre_trade`: order/trade update receive, strategy applied, uniform order publish。

## 时钟要求

- 本地同机链路可直接比较 `get_timestamp_us()`。
- 跨机器链路必须记录每台机器的 `chronyc tracking` 或 PTP offset，统计结果需标注最大时钟误差。
- 交易所时间到本地时间的延迟必须标注“交易所时间源 + 本机时钟”的误差界限。
- 若出现负延迟，不能删除；必须按 venue/symbol/order_id 列入异常清单。

## 历史数据统计流程

1. 固定统计窗口，例如最近 24 小时：

```bash
/home/fanghaizhou/order_export/bin/order_export \
  --base-dir /home/fanghaizhou \
  --env-name bybit_mm_alpha \
  --input-dir /home/fanghaizhou/bybit_mm_alpha/data/persist_manager \
  --output-root /home/fanghaizhou/mkt_signal/tmp_latency_exports/bybit_mm_alpha \
  --start 2026-05-05T08:43:58Z \
  --end 2026-05-06T08:43:58Z
```

2. 校验导出 schema 与基础质量：

```bash
python3 scripts/check_order_export_parquet.py <export_dir>
```

3. 统计延迟：

```bash
python3 scripts/analyze_order_latency.py <export_dir_1> <export_dir_2> --top 20
```

4. 对 `negative > 0`、p99/max 明显异常的样本，按 `client_order_id` 回查：

- `uniform_orders.parquet`
- `order_updates_unmatched.parquet`
- `trade_updates_unmatched.parquet`
- account monitor 日志
- trade engine 日志
- 交易所 REST order query

## 主动测试流程

1. 先跑只读/低风险链路：开启 trace 埋点但不主动下单，确认行情、聚合、信号、pre_trade 入口的 trace 完整。
2. 小额 maker-only 订单测试 ack 延迟：使用最小数量、远离盘口或 post-only，确保可撤。
3. 小额可控成交测试成交回报延迟：只在人工确认风险后执行，记录成交事件 `update_ts -> local_ts`。
4. 分别在低负载和高负载时段采样，避免只给单一时段结论。
5. 每轮测试完成后立即导出 Parquet，保存：
   - 固定 UTC 窗口。
   - git commit。
   - 进程列表和启动脚本。
   - NTP/PTP offset。
   - 统计输出。

## 验收标准

- 每个环境有样本数、订单数、成交更新数、符号分布、状态分布。
- 每个延迟指标有 `n/negative/min/p50/p90/p95/p99/max/mean`。
- `uniform.trade_update_to_local_ms` 是成交从交易所更新时间到本地的主口径。
- 所有负延迟和 p99/max outlier 都有可回查的 `client_order_id`。
- 对未埋点链路段明确标记为“不可由现有数据证明”，不能写成结论。
