# ArbHedge（XARB）`from_key` 规则

格式：

```text
time:pnlu_factor:rl_factor:pct_change:threshold_pct:spread_rate
```

字段说明：
- `time`：`get_timestamp_us()`，微秒时间戳。
- `pnlu_factor`：Redis 的 PNLU 因子，缺失时回退为 `0.000000`。
- `rl_factor`：`rl_return_volatility` 因子，缺失时回退为 `0.000000`。
- `pct_change`：止损判断中的价差变动百分比。
- `threshold_pct`：止损阈值百分比。
- `spread_rate`：对冲信号生成时的中间价价差率。

信号与查询字段口径：
- hedge query 使用 `ArbHedgeSignalQueryMsg.hedge_base_qty`（base 口径数量）。
- `ArbHedgeCtx` 量价均为 `qv` 编码：
  - 数量：`hedge_qty_qv`
  - 价格：`hedge_price_qv`

说明：
- `aggressive` 逻辑仍用于报价偏移（offset）决策，但不写入 `from_key`。
- 为统一追踪，`ArbHedge` 的 `from_key` 现在同时携带 `pnlu_factor` 与 `rl_factor`。
- 该调整不改变 Hedge 决策逻辑，只增加记录信息。

示例：

```text
1738912345678901:1.250000:0.001500:0.006200:0.005000:0.001234
```

代码来源：
- 生成格式：`src/funding_rate/xarb_decision.rs` 的 `build_hedge_from_key`
- 设置到上下文：`src/funding_rate/xarb_decision.rs` 的 `handle_backward_query`
