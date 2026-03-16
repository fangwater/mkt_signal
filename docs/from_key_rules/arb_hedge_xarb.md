# ArbHedge（XARB）`from_key` 规则

格式：

```text
time:ret_score=...:ret_thr=...:vol=...:env_score=...:env_thr=...:spread=...:pct_change=...
```

字段说明：
- `time`：`get_timestamp_us()`，微秒时间戳。
- `ret_score`：return score，缺失时回退为 `0`。
- `ret_thr`：return threshold，缺失时回退为 `0`。
- `vol`：`rl_return_volatility` 因子值，缺失时回退为 `0`。
- `env_score`：环境分数；环境模型关闭时回退为 `pnlu` 口径。
- `env_thr`：环境阈值；缺失时回退为 `0`。
- `spread`：对冲信号生成时的中间价价差率。
- `pct_change`：止损判断中的价差变动百分比。

信号与查询字段口径：
- hedge query 使用 `ArbHedgeSignalQueryMsg.hedge_base_qty`（base 口径数量）。
- `ArbHedgeCtx` 量价均为 `qv` 编码：
  - 数量：`hedge_qty_qv`
  - 价格：`hedge_price_qv`

说明：
- `aggressive` 逻辑仍用于报价偏移（offset）决策，但不写入 `from_key`。
- `env_score/env_thr` 记录实际环境信号值；当环境模型关闭时，就是 `pnlu` 的值。

示例：

```text
1738912345678901:ret_score=0.12345678:ret_thr=0.04500000:vol=0.00200000:env_score=0.00272464:env_thr=0.00167874:spread=0.001234:pct_change=0.006200
```

代码来源：
- 生成格式：`src/funding_rate/xarb_decision.rs` 的 `build_hedge_from_key`
- 设置到上下文：`src/funding_rate/xarb_decision.rs` 的 `handle_backward_query`
