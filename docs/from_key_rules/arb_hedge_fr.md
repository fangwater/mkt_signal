# ArbHedge（FR）`from_key` 规则

格式：

```text
time:request_seq:spread_rate
```

字段说明：
- `time`：`get_timestamp_us()`，微秒时间戳。
- `request_seq`：`ArbHedgeSignalQueryMsg.request_seq`。
- `spread_rate`：开仓腿与对冲腿的中间价价差率。

信号与查询字段口径：
- hedge query 使用 `ArbHedgeSignalQueryMsg.hedge_base_qty`（base 口径数量）。
- `ArbHedgeCtx` 量价均为 `qv` 编码：
  - 数量：`hedge_qty_qv`
  - 价格：`hedge_price_qv`

说明：
- `aggressive` 逻辑仍用于报价偏移（offset）决策，但不再写入 `from_key`。

示例：

```text
1738912345678901:42:0.001234
```

代码来源：
- 生成格式：`src/funding_rate/fr_decision.rs` 的 `build_hedge_from_key`
- 设置到上下文：`src/funding_rate/fr_decision.rs:963`
