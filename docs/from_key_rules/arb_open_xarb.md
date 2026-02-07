# ArbOpen（XARB）`from_key` 规则

格式：

```text
time:pnlu_factor:rl_factor:spread_rate
```

字段说明：
- `time`：`get_timestamp_us()`，微秒时间戳。
- `pnlu_factor`：Redis 的 PNLU 因子，缺失时回退为 `0.000000`。
- `rl_factor`：`rl_return_volatility` 因子，缺失时回退为 `0.000000`。
- `spread_rate`：开仓腿与对冲腿的中间价价差率，计算公式为 `(mid_open - mid_hedge) / mid_open`。

说明：
- 为统一追踪，`ArbOpen` 的 `from_key` 现在同时携带 `pnlu_factor` 与 `rl_factor`。
- 该调整不改变 Open 决策逻辑，只增加记录信息。

示例：

```text
1738912345678901:1.250000:0.001500:0.001234
```

代码来源：
- 生成格式：`src/funding_rate/xarb_decision.rs` 的 `emit_open_signals`
- 发出 ArbOpen：`src/funding_rate/xarb_decision.rs` 的 `emit_open_signals`
