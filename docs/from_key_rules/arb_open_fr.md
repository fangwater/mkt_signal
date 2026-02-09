# ArbOpen（FR）`from_key` 规则

格式：

```text
time:funding_ma:predicted_funding_rate:loan_rate:spread_rate
```

字段说明：
- `time`：`get_timestamp_us()`，微秒时间戳。
- `funding_ma`：`MktChannel::get_funding_rate_mean` 的结果，缺失时回退 `0.000000`。
- `predicted_funding_rate`：`RateFetcher::get_predicted_funding_rate` 的结果，缺失时回退 `0.000000`。
- `loan_rate`：`RateFetcher::get_predict_loan_rate` 的结果，缺失时回退 `0.000000`。
- `spread_rate`：开仓腿与对冲腿的中间价价差率，计算公式为 `(mid_open - mid_hedge) / mid_open`。

编码约定：
- 浮点字段固定保留 6 位小数（`:.6`）。
- `spread_rate` 既写入 `ArbOpenCtx.spread_rate`，也拼进 `from_key`。

示例：

```text
1738912345678901:0.000123:0.000456:0.000078:0.001234
```

代码来源：
- 生成格式：`src/funding_rate/fr_decision.rs` 的 `build_from_key`
- 发出 ArbOpen：`src/funding_rate/fr_decision.rs` 的 `emit_signals`
- `spread_rate` 字段设置：`src/funding_rate/fr_decision.rs` 的 `build_open_context`
