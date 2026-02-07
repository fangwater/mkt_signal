# ArbClose（FR）`from_key` 规则

格式：

```text
time:funding_ma:predicted_funding_rate:loan_rate:spread_rate
```

字段说明：
- `time`：`get_timestamp_us()`，微秒时间戳。
- `funding_ma`：资金费率均值。
- `predicted_funding_rate`：预测资金费率。
- `loan_rate`：预测借贷费率。
- `spread_rate`：开仓腿与对冲腿的中间价价差率。

说明：
- FR 的 `ArbClose` 与 `ArbOpen` 复用同一个 `from_key` 构造函数。
- pre-trade 会将 `ArbClose` 转为反向开仓执行，但 `from_key` 原样保留。

示例：

```text
1738912345678901:0.000123:0.000456:0.000078:0.001234
```

代码来源：
- 复用生成函数：`src/funding_rate/fr_decision.rs:1142`
- 发出 ArbClose：`src/funding_rate/fr_decision.rs:1056`
- pre-trade 转换：`src/pre_trade/signal_channel.rs:410`
