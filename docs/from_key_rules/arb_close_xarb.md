# ArbClose（XARB）`from_key` 规则

格式：

```text
time:dump:spread_rate
```

字段说明：
- `time`：`get_timestamp_us()`，微秒时间戳。
- `dump`：固定字面量，表示强制调仓/平仓来源。
- `spread_rate`：平仓时双腿中间价价差率。

说明：
- dump 强平路径复用 `ArbClose` 信号类型。
- 相比旧格式 `time:dump`，现在追加 `spread_rate`。

示例：

```text
1738912345678901:dump:0.001234
```

代码来源：
- 生成格式：`src/funding_rate/xarb_decision.rs` 的 `emit_close_signals`
- 发出 ArbClose：`src/funding_rate/xarb_decision.rs` 的 `emit_close_signals`
