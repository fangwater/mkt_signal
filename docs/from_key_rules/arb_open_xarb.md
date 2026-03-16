# ArbOpen（XARB）`from_key` 规则

格式：

```text
time:ret_score=...:ret_thr=...:vol=...:env_score=...:env_thr=...:spread=...:open_scale=...
```

字段说明：
- `time`：`get_timestamp_us()`，微秒时间戳。
- `ret_score`：return score，缺失时回退为 `0`。
- `ret_thr`：return threshold，缺失时回退为 `0`。
- `vol`：原始 `rl_return_volatility` 因子值，未乘 `open_scale`。
- `env_score`：环境分数；环境模型关闭时回退为 `pnlu` 口径。
- `env_thr`：环境阈值；缺失时回退为 `0`。
- `spread`：开仓腿与对冲腿的中间价价差率，计算公式为 `(mid_open - mid_hedge) / mid_open`。
- `open_scale`：开仓 plan 的波动边界缩放系数。

说明：
- `ArbOpen` 的实际报价 plan 使用 `vol * open_scale` 作为单边展开边界。
- `env_score/env_thr` 记录实际环境信号值；当环境模型关闭时，就是 `pnlu` 的值。

示例：

```text
1738912345678901:ret_score=0.12345678:ret_thr=0.04500000:vol=0.00200000:env_score=0.00272464:env_thr=0.00167874:spread=0.001234:open_scale=1.200000
```

代码来源：
- 生成格式：`src/funding_rate/xarb_decision.rs` 的 `emit_open_signals`
- 发出 ArbOpen：`src/funding_rate/xarb_decision.rs` 的 `emit_open_signals`
