# from_key 规则说明（Arb / Hedge）

本文档描述 `from_key` 在 FR / XARB 的构造规则、格式和落地位置。

## 通用规则

- `time` 统一使用 `get_timestamp_us()`，单位微秒（us）。
- 所有浮点数固定保留 6 位小数（`:.6`）。
- `from_key` 是 UTF-8 字符串，最终存为 `Vec<u8>`。

## ArbOpen / ArbClose（FR）

**格式**  
```
time:funding_ma:predicted_funding_rate:loan_rate
```

**来源**  
- `funding_ma`：`MktChannel::get_funding_rate_mean`
- `predicted_funding_rate`：`RateFetcher::get_predicted_funding_rate`
- `loan_rate`：`RateFetcher::get_predict_loan_rate`

**说明**  
FR 的 `ArbClose` 与 `ArbOpen` 使用同一格式（包含 funding 相关字段）。

## ArbOpen / ArbClose（XARB）

**格式**
```
time:pnlu_factor
```

**说明**
- `pnlu_factor` 来源于 Redis 的 pnlu 因子（缺失时为 `0.000000`）。
- XARB 在 dump list 触发 close 时，`from_key` 直接为：
```
time:dump
```

## ArbHedge（FR）

**格式**
```
time:request_seq:aggressive
```

**说明**
- `request_seq` 来自 `ArbHedgeSignalQueryMsg`。
- `aggressive` 为 `0/1` 标记（`request_seq >= hedge_aggressive_seq_threshold` 时为 1）。

## ArbHedge（XARB）

**格式**
```
time:rl_factor:aggressive:pct_change:threshold_pct
```

**说明**
- `rl_factor` 来源于 Redis `rl_return_volatility`（缺失时为 `0.000000`）。
- `aggressive` 为 `0/1` 标记。
- `pct_change` / `threshold_pct` 来源于 stop-loss 逻辑的价差变动计算。
- stop-loss 的 hedge 信号同样使用该格式（`rl_factor` 可能为 0）。

## Strategy 保存

`HedgeArbStrategy` 会保存两份来源标记：
- `open_from_key`：来自 `ArbOpen` 信号
- `hedge_from_key`：来自 `ArbHedge` 信号（每次 hedge 信号会覆盖更新）

若 MT 直接对冲且当前 `hedge_from_key` 为空，会回退使用 `open_from_key`。
