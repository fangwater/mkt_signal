# from_key 规则总览

本文档给出当前代码实现对应的 `from_key` 总览。详细拆分规则见 `docs/from_key_rules/`。

## 重要说明

- `from_key` 与 `signal` **不是一一对应**。
- dump 强平复用 `ArbClose`，不存在单独的 `DumpClose` 信号类型。
- `time` 统一使用 `get_timestamp_us()`（微秒）。
- 浮点字段统一 `:.6` 精度。
- `from_key` 为 UTF-8 字符串，落地为 `Vec<u8>`。

## 协议更新（ArbHedge）

- `ArbHedgeSignalQueryMsg` 的数量字段为 `hedge_base_qty`（base 口径），用于向上游决策侧请求对冲报价。
- `ArbHedge` 信号上下文 `ArbHedgeCtx` 的量价使用 `qv`（tick/count）编码：
  - `hedge_qty_qv`
  - `hedge_price_qv`
- `fr/xarb decision` 在返回 `ArbHedge` 信号前已完成量价对齐；下游策略直接消费对齐后的 `qv` 值。

## 当前格式（按策略/信号）

### FR

- `ArbOpen` / `ArbClose`
  - `time:funding_ma:predicted_funding_rate:loan_rate:spread_rate`
- `ArbHedge`
  - `time:request_seq:spread_rate`

### XARB

- `ArbOpen`
  - `time:ret_score=...:ret_thr=...:vol=...:env_score=...:env_thr=...:spread=...:open_scale=...`
- `ArbHedge`
  - `time:ret_score=...:ret_thr=...:vol=...:env_score=...:env_thr=...:spread=...:pct_change=...`
- `ArbClose`（dump 路径）
  - `time:dump:spread_rate`

### MM

- `MMOpen` / `MMHedge` 的 `from_key` 目前由上游透传，仓库内未定义固定字段拼装格式。

## 详细规则文档

- 索引：`docs/from_key_rules/README.md`
- FR：
  - `docs/from_key_rules/arb_open_fr.md`
  - `docs/from_key_rules/arb_hedge_fr.md`
  - `docs/from_key_rules/arb_close_fr.md`
- XARB：
  - `docs/from_key_rules/arb_open_xarb.md`
  - `docs/from_key_rules/arb_hedge_xarb.md`
  - `docs/from_key_rules/arb_close_xarb.md`
- MM：
  - `docs/from_key_rules/mm_open.md`
  - `docs/from_key_rules/mm_hedge.md`
