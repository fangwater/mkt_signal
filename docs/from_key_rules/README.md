# from_key 规则拆分索引

`from_key` 规则按“策略类型 + 信号类型”拆分。

## 重要说明

- `from_key` 与 `signal` **不是一一对应**。
- 同一 `signal` 在不同策略下可使用不同格式。
- dump 强平会复用 `ArbClose`，不会新增 `DumpClose` 信号类型。

## Arb（按 FR/XARB 拆分）

- [ArbOpen（FR）](arb_open_fr.md)
- [ArbOpen（XARB）](arb_open_xarb.md)
- [ArbClose（FR）](arb_close_fr.md)
- [ArbClose（XARB）](arb_close_xarb.md)
- [ArbHedge（FR）](arb_hedge_fr.md)
- [ArbHedge（XARB）](arb_hedge_xarb.md)

## MM

- [MMOpen](mm_open.md)
- [MMHedge](mm_hedge.md)

## 通用约定

- 时间戳统一使用微秒（us）。
- 浮点字段默认保留 6 位小数（仅适用于定义了数值格式的规则）。
- `from_key` 最终为 UTF-8 字符串并存储为 `Vec<u8>`。
