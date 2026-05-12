# from_key 规则拆分索引

`from_key` 规则按“策略类型 + 信号类型”拆分。

## 重要说明

- `from_key` 与 `signal` **不是一一对应**。
- 同一 `signal` 在不同策略下可使用不同格式。
- dump 强平会复用 `ArbClose`，不会新增 `DumpClose` 信号类型。

## Arb

- [ArbOpen（FR）](arb_open_fr.md)
- [ArbClose（FR）](arb_close_fr.md)
- [ArbHedge（FR）](arb_hedge_fr.md)

## MM

- [MMOpen](mm_open.md)
- [MMHedge](mm_hedge.md)

## 通用约定

- 时间戳统一使用微秒（us）。
- 浮点字段默认保留 6 位小数（仅适用于定义了数值格式的规则）。
- `from_key` 最终为 UTF-8 字符串并存储为 `Vec<u8>`。

## 相关文档

- [Trade Update 幂等规则](../trade_update_idempotency.md)
- [统一订单映射说明](../uniform_order_mapping.md)
- [Binance Margin WS FULL 回报处理约定](../binance_margin_ws_full_handling.md)
