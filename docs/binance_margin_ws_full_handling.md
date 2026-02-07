# Binance Margin WS `FULL/RESULT` 回报处理约定

## 背景

在 Binance margin 的 WS 下单接口中，`newOrderRespType=FULL/RESULT` 时，
交易所回报可能直接给出 `status=FILLED` / `PARTIALLY_FILLED`（并带 `fills`），
不保证先经过 `NEW` 阶段。

这会与策略侧基于本地状态机的 `NEW -> TRADE/TERMINAL` 假设产生冲突，
并可能和 account ws 的正常订单事件重复。

## 约定（当前实现）

- 仅针对 `BinanceMargin + WS` 回报：
  - 只处理 `status=NEW`，并转成 `order update`。
  - `status != NEW`（如 `PARTIALLY_FILLED` / `FILLED` / `CANCELED`）一律跳过。
- 非 `BinanceMargin` 的 WS 回报保持原逻辑。

## 目的

- 避免在下单回包里处理“立即成交”引发的重复推进；
- 统一由 account ws 的正常推送来处理成交与终态。

## 代码位置

- `src/strategy/mm_open_strategy.rs`
- `src/strategy/mm_hedge_strategy.rs`

## 补充：Binance UM WS 下单回报类型

- 仅在 Binance UM 的 WS 下单请求中附加 `newOrderRespType`。
- 固定值：`RESULT`（大写）。
