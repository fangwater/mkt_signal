# Trade Update 幂等规则

本文档描述当前 `trade update` 的公共幂等判定规则，以及策略侧使用方式。

## 目标

- 避免同一 `client_order_id` 的重复成交更新被重复写入状态与持久化。
- 保持三类策略行为一致：`HedgeArbStrategy`、`MarketMakerOpenStrategy`、`MarketMakerHedgeStrategy`。

## 公共判定入口

公共函数：`OrderManager::should_skip_idempotent_trade_update(...)`

位置：`src/pre_trade/order_manager.rs`

返回值：
- `None`：允许处理该成交更新。
- `Some(TradeUpdateSkipReason::DuplicateFilled)`：跳过重复 `Filled`。
- `Some(TradeUpdateSkipReason::StaleOrDuplicatePartial)`：跳过过期/重复 `PartiallyFilled`。

## 规则细节

### 1) Filled -> Filled 重复

当满足以下条件时直接跳过：

- incoming `status == Filled`
- 本地订单 `order.status == Filled`

含义：同一个订单已经终态成交，再收到 `Filled` 回报，按重复更新处理。

### 2) PartiallyFilled 过期/重复

仅在 incoming `status == PartiallyFilled` 时检查：

- `incoming_cum_qty < local_cum_qty - 1e-12`，视为回退（过期）
- 或 `|incoming_cum_qty - local_cum_qty| <= 1e-12` 且 `incoming_update_ts <= local_update_ts`，视为重复/倒挂

其中：
- `local_cum_qty = order.cumulative_filled_quantity`
- `local_update_ts = order.timestamp.filled_t`

## 策略接入点

以下函数均已统一调用公共幂等函数：

- `src/strategy/hedge_arb_strategy.rs` 的 `apply_trade_update`
- `src/strategy/mm_open_strategy.rs` 的 `apply_trade_update`
- `src/strategy/mm_hedge_strategy.rs` 的 `apply_trade_update`

调用方式统一为：先判定是否 `skip`，再执行本地订单状态更新与后续策略逻辑。

## 与 order update 幂等关系

- `order update` 使用 `OrderManager::should_skip_idempotent_order_update(...)`
- `trade update` 使用 `OrderManager::should_skip_idempotent_trade_update(...)`

两者均收敛到 `OrderManager`，保持“公共判定 + 策略消费”的一致风格。
