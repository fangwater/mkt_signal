# Cancel Pending 保护逻辑

## 背景
1. 策略收到开仓信号后先向 margin 提交限价单，订单状态进入 `COMMIT/NEW`。
2. 如果价差迅速反转，会立即触发撤单信号；此时只向交易所发出了取消请求，还没有收到 `CANCELED` 回报。
3. 原先 `is_active_open()` 会因为 `order_manager` 中查不到活跃的 margin 订单而直接返回 `false`，导致策略被 `pre_trade` 清理，撤单回报随后到来时已经没有对应的策略可以处理。

## 优化点
- 为开仓流程新增 `cancel_pending` 标记以及触发时间，撤单请求成功送出后进入等待态。
- `is_active_open()` 在 `cancel_pending` 且本地没有活跃订单时，会在 5s 超时窗口内继续返回 `true`，避免策略被提前移除。
- 收到撤单终态（`CANCELED/EXPIRED/REJECTED`）或超时后，会清除等待标记并恢复原有的生命周期判断。
- 快照结构增加 `cancel_pending` 与 `cancel_pending_since_us` 字段，确保热重启/回放时状态一致。

## 变更前后对比
- **变更前**：撤单信号刚发出，`force_cancel_open_margin_order` 更新订单为 `cancel_requested`；若随后 `order_manager` 中暂时查不到该订单，`is_active_open()` 立即返回 `false`，策略被移除，回报丢失。
- **变更后**：同样场景下，策略进入 `cancel_pending` 等待，`is_active_open()` 在等待窗口内继续保持活跃；收到撤单回报后正常清理，若超过 5s 仍未收到回报则打印告警并强制结束，避免无限等待。

