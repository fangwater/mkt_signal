# 对冲流程说明

本文档记录 `BinSingleForwardArb` 策略在 pre-trade 模块中的对冲逻辑，便于排查和回归。

## 1. 开仓与增量对冲

### 1.0 流程概览（最新机制）

- **分离生命周期**：`BinSingleForwardArb` 策略在 open/close 两种模式下独立运行；open 仅负责 margin 开仓与 UM 对冲，完成后立即退出；close 则从账户敞口读取现有头寸，完成二次平仓后回收。
- **开仓阶段**：
  - Margin 成交累计量与已对冲量比对得到增量，满足步进、最小数量与名义金额后即刻触发对冲。
  - **MT**：直接下 UM 市价单（沿用历史逻辑）。
  - **MM**：向资金费率进程申请限价参数后，提交 GTX 限价单，确保 maker 身份。
  - 如对冲信号总线不可用，策略会在本线程直接创建市价单，确保 hedge 不丢失。
- **平仓阶段**：平仓信号会直接构造一个“反向建仓”策略（close 模式）。该策略读取 `ExposureManager` 的 spot/UM 净仓，按当前可用持仓推导目标数量，并沿用与开仓阶段相同的步进、最小下单量、名义金额等校验。满足条件后先下 margin 限价平仓单，成交后再参考原先记录的仓位方向触发 UM 对冲（MT 走市价，MM 走 GTX 限价），将数量对齐步进并限制在账户剩余持仓范围内完成收尾。
- **信号派发**：close 模式策略会在成交回报里调用 `trigger_um_close_signal`，把待平数量写入 `BinSingleForwardArbCloseUmCtx`，确保后续的 UM 市价单具备充足信息实现“反向建仓式”平仓。


1. `Binance` margin 开仓单（限价或市价）被提交，`OrderManager` 保存原始下单信息。
2. 当收到 `executionReport`：
   - 订单状态更新为 `PARTIALLY_FILLED`/`FILLED`；
   - `cumulative_filled_quantity` 同步到内存。
3. 每次成交回报都会计算增量：
   ```
   delta = filled_qty - hedged_filled_quantity
   ```
   如果 `delta <= 0`，对冲逻辑直接返回。
4. 有效增量会进入 `emit_hedge_signal`：
   - 优先尝试走自有信号通道（`signal_tx`）发送 `BinSingleForwardArbHedge`；
   - 若队列发送失败或未配置，则直接在当前线程调用 `create_hedge_um_order_from_margin_order`。

## 2. 创建 UM 对冲单

`create_hedge_um_order_from_margin_order` 负责真正下发 UM 市价单，并追加大量 Debug 日志：

1. **基础校验**：确认策略 ID、数量合法。
2. **步进 / 数量约束**：
   - 使用 `MinQtyTable` 的 `futures_um_step_by_symbol` 将增量数量向下对齐；
   - `minQty` 校验未达阈值则暂缓对冲并输出日志。
3. **名义金额约束**：
   - 调用 `PriceTable` 获取 `markPrice`，计算 `notional = price * qty`;
   - 若未获取到价格或名义金额低于 `minNotional`，同样会记录 Debug 日志并等待下一次成交。
4. 所有约束满足后：
   - 发送 REST 请求构造市价单参数；
   - 更新 `OrderManager` 中的对冲订单条目；
   - 同步 `hedged_filled_quantity`，用于后续增量计算；
   - 记录创建成功的日志（含累计已对冲总量）。

## 3. 聚合与平仓

策略内部将所有对冲、平仓订单 ID 存储为 `Vec<i64>`，并提供辅助函数：

- `aggregate_um_hedge_position`：遍历 UM 对冲单，汇总可用数量，用于平仓或状态判断；
- `first_unfilled_order`：生命周期检测时用于定位未完成的订单。

在需要平仓时：

1. `trigger_um_close_signal` 会调用 `aggregate_um_hedge_position`，并输出聚合结果；
2. `close_um_with_market` 再次使用聚合结果确认未平仓头寸、检查数量/名义约束，最终下发平仓市价单；
3. 所有相关操作都追加了 Debug 日志，方便追踪 “聚合 → 下单 → 更新” 的全过程。

## 4. 快照与恢复

为支持持久化，策略快照结构改为保存 `Vec<i64>`：

- 为兼容历史数据，保留了 `legacy_*` 字段，反序列化时若新字段为空会自动回填。


## 5. 调试建议

1. 开启 `debug` 日志级别，关注关键前缀：
   - `创建 UM 对冲订单成功`：确认真实下单参数；
   - `对冲数量 ... 小于最小下单量` / `对冲名义金额 ... 低于阈值`：说明增量被延后；
   - `聚合 UM 对冲头寸`：验证平仓前的持仓视图；
   - `生成快照` / `从快照恢复`：追踪持久化链路。
2. 若需要进一步排查，可在 `OrderManager` 中打印 `hedged_filled_quantity` 与 `cumulative_filled_quantity` 比例，确认未出现重复对冲。

通过上述日志与机制，可在 pre-trade 模块完整追踪 margin 成交、UM 对冲、市价平仓及策略持久化全流程。*** End Patch
