# Bitget 近似逻辑记录（UTA Account Monitor）

> 目的：记录当前 Bitget UTA 账户监控中为了兼容不同推送结构而采用的“近似/兜底”逻辑，便于后续逐项替换成精确语义。

## 1) 订单价格近似（重点）

### A1. `orderStatus` 推断成交事件（Trade）
- 逻辑：`partially_filled` / `filled` 统一映射为 `ExecutionType::Trade (u8=5)`。
- 代码位置：
  - `src/common/bitget_account_msg.rs:123`
  - `src/parser/bitget_account_event_parser.rs:228`
- 影响：只能判断“是否成交相关事件”，不能区分本次增量成交与纯状态刷新。

### A2. 成交事件价格取 `avgPrice`（无则回退 `price`）
- 逻辑：
  - 若 `execution_type == 5`：`price = avgPrice.unwrap_or(order_price)`
  - 否则：`price = order_price`
- 代码位置：`src/parser/bitget_account_event_parser.rs:245`, `src/parser/bitget_account_event_parser.rs:251`
- 影响：`avgPrice` 是累计均价，不是“本次 fill 价格”；当订单多次成交时会产生价格误差。

### A3. `last_executed_price` 近似
- 逻辑：
  - 若 `execution_type == 5`：`last_executed_price = avgPrice.unwrap_or(order_price)`
  - 否则：`last_executed_price = 0.0`
- 代码位置：`src/parser/bitget_account_event_parser.rs:265`
- 影响：`last_executed_price` 实际承载的是“累计均价近似值”，不是严格的 last fill。

### A4. `TradeUpdate::price()` 取值规则
- 逻辑：
  - `execution_type == 5` 时优先 `last_executed_price`，否则回退 `price`
  - 非 Trade 时返回 `price`
- 代码位置：`src/strategy/bitget_basic_impl.rs:103`
- 影响：Trade 路径仍受 A2/A3 的累计均价误差影响。

## 2) 订单字段兼容兜底（非价格）

### B1. 订单字段多键兼容
- 逻辑：
  - `orderId | ordId`
  - `clientOid | clOrdId`
  - `symbol | instId`
- 代码位置：`src/parser/bitget_account_event_parser.rs:183`, `src/parser/bitget_account_event_parser.rs:191`, `src/parser/bitget_account_event_parser.rs:194`
- 影响：兼容性更好，但字段漂移时可能掩盖上游 schema 变化。

### B2. Maker/Taker 推断
- 逻辑：`tradeScope | liquidity`，仅识别 `maker`/`m` 为 maker。
- 代码位置：`src/parser/bitget_account_event_parser.rs:232`
- 影响：若交易所扩展新枚举值，可能误判为 taker/unknown。

### B3. 交易场所（Margin/Futures）推断
- 逻辑：按 `category/instType` 文本包含关系推断 futures（`future/swap/perp`），否则 spot/margin。
- 代码位置：`src/parser/bitget_account_event_parser.rs:370`
- 影响：字符串规则属于启发式；新增类别命名时可能分类错误。

## 3) 账户与持仓字段近似

### C1. 余额口径近似
- 逻辑：余额按 `available -> equity -> balance` 优先级选取。
- 代码位置：`src/parser/bitget_account_event_parser.rs:72`
- 影响：不同业务场景（可用、权益、总额）语义不同，当前统一映射为 `BasicBalanceMsg.balance`。

### C2. 借贷/利息口径近似
- 逻辑：
  - borrowed：`borrowAmount -> liability`
  - interest：`interest -> accruedInterest`
- 代码位置：`src/parser/bitget_account_event_parser.rs:83`, `src/parser/bitget_account_event_parser.rs:86`
- 影响：字段语义跨产品线可能不完全一致，仅做统一账户侧近似。

### C3. 持仓数量与方向近似
- 逻辑：
  - 数量：`size -> total -> pos`
  - 方向：`posSide/holdSide` 映射到 `L/S/N`
- 代码位置：`src/parser/bitget_account_event_parser.rs:123`, `src/parser/bitget_account_event_parser.rs:133`
- 影响：净持仓与双向持仓模式下语义可能不完全等价。

### C4. 未实现盈亏字段兼容
- 逻辑：`unrealisedPnl -> unrealizedPnl -> upl`
- 代码位置：`src/parser/bitget_account_event_parser.rs:156`
- 影响：字段来源可能跨频道/版本，当前以“可解析即采用”为策略。

## 4) 后续优化建议（按优先级）

1. **订单成交价精确化**：若 Bitget 后续提供单次成交价/成交增量字段，优先替换 A2/A3。
2. **订单事件精确分类**：结合成交量增量（如 `cumExecQty` 差分）辅助判断是否真实 Trade。
3. **字段白名单版本化**：按 UTA 文档版本固定字段，减少启发式字符串推断。
4. **回放样本校验**：用真实私有流样本对 price、maker、venue、position side 做回归测试。

---

维护说明：
- 本文档对应当前 `pairmm` 分支 Bitget UTA 接入实现。
- 每次替换/删除近似逻辑，请同步更新本文件。
