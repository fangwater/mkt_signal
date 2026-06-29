# Gate 动态降杠杆、限仓与 UniMMR 关系说明

这份文档说明 Gate futures 在当前 FR 流程里为什么会因为杠杆 limit /
限仓导致高频报单，为什么超限后可能卡住减仓，以及为什么动态降低
Gate cross leverage limit 是一种“修复交易所限仓”的手段，而不是一种
“修复 UniMMR / 增加保证金”的手段。

## 1. 问题本质

Gate futures 的 `cross_leverage_limit` 会影响交易所侧允许的合约仓位上限。
在当前系统里，核心字段是：

```text
cross_leverage_limit
risk_limit
position.value
```

我们实际关心的安全上限不是裸 `risk_limit`，而是带 buffer 的上限：

```text
exchange_cap = risk_limit - buffer
```

当前 Gate FR runbook 常用：

```text
buffer = 2000 USDT
```

当某个合约满足：

```text
abs(position.value) > risk_limit - buffer
```

说明这个合约当前仓位已经接近或超过 Gate 当前允许的交易所侧仓位上限。
这时系统即使产生的是合理的策略意图，Gate 也可能因为合约保证金 /
风险限额不足而拒绝下单。

这里的“保证金不足”不要理解成账户没有 USDT，而应该理解成：

```text
在当前 cross_leverage_limit 和 risk_limit 档位下，
Gate 不允许这个合约继续以当前状态接收相关订单。
```

## 2. 为什么会导致高频报单

高频报单的链路通常是：

1. 策略、hedge 或 close 逻辑看到某个合约还有敞口需要处理。
2. 系统发出 futures order。
3. Gate 因为当前合约限仓 / 风险限额 / 保证金约束拒绝订单。
4. 仓位没有变化。
5. 下一轮信号或 retry 又看到同一个敞口仍然存在。
6. 系统再次发单。

结果就是：

```text
敞口未减少
    -> 逻辑继续认为需要处理
    -> 再发单
    -> Gate 再拒绝
    -> 继续重复
```

所以这类高频报单的根因不一定是策略想过度交易，而可能是：

```text
交易所侧当前合约 risk_limit 太低，订单无法真正改变仓位状态。
```

如果只从 retry 侧处理，而不修复 Gate 合约限仓，系统会一直看到同一个
未处理敞口。

## 3. 这个仓位限制是动态的

不能把 Gate 的 `risk_limit` 或 Redis `max_u` 当成永久静态值。

Gate 的有效仓位限制会受以下因素影响：

- 当前 `cross_leverage_limit`;
- Gate 当前给这个合约匹配的 risk limit tier;
- Gate 每个合约自己的 `risk_limit_tiers` 表;
- 当前 `position.value`，它会跟着 mark price 变化;
- 手动或自动调整过的 leverage limit;
- Gate 自身的风险状态更新。

因此，一个之前安全的 Redis `max_u`，之后可能变得过松；一个之前能覆盖
当前仓位的 Gate `risk_limit`，之后也可能因为价格变化或风险档位变化而
不再覆盖。

操作上应该按实时状态判断：

```text
不要只相信旧的 max_u；
需要重新读取 Gate position.value 和 risk_limit；
再判断 abs(position.value) 是否超过 risk_limit - buffer。
```

## 4. 超出限制会导致减仓失败

最危险的情况不是“无法继续开大仓”，而是：

```text
系统想处理敞口，但订单被 Gate 限制挡住，导致敞口处理不了。
```

表现可能是：

- hedge order 被连续拒绝;
- close / reduce 流程一直看到剩余敞口;
- retry 逻辑持续提交，但仓位没有变化;
- 策略状态认为风险还没处理完;
- 该 symbol 被卡在无法收敛的状态。

这会导致减仓或敞口处理卡住。

所以一旦当前仓位已经超过：

```text
risk_limit - buffer
```

就不应该只把它当成普通下单失败处理。它应该被识别成：

```text
Gate 合约限仓需要修复。
```

## 5. 推荐修复方式：动态降低 Gate cross leverage limit

在 Gate cross futures 里，降低 `cross_leverage_limit` 可能会提升 Gate
返回的 `risk_limit`，从而扩大交易所允许的合约仓位上限。

之前 jp2 / `gate_fr_arb01` 的实际例子：

```text
AIN_USDT      cross 3 -> 2   risk_limit 20000 -> 50000
HNT_USDT      cross 5 -> 4   risk_limit 20000 -> 30000
ZEREBRO_USDT  cross 4 -> 3   risk_limit 50000 -> 100000
```

推荐动作是：

```text
if abs(position.value) > risk_limit - buffer:
    lower cross_leverage_limit by one step
    re-read Gate position
    stop once abs(position.value) <= new_risk_limit - buffer
```

不要一次性盲目降到最低杠杆。应该一档一档降，每次降完都重新读取：

```text
cross_leverage_limit
risk_limit
position.value
```

确认新的 `risk_limit - buffer` 已经覆盖当前仓位后，再停止。

修复 Gate cap 后，再同步 Redis per-symbol cap：

```text
max_u = min(new_risk_limit - buffer, base_max_u)
```

当前 Gate FR 场景常用：

```text
base_max_u = 98000 USDT
```

这个 Redis `max_u` 的作用是限制后续继续扩仓。它不会自动减少已经存在的
仓位。

## 6. 降低杠杆和保证金 / UniMMR 的关系

这里必须区分两个概念：

```text
动态降低 Gate cross_leverage_limit
    = 修复交易所侧合约仓位上限 / risk_limit

提高 UniMMR 或改善账户保证金状态
    = 账户级风险控制问题
```

降低 Gate `cross_leverage_limit` 不应该被解释成：

```text
增加了账户保证金
释放了保证金
降低了当前 maintenance_margin
直接提高了 UniMMR
```

它本质上解决的是：

```text
Gate 当前给这个合约的 risk_limit 太小，导致订单无法处理仓位。
```

不是解决：

```text
账户整体保证金不够。
```

## 7. 当前 Gate maintenance_margin 的计算口径

根据我们对 jp2 / `gate_fr_arb01` 的对账，Gate futures 的
maintenance margin 更接近逐合约计算后求和：

```text
maintenance_margin_i =
    max(abs(position_value_i) * tier.maintenance_rate - tier.deduction, 0)

account_maintenance_margin =
    sum(maintenance_margin_i)
```

注意：

```text
tier 是按每个合约自己的 position_value 匹配；
不是把所有合约 position_value 加总后匹配一个总 tier。
```

每个合约的 `risk_limit_tiers` 表不同，所以必须逐合约算。

这套 maintenance margin 口径和 `cross_leverage_limit` 不是一回事。
调整 `cross_leverage_limit` 主要影响 Gate 允许的 `risk_limit` / 限仓，
不应假设它会直接改善当前 UniMMR。

## 8. UniMMR 仍然应该由 UniMMR 和总杠杆率控制

当前系统里，UniMMR close gate 消费的是：

```text
BasicAccountRiskMsg.margin_ratio
```

逻辑上是：

```text
margin_ratio < trigger  => 允许 close / 风险收敛
margin_ratio > recover  => 恢复 normal
中间区间               => 保持上一状态
```

重要边界：

```text
margin_ratio = 1.0
```

高于 1 越多越安全。

所以系统应该分层控制：

```text
1. UniMMR close gate
   控制账户保证金健康度触发的 close / 风险收敛。

2. 总杠杆率 / 账户级 exposure 控制
   控制整个账户的总体风险。

3. per-symbol max_u
   控制每个 symbol 后续可扩仓上限。

4. Gate dynamic leverage repair
   当某个合约当前 position.value 已经接近或超过 Gate risk_limit 时，
   动态降低 cross_leverage_limit 来修复交易所侧限仓。
```

动态降杠杆只解决第 4 层。它不能替代第 1 层和第 2 层。

换句话说，即使自动降低了 Gate `cross_leverage_limit`，系统仍然必须继续受
以下约束：

```text
UniMMR threshold
total account leverage limit
per-symbol max_u
strategy-side position limit
```

## 9. 推荐触发条件

对 Gate online futures symbols，周期性检查或在下单前检查：

```text
value = abs(position.value)
cap = risk_limit - buffer
```

如果：

```text
value > cap
```

则该 symbol 进入 Gate exchange-cap danger 状态。

推荐处理：

1. 不要把连续拒单只当成普通 retry。
2. 标记该 symbol 需要 Gate cap repair。
3. 降低 `cross_leverage_limit` 一档。
4. 重新读取 `risk_limit`、`cross_leverage_limit`、`position.value`。
5. 重复直到：

```text
abs(position.value) <= risk_limit - buffer
```

6. 写入修复后的 Redis cap：

```text
max_u = min(risk_limit - buffer, base_max_u)
```

7. UniMMR 和总杠杆率控制保持不变。

## 10. 安全要求

动态降低 Gate leverage limit 是真实交易所状态变更，必须按 live-risk
操作处理：

- 先做 read-only audit;
- 打印 symbol、current value、risk_limit、cap、over_by;
- 只修改 `abs(position.value) > risk_limit - buffer` 的 symbol;
- 每次只降低一档;
- 每次修改后重新读取 Gate position;
- Gate cap 修复后再更新 Redis `max_u`;
- 不要假设该操作改善了 UniMMR;
- 不要把该操作当成扩大账户总风险的许可。

最终希望达到：

```text
abs(position.value) <= Gate risk_limit - buffer
abs(position.value) <= Redis max_u, unless the existing position was already above the new policy cap
UniMMR still controlled by account-risk close gate
total leverage still controlled by account-level risk logic
```
