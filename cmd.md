# 支持做市模式的报单信号
## 开仓决策
- [1] 量价因子列表-->不同的symbol选择factor动态组合成特征向量->预测模型-> 0/1 开仓或者不开仓（主决策因子）
- [2] 计算fairpirce，或者直接基于midprice，确定报单的中心价。
- [3] 基于波动率因子rl_return_volatility决定报单范围
- 
现在manual signal 多空同时报单，简化这一点。

## 对冲决策
- [1] 基于波动率因子 `rl_return_volatility` 确定对冲中心范围（参考 xarb）
  - xarb：直接使用 `rl_return_volatility` 作为中心偏移。
  - MM：不直接照搬中心偏移，增加两个修正系数：
    - `hedge_vol_upper_scale`（对冲侧上界修正）
    - `hedge_vol_lower_scale`（对冲侧下界修正）
  - 两个参数都放在 `mm_strategy_params_{venue}`。
  - 记 `rl_return_volatility` 最终值为 `x`，则原始偏移区间为：
    - `upper_offset = x * (1 + hedge_vol_upper_scale)`
    - `lower_offset = x * (1 + hedge_vol_lower_scale)`
    - 对冲报单范围：`[x(1+hedge_vol_upper_scale), x(1+hedge_vol_lower_scale)]`
  - 归一化后用于后续步骤的区间表达：
    - `offset_min = min(lower_offset, upper_offset)`
    - `offset_max = max(lower_offset, upper_offset)`
  - 说明：这里定义的是“偏移区间”，不是最终价格；最终价格仍需结合方向（buy/sell）和盘口（bid0/ask0）再映射。

- [2] 上下边界修正（`price_offset_limit`）
  - 做市对冲是 maker 挂单：
    - 不能穿价；
    - 也不能离盘口过远。
  - 在策略参数中新增对冲偏移限制（建议命名）：
    - `hedge_price_offset_limit_upper`：偏移上界（最大允许偏移）
    - `hedge_price_offset_limit_lower`：偏移下界（最小允许偏移）
  - 对第 [1] 步得到的区间 `[offset_min, offset_max]` 做 cut：
    - `limit_lower = max(0, hedge_price_offset_limit_lower)`
    - `limit_upper = max(limit_lower, hedge_price_offset_limit_upper)`
    - `final_offset_min = clamp(offset_min, limit_lower, limit_upper)`
    - `final_offset_max = clamp(offset_max, limit_lower, limit_upper)`
  - 关键约束：下界最低到 `0`，不能继续“便宜”到负价格（offset 不允许导致价格<0）。
  - 最终得到可用于后续拆档的偏移范围：`[final_offset_min, final_offset_max]`。


- [3] 对冲盘口偏移系数（区间压缩/放大算法）
  - 输入来自 hedge query：
    - `period_buy_qty`
    - `period_sell_qty`
    - `net_qty`（当前敞口，`both`）
  - 先定义对冲方向：
    - `net_qty > 0`：需要 `sell`（价格希望更靠近 `ask0`）
    - `net_qty < 0`：需要 `buy`（价格希望更靠近 `bid0`）
  - 基于 query 计算“迫切度” `urgency ∈ [0,1]`：
    - `flow = period_buy_qty + period_sell_qty`
    - `imbalance = abs(period_buy_qty - period_sell_qty) / (flow + 1e-9)`
    - `inventory_pressure = abs(net_qty) / (abs(net_qty) + flow + 1e-9)`
    - `urgency = clamp(0.7 * inventory_pressure + 0.3 * imbalance, 0, 1)`
  - 用第 [2] 步得到的区间 `[final_offset_min, final_offset_max]` 做再放缩：
    - `mid = (final_offset_min + final_offset_max) / 2`
    - `half = (final_offset_max - final_offset_min) / 2`
    - `scale = clamp(hedge_offset_scale_max - urgency * (hedge_offset_scale_max - hedge_offset_scale_min), hedge_offset_scale_min, hedge_offset_scale_max)`
      - 示例：`hedge_offset_scale_min=0.5, hedge_offset_scale_max=1.8`
      - 解释：越迫切，`scale` 越小（压缩）；越不迫切，`scale` 越大（放大）。
    - `shift = urgency * hedge_offset_shift_ratio * half`
      - 示例：`hedge_offset_shift_ratio=0.8`
      - 解释：越迫切，区间整体越向盘口内侧（更靠近 bid0/ask0）平移。
    - `mid2 = max(0, mid - shift)`
    - `half2 = half * scale`
    - `offset_min_2 = max(0, mid2 - half2)`
    - `offset_max_2 = max(offset_min_2, mid2 + half2)`
    - 再做一次边界保护：
      - `offset_min_2 = clamp(offset_min_2, limit_lower, limit_upper)`
      - `offset_max_2 = clamp(offset_max_2, limit_lower, limit_upper)`
  - 结果：得到最终用于拆档的偏移区间 `[offset_min_2, offset_max_2]`。
  - 约束：所有 offset 最低只能到 `0`，不允许负偏移。

- [4]根据合约price_tick定价
此时得到了最终的报单范围，需要做的事情是拆单。策略参数包含对冲侧拆单档数k。
盘口值*[] 即拆k档次，并从orderbook发送一个查询消息，等待tlen查询同步查询，补全当前所有价格档位的tlen(盘口价格厚度)

- [5]根据单手量，将累积需要对冲的头寸进行拆单。
已知总qty，单手量，并有了对齐prick_tick的k档价格。我现在要进行的计算是把

从离盘口最近的位置开始拆单，按照等比分布，

相当于我要把现在mm hedge的分单逻辑直接在mm signal直接完成，不再做多次。
