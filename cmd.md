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


- [3] 对冲偏移简化映射（仅看净头寸 + 风控预算）
  - 输入来自 hedge query：`period_buy_qty`, `period_sell_qty`, `net_qty`。
    - 当前版本只使用 `net_qty`（base 口径）；`period_buy_qty/period_sell_qty` 先保留，不参与映射。
  - 方向硬规则：
    - `net_qty > 0`：必须 `sell`
    - `net_qty < 0`：必须 `buy`
    - `net_qty == 0`：不触发对冲下单（或沿用现有空仓分支）
  - 第一步：先把净头寸转成敞口净值（USDT）
    - `mid = (hedge_bid0 + hedge_ask0) / 2`
    - `inv_notional = |net_qty| * mid`
  - 第二步：用风控参数计算“单币敞口净值预算”（USDT）
    - 风控来源：`pre_trade_risk_params`
    - `symbol_exposure_u = max_pos_u * max_symbol_exposure_ratio`
  - 第三步：计算放缩因子（你确认的核心公式）
    - `\varepsilon = 10^{-9}`
    - `x = inv_notional / (symbol_exposure_u + \varepsilon)`
    - `scale = 1 / (1 + x)`
    - 性质：`inv_notional` 越大，`x` 越大，`scale` 越小（更贴近盘口，提升去库存速度）。
  - 第四步：将第 [2] 步得到的偏移区间按金额和比例双口径放缩

$$
\begin{aligned}
\Delta_{\min} &= mid \cdot final\_offset\_min, \\
\Delta_{\max} &= mid \cdot final\_offset\_max, \\
\Delta_{\min,2} &= \Delta_{\min} \cdot scale, \\
\Delta_{\max,2} &= \Delta_{\max} \cdot scale, \\
offset\_{min,2} &= \operatorname{clip}(\Delta_{\min,2}/mid,\ limit\_lower,\ limit\_upper), \\
offset\_{max,2} &= \operatorname{clip}(\max(offset\_{min,2},\Delta_{\max,2}/mid),\ limit\_lower,\ limit\_upper).
\end{aligned}
$$

  - 结论：最终用于拆档的偏移范围为 `[offset_min_2, offset_max_2]`。

- [4] 根据合约 `price_tick` 定价并生成拆档价格
  - 目标：把第 [3] 步的偏移区间 `[offset_min_2, offset_max_2]` 映射为可下单的 `k` 档价格，并补齐每档盘口厚度 `tlen`。
  - 新增参数：`hedge_split_levels`（对冲拆档数，记为 `k`，约束 `k >= 1`）。
  - 输入：`side`、`hedge_bid0`、`hedge_ask0`、`price_tick`、`offset_min_2`、`offset_max_2`。
  - 偏移拆档（从近到远，等距）：
    - 若 `k = 1`：`\delta_0 = offset_min_2`
    - 若 `k > 1`：

$$
\delta_i = offset\_min\_2 + \frac{i}{k-1}(offset\_max\_2 - offset\_min\_2),\quad i=0,1,\dots,k-1
$$

  - 将偏移映射为原始价格（offset 视作相对比例）：
    - `side = sell`：`p_i^{raw} = hedge_ask0 * (1 + \delta_i)`
    - `side = buy`：`p_i^{raw} = hedge_bid0 * (1 - \delta_i)`
  - 按 `price_tick` 对齐（maker 不穿价）：
    - `sell`：`p_i = align_price_ceil(p_i^{raw}, price_tick)`
    - `buy`：`p_i = align_price_floor(p_i^{raw}, price_tick)`
  - 去重与排序：
    - 对齐后若出现重复价位，按“离盘口由近到远”去重保序，得到最终 `price_levels[]`。
    - 距离定义：
      - `sell`：`p` 越小越近（靠近 `ask0`）
      - `buy`：`p` 越大越近（靠近 `bid0`）
  - 厚度补全：
    - 对 `price_levels[]` 发起 orderbook 查询，等待同步回包，补齐每档 `tlen_i`（盘口价格厚度）。
  - 输出：`[(price_i, tlen_i)]`，用于第 [5] 步的按量拆单分配。

- [5] 拆单分配（仅前序分配模式，近端优先）
  - 目标：基于第 [4] 步的 `price_levels[]`，先生成 `amount_list`（单手量对应 qty），再按近到远顺序分配。
  - 输入：
    - `price_levels[]`（已对齐 `price_tick`，按近到远）
    - `Q = |net_qty|`（base 口径待对冲量）
    - `amount_u`（USDT 单手名义金额）
    - `venue`、`qty_step_venue`、`min_qty_venue`、`contract_multiplier`

### [5.1] 先生成 `amount_list`（每档一手 qty）
  - 先算每档按金额换算的 base 数量：
    - `q_base_raw_i = amount_u / price_i`
  - 按 venue 统一口径：
    - `margin/spot`：`contract_multiplier = 1`
    - `futures`：`contract_multiplier = m`（每张合约折算 base 数量）
  - venue 下单单位对齐：

$$
\begin{aligned}
q_{venue,raw,i} &= \frac{q_{base,raw,i}}{m},\\
q_{venue,1hand,i} &= align\_qty\_ceil\Big(\max(q_{venue,raw,i},\ min\_qty_{venue}),\ qty\_step_{venue}\Big),\\
q_{base,1hand,i} &= q_{venue,1hand,i} \cdot m.
\end{aligned}
$$

  - 结果：
    - `amount_list_venue[i] = q_venue_1hand_i`（真实下单单位）
    - `amount_list_base[i] = q_base_1hand_i`（风险与剩余计算单位）

### [5.2] 前序分配（唯一模式）
  - 规则：仅从近到远分配，不做均匀分配，不做复杂权重。
  - 要求：支持多手，不限制“每档最多 1 手”。

$$
\begin{aligned}
Q_{rem} &= Q,\\
n_i &= 0 \quad (i=0,1,\dots,k-1),\\
\text{while}\ \exists i:\ Q_{rem} \ge q_{base,1hand,i}:\\
&\quad \text{for } i=0\ldots k-1:\\
&\qquad \text{if } Q_{rem} \ge q_{base,1hand,i}:\\
&\qquad\qquad n_i = n_i + 1,\\
&\qquad\qquad Q_{rem}=Q_{rem}-q_{base,1hand,i}
\end{aligned}
$$

  - 最终每档下单量：
    - `order_qty_venue_i = n_i * q_venue_1hand_i`
    - `order_qty_base_i = n_i * q_base_1hand_i`
  - 特性：
    - 严格前序优先（每一轮都从近到远）
    - 自动支持极大仓位下的多手分配
    - 当各档一手数量接近时，分布近似均衡；当差异较大时，近端档位会自然获得更多手数

### [5.3] 剩余量
  - `Q_rem` 始终是 `base qty` 口径。
  - 当 `Q_rem` 不足下一档一手时，不下碎单，直接保留到下一轮 query。

### [5.4] 具体例子（futures，含合约乘数）
  - 已知：
    - `price_levels = [100, 102, 105, 110]`（近到远）
    - `Q = |net_qty| = 620`（base）
    - `amount_u = 5000`（USDT）
    - `venue = futures`
    - `contract_multiplier = m = 10`（1 contract = 10 base）
    - `qty_step_venue = 1 contract`
    - `min_qty_venue = 1 contract`
  - 第一步：计算每档 1 手对应数量
    - `q_base_raw = amount_u / price`
      - `[50.000000, 49.019608, 47.619048, 45.454545]`
    - `q_venue_raw = q_base_raw / m`
      - `[5.000, 4.902, 4.762, 4.545]` contracts
    - 向上对齐后：
      - `q_venue_1hand = [5, 5, 5, 5]` contracts
      - `q_base_1hand = [50, 50, 50, 50]`
  - 第二步：前序分配（轮转多手）
    - 初始 `Q_rem = 620`
    - 每完整一轮（4 档都下一手）消耗 `200 base`
    - 前 3 轮后：`Q_rem = 620 - 3*200 = 20`
    - 第 4 轮起：`Q_rem=20 < 50`，所有档位均不足一手，停止
  - 输出：
    - 手数：`n = [3, 3, 3, 3]`
    - 下单：`[(100,15), (102,15), (105,15), (110,15)]`（单位：contracts）
    - 剩余：`residual_base_qty = 20`（base）

  - 输出：
    - 下单列表：`[(price_i, order_qty_venue_i)]`（仅 `order_qty_venue_i > 0`）
    - 剩余：`residual_base_qty = Q_rem`

## 参数汇总（写入 `mm_strategy_params_{venue}`）

### 开仓侧（open）
- `order_amount`：单笔下单量（USDT）
- `open_price_offsets`：开仓档位模板（JSON数组）
- `open_vol_upper_scale`：开仓波动率上界修正系数
- `open_vol_lower_scale`：开仓波动率下界修正系数
- `open_price_offset_limit_upper`：开仓偏移上界（price_offset_limit）
- `open_price_offset_limit_lower`：开仓偏移下界（price_offset_limit）
- `open_order_timeout`：开仓订单超时（秒）

### 对冲侧（hedge）
- `next_query_delay_ms`：对冲 query 触发间隔（ms）
- `hedge_split_levels`：对冲拆档数（`k`）
- `hedge_order_amount_u`：对冲单手名义金额（USDT，若未配置可回退使用 `order_amount`）
- `hedge_vol_upper_scale`：对冲波动率上界修正系数
- `hedge_vol_lower_scale`：对冲波动率下界修正系数
- `hedge_price_offset_limit_upper`：对冲偏移上界（price_offset_limit）
- `hedge_price_offset_limit_lower`：对冲偏移下界（price_offset_limit）
- `hedge_aggressive_seq_threshold`：对冲激进阈值
- `max_hedge_price_pct_change`：对冲价格最大变动阈值（%）

### Pre-Trade 风控（写入 `<dir>:<open>:<hedge>:pre_trade_risk_params`）
- `max_pos_u`：单币种基准上限（USDT）
- `max_symbol_exposure_ratio`：单币种敞口比例（0~1）
- `symbol_exposure_u = max_pos_u * max_symbol_exposure_ratio`（供 MM 对冲放缩计算）

### 通用
- `signal_cooldown`：信号冷却时间（秒）




需要完全理顺合约乘数的相关问题
[1] 从arb hedge


• 你这两个判断方向都很专业，我给你一个“准确版结论”：

  - 1) 你提的做法合理：在策略里缓存两个乘数（开仓腿/对冲腿），margin=1，futures=查表。这会明显降低口径混乱。
  - 2) 但“order update 直接覆盖本地 base 累计”这句，对 arb hedge 不完全成立。
      - cumulative_open_qty / cumulative_hedged_qty 是在 apply_trade_update 里先 qty_to_base(...) 后写入的，见 src/strategy/
        hedge_arb_strategy.rs:1773、src/strategy/hedge_arb_strategy.rs:1840、src/strategy/hedge_arb_strategy.rs:1851。
      - order_manager 里 order.cumulative_filled_quantity 仍是交易所单位（合约场景是 contracts），见 src/strategy/hedge_arb_strategy.rs:1809。

  真正要警惕的是这里：

  - try_hedge_with_residual 把 base_qty 直接传给 check_min_trading_requirements，见 src/strategy/hedge_arb_strategy.rs:1427、src/strategy/
    hedge_arb_strategy.rs:1441。
  - 但 check_min_trading_requirements 的 min_qty 校验是按 venue 原生单位（合约=contracts）做的，见 src/pre_trade/monitor_channel.rs:943、src/
    pre_trade/monitor_channel.rs:966。
  - 这块在 futures 下确实可能单位不一致，你的担心是对的。

  如果你愿意，我下一步可以给你一版“arb hedge 统一单位规范（不改代码，只出改造清单）”。


1、解决fr，xarb信号下发的qty问题
2、解决mm模式的下发信号问题
