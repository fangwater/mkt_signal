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


- [3] 对冲盘口偏移函数映射（基于同/反向成交的区间压缩与放大）
  - 输入来自 hedge query：`period_buy_qty`, `period_sell_qty`, `net_qty`（当前敞口，`both`）。
  - 方向硬规则：`net_qty > 0` 必须 `sell`；`net_qty < 0` 必须 `buy`。
  - 记号：
    - `n = net_qty`
    - `q_b = period_buy_qty`
    - `q_s = period_sell_qty`
    - `\varepsilon = 10^{-9}`
  - 公式（MathJax）：

$$
\begin{aligned}
q_{\text{same}} &=
\begin{cases}
q_s, & n > 0\\
q_b, & n < 0
\end{cases},
\qquad
q_{\text{opp}} =
\begin{cases}
q_b, & n > 0\\
q_s, & n < 0
\end{cases}, \\
\mathrm{flow} &= q_{\text{same}} + q_{\text{opp}} + \varepsilon, \\
a &= \frac{q_{\text{same}} - q_{\text{opp}}}{\mathrm{flow}} \in [-1,1], \\
p_{\text{inv}} &= \frac{|n|}{|n| + \mathrm{flow} + \varepsilon}, \\
p_{\text{adv}} &= \max(0,-a), \\
u &= \operatorname{clip}(\max(p_{\text{inv}},p_{\text{adv}}),0,1), \\
s(x) &= x^2(3-2x), \\
g_a &= s(\max(0,a)),
\qquad
g_u = s(u), \\
c &= \frac{\mathrm{final\_offset\_min}+\mathrm{final\_offset\_max}}{2}, \\
w &= \mathrm{final\_offset\_max}-\mathrm{final\_offset\_min}, \\
\beta_{\text{exp}} &= \max(0,\alpha_{\max}-1),
\qquad
\beta_{\text{cmp}} = \max(0,1-\alpha_{\min}), \\
\mathrm{scale} &= \operatorname{clip}\left(1+\beta_{\text{exp}}g_a-\beta_{\text{cmp}}g_u,\alpha_{\min},\alpha_{\max}\right), \\
\mathrm{shift\_cap} &= \min\big(\max(0,c-\mathrm{limit\_lower}),\max(0,\mathrm{limit\_upper}-c)\big), \\
d &= g_a-g_u, \\
\mathrm{shift} &= \mathrm{shift\_cap}\,\tanh(d), \\
c_2 &= \operatorname{clip}(c+\mathrm{shift},\mathrm{limit\_lower},\mathrm{limit\_upper}), \\
w_2 &= w\cdot\mathrm{scale}, \\
o_{\min} &= \max\left(0,c_2-\frac{w_2}{2}\right), \\
o_{\max} &= \max\left(o_{\min},c_2+\frac{w_2}{2}\right), \\
\mathrm{offset\_min\_2} &= \operatorname{clip}(o_{\min},\mathrm{limit\_lower},\mathrm{limit\_upper}), \\
\mathrm{offset\_max\_2} &= \operatorname{clip}(o_{\max},\mathrm{limit\_lower},\mathrm{limit\_upper}).
\end{aligned}
$$

  - 其中：`\alpha_{\min}=hedge_offset_scale_min`，`\alpha_{\max}=hedge_offset_scale_max`。
  - 性质：
    - `a>0`（同向成交占优）时，`g_a` 增大，倾向于放宽区间并外移；
    - `a<0` 或库存压力大时，`u` 与 `g_u` 增大，倾向于压缩区间并向内贴近；
    - 全流程保持 `offset >= 0`，最终区间受 `[limit_lower, limit_upper]` 保护。
  - 说明：当 `net_qty == 0` 时不触发对冲下单（或沿用现有空仓分支逻辑）。

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
  - 厚度补全：
    - 对 `price_levels[]` 发起 orderbook 查询，等待同步回包，补齐每档 `tlen_i`（盘口价格厚度）。
  - 输出：`[(price_i, tlen_i)]`，用于第 [5] 步的按量拆单分配。

- [5]根据单手量，将累积需要对冲的头寸进行拆单。
已知总qty，单手量，并有了对齐prick_tick的k档价格。我现在要进行的计算是把

从离盘口最近的位置开始拆单，按照等比分布，

相当于我要把现在mm hedge的分单逻辑直接在mm signal直接完成，不再做多次。

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
- `hedge_vol_upper_scale`：对冲波动率上界修正系数
- `hedge_vol_lower_scale`：对冲波动率下界修正系数
- `hedge_price_offset_limit_upper`：对冲偏移上界（price_offset_limit）
- `hedge_price_offset_limit_lower`：对冲偏移下界（price_offset_limit）
- `hedge_offset_scale_min`：区间缩放下限
- `hedge_offset_scale_max`：区间缩放上限
- `hedge_offset_mapping_mode`：偏移映射模式（建议默认 `smoothstep`）
- `hedge_aggressive_seq_threshold`：对冲激进阈值
- `max_hedge_price_pct_change`：对冲价格最大变动阈值（%）

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