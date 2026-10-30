# 全深度盘口因子计算口径

最后更新：2026-10-30 UTC。

本记录依据用户提供的 Python 实现，保存本批 23 个因子的名称、公式、输入字段和
有效值条件，作为后续实现及研究结果核对的依据。
[原始 Python 参考实现](reference/depth_book_factors.py) 与本文一起保留。

## 输入与输出

每个函数读取一个已完成的 baseline bar DataFrame，逐行计算并返回与输入行顺序一致的
一维 `numpy.ndarray`，数据类型为 `float64`。原始字段由 `pipeline.depth_book` 写入；
bar 内的盘口采样、聚合方式及原始字段缺失值由上游定义。

`bps1/5/10/20/50/100` 表示距 mid 对应基点范围内的**累计名义额**，1 bp = 0.01%。
例如 `bps50` 包含 `bps5` 覆盖的范围。`all` 使用上游提供的整本盘口名义额。
这些公式本身逐行计算，未设置时间窗口、zscore、分位归一化、裁剪或方向符号。

## 公共算子

以下 `finite(x)` 表示 `np.isfinite(x)`。未通过有效值条件的输出保留为 `NaN`。

| 算子 | 公式 | 有效值条件 |
| --- | --- | --- |
| `I(B, A)`，对应 `_imb` | `(B - A) / (B + A)` | `finite(B) & finite(A) & (B + A > 0)` |
| `R(N, D)`，对应 `_ratio` | `N / D` | `finite(N) & finite(D) & (D > 0)` |
| `L(B, A)`，对应 `_log_ba` | `ln(B / A)`，先相除再取自然对数 | `(B > 0) & (A > 0)` |

条件严格沿用参考代码：`L` 没有显式 `isfinite` 检查；`I` 也没有额外要求
`B`、`A` 各自非负，或对求和结果另做有限性检查。计算产生的 `NaN`、`inf`
按 NumPy 原有行为保留，输出不再清洗。

## 累计深度不平衡：7 项

令 `B_b = depth_bid_notional_bps{b}`、`A_b = depth_ask_notional_bps{b}`。

| 因子名 | 公式 |
| --- | --- |
| `depth_imb_bps1` | `I(B_1, A_1)` |
| `depth_imb_bps5` | `I(B_5, A_5)` |
| `depth_imb_bps10` | `I(B_10, A_10)` |
| `depth_imb_bps20` | `I(B_20, A_20)` |
| `depth_imb_bps50` | `I(B_50, A_50)` |
| `depth_imb_bps100` | `I(B_100, A_100)` |
| `depth_imb_all` | `I(depth_bid_notional_all, depth_ask_notional_all)` |

买卖名义额均非负且总和为正时，不平衡值位于 `[-1, 1]`。

## 买卖深度对数比：3 项

| 因子名 | 公式 |
| --- | --- |
| `depth_log_ba_bps5` | `L(B_5, A_5)` |
| `depth_log_ba_bps20` | `L(B_20, A_20)` |
| `depth_log_ba_bps100` | `L(B_100, A_100)` |

## 深度分布占比：4 项

| 因子名 | 公式 | 有效值条件 |
| --- | --- | --- |
| `depth_near_far_bid` | `B_5 / B_50`，50 bp 买侧名义额中位于 5 bp 内的占比 | 使用 `R(B_5, B_50)` |
| `depth_near_far_ask` | `A_5 / A_50`，50 bp 卖侧名义额中位于 5 bp 内的占比 | 使用 `R(A_5, A_50)` |
| `depth_far_share_bid` | `(B_100 - B_10) / B_100`，100 bp 买侧名义额中位于 10 bp 外的占比 | `finite(B_10) & (B_100 > 0)` |
| `depth_far_share_ask` | `(A_100 - A_10) / A_100`，100 bp 卖侧名义额中位于 10 bp 外的占比 | `finite(A_10) & (A_100 > 0)` |

`far_share` 没有显式检查 100 bp 分母的有限性。代码也没有验证累计名义额的大小关系，
没有将占比裁剪至 `[0, 1]`。

## 到达指定名义额的距离差：3 项

| 因子名 | 公式 |
| --- | --- |
| `depth_gap_bps_10k` | `depth_ask_bps_to_10k - depth_bid_bps_to_10k` |
| `depth_gap_bps_50k` | `depth_ask_bps_to_50k - depth_bid_bps_to_50k` |
| `depth_gap_bps_100k` | `depth_ask_bps_to_100k - depth_bid_bps_to_100k` |

输出单位为 bp，方向为卖侧距离减买侧距离。直接对两个 `float64` 数组相减，
没有额外有效值条件。指定名义额的计价币种及盘口不足时的字段取值由上游定义。

## 盘口增减流量：5 项

| 记号 | 输入字段 |
| --- | --- |
| `B_add` / `A_add` | `depth_bid_add_notional` / `depth_ask_add_notional` |
| `B_sub` / `A_sub` | `depth_bid_sub_notional` / `depth_ask_sub_notional` |
| `TB_add` / `TA_add` | `depth_touch_bid_add_notional` / `depth_touch_ask_add_notional` |
| `TB_sub` / `TA_sub` | `depth_touch_bid_sub_notional` / `depth_touch_ask_sub_notional` |

`touch` 的盘口范围沿用上游原始字段定义。

| 因子名 | 公式 |
| --- | --- |
| `depth_flow_add_imb` | `I(B_add, A_add)` |
| `depth_flow_sub_imb` | `I(B_sub, A_sub)` |
| `depth_flow_net` | `I(B_add - B_sub, A_add - A_sub)` |
| `depth_touch_add_imb` | `I(TB_add, TA_add)` |
| `depth_touch_net` | `I(TB_add - TB_sub, TA_add - TA_sub)` |

两种 `net` 均先分别计算买侧、卖侧净增量，再调用同一个 `I` 算子。
其分母为两侧净增量之和，必须严格大于 0，否则输出 `NaN`。
净增量可以为负，因此通过该条件时结果也可能超出 `[-1, 1]`；分母没有改用绝对值之和。

## 档位数不平衡：1 项

`depth_level_imb = I(depth_n_bid, depth_n_ask)`。
输入是买卖侧档位数，按 `float64` 读取，沿用 `I` 的有效值条件。

## 名称与枚举顺序

参考实现的 `DEPTH_FACTOR_NAMES` 共 23 项，顺序依次为本文的累计深度不平衡 7 项、
对数比 3 项、分布占比 4 项、距离差 3 项、增减流量 5 项、档位数不平衡 1 项。
`depth_factor_functions()` 按该元组顺序返回同名函数列表。
