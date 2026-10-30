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

## 实盘实现

CTA Perp 的 Kafka 发布器（1m、5m、15m 共用实现）直接复用
`depth_pub::orderbook::OrderBook` 维护收到的全部价格档位。新因子使用整本盘口，
不经过现有 depth20 输出截断；原有 IPC 消息布局保持不变。
实现位于 `src/factor_pub/depth_book.rs`，23 个名称可用于币种因子表和逐组 NQ 规则。
此路径不需要成交大小分类阈值，也不依赖研究端 `pipeline.depth_book`。

- 名义额为 `price × amount`；mid 为当前最优买卖价的均值。每侧累计
  1/5/10/20/50/100bp 的名义额和整本名义额，档位数统计所有正数量价格档。
- `bps_to_10k/50k/100k` 从最优价向外累加该侧名义额，取首次达到目标的档位
  距 mid 的 bp；整本仍不足目标时为 NaN，不外推。
- 数量更新是绝对量。逐条对比更新前档位，正差计入 add、负差的绝对值计入 sub，
  均以该档价格换算名义额。touch 取本条消息更新前的最优买/卖价及更优价格。
  重复价格、盘口交叉修复、局部快照合并沿用 `depth_pub` 的订单簿规则；
  首条建簿消息和标记为 snapshot 的消息不计入增减量，交叉修复删除不伪造撤单流量。
- bar 区间为 `[start, end)`，边界消息归入下一根。盘口存量取 bar 结束前最后状态，
  八个 add/sub 流量字段在 bar 内求和；5s → 1m → 5m/15m 重采样仍按这两种规则。
  无成交时也能生成盘口 bar；未闭合 bar 不参与计算。
- 因子原始值严格按下方 Python 公式计算，再进入现有 CTA 的 zscore 和分位窗口。
  NQ 仍为 120 根，因子分位窗口保持两天（15m 为 192 根）。

Kafka 保留区间可能从某段增量中间开始，因此这里的整本指当前已重建的全部档位，
不把 20 档快照当作交易所全量快照，也不改变既有 Kafka 留存和恢复规则。
空 RX01 清单仍阻止启动，新增计算能力不会自行配置交易币种、方向、NQ 或 Manager。

`depth_flow_net`、`depth_touch_net` 的净流量分母可能非正，原公式此时返回 NaN。
现有 15min zscore 要求最近 96 根至少 67 个有限原始值；若不足，这些因子可以持续
未就绪。计算和预热均保留这个口径，不改用绝对值分母或降低样本门槛。

验证基准：`tests/fixtures/depth_book_factors.json` 保存原始 Python 的逐行输出和源码
SHA-256；Rust 测试逐项对照 23 个结果，Python 测试确认基准未偏离原始实现。

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
