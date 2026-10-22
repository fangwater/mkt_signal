# RX02 新因子配置与回放审计（尚未切换实盘）

操作员原始输入在 `requested_factors.json`：23 币、23 因子、24 个符号分组、70
个 symbol × factor 组合。随后要求去掉所有涉及大小单的因子，已从候选清单去掉
`1INCHUSDT / baseline_047`，保留该币的 `baseline_200`。最终候选为 23 币、22
因子、69 组合；未改实盘 Redis、配置、进程或仓位目标。

当前线上 RX02 为 1min、v1 每币综合模式、50 USDT，47 币、17 因子、192
组合，同时向 JP 与 SG 的 Manager 发布。候选保留原 normalize 1440/min1000、
clip 3 及 Kafka lookback 259200 秒。`preflight_48h.toml` 仅用于分析严格两天
历史的情况，不将线上配置自动缩短为两天。变更集合见 `configuration_diff.json`。

唯一真实大小单依赖是 `baseline_047`，计算 300 根 small_buy − small_sell 的均值。
在线阈值服务没有 1INCHUSDT 的分类阈值。第一次用原始 70 组合启动独立
`--replay-only` 诊断时，现有程序因 14 币缺阈值直接拒绝启动；没有建立历史输出
目录、IPC publisher 或交易目标。现场输出在 `startup_preflight.json`，临时远端
预检目录已删除，原 publisher PID/启动时间未变。

其余 22 个因子逐项经过现有 1min `SymbolFactorState` 计算链测试：3070 根
合成的完整分钟数据，分别使用全零和持续变化的全部 large/medium/small 字段，
要求分数、分位和 ready 完全一致。全部通过。原始因子、标准化和 ready 首根
位置见 `warmup_bounds.json`；代码和测试日志在 `warmup_probe.rs` / `.log`。
这是算法最小样本审计，不是实际 Kafka 全量回放成功的证明。

| 因子 | 原始值首次有效 | 标准化首次有效 | ready 首根 | 最短完整历史 | 相比两天缺少 |
| --- | ---: | ---: | ---: | --- | --- |
| factor_114 | 631 | 1630 | 3069 | 51 小时 9 分钟 | 189 分钟 |
| buy_avg_price | 500 | 1499 | 2938 | 48 小时 58 分钟 | 58 分钟 |
| TP_VPI_006 | 373 | 1372 | 2811 | 46 小时 51 分钟 | 0 |
| sell_avg_price | 360 | 1359 | 2798 | 46 小时 38 分钟 | 0 |

其余因子需求更短。`factor_114` 的 30 根盘口价格变化、600 根滚动和的一阶差分
需 631 根原始分钟；再叠加 999 根标准化积累和 1439 根分位积累：
`631 + 999 + 1439 = 3069`。缺失数据会增加实际时间，运行回放所需的墙钟时间
不等同于这些历史时长。初次本地 Kafka 水位读取成功，但最早消息采样超时。
后续远端完整回放已完成，现场历史覆盖以本文下方的实测结果为准。

已修正配置兼容：Perp helper 接受 `buy_avg_price` 和 `sell_avg_price`，发布器
对已验证的 22 个因子不再要求大小单分类阈值。`baseline_047`、其他未验证因子
仍保留严格阈值检查。发布器相关 Rust 测试 13 项、配置服务器测试 21 项通过，
`cargo fmt --check`、`cargo check --bin cta_perp_factor_model_1m_pub` 和 release
构建通过。正式配置位于 `config/cta_perp_rx02/`，构建校验在
`build_manifest.json`。

按操作员最后要求，发布就绪条件是 Kafka 回放追上实时且解码/计算无错误，
不要求 69 个组合全部 ready。保留现有 v1 逐币综合算法：某币已配置的因子尚未
齐备时，该币保留原仓位、不产生新方向，其他已就绪币正常计算。RLC 的
`factor_114` 和 ZRX 的 `buy_avg_price` 可稍后就绪；不是临时忽略未就绪因子
重新计算平均值。移出新清单的币仍通过原策略零目标退出，不删除仓位账本。

独立回放在 `/home/ubuntu/cta-audits/rx02-factor-replay-20261022T0540Z` 运行，
固定 `--replay-only`，独立历史目录，不读取实盘凭据、不创建 IPC publisher。
审计进程使用 nice 19 / CPU18；任务完成后自动退出。开始时间为 05:36:20 UTC。
回放脚本在 `run_replay.py`，结果包含实盘文件哈希与进程身份前后比对。
独立回放退出后不保留内存因子状态，正式发布器仍需自己的 Kafka 回放。

2026-10-22 05:50:28 UTC 完成，耗时 848.041 秒，退出码 0，无解码/计算错误，
日志明确记录 `CTA history replay complete; no IPC publishers created`。
反复读取 Kafka 最新水位并消费到头后退出，而不是只读启动时的固定快照。
23 币最终闭合分钟均为 05:50 UTC；19 币从 10 月 20 日 05:05 UTC 开始，
KAVA/KNC/SUSHI/XTZ 从 05:06 UTC 开始，各有 2926/2925 根，23/23 NQ 就绪。
现场保留历史约 48 小时 45 分钟，并非完整 3 天。

最终 67/69 组合 ready，21/23 币全因子就绪，只有以下两项尚未 ready：

| 组合 | 最近 1440 样本内有效标准分 | 至少还需有效分钟样本 |
| --- | ---: | ---: |
| RLCUSDT / factor_114 | 1297 | 143 |
| ZRXUSDT / buy_avg_price | 1428 | 12 |

两项当前标准分已有效；连续有效数据下可在后续 143/12 分钟补齐，缺失或无效
数据可能延长。以上是本次已退出审计的状态，不能当成一个仍在运行的预热进程。
正式启动的新进程须重新回放，并以它的最新实际样本为准。完整结果见
`replay_result.json`、`history_analysis.json`；实盘二进制/配置哈希与四个 RX02
进程的 PID/启动身份前后完全一致。全部 43 项相关回归测试通过。

真实策略切换由操作员执行，当前未替换线上程序/配置、未写 Redis、未重启信号，
不能将本次回放称为新仓位已推送或成交。操作交接见 `operator_handoff.md`。

审计运行时只临时向 publisher `app.rs` 追加一个 `#[cfg(test)]` 模块包含
`warmup_probe.rs`，执行 `cargo test --lib rx02_preflight_probe -- --nocapture`。
测试 1 passed，1354 filtered；临时模块已精确移除，没有留下运行时代码改动。
仓库原有并发改动保留。
