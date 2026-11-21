# Spread02 本地 Tardis 小时预热修复

当前状态：已部署并完成验收，08:06 UTC新代次追平Kafka，PID1140614、CPU18。
Spread01小时109/109、Spread02小时130/130就绪，227项普通分钟窗口已续接，
8份仓位账本保留；Spread01已处理新的实时bar，Manager确认正常。
Spread02继续100U/规则、发布关闭，总体326/341就绪，其余15项分钟因子保持原门槛。
最终回执是 `continuation_verification.json`；`verification.json` 是首次缺数据时的历史检查点。

修复的原因是原生小时种子只加载 Binance，OKX 的普通公式与 NQ 仍依赖 Kafka
及之后的实时积累。新的小时历史包按市场隔离，读取实际 worker 的合并计划，覆盖
普通公式、NQ 和双市场 spread。Kline 格式继续有效，历史 OHLC 与成交字段从
Tardis trades 聚合，需要盘口与 BBO 的计算再使用 Tardis incremental_book_L2。

所有参数、因子方向、数量单位与就绪门槛保持原值。OKX 保留原始合约张数，
大小单按 price × contracts 分类；历史月份使用上个自然月完整成交的 KLL，
当前月使用只读共享阈值快照。Binance BCH/FIL 的旧包当前月阈值不同，已单独重建。

历史包在本机 `data/ops/cta_spread02_tardis_warmup_20261113/history/`，不写 NFS。
Binance 两个只作小时价差参考的标的 ETH/XRP 使用 72 小时真实 BBO，覆盖实际
48 根价差窗口；OKX ETH 使用 240 小时，覆盖 NQ 的 120+120-1 根完整窗口。
其余标的采用 816 小时。两市场截止点均为 2026-11-11 00:00 UTC。

`assemble_history.py` 和 `assemble_okex_history.py` 只组装已完成且校验和一致的
输入；`verify_readiness.py` 汇总真实引擎播种后的小时就绪结果。旧 Binance 包中
可复用的九个文件先核对 SHA-256 和当前月阈值，来源见 `binance_reused_inputs.json`。
原始日文件完整性由构建器检查，坏文件、日期中间缺口与尾部缺日都报错。

首次检查的数据阻塞（续办时已解决）：NFS 的 17 个 OKX 标的 `trades` 均缺 2026-11-11 日文件，
相同标的的当天 L2 齐全。Kafka 最早源 period 已到当日 04 点之后，旧历史截止到
00:00，存在约四小时衔接缺口。盘点见 `source_latest_days.json`，真实失败样本
见 `missing_day_build_probe.json`；JP 直接读取的最早 period 见
`remote_retained_kafka_start.json`。没有启动下载或改动任何下载队列。

小时包验证与启动保护：

```bash
cta-hourly-history check --history /path/to/history --config /path/to/cta_factor_spread.toml
cta-factor-spread --config /path/to/cta_factor_spread.toml --check \
  --validate-hourly-history --validate-kafka-continuity --hourly-history /path/to/history
```

环境启动脚本会在停止任何旧进程之前执行上述预检，缺失或断档时返回错误。
历史读取与预检使用不订阅消费组的 Kafka 客户端，直接指定保留 offset，避免
先订阅、再退出消费组时的 JoinGroup 竞态阻塞历史读取。
历史只恢复计算窗口，不发布过去的因子点、不重建仓位、不启用策略。
本次不更新 CPU/IP 清单，因为没有改变生产部署、CPU 绑定或网络配置。

后续原始文件齐全后：用同一配置和控制快照构建 `[2026-11-11, 2026-11-12)`
的双市场追加包，在新目录 extend；再次检查实际最早 Kafka hour（留存会推进）。
运行只读 replay 到捕获的 Kafka head，验证每个频率的就绪与时间递增；保存线上
日志和账本，校验准确 release 文件的 SHA-256 后，才通过环境本地脚本切换。
Spread02 保持每规则 100 USDT、关闭发布。当前工作不是启用策略的授权。

代码与测试：按市场播种、价差双腿播种、重叠时段去重、原始 OKX symbol/数量、
来源/覆盖/校验和错误、各 Kafka 分区衔接检查、预检失败不触碰 supervisor 均有覆盖。
规则已写入根目录 `AGENTS.md` 的 `CTA Hourly Factor Warm-Up`。

最终校验文件：

- `hourly_readiness.json`：两个策略组的小时就绪数；这是候选历史包的离线结果。
- `remote_hourly_seed_check.json`：JP 候选包播种结果，与本地完全一致。
- `history_bundle.json` / `remote_archive_verification.json`：18,547,565 字节输入包及各文件 SHA-256。
- `candidate_binary_verification.json`：最终 release 二进制与 JP 审计目录副本一致；没有安装进运行环境。
- `remote_start_preflight.json`：历史末尾 1794355200000，小于 Kafka 首小时 1794369600000；按预期拒绝。
- `live_state_after.json`：原 PID 1965122、原代次和配置保持；Manager 无待确认，Spread02 发布仍关闭。
- `factor_tests.log`：66 项；`builder_tests.log`：4 项；`lifecycle_tests.log`：2 项；`period_pbs_tests.log`：19 项，均通过。

首次检查时未进行完整回放或生产切换；后续数据补齐、回放与切换见下面的续办记录。
最初消费组订阅导致的读取超时已修复，原错误回执保存在 `diagnostics/`；
最终 JP 预检读取成功后明确报出历史边界缺口。

## 下载补齐后的续办

2026-11-13 06:10 UTC 再次盘点，原缺失的 17 个 OKX trades 日文件全部出现；
两市场共 68 个原始 trades/L2 文件完整 gzip 校验通过，验证期间大小与 mtime 未变化。
使用原控制快照生成 11/11 的 24 小时追加包，并在新目录合并至 11/12 00:00 UTC。
70 项当前月共享阈值与原快照完全一致。227 项小时计算依然全部就绪；
JP 新包 SHA-256 与逐币文件校验通过，连续性预检返回 0，先前四小时缺口已消除。

06:17 UTC开始完整Kafka只读回放；随后已完成回放和生产切换，见最终验收。
证据：`download_recheck.json`、`append_source_validation.json`、
`threshold_recheck_summary.json`、`extended_seed_check.json`、
`extended_history_bundle.json`、`continuation_preflight_summary.json`。

## 分钟状态保护与生产切换

首次完整只读回放处理190,180条记录，小时两组109/109和130/130均就绪。
复核发现冷重启会让Spread01已在线的ADA 15min baseline_104、TRB 5min
TP_VPI_001/005失去足够排名样本。因此新增普通分钟状态检查点：小时仍强制使用
Tardis，分钟仅延续本worker自己的有限归一化/分位窗口，不涉及持仓或小时种子。

从原代次完整事实日志迁移386,163条普通分钟记录，逐条重算核对输出后生成227项
窗口检查点。恢复需参数一致、所需大小单阈值一致，Kafka重叠时段只重建raw输入，
不重复推进该检查点的排名；后续每60秒原子保存当前有限窗口。
68项CTA测试、16项rolling_common测试、4项构建测试通过；新增回归证明冷回放
会失去就绪状态，但检查点续接后的新输出与连续运行完全一致，并拒绝小时、
错误窗口、必要阈值变化和衔接缺口。

07:39 UTC 首次停止检查过早，脚本返回时原进程尚未完全退出；已确认退出后续接
安装。07:42 UTC 新PID1140614启动，CPU18，227项普通分钟窗口恢复，8份账本
逐份确认原样保留，受保护进程未改变。旧日志和原环境在远程私有目录
`before_cutover_20261113T073912Z`；不复制其env.sh到本地或回执。
切换脚本现已等待真实进程退出，不再以刚返回的stop结果或/proc存在作为唯一判据。
07:42启动后新进程追平Kafka，08:06完成接管；Spread02发布关闭。

## 最终验收

08:06 UTC Kafka追平，新代次 `4c56e327-b4da-4b4d-a1cd-a92c07e59f67`。
新进程无运行/仓位错误；小时109/109、130/130，原Spread01的三条分钟规则保持就绪。
已确认新1min bar推进、Manager成功确认、待发布数为0，分钟检查点持续刷新。
Spread02四表未启用，持仓账本仍从未启用、时钟/观测为空、目标全零。
两个Exec及其他受保护进程身份和CPU保持，配置与319条Spread02规则未改写。

最终证据：`continuation_verification.json`、`continuation_verified.json`、
`live_continuation_confirmed.json`、`cutover_installed.json`、`cutover_manifest.json`。
补齐后的历史包：`extended_history_bundle.json`（19,131,524字节），原始文件校验：
`append_source_validation.json`。初始完整只读回放190,180条，见`continuation_replay_summary.json`；
分钟迁移逐点验证及窗口状态恢复见`minute_state_export.json`和`minute_state_tests.log`。
停止退出延迟的原失败检查和续接成功回执均保留，所有相关日志已在停止前备份。
