# 隔离核心分配登记(jp-meta-elvpn / sg)

最后更新: 2026-10-16 10:15 UTC。**部署、迁移、下线任何绑核进程时,请同步更新本表。**
source IP / `local_ips` 变更同步更新 `docs/jp-meta-elvpn_ip_binding.md`。

2026-10-13 13:42 UTC RX01 已正式启用，独立执行环境 `binance_exec_trade06`，RapidX Binance
现货开方向、退出时永续对冲；五因子、16币、46组合，单份100 USDT。
六个Exec进程实测均使用 CPU mask `0-5,48-95`：persist_manager PID2987736、
Exec Config PID2993382、viz_server PID3003411、rapidx_account_monitor PID3046353、
exec-pre-trade PID3307344、trade_engine PID3381925。
未新增隔离核绑定。Spread publisher PID663465 保持 CPU17；signal PID3132535
使用 CPU0-5,48-95；配置/页面 PID3183433/3390010 使用 CPU0-5,48-95。
旧RX01仅保留账户监控和历史持久化（后者CPU15），没有启动旧执行链。
Manager source06 / virtual03 已建立，lgq 可见并可管理。现货与期货快照均就绪，
12条旧库存腿由独立inventory策略承接；RX02保持启用。下方09:39预部署状态为历史。
验收：`artifacts/cta_spread_rx01_activation/live_validation.json`。

2026-10-13 09:20 UTC Spread RX01 更新为16币/5因子/46组合（配置480387a3）。
操作前核查CPU17没有专用线程占用；通过环境本地start脚本重启因子发布器至
PID663465，5线程全部CPU17，env.sh持久化核绑定。其余signal/config/dashboard
PID3199322/3183433/3390010保持CPU0-5，仓位发布关闭。双市场Kafka历史重新
预热；09:40 UTC已追至实时，46/46组合就绪。新Perp/Exec未重启、未改配置。
CPU17原空闲登记由本条替代。

09:39 UTC新增RX01专用Exec运行目录 `binance_exec_trade06`，本地arbmm
release构建后经publish-exec逐文件SHA-256校验安装，六个二进制与脚本已齐备。
凭据在远端内部核对为旧RX01，且与RX02不同，未输出秘密。全部进程仍停，
尚未占用新CPU或监听10046/18166；未建立Manager source、virtual跟随或库存承接。
此次仅上线Spread信号观察，仓位发布仍关闭；不得直接启动空目标Exec接管旧库存。

2026-10-13 09:14 UTC 按操作者要求退役旧 special 因子发布器。通过 RX02
环境本地 stop 脚本移除 supervisor 登记并停止 PID1975542；全机已无
`cta_special_factor_model_1m_pub` 进程。RX02/RX03 的对应二进制、配置和启停
脚本共9个文件归档至 `/home/ubuntu/retired_cta_publishers/20261013T091434Z`，
两旧环境设置 `CTA_SPECIAL_RUN_FACTOR_PUBLISHER=0`，历史数据、仓位记录保留。
新 Perp/Spread 与 Exec trade05 的 PID、CPU pin 均保持，Perp 全24组合从
09:13分钟继续更新至09:14分钟。旧环境不再有运行进程；下方保留共享publisher
的记录已被本条替代。virtual02/lgq_cta 的5个新因子各1份、4个退出因子0份，
trade05倍率1；实际绑定、Manager目标与Exec目标核对一致，待投递队列为空。
证据：`artifacts/cta_rx02_virtual_validation/after_retirement.json`。

2026-10-13 08:57 UTC 操作者执行 RX02 正式切换，09:03 UTC 只读核验通过。
`binance-cta-perp-rx02` signal/config/dashboard PID 为 421006/406886/411079，
均使用 CPU0-5；publisher PID4133794 保持 CPU18。新发布 enabled=true，
24/24 组合实时就绪、单份100 USDT、publication_error=null。
旧 `binance-cta-special-rx02` signal/config/dashboard 已停，仅保留共享
publisher PID1975542/CPU0-5。Exec trade05 未重启。切换 completed 标记已存在，
四个新增因子策略已到达 Exec，检查时全部目标为零、无待执行量或活动订单量；
账户仍有约16.53 USDT的旧零碎空仓。下方观察阶段 enabled=false/PID 为历史记录。

2026-10-13 08:41:32 / 08:42:40 UTC，Binance Futures market / bookticker
进一步更新为PID255763/CPU14、PID266181/CPU9，源码`37fd96a3`。
最终release SHA-256 `b95c028636a752cb55de11734c8a1957873a74e8139a21871365c98a279b8c58`，
两个实际运行进程的/proc/exe均校验一致。日志仍同步写有界tmpfs、无日志worker；
fd1/fd2改为/dev/null，Rust panic通过同一个有界logger记录，消除直接打印无限增长旁路。
下述初次tmpfs上线的PID及stdio安排已由本条替代；CPU、IP及其他进程不变。

2026-10-13 08:27:23 / 08:29:59 UTC，Binance Futures market / bookticker
分别更新为 PID117542 / CPU14、PID143132 / CPU9。发布源码`e98125c6`，release
SHA-256 `3ecf140f46bc94a0e71ff7f265a99a99f426648f19d047b18405c31a5b53dc58`。
删除专用`bbo-log-writer`，CPU0不再承担该日志worker；行情仍使用current_thread
运行时，既有DNS/配置读取可能短暂出现Tokio阻塞worker，不是日志线程。
首次上线logger/stdio写tmpfs；随后`37fd96a3`将原始stdio改为/dev/null，
Rust panic经有界logger记录，关闭容量旁路。logger写`/dev/shm/spread_pbs-1000/`，每角色当前日志
2MiB、上一份2MiB；常规状态300秒，HFQ延迟摘要仍60秒。JP swap未启用。
经本机release构建、上传SHA校验、原子安装、环境角色启动脚本逐个重启；
ZMQ PID1792862/CPU5与三个FR PID828134/1922219/2848056保持。
未改env.sh、源IP、ENI、路由或IRQ，未启动旧model_pub/model_1m_pub。
回退文件`spread_pbs.before-tmpfs-e98125c6`和同名start脚本备份留在环境目录；
若回退还需配套恢复采集器日志路径，不能只换二进制。
详见`docs/binance_futures_latency.md`及独立项目`docs/ticker_tmpfs_logs.md`。
下面07:19–07:43的日志worker记录为历史方案，已被本次同步tmpfs方案替代。

2026-10-13 08:09 UTC 经环境本地脚本重启新 Perp publisher 至 PID 4133794，
CPU18。首次预热因旧 1800 秒上限退出，本次配置改为 3600 秒（`2a28edd2`），
08:25:19 UTC 完成预热；08:25:56 现场 24/24 组合实时就绪、9/9 NQ 就绪，
解析/计算错误为 0。新 signal/config/dashboard PID 3985389/3973622/3979991
保持运行于 0-5，enabled=false；旧 RX02 四进程、RX01 及 trade05 六进程保留。
证据：`artifacts/cta_perp_rx02_preview/live_validation.json`。

2026-10-13 08:04 UTC 新 Perp publisher PID 3841796 的 5 个线程从
housekeeping 0-5 原地迁到 CPU18，未重启、保留 Kafka 预热状态。操作前确认
CPU18 没有用户线程使用；环境 `CTA_PERP_FACTOR_MODEL_1M_CORE=18` 已持久化。
新 signal/config/dashboard 仍为 0-5；旧 RX02、RX01、Exec 的 affinity 未改。

2026-10-13 07:50–07:52 UTC `binance-cta-perp-rx02` 已启动配置服务 PID
3973622（19186）、Dashboard PID 3979991（10196）、观察信号 PID 3985389；
因子 publisher PID 3841796 保留。均为 housekeeping 0-5，发布保持关闭。
新旧 RX02 页面使用独立端口，新路径 `/cta/binance-cta-perp-rx02/` 已接通；
旧 special 四进程和 Exec 未重启。切换脚本 `41461ead` 支持先停止观察进程再承接
旧状态；`73a68c85` 补齐只读历史回放配置。07:56 UTC 页面检查通过，Kafka 仍在预热。

2026-10-13 07:35 UTC 在 `binance-cta-perp-rx02` 安装 CTA Perp 预备环境，
仅启动 Kafka 因子发布器 PID 3841796，affinity 0-5，未新增专用 CPU 绑定。
二进制 `976d557e`，切换脚本 `f72ab0e4`；配置 `enabled=false`，没有新仓位
发布器、配置页或 Dashboard 进程。旧 special RX02 四进程
140816/147320/945585/1975542 保持运行；RX01 与 Exec 未重启。
07:36 UTC 历史仍在预热，尚未启用新目标；详见 `docs/cta_perp_rx02_cutover.md`。

2026-10-13 07:38–07:41 UTC 追加只读核验直接抓到日志worker CPU0的ext4
`write(fd=2)`事务等待，两个连续采样簇跨度2.711/5.052秒；主接收线程同轮D=0。
有界日志隔离已在真实磁盘阻塞下生效；不等于所有I/O路径均已消除。
同期一次行情断开抓到远端主动FIN，早于后一次日志阻塞；其根因仍未确定。
追加只读工具已按截止时间退出，未修改生产配置。

2026-10-13 07:26–07:34 UTC 完成异步日志上线后的 ens42 RX 合并复测。
CPU7 六路公共 BTC 探针已退出，恢复未分配；NIC 恢复 Adaptive RX on / rx20 /
tx64，selector 恢复 auto，回退 timers 均 inactive。首组改善，但末段07:33:53
单路reset自动换远端，第二组不具备固定连接归因条件，因此不固化网卡改动。
13分钟接收线程3838个只读采样中D=0，最大TCP未读8688字节；不代表长期无其他阻塞。
报告：`../hfq_latency_lab/docs/ticker_async_followup.md`。日志worker CPU0保持。

2026-10-13 07:19:41 UTC 更新 Binance Futures bookticker：PID 3685126，
主接收线程 CPU9；新增同进程 `bbo-log-writer` TID 3685127 显式固定 housekeeping
CPU0，使用有界非阻塞队列隔离磁盘写入。发布源码 `896de806`，本地 release
SHA-256 `c3f5a1eda2cc70095c5ddbca011dd56f5cbe9773e0a124335f9607b379069ef7`，
上传校验后原子安装，通过环境 `scripts/start_spread_pbs.sh --role bookticker` 启动。
两条 USD-M 发布连接均 4/4 ACK，COIN-M 持续发布；market 1784924 / CPU14、
ZMQ 1792862 / CPU5、三个 Binance FR 828134/1922219/2848056 均保留。
model_pub/model_1m_pub 仍停用。未改 source IP、ENI、路由；证据见
`../hfq_latency_lab/data/experiments/20261013-async-bbo/`。

2026-10-13 06:49 UTC 仅更新 RX01 Spread 只读信号观察页：Dashboard PID
由 3190607 更新为 3390010，CPU mask 仍为 `0-5`，监听仍为 `127.0.0.1:10186`。
页面提交 `ff3a8082`，新增 `/cta/binance-cta-spread-rx01/signals/`，后台只读导入
Kafka 因子历史并补现货公共 K 线。factor/config/signal PID 3150153/3183433/3199322
保持不变，首次预热未重启；RX01 发布仍关闭，旧 RX01 账户监控/持久化、
RX02 和 trade05 所有进程及账户配置均保持不变。
证据：`artifacts/cta_spread_signals/validation.json`。

2026-10-13 完成独立 ticker 诊断：CPU7/17 临时运行有截止时间的公共行情探针和
只读接收队列观察，均已退出，恢复未分配；没有重启生产 publisher/relay/FR。
ens42 短时进行 RX 中断合并 ABAB 实验，结束恢复 Adaptive RX on / rx-usecs 20 /
tx-usecs 64，回退 timer 已停用；候选试探临时 off 后恢复原 auto 配置。
未改 source IP、ENI、路由或常驻 CPU 分配。结果和恢复证据见独立项目
`../hfq_latency_lab/docs/ticker_latency_diagnosis.md`、该项目的 `data/experiments/20261013-diagnosis/`。
实测主接收线程曾在文件系统事务等待中停顿至少 2.6 秒；同步日志是强嫌疑，
尚未捕获其具体阻塞 syscall。此结果不等于已经修复该问题。

2026-10-13 06:28 UTC 仅将 RX01 切换为独立 CTA Spread 观察环境：
`~/binance-cta-spread-rx01` 的 factor publisher / config / dashboard / signal
PID 为 3150153 / 3183433 / 3190607 / 3199322，CPU mask 全为 `0-5`，
未分配额外隔离核。二进制来自本地 `arbmm` release `456be044`，页面/脚本
命名更新为 `470af607`；上传 SHA-256 校验后原子安装，通过环境内脚本启动。
19 个币种、5 个因子、57 个组合，`enabled=false`；06:28 验收时历史回放仍在
推进，因子和价差就绪均为 0/57，不能视为实时信号验收完成。
旧 RX01 config / Viz 已通过其环境脚本停止；交易进程继续停盘，
account_monitor PID 2394028（0-5）与 persist_manager PID 2392640（15）保留。
RX02 四进程、trade05 六进程 PID 全部保持，旧 RX01/RX02 的 env.sh 和
trade_engine.toml 校验不变。新页面使用回环 10186，配置页 19174，4191 网关
入口为 `/cta/binance-cta-spread-rx01/`；旧 RX01 页面入口跳转到新环境。
验证记录：`artifacts/cta_spread_rx01_deployment/validation.json`。

2026-10-13 Binance USD-M ticker 双连接选优部署到既有 bookticker / CPU9：
仍为两条发布连接，仅试验时额外开一条全订阅影子连接，不分配 CPU7 或新常驻进程。
只通过环境内 `start_spread_pbs.sh --role bookticker` 切换该角色；
market PID 1784924 / CPU14 和三个 Binance FR 原进程保持运行。
运行配置 `~/spread_pbs/binance-futures/config/binance_bbo_selection.toml`，
机制、阈值、热停用及验收结果见 `docs/binance_futures_latency.md`。
原始二进制和脚本备份 `.before-selection-20261013`；构建与发布证据
`../hfq_latency_lab/data/deploy/20261013-bbo-selection/`。源 IP 仍只用 ens42 的 .90/.91。

2026-10-13 05:04 UTC 完成独立 `hfq_latency_lab` BTC ticker 临时连接试验：
CPU7 临时运行限时探针，未注册常驻 supervisor；结束后复核无探针、旧 model_pub
或单独绑定 CPU7 的进程，CPU7 恢复未分配。生产行情配置和 FR PID 均未修改，
三个 Binance FR 波动率输入均为 372/372 fresh。结果位于独立项目
`../hfq_latency_lab/docs/ticker_path_experiment.md`；候选节点未写入生产配置。

2026-10-13 04:30 UTC 按用户要求停止全部旧 `model_pub`：
在 `~/model_pub/binance-futures-mid-re-{30s,1m,5m}` 分别执行环境内
`scripts/stop_model_pub.sh`，脚本完成 supervisor 条目删除及遗留进程清理。
复核三个原 PID 1110763 / 1111090 / 1111465 均退出；现场无 `model_pub`、
`model_1m_pub`，也无单独绑定 CPU7 的进程。原表中的十个 `model_1m_pub`
本次操作前已不在线，因此同步修正 CPU7 为未分配。
部署目录、配置和历史数据保留；普通 intra/CTA special 模型、tradeflow、
fusion features 与三个 Binance FR trade_signal 继续运行，后者波动率输入均为
372/372 fresh、stale=0。未处理缺 depth 问题，未改 source IP / ENI / 路由。

2026-10-13 03:09 UTC 按用户要求暂停旧 RX01：通过
`~/binance-cta-rx01/intra_scripts/stop_intra_{trade_engine,trade_signal,pre_trade}.sh`
停止三个交易进程并核验实际退出。账户监控、persist_manager（CPU15）、Viz、
配置服务及普通共享因子 publisher 保持运行；隔离核分配不变。
停机后 `rapidx_open_orders --exchange binance` 只读查询确认 SPOT/MARGIN/PERP
挂单均为 0，未提交撤单或对冲交易。账户仍有方向敞口，尚未完成中性化；
不得将本次进程停机视为空仓或仓位承接完成。新 CTA Spread 发布保持关闭。

2026-10-13 04:04 UTC 发布 Binance USD-M BTCUSDT 轻量延迟观测：
`~/spread_pbs/binance-futures` 的 market / bookticker 通过环境内脚本重启，
实测 PID 分别为 1784924 / 1787779，仍绑定 CPU14 / CPU9。
`~/spread_bbo_zmq_pub/binance-futures` relay PID 1792862，仍绑定 CPU5。
新增三频道 Q−E/Q−T 的分钟 P50/P99，停用该 relay 的混币 ingress KLL。
本地 `arbmm` release 产物上传、SHA-256 校验、原子安装；运行中 spread_pbs
哈希为 `d501881e54bb7805eb806aae06153970e2aa8eda74f623a345befa89c1a2a314`。
未新增行情核；统计复用现有接收时间、生产去重和 stats tick。
研究机 `~/hfq_latency_lab` 每分钟只读 JP 分钟日志并与外部公开数据配对。
详见 `docs/binance_futures_latency.md`；回退备份为原文件旁的 `.before-latency-20261013`。
本次停止脚本耗时约 67 秒，启动约 76 秒，不能仅凭短超时判定操作未发生；须核验实际 PID。

## jp-meta-elvpn(ip-172-31-35-228,c7i.metal-24xl)

CPU 布局:`0-5` housekeeping(OS、SSH、PM2、系统服务),`6-47` 隔离
(isolcpus/nohz_full/rcu_nocbs),`48-95` 超线程 sibling 已因 `nosmt=force` 下线。
内核默认 `irqaffinity=0-5`,irqbalance 关闭;现场 NIC 数据面 IRQ **不**落在 0-5,
而是一卡一核(见下表 46/47)。ENA 管理中断(`ena-mgmnt`)仍在 `0-5`。
详细内核参数见 `jp-meta-elvpn_hfq_low_latency_tuning_20260618.md`。

| 核 | 进程 | 备注 |
|----|------|------|
| 5 | spread_bbo_zmq_pub(binance-futures) | 绑在 housekeeping 边缘核 |
| 6 | fusion_factor_pub(binance-futures) | |
| 7 | 未分配 | 2026-10-13 停止旧 model_pub ×3；现场无 model_1m_pub 或单独绑定 CPU7 的进程 |
| 8 | spread_pbs binance-margin | |
| 9 | spread_pbs binance-futures bookticker | BBO 专核；USD-M 双连接选优，试验时短暂增加一条影子连接 |
| 10 | spread_pbs gate-both | |
| 11 | spread_pbs bitget-both | 忙时单核饱和,候选拆分(margin/futures 两进程) |
| 12 | spread_pbs okex-both | |
| 13 | depth_pub_general | 本机 8 路 depth25（BN/OKX/Bitget/Gate × margin+futures）；**不含 Bybit**（Bybit 在 sg）。pm2 `dp_general` |
| 14 | spread_pbs binance-futures market | 原 depth_pub binance-both 腾出；trade/incremental/derivatives |
| 15 | persist_manager ×N | okex_mm_alpha、fr_arb03/04、bitget_fr_arb02、gate_fr_arb01/02 等堆叠 |
| 16 | spread_pbs gate-bitget-futures-bbo | 独立 Gate / Bitget USDT 合约 BBO；`spp_gate_bitget_futures_bbo`；IPC `futures_bbo` |
| 17 | cta_spread_factor_model_1m_pub(binance-cta-spread-rx01) | 2026-10-13 新清单已实时就绪，PID663465/5线程；CTA_SPREAD_FACTOR_MODEL_1M_CORE=17 |
| 18 | cta_perp_factor_model_1m_pub(binance-cta-perp-rx02) | 2026-10-13 原地迁入，5 线程；CTA_PERP_FACTOR_MODEL_1M_CORE=18 |
| 19 | (空) | 原 binance-intra-arb01 已下线退役 |
| 20 | account_monitor(okex_mm_alpha) | okex MM 从 20 起 |
| 21 | trade_signal(okex_mm_alpha) | |
| 22 | pre_trade(okex_mm_alpha) | |
| 23 | trade_engine(okex_mm_alpha) | 单线程 |
| 24-27 | (空) | 原 binance_mm_alpha 已下线退役 |
| 28-35 | (空) | 原 binance-intra-arb02 已下线删除；`binance-cta-special-rx03` 独立 BBO(`spread_pbs_cta_rx03`)已于共享服务 max_nodes=64 重建后回收，core 28 释放 |
| 36-45 | (空) | |
| 46 | NIC IRQ: ens41 全部 Tx-Rx 队列(16) | 默认路由/主网卡;禁止再绑用户进程 |
| 47 | NIC IRQ: ens42 全部 Tx-Rx 队列(16) | 第二块网卡;禁止再绑用户进程。原 pred_rnn_infer 已下线 |

2026-10-09 03:04 UTC 新增 `~/spread_pbs/gate-bitget-futures-bbo`，代码 `78150e7a`，
本地 release 构建上传、SHA-256 校验后原子安装，使用环境内 start 脚本启动。
单进程绑定 CPU16，Gate / Bitget 各两条 SBE BBO 连接，源 IP 分别为
`172.31.46.90` / `172.31.46.91`（ens42，table 101）；不订阅深度/成交/ticker。
原 `gate-both` / `bitget-both` 进程及 FR 消费通道维持运行；新通道为
`futures_bbo/{gate-futures,bitget-futures}/ask_bid_spread`。

2026-10-11 行情巡检迁入独立 `~/fr_monitor`，常驻 `--dry-run`，不发送新钉钉告警；未设置独立 CPU pin，实测继承 CPU mask `0-5,48-95`。`public-infra-monitor.service` 停用，卸载其常驻 TCP 重传/状态 BPF。NAV 不再管理行情页面和采集。

2026-10-11 当前 special/Exec：RX02 只运行共享 publisher、signal/config/dashboard（4 个进程）；标准 `binance_exec_trade05` 使用 RX02 账户，独立运行 exec-pre-trade、trade_engine、RapidX account_monitor、persist_manager、Viz 和 Exec Config（6 个进程），跟随虚拟账户 `lgq_cta`，五因子各 1 份，ctaker 实盘执行。RX03 整套进程已停止；两个旧 companion 停止。所有新增/保留进程均未设置独立 CPU pin，使用 housekeeping 0-5；共享 publisher PID 1975542 保留。下方带日期的旧状态为历史记录。

2026-10-12 06:05 UTC RX02 单份 200 USDT 发布（`47becad0`）：只重启 `cta_special_signal`（PID 140816）、配置服务（147320）、dashboard（152544）；三者现场 CPU mask 均为 `0-5`，未新增隔离核绑定。共享 publisher PID 1975542 与 trade05 六个执行进程均保持原 PID，CPU mask 仍为 `0-5`。现有 32 个有效币种/因子组合各独立使用 200 USDT，旧仓保持原 qty 到退出；Manager 发布正常，32 项目标与 Exec 一致。`env.sh` 和 `trade_engine.toml` 校验未变，source IP 分配未改动。证据：`artifacts/cta_special_per_pair_200_release/validation.json`。

2026-10-11 05:46 UTC trade05 待执行量/权益修复：本地 release 上传并校验 SHA-256 后原子安装，通过环境脚本重启 exec-pre-trade（PID 3371755）、Viz（PID 3371634），并重启 Manager 网页服务。trade_engine、账户监控、persist_manager 与 RX02 四个信号进程继续运行，RX03 无进程。CPU 分配仍为 housekeeping 0-5。Manager 已解析 RapidX 账户权益；待下单量按步长、minQty、minNotional 检查，真实余量保留在差额中。线上证据：`artifacts/trade05_pending_equity_live_validation.json`、`artifacts/trade05_pending_equity_live_browser.json`。

未绑核、跑在 housekeeping 0-5 的交易/数据栈(截至本次盘点):
binance_fr_arb01/02/03/04、gate_fr_arb01/02/03、bitget_fr_arb01/02、
okex-intra-arb01 全套、trade_flow_feature ×8、rolling_metrics ×5、fusion_factor_1m、
persist_center、predict_file 及各类 viz/config/dashboard 服务。
`okex_mm_alpha` 的 persist_manager 与其它 persist 一起堆叠在 15。
`binance-cta-rx01` 的 persist_manager 同样堆叠在 15。
`bitget_fr_arb03` 已启动；其 persist_manager 绑核 15，其余进程未绑核。
2026-10-04 15:11 UTC 重新发布 `gate_fr_arb03`（`322c482c`，Gate WS 保证金恢复解锁），
六个交易栈程序已重启并核验：persist_manager 绑核 15，account_monitor、
trade_signal、pre_trade、trade_engine、viz_server 和 FR dashboard 未固定绑核。
2026-10-07 15:38 UTC 按 publish FR 流程重新发布 `binance_fr_arb03`（`d35f67ea`，
含 Binance FR 限仓价格 buffer 修复）；六个交易栈程序均已核验运行文件校验值。
persist_manager 仍绑核 15，其余五个程序未固定绑核，在线可用核为 housekeeping 0-5。
trade_signal 在撤单、替换和 F 现货对齐期间停止，对齐核验后已恢复。
2026-10-14 22:46 UTC 重新发布 `binance_fr_arb04`：由本机已同步的 `arbmm`
`6ff0248e` 工作树（含当前本地修复）构建，上传并校验后原子安装六个 Rust 程序。
persist_manager 仍绑核 15，其余五个程序实测 CPU mask 为 `0-5,48-95`；
配置服务也已恢复。trade_signal 在撤单、PARTI 补对冲和基础栈启动期间停止，
账户快照与限仓缓存就绪后已恢复；47/47 在线交易对杠杆确认 5x，限仓缓存无缺失。
共享 `delist_risk_server` 同步发布 margin 资格过滤修复，实测 CPU mask
`0-5,48-95`。隔离核、IRQ 和账户配置未调整。构建、成交和运行文件校验记录见
`artifacts/binance_fr_arb04_republish/20261014T2227Z/`。
2026-10-08 06:00 UTC 完成 `gate-intra-arb01`、`bitget-intra-arb01` 退役：
仅将各自 `data/persist_manager` 的 RocksDB 归档至 `~/retired_data/`，逐文件校验后
删除部署目录、退役配置服务的 PM2 历史条目及对应 Nginx 转发；两环境无运行进程。
`bitget-gate-cross-arb01` 仅配置服务在线，交易栈仍处于停止状态；
Gate FR 三套交易进程的 PID 与操作前一致。归档清单见 IP 绑定文档。
2026-10-10 04:52 UTC 按用户要求停盘并清仓 `binance-cta-special-rx02/rx03`：
先通过环境内脚本停止 signal 与 pre_trade，再按独立 RapidX portfolio 查询并以
reduce-only 市价单平掉 rx02 的 6 个、rx03 的 5 个币种持仓，复核两账户持仓与
SPOT/MARGIN/PERP 挂单均为 0，余额仅 USDT，借款为 0。成交身份已在账户日志核验，
执行持久化待确认数为 0。随后通过环境内 `stop_cta_special.sh --execute` 停止全栈，
共享 `cta_special_factor_shared_binance_futures` 同时停止，现场两环境运行进程均为 0。
这些进程原运行于 housekeeping 0-5，隔离核分配不变；rx01 的 7 个进程 PID 与
操作前一致，普通 `intra_factor_model_1m_pub` 继续运行。部署目录、配置和历史数据保留，
逐单成交及停机复核记录位于各环境 `run/cta_special_shutdown.json`。

2026-10-10 05:28 UTC 按用户要求重新发布并启动 rx02/rx03 加权 CTA special：
在维护工作区同步的 `arbmm` 上本地 release 构建，将本次 CTA 修改对应的精确产物
上传到临时目录、逐文件校验 SHA-256 后原子安装；已有凭据、namespace、账户与执行参数保留。
rx02 共 8 个进程，rx03 共 7 个进程，均未新增独立绑核，仍运行于 housekeeping 0-5。
rx02 独占共享 special 因子 publisher，计算 16 个币种 × 5 个因子；rx03 只订阅。
每币随机分配 2–5 个因子、权重和正负号，两个 env 的 Redis key 独立。
已验证权重、符号与因子列表热更新，signal/pre_trade PID 不变，测试后恢复初始随机表。
05:40 UTC 已通过独立账户的空仓、零挂单与账户恢复检查，并将两个 env 交易开关设为 enabled；
signal 与 pre_trade 均已热加载。05:41 UTC 共享 publisher 完成回放，16 币 × 5 因子持续发布，
decode/evaluation/publish 失败均为 0；05:43 UTC 两环境各 16 币的因子与 NQ 全部就绪，已产生新交易。
逐币复算已验证符号修正、归一化权重、分位数投票、V 与执行系数完全一致。
本次只读查询到 Kafka 首条保留记录为 2026-10-08 04:26 UTC，约 49 小时历史，实际足够预热。
标准化早期有限 raw 同时写入 capped 窗口，因此两窗口预热不相加；
标准化加分位数的最低连续有效样本为 2439 分钟（约 40.7 小时），还需考虑各原始因子的自身历史。
rx01 与普通因子 publisher 的 PID 保持不变；公共 TLen 保留其 BTC 和旧因子元数据。
安装备份与验证记录位于各环境 `run/deployment_backups/`、`run/cta_special_redis_reload_test.json`、
`run/cta_special_resume.json`、`run/cta_special_live_validation.json`、`run/cta_special_weighted_vote_validation.json`。

2026-10-10 05:56 UTC 用户明确要求仅开启新版信号、保持空仓并关闭交易；此前误将发布理解为恢复交易。
两个 env 的 `config/cta_special.json enabled=false` 已热加载，通过各环境脚本停止
`pre_trade` 与 `trade_engine` 并复核实际进程退出。以 reduce-only 市价单平掉
rx02 的 FIL、DOT 和 rx03 的 AAVE，三笔均完全成交；05:59 UTC 独立账户查询
持仓与 SPOT/MARGIN/PERP 挂单均为 0，执行与账户流水持久化待确认均为 0。
当前 rx02 保留 6 个进程，rx03 保留 5 个进程：新版信号、配置、图表、账户监控、
持久化继续运行，rx02 另外拥有共享 special 因子 publisher；均使用 housekeeping 0-5。
每个 env 的 16 币因子与 NQ 全部就绪，关交易后持续记录 `observed/disabled`，
未再发布执行信号。随机 Redis 因子表保留，可动态调整。
当前停机与空仓证据为 `run/cta_special_signal_only_shutdown.json`；本次平仓逐单记录
在 `run/cta_special_shutdown.json`，旧清仓记录已保存到 `run/deployment_backups/`。

2026-10-10 06:42 UTC 在 rx02/rx03 发布有符号权重配置：整表为 symbol → factor → signed weight，
保存检查当前 publisher 计算范围。rx02 使用指定 16 币 32 项组合，rx03 保留原组合；
共享 publisher 扩展到 16 × 6（增加 baseline_004，保留 rx03 的 factor_004）。
Kafka 闭合因子历史落盘与 rx02 数量仓位回放都复用现有 publisher/dashboard，
进程数量仍为 rx02 6、rx03 5，housekeeping 0-5，未新增绑核。
rx02 禁用移动止损，单次回放增量 100 × |V|，现有 rolling/NQ 不变。
两环境 enabled=false，pre_trade/trade_engine 保持停止。
07:12 UTC 仅重启信号进程安装负权重同步修复，publisher 未重启；07:15 UTC 两环境各
16 个币全部就绪，rx02 实时/回放逐币一致。07:16 UTC 复核两个账户持仓、挂单均为零。

2026-10-10 10:30 UTC rx02/rx03 已切换为每因子独立满多/满空/空仓信号，分别为 5 个因子策略、32/52 项币种绑定。发布端生成普通 qty，退出使用 clear；标准 Exec 只增加 clear 支持。两份 companion 目录 `binance-exec-cta-special-rx02/rx03` 已准备，但没有启动 Exec、旧 pre_trade 或 trade_engine。现有 signal/config/dashboard 已经由环境本地脚本重启，共享 publisher PID 1975542 保留，进程数仍为 rx02 6、rx03 5，housekeeping 0-5，未新增 CPU 绑定。两账户持仓/挂单为零；前端按因子展示历史模拟 qty、PnL 与 B/S 点。companion 的 Manager 市场规则缓存尚未配置，未来启用前须由 Manager 正常提供；当前只观察信号。

2026-10-10 14:51 UTC 更新既有 rx02/rx03 单因子交易面板，只画实际模拟开平仓的配对 B/S，保留 NQ。仅 dashboard 通过环境本地脚本重启（rx02 PID 2456915、rx03 PID 2457787）；共享 publisher PID 1975542 和两份 signal/config 进程保持原样，进程数量仍为 6/5，housekeeping 0-5，CPU 绑定及 source IP 无变化。两份 signal 配置均 enabled=false，原环境和 companion 均没有运行 pre_trade、exec-pre-trade 或 trade_engine。

2026-10-10 16:39 UTC rx02/rx03 简化信号配置并将执行参数归 CTA Manager 管理，只重启 signal/config/dashboard：rx02 PID 2561169/2560891/2561832，rx03 PID 2562970/2562876/2563089。共享 publisher PID 1975542、账户监控和持久化未重启；进程数仍为 6/5，housekeeping 0-5，CPU/source IP 无变化。仅移除本地冗余信号字段、Risk Params 接口与因子整表内 exec 参数；既有 Exec 文档未改写。交易 enabled=false，执行进程均停止。

2026-10-10 17:03 UTC rx02/rx03 将满仓金额合并到 CTA 信号的单份 max_pos_u（保留 1000 USDT），移除逐因子金额表与币种金额覆盖。仅通过环境本地脚本重启 signal/config/dashboard：rx02 PID 2582314/2581462/2585918，rx03 PID 2587152/2587076/2587842。共享 publisher PID 1975542、账户监控和持久化保留，进程数仍为 6/5，housekeeping 0-5，CPU/source IP 无变化。既有 Exec 文档和因子绑定原样保留；交易 enabled=false，原环境与 companion 均无执行进程。

2026-10-13 02:39 UTC 修复 `binance-cta-special-rx02` 图表历史导入器对 Manager 目标仓位发布的校验：接受合法 `publication.target`（包括零仓位及 `clear`），恢复历史回放和 K 线 B/S 标记。仅替换环境内 `scripts/cta_special_signal_history.py`，经环境本地 `start_cta_special_dashboard.sh` 重启 dashboard，PID 945585；未新增 CPU 绑定，未重启信号或执行进程。原配置 `enabled=true` 保留。

其中 fr_arb / okex-intra 的 trade_engine 与 housekeeping 上的系统服务同核,
数据面 NIC IRQ 已迁到 46/47,不再与它们抢硬中断。如在意调度抖动仍可迁入空闲隔离核。

NIC IRQ 策略(jp-meta,2026-08-16 现场):

- 一网卡一核:`ens41` → 46,`ens42` → 47;每卡 16 条 combined 队列的 `smp_affinity` 全部钉在该核。
  隔离段末尾两核专放 IRQ;systemd `pin-aws-ena-irq@ens41/ens42`。
- 目的:把硬中断/NAPI 从 housekeeping 和交易核清出去,两卡互不抢同一 IRQ 核。
- 约束:46/47 只做 IRQ,不跑 spread/trade/persist。队列数未按核裁剪,RSS 在单核上串行;
  是否够用看该核 `%soft`/ksoftirqd,而不是看队列个数。busy_poll 收包时 IRQ 核 CPU 可以很低
  (中断仍在响,包已被用户态抽走)。

L3 说明:c7i.metal-24xl 的 L3 为全芯片共享(`shared_cpu_list=0-47`),
跨核没有 L3 惩罚;"8 核一组"的分组只是部署约定。

## sg(SSH: `sg`,ip-172-31-7-123,c7a.4xlarge,apse1-az3)

CPU 布局:`0-7` housekeeping,`8-15` 隔离;AMD 实例无 SMT,全部为物理核。

2026-10-16 10:15 UTC 新增 lgq 的 `bybit_exec_trade02`（Manager 别名 `bybit-02`）。
账户监控、persist_manager、Viz、Exec Config 运行于 housekeeping 0–7；
exec-pre-trade、trade_engine、trade_signal 均保持停止，未开启交易或接入 RX02 实时推送。
复用已运行的 Bybit 行情发布器，8–15 核分配不变；两套 intra 仍全停。
Viz/Config 仅监听 `127.0.0.1:10042/18162`，通过 4191 `/exec_trade02/` 鉴权入口访问。
新 namespace 和持久化目录独立；账户请求沿用 arb02 下单 ENI `172.31.7.124`，
未调整 ENI、EIP、路由或旧环境配置。RX02 执行模板、10项绑定、份额与风控已复制，
初始目标全部为零。证据：`artifacts/sg_bybit02_exec_deploy/20261016T100107Z/validation.json`。

2026-10-16 新部署独立 CTA Manager（`crypto-cta-manager-web.service`）与本机
PostgreSQL 16，均限制 CPU 0–7；API 仅 `127.0.0.1:18201`，数据库仅
`127.0.0.1:5432`，复用 4191 网关 `/manager/`。初次部署时 Bybit/Bitget Exec 仅预留禁用
source；当前 Bybit Exec 状态见上文，Bitget 仍禁用。Bybit intra01/02 继续全停。公共数据出口预设
行情 ENI `172.31.10.217`，私有请求仍使用账户首个交易 IP，未新增源 IP 分配。
主机调优记录见 `sg_hfq_low_latency_tuning_20260816.md`。
NIC IRQ:一卡一核(`enp39s0`→10,`enp40s0`→11),`pin-aws-ena-irq@enp39s0/enp40s0`;
`busy_poll`/`busy_read` 与 jp 对齐为 200/50(2026-08-17)。

| 核 | 进程 | 备注 |
|----|------|------|
| 8 | spread_pbs bybit-both(market 角色) | trade/incremental/derivatives |
| 9 | spread_pbs bybit-both(bookticker 角色) | BBO 专核 |
| 10 | NIC IRQ: enp39s0 下单网卡 | 禁止再绑用户进程 |
| 11 | NIC IRQ: enp40s0 行情网卡 | 禁止再绑用户进程 |
| 12 | account_monitor_bybit(bybit-intra-arb01) | 2026-10-16 已停止，保留配置 |
| 13 | trade_signal(bybit-intra-arb01) | 2026-10-16 已停止，保留配置；start-intra 默认不拉起 |
| 14 | pre_trade(bybit-intra-arb01) | 2026-10-16 已停止，保留配置 |
| 15 | trade_engine(bybit-intra-arb01) | 2026-10-16 已停止，保留配置；单线程；`TRADE_ENGINE_IPC_CORE` 已废弃 |

未绑核、跑在 housekeeping 0-7 的热路径进程(截至本次盘点):
mm_bybit_alpha 全套(trade_engine/trade_signal/pre_trade/account_monitor/persist_manager)、
depth_pub、若干 persist_manager。bybit-intra-arb02 原有全套进程使用 0-7，2026-10-16 已全部停止。
这些与 housekeeping/系统中断同核竞争;隔离段 IRQ 已迁到 10/11。

2026-10-16 07:43 UTC 按操作者要求，`bybit-intra-arb01`、`bybit-intra-arb02`
已停止全部 7 类环境进程（包括 trade_signal、Config、Viz、persist_manager），
通过环境本地脚本停止并核对 `/proc`，两账户现货全部订单类型和 USDT linear 挂单均为零。
从本地已同步 `origin/arbmm` 的工作区构建、校验并原子发布 6 个运行二进制及配套脚本，
release ID 为 `b68065b7cb50bef48e111f341ae1551cfea4982336974537716e3dd87fc8c314`。
本次构建包含既有未提交改动；来源与校验记录见证据目录。发布后保持全停，未做在线启动测试。
env.sh、交易配置和原核心分配保留，两环境的 `local_ips` 仍为
`["172.31.7.124", "172.31.7.124"]`，没有重新分配 CPU、源 IP、ENI 或路由。
18 笔人工对冲单全部成交；最终单币最大残余敞口分别约 4.98/4.13 USDT，
其余为最小下单限制内的尾差，借币利息仍可使后续敞口变化。
SG 备份：`/home/ubuntu/bybit_intra_redeploy_backup_20261016/<env>/runtime-before.tar`。
证据：`artifacts/bybit_intra_redeploy/20261016T0741Z/`。

2026-10-11 07:07 UTC trade05 执行完成状态修复：仅更新 exec-pre-trade（PID 3477503）和 Viz（PID 3477383），通过环境脚本重启；其余执行进程和 RX02 四个信号进程保持运行，RX03 停盘。CPU 仍为 housekeeping 0-5；修复活动批次未成交时被最小下单余量永久挂起的问题。1,243 项单元测试通过；线上原先 8 项未结束状态已恢复，全部 15 项后端记录完成、待下单量及剩余批次数归零。证据：`artifacts/trade05_completion_live_validation.json`。
