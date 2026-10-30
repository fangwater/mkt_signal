# 隔离核心分配登记(jp-meta-elvpn / sg)

最后更新: 2026-10-30 10:24 UTC。**部署、迁移、下线任何绑核进程时,请同步更新本表。**
source IP / `local_ips` 变更同步更新 `docs/jp-meta-elvpn_ip_binding.md`。

2026-10-30 10:24 UTC，新RX01 Perp 15min按操作员清单进入观察预热：38币、89因子、
145个计算组合、146条独立仓位规则，SKL/factor_170的NQ关闭和0.50分别维护。
经确认排除ICX/STORJ八条；原始清单保留。源提交`31a7e358`，本地release上传校验
SHA-256并原子安装；CPU17 publisher PID3646786，Config PID3647069（19185）、
Dashboard PID3648167（10195）、signal PID3648834；`enabled=false`，未向Manager发布目标。
完整BTC/ETH Kafka验收各194根15m bar，23/23深度因子于第162根全部就绪，末bar10:00 UTC。
6币完整2026-09 Tardis成交补算P50/P90阈值，用于2026-10；合约金额表43→49，
已有值、现货表及公共factor-plan均保持。Exec06仅发布当前程序，进程仍停止；
账户永续、借贷及普通/算法挂单为零，55笔现货尾差约3.58 USDT，全部数量向下取整为零。
旧Exec Redis账本仍待归档重置，不能恢复为新策略仓位；旧Spread归档保持。
本次安装57个受保护进程保持；source IP、ENI/EIP和路由未改。
证据：`artifacts/rx01_new_factor_readiness_20261030/`。

2026-10-30 09:41 UTC，按操作员确认修正RX01两个净流量因子分母为
`abs(bid_net)+abs(ask_net)`，源提交`dc8d82f6`。本地release构建后仅更新
`~/binance-cta-perp-rx01/cta_perp_factor_model_15m_pub`及说明，SHA-256核验并原子替换。
7项Rust及3项Python针对性测试通过；195根合成回放中两项按原96/67、192/96门槛就绪。
本次未重复完整Kafka回放，下方09:05的22/23结果属于修复前版本。
57个受保护进程保持，env、空清单、`enabled=false`及关闭端口保持；CPU/IP分配不变。
空universe启动保护通过，未开启常驻进程或交易。证据：`artifacts/rx01_depth_net_fix_20261030/`。

2026-10-30 08:46 UTC，更新 `~/binance-cta-perp-rx01` 的15min待清单框架，
本地arbmm release产物及因子校验文件经SHA-256核验后原子安装，新增23个
`depth_*` 因子，Kafka消息直接复用`depth_pub::OrderBook`维护全部收到的档位。
源提交`36ce1491`，边界精度修正`6f49c3e9`。操作员的env、空universe、空币种因子表和交易配置哈希保持，
`enabled=false`、无Manager绑定、四个常驻服务均未启动，10195/19185继续关闭。
CPU17仅用于BTC/ETH独立`--replay-only`验收，不创建IPC发布器或交易信号；
正式CPU/IP分配不变，未改动source IP、ENI、路由或其他RX/Exec。
BTC/ETH各195根15min回放追至09:00，22/23项就绪；`depth_flow_net`按原公式因有效样本不足未就绪。
验收时57个受保护进程PID和启动时间保持，临时上传目录已清理。证据：`artifacts/rx01_depth_factors_20261030/`。

2026-10-30 03:47 UTC，旧 `binance-cta-spread-rx01` 已按要求退役：环境本地脚本停止
signal/publisher/dashboard/config，残留三个常驻进程及 supervisor 登记已移除，
10186/19174 监听关闭，旧别名和 Spread 网页入口均返回410，Manager旧配置链接移除。
原环境及历史整体同盘重命名至 `/home/ubuntu/retired_data/rx01_spread_20261030T033029Z/binance-cta-spread-rx01`，
364个文件的inode、大小及mtime逐项保持，历史数据未删除。
共享 Binance 执行 Vol 已迁入 `~/exec_volatility_binance`，
`exec_volatility_pub` PID2865016固定CPU20，保留原两条IPC服务和算法；
先旁路预热及BTC/ETH两市场数值核对，再交接发布权。
80个受保护交易/行情进程的PID及启动时间保持。
新 `~/binance-cta-perp-rx01` 已准备独立15min框架，空币种/因子/逐组NQ清单、
`enabled=false`、无Manager绑定，四个进程均未启动；CPU17及loopback10195/19185预留。
未改动ENI/EIP、source IP或路由。证据：`artifacts/rx01_spread_retirement_20261030/`。

2026-10-29 07:34 UTC，按补充要求将已退役的Bybit CTA/MM从管理移除。
SG CTA配置删除`bybit_exec_trade01`，残留10条零份额绑定及账户权限移除；
JP CTA删除`sg-cta`汇总provider、停用对应Exec与MM外部目录项。
NAV停用`bybit_mm_alpha`和旧CTA登记`bybit-intra-arb02`（别名`bybit cta`），
关闭自动对齐；重载两地主机CTA及JP NAV管理服务使采集范围生效。
NAV其他登记逐项保持，`bybit_fr_arb01`仍启用；SG原14个FR/共享行情PID保持。
CPU和交易IP分配不变，原历史schema、不可变快照及订单归档保留。
两地主机备份均位于
`~/crypto_cta_manager/maintenance-backups/bybit_management_remove_20261029T073240Z/`。

2026-10-29 07:13–07:24 UTC，按要求退役 SG `bybit_exec_trade01`（CTA）及
`bybit_mm_alpha`。通过环境本地脚本停止两套下单链路，撤销MM的7笔挂单；
CTA 10个、MM 3个永续仓位以13笔reduce-only市价单全部平仓，交易所核验
分别70/11条成交均为taker，逐单累计成交量等于请求量。最终现货、永续、
各类挂单、借款及利息均为0，仅保留USDT余额19750.40950714/21624.6611539。
两套账户监控、持久化、Viz及Config全部停止，原部署目录删除；专用CTA
`spp_bb_fu_bybit_exec_trade01`（原PID3818626、CPU6）同时停止，CPU6不再占用。
Manager已解除virtual01跟随，10条历史绑定份额归零，source/overview export禁用，
历史目录指向归档库；Manager服务恢复正常运行。MM页面返回410，6352同步监听移除。
两份订单tar.gz含原RocksDB及清仓回执，逐文件SHA-256与原数据核验一致，
SG归档目录 `~/retired_data/bybit_cta_mm_20261029T071314Z/`；本地副本位于
`data/order_archives/bybit_cta_mm_20261029T071314Z/`，整包SHA-256复核通过。
FR与共享spread/depth/TFF/rolling共14个PID保持，CPU8–15、ENI/EIP/路由不变。

2026-10-29 07:12 UTC 按要求停止 JP `okex_mm_alpha`：通过环境本地脚本先停
trade_engine，撤销其停止后剩余的53笔OKX SWAP挂单并复核为0，再停止signal、
pre_trade、account_monitor、Config、persist_manager及Viz；7个进程全部退出，
10212/18112/50055监听关闭。CPU20–23已无该环境进程，CPU15不再运行其
persist_manager；原绑核配置保留。另89个已记录的交易及行情进程PID未变。
停机时只读查询有14个币种的永续持仓，当时未平仓；后续已按要求清仓，见下文。
部署目录、配置、原订单库及
源IP预留保留，订单归档及停机核验目录为
JP `~/retired_data/okex_mm_alpha_20261029T071005Z/`。
07:15 UTC 完成 `okex_mm_alpha_orders_rocksdb.tar.zst` 归档：664个文件，原始
9,440,184,779字节，压缩后1,507,597,216字节；完整文件清单、大小和逐文件
SHA-256均与原库一致，归档期间原库未变化。归档仅含订单RocksDB，不含配置或
凭据；归档、清单及核验记录权限0600。详情及归档SHA-256见IP绑定文档。
07:25 UTC 按追加要求完成全部仓位清理：14笔SWAP市价平仓单全部成交，18笔
成交逐币数量与原持仓一致；ETH/DOGE借款偿清，8个币种的现货尾差全部兑换为
USDT（含还款后新增尾差）。最终交易账户仅USDT，全部品种持仓、普通挂单、
止盈止损/触发/移动止损挂单及借款均为0。交易进程始终停止，CPU/IP及配置未变。
清仓成交、还款、兑换及复核记录已补充至同目录
`okex_mm_alpha_flatten_evidence.tar.zst`（10,557字节、10文件、逐文件校验通过）；
原订单RocksDB归档保持不变，清仓后的交易所事实另存于该补充归档。

2026-10-29 02:21 UTC 按要求恢复 RX03 / `binance_exec_trade07`：先以关闭发布
启动 signal（PID4181089），核对45/45组合就绪，再补齐Manager的23个新因子，
保留2个退出范围的旧因子及原策略名用于显式`clear`，共28份绑定。
保留现场每组合200 USDT、`lgq_grid_prep`、shares1和virtual04跟随倍率1；
启用发布并核对旧28项组合均为clear后，通过环境本地脚本启动pre-trade
（PID4183206）。因子publisher PID2307925仍在CPU19；原Exec其他5个进程、
Config/dashboard、Manager、交易IP与隔离核配置均未改。
恢复前BTC/ETH/SOL/ADA/UNI/XLM有配置外持仓，均已获得零目标，执行按交易所
最小下单量处理尾差。备份及验证在RX03 `run/resume_20261029/`，本地证据
`artifacts/rx03_resume_20261029/`。
02:25 UTC复核45组合就绪、Manager无待发布、账户恢复就绪且持久化队列为零；
ADA已清零，BTC/SOL/UNI尚有grid退出挂单，ETH/XLM尾差受最小下单限制，
不能将显式clear发布完成记作账户已经全部清零。

2026-10-28 RX03 `binance-cta-perp-rx03` 按要求暂停交易后更新标的×因子NQ：
signal及Exec07 pre-trade停止、发布关闭；撤销10笔遗留挂单，复核普通/算法挂单
均为零，保留仓位。Config Server PID2288047；因子publisher PID2307925，
仍绑CPU19，16币/26因子已追到实时，45项组合保留线上300 USDT和全局开平参数。
10:15 UTC核对43/45因子组合及16/16 NQ就绪；ALGO/ZRX的buy_avg_price仍需预热。
signal二进制已安装但不启动。只读dashboard同步加载新JSON组合并重启为
PID2326926，4191接口核对16币/45组合且发布关闭。Exec07 trade_engine、账户
监控、持久化、Viz和Config保持原进程；未改Manager绑定、CPU和交易IP。
备份在环境 `run/nq_backup_20261028T094509Z`，证据 `artifacts/rx03_nq_20261028/`。

2026-10-28 按操作员要求退役 JP RX01 / `binance_exec_trade06`：Spread signal
已停止并关闭发布，Exec pre-trade、trade_engine、账户监控、持久化、Viz 和 Config
均已停止；专用 Spread 因子发布器及页面辅助进程保留。以 RapidX 市价单清仓，
98 笔订单对应 416 条 taker 成交，最终永续、负债、普通及条件挂单均为零；
55 种现货碎额均小于数量步长，合计约 3.80 USDT，USDT 余额 19007.33043254。
Manager 已解除跟随、停止并删除 65 条绑定，主机配置已移除该来源。最终订单库、
账户事件和清仓成交归档及校验文件在
`~/crypto_cta_manager/archives/rx01_retire_20261028/`；原数据目录已移至其中。

2026-10-27，新增 JP `gate_fr_arb04` / `gate_fr_arb05`，分别对应
`GateAM36_LTP` / `GateAM05_LTP`。由本机已同步的 `arbmm` `774352f5`
工作树（含部署时已有本地改动）构建七个 release 程序，SCP 上传、SHA-256 校验后
原子安装。仅启动 arb04 的只读 `account_monitor`（PID3060643，CPU mask
`0-5,48-95`），未新增隔离核绑定；`PERSIST_MANAGER_CORE=15` 仅为停用程序的
预配置。两环境的 trade_engine、pre_trade、trade_signal、persist_manager、
Viz、Config 和 FR dashboard 均未启动，Redis 名单前缀为空。
NAV 按上述 SubAccount ID 登记并重载，公共 `st_ms=1793091610625`
（09:00:10.625 UTC）；arb04 实时权益采集及四类历史首轮同步通过。
arb05 的统一账户切换 API 被 Gate 拒绝：余额必须大于 500 USDT；仍为 classic，
NAV 登记实际模式并显示 unavailable，未启动账户监控或调拨资金。
证据：`artifacts/gate_fr_arb04_05_deploy_20261027/`。

2026-10-26 04:03–04:22 UTC，按用户明确的“仅新表完全没有的币直接平仓”规则，
更新 RX01 Spread signal：新表内17币保持原目标及退出对冲，完全移除的币持续发布
现货/永续 `qty="clear", signal=1`，不受普通金额容差影响。本地同步arbmm构建release，
上传校验SHA-256后原子安装，signal PID2879622 → PID3267446；publisher PID3173585
仍CPU17，Config PID2879569、Dashboard PID3207816未重启。初次历史批量退出触发
RapidX 429后，通过环境本地脚本暂停并恢复Exec06 pre_trade，PID502412 → PID3289558；
其他五个Exec进程保持。signal与pre_trade全部线程仍`0-5,48-95`，ENI/source IP/监听保持。
344个退场组合688个市场目标及inventory的76个市场目标明确清仓；通过已有内部对敲
记账承接剩余旧仓，再按币逐项ctaker退出，未清零账本或重置历史快照。
新表内456个历史退出对冲组合数量保持（仅JSON浮点末位差）。普通lgq_grid_spread模板、
17币/55组合、单组合10 USDT、entry/exit/NQ、env.sh及跟随倍率均保持。
04:22核验退场因子仓位为0，相关挂单/pending为0；旧币绝对仓位市值约2519.62 →
34.06 USDT，剩47个市场零头均低于最小下单门槛，最大约4.80 USDT；不声明账户完全归零。
新清单IOST借币不足差额仍保留。1710项信号目标链路一致，交易所/Exec账户数量一致。
36项Spread Rust测试通过。证据：`artifacts/cta_spread_rx01_factor_update_20261026/coin_retirement_result.json`；
远端回执：`~/binance-cta-spread-rx01/run/factor_update_20261026/coin_retirement/`。

2026-10-26 03:42:36 UTC，RX01 已切换新清单：17币、7因子、55组合；
按用户确认排除现货和合约规则均缺失的ICX。复用本地与远端SHA-256一致的既有
release publisher（`693b9277…43f0d023`），通过环境本地脚本旁路回放后交接，
PID3973994 → PID3173585，全部线程仍CPU17，进程名`cta_spread_factor_rx01_20261026`。
signal PID2879622、Config PID2879569、Dashboard PID3207816及Exec06六个进程
未重启；三个信号服务仍为`0-5,48-95`。enabled=true、单组合10 USDT、现有
entry/exit/NQ、lgq_grid_spread及virtual03→Exec06倍率1保持；env.sh仅更新publisher进程名。
55/55组合因子、spread和NQ就绪，解码/计算/发布错误均0；133个移除组合的内部
方向状态归零，对冲库存保留。1710项目标在signal/Manager/Redis/Exec间一致，待投递0。
03:46核验新清单110个市场目标中109个在原10 USDT容差内；baseline_140/IOST现货
仍因借币不足差约19.82 USDT，不能声明全部实仓到位。另有20项历史对冲库存目标
超容差（借币不足、最小下单限制及仍在执行的旧目标）。交易所110项余额/仓位与Exec
账户快照一致；切换后已观察到20笔成交记录。source IP、ENI、路由和监听保持。
证据：`artifacts/cta_spread_rx01_factor_update_20261026/deployment_result.json`；
远端备份与回执：`~/binance-cta-spread-rx01/run/factor_update_20261026/`。

2026-10-23 05:57 UTC，RX04 5min 接入原 RX02 的05号页面/端口槽位并开启交易。
新账户使用独立环境/source `binance_exec_rx04_5m05`，别名 `RX04 5min`；
`virtual05` 有35项因子绑定、覆盖15币/48组合，实账户跟随倍率1，采用lgq_grid_prep。
RX04策略 `enabled=true`、单份100 USDT；开启前只重启仓位signal按既有历史重算。
本地arbmm `f9424e8d` release构建后，37份程序/脚本校验SHA-256并原子发布。
新Exec六个进程在线：persist_manager 2011312、trade_engine 2011593、
rapidx_account_monitor 2011864、exec-pre-trade 2012013、viz_server 2012588、
config_server 2012847；各进程全部线程CPU mask均为 `0-5,48-95`，无专用绑核。
Manager PID2010667全部线程仍为CPU24；RX04 signal PID2010605、配置620503、
dashboard 620577全部线程为 `0-5,48-95`；publisher PID705737全部线程保持CPU28。
89个其他受保护进程身份及新版RX02/Exec08绑定保持；仅RX04策略配置开关改变，
其余受保护环境配置哈希保持。
旧Exec05仍停用并保留全部历史。证据：`artifacts/rx04_5min_slot05_20261023/`。

2026-10-22 15:00–15:06 UTC，Exec08已接替JP RX02的执行：virtual02倍率1、
`lgq_cta_symbol_mean` / `lgq_grid_prep`，23币、单币50 USDT。
经用户授权API开启USD-M多资产模式，单向持仓保持。补齐STANDARD USD-M Grid兼容，
本地release构建、校验后原子发布；Exec08 engine PID342673、executor PID343474、
Config PID315020，全部CPU0–5；原account_monitor/persist/Viz未重启。
Manager PID315038仍CPU24；RX02 signal PID351338更新新账户页面关联，publisher
PID160373保持CPU18。5个非零仓位在原容差内完成，8笔成交均Maker，挂单/待投递均0。
其他受保护环境进程身份未变。旧trade05继续停止。详见
`docs/cta_perp_rx02_cutover.md`及`artifacts/rx02_exec08_grid_20261022/verification.json`。

2026-10-22 13:57–13:59 UTC，按用户明确要求以 taker 清理旧 RX02：
JP `binance_exec_trade05` 的24个非零永续仓位通过 RapidX MARKET / reduceOnly
全部成交归零（含FIL/XRP/LINK小额尾仓）；普通挂单、条件单及未决平仓单均0。
旧交易进程和Manager source继续停止/禁用，未启动旧策略。SG只读Exec快照确认
旧仓位归零，现有非零仓位均属于新版 `lgq_cta_symbol_mean`，未手动干预新版持仓。
同次将新账户 `lgq_ltp_81857` / trade08 的20000 USDT从全仓杠杆钱包直接划至
USD-M合约钱包（`MARGIN_UMFUTURE`）。单次POST回执tranId `423596580418`，
划转历史CONFIRMED；13:59复核合约wallet/equity/available均20000 USDT，
Margin及Spot均0、借款/锁定均0。trade08仍仅观察进程，无策略绑定、未启动交易。
无CPU、local_ips、ENI/EIP、路由或监听变更。清仓和划转证据分别在
`artifacts/rx02_old_positions_cleanup_20261022/`、`artifacts/lgq_ltp_wallet_transfer_20261022/`。


2026-10-22 13:43:58 UTC，RX02 已切换新版仓位推送，JP、SG 双 Manager 均接收：
23币、22因子、69组合，`lgq_cta_symbol_mean`，保留 v1 算法、单币50 USDT和现场
entry/exit/NQ参数。当前68/69组合、22/23币就绪；RLC/factor_114仍需积累样本，
RLC目标为clear，未降低窗口或跳过未就绪因子。13:41快照还缺至少122个有效分钟；
实际时间受后续数据连续性影响。
JP virtual02 无执行跟随；SG virtual01 仍由 bybit_exec_trade01 以倍率1跟随。
两边9个旧虚拟策略绑定移除，旧策略及移出币种目标clear。SG账户保留9条份额0的
历史记录，唯一有效绑定为symbol_mean，待投递0。JP已停用trade05残留的10条
账户绑定已备份后移除，修复其导致的发布502；该source继续禁用，旧真实仓位未操作。
旧Perp运行状态归档在 `~/binance-cta-perp-rx02/run/push_switch_20261022/before/`；
旧Special、Exec05继续停机，09:19完整归档保留。trade08无策略绑定，未启动交易。
publisher PID160373固定CPU18，signal PID177222、Config PID168550、Dashboard
PID168607使用housekeeping CPU；保留loopback19186/10196、已有4191入口和默认出口。
本地arbmm release上传、SHA-256核验后原子安装；双Manager目标逐项匹配，发布无错误。
证据：`artifacts/cta_perp_rx02_push_switch_20261022/verification.json`、`sg_verification.json`。

2026-10-22 13:23–13:28 UTC，`binance_exec_trade08` / `lgq_ltp_81857` 替换用户
提供的新 Key。新 Key 的 FAPI/Spot 查询成功，PAPI 不授权；本地读取配置更新为
STANDARD，未通过 API 改动账户模式或划转资金。账户监控 PID157817、Config
PID162464 经环境本地脚本重启；持久化/Viz 保持，均使用 housekeeping CPU。
标准账户快照和 UM WS 轮询正常，USD-M 持仓/挂单均0，交易进程仍停止、Manager
无绑定。凭据仅保存在远端0600的私有文件。证据：
`artifacts/lgq_ltp_key_rotation_20261022/verification.json`。

2026-10-22 10:18 UTC，RX01 已切换用户新因子清单：18币、15因子、153组合；
ICX、STORJ 因当前账户不可交易排除（原清单20币/169组合，减少16组合）。
15个因子均不依赖大小单；补齐6个非大小单因子的免阈值名单，缺少成交大小阈值
不再误阻塞。完整1分钟数据测试需2439–2499根（40小时39分–41小时39分），
两天足够；现场保持3天回溯配置，Kafka追平后153/153组合因子、spread和NQ均就绪。
publisher已本地release构建、SHA-256核验、原子安装并通过环境本地脚本交接，
PID330224 → PID3973994，CPU17，进程名 `cta_spread_factor_rx01_20261022`。
signal/config/dashboard进程未重启；全市场5秒Vol继续发布，核验合约524/525、
现货362/363就绪。现场enabled=true、单组合10 USDT、entry/exit阈值均保留。
Manager新增6个策略绑定，继承现场 `lgq_grid_spread`、shares=1、virtual03→Exec06
倍率1；现有绑定和对冲库存保留。702个移除组合的内部方向状态已归零；当前所有策略目标
与Manager核对一致，publication_error为空，待投递0。未改source IP、ENI或路由。
19项Rust及22项Python定向检查通过。证据：
`artifacts/cta_spread_rx01_factor_update_20261022/verification.json`；远端备份及回执：
`~/binance-cta-spread-rx01/run/factor_update_20261022/`。

2026-10-22 09:49 UTC，按用户要求通过 Binance PAPI 将 trade08 的全部 USD-M
合约改为单向持仓，POST 成功并回读 `dualSidePosition=false`。交易进程仍停止，
4个观察进程及 CPU 绑定保持。账户仍为 PM/UNIFIED；公开 API 文档未找到 PM
退出至经典模式的接口，普通合约权限 `enableFutures=false`。经典模式切换及后续
划转等待账户后台处理；未提交任何资金划转。09:51 UTC 核验 USDT 为 PM 可用20000、
Spot/UM/CM 钱包均0，无锁定或借款。回执：
`data/ops/exec_trade08_rx02_retire_20261022/one_way_change.json`、`classic_pending.json`。

2026-10-22，按用户要求停止并归档 RX02，不迁移策略：
`binance-cta-perp-rx02`、`binance-cta-special-rx02`、`binance_exec_trade05`
均无运行进程；通过环境本地脚本停机，释放原 RX02 factor 的 CPU18。
Manager 禁用 trade05，解除其虚拟账户跟随，保留旧绑定及订单历史。
完整运行目录归档至 JP `~/retired_data/rx02_20261022T091913Z/rx02_runtime.tar.zst`，
SHA-256 和 tar 内容比对通过；原目录保留并添加 `ARCHIVED.json`。
归档含私密运行配置，权限0600，只留在远端。停机后只读查询：普通/条件挂单均0，
仍有24个非零永续持仓；未平仓、未转移仓位。

新增 `binance_exec_trade08` / `lgq_ltp_81857`，Binance 原生 USD-M、Portfolio Margin。
从本机 `/home/fanghaizhou/mkt_signal` 已同步的 `arbmm` 提交 `d3aedea0` 构建 release，
经 scp、逐文件 SHA-256 校验后原子安装，JP 未运行 Cargo。
仅通过环境本地脚本启动 persist_manager PID3919545、account_monitor PID3919714、
Viz PID3919789、Config PID3919881；有效 CPU 均0–5（mask含已下线48–95）。
trade_engine、exec-pre-trade、trade_signal 均停止，Manager 中策略绑定/跟随均0；
账户查询正常，新账户 USD-M 持仓和挂单均0。Manager 仍固定 CPU24，RX01/RX03
受保护进程保持。Viz/Config 为 loopback10048/18168，网关 `/exec_trade08/`，
`lgq` 已获配置权限。库测试1351项、rolling_metrics 8项、Viz 6项通过，库测试3项忽略。
本机构建/发布/验收证据：`data/ops/exec_trade08_rx02_retire_20261022/`。

2026-10-21 09:51 UTC，仅更新RX04的5min publisher，代码 `5a0c8f32`：
TP_VPI_006的内层360根滚动和改为至少280个有效值，外层14根均值不变；
z仍288/200、分位仍576/288。Pandas数值对照及1351项Rust测试通过。
本地release上传并核验SHA-256后原子替换，环境本地脚本重启publisher：
PID618685→705737，全部线程仍CPU28。正在重新回放Kafka预热；已有signal、
config、dashboard未重启，页面可访问，仍为enabled=false。62个受保护进程和
RX02/RX03配置哈希保持。回滚二进制保存在RX04的 `run/tp280_before/`。

2026-10-21 09:11 UTC，新增 `binance-cta-perp-rx04` 的 5min 观察环境。
Binance 永续共15币、35因子、48个有符号组合，单份200 USDT，`enabled=false`；
独立 namespace/model scope `binance_cta_perp_rx04`，未绑定 Manager/Exec。
本地 `arbmm` release `4f782415` 构建上传并逐文件 SHA-256 校验后原子安装，
仓位表前端追加 `1d0ce049`。通过环境本地脚本启动4个进程：publisher PID618685
全部线程CPU28；signal PID620641、config PID620503、dashboard PID620577
使用housekeeping（有效CPU0–5）。loopback页面端口10198/19188，4191网关路径
`/cta/binance-cta-perp-rx04/`。62个既有进程及RX02/RX03的6份配置哈希保持。
Kafka请求回放5天，实际保留数据从2026-10-19 08:30 UTC开始；长窗口因子需继续
积累有效样本，未缩短公式/z/分位窗口。部署和浏览器验收证据：
`artifacts/cta_perp_rx04_deployment/`。

2026-10-21 08:14–08:15 UTC，修复 RX01 大仓位状态消息超出 64 KiB 而停更的问题。
仅配套替换 `binance_exec_trade06` 的 executor/Viz：PID384533→502412、
3552394→502880，均仍 CPU0–5。状态通道现为1 MiB，保留2条历史、8条订阅缓存；
连续快照恢复约1,500行、延迟低于1秒。另134个受保护进程和118份配置哈希保留，
Manager仍CPU24、共享Vol publisher仍CPU17。模板已拆分为RX01使用
`lgq_grid_spread`、RX02/RX03使用`lgq_grid_prep`，份数和跟随倍率不变。
测试、构建与验收见 `docs/exec_grid.md`。

2026-10-21 07:20 UTC，全symbol Vol 与 Grid 布局上线，release
`20261021T064822ZGridAllSymbols`（Exec `d3b724af` / 前端 `82f58cb`）。新publisher
后台回放时原实例持续服务；交接后仅保留 `cta_spread_factor_binance_futures`，
PID330224，全部线程CPU17/nice0。预热先临时使用CPU0，后以nice19使用CPU17空闲时间，
交接恢复nice0。Exec05/06/07 PID384442/384533/384621，均保持CPU0–5。
Manager PID166260/CPU24、Config/Viz/trade_engine未重启，156个受保护进程身份不变。
当前合约524/524、现货362/362币Vol新鲜就绪，RX01/02/03所有目标覆盖，算法仍为CTaker。
53份配置哈希不变；factor env.sh仅增加当前进程名。验收与清理见 `docs/exec_grid.md`。

2026-10-21 05:33 UTC，发布 Grid 分市场倍数及可选 Maker 轮数，Exec 源 `bf9d0ad8`、
Manager 源 `d5c3cc1`，release `20261021T053158ZGridControls`。仅通过环境本地脚本
更新 Exec05/06/07 执行器与 Config，以及 RX01 共享因子 publisher；Manager 通过原
systemd unit 重启。Exec PID 为163684/165922/166169（CPU0–5），publisher PID166240
（CPU17），Manager PID166260（CPU24）。另152个常驻进程和54份配置哈希保留，
87个实账户、77个虚拟账户绑定和四张仓位快照表不变；当前算法未切换到Grid。
05:53 UTC 完成原配置Kafka回放，恢复Spread/Vol输出；两市场各48/48币就绪，
三个Exec均已订阅两路Vol。最终验收见 `docs/exec_grid.md`。

2026-10-19 13:10 UTC，收窄Chase新目标的退避重置条件，仅解除借币不足状态；
保留其他拒单的原退避。经本机release重建与SHA-256核验，只补发Exec06执行器，
PID3552480→3581326，CPU mask仍为 `0-5`，Viz保持3552394。当前38个已激活永续
完成核验：36个4倍、BEL/ZIL上限3倍，BAND4倍。仓位推送恢复原配置，借币不足11个
目标已完成且无429。最终执行器SHA-256：
`e1b3824b0bc7a7ef4c2620f3f10ab6ca076b050698928d6f4ca360bdce4e9777`。
最终回执：`~/binance_exec_trade06/run/borrow_stop_20261019T130500Z/`。

2026-10-19 12:54–12:55 UTC，发布 RX01 `binance_exec_trade06` 的 RapidX 借币不足
终止执行修复及默认4倍杠杆。仅通过环境本地脚本更换 exec-pre-trade PID3224367
→3552480、Viz PID3199538→3552394，两者实测 CPU mask 仍为 `0-5`。
构建源为本机已同步的 arbmm `b4251236` 加当前补丁，上传后逐文件校验 SHA-256 并
原子安装；短暂停止仓位推送，核验账户快照和杠杆后恢复原配置。
36个已激活永续已实际回读确认：34个4倍，BEL/ZIL因场内上限3倍；BAND为4倍。
启动后11个借币不足目标已结束，active_batches/live_order_qty/pending_qty均0，无429。
其余Exec组件、RX01 signal/config/dashboard及CPU17 publisher保留原PID和绑核。
部署备份与回执：`~/binance_exec_trade06/run/borrow_stop_20261019T125000Z/`；
本地证据：`artifacts/rapidx_borrow_stop_20261019/`。下方10:02–10:20记录为快照切换历史。

2026-10-19 10:02–10:20 UTC，RX01 Spread / Exec06 按用户要求先 taker 清仓，
再建立新账户及策略快照，并于10:17:05 UTC恢复交易。清仓后永续、负债和挂单均0，
约2.7593 USDT的20种现货零头保留在inventory；新快照时间戳
`1792404818623482`（10:13:38 UTC），历史订单及旧快照保留。
57个因子策略从零仓启用，旧factor_052绑定份额归零；当前线上单组合10 USDT、
退出long=0.3/short=0.7的现场设置保留。773/794组合因子就绪，21个缺大小阈值的组合留空。
publisher PID2798755/CPU17、signal PID2879622及配置服务PID2879569保持；
新增dashboard PID3207816。Exec06 persist_manager/account_monitor/Viz/Config/
trade_engine/exec-pre-trade分别PID3199097/3199439/3199538/3199632/3207923/3224367，
全部实测CPU mask `0-5`；未启动Exec trade_signal。1632个Manager→Redis目标一致，
Manager健康、待投递0，已看到恢复后的新成交。原二进制、env.sh、source IP和绑核配置保持。
远端备份与回执：`~/binance-cta-spread-rx01/run/reenable_20261019/`；
本地核验：`artifacts/rx01_reenable_20261019/verification.json`。

2026-10-19 07:26–07:30 UTC，仅修正并发布 Spread 退出默认值：long=0.7、short=0.3。
Rust、配置服务、页面说明及本仓部署副本一致；原始ds研究文件保留。通过环境本地脚本
重载 RX01 signal PID2879622、配置服务 PID2879569，CPU mask均仍为 `0-5,48-95`。
publisher PID2798755/CPU17及Kafka generation保持，794条内部仓位仍为0，
发布关闭、Exec06和dashboard停机。RX02/RX03 Perp配置及94个受保护进程未改。
本地release signal SHA-256为 `70f529dbada7969b345565338bb08b5d8e5ac24b11ef481e380a05130b77b52a`；
43项Rust、18项Python测试通过，配置页、健康检查及48币/794组合导入预览通过。
备份及回执：`~/binance-cta-spread-rx01/run/spread_exit_defaults_20261019T072100Z/`；
本地核验：`artifacts/rx01_corr50_warmup/spread_exit_defaults/validation.json`。

2026-10-19 07:10–07:12 UTC，RX01 Spread Kafka因子回溯完成并追至实时。
48币均有历史，762/794个组合因子就绪；21个因大小阈值缺失按用户要求留空，
另11个 `factor_114` 仍未就绪。07:10快照中693个组合的NQ和spread过滤同时就绪，
该数量随实时数据变化。累计解码/计算/发布错误均0，794条内部仓位仍为0，
发布关闭，Exec06及dashboard停机，其他受保护进程及账户配置保持。
07:12只读核验发现线上exit已改为long=0.7、short=0.3，保留该现场修改；
原研究JSON及本地模板仍为0.3/0.7。证据：`artifacts/rx01_corr50_warmup/final.json`
及 `final_config.json`，库级回归1298项通过、0失败、3忽略。

2026-10-19 07:03 UTC，按用户要求删除 RX01 Spread 的 ICXUSDT、STORJUSDT 配置，
减少79组合，现为48币/57组/794组合，内部状态全为零。Redis通过配置服务CAS更新，
磁盘策略表、publisher universe和部署JSON同步；原始ds研究文件保留。
为保留Kafka预热进度，publisher PID2798755/CPU17未重启，内存仍为本轮启动快照，
signal仅采用新的48币范围；磁盘48币universe在publisher下次启动时生效。
signal通过环境本地脚本恢复为PID2830224，CPU mask仍 `0-5,48-95`，发布关闭；
配置服务保持，dashboard及Exec06停机。三个Spread停止脚本的pmdaemon清理超时
从5秒改为45秒，避免冷加载尚未完成就中断删除、留下旧进程登记。
备份及回执：`~/binance-cta-spread-rx01/run/corr50_remove_symbols_20261019T065917Z/`。

2026-10-19 06:47 UTC，按用户选择为 RX01 Spread 预热因子、备份并重置旧内部状态，
从当前时点起仓；未回溯历史仓位、未向 Manager/Exec 发布目标。旧5组/46组合状态
已备份，新57组/873组合内部仓位全为0。publisher 与 signal 在本地 arbmm release
构建、SHA-256核验及原子安装后，经环境本地脚本启动：publisher PID2798755，
CPU17；signal PID2798817，CPU mask `0-5,48-95`，`enabled=false`。
配置服务 PID2708645 继续运行，dashboard、Exec06保持停机。Kafka预热进行中；
29币缺少成交大小阈值，按用户要求保持空缺，相关 `baseline_046` / `net_buy_small`
组合不就绪，其余54个底层因子独立计算。未修改共享TLen元数据、source IP或绑核配置。
备份及发布回执：`~/binance-cta-spread-rx01/run/corr50_warmup_20261019T064500Z/`；
本地核验：`artifacts/rx01_corr50_warmup/`。

2026-10-19 06:04 UTC，JP RX01 Spread 按 `ds/twoex_deploy_corr50.json`
更新为50币、57个有符号因子组（56个底层因子）、873组合，单份100 USDT，
`enabled=false`。本地 arbmm release 构建的 signal/publisher 经SHA-256核验后
原子安装，同时重构并发布配置服务及NQ/前一分钟spread双过滤；仅配置服务启动，
PID2708645，CPU mask `0-5,48-95`，loopback19174健康。signal、CPU17 publisher、
dashboard和Exec06继续停机，未新增隔离核分配。原仓位状态、env.sh及Exec06交易配置
哈希保持；新清单尚未预热或启用交易。备份及发布回执：
`~/binance-cta-spread-rx01/run/corr50_publish_20261019T055900Z/`；
本地核验：`artifacts/rx01_corr50_deploy/validation.json`。

2026-10-19 05:12 UTC，按用户要求通过 `scripts/stop-exec.sh` 调用 JP
`binance_exec_trade06` 环境本地脚本，停止 trade_engine、exec-pre-trade、
account_monitor、persist_manager、viz_server、config_server，复核无匹配进程。
Exec06 无独立隔离核绑定；RX01 Spread 保持停机，未调整 source IP 或绑核配置。
05:18 UTC RX01 保留现货及负债，通过11个币的USDT永续成交对齐净敞口。
复核普通/条件挂单均为0、工具pending为空；逐币仅剩低于数量/金额门槛的尾差，
绝对市值合计约37.53 USDT（不可用不同币抵消判断中性）。
证据：`artifacts/rx01_neutralize_20261019/verification.json`；远端交易journal保留在
`~/binance-cta-rx01/run/rapidx_neutralize.json`。恢复交易前须重新核对实际仓位与内部目标。

2026-10-19 05:09 UTC，按用户要求暂停 JP `binance-cta-spread-rx01`。
通过环境本地 `scripts/stop_cta_spread.sh --execute` 停止 signal、factor publisher、
dashboard、config server，核查该环境 cwd 下无剩余进程。CPU17 的 Spread publisher
已停止，保留原绑核配置供恢复；未操作 Exec06、账户仓位或 source IP。

2026-10-18 13:34–13:38 UTC，修复 Binance FR arb02/03/04 无新增订单的阈值依赖：
Binance rolling 的 bidask/askbid 原 min_periods=40000、1秒采样，主机重启后
预热未完成，全部分位数为null，导致 FR spread_missing_refs 覆盖全部在线币。
经用户授权，仅将这两个因子的 min_periods 改为1，rolling_window=86400、
resample_interval_ms=1000、分位点及其余参数不变；三套已重载有效阈值并产生新订单。
随后从本地 arbmm 4b35b157 构建发布 rolling_metrics，7项 release 单元测试通过；
校验SHA-256后原子替换 Binance 单套二进制，通过环境内 scripts/rolling_metrics
启停脚本恢复，PID697198，CPU mask仍为0-5,48-95。未更新 Gate/Bitget rolling，
未重启九套FR组件，未改源IP或路由。新版重建内存样本，min_periods=1已生成新阈值。
二进制、配置备份及校验记录：`~/backups/binance_rolling_upgrade_20261018/`；
门槛修改前后参数：`~/backups/binance_rolling_min_periods_20261018/`。

2026-10-18 09:06–09:10 UTC，用户授权检查并开启九套 FR 交易：
Binance arb02/03/04、Gate arb01/02/03、Bitget arb01/02/03 的 trade_signal
均通过环境本地脚本逐套启动，每套共8进程，persist_manager 全部存活；
signal CPU mask 均为 `0-5,48-95`。persist_manager 实测除 Bitget arb01
为 `0-5,48-95` 外，其余八套为15；未修改绑核、local_ips 或网络配置。
启动前已对齐可下单敞口；Binance 审计轮询一度触发共享出口 IP 6000 weight/min
限流，减速后完成余单，并恢复因限流在初始化阶段退出的 arb03 pre_trade。
最终九套账户均 free_trade，Viz/Config 正常，signal 行情缓存覆盖全部在线币；
Gate/Bitget 已观察到 pre_trade 接收开平仓信号，Binance 抽样窗口无信号触发。
Gate arb01/02 仍因非FR列表内的零碎现货 SLVON/TSLAX/CRCLX 缺合约 mark 而
跳过集中度检查，未改写风控参数；Gate 个别币仍有 rl_vol 快照未就绪告警。
九套 signal start/stop 的 PM2 delete 已改为仅按精确进程名删除，避免 namespace
连带删除基础组件，仓库脚本同步修复并通过 bash -n。
已按用户要求清理本次临时操作日志，保留订单/成交JSON和最终核验快照；
最终记录：`~/backups/fr_enable_20261018/final-state.json`、`post-enable-health.json`、
`cleanup.json`；各环境成交证据在 `~/backups/<env>_neutral_20261018/`。

2026-10-18 08:43–08:50 UTC，JP 重启后经环境本地脚本恢复
`binance_exec_trade01`–`07`，每套 persist_manager、trade_engine、账户监控、
exec-pre-trade、Viz、Config 共6进程，全部实测 CPU mask `0-5,48-95`；
7套仓位快照 position_ready=true，鉴权 Viz snapshot / Config 接口通过。
CTA Manager 已由 user systemd 自动恢复，PID2235、CPU24，health=ok、
source_count=7、last_refresh_error=null，无需重复重启。
另恢复 RX01 Spread → Exec06、RX02 Perp → Exec05、RX03 Perp → Exec07，
各自 factor/config/dashboard/signal 共4进程；factor 分别 PID106525/100190/106578、
CPU17/18/19，其他进程沿用 `0-5,48-95`。旧 special/companion 未启动。
三套 Redis 因子表缺失，使用与最后运行 status 完全一致的环境本地 JSON，经原
factor_config 校验脚本恢复（16币46组合、47币192组合、14币34组合）；
Exec06/07 缺失的风险 hash 按部署脚本及原 activation risk_reference 一致值恢复。
因子依赖 `~/tlen_config_shared/scripts/start_tlen_config_server.sh` 的6322服务，
已先恢复该服务再重启之前因依赖缺失退出的 RX01/RX03 factor。
三套因子完成原配置3天 Kafka 历史预热：RX02 于09:15:28、RX03于09:17:34、
RX01于09:18:58 UTC建立实时输出。09:19–09:20验收：RX01 46/46组合、
RX02 47/47币种、RX03 34/34组合均实时就绪；signal→Exec目标分别核对92/47/34项，
无差异、无发布错误，Manager health=ok、待投递/错误队列均0，7套Exec仓位快照
均在1秒内更新且position_ready=true。BNB自动储备另有保护告警：Exec03提示
上次操作结果未知，Exec04最新轮询因共享出口权重预算耗尽而未发请求；未解除
保护或手动重试。少量执行记录受最小下单量/不可交易状态限制。
未修改 env.sh、二进制、local_ips、ENI/EIP或路由。
远端配置/仓位备份及启动日志：
`~/backups/jp_exec_signal_recovery_20261018/`。

2026-10-18 08:28–08:37 UTC，恢复 Binance FR arb02/03/04、Gate FR arb01/02/03、
Bitget FR arb01/02/03，每套运行 account_monitor、persist_manager、trade_engine、
pre_trade、viz_server、fr_config_server、fr_signal_dashboard；**九套 trade_signal
均未启动**。启动前逐账户撤单并复查：Gate arb02 残留 IDOL_USDT 4笔、INIT_USDT
1笔已撤，其余八套没有挂单。persist_manager 仍绑定 CPU15，其余沿用各环境配置，
没有修改 local_ips/ENI/EIP/路由。九套 Nginx 页面、healthz、snapshot、Config
symbol-lists/risk-params 均200，Viz及FR WebSocket均实际收到数据。

Binance mark price 依赖 `~/binance_futures_ipc_proxy/scripts/start.sh`：必须在
NAV、FR dashboard、rolling_metrics、pre_trade 等代理消费者之前启动。消费者
先启动会创建64 subscribers/20 nodes通道，而代理要求128/128，导致代理启动
失败。本次暂停三个 Binance pre_trade/FR dashboard、Binance rolling_metrics
及 systemd crypto-nav-manager，待消费者退出后原通道自动清理，代理重建128/128
通道，再恢复消费者；没有手工删除共享IPC文件。代理PID58430持续转发，九套
pre_trade均确认收到mark price。Gate仍有冷启动价格未齐警告，08:37–08:38 UTC arb01/02/03
分别剩3/4/1项：ACH尚未收到mark，SUPER已恢复；SLVON/TSLAX/CRCLX为只有现货的资产，
不能把它们算作对应USDT合约行情故障。未将此抽样写成全资产风控就绪。

环境内 Config/FR dashboard start/stop 的 PM2 delete 已收窄到精确进程名，避免
带 `--namespace` 同时删除同环境的其他服务；保持其余环境脚本原样。
远端证据及修改前备份：`~/backups/binance_fr_recovery_20261018/`、
`~/backups/fr_recovery_20261018/`。

2026-10-18 08:24–08:27 UTC，JP 主机重启后，按原部署脚本恢复 Binance、Gate、
Bitget 公共行情及依赖，共19进程：7个 spread_pbs（含共享 depth 必需的 OKX
both）、depth_pub_general、Binance BBO ZMQ relay、4个 rolling_metrics（含
Bitget/Gate futures 配对）、6个 trade_flow_feature_pub。spread CPU 仍为
8/9/10/11/12/14/16，depth 为13，relay 为5，其余继承0-5,48-95。
共享 depth 必须等 OKX IPC 创建后启动，否则报 DoesNotExist 并退出；trade-flow
须等 depth 通道创建后启动。本次补齐依赖后，depth PID37298 持续发布八路数据。
只读巡检的八路 BTC/ETH BBO 均为 fresh；该抽样不代表全币种验收，rolling
指标仍有预热/未就绪项。未启动交易/信号栈，未替换二进制或更改源IP/路由。
远端证据：`~/backups/marketdata_recovery_20261018/`。

2026-10-15 13:59 UTC RX02/RX03 Perp 静默回放与上线验收完成。RX02 使用 v1、
17 因子/47 币/192 组合、单份300 USDT；ICXUSDT 不在可交易规则缓存中，
其3个请求组合保留在排除清单。RX03 保留 v0、5 因子/14 币/34 组合、单份100 USDT。
两套 factor/config/dashboard/signal 已更新，因子 PID3996876/4053379 仍用 CPU18/19。
共享 Manager clear 支持 release `20261015T124324Z`，PID3907213、CPU24；
RX02 的9个旧策略55条目标已明确发布 clear，旧绑定份额归零并保留历史。
用户另行授权按 RapidX 杠杆上限修复 Exec05：pre-trade PID341167、trade_engine
PID440232，均保留 CPU0-5。两组件配套后账户快照就绪；最终 Exec05 的36条、
Exec07 的16条执行记录全部完成，余差仅为交易所最小下单量限制。RX01 配置及
其他44个受保护进程身份保持，未改 local_ips/ENI/EIP/路由。证据：
`artifacts/cta_perp_replay_cutover/`。

2026-10-15 06:13 UTC 恢复 Exec04 / prc 的 BNB 储备对冲：复用已在 Exec03
验证的 `8a263e04` release，配套替换 exec-pre-trade（PID3885210）、
viz_server（PID3875816）、account_monitor（PID3869376）。三个进程仍为 CPU
mask 0-5，未新增绑核或隔离核。仓位通道改用现有 64 KiB 格式，解决约 38 KB
快照超过旧 32 KiB 上限导致停更的问题；账户监控同步匹配风控消息格式。
BNBUSDC 储备空头 -25.24 BNB 成交完成，实测净储备敞口约 +0.000489 BNB。
其他133个受保护进程、交易IP及配置保持；Manager、交易引擎、持久化、Config、
Nginx与公共行情未重启。独立 Manager 项目的
`docs/jp_meta_prc_bnb_recovery_20261015.md` 和
`artifacts/jp_meta_prc_bnb_recovery_20261015/` 记录证据，不构成运行依赖。

2026-10-15 03:47 UTC 仅替换 Exec06 / RX01 的 `rapidx_account_monitor`，
修复账单可用余额上下文的 1e-20 USDT 尾差导致恢复循环、Manager 实时权益为空。
PID3046353 → PID2309899，CPU mask 保持0-5；Manager及其余190个受保护进程
PID/start time/绑核、33份配置和4张初始仓位快照表校验保持。
账单追至实时，成交/账单持久化队列归零；lgq 桌面/手机工作台权益恢复且与API一致。
没有新增隔离核或更改交易IP。证据保存在独立 Manager 项目的
`artifacts/jp_meta_rx01_equity_20261015/`，不构成运行依赖。

2026-10-14 08:05 UTC RX03 CTA Perp 已接入独立 `binance_exec_trade07` 实盘，
Manager source `trade07 / lgq_cta_RX03` 跟随 `virtual04 / cta_perp_rx03`，
倍率1，五因子各1份，14币/34组合，每组合100 USDT。启用前账户为空仓、无挂单；
新Exec使用独立namespace、Redis键及RocksDB，没有迁移旧持仓或历史。
执行模板 `binance_cta_perp_rx03_ctaker` 复制RX02当前 `lgq_cta_ctaker`，
五份实际运行参数逐项相同；买卖各最多10个挂单，10秒200单、每分钟400单。
Maker费率-0.00002、Taker 0.000175、理论TWAP 0.0004。
34个实时目标与Exec一致，账户position_ready、发布日志健康，Manager无待发布项。
证据：`artifacts/cta_perp_rx03_activation/`。
Exec07的六个进程均使用housekeeping CPU0-5,48-95；未增加隔离核。
RX03因子publisher仍为PID2038356、5线程CPU19，signal加载新Manager配置后重启；
config/dashboard保留。现有其他环境的进程与配置核对保持。

此前观察部署记录：
2026-10-14 07:10 UTC 新增观察环境 `binance-cta-perp-rx03`：14币、5因子、34组合，
单份100 USDT，`enabled=false`。本地arbmm release构建，经34文件SHA-256校验后
原子安装目录，只通过环境本地脚本启动。因子publisher PID2038356，全部5线程
绑定CPU19；signal PID2062080、config PID2049101、dashboard PID2054156
均为CPU0-5,48-95。独立model scope为 `binance_cta_perp_rx03`，独立publisher
进程名及Redis清单，不复用RX02的单publisher通道。07:26完成Kafka预热，
34/34组合实时就绪，NQ 14/14就绪；07:27、07:28、07:29全部组合连续更新。
发布始终关闭，B/S为历史模拟。
原50个CTA/Exec/Manager进程PID、CPU mask及55份配置哈希保持。
无Manager/Exec绑定，未启动交易引擎。证据：`artifacts/cta_perp_rx03_deployment/`。

2026-10-14 04:38 UTC 修复Exec06 RapidX现货市价买入的quoteOrderQty构造及金额精度，
本地arbmm release构建、SHA-256核验、原子替换后，仅经环境本地脚本重启exec-pre-trade。
PID3307344 → 中间PID375418 → 最终PID448295，CPU mask保持0-5；
其他44个受保护进程PID/绑核及15个配置/环境文件哈希均保持，signal PID176562继续实盘发布。
交易IP、账户/策略配置与库存目标未调整，无隔离核重新分配。
证据：`artifacts/rapidx_cash_market_buy_fix/`。

2026-10-14 04:12 UTC RX01 Spread 完成46组合100 USDT绝对目标重建并开启实盘推送。
signal通过环境本地脚本重启以载入状态，PID46021 → PID176562，CPU0-5保持；
publisher CPU17及其余44个受保护进程PID/绑核均不变，无隔离核重新分配。
五因子92个现货/永续目标与Exec06一致，独立inventory保持原绑定。
证据：`artifacts/cta_spread_rx01_live_enable/`；下方04:01关闭状态是部署时历史。

2026-10-14 04:01 UTC 仅重新部署 RX01 Spread signal 的绝对目标替换修复，
`~/binance-cta-spread-rx01` signal PID3132535 → PID46021，实测CPU mask仍为0-5。
publisher PID663465/CPU17、配置PID3183433、页面PID3390010及所有Exec保持原PID与绑核；
共44个受保护进程核对一致，未新增或重新分配隔离核。46/46组合就绪，
发布保持 `enabled=false`，未清理历史累计目标或调整账户仓位。
本地arbmm release构建并逐文件校验后原子安装，仅使用环境本地signal脚本重启。
证据：`artifacts/cta_spread_rx01_absolute_target_deploy/`。

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
| 15 | persist_manager ×N | fr_arb03/04、bitget_fr_arb02、gate_fr_arb01/02 等堆叠；okex_mm_alpha 已停止 |
| 16 | spread_pbs gate-bitget-futures-bbo | 独立 Gate / Bitget USDT 合约 BBO；`spp_gate_bitget_futures_bbo`；IPC `futures_bbo` |
| 17 | cta_perp_factor_model_15m_pub(binance-cta-perp-rx01) | PID3646786；38币/89因子/146仓位规则；2026-10-30观察预热，enabled=false |
| 18 | cta_perp_factor_model_1m_pub(binance-cta-perp-rx02) | 2026-10-22 新版23币/22因子/69组合，PID160373，双Manager推送；68/69就绪，RLC继续预热 |
| 19 | cta_perp_factor_model_1m_pub(binance-cta-perp-rx03) | 16币/26因子，PID2307925；2026-10-29恢复45组合交易，单份200 USDT；CTA_PERP_FACTOR_MODEL_1M_CORE=19 |
| 20 | exec_volatility_pub(exec_volatility_binance) | 共享Binance现货/永续Vol，PID2865016；独立于RX01生命周期 |
| 21 | (空) | 原 okex_mm_alpha trade_signal 已停止，配置保留 |
| 22 | (空) | 原 okex_mm_alpha pre_trade 已停止，配置保留 |
| 23 | (空) | 原 okex_mm_alpha trade_engine 已停止，配置保留 |
| 24 | crypto_cta_manager / cta_web | Manager 网页服务，2026-10-22 实测仍固定 CPU24 |
| 25-27 | (空) | 原 binance_mm_alpha 已下线退役 |
| 28 | cta_perp_factor_model_5m_pub(binance-cta-perp-rx04) | PID705737全部线程；CTA_PERP_FACTOR_MODEL_5M_CORE=28；2026-10-23开启5min实盘，仓位重算保留publisher |
| 29-35 | (空) | 原 binance-intra-arb02 已下线删除 |
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
binance_fr_arb01/02/03/04、gate_fr_arb01/02/03、gate_fr_arb04 的只读账户监控、bitget_fr_arb01/02、
okex-intra-arb01 全套、trade_flow_feature ×8、rolling_metrics ×5、fusion_factor_1m、
persist_center、predict_file 及各类 viz/config/dashboard 服务。
`binance_exec_trade08` 的engine、executor、监控、持久化、Viz/Config均有效CPU0–5；
已通过virtual02跟随RX02，Grid实盘执行，独立trade_signal保持停止。
`okex_mm_alpha` 已于2026-10-29停止，其 persist_manager 原绑核15，配置保留。
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

2026-10-23 10:39–10:49 UTC，经操作者再次授权，FR signal/pre_trade/trade_engine
先停机，账户13笔挂单全部撤销，21个永续持仓使用reduce-only市价单全部平仓，
现货通过市价买回/卖出并显式no-convert-repay清理。重启前交易所回查确认
永续持仓=0、挂单=0、全部币种借款=0；仅留每币低于最小交易金额的现货零头，
合计约52.15 USDT。旧dump与pos_dump均为空；开仓名单和UniMMR候选均为指定19币，
仅正套。10:45–10:46 UTC通过环境脚本恢复trade_engine PID1603071/CPU15、
pre_trade PID1603176/CPU14、trade_signal PID1603903/CPU13，实盘信号已启用。
account_monitor PID1581248/CPU12及4个读写辅助进程持续运行，8进程验收通过；
14个无关行情/MM/bridge进程与私有API源IP不变。保留当前配置中的操作者调整：
max_leverage=1.5、UniMMR锁定/恢复5.0/5.1、强平/恢复4.5/4.6，未覆盖回初始参考值。
停机前约193673 USD累计借款对应约11560 USD维持保证金，UniMMR约4.79；
清仓还款后交易所初始/维持保证金和MMR均为0，重启后的暴露快照已刷新。
证据位于SG `~/bybit_fr_arb01/run/reset_20261023/` 及本地
`data/ops/bybit_fr_reset_20261023/`。下段是首次切换时的历史状态。

2026-10-23 09:51–10:15 UTC，经操作者授权，`bybit-intra-arb01` 切换至
`bybit_fr_arb01` 实盘 FR。旧 signal/pre_trade/trade_engine/account_monitor 已通过
环境脚本停止；旧 Config/Viz/persist_manager 保留历史读取。新 FR 共8进程在线：
account_monitor PID1581248/CPU12、trade_signal PID1589108/CPU13（no_emit=0）、
pre_trade PID1582841/CPU14、trade_engine PID1580489/CPU15；persist PID1580196、
Config PID1514349、Viz PID1514449、FR dashboard PID1514509 均为0–7。
19个指定币种仅正套，27个旧持仓/原清仓币种放入dump，UniMMR平仓候选覆盖46币。
参数参考实时 `binance_fr_arb02`：MM、100 USDT/单、max_pos_u=5000、max_leverage=2.1；
Bybit rolling 在原分位数中补25，46币阈值全部加载。已确认真实平仓/对冲成交和
独立RocksDB写入，14个无关行情/MM/bridge进程身份及绑核不变。
NAV 原 intra 停用、新 FR 启用，统计起点为10:03:29.591 UTC；IP/路由不变。
已补通知URL，但SG与参考JP的18100服务均未运行；EUR/HFT历史零头无标记价，
集中度自动dump扫描当前跳过，其他单币/杠杆/UniMMR风控仍执行。详见
`docs/delist_risk_server.md`；证据/备份：`data/ops/bybit_fr_cutover_20261023/`、
SG `~/bybit_fr_arb01/run/cutover_20261023/`。

2026-10-23 06:47–07:10 UTC，预部署 `bybit_fr_arb01`，尚未从 `bybit-intra-arb01`
切换交易。新环境的 Config PID1514349、Viz PID1514449、FR dashboard PID1514509
运行于 housekeeping `0-7`，分别监听回环 `20041/20141/20151`。
account_monitor、pre_trade、trade_engine、trade_signal、persist_manager 均已发布但停止；
env.sh 为未来切换预留 `12/14/15/13`，当前不占用这些隔离核，原 intra 七个 PID 不变。
短暂的 `TRADE_SIGNAL_NO_EMIT=1` 信号检查使用 CPU0，进入 FundingArb 后因新环境未启动
pre_trade、缺少查询 IPC 而退出，已通过环境脚本停止；没有启动下单链路。
新 FR namespace、Redis 名单、RocksDB 目录独立，名单为空；NAV 登记 `enabled=false`。
正式切换时先停止旧 intra 的交易进程，再启用这些核绑定；需重新核对币种、持仓及完整启动顺序。
JP delist 及其 SG 隧道更新后维持 CPU `0-5`，JP 九套 FR 的72个 PID 不变。
部署和验证证据：`data/ops/bybit_fr_prepare_20261023/`，SG
`~/bybit_fr_prepare_20261023/` / `~/bybit_fr_arb01/run/`，JP
`~/delist_risk_server/run/bybit_fr_prepare_20261023/`。

2026-10-19 08:21–08:28 UTC，按操作者要求，将原 intra arb02 账户的预备 Exec 02
改为 `bybit_exec_trade01`（`bybit-01`），归 LGQ configure，跟随既有 virtual01，
10项绑定、1项启用、multiplier=1，执行模板及五项风控保留。旧Exec 02禁用、解绑、
份额归零且全停；新Exec只启动Config PID3656055、Viz3656122、persist3656242、
account_monitor3656401，Manager为PID3656412，均运行0–7。
exec-pre-trade/trade_engine/trade_signal及两套intra继续全停，RX02未接实时推送。
11个无关MM/公共行情进程身份及20份既有非Manager配置哈希不变，8–15核分配不变。
Viz/Config仅监听 `127.0.0.1:10041/18161`，4191 `/exec_trade01/`鉴权、WS通过。
08:22:01.653 UTC新建12条账户快照：6条现货净持仓、6条USDT永续，余额/借币/利息
保留为不可变审计依据；现货和永续FIFO分离，未增加现货执行范围。旧订单不再要求归档，
原数据保留原目录，没有复制或重标为新Exec历史。复算确认12条初始持仓，历史成交和费用均0。
证据见独立Manager仓库 `artifacts/sg_exec01_20261019/`及`docs/sg_deployment.md`。

2026-10-19 08:01 UTC，仅更新 SG CTA Manager 后端、前端及三个分析工具，
release `20261019T080045Z`、Manager checkout `6e422e1`。Manager PID3647035，
affinity仍为0–7，health=ok；补齐报价币手续费字段及上海零点每日权益表。
15个受保护进程、21份现场配置、策略/权限/快照/跟随配置哈希一致，8–15核分配不变。
LGQ 的 `bybit_exec_trade02`（`bybit-02`）账户监控、persist、Viz、Config仍运行，
exec-pre-trade/trade_engine/trade_signal及两套intra保持停止，RX02未接实时推送。
当时尚无 `bybit_exec_trade01`；arb02原约551 MiB数据仍在旧目录，旧运行文件备份
不含订单数据。后续按用户指示免归档并切至01，见上文。升级核验见独立Manager仓库
`artifacts/sg_manager_20261019/`及`docs/sg_deployment.md`。

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
| 12 | account_monitor_bybit(bybit_fr_arb01) | 2026-10-23 FR切换，PID1581248 |
| 13 | trade_signal(bybit_fr_arb01) | 2026-10-23 清仓后重启，PID1603903；实盘信号启用 |
| 14 | pre_trade(bybit_fr_arb01) | 2026-10-23 清仓后重启，PID1603176 |
| 15 | trade_engine(bybit_fr_arb01) | 2026-10-23 清仓后重启，PID1603071；单线程；`TRADE_ENGINE_IPC_CORE` 已废弃 |

未绑核、跑在 housekeeping 0-7 的热路径进程(截至本次盘点):
depth_pub、trade_flow_feature_pub、rolling_metrics及若干 persist_manager。
原MM alpha及CTA Exec全套已于2026-10-29停止并删除部署，CTA专用CPU6 publisher停止；
bybit-intra-arb02 原有全套进程使用 0-7，2026-10-16 已全部停止。
这些与 housekeeping/系统中断同核竞争;隔离段 IRQ 已迁到 10/11。

2026-10-21 01:04–01:15 UTC，按操作者要求先中性对齐，再恢复 `bybit-intra-arb01`
全套 7 类进程。仅调整 USDT 永续，9 笔人工对冲单全部成交，总成交额约 83.94 USDT；
启动前单币最大绝对净敞口 4.37 USDT，绝对敞口合计 28.62 USDT，其余受最小下单量限制。
沿用 10-16 发布的二进制，release guard 通过；env.sh、trade_engine.toml、manifest、
Viz 和 Config 配置的 5 项哈希保持一致。运行后原平仓逻辑继续执行；UniMMR 约 4.46，
低于原开仓触发线 4.5，恢复线 4.6，未调低风控。Config/Viz/persist 使用 CPU 0–7，
端口为 19191/10174，intra arb02 保持停止。
运行日志确认 ArbOpen 被 UniMMR reduce-only lock 拦截；同时本地 UniMMR 通知服务
连接报 `Connection refused`，通知通道尚未恢复。

恢复时发现旧共享行情及 MM alpha 的 `/tmp/iceoryx2` 服务/节点文件已被删除，
旧进程持有已删除 inode，新进程无法发现服务。通过环境本地脚本重启 Bybit shared
spread（market PID 393335 / CPU 8；bookticker PID 393356 / CPU 9）、bridge、depth、
两路 trade_flow_feature、rolling 和 MM alpha；所有重启组件的 deleted IPC fd 归零。
MM alpha 全套回到 CPU 0–7，UTC `18:00–02:00` 禁开仓配置保留；停止时确认永续挂单为零。
独立 Exec publisher PID 3818626 / CPU 6 保持运行，其服务名带 `bybit_exec_trade01_`。
共享 spread 启动脚本已按 `MARKET_DATA_IPC_NAMESPACE` 区分进程，避免将该独立发布器
误判为共享服务冲突；只补丁修改环境副本并备份原脚本，没有覆盖其其他配置。
主机原有 `/tmp` 30 天清理规则，新增 `/etc/tmpfiles.d/mkt-signal-iceoryx2.conf`
排除 `/tmp/iceoryx2` 的按时间清理；未确认此前具体删除动作，也未全局清理 IPC。
交易/账户连接仍使用 `172.31.7.124`，共享行情使用 `172.31.10.217`，ENI/路由保持。
SG 证据及备份：`~/backups/bybit_intra_arb01_restore_20261021/`。

01:27 UTC 单独发布修正后的 `rolling_metrics`，当前 PID 401778 / CPU 0–7。
实测 derivatives IPC 使用微秒时间戳，原 rolling 将其作为毫秒重采样，导致 premium
样本数迅速虚增至 14400；此前该读数不代表四小时历史已经积累。现于聚合边界将
premium/funding 时间转换为毫秒，不变更 IPC 微秒格式和 Redis 参数。
本机 `arbmm` release 构建、8 项测试及格式检查通过，上传后 SHA-256 核验并原子安装；
新二进制 SHA-256 为 `bf8958e68b46f8ec92a18f8baa08bbe1e0fc3c3559d6b1dd6f3e29c82d030aa1`。
旧二进制及两份 rolling 输出 hash 已备份。窗口从零积累，BTC/ETH/SOL/TRX 三档 premium
分位值均恢复，每 30 秒增加约 30 个样本；完整四小时窗口仍需积累。
验证回执见远端 `rolling_after_timestamp_fix.json` / `rolling_deploy.json`，
本地只读证据保存在 Git 忽略目录 `data/ops/bybit_intra_arb01_restore_20261021/`。

2026-10-21 00:51 UTC 按操作者要求，退役 SG 的 Bybit/Binance 跨所 rolling
`rm_bb_fu_bn_fu`（原 PID 1224991，CPU 0–7）。通过环境本地
`scripts/rolling_metrics/stop_rolling_metrics.sh` 删除 supervisor 记录并清理残留进程，
将 `~/rolling_metrics/bybit-futures-binance-futures` 移入
`~/backups/retire_bybit_binance_rolling_20261021T005151Z/runtime`。
同目录保留原日志、supervisor 配置及 Redis 备份；删除其专属参数、跨所输出和经写入时间核对
属于该进程的 Binance 单市场输出，避免遗留阈值继续被消费。
SG rolling 仅保留 `rm_bb_mg_bb_fu`（PID 1225163，CPU 0–7）；其 Bybit 单市场输出不删除，
由现货/永续 rolling 继续刷新，消除跨所进程交替覆盖 premium 分位阈值的问题。
00:52:30–00:53:30 UTC 四次抽样确认 BTC/ETH/SOL premium 各 3 档分位阈值持续存在，
样本数均为 14400，输出时间推进；已删除的 3 个 Redis key 未重建。
验证明细保存在上述备份目录的 `validation.json`。
`bybit_mm_alpha` 全套进程保持在线，原 UTC `18:00–02:00` 禁开仓配置保持启用。
本次未变更专用 CPU、源 IP、ENI 或路由。

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
