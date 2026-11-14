# 隔离核心分配登记(jp-meta-elvpn / sg)

最后更新: 2026-11-14 13:45 UTC。**部署、迁移、下线任何绑核进程时,请同步更新本表。**

2026-11-14，按授权对 JP `gate_fr_arb04` 撤单、重发最新7个二进制并以合约taker对齐。
仅将本盘`env.sh`的`ENABLE_IPC_FAST_POLL`从1改为off；全部运行进程确认fast poll关闭。
本机构建基线`arbmm 483937e8`，包含已记录的现有工作区补丁（SHA前缀910adcc）；
经`publish-jp-meta-fr.sh --skip-build`发布6个产物，dashboard独立SCP/SHA核验原子安装，
全部生命周期操作经环境本地脚本；信号已恢复。新PID：config799918、Viz800500、
persist801270（CPU15）、engine803309、pre_trade803614、account_monitor803896、
dashboard806697、signal806797；其余保持原CPU mask0-5,48-95（有效在线0-5）。
撤单后挂单复核为0；4笔合约SELL IOC（MUBARAK两笔、CTR和O各一笔）共302.36290U，
均有taker成交和持仓确认，单笔不足100U、提交间隔至少5.612秒；现货和BNB保持。
恢复信号前可估价非BNB单币残余最大4.88344U，另3项历史零碎各5e-9未动；
恢复策略后新成交会再次形成策略敞口，不将该检查点当作持续零敞口保证。
四个原高负载进程采样合计CPU由约243%降至7.16%；Redis30秒29次同步最大6.242ms。
7项fast-poll相关回归和全部release构建通过；145份配置指纹仅本盘env变化，
179个已记录其他常驻进程身份/CPU保持；2个独立NAV有限期sync_history任务消失，
退出码不可得，原始审计保留例外。源IP、策略参数、账户模式和杠杆未手工改动。
回执：本机`data/ops/gate_fr04_fast_poll_20261114/`；主机
`~/gate_fr_arb04/backups/fast-poll-off-20261114T1314Z/`（含凭据的原env仅留主机0600备份）。

2026-11-14，JP RapidX 账户监控改为内存/IPC实时状态和有界事实投递箱，
源码`arbmm bba62aa4/585a1d7d`；本机构建、SCP/SHA-256核验后原子安装，
经环境本地脚本仅替换三路monitor/persist：rx01 PID413434/376822、
rx03 PID415832/388029、rx04_5m05 PID364231/327610，均继续CPU0-5,48-95。
Manager `08fad46`单独发布为PID438574，继续CPU24；Redis7.4.12 PID4100946
继续CPU28，交易/Viz/Config与源IP保持。旧档案14033126行已导入并收齐ACK，
原13.11GB JSONL保留且停止增长；三路IPC新鲜度与恢复就绪验证通过。
145配置散列、四张不可变快照表和180个受保护常驻进程身份/CPU保持；
另一个有限期NAV sync_history PID289921已退出现场，退出码不可得，审计保留例外。
监控fsync采样每路2次/30秒；热运行仍有少量Redis推送超时，未宣称根治。
详情见`docs/rapidx_integration.md`；现场回执
`~/crypto_cta_manager/backups/rapidx-ipc-20261114/`。

2026-11-14 05:55 UTC，JP Delist 重新发布 Redis 客户端修复，参数巡检每轮复用
连接并关闭 Redis 7.0 不支持的 CLIENT SETINFO。新 PID 3996586，继续 CPU28；
Redis PID 2769589 和 Manager PID 2268661 保持，95 个受保护进程身份未变化，
Delist 配置散列未变。编译及参数巡检 5 项测试通过；本地发布产物 SHA-256
`7ec5bd797e5dc99ae0cfa32251d3e2e36d8c0c183d63cec33f1753414e52a08d`。
20 秒采样 Delist 新建连接 32 次，其他现有客户端仍发送 SETINFO，Redis ERR
仍增长；不能据此认定推送 502 已解决。Bitget03 UniMMR 空列表告警重启前已存在。
回滚和现场基线：`~/delist_risk_server/backups/redis_client_fix_20261114T0553Z/`。

2026-11-13 07:42 UTC，JP `cta-factor-spread` 已安装本地 `arbmm` release，SCP/SHA-256
校验后经环境本地脚本重启，PID1140614，继续CPU18。两市场17币Tardis小时输入
补至11/12 00:00 UTC，227个普通分钟归一化/分位窗口从原进程事实记录续接；
8份仓位账本保留，Spread02四表继续100U/规则、发布关闭。08:06 UTC回放追平；
Spread01小时109/109、Spread02小时130/130就绪，既有三条分钟规则未退回预热；
两个Exec、Prep和配置服务等受保护进程身份及CPU保持；无source IP/ENI/端口变化。
回执：`artifacts/cta_spread02_tardis_warmup_20261113/`。

2026-11-12 15:34 UTC，JP `bybit_fr_arb01`完成整套重发：7个本地release产物源自
`arbmm 87bf37fb`，SCP/SHA-256核验、原子安装后，经本环境脚本恢复8服务含signal。
persist继续CPU15，其余保持0-5,48-95；配置与`.35.232/.35.233`源IP、86个其他已记录
FR/Bybit行情进程身份/CPU保持。SG旧FR/intra01实际进程复查为空。
停盘零挂单时仅用3笔合约taker对齐VELO/SLX/DYDX，共19.232852U，间隔至少5.215秒；
现货、BNB及参数不动，恢复前可估价非BNB单币尾差均不足5U。BSB批量抵押品列表漏项
以单币查询修复`8ebec272`解决，40项回归通过；未修改抵押开关或币种列表。
15:34运行哈希/双路认证/实际socket源IP验收通过，启动缺价已随行情补齐，Delist完整/ok。
另核验Gate四盘仍为`293f6352`的旧guard，尚未包含`90b24c42`仅异常拦截修正。
回执：`artifacts/bybit_fr_publish_20261112/`。

2026-11-12 15:01 UTC，Delist独立发布FR单币180秒持续确认及1.5倍严重超限即时线，
最终源码`arbmm 68f31629`、PID3195932，全部线程继续CPU28。124个原有采样应用进程
身份/CPU及22份配置指纹保持；同期Binance重发新增进程单独记录。15:04:26复核11账户完整/ok、
通知队列为空。15:04仅修复Bybit01静态页面ACL，7个应用进程及生产Nginx身份/CPU保持。
详情及两组回执见`docs/delist_risk_server.md`。

2026-11-12 15:01 UTC，JP Binance FR02/03/04完成整套重发，构建源码`arbmm b91dffca`。
7个本地release产物经SCP/SHA-256及原子安装，24服务含signal经环境本地脚本恢复。
三盘persist继续CPU15，02/04 config_server恢复原CPU0-5，其他服务沿用原housekeeping；
环境配置和`.35.231`源IP保持，Gate/Bitget的56个已记录进程身份/CPU保持。
停盘撤单后仅以合约SELL taker对齐6币，共10笔615.308376U，单笔低于100U、按5秒调度；
BNB及现货库存不动。恢复前非BNB每币尾差均小于5U，三类挂单均为0；恢复后新成交继续
由策略对冲。03的margin开仓51015/51042已触发既有冷却，线上确认频率退避1/2/4分钟；
拒单本身不表示冷却失效，回执已更正此前“未修复”的不准确描述。
详情和运行回执：`artifacts/binance_fr_publish_align_20261112/`。

2026-11-12 14:27 UTC，完成JP Bitget FR 01/02/03整套重发，源码`arbmm 2136a80d`。
7个二进制本地release构建、SCP/SHA-256核验及原子安装；24个服务（含3路signal）
通过环境本地脚本恢复，快照及运行哈希验收通过。02/03 persist_manager继续CPU15，
config_server原绑核0-5在启动后恢复；其余服务保持原housekeeping范围，01各服务为
0-5,48-95。环境配置和`.35.231`源IP不变，98个已记录其他交易/行情进程身份/CPU保持。
发布前逐盘核验零挂单；02停机间隙新增的10笔AUSDT margin单经撤回复核，03等自然清零
再更新。无手工对齐或杠杆改动。回执`data/ops/bitget_fr_resume_20261112/`及
`artifacts/gate_fr_on_repair_20261112/bitget_exception_guard_publication_verification.json`。

此前同版本Delist聚合通知已独立发布，PID3070247，继续CPU28；该次重启前后的142个
其他已记录应用进程及10份配置指纹保持。页面`46fe3434`热更新“待处理项目”字样，无重启。
Gate四盘本次没有发布交易进程。详情见`docs/delist_risk_server.md`。

2026-11-12 13:51 UTC，仅更新JP `~/delist_risk_server` 方向maker正常区间报警规则
`arbmm 0128f5f5`。本地release构建、SCP/SHA-256及原子安装后，经环境本地脚本
重启为PID3034380，全部线程继续CPU28。142个其他应用进程身份/CPU及10份配置
指纹保持，Delist开机条目更新且其他34条保留；11个FR首轮及220秒后数据完整且均无敞口报警。
交易服务、参数、源IP及网络配置不变。回执 `data/ops/delist_maker_band_20261112/`，
详见 `docs/delist_risk_server.md`。

2026-11-12 13:08 UTC，仅发布Delist正常尾差/成对平仓报警修复 `arbmm ff83434d`。
环境本地脚本重启为 PID2945573，继续CPU28；142个其他应用进程身份/CPU及10份配置
指纹保持。Bitget01/02/03首轮均无敞口报警，11个FR账户数据完整；交易服务、参数和
源IP不变。回执 `data/ops/delist_close_20261112/`，详情见 `docs/delist_risk_server.md`。


2026-11-12 12:51 UTC，仅更新 JP `~/delist_risk_server` 的 Redis 敞口报警逻辑，
源码 `arbmm 7b91d091`；本地release构建、SCP/SHA-256校验和原子安装后，经环境脚本
重启为 PID2909567，继续 CPU28。142个其他应用进程 PID/start_ticks/CPU 及10份
Delist配置指纹保持；11个 FR 快照数据完整，未启动/停止任何交易策略或改变trade signal。
仅更新 Delist 开机条目、保留其他34条；源IP/网络配置保持。
回执 `data/ops/delist_redis_7b91d091_20261112/`，详见 `docs/delist_risk_server.md`。


2026-11-12 11:02 UTC，JP 新增独立 `~/ltp_transfer_server`，PM2 `ltp-transfer-server`，全部线程运行于
housekeeping `0-5`，回环监听 `127.0.0.1:8788`，经已有4191网关 `/delist/transfers/`
接入Delist登录。仅启动新服务、热更新Delist导航并平滑重载Nginx；Delist及已有
交易/行情/Redis进程身份与CPU保持，Nginx master1862269保持。仅更新此服务的
PM2开机条目，保留其他34项。控制网卡源 `172.31.35.228`，未新增独占核或IP。
本机构建并SCP/SHA-256核验后原子安装，二进制SHA-256
`e544ac67f01b6ecd4444081074308ba772f060e7826e544e3bfdcb7ed4026845`。
真实只读验收2个DMA账户、6种钱包，Binance→OKX USDT网络AVAXC/ERC20/TRC20；
未执行资金划转。回执 `artifacts/ltp_transfer_server/`，说明 `docs/ltp_transfer_server.md`。

2026-11-12 10:33–10:43 UTC，按授权启动 JP `gate_fr_arb01–04` 的 `trade_signal`，
四盘各8个服务已验收；信号继续housekeeping（`0-5,48-95`），持久化继续CPU15。
随后仅 `bitget_fr_arb01` 完成 `05425d6e` 全套发布，7个本地release产物经SHA-256
核验、原子安装，8个服务均恢复，全部继续原housekeeping CPU范围。
用户要求暂停后，02在替换产物前终止发布，已通过本环境脚本恢复原版交易服务，
10:43:25 UTC验收原版8服务运行；03未变更。02/03既有CPU分配保持。
正常maker反复零成交触发旧 `hedge_stalled` 的问题正在本地修正，尚未发布，
不得据此声称线上拦截已解除。未手工对齐或修改风险参数；配置和源IP保持。
回执：`artifacts/gate_fr_on_repair_20261112/README.md` 的 signal resume / partial
Bitget publication 记录。

2026-11-12 10:13 UTC，JP `gate_fr_arb01–04` 按授权发布已推送 `arbmm` 提交
`293f6352`，包含单队列对冲修复、紧急减仓保护与缺价REST补充。本地隔离源码release构建，
每盘7个二进制经SHA-256校验后原子替换；通过环境本地脚本恢复7个非signal服务，
四盘 `trade_signal` 均保持停止。28个运行进程及哈希已验收，persist_manager继续CPU15，
其余服务继续`0-5,48-95`。环境配置、源IP和既有50040/50050/50080同步监听保留。
发布前后现货/合约数量一致、挂单为0；10:16 UTC四盘Delist均为完整数据，启动缺价提示已解除。
日志与回退二进制保存在各环境 `backups/gate_fr_publish_293f6352_20261112/`。
回执：`artifacts/gate_fr_on_repair_20261112/scheduler_deployment_verification.json`。

2026-11-12，按标准发布流程更新 JP `binance_fr_arb03`。先停信号、停机撤单，
手工合约对齐后再启动基础服务与信号；初版恢复发现多档买单复用现金快照，已再次
暂停并发布 `163916fa` 补充冻结/返还现金约束。修正版恢复后跨两个资金查询周期
51015/51042均为0，Delist现场已显示对应本地BUY拦截状态。
persist_manager PID2383326保持CPU15；pre_trade PID2413027、trade_engine PID2412919、
account_monitor PID2386471、signal PID2416779、Viz PID2383007、Config PID2381858、
dashboard PID2341948保持housekeeping（`0-5,48-95`）。源IP仍为172.31.35.231，
`env.sh`、交易引擎和Viz配置哈希保持。发布及偏差回执见
`artifacts/binance_fr_arb03_publish_20261112/README.md`。
首批非BNB手工对齐42笔订单/47条taker成交，名义118.78U；其中36笔不足5U，处理过细。
BNB曾误买0.13合约，用户获知后明确要求暂时保留、不要再操作；后续无BNB成交。
最终恢复前又补清运行阶段留下的AWE/FLOW两项约100U残留，分别1笔合约SELL；
全程非BNB手工44笔订单/58条成交。最终恢复前尾差8.3296U（不含BNB），
最大单币不足5U；资金/最新持仓同时验收通过后恢复信号，之后敞口随策略变化。

2026-11-12 08:33 UTC，按用户追加授权恢复 JP `gate_fr_arb01–04`，四盘均不开
`trade_signal`。已提交 `arbmm` 源码 `c2419da3` 在当前本机仓库内隔离导出并 release
构建，8个二进制上传SHA-256校验；每盘7个存在差异的二进制原子替换（含只更新文件的
trade_signal），随后通过环境本地脚本启动 account_monitor/trade_engine/pre_trade/
persist_manager/Viz/Config/dashboard。每盘7进程及实际运行文件哈希逐项验证。
persist_manager继续CPU15，其余Gate进程保持`0-5,48-95`；Delist PID2320719继续CPU28。
01同步端口50040初次短暂占用，保留日志后仅重启该persist_manager恢复监听，配置不变。
恢复前全币合约对齐301笔/20,778.840933U（另有前序ON38笔），现货未变、挂单0；
恢复后现货/合约均与完成快照一致，无重复对冲。源码范围、备份、PID、哈希及未消除尾差：
`artifacts/gate_fr_on_repair_20261112/README.md`。

2026-11-12，按用户要求通过环境本地脚本停止 JP `gate_fr_arb01–04` 全套服务，
包含 signal/pre_trade/trade_engine/account_monitor/persist_manager/Viz/Config/dashboard；
07:46 UTC `/proc` 复核四环境无运行进程，交易所挂单为零。原 CPU/IP 分配保留，
未发布或重启 Rust 产物。ON 经 38 笔合约 taker SELL 完成对齐，最短间隔 5.079 秒，
各户只余不足 10 ON 的尾差；后续全币合约对齐按用户授权单独执行，策略保持停盘。
最初错误使用现货优先的成交、修正及审计见
`artifacts/gate_fr_on_repair_20261112/README.md`。

2026-11-12 06:50 UTC，Delist新增Spread02配置直达按钮，CTA链接走自身登录网关。
仅原子更新Delist静态页与Nginx片段并平滑重载，Nginx master PID1862269保持；
178个采样应用进程身份/CPU和29份计算配置保持，无服务重启或CPU/IP变更。
四张Spread02表仍关闭，回执`artifacts/cta_spread02_delist_access/`。


2026-11-12，JP新增Spread02：18币、319条OKX因子规则，四频段48/79/64/128条，
每规则100U、四张表发布关闭，复用Spread01的OKX/Binance双Exec，未新增执行绑定。
共享`cta-factor-spread` PID1965122继续CPU18，合并532项计算；配置服务PID1954474
继续housekeeping（`0-5,48-95`）、回环19220；Delist配套解析更新PID1969486仍CPU28。
通过环境本地脚本启停，仅上述三个服务更换进程；两侧Exec、Prep及其他常驻服务保持。
原Spread01四份配置、仓位账本逐项保留；首次启动因小时包落后于Kafka保留边界而拒绝，
已以11-09/10真实Tardis输入扩展到12币×816小时、截止11-11，88/88项普通因子就绪。
新包位于`~/cta-hourly-history/spread01_20261111/data`，只修改该环境历史包路径；
旧包、私有配置和停机时完整仓位均保留。OKX阈值仅补10个缺项，原9项及Binance表保持。
Spread02的OKX小时输入由Kafka及实时数据自然预热；不混用Binance历史。
没有新增独占核，未改交易IP、ENI/EIP、路由或执行配置。
06:25 UTC完成185,633条回放并切换实时，四份Spread01目标已获Manager确认，
后续实时分钟决策与发布均已核验。Spread02保持零仓位、未启用，237/341项计算就绪，
其余继续自然预热；Delist目录/生效计划一致且错误为空。
候选、恢复与校验回执：`artifacts/cta_spread02_deployment/`；远端私有回退：
`~/cta-factor-spread/run/spread02_20261112T055433Z/before/`。


2026-11-11 09:04 UTC，JP Delist 移除 SG Redis、远程平仓及备份主机配置，
经环境本地脚本停止 SG SSH 隧道并重启 Delist PID3513519，保持 CPU28。
11个 JP Viz 快照目标全部验证通过，Bybit FR 交易及 NAV 保持运行。回退配置位于
JP `~/delist_risk_server/backups/retire_sg_20261111/`。

2026-11-11 08:57 UTC，按用户要求停止 SG 剩余旧应用：Bybit intra01 的
Config/Viz、Bybit 双路 spread、depth、现货和永续 trade-flow、rolling metrics、
SG public bridge，以及 SG TLen Config 和 CTA Manager；CTA Manager 已禁用自启动。
PM2 保存列表已清空，旧列表备份在 SG `~/backups/retire_sg_apps_20261111/`。
SG 未发现交易、行情或上述应用进程；系统设施、Redis、数据库和共享连接保留。
JP Bybit FR 交易及行情仍运行，NAV active；当时杠杆约1.62，原1.60上限仍限制新开仓。

2026-11-11 05:34–05:44 UTC，按用户确认将 EDGE 永续单笔 reduce-only 市价买入
2402枚，15笔成交均为taker，平均价0.37282265、手续费0.16119366 USDT；NAV已核验
15笔成交及手续费。停止信号后旧hedge订单自动撤销，手工对齐后重启信号时发现旧
pre_trade内存仍保留待对冲数量并产生9笔重复买单；立即停signal和pre_trade、全部撤单，
确认无重复成交，再重启pre_trade重建仓位，先不发单验证后恢复实盘信号。
当前pre_trade PID3081168、signal PID3085759，行情market PID3086525/CPU33、
bookticker PID3086954/CPU34；SG全部FR进程停止。EDGE净差约0.13 USDT、总净差约
23.8 USDT、风险free_trade、UniMMR约6.06、pos_dump为空，现无活动订单。
当前杠杆约1.64高于原上限1.60，新开仓被风控拦截；未修改原杠杆或风险参数。
迁移/订单记录在JP `~/bybit_fr_arb01/run/migration_20261111/`。

2026-11-11 05:02–05:30 UTC，将 `bybit_fr_arb01` 从 SG 迁至 JP：SG 全套停止，
停信号后现货/永续订单两次复查为0；1.4G RocksDB 141文件逐项SHA-256核验后迁至JP。
JP persist PID3004553/CPU15；account_monitor PID3005120、trade_engine PID3009154、
pre_trade PID3011353、Viz PID3011461、Config PID3011519、dashboard PID3011568
均在 housekeeping；交易两路 `.35.232/.35.233`、REST及WS认证通过。JP 网关、
Delist和NAV已改读本地快照，NAV PID3044325已订阅本地账户风险IPC；Delist
PID3042125、SG共享隧道 PID3041901 均CPU28。JP行情 market PID3048782/CPU33、
bookticker PID3048809/CPU34，接口20041/20141/20341为本机回环。
05:25 短暂恢复信号后 EDGE 浓度平仓形成约895 USDT净空；05:28暂停signal并撤销
EDGE现货单，原合约对冲maker单已超时撤销；该历史净差随后已按上文对齐。
`~/bybit_fr_arb01/run/migration_20261111/`保留迁移哈希及配置回退；SG原库保留。

2026-11-11 05:15 UTC，发布 `arbmm f6d4c06f` 修复 Delist 的 Spread 备份周期校验。
仅通过环境本地脚本重启 Delist PID3024821 与 SG 隧道 PID3024552，全线程仍 CPU28。
今日四周期备份已补齐，52份 payload 及 COS 回读校验通过；本地归档/保留清理
两条告警已恢复。原有165个其他进程身份/CPU及56份配置、脚本、页面指纹保持。
回退 `~/delist_risk_server/backups/spread_backup_f6d4c06f/`；
核验 `data/ops/delist_spread_backup_20261111/`，详情见 `docs/delist_risk_server.md`。

2026-11-11 04:42 UTC，JP NAV 的 `bybit_fr_arb01` 凭据读取改为本机受限文件；
原 JP PostgreSQL 历史、起点和授权保留。通过 `.35.232` 增量同步成交、资金费及
利息；04:45定时任务随后使用本地凭据成功执行，三项水位更新至04:45 UTC。
NAV 用于 Bybit 的7个候选交易网卡出口均已认证。
SG FR 仍交易，实时快照继续走原JP回环网关；NAV未重启、原PID483448/CPU24保持，
FR总览live且权益、敞口可用。登记 `host=local` 表示NAV凭据所在地，非交易环境
已迁移：若NAV在FR迁机前重启，本地账户风险IPC可能不可用，FR总览依靠原SG快照；
迁机时需切换快照及验证本地IPC。回退登记和PG schema备份在JP
`~/crypto_nav_manager/maintenance-backups/bybit_fr_local_collect_20261111/`。

2026-11-11 04:10 UTC，发布 FR 年化异常表格，Delist 二进制来自
`arbmm d71d7841`；Delist PID2887312 和 SG 隧道 PID2887284 均绑 CPU28。
回退备份在 `~/delist_risk_server/backups/fr_yield_ui_d71d7841/`；
NAV 目录13项，11个 FR PnL 请求均返回 HTTP 200。


2026-11-11 03:58–04:01 UTC，在 JP 新增 Bybit 现货/永续行情：`spread_pbs`
market PID2860248/CPU33、bookticker PID2860294/CPU34；两者分别使用行情网卡
`.46.90/.46.91`。`depth_pub` PID2872256、现货/永续 trade-flow PID2870204/2871330
和滚动指标 PID2862730 均运行于 housekeeping；行情及 depth25 已收到实际数据，
滚动指标仍在积累样本。按 Gate 配置 trade-flow 的 `depth_channel=none`，独立保留
depth25。仅迁移行情参数，SG FR 交易及 JP NAV 采集位置保持；FR 上 JP 的部署仍待定。
SG 旧 `bybit-intra-arb01` 持久化已停，订单库压缩备份保存在本机
`data/order_archives/bybit_intra_arb01_20261111/`，传输 SHA-256 已核验，归档内容与
停机源 RocksDB 逐文件比较一致。`depth_channel=none` 与 Gate 一样仅发布 RL 波动率，
不发布完整 trade-flow 特征；需要完整特征时须另行启用 depth25 并验证。

2026-11-11 03:40 UTC，发布 FR 七日及三十日年化表格，Delist 二进制来自
`arbmm 64f220c7`；Delist PID2820172 和 SG 隧道 PID2820145 均绑 CPU28。
回退备份在 `~/delist_risk_server/backups/fr_yield_table_64f220c7/`；
NAV 目录13项，11个 FR PnL 请求均返回 HTTP 200。


2026-11-11 02:53 UTC，发布 FR 七日年化提示，Delist 二进制来自 `arbmm`
`958bb8a3`；新 Delist PID2719199 和 SG 隧道 PID2719174 均绑 CPU28。
发布前备份在 `~/delist_risk_server/backups/fr_yield_958bb8a3/`；
NAV 目录13项，11个 FR PnL 请求均返回 HTTP 200。

source IP / `local_ips` 变更同步更新 `docs/jp-meta-elvpn_ip_binding.md`。

2026-11-10 09:25–09:35 UTC，补齐 `okex_mm_83777` 的NAV历史采集及CTA净值汇总。
NAV代码 `6c0b270` 将现用环境接入15分钟定时成交/资金费采集和既有HTTP汇总，
沿用独立金额FIFO；登记、统计起点和权限保持。补采184条成交、资金费0条但成功
水位完整，并采集BTC/ETH费率。仅本机构建NAV，SCP暂存、SHA-256校验和原子
安装后重载 `crypto-nav-manager.service`，PID483448仍CPU24；CTA PID3909484和
193个采样进程的身份/CPU mask保持，MM交易五进程未启停。NAV明细及CTA总览
均live，lgq查看和prc隔离、静态页面/脚本均核验；4个诊断会话已删。
CTA沿用闭合4小时曲线，截至08:00的27点完整；首笔实盘08:03，当前收益已在
首页/NAV明细显示，12:00自然采样点再纳入后续成交。没有新增Exec source或
交易绑定，未改MM参数、local_ips、ENI或共享行情。回执
`data/ops/okex_mm_nav_20261110/`；远端回退与采集备份在
`~/crypto_nav_manager/maintenance-backups/okex_mm_onboard_20261110/`。

2026-11-10 08:42 UTC，复核 `bitget_fr_arb01` 的CTA/NAV/Delist接入，补齐恢复时
遗漏的Delist私有Viz映射：`bitget_fr_arb01 -> http://127.0.0.1:20151/snapshot`。
线上与仓库 `config/delist_snapshot_targets.toml` 各只新增该条，其他10条逐项保持。
仅通过Delist环境本地脚本重载监控，PID379033仍CPU28；110个采样交易/行情/
NAV/CTA进程的PID和CPU mask保持。CTA的lgq查看授权、NAV活动登记与delist读取
授权均已回读，CTA实际汇总为live、无loading；新盘起点仍07:56:36.692 UTC。
Delist报告已为fresh、complete=true、risk_status=ok、errors=[]，首次核验净差约
0.04558 USD；后续新建仓的既有对冲计划按原逻辑观察。未调整交易参数、网卡、
源IP或监听。证据及回退：`data/ops/bitget_fr_arb01_restore_20261110/delist_snapshot_fix/`
及远端同名维护目录。

2026-11-10 08:16–08:28 UTC，按操作员指令将 `bitget_fr_arb01` 资金账户的
20,000 USDT划入UTA（transferId `1503962714985279488`），私有接口成功且余额回读
确认UTA可用20,000 USDT，资金账户仅余小于1e-8 USDT尾数。沿用Bitget02/03一致
的正套名单，排除合约最高4倍、与现有FR固定5倍要求冲突的HUSDT，启用其余21币；
反套/主动清仓名单为空，UniMMR候选为该21币，抵押品检查全部通过。
通过环境本地脚本重启引擎PID334847、pre_trade PID334958，并于08:21:50启动
trade_signal PID335050；21个合约5倍设置全部确认，失败0，风控与共享策略参数保持。
引擎 `TRADE_ENGINE_READ_ONLY=false`、信号 `TRADE_SIGNAL_NO_EMIT=false`；
原5个监控/记录/页面进程保持，8组件主线程全部CPU0-5，未新增独占核。
沿用已SHA-256核验的恢复产物，未另行发布；`trade_engine.toml` 与退役前配置字节一致，
两条交易连接继续`.231/.231`。112个非目标采样进程PID/CPU全部保持，NAV/Delist/CTA未重启。
08:26:45交易所确认CELO现货4条及永续1条成交，永续short967.3，另有2笔永续对冲
和7笔现货挂单；实时敞口约199.67 USD，风险free_trade，策略继续按既有规则建仓。
新NAV起点仍07:56:36.692 UTC，无旧初始快照；新库首4条成交始于08:23:01，
累计收益约0.03960 USD，仅来源成交返佣，20,000划转未作为收益。
旧40,052/7,427/1,596条成交/资金费/利息归档计数保持。回执、实盘观察及NAV验证：
`data/ops/bitget_fr_arb01_restore_20261110/go_live/`；远端同名维护目录保留私有env回退。

2026-11-10 07:55–08:08 UTC，按操作员要求恢复 JP `bitget_fr_arb01`，沿用原账户、
稍后入金，作为新盘子独立核算。同步 `arbmm a00dc38f` 工作区本机构建7组件release，
精确SCP暂存、逐文件SHA-256核验后原子安装；原运行配置和环境本地脚本保留。
新目录不复制旧data/run，原运行目录及订单归档仍在
`~/retired_data/bitget_fr_arb01_20261109/`。旧NAV schema改名为
`retired_bitget_fr_arb01_20261109`，其中40,052条成交、7,427条资金费、1,596条利息
及旧快照/水位保留；活动schema重建，统计起点设为 `2026-11-10 07:56:36.692 UTC`，
无旧初始快照，累计盈亏0，监控曲线3个点均0。旧清仓及交易名单清空，风控参数保留。
通过环境本地脚本启动7组件，主线程掩码均继承housekeeping `0-5`，不新增独占核：
trade_engine283718、account_monitor283860、pre_trade284154、viz_server284256、
config284526、fr_signal_dashboard284787、persist_manager302412。
账户暂无USDT，仅BGB约0.1442 USD，交易所确认零永续/挂单/负债；
`TRADE_ENGINE_READ_ONLY=true`，trade_signal保持停止，入金后另行启用交易。
恢复NAV/CTA只读目录、Delist白名单和原20051/20151/20161入口；NAV291158仍CPU24，
Delist291287及既有SG隧道仍CPU28，CTA3909484仍CPU24。新同步source为
`bitget_fr_arb01_20261110`，监听回环50082；6382仅预留，旧50057保持不可用，
center历史source及collector未改。初次监听遇到临时端口占用，经环境脚本仅重启
目标persist_manager后监听及gRPC空库校验通过。117个其他采样进程身份/CPU保持；
同期OKX MM的2项进程变化见其独立实盘启用记录，未计入本次保持证明。
本机证据：`data/ops/bitget_fr_arb01_restore_20261110/`；私有回退/维护包：
`~/maintenance/bitget_fr_arb01_restore_20261110/`。入金不作为交易收益，新盘不回溯旧盘历史。

2026-11-10 08:01–08:07 UTC，按操作员明确指令开启 `okex_mm_83777` BTC/ETH
USDT永续实盘。先核验UID556411065144578537、零挂单/仓位/成交及两币cross杠杆5，
确认vol输入持续更新、70分位开仓限制开启、Tlen关闭，再将环境
`TRADE_ENGINE_READ_ONLY=0`、`TRADE_SIGNAL_NO_EMIT=0`；二进制及原引擎SHA-256
启动校验保持。本地 `arbmm` 与 `origin/arbmm` 同步，无构建或其他产物发布。
只经环境本地脚本先停signal、重启引擎PID291094/CPU32，再启动signal
PID291515/CPU30；账户监控PID275138/CPU29、pre_trade PID424228/CPU31及
持久化PID421889/CPU15保持。signal重启后按原规则积累至少10个vol样本，随后
两币真实挂单及maker成交已由交易所确认，08:07已见ETH11笔/BTC6笔maker fill，
两币各5轮MMHedge query/reply正常，账户风险为 `free_trade`。维持单笔50U、
单币500U及内部杠杆上限5；未改源IP、
共享行情或其他环境。环境私有回退为 `~/okex_mm_83777/backups/go_live_20261110/env.sh`，
核验回执在同环境 `run/go_live_20261110/`，本地副本 `data/ops/okex_mm_live_20261110/`。

2026-11-10 07:54–07:57 UTC，按操作员要求关闭 `okex_mm_83777` 的 Tlen，
BTC/ETH USDT 永续 cross 杠杆及环境内部 `max_leverage` 均设为5；交易所及
Redis回读、signal热加载和风险快照均核验。修复OKX空仓 `mgnRatio=""` 被当作0：
仅明确 `mmr=0` 且调整后权益为正时使用有限安全值，保留明确低比率及异常数据的
原处理。基于同步 `arbmm a00dc38f` 加本次解析器补丁的独立本机构建快照发布，
未混入工作区其他改动；193项解析器测试及release通过，精确SCP上传、SHA-256
核验、原子安装后仅经环境本地脚本重启账户监控，PID275138仍CPU29。
引擎/pre_trade/signal仍PID422890/424228/426569、CPU32/31/30；185个存活
受保护进程的可执行文件/CPU mask核对未变。线上风险恢复 `free_trade`，零仓位、
零敞口；保留 `TRADE_ENGINE_READ_ONLY=1`、`TRADE_SIGNAL_NO_EMIT=1`，未开启交易。
`env.sh`、`trade_engine.toml`、CPU分配及 `.229/.230` 源IP不变。
证据 `data/ops/okex_mm_readiness_20261110/`，远端回退二进制及核验元数据在
`~/okex_mm_83777/backups/readiness_20261110/`，运行回执在同环境 `run/readiness_20261110/`。

2026-11-10 04:36 UTC，CTA Prep/Spread配置入口修复额外Delist登录导致的401，
沿用Manager会话及全部相关账号的configure权限。仅替换共享Nginx片段并平滑重载；
Nginx主进程仍1862269，97个受保护进程的PID/绑核及35份配置/二进制哈希未变。
未改CPU分配、源IP或策略参数，回执见 `artifacts/cta_factor_config_auth_20261110/`。

2026-11-10 04:01 UTC，Spread01整体替换为12币、209条规则，按1min/5min/15min/1h
维护28/56/18/107条规则及四张仓位表，不设券池；215项共享计算仍在CPU18。
旧两侧仓位及IOTA -0.1尾仓已按用户确认全部成交平掉，交易所确认挂单/仓位为0；
旧订单库、Redis分仓和因子账本已归档后重置。Manager定向移除1246条旧目标记录，
核对同范围其他1,342,639条目标未变；共享服务恢复后已完成缓存初始化。
12币×768根（9216根）原生小时输入已安装，导入检查88/88项普通因子就绪，
worker已从保留Kafka追平，1h109/109、1min35/35、5min46/49、15min21/22项就绪；
四张表均已开启，两侧8条绑定各跟随1份，每规则100U，执行模板仍为ctaker。
OKX/Binance两侧执行进程已恢复且position_ready=true。source ID、namespace及
local_ips沿用：OKX `.229/.230`，Binance `.228/.228`。完整回执与最终启用状态见
`artifacts/cta_spread01_replacement/`。

2026-11-10 02:29 UTC，修复 FR 账户目录偶发超时。NAV `c8cb8fa` 的现有
`/api/strategies` 新增 `include_credentials=false`：保持原有登录/账户授权筛选，
仅跳过环境文件及远端 SSH 凭据检查；Delist 生产 `arbmm cc25b455` 使用该模式。
均由本机构建、SCP 暂存、SHA-256 校验后原子安装；NAV PID3764734 保持 CPU24，
Delist PID3773328、SG 隧道 PID3773302 全线程保持 CPU28，仅经环境本地脚本重载。
95个受保护交易/行情进程身份及CPU mask、20份Delist配置/脚本指纹保持。
12项目录中的10套FR快照完整，通知队列为空；30次元数据查询中位1.298ms、最大3.586ms。
现有凭据查询字段保留，匿名元数据查询仍401；未修改交易参数、source IP、路由或监听。
构建、测试限制和核验见 `data/ops/fr_catalog_20261110/`；回退包为
`~/delist_risk_server/backups/fr_catalog_cc25b455_20261110/` 与
`~/crypto_nav_manager/maintenance-backups/fr_catalog_20261110/`。

2026-11-09 14:28 UTC，1h已并入现有 `cta-prep-binance-futures` 并启用。
主进程PID2168358继续使用CPU17，13份组合包含原9份分钟配置与4份小时配置，共596项计算；
原仓位账本保留，Kafka回放完成，小时另导入34标的×744根历史，264/265项就绪。
临时独立小时进程已通过环境脚本停止并归档，CPU19未占用，没有新增独占核。
原config server PID2169023继续使用19220，小时配置与分钟配置位于同一目录；
四张小时表已启用，RX01每规则50U、RX03每规则100U，grid跟随1份；绑定分别4条/9条。
原分钟配置与绑定保持，260/331项计算就绪，其余继续等待；启用未再次回放，
小时从15:00 UTC新封bar开始决策。RX03 1min继续下架，无活动委托/非零目标，
16条低于最小下单量的真实尾量约32.35U保留。交易仍使用原账户与source IP。
备份和回执：`~/cta-hourly-history/merged-deployment/`，脱敏核验见
`artifacts/cta_independent_1h/verified.json`；临时目录归档在
`~/retired_data/cta-prep-binance-futures-1h_20261109T140207Z/runtime/`。

2026-11-09 10:14 UTC，JP RX03 / `binance_exec_rx03` 的 Prep02 三券池1min已下架。
58条规则、三份组合与三条跟随绑定移除，六条5min/15min绑定及其他组合保持。
原策略完整clear目标执行完成，无活动委托、批次或非零目标；22条低于交易所最小下单量的
分仓尾量约35.33U及原归属保留，未伪造平仓。进程未重启，CPU、source IP、`local_ips`、
ENI/EIP、路由、监听及env.sh保持。备份和脱敏核验见
`artifacts/cta_rx03_1min_retirement/README.md`。

2026-11-09 09:47 UTC，JP Delist发布生产 `arbmm bc28e9a7` 的内部Viz直连修复。
本地核验3051个源码文件，隔离无关工作区改动，以 `--release --locked` 构建；上传显式暂存路径、
核对SHA-256并原子安装后，仅通过环境本地脚本重启Delist和其SG监控SSH隧道。
Delist PID1609111、隧道PID1609083，全部线程继续CPU28；Redis PID2769589及绑核保持。
新二进制SHA-256：`4641987e12ace0ff05e9ba05d429168eba93a4b7e3a67ceb9c7f3f8fcb54f33d`。
净敞口、下架仓位、人工flatten预检和移除名单前复核统一从显式账户映射直连Viz，
校验返回namespace；不再经过4191及Delist自己的鉴权。JP九套直读原Viz端口，SG Bybit
增加本机 `127.0.0.1:24141 -> sg:127.0.0.1:20141`，现有16379 Redis及14191网页转发保留。
10套目标CLI校验和实际TCP连接确认通过，当前10套快照完整、NAV目录current、巡检无数据错误；
下架dump/flatten源恢复正常，发布后清仓执行审计新增0条。未改网页登录及单账户授权规则，
未登录/status仍401。原16个配置/页面文件、控制参数、采样Redis摘要保持；
其余172个基线进程身份/CPU mask保持，基线中5个VSCode开发会话进程已退出。
保留线上启动脚本原有CTA路径参数差异，仅合入本次快照配置和SG Viz转发；未重启交易栈。
库测试1522通过/3忽略，Delist服务69项通过；回退为
`~/delist_risk_server/backups/direct_viz_bc28e9a7_20261109/`，本机构建及前后核验在
`data/ops/delist_direct_viz_20261109/verification.json`。

2026-11-09 08:06 UTC，JP `binance_fr_arb02/03/04` 按要求统一重发完整二进制栈。
本地从已同步的生产 `arbmm ecd0c24f` 导出并核验3049个源码文件，以 `--release --locked`
构建；SCP显式暂存路径上传、SHA-256核对、逐套停止及原子安装后，通过环境本地脚本启动。
每套7个组件均核验真实运行文件，3套21个进程的文件哈希与本次构建一致：账户监控、引擎、
pre_trade、信号、持久化、Viz和FR dashboard。包含 `bf3f5698` 配对平仓敞口修复；
FR02旧监控的68字节AccountRisk无法被新pre_trade读取，此次统一为当前76字节协议，
3套账户风险解码、实时快照、交易所UniMMR对照及双腿启动快照全部通过，再恢复信号。
pre_trade SHA-256 `43bebeaf4c4c34e1c2a292426ea9bffeae848d2a8715d0f774c8cd8c305d2a07`；
Binance监控 `913f845bb42eb8abec4da776fc5cf45a8deb74db84ad627a0e82dbfece6f2eb3`。
3套Redis风险/策略/覆盖键摘要、233个已采样配置/脚本指纹和源IP保持。
持久化仍CPU15，其他组件CPU mask仍`0-5,48-95`，未调整隔离核。

| 环境 | account_monitor | trade_engine | pre_trade | trade_signal | persist_manager | Viz | FR dashboard |
| --- | --- | --- | --- | --- | --- | --- | --- |
| binance_fr_arb02 | 1351058 | 1349609 | 1349709 | 1352104 | 1349305 | 1349163 | 1351118 |
| binance_fr_arb03 | 1360327 | 1359115 | 1360207 | 1361840 | 1357905 | 1357543 | 1360385 |
| binance_fr_arb04 | 1401002 | 1399632 | 1399918 | 1403306 | 1399093 | 1398715 | 1401064 |

停信号后先撤现货首腿，核对真实挂单和在途。信号同时回复对冲报价，停止后不能假定旧
对冲还能持续重报价；FR04遗留OPEN约99U净多因此在旧pre_trade退出后另用每单不超过50U
的GTX限价卖单收尾，实际成交1163枚、3条成交均maker、0条taker，净差剩约0.057U，双边挂单0。
3套常规配对仓位及原参数保留；FR01继续退役；Gate/Bitget本轮未发布。
其余154个基线进程身份/CPU mask保持，含共享Exec02六个进程；
另外2个原NAV `sync_history` 批处理已结束，未计入长期进程保持数。
协议相关定向测试5项通过；Viz配置检查及全部运行文件校验通过。
旧7个二进制及回执：`~/<env>/backups/fr_full_release_ecd0c24f_20261109/`；
构建、前后审计、OPEN成交证据及最终核验：`data/ops/fr_full_release_ecd0c24f_20261109/`。

2026-11-09 07:12 UTC，退役 JP `binance_fr_arb01` 的FR运行环境。原FR进程已全部停止，
原生挂单0；与运行中 `binance_exec_trade02` 共用标准账户，保留其账户资产、配对持仓和
Exec六个进程/配置，未向交易所发单或清仓。旧运行目录移至私有
`~/retired_data/binance_fr_arb01_20261109/runtime/`，128个运行文件逐项核验；16个订单文件
另打包回传本机，压缩包和逐文件SHA-256一致，凭据只保存在远端私有归档。
清理旧NAV读取授权、source-IP环境归属、已禁用monitor source和Manager两条旧路径；
旧FR网页各入口均410。Manager重载为PID1276059/CPU24，活动/目标发布验证正常；
122个其他进程身份/CPU mask保持。未释放独占核，共享`.231`出口保留。
核验及订单归档：`data/ops/binance_fr_arb01_retire_20261109/`。

2026-11-09 06:34 UTC，完成 JP `gate_fr_arb02/03` 和 `bitget_fr_arb02/03` 配对平仓敞口修复滚动发布。
复用已核验的本地 `arbmm bf3f5698` release产物，仅原子替换 `pre_trade`：
`a4786154427bcfee7a4a22486874b12731fefedbc92fa26d472f6a6777576c2a`。
逐套通过环境本地脚本停信号，撤销未成交现货首腿挂单，保留原对冲进程处理在途；
确认原生现货/合约挂单均0及无新增大额双腿净差后安装，再启动pre_trade并等待双腿账户
快照、风险页面就绪，最后恢复信号。四套风险参数、已采样覆盖键摘要和114个运行文件保持。
四套兼容Viz运行文件均已为`bed95325…ebb7de`，继续保留原PID；引擎、账户监控、持久化、
Viz及FR dashboard共20个受保护进程身份/CPU mask保持，persist仍CPU15。
新pre_trade及信号全部CPU mask仍`0-5,48-95`，全部运行文件校验与实时风险快照验收通过。
Bitget 01运行目录已退役，本次只处理仍在线的02/03；Gate01/04继续运行。

| 环境 | pre_trade PID | trade_signal PID | 原总敞口比例 | 原单币比例 |
| --- | --- | --- | --- | --- |
| gate_fr_arb02 | 1181605 | 1183891 | 0.005 | 0.005 |
| gate_fr_arb03 | 1190196 | 1190519 | 0.1 | 0.2 |
| bitget_fr_arb02 | 1194727 | 1195059 | 0.05 | 0.02 |
| bitget_fr_arb03 | 1197491 | 1199763 | 0.05 | 0.02 |

每套旧二进制/发布回执：`~/<env>/backups/fr_remaining_patch_bf3f5698_20261109/`；
本地四套前后审计及最终运行状态：`data/ops/fr_remaining_patch_bf3f5698_20261109/summary.json`、
`data/ops/fr_remaining_patch_bf3f5698_20261109/final_live_inventory.json`。

2026-11-09 06:04 UTC，JP `gate_fr_arb04` 核对临时maker净差对齐已结束，再按原参数恢复正常交易。
启动前原生现货/合约挂单均0，绝对净差合计39.52 U、单币最大4.69 U，42个配对仓位保留。
整份Redis风险参数摘要与操作前一致：max_pos_u=150000、max_leverage=1.5、总敞口0.005、
单币0.01、UniMMR触发/恢复3.4/3.5、应急3.0/3.1，无需重写参数。
经环境本地脚本启动trade_engine1122891、修补版pre_trade1124318、trade_signal1125346，
全部CPU mask仍`0-5,48-95`；Gate四路交易WS登录通过，账户快照就绪，正常开/平仓信号恢复。
验收发现旧Viz可更新敞口明细但无法解码新版风险快照，总数仍缓存停机前数据；从同一
`arbmm bf3f5698`源码快照本地构建`viz_server`，临时上传、SHA-256核验及原子替换后，
通过环境本地脚本重启Viz为PID1136414，CPU mask仍`0-5,48-95`，配置加载通过。
新Viz SHA-256：`bed95325407447614e93733ff711d0bbb6ee3ab13d1762b8057bdc0e42ebb7de`；
风险快照恢复实时、重启后解码错误0。验收时真实净敞口146.15 U、页面144.61 U，含新交易在途。
原有账户监控3841051、持久化3839213（CPU15）及FR dashboard3841289保持；28个已采样
配置文件、风险参数摘要及`.231/.231`源IP保持。旧Viz备份与pre_trade回退同目录：
`~/gate_fr_arb04/backups/paired_close_bf3f5698_20261109/`；本地核验：
`data/ops/gate_close_patch_bf3f5698_20261109/restarted_arb04.json`。

2026-11-09 05:52 UTC，JP `gate_fr_arb01` 经环境本地脚本恢复trade_engine、修补版pre_trade
和trade_signal；PID分别1101274、1101290、1102715，CPU mask均`0-5,48-95`。
pre_trade仍为 `bf3f5698` 构建的`a4786154…7576c2a`，Gate四路交易WS登录通过，
启动账户快照完成。原有账户监控、持久化及Viz保持运行，`gate_fr_arb04`交易栈继续停止。
恢复前Viz缓存停在04:19、总敞口约13144 U；交易所真实绝对净差约69 U、单币最大4.85 U。
恢复后Viz时间戳更新，正常开/平仓信号及交易执行已恢复；OPEN现货减仓1748枚约155 U，
对应Gate合约已挂112张和62张reduce-only买回maker单，8枚余量低于合约步长待合并。
该记录时第二腿尚未全部成交，不能将首腿成交计作双腿完成。总敞口阈值0.005、单币0.008
和UniMMR 3.5/3.6均保持，当前UniMMR约3.9，未触发低UniMMR主动减仓。
未改源IP/隔离核，核验：`data/ops/gate_close_patch_bf3f5698_20261109/restarted_arb01.json`。

2026-11-09 05:39 UTC，JP `gate_fr_arb01` / `gate_fr_arb04` 发布配对平仓敞口修复。
在本地维护工作区内从已推送 `arbmm bf3f5698` 导出并逐文件核对3048个源码文件，
使用锁定依赖构建release；仅SCP上传并原子替换两套 `pre_trade`，SHA-256均为
`a4786154427bcfee7a4a22486874b12731fefedbc92fa26d472f6a6777576c2a`，远端CLI加载通过。
两账户的trade_signal、pre_trade、trade_engine继续停止，临时maker对齐执行器已结束；
交易所现货/合约挂单均为0，单币最大净尾差约4.85/4.68 U，绝对净尾差合计68.84/39.48 U。
两套共10个原有常驻进程身份及CPU mask保持：persist仍CPU15，其余延续原housekeeping。
55个已采样运行文件及Redis风险参数摘要保持，总敞口阈值均0.005，单币分别0.008/0.01；
交易源IP仍`.231/.231`。每套旧二进制与发布回执位于
`~/gate_fr_arb0{1,4}/backups/paired_close_bf3f5698_20261109/`；本地构建及核验记录：
`data/ops/gate_close_patch_bf3f5698_20261109/verification.json`。

2026-11-09 02:35–02:50 UTC，退役 JP `bitget_fr_arb01`。通过环境本地脚本停止
并移除其8个进程，撤销10笔ZBT现货挂单；21笔reduce-only永续市价平仓及1笔ZBT
现货市价卖出共53条成交，逐单数量相符且全部为taker。交易所复核永续、挂单、
负债均为0；仍有20种低于数量步长的现货零头，合计约0.0966 USD，不能用市价单
卖出。原订单库187个文件打包为294 MiB tar.gz并回传本机，归档及逐文件SHA-256
全部匹配；运行目录移至 `~/retired_data/bitget_fr_arb01_20261109/runtime/`。
NAV/CTA活动登记及相关读取授权移除，Delist目录由13项降至12项、FR入口白名单
移除该环境，旧网页各入口HTTP410；停止6357同步转发。重载NAV和Delist监控，
未重启其他交易栈；Delist PID705377、SG隧道PID705321仍全部CPU28。CTA Manager
移除K线/资金费率两处旧环境配置路径后重载，PID718583仍CPU24；121个其他交易
及行情进程身份保持。PM2保存的启动清单及其备份也仅删除该环境的3项旧登记。
该环境原进程在housekeeping掩码上运行，未释放独占隔离核；共享`.231`出口保留。
本机订单与清仓核验：`data/ops/bitget_fr_arb01_retire_20261109/`。

2026-11-06 15:29–15:40 UTC，JP Manager 发布 `b8eb962` 的目标推送异步 PG 副本。
既有本地工作区 release `--no-default-features` 构建，按上次线上源码清单核对保留
既有功能；SCP临时路径、SHA-256核验及6个来源的不可变分仓指纹预检通过后原子
安装，只替换 `cta_web` 并重启其原 systemd 用户服务。新 PID1475583，全部线程
仍 CPU24、NRestarts=0，8个可发布来源的启动账户模式检查通过，理论索引已就绪。
验收窗口785次自然请求全200，P95 25ms；其中rbf_small 129次全成功，两账户Chase
通道的33个目标与模板乘份数一致。12个活跃策略的PG副本追齐，27条绑定及11张
配置/不可变快照表摘要保持。25份非二进制运行文件、前端指向及215个其他长期
进程身份/存活原线程CPU mask保持；一个sleep辅助进程结束。Redis/Delist继续CPU28，
未改交易源IP、网络或交易进程。回退及核验分别在
`~/crypto_cta_manager/backups/target-pg-async-20261106T152335Z/` 与同名 `run/`。

2026-11-06 14:39–14:40 UTC，按操作员要求将 JP Delist 与轻载 Redis 共用 CPU28。
先在线迁移全部线程，再仅通过 `~/delist_risk_server/scripts/start_delist_risk_server.sh`
重启 Delist 和其 SG 隧道，验证持久化：Delist PID1367608、隧道 PID1367582，
全部线程均 CPU28；环境新增 `env.sh` 的 `DELIST_CORE=28`，启动脚本经 taskset
启动两者，PM2 自动重启也保留绑定。线上脚本仅增 CPU 前缀，保留其 CTA 参数差异。
Redis PID2769589、7个线程及 systemd CPUAffinity=28 保持，无 Redis 重启。
214个其他长期进程身份及仍存活的原线程 CPU mask 保持；二进制、私有配置和网络未改。
迁移后200次长连接只读 PING 全通过，P95 0.349ms、最大2.991ms；JP/SG参数
监控恢复、11个FR账户快照完整、通知队列为空。shell语法及无绑核/绑核/无效CPU
三种模拟启动验证通过。回退与核验：`~/delist_risk_server/run/cpu28-20261106T143858Z/`。

2026-11-06 08:43–08:48 UTC，发布全部参数页变更审计和钉钉通知。JP 只重启
Delist/其托管隧道、CTA配置、10个FR配置及OKX MM83777配置，SG只重启Bybit FR配置；
全部使用环境本地脚本。Delist PID594573、隧道 PID594544、CTA配置 PID602445保留
CPU0–5，SG Bybit配置 PID1016740保留CPU0–7；其余配置进程保留各自原mask。
JP 130及SG 12个其他运行进程身份和CPU mask均保持，未改交易参数、隔离核或源IP。
所有配置后端收回loopback，MM补Delist登录，CTA保留Manager权限并增加Delist前置。
release精确上传、SHA-256核验及原子安装；JP196/SG20个参数对象基线正常、无启动
参数通知。详见 `docs/delist_risk_server.md`；证据
`../artifacts/parameter_changes_20261106/`，私有回退
`~/delist_risk_server/run/parameter-changes-20261106/`。

2026-11-06 07:26 UTC，JP `okex_mm_83777` 换用独立 OKX 子账户
UID `556411065144578537`，`.229/.230` 两个交易出口均只读鉴权成功。按操作员要求
直接删除该环境旧观察订单库（874898字节），新 RocksDB 独立重建，未归档旧库；
Redis 风控、币种及策略配置保持。通过环境本地脚本恢复 Config、公共利率缓存、Viz、
persist_manager、trade_engine、pre_trade、account_monitor 和 trade_signal 共8个进程。
引擎 PID422890/CPU32，pre_trade PID424228/CPU31，监控 PID424427/CPU29，
信号 PID426569/CPU30，持久化 PID421889/CPU15；Viz PID421759、Config PID413152、
利率缓存 PID413204 使用 housekeeping。所有进程新凭据及 `TRADE_ENGINE_READ_ONLY=1`、
`TRADE_SIGNAL_NO_EMIT=1` 已核验，日志确认拒绝交易和抑制信号，MM 路径不设杠杆。
公网 Config/Viz/snapshot 均HTTP200，新账户及页面均零仓位/敞口，零挂单，启动后零成交；
交易账户现有约1000 USDT，本次未划转资金。145个受保护进程身份/CPU mask及
Exec/Intra、共享采集中心配置、原二进制保持。未改网络；中心 collector 未接入此 MM。
核验记录：`~/okex_mm_83777/run/observe_account_20261106T071851Z/result.json`。

2026-11-06 07:15 UTC，JP `~/delist_risk_server` 发布MM参数备份；本地同步arbmm
`103b1333`加本次补丁构建release，上传核验SHA-256并原子安装，环境本地脚本重启。
Delist PID406318、SG隧道 PID406290，CPU均为 `0-5,48-95`；156个既有服务进程
身份/CPU mask及既有FR/CTA归档哈希保持。未操作MM启停或写入其参数，保留操作员
同期对MM凭据和进程的变更。独立 `data/mm_backups` 保存参数及env/config文件；
COS新顶层前缀403后改用既有授权树下 `fr-config-backups/mm-config-backups`。
`okex_mm_83777` 首份日归档7:11 UTC完成，9个Redis键中3个核心键存在、6个可选
覆盖键缺失，5个配置文件；逐项校验及独立云端回读通过，下一自然5分钟轮次通过。
本次真实云端失败/恢复各钉钉确认一次；当前3项MM备份状态正常、发送队列为空。
62项Delist测试（含隔离Redis真实恢复）、shell/格式及320–1440px浏览器验证通过。
源IP与隔离核分配保持；详情 `docs/delist_risk_server.md`，证据
`../artifacts/mm_parameter_backup_20261106/`，回退及核验
`~/delist_risk_server/run/mm-parameter-backup-20261106/`。

2026-11-06 06:54 UTC，按操作员要求替换 JP `~/okex_mm_83777/env.sh` 的三项 OKX
凭据，权限保持 `0600`；只改凭据，保留 `TRADE_ENGINE_READ_ONLY=1` 和
`TRADE_SIGNAL_NO_EMIT=1`。新凭据在现有 `.229/.230` 出口的只读账户查询均返回
`50110`（IP 不在白名单），新账户 UID 尚未确认。通过该环境本地脚本停止 signal、
pre_trade、trade_engine、account_monitor、persist_manager、Viz、Config 和利率缓存，
现场确认无残留 MM 进程；CPU29–32 保留，未恢复观察或交易。145个受保护进程身份及
CPU mask、Exec/Intra 的 env.sh、MM 二进制和 trade_engine.toml 均保持。
旧 MM 持久化保留为原账户证据，恢复前须确认新 UID 并隔离不同账户历史。
原配置及核验记录位于
`~/okex_mm_83777/backups/credential_rotation_20261106T065308Z/`；未修改网络或共享行情。

2026-11-06 06:38 UTC，JP `~/delist_risk_server` 发布当前共享 CTA 备份的钉钉告警。
本地同步arbmm构建release，临时路径上传、SHA-256核验并原子安装，环境本地脚本
重启；Delist PID320612、SG隧道 PID320586，CPU均为 `0-5,48-95`。
163个其他服务进程身份/CPU mask保持，私有配置与现有CTA归档哈希保持。
复用现有钉钉机器人和持久化outbox，当前Prep/Spread的本地归档、完整性、COS
上传回读及保留清理失败分别首报与恢复；连续失败不重复，通知失败持久化重试。
59项Delist测试通过，线上鉴权配置只校验未发送测试消息；8个CTA备份状态正常、
发送队列为空。源IP与隔离核分配保持；证据
`../artifacts/cta_backup_alerts_20261106/`，回退产物及核验在
`~/delist_risk_server/run/cta-backup-alerts-20261106/`。

2026-11-06 06:18 UTC，JP `~/delist_risk_server` 发布共享 CTA Prep／Spread 备份迁移。
本地同步 `arbmm bf0d5c94` 加本次备份补丁构建 release，经临时路径上传、SHA-256
核验及原子安装，沿用环境本地脚本重启。Delist PID274437、SG隧道 PID274411，
均 CPU `0-5,48-95`；163个其他进程身份、CPU mask及全部Delist配置哈希保持。
新归档覆盖12个Prep组合、1个Spread组合及4个关联Exec，校验云端回读后删除
旧4个signal环境的10份本地归档/回执与16个COS对象，并清理本次初始验收版本。
两套备份/完整性/上传/保留状态通过；57项Delist测试及320–1440px浏览器回归通过。
未改源IP或隔离核分配，详见 `docs/delist_risk_server.md`；证据
`../artifacts/cta_shared_backup_20261106/`，回退产物及删除清单
`~/delist_risk_server/run/cta-shared-backup-20261106/`。

2026-11-06，`okex_mm_83777` 全套启动为观察模式：交易引擎只读拒绝全部交易
请求，trade_signal 不发布信号；账户监控、pre_trade、持久化、Viz、Config 运行。
account_monitor/trade_signal/pre_trade/trade_engine 分别绑 CPU29/30/31/32，
持久化在 CPU15；同账户 `okex_exec_trade01` 保持原进程与绑定。另启动只读行情
`trade_flow_feature/okex-futures` 和本地 OKX 借贷利率缓存，补入 BTC 行情阈值。
持久化 PID4117078 已加载3个 MM 币种，行情为 `okex-futures`；信号 PID4096168、
引擎 PID4075038、pre_trade PID4076675、监控 PID4078598，均核验绑核及只读开关。
环境启动时核验只读引擎 SHA-256，防止后续覆盖成未验证的二进制后静默启动；
更新引擎时须先验证只读拦截，再同步更换 env.sh 中的校验值。

2026-11-06 02:32 UTC，旧 `okex_mm_alpha` 的订单和清仓证据归档由 JP 迁至本机
`/mnt/nvme-raid0-28t/fanghaizhou/order_archives/okex_mm_alpha_20261029T071005Z/`；
26 个文件逐项 SHA-256 一致、两份压缩包校验通过后删除 JP 原归档目录。未操作进程。

2026-11-06 02:28 UTC，按操作员要求将 JP 共享 `redis-server.service` 的全部7个线程
从 housekeeping CPU0-5 在线迁到独立物理核CPU28，包括主线程及AOF fsync后台线程。
Redis PID2769589、启动标识和NRestarts=0保持；294个受保护进程身份及既有线程绑核
均核对保持。CPU28属于既有隔离段，SMT sibling已下线；29-32仍保留给已配置的MM。
新增 `/etc/systemd/system/redis-server.service.d/cpu-affinity.conf`，清除继承列表后设置
`CPUAffinity=28`，`daemon-reload`核验有效值28；未重启Redis或其他服务。
原Redis配置、AOF everysec策略及原服务单元保持，备份及线程基线在
`~/crypto_cta_manager/backups/redis-cpu28-20261106T022854Z/`。
同内容配置模板在Manager仓库 `deploy/jp_meta/redis-server.service.d/cpu-affinity.conf`。
迁移后至02:31:53 UTC，自然绑定发布633次成功、0次失败，其中rbf_small/trade03
成功58次，绑定发布最大耗时17ms；125秒内连续1241次只读PING全部成功，P95约0.098ms、
最大3.663ms，无新增AOF fsync延迟告警。复核7个Redis线程仍全在28、294个受保护
进程身份及既有绑核保持。这是短期运行观测，不据此认定间歇性故障已永久消除。

2026-11-06 02:24 UTC，修复旧 MM 目录删除后 Manager 的 K 线和 funding IP 排除列表
仍指向旧配置的问题；两处改为 `okex_mm_83777/trade_engine.toml`，其余配置未变。
仅重启 Manager，PID3952558、CPU24；交易进程未重启。旧地址覆盖仍保持，专用 K 线
出口不变。Manager 重启后理论索引需要重新预热。远端原配置备份在
`~/crypto_cta_manager/config/backups/mm_83777_exclusion_20261106T022354Z/`。

2026-11-06，JP 新建 `okex_mm_83777`，账户别名 `okex_83777`，取代已停止的
`okex_mm_alpha` 槽位，旧目录已移除且订单归档校验保留；本地同步 arbmm `bf0d5c94` 构建六个二进制，上传校验后
原子安装。新环境预留 account_monitor CPU29、trade_signal CPU30、pre_trade
CPU31、trade_engine CPU32、persist_manager CPU15；当时所有进程停止。使用独立
namespace 和空订单库，config/viz 复用 18112/10212，未启动交易。该账户同时由
`okex_exec_trade01` 使用，不能同时启动 MM 与 Exec 管理相同现货库存。

2026-11-05 03:40–03:41 UTC，Gate arb01最后一笔O对冲成交，连续三次原生零挂单、
停信号后再次核验零挂单，仅剩净尾差合计140.47 U、单币最大27.90 U，随后发布
与arb02相同的本地arbmm `6a5d6416` 构建产物；SCP临时路径、SHA-256核验及原子安装。
pre_trade PID1027075、trade_signal PID1027183，均为 `0-5,48-95`；
其他136个受保护进程身份保持，persist仍CPU15，`.231/.231`源IP和网络保持。
env.sh、已采样配置、风控参数和名单均核对保持；未强制taker或人工平仓。
Gate arb01/02现均完成现货估值修复发布并恢复信号；仅替换pre_trade，账户监控、
交易引擎、持久化及行情服务均未重启。03:45 arb01为61个已估值资产、BGSC合约mark
仍未知；arb02为50个已估值、未知0。现货补价无请求错误，缺价不再阻断其他资产巡检。
验收及哈希见
`../artifacts/gate_spot_valuation_release_20261105/README.md`。

2026-11-05 02:53 UTC，JP `gate_fr_arb02` 发布现货估值/逐资产集中度修复。
本地同步 arbmm `6a5d6416` 构建 `pre_trade`，SCP 临时路径上传、SHA-256 核验后
原子替换；停信号前连续三次、停信号后再次确认原生现货/合约挂单均0。
仅重启 pre_trade PID925127 和 trade_signal PID925266，CPU mask 均为
`0-5,48-95`；其余136个受保护进程身份保持，persist仍CPU15。
env.sh、已采样配置、风控参数及名单保持；原 `.232/.233` 出口和网络保持。
集中度按分钟刷新，SLVON/TSLAX/CRCLX已补价；个别合约mark缺失时仅保留对应
资产未知状态，其他资产继续检查，pos_dump未新增。
arb01首次因验收只检查stdout、遗漏stderr启动日志而自动回退并恢复信号；
验收修正后等待策略在途双腿平仓至03:40，最终发布见上段，未强制taker或人工平仓。
进展及最终验收见 `../artifacts/gate_spot_valuation_release_20261105/README.md`。

2026-11-04 12:55–13:02 UTC，Gate arb01最后一笔对冲收尾后，两次原生确认零挂单、
最大净尾差约11 U，再经环境本地脚本停止基础组件，完成最后一套ADL修复发布。
仍使用本地arbmm `038e1dac` 构建并冻结的同一批6个release产物，SCP临时路径上传、
SHA-256核验及原子安装；env.sh、全部已采样配置、账户模式和原风控参数保持。
Config PID3354192（20021）、Viz3354337（20121）、persist3356694（CPU15）、
trade_engine3357145、pre_trade3357408、account_monitor3359755、trade_signal3360357；
原dashboard61710（20131）保持，除persist外均CPU mask `0-5,48-95`，源IP仍`.231/.231`。
Gate arb01/02/03现已全部恢复信号，4路交易登录、6路账户订阅及主备futures.usertrades
均确认正常；原生小数张数、官方乘数和Viz持仓换算核对通过。最终三套净尾差合计分别
95.01/122.70/64.98 U，单币最大11.50/15.10/22.62 U，现货及合约挂单均0；
末次一分钟243/184/126条信号、无新增运行错误，实际PID稳定。原有TAKE/SFP借贷费率
缺失保持缺数据拦截，未伪造费率或改变券池；FTM/MATIC纳币级无价尾量单列保留。
Bitget/Gate arb04的24个受保护进程身份保持，网络、CPU分配及共享因子未改。
本次未触发真实ADL交易测试。统一备份路径、最终PID、哈希及验收见
`../artifacts/gate_fr_adl_release_20261104/README.md`。下方12:42/12:51为分阶段发布记录。

2026-11-04 12:44–12:51 UTC，Gate arb02的O、BANK及最后一笔合约对冲收尾后，
再次原生确认零挂单、最大净尾差约15 U，再停止基础组件并发布ADL修复。
安装与arb03相同的6个本地release产物，经SCP临时路径、SHA-256核验及原子安装，
环境本地脚本恢复基础服务和原有信号；env.sh、全部已采样配置及账户设置保持。
Config PID3319943（20022）、Viz3320213（20122）、persist3320546（CPU15）、
trade_engine3323074、pre_trade3323443、account_monitor3323698、trade_signal3326279；
原dashboard62454（20132）保持，除persist外均CPU mask `0-5,48-95`。
4路交易登录、6路账户订阅及主备futures.usertrades正常；小数张数/乘数核验通过，
净尾差合计123.00 U、单币最大15.17 U、现货和合约挂单均0；FTM/MATIC的纳币级
旧尾量保持并单列为无价，不伪造金额。原`.232/.233`出口及既有默认路由`.228`外连保持。
arb03继续运行，arb01仍等待两笔已有对冲，尚未发布；Bitget/Gate arb04受保护进程保持。
12:51末次一分钟91条信号、无新增运行错误；合约挂单0、恢复信号后正常现货挂单10笔。
备份及验收见 `../artifacts/gate_fr_adl_release_20261104/README.md`。

2026-11-04 12:28–12:42 UTC，JP `gate_fr_arb03` 发布ADL现货修复并恢复原有信号。
本地同步、干净arbmm `038e1dac` 构建6个release产物，经标准publish的SCP临时路径、
逐项SHA-256校验及原子安装，启停均经环境本地脚本。停止前/停信号后均确认
现货及合约挂单0，再停止基础组件；env.sh、全部已采样配置与账户设置保持。
Config PID3275510（20023）、Viz3275682（20123）、persist3278090（CPU15）、
trade_engine3278567、pre_trade3279685、account_monitor3281204、trade_signal3287955；
原dashboard63312（20133）未重启，除persist外CPU mask均为`0-5,48-95`。
4路交易连接、6路账户订阅及主备futures.usertrades正常；原生小数张数/乘数核对通过，
净尾差合计65.23 U、单币最大22.74 U，现货/合约挂单0，末次一分钟105条信号、无新增运行错误。
TAKE/SFP借贷费率缺失在发布前12:00已存在，继续按原逻辑拦截相关缺数据标的。
Gate arb01/02按用户要求仍等待既有对冲收尾，尚未发布；O/BANK大额对冲已完成，
arb01仍有2笔合约单，arb02于12:41连续两次零挂单，待重新核验后发布。
arb02报价仍依赖trade_signal，短暂停信号后已恢复，
未强制taker或修改策略参数。Bitget及Gate arb04的24个交易进程身份保持；
源IP/网络/共享因子未改。三套旧发布文件均在各环境`run/adl_release_20261104T121136Z/`。
当前部署状态、哈希及验收见 `../artifacts/gate_fr_adl_release_20261104/README.md`。

2026-11-04 12:11–12:13 UTC，按用户要求从Prep02 last-20的1min/15min删除ICX共3条规则，
并从Prep/Spread券池定义移除ICX；Prep规则102→99、计算408→404，九策略身份保持。
经既有Config API预览、revision并发校验和热加载，Prep/Spread代次及全部进程PID保持，
无重启、重新回放或持仓迁移。ICX实际仓位及挂单0；RX03九条绑定沿用
`lgq_grid_prep` / grid / shares=1，无pending publish，position_ready=true。
交易所账户总数量与完整Exec分仓逐币一致；全部37行execution_complete，活动批次/委托0。
新1min三券池切换后分别确认last-20/top-20/top-5成交20/18/1笔；低于交易所最小量及
目标容差的尾差按既有完成口径保留。Prep404项和Spread50项均有观测，无因子/仓位错误，
部分Prep窗口未就绪仍按现有规则跳过。没有修改env.sh、主TOML、执行参数、CPU或源IP。
备份 `~/crypto_cta_manager/maintenance-backups/cta_icx_cleanup_20261104T121113Z/`；
核验见 `../artifacts/cta_icx_cleanup_20261104/README.md`。

2026-11-04 10:14–10:45 UTC，依次重发 JP `bitget_fr_arb01/02/03` 的 ADL 现货修复，
三套基础组件及原有 trade_signal 均已恢复。源码为同步、干净的 `arbmm` 提交
`516116fa`（包含 Bitget `f281e5f4` 及共享去重回归 `5f52d8f4`）。6个 release
二进制在本地活跃工作区构建，经标准 publish 脚本 SCP 临时路径上传、逐项
SHA-256 核对后原子安装；启停均经环境本地脚本，未在生产机编译。

| 服务 | bitget_fr_arb01 PID | bitget_fr_arb02 PID | bitget_fr_arb03 PID |
| --- | --- | --- | --- |
| Config（端口） | 2962347（20051） | 2996724（20052） | 3028830（20053） |
| Viz（端口） | 2964545（20151） | 2996936（20152） | 3031076（20153） |
| persist_manager | 2964944 | 2999246 | 3033636 |
| trade_engine | 2967754 | 2999856 | 3034228 |
| pre_trade | 2968088 | 3000374 | 3036529 |
| account_monitor | 2970516 | 3002512 | 3038826 |
| trade_signal | 2973252 | 3005114 | 3039325 |
| 原 dashboard（端口，未重启） | 64097（20161） | 64843（20162） | 65688（20163） |

CPU mask 保持原值：arb02/03 的 persist_manager 为 `15`，arb01 的 persist_manager
及表中其余进程均为 `0-5,48-95`。env.sh、全部已采样配置、账户模式和 `.231/.231`
源IP保持。三套原有21/22/21组合约对冲仓仍在，原生净尾差绝对值合计分别为
`48.85/41.87/52.34 U`，单币最大 `12.17/6.92/8.27 U`，现货、杠杆及合约挂单均0。
主备订阅和交易连接正常、档位缓存 missing=0；末次一分钟分别发出75/74/76条信号，
无新增非风控运行错误。原有2.60杠杆上限继续拦截新开仓，未放宽参数；启动初期
短暂缺因子快照已恢复。未触发实盘ADL测试。Gate未发布或重启，24个既有进程的
PID、exe、cwd、CPU身份保持。三套旧发布文件备份均在各环境
`run/adl_release_20261104T101356Z/`；发布哈希、完整PID与验收见
`../artifacts/bitget_fr_adl_release_20261104/README.md`。

2026-11-04 10:45 UTC，JP Prep02 按 `top-5 / top-20 / last-20 × 1min / 5min / 15min`
拆为九个独立策略，102条规则；`top-20 × 5min` 保留空策略。09:47:31.222 UTC切换点
将唯一规则直接归组、重复规则等分数量及冻结入场金额，并按已完成执行的实际分仓处理22项尾差。
Manager追加零和归属记录，账户数量、FIFO与history/7d/1d/after总盈亏校验不变；
历史成交、费用和旧策略盈亏保留，切换后按九个新策略统计。九条RX03绑定沿用
`lgq_grid_prep` / grid / shares=1；每规则额度沿用线上100U，原三条绑定和执行配置退役。
Prep PID2970444 / CPU17（12张活动表、3张退役表、408项共享计算）；
Spread PID2970968 / CPU18（原Spread01一张表、50项计算）；Config PID2968323 / CPU `0-5,48-95`。
RX03 pre-trade PID3016803、trade_signal PID3016920，CPU均 `0-5,48-95`；
trade_engine3936910、persist3684036、account_monitor3685214和Manager2097112 / CPU24保持。
Delist同步券池清单，最终以包含现货尾差修复的 `37ea9cae` 源码本地构建更新，
PID3033546、SG隧道3033518，均CPU `0-5,48-95`。Prep/Spread/Config源码 `f281e5f4`，
迁移工具源码 `37ea9cae`；所有产物本地release、SCP临时上传、SHA-256核验及原子安装，
仅通过环境本地脚本启停。两worker回放完成、Manager发布确认且无因子/仓位错误；
部分Prep窗口仍未就绪，ICX四项计算无行情，保持跳过；Spread50项均已就绪。
桌面/手机九策略与空表只读验收通过。env.sh、主TOML、原执行参数和源IP/ENI/路由保持；
同期Bitget FR另有独立发布，不将其PID变化归为本次操作。私有审计
`~/crypto_cta_manager/maintenance-backups/cta_symbol_groups_20261104T093943Z/`；
脱敏产物清单、操作工具与验收见 `../artifacts/cta_symbol_groups_20261104/README.md`。

2026-11-04 10:04 UTC，JP `~/delist_risk_server` 完成独立FR钉钉与敞口口径修正发布，
最终构建源码 `b77e5da1`，Delist PID `2922632`、SG隧道 PID `2922607`，均 CPU `0-5`。
按同步的arbmm已提交树在活跃本地工作区隔离构建，1,784个源码文件核对Git blob；
release、56项Delist测试、鉴权及桌面/手机验证通过，上传SHA-256校验并原子安装。
实际净量扣除同向策略对冲计划覆盖后才计入告警；正常建仓不再触发风险/恢复。
验收11个FR新鲜且在阈值内，13项已确认现货尾差保留明细并排除，数据不完整为0；
4条旧尾差缺价告警撤销，没有补发恢复。独立钉钉28条已确认、0积压、无错误，
Luna实际处理逐次推送，原24小时公告扫描完成心跳，风险仅首次/确认恢复推送。
沿用线上启动脚本和参数、四份私有配置及既有SG隧道端口；92个受保护交易/行情/CTA
进程的PID、归属及CPU mask均保持。无交易操作及IP/监听/隔离核分配变化。
备份 `~/delist_risk_server/run/fr_spot_tails_20261104/`，说明与验收见
`docs/delist_risk_server.md`、`../artifacts/fr_dingtalk_20261104/README.md`。

2026-11-04 06:32 UTC，JP `~/delist_risk_server` 发布 FR 敞口检查（源码 `c5f52f66`）。
从同步 `origin/arbmm` 的已提交树在活跃本地工作区 `data/ops/fr_exposure_check_20261104/build-source`
隔离构建，1,770个源码文件逐项核对 Git blob，保留另一项正在进行的 Gate ADL 修改。
本地 release 构建、40项 Delist 测试、鉴权/自动刷新/缺价与恢复及桌面/手机浏览器验证通过；
SCP临时路径上传、SHA-256核对及原子安装后，使用环境本地启动脚本重启 Delist 与既有 SG 隧道。
Delist PID `2469971`、SG隧道 PID `2469945`，CPU均为 `0-5`；
92个既有交易/行情/CTA进程的 PID、归属与CPU mask均保持。
鉴权与服务配置、原有自动下架处理参数保持；敞口模块仅并发读取11个 FR 的本机网关快照，
不产生交易或外部通知。默认10秒检查，单币100U/单账户500U持续30秒提示，过期或缺价保持未知。
06:32初版把Gate04原始两腿差额1854.24U计作风险，并把Gate01/02、Bybit及Gate04尾差缺价计作不完整；
该历史口径已由上述10:04发布修正为未覆盖差额及明确尾差排除。无源IP、端口或核心分配变化。
原文件备份 `~/delist_risk_server/run/fr_exposure_20261104_1cb089b7/`，本次修正版前备份
`~/delist_risk_server/run/fr_exposure_20261104_c5f52f66/`；说明与验收见
`docs/delist_risk_server.md`、`../artifacts/fr_exposure_check_20261104/README.md`。

2026-11-04 03:35 UTC，跨所账户展示名去掉固定频率后缀，改为
`okex_binace_cross`；账户/source ID、Exec别名、策略及绑定保持。
修复组合今日盈利要求两腿毫秒时间戳完全相同的问题：同一上海交易日且两侧
数据完整时汇总，数据时间取较旧一侧；跨上海日或缺值仍保持未知。
本机no-default-features release构建、504项测试通过（25忽略）、fmt、上传
SHA-256及不可变归属校验通过。仅Manager重启，PID2097112 / CPU24；前端、
117个受保护进程、交易配置/模板/绑定/快照、两侧原日初权益锚点均保持。
03:35核验今日盈利6.03534745 USDT、收益率0.01107209%，桌面/手机及自动刷新
正常，lgq/prc隔离通过；行情变化会更新数值。源IP/CPU分配未变。审计
`../crypto_cta_manager/artifacts/cross_account_name_profit_20261104/`；远端备份
`~/crypto_cta_manager/maintenance-backups/cross_account_name_profit_20261104T033100Z/`。

2026-11-03 15:41 UTC，发布 JP `binance_fr_arb03` 的 Binance PM USDT 购买能力与 51042 退避修复。
基于同步 `origin/arbmm` 的 `6ee63160` 工作区本地构建6个 release 二进制；
通过既有停止/发布/启动包装器及环境本地脚本操作，停机后交易所现货/合约挂单核验为空，
SCP 临时路径上传并逐项 SHA-256 校验、原子安装。Config PID590456、Viz592648、
trade_engine595521、pre_trade595779、account_monitor598066、trade_signal600644
均为 CPU `0-5,48-95`；persist_manager592997 保持 CPU15；原 dashboard61245 保留。
现货买单单独检查 USDT 现金与可借额度，资金缺失/过期或不足时拦截新开仓；
51014 使用借款/币种方向冷却，51042 使用账户内现货新开仓递增退避；ArbClose、
对冲、撤单路径保留。页面分别展示账户可用保证金、USDT现金、可借额度、买入可用额度。
启动限额加载 `limits=783 online_symbols=70 missing_online=0`；15:40 信号恢复后已出现
ArbOpen/ArbClose；15:41:06–15:43:06完整两分钟窗口51014/51042均为0、波动率
抑制为0，购买能力仍为0且本地拦截买入；这不代表交易所 USDT 可借额度已恢复。env.sh、交易/Viz配置与源IP `.231/.231` 哈希保持。
1893项库测试通过、3项忽略，桌面/手机浏览器真实WS验证通过。最终观察窗口见
`../artifacts/binance_fr_arb03_capacity_fix_20261103/README.md`；旧文件备份为
`~/binance_fr_arb03/run/capacity_fix_20261103T153229Z/`。

2026-11-03 15:18–15:36 UTC，恢复Bitget FR共用仓位档位缓存。原PM2路径
`~/mkt_signal/scripts/bitget_position_tier_sidecar.py` 已不存在，改为独立部署
`~/bitget_position_tier_sidecar`，PID544774 / CPU `0-5,48-95`，通过环境本地脚本启动并
保存PM2新路径。注册3套FR，当前在线22币，冷启动一次性补全22/22后启动常驻刷新；
三套pre_trade均确认热加载missing=0，原PID129512/142609/142765保持。公开请求沿用
`127.0.0.1:28902`，无新公共API或源IP。证据 `../artifacts/bitget_tier_restore_20261103/`。

15:33 UTC，发布Delist当前CTA监控映射。本地同步arbmm `6ee63160` release构建，
26项回归及浏览器检查通过，上传SHA-256校验后原子安装程序和页面；通过环境本地
启动脚本重启监控服务及其SG隧道，delist PID580273 / CPU `0-5,48-95`。
监控改读Prep01/02各3周期与Spread01，共7个共享进程持有的仓位表，按worker心跳判断
新鲜度；3个退役RX目录不再计入实时策略。线上只读诊断确认7项enabled=true、stale=false、
errors为空；绑定为Prep01→RX01、Prep02→RX03、Spread01→Binance/OKX两侧Exec。
配置/鉴权哈希与既有自动remove/dump/flatten开关保持。新CTA计算及执行进程未由本次重启；
同期另有Binance FR03/NAV进程变化，回执未将其误报为全主机PID保持。
备份 `~/delist_risk_server/run/current_cta_mapping_20261103T1533Z/`，证据
`../artifacts/delist_strategy_alerts_20261103/`。源IP、ENI及隔离核分配保持。

2026-11-03 15:30 UTC，按用户选择，Manager 的 RapidX 保证金列接入监控快照
`Accounts.frozenMargin`，标注“LTP占用”，占比按占用/实时权益；原生初始保证金
独立保留，旧观测含义不变，混合口径不合计。同时显示已上报的RapidX UniMMR。
仅发布并重启Manager，PID561579 / CPU24；503项Rust测试通过（25忽略）、
52项前端测试及构建通过，本机no-default-features release、上传SHA-256和
不可变归属校验通过。117个受保护进程、交易配置、模板、绑定及仓位快照保持。
RX01/RX03占用及比例、桌面/手机、UniMMR、lgq/prc隔离均核验；Gate arb04、
Bybit arb01及OKX MM退役展示保持。只读现有金融快照，未加私有请求、未重启
交易或账户监控，源IP/CPU分配不变。审计
`../crypto_cta_manager/artifacts/rapidx_margin_display_20261103/`；远端备份
`~/crypto_cta_manager/maintenance-backups/rapidx_margin_display_20261103T152300Z/`。

2026-11-03 15:01 UTC，JP Manager Funding/FR主面板最终核验完成。跨所账户当前
区间在原共享请求预算及WAF退避内优先，缺失覆盖补齐后释放优先级；仍共用单一
缓存。今日09:35:18启动至14:50 UTC的估算完整、两腿/策略可加，总计0.00008035
USDT；固定交易净值汇总与曲线前后一致。10项Funding回归与完整497项测试通过
（25忽略），本机构建no-default-features、上传SHA-256及不可变归属校验通过。
仅Manager重启，最终PID495563 / CPU24；前端保持，发布前基线117个受保护
进程、交易配置、模板、绑定及仓位快照均保持。更早核验期间另有操作独立
恢复Binance FR arb03，本次未操作或覆盖，最终基线已记录其新身份。
Gate FR arb04解除主面板排除并授权lgq；arb05保持排除；NAV仅停用已退役
okex_mm_alpha目录，历史保留；Bybit FR arb01实时正常，68点曲线完整，Gate
arb04为44点。桌面/手机、外部账户prc隔离通过；NAV、SG及交易进程未重启。
源IP、CPU分配及监听未变。审计
`../crypto_cta_manager/artifacts/fr_overview_and_funding_20261103/`；远端备份
`~/crypto_cta_manager/maintenance-backups/fr_overview_and_funding_20261103T145300Z/`。

2026-11-03 14:41–14:56 UTC，按要求重新发布并恢复 `binance_fr_arb03`。
本地同步 `origin/arbmm` 的 `6ee63160` 工作区 release 构建6个二进制，上传临时文件、
SHA-256校验后原子安装；已有未提交改动记录于发布证据，没有部署51042退避修复。
环境本地脚本恢复 persist_manager PID468517 / CPU15；trade_engine 469039、
pre_trade 471322、account_monitor 472267、trade_signal 476386、Viz 466178、
Config 466005均为CPU `0-5,48-95`；原dashboard PID61245保留。私有symbol限额
启动加载783项，70个在线币种missing=0。env.sh、交易/Viz配置与源IP `.231/.231` 保持。
新信号发现共享Binance Futures波动率缺失；只读定位旧tff_bn_fu在10月31日因BTC金额阈值
缺失退出。当前阈值含BTC，故仅用其环境本地脚本恢复数据进程PID497809 / CPU
`0-5,48-95`，沿用原二进制和全部配置，未改共享阈值。14:59:56已重新产生ArbOpen；
14:59:40–15:00:07仍出现51042拒单，涉及31币（不含AGLD/AWE/USUAL三个已确认
超过账户symbol限额的币），不能将重发视为拒单已解决。
发布验收及限制见 `../artifacts/binance_fr_arb03_republish_20261103/README.md`；
远端旧文件备份 `~/binance_fr_arb03/run/republish_20261103T143605Z/`。

2026-11-03 14:35 UTC，发布 JP Manager 跨所 Funding 查询修复：执行页对加载中
的固定账户/区间自动重试；累计历史先提供最近所需覆盖，早期历史随后补齐，
每页HTTP释放加载名额，当前区间完整时不等待更早数据。此组合只计启动后查询
区间内结算，OKX/Binance 各自持仓、费率及标记价独立核算后相加。
仅 Manager 重启，最终PID441955 / CPU24；前端在14:16发布，后续仅更新API。
公开静态目录权限曾导致页面错误，已明确修正为目录0755/文件0644并复核200；
桌面/手机及双WebSocket连接101通过。9项Funding回归及主库完整496项测试
通过，25项忽略；本机no-default-features构建、上传SHA-256与不可变归属校验通过。
117个受保护进程、交易配置、模板、绑定和仓位快照保持，未改源IP或CPU分配。
审计 `../crypto_cta_manager/artifacts/cross_funding_loading_20261103/`；
远端备份 `~/crypto_cta_manager/maintenance-backups/cross_funding_priority_20261103T143300Z/`。

2026-11-03 13:52 UTC，修复 JP Manager 跨所账户实时权益：启用的组合成员即使
`monitor_enabled=false` 仍订阅账户IPC，独立健康告警开关保持。仅原子发布并重启
Manager，PID351469 / CPU24；本机构建 no-default-features，上传SHA-256及
不可变归属校验通过。494项测试通过、25项忽略；117个交易/行情受保护进程、
配置、模板、绑定、仓位快照及前端保持。只读核验 OKX/Binance 两侧权益均新鲜，
组合54497.17 USDT等于34808.64+19688.53；首页权益亦恢复，保证金比率独立。
lgq可见、prc仍不可见；未改CPU分配或源IP。审计
`../crypto_cta_manager/artifacts/cross_account_equity_20261103/`；远端备份
`~/crypto_cta_manager/maintenance-backups/cross_account_equity_20261103T134700Z/`。

2026-11-03 13:21 UTC，修复 JP Manager 的 OKX 分钟K线确认延迟处理：已结束分钟
仍为confirm=0时，保存同页已确认K线，缺口保持等待并重试，不把未确认价格用于净值。
本机release（no-default-features）构建和上传SHA-256校验通过；只原子替换并重启
cta_web，PID281673 / CPU24。493项测试通过，25项忽略；不可变归属校验通过。
117个受保护进程身份、交易配置、绑定、执行模板及仓位快照均保持，前端未变；
固定范围账户/策略净值汇总及14个曲线点前后一致，最新区间重算成功，K线错误为空。
源IP未变。审计 `../crypto_cta_manager/artifacts/okx_candle_confirmation_20261103/`；
远端备份 `~/crypto_cta_manager/maintenance-backups/okx_candle_confirmation_20261103T131300Z/`。

2026-11-03 10:35 UTC，修复 OKX 现货规则刷新误清永续乘数：规则快照只更新所属
venue 的持仓换算表。交易所确认 TRB 每张0.1币、实际空62张后，备份并将错误的
`SYSTEM_POSITION_CLOSE` -55.75 币从账本移除，策略实际持仓改为 -6.2 币；
交易所历史成交确认无异常平仓成交。针对性回归和主库1450项测试通过（3忽略），
本机release构建上传并校验SHA-256、备份旧程序后原子安装。环境脚本恢复 OKX
pre-trade PID4127465 / CPU22，观察规则刷新后TRB仍为-6.2且无异常系统平仓；
恢复Binance pre-trade PID4133978 / CPU26，ctaker及两侧1份绑定保持，信号继续运行。
10:35只读核验OKX DOT多107张、TRB空62张、ETC空2.87张、NEAR空5.2张；
Binance DOT空107.2、TRB多6.2、ETC多14.35，旧IOTA空0.1保留，两侧无挂单。
目标可能随信号变化；源IP未变。审计在
`~/crypto_cta_manager/maintenance-backups/cta_spread_ctaker_20261103/`。

2026-11-03 09:39 UTC，发现 OKX 现货规则刷新错误覆盖永续合约乘数，TRB 实际
-62张（-6.2币）在内部交替显示为 -62/-6.2 币并形成错误 SYSTEM_POSITION_CLOSE
账本。通过环境脚本暂停 OKX/Binance Spread 两侧 pre-trade，持仓与目标保留；
只读查询确认 OKX DOT 107张多、TRB 62张空，两侧无挂单，Binance 原 IOTA -0.1
尾仓仍在。该暂停状态已由上方10:35修复恢复记录替代。

2026-11-03 09:38 UTC，按要求启用新账户RX01 / RX03实盘：分别绑定Prep01 / Prep02
的1min、5min、15min，六条shares=1、六个signal.enabled=true。规则、独立开平仓、
执行模板及额度保持（RX01为100/100/50U，RX03三个频段各100U）。通过环境本地脚本
启动六个下单进程，均在housekeeping 0-5：RX01 trade_engine/pre_trade/trade_signal
为PID3934587/3934756/3934963，RX03为PID3936910/3937159/3937530。
首次开启仓位表发现BBO订阅缺Tokio LocalSet而panic，尚未发布非零目标；源码9b343d29
修复两个计算worker共用入口。33项CTA测试及fmt通过，本机release构建、上传校验SHA-256
后原子安装；只用环境脚本恢复Prep PID3956979 / CPU17，重新回放至09:29追上实时。
核验404项中333项ready、94项NQ全就绪、共享阈值51币；六份目标与Exec逐项一致、推送
全部成功，计算/仓位错误为空。Manager09:37快照RX01已有60笔、RX03已有29笔成交，
两个新账户initial_position_count均为0；六频段独立PnL摘要全部有成交。
复核Prep全部线程仅CPU17，Spread PID3960454全部线程仅CPU18，均在隔离范围6-47；
启动参数持久化。源IP/local_ips和原观察进程保持。并行操作另行启用Spread并调整其模板，
本次未覆盖其修改；核验回执区分该外部变化。证据 `artifacts/cta_prep_live_20261103/`；
私有备份 `~/crypto_cta_manager/maintenance-backups/cta_prep_live_20261103T085500Z/`。
临时上传目录已清理，旧计算程序和写前配置保留。

2026-11-03 09:35 UTC，Spread01 的执行模板 `lgq_grid_spread` 从 grid 改为
`ctaker`，分别发布至 OKX/Binance 两个Exec，运行中的 pre-trade 均确认加载；
每侧8币无执行参数覆盖，份数和其他参数保持。ctaker 对可执行目标差额使用市价单。
切换前两侧无挂单；09:36只读核验 OKX DOT 多仓和 TRB 空仓各约100 USDT，
Binance仍只有原IOTA -0.1尾仓，无挂单。未重启进程或变更源IP；回执
`artifacts/cta_spread_ctaker_20261103/verified.json`，原模板及发布记录在
`~/crypto_cta_manager/maintenance-backups/cta_spread_ctaker_20261103/`。

2026-11-03，按要求启动 OKX/Binance CTA Spread：计算 PID3960454 / CPU18；
OKX trade_engine PID3936778 / CPU23、pre-trade PID3937073 / CPU22、trade_signal
PID3937424；Binance trade_engine PID3939283 / CPU27、pre-trade PID3939493 / CPU26、
trade_signal PID3939914。启动前只读核对 OKX 无仓位挂单、Binance 无挂单，原有
IOTAUSDT -0.1 尾仓保留，未主动下单清理。首次启用时旧计算进程因缺少 Tokio
LocalSet 崩溃、未成功发布目标；随即关闭推送、两侧份数归零。Prep 并行修复已将
源码9b343d29的构建产物安装至 Spread；核对SHA-256后重新启动并完成回放。
09:29开启Spread01仓位推送，OKX/Binance两条绑定各1份，单因子 max_pos_u=100；
行情通道订阅成功，50项因子和8项价差均ready，Manager确认发布，无待发送或错误。
核验时两所16项目标均0；两所 source IP 未改变。回执
`artifacts/cta_spread_live_20261103/verified.json`，远端私有备份
`~/crypto_cta_manager/maintenance-backups/cta_spread_live_20261103T090000Z/`。

2026-11-03 08:33 UTC，补齐 Prep01 / Prep02 共51币的 Binance Futures 月度大小单阈值：
完整2026-10成交934404516笔、1581文件，每币744小时；P50/P90以USDT用于2026-11。
共享Redis合约金额表0→51，逐项读回一致；OKX8项、现货表及公共因子计划保持。
源码a0d39e69让Prep从同一共享表加载并每180秒复查，校验月份/单位/数值；阈值变化
重建回放，失败保留最后成功快照。主库1450项通过（3项忽略），本机release构建、
上传SHA-256核对及原子安装后，仅用环境脚本重启Prep：PID3855958 / CPU17。
Spread产物同步更新而计算保持停止，Config/Manager及87个受保护进程身份保持。
08:33:52快照404项中331项ready：1min132/134、5min56/69、15min143/201；94项NQ
全就绪，大小阈值缺项0。73项未就绪中58项为15min：28项原始值缺失、3项标准化值
缺失、27项排名样本不足；其余1min2项、5min13项也是排名样本不足，不降低门槛。
七份仓位推送false、八条绑定shares=0，四套Exec下单进程停止；源IP/local_ips未改。
证据 `artifacts/binance_order_size_202610_20261103/`；私有数据审计
`~/tlen_config_shared/run/binance_order_size_202610_20261103T080500Z/`，程序备份
`~/crypto_cta_manager/maintenance-backups/cta_factor_health_bnthreshold_20261103T081500Z/`。
临时发布目录已清理，旧产物及核验回执保留。

2026-11-03 08:13 UTC，Gate FR `gate_fr_arb04` 按要求发布并启用交易：本机
`arbmm` release 构建七个二进制，上传校验 SHA-256 后原子安装；原产物保存在环境内
`run/publish_20261103T0806/old/`。persist_manager PID3839213 绑 CPU15；账户监控、
trade_engine、pre_trade、trade_signal、Viz、Config 和 dashboard 运行在 housekeeping
0-5。`local_ips` 保持 `.231/.231`，风控沿用操作员设置的 max_u 150000、单币敞口
比例 0.01；68 个正套币和 6 个 100U 覆盖值保持。补入本地通知服务地址后 pre_trade
两侧初始快照就绪，Gate 登录成功，已有实际挂单和对冲。启动后 TAKE/SFP 借币费率缺失
导致对应币种拦截；MTL/CHR/TURTLE/NOM 被 Gate 报 `RISK_CHECK_MARKET_FORBIDDEN`；
housekeeping 上观察到部分过期 ArbOpen 信号被丢弃，需持续关注。复核时 GWEI、ON
各约 99U 现货净敞口，对应合约 maker 对冲单已挂出，等待成交。

2026-11-03 07:23 UTC，完成 CTA Kafka / NQ 健康修复及 ICX 下线清理，源码44fe891c。
Kafka PARTITION_EOF 正常返回空轮询，后续有效记录清理旧Kafka错误；NQ允许从保留的增量
盘口重建有效双边报价，显式不完整snapshot仍保持缺失侧无效。Period库19项、主库1448项
测试通过（3项忽略），本机release构建后上传并校验SHA-256，再原子替换两个计算程序。
仅通过环境脚本重启Prep，PID3729504 / CPU17；Spread程序同步更新但计算仍停止，
Config、Manager和82个其他受保护进程未重启。全部配置、执行参数和绑定保持，源IP未改。
用户要求移除ICX：Prep02的1min删1条、15min删2条规则，计算408→404；Manager两份
ICX零目标及停止执行配置同步移除，其他标的、额度及NQ规则保持。回放追上实时后404项
都有输出，94项NQ全部ready，Kafka/仓位表错误为空。核验时288项ready；另60项缺大小单
阈值、35项归一化/排序样本不足、21项公式原始值缺失，仍保持未就绪，不降低就绪要求。
七份仓位推送保持false，八条绑定shares=0，四套Exec下单进程停止。回执及诊断：
`artifacts/cta_factor_health_20261103/`；发布私有备份
`~/crypto_cta_manager/maintenance-backups/cta_factor_health_20261103T070200Z/`。
ICX规则/目标另有私有备份，临时发布目录和Kafka诊断程序已清理。

2026-11-03 06:43 UTC，RX01 / RX03 按要求切换为全新执行账户 `binance_exec_rx01` /
`binance_exec_rx03`，分别绑定 Prep01 / Prep02 的1min、5min、15min，六条绑定均shares=0。
原Exec06/07各观察进程通过环境脚本停止后，整个运行目录同盘归档；新namespace、空订单库
和空账户journal不导入旧订单/仓位/盈亏锚点。旧PostgreSQL不可变历史仍隔离在退役source下，
旧入口、绑定及Redis前缀移除，新账户累计盈亏与策略目标为0，实际永续仓位及挂单均0。
`ff6d0575` 已删除RapidX监控自动历史成交补抓及回看参数，实时成交继续落库；新账单从
当前会话采集。381项测试与格式检查通过，本机release上传并核对SHA-256后原子安装。
仅启动新账户监控/persist/Viz/Config，并重启Manager；原4191路径、loopback端口、
源IP与风控保留，Nginx鉴权头及LGQ configure权限改指新source。其余15条账户绑定及
77个受保护进程身份保持。七份signal.enabled=false，所有下单进程仍停止。
检查发现Prep计算输出持续更新，但404项最新输出仅194项ready且保留Kafka No more messages
状态错误；Spread计算停止。未启用推送或交易。RX01现货零碎余额未出售或登记为新Prep初仓。
私有备份/旧运行目录在 `~/crypto_cta_manager/maintenance-backups/cta_prep_fresh_20261103T063500Z/`；
脱敏回执、发布哈希与开关检查见 `artifacts/cta_prep_fresh_20261103/`。临时发布目录已清理。

2026-11-03 06:19 UTC，按用户要求完成停止状态的 CTA 绑定：RX01 / Exec06 对应
Prep01 的 1min/5min/15min，RX03 / Exec07 对应 Prep02 的 1min/5min/15min，
现有 OKX–Binance 组合账户对应 Spread01 的 1min。七份策略、八条物理绑定全部
shares=0；Manager 正常保存同名零目标，后续自动推送排除停止绑定。七个 signal.enabled
保持 false，四套 Exec 下单进程保持停止，Prep / Spread 沿用原执行模板。
恢复 Exec07 原 source 与 LGQ 查看入口，不恢复旧因子、virtual04 或 Funding 出口例外，
RX04 / 旧 LTP 入口仍移除。仅重启 Manager，PID3642376 / CPU24；83 个受保护进程
身份、全部因子/Exec运行配置、执行模板及 PRC 的13条账户绑定保持。源IP、ENI/EIP、
local_ips、端口和CPU分配均未改动。API/数据库/Redis复核绑定、零目标及跨所投影通过。
私有备份 `~/crypto_cta_manager/maintenance-backups/cta_disabled_bindings_20261103T061615Z/`；
脱敏回执 `artifacts/cta_disabled_bindings_20261103/verified.json`。

2026-11-03 05:50 UTC，cross01执行页改为直接整合两所执行数据：一个账户汇总、一张执行表、
共用筛选及同名策略展开明细，移除嵌入Viz页面。公共账户仍为 `okex_binace_cross_1min`，
权益/名义金额加总，组合杠杆重算，缺失或过期数据不补零；两侧风控和交易配置独立。
前端 `cross01_unified_viz_20261103T054937Z` 只原子切换静态文件，无进程重启，
Manager PID3538262 / CPU24、87个受保护进程身份、API/配置和快照/绑定保持。
两侧执行及信号发布仍关闭；真实浏览器确认零iframe、一张表及两路WebSocket101。
52项前端测试通过，回执 `artifacts/cross01_unified_viz_20261103/`。
以下05:19的嵌入Viz布局已被本次替换；CPU分配、源IP、Nginx和路由没有变更。

2026-11-03 05:40 UTC，按用户补充的四列 JSON 规范，更新 Config 产物 `212a531e`：
因子每行只含 `标的` 字符串、小写 `nq`、`符号`、`因子`，频率统一取页面所选频段。
仅通过环境脚本重启 Config（PID3578063，loopback19220）；Prep PID3522829
及 start_ticks137144251 保持，81 个受保护进程身份保持，全部运行配置哈希/revision 保持。
未替换或修改现有因子清单、金额、开关或执行绑定。回执 `artifacts/cta_config_rows_20261103/`。

2026-11-03 05:31 UTC，按用户要求精简 CTA 配置页，发布 `38c037e9` 的 Config 产物。
页面恢复旧 CTA 深色风格，按策略组和频段标签分别配置；因子仅使用
`频率/NQ/因子/符号/标的数组` JSON，移除仓位展示、计算参数及逐行/替代格式输入。
仅替换并通过环境脚本重启 `cta-factor-config`，新 PID3562496，仍监听 loopback19220。
Prep PID3522829 / CPU17 / start_ticks137144251 保持，81 个受保护进程身份一致；
两套环境全部因子 JSON、主 TOML、env.sh 哈希和配置 revision 均保持，没有修改开关、
额度、交易绑定或源 IP。七份配置的桌面/手机验证及仅校验不写盘检查通过。
审计 `artifacts/cta_config_simplify_20261103/`；原配置 URL 保持。

2026-11-03 05:19 UTC，修正cross01展示误解：Manager只显示一个账户，公共别名
`okex_binace_cross_1min`，内部ID `cta_cross_spread_rx01`；两侧
`okex_exec_cross01` / `binance_exec_cross01`仅为账户内部执行器别名。
恢复该账户的两源映射，账户列表、总览、净值和策略选择器统一显示一次；账户的执行监控
入口同页拼接两套原生Viz，风控和费率分市场管理，净值及同名策略盈亏加总。
前端 `cross01_single_account_20261103T051501Z`；沿用API二进制，只重启Manager，
PID3538262仍CPU24。87个受保护进程身份、执行配置、别名和快照/绑定保持。
新账本订单和初始仓位均0，Funding仅预估，下单及信号发布保持关闭。
没有改变CPU分配、源IP、Nginx或其他运行环境。41项前端测试、生产构建和真实归属预检通过。
回执 `artifacts/cross01_single_account_20261103/`；下方两账户展示记录已被本次纠正。

2026-11-03 05:08 UTC，用户确认 Prep02 两条 SOL/DOGE 15min 规则使用
`TD_MT_040` 后，发布 `arbmm` 的 `fa63d6e4` 多策略组产物。Prep01 / Prep02
各自 1min/5min/15min，共六张表、408 项合并计算；Spread01 配置和产物同步升级，
其计算进程保持停止。Prep PID3522829 固定 CPU17，Config PID3522750 使用 housekeeping，
监听 `127.0.0.1:19220`。Prep 已开始历史预热；七张表 `enabled=false`，Manager
按新组名注册的七个策略均为零目标、无执行绑定，四个未启用的旧表定义已移除。
旧版空账本及替换前产物/配置均先备份，未覆盖其他操作员的配置；Exec06 与跨所两腿
下单进程保持停止，RX01 USDT 永续仓位/挂单仍 0，81 个受保护进程身份保持。
七份页面的真实 Chromium 预览、组选择、行数、额度、桌面/手机检查通过，预览未修改开关。
配置入口 `/cta/cta-prep-binance-futures/config/?worker=cta-prep-binance-futures#prep02_binance-futures_1min`。
没有改动交易源 IP、ENI/EIP 或路由。远端审计
`~/crypto_cta_manager/maintenance-backups/cta_prep02_deploy_20261103T050422Z/`；
脱敏回执 `artifacts/cta_prep02_deploy_20261103/verified.json`。

2026-11-03 04:25 UTC，按用户“先开发，再上线”的要求，停止新
`~/cta-prep-binance-futures` 和 `~/cta-factor-spread` 计算进程，实际进程扫描均为 0。
此前已完成 RX01（Exec06）USDT 永续 taker 清仓：实际合约仓位、普通/算法挂单均 0，
91 个旧因子账户绑定、virtual06 跟随及 Exec 活跃策略索引均已移除。
旧 RX01/03/04 和跨所 Spread 的 signal/publisher/config/dashboard 已停止；不恢复旧交易。
两个新运行目录和单组策略定义保留，四个仓位开关均关闭且无执行绑定；Exec06 及跨所
两腿下单进程保持停止，监控/持久化/Viz/Config 保留。新统一配置服务仍监听 loopback19220，
4191 CTA 网关及导航已安装，旧配置 URL 返回 410；多策略组功能仅本地开发，后续不自动上线。
CPU17/18/19/28 当前无 CTA 因子进程；源 IP、ENI/EIP、路由、其他交易进程保持。
回执：`artifacts/rx01_taker_clear_20261103/`、`artifacts/cta_factor_replace_20261103/user-paused.json`。

2026-11-03 04:18 UTC，按操作员最终选择，cross01改为两个普通账户入口：
`okex_exec_cross01` / `binance_exec_cross01`，真实source仍为
`okex_exec_trade01` / `binance_exec_spread01`。移除旧逻辑账户配置
`cta_cross_spread_rx01`，增加 `/manager/exec/?group=cross01` 拼接执行监控，
原生Viz各自WebSocket，支持上下/左右布局及可展开的组合策略盈亏。
两源分别FIFO核算后相加，Funding仅预估；新账本订单/初始仓位/绑定均0。
仅切换前端并重启Manager，PID3427918仍CPU24，未改API二进制、Nginx、
任何执行配置、风控、CPU分配或源IP。87个受保护进程身份及快照/绑定保持；
未启动pre-trade、trade_engine、trade_signal或策略发布。41项前端测试和生产构建通过。
前端 `cross01_exec_viz_20261103T041000Z`，回执
`artifacts/cross01_exec_viz_20261103/`；下方较早单账户展示记录仅作历史。

2026-11-03 03:42 UTC，Spread聚合账户统一使用预估Funding，两源实际账单采集均关闭且记录数0。
聚合净值接口预估Funding为0、覆盖完整；前台仅保留预估口径，移除本账户的实际采集开关，
其他账户设置保留。本次仅切换静态前端 `cta_estimated_funding_20261103T033844Z`，未重启进程或修改运行配置/交易参数。
OKX公开费率、结算前标记分钟及1m成交K线已部署，官方样本核验10根连续已收盘K线、
3次Funding结算及其标记价通过；只发公开GET，未调用账户收入或交易API。
37项前端测试及浏览器检查通过。回执 `artifacts/cta_spread_data_audit_20261103/`。

2026-11-03 03:10 UTC，按账户使用方式进一步替换Spread前端：删除独立的双Exec概览组件，
`okex_binace_cross_1min` 复用与RapidX相同的普通账户页，展示费率、合约杠杆、策略启用及净值入口。
OKX/Binance作为同一账户内的交易市场选择，切换参数不改变公共账户URL或净值范围；
同名策略显示为一条记录，各市场算法和份数保持原值，不相加。前台不显示执行腿管理卡片或内部Exec别名。
本次只切换前端webroot，没有重启进程或修改运行配置、风险参数和订单/持仓；Manager保持CPU24。
37项前端测试及桌面/手机对照RapidX页面核验通过。当前前端 `cta_multi_market_20261103T030156Z`，
回执 `artifacts/cta_spread_alias_20261103/multi-market-browser-verification.json`。

2026-11-03 02:45 UTC，按操作员澄清移除JP旧 `lgq_ltp_81857` 账户入口，
对应 `binance_exec_trade08`，此前已停用但仍显示在Manager账户列表。
检查旧环境无交易进程、绑定/跟随/待发布任务均0后，备份并从Manager运行配置移除
该source和对应source_viewers；仅重启Manager，现PID3279392、CPU24。
账户列表及总览已无旧入口。新 `okex_binace_cross_1min` 组合及
`okex_exec_trade01` / `binance_exec_spread01`两腿保持，87个受保护进程PID/启动时间一致。
旧source记录、symbol索引、仓位快照及日净值记录指纹保持，虚拟账户定义保持；
交易所下单/撤单/清仓调用数0，source IP及CPU19/28因子计算保持。
回执 `artifacts/ltp_81857_manager_remove_20261103/verified.json`；完整私有备份
`~/crypto_cta_manager/maintenance-backups/ltp_81857_manager_remove_20261103T024033Z/`。

2026-11-03 02:41 UTC，JP Spread保持两套Exec，对Manager使用者使用一个账户，别名设为
`okex_binace_cross_1min`，底层账户ID仍为 `cta_cross_spread_rx01`。内部两腿显示名统一为
`okex_exec_cross01`、`binance_exec_cross01`；实际环境目录、namespace和持久化source ID均保留。新增组合账户别名元数据
及普通别名编辑接口；账户页和策略启用页以一个账户为入口，内部切换OKX/Binance执行参数，
URL保留公共账户身份。仅重启Manager，当前PID3268950仍CPU24；89个受保护进程及全部执行配置
校验一致。两套Exec、独立风控、源IP和原有仓位快照/绑定均保持，未启动交易。
473项库测试、34项前端测试和18项隔离PostgreSQL测试通过，桌面/手机别名编辑与执行腿切换
检查通过；真实数据库5个source不可变归属校验通过。产物使用 `--no-default-features`。
回执 `artifacts/cta_spread_alias_20261103/`；远端维护记录
`~/crypto_cta_manager/maintenance-backups/cta_spread_alias_20261103T023524Z/`。

2026-11-03 02:28 UTC，补齐 RX03 / RX04 在 Manager 的入口清理：此前仅清仓并解绑，
两条source仍启用，virtual04/virtual05也保留，故账户页仍有 `RX04 5min`。
现从线上Manager配置移除 `binance_exec_trade07` / `binance_exec_rx04_5m05`，
同步移除对应source_viewers和RX03 funding出口例外，并备份后通过API删除无跟随者的
virtual04/virtual05及其配置绑定。账户页、总览、虚拟账户列表均确认无这两个入口。
仅重启Manager，现PID3252503、CPU24；首次等待55秒不足触发配置自动回退，
延长历史缓存预热等待后成功。87个受保护交易/监控/持久化/因子进程PID及启动时间保持。
历史source记录、订单库及仓位快照保留；签名复核两账户持仓、普通/算法挂单仍均0。
CPU19/28因子计算和 `.228` source IP保持。完整备份
`~/crypto_cta_manager/maintenance-backups/rx03_rx04_manager_entries_20261103T022116Z/`；
脱敏回执 `artifacts/rx03_rx04_taker_clear_20261102/manager_entries_verified.json`。

2026-11-02 15:05 UTC，按要求将 RX03 / RX04 从执行链路移除并 taker 清仓至0：
`binance-cta-perp-rx03`、`binance-cta-perp-rx04` 均设为 `enabled=false`，
通过环境本地脚本停止 signal，以及 `binance_exec_trade07` / `binance_exec_rx04_5m05`
各自的 exec-pre-trade、trade_engine。63份因子策略先发布 `clear, signal=1`；
因子子仓位低于最小下单量留下的12/8笔尾仓，按账户实际整手数量提交 reduce-only
MARKET，全部成交并逐单对账为0。15:05 UTC再次签名核验两账户永续持仓、普通及算法挂单均0。
Manager解除 virtual04/virtual05 跟随并删除28/35个账户绑定；实际Exec活跃策略索引均为空。
账户监控、持久化、Viz/Config及历史数据保留，下单进程保持停止。CPU19 publisher
PID2307925、CPU28 publisher PID705737继续运行，CPU绑定和 `.228` source IP未变。
回执 `artifacts/rx03_rx04_taker_clear_20261102/final_verification.json`；完整私有审计在
`~/crypto_cta_manager/maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/`。

2026-11-02 14:59 UTC，JP CTA Spread 已修正为一个 Manager 账户 `cta_cross_spread_rx01`，
内部执行源为 `okex_exec_trade01`（OKX开仓）和 `binance_exec_spread01`（81857对冲/平仓）。
普通账户、总览及净值入口均只出现一个账户；两腿FIFO、费率和风控仍独立，净值累加。
仅重启Manager，当前PID1537702继续绑CPU24；93个受保护进程PID/启动时间、交易配置
和全部源IP校验保持不变。两条下单链路及信号发布继续关闭。新订单、初始仓位快照、执行绑定均为0。
实际数据库5个source不可变归属校验通过；473项库测试通过，完整Rust测试491通过/24忽略，
默认开发构建及桌面/手机浏览器检查通过。首次60秒等待误判约3分钟的冷启动并回退，
调整检查窗口后成功上线，未修改历史数据。Manager产物仍使用 `--no-default-features`。
回执 `artifacts/cta_cross_spread_deploy_20261102/managed-account-verification.json`，
远端维护记录 `~/crypto_cta_manager/maintenance-backups/cta_spread_account_20261102T144713Z/`。

2026-11-02 14:07 UTC，只读复核 JP `gate_fr_arb04` / `gate_fr_arb05`：
当前分别绑定 `GateAM36_LTP` / `GateAM40_LTP`；13:47 UTC Gate 签名查询确认
两者均为 `multi_currency`，且 `usdt_futures=true`。arb04 可用 USDT 约
1,000,000，arb05 为 0。arb04 仅运行 `account_monitor`（PID4175337，
CPU mask `0-5,48-95`），arb05 无运行进程；两环境交易进程均停止，
没有新增隔离核绑定。两环境 `local_ips` 仍均为 `.231` / `.231`。

2026-11-02 13:57 UTC，部署新组合账户 `CTA Spread · OKX / Binance`，
Manager组合键 `cta_cross_spread_rx01`，两个新source为 `okex_exec_trade01` 与
`binance_exec_spread01`。后者仅复用81857凭据，旧trade08的订单、快照和namespace
保持独立且继续停机；没有导入旧订单或创建任何初始仓位快照。
OKX账户监控PID1397233绑CPU21，Binance账户监控PID1361356绑CPU25；
两套persist_manager分别PID1358514/1361017，共用CPU15。Viz/Config运行于housekeeping，
端口分别10049/18169和10050/18170。CTA配置页19184已启动，发布关闭、清单为空。
Exec pre-trade、trade_engine、trade_signal及策略/因子发布进程均未启动；
CPU22/23及26/27仅预留。两套风控各写自己的 `<env>:<venue>:pre_trade_risk_params`。
Manager PID1395180继续CPU24，组合净值按两腿累加，API实测0，订单与初始仓位均0；
组合与两账户仅lgq可见，prc直达网关返回403。87个既有交易相关进程保持原PID/启动时间。
本次Manager首次发布触发未发布日FIFO/JSON解析改动的旧归属校验错误，已回退并修正：
正式产物使用 `--no-default-features`，保留原JSON解析和历史校验，真实数据库5个source
的不可变归属指纹校验通过；471项库测试通过、7项忽略。没有更改历史归属或持仓快照。
OKX新Exec监控复用显式源IP REST client，WS及私有REST均使用原 `.229/.230` 交易地址。
13:05只读查询81857仍有IOTAUSDT -0.1尾仓，本次未交易，未把尾仓种入新策略账本。
本地release校验、回退及最终上线回执：`artifacts/cta_cross_spread_deploy_20261102/`；
远端维护备份 `~/crypto_cta_manager/maintenance-backups/cta_cross_spread_20261102T131916Z/`。

2026-11-02 10:26 UTC，仅准备新 `okex-binance-cta-spread-rx01` 与原生 OKX
`okex_exec_trade01` 的脚本、配置；未安装二进制或启动进程。OKX Exec 从已停机
`okex_mm_alpha` 仅复制三项凭据，使用独立 namespace、订单库和10049/18169端口；
预留账户监控CPU21、Exec pre-trade CPU22、trade_engine CPU23，publisher预留CPU18，
Config预留19184。原MM env及配置未改，CPU20仍属于共享执行Vol。
Binance对冲腿复用停止中的 `binance_exec_trade08` / `lgq_ltp_81857`，配置未改。
10:23 UTC签名只读核验：OKX所有持仓/挂单/借款0，仅USDT；Binance仍有
IOTAUSDT -0.1（约0.004 USDT），普通/算法/现货挂单0，未提交平仓单。
新策略enabled=false、币种/因子清单为空；未创建Manager策略或绑定。
Manager双腿投影及独立估值测试已通过，在线旧程序尚未更新；完整候选配置仅存于
`~/crypto_cta_manager/maintenance-backups/cta_cross_spread_prepare_20261102/`，
两source均禁用，在线Manager配置、二进制及PID828467保持。

2026-11-02 按操作员要求下线 JP RX02 及 `binance_exec_trade08`：通过环境本地
脚本停止信号、publisher、页面和执行账户各进程；CPU18 的 RX02 publisher 已停止。
Manager 禁用 Exec08 并移除其 virtual02 跟随和唯一执行绑定，保留账户历史。
RX02 运行目录移至 `~/retired_data/rx02_retired_20261102/`，Delist 不再发现该策略。
停机前 Binance 普通 UM 挂单为零；Exec08 状态显示 IOTAUSDT -0.1 的最小交易量
尾仓，未平仓或划转。RX03 publisher PID2307925 及其他已核对交易进程保持。
维护前配置及状态在 `~/crypto_cta_manager/maintenance-backups/rx02_exec08_retire_20261102/`。

2026-10-30 10:59–11:03 UTC，RX01发布器更新为`ec539eb9`本地release，
SHA-256核验后原子安装；CPU17 PID3716037。38币各672根闭合15min baseline由
2026-10-22至28连续Tardis原始成交/L2复用线上聚合生成，真实月度分类阈值保持；
历史长窗口原始公式不变，缓存随实盘闭合bar持续保存，重启不再只依赖Kafka留存。
当前正在追平Kafka，`enabled=false`，15min signal停止，尚无Manager绑定；
旧的部分预热观察仓位和历史已归档，不作为新目标恢复。
Dashboard PID3648167（10195），Config PID3716937迁至空闲19195，避免旧RX03入口串用。
Exec06仍停止；11:00前只读核验永续、普通/算法订单、全部资产债务（含USDT）及显式贷款为0，
55笔现货尾差约3.61 USDT且数量均低于lot step。1407项Rust库测试及转换测试通过。
本次未改动交易source IP、ENI/EIP或路由。

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
| 13 | depth_pub_general | 本机 8 路 depth25（BN/OKX/Bitget/Gate × margin+futures）；Bybit 由独立 depth_pub 负责。pm2 `dp_general` |
| 14 | spread_pbs binance-futures market | 原 depth_pub binance-both 腾出；trade/incremental/derivatives |
| 15 | persist_manager ×N | 原FR等及okex_exec_trade01/binance_exec_spread01持久化；新账户MM PID421889，旧MM alpha停止 |
| 16 | spread_pbs gate-bitget-futures-bbo | 独立 Gate / Bitget USDT 合约 BBO；`spp_gate_bitget_futures_bbo`；IPC `futures_bbo` |
| 17 | cta-prep-binance-futures | PID2168358；Prep01四频段与Prep02三券池×5min/15min/1h，同进程596项计算，回放完成且四张小时表已启用；原仓位账本保留；无额外独占核 |
| 18 | cta-factor-spread | PID1140614；Spread01/Spread02四频段共享532项计算；双市场Tardis小时包截止2026-11-12，227个普通分钟窗口续接；已追平，小时Spread01 109/109、Spread02 130/130就绪；Spread01开启、Spread02每规则100U且发布关闭；双Exec保持 |
| 19 | 空闲 | RX03 旧因子/信号/页面已停止，Exec07 已清仓解绑并移除 Manager 入口 |
| 20 | exec_volatility_pub(exec_volatility_binance) | 共享Binance现货/永续Vol，PID2865016；独立于RX01生命周期 |
| 21 | okex_exec_trade01 account_monitor | PID3911954；UID241049181320564736，与当前MM为不同子账户 |
| 22 | okex_exec_trade01 exec-pre-trade | PID3919273；规则刷新按venue隔离 |
| 23 | okex_exec_trade01 trade_engine | PID3918956 |
| 24 | crypto_cta_manager / cta_web + crypto_nav_manager | CTA PID3909484；NAV PID483448，MM83777统计接入，交易进程保持 |
| 25 | binance_exec_spread01 account_monitor | PID3912185；81857独立执行环境 |
| 26-27 | binance_exec_spread01 exec-pre-trade / trade_engine | PID3920104 / PID3919575 |
| 28 | redis-server.service + Delist | Redis PID2769589，全部7线程及systemd CPUAffinity=28；Delist PID3195932（11-12单币持续报警与独立系数），env.sh DELIST_CORE=28；SG隧道于11-11停止 |
| 29 | okex_mm_83777 account_monitor | PID275138；UID556411065144578537，空仓风险解析已修复，实盘账户监控 |
| 30 | okex_mm_83777 trade_signal | PID291515；BTC/ETH实盘，vol门限开启、Tlen关闭 |
| 31 | okex_mm_83777 pre_trade | PID424228；实盘风控/开仓/对冲 |
| 32 | okex_mm_83777 trade_engine | PID291094；实盘交易开启，二进制SHA-256启动校验保留 |
| 33 | spread_pbs bybit-both market | 现货/永续成交、增量及衍生指标 |
| 34 | spread_pbs bybit-both bookticker | 现货/永续 BBO |
| 35 | (空) | 原 binance-intra-arb02 已下线删除 |
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
binance_fr_arb02/03/04、bitget_fr_arb01（新盘建仓运行）/02、
okex-intra-arb01 全套、trade_flow_feature ×8、rolling_metrics ×5、fusion_factor_1m、
persist_center、predict_file 及各类 viz/config/dashboard 服务。
`binance_exec_trade08` 的engine、executor、监控、持久化、Viz/Config已于2026-11-02停止；
原 virtual02/RX02 跟随和 Grid 绑定已移除。
`gate_fr_arb01/02/03/04` 已于2026-11-12 10:33 UTC恢复全部8服务（含 `trade_signal`），原 CPU 分配保留。
`okex_mm_alpha` 已于2026-10-29停止，槽位于2026-11-06由 `okex_mm_83777` 替代。
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
| 8 | 原 spread_pbs bybit-both(market 角色) | 2026-11-11 已停止 |
| 9 | 原 spread_pbs bybit-both(bookticker 角色) | 2026-11-11 已停止 |
| 10 | NIC IRQ: enp39s0 下单网卡 | 禁止再绑用户进程 |
| 11 | NIC IRQ: enp40s0 行情网卡 | 禁止再绑用户进程 |
| 12 | 原 bybit_fr_arb01 account_monitor | 2026-11-11 已迁至 JP，SG 进程停止 |
| 13 | 原 bybit_fr_arb01 trade_signal | 2026-11-11 已迁至 JP，SG 进程停止 |
| 14 | 原 bybit_fr_arb01 pre_trade | 2026-11-11 已迁至 JP，SG 进程停止 |
| 15 | 原 bybit_fr_arb01 trade_engine | 2026-11-11 已迁至 JP，SG 进程停止 |

此前未绑核、跑在 housekeeping 0-7 的 depth_pub、trade_flow_feature_pub、
rolling_metrics 已于 2026-11-11 停止；当时未发现 persist_manager 运行。
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
