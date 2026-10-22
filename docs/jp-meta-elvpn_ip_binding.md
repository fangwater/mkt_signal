# jp-meta-elvpn IP 绑定

最后更新: 2026-10-22 09:39 UTC。**分配、改写 `trade_engine.toml local_ips`、新开或下线任何用独立 source IP 的环境时，请同步更新本文件。**

2026-10-22，RX02 的 Perp、旧 Special 和 `binance_exec_trade05` 全部停止并归档，
不向新账户迁移策略。Manager 禁用 trade05 并解除跟随，原配置/历史保留；
完整归档在 JP `~/retired_data/rx02_20261022T091913Z/`，原目录保留归档标记。
旧账户只读核验普通/条件挂单均0，24个非零永续持仓保留，未平仓。
`.228` 是共享出口，未释放；没有改动 ENI/EIP、路由或 IRQ。

新增原生 Binance PM USD-M Exec `binance_exec_trade08`，显示名 `lgq_ltp_81857`。
`trade_engine.toml local_ips=["172.31.35.228", "172.31.35.228"]`，
account_monitor 已建立的认证连接实测均使用 ens41 / `.228`（公网 `13.115.227.29`）。
与当前 `binance_fr_arb02` 为不同账户；凭据仅保存在新环境权限0600的 `env.sh`。
本机 `arbmm` 提交 `d3aedea0` release 构建，scp 上传并校验 SHA-256 后原子安装；
只运行账户监控、持久化、Viz 和 Config，没有策略绑定/跟随，交易进程全部停止。
Viz/Config 仅监听 `127.0.0.1:10048/18168`，已有4191鉴权网关新增 `/exec_trade08/`，
配置持久化于 `/etc/nginx/snippets/crypto_cta_manager.conf`，`nginx -t` 通过后 reload；
`lgq` 已获配置权限，页面、快照和配置入口通过鉴权访问均HTTP200。
Manager 为 trade08 生成独立 Binance futures 规则缓存，未添加 COIN-M 或 RapidX cash。
其余账户 source IP 与监听保持；证据：`data/ops/exec_trade08_rx02_retire_20261022/`。

2026-10-21 09:51 UTC，仅替换并重启RX04的5min因子publisher以调整
TP_VPI_006内层min_periods=280。仍使用默认出口；env.sh、loopback10198/19188、
4191网关、ENI/EIP和交易source IP均保持，未绑定Manager/Exec，观察模式不变。

2026-10-21 09:11 UTC，新增 `binance-cta-perp-rx04` 5min 观察环境，15币、
35因子、48组合，`enabled=false`，未绑定Manager/Exec。Kafka和公开K线沿用
默认出口 `172.31.35.228`，未分配额外EIP/ENI地址或交易source IP。
页面仅监听loopback10198/19188，经已有4191网关提供仓位表、`signals/`和`config/`：
`/cta/binance-cta-perp-rx04/`。新增持久化snippet
`/etc/nginx/snippets/cta_perp_rx04.conf`，由 `sites-available/crypto_proxy_4191.conf`
include；`nginx -t`通过后reload。既有RX02/RX03配置哈希及62个进程身份保持。
证据：`artifacts/cta_perp_rx04_deployment/`；网关修改前后副本保留在新环境 `run/`。

2026-10-21 08:14–08:15 UTC，仅替换 RX01 Exec06 执行器和 Viz 以恢复大仓位
快照推送。全部交易IP、env.sh、trade_engine.toml、ENI/EIP、路由和监听端口保持，
118份配置哈希核对一致；交易引擎、账户监控和信号发布器均未重启。
Manager 模板拆分仅改变算法绑定及prep合约参数，不涉及网络绑定。见 `docs/exec_grid.md`。

2026-10-21 07:20 UTC，全symbol Vol 发布及三个Exec订阅容量升级完成，release
`20261021T064822ZGridAllSymbols`（Exec `d3b724af` / Manager前端 `82f58cb`）。
交易IP、trade_engine.toml、ENI/EIP、路由和监听端口全部保持；53份配置哈希不变。
factor env.sh仅新增 `CTA_SPREAD_FACTOR_PROCESS_NAME=cta_spread_factor_binance_futures`，
用于后台回放后的单实例交接，不涉及网络或账户参数。156个受保护进程身份、87个实账户
及77个虚拟账户绑定保持，Manager/Config/Viz/trade_engine未重启。详见 `docs/exec_grid.md`。

2026-10-21 05:33 UTC，Grid 执行框架发布到 Exec05/06/07、RX01 共享因子 publisher
和 Manager，release `20261021T053158ZGridControls`，Exec `bf9d0ad8` / Manager `d5c3cc1`。
仅更换执行器、Config、因子 publisher、Manager API/前端；所有 env.sh、trade_engine.toml、
source IP、ENI/EIP、路由、监听端口和CPU绑定保持原样，54份配置哈希核对一致。
87个实账户与77个虚拟账户绑定不变，当前策略未切换Grid。详见 `docs/exec_grid.md`。

2026-10-19 13:10 UTC，只补发Exec06执行器以保留普通拒单退避，已恢复RX01原推送配置。
账户查询与交易仍显式绑定 `172.31.35.228`，其余网络及监听配置保持。
最终回执：`~/binance_exec_trade06/run/borrow_stop_20261019T130500Z/`。

2026-10-19 12:54–12:55 UTC，RX01 `binance_exec_trade06` 已发布借币不足终止执行修复，
RapidX Exec 默认杠杆改为4倍，BEL/ZIL按场内上限3倍，BAND实际回读为4倍。
仅更新exec-pre-trade和Viz，环境本地脚本重启后恢复原仓位推送设置；source IP继续为
`172.31.35.228`，env.sh、trade_engine.toml、ENI/EIP、路由和监听端口保持。
启动后11个借币不足目标均无剩余执行批次或挂单，未触发429。
备份及回执：`~/binance_exec_trade06/run/borrow_stop_20261019T125000Z/`；
本地证据：`artifacts/rapidx_borrow_stop_20261019/`。

2026-10-19 10:02–10:20 UTC，RX01 Spread / Exec06 完成taker清仓及新账户、策略快照，
10:17:05 UTC恢复交易。快照时永续、负债和挂单为0，仅约2.7593 USDT现货零头归inventory。
沿用 `~/binance_exec_trade06/trade_engine.toml` 的 `local_ips=["172.31.35.228"]`，
账户查询及交易继续显式绑定该地址；原env.sh、交易配置、ENI/EIP和路由保持。
Viz/Config仍为loopback10046/18166，Spread dashboard/config为10186/19174。
本次SSH临时使用本机bashrc的HTTP代理访问现有公网SSH端点；常规 `jp-meta-elvpn`
恢复并验证后，临时代理配置及控制连接已删除。未改写永久SSH配置。
备份及快照回执：`~/binance-cta-spread-rx01/run/reenable_20261019/`；
本地核验：`artifacts/rx01_reenable_20261019/verification.json`。

2026-10-19 07:26–07:30 UTC，仅发布 RX01 Spread 的退出默认值0.7/0.3及配置页说明，
signal和配置服务经环境本地脚本重载。publisher及Kafka预热保持，794条内部仓位为0，
发布关闭、Exec06停机；Perp配置未改，公网4191配置入口HTTP200。
env.sh、Exec账户文件、source IP、local_ips、ENI/EIP和路由均保持。
发布回执及核验：`artifacts/rx01_corr50_warmup/spread_exit_defaults/validation.json`。

2026-10-19 07:10–07:12 UTC，RX01 Spread Kafka回溯已追实时，48币/794组合，
762个因子组合就绪，21个缺大小阈值及11个factor_114未就绪；发布保持关闭，
Exec06及dashboard停机。配置页公网4191入口验证HTTP200。未改动source IP、
local_ips、env.sh、Exec交易配置、ENI/EIP或路由；线上exit现场修改0.7/0.3已保留。
最终核验：`artifacts/rx01_corr50_warmup/final.json`、`final_config.json`。

2026-10-19 07:03 UTC，RX01 Spread 按用户要求删除 ICXUSDT、STORJUSDT 的79个组合，
当前48币/57组/794组合内部零仓。配置文件及Redis清单同步，signal经本地脚本重载，
publisher保留当前Kafka预热，发布关闭、Exec06停机；无source IP、local_ips、
env.sh、交易配置、ENI/EIP或路由变更。备份及回执：
`~/binance-cta-spread-rx01/run/corr50_remove_symbols_20261019T065917Z/`。

2026-10-19 06:47 UTC，RX01 Spread 经环境本地脚本启动因子预热及发布关闭的signal，
旧内部状态备份后重置为57组/873组合零仓；配置服务继续运行，dashboard和Exec06停机。
29币缺少成交大小阈值，按用户要求保持空缺，相关组合保持未就绪。
未修改 env.sh、Exec06交易配置、共享TLen阈值、source IP、local_ips、ENI/EIP或路由。
备份及发布回执：`~/binance-cta-spread-rx01/run/corr50_warmup_20261019T064500Z/`；
本地核验：`artifacts/rx01_corr50_warmup/`。

2026-10-19 06:04 UTC，仅更新 RX01 Spread 的50币/57组/873组合配置、双过滤程序
及配置页面，配置服务在原loopback19174恢复健康；signal、publisher、dashboard和
Exec06继续停机，发布关闭。env.sh、Exec06 `trade_engine.toml` 和原仓位状态哈希
均保持，未更改source IP、local_ips、ENI/EIP、路由或网关。
发布与核验记录：`artifacts/rx01_corr50_deploy/`。

2026-10-19 05:09–05:18 UTC，按用户要求暂停 RX01 Spread 和 Exec06 全套进程，
随后保留现货/负债，以USDT永续对齐RX01逐币净敞口，仅剩交易规则以下尾差。
通过环境本地停止脚本操作；未改动 `local_ips`、ENI/EIP或路由，原source IP配置保留供恢复。

2026-10-18 JP 重启恢复 Exec01–07 及 RX01 Spread / RX02、RX03 Perp 信号链路。
CTA Manager user service 已自动恢复。沿用所有环境现有 env.sh、trade_engine.toml、
Exec01–07 的10041–10047/18161–18167及CTA看板/配置入口，无IP、ENI/EIP、
路由、网关或二进制变更。三套 Redis 因子配置及 Exec06/07 缺失的风险配置按
本地部署证据恢复。09:19–09:20 UTC 三套factor已完成3天Kafka预热，
RX01/02/03分别46/47/34项实时就绪，signal→Exec的92/47/34项目标一致，
Manager health=ok、7个source、待投递和错误队列均0。
远端配置/仓位备份及启动日志：`~/backups/jp_exec_signal_recovery_20261018/`；CPU见核心分配登记。

2026-10-15 13:59 UTC RX02/RX03 Perp 静默回放完成并恢复最终目标发布；RX02 为
v1/17因子/47币，RX03 保持 v0/原14币。仍使用4191网关及
loopback19186/10196、19187/10197。Manager 已更新显式 clear 支持，
RX02 旧9策略55条 clear 经 Exec Redis 核验，旧绑定份额归零。
Exec05 配套更新 pre-trade 与 trade_engine，以支持 RapidX 明确的较低杠杆上限
及新运行时所需账户快照查询；实际执行已恢复。RX02 新策略发布令牌仅保存在
远端 env.sh。交易配置、local_ips、ENI/EIP、路由及监听端口均保持原值，
Exec05/07 继续使用原有交易出口。证据：`artifacts/cta_perp_replay_cutover/`。

2026-10-15 06:13 UTC Exec04 / prc 的 BNB 对冲恢复仅配套更新 pre-trade、Viz、
账户监控，复用 Exec03 已验证的 `8a263e04` release。继续保留
`local_ips=["172.31.35.228", "172.31.35.234"]`，新账户监控启动日志确认认证
账户查询显式绑定 `.228`。未改写 env.sh、交易 TOML、ENI/EIP、路由、网关或
Manager 配置；其余133个受保护进程保持。原三组件及 Viz TOML 的备份在
Manager 主目录 `backups/prc-bnb-20261015T060515Z`，BNBUSDC 空头已完成。

2026-10-15 03:47 UTC 仅替换 Exec06 / RX01 的 RapidX 账户监控（PID2309899），
修复账单可用余额极小尾差触发的恢复循环。沿用 `local_ips=["172.31.35.228"]`，
认证账户查询继续显式绑定该交易地址；env.sh、交易TOML、ENI/EIP、路由、网关
以及Manager均未改动。实时权益及账单追赶已恢复，其他190个受保护进程保持。

2026-10-14 08:05 UTC RX03 CTA Perp 已接入独立 `binance_exec_trade07` 实盘，
Manager source `trade07 / lgq_cta_RX03` 跟随 `virtual04 / cta_perp_rx03`，
倍率1，五因子各1份，14币/34组合，每组合100 USDT。启用前账户为空仓、无挂单；
新Exec使用独立namespace、Redis键及RocksDB，没有迁移旧持仓或历史。
执行模板 `binance_cta_perp_rx03_ctaker` 复制RX02当前 `lgq_cta_ctaker`，
五份实际运行参数逐项相同；买卖各最多10个挂单，10秒200单、每分钟400单。
Maker费率-0.00002、Taker 0.000175、理论TWAP 0.0004。
34个实时目标与Exec一致，账户position_ready、发布日志健康，Manager无待发布项。
证据：`artifacts/cta_perp_rx03_activation/`。
Exec07交易与认证账户查询固定使用 `172.31.35.228`，沿用RX03原RapidX白名单；
未新增EIP/ENI/路由。该环境trade_engine.toml加入Manager Kline交易IP排除清单。
4191网关新增带Manager鉴权的 `/exec_trade07/`、`config/`；对应loopback10047/18167。
信号页面继续使用 `/cta/binance-cta-perp-rx03/` 和loopback10197/19187。
信号路由持久化为 `/etc/nginx/snippets/cta_perp_rx03.conf`，由
`~/nginx_locations.txt` 的单条external映射生成；Exec07路由仍归Manager鉴权snippet。

此前观察部署记录：
2026-10-14 07:10 UTC 新增 `binance-cta-perp-rx03` 观察环境，14币/34组合、
单份100 USDT、发布关闭。Dashboard/Config分别监听loopback10197/19187，
已有4191 Nginx网关新增 `/cta/binance-cta-perp-rx03/`、`signals/`、`config/`；
保留RX01/RX02及旧RX03路径。独立Kafka因子publisher使用默认出口，
没有LTP凭证、Manager连接或执行账户绑定，无新 `local_ips`、EIP、ENI或路由变更。
原50个相关进程和55份配置哈希核对保持；07:26已完成预热，34/34组合实时就绪。
仓位发布始终关闭。publisher实测两条Kafka连接源地址均为 `172.31.35.228`。
网关修改前后配置保留在新环境 `run/`；证据：`artifacts/cta_perp_rx03_deployment/`。

2026-10-14 04:38 UTC 仅部署Exec06现货市价买入quoteOrderQty与精度修复，
exec-pre-trade最终PID448295，CPU0-5；交易绑定仍为 `172.31.35.228`。
认证账户查询重读live TOML并固定绑定该地址；未改local_ips、env.sh、ENI/EIP、路由或网关。
其余44个受保护进程与15个配置/环境哈希不变，Spread实盘发布继续。
证据：`artifacts/rapidx_cash_market_buy_fix/`。

2026-10-14 04:12 UTC 按操作者要求开启RX01 Spread实盘推送，先重建五因子
46组合最近一次100 USDT的完整绝对目标，signal PID176562、CPU0-5。
source IP、env.sh、交易TOML、ENI/EIP、路由、网页与网关不变，Exec未重启。
`binance_exec_trade06` 交易绑定仍为 `172.31.35.228`；92个目标逐项核对一致。
下方04:01关闭状态为当次部署历史。证据：`artifacts/cta_spread_rx01_live_enable/`。

2026-10-14 04:01 UTC 仅重新部署 `binance-cta-spread-rx01` 的signal目标累加修复，
signal PID46021，46/46组合就绪。该时刻发布仍关闭，04:12恢复见上文。
未改source IP、交易绑定、env.sh、ENI/EIP、路由、网页监听或网关路径；
`binance_exec_trade06` 继续使用原 `172.31.35.228`，Exec及其他发布器未重启。
已有累计目标和账户仓位未清理。证据见
`artifacts/cta_spread_rx01_absolute_target_deploy/`。

2026-10-13 13:42 UTC `binance_exec_trade06` 正式接管RX01现货/永续，使用已有
`local_ips=["172.31.35.228"]`；未新增EIP/ENI或修改路由。
Manager source06 别名 `lgq_cta_RX01`，跟随 `virtual03/cta_spread_rx01` 倍率1。
Viz/Config分别监听loopback10046/18166，通过4191鉴权网关 `/exec_trade06/`
和 `/exec_trade06/config/` 访问；lgq 已获配置管理权限。
Spread 发布已启用、46/46组合实时就绪，旧库存独立承接。下方预部署状态为历史。

2026-10-13 09:20 UTC Spread RX01清单更新为16币/46组合，publisher在CPU17
重新预热；仓位发布保持关闭，未变更独立Redis环境、网页监听、网关路径、
source IP、local_ips、ENI、EIP或路由。RX02 Perp和Exec配置保持。09:40 UTC
新清单46/46组合实时就绪。

09:39 UTC预部署RX01专用 `binance_exec_trade06`，采用RapidX执行后端；
凭据与旧RX01在远端内部核对一致，与RX02不同。复制旧RX01的
`trade_engine.toml local_ips=["172.31.35.228"]`，无新IP分配。运行包已校验安装，
所有Exec进程保持停止，Manager source/virtual尚未建立，未开始承接库存。
10046/18166为本环境保留端口，尚未启动监听。

2026-10-13 09:14 UTC 旧 special 因子发布器已停并归档部署文件，旧 RX02/RX03
均无运行进程。新 Perp 通过 virtual02/lgq_cta 组合、由 trade05 倍率1跟随；
Manager与Exec的绑定/目标一致、投递队列为空。Exec及新Perp/Spread未重启，
没有更改source IP、local_ips、ENI、EIP、路由或监听地址。下方旧publisher
保留记录为历史状态。

2026-10-13 08:57 UTC 操作者完成 RX02 切换，09:03 UTC 核验新 Perp 发布已启用。
旧 special 配置/页面服务已停，Nginx 已安装切换配置；新页面沿用
`/cta/binance-cta-perp-rx02/` 和 loopback19186/10196，旧页面路径跳转新入口。
执行仍由 `binance_exec_trade05` 使用既有 `.228` 地址完成；没有改变 source IP、
`local_ips`、EIP、ENI 或路由。旧共享因子 publisher 保留。

08:25 UTC 新 Perp publisher 已在 CPU18 完成预热（PID 4133794）。四进程在线，
发布保持关闭；本次只调整 CPU/预热超时，没有改变任何 source IP、EIP、ENI
或路由。公网 4191 的新状态页、配置页和 B/S 页均已验证 HTTP 200。

2026-10-13 07:52 UTC 新 RX02 配置页/观察页已启动，监听 loopback
19186/10196，4191 网关新增 `/cta/binance-cta-perp-rx02/`；旧 special 路径
及 19182/10192 保留。观察信号 enabled=false，不调用 Manager 发布目标。
Dashboard 直连既有 Binance Futures 公共 Klines，不新增公共 API 缓存端点。
没有新增独立 source IP、`local_ips`、EIP、ENI 或路由调整。

2026-10-13 07:35 UTC 新增 `binance-cta-perp-rx02` 预备目录，仅启动 Kafka
因子预热；仓位发布保持关闭。它不部署交易引擎、不复制 LTP 凭证、不申请独立
source IP，没有 `local_ips`、EIP、ENI 或路由变更。旧 RX02 与 trade05 原配置
保持不变；新页面 Nginx 候选文件仅验证、尚未安装。

2026-10-13 06:49 UTC 更新 Spread 只读 Dashboard，增加 signals 页面及现货
公共 K 线后台。Spot API 默认路由实测仍经 `ens41` / `172.31.35.228`，
未配置交易连接、独立 source IP、ENI、EIP 或新监听；10186/19174 与 4191
网关路径继续沿用。RX01 发布关闭，Kafka publisher、RX02 和 trade05 未重启。


2026-10-13 06:28 UTC 新增独立观察环境 `binance-cta-spread-rx01`，
发布关闭；因子进程读取 Kafka 的两条 TCP 连接现场源地址均为 `172.31.35.228`。
新配置/观察页仅监听 `127.0.0.1:19174` / `127.0.0.1:10186`，经现有 4191
网关的 `/cta/binance-cta-spread-rx01/` 访问，旧 RX01 入口跳转到新入口。
未新增交易连接、EIP、ENI 或路由；旧 RX01/RX02 的 env.sh 和 trade_engine.toml
哈希保持一致。旧 RX01 保留账户监控、持久化和 .228 配置，未迁移账户持仓。
RX02 及 trade05 未重启、未改配置。新环境独立使用 Spread universe/Redis 清单，
未写入 RX02 或 TLen 的共享因子计划。

2026-10-13 USD-M bookticker 增加双连接自动选优：启动仍使用现有 .90/.91 双路；
候选 source pool 仅为 `172.31.46.90` / `172.31.46.91`，远端取实时 DNS。
晋升后两条连接允许来自同一 source IP、不同远端；实际分配查看
`binance_bbo_selection` 状态日志，不再把固定 primary/secondary 当作永远不变的 socket。
不新增/重分配 EIP、私网 IP、ENI，不改路由、IRQ 或交易环境的 `local_ips`。
COIN-M 与 market 角色继续原连接策略；详见 `docs/binance_futures_latency.md`。

2026-10-13 04:04 UTC Binance futures 行情延迟观测发布：仅替换 spread_pbs、
BBO relay 二进制及启动脚本中的观测开关透传；env.sh、ENI、EIP、路由、
`local_ips` 与源 IP 分配未修改。三个进程继续使用既有环境；绑核见 core_allocation。

绑核登记见 `docs/core_allocation.md`。本文件只记公网/私网 IP 与策略环境的对应关系。

2026-10-13 03:09 UTC RX01 的 trade_engine、trade_signal、pre_trade 已通过
环境内脚本停止并核验退出，账户监控和数据/页面服务保留。环境及 source IP
配置保留，`172.31.35.228` 未释放；ENI、EIP、路由与 `local_ips` 均未修改。
只读核验挂单为 0，未执行仓位中性化交易。

2026-10-13 02:39 UTC RX02 图表 B/S 恢复仅更新历史导入脚本并重启 dashboard，继续使用本机 `127.0.0.1:10192` 和既有 4191 代理；source IP、`local_ips`、ENI、EIP 及路由均未变更。

不要把公网 IP 直接加到 Linux 网卡上。验证出口用：

```bash
curl --interface <private-ip> https://checkip.amazonaws.com
```

## 约定

`ens41`（主网卡，metric 100）上的 `172.31.35.228/20`–`.234/20` 对应 7 个公网 IPv4：

- `.228` / `13.115.227.29`：主 IP、默认出口、SSH/管理入口；**套利**固定 IP。
- `.229` / `52.193.90.33` 与 `.230` / `54.238.72.43`：MM 使用（当前 `okex_mm_alpha`；原共用的 `binance_mm_alpha` 已下线退役）。
- `.231` / `52.69.78.134`：资金费率固定 IP。
- `.232` / `54.238.97.67` 与 `.233` / `54.64.165.84`：`gate_fr_arb02`。
- `.234` / `54.64.228.233`：Exec `trade02`、`trade03`、`trade04` 的第二个 source IP 配置。

`ens42`（第二块网卡，metric 200）上的 `172.31.46.90/20`–`.93/20` 已绑定。无 `trade_engine.toml` 引用，但现场行情/`ipc_bridge` 已有 socket。不要在未更新本表前改作交易 source IP。

## 主机

- SSH 别名：`jp-meta-elvpn`
- hostname：`ip-172-31-35-228`
- `ens41` MAC：`06:52:65:8f:d7:37`；DHCP 主地址 `.228`，其余为 secondary
- `ens42` MAC：`06:17:74:aa:b2:f9`

## ens41 映射

| 私网 IP | 公网 IP | 状态 | 当前用途 |
| --- | --- | --- | --- |
| `172.31.35.228` | `13.115.227.29` | 已使用 / 固定 | 套利；SSH/默认出口。`binance-cta-rx01`、`okex-intra-arb01`；RX01 Spread 接 Exec `trade06`，RX03 Perp 接 Exec `trade07`；Exec `trade02`–`trade04` `local_ips[0]`；新 `trade08` 原生PM监控。RX02 / `trade05` 已停机归档 |
| `172.31.35.229` | `52.193.90.33` | 已使用 | `okex_mm_alpha` `local_ips[0]`（原 `binance_mm_alpha` 已退役） |
| `172.31.35.230` | `54.238.72.43` | 已使用 | `okex_mm_alpha` `local_ips[1]`（原 `binance_mm_alpha` 已退役） |
| `172.31.35.231` | `52.69.78.134` | 已使用 / 固定 | 资金费率。`binance_fr_arb01`–`04`、`gate_fr_arb01`/`03`、`bitget_fr_arb01`/`02`/`03` |
| `172.31.35.232` | `54.238.97.67` | 已使用 | `gate_fr_arb02` `local_ips[0]` |
| `172.31.35.233` | `54.64.165.84` | 已使用 | `gate_fr_arb02` `local_ips[1]` |
| `172.31.35.234` | `54.64.228.233` | 已配置 | Exec `trade02`、`trade03`、`trade04` `local_ips[1]` |

## ens42 映射

| 私网 IP | 公网 IP | 状态 | 当前用途 |
| --- | --- | --- | --- |
| `172.31.46.90` | `52.69.209.108` | 已使用 | 现场 socket：`ipc_bridge`、各 `spread_pbs`、`spread_bbo_zmq_pub`（未写入 `trade_engine.toml`） |
| `172.31.46.91` | `54.199.82.56` | 已使用 | 少量 `spread_pbs` |
| `172.31.46.92` | `52.192.54.88` | 已使用 | `rclone mount` |
| `172.31.46.93` | `18.181.48.65` | 已使用 / 非交易 | CTA Manager 1 分钟 K 线 REST 缓存；少量 `ipc_bridge` socket |

`ens42` 没有策略 `local_ips` 引用；行情和 IPC 连接已使用这块网卡。
2026-10-09 03:04 UTC 新增 `~/spread_pbs/gate-bitget-futures-bbo`（CPU16）：
Gate 和 Bitget USDT 合约各两条 BBO 连接，primary 绑定 `172.31.46.90`、
secondary 绑定 `172.31.46.91`。现场 `ss` 核验共四条 TLS 连接均使用这两个源 IP，
`ip route get` 均经 ens42 / table 101。复用现有行情源地址，未新增 IP 或路由规则。
2026-10-05 部署 CTA Manager 分钟 K 线缓存：`[kline].local_ip = "172.31.46.93"`，
`public_ip = "18.181.48.65"`，请求 Binance USDⓈ-M `/fapi/v1/klines?interval=1m`。
通过 IMDSv2 与绑定私网地址的外部请求核验映射；检查全部 24 份现场
`trade_engine.toml` 后确认 `.93` 未用于下单。Manager 排除所有交易私网地址和
`ens41` 的 7 个交易公网地址；`0.0.0.0` 交易绑定按默认路由 `.228` 处理。
`.93` 保留给非交易请求；不要同时用作交易 source IP。此次未修改任何交易配置。

本机 `ip -br addr`（2026-08-16）：

```text
ens41  172.31.35.229/20 172.31.35.230/20 172.31.35.231/20
       172.31.35.232/20 172.31.35.233/20 172.31.35.234/20
       172.31.35.228/20 metric 100
ens42  172.31.46.91/20 172.31.46.92/20 172.31.46.93/20
       172.31.46.90/20 metric 200
```

## 当前 `trade_engine.toml local_ips`

2026-10-04 15:11 UTC 核验并重新发布 `gate_fr_arb03`（`322c482c`，Gate WS 保证金
恢复解锁）：两个交易连接仍绑定 `172.31.35.231`，部署前后 `trade_engine.toml`
校验一致。

2026-10-07 15:38 UTC 按 publish FR 流程重新发布 `binance_fr_arb03`（`d35f67ea`）：
两个交易连接的 `local_ips` 仍为 `172.31.35.231`，trade_engine / account_monitor
启动日志已核验该 source IP；部署前后 `trade_engine.toml` 和 `env.sh` 校验一致。

2026-10-14 22:46 UTC 重新发布并恢复 `binance_fr_arb04`：两个连接的
`trade_engine.toml local_ips` 仍为 `172.31.35.231, 172.31.35.231`；
环境根目录 `trade_engine.toml`、`env.sh` 与 `config/viz.toml` 部署前后 SHA-256
一致。共享 `delist_risk_server` 同步发布并恢复健康，未修改监听配置、源 IP、
EIP、ENI 或路由。精确发布清单与核验记录见
`artifacts/binance_fr_arb04_republish/20261014T2227Z/`。

2026-10-08 06:00 UTC 完成 `gate-intra-arb01`、`bitget-intra-arb01` 退役，
RocksDB 归档并逐文件校验后删除部署目录及 `local_ips` 配置；共享出口 `.228`
仍由其他环境使用。`bitget-gate-cross-arb01` 的 Bitget 凭据已独立写入其 `env.sh`，
与原 `bitget-intra-arb01` 相同，Gate 凭据保持一致；Cross 配置服务已重新加载，
交易栈仍停止，不依赖已删除的目录。

```text
binance-cta-rx01             172.31.35.228                  RapidX/LTP（仅账户监控与持久化保留）
binance-cta-spread-rx01      172.31.35.228                  配置服务、因子及signal运行，Kafka已追实时，发布关闭，Exec06停机；Kafka 默认出口
binance-cta-special-rx02     172.31.35.228                  已停止并归档，原配置保留
binance-cta-perp-rx02        默认出口 / 172.31.35.228       已停止并归档，不迁移策略
binance-cta-perp-rx03        默认出口 / 172.31.35.228       独立Perp实盘信号，loopback10197/19187，发布到Exec07
binance-cta-perp-rx04        默认出口 / 172.31.35.228       独立5min观察信号，loopback10198/19188，未绑定Manager/Exec
binance-cta-special-rx03     172.31.35.228                  RapidX/LTP（停盘，无运行进程）
okex-intra-arb01             172.31.35.228, 172.31.35.228
okex_mm_alpha                172.31.35.229, 172.31.35.230
binance_fr_arb01             172.31.35.231, 172.31.35.231
binance_fr_arb02             172.31.35.231, 172.31.35.231
binance_fr_arb03             172.31.35.231, 172.31.35.231
binance_fr_arb04             172.31.35.231, 172.31.35.231
gate_fr_arb01                172.31.35.231, 172.31.35.231
gate_fr_arb03                172.31.35.231, 172.31.35.231
bitget_fr_arb01              172.31.35.231, 172.31.35.231
bitget_fr_arb02              172.31.35.231, 172.31.35.231
bitget_fr_arb03              172.31.35.231, 172.31.35.231
gate_fr_arb02                172.31.35.232, 172.31.35.233
bitget-gate-cross-arb01      0.0.0.0, 0.0.0.0
binance_exec_trade01         0.0.0.0, 0.0.0.0
binance_exec_trade02         172.31.35.228, 172.31.35.234
binance_exec_trade03         172.31.35.228, 172.31.35.234
binance_exec_trade04         172.31.35.228, 172.31.35.234
binance_exec_trade05         172.31.35.228                  RX02 RapidX，全部停止并归档，Manager禁用且解除跟随；原配置保留
binance_exec_trade06         172.31.35.228                  RX01 RapidX，在线，follow virtual03/cta_spread_rx01，ctaker；默认4倍（BEL/ZIL上限3倍）
binance_exec_trade07         172.31.35.228                  RX03 RapidX，永续在线，follow virtual04/cta_perp_rx03，ctaker
binance_exec_trade08         172.31.35.228, 172.31.35.228   lgq_ltp_81857，原生PM USD-M，仅监控/持久化/页面，无策略绑定，交易停止
```

`0.0.0.0` 表示不绑独立 source IP，走默认出口（`.228`）。

## 已退役 intra RocksDB 归档

以下两份 intra RocksDB 数据归档及各自 `.sha256` 文件位于 JP `~/retired_data/`，
权限为 `600`；归档不含 `env.sh`、配置、凭据或程序。两份归档均已核对完整文件清单、
文件大小与逐文件 SHA-256，回收两个部署目录约释放 4.28 GB。

| 环境 | tar.gz 文件 | 压缩后字节数 | SHA-256 |
| --- | --- | ---: | --- |
| `gate-intra-arb01` | `gate-intra-arb01_rocksdb_20261008T055407Z.tar.gz` | 150730111 | `093066d7667078c3f04158254e353df996794a4e042ce6316d959f7dda1bca8f` |
| `bitget-intra-arb01` | `bitget-intra-arb01_rocksdb_20261008T055407Z.tar.gz` | 519207764 | `c5968d1ba61241ed6584a6ca4a69ac0c1d14ba75ba91f5248b676859a1e00b4d` |

## 现场 socket 快照（2026-08-16）

| 私网 IP | 已建立 socket 数 |
| --- | ---: |
| `172.31.35.228` | 78 |
| `172.31.35.229` | 203 |
| `172.31.35.230` | 10 |
| `172.31.35.231` | 195 |
| `172.31.35.232` | 5 |
| `172.31.35.233` | 5 |
| `172.31.35.234` | 0 |

## 使用规则

1. 改 `local_ips`、给环境分配新 EIP、或下线占用 IP 的环境后，立刻改本文件的日期、表格和引用列表。
2. `.228` 不要改作他用。
3. `ens42` 的四个地址在写入任何 `trade_engine.toml` 之前先占表；`.234` 已由 Exec 环境共用配置。
4. `okex_mm_alpha` 现独占 `.229/.230`（原共用的 `binance_mm_alpha` 已下线退役）。

2026-10-10 06:42 UTC 有符号权重与 rx02 Kafka 仓位回放发布沿用现有本机服务和 Nginx
4191 网关，未更改 source IP、ENI、local_ips 或路由。rx02/rx03 均继续关闭真实交易。

2026-10-10 10:30 UTC 两个 special 环境改为独立因子策略，新增准备目录 `binance-exec-cta-special-rx02/rx03`，其 env.sh 引用原环境，trade_engine.toml 也引用原配置；执行进程没有启动。不分配额外 EIP 或 source IP，不改写 `.228`、ENI、local_ips、路由及既有 4191 Nginx 代理。真实交易关闭，零持仓/挂单，CPU 仍为 housekeeping 0-5。

2026-10-11 RX02 执行迁入独立 `binance_exec_trade05`，只有一个 source IP `.228`，沿用既有 RapidX 账户白名单。Manager 的 RapidX sym/info 是签名私有请求，绑定该 source 的 trade_engine.toml 首地址；`.93` 仍只供非交易行情请求，未加入 RapidX 请求。新 trade05 配置加入 Manager Kline 交易 IP 排除清单。RX03 停止，原配置和历史保留。没有新增 EIP、ENI 地址、策略路由或 CPU pin。

2026-10-11 05:46 UTC trade05 待执行量及权益修复只更新 exec-pre-trade、Viz 和 Manager 程序，未改动账户 source IP、ENI、EIP 或路由；RX02 执行继续使用 `.228`。

2026-10-11 07:07 UTC trade05 完成状态修复仅替换 exec-pre-trade/Viz，source IP 继续使用 `.228`，未变更 ENI、EIP 或路由。

2026-10-11 NAV 行情监控迁入独立 `fr_monitor`（只读 IPC/Viz/RocksDB，回环 18180），停用 `public-infra-monitor` 与常驻 BPF。未分配交易 source IP，未修改 ENI、EIP、trade_engine local_ips 或路由；NAV 中 RX02/RX03 退役数据由 `binance_exec_trade05`（别名 `lgq_cta`、既有 RapidX `.228`）替代。原交易目录及 RocksDB 保留。

## SG Bybit Exec 预部署

2026-10-21 01:04–01:15 UTC，`bybit-intra-arb01` 中性对齐后恢复全部进程，
`local_ips=["172.31.7.124", "172.31.7.124"]` 及 env.sh 哈希保持。
恢复共享 IPC 时同步重启共享 Bybit 行情链路和 `bybit_mm_alpha`；两账户的交易/监控
外连源 IP 均实测为 `172.31.7.124`，共享 spread 的 12 条行情连接均为
`172.31.10.217`。独立 Exec publisher PID 3818626 / CPU 6 未重启，其 14 条行情
连接同样使用 `172.31.10.217`。未分配新 IP，未改动 ENI/EIP、路由或监听端口。
intra arb02 仍停止；CPU 和恢复记录见 `core_allocation.md`，证据见 SG
`~/backups/bybit_intra_arb01_restore_20261021/socket_sources.json`。

2026-10-19 08:21–08:28 UTC，原 intra arb02 的预备Exec 02迁至
`/home/ubuntu/bybit_exec_trade01`（`bybit-01`），LGQ configure、follow virtual01。
新目录 `trade_engine.toml local_ips` 仍为 `["172.31.7.124", "172.31.7.124"]`，
沿用同一账户凭据和下单ENI公网出口 `47.131.162.78`；四条账户监控外连均核验为
`172.31.7.124`，未新增IP或修改ENI/EIP/路由。原20份非Manager配置哈希不变。
仅账户监控、persist、Viz、Config及Manager运行0–7；新Exec执行进程、旧Exec 02、
两套intra全停。公共规则继续使用行情ENI，未接RX02实时推送。
用户取消旧订单归档要求，旧数据留在原目录；08:22:01.653 UTC新快照包含6条现货净仓、
6条USDT永续及余额/借币审计依据，未增加现货下单范围。服务和CPU见`core_allocation.md`，
证据见独立Manager仓库 `artifacts/sg_exec01_20261019/`及`docs/sg_deployment.md`。

2026-10-19 08:02 UTC 复核：仅升级 SG CTA Manager，release
`20261019T080045Z`、checkout `6e422e1`，健康及LGQ鉴权入口通过。
当时 `bybit_exec_trade02`（别名 `bybit-02`）归LGQ配置管理；后续已迁至Exec 01，见上文。
账户监控4条外连源地址均为`172.31.7.124`，`local_ips`、env.sh及21份配置哈希一致，
没有新增源IP或调整ENI/EIP/路由。Exec交易进程、旧intra保持停止，未接RX02实时推送。
旧arb02约551 MiB数据仍保留原路径，后续用户明确不再要求归档，未做历史迁入；既有
`runtime-before.tar`不含订单数据。完整核验见独立Manager仓库
`artifacts/sg_manager_20261019/`及`docs/sg_deployment.md`。

2026-10-16 10:15 UTC 新增 `/home/ubuntu/bybit_exec_trade02`，lgq 管理、别名
`bybit-02`。`trade_engine.toml local_ips` 为 `["172.31.7.124", "172.31.7.124"]`，
沿用原 arb02 的下单 ENI / 公网出口 `47.131.162.78`，没有新增 IP 或更改路由。
账户监控全部外连已核验使用 `172.31.7.124`；trade_engine、exec-pre-trade、
trade_signal 保持停止，旧 Bybit intra01/02 也保持停止。Manager 公共规则缓存使用
行情 ENI，独立 namespace / Redis 前缀 / RocksDB，未搬迁旧 intra 持仓与历史。
服务与 CPU 清单见 `core_allocation.md`；证据见
`artifacts/sg_bybit02_exec_deploy/20261016T100107Z/validation.json`。
