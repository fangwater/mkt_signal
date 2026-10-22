# RX02 切换到 CTA Perp

最后更新：2026-10-22 15:06 UTC

15:00 UTC，新账户 `lgq_ltp_81857` / `binance_exec_trade08` 已开始实盘执行 RX02。
JP virtual02 由 Exec08 以倍率1跟随，唯一绑定 `lgq_cta_symbol_mean`，使用现场
`lgq_grid_prep`：合约3档、10秒一轮、无限Maker重挂、单手100 USDT、Vol倍数1、
目标容差10 USDT；RX02仍为23币、单币50 USDT。旧trade05继续停用且无进程。
按用户明确授权，14:55:55 UTC单次POST开启 Binance USD-M Multi-Assets Mode，
回执code200，回读true；单向持仓`dualSidePosition=false`/`BOTH`保持。STANDARD运行模式不改为PM。
多资产模式下账户汇总为交易所折算USD，不能用该汇总值检查USDT币余额是否等于20000。
15:06验收：5个非零目标全部在原容差内完成，8笔实际成交均Maker，挂单0；
Manager待投递0，JP/SG推送无错误，RLC仍在预热且目标clear。
Exec08的engine/executor PID342673/343474、Config PID315020，均CPU0–5；
Manager PID315038固定CPU24，RX02 signal PID351338更新页面跟随账户关联。
publisher PID160373仍CPU18。交易source IP `.228`、监听10048/18168及4191网关保持。
配置入口 `http://13.115.227.29:4191/exec_trade08/config/`（Manager登录后可用）。
Grid兼容改动经过1354项Exec库测试、398项Manager库测试及42项Python测试，
本地release构建、SHA-256核验并原子安装。证据在
`artifacts/rx02_exec08_grid_20261022/`，远端回执/私有备份在Exec08的
`run/grid_20261022/`；以下各时点描述为历史状态。

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

## 2026-10-13 首次迁移（历史记录）

目标主机 `jp-meta-elvpn`；旧目录 `~/binance-cta-special-rx02`，新目录
`~/binance-cta-perp-rx02`。二进制来自本地 `arbmm` 提交 `976d557e`。
新清单为 9 币、5 因子、24 项绑定，NQ 为 120/min60；单份沿用线上
100 USDT，策略前缀沿用 `lgq_cta_`。Spread RX01 不在操作范围内。

09:14 UTC 已按要求退役旧共享 special 因子发布器：旧 RX02/RX03 对应运行
文件移出环境目录并归档，启动开关关闭，全机无旧因子发布进程。新 Perp、Spread、
Exec 进程不变，新 Perp 全24组合继续分钟更新。历史数据与仓位记录保留。

只读查询 Manager 实际持久配置并比对 Exec 接收配置：`virtual02 / lgq_cta`
包含 baseline_141、baseline_200、TD_TI_024、baseline_164、factor_036 各1份；
TD_TI_002、baseline_004、factor_051、factor_170 为0份。唯一跟随账户为
`binance_exec_trade05`，倍率1，执行策略统一 `lgq_cta_ctaker`；9项绑定与真实
账户生效绑定一致，投递队列为空，各策略目标逐项匹配。factor_036中的9个历史
币种仍以零目标保留用于清理，唯一活跃配置为NEAR，不计入新24组合。
证据：`artifacts/cta_rx02_virtual_validation/validation.json` 及
`artifacts/cta_rx02_virtual_validation/after_retirement.json`。检查时所有目标为零，
该核验确认组合配置与投递成功，不声称已验证新策略非零目标成交。

**当次状态：已正式启用。** 操作者在 08:45 UTC 先退休31个旧组合，08:57 UTC
完成正式切换；`run/cutover_completed.json` 已写入，不要重复运行切换命令。
09:03 UTC 只读核验：enabled=true、24/24实时就绪、发布无错误，旧仓位信号已停，
共享因子发布器保留。Exec 已安装全部五个新因子（含保留的factor_036）和四个
退出因子的零份绑定；检查时没有非零目标、待执行量或活动订单量，旧账户零碎
空仓约16.53 USDT。零目标表示当时尚未触发开仓条件，不表示发布未启用。
下方部署/预热记录及命令用于记录此次操作过程。

只清理旧组合、暂不启用新策略时，使用独立的操作者脚本：

```bash
ssh jp-meta-elvpn 'cd ~/binance-cta-perp-rx02 && source env.sh && python3 scripts/retire_cta_rx02_bindings.py'
ssh -t jp-meta-elvpn 'cd ~/binance-cta-perp-rx02 && source env.sh && python3 scripts/retire_cta_rx02_bindings.py --execute'
```

第一条只读；第二条由操作者执行，会触发真实平仓。按新 symbol × factor 清单
保留 `NEARUSDT / factor_036`，移除其余 31 个旧组合，不因为币种仍在新清单中
而保留旧因子仓位。脚本备份旧清单与目标仓位，通过现有配置 helper 原子缩减
旧 Redis 清单，再同步旧配置文件；旧信号继续运行，为退出组合持续发布零目标。
保留组合继续按原信号运行，新 Perp 发布保持关闭。不会删除持仓账本、改 Manager
份数、启动新策略或重启 Exec。默认模式不写任何远端状态。

`[TARGETS ZERO]` 仅表示旧发布器已加载缩减清单、目标归零且未报告发布错误；
必须另外检查 Manager 投递和 Exec 实际仓位/成交。异常时不自动恢复旧清单，
以免已经平掉的旧组合重新开仓。此次退休不生成切换 started 标记，后续正式
切换仍从旧发布器读取最终状态，保留已退出组合的零目标。

08:25 UTC 新版预热完成，24/24 组合实时就绪、9/9 NQ 就绪；四个进程在线，
新仓位发布保持关闭。publisher PID 4133794 位于 CPU18，另外三个进程位于
0-5。新旧 RX02 及 Exec 仍各自独立运行，没有执行仓位切换。B/S 页面已验证
历史开多、开空、平仓标记及快照点击；观察日志没有实际仓位发布记录。
08:25 和 08:26 两根闭合分钟的全部 24 个组合均继续更新，切换脚本只读检查
已返回 `[READY]`；证据为 `artifacts/cta_perp_rx02_preview/continuity_validation.json`。

07:52 UTC 新配置页、Dashboard 和 enabled=false 的观察信号已全部启动，
旧 RX02 四进程保持运行。新页面路径已生效，旧页面尚未重定向。07:56 UTC
浏览器验证清单、金额、五个因子回放及手机布局通过；Kafka 仍在预热。

07:35 UTC 文件已安装，SHA-256 校验通过；新因子发布器 PID 3841796 已启动，
07:36 UTC 仍在 Kafka 历史预热。新信号未启动，旧 RX02 保持运行。切换脚本默认
检查已通过，实际 Manager 绑定检查需在操作者执行时登录完成；没有执行交易切换。
07:37 UTC 复核历史持续推进：NQ 历史就绪 9/9，因子历史就绪 0/24，仍未追上
实时。现场证据：`artifacts/cta_perp_rx02_staging/validation.json`。

发布文件和因子预热不等于启用仓位发布。预备环境的配置保持 `enabled=false`，
允许启动信号进程观察模拟状态；旧信号仍负责实时仓位。新配置页使用 19186，
Dashboard 使用 10196，与旧页的 19182/10192 独立。新页面路径为
`/cta/binance-cta-perp-rx02/`，因子 B/S 页为其 `signals/`，配置页为其 `config/`。
观察模式的方向和仓位不能当成实际目标或成交。
Perp 冷启动预热超时为 3600 秒，RX02 publisher 使用 CPU18。首次运行曾在
housekeeping 核争用下触及旧的 1800 秒上限；不能把进程曾启动视为已预热成功。
部署同时安装 `config/cta_perp_replay.json`，从现有历史最早记录开始回放。
默认模拟本金 10000 USDT、费用为零，只用于展示毛收益，不能当成实际资金或净收益；
每个组合的模拟下单金额仍取信号配置的 100 USDT。已有回放配置不被覆盖。

先只读检查安装内容与预热：

```bash
ssh jp-meta-elvpn 'cd ~/binance-cta-perp-rx02 && source env.sh && python3 scripts/activate_cta_perp_rx02.py'
```

需要操作者在自己的终端执行实际切换：

```bash
ssh -t jp-meta-elvpn 'cd ~/binance-cta-perp-rx02 && source env.sh && python3 scripts/activate_cta_perp_rx02.py --execute'
```

脚本现场询问 Manager 管理员用户名与密码，密码不回显、不写磁盘。默认运行
不访问需登录的 Manager 配置；`--execute` 首先登录并只读核对全部绑定。
若预热未追上实时、任一组合未就绪、文件校验失败、单份参数或 Manager 配置
已变化，脚本在停旧信号之前退出。9 个币种的历史必须新鲜且配置的 24 个
因子/NQ 均就绪。保持旧共享 special 因子发布器运行，避免影响其他消费者。

切换步骤：

1. 核对 `lgq_cta` 只有 `binance_exec_trade05` 一个跟随账户、乘数 1，旧五个
   策略各一份，执行模板 `lgq_cta_ctaker`，没有待发布配置。
2. 先停止新环境的观察信号，再通过旧环境脚本停仓位发布器并确认退出；备份
   新环境模拟状态，完整复制旧环境最终仓位状态，包括已被
   移除的因子和币种。保留 NEAR × factor_036 的原数量和历史策略归属。
3. 注册四个新因子策略的零目标；Manager 中保留旧绑定身份，将退出清单的
   四个旧因子份数置零，新增四个因子各一份。此步骤可能立即触发真实平仓。
4. 切换配置页、Dashboard 和仓位发布器；新发布器以 enabled=true 启动，
   依靠历史分钟保护承接旧数量，只用启动后的新分钟产生新开仓信号。
5. 检查仓位发布状态，再把 Nginx 路径迁移到 `/cta/binance-cta-perp-rx02/`；
   旧页面路径重定向到新路径。实际成交仍由 Exec 决定。

迁移没有承诺旧仓平仓与新策略开仓原子净额撮合；先到达的零目标可能先成交。
保留原策略并发布零目标，不主动删除 Exec 策略或操作其持仓账本。

脚本保存 `run/cutover_started.json`、完整旧仓位快照和 Nginx 备份。
开始变更后若失败，将禁用并停止新仓位发布器，保留现场；不自动恢复旧发布器，
因为部分新目标可能已成交。出现 started 标记后拒绝自动重跑，须核对 Manager
实际配置、Exec 仓位和发布状态后制定恢复步骤。只有成功切换才写 completed 标记。

切换后核验：新信号持续更新且无 publication_error、五个新因子策略按原份数
跟随、四个旧因子为零份、旧 factor_036 退出币种为零目标、Manager 发布队列
完成，并查看 Exec 实际持仓与订单。服务健康不等于交易已完成。
