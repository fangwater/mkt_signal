# sh_dev 易达 paper 与 Viz

最后更新：2026-11-23

用户指定的部署主机为 `ssh sh_dev`，运行目录 `/home/ubuntu/yida_paper01`，使用 SDK `1.502.55.35`。
2026-11-18 易达官方仿真恢复登录，作为 `sh_dev_yida_paper01` 接入国内期货 Manager。
易达与 [创元 CTP](sh_dev_ctp.md) 分别使用独立的账户来源、持仓账本和计划订阅。

## 当前服务

2026-11-18 更新 8811 Viz。资金区展示七项柜台资金指标，持仓展开显示方向、总手数和今昨仓。公网浏览器验证 CTP 资金经 WebSocket 更新。

| 服务 | 配置／入口 | 状态与范围 |
| --- | --- | --- |
| `yida_paper01-trader.service` | `config/yida.toml`、`config/server.cfg` | `public_simulation=true`，TCP，订阅 Manager 的仿真目标 |
| `yida_paper01-persist.service` | `bin/persist_manager --core 3` | 写 `data/persist_manager`，stdout 即 trader 日志 |
| `yida_paper01-viz.service` | `config/viz.toml` | 按用户要求监听 `0.0.0.0:8811`，浏览器直接访问 VPS 公网地址的 8811 端口，无需 SSH 转发 |

服务由 Ubuntu 用户的 systemd 管理，已开启 user linger，退出 SSH 后继续运行。交易端只在冷启动状态码 75（柜台重启、登录／恢复超时）时由 drop-in `10-cold-restart.conf` 在 60 秒后重启，与创元 CTP 相同；登录／认证错误等其他退出不自动重启，避免重复提交。persist_manager 与 Viz 失败时自动重启。所有启停都通过环境内脚本，不直接操作监管器。

当前执行范围为 `SHFE:rb2701`，单笔和绝对目标上限均为 1 手。Manager 组
`yida-paper01-rb` 按当前柜台对账快照初始化为零手，执行端通过同一条 SSH 反向隧道订阅 gRPC。
账户完整恢复和展示既有持仓。已启用 `observation_l1=true`，同一 SDK 登录订阅现有持仓和执行合约。
接入验收时，Manager 已同步 30 条持仓，执行端收到 `no_batch`。

2026-11-23 北京时间约 13:00 随创元一起换装 arbmm `551831fc`（目标无 lease／交易日、无本地限速，见
[创元记录](sh_dev_ctp.md#目标常驻执行2026-11-23)）：`yida-trader` sha256 `d4e46ae4…`，`persist_manager`
`00d7e73e…`；`yida.toml` 删除 `max_actions_per_second`、`target_max_age_ms`。SDK 头文件取自本地
`~/ds/ydClient_1_502_55_35`（`ydApi.h` 哈希与 [易达说明](yida_trade.md) 一致）。易达仍在未确认报单时阻断账户
（`query_unconfirmed_orders = false`）。旧二进制和配置备份在 `data/ops/no-lease-20261123T045916Z/`。
双 SDK feature 的 cn_futures 197 项测试在 sh_dev 通过；Manager 为 `no_batch`，上线未下单。

## 操作入口

```bash
ssh sh_dev
cd /home/ubuntu/yida_paper01
./scripts/yida_paper_env.sh status
./scripts/yida_paper_env.sh state

# 停止前归档日志，通过私有 FIFO 让交易进程正常 shutdown；不撤销柜台订单。
./scripts/yida_paper_env.sh stop trader
./scripts/yida_paper_env.sh start trader

# 整体启停：start 依次拉起 viz、persist、trader；stop 先停 trader，等待 2 秒后再停 persist。
./scripts/yida_paper_env.sh stop
./scripts/yida_paper_env.sh start

# 只更新或启停页面服务时，使用 viz 组件。
./scripts/yida_paper_env.sh stop viz
./scripts/yida_paper_env.sh start viz
```

环境目录为 0700，`env.sh` 和私有配置为 0600。启动脚本在
`YIDA_PAPER_EXECUTE=1` 时校验官方仿真配置、同源的 `[exec]` 与 Manager 订阅凭据，然后传入 `--execute`；
另校验 `IPC_NAMESPACE` 等于 `position_ipc.namespace`、`persistence_directory` 为本环境 `data/persist_manager`，
否则 persist_manager 收不到记录。
交易 stdin 由权限 0600 的 `run/commands.fifo` 保持，`state` 响应写入私有日志。
正常停止先发送 `shutdown`，等待 SDK 尾部回报和持久化完成。

应用亲和性：业务 CPU 0、SDK CPU 1、后台 CPU 3、NUMA 节点 0；persist_manager 与 Viz 在 CPU 3。
交易端是云主机部署，`poll = { mode = "sleep", idle_us = 100000 }`：空闲时挂起，原生回报或 L1 一到立即唤醒，最长 100 ms 检查一次 Manager 目标与超时，不自旋占用业务核。主机暴露 4 个不同 core ID，CPU 2 没有分配给该环境。没有修改内核 CPU 隔离或 IRQ 策略，不宣称独占物理核或实测低延迟。

## 数据与验证

- 持仓服务：`yida_paper01/viz_pubs/cn_futures_positions`，默认每秒全量推送；Viz 的持仓不回退读库。
- Exec 库：`data/persist_manager`（`cn_uniform_orders` / `cn_trade_updates` 两张二进制表，见[持久化](cn_order_persistence.md)），
  由同目录的 persist_manager 写入；云端导出器 `sh_dev_yida_paper01` 读同一路径。Viz 只订阅持仓 IPC，不读库。
- 日志：`logs/trader.log`（persist_manager 打印的 trader 日志，交易进程 stderr 也追加于此）、`logs/persist.log`、`logs/viz.log`；启停归档位于 `data/stops/`。
- 配置和二进制在本地准备；上传至独立临时目录，逐文件 SHA-256 校验后原子安装。本机和远端的部署收据均不包含凭据。

2026-11-13 只读验证：官方 SDK 版本检查、登录及完整账户恢复通过，连接／传输／核心均就绪。柜台返回交易日 `20261112`，保留该原生值；30 条非零持仓持续推送，`can_trade=false`、投影错误 0、本次报撤单意图 0。正常停止并重启后，publisher epoch 已更新，持仓基线 SHA-256 一致，持久化读取正常。没有提交测试订单，因此本轮结论是 paper 登录、恢复、持仓及观察链路正常。

前端沿用当前 U 本位 Exec 的紧凑样式，默认按合约查看净持仓，展开后查看多空、今昨仓及冻结明细；不显示账户名称、账户列或账户筛选，交易所以中文简称显示。数量以原生手数显示，预算以人民币元显示；合约显示中文品种名与到期年月，期权另显示看涨／看跌与行权价；保留原始代码，支持中文搜索。人民币金额按用户确认的“净手数 × 原生合约乘数 × 易达 L1 最新价”计算，期货显示净名义金额、期权显示净权利金市值；不当作保证金、盈亏或期权标的敞口。缺价显示未知；历史行情明确标记并展示原生日期／时刻。执行进程 → IPC → Viz → WebSocket 直接推送，页面没有 HTTP 数据初始化、读库、委托／成交页或订单状态机。公网 HTTP 与 WebSocket 使用同一端口，`status` 读取 `/healthz` 的来源健康摘要。

本地私有收据目录：`data/ops/sh_dev_yida_paper01_20261113T144307Z/`（Git 忽略）。远端旧收据已在 2026-11-21 清理中删除。

2026-11-13 直推改造验收：本地 6 项状态契约测试、10 项 Viz 测试、Clippy 及浏览器契约检查通过；公网浏览器收到连续 WebSocket 心跳，30 个合约／30 条持仓、多头 506 手、空头 387 手，页面没有 `/snapshot` 或 `/healthz` 数据请求。仅重启 Viz 的发布前后交易进程 PID 相同，持仓摘要一致。桌面与手机截图及收据在上述私有目录的 `direct-ws-*` 文件中。

部署检查另发现 paper 曾于主机本地时间 2026-11-14 00:00:41 因 `native SDK fatal/missing-order event; stop and recover` 退出。已保留日志，通过环境脚本只读恢复一次，完整持仓与连接恢复；未改为自动重启，也未提交报单或撤单。

2026-11-14 前端调整：移除账户名称、账户列与账户筛选，聚合行／展开行／分方向行及筛选器统一显示中文交易所。现有浏览器契约检查、本地 release 构建及公网桌面／手机检查通过。本次检查时 paper 已异常退出，归档日志后按原有只读配置恢复一次；30 条持仓恢复，原生交易日为 `20261113`、`can_trade=false`。随后仅替换并重启 Viz，交易进程 PID 和 Viz 配置保持一致。收据及截图：`data/ops/cn_viz_labels_20261114*/`（Git 忽略）。

2026-11-14 用户要求清掉全部期权：原有 20 条期权持仓合计 462 手。通过当前已校验 release 二进制的原生市价平昨／FAK 路径，柜台事实成交累计平掉 **189 手**，仍剩 **273 手、11 条期权持仓**，未宣称全部完成。剩余合约连续三轮完整 FAK 轮询均为零成交撤销；额外一笔 `lu2701C3550` 的市价 Day 卖平等待 90 秒仍未成交，已撤回并重新查询确认终态。最终未完成委托 0、冻结 0，原有 10 条期货持仓数量完全一致，常驻服务已恢复原有只读配置。

本轮临时执行使用原生期权元数据与分日期持仓，单笔 1 手，单连接轮询阶段全账户至少间隔 1 秒；每条新单均等待上一条事实终态，完整柜台查询校验仓位，未开仓或改动期货。中间出现账户失效／单连接保护时停止新增下单并完整恢复，没有解除原生门控。执行中仅为官方 paper 使用临时合约白名单、独立审计与 RocksDB，常驻账户配置、SDK 和二进制未修改。

清仓收据、完整原生终态和剩余列表保存在本机 `data/ops/yida_close_options_20261114T010245Z`（Git 忽略），关键文件为 `option-close-result.json`、`option-close-final-state.json`、`remaining_options.csv`；远端当前结果为 `data/option-close-result.json`。

2026-11-14 净持仓展示调整：默认表只保留合约、中文交易所、净持仓（手）、来源状态、更新时间五列；正数净多、负数净空。多空、今昨仓、冻结和可平数量只在合约展开后显示。概览以净多／净空合约数统计，不跨不同合约相加净手数。现有浏览器契约检查、本地 release 构建和公网桌面／手机检查通过；仅更新并重启 Viz，交易进程 PID 与配置保持一致。收据及截图：`data/ops/cn_viz_net_20261114*/`（Git 忽略）。

2026-11-14 易达 L1 与人民币估值：配套更新 `yida-trader`、`cn-futures-viz` 和两份本地私有配置，内部 IPC 同步迁移到当前 4 MiB 帧。只读登录与 TCP L1 均就绪，原生目录／行情覆盖 23 个合约，实际 21 个持仓合约估值覆盖 **21/21**。交易规则白名单仍只有 2 个期货合约；原 758 条柜台委托及全部仓位在启用 L1 前后保持一致，没有新增报撤单，期权仍为此前未成交的 11 条／273 手。当前仿真 L1 原生交易日为 `20261113`，与接收自然日／时刻不一致的值按历史行情显示，不因刚收到回调而当作实时行情。

本地共享契约／IPC／Viz、Yida L1 解析与代次恢复、独立队列溢出隔离、配置白名单和前端金额／历史时刻检查通过；Yida release 构建及 CTP 静态编译通过，未执行 CTP。公网浏览器逐合约核对乘数、最新价、净手数与人民币金额，并验证桌面、展开明细和手机布局；保持 WebSocket 直推。收据及截图：`data/ops/yida_l1_valuation_20261114T031907Z`（Git 忽略），远端部署收据 `data/l1-valuation-deployment.json`。

2026-11-14 页面精简：金额行只显示数字，历史行情日期集中提示一次，计算口径与具体时刻移到悬浮／展开明细。概览保留合约数、期货金额、期权金额和行情覆盖四项，移除重复采集时间和本地预算；页面响应禁止缓存。本次只发布 Viz 二进制。验收期间原生通道断开并出现 SDK fatal，保存日志后通过环境脚本只读恢复一次，未报撤单。柜台重新返回 30 条持仓（期货 10 条不变、期权 20 条／462 手）、0 条原生委托，与断线前 21 条持仓／758 条委托不同，疑似仿真数据重置；页面展示本次柜台事实，历史成交审计仍保留。行情与金额覆盖 30/30，桌面和手机公网检查通过。保证金与杠杆尚未接入，原因及所需口径见 [Viz 持仓语义](cn_futures_viz.md#持仓与显示语义)。收据：`data/ops/cn_viz_compact_20261114T035915Z/`（Git 忽略）；远端 `data/cn-viz-compact-deployment.json`，事发日志位于本次 backup 的 `native-inconsistency/`。

## 2026-11-21 Exec 账本切换与清理

按用户要求清理并重新部署。删除旧 JSON 日志 `data/events.jsonl`、2026-11-14 期权平仓试验目录、
Viz 临时发布目录、旧 `data/viz-secondary`、历史部署／状态 JSON 和旧 `data/stops/` 归档；旧
`data/flow/persist_manager` 已于 11-20 删除。`yida.toml` 去掉 `journal`、`sync_before_send`、`idle_sleep_us`，
`[ctaker]` 改为 `[exec]`，新增 `persistence_directory` 与 100 ms `poll`；`yida_paper_env.py` 新增 `persist`
组件（见上表）。二进制：`yida-trader` 与本仓库同一提交构建，`persist_manager` 与创元相同。部署前 `sh_dev` 运行
`cn_futures` 全部 188 项库测试通过。

启动后 placement_audit 无入侵线程（RocksDB 后台线程在 CPU 3），柜台交易日 `20261120`，无未结委托，rb2701 无持仓；
账户其余 30 条历史仿真持仓不在 Manager 组合范围内，Exec 视作 `__unallocated__`。云端导出器改读新库并完成首次捕获，
Manager 以 rebase（revision 1，期初为空）在新库上重设锚点。

09:00:12 官方仿真断开，随即收到原生 fatal 回调（`kind=16`、`-9901`，与 11-14 00:00:41 相同），交易端按设计锁定并退出，
无柜台订单。09:03 通过环境脚本重启一次，登录、恢复、Manager 订阅均正常。当时桥接把 `ServerRestarted` 与回调异常
都报成同一个 fatal，进程空等 30 秒后以状态码 1 退出；随后改为独立事件加冷启动（见[易达冷启动](yida_trade.md#冷启动)）。

