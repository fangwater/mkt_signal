# sh_dev 易达 paper 与 Viz

最后更新：2026-11-14

用户指定的部署主机为 `ssh sh_dev`，运行目录 `/home/ubuntu/yida_paper01`。当前已部署并启动易达官方 SHFE／INE paper 账户观察和国内期货 Viz，使用 SDK `1.502.55.35`。CTP 仍未部署。

## 当前服务

| 服务 | 配置／入口 | 状态与范围 |
| --- | --- | --- |
| `yida_paper01-trader.service` | `config/yida.toml`、`config/server.cfg` | `public_simulation=true`、TCP；只带 `--connect`，不带 `--execute` |
| `yida_paper01-viz.service` | `config/viz.toml` | 按用户要求监听 `0.0.0.0:8811`，浏览器直接访问 VPS 公网地址的 8811 端口，无需 SSH 转发 |

服务由 Ubuntu 用户的 systemd 管理，已开启 user linger，退出 SSH 后继续运行。交易端不自动重启，避免登录／认证错误后重复提交；Viz 失败时自动重启。所有启停都通过环境内脚本，不直接操作监管器。

交易白名单为 `SHFE/cu2612`、`INE/sc2612`，账户实际持仓仍完整恢复和展示，包括白名单外既有持仓。已启用 `observation_l1=true`，在同一个 SDK 登录内订阅现有持仓及原白名单的 TCP L1；`TCPMarketDataCPUID=1`，`RecalcMode=off`。没有启动第二个行情登录或额外采集进程。行情观察不扩大可交易白名单；本地执行预算不作为账户权益。独立主行情仍由 `cn-futures-pbs` 接入。

## 操作入口

```bash
ssh sh_dev
cd /home/ubuntu/yida_paper01
./scripts/yida_paper_env.sh status
./scripts/yida_paper_env.sh state

# 停止前归档日志，通过私有 FIFO 让交易进程正常 shutdown；不撤销柜台订单。
./scripts/yida_paper_env.sh stop trader
./scripts/yida_paper_env.sh start trader

# 只更新或启停页面服务时，使用 viz 组件。
./scripts/yida_paper_env.sh stop viz
./scripts/yida_paper_env.sh start viz
```

环境目录为 0700，`env.sh` 和私有配置为 0600；凭据不写入仓库、命令参数、状态输出或 Viz 进程环境。操作脚本固定只读运行，不提供开启 `--execute` 的选项。交易 stdin 由权限 0600 的 `run/commands.fifo` 保持，避免无人值守时因 EOF 退出；`state` 响应保留在私有日志中。正常停止先发送 `shutdown`，等待 SDK 尾部回报和持久化完成。

应用亲和性：业务 CPU 0、SDK CPU 1、后台 CPU 3、NUMA 节点 0；Viz 在 CPU 3。主机暴露 4 个不同 core ID，CPU 2 没有分配给该环境。没有修改内核 CPU 隔离或 IRQ 策略，不宣称独占物理核或实测低延迟。

## 数据与验证

- 持仓服务：`yida_paper01/viz_pubs/cn_futures_positions`，默认每秒全量推送；Viz 的持仓不回退读库。
- 交易进程主库：`data/flow/persist_manager`。Viz 只订阅持仓 IPC，不再读库；旧 `data/viz-secondary` 仅为历史缓存。
- 日志：`logs/trader.log`、`logs/viz.log`、`data/events.jsonl`；启停归档位于 `data/stops/`。
- 配置和二进制在本地准备；上传至独立临时目录，逐文件 SHA-256 校验后原子安装。本机和远端的部署收据均不包含凭据。

2026-11-13 只读验证：官方 SDK 版本检查、登录及完整账户恢复通过，连接／传输／核心均就绪。柜台返回交易日 `20261112`，保留该原生值；30 条非零持仓持续推送，`can_trade=false`、投影错误 0、本次报撤单意图 0。正常停止并重启后，publisher epoch 已更新，持仓基线 SHA-256 一致，持久化读取正常。没有提交测试订单，因此本轮结论是 paper 登录、恢复、持仓及观察链路正常。

前端沿用当前 U 本位 Exec 的紧凑样式，默认按合约查看净持仓，展开后查看多空、今昨仓及冻结明细；不显示账户名称、账户列或账户筛选，交易所以中文简称显示。数量以原生手数显示，预算以人民币元显示；合约显示中文品种名与到期年月，期权另显示看涨／看跌与行权价；保留原始代码，支持中文搜索。人民币金额按用户确认的“净手数 × 原生合约乘数 × 易达 L1 最新价”计算，期货显示净名义金额、期权显示净权利金市值；不当作保证金、盈亏或期权标的敞口。缺价显示未知；历史行情明确标记并展示原生日期／时刻。执行进程 → IPC → Viz → WebSocket 直接推送，页面没有 HTTP 数据初始化、读库、委托／成交页或订单状态机。公网 HTTP 与 WebSocket 使用同一端口，`status` 读取 `/healthz` 的来源健康摘要。

本地私有收据目录：`data/ops/sh_dev_yida_paper01_20261113T144307Z/`（Git 忽略）；远端账户恢复／重启收据：`data/deploy-verification.json`。

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
