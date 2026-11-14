# sh_dev 易达 paper 与 Viz

最后更新：2026-11-14

用户指定的部署主机为 `ssh sh_dev`，运行目录 `/home/ubuntu/yida_paper01`。当前已部署并启动易达官方 SHFE／INE paper 账户观察和国内期货 Viz，使用 SDK `1.502.55.35`。CTP 仍未部署。

## 当前服务

| 服务 | 配置／入口 | 状态与范围 |
| --- | --- | --- |
| `yida_paper01-trader.service` | `config/yida.toml`、`config/server.cfg` | `public_simulation=true`、TCP；只带 `--connect`，不带 `--execute` |
| `yida_paper01-viz.service` | `config/viz.toml` | 按用户要求监听 `0.0.0.0:8811`，浏览器直接访问 VPS 公网地址的 8811 端口，无需 SSH 转发 |

服务由 Ubuntu 用户的 systemd 管理，已开启 user linger，退出 SSH 后继续运行。交易端不自动重启，避免登录／认证错误后重复提交；Viz 失败时自动重启。所有启停都通过环境内脚本，不直接操作监管器。

交易白名单为 `SHFE/cu2612`、`INE/sc2612`，账户实际持仓仍完整恢复和展示，包括白名单外既有持仓。原生行情连接关闭，`RecalcMode=off`；本环境没有启动易达行情备份或其他行情发布进程。本地执行预算不作为账户权益。后续行情继续通过独立 `cn-futures-pbs` 接入。

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

前端沿用当前 U 本位 Exec 的紧凑样式，按合约聚合／分方向查看持仓；不显示账户名称、账户列或账户筛选，交易所以中文简称显示。数量以原生手数显示，预算以人民币元显示；合约显示中文品种名与到期年月，期权另显示看涨／看跌与行权价；保留原始代码，支持中文搜索。持仓金额因缺少估值价与乘数暂不计算。执行进程 → IPC → Viz → WebSocket 直接推送，页面没有 HTTP 数据初始化、读库、委托／成交页或订单状态机。公网 HTTP 与 WebSocket 使用同一端口，`status` 读取 `/healthz` 的来源健康摘要。

本地私有收据目录：`data/ops/sh_dev_yida_paper01_20261113T144307Z/`（Git 忽略）；远端账户恢复／重启收据：`data/deploy-verification.json`。

2026-11-13 直推改造验收：本地 6 项状态契约测试、10 项 Viz 测试、Clippy 及浏览器契约检查通过；公网浏览器收到连续 WebSocket 心跳，30 个合约／30 条持仓、多头 506 手、空头 387 手，页面没有 `/snapshot` 或 `/healthz` 数据请求。仅重启 Viz 的发布前后交易进程 PID 相同，持仓摘要一致。桌面与手机截图及收据在上述私有目录的 `direct-ws-*` 文件中。

部署检查另发现 paper 曾于主机本地时间 2026-11-14 00:00:41 因 `native SDK fatal/missing-order event; stop and recover` 退出。已保留日志，通过环境脚本只读恢复一次，完整持仓与连接恢复；未改为自动重启，也未提交报单或撤单。

2026-11-14 前端调整：移除账户名称、账户列与账户筛选，聚合行／展开行／分方向行及筛选器统一显示中文交易所。现有浏览器契约检查、本地 release 构建及公网桌面／手机检查通过。本次检查时 paper 已异常退出，归档日志后按原有只读配置恢复一次；30 条持仓恢复，原生交易日为 `20261113`、`can_trade=false`。随后仅替换并重启 Viz，交易进程 PID 和 Viz 配置保持一致。收据及截图：`data/ops/cn_viz_labels_20261114*/`（Git 忽略）。

2026-11-14 用户要求清掉全部期权：原有 20 条期权持仓合计 462 手。通过当前已校验 release 二进制的原生市价平昨／FAK 路径，柜台事实成交累计平掉 **189 手**，仍剩 **273 手、11 条期权持仓**，未宣称全部完成。剩余合约连续三轮完整 FAK 轮询均为零成交撤销；额外一笔 `lu2701C3550` 的市价 Day 卖平等待 90 秒仍未成交，已撤回并重新查询确认终态。最终未完成委托 0、冻结 0，原有 10 条期货持仓数量完全一致，常驻服务已恢复原有只读配置。

本轮临时执行使用原生期权元数据与分日期持仓，单笔 1 手，单连接轮询阶段全账户至少间隔 1 秒；每条新单均等待上一条事实终态，完整柜台查询校验仓位，未开仓或改动期货。中间出现账户失效／单连接保护时停止新增下单并完整恢复，没有解除原生门控。执行中仅为官方 paper 使用临时合约白名单、独立审计与 RocksDB，常驻账户配置、SDK 和二进制未修改。

清仓收据、完整原生终态和剩余列表保存在本机 `data/ops/yida_close_options_20261114T010245Z`（Git 忽略），关键文件为 `option-close-result.json`、`option-close-final-state.json`、`remaining_options.csv`；远端当前结果为 `data/option-close-result.json`。
