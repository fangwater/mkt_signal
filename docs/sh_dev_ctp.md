# sh_dev 创元 CTP

最后更新：2026-11-23

部署主机为用户指定的腾讯云 CVM `ssh sh_dev`，独立环境为
`/home/ubuntu/ctp_chuangyuan01`。使用上期技术官方 CTP 6.7.11，
`ctp-trader` 在同一个进程内接入 TraderApi 和 MdApi。

创元实盘以 `--connect --execute` 启动。环境配置中的 `[exec]` 仅允许
`SHFE:rb2701`，执行端订阅 Manager gRPC。`confirm_settlement = true` 使 Trader
在登录后查询柜台确认状态，并在当日尚未确认时调用 CTP SDK 的结算确认接口。
移除 `[exec]` 并关闭结算确认后，环境脚本以 `--read-only` 启动账户查询。

## 合约范围

`config/md.toml` 使用 `shfe_dominant_url = "http://127.0.0.1:18313/api/v1/dominant"`。
`ctp-trader` 在每次 MdApi 登录成功后，从登录响应取原生交易日，
通过已有反向隧道发送 `GET /api/v1/dominant?trading_day=YYYYMMDD`。
Rust 后台线程解析响应，验证生效日、来源和合约后，将名单交给行情线程订阅。
只使用上期所 `xsge` 的 `dominant` 字段，能源中心 `xsie` 独立。
实际订阅名单、生效日和来源见日志 `md_dominant_selected`。

服务返回 `scheduled_next` 且未核对交易日历时，仅接受最新数据后的第一个工作日，
跳过普通周末。节假日跨多个工作日需服务器确认生效日或发布当日数据。
接口超时、空名单或名单校验失败期间继续观察账户，并在后台限频重试。
重连用新登录响应重新请求名单，前一次会话的响应不能安装到新会话。
只读运行中的 Md 故障会停止行情消费并保留账户观察；SDK 对象仍仅在冷退出时
释放。开启执行时的 Md 错误继续使整个运行时停止。

行情主力订阅可独立用于观察。新版 `trade.toml` 删除旧的六个测试合约，
执行端使用 Manager 目标中的具体合约，需要执行时自动查柜台规则和补订行情。
账户委托、成交和持仓查询覆盖全账户。

2026-11-19 已通过完整空仓对账，将 Manager 的 `cn-futures-01-rb` 七个
小时因子绑定到 `SHFE:rb2701`。信号实例按组发布目标，Manager 按账户汇总并
经 gRPC 交付目标向量。创元已部署按 Manager 净目标和交易所下单规格确定委托
手数的版本。北京时间 10:04 核验 SDK 已确认结算，柜台完成 0 持仓、0 委托对账。
Manager 的螺纹净目标为 0 手，执行订阅已连接。

## Exec 策略账本与独立持久化（待部署）

`exec-strategy-ledger` 分支把策略归因从 Manager 移回 Exec，持久化拆成独立
`persist_manager` 进程，与 crypto 一致（见[持久化说明](cn_order_persistence.md)
和 [Strategy ledger](native_exec.md#strategy-ledger)）。上线需同时满足：

- 环境内新增 `persist_manager` 服务，与 `ctp-trader` 同目录运行，写 `data/persist_manager`；
  两者设置相同的 `IPC_NAMESPACE`。`persist_manager` 先于 `ctp-trader` 启动。
- Manager 与信号发布端同版本切换：Manager 计划改为各策略完整目标，信号批次不再带 BBO。
- 切换时删除旧库（`data/persist_manager`）与 Manager 的旧同步副本，全部按新格式部署；新库只有
  `cn_uniform_orders` / `cn_trade_updates` 两张二进制表。Exec 账本为空，账户持仓整体是 `__unallocated__`。
- 创元账户的旧持仓归属已知策略：`ctp-trader` 对账完成后、Manager 开始下发目标前，用
  `assign_position` 命令按策略和成本价逐合约记入（见 [Strategy ledger](native_exec.md#strategy-ledger)），
  命令拒绝超出未归属余量的手数。其余未指定的持仓留在 `__unallocated__`，由内部对冲按 mid 转给目标策略；
  没有策略目标的合约会被 Exec 平掉。
- trader 日志（订单、成交、exec 决策、账本事件）改由 `persist_manager` 输出，环境脚本须一起拉起并收集它的日志；`ctp-trader` 进程内只剩业务线程与 SDK 线程。
- 交易 TOML 的 `idle_sleep_us` 改为 `poll`：独占隔离核用 `{ mode = "busy" }`，共享主机用 `{ mode = "sleep", idle_us = 64000 }`。
- 部署前在 `sh_dev` 运行 `cn_futures` 的 exec / algo / persist / runtime 测试；开发机只编译。

2026-11-20 夜盘完成切换（北京时间约 23:00）。切换前创元交易进程已在 17:24 收盘断线时退出，无未结委托。
sh_dev 上先运行 `cn_futures` 全部 188 项库测试并通过，再安装同一提交 `d978b86e` 的 `ctp-trader`、
`persist_manager` 与新环境脚本：`ctp_env.sh install` 新增 `ctp_chuangyuan01-persist.service`（CPU 3，
stdout 即 trader 日志），`start` 先起持久化再起交易，`IPC_NAMESPACE=ctp_chuangyuan01`；交易配置
`[ctaker]` 改为 `[exec]`，`poll = { mode = "sleep", idle_us = 1000 }`。旧库移至
`data/pre-exec-ledger-20261120T143221Z/`。对账为交易日 `20261123`、rb2701 空 1 手昨仓，随后
`assign_position` 以原成本 3007 记入 `cn-futures-01-rb-1h`。Manager 以 rebase 在新流水上重设锚点
（revision 34，分钟收盘 2997 估值）。云端导出器同步更新并重置导出状态，8811 Viz 同步更新。

2026-11-21 两项调整：启动恢复只读打开 RocksDB 时会留下 flush/compaction 线程，原先在业务线程上创建，
`placement_audit` 列出 `rocksdb:low` / `rocksdb:high` 两个入侵线程；现在恢复在后台 CPU 域内执行，线程落在 CPU 3，
审计为空。云主机部署把 `poll` 改为 `{ mode = "sleep", idle_us = 100000 }`（100 ms，上限）；同日 sleep 改为挂起在 SDK 回调队列上、事件到达即唤醒，行情与回报不再等 100 ms。周六 08:52 换装重启，
前置未开放时进程等待重连。

## 目标常驻执行（2026-11-23）

11-23 上午创元 TA 等合约有目标却未执行：10:00 一批目标下 AP、SA 成交后，SM 第三笔触发本地
`max_actions_per_second = 2` 被拒，Exec 全局 halt；10:15 的新目标落在盘中休息，60 秒 lease
在 10:30 开盘前过期。用户要求任何时候都直接执行推送仓位，午休（北京时间约 12:40）部署
arbmm `551831fc` 与 Manager `4658e51`：

- 目标无 lease、无交易日，直到新批次替换；Exec 重启后重新接收当前目标。无本地限速。
- 拒单或被扣住的单只暂停该合约，按 `retry_interval_ms` 重试；行情重连保留目标。
- 无合约白名单；准入不等费率查询和行情订阅回执，缺规则的合约单独等待并每秒补请求。
- 报单/撤单 SDK 结果不确定时只将该单标为未确认，3 秒后后台 `ReqQryOrder` 结算。
- `trade.toml` 删除 `max_actions_per_second`、`target_max_age_ms`、`contracts`；环境脚本不再要求合约范围。
  ctp-trader sha256 `98a532a5…`，persist_manager `00d7e73e…`；旧二进制、配置和脚本备份在
  `data/ops/no-lease-20261123T043717Z`。sh_dev 上 cn_futures 193 项测试通过。
- 易达 paper 随后同版本换装，见 [易达记录](sh_dev_yida_paper.md)。
- 13:30 开盘 9 笔中 8 笔一秒内成交；lc2701 买开 1 手被广期所以 40024（自 2026-09-22 LC2610–LC2709
  每次最小开仓 2 手，CTP 合约最小下单量仍报 1）拒绝，并按 1 秒重试被拒 141 次，13:33 停机。
  修复 arbmm `c6b776af`：交易所／柜台拒单后只退避该合约（从 `retry_interval_ms` 翻倍至 5 分钟，
  成交或该合约净目标变化时清零，同净目标的新批次不再重置）。CTA 的 `lots_per_rule` 改为 v1 每方向手数
  （`a97d7c5e`），可在配置中心 JSON／表单修改，保存后若目标变化立即推送；14:13 经配置中心把 LC 设为 2，
  Manager 确认 rev 84，lc2701 一笔买开 2 手成交。13:49 换装 ctp-trader `dbadc078…`（备份 `data/ops/backoff-20261123T054938Z/`），
  重启后 lc 拒单间隔按 1、2、4… 秒退避，其余合约均到达目标。

## 操作入口

```bash
ssh sh_dev
cd /home/ubuntu/ctp_chuangyuan01
./scripts/ctp_env.sh status
./scripts/ctp_env.sh state
./scripts/ctp_env.sh stop
./scripts/ctp_env.sh start
```

所有启停通过环境内 Bash 脚本。`stop` 先归档日志，systemd 再发送 `SIGTERM`，
Rust 运行时排空 SDK 回报并关闭持久化写入。
`state` 只排队读取状态，响应留在私有日志。`status` 显示进程状态，
不将进程存活等同于柜台就绪。

Ubuntu 用户 systemd 服务为 `ctp_chuangyuan01-trader.service`，已启用开机启动和
user linger。组合进程在前置离线时等待 SDK 重连。资金查询超时、连续三次重试仍未
就绪，柜台连接或控制请求响应超时，或行情登录成功但前置尚未返回有效交易日时，进程排空审计后以状态码 75
结束，systemd 按 drop-in `10-cold-restart.conf` 在 60 秒后冷启动（与易达 paper 相同）。等待行情交易日期间，Viz 显示「等待行情交易日」。其他退出
保持 `Restart=no`。环境脚本根据 `[exec]` 选择运行模式，实盘配置要求明确的
合约范围和 Manager gRPC 订阅。

## 文件与资源

- 环境目录权限 0700；`env.sh` 和两份私有 TOML 权限 0600。
- `config/trade.toml` / `config/md.toml` 保存环境专属配置；凭据从 `env.sh` 读取。
- 官方库位于 `sdk/6.7.11/`；主程序为 `bin/ctp-trader` 和独立诊断工具 `bin/ctp-md`。
- Trader/Md 分别使用 `data/trade-flow` / `data/md-flow`，主库为 `data/persist_manager`。
- 交易记录保存在 RocksDB `cn_uniform_orders` / `cn_trade_updates`（只有这两张二进制表）；原始回报只在 `logs/trader.log`。停机前仅将 `logs/trader.log` 归档到 `data/stops/`。2026-11-18 核验新版只读对账后，清理旧 `data/events.jsonl` 和 12 份停机副本，释放约 34.5 GB。RocksDB 使用原库。
- 账户资金和持仓观察使用独立 IPC namespace `ctp_chuangyuan01`，接入现有 `yida_paper01` 的 8811 Viz；CTP／易达独立切换，不合并资金或持仓。

按该主机实际拓扑配置 NUMA 0、业务 CPU 0、Trader SDK CPU 1、
Md SDK CPU 2、后台 CPU 3。主机暴露四个不同 core ID，没有 SMT 兄弟。
现有易达 Viz 仍在 CPU 3；没有空闲的独立 housekeeping 核，也未修改
IRQ 或内核隔离设置，因此这次是只读接入部署，不宣称独占核或低延迟验收。
部署前易达 trader 已失败；本次更新其匹配当前 IPC 的二进制但不启动它，
通过环境脚本更新并重启现有 Viz。

## 发布与验证

程序在本地维护的 `arbmm` 工作树构建，上传临时文件后逐文件验证
SHA-256 并原子安装；远端没有编译。CTP 程序、SDK 版本检查、探针和
回归测试均只在 `sh_dev` 执行，本机只做静态检查和编译。

首次登录暴露两个问题，原始证据均已保留：

1. 初始全量合约目录约 2.94 万条回报，原 8192 队列不足。新环境的
   Trader/Md `event_capacity` 设为 131072，继续保留有界队列、完整审计和溢出停止行为。
2. 创元 Md 登录成功回调使用 `request_id=0`。仅在当前连接有一个待决
   登录时接受该响应，继续核对错误、响应存在性、交易日和完整订阅；
   不匹配的非零 ID、重复或未请求的响应仍被忽略。

`sh_dev` 上的 5 项行情状态测试和 2 项配置测试通过。官方 SDK 版本
检查通过；固定 release 的独立行情验证完成 6 个订阅并收到 6 条行情回调。
交易端认证、登录、合约、手续费、保证金、资金及两轮完整账户对账通过，
原生交易日为 `20261116`，当时持仓为空。行情原生时间保留，收到回调
不等于该时刻处于连续交易或价格刚发生更新。

私有本地收据：`data/ops/ctp_sh_dev_20261116T035821Z/`（Git 忽略）。
远端发布清单为 `data/deployment-manifest.json`；初次目录故障的配置
保存在 `data/initial-catalog-failure/`，行情修复前二进制保存在
`data/before-md-login-fix/`，原始日志在 `data/stops/`。

上期所扩展的私有收据为 `data/ops/ctp_shfe_20261116T045446Z/`。
远端 `data/shfe-subscription-catalog.json` 保存筛选依据和完整合约清单，
`data/shfe-md-validation.json` 保存独立行情验收，
`data/shfe-deployment-verification.json` 保存组合进程恢复及账户前后核对结果。
发布前归档日志，通过环境脚本停止原进程；新配置完整验证后再启动组合进程。
独立行情验收确认上期所 **238/238** 订阅成功，收到 238 个合约的当日
午休前快照，原生时间为 `11:30:00`；不将其称为盘中连续更新验收。
组合服务恢复行情就绪和完整账户对账，持仓、委托均为 0，与发布前一致。

## 冷启动文件描述符

上期所扩展时，已有审计库积累了超过 1100 个 SST 文件。原 RocksDB
无限表文件缓存会在冷启动时打开大量文件，随后原生 SDK 无法正常建连。
SSH 临时诊断的 `nofile` 软限制为 1024，直接报 `Too many open files`；
常驻 systemd 服务的软、硬限制均为 65536，系统文件句柄总量没有耗尽。

在 `sh_dev` 用相同 `nofile=65536` 做 MdApi 对照：低编号描述符下
243 个订阅正常；预先继承 1100 个描述符后无法建连。两份官方 SDK
均使用 `select`，该结果与其高编号描述符处理限制一致。没有修改
系统 `file-max`、Unix socket 队列或 systemd 上限。

`NativeStore` 现通过 `RocksDbTuning.max_open_files=128` 限制表文件缓存，
保留审计、订单、成交和同步 WAL。其他持久化调用者默认行为不变。
用包含 1204 个 SST 文件的原账户库冷启动，实际打开 48 个描述符、
最高编号 47，组合进程行情订阅恢复。没有删库或清除事实记录；
诊断时备份的 SDK flow 已恢复。

早期构造失败还遗留了本环境的孤立 IPC 静态描述符；核对无对应动态资源
及存活 CTP 进程后，仅归档并移除该描述符，未清理易达 Viz 的 IPC。
对照测试和冷启动记录分别位于远端 `data/md-fd-budget-diagnostic.json`
和 `data/fd-fix-preflight.json`。

最终常驻服务启动并完成账户对账后，打开 75 个描述符、最高编号 74，
`nofile` 仍为 65536。`sh_dev` 上 150 项 `cn_futures` 测试与 15 项
嵌入式持久化测试通过，2 项原有测试保持忽略；本地 runtime 持久化
调用方静态编译通过。本次没有提交订单或确认结算。

## 账户资金与五档快照

CTP 资金来自柜台 CNY 资金查询，展示权益、可用、可取、保证金、冻结、
持仓盈亏、平仓盈亏与手续费，持仓保留全账户事实。
资金查询有独立时间戳；超过 30 秒保留最后事实并标记延迟，
IPC 心跳不使旧余额变新。结算未确认不会使健康的只读查询失效。
明确的资金查询临时错误 90（“查询未就绪，请稍后重试”）在对应请求尚无资金行时
延时至少 2 秒重试，最多 3 次。首次命中后暂停报单，查询恢复后完成完整账户对账
和持久化，再进入交易就绪。离线和冷启动等待在 Viz 单独显示。认证拒绝、订单错误、
回报丢失或审计故障依旧进入原有异常路径。资金时间戳取最后成功查询。

本账户已收到具有买卖五档的 MdApi 深度快照；既有 238 个 SHFE 样本中，
各档存在正价格的合约数依次为 235／229／212／203／187。
这证明实际接入有五档快照，不代表完整逐笔委托、逐笔成交或全深度 L2。
主力查询供每次行情登录选约。

## 当前发布状态

2026-11-18 已在 `sh_dev` 发布按柜台交易日直连 HTTP 的版本（功能提交
`2eda27d7`）。`ctp-trader` 以只读模式登录，使用柜台的 `20261118` 请求
主力名单。本次收到 19 个上期所合约的订阅确认，实际名单与 HTTP 响应一致，
Trader 在同一交易日完成账户对账。

环境服务改用 Bash 启动和 systemd 停机，旧 Python 启动入口及生成的名单
文件移入发布备份。171 项原生回归在 `sh_dev` 通过，2 项忽略。运行验收、
二进制校验和及维护前备份在远端 `data/ops/dominant-http-20261118/`。
二进制由共享工作区构建，保留了已有的文件描述符上限修复。

2026-11-16 账户 IPC、资金面板和首次主力接入的历史记录保存在本地
`data/ops/ctp_dominant_viz_20261116T082201Z/`（Git 忽略）。
