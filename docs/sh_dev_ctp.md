# sh_dev 创元 CTP

最后更新：2026-11-19

部署主机为用户指定的腾讯云 CVM `ssh sh_dev`，独立环境为
`/home/ubuntu/ctp_chuangyuan01`。使用上期技术官方 CTP 6.7.11，
`ctp-trader` 在同一个进程内接入 TraderApi 和 MdApi。

创元实盘以 `--connect --execute` 启动。环境配置中的 `[ctaker]` 仅允许
`SHFE:rb2701`，执行端订阅 Manager gRPC。`confirm_settlement = true` 使 Trader
在登录后查询柜台确认状态，并在当日尚未确认时调用 CTP SDK 的结算确认接口。
移除 `[ctaker]` 并关闭结算确认后，环境脚本以 `--read-only` 启动账户查询。

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
user linger。组合进程在前置离线时等待 SDK 重连。资金查询超时或连续三次重试仍未
就绪时，进程排空审计后以状态码 75 结束，systemd 在 60 秒后冷启动。其他退出
保持 `Restart=no`。环境脚本根据 `[ctaker]` 选择运行模式，实盘配置要求明确的
合约范围和 Manager gRPC 订阅。

## 文件与资源

- 环境目录权限 0700；`env.sh` 和两份私有 TOML 权限 0600。
- `config/trade.toml` / `config/md.toml` 保存环境专属配置；凭据从 `env.sh` 读取。
- 官方库位于 `sdk/6.7.11/`；主程序为 `bin/ctp-trader` 和独立诊断工具 `bin/ctp-md`。
- Trader/Md 分别使用 `data/trade-flow` / `data/md-flow`，主库为 `data/persist_manager`。
- 交易审计保存在 RocksDB `cn_trading_events`。停机前仅将 `logs/trader.log` 归档到 `data/stops/`。2026-11-18 核验新版只读对账后，清理旧 `data/events.jsonl` 和 12 份停机副本，释放约 34.5 GB。RocksDB 使用原库。
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
