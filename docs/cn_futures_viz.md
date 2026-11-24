# 国内期货 Viz

`cn-futures-viz` 通过 Iceoryx2 IPC 接收执行进程的全量快照，立即经 WebSocket 推送。账户持仓之外，Viz 可通过 Manager 读取经过授权的策略归因，通过既有行情隧道为 CTP 持仓补充 L1。Viz 无需 SDK、数据库或交易凭据。CTP 与易达共用 `crates/cn_futures_state` 契约。

合约行显示柜台净仓、策略净目标、人民币净金额、状态和时间，支持按中文名称或原始代码筛选。配置策略归因的账户默认展开合约明细。「1h 因子信号」默认收起，点击后可查看每个因子最近一次实时决策的目标手数，包括 0 手。「策略归因」并列显示 Manager 收到的信号目标和策略账本净仓。列表默认显示有柜台持仓的合约。勾选「显示零仓位合约」后，也可查看柜台零仓位记录，以及仍有非零策略目标或策略账本仓位的合约。多空和今昨仓通过「柜台明细」查看。来源、交易所和原生合约共同确定持仓身份。`sh_dev` 的 8811 Viz 部署见 [部署记录](sh_dev_yida_paper.md)。

## 启动与配置

```bash
cargo build --release -p viz_server --bin cn-futures-viz
cp config/cn_futures_viz.example.toml config/cn_futures_viz.toml
# 填写展示别名、IPC namespace 与账户摘要。
target/release/cn-futures-viz --config config/cn_futures_viz.toml --check-config
target/release/cn-futures-viz --config config/cn_futures_viz.toml
```

默认监听 `127.0.0.1:8811`。`--check-config` 只解析配置，不打开 IPC 或 SDK。用户指定 CTP 外部部署／执行主机为 `sh_dev`（`ssh sh_dev`）；本机和所有内网机器仅允许 CTP 编辑及编译，不运行 CTP SDK。**每个实盘／仿真账户环境运行自己的 Viz**，与该环境的 Exec、persist_manager 并列，配置中只能有一个 `[[counters]]`（多于一个时 `--check-config` 报错）。
这样一个账户的帧格式或二进制变更只需替换该环境自己的三个进程，不牵连其他账户。易达环境由 `scripts/yida_paper_env.py` 的 `viz` 组件管理；
CTP 环境由 `scripts/ctp_env.sh` 管理：`install` 同时安装 `<环境>-viz.service`，`start|stop [all|trader|viz]` 可单独启停 Viz，
Viz 读取环境内 `config/viz.toml`，不加载 `env.sh` 凭据。各环境的 Viz 使用不同端口；`/account-funds/<manager_source_id>` 的消费方需指向该账户自己的 Viz。

2026-11-24 之前，`sh_dev` 的 8811 Viz 同时配置易达 paper 和 [CTP 只读环境](sh_dev_ctp.md)。改为每个环境独立部署前，须先从 8811 的配置中移除 CTP 柜台，
并在创元 CTP 环境内配置并启动它自己的 Viz，新版 Viz 才能通过配置校验。

交易配置的根级：

```toml
position_ipc = { namespace = "cn_yida_sim", interval_ms = 1000 }
```

对应 Viz 配置：

```toml
stale_after_ms = 15000
# CTP 净金额可复用 sh_dev 现有回环行情与元数据隧道。
# l1 = { redis_port = 13005, metadata_port = 18312 }
# manager_port = 18211
# cta_signal_port = 19401

[http]
bind = "127.0.0.1"
port = 8811
ws_path = "/ws"

[[counters]]
id = "yida-sim"
label = "易达仿真柜台"
account = "simulation-observe" # 仅展示别名
adapter = "yida"              # 或 "ctp"
position_namespace = "cn_yida_sim"
expected_account_hash = "<64-character account hash>"
# manager_source_id = "<对应的 Manager 原生来源 ID>"
```

namespace 必须匹配交易进程，每个账户独立；账户摘要与 adapter 共同校验来源，摘要不发送给浏览器。旧的数据库、行情源、轮询配置已删除，不保留兼容分支。独立行情发布器 `cn-futures-pbs` 仍照常维护，见 [发布器说明](cn_futures_pbs.md)。

`cta_signal_port` 指向同主机回环上的 CTA 只读状态入口。Viz 在 Manager 校验会话后，按相同账户、策略组和原生合约查看因子信号。策略目标与归因仓位来自 Manager。CTA 状态不可用时仍可查看策略归因。

## 持仓与显示语义

服务名 `<namespace>/viz_pubs/cn_futures_positions`，长度前缀 bincode，固定 4 MiB IPC 容量、历史 1 条、订阅缓冲 2 条、单发布者和安全覆盖。交易端默认每秒采集一次；Viz 每 10 ms 检查接收队列，源晚启动时每秒重试打开服务。

快照保留业务线程采集时间、柜台原生交易日、进程 epoch、序号、完整持仓恢复标记、原始连接／就绪／可交易标记、本地预算，以及按交易所、合约、多空方向隔离的今仓、昨仓、冻结今仓和冻结昨仓。策略归因取 Exec 策略账本（经 Manager 读取）中的有符号手数，目标取当前有效目标。柜台净仓和策略合计存在差额时，界面显示差额。合约目标和策略归因数量缺失时显示不可用。

每帧整体替换。已恢复的空快照表示空仓；`position_ready=false` 表示未知，不能当成空仓。断线保留最后已知仓位，心跳超时明确显示过期。重启 epoch 切换后直接展示新帧，不拼接旧仓位。Viz 校验完整帧、账户绑定及同 epoch 序号，拒绝倒退帧。

数量直接使用柜台原生**手数**，以整数显示。金额以**人民币元**显示，保留两位小数和千位分隔（例如 `¥ 1,234,567.89`）。概览显示所选账户的合约数、持仓金额和行情覆盖。根据实际持仓的合约类别显示期货或期权金额；两类均有持仓时分别列出。空仓或类别未知时统一显示「持仓金额」。历史行情提示集中在表头，计算口径、行情日期和时刻及缺值原因放在悬浮说明或展开明细中。

持仓人民币金额按 `柜台净手数 × 合约乘数 × L1 估值价` 计算。易达沿用原生目录与 L1 最新价。CTP 从既有通联 Redis 行情取同交易日、同交易所、同合约的买一卖一中间价，单边或无有效盘口时使用同一快照的最新成交价。CTP 期货乘数来自行情 gRPC 的已验证目录。Viz 为 IPC 和经鉴权的 Manager HTTP 仓位使用同一个行情缓存。对应交易日尚无实时报价时，从同一行情服务获取最近已确认完成分钟里的最后一档有效双边盘口，标为历史行情并显示所属时段。收到同交易日的实时报价后转用实时链路。期货汇总净名义金额，期权汇总净权利金市值。确认空仓时显示零，估值资料不全时显示 `--`。停更时保留进程已收到的同交易日报价并标明时间。

CTP 资金字段见下文「账户资金」。易达页面没有保证金、保证金占用率或账户杠杆。本地预算不能充当这些数据。易达 `YDExtendedAccount` 提供相关字段，但 SDK 编程指南 §4.11 明确：保证金、持仓盈亏和期权市值由客户端计算，并非柜台主动推送的实时资金。本环境 `RecalcMode=off`，不能直接将其当实时权益并派生杠杆；后续接入需同时明确资金重算与行情时间。保证金占用率可按保证金／资金权益定义，期货杠杆可按逐合约名义金额绝对值之和／资金权益定义，期权权利金不能充当标的敞口。

易达配置 `observation_l1=true` 后，现有 SDK 登录会订阅持仓及原交易白名单的 L1。原生配置相应使用 `ConnectTCPMarketData=yes`，配置 placement 时必须把 `TCPMarketDataCPUID` 放在 SDK CPU 域。行情回调走独立、容量 1024 的有损队列，拥堵／无效价格只影响观察，不挤占可靠委托成交队列；不扩大交易白名单，不参与策略或订单风险定价。`RecalcMode=off` 和只读门控保持原义。

`Snapshot.marks` 携带每合约原生类别、乘数、Last／BBO、原生交易日、原生时间戳及 SDK 格式化时刻、本机接收时间、初始快照／暂停标记；`l1_connected` 和 `l1_dropped` 提供行情通道诊断。SDK 的 TimeStamp 从系统交易日起点累计，不是 Unix 时间；格式化必须使用 SDK 的 `timeStamp2String`，不能自行假设零点或替换成采集时刻。重启／对账按观察代次清除旧行情，初始指针快照始终保留初始标记，心跳不刷新行情时间。原生行情日早于本机中国日期，或 SDK 格式化行情时钟与接收时刻明显不符时，即使刚收到回调也标为历史；夜盘未来交易日不反向改写为自然日。快照、暂停、断线或停更价格可以显示带时间说明的**历史估值**；不冒充实时行情。

最大 8192 条持仓与 8192 条估值元数据在同一 4 MiB 完整帧中，冷初始化三个缓冲后采集不分配堆内存。CTP 账户发布者仍提供柜台资金和持仓。Viz 在转发时补充当前持仓合约的 L1，持仓 IPC 的字段与格式保持一致。

合约示例：`SHFE/cu2612` → `沪铜 · 2026年12月`，旁边保留 `cu2612`。名称表按交易所区分，仅用于显示，不作为交易规则或乘数来源。郑商所 YMM 合约使用来源交易日的年份确定最近的十年周期，例如交易日 2026 年的 `SR701` → `白糖 · 2027年01月`；没有交易日时保留年月码，未知品种或非标准代码原样展示。期权同时解析看涨／看跌及行权价，例如 `sc2612C780` → `原油 · 2026年12月 · 看涨 · 行权价 780`，也支持中金所／大商所带分隔符的代码。中文名称和原始代码均可搜索。

## 账户资金

当前快照的可选 `account` 包含柜台 CNY 权益、可用、可取、当前保证金、
冻结保证金／资金／手续费、持仓盈亏、平仓盈亏与手续费，以及独立查询时间。
2026-11-23 起同时携带资金行的原生交易日 `trading_day`、上日结算权益
`pre_balance` 以及当日入金 `deposit`／出金 `withdraw`，供 Manager 按交易日
（夜盘归下一交易日）计算昨日权益与当日盈亏。CTP 资金行交易日为空时使用登录交易日。
资金字段仅用于展示：构造或校验失败只记日志并发布 `None`，不中断交易会话。
该变更修改了 bincode 帧布局，Exec 与 Viz 必须同时替换；旧版本保存的
`account_snapshot_path` 检查点无法解码时只告警并等待新的实时快照。
CTP 按相关联的成功资金查询更新；易达未提供对应账户资金时为 `None`，
不将本地执行预算或持仓名义金额当作权益。

页面显示权益、可用、可取、占用保证金、持仓盈亏、平仓盈亏和当日手续费。缺失显示 `--`。查询超过 30 秒则保留上次结果并标记延迟。
数据源断开或进程异常退出时，保留上次查询结果并标记历史。面板用「柜台运行中断」标记进程异常退出，IPC 每秒心跳不能刷新资金查询时间。
核心因未确认结算阻止交易时，已恢复的只读资金／持仓仍可显示已同步。
账户源隔离，页面不添加主力查询组件。

### 逐笔浮盈

柜台「持仓盈亏」是 CTP `PositionProfit`，为盯市口径：昨仓按昨结算价、今仓按开仓价计算，
因此不等于各笔开仓以来累计浮盈之和。页面另显示「逐笔浮盈 · 累计」：

- 业务会话就绪后，CTP Exec 每 5 秒在其他后台查询之后执行一次持仓查询（`ReqQryInvestorPosition`），
  读取每行的 `OpenCost`、`PositionCost` 和 `PositionProfit`。各日期行按交易所、合约和方向汇总后，
  写入快照的 `position_costs`（含 `observed_us`）。该查询只用于展示，不参与对账，也不进入 Exec 账本。
  查询报错或某行无法解码时，只记日志并保留上次完整结果，不中断交易会话；断线时清空。
- Viz 计算公式：多头为 `L1 估值价 × 乘数 × 手数 − OpenCost`，空头取反；不含手续费。估值价优先用最新成交价，
  没有时（如休市期间 CTP 回退到已完成分钟的盘口）用买卖一中间价。
  若查询时的手数与当前柜台持仓不同（期间有成交），显示 `--`，直到下一次查询。
  合约行有「逐笔浮盈 · 元」列；「柜台明细」显示开仓均价和逐笔浮盈；
  资金栏的累计值为全部持仓合约之和，任一合约缺数据时显示 `--`。
- 易达不查柜台，桥接读取扩展 API 本地维护的 `YDExtendedPosition`：在 caught-up 快照和每次
  `notifyExtendedPosition` 时，发送 `POSITION_COST` 事件（kind 20）。事件携带按先开先平维护的
  `TotalOpenPrice`（开仓价之和）和 `PositionProfit`。编程指南 §3.2.3.8 说明日初持仓自带「日初开仓均价」，
  因此昨仓也按原始开仓价计算，而非昨结算价。Rust 用 `TotalOpenPrice × 乘数` 得到开仓成本，
  按日期行保存，汇总后发布。该事件不进入会话、核心或 trader.log。任一行无法解码时，
  不再发布开仓成本，直到下一个 SDK 会话；`PositionCost` 恒为 0。
- 该变更修改了 CTP 桥接事件布局（`RawEvent` 928→952 字节）、易达桥接事件布局（尾部增加两个 double）
  和持仓 IPC bincode 帧。每个账户环境须同时替换自己的 Exec、`persist_manager`
  （`cn_native_persist` 解码控制事件）与 Viz；不同账户环境互不影响。旧检查点无法解码时只告警，并等待新的实时快照。

2026-11-16 发布后的连接异常期间，柜台前置拒绝连接，CTP 面板因缺少新账户快照显示未同步和未知资金。账户恢复记录见 [sh_dev 创元 CTP](sh_dev_ctp.md)。

## HTTP 与 WebSocket

| 路径 | 行为 |
| --- | --- |
| `/` | 国内期货持仓前端，直接打开配置的 WebSocket；`Cache-Control: no-store` 避免继续使用旧页面 |
| `/healthz` | 运维健康摘要：来源新鲜度、原始就绪标记、持仓行数；HTTP 在线不代表账户就绪 |
| 配置 `ws_path`，默认 `/ws` | 连接即重放每个源最新帧，后续即时转发 |
| `/strategy-positions/<source>` | 对显式绑定的账户转发同主机 Manager 会话，按合约返回目标与归因净仓 |
| `/account-funds/<manager_source_id>` | 只读：按显式 `manager_source_id` 返回该柜台实时帧的 `account`、查询时间、交易日、运行与连接状态及错误；不返回冷启动检查点，未绑定来源返回 404 |

柜台资金和持仓由 WebSocket 初始化与更新。已配置归因的账户每五秒读取策略明细。Manager 负责验证登录、账户和策略查看权限，响应不缓存。未配置来源返回 404。慢 WebSocket 客户端发送超时断开，队列溢出时重放所有源最新完整帧。

唯一 WS 帧类型 `cn_futures_state`：`source_id`、展示 `label`／`account`、`adapter`、`namespace`、`stale_after_ms`、`entry`、`error`。`entry` 来自公共 Snapshot，CTP 行情可由 Viz 补充。账户摘要不发送，epoch／sequence／l1_dropped 编成十进制字符串以保留 JS 整数精度。源不可用时 `entry=null` 或保留最后已知帧并附错误。

交易端 `position_ipc.account_snapshot_path` 和 Viz 同账户的 `account_snapshot_path`
可指向同一个私有文件。退出时，交易后台线程把最后一份确认的账户观察写到同一
目录的临时文件，fsync 后原子换名，再 fsync 目录。未完成对账的启动和断线观察
保留已有文件。Viz 校验 namespace、adapter、账户摘要和时间，在 `saved_entry`
回放历史资金及原生多空今昨仓。页面优先使用当前 Manager HTTP 读数和已确认
的实时柜台观察，再按观察时间采用历史值。历史显示保留原始时间，健康状态仍取
实时数据。交易启动使用柜台正式对账。

```bash
cargo fmt -p viz_server --check
# 仅 SDK-free 契约、IPC 与 Viz，不执行 CTP。
cargo test -p cn_futures_state -p viz_server --lib
cargo clippy -p cn_futures_state -p viz_server --all-targets --no-deps -- -D warnings
```

验证覆盖来源绑定、字段保真、全量替换、未知与空仓、IPC 直达、各源 WS 重放、250 ms 内转发、退出快照和 HTTP 读数优先。浏览器验收检查真实公网 WebSocket、中文合约搜索、手数／元显示、展开与分方向、断线／重启、桌面及手机布局。
