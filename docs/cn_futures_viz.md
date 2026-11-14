# 国内期货 Viz

`cn-futures-viz` 是 SDK-free 的只读持仓转发器：**执行进程全量快照 → Iceoryx2 IPC → Viz → WebSocket → 浏览器**。收到 IPC 后立即推送，不再增加一秒合并定时器。Viz 不读数据库、不扫描委托／成交、不维护或派生订单状态机。交易进程原有 OMS 与持久化继续各自工作。CTP 与易达共用 `crates/cn_futures_state` 当前契约。

前端沿用当前 U 本位 Exec 的紧凑状态栏、指标带、聚合表与展开行，支持按合约／分方向持仓、交易所／中文名称／原始代码筛选；前端不展示账户名称、账户列或账户筛选，交易所以中文简称显示（中金所、上期所、上期能源、郑商所、大商所、广期所）。`sh_dev` 的易达 paper 已按用户要求公网监听 `0.0.0.0:8811`，无需 SSH 转发，见 [部署记录](sh_dev_yida_paper.md)。

## 启动与配置

```bash
cargo build --release -p viz_server --bin cn-futures-viz
cp config/cn_futures_viz.example.toml config/cn_futures_viz.toml
# 填写展示别名、IPC namespace 与账户摘要。
target/release/cn-futures-viz --config config/cn_futures_viz.toml --check-config
target/release/cn-futures-viz --config config/cn_futures_viz.toml
```

默认监听 `127.0.0.1:8811`。`--check-config` 只解析配置，不打开 IPC 或 SDK。用户指定 CTP 外部部署／执行主机为 `sh_dev`（`ssh sh_dev`）；本机和所有内网机器仅允许 CTP 编辑及编译，不运行 CTP SDK。本次部署是易达 paper，没有部署 CTP。

交易配置的根级：

```toml
position_ipc = { namespace = "cn_yida_sim", interval_ms = 1000 }
```

对应 Viz 配置：

```toml
stale_after_ms = 15000

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
```

namespace 必须匹配交易进程，每个账户独立；账户摘要与 adapter 共同校验来源，摘要不发送给浏览器。旧的数据库、行情源、轮询配置已删除，不保留兼容分支。独立行情发布器 `cn-futures-pbs` 仍照常维护，见 [发布器说明](cn_futures_pbs.md)。

## 持仓与显示语义

服务名 `<namespace>/viz_pubs/cn_futures_positions`，长度前缀 bincode，固定 1 MiB IPC 容量、历史 1 条、订阅缓冲 2 条、单发布者和安全覆盖。交易端默认每秒采集一次；Viz 每 10 ms 检查接收队列，源晚启动时每秒重试打开服务。

快照保留业务线程采集时间、柜台原生交易日、进程 epoch、序号、完整持仓恢复标记、原始连接／就绪／可交易标记、本地预算，以及按交易所、合约、多空方向隔离的今仓、昨仓、冻结今仓和冻结昨仓。Viz 不推断策略归属、目标仓位、账户权益或逐合约交易许可。

每帧整体替换。已恢复的空快照表示空仓；`position_ready=false` 表示未知，不能当成空仓。断线保留最后已知仓位，心跳超时明确显示过期。重启 epoch 切换后直接展示新帧，不拼接旧仓位。Viz 校验完整帧、账户绑定及同 epoch 序号，拒绝倒退帧。

数量直接使用柜台原生**手数**，以整数显示；不把合约手数当成标的数量。预算以**人民币元**显示，保留两位小数和千位分隔（例如 `¥ 1,234,567.89`），它是执行端本地预算，不是账户权益。持仓人民币金额须有估值价和可信合约乘数后按 `手数 × 合约乘数 × 估值价` 计算；当前 IPC 没有这两项，因此不虚构持仓金额。

合约示例：`SHFE/cu2612` → `沪铜 · 2026年12月`，旁边保留 `cu2612`。名称表按交易所区分，仅用于显示，不作为交易规则或乘数来源。郑商所 YMM 合约使用来源交易日的年份确定最近的十年周期，例如交易日 2026 年的 `SR701` → `白糖 · 2027年01月`；没有交易日时保留年月码，未知品种或非标准代码原样展示。期权同时解析看涨／看跌及行权价，例如 `sc2612C780` → `原油 · 2026年12月 · 看涨 · 行权价 780`，也支持中金所／大商所带分隔符的代码。中文名称和原始代码均可搜索。

## HTTP 与 WebSocket

| 路径 | 行为 |
| --- | --- |
| `/` | 国内期货持仓前端，直接打开配置的 WebSocket |
| `/healthz` | 运维健康摘要：来源新鲜度、原始就绪标记、持仓行数；HTTP 在线不代表账户就绪 |
| 配置 `ws_path`，默认 `/ws` | 连接即重放每个源最新帧，后续即时转发 |

没有 `/snapshot` 或交易／配置代理接口。前端不通过 HTTP 初始化或轮询数据，刷新操作只重连 WebSocket。慢客户端发送超时断开，队列溢出时重放所有源最新完整帧。

唯一 WS 帧类型 `cn_futures_state`：`source_id`、展示 `label`／`account`、`adapter`、`namespace`、`stale_after_ms`、`entry`、`error`。`entry` 直接来自公共 Snapshot：移除账户摘要，epoch／sequence／replaced_snapshots 编成十进制字符串以保留 JS 整数精度，其他业务字段原样保留。源不可用时 `entry=null` 或保留最后已知帧并附错误，不制造零仓位。原 crypto Viz 的 HTTP／WS 契约独立保留。

```bash
cargo fmt -p viz_server --check
# 仅 SDK-free 契约、IPC 与 Viz，不执行 CTP。
cargo test -p cn_futures_state -p viz_server --lib
cargo clippy -p cn_futures_state -p viz_server --all-targets --no-deps -- -D warnings
```

验证覆盖来源绑定、字段保真、全量替换、未知与空仓、IPC 直达、各源 WS 重放、250 ms 内转发、自定义路径和移除 HTTP 快照接口。浏览器验收检查真实公网 WebSocket、中文合约搜索、手数／元显示、展开与分方向、断线／重启、桌面及手机布局。
