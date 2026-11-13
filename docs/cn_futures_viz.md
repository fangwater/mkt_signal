# 国内期货 Viz

`cn-futures-viz` 复用现有 crypto `viz_server` 的 Axum、HTTP 快照、WebSocket 缓存重放、每秒合并发送与慢客户端超时。独立入口与页面位于同一 crate，原 crypto 入口保持现有配置。国内端共用一套观察逻辑：`adapter = "ctp"` / `"yida"` 选择并校验执行数据库身份，行情统一读取 `cn_futures_pbs::msg::MarketDataMsg`。

## 启动

```bash
cargo build --release -p viz_server --bin cn-futures-viz
cp config/cn_futures_viz.example.toml config/cn_futures_viz.toml
# 修改柜台数据库与独立 secondary 路径；只看行情可以不配置 counters。
target/release/cn-futures-viz --config config/cn_futures_viz.toml --check-config
target/release/cn-futures-viz --config config/cn_futures_viz.toml
```

浏览 `http://127.0.0.1:8811/`。默认只监听本地；远程观察可经现有受控反向代理转发 HTTP 与 WebSocket。服务本身不提供登录，需要暴露到其他机器时由部署环境提供访问控制。

`--check-config` 只解析配置和校验路径布局，不打开 RocksDB、IPC 或 SDK。二进制不依赖 `cn_futures` 交易 crate，不链接或加载 CTP / 易达 SDK，也不使用交易账户凭据。可以在当前开发机运行 SDK-free Viz、原生数据库与行情 IPC 的测试；CTP 客户端运行仍须用户指定并授权的外部主机。本页不构成任何 CTP 部署或执行授权。

## 数据来源

每个 `[[counters]]` 指向执行进程已生成的归一化 RocksDB。使用 `persist_common::cn_futures` 的现有订单与事实成交类型，以及 `persist_manager::cn_futures` 的列族、证据类型；不创建另一套执行记录格式。独立读线程打开 RocksDB secondary，只写配置中的 secondary 目录。主库及任意 secondary 路径不得重叠，不能复用 Manager 正在使用的 secondary。

每轮只 catch up 一次，读取期间不推进句柄；RocksDB secondary 不支持 `snapshot()`，因此不调用它。初次扫描所有证据，之后按序号增量处理；主库序号变化时扫描订单／成交投影，内存仅保留 `row_limit` 行。未决委托优先显示，已隐藏行数独立返回，汇总数量覆盖全部存储记录。数据库或投影读取失败显示 `unavailable`，不把旧读取结果继续当作新快照。读线程超过过期阈值未更新时独立返回 `observer_stale=true`。

每个 `[[market_data]]` 订阅 `<service_root>/<venue>/l2`，仅打开现有服务，不创建行情发布者。发布者晚启动时每秒重试。公司内网示例读取独立 `cn-futures-pbs --zmq` 进程产出的六交易所行情，**这是公司内网接入方式**，配置见 [行情发布器](cn_futures_pbs.md)。不在 Viz 内直连通联、CTP 或易达行情。Colo／公网部署用各自的 service_root 和 environment 标识，继续使用同一消息处理逻辑；这不代表上游 Colo／公网适配器已经实现。

行情按来源、交易所、原生大小写合约隔离。进程 epoch／源 generation 改变、重置标记或 IPC 序号缺口都会清除该交易所旧盘口；收到新盘口不代表历史成交或因子连续性恢复。缺失浮点和数量输出 null，显式零数量保留，所有行情 u64 序号与数量用十进制字符串输出，避免浏览器精度损失。`symbols` 是精确合约过滤，空数组表示全部；超过 `max_symbols` 不加入新合约，累计丢弃消息数可见。

## 状态与口径

- 委托保留交易日、交易所、完整原生会话与引用、系统号；这些原生 ID 始终为字符串。已发送撤单显示 `cancel_pending`，收到撤单终态后才显示已撤。终态但事实成交数与回报累计数不同显示 `settling`。发送结果未明始终显示 `uncertain`。
- `filled` 是事实成交，`reported_filled` 是订单回报累计量；不相互代替。手续费未知输出 null，易达已报 CNY 费用与真实零值原样保留；CTP 未报费用不估算。SDK `native_time` 原样展示，不当作 Unix 时间。
- 持仓按交易所／合约／方向展示今昨仓和冻结今昨仓。持仓只来自最后一份 `state` / `reconciled` 快照，之后若有事实成交返回 `positions_may_lag`。Viz 不靠成交自行重建持仓。
- 现有交易进程在恢复完成或收到 `state` 请求时记录完整状态，**目前没有周期性状态心跳**。Viz 不发送 `state` 命令；仅收到订单／成交不刷新持仓快照时间。页面分别标注最新事件与快照时间；运行中长期没有新快照会显示 stale。这一版没有改交易业务循环。
- 恢复成功仅证明那一刻完成恢复。后续 CTP／易达断线、恢复中、交易阻断、传输失败、重启与停止证据覆盖旧状态。传输恢复回报本身不清除之前的执行阻断，仍等待新的执行状态／恢复快照。旧进程快照可以留作历史展示，但 `snapshot_current_runtime=false`。投影冲突计数保留，仅在后续完整恢复证据后解除观察侧的错误阻断。
- `reported_can_trade` 是执行进程显式报告值；`reconciled` 不会生成 true，也不替代行情就绪、权限或传输门控。`observing` 表示近期有可观察状态，不是交易授权。`/healthz` 仅表示 HTTP 服务在线。
- `available_local_budget` 单独标成本地执行预算，不能当账户权益／保证金。CTP `state` 证据若包含 account，则只输出财务字段及记录时间；这不是新一次柜台资金查询。易达当前未归一化提供的账户权益显示未知。历史本地来源与当前进程 owned 字段均不能证明策略归属。

## HTTP 与 WebSocket

| 路径 | 行为 |
| --- | --- |
| `/` | 中文观察页；柜台、交易所、合约与行情来源筛选，点击查看完整身份和五档 |
| `/healthz` | HTTP 进程健康 |
| `/snapshot` | 与 crypto 相同的 `{ts_ms, entries}` 缓存结构 |
| 配置 `ws_path`，默认 `/ws` | 连接先重放最新消息，此后每秒合并发送 |

当前消息类型为 `cn_execution`（含各柜台 sources、生命周期、订单和成交）与 `cn_market`（各行情来源的 channels、quotes）。两类均含微秒 `ts_us`。执行消息给出 `ws_path` 与 `update_interval_ms`，页面据此连接并检测观察链路停更。只提供 GET 观察接口，不代理配置或交易命令。

页面的持仓与行情过期按各自事实时间判断。停止的柜台显示 stopped；其历史记录仍可查看。实盘与仿真必须使用不同源 ID、数据库、secondary 与行情 service_root；服务不会在柜台间自动切换或汇总账户风险。

## 验证

```bash
cargo fmt -p viz_server --check
cargo test -p viz_server --lib
cargo clippy -p viz_server --all-targets --no-deps -- -D warnings
```

上述 Viz 测试仅使用 SDK-free 共享类型、测试 RocksDB 与独立测试 IPC，不执行 CTP 代码。覆盖迟到成交、撤单确认、重启／断线／传输／过期证据，活主库 secondary 追平和主库字节不变，64 位字符串身份、行情缺失值／零值及重置／缺口，以及 HTTP／WS 只读传输。

2026-11-13 验证：14 项后端测试、针对本 crate 的 Clippy、格式和 release 构建通过。SDK-free 观察器读取既有易达仿真数据库的 17 笔委托与 10 笔成交，状态正确显示 stopped；公司内网 ZMQ→独立测试 IPC→Viz 收到六交易所 383 个合约快照，发布器正常退出后旧盘口清空。浏览器验证桌面／手机布局、筛选、原生记录详情、五档与自定义 WS 路径，未出现 JS 错误；主库 14 个文件 SHA-256 保持不变。收据位于 Git 忽略目录 `data/ops/cn_futures_viz_20261113T065745Z/`。测试进程已退出，没有常驻部署或 CTP SDK 执行。
