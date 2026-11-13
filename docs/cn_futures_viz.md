# 国内期货 Viz

`cn-futures-viz` 复用 crypto `viz_server` 的 HTTP、WebSocket 缓存重放、每秒合并发送与慢客户端超时。**持仓、连接、恢复状态和本地预算由交易进程通过 IPC 定时推送；委托与事实成交继续读取归一化 RocksDB。** 行情读取独立 `cn-futures-pbs` 的统一行情 IPC。CTP 与易达使用同一份当前内部契约。

## 启动与配置

```bash
cargo build --release -p viz_server --bin cn-futures-viz
cp config/cn_futures_viz.example.toml config/cn_futures_viz.toml
# 填写柜台源、账户摘要和独立 secondary 路径。
target/release/cn-futures-viz --config config/cn_futures_viz.toml --check-config
target/release/cn-futures-viz --config config/cn_futures_viz.toml
```

默认浏览 `http://127.0.0.1:8811/`。原 crypto 入口与配置保持独立。默认只监听本地；远程观察可经现有受控代理转发 HTTP 和 WebSocket，访问控制由部署环境提供。

交易配置在根级增加：

```toml
# CTP / 易达各自使用独立 namespace；同账户风险域只配置一个发布者。
position_ipc = { namespace = "cn_yida_sim", interval_ms = 1000 }
```

对应 Viz 配置：

```toml
[[counters]]
id = "yida-sim"
label = "易达仿真柜台"
account = "simulation-observe" # 展示别名
adapter = "yida"              # 另一适配器为 "ctp"
position_namespace = "cn_yida_sim"
expected_account_hash = "<cn_trading_meta 中 identity.account_hash 的 64 字符值>"
rocksdb_path = "/absolute/yida-env/flow/persist_manager"
secondary_path = "/absolute/viz-state/yida-sim"
```

`position_namespace` 必须匹配交易进程，`expected_account_hash` 同时绑定 IPC 与数据库的账户／适配器／柜台摘要；它是摘要，不是凭据。每个源独立配置，两账户不能共享命名空间或 secondary。持仓接收不依赖 RocksDB 可读；数据库出错时委托／成交显示不可用，正确绑定的持仓 IPC 仍能正常更新。

没有启用交易端 `position_ipc` 时，Viz 等待持仓 IPC；**不回退到数据库里的历史持仓**。交易配置省略该项则不创建观察发布线程。示例配置已给出 CTP 与易达各自的命名空间。

`--check-config` 只解析配置与路径，不打开数据库、IPC 或 SDK。Viz 和 `cn_futures_state` 均不依赖交易 SDK。CTP 客户端仍只能在用户明确指定并授权的外部主机执行，本机和所有内网机器仅允许 CTP 编辑及编译；本实现没有运行 CTP。

## 持仓 IPC 与刷新语义

唯一消息定义在 SDK-free `crates/cn_futures_state`。服务名是 `<namespace>/viz_pubs/cn_futures_positions`，使用与 Exec 相同思路的长度前缀 bincode／Iceoryx2 全量状态消息，每条固定 IPC 容量 1 MiB、历史 1 条、订阅缓冲 2 条、单发布者和安全覆盖。默认每秒采集一次，Viz 开启服务后在下次发布时接收快照。

消息保留账户摘要、适配器、进程 epoch、单调序号、**业务线程采集时间**、柜台交易日、持仓恢复标记、连接／核心就绪／交易传输／可交易状态、本地执行预算，以及按交易所、合约、多空方向隔离的今仓、昨仓、冻结今仓、冻结昨仓。

交易核心根据真实柜台恢复与实时事实成交维护仓位，报撤单维护冻结；快照直接复制这些当前值，不通过订单／成交历史另算一份仓位，也不按策略目标推导。每帧整体替换：已平仓行的消失或显式零值能清除旧显示，空的已恢复快照代表当前账户为空。`position_ready=false` 表示尚未取得完整初始持仓，不能当成空仓。断线时保留核心已知仓位并标记 disconnected；重启的新 epoch 在恢复前清除上个进程的展示。

业务线程仅在定时到期时复制轻量状态到三个冷初始化缓冲之一，使用非阻塞队列替换后台尚未处理的旧快照。不扫描订单、构造 JSON、等待磁盘、消费者或发布确认。后台线程继承配置的 background CPU／NUMA 放置，负责编码和 IPC 发送。最多 8192 条多空持仓行；超限或采集失败整帧标为失败，不截断成部分账户。显式配置的发布器初始化失败会在 SDK 启动前返回错误；运行中发布故障不改变交易状态或重试订单，Viz 依据最后采集时间显示不可用／过期。

Viz 校验账户摘要、适配器、完整帧、数量与冻结关系、重复身份和同 epoch 序号；拒绝跨账户或倒退数据。超过 `stale_after_ms` 未收到新采集，显示 stale。旧 IPC 值可留作历史展示，但不会用数据库快照刷新其时效。正常退出尽力发送 stopped，异常退出尽力发送 failed；PUB/SUB 是有损观察通道，进程被杀或终态丢失时依靠心跳过期识别。HTTP 连接在线与持仓新鲜度分别显示。

## 委托、成交与行情

每个柜台由独立读线程打开 RocksDB secondary，只写配置中的 secondary 目录。每轮 catch up 一次，扫描期间固定句柄；不调用 RocksDB secondary 不支持的 `snapshot()`。证据增量读取，主库序号变化时扫描订单／成交投影；`row_limit` 限制返回行数，未决委托优先，隐藏条数与全库汇总单独返回。读取失败不会影响持仓订阅。

委托保留交易日、交易所、完整原生会话／引用和系统号，ID 始终为字符串。撤单发送不代表已撤；终态但事实成交未齐显示 settling；发送结果未明显示 uncertain。`filled` 只统计事实成交，`reported_filled` 是订单回报累计量。手续费未知是 null，易达已报 CNY 费用与零值原样保留，CTP 未报费用不估算。原生 `native_time` 不当作 Unix 时间。

账户权益仅在数据库中的 CTP account 证据提供时展示，带独立记录时间；它不是最新主动查询。实时 IPC 的 `available_local_budget` 明确标为本地执行预算，不作为权益／保证金。当前观察账户持仓，不推定策略归属；`can_trade` 仅转发执行进程当前的全局门控报告，不替代逐合约风险检查。

行情订阅 `<service_root>/<venue>/l2`，只打开现有服务，源晚启动时自动重试。公司内网示例是独立 `cn-futures-pbs --zmq` 产出的六交易所行情，**明确属于公司内网通联接入方式**，详见 [发布器](cn_futures_pbs.md)。Colo／公网部署使用独立 root 和 environment，继续复用同一消息处理；上游适配器是否可用以发布器文档为准。

行情按来源／交易所／原生大小写合约隔离。epoch、generation、reset 或序号缺口清除该交易所旧盘口；新的完整盘口不修复历史成交／因子连续性。缺失值输出 null，显式零数量保留，u64 序号和数量输出十进制字符串。`symbols` 精确过滤，`max_symbols` 限制缓存并显示容量丢弃数。

## HTTP 与验证

| 路径 | 行为 |
| --- | --- |
| `/` | 中文观察页，筛选柜台／交易所／合约／行情源，查看原生记录及五档 |
| `/healthz` | 只表示 HTTP 进程在线 |
| `/snapshot` | 与 crypto 相同的 `{ts_ms, entries}` 缓存结构 |
| 配置 `ws_path`，默认 `/ws` | 首次重放最新 HTTP 消息，后续每秒合并发送 |

`cn_execution` 包含每个源独立的 `position_state`（实时 IPC）、`lifecycle`（持久化证据）及订单／成交；`cn_market` 包含行情 channels／quotes。前端持仓表和运行状态卡片只读 `position_state.snapshot`。只提供 GET 观察接口，不代理配置或交易命令。

```bash
cargo fmt -p cn_futures_state -p cn_futures -p viz_server --check
# 仅 SDK-free 契约、IPC、Viz 测试；不运行 cn_futures/CTP 测试。
cargo test -p cn_futures_state -p viz_server --lib
cargo clippy -p cn_futures_state -p viz_server --all-targets --no-deps -- -D warnings
```

覆盖全量替换、平仓清除、冻结更新、账户绑定、重启／断线／过期、容量、帧校验、读库不回退持仓、数据库不可用时继续接收 IPC，以及原有订单／成交与 HTTP／WS 行为。业务侧发布队列的 1000 次捕获实测零堆分配，编码及完整消息初始化在后台执行。CTP／易达运行时集成仅做双 SDK 编译检查，不以 SDK-free 测试代替真实柜台验收。

2026-11-13 验证：6 项 `cn_futures_state` 与 16 项 Viz 测试通过，两个 SDK 的 all-targets 编译／Clippy 和本地 release 构建通过。独立 SDK-free 合成持仓源→IPC→Viz→HTTP／WS→桌面／手机页面联测覆盖恢复前未知、今昨仓／冻结更新、空仓清除、断线和心跳过期；另一个 Viz 在数据库不可用时仍持续收到同一持仓源。原易达仿真历史的 17 笔委托和 10 笔成交保持可读，主库 14 个文件 SHA-256 不变。收据：`data/ops/cn_positions_ipc_20261113T073735Z/receipt.json`（Git 忽略）。本轮未运行交易客户端，未连接柜台，也未常驻部署。
