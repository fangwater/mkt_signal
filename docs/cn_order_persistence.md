# 国内原生订单回报与持久化

CTP / 易达交易进程直接使用 `persist_manager` 的嵌入式 RocksDB 存储。业务线程负责解析、风控和 OMS；原始回报与发送前审计的 JSON 构造在后台完成；有界队列把发送意图、原始回报、归一化订单、事实成交交给后台写盘线程。这个入口不启动 IPC、Redis、Tokio runtime 或独立 persist_manager 进程。

交易与持久化共享 `persist_common::cn_futures` 的唯一数据契约，`cn_futures::model` 重导出这些类型。原有加密市场的 binary `uniform_orders` 编码保持原状；国内记录保留交易日、交易所、完整会话／报单组、字符串原生系统号、开平标志、手数和期权元数据，不借用 crypto venue 编号、策略来源或费用字段表达这些信息。

## 配置与写盘保证

`persistence_directory` 可选，默认是 `flow_directory/persist_manager`。数据库绑定适配器、账户与柜台地址的长度编码 SHA-256 摘要；不保存密码、授权码或 AppID。另一个账户或柜台使用同一路径会在加载 SDK 前失败。RocksDB 自带写锁，flow 目录另有排他锁。

CTP 固定异步发送审计，不再接受 `sync_before_send` 配置。易达保留此选项，`sync_before_send = false` 是其默认值：发送前记录和回报都异步入队。后台拿到第一条后，最多收取当前已经可用的 64 条记录，用一个同步 WAL WriteBatch 提交，不等待凑满批次。入队成功并不保证进程被强杀前已持久化；尚未完成 WAL 提交的尾部事件存在崩溃窗口。

易达 `sync_before_send = true` 时，逐笔与批量报撤都会在调用 SDK 前等待后台写盘确认。它增加发送延迟，但确保已发送请求有持久化的发送前证据。该模式仍不等待成交回报逐条写盘，回报恢复依赖柜台的完整追平／查询。

成功处理的实时成交与相应订单投影在同一个 WriteBatch 提交；回报累计数量保存在 `reported_filled`，只有事实成交计入 `filled`。原始回报先入队，解析失败也保留原始证据。重复成交按交易所、交易日、合约、方向、原生成交 ID 去重；内容或费用冲突保留错误证据并阻止继续交易，不能覆盖之前的事实。

批次内的后续记录能看到前缀中的订单和成交，因此同批重复成交、费用补充与重连恢复屏障仍按原顺序验证。投影冲突时，只提交此前的合法前缀和本条错误证据，后续记录不提交；错误记录及其后续等待者收到失败。WAL 写入失败不推进内存订单或序号。同步发送和恢复所需的确认均在实际 WAL 提交后发出。CTP 恢复改为 checkpoint 非阻塞轮询：业务循环继续处理行情/回报，提交完成前门控新报撤，失败或超时终止运行；成功后只解除恢复等待，不能清除其他故障。CTP 正常关闭也以完成状态轮询排空后台，业务路径不使用阻塞式确认接收。

`preallocated_orders = 4096` 是业务线程订单槽位及当前会话引用索引的容量提示，省略时使用同样的初始容量。恢复后保留该提示，历史超过容量会自动增长，不能把它当作订单上限。原始回报和发送前审计直接复制进有界队列的预分配槽位，后台再构造 JSON；归一化快照、字符串身份和成交去重仍有分配。

队列满、写盘失败和确认超时均锁定故障并停止运行，不重试报单。正常退出先结束 SDK 回调生产者，再处理已送达的尾部回报，最后等待写盘队列清空并检查关闭结果；不会自动撤销柜台订单。JSONL 审计仍保留，100 ms 刷盘，RocksDB 是归一化记录的持久化来源。

实时投影在业务线程复制定长订单／成交状态，合约规则通过不可变共享引用固定为入队时的版本，后台才构造归一化记录。后台不借用或读取可变的 OMS。继续成交、拒单、规则刷新和重连恢复不能改变队列里的旧快照。单笔事实成交与对应订单仍共享一个事件和 WriteBatch；多订单投影也保持单事件提交。满队列、字段越界和写盘错误保持故障停止语义，完整恢复仍走持久化确认屏障。

## 列族与读取

| 列族 | 内容 |
| --- | --- |
| `cn_trading_meta` | 账户／柜台摘要与持久化事件序号 |
| `cn_trading_events` | 按序追加的脱敏原始证据、发送回执、恢复结果与投影错误 |
| `cn_uniform_orders` | 每个交易日、交易所、完整原生 OrderKey 的最新归一化订单 |
| `cn_trade_facts` | 去重后的事实成交；通过交易日、交易所与系统订单号关联订单 |

`NativeOrderRecord` 同时保存当前 OMS 投影与原始 `submitted_intent`。恢复后 `order.owned=false`；历史 `locally_submitted=true` 仅说明曾由本地提交，不代表新进程或任何策略拥有该订单。SDK 未提供的成交流动性角色、策略归属及 CTP 手续费不作推定。易达手续费按回报原值保留为 CNY，原生时间保留其原始整数值，不作为 Unix 时间戳使用。

Day / FAK / FOK 从真实回报字段恢复；CTP 使用 TimeCondition / VolumeCondition，易达使用原生 OrderType。撤单终态若已有全量事实成交，归一化恢复为 Filled，原始撤单状态仍在证据中。

离线导出复用 `persist_manager::RocksDbStore`，无需 SDK 或凭据：

```bash
cargo build --release -p persist_manager --features embedded --bin cn_order_export
# 先正常停止交易进程，避免与写锁冲突。
target/release/cn_order_export \
  --input-dir /absolute/env/flow/persist_manager \
  --output-dir /absolute/export/new-directory
```

输出各列族的 JSONL 文件，文件默认权限 0600，拒绝覆盖已有导出文件。库接口 `cn_futures::export_store` 也可以接收 RocksDB secondary，供后续监控读取使用。当前入口不接原加密市场的 Parquet 导出、gRPC 同步或 account_monitor；这些不能视为已完成接入。

## 重启恢复

启动保留交易阻断，取得完整且通过核心校验的柜台快照后，还须通过后台持久化恢复屏障：上次本地未确认或仍未终结的订单必须在快照中找到；缺失、身份冲突、已知成交量倒退或终态倒退都阻止继续交易。不能清空数据库来绕过未决请求，也不根据一次查询缺失认定订单未发送。

恢复还会读取数据库中该交易日／会话已消耗的报单引用上界，包括本地拒单；易达在原生引用分配器中跳过已持久化引用，避免柜台最大引用未记录本地拒单而造成身份重用。

## 验证入口

包含 CTP 代码的测试仅限用户指定并授权的外部主机，当前开发机及所有内网机器只可编辑和编译；以下历史记录不构成运行授权。本轮固定布局/非阻塞改造只编译新增用例，未执行测试。布局与策略接口见 [ctp_layout.md](ctp_layout.md)。

```bash
LIBCLANG_PATH=/absolute/libclang-directory \
CTP_SDK_DIR=/absolute/ctp-reference \
YIDA_SDK_DIR=/absolute/yida/include \
  cargo test --locked -p cn_futures -p persist_common -p persist_manager \
  --features cn_futures/ctp-sdk,cn_futures/yida-sdk,persist_manager/runtime --lib
```

新增存储测试实际重开 RocksDB，验证乱序与重复成交、冲突证据、批次原子性、跨组／交易所／交易日身份、账户绑定、未决请求恢复及导出。进程崩溃测试在发送意图获得同步确认后直接 `process::exit`，跳过析构，再由父进程重开数据库验证未决请求和引用上界仍在。

CTP / 易达合成 SDK 脚本现在每场景都调用同一目录的 `cn_order_export`，读取真实 DB 核对订单、事实成交和证据。先构建 release 的交易程序与 exporter，再运行各自桥接脚本。官网仿真脚本也会检查实际导出；`--restart-check` 额外用同一 flow 与数据库启动第二个只读进程，核对账户仓位和恢复后的导出，`--sync-before-send` 可覆盖同步发送前记录模式。

2026-11-10 本地回归结果：`cn_futures` 双 SDK 93 项常规测试、`persist_common` 15 项、`persist_manager/runtime` 74 项全部通过；官方 CTP 库离线构造／释放单独通过。易达桥接包含 42 个实际重开 DB 核对的 SDK 场景和 1 个拒绝触碰已有持仓的联调脚本保护场景；CTP 桥接落盘检查通过。`cargo fmt`、三个相关 crate 的 `clippy --no-deps -D warnings` 与本地 release 构建通过。

本轮官网公共仿真使用默认异步模式，验证 SHFE `cu2612` / XTCP 的 17 笔报单、10 笔事实成交，以及 INE `sc2612C640` / UDP 的 31 笔报单、24 笔事实成交；包含限价／市价、Day／FAK／FOK、回报前撤单、席位选择和批量报撤。各组退出后实际重开并导出 RocksDB，又使用原 flow 和 DB 启动第二个只读进程、恢复账户并再次导出。各组首次结束时的订单／成交事实行数分别为 861／618 和 892／642（包含完整当日账户历史，不能当作本轮新发单数量）；测试前后的 30 条既有持仓保持一致，本次订单全部终结，新增仓位清理完成。没有生产部署。公共仿真也暴露并回归了手续费浮点往返精度问题：共享 JSON 依赖启用 `float_roundtrip`，避免将同一事实的浮点解析误差视作费用冲突。

业务线程的未结订单索引、后台序列化与本地基准结果见[热路径性能说明](cn_futures_performance.md)。
