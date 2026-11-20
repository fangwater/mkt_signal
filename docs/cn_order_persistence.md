# 国内原生订单回报与持久化

最后更新：2026-11-20 UTC。

CTP / 易达交易进程与 crypto 的 pre_trade + trade_engine 对齐：解析、风控、OMS、报撤单、执行算法、策略归因账本，以及经 `PersistChannel` 发布记录，都在一个业务线程内完成（另有厂商 SDK 自身线程）。记录在循环里写进复用缓冲区，经 iceoryx2 一次拷贝发布给同一部署目录下的独立 `persist_manager` 进程（服务 `<IPC_NAMESPACE>/persist_pubs/cn_native`），与 crypto `PersistChannel` 一样不等待确认。

## 存储只有两路 crypto 格式记录

国内库与 crypto 的 `uniform_orders` / `trade_updates` 同构，**不存任何 JSON**，也没有事件、meta 或账本列族：

| 列族 | 内容 |
| --- | --- |
| `cn_uniform_orders` | crypto `UnifiedOrderRecord` 存储布局的事件流水：订单生命周期、按策略拆分的成交、内部对冲 |
| `cn_trade_updates` | crypto `TradeUpdate` 布局的事实成交，每笔柜台成交只记一次 |

键是 20 位十进制微秒时间戳，与 crypto 相同；`persist_manager` 保证同一列族内严格递增，只追加、不覆盖、不删除。`persist_manager` 不做投影或校验，按收到的字节原样写入并批量同步 WAL；记录由 Exec 生成最终形态。

国内交易所是 `TradingVenue` 的新编码：CFFEX 64、SHFE 65、INE 66、DCE 67、CZCE 68、GFEX 69。国内成交实现 crypto `TradeUpdate` trait，数量单位为手。

### 国内扩展段

crypto 字段对不上的国内事实放在 crypto 布局之后的可选扩展段（`persist_common::record_extension`，魔数 `0xFFFF_FFFE`，大于任何 crypto provenance 长度，解码器可区分）。crypto 记录不写扩展段，字节与现有解码器不变。字段：

| 字段 | 含义 |
| --- | --- |
| `position_effect` | 开仓 / 平仓 / 平今 / 平昨 |
| `trading_day` | 柜台交易日（夜盘属于下一交易日） |
| `trade_id` | 柜台成交号；恢复时柜台会重报成交，用它去重 |
| `last_filled_quantity` | 本笔成交手数 |
| `commission` | 本记录手续费（CNY）。易达用柜台值；CTP 用已查询费率按开平估算 |
| `client_order_text` | 原生订单身份 `会话/OrderRef` |

`client_order_id` 是交易日、交易所、会话、OrderRef 的稳定 63 位哈希，同一订单的 uniform order 与 trade update 共用，跨重启不变；准确身份以扩展段为准。

### uniform order 的三类记录

- **订单生命周期**：`amount_update = 0`，状态或成交量变化时记一条（报单在 SDK 调用之后、同一轮循环内发布，不推迟报单）。`from_key` 为 `exec`（本进程报单）、`exec:<batch_id>`（执行目标批次的报单）或 `observed`（其他会话或恢复看到的委托）。状态按 crypto 编码，国内拒单记为 Expired。
- **策略成交**：一笔柜台成交按 Exec 的归因拆成每个策略一条，`amount_update` 为该策略手数，`from_key` 为策略名；未归属部分记入 `__unallocated__`。其他会话的成交、无目标时的成交、恢复时新发现的成交整笔记入 `__unallocated__`。手续费按手数比例分摊。
- **内部对冲**：`ttype = 2`（与 crypto BatchExec 相同），成交价为当时报价 mid，买卖腿各一条，`from_key` 为双方策略。

某策略在某合约的持仓 = 该 `from_key` 所有成交与对冲记录的带符号 `amount_update` 之和。Manager 按 crypto 口径从这些记录计算策略 NAV。

## 不落库的记录

厂商原始回报、错误、生命周期、执行决策（`exec_quote_decision`、策略执行回执、`ctp_strategy_dispatch_blocked`）只作为 trader 日志，由 `persist_manager` 打印到 stdout，不写库。账户、持仓、费率查询回包只更新内存状态。密码、授权码与 AppID 在 Exec 侧脱敏后才离开进程。

## 配置与写盘保证

交易进程与 `persist_manager` 必须使用相同的 `IPC_NAMESPACE`，并在同一部署目录运行：`persist_manager` 固定写 `data/persist_manager`，交易 TOML 的 `persistence_directory`（默认 `flow_directory/persist_manager`）须指向同一路径。一个账户一个库，账户隔离靠部署目录。iceoryx2 服务保留 4096 条历史，`persist_manager` 晚启动或重启时可补收，超出部分丢弃（与 crypto 相同）。发布成功不保证落盘；进程被强杀前尚未提交的尾部存在崩溃窗口。本地发布失败（共享内存不可用）锁定故障并阻断后续交易，已发出的订单不受影响。

## 重启恢复

启动时交易进程只读打开 `persist_manager` 的库一次，从两路流水重建：

- 每个交易日 / 会话已用过的最大 OrderRef，新引用从其后开始；
- 上一进程本地报出、最后一条生命周期仍未终结的委托；
- 策略归因账本（按策略累加成交与对冲记录，不含 `__unallocated__`）；
- 已记录的成交号集合。

数据库不存在或缺少两张表时视为空历史。取得完整且通过核心校验的柜台快照后，上一进程的未决委托必须全部出现在快照中，否则阻断交易并记录 `trading_blocked`。快照中的成交若不在已记录集合，补记 trade update 与整笔 `__unallocated__` 成交，之后由内部对冲按 mid 转给相应策略；已记录的成交不重复写入。崩溃窗口内未落盘的归因在重启后表现为 `__unallocated__` 余量，手数始终与账户一致。

离线查看用 `cn_order_export --input-dir /absolute/env/flow/persist_manager --output-dir NEW_DIR`（先停止 `persist_manager`），把两张二进制表渲染成人读的 JSONL；导出文件不是存储格式。

## 验证入口

包含 CTP 代码的测试仅限用户指定的外部主机 `sh_dev`，当前开发机及所有内网机器只可编辑和编译（见 [AGENTS.md](../AGENTS.md#ctp-execution-location-restriction)）。本轮改造在开发机只做了编译检查；`persist_common`（扩展段与 uniform 编解码往返）与 `persist_manager`（键唯一递增）的测试在本机通过，`cn_futures` 的持久化、恢复、执行算法测试须在 `sh_dev` 运行。
