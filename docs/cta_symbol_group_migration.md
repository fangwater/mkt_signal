# CTA 券池分仓与盈亏迁移

最后更新：2026-11-04 UTC（本地实现与验证，尚未应用到线上）

Prep02 固定注册九个策略：`top-5`、`top-20`、`last-20` 分别拥有
`1min`、`5min`、`15min`。`top-20 × 5min` 当前没有规则，仍保留空策略、零仓位。
其余八个配置合计 102 条规则。券池是策略身份的一部分，例如
`cta-top-5-prep02-bn-1min-pos`；不能直接把原三张账本重命名。

## 已确认的分配口径

- 按标的、因子、方向和 NQ 条件匹配旧子仓位。唯一匹配直接归组；命中多个券池则等分
  `qty` 与冻结的 `entry_notional`，最后一份吸收浮点余数，保持数量与入场金额守恒。
- 跨池拆分会改变规则在单个策略内的名称。例如 DOGE 的两种 baseline_141 NQ 条件
  从原账本的带 NQ 后缀名称，分别变成两个券池内的独立 baseline_141。决策时钟随规则迁移。
- 持仓中的规则没有目标、旧账本不是独立因子模式、目标已有库存或旧目标尚未执行到位时，
  拒绝准备，不按币种猜测归属或补成交。
- 多个真实账户分别按原 Manager 有效跟随份数分配，保留 Batch/Chase 家族和其他策略。
  新策略以后仍使用原份数；只对迁移时的重复规则旧仓等分。
- 历史盈亏、费用及成交归属保留原样。切换时用同一根已完成分钟收盘价生成 Manager 的
  零和 ownership correction。旧策略结转切换前的浮盈亏，新策略从该价格开始，后续盈亏和
  估算资金费归新策略。转移不是交易，不增加订单、成交量或手续费。

## 准备工具

`cta-symbol-group-migrate` 只读取捕获的数据，自动生成可校验的迁移包。
不写线上 Redis、Manager PostgreSQL 或 Exec RocksDB，不发订单，也不启停进程。
工具默认把完整结果写到 stdout；`--output-dir` 只允许创建新的诊断目录，
拒绝覆盖已有输出或把输出放入输入及运行状态目录。

```bash
cargo build --release --bin cta-symbol-group-migrate
target/release/cta-symbol-group-migrate \
  --config config/cta_prep_binance_futures.toml \
  --positions-dir /absolute/capture/positions \
  --request /absolute/capture/request.json \
  --output-dir /absolute/diagnostics/prep02-symbol-groups
```

原仓位目录必须包含三份现有进程持久化的
`cta-prep02-bn-{1min,5min,15min}-pos.json`。没有旧仓位的全新部署不需要迁移；
已有环境不能用伪造的空账本替代缺失记录。

请求 JSON 的字段如下；不得放入凭据：

| 字段 | 来源和要求 |
|---|---|
| `strategy_group` | 本次为 `prep02` |
| `cutover_ts_us` | 与最终 Exec 快照同一时刻，微秒、毫秒边界 |
| `affected_source_ids` | Manager 当前完整受影响账户清单；必须与 `sources` 一一对应 |
| `sources` | 下表所列的账户捕获；非零旧仓不能省略 |

每个 `sources` 元素：

| 字段 | 来源和要求 |
|---|---|
| `source_id` | Manager 的稳定 source ID |
| `shares` | 原三个策略名到有效跟随份数的映射，包含虚拟账户乘数 |
| `ledgers` | `batch_exec` / `chase_exec` 两个完整 `position_allocations` 原值；未使用家族明确为 `null`，不得伪造空账本 |
| `ledgers_after` | Exec 快照后的第二次账本捕获，必须与第一次完全一致 |
| `exec_snapshot` | Viz 的完整 `exec_pre_trade_state` entry；`position_ready=true`，`ts_ms` 与切换时刻一致，所有行无活动批次和活动委托 |
| `allocation_audit` | Manager 同源同时间完整重放得到的 `source_id`、`snapshot_ts_us`、`account`、`strategies`，使用既有 `AllocationAudit` JSON |
| `strategy_anchor` | 该账户是否使用策略初始仓位快照；为 true 时输出显式不变的 `__unallocated__` 对照腿 |
| `marks` | 币对到 `{ "close_ts_us": ..., "price": ... }`；时间为切换时刻所在分钟的起点，即上一根完整分钟的收盘时间 |
| `residual_quote_limit` | 默认 0，要求实际分仓精确等于旧目标；可显式允许最多 10 USDT 的已完成执行尾差，仍须验证 Exec 的完成原因、原目标及零 pending/活动委托 |

已完成执行的最小下单量/目标容差尾差随实际 Exec 数量迁移，不通过交易强行对齐。
尾差优先按新券池现有贡献的绝对数量比例分配；没有当前信号贡献时，在该符号的已配置
目标券池间等分。原始实际数量、新归属及调整量全部写入审计；信号子仓位不伪造尾差。

完整 Manager 投影必须与 Exec 的观察账户和旧策略归属相符。策略快照字段采用 Manager
现有契约，不新增版本、历史成交副本或跨仓库运行时依赖。

迁移包包含：

- `migration.json`：原账本 SHA-256、配置/请求摘要、逐规则拆分明细、原策略到九个新策略的
  映射、各账户的新旧份数及全部输出摘要。
- `signal_positions/`：九张新账本与三张已退役的零仓旧账本；
  `original_signal_positions/` 保留原始语义账本，原捕获文件不变。
- `manager-targets.json`：九张新目标表及三张旧目标清零表，供切换时注册与接管。
- `sources/<source>/`：两份原 Exec 分配值、两份新分配值及需要时的
  `manager-allocation.json`。后者直接使用现有 `nav_allocation_reconcile --candidate`
  契约，无成交或费用字段。

## 部署时的协调切换

生成迁移包不是完成部署。必须在一次受控切换中应用全部三层状态，不能先把旧信号停用清仓，
也不能仅给新策略发布目标。整个切换保持相关信号和 Exec 下单流程停止，使用各环境本地脚本
管理进程；Manager、账户监控和持久化按实际排空需求保留。先排空活动委托和在途成交，再捕获
最终账本。部署过程必须检查原始文件和两次 Redis 原值与包内摘要一致，任何漂移均重新准备。

先使用 Manager 既有的 `nav_allocation_reconcile --candidate` 做只读验证，确认完整历史、
切换前归属、账户 FIFO 和各窗口总盈亏均不变。显式应用时使用该工具的
`--apply --note` 追加不可变归属记录；不覆盖旧快照，不修改原始订单或历史 from_key。
没有调试目录、完整投影或已完成分钟价格时，不跳过验证。

九个新策略的 Manager 绑定复制原策略的执行模板、份数和权限；原三条绑定退出。
逐账户核对新有效份数与包内 `new_shares` 一致。随后在计算进程运行锁保护下安装全部新/退役
信号账本，对两个完整 Exec Redis 分配值做原值比较后替换，并保留可恢复备份。
更新目标和绑定时 Exec 必须仍停止，避免中间态变成真实平仓或开仓。
安装后确认每币每执行家族总数量不变、新目标与新分配完全一致，才按环境脚本恢复运行。

本工具准备所有拆分状态和 Manager 候选；捕获、注册绑定、写入线上状态与恢复进程由部署流程
协调执行，没有在日常热加载或进程启动时自动运行的迁移分支。本次尚未执行这些线上操作。
