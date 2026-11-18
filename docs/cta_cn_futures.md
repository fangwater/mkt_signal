# 国内期货 CTA 目标手数进程

最后更新：2026-11-18

国内 CTA 按 `cta-cn_futures-01 / 02 / ...` 实例隔离；当前 `01` 位于 `/home/fanghaizhou/cta-cn_futures-01`，由环境脚本包装
PM2 管理。范围为 `SHFE/SN`，支持 `1min / 5min / 15min / 1h`；各频率独立配置规则、
开关和每规则手数，并各自维护绝对目标。1h 按用户表配置 10 条规则，事实账本保留；
新增频率默认关闭、空规则。每条规则输出 `-1/0/+1`，乘以 `lots_per_rule` 后求和。复用 CN 原始因子、Prep 小时归一化、
百分位和开平仓判断，不做金额到数量的换算。可通过组级发布凭据整批推送 Manager；不加载交易 SDK、不直接报撤单。
该进程使用公司 ZMQ；本机禁止执行 CTP SDK 的约束保持有效。

当前环境的 1h 因子表如下，每规则 1 手；1min / 5min / 15min 仍为关闭、空规则。

| 因子 | 符号 | NQ |
|---|---|---|
| td_pr_010 | +1 | 0.20/0.80 |
| baseline_095 | +1 | 关 |
| factor_094 | -1 | 0.20/0.80 |
| baseline_094 | +1 | 关 |
| baseline_110 | -1 | 关 |
| td_pr_011 | +1 | 关 |
| baseline_035 | +1 | 关 |
| factor_093 | -1 | 关 |
| baseline_068 | +1 | 关 |
| baseline_101 | -1 | 关 |

因子替换前用候选配置通过环境本地 `preflight`；仅替换规则，不修改开平阈值、
手数或事实仓位。历史预热不产生目标，下一次有效实时决策才按新表更新。

## 实例与临时逐因子归因

`01 / 02` 各自使用环境目录、配置、数据目录、HTTP 端口和进程名。生命周期脚本从配置读取实例名并核对目录，禁止用 `01` 的目录启动 `02`。`config/cta_cn_futures_01.json` 维护用户的十因子表；`config/cta_cn_futures_02.json` 是关闭、空规则的独立实例模板，不自动启用。

`publication.per_factor_intervals` 控制临时归因展开，例如 `["1h"]`：

- 普通模式：同实例每频段一个策略，如 `cn-futures-01-1h`、`cn-futures-02-5min`。
- 逐因子模式：`cn-futures-01-td_pr_010-1h`、`cn-futures-01-factor_094-1h` 等。
- 两种表示互斥；同一频段不会把聚合目标与子策略目标同时加进 Manager。公式、NQ、每规则手数、因子事实旗标和本地聚合展示保持原义。
- 同一实例的全部策略组成一个 `strategy_group = cn-futures-01`，每次只 POST 一个完整批次；禁止遍历因子逐条发布。Manager 按账户统一输出净目标，同时保存各子策略持仓和内部调拨。

发布由 `publication.enabled` 显式开启，仓库模板默认关闭。配置示例：

```json
{
  "enabled": false,
  "manager_url": "http://127.0.0.1:18202/api",
  "source_id": "native-account",
  "token_env": "CN_CTA_01_PUBLISH_TOKEN",
  "per_factor_intervals": ["1h"]
}
```

以上是根配置内的 `publication` 对象。`manager_url` 使用部署网关实际地址，不包含凭据；组级发布令牌只从环境变量读入，不进入配置面板、状态、出站日志或 outbox。先在 Manager 创建子策略、核对账户事实持仓并初始化 `strategy_groups`，再通过原会话授权的 `POST /api/native/sources/{id}/netting/publish-token` 为该组生成令牌。每个实例使用独立令牌；源账户与组名均须一致。

发送端先读取 `netting/publication?strategy_group=cn-futures-01` 的组名单、完整合约范围和 revision，补齐合约零目标后，原子 POST `target-batches`。批次含 `strategy_group / batch_id / expected_revision / decision_us / signal_times / strategies / quotes`。`signal_times` 留存各子策略的真实 bar 决策时间；外层 `decision_us` 是整批发布时点。BBO 来自当前公司 ZMQ，原生合约和价格观测时间原样保留。

首次启动仅记录既有决策基线，历史预热不发过去目标；实际新决策才可触发推送。断网时保留最新完整目标；已派发但结果未知的请求在 `data/manager_publication.json` 中持久化，重启后逐字重试原批次，不能改写它的 revision、价格或部分策略。Manager 409 表示该请求未接纳，才重新获取 CAS；过期且已明确拒绝的信号不重放。任何未落盘的待发请求不得发送。

01 组首次启用逐因子发布时，已核对 Manager 只有空仓初始化记录，并归档旧的 +3 手本地信号目标。2026-11-18 从空信号账本启动，历史分钟和小时只用于因子预热，目标从接续完成后的首个有效实时小时决策开始计算。后续例行重启继续从已保存的实时决策接续。

`/state` 和 `/targets` 新增 `strategy_group / strategies / manager_publication`。已有 `positions` 是本地聚合展示，消费端不能把它与 `strategies` 再求和。发布状态显示待确认批次、已确认批次和错误，不暴露令牌。

本轮统一执行边界是 Manager 的整批净目标输出与可拆分归因，未新增原生自动报撤单适配器；Manager 的 `execution_enabled=false` 保持真实。环境切换必须另行完成真实输入预热、归因初始化与存量持仓核对，不能直接把旧聚合策略的真实归属改名。

## 数据接续

1. 启动后立即订阅公司 ZMQ `tcp://10.61.20.12:13005`，在内存中持续接收。
2. 从 REST `http://127.0.0.1:18313/api/v1/dominant` 选择 `xsge/SN` 的原生合约。
   下一原生交易日只采用明确的 `scheduled_next`，不推算交割月份。
3. 从 `http://127.0.0.1:18312` 的 `cn_market.v1.CnFuturesMarket` 查询分钟、阈值
   和时段。参数未齐时暂存原始 ZMQ；参数齐后复用共享 `cn_futures_core`，持续合成
   1min baseline。首次接入的残缺分钟丢弃，第一根完整分钟固定为接续边界。
4. gRPC 因 MDL 延迟而落后是正常情况。本地继续保留第一根完整分钟和所有后续分钟，
   每 5 秒补查 gRPC，等它到达并最终完成这根固定分钟。不能移动起点追最新分钟。
5. 核对边界分钟的成交、分桶、OHLC、mid 和五档输入。用 gRPC 前缀（包含边界分钟）
   预热，再按顺序消费边界之后的 ZMQ 缓存。边界只消费一次，保留尚未完整的小时。
6. 缓存追完后接实时，释放 gRPC 历史分钟和启动缓存，结束历史查询。gRPC 仅用于
   进程初始化；下一次查询历史须等进程重启。追赶只恢复因子窗口，保留事实目标
   账本和决策时钟；激活后新闭合的小时才可产生新目标。

网络请求期间 ZMQ 捕获持续运行；控制器先排空排队的数据和 reset 标记，再对齐。
分钟主要由来源的下一分钟快照确认闭合，不能仅因 gRPC 请求耗时、本机时间推进，
就将滞后来源中的残缺分钟当作完整分钟。时段末尾只有在队列已排空、末秒快照新鲜
并经过短暂宽限后才允许收尾。

初始化期间，断连、身份冲突、队列丢失或缓冲耗尽会重置接续边界；gRPC 暂时失败
或落后不会清除本地分钟缓存。初始化完成后，普通交易日切换继续使用 ZMQ、启动时
获取的参数和已经预热的因子窗口。运行期间发生来源缺口、合约换主力或缺少新月份
参数时，保留已发布目标并标记 `restart_required`；重启后重新初始化，不在运行代
重新调用 gRPC。主力 REST 查询继续独立运行。

1h 沿用既有研究口径：连续源分钟从片段第一分钟起每 60 根一组，保留时段短尾。
1min 直接使用源分钟；5min / 15min 按整点时钟边界聚合，首次历史的残缺组丢弃。
真实零成交分钟占用时间槽；休市和源缺行不凭空补分钟。mid 使用行情提供的值；
1h 缺少或无效时使用 close，分钟频率缺少或无效时暂停并保留目标。缺少五档、分桶或适用月份阈值时暂停，不用零值替代。

## gRPC 契约

- `GetMinuteBars`：响应仅 `contract / bars / data_state`，按 30 日分页读取 120 天，
  `include_empty_minutes=true`；分钟暂态数据可反复更新，覆盖查询区间时删除已消失的行。
- `GetSizeThresholds`：查询 SHFE/SN，按 `applies_month`（上海自然月）使用产品 P50/P90。
  不在策略侧维护导出器或阈值文件，不默认使用别的月份。
- `GetTradingSessions`：查询 SHFE/SN 时段；快照日期必须与分钟数据一致。
- 归档分钟为最终数据。暂态分钟须满足 `open_time_ms + 60000 <= live_through_ms`
  才能作为 gRPC 接续边界。来源尚未补到边界时继续等待并积累本地分钟。

从 2026-11 起，服务端分桶采用前一完整上海自然月、逐交易日主力合约合并后的
成交区间金额阈值；更早归档保留服务端定义的旧分类。大小指快照推断成交区间，
不是交易所提供的逐笔订单大小。相比旧部署的前月全合约阈值，口径发生变化，
应从服务端数据重新预热，不能宣称新旧分桶相同。

策略不再调用 `StreamIntradayFrames`，不读写历史 PB 页、行情 JSONL、会话文件或
阈值文件，也不承担市场服务的落盘工作。保留的文件是事实 `positions.json`、
`targets.json`、配置及其审计、访问凭据、进程锁和运维验证收据。


## 固定窗口

分钟频率沿用 Prep 参数；国内商品 1h 独立使用下列参数。单位为实际形成的该频率 bar，而非连续自然时钟：

| 频率 | 因子 z-score 窗口 / 最少有效值 | 因子分位窗口 / 最少有效值 | NQ 极值 / 分位 / 最少有效变化值 |
|---|---|---|---|
| 1min | 1440 / 1000 | 2880 / 1440 | 120 / 120 / 60 |
| 5min | 288 / 200 | 576 / 288 | 120 / 120 / 60 |
| 15min | 96 / 67 | 192 / 96 | 120 / 120 / 60 |
| 1h | 120 / 60 | 48 / 24 | 120 / 120 / 60 |

因子自己的原始公式还有各自定义的窗口；先在 z-score 窗口内用样本标准差
（ddof=1）计算均值±3σ，再统一截断该窗口全部有效原值并重算均值、标准差及当前 z。
零方差为 0；1h z-score 缺失时前向填充上一个有效值，分钟频率当前原始值缺失时不输出有效交易分位。1h 分位用小于或等于当前值的数量排名，其他频率对相同值取中点排名。NQ 对最近 120 个有效价格算最高／最低，
多侧为 `(mid - 最低) / 最低`，空侧为 `(mid - 最高) / 最高`；对两侧各自最近
120 个有效变化值求分位，不做 z-score。1h 使用 mid，缺失时使用 close；NQ 只限制新开仓。

## 配置面板与状态

公网配置面板：`http://124.223.189.200/cta-cn-futures/`。复用 Prep 配置页面，
固定 SN，通过四个频率标签分别配置；以手数替代金额，可编辑因子、符号、NQ、开关及开平阈值。
GET `/api/config` 无需授权；POST `/api/preview`、`/api/config` 需要环境私有凭据。
配置读取不包含连接地址、路径或凭据。公网继续使用现有反向隧道，云端 Nginx
配置为 `config/cta_cn_futures_public.nginx.conf`，通过环境本地的
`scripts/cta_cn_futures_public.sh` 管理。

保存前校验 revision、字段和实际已观测小时上的因子就绪状态。保存重建公式窗口，
不改事实仓位，不补历史目标。关闭策略停止新决策，保留旧目标。外部直接改配置
需要经环境脚本重启。调整手数只影响后续实时决策，不重算已有目标。

内部只读接口仍为 `127.0.0.1:19401/state` 和 `/targets`，不通过公网面板转发。
`/state` 提供因子就绪、实时就绪、错误、`initialization_complete` 和 `restart_required`；
初始化期间额外提供 gRPC 水位、固定首分钟和缓存分钟数。`frequencies` 按频率返回
独立因子状态和目标，原顶层 `positions`、`interval` 保留 1h 口径；`/targets` 同时
提供这两种视图。原 1h 继续使用 `positions.json / targets.json`，新增频率分别使用
`positions_<频率>.json / targets_<频率>.json`。单个分钟频率的数据错误只暂停该频率，
保留其事实目标，不阻止正常的其他频率。
消费 `/targets` 必须检查 `factor_ready / live_ready / decision_ms`。换主力后的
首次有效决策同时输出旧合约目标 0 和新合约目标，重复读取不产生增量交易。

## 运维和发布

仅通过环境本地脚本：

```bash
cd /home/fanghaizhou/cta-cn_futures-01
./scripts/cta_cn_futures_process.sh status
./scripts/cta_cn_futures_process.sh preflight --execute
./scripts/cta_cn_futures_process.sh restart --execute
```

省略 `--execute` 只打印计划。preflight 在临时目录仅复制事实账本，通过 gRPC
验证新参数接口、输入和启用频率的因子就绪，并复制各频率事实账本；失败则不停止现有进程。支持
`preflight --binary <候选文件> --config <候选配置> --execute`，便于安装前检查。
停止或重启前保存日志与账本，不使用裸 PM2 命令操作环境。

网络接续改造使用已推送的 `market_replay` 正式协议提交
`5ee7d1d793e1fd02f78280c224d710390767aa59`，proto 和共享 core 固定到同一个 rev，
不依赖临时 proto 或路径覆盖。部署仍须先验证生产服务提供新参数方法、历史输入
和实时分钟接续；预检失败则保留当前代。环境配置只移除旧
`session_url / size_thresholds` 两项，保留用户参数、凭据和事实账本。
本次发布更新 worker、环境本地生命周期脚本和配置；保留配置访问凭据及事实账本。
源码和构建校验收据位于 `data/ops/cta_cn_grpc_refactor_check/`，发布收据由
`data/ops/cta_cn_futures_01_latest.txt` 指向（均不提交到 Git）。

当前验证：23 项信号测试、8 项 SDK-free 行情测试和 3 项生命周期检查通过；
真实 gRPC 预热 `sn2612` 的 36,720 根分钟形成 712 根小时，当前用户表 10/10 就绪，事实账本不变。
因子表替换的预检、配置备份和加载核验由
`data/ops/cta_cn_futures_user_factors_latest.txt` 指向（不提交到 Git）。
盘后启动的版本先保留已有目标，等待下一交易时段形成首根完整 ZMQ 分钟，并完成
gRPC 对齐；`initialization_complete / live_ready` 以内部接口实际返回为准。
