# 国内期货 CTA 目标手数进程

最后更新：2026-11-23

国内 CTA 按 `cta-cn_futures-01 / 02 / ...` 实例隔离；当前 `01` 位于 `/home/fanghaizhou/cta-cn_futures-01`，由环境脚本包装
PM2 管理。01 的小时配置共覆盖 23 个品种、196 条规则：

| 交易所 | 品种 |
|---|---|
| CFFEX | IC、IM |
| SHFE | AG、HC、NI、RB、SN、SP、SS |
| INE | BC |
| CZCE | AP、FG、OI、PF、SA、SM、TA |
| DCE | EB、J、JM、PG、Y |
| GFEX | LC |

燃料油 FU 已排除。各品种配置保留研究表的方向和 NQ。锡的 `td_pr_012` 有两个方向，
每条规则有独立的 `id` 和目标仓位。
中证500 IC 有 25 条规则，中证1000 IM 有 24 条。两者均无夜盘，连续交易时段为
北京时间 09:30–11:30、13:00–15:00。IC 的 `td_pr_002` 正反向分别保留独立规则 ID。
每个品种支持 `1min / 5min / 15min / 1h`，分别维护因子窗口、分钟接续和绝对手数账本。
所有品种的 1min / 5min / 15min 均关闭、空规则。新增频率默认关闭、空规则。
每个品种的每个频段独立输出 `-1/0/+1` 手。复用 CN 原始因子、Prep 小时归一化、
百分位和开平仓判断。可通过组级发布凭据整批推送 Manager。
该进程使用公司 Redis；本机禁止执行 CTP SDK 的约束保持有效。

2026-11-19，原生单合约历史预检曾卡在国际铜的买五档和高低价窗口。
改用正式 HFQ 后，21 个品种的 147 条规则全部通过预检，随后切换进程并核对原有账本。
当时的运行代在休市期间加载了历史，等待各品种下一交易时段的 ZMQ 接续。

因子替换前用候选配置通过环境本地 `preflight`；仅替换规则，不修改开平阈值、
手数或事实仓位。历史预热不产生目标；接上实时行情后，最新一根未决策的 bar 补做一次决策（见「Manager 发布」），之后按实时 bar 更新。

## 02：易达仿真镜像（2026-11-24）

`cta-cn_futures-02` 位于 `/home/fanghaizhou/cta-cn_futures-02`，HTTP `127.0.0.1:19402`。它复制 01 中上期所品种的 1h 规则（SN 14 条，
AG 6、HC 5、NI 6、RB 6、SP 6、SS 4），从空账本启动，发往易达官方仿真 `sh_dev_yida_paper01`。
策略组为 `cn-futures-02`（SN）和 `cn-futures-02-<品种>`，成员为 `<组>-1h`。各组令牌在 02 环境的 `env.sh` 中，
变量名为 `CN_CTA_02_PUBLISH_TOKEN[_品种]`。Manager 登记用 `~/.config/cn-futures-cta-manager/admin-login.env` 会话，
通过 `extend-scope` 完成（revision 8，原 `yida-paper01-rb` 组和 Exec 账本不变），收据在 `data/ops/yida-cta02/`（Git 忽略）。
易达执行白名单扩至 sn2701、ag2702、hc2701、ni2701、rb2701、sp2701、ss2701，单笔和绝对目标仍为 1 手。
国际铜 BC 未纳入：仿真账户 bc2612 有 5 手历史多仓，纳入后 Exec 会按目标平掉。
预检全部规则就绪。下一时段行情接续后，最新一根 bar 补做决策并立即发布。

## Manager 发布与策略归因

`01 / 02` 各自使用环境目录、配置、数据目录、HTTP 端口和进程名。生命周期脚本从配置读取实例名并核对目录，禁止用 `01` 的目录启动 `02`。`config/cta_cn_futures_01.json` 维护锡规则与 `additional_products`；`config/cta_cn_futures_02.json` 是关闭、空规则的独立实例模板。

- 按品种与频段生成一个策略，如 `cn-futures-01-1h`、`cn-futures-01-sa-1h`。
- 同品种的策略组成独立发布组，每次提交该品种的完整策略批次。01 实例的 SN 沿用
  `cn-futures-01`，附加品种 RB、SA、SM 分别使用 `cn-futures-01-rb`、
  `cn-futures-01-sa`、`cn-futures-01-sm`。各品种共用 Redis 入口，独立封 bar、触发发布和重试。
  Manager 把账户全部策略的完整目标交给 Exec；与 crypto 一致，由 Exec 轧差、内部对冲并归因成交。
- 01 的每个品种使用一个 `1h` 策略，纯碱为 `cn-futures-01-sa-1h`。
  `/state` 的 `factor_signals` 展示各因子的 `-1/0/+1` 方向，最终手数见 `positions`。
  `publication.retire_hourly_factor_members` 用于已登记旧因子的组。切换前核对并结清旧批次，随后在原组登记聚合策略。
  下一次有效实时决策在同一批次提交聚合目标和旧成员的零目标；旧成员的持仓由 Exec 内部对冲转给聚合策略，归因记录在 Exec 的 uniform order 流水。

2026-11-20 午休，01 按品种启用小时聚合。21 个品种的 147 条规则通过预检，切换前后的工作进程账本校验一致。Manager 对账后结清 11:30 已过租约的纯碱外部需求，并为锡、螺纹和纯碱登记各自的小时聚合策略。Viz 验收确认纯碱显示 10 条因子信号。发布收据位于 `data/ops/cta-hourly-live.df06r6ya/`（Git 忽略）。

发布由 `publication.enabled` 显式开启，仓库模板默认关闭。配置示例：

```json
{
  "enabled": false,
  "manager_url": "http://127.0.0.1:18202/api",
  "source_id": "native-account",
  "token_env": "CN_CTA_01_PUBLISH_TOKEN",
  "per_factor_intervals": [],
  "retire_hourly_factor_members": true,
  "products": ["RB", "SA"]
}
```

`products` 选定可发布的品种。2026-11-23，01 的实盘配置发布 19 个品种。锡 SN 继续计算本地信号，Manager 保留停发前的目标，锡持仓由人工处理。
附加品种继承 Manager 地址、源账户和归因配置，
令牌环境名在根变量后附品种后缀，例如 `CN_CTA_01_PUBLISH_TOKEN_RB`。
各组在 Manager 单独登记成员及令牌。已有 SN 归因可通过 Manager 的 `extend-scope` 注册附加品种组，
保留原组和已归属仓位。同账户的批次共享 revision；新批次随时替换目标，不等待旧批次成交或归因。

2026-11-23 收盘后，`cn-futures-01-ic-1h` 与 `cn-futures-01-im-1h` 在创元 CTP 账户分别绑定
`CFFEX:IC2612` 和 `CFFEX:IM2612`。柜台最近一次完整对账中两品种为空仓，Manager 以零手初始化，
原有目标版本 106 和 Exec 归因保持一致。gRPC 每个股指提供 38,400 根分钟、640 根小时 bar，49 条新规则全部通过预检。
切换后两个品种均完成历史预热，等待下一个日盘的 Redis 接续。实盘发布与账本校验收据由
`data/ops/cta_ic_im_latest.txt` 指向（不提交到 Git）。

`manager_url` 使用部署网关实际地址，不包含凭据；组级发布令牌只从环境变量读入，不进入配置面板、状态、出站日志或 outbox。先在 Manager 创建子策略、核对账户事实持仓并初始化 `strategy_groups`，再通过原会话授权的 `POST /api/native/sources/{id}/netting/publish-token` 为该组生成令牌。每个发布组使用独立令牌，令牌绑定对应源账户和组名。

发送端先读取 `netting/publication?strategy_group=cn-futures-01` 的组名单、完整合约范围和 revision，补齐合约零目标后，原子 POST `target-batches`。批次含 `strategy_group / batch_id / expected_revision / decision_us / signal_times / strategies`。`signal_times` 留存各子策略的真实 bar 决策时间；外层 `decision_us` 是整批发布时点。批次不再携带 BBO：内部对冲由 Exec 用自己的实时行情定价。

准备由 Exec 执行的账户启用 `publication.include_trading_day`。发布批次附带本次实时决策的原生交易日，Exec 接纳目标时与柜台交易日核对；Manager 接收批次不读取 Exec 副本。执行侧配置见 [Manager 目标交付](native_exec.md#manager-delivery)。

历史预热本身不发目标。2026-11-24 起，预热完成且实时行情首次新鲜可用时，最新一根已完成但未决策的 bar
补做一次决策（日志 `catch_up_decision`），按当前原生交易日立即推送。例如收盘后启动时，当日 15:00 的最后一根 bar
在下一时段开盘时决策。效果与进程整日在线时一致：收盘 bar 当场决策，目标常驻，开盘执行。
更早的历史 bar 只用于预热；账本中已决策过的 bar 不会重复决策。断网时保留最新完整目标；已派发但结果未知的请求在 `data/manager_publication.json` 中持久化，重启后逐字重试原批次，不能改写它的 revision 或部分策略。附加品种在各自
`data/products/<品种>/manager_publication.json` 保存待确认批次。拆分旧发布组前须结清旧组待确认批次。Manager 409 表示该请求未接纳，才重新获取 CAS；过期且已明确拒绝的信号不重放。任何未落盘的待发请求不得发送。

01 组首次启用逐因子发布时，已核对 Manager 只有空仓初始化记录，并归档旧的 +3 手本地信号目标。2026-11-18 从空信号账本启动，历史分钟和小时只用于因子预热，目标从接续完成后的首个有效实时小时决策开始计算。后续例行重启继续从已保存的实时决策接续。

`/state` 和 `/targets` 的 `strategy_group / strategies / manager_publication` 对应主品种，
附加品种状态在 `products`，也可从各自的 `/products/<小写品种>/state` 和 `/targets` 读取。已有 `positions` 是本地聚合展示，消费端不能把它与 `strategies` 再求和。发布状态显示待确认批次、已确认批次和错误，不暴露令牌。

执行与归因边界与 crypto 一致：Manager 只路由策略目标并读取 Exec 证据计算策略 NAV；Exec 的策略账本见 [Strategy ledger](native_exec.md#strategy-ledger)。环境切换必须另行完成真实输入预热、归因初始化与存量持仓核对，不能直接把旧聚合策略的真实归属改名。

## 数据接续

国内期货 CTA 使用按交易日主力拼接的 `baseline_data_1min_hfq` 预热因子窗口，
原生合约分钟用于接续公司 Redis 和发布目标。复权基准采用已完成档案交易日；
若下一交易日换主力，用已确认的上一日参考价差调整历史价格，保持新主力原生价格基准。
成交额、手数和大小单分桶保持原生单位。

1. 启动后立即订阅公司 Redis `10.61.20.11:9380`，在内存中持续接收。订阅连接每 5 秒发送一次 PING，5 秒未收到 PONG 时断开重订，重新校验分钟接续。
2. 从 REST `http://127.0.0.1:18313/api/v1/dominant` 按交易所和品种选择主力。郑商所参考代码如 `SA2701` 转为 `SA701`，用于 Redis 匹配和 Manager 目标。
   下一原生交易日只采用明确的 `scheduled_next`，不推算交割月份。
3. 从 `http://127.0.0.1:18312` 的 `cn_market.v1.CnFuturesMarket` 分页获取同一代 HFQ，
   再请求档案收尾和之后的原生分钟，以及阈值和时段。参数未齐时暂存原始 Redis；
   参数齐后复用共享 `cn_futures_core`，持续合成
   1min baseline。首次接入的残缺分钟丢弃，第一根完整分钟固定为接续边界。
4. 启动预热依次消费 HFQ、已完成的原生历史分钟和完整的 Redis 分钟。Redis 首根完整分钟
   固定为两者的取舍边界。因子达到就绪后即可接实时，gRPC 的水位可以落后于这条边界。
5. 盘中发生缺口，记录合约、交易日和缺口起点，保留成熟的因子窗口和已发布目标。重新收到
   有效 Redis 分钟后继续决策，丢弃受缺口影响的未闭合 bar。`redis_gap_since_ms` 表示待修复缺口。
6. 2026-11-24 起盘中也回补：缺口出现后立即请求 Market Replay，它在 NFS MDL 之后用 ZMQ 分钟缓存补齐，
   通常一分钟内就能提供缺失分钟。后台重算期间新封口的实时分钟，在采纳结果后按序重放；
   重算区间内结束、没有实时决策的最新 bar 按补做决策处理一次。缺口未修复时，小时 bar 不做实时决策，
   否则会从缺口后的第一分钟起算、错位成 13:31–14:30 这样的 bar。下面描述的休市回补规则仍然适用。
   原规则：只在该品种的交易时段结束后请求 Market Replay 回补，午休和收盘均可处理。后台重算核验
   缺口内每根交易分钟，沿用缺口外已接收的 Redis 完整分钟。结果只在交易时段外、事实账本和配置
   仍一致时采用。历史重算只更新因子状态，后续有效实时策略 bar 才能发布新目标。
   回补不足时保留待修复状态和 `history_repair_error`，供下一个休市窗口重试。
7. 交易分钟的判定是时段窗口加星期规则：06:00 前算前一晚夜盘，周六白天、周日、周日夜盘不算。
   无行情时的超时缺口只检查当前交易日（当日 18:00 前）的分钟，下一交易日从它的首笔行情开始。
   2026-11-21 曾把周六 09:00 记为 ni/ss 等合约的缺口，Market Replay 无法回补，`live_ready` 一直为 false；
   11-22 修复后重启。节假日仍无日历：假期前最后一晚的夜盘时段可能在首根分钟时记为缺口，不影响 `ready`。

### 行情缺口钉钉

`cta-cn-futures-gap-alert@01.timer` 每约 15 秒运行一次独立检查。首次从 CTA
回环 `/state` 补报当前未修复缺口，之后读取环境目录 `logs/worker.error*.log` 中的
`redis_minute_gap` 事件。缺口按交易日、合约和起始分钟去重，同一批缺口合并发送。
本地 `data/redis-gap-alert.sqlite3` 保存读取位置和通知回执。钉钉确认接收后记录已发送，
发送失败保留记录并退避重试。
2026-11-24 起只报**未修复**的缺口。重启后残缺的首分钟、主力行情静默等缺口属于常态，CTA 会在盘中从
Market Replay 回补。脚本记录缺口后不立即发送：CTA `/state` 显示该合约已无缺口（`redis_gap_since_ms`
为空或晚于该缺口）时，静默关闭；10 分钟后仍未修复才发钉钉。`/state` 读不到时，30 分钟后再保守发送。

将 `scripts/cta_cn_futures_gap_alert.py` 安装到 CTA 环境的 `scripts/`，将
`scripts/systemd/cta-cn-futures-gap-alert@.{service,timer}` 安装到用户 systemd 目录。
为服务实例设置 `CTA_GAP_DINGTALK_SEND` 为已部署的 Market Replay
`dingtalk-send` 绝对路径。服务加载该渠道现有的私有环境文件
`~/.config/market_replay/env`。先在 CTA 环境目录运行
`python3 scripts/cta_cn_futures_gap_alert.py --dry-run`，再启用对应实例的 timer。

### 分钟闭合和恢复

网络请求期间 Redis 捕获持续运行；控制器先排空排队的数据和 reset 标记，再对齐。
期货 L2 接收器支持同时运行双订阅。备用连接保留各交易所按接收顺序排列的短窗口，
用交易日、自然日、合约及 SeqNo 查找最后已交付快照，核对字段后仅消费后续记录。
两条连接都断开、缺少重叠或字段冲突时，把行情 reset 传给策略记录缺口。双订阅只在内存保留快照。
分钟由交易所时间戳确认闭合，不能仅因 gRPC 请求耗时、本机时间推进，
就将滞后来源中的残缺分钟当作完整分钟。主力和次主力（market replay
`GetContractSelections(SECONDARY_OI)`，随主力一起获取）都已送出 M 分钟最后一个
快照切片时立即封 M：各合约自身实测的最小快照间隔（SHFE 500ms、CZCE 250ms，
至少观测 20 次后才启用）加上最新快照时间达到 M+60s。切片内行情无变化时交易所
不推送，此时退回主力下一分钟快照确认；拿不到次主力时同样退回。封口后若再收到
该分钟的主力快照，判为来源异常并走 gRPC 追平，不静默丢弃。

**交易所时钟兜底（2026-11-24）。** 接收线程按交易所记录全部合约（含未订阅品种）的最新交易所时间，
每条行情先放入所属品种队列，再推进该时钟；品种循环先读时钟、再排空队列。同一交易所的 Redis 推送是有序的，
所以时钟越过 M+60s+1s 时，主力不会再有 M 分钟的快照到达。此时用已收到的快照封 M（日志 `by:"venue_clock"`），
主力在分钟末的静默不再被判成缺口。
只封主力已有快照的分钟；整分钟都没有快照的仍按缺口处理。时段末尾（午休、收盘）同理：
墙钟超过结束时间 2 秒，且同交易所的行情到达最后 1 秒、5 秒内仍有推送，即可收尾，不再要求主力自己有末秒快照。
起因：当天 j2701 在 09:18:59.335–09:19:06.085 交易所无推送，PF701 在 11:29:58.500 后午休前无推送，
旧的 5 秒超时和末秒规则分别误报缺口，丢掉相邻分钟和所在小时的决策。

2026-11-18 全天与 11-19 上午的 MDL 归档统计：两者都有最后切片的分钟占比，
SN 99–100%、RB 93–97%、SA 54–57%、SM 46–53%（CZCE 次主力与 SM 主力常缺末切片）。

初始化期间，断连、身份冲突、队列丢失或缓冲耗尽会重置接续边界；gRPC 暂时失败
或落后不会清除本地分钟缓存。初始化完成后，普通交易日切换继续使用 Redis、启动时
获取的参数和已经预热的因子窗口。来源 reset 后从完整的 Redis 分钟恢复实时决策，
Market Replay 由休市时段的后台任务处理。主力 REST 查询继续独立运行。

1h 沿用既有研究口径：连续源分钟从片段第一分钟起每 60 根一组，保留时段短尾。
末分钟提前封口时，小时 bar 等到自己的结束时间才推进因子并触发决策。
1min 直接使用源分钟；5min / 15min 按整点时钟边界聚合，首次历史的残缺组丢弃。
真实零成交分钟占用时间槽；休市和源缺行不凭空补分钟。mid 使用行情提供的值；
1h 缺少或无效时使用 close，分钟频率缺少或无效时暂停并保留目标。缺少五档、分桶或适用月份阈值时暂停，不用零值替代。

## gRPC 契约

- `GetHfqMinuteBars`：按产品和归档代分页读取历史。国际铜和锰硅请求 240 天，其余品种
  使用各自配置的历史长度。跨页校验档案代、复权基准日和乘数。
- `GetMinuteBars`：响应 `contract / bars / data_state`，用于 HFQ 归档后的原生分钟接续；
  `include_empty_minutes=true`；分钟暂态数据可反复更新，覆盖查询区间时删除已消失的行。
- `GetSizeThresholds`：按交易所与品种查询，按 `applies_month`（上海自然月）使用产品 P50/P90。
  不在策略侧维护导出器或阈值文件，不默认使用别的月份。
- `GetTradingSessions`：查询该交易所与品种的时段；快照日期必须与分钟数据一致。
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

## 仓位算法与切换

每个品种和频段配置 `position_algorithm`，缺省为 `v0`。01 的 21 个商品期货小时频段使用 `v1`，IC、IM 使用 `v0`，使各规则的 NQ 参数参与开仓判断。

- `v0` 用 `[0, 1]` 分位、逐规则 NQ 和逐规则开平状态判断方向。对各规则的 `-1/0/+1` 求和取符号，
  正数为多一手，负数为空一手，零为空仓。
- `v1` 将方向修正后的分位映射为 `2q-1`，对同一根已闭合策略 bar 的所有规则等权平均。
  重复公式按规则 ID 分别参与平均。开多 `> 0.5`、平多 `< -0.2`、开空 `< -0.5`、平空 `> 0.2`。
  开仓用该频段 `entry` 的统一 NQ 开关和阈值。

配置面板切换到 `v1` 时填入上述阈值，切回 `v0` 时填入对应分位 `0.75 / 0.40 / 0.25 / 0.60`（开多、平多、开空、平空）。
直接编辑 JSON 时同时设置 `entry` 与 `exits` 中的四个因子阈值。未配置算法的旧文件沿用原 `v0` 阈值。
保存配置和历史预热保留上一笔目标与决策时钟，下一根有效实时 bar 采用新算法。
切到 `v1` 时从原目标方向继续判断。切回 `v0` 时各规则从空状态开始判断。
旧账本若保存了多手目标，首根有效实时 bar 会将其更新为新的单手目标。

## 配置面板与状态

锡的公网配置面板为 `http://124.223.189.200/cta-cn-futures/`。新增品种使用本机面板
`http://127.0.0.1:19401/products/<小写品种>/`。每个面板按频率配置因子、方向、NQ、开关、仓位算法及开平阈值。
GET `/api/config` 无需授权；POST `/api/preview`、`/api/config` 需要环境私有凭据。
配置读取不包含连接地址、路径或凭据。公网继续使用现有反向隧道，云端 Nginx
配置为 `config/cta_cn_futures_public.nginx.conf`，通过环境本地的
`scripts/cta_cn_futures_public.sh` 管理。

保存前校验 revision、字段和实际已观测小时上的因子就绪状态。保存重建公式窗口，
不改事实仓位，不补历史目标。关闭策略停止新决策，保留旧目标。外部直接改配置
需要经环境脚本重启。

内部只读接口仍为 `127.0.0.1:19401/state` 和 `/targets`，不通过公网面板转发。
`/state` 提供因子就绪、实时就绪、错误、`initialization_complete` 和 `restart_required`；
初始化期间额外提供 gRPC 水位、固定首分钟和缓存分钟数。`frequencies` 按频率返回
独立因子状态和目标，原顶层 `positions`、`interval` 保留 1h 口径；`/targets` 同时
提供这两种视图。附加品种在 `products` 下各自报告，`strategies` 展示当前品种的策略。
锡的 1h 继续使用 `positions.json / targets.json`，附加品种分别在 `data/products/<品种>/` 下保存；分钟频率分别使用
`positions_<频率>.json / targets_<频率>.json`。单个分钟频率的数据错误只暂停该频率，
保留其事实目标，不阻止正常的其他频率。
消费 `/targets` 必须检查 `factor_ready / live_ready / decision_ms`。换主力后的
首次有效决策同时输出旧合约目标 0 和新合约目标，重复读取不产生增量交易。

进程读取 gRPC 时段后，休市期间状态为 `market_closed`。交易时段内，历史读完后
使用 `waiting_live` 等待新鲜 Redis 输入。因子预热与实时新鲜度共同决定就绪。

## 运维和发布

仅通过环境本地脚本：

```bash
cd /home/fanghaizhou/cta-cn_futures-01
./scripts/cta_cn_futures_process.sh status
./scripts/cta_cn_futures_process.sh preflight --execute
./scripts/cta_cn_futures_process.sh restart --execute
```

省略 `--execute` 只打印计划。preflight 在临时目录使用各频率事实账本的副本，通过国内行情 gRPC
验证历史分钟、交易时段、分桶阈值和启用频率的因子就绪。预检失败时保留现有进程。支持
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

本次实测 gRPC 预热 `sn2612` 10/10、`rb2701` 7/7、`SA701` 9/9、`SM701` 12/12。
锰硅的 `td_mt_043` 使用 360 根原始小时成交量，后续还需归一化与分位预热；
将历史请求扩到 240 天后，该品种形成 805 根小时，原有公式和窗口参数沿用。
四个品种的预检及加载收据由 `data/ops/cta_cn_futures_scope_latest.txt` 指向（不提交到 Git）。
2026-11-18 盘后，四个品种的 gRPC 历史均通过收盘 15:00 的预检。四品种版本已拉起，
当时的运行时已读取全部历史分钟，等待各自首根完整 ZMQ 分钟进行接续。锡的目标账本保留。
Manager 已创建新增品种的 28 个目录策略。创元 CTP 当前只读进程登录超时，
新增品种组的归因登记和发布令牌待当前运行代完成对账。盘后部署收据由
`data/ops/cta_cn_futures_postclose_latest.txt` 指向。
因子表替换的预检、配置备份和加载核验由
`data/ops/cta_cn_futures_user_factors_latest.txt` 指向（不提交到 Git）。
盘后启动先保留已有目标和预热因子，等待下一交易时段的 Redis 行情；
`initialization_complete / live_ready` 以内部接口实际返回为准。

2026-11-20 午休，01 的实时源切换到公司 Redis 二级 feeder。21 个品种的 147 条规则
通过 gRPC 预检，21 份持仓账本校验和一致。CTA 直连 Redis，8811 Viz 经
`market-redis` 穿透，两个进程均收到六所五档订阅确认。实盘进程在内存中处理实时快照。

同日午休，01 启用了双 Redis 订阅和休市回补。13:30 开盘后 21 个品种进入 `live`，
13:32 前后各封出第一根完整 Redis 分钟。切换后首个未验证的 13:30 分钟记为待回补，
因子与实时信号继续推进。上线时 21 份策略配置和 21 份持仓账本的校验和一致。
