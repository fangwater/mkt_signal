# OKX / Binance CTA Spread

最后更新：2026-11-10 04:01 UTC

Spread01已按用户209条清单整体替换，12币、不分券池，1min/5min/15min/1h分别
28/56/18/107条规则。仍是公共账户`okex_binace_cross`（内部`cta_cross_spread_rx01`）
下的OKX/Binance两套Exec；四张策略表均已开启，两侧8条绑定各1份，每规则100U，ctaker执行。
旧仓及IOTA尾仓已全部成交平掉，旧订单、分仓、因子账本和目标历史已归档后清理。
1h预热实际使用本地Tardis原始trades及incremental_book_L2合成输入：11币带L2，
DOGE小时规则仅需成交；12币各768根，共9216根，保留原公式、20档盘口和盘口变化统计。
价差窗口由两所保留Kafka回放建立；实测OKX最早可读消息为2026-11-08 00:10:09 UTC。
线上1h109/109项就绪，1min35/35、5min46/49、15min21/22；其余分钟项继续自然预热。
启用未改变计算代次，不追开历史仓位；从启用后的新封闭bar决策。
清单、数据来源、SHA-256及最终回执见`artifacts/cta_spread01_replacement/`。

2026-11-03 10:35 UTC，OKX 现货规则刷新误覆盖永续乘数的问题及 TRB 错误账本已修复；
两侧 Spread pre-trade 恢复，策略继续按 `ctaker` 执行。刷新观察确认 TRB 持仓口径
稳定，交易所确认 Binance 的 DOT、TRB 和 ETC 对冲仓已成交。详细记录见
`docs/core_allocation.md`。

2026-11-03 09:35 UTC，Spread01 两侧执行算法改为 `ctaker`，覆盖全部8个币种；
Manager 模板和两侧 Redis 配置已核验，两个 pre-trade 均热加载，不需重启。该算法
以市价单执行可下单的目标差额；两侧绑定份数及其他配置保持。09:36 查询到 OKX
DOT 多仓和 TRB 空仓各约100 USDT，Binance 仍有原 IOTA -0.1 尾仓；两侧无挂单。
回执 `artifacts/cta_spread_ctaker_20261103/verified.json`。

2026-11-03 09:29 UTC 已开启 Spread01：`cta-factor-spread` 运行在 CPU18，
`cta-spread01-okx-bn-1min-pos` 推送开启，OKX 和 Binance 两条绑定各 1 份，
每个标的×因子 `max_pos_u=100` USDT。50 项计算和 8 项价差均就绪，Manager 已
确认目标发布；核验时两侧目标均为零。两所执行进程已运行，原 Binance IOTAUSDT
-0.1 尾仓仍保留。启动期间旧计算进程因缺少 Tokio LocalSet 崩溃，关闭推送和绑定后
已使用 `9b343d29` 修复产物重启、重新回放并验证。回执
`artifacts/cta_spread_live_20261103/verified.json`；以下涉及关闭状态和 200 USDT
的描述为先前准备记录。

操作员2026-11-03再次澄清后的当前展示：**一个Manager账户，内部两套Exec**。
公共别名 `okex_binace_cross_1min`，内部账户ID `cta_cross_spread_rx01`。
账户列表、总览、净值及策略选择器只显示这一个账户。此前改成两个普通账户入口是误解，
已恢复单账户映射；两侧名称 `okex_exec_cross01`、`binance_exec_cross01` 只标识执行器。
真实source/目录仍为 `okex_exec_trade01`、`binance_exec_spread01`，历史归属和namespace不改名。

账户页 `/manager/account/?source=cta_cross_spread_rx01` 沿用普通账户布局。
通过交易市场选择器管理OKX/Binance各自费率、风险和执行配置；净值及同名策略盈亏分别FIFO
核算后加总，始终保留公共账户范围。两侧shares及风控不合并。

该账户的“执行监控”入口：
`http://13.115.227.29:4191/manager/exec/?group=cross01`。
标题使用公共别名，直接订阅两套原生WebSocket及snapshot，合成一个账户汇总和一张执行表，
没有iframe或嵌入的Viz页面。共用交易所、策略及品种筛选；按品种保留市场区分，按策略
合并同名策略并可展开各市场明细。账户配置、净值及策略启用链接均回到同一个账户。
权益和名义金额加总，组合杠杆按总名义金额/总权益重算；挂单与待下单金额按绝对值合计，
不因两侧方向相反而抵消。账户仓位按source/市场/品种去重，策略筛选使用策略仓位。
不同市场或品种的数量不强行相加；缺失、断线或生产时间超过5秒的来源不补零，相关汇总显示不可用。
旧snapshot不能覆盖新实时数据；元数据轮询和手动刷新不重建WebSocket。
页面可展开组合策略盈亏，查询公共账户ID后由Manager展开两源并核对范围。
完整可见两侧才显示监控；仍仅lgq可见，prc不能访问两边网关。

Funding统一使用公开预估，两源实际账单采集关闭且记录数0，账户页/策略页隐藏实际采集控件。
OKX公开已结算费率、结算前标记分钟及1m成交K线解析已部署，官方BTC样本核验通过；
详见 `artifacts/cta_spread_data_audit_20261103/`。

策略账本未导入旧trade08订单，没有初始账户/策略仓位、归属快照或执行绑定。
交易和信号发布保持关闭，各自风险参数、订单库及源IP独立。
81857此前观察到的IOTAUSDT -0.1尾仓未平仓、未种入新账本，不能将新账本空仓当作交易所空仓。
旧MM/trade08历史保留，凭据仅在远端环境文件中。

本轮前端 `cross01_unified_viz_20261103T054937Z`，仅原子切换静态文件，公共账户映射、API和配置均不变。
Manager PID3538262仍CPU24，无进程重启；87个受保护进程身份、执行配置、别名、快照及绑定保持。
52项前端测试及生产构建通过。真实浏览器确认一个公共账户、一个汇总区、一张执行表、零iframe，
两侧WebSocket均101，刷新期间连接稳定；桌面/手机布局、组合空账本及权限隔离通过，无运行时错误。
浏览器内临时样例验证反向金额加总、筛选、实时更新、过期和拒绝访问处理，未写入服务器或交易状态；
实际两侧执行snapshot仍为空，执行保持关闭，不将其视为交易所空仓证明。
部署/浏览器回执 `artifacts/cross01_unified_viz_20261103/`；远端审计
`~/crypto_cta_manager/maintenance-backups/cross01_unified_viz_20261103T054937Z/`。
以下信号实现及历史准备记录保留，不表示当前交易已启用。

1min 仓位表 `cta-spread-okx-bn-1min-pos`，由 `cta-factor-spread` 进程维护。OKX 仍通过已有的
原生统一账户 Exec 执行，部署 venue 为 `okex-futures`，本策略只发布该账户的
USDT 永续目标。Binance 使用独立的 `binance-futures` Exec 合约账户。两个账户
分别维护原有账户模式、namespace、执行账本和持久化数据。

因子和 NQ 沿用 Binance 永续的 1min 计算；价差把旧 Spread 的现货价格替换成
OKX 永续价格：`(okex_mid - binance_mid) / okex_mid`。同一分钟末盘口配对，
2880 分钟槽、至少 1440 个有效价差样本，按标的独立计算分位。入场使用前一分钟
价差分位：开多要求严格低于 0.30，开空要求严格高于 0.70；等于边界不入场。
NQ 取该币种 Binance 永续 mid，高低点和变化值分位窗口均为 120 根有效 bar，
至少 60 个有效变化值，整个结果滞后 1 根 1min bar。默认开多买入变化不低于
自身 Q50，开空卖出变化不高于自身 Q50。因子门槛、反转及退出规则与原 Spread 一致。
前一分钟价差为空、行情缺失
或样本未就绪时阻止入场，因子退出仍可执行。
Kafka 的两路 topic 是 `binance-futures` 和 `okex-swap`；不以 `okex-futures`
作为 Kafka topic。这份实现按两路 Kafka 可用的前提准备，尚未进行实盘验收。

方向在 OKX 永续开仓。若开多得到 +2 ETH，目标为 OKX +2 / Binance 0；退出后
目标为 OKX +2 / Binance -2；反向入场得到 -3 ETH 时一次替换为 OKX -3 /
Binance 0。数量均为基础资产数量，由现有 Exec 规则处理合约张数。持续同向信号
保持冻结数量，不累积库存；删除币种会持续发布两腿显式 clear。

每个 symbol × factor 保留独立子仓位和单份 `max_pos_u`；发布时按币种、venue
汇总为 `cta-spread-okx-bn-1min-pos` 一份策略，一次发布完整两腿：

```json
{
  "strategy_name": "cta-spread-okx-bn-1min-pos",
  "targets": {
    "okex-futures:ETHUSDT": {"qty": 2, "signal": 0},
    "binance-futures:ETHUSDT": {"qty": -2, "signal": 0}
  }
}
```

Manager 将该策略分别绑定至 OKX / Binance 账户，各账户只接收属于自己的腿。
账户份数应配对配置；单独停止一腿会改变跨所净敞口。两个 REST/Exec 目标发布
独立完成并重试，不提供交易所之间的原子执行保证。Manager 在目标归档中冻结
各账户的实际投影与份数，NAV、FIFO、Funding 和交易费用仍逐账户逐市场计算。

当前入口为 `cta-factor-spread`，默认配置 `config/cta_factor_spread.toml`。
组合放在 `config/cta_factor_spread/`，正式清单为
`config/cta_factor_spread/okex-futures_binance-futures_1min.json`，来自用户提供的
TRB、DOT、ADA、NEAR、ATOM、DOGE、ETC、CRV 共 8 个标的、34 条带符号的因子规则。
普通因子取 Binance 永续，全部规则默认 NQ 0.50；程序自动补齐 8 项 NQ 和
8 项 OKX/Binance 价差，合并为 50 项计算。原 BTC 示例已从活动清单移除。
DOT、DOGE 的 `baseline_035` 与 CRV 的 `baseline_047` 依赖真实大小单分类；
须在主 TOML 的 `amount_thresholds` 配置对应 Binance 永续币对的成交额阈值。
当前源配置未提供这些阈值，因此这三条规则保持未就绪，其余规则正常预热计算。
发布保持关闭，`signal.max_pos_u=200` 保留现有单份 USDT 配置；`signal.spread` 配置
价差过滤阈值。普通因子和 NQ 使用 Binance 永续，价差使用 OKX/Binance 同分钟
末盘口；因子、前一分钟价差与仓位决策在同一进程内衔接。仓位状态位于
`data/cta_factor_spread/positions/cta-spread-okx-bn-1min-pos.json`，不再维护第二份
Redis 因子绑定表或启动 `cta_cross_spread_signal`。旧独立入口已移出构建清单，
后面的部署记录描述历史准备状态；本轮没有部署或迁移线上仓位。

观察和核算使用 Manager 的账户 NAV；原 Binance 现货图表不适用于这个策略。

Manager以一个公共账户选择两条执行源。API接收 `cta_cross_spread_rx01` 后展开为
`okex_exec_trade01`、`binance_exec_spread01`，逐source/venue核算FIFO、费用和预估Funding后
再加总账户与同名策略PnL；原始订单、RocksDB记录键及历史快照不改写，不跨所互相平仓。
账户内将两套执行流整合成一张执行表，配置/风控分市场管理；不能因有两个执行源而显示为两个用户账户。

仅准备文件的入口是 `scripts/deploy_cta_cross_spread.sh`，默认 dry-run，
遵守现有本地构建和发布约束。运行环境内使用
`scripts/start_cta_cross_spread.sh --execute` /
`scripts/stop_cta_cross_spread.sh --execute`。环境需要配置独立
`IPC_NAMESPACE`、Manager gateway 和发布凭据；策略名由市场对和频段确定。
模式实现阶段未创建线上策略、绑定账户、部署或启动任何进程。

2026-11-02 10:26 UTC，JP已准备第一套配置框架：
`~/okex-binance-cta-spread-rx01`，配置源 `config/cta_cross_spread_rx01/`。
OKX开仓腿为新原生统一Exec `okex_exec_trade01`，仅复制旧 `okex_mm_alpha`
三项凭据，独立namespace及订单库；Binance平仓腿复用原生STANDARD USD-M
`binance_exec_trade08` / `lgq_ltp_81857`，保留原配置与历史。
新环境仅含配置/脚本，尚未安装二进制；全部进程停止，enabled=false，
正式币种/因子清单留空，200 USDT仅保留模板默认，尚非正式仓位规模。
预留OKX Viz/Config 10049/18169、新策略Config 19184，尚无监听或网关路由。

Manager源码支持两所目标投影，Manager两项和信号两项Rust测试及两项Python测试通过。
JP在线Manager仍为旧程序，未发布本地工作区并行进行的NAV/权限修改；线上配置
与进程保持。候选配置在
`~/crypto_cta_manager/maintenance-backups/cta_cross_spread_prepare_20261102/cta-manager.toml.candidate`，
新OKX及原Binance08 source均关闭，账户可见范围仅lgq；没有创建策略或绑定。

10:23 UTC签名只读查询确认OKX全品种持仓、已查挂单及负债为零；Binance仍有
IOTAUSDT -0.1，约0.004 USDT，各类已查挂单为零。本轮未提交平仓或其他交易请求，
不能把该账户记为零持仓。正式启用前须处理尾仓、发布支持跨所的Manager及Exec/
信号产物、核对旧Exec账本、准备正式清单和配对绑定，并验证双腿规则、风险配置
及带账户鉴权的网关。只读查询与配置安装回执见
`artifacts/cta_cross_spread_prepare_20261102/`。
