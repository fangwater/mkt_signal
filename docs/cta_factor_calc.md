# CTA 因子计算与仓位表

最后更新：2026-11-09（1h Tardis 原生输入历史包及 Kafka 衔接）

## 券池维度

Prep 和 Spread 现在按 `strategy_group × symbol_group × 市场 × interval` 维护独立策略。
券池使用英文名称 `top-5`、`top-20`、`last-20`；它们是显式成员清单，不会自动按市值排名。
同一标的可以出现在多个券池，各券池分别维护规则、开关、额度、开平仓阈值、仓位账本和
Manager 归属。比如 `cta-top-5-prep02-bn-1min-pos` 与
`cta-top-20-prep02-bn-1min-pos` 是两个策略，公共因子计算仍只进行一次。
Spread 对应 `cta-top-5-spread01-okx-bn-1min-pos`。

每个计算进程的组合目录内新增 `symbol_groups.json`，直接接受组名到完整 USDT 币对数组的 JSON：

```json
{
  "top-5": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"],
  "top-20": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT", "ETCUSDT", "DOTUSDT", "BCHUSDT", "FILUSDT", "NEARUSDT", "AXSUSDT", "ATOMUSDT", "AAVEUSDT", "TRXUSDT", "UNIUSDT"],
  "last-20": ["1INCHUSDT", "EGLDUSDT", "YFIUSDT", "KNCUSDT", "RSRUSDT", "KAVAUSDT", "ZENUSDT", "QTUMUSDT", "BELUSDT", "RLCUSDT", "DASHUSDT", "KSMUSDT", "ZRXUSDT", "IOTAUSDT", "ONTUSDT", "BANDUSDT", "SKLUSDT", "IOSTUSDT", "BATUSDT"]
}
```

组名以小写字母开头，长度 1–32，仅含小写字母、数字和 `-`；`all` 保留给未限定券池的旧配置。
组内不接受重复标的，组间可以重叠。组合顶层设置 `"symbol_group":"top-5"`，
规则和逐币阈值覆盖必须属于该券池；不属于券池、引用未知组或删除仍被引用的组都会拒绝保存。
新增券池不会自动生成交易规则。原来没有 `symbol_group` 的 Prep01 / Spread01 配置
在页面标为 `all`，保留原策略名与账本归属。已有带 `signal` 的组合不允许原地修改券池。

页面在策略组旁增加券池选择，可分别切换频段；未配置的券池频段以空规则和关闭开关开始，
填写规则后保存。折叠的“券池定义 JSON”支持校验和保存。定义、规则都通过整份清单校验、
共享文件锁和内容摘要处理并发冲突；Prep 与 Spread 分别保存定义，各自热加载。
Delist 的只读清单和备份也包含券池定义。

2026-11-09 按用户要求，RX03 / Prep02 的整个1min频段已下架：三个券池共58条规则移除，
三条 Manager 绑定删除，原策略目标清零并执行完成。保留六条5min/15min绑定及41条规则；
RX01 / Prep01 1min 保持。22条低于交易所最小下单量的真实分仓尾量约35.33U仍保留历史归属，
无活动委托或批次。仓库内三份 Prep02 1min 模板同步移除；1h 参数按用户标准清单独立维护。
详见 [RX03 1min 下架回执](../artifacts/cta_rx03_1min_retirement/README.md)。

2026-11-04 用户清单已写入 Prep02 模板并部署到 JP。20 项输入中两个空标的项不展开，
跨券池重复规则分别保留；12:11 UTC按用户要求删除ICX的3条规则后，
现为99条规则、9张表（3券池 × 3频率）。`last-20`保留名称，成员现为19个：

| 券池 | 1min 规则 | 5min 规则 | 15min 规则 |
|---|---:|---:|---:|
| `top-5` | 5 | 2 | 7 |
| `top-20` | 27 | 0（保留空策略） | 10 |
| `last-20` | 26 | 5 | 17 |

文件名示例 `top-5_prep02_binance-futures_1min.json`。所有模板发布开关为 false，
金额和阈值沿用原 Prep02 同频段配置。原三张未分券池的 Prep02 模板已替换；
Prep01三张及Spread01一张保留，总计13张活动表，Prep现为404项共享计算。
线上保留发布前实际参数：九个Prep02均开启、每规则100U、RX03 shares=1及原grid模板；
Prep01 / Spread01参数保留。09:47:31.222 UTC完成存量分仓，10:45 UTC核验两worker
回放与发布确认完成、执行端position_ready=true。历史盈亏不重写，归属守恒校验通过。
12:11 UTC已从Prep两份规则与Prep/Spread券池定义清理ICX，计算408→404，代次和进程保持；
ICX实际仓位与挂单为0。部分Prep窗口仍未就绪，Spread50项均就绪。
RX03九条grid/shares=1绑定正常，真实新策略成交已确认，全部分仓总量与交易所一致，
无未完成执行行、活动批次或委托。见 [ICX清理与RX03核验](../artifacts/cta_icx_cleanup_20261104/README.md)。
部署回执见 [九策略上线记录](../artifacts/cta_symbol_groups_20261104/README.md)。
用户已确认自动迁移口径：唯一匹配的旧规则直接归组；跨券池重复的旧规则等分数量和入场金额，
总仓不变。历史盈亏保留在旧策略，切换后按九个新策略统计。空策略保留身份和零目标，
不会自动补齐规则。`cta-symbol-group-migrate` 会准备信号、Exec 和 Manager 的迁移包，
详见 [券池迁移](cta_symbol_group_migration.md)。工具本身不执行线上切换。

CLI 替换券池定义默认仅预览；加 `--execute` 才写入当前进程的组合目录：

```bash
cta-prep-binance-futures --config config/cta_prep_binance_futures.toml --put-symbol-groups /path/to/groups.json
cta-factor-spread --config config/cta_factor_spread.toml --put-symbol-groups /path/to/groups.json
```

## 按频段维护仓位表

因子计算继续共享输入与预热状态，仓位和 Manager 归属按 `strategy_group`、`symbol_group`、市场与频段独立。
同一进程可同时维护 `prep01` / `prep02` 或 `spread01` / `spread02`，每组有自己的
因子规则、NQ 条件、开平仓阈值、USDT 额度及各频段 `signal.enabled`。例如：

| 仓位策略名 | 频率 | 单份 USDT 配置 |
|---|---|---|
| `cta-prep01-bn-1min-pos` | 1min | `binance-futures_1min.json.signal.max_pos_u` |
| `cta-prep01-bn-5min-pos` | 5min | `binance-futures_5min.json.signal.max_pos_u` |
| `cta-prep01-bn-15min-pos` | 15min | `binance-futures_15min.json.signal.max_pos_u` |
| `cta-spread01-okx-bn-1min-pos` | 1min | `cta_factor_spread/okex-futures_binance-futures_1min.json.signal.max_pos_u` |

组合文件名必须与 `name` 一致；用 `prep02_binance-futures_1min` 等名字区分同频段配置。
每份 JSON 显式填写 `strategy_group`（1–32 位小写字母、数字或下划线，以字母开头）。
Manager 策略名为 `cta-<strategy_group>-<市场缩写>-<频段>-pos`，Spread 增加参照市场缩写。
同一策略组/券池/市场/频段只允许一份带 `signal` 的组合；不同组可同时配置同市场同频段。
同一组可包含 1min / 5min / 15min / 1h 四个 Prep 频段，Spread 支持 1min / 1h。
两个计算进程各持有自己的单实例运行锁。
共享完整因子 ID 的定义必须一致；各组可以有不同符号权重、NQ 门槛及交易参数。

每个 symbol × factor / NQ 规则仍独立开平仓、冻结入场 qty、持久化和观察；
现有 v0/v1 算法选择保持。发布时将本频段所有子仓位按币种相加，向 Manager
一次提交该策略的完整绝对目标表。不同频段分别发布，不在信号层互相抵消。
Spread 的方向腿和对冲腿按各自 venue 分别求和，保留已退出规则的两腿库存。
删除最后一个 Prep 规则仍显式 clear；从 Spread 整体移除币种仍对两腿发布 taker clear。
某因子退出只撤销自己的贡献，不会清掉同币种其他因子的目标。

USDT 额度仍按当前单份口径使用：v0 每条规则一份，v1 每个标的一份，
Spread 每个 symbol × factor 一份；不是整个频段共享一个总金额上限。
修改某组某频段的额度只影响该表之后的新开仓。Manager 按组及频段的独立策略身份
配置跟随份数、观察成交、PnL 和资金费；内部子仓位明细继续留在信号状态和日志。
停用或删除一组不修改其他组的仓位与配置；只要仍有一组引用，因子计算就继续。
已有仓位组合不允许原地修改组、市场或频段，须建立新的组合并明确处理原归属。

仓位表由因子进程内的 `cta_factor_calc::positions` 模块维护，不再启动独立
`cta-signal` / `cta_*_signal` 进程。Prep 进程持有各策略组配置的四个频段表，Spread 进程
持有各策略组的 OKX–Binance 1min / 1h 表。仓位直接读取当前有效计算代次的结果，无需通过
IPC 订阅自己的因子流；原因子 IPC 仍可供其他只读消费者使用。

每张表在 `<history_dir>/positions/<仓位表名>.json` 持久化完整组合、子仓位、
最后决策时间及退役状态；状态先原子写入并 fsync，再向 Manager 提交完整目标。
`<history_dir>/state.json.position_tables` 包含当前仓位表、目标及投递状态，
`positions_error` 单独报告仓位故障。配置页只维护参数和因子 JSON，不展示仓位表。
备份会包含这些持久化表；子仓位变更记录写入同目录的 `.events.jsonl`，Manager
成功投递记录写入 `.deliveries.jsonl`。仓位决策和目标提交每秒检查一次。

首次历史预热以及候选回放都不改仓；新代次接管时更新决策边界，只处理之后的
新 bar。控制参数即时按统一组合读取，新增或改定义的因子在当前有效代次尚不
包含相同定义时暂停该表决策与发布。暂停发布保留子仓位，不生成模拟仓位。
移除 `signal` 或删除组合时以原表名发布清零目标，持久化后持续重试，直到
所有 Manager 确认后才完成退役；重启恢复尚未完成的清零任务。
Spread 因子与 NQ 使用同一根最新封闭 bar；NQ 不额外滞后，入场价差仍使用上一根 bar
（1min 取前一分钟，1h 取前一小时）。上一根价差来自同一个
计算代次保存的前值。依赖缺失最多等待 5 秒，之后仍允许因子退出。

切换部署须先停旧独立信号进程，核对并明确迁移旧因子策略的账本、目标和
Manager 绑定，再启用新表；不会自动导入旧目录、叠加发布或重写历史归属。
2026-11-03 已按用户授权完成 RX01 USDT 永续 taker 清仓与 91 个旧因子绑定移除。
旧 Prep/Spread 信号、因子和配置服务已停止；新单组产物曾安装并启动，随后按用户
“先开发再上线”的纠正于 04:25 UTC 停止两个新计算进程。四个开关均关闭、执行绑定为 0，
Exec 下单进程保持停止。用户随后确认将 Prep02 的 `TD_MI_040` 修正为 `TD_MT_040` 并授权部署。
05:08 UTC，多组版本 `fa63d6e4` 已发布：Prep01/Prep02 各三个频段，408 项共享计算，
Prep 进程 CPU17 运行并预热，七张表开关均关闭、无绑定；Spread 产物同步升级但计算不启动。
四个未启用旧表的空账本已备份，Manager 旧定义替换为七个带组名的新定义；无历史仓位迁移。
81 个受保护进程保持，页面预览与权限边界通过，见 `artifacts/cta_prep02_deploy_20261103/`。

`cta_factor_calc` 是公共计算核心，提供两个独立运行入口：

- `cta-prep-binance-futures`：默认读取 `config/cta_prep_binance_futures.toml`。
- `cta-factor-spread`：默认读取 `config/cta_factor_spread.toml`。

`cta-prep-binance-futures` 统筹下列三份频段配置的 Binance 永续因子需求：

| 组合名称 | 频率 | 普通因子条目 | NQ 条目 |
|---|---|---:|---:|
| `binance-futures_15min` | 15min | 145 | 35 |
| `binance-futures_1min` | 1min | 45 | 16 |
| `binance-futures_5min` | 5min | 48 | 15 |

Prep01 三份配置合并为 304 个计算条目；加入 Prep02 三个频段后合并为 408 项。
仍仅订阅一份 `binance-futures` Kafka 流。
三份 JSON 分别从原 RX01/15min、RX03/1min、RX04/5min 配置整理，因子行和
信号参数保持原值。RX 编号不再作为组合或策略身份；`rx01` 仅为账户别名。
初始清单从各自的 `cta_perp_symbol_factors.json` 和策略规则导入；现在以统一四列组合 JSON 为
唯一配置源，NQ 依赖由规则推导。15min 配置的
SKL × factor_170 存在两条不同 NQ 条件的交易规则，因子只计算一次；原策略的
146 条规则保留，不能把合并计算误当作合并交易持仓。仓库内模板的
`enabled=false` 不代表线上策略已停；运行关系按用户给定的现状维护。

两个入口分别维护组合清单、进程生命周期和输出流；Spread 同样支持普通单市场
因子、NQ、跨市场 spread。两个入口各自独立回放所需 Kafka；
同一进程内被多份组合引用的因子只算一次，两个独立进程的重叠条目各自维护状态。
不再按 RX 编号或 1m/5m/15m 启动不同的因子程序。因子进程兼任仓位表维护和
Manager 目标发布；交易账户绑定、份数与订单执行仍由下游 Manager/Exec 负责。

## Prep02 券池拆分前的用户清单（2026-11-03）

2026-11-03 用户提供 18 组规则，按标的展开 102 条，移除 3 条完全重复规则后为 99 条。
频率 `1m` / `5m` / `15m` 分别规范为 `1min` / `5min` / `15min`，标的补 `USDT`。
相同因子但不同 NQ 条件保留为独立规则，例如 DOGE 1min 的 baseline_141。

| 频率 | 标的数 | 规则数 | 单份 USDT | 配置状态 |
|---|---:|---:|---:|---|
| 1min | 37 | 59 | 100 | `prep02_binance-futures_1min.json`，发布关闭 |
| 5min | 6 | 7 | 200 | `prep02_binance-futures_5min.json`，发布关闭 |
| 15min | 24 | 33 | 200 | `prep02_binance-futures_15min.json`，发布关闭 |

额度及开平仓参数暂沿用 Prep01 同频段模板，均保留各因子独立持仓。已部署，发布保持关闭。
研究库 `final_factor_pool_update20260817.py:4286` 定义 `TD_MT_040` 为最近 10 根
bar 的 `high-close` 之和，与 Rust 实现一致。用户确认将 SOL/DOGE 15min 的两条
`TD_MI_040` 改为 `TD_MT_040`，99 条规则均通过计算器计划校验。
原始输入、去重及名称修正回执在 `artifacts/cta_prep02_config_20261103/`。

## Manager 线上仓位策略目录（2026-11-03 快照）

2026-11-03 完成目录整理后，Manager 全局保留 13 个仓位策略：

| 类别 | 数量 | 策略 |
|---|---:|---|
| Prep01 | 3 | `cta-prep01-bn-{1min,5min,15min}-pos` |
| Prep02 | 3 | `cta-prep02-bn-{1min,5min,15min}-pos` |
| Spread01 | 1 | `cta-spread01-okx-bn-1min-pos` |
| PRC RBF | 3 | `rbf_big`、`rbf_mid_1min`、`rbf_small` |
| PRC funding | 1 | `funding` |
| PRC CTA | 2 | `CTA_TOP_V1`、`CTA_TOP_V2` |

七个新策略已注册为零目标，发布开关为 false，执行绑定为 0；不开交易阶段不会实时更新仓位。
229 个旧因子目录定义及 virtual02/virtual03 两个无人跟随的旧模板已先备份再移除，
PRC 的真实账户绑定、权限、令牌和 virtual01 保留。没有发送目标更新或订单。
完整私有备份在远端，脱敏回执见 `artifacts/cta_catalog_cleanup_20261103/verified.json`。

## 统一配置入口和 JSON

统一导航入口为 `/delist/`，固定提供 CTA Prep 和 CTA Spread 两个配置链接。
链接通过 `?worker=cta-prep-binance-futures` / `?worker=cta-factor-spread` 选择清单；
配置页也提供“返回 Delist”。Spread 的便捷入口 `/cta/cta-factor-spread/config/`
跳转到同一编辑器的 Spread 清单，保留各自独立的计算与回放状态。

新的页面路径为 `/cta/cta-prep-binance-futures/config/`，由独立
`cta-factor-config` 提供（默认仅监听 `127.0.0.1:19220`）。这是源码中准备的
入口。页面沿用旧 CTA Perp 配置中心的深色样式，先选择 Prep/Spread、策略组与券池，
再用 1min/5min/15min/1h 标签切换当前频段；Spread 展示 1min / 1h。每个频段独立维护
开关、USDT 金额、开平仓阈值和因子 JSON；页面没有仓位表、计算窗口参数、逐行编辑器
或其他因子输入格式。配置服务器只写组合 JSON，不直接发布持仓或订单。

配置预览、保存及 worker 的 `--put-combination` / `--import-symbol-factors`
会先查询 Binance USD-M 完整合约目录，仅保留 `status=TRADING` 的 Binance
因子来源及交易腿标的；`SETTLING`、暂停、未上市和目录已移除的标的自动剔除。
页面及命令行的 `excluded_symbols` 列出标的、原状态和删除的规则数，连同失去引用的
NQ、参数和该标的阈值覆盖一起清理。其他市场来源的独立计算规则不受影响。
目录成功结果在配置服务进程内复用最多 60 秒；请求或目录解析失败时编辑报错，
不把失败当作空目录保存。此过滤只发生在显式配置编辑期间，不在因子运行层检查
小时数据完整性，也不在读取配置或运行时自动删除现有规则。

旧策略的可编辑 Config 路由在新网关模板中直接返回 410；历史信号面板保持
原身份。新配置不再依赖各策略的 `cta_perp.json`、磁盘 symbol-factor 表或
`<env>:cta_symbol_factors` Redis 键。

页面唯一的因子输入为四列规则数组：`标的` 是字符串，`nq` 使用小写字段名：

```json
[
  {"标的":"1INCH","nq":"0.50","符号":-1,"因子":"TP_VPI_005"},
  {"标的":"1INCH","nq":"关闭","符号":1,"因子":"baseline_100"}
]
```

频率由页面选中的 1min/5min/15min/1h 统一指定，JSON 不包含频率字段；`标的` 数组、
大写 `NQ` 或其他字段都会被拒绝。每条规则单独配置该标的的 NQ 和因子方向。
完全重复的规则去重；不同 NQ 条件继续独立保留。页面读写时保留未修改规则的顺序、
Spread 因子来源市场以及既有内部计算参数和阈值覆盖。校验预览不写盘；保存只写当前频段，
并发冲突保留草稿。新界面不会读取或展示仓位明细。

以下是进程内部持久化格式，不是页面提供的另一种输入方式。

每份组合一份 JSON，文件名与 `name` 相同。沿用最新的四列因子行，仅在组合层
补充 `strategy_group`、`venue`、`interval`。例如：

```json
{
  "name": "binance-futures_15min",
  "strategy_group": "prep01",
  "venue": "binance-futures",
  "interval": "15min",
  "factor_rules": [
    {"标的": "SKLUSDT", "nq": "关闭", "符号": -1, "因子": "factor_170"},
    {"标的": "SKLUSDT", "nq": "0.50", "符号": -1, "因子": "factor_170"}
  ],
  "signal": {
    "enabled": false,
    "max_pos_u": 200,
    "position_algorithm": "v0",
    "symbol_overrides": {},
    "entry": {
      "factor_long_quantile": 0.9,
      "factor_short_quantile": 0.1,
      "nq_change_enabled": true,
      "nq_long_quantile": 0.5,
      "nq_short_quantile": 0.5
    },
    "execution": {
      "factor_exit_quantile_long": 0.3,
      "factor_exit_quantile_short": 0.7
    }
  }
}
```

单 venue 可用 `binance-futures`、`okex-futures`、`binance-margin`；内部标的
使用 `BTCUSDT` 或 `BTC`。频率为 `1min`、`5min`、`15min`、`1h`。程序自动生成
`binance-futures-btcusdt-1min-factor_036` 这样的计算 ID；NQ 不为“关闭”时，
自动加入相同市场、币种、频率的 NQ 依赖，不再手工维护第二份因子字典。
同一计算可被不同规则和组合引用。完全相同的因子/符号/NQ 行会被拒绝；同因子
的不同 NQ 规则保留各自的策略持仓。零权重行不创建计算。

跨市场 spread 行使用 `因子=spread` 和 `reference_venue`。同一组合需要另一
市场或频率时，可在具体行显式覆盖 `venue`、`interval`；普通单 venue 行仍只有
原四列。`config/cta_factor_spread/okex-futures_binance-futures_1min.json` 为正式 Spread 清单：
TRB、DOT、ADA、NEAR、ATOM、DOGE、ETC、CRV，共 34 条普通因子规则，符号沿用用户提供值。
默认 NQ 0.50，自动补齐各标的的 NQ 和价差，合并为 50 项计算；发布保持关闭。
`cta-prep-binance-futures` 只接受 Binance 永续输入；OKX 和混合市场计算由独立
`cta-factor-spread` 承担，可用同一个配置服务程序的 `--spread --config ...`
入口维护。也可在主配置服务增加 `--spread-config /path/cta_factor_spread.toml`，
在同一页面切换两个独立计算进程；环境脚本使用 `CTA_SPREAD_FACTOR_CONFIG`。
两份目录、回放状态和并发保存摘要保持独立。Prep 仓位表仅支持 Binance 永续；
Spread 仓位表支持 OKX–Binance 永续 1min / 1h、v0，并要求显式填写
`signal.spread.long_quantile` 与 `short_quantile`。普通因子行的 `venue` 指向
`binance-futures`，组合的 `venue` / `reference_venue` 分别为 OKX / Binance；
程序自动补齐每个标的的 spread 依赖，无需手工重复添加计算行。

`signal` 可省略，表示只维护因子计算。
已有组合移除 `signal` 或删除组合时，信号撤销完整目标，等待所有 Manager
确认后将该表标记为已退役；计算进程和其他频段继续运行。
保留 `signal` 而设置 `enabled=false` 仍是暂停发布。
`v0` 按因子规则独立持仓，`v1` 对每个
标的的有符号因子值等权取均值。`v1` 要求所有行使用共同 NQ 条件。
NQ 在 1min、5min、15min、1h 使用同一套包含边界的判断，不再配置 `nq_mode`。
默认开多要求买入变化不低于自身 Q50，开空要求卖出变化不高于自身 Q50；
Q50 按变化值与线性中位数比较，避免相等值被排名中的重复样本误拒绝。
行内显式的其他 NQ 条件及“关闭”保留，其他分位按排名包含边界判断。
`max_pos_u` 的含义分别为每条因子规则、每个标的的单份 USDT
额度。现有非零持仓仍会阻止信号在运行中切换持仓算法；市场/频率需创建新组合。

`parameters` 仍可按完整计算 ID 固定窗口定义。默认普通因子归一化窗口为 1 天、
分位窗口为 2 天，最少样本随频率换算；同 ID 的定义冲突会拒绝保存。
Prep 和 Spread 的所有频段普通因子共用当前窗口全量截断算法：每次计算当前根时，
先对包含当前根的原始窗口计算均值和样本标准差（`ddof=1`），再用同一组
`均值 ± 3 × 标准差` 边界重新截断窗口内全部有效原始值，最后对这批截断值
重算均值、样本标准差和当前根的 z-score。窗口保存原始值，不保存各根当时的
截断值；已经输出的历史 z-score 不回写。零方差仍输出 0，无法计算时仍仅向前填充。
本次算法调整保留已有窗口、缺失值和交易规则；NQ 与原始价差不经过普通因子标准化。

1h 使用 Kafka period 数据时间驱动。`PeriodMessage.ts` 是
已完成 period 的结束时间，`post_ts` 是发送时间；精确落在结束边界的事件属于
正在结束的 period。小时聚合须沿用这套边界归属和完成进度，以 period 数据
推进收线与决策，本机时钟到整点或消息抵达时间均不能单独触发收线。
1h 直接聚合 period 中的盘口和成交，在 `period.ts` 越过小时结束时间时收线，
边界归属为 `(t-1h,t]`，精确整点的事件属于正在结束的小时。因子层不检查小时内
数据是否齐全，不要求收足 60 分钟或足量 period，不丢弃首次不足一小时的输入，
也不要求末分钟有报价；使用该小时实际收到的最后一份盘口 mid。
完全没有输入的小时不补造，也不推进因子、标准化、交易分位或 NQ 的样本窗口。
原因子公式的 N 根参数保持原值，每根对应一个实际聚合出的小时 bar。

1h 的默认参数为：

| 层 | 窗口 | 最少有效样本 | 口径 |
|---|---:|---:|---|
| 原始因子标准化 | 24 根 | 16 根 | 当前窗口统一 ±3σ 截断后重算 z |
| 标准化因子交易分位 | 48 根 | 24 根 | `count(z ≤ 当前 z) / count(有效 z)`，含当前根 |
| NQ 极值 / 变化值分位 | 120 根 / 120 根 | 60 个有效变化值 | 历史 close 补充，Kafka 部分使用封闭 bar 的 mid，无额外滞后 |

1h NQ 是历史价格来源的特例。Kafka 只保留约两天时，启动及新计算代次先从 Binance
USDT 永续公共 `GET /fapi/v1/klines?interval=1h` 读取更早的已闭合小时 close。
默认读取 `120 + 120 - 1 = 239` 根，使最终保留的 120 个变化值各自都有完整的
120 根极值回看；60 个有效变化值的就绪门槛不变。自定义窗口按
`lookback + rank_window - 1` 计算请求根数。

补充截止点由第一条 Binance Kafka `PeriodMessage.ts` 确定：只使用其所属小时开始
以前、含开始边界的闭合 K 线。边界上的 Kafka 事件属于正在结束的小时，因此如果首个
period 恰在整点结束，种子截止到前一个整点。先灌入更早的 close，再回放 Kafka mid，
重叠小时不重复计数，不读取未收盘或相对于回放起点的未来 K 线。
不强制 K 线数量或连续覆盖；新上市标的按实际返回的有效历史预热。

下载在后台异步执行，候选代次等待种子期间，已有代次与仓位表仍可继续运行。
只预热 1h Binance NQ 的价格状态，普通因子的原始输入、标准化与价差窗口继续从 Kafka
计算；种子不生成历史持仓或发布历史因子点。其余频段仍使用 Kafka mid。
逐币补充根数、截止点和错误写入 `state.json.hourly_nq_warmup`，完整种子保存为
`generations/<代次>/hourly_nq_seed.json`，明确标记来源为小时 K 线 close。
网络或响应错误保留可用 Kafka 回放并显示补充错误，不伪造价格，也不降低 NQ 就绪门槛。
每个代次每币只做一轮补充；429/418 后停止本轮后续请求，不立即重试。
公共请求基址及限速策略见 [公共 API 清单](public_api_cache.md)。

交易分位不足 24 个有效 z 或当前原始因子无效时输出未知并保持仓位。1h 的开多 / 平多
默认阈值为严格高于 0.90 / 严格低于 0.30；开空 / 平空为严格低于 0.10 / 严格高于 0.70。
同一根可以先退出再反向开仓；NQ、Spread 价差或报价不允许新开仓时，仍完成因子的退出。
因子方向由配置行的 `符号: +1/-1` 决定：正号使用 `q`，负号使用 `1-q`。
NQ 0.50 的线性中位数比较、0.20/0.80 的包含边界判断及 Spread 额外价差过滤沿用现有行为，
相关性筛查不在本次处理范围。原始因子为空时，标准化内部保留上一根 z，但该根交易分位
仍为空并保持仓位；原始因子有效而标准化暂时算不出时，若有上一根 z 则前填后参与分位，
若没有则继续未知。保留这一原有口径，不把原始因子为空的 bar 当成有效交易分位样本。

Prep 的因子退出独立于同根 NQ 是否存在或就绪。缺失、旧时间戳或不足 60 个样本的 NQ
均只阻止新开仓；不使用旧 NQ 替代当前根。空仓等待当前 NQ 时保留同根的入场机会，
1h 已退出的反向入场也可在同根 NQ 到达后继续判断；相同等待状态不重复记账或写盘。

配置页可从现有策略组新建 1h，默认为关闭、空规则、v0，独立维护金额及开平仓线。
未限定券池的 1h 也允许保存空规则作为零目标策略。不会自动复制其他频段的规则。
策略名使用 `cta-<组>-bn-1h-pos` 或 `cta-<组>-okx-bn-1h-pos`，券池前缀沿用既有规则。

NQ 使用 120 根有效 bar 的极值窗口和 120 个有效变化值的分位窗口，60 个有效变化值就绪。
1min / 5min / 15min / 1h 全部使用有效样本窗口，包含最新封闭 bar 的 mid；
数值、分位、Q50 和就绪状态直接对应同一根封闭 bar，不再额外后移。
当前 mid 无效时立即输出未知且不消耗有效样本窗口，下一根有效 bar 可立即使用。
默认 0.50 / 0.50；买入变化为 `(mid - 窗口最低 mid) / 窗口最低 mid`，
卖出变化为 `(mid - 窗口最高 mid) / 窗口最高 mid`。Prep 和 OKX/Binance Spread
的 NQ 均取该币种 Binance 永续价格：Kafka 部分使用 mid，1h 回放起点之前以历史
小时 K 线 close 补充；普通因子的无效当前值保持未知。
Spread 同一闭合 bar 的原值为 `(第一个市场 - 第二个市场) / 第一个市场`，
任一路输入缺失即未就绪；因子自身没有额外信号滞后。

页面保存和预览均校验整份合并清单。保存带当前清单的内容摘要，CLI 与
Web 共用跨进程文件锁和原子替换；并发修改返回 409，保留页面未保存的草稿。
删除组合保留其他组合引用和历史记录；切换后的信号读取到文件删除后，以原
策略身份发布零目标，不创建新身份清仓。

## 独立输入与数据契约

一个市场/币种维护一份原始分钟基线，再分发至所需 1/5/15min 状态；1h 从同一份
period 输入直接聚合原生小时基线，不经分钟补空路径。同一
市场/币种/周期的普通因子共享基线历史求值器，各因子维护自己的归一化和排名。
NQ、价差共享对齐后的盘口收盘数据。分钟路径保留现有末分钟盘口要求；1h 使用该小时
实际收到的末份盘口 mid（无效则未知），不检查覆盖完整性。数量沿用现有 PeriodMessage 的市场原生单位：
Binance 为基础资产数量，OKX 永续为原始合约张数，采集端不乘 `ctVal`。
大小单统计与实时分类统一使用 `price * amount`，阈值按市场和币种独立维护；
OKX 阈值的单位是 `price_x_contracts`，不能当作真实 USDT 成交额。

依赖成交大小分类的公式需要真实 `amount_thresholds`。配置键是
`市场:内部币对`，值为 `[medium_notional_threshold, large_notional_threshold]`。
缺失时仅这些公式未就绪，其他公式照常计算；不会填造分类阈值或有效零值。
这些主进程参数和 Kafka 配置在启动时读取；修改它们需要重启。组合目录支持热更新。

两个计算进程使用一份当前 IPC 格式和不同的服务名：

```text
factor_output/cta-prep-binance-futures
factor_output/cta-factor-spread
```

标量观测的事件示例：

```json
{
  "event": "point",
  "generation": "replay-generation-id",
  "point": {
    "factor_id": "okex-futures-binance-futures-btcusdt-1min-spread",
    "ts_ms": 1800000060000,
    "fields": {
      "value": {"raw": 0.001, "value": 0.001, "quantile": 0.6, "median": 0.0008, "ready": true}
    }
  }
}
```

字段统一为 `raw`、`value`、`quantile`、`median`、`ready`；未知值使用 null。普通因子的
`value` 是因果标准化结果，NQ/spread 的 `value` 为原值。NQ 的 `fields` 包含
`long`、`short`，各字段结构相同。删除不再被引用的因子发出 `removed` 事件。
回放代次更换会使消费者丢弃旧代次缓存，也允许同一数据时间的新定义替换旧值。
Rust 消费端使用 `cta_factor_calc::client::FactorFeed`，按 ID/字段选择需要的输入；
`fresh()` 检查就绪、未来时间和新鲜度，`poll()` 仅返回当前代次最新观测。
信号层仍须自行配置组合算法、决策周期、过滤阈值及仓位规则。

## 配置、删除与回放

只读检查合并计划，不连接 Kafka、不创建 IPC 或历史文件：

```bash
cta-prep-binance-futures --config config/cta_prep_binance_futures.toml --check
cta-factor-spread --config config/cta_factor_spread.toml --check
```

新增/替换和删除默认只打印计划；`--execute` 只修改指定组合文件：

```bash
cta-prep-binance-futures --put-combination /path/momentum.json
cta-prep-binance-futures --put-combination /path/momentum.json --execute
cta-prep-binance-futures --delete-combination momentum
cta-prep-binance-futures --delete-combination momentum --execute
```

原有 `symbol -> factor -> signed weight` 格式可在导入时补市场与频率：

```bash
cta-prep-binance-futures --import-symbol-factors /path/symbol_factors.json \
  --name binance-futures_15min --strategy-group prep01 --market binance-futures --interval 15min --include-nq
```

检查计划后加 `--execute` 保存。正负权重不变。旧 `_positive`/`_negative` 后缀
映射到实际公式，导入结果也是四列行格式；若两份旧 sleeve 碰到同一个 ID，导入
会拒绝，须明确拆分。`--include-nq` 将导入行的 NQ 设为 0.50。

进程每秒检查组合目录，非法/冲突配置保留原计划。只改权重或删除条目时保留
仍需使用的预热状态；一个因子直到最后一个非零引用消失才停止计算。新增条目
或改变定义时创建独立候选回放，原输出继续服务。候选追上所有所需 topic 的
最新 head 后统一交接；失败保留原运行代次并记录错误。必要 topic 为空不会
被当作回放成功。回放追上与因子窗口就绪分别记录，不伪造历史覆盖。

`--replay-only` 回放一个固定 Kafka head，写历史后退出，完全不创建 IPC。
历史位于配置的 `history_dir/generations/<generation>/`，观测按日期 JSONL 保存，
每代保留自己的初始计算计划。根目录 `state.json` 是当前计划、候选计划、代次、
最新观测和错误的原子快照。删除组合不删除历史，不拼接不同代次来重复计算信号。

环境内运行 `scripts/start_cta_prep_binance_futures.sh --execute` /
`scripts/stop_cta_prep_binance_futures.sh --execute`；Spread 使用对应同名脚本。
脚本默认 dry-run，读取环境自己的 `env.sh`；CPU 可设置 `CTA_PREP_BINANCE_FUTURES_CORE` /
`CTA_FACTOR_SPREAD_CORE`。生产构建、上传校验和安装仍须遵守 arbmm 和本地构建约束。

现行仓位表与券池分仓已按上述部署记录上线。原独立仓位信号入口已从
`Cargo.toml` 的二进制清单中移除，旧停止脚本保留用于切换前关闭历史进程。
统一 `start_cta_signal.sh` 拒绝再启动独立信号；启停因子进程即同时管理其仓位表。
模板 `enabled=false` 不能覆盖线上实际启用状态，发布前应核对运行快照和完整账本。

本地只读检查：

```bash
cta-factor-config --config config/cta_prep_binance_futures.toml --check
cta-prep-binance-futures --config config/cta_prep_binance_futures.toml --check
cta-factor-spread --config config/cta_factor_spread.toml --check
```

配置环境内 `scripts/start_cta_factor_config.sh --execute` 启动统一编辑服务，默认
监听 loopback。因子环境内通过对应 `start_cta_prep_binance_futures.sh` /
`start_cta_factor_spread.sh` 管理运行，脚本默认 dry-run；`env.sh` 配置
`CTA_MANAGER_API_URLS` / `CTA_MANAGER_API_URL` 和发布凭据，脚本仅传递所需变量。

Nginx 模板由 `scripts/render_cta_factor_config_nginx.py --source-ids <完整账户ID范围>`
生成；安装时替换旧 Config locations，不能与旧编辑路由同时保留。认证沿用
Manager 会话，要求完整账户范围的 configure 权限；同时部署 Manager 的
`X-CTA-Access: configure` 校验。旧单账户查看权限不能获得跨组合写权限。

## 1h 原生输入历史包

`cta-hourly-history build` 从已有 Tardis `trades` / `incremental_book_L2` 合成
Binance 永续小时输入。它复用线上成交与盘口聚合器和因子引擎，不导入预先计算的
z-score、交易分位或仓位。每币文件保存 OHLC、成交与大小单字段、20 档盘口、
盘口流量统计及 NQ 的小时末 mid；仅成交输入的标的使用已授权的小时 close 补充 NQ。
这些是本地合成数据，不能称为交易所或 Tardis 官方小时产品。

大小单因子读取上个自然月的完整成交并按现有小时 KLL 合并方法生成历史月份阈值；
当前月使用 JP 共享阈值的只读快照。逐币阈值与来源事件数写入元数据，
每币 gzip/bincode 文件带 SHA-256，整个目录由一个 `manifest.json` 描述。
合成跳过无输入小时，不检查 60 分钟完整性，不把旧盘口填成缺失小时。
事件按 Kafka 使用的 `(t-1h,t]` 小时归属；原始因子参数 N 不变。

```bash
cta-hourly-history build --rules /path/to/rx01.json --rules /path/to/rx03_top-5.json \
  --rules /path/to/rx03_top-20.json --rules /path/to/rx03_last-20.json \
  --source-root /mnt/nfs/30.133_xt_crypto/TardisSource \
  --from 2026-10-08 --until 2026-11-08 --output /path/to/history \
  --control-snapshot /path/to/current_control_snapshot.json --workers 6
cta-hourly-history check --history /path/to/history --rules /path/to/rx01.json
```

`--until` 是不含当天的 UTC 日界。相同参数的逐币完成文件可续跑，已完成的文件重新校验后复用。
Prep 与 Spread 都通过 `--hourly-history /path/to/history`，或
`CTA_HOURLY_HISTORY_DIR=/path/to/history` 读取历史；没有小时计算时不加载它。
种子只重建公式原始窗口、24 根标准化、48 根分位及 NQ 状态，不发布历史点或重建历史仓位。
原有 K 线 NQ 补充继续用于未包含在包内的标的。

读取历史包时，专用 Kafka 消费者从实际保留分区的最早 offset 开始，不提交 offset。
导入包的完整截止小时，重叠 Kafka 只重建盘口，不重复计算已导入小时；
超过截止边界后再推进小时因子。缺少首个保留 Kafka 小时以前的衔接数据时明确报错，
不跳过中间缺口。包是有明确截止时间的快照，Kafka 留存移过该截止点之前需要刷新。
`state.json.hourly_history_warmup` 和 `generations/<代次>/hourly_history_seed.json`
记录各币实际种子根数、因子就绪状态与衔接边界。

```bash
cta-hourly-history replay --history /path/to/history --rules /path/to/rx01.json \
  --rules /path/to/rx03_top-5.json --rules /path/to/rx03_top-20.json \
  --rules /path/to/rx03_last-20.json --control-snapshot /path/to/current_control_snapshot.json
```

`replay` 是只读检查：导入历史，再消费到启动时捕获的 Kafka 末尾；
验证小时因子时间严格递增，拒绝重复导入的历史小时，并输出未就绪及缺失因子。
它不连接 Manager/Exec，不写策略配置或 Kafka offset。
