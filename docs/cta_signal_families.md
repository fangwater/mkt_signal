# CTA 仓位信号：Perp 与 Spread

最后更新：2026-10-13 13:42 UTC

2026-10-13 13:42 UTC **RX01 Spread 已启用实盘发布并接入 Exec06**。操作者确认沿用
现货开方向、退出时期货对冲；开仓至退出期间允许方向敞口。
现场原单份金额实际为100 USDT，本次保持100；下方200及未启用状态为旧记录。

- RX01/RX02 均按符号修正后的因子分位：q>0.90做多、q<0.70退多，
  q<0.10做空、q>0.30退空；严格不等式，原有过滤保留。RX01价差q30/q70，
  RX02 NQ q50/q50过滤启用。
- RX01五因子16币46组合，发布在线且46/46就绪、journal健康、publication_error为空。
- `binance_exec_trade06` 跟随 `virtual03/cta_spread_rx01` 倍率1；五个
  `binance_cta_spread_rx01_` 因子策略与独立 `inventory` 策略各1份，ctaker执行。
  7币12腿旧库存按启用前账户净现货和期货实际数量继承，并写入账户/库存策略NAV基准。
  启用前分配全部核对一致，待下单与活动订单量为零。
- lgq 对source06有configure权限，对virtual03与六个策略有管理权限；
  `/exec_trade06/`、`/exec_trade06/config/` 已以lgq身份验证。
- Manager费用参数对齐RX02：maker=-0.00002，taker=0.000175，理论TWAP=0.0004；
  这是管理端配置口径，未变更交易所费率。
- 本地arbmm release发布，SHA-256校验和原子安装；`dfb70364` 修复双市场Exec
  风控字段加载，`c484f85d` 按Exec市场查询现货快照，`480bc990` 补RapidX资产
  快照完成标记。Manager另修复非USDT现货规则的误拒绝，现货规则480/永续744。
  风控18项、快照runner6项、trade_engine357项、Manager295项测试通过（3项忽略）。
- 验收及部署证据：`artifacts/cta_spread_rx01_activation/`。远端证据在
  `~/binance_exec_trade06/run/activation_20261013/`；该目录含环境备份，禁止整体导出。

2026-10-13 09:20 UTC 更新 JP `binance-cta-spread-rx01` 最新清单，配置提交
`480387a3`。独立 Redis、磁盘 signal 清单、publisher universe 同步为16币、
5因子、46个 symbol × factor 组合。`+/-` 映射为有符号权重 `+1/-1`。
不新增币种，移出 BTC、ETH、TRX；保留33个旧组合，移出24个旧组合，新增13个。

| Spread 因子 | sign | 币种数 |
|---|---:|---:|
| `baseline_141` | +1 | 15 |
| `baseline_164` | +1 | 15 |
| `TD_TI_027` | -1 | 8 |
| `TD_TI_024` | -1 | 5 |
| `factor_052` | +1 | 3 |

publisher 经环境本地脚本重启到 PID663465/CPU17；09:40 UTC Kafka 双市场
历史已追至实时，46/46组合因子与价差就绪。
原二进制不变。signal/config/dashboard 保持 PID3199322/3183433/3390010，
实时热加载配置，发布保持 `enabled=false`，单份200 USDT、价差q30/q70不变。
没有启用RX01交易，没有变更RX02 Perp的清单、virtual组合或Exec。
环境备份为 `run/scope_before_20261013T091935Z`；release.json单独记录config_commit。
页面 `/cta/binance-cta-spread-rx01/`，配置页为其 `config/`，B/S页为其 `signals/`。
当前验收记录见 `artifacts/cta_spread_rx01_latest/`。

最终执行账户明确为 **RX01**。09:39 UTC已预部署专用 `binance_exec_trade06`
运行包（本地arbmm release，逐文件SHA-256校验后安装）；远端凭据核对属于
旧RX01且不同于RX02。全部Exec进程停止，尚未注册Manager source或绑定virtual。
计划使用独立 `cta_spread_rx01` virtual、五个因子策略和一份库存承接策略；
库存需以正式切换时的实际现货净资产与合约仓位重新读取，不能用本次快照
或旧策略状态直接填入新因子。旧账户仍有已对冲库存，不能当作空仓启动Exec。
本次完成的是信号观察上线与执行包预部署，尚未启用实盘仓位发布。

首次部署历史：06:28 UTC安装19币/57组合的旧清单，二进制`456be044`、
脚本/页面`470af607`，当时尚在预热。该清单已由上述最新配置替代。
原验收记录见 `artifacts/cta_spread_rx01_deployment/validation.json`。

币种映射使用 USDT 符号；负 sign 按 `1-q` 修正因子分位。价差过滤保持 q30/q70，
单份金额保持现有 200 USDT，Manager 份数未变更。
不能直接把旧 RX01 逐 lot 状态或 RX02 的状态文件复制成 Spread 状态。

## Spread 信号观察页

2026-10-13 06:49 UTC 已部署页面提交 `ff3a8082`，仅重启 Spread Dashboard
至 PID 3390010（CPU 0–5）；因子预热继续，RX02/Exec 不变，发布保持关闭。
验收 57 个组合 API 全部可读、均有现货 K 线，行情无错误，价差历史就绪
57/57、因子 0/57、实时 0/57（该时刻仍在首次 Kafka 回放中）。线上浏览器
验证桌面/手机、factor 符号及币种切换通过，详见
`artifacts/cta_spread_signals/{validation,browser_validation}.json`。

后续线上 B/S 验证：历史回放推进到 10-12 23:33 UTC，42/57 个组合因子
已就绪。已实际点击 ETH × baseline_164 的 10-12 22:03 现货 B 标记，
并检查 22:14 合约 S 对冲退出；图表均有行情价格，浏览器无错误。
这仍是历史信号回放，不是成交，实时就绪当时为 0/57，发布持续关闭。
证据：`artifacts/cta_spread_signals/live_bs_validation.json`。

`/cta/binance-cta-spread-rx01/signals/` 按 factor 独立切换其已配置币种，
展示现货 1m K 线、修正因子分位、价差分位及方向变化 B/S 点。
绿色/红色三角是现货开仓，金色菱形是合约对冲退出；允许反转时同一分钟
包含先对冲、再反向开仓两个事件。边界和反转规则与 Spread Rust 方向规则一致。

后台只读导入 `data/cta_spread/factor_history/*.jsonl` 至独立
`data/cta_spread/signal_history.sqlite3`，持续保存观测，不写目标、配置、Redis
或发布器状态。发布关闭时仍可观察。它按当前配置从最早可用历史空仓开始
因果回放方向意图，不校验历史 BBO、报价新鲜度或计算成交数量、费用及 PnL；
B/S 不能作为事实发布/成交证据，事实执行继续查看 Manager。
回放状态从显示窗口之前承接，负 sign 使用 `1-q`；未就绪因子保持状态，
价差未就绪阻止新开仓但不阻止退出，不用未来数据补齐历史就绪状态。

信号时间为分钟结束边界，标在下一分钟现货 K 线上，不能倒移到上一根。
K 线由独立只读后台直连 Binance 公共 Spot Klines 补缺，缺失时不编造价格，
对应事件仍列在事件表。图表支持 1/6/24 小时；Kafka 追赶期间窗口跟随已有
历史末端，明确显示数据时间/滞后、历史就绪和实时就绪，心跳不代表可交易。
本地浏览器测试覆盖标记点击、反转双动作、负号、独立清单、移动端、反向代理
路径和行情缺失；合成截图仅用于测试，不作为实盘信号证据。

## RX02 Perp 新清单与 NQ 短窗口

当前 RX02 已在08:57 UTC正式启用，24组合通过virtual02/lgq_cta发布到
trade05，倍率1；09:14 UTC旧special因子发布器已退役。实际组合/投递核验见
[RX02切换说明](cta_perp_rx02_cutover.md)。下方为预热和切换前的历史记录。

2026-10-13 08:25 UTC 新 Perp 已完成 Kafka 预热，24/24 组合实时就绪、
9/9 NQ 就绪，仍为 enabled=false。首次预热触及旧 1800 秒上限，因此将
上限改为 3600 秒并在独立 CPU18 重启 publisher；新 PID 4133794。
发布器日志确认 `percentile=2880/1440 nq=120/120/60`、解析/计算错误为 0。
桌面/手机已验证真实历史的 B 开多、S 开空、平仓标记及交易快照点击；这些是
历史模拟，不是成交。新信号日志只有 observed，没有 published/partial；
旧 RX02 仍负责实时仓位。验收见 `artifacts/cta_perp_rx02_preview/`。

2026-10-13 07:52 UTC 新 RX02 四个进程均已启动，仓位发布保持关闭。
配置服务 19186 / Dashboard 10196 与旧 RX02 端口独立，网关路径
`/cta/binance-cta-perp-rx02/`、`config/`、`signals/` 已接通，旧页面仍保留。
新观察信号不承接旧实际仓位，仅记录模拟方向；最终切换时先停止观察进程，
备份模拟状态，再复制已停旧进程的完整最终状态。07:56 UTC 浏览器验证 9 币/
24 组合、100 USDT、桌面/手机、五个因子回放加载通过，无 JS 错误；Kafka
仍在追赶历史，不能以页面健康替代实时因子就绪。证据位于
`artifacts/cta_perp_rx02_preview/`。

2026-10-13 07:35 UTC 已将本地 release 二进制 `976d557e` 与切换脚本
`f72ab0e4` 校验后安装到 JP `~/binance-cta-perp-rx02`。只启动新 Kafka 因子
发布器 PID 3841796（CPU 0-5）；新配置保持 enabled=false、单份 100 USDT，
新仓位发布器及两个页面服务均未启动。独立 Redis 清单已初始化。07:36 UTC
仍在历史预热，尚未切换旧 RX02、未发布新仓位目标。Nginx 候选配置测试通过但
没有安装。用户执行的切换命令与状态承接详见 [RX02 切换说明](cta_perp_rx02_cutover.md)。

2026-10-13 更新 CTA Perp 配置与计算；尚未切换线上旧 RX02。
NQ 的价格极值窗口和分位窗口均为 **120 个分钟槽位**，至少 **60 个有效
NQ 样本**后就绪。缺失分钟占槽位、不过滤掉时间间隔，不计入有效样本，
第 121 分钟开始淘汰最早样本。BUY_NQ/SELL_NQ 改为相对最近 2h 最低/最高
mid 的变化率。分位仍包含当前值、采用中间排名；开仓阈值与退出规则不变。
因子分位仍为 2880/min1440，归一化仍为 1440/min1000，Spread 不受影响。

| Perp 因子 | sign | 币种 |
|---|---:|---|
| `baseline_141` | +1 | AAVE、DASH、ETH、NEAR、SOL、XLM、XRP、ZEC |
| `baseline_200` | -1 | AAVE、DASH、DOGE、SOL、XLM、ZEC |
| `TD_TI_024` | -1 | AAVE、DASH、DOGE、NEAR、XLM、ZEC |
| `factor_036` | -1 | NEAR |
| `baseline_164` | +1 | AAVE、DASH、ZEC |

RX02 只读现场状态与旧仓库清单一致：16 币/32 个绑定 → 9 币/24 个绑定，
没有新增币种，移除 AVAX、BNB、DOT、FIL、LINK、LTC、UNI。
只有 NEAR × factor_036(-1) 原样保留；其余移除 31 个绑定、新增 23 个绑定。
新清单和发布器 universe 同步维护在 `config/cta_perp_symbol_factors.json`
和 `config/cta_perp_factor_model_1m_pub.toml`，已写入新环境独立 Redis，旧环境清单未改。

零目标与删除策略含义不同：保留原策略并发布 qty=0 时，Exec 使用原策略的
账本仓位执行目标调仓；删除策略或发现无法归属的账户余量时，剩余仓位可进入
`SYSTEM_POSITION_CLOSE`。同币种已同时加载的新旧目标可通过内部交叉减少
实际下单差额；先清零、后发布新目标不保证避免先平后开。线上旧 RX02 仍在
发布且旧绑定有持仓，不能把直接覆盖清单描述为无交易影响的配置更新。

## 统一命名

| 项目 | CTA Perp（原 special / RX02） | CTA Spread（新 RX01） |
|---|---|---|
| 建议环境名 | `binance-cta-perp-rx02` | `binance-cta-spread-rx01` |
| 因子发布器 | `cta_perp_factor_model_1m_pub` | `cta_spread_factor_model_1m_pub` |
| 仓位发布器 | `cta_perp_signal` | `cta_spread_signal` |
| 策略配置 | `config/cta_perp.json` | `config/cta_spread.json` |
| 配置服务 | `scripts/cta_perp_config_server.py` | `scripts/cta_spread_config_server.py` |
| 仓位页 | `web/cta_perp/` | `web/cta_spread/` |
| 配置页 | `web/cta_perp_config/` | `web/cta_spread_config/` |
| 模型服务前缀 | `model_output/cta_perp-binance-futures-1m-` | `model_output/cta_spread-binance-futures-1m-` |
| 独立 Rust 模块 | `src/cta_perp/` | `src/cta_spread/` |

两套各自维护策略状态、因子发布、参数校验和前端，不用同一策略开关切换含义。
通用 Manager/Exec 的 targets 接口不变：每个 factor 是一份仓位策略，每个
symbol × factor 独立持仓，负权重取 `1-q`，权重绝对值不改变金额。
每个组合开仓使用一份 `max_pos_u`（默认 200 USDT），持仓期间冻结数量。
Manager 的绑定/虚拟账户配置份数，Exec 执行聚合后的实际目标；信号端不维护
第二套份数或订单参数，也不直接下单。

`{env}:cta_symbol_factors` 的 Redis 键后缀保持一致，环境名隔离两套绑定。
两套 publisher 分别以各自 `config/cta_{perp,spread}_factor_model_1m_pub.toml`
的 universe 决定币种和因子，不读取共享 `factor_plan_1m` 或
`amount_thresholds_1m` 清单。TLen 的通用 `amount_thresholds` 仅提供回放所需
的成交大小分类阈值；缺失阈值时启动校验报错，不扩大或借用另一套策略清单。
Perp 的 NQ 开仓过滤、原退出规则和历史回放保留。
旧普通 CTA 因子发布器、CTA/CtaSpecial 专用执行分支、规则服务及页面入口已移除。
通用 Intra 不再接受旧 CTA 环境，旧 CTA 开仓消息也会被拒绝；停止脚本仍可识别
旧环境，便于关闭历史部署。当前仅维护 Perp 与 Spread 两套仓位发布链路。

旧 RX01 的 trade_signal、pre_trade、trade_engine 已停；账户监控和持久化保留。
用户已执行合约对冲，最近只读核对仅剩小于交易最小量/最小名义的净敞口。
旧 RX01 的配置页和 Viz 已下线，账户监控、持久化及历史目录保留。
新 Spread 没有复制旧 lot 仓位，也没有配置 Manager 发布连接或账户绑定。
未来启用前仍须核对现有挂单、持仓承接、因子绑定及 Manager 份数；当前发布保持关闭。

## Spread 规则

每个币种对齐现货和永续同一个闭合 1 分钟的最后盘口 mid：

`spread = (spot_mid - perpetual_mid) / spot_mid`

2880 个分钟槽位，至少 1440 个有效配对，当前值包含在窗口内。缺失分钟占槽位、
不补旧价格，分位使用相同值的中间排名。配置的 q30/q70 指分位排名门槛，
不是把两个市场的价格绝对大小当成套利条件。因子分位保留原因果归一化算法。

| 因子开仓信号 | spread rank < .30 | .30 ≤ rank ≤ .70 | spread rank > .70 |
|---|---|---|---|
| q > .90 做多 | 买现货 | 不开仓 | 不开仓 |
| q < .10 做空 | 不开仓 | 不开仓 | 卖空现货 |

开仓始终使用现货目标：因子多头仅在低价差分位开仓，因子空头仅在高价差分位开仓。
方向不匹配或价差未就绪不入场。使用当前有效且不超过 5 秒的现货 BBO 换算数量。
即使现货流缺失，因子仍可用于退出：实时缺少配对超过 5 秒时发布带
`filter_ready=false` 的因子 bar，不以旧价差替代。因子本身未就绪时保持状态。
现货卖空要求 RapidX Binance 的 MARGIN 模式；不要把这套双向策略配置为 SPOT。

exit long / exit short 仍由修正后的因子分位触发，价差不触发或阻止退出。
退出时保留现货目标，设置 `perpetual_qty = -cash_qty`，方向数量归零。
因此方向退出后仍可能持有期现仓位，不能把它的 PnL 全部记为已实现。
实际成交、两市场独立 FIFO 和 PnL 使用 Manager；Spread 页面展示信号目标，
不套用 Perp 单市场的模拟回测。

两套 CTA 使用同一仓位发布请求：`strategy_name` 和 `targets`，
每个目标包含绝对 `qty` 和与 Perp 相同的 `signal=0`。
Spread 发布裸币种现货和 USDT 永续两个目标；Perp 发布合约目标。
信号服务不携带订单模板覆盖，不指定 maker/taker，也不创建执行模板。
份数和执行策略由 Manager 配置，Exec 按分配后的目标执行。
目标数量换算与 Perp 一致：开多参考当前 ask，开空参考当前 bid；
参考价只用于将单份 USDT 换算成数量，不是委托价格或执行方式指令。

两套 config server 独立维护，配置读取、保存、版本冲突校验和 symbol × factor
编辑流程一致。共用相同的发布开关、单份 USDT 和因子开仓/退出参数结构，
Perp 使用 NQ 开仓过滤参数，Spread 使用历史 mid 价差分位过滤参数。
执行配置入口统一位于 Manager。

反向开仓信号同时通过价差过滤时，直接反转，不等待下一根 bar：
先在目标账本中用永续对冲旧方向，再在现货加入新方向的一份数量，
一次向 Manager 发布最终两个绝对目标。没有中间清仓请求。
反向信号未通过新现货方向的价差过滤，或现货 BBO 无效，则只对冲旧方向，不开反向仓位。
同向信号不加仓；重复发布始终是幂等的完整绝对目标。

例如现货 +2 / 永续 0，反向做空数量为 -3 且价差处于高分位：
发布现货 -1 / 永续 -2，净方向为 -3。再退出空方向时发布现货 -1 / 永续 +1，
净方向归零。没有通过高分位过滤时，只发布现货 +2 / 永续 -2。
执行尚未完成时再次更新，Exec 以最新目标减实际已执行数量计算差额。

每个组合持久化方向、方向数量、现货数量、合约数量和入场金额。
发布前先原子替换并 fsync 状态；重启校验数量关系，禁止把旧单市场状态默认为零。
删除绑定会退出其方向并保留已对冲的两腿目标，不能通过删除配置遗忘现货。
真正清掉所有期现库存须作为单独的 Manager/Exec 操作。
关闭发布时保留目标状态，只更新观察数据，不生成新的模拟持仓供以后执行。

## 本地检查和准备

```bash
cargo check --bin cta_perp_signal --bin cta_spread_signal \
  --bin cta_perp_factor_model_1m_pub --bin cta_spread_factor_model_1m_pub
cargo test --lib
python3 -m unittest scripts.tests.test_cta_perp_servers \
  scripts.tests.test_cta_perp_position_replay scripts.tests.test_cta_perp_signal_history \
  scripts.tests.test_cta_spread_servers
scripts/deploy_cta_spread.sh --env-name binance-cta-spread-rx01
```

最后一条默认 dry-run。`--execute` 只安装信号套件，不启动、不创建 Exec companion，
不覆盖已有 env.sh、参数或状态。首次安装的信号配置关闭发布；初始化 Redis
symbol/factor 表后再启动观察。生产构建必须在本地干净且同步的 arbmm 上完成。
远端只能接收校验过的本地 release 产物；信号套件发布与标准 Exec 发布分别处理。
Perp 与 Spread 的环境本地 start/stop 脚本只管理 publisher、signal、config、dashboard。

线上改名不能只替换二进制：须一并迁移配置/状态文件名、Redis 环境前缀、IPC
订阅和启动脚本。已有 Manager 策略名称、历史来源和实际持仓归属不得自动重命名。
