# CTA 仓位信号：Perp 与 Spread

最后更新：2026-10-13

2026-10-13 06:28 UTC 已在 JP 部署并启动 `binance-cta-spread-rx01` 四个
信号/页面进程；二进制 `456be044`，脚本/页面 `470af607`。仅迁移 RX01，
线上 RX02 原 special 服务与 trade05 保持不变；CTA Perp 的新名称和代码暂未部署。
新 RX01 的 `config/cta_spread.json` 保持 `enabled=false`，未启动交易执行。
`config/cta_spread_symbol_factors.json` 已按确认名单配置 19 个币种、5 个因子，
共 57 个 symbol × factor 组合；发布器 universe 同步覆盖全部币种和因子。
该清单已写入独立 Redis 键 `binance-cta-spread-rx01:cta_symbol_factors`。
06:28 验收时四进程在线，页面/API 正常，首次 Kafka 历史回放仍在推进，
因子与价差均未就绪（0/57）；待追上实时后再核验连续闭合分钟，当前不宣称信号已稳定。
页面 `/cta/binance-cta-spread-rx01/`，配置 `/cta/binance-cta-spread-rx01/config/`，
均通过既有 JP 4191 网关访问；旧 RX01 页面地址跳转到新地址。
现场证据见 `artifacts/cta_spread_rx01_deployment/validation.json`。

| Spread 因子 | sign | 币种数 |
|---|---:|---:|
| `baseline_164` | +1 | 18 |
| `baseline_141` | +1 | 18 |
| `factor_170` | -1 | 7 |
| `factor_113` | -1 | 6 |
| `factor_052` | +1 | 8 |

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
