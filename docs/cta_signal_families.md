# CTA 仓位信号：Perp 与 Spread

最后更新：2026-10-13

本次为代码与页面改造，未部署、未重命名线上环境、未启动新交易。
新 RX01 的 `config/cta_spread.json` 默认 `enabled=false`，币种/因子模板为空。
不能直接把旧 RX01 逐 lot 状态或 RX02 的状态文件复制成 Spread 状态。

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
Perp 的 NQ 开仓过滤、原退出规则和历史回放保留。
旧普通 CTA 的 `intra_factor_model_1m_pub` / `trade_signal --arb-mode cta`
仍是旧 RX01 的运行链路；本次没有停掉它或让新 Spread 与它共用账户执行。
后续切换需要单独核对现有挂单、持仓、因子绑定及 Manager 份数。

## Spread 规则

每个币种对齐现货和永续同一个闭合 1 分钟的最后盘口 mid：

`spread = (spot_mid - perpetual_mid) / spot_mid`

2880 个分钟槽位，至少 1440 个有效配对，当前值包含在窗口内。缺失分钟占槽位、
不补旧价格，分位使用相同值的中间排名。配置的 q30/q70 指分位排名门槛，
不是把两个市场的价格绝对大小当成套利条件。因子分位保留原因果归一化算法。

| 因子开仓信号 | spread rank < .30 | .30 ≤ rank ≤ .70 | spread rank > .70 |
|---|---|---|---|
| q > .90 做多 | 买现货 | 不开仓 | 买永续 |
| q < .10 做空 | 卖永续 | 不开仓 | 卖空现货 |

价差未就绪不入场。行情使用当前有效且不超过 5 秒的目标市场 BBO 换算数量。
即使现货流缺失，因子仍可用于退出：实时缺少配对超过 5 秒时发布带
`filter_ready=false` 的因子 bar，不以旧价差替代。因子本身未就绪时保持状态。
现货卖空要求 RapidX Binance 的 MARGIN 模式；不要把这套双向策略配置为 SPOT。

exit long / exit short 仍由修正后的因子分位触发，价差不触发或阻止退出。
退出时保留现货目标，设置 `perpetual_qty = -cash_qty`，方向数量归零。
因此方向退出后仍可能持有期现仓位，不能把它的 PnL 全部记为已实现。
实际成交、两市场独立 FIFO 和 PnL 使用 Manager；Spread 页面展示信号目标，
不套用 Perp 单市场的模拟回测。

反向开仓信号同时通过价差过滤时，直接反转，不等待下一根 bar：
先在目标账本中用永续对冲旧方向，再在选定市场加入新方向的一份数量，
一次向 Manager 发布最终两个绝对目标。没有中间清仓请求。
反向信号未通过价差过滤，或新市场 BBO 无效，则只对冲旧方向，不开反向仓位。
同向信号不加仓；重复发布始终是幂等的完整绝对目标。

例如现货 +2 / 永续 0，反向开仓数量为 -3：选永续入场则发布现货 +2 / 永续 -5；
选现货入场则发布现货 -1 / 永续 -2。净方向均为 -3。
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
