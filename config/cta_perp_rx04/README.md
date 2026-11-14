# RX04 CTA Perp 5min

新增 bin：`cta_perp_factor_model_5m_pub`、`cta_perp_5m_signal`。
本目录为用户指定的 15 币、35 因子、48 项有符号绑定；默认 `enabled=false`，
单份金额沿用 Perp 模板的 200 USDT。2026-10-21 已部署到 JP 的
`~/binance-cta-perp-rx04`，运行信号及仓位展示。
2026-10-23 05:57 UTC已开启交易：账户与虚拟账户均命名
`RX04 5min`，virtual05有35项因子绑定；新账户环境/source为
`binance_exec_rx04_5m05`，复用原RX02的 `/exec_trade05/` 入口和10045/18165端口。
跟随倍率1，策略 `enabled=true`，线上单份金额为100 USDT，Exec六个进程在线。
开启前仅重启仓位signal，从现有5min因子历史按当前配置重算仓位；48组信号就绪，
26组非零仓位的入场名义金额均为100 USDT。因子publisher及预热历史保留。
新账户的namespace、
Redis和数据目录独立，旧RX02的Exec05历史保留；新版RX02继续使用Exec08。
2026-10-23 05:40 UTC，35个因子绑定全部改用现有 `lgq_grid_prep` 下单模板：
合约3档、10秒一轮、单手100 USDT、Vol倍数1、目标容差10 USDT、无限Maker重挂。
共享模板参数保持不变；Manager账户监控已开启。
远端回执及私有备份：`run/slot05_20261023/`；脱敏记录：
`artifacts/rx04_5min_slot05_20261023/`。

以下为当前源码口径；2026-10-30 将 NQ 从 24 根改为 120 根，本次尚未部署线上。

| 环节 | 数据与窗口 | 最少有效样本 |
|---|---|---|
| 因子公式 | 从 Kafka 原始成交/盘口聚合的完整 5min bar；公式窗口按 bar 计 | 各公式自身要求 |
| 因子 z | 288 根（1 天），沿用因果截尾及标准化，clip=3 | 200 根 |
| 因子分位 | 576 根（2 天），负向因子使用 `1-rank(z)`，等价于并列值取中点的 `rank(-z)` | 288 根 |
| NQ 极值 | 本币最近 120 个有效 mid，包含当前价 | 有效 mid 即参与 |
| NQ 门槛 | 最近 120 个有效 buy/sell change 的线性插值 Q50 | 各 60 个 |

5min 专项公式调整：`TP_VPI_006` 内层使用
`rolling(360, min_periods=280).sum()`，外层保持 `rolling(14).mean()`；
Python 参考调用 `TP_VPI_006(df, min_periods=280)`。默认参考及1min仍为360个样本。
连续有效数据下，原始值首出293根，z首出492根（41小时），最终分位首出779根
（64小时55分钟）；z的288/200和分位的576/288不变。

NQ 的 mid 取本币 5min 区间最后一分钟内最后一个有效一档盘口中间价。
无该分钟有效盘口时，该根 NQ 缺失，且不推进有效样本窗口。因此 120 根名义上为
10 小时，有缺口时实际跨度会延长。各频段 NQ 均保留 120 根，不按分钟数压缩；
因子分位 rolling 则按频段等比例换算，保持两天。`buy_chg >= Q50(buy_chg)` 允许开多，
`sell_chg <= Q50(sell_chg)` 允许开空；相等时放行，NQ 不影响持仓及因子退出。

UTC 区间 `[T,T+5min)` 的图表及历史 `bar_start_ms` 是 T；IPC 和历史
`model_ts_ms` 保留可用时间 T+5min，执行报价取收盘后到达的盘口，避免未来数据。
因子/NQ 的历史记录和实时 IPC 携带同一组值与门槛，回放复用同一开平规则。

5min IPC 使用 `model_output/<scope>/cta_perp-binance-futures-5m-<factor>`，
1min 通道仍为原名称。两端设置相同 `CTA_PERP_MODEL_SCOPE=binance_cta_perp_rx04`；
环境目录用独立的 `binance-cta-perp-rx04`，Redis key 自动按该目录隔离。
5min 因子历史默认在 `data/cta_perp_5m/factor_history`。现有 1min NQ 语义不变。

准备信号部署文件的入口（默认仅打印计划）：

```bash
scripts/deploy_cta_perp_5m.sh --env-name binance-cta-perp-rx04
```

实际发布仍遵守仓库的本地 release 构建、上传校验及环境本地启停流程。
安装脚本把本目录三份配置安装到环境的 `config/`，保留既有配置，并拒绝跨周期覆盖。
`start_cta_perp.sh` / `stop_cta_perp.sh` 根据 `cta_perp.json` 的 `bar_interval`
选择相应 bin；也提供独立的 5min publisher/signal 启停脚本。
配置服务、回放和图表按该字段选择 5min 数据，不支持运行中切换周期。

Kafka 配置预热 5 天，公式就绪仍取决于实际保留的原始行情。本清单 35 个公式
已核对仅依赖价格、成交量及盘口，不依赖成交大小分类阈值。上线时 Kafka 实际
保留数据从 2026-10-19 08:30 UTC 开始；缺少更早历史时，长窗口因子继续积累
有效样本，不缩短公式、z 或分位窗口。

线上 publisher 全线程 CPU28，页面监听 `127.0.0.1:10198`，配置监听
`127.0.0.1:19188`，经4191网关访问：

- [仓位表](http://13.115.227.29:4191/cta/binance-cta-perp-rx04/)
- [5min 信号](http://13.115.227.29:4191/cta/binance-cta-perp-rx04/signals/)
- [配置](http://13.115.227.29:4191/cta/binance-cta-perp-rx04/config/)
- [Manager账户配置](http://13.115.227.29:4191/manager/config/)：选择 `RX04 5min`。

Exec页面与配置上游已在线，运行日志、目标一致性与真实账户持仓均已核验。
调整单份金额只影响之后的新入场；若需重算已有模拟仓位，先关闭策略，使用明确的
恢复请求重启仓位signal，保留因子publisher，验收恢复结果后再开启。实盘已有仓位
时，重新发布目标会触发实际调仓，须按当次授权处理。

部署和在线验收记录保存在 `artifacts/cta_perp_rx04_deployment/`。
