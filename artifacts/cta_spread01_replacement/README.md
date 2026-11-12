# Spread01 全量替换

用户清单：209 条规则、12 个标的；旧 34 条规则全部下线。

| 频率 | 规则数 | Manager 策略 |
|---|---:|---|
| 1min | 28 | `cta-spread01-okx-bn-1min-pos` |
| 5min | 56 | `cta-spread01-okx-bn-5min-pos` |
| 15min | 18 | `cta-spread01-okx-bn-15min-pos` |
| 1h | 107 | `cta-spread01-okx-bn-1h-pos` |

标准规则为每频段独立的四列数组：`标的`、`nq`、`符号`、`因子`。
`binanceswap` → `binance-futures`；`0.50/0.50` → `0.50`；`NQ关` → `关闭`。
同因子不同 NQ 条件分别保留，209 条没有完全重复行。计算 ID 自动合并为215项。

保持一个公共组合账户 `okex_binace_cross`，两侧为 `okex_exec_trade01` / `binance_exec_spread01`。
所有频段均属 `spread01`，无券池；因子均取 Binance 永续，交易腿为 OKX / Binance 永续。
每条规则沿用100U、v0、ctaker和每侧1份。开仓0.9/0.1，平仓0.3/0.7；价差使用前一根同频段的0.3/0.7过滤。

用户已确认：两侧所有旧仓及尾仓全部平掉，再归档并清空活动记录。
归档位于 JP `~/cta-audits/spread01_reset_20261110T030317Z/`，本地副本位于
`data/ops/cta_spread01_replacement/`。归档包含原生 RocksDB、订单 parquet、Redis、配置与交易所核验回执。

已验证 parquet：OKX uniform_orders 1775行，未匹配订单更新4行、成交更新12行；
Binance uniform_orders 5352行，未匹配更新均0行。行数是事实记录数，不是独立订单数。

1h 共107条规则、88项普通因子、9项NQ、12项价差。
本地 Tardis trades / incremental_book_L2 合成 2026-10-08 至2026-11-09（不含）原生小时输入，
12币×768根。沿用原公式、24根标准化、48根交易分位和NQ样本门槛；不生成历史持仓。
1min/5min/15min 不额外注入历史包，由保留Kafka与实时输入正常预热。

Kafka 实测（2026-11-10 UTC）：两主题均可从最早 offset 解压并解析 PeriodMessage，
不提交offset、不改变其他消费者。完整日志位于远端审计目录。

本文件为操作范围与已完成证据；最终运行状态另见 `verified.json`。

Manager 旧目标清理：1246条（含清理前最后一条空目标）已归档并从活动库删除。
相同时间范围内其他1,342,639条目标内容指纹保持；原1min目录及权限沿用，目标已重置为空。
共享Manager停启及定向维护用时70.8秒，服务启动后继续等待HTTP缓存初始化完成。

小时输入包已完成：12币×768根=9216根，88/88项普通因子在导入检查中就绪，9项所需NQ均补齐。
压缩包6,491,157字节，SHA-256 `53c9580ea0a3f048d644be130fffd40595e6ef7167af10ae998374ce246ea1be`。
原始事件仅在本地合成；JP只接收已校验输入和本地release产物。

2026-11-10 04:01 UTC：Kafka追平，四张表均已启用；两侧共8条ctaker绑定各1份，每规则100U。
计算代次保持`2e0557ae-8abe-4e81-acc4-e2159f0aac23`，Manager发布确认完成，无待发布策略或错误。
就绪：1h109/109、1min35/35、5min46/49、15min21/22，其余4项分钟计算自然预热。
本地L2合成覆盖11币，DOGE小时规则仅需成交；总计560,722,606个盘口事件、214,300,242笔导出期成交。
归档和重置不导入历史仓位；策略只决策启用后的新封闭bar。

自然预热中的4项：
- `binance-futures-adausdt-15min-baseline_104`
- `binance-futures-trbusdt-5min-td_ti_002`
- `binance-futures-trbusdt-5min-tp_vpi_001`
- `binance-futures-trbusdt-5min-tp_vpi_005`
