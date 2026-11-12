# Spread02 OKX factors

用户原始清单直接保存为 `requested_factors.json`，共18币、319条独立规则。
同币、因子、频率的不同NQ条件分别保留；`okxswap`映射为`okex-futures`，
`0.50/0.50`映射为`0.50`，`NQ关`映射为`关闭`。

| 频率 | 独立规则 | 策略 |
| --- | ---: | --- |
| 1min | 48 | `cta-spread02-okx-bn-1min-pos` |
| 5min | 79 | `cta-spread02-okx-bn-5min-pos` |
| 15min | 64 | `cta-spread02-okx-bn-15min-pos` |
| 1h | 128 | `cta-spread02-okx-bn-1h-pos` |

用户确认复用Spread01双Exec：`okex_exec_trade01`与`binance_exec_spread01`，
Manager公共账户`okex_binace_cross`。每规则100 USDT，四张表发布保持关闭，
本次只部署与预热，不新增执行跟随绑定，不生成历史仓位。
因子/NQ使用OKX永续；执行方向仍为OKX方向腿、Binance退出对冲腿。
入场因子0.9/0.1，退出0.3/0.7，上一根同频率价差0.3/0.7过滤。

本批341项计算与原Spread01共享24项价差，合并532项；独立仓位和策略身份保留。
线上四份Spread01配置按原文件保留。共享进程CPU18，配置服务仍19220。
新增四份配置文件均带`spread02_`前缀，配置页可选择Spread02。

新增10币OKX月度阈值来自完整2026-10逐笔文件，每币744小时，
共195,325,193笔成交；使用现有小时KLL P50/P90与`price_x_contracts`口径。
backfill只新增10项，原9项和公共因子计划保持，合约金额表扩为19项。
CSV SHA-256：`0bf7e53c93f8e9918ce8767ab70e80959d842bac1462052bb88486c25fcf10ef`。

原Spread01小时历史包截止2026-11-09，首次启动因Kafka保留起点已后移而拒绝。
随后从本地Tardis trades/incremental_book_L2合成11-09、11-10两天，
用`cta-hourly-history extend`衔接为12币×816根，截止11-11 UTC；88/88项普通因子就绪。
只更新远端`CTA_HOURLY_HISTORY_DIR`指向新包，其余环境设置保持；旧包及仓位完整保留。
Spread02 OKX小时输入仍由保留Kafka和实时行情自然预热；该Binance包不冒充OKX数据。

Delist读取相同配置契约，因此同步发布解析更新；CPU28及监控配置保持。
配套回归覆盖Spread01/Binance与Spread02/OKX各四个频段的完整备份往返。

本地验证：Rust库1539通过、3忽略；配置服务4通过；小时工具3通过；
配置页8个频段读写往返及新行来源检查通过，319条规则与原请求逐项一致。
Delist完整74项中70通过、4项现有测试失败：三个旧配置数量断言已落后于原Prep/Spread
模板，另一个测试环境找不到`redis-server`。新增双来源八表备份回归通过。
这些失败未被解释为线上验证通过；实际部署后另外只读检查当前目录和监控状态。

最终线上状态、进程保持证明及预热计数见`verified.json`；候选与安装哈希见
`binary_manifest.json`。私有配置备份只留远端维护目录，不复制到本地或输出。

2026-11-12 06:25 UTC，185,633条保留消息回放完成并切换实时。
06:26验收532项合并计算、八张表状态正确；Spread02为零仓位/未启用，237/341项计算就绪。
06:27确认Spread01已处理新的实时分钟，Manager确认且待发布为空；Delist无错误、计划一致。
原四份Spread01配置文件字节一致，停机账本逐项恢复；只有历史包路径按实际覆盖更新。
所有非目标常驻应用的原PID/CPU保持，另有两个定时NAV补采批次正常结束，单独记录。
原生输入包与追加包保留在本机`data/ops/cta_spread02_deployment/`，不纳入源码提交。
