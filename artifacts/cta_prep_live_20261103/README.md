# RX01 / RX03 Prep 实盘启用

用户要求：六条绑定份额全部设为1，Prep01 / Prep02的1min、5min、15min全部enable，
启动RX01 / RX03交易。当前任务仅操作这两个新执行账户及六份Prep仓位表。

初始只读核对两个RapidX Binance永续账户仓位、普通订单、算法订单均为0；已有
Exec二进制与ff6d0575发布回执一致。使用环境本地脚本依次启动trade_engine、
exec-pre-trade、trade_signal；监控、持久化、Viz、Config保持。

六条Manager绑定通过已有绑定API设为shares=1；六份因子组合通过配置API携带revision，
只将signal.enabled改为true。因子规则、独立开平仓、USDT额度、执行模板及风控未调整。
RX01的1min/5min各100U、15min50U，RX03三个频段各100U。

首次启用暴露仓位表合并进程的Tokio LocalSet缺失：BBO订阅的spawn_local在开启时panic。
当时未发布非零目标、未产生新订单。9b343d29在两个计算worker共用入口包裹LocalSet，
33项CTA针对性测试及fmt通过。本机arbmm release构建，Git同步后核对artifact哈希，
上传显式临时目录并校验SHA-256，原子安装。只用环境脚本恢复Prep，PID3956979 / CPU17。
全部六份enable保持true，执行进程继续运行；恢复后完整重放行情，不伪造就绪或降低门槛。
开启状态的线上启动检查确认spawn_local路径正常，六份仓位表均启用且没有计算/仓位错误。

执行进程均在housekeeping CPU0-5：

| 账户 | trade_engine | exec-pre-trade | trade_signal |
|---|---:|---:|---:|
| binance_exec_rx01 | 3934587 | 3934756 | 3934963 |
| binance_exec_rx03 | 3936910 | 3937159 | 3937530 |

Config PID3578063保持；源IP/local_ips未改。

本地源码提交9b343d29已push；二进制摘要见manifest.json，构建回执见build.json。
私有写前配置、绑定、账户快照、旧程序及脚本日志位于：
`~/crypto_cta_manager/maintenance-backups/cta_prep_live_20261103T085500Z/`。
完整行情回放、六份推送及实盘执行验证均已完成，见下方回执。

最终核验：回放于09:29 UTC追上实时，generation 51d31fd2-9ced-4b40-8137-d32301ad9cbc。
404项计算中333项就绪，94项NQ全部就绪，51币大小单阈值已加载；计算、仓位表、推送
错误均为空，六份Manager目标与Exec Redis目标逐项一致，Manager没有待发布项。
09:37核验两个新账户的initial_position_count均为0；Manager已读取RX01的60笔和
RX03的29笔成交，实时订单写入各自新RocksDB。当前Manager分析直接读RocksDB；旧的
PostgreSQL事件表仍为空不代表实盘记录缺失，也未重新导入退役账户的历史。
六份策略的独立PnL摘要均有成交，具体按频段的只读核验在frequency_stats_verified.json。

CPU最终检查：Linux隔离范围6-47，Prep PID3956979全部线程仅CPU17，Spread
PID3960454全部线程仅CPU18，启动配置已持久化；回执affinity_verified.json。

同一工作期间另一个操作员独立启用了Spread并调整其执行模板；未覆盖该并行操作。
本任务审计保持Prep因子、金额、执行模板和非Spread绑定，允许并记录外部Spread的
配置、计算进程与绑定变更。activation/recovery回执分别核对89/101个受保护进程保持。

本次临时二进制发布目录已清理，脚本、旧二进制及写前配置保留在私有审计目录。
