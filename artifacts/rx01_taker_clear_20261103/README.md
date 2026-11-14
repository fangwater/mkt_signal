# RX01 USDT 永续 taker 清仓与解绑

用户授权：先 taker 清仓，再解除所有旧仓位绑定。
目标为 RapidX Binance USDT 永续，Exec 环境 `binance_exec_trade06`，旧信号 `binance-cta-perp-rx01`。

先关闭旧信号并用原 91 个策略身份发布 clear，停止产生补偿任务的 Exec 下单进程，
再以精确数量 reduce-only MARKET 收尾。逐单持久化意图和成交结果后签名查询核对。
最终 USDT 永续仓位、普通/算法挂单均 0；91 个旧账户策略绑定、virtual06 跟随及
Exec 活跃策略索引已移除。源账户及监控、持久化、Viz/Config 和历史数据保留，
下单进程不恢复。清仓范围不包含历史现货零头。

脱敏终态见 `verified.json`。完整私有意图/成交/移除回执保留于远端
`~/crypto_cta_manager/maintenance-backups/rx01_taker_clear_20261103T034024Z/`。
`operate.py` 是本次审计工具；不要重复执行已完成的清仓阶段。
