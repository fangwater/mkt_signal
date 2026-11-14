# RX03 1min 下架

2026-11-09 UTC，经用户明确要求，下架 JP `binance_exec_rx03` 的 Prep02
`top-5`、`top-20`、`last-20` 三个 1min 策略，共58条规则、36个标的。

先保存私有配置、分仓和执行快照，将原绑定 shares 设为0，通过原策略归属清仓；
再通过带 revision 校验的 Config API 删除三个组合，确认信号表退役。
重新投递完整 clear 目标后，三策略全部执行完成，无委托、无批次、无非零目标，
最后移除三条 Manager 绑定。RX03 剩余六条5min/15min绑定和其他组合保持。

22条低于交易所最小下单量的真实分仓尾量，合计绝对名义约35.33 USDT，
连同停用的零目标 Exec 项及全局历史策略定义保留；未伪造成交、抹掉账本或迁移尾量。
本次不重启进程，不改CPU、源IP或执行账户设置。

私有快照及完整投递回执：
`~/crypto_cta_manager/maintenance-backups/cta_rx03_1min_retire_20261109T100830Z/`。
本目录的 `verified.json` 是脱敏回执；两个脚本默认只读，写入需明确 `--execute`。
