# JP 旧 lgq_ltp_81857 入口移除

2026-11-03 02:45 UTC完成，并通过认证API和历史指纹核验。

移除对象为已停用的 Manager source `binance_exec_trade08`，显示名
`lgq_ltp_81857`。操作前已确认旧环境没有交易进程，执行绑定、跟随关系及待发布任务均0。

新跨所组合 `cta_cross_spread_rx01`（别名 `okex_binace_cross_1min`）及其
`okex_exec_trade01`、`binance_exec_spread01` 两个执行源保持。81857的凭据也用于
新组合的Binance执行源，因此本次只移除旧source及对应的source_viewers配置。
交易所下单、撤单、清仓调用数均0。

`remove_entry.py`先生成私有配置备份，检查候选配置只包含以上两处移除，使用线上
同一Manager二进制执行配置及不可变归属预检；显式`--execute`后原子安装并重启Manager。
健康检查等待历史缓存加载，并保留配置自动回退路径。

最终`verified.json`记录账户页和总览无旧入口、新组合名称及两腿身份保持、历史source/
symbol索引/仓位快照/日净值记录的指纹保持，以及受保护进程PID和启动时间保持。
虚拟账户定义保持；旧环境与source历史记录保留。

完整私有备份：

```text
~/crypto_cta_manager/maintenance-backups/ltp_81857_manager_remove_20261103T024033Z/
```

远端操作脚本安装在已有的
`~/crypto_cta_manager/maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts/legacy_81857_entry.py`，
复用该目录现有的`manager_entries.py`、`manager_ops.py`、`clear_positions.py`和
RapidX签名/进程检查辅助模块。所有凭据仅在远端进程内存读取。
