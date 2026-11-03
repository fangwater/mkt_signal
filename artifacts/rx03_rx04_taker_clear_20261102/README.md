# RX03 / RX04 taker 清仓与解除执行绑定

2026-11-02 15:05 UTC，JP `jp-meta-elvpn` 上操作完成。

| 信号 | Exec source | 删除绑定 | 补充尾仓市价单 | 最终永续持仓/普通挂单/算法挂单 |
| --- | --- | ---: | ---: | --- |
| RX03 | `binance_exec_trade07` | 28 | 12 | 0 / 0 / 0 |
| RX04 5min | `binance_exec_rx04_5m05` | 35 | 8 | 0 / 0 / 0 |

两份 `config/cta_perp.json` 的 `enabled=false`，signal停止。
先按原63个策略身份发布 `qty=clear, signal=1`，由Exec执行taker清仓；
各因子剩余量低于最小下单量时，停止目标Exec的pre-trade与trade_engine，
按账户实际剩余整手数量补充reduce-only MARKET。20笔全部成交，逐单查询并对账归零。
每次下单前落盘intent，结果未确认时仅按clientOrderId查询，不重复下单。

归零后通过Manager API解除virtual04/virtual05跟随，将份额置0并删除28/35个绑定；
通过各自Exec Config API删除实际Redis活跃策略索引，最终两个索引均为空。
11月2日清仓时保留虚拟账户、全局仓位策略、订单记录及旧配置，未给人工尾仓成交虚构策略归属。
下单进程保持停止，账户监控、持久化及页面保留。CPU19/28因子publisher继续运行。
没有操作RX01，没有修改源IP、路由或资金划转。

`final_verification.json` 为不含凭据的最终签名查询及运行态回执。
完整配置备份、63份发布回执、逐单journal及移除回执仅保存在远端私有目录：

```text
~/crypto_cta_manager/maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/
```

操作脚本均默认只读/计划，交易和移除需要显式`--execute`。
远端`scripts/`同时包含本目录Python文件，以及仓库现有的
`neutralize_rapidx_binance.py`、`binance_local_ip.py`、`rapidx_retirement_execute.py`。
认证仅在远端进程内存读取，不保存或输出凭据。

2026-11-03 02:28 UTC补齐Manager入口清理：此前仅解除执行绑定，账户来源仍为
enabled=true，虚拟账户仍在，用户因此仍看到`RX04 5min`。
`manager_entries.py`先备份并验证候选配置，再移除两个source、对应source_viewers和
RX03 funding出口例外；只重启Manager。通过API删除无跟随者的virtual04/virtual05，
账户页、总览和虚拟账户列表均已确认无残留；全局仓位策略、历史source记录与仓位快照保留。
首次55秒健康检查等待不足已自动回退；调整预热等待后成功。历史source登记记录的
updated_at会被恢复原配置时的正常register_sources刷新，不作为不可变仓位快照指纹。
87个受保护交易/监控/因子进程PID及启动时间保持，交易所持仓及普通/算法挂单仍为0。
最终回执`manager_entries_verified.json`；完整私有备份在：

```text
~/crypto_cta_manager/maintenance-backups/rx03_rx04_manager_entries_20261103T022116Z/
```
