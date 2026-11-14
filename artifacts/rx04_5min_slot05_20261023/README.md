# RX04 5min：复用原RX02的05号槽位，100 USDT、倍率1实盘

2026-10-23 UTC，用户授权接入新LTP凭据、替换原RX02槽位并明确标注5min，
随后授权开启、倍率1，并最终选择按当前100 USDT重算后开启。目标为JP
`jp-meta-elvpn`，Binance USDT合约，RX04现有
15币、35因子、48项有符号绑定。

- Manager与虚拟账户别名：`RX04 5min`；虚拟账户 `virtual05`。
- 新实账户source/环境：`binance_exec_rx04_5m05`，account槽位 `trade05`。
- 复用 `/exec_trade05/`、loopback10045/18165；六个Exec进程已在线。
- 策略配置页保持 `/cta/binance-cta-perp-rx04/config/`，周期5m、enabled=true、
  max_pos_u=100。05:57 UTC开启，实账户跟随倍率1，35个有效绑定份额均为1。
- 05:40 UTC按用户要求，35项绑定改用
  现有 `lgq_grid_prep`，共享参数不变：合约3档、10秒一轮、单手100 USDT、
  Vol倍数1、目标容差10 USDT、无限Maker重挂。风险参数沿用旧槽位
  （400单/分钟、300单/10秒）。
- Manager source的enabled与monitor_enabled均为true。已产生实际订单及成交。
- 启用前只重启仓位signal，利用既有5min因子历史按当前配置静默重算。
  48组信号全部就绪，26组非零仓位的entry_notional均为100；publisher未重启。

新凭据只读鉴权后确认属于不同portfolio，不能将原RX02的成交账本当作新账户。
因此保留旧 `binance_exec_trade05` 的source、数据和私有配置，继续禁用；
仅移出旧source的gateway/Exec URL，将原页面鉴权指向新source。
新账户使用独立IPC namespace、Redis前缀和空数据目录。
凭据通过无回显输入和SSH stdin传入，存放于远端0600 env.sh；没有复制到仓库。
启动前新账户只读校验为NORMAL/NET，10000 USDT、无非USDT余额、无合约仓位、无挂单、
无显式RapidX借款。`local_ips=["172.31.35.228"]`；Manager Kline排除列表同步。

本地arbmm `f9424e8d` release构建6个程序后，经标准 `publish-exec.sh --skip-build`
上传、SHA-256读回并原子安装37份程序/脚本，通过标准start-exec启动六个进程。
Manager PID2010667全部线程CPU24；signal PID2010605、配置、dashboard及六个Exec
进程全部线程CPU mask为0–5,48–95；因子publisher仍为PID705737/CPU28。
89个其他受保护进程身份、新版RX02/Exec08绑定保持；本次只修改RX04策略开关，
其余受保护环境配置哈希保持。48项目标与Redis执行配置一致，待投递0、无发布错误。

公开验收JSON在本目录，私有备份及完整操作回执保留于远端
`~/binance-cta-perp-rx04/run/slot05_20261023/`。
`ops_remote.py`是本次分阶段操作记录，不是日常重复运行脚本；各阶段要求未变更的
前置状态。历史或重复调用会被断言拒绝。

`catalog_configured.json`记录初次接入时的ctaker配置；最新下单配置见
`grid_verified.json`，替换前备份为`grid_before.json`，操作脚本为`set_grid.py`。
该次仅通过Manager API替换virtual05的下单模板引用，随后验证35份运行配置
均为Grid、目标全0、有效份额0、待投递0，所有进程身份及RX02绑定保持。

`activate.py`记录本次重算、启动监控、倍率1和开关启用的分阶段操作；
`activate_100/preflight.json`、`recovered.json`、`enabled.json`及`live_verified.json`
记录前后状态。`audit_live.py`只读取执行和交易账户状态，`execution_summary.json`
及`activate_100/execution_*.json`记录真实持仓、挂单、完成度和CPU核验时间。
私有Manager配置备份只留远端，不下载或提交。

2026-10-28整理记录后，重复`execution_*.json`、预检和恢复过程快照仅在本地
保留并Git忽略；`enabled.json`、`live_verified.json`、`execution_summary.json`、
发布清单、最终验收和历史操作脚本继续纳入Git。

06:02 UTC验收：26组初始目标中24组已达到目标或10 USDT容差，余下2组VET
仍有Grid限价挂单，按既有无限Maker重挂参数继续执行；没有切换Taker。
实际账户已有11币非零持仓，普通挂单2笔、算法单0笔；信号待投递0、发布错误为空，
六个Exec日志均无ERROR/FATAL。启动时BEL默认杠杆4被交易所上限约束为3，
这是执行链路确认的交易所限制，跟随倍率仍为1。
