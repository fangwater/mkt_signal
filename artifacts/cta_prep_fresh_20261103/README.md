# RX01 / RX03 全新执行账户

按用户更正后的范围处理 RX01、RX03，保持交易关闭。

| 账户 | 新 source / IPC / 运行目录 | 停止绑定 |
| --- | --- | --- |
| RX01 | binance_exec_rx01 | Prep01 1min / 5min / 15min |
| RX03 | binance_exec_rx03 | Prep02 1min / 5min / 15min |

两账户仍使用原 RapidX 交易账户及原凭据。原 Exec06 / Exec07 运行目录在停止所有进程后
整体同盘重命名归档，含订单 RocksDB、账户 journal、运行状态及配置；新目录不复制任何
旧 data/run。旧 source 从 Manager 配置和入口移除，绑定删除，Redis 前缀备份后清除。
旧 PostgreSQL 不可变历史保留在退役 source 下，与新账户隔离，不迁移任何初始仓位、
分配纠正或盈亏锚点。新账户的订单历史、策略目标和累计盈亏均为0。

RapidX 监控删除自动历史成交补抓及回看参数；实时 Trades 继续持久化，本地 journal
仍可重放已经收到的事实。新 journal 的账单采集从会话当前时刻开始，重启后续接账单游标。
原账户快照、借款查询及真实余额继续读取。RX01 的现货钱包有零碎余额，未出售，
未导入新 Prep 永续策略的初始持仓。两账户实际永续仓位及普通/算法挂单均0。

新监控本机 release 构建，381项测试及完整格式检查通过；其他五个二进制复用既有本机
release 产物。全部上传至显式 staging，核对 SHA-256 后原子安装，未在服务器编译。
只通过环境脚本启动 persist_manager、account_monitor、Viz、Config。原入口URL与端口
保留，Nginx 鉴权头改指新 source，LGQ configure 权限随账户迁移；Manager 重启加载。
原风控、凭据、源IP及其他15条账户绑定保持，77个其他受保护进程身份保持。

两个新监控均 recovery_ready=true，待落库数量0。六条 Prep 绑定和 Spread 两侧绑定
共八条 physical bindings 全部 shares=0，七份 signal.enabled=false，下单进程停止。
检查时 Prep 计算在线，404项最新输出中194项ready，并存在 Kafka No more messages
状态错误；Spread计算停止。没有因此开启任何推送或交易，详见 signal-precheck.json。

完整私有审计（含旧运行目录、凭据、Redis及Manager配置备份）仅在远端：
`~/crypto_cta_manager/maintenance-backups/cta_prep_fresh_20261103T063500Z/`。
本目录只保存脱敏回执。operate.py 的 apply 需要 --execute；verify 为只读。
