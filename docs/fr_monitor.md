# FR Monitor

独立的后台巡检与统一看板，参考 `../crypto_cta_manager/src/monitor.rs` 的检查、告警节流和恢复语义。
代码在 `crates/fr_monitor/`；现有交易进程不需要升级。Monitor 只读本机行情 IPC、
各盘子 Viz `/snapshot` 和持久化 RocksDB。钉钉是唯一可选的外部写入，必须显式传入
`--execute`。默认和 `--dry-run` 均不读取 webhook 环境变量，不发送通知。

## 当前盘子配置

`config/fr_monitor.toml` 是 2026-10-08 对 `jp-meta-elvpn` 只读核对后整理的清单：

| 交易所 | 启用盘子 | Viz 端口 |
| --- | --- | --- |
| Binance | `binance_fr_arb02`–`04` | 20132–20134 |
| Gate | `gate_fr_arb01`–`03` | 20121–20123 |
| Bitget | `bitget_fr_arb01`–`03` | 20151–20153 |

`binance_fr_arb01` 保留为 `enabled=false`：核对时其 Viz 未响应，未找到该环境目录下的运行程序。
重新上线该盘子时应复核端口、namespace 和进程再启用。

默认市场检查范围是 Binance、Gate、Bitget、OKX 的现货/合约 BBO 中的 `BTCUSDT`、`ETHUSDT`。
它检测这两个代表币种的行情链路；**不会宣称覆盖所有在线币种**。需要逐币检查时，
在相应 `sources.markets.symbols` 中明确加入该 venue 的内部 symbol，使用其真实合约名称；
不能从资产名推测带乘数的合约代码。每个 symbol 独立计时，不以其他活跃币种掩盖停更。

敞口检查覆盖 Viz 返回的所有资产。默认单资产 1,000 USDT、绝对敞口合计 5,000 USDT，
均需结合盘子规模调整。这些是监控阈值，不会写入交易风险配置。UniMMR 开仓/强平线、
杠杆上限直接读取对应 pre-trade 快照。

## 检查与告警

行情巡检与盘子巡检独立调度，共用一个只读服务和看板：

- 行情以 venue 为单位，每 5 秒检查一次（`market_poll_secs`）。`sources.markets` 声明
  盘子依赖，启动时合并为 venue → symbol → 盘子集合；每个 venue 只订阅、检查和告警一次。
  当前 9 个盘子共用 6 个行情源，另通过顶层 `markets` 保留 OKX 现货/合约两路共享行情，共 8 路；现货和合约分别检查。逐币结果用于计算真正受影响的盘子，
  同一 venue 部分币种恢复时继续保留该行情事件，全部恢复后才发送恢复通知。
- 每个盘子独立按 `poll_secs`（默认 10 秒）读取风险、敞口及订单，完成后立即更新看板。
  一个盘子 HTTP 超时或订单读取阻塞不会阻塞行情和其他盘子。巡检超过
  `3 × poll_secs + 30` 秒未完成时标为未知，既有风险保留但不作为已确认风险重复发送。
- 两个钉钉通道各有独立发送任务，发送期间不持有巡检状态锁。

检查内容：

- 行情：有效 BBO 的接收时间和可用的交易所时间；默认 30 秒未更新异常。
  Binance 现货没有交易所时间时使用接收时间，并排除连接初期的保留样本。
- 风险：pre-trade 快照和每条账户原始风险快照分别检查时间，默认 90 秒过期。
  检查权益非正、杠杆超上限、账户 warning/reduce_only/liquidation 和 UniMMR 阈值。
- 敞口：逐资产 `abs(net_usdt)` 和这些绝对值的合计。不同资产不能正负抵消。
- 订单：复用 persist_manager 的持久化解码器，合并 `uniform_orders`、`order_updates`、
  `trade_updates` 及两类 unmatched 表。按 venue + symbol + client_order_id 隔离。
  已成交/撤销等终态，包括 unmatched 中的终态，会消除停滞；历史 unmatched 记录本身
  不构成当前异常。默认 300 秒无进展提示核对，长期预期挂单也可能触发此提示。
- 启动扫描最近一小时；之后按游标增量读取，并保留 60 秒交叠。活跃订单留在内存中，
  不会因滑出读取窗口就“恢复”。读取失败不推进游标。每 CF 超过扫描上限会明确报数据未知，
  不把截断结果当成完整事实。**重启后的检查不能证明启动窗口以前的全部订单已经结束**；
  它不是交易所 open-orders 对账工具。迟于交叠窗口写入的旧时间戳记录也可能不在增量扫描内。
- 数据读取失败/过期时保留上一条风险为“待重新确认”，不会生成虚假恢复通知。

普通异常持续 30 秒后发送，critical 立即发送；未解决的同一异常默认每五分钟重复，
严重程度改变会提前发送，恢复只发送一次。数字变化不会每轮触发新通知。
行情告警作用域为 `market/<venue>`，包含异常币种摘要和受影响盘子，走 CTA 的 market webhook；
订单、风险、敞口仍按盘子归属，走 order webhook。不会把同一行情故障复制成多个盘子故障。
失败使用有上限的指数退避；只有钉钉 HTTP 成功且 `errcode=0` 才确认送达。
每通道每轮最多发送 4 条，其余保留待发；发送期间发生恢复或复发也会继续保留对应通知。

告警状态、订单增量游标、最近 360 次权益/敞口采样及最近 100 条通知记录只保留在内存。
重启后重新建立状态，持续异常可能再次首报。这里的权益曲线不是扣除出入金后的收益率，
也不替代 crypto_nav_manager 的收益归因。

## 构建与本地检查

```bash
cargo test -p fr_monitor
cargo test -p persist_manager --features runtime --lib
cargo build -p fr_monitor
./target/debug/fr_monitor --config config/fr_monitor.toml --once --dry-run
```

该配置的路径和端口属于 JP 主机。本地缺少这些数据源时应显示数据未知，不能把它当作
线上巡检结果。单次模式打印 JSON；常驻模式同时运行只读 HTTP 服务：

- `/`：行情总览与盘子总览两个页签。行情页显示 venue、逐币状态、依赖关系及受影响盘子；
  盘子页分别显示交易侧状态和依赖行情状态，支持交易所/盘子筛选、仅异常、资产明细及趋势。
- `/api/status`：独立的 `markets`、`sources` 快照、未解决异常、通知记录及采样；
  `market_ts_ms` 和每个 source 的 `checked_ts_ms` 分别记录巡检时间；`Cache-Control: no-store`。
- `/healthz`：行情和每个盘子的巡检是否都在更新；返回 `market_ok` / `sources_ok`。
  任何一路尚未完成或停更均返回 503。200 不表示被监控行情或盘子健康，业务异常在 `/api/status`。

快照缺失或账户数据过期时，总览合计留空，明细中的最后已知值明确标注。
只允许绑定本机回环地址，默认 `127.0.0.1:18180`；通过 SSH 转发或已有鉴权网关访问。
所有数据接口都只有 GET。

2026-10-11 起，行情巡检由独立 FR Monitor 接管，NAV 移除行情网络页面，
`public-infra-monitor.service` 停用以卸载常驻 BPF。FR Monitor 使用业务 BBO 的
逐币新鲜度判断行情状态，不保留原采集器的 TCP/BPF 网络诊断和 24 小时网络历史。
当前部署目录为 `/home/ubuntu/fr_monitor`，使用 `--dry-run` 常驻运行；用户明确
要求暂不启用新告警，因此不读取 webhook、不发送钉钉消息。

4191 网关通过 `deploy/nginx-fr-monitor.conf` 提供 `/fr-monitor/` 及其
`api/status`、`healthz` 子路径，复用 NAV 管理员会话鉴权。先在 `/nav/` 登录管理员，
再访问 `/fr-monitor/`；普通用户和未登录访问不获授权。

## 发布和启动

生产发布仍遵守仓库规则：在与 `origin/arbmm` 同步的本地 `arbmm` 工作树中构建 release，
上传临时路径，核对 SHA-256 后原子安装。不得在生产主机运行 Cargo。

```bash
cargo build --release -p fr_monitor
```

建议独立目录 `~/fr_monitor/`，包含：

```text
fr_monitor
config/fr_monitor.toml
config/iceoryx2.toml
scripts/start_fr_monitor.sh
scripts/stop_fr_monitor.sh
env.sh
```

`iceoryx2.toml` 必须与该主机的行情 publisher 使用同一 IPC 配置。Monitor 用户必须能
只读打开各盘子的 RocksDB，并访问行情 IPC。没有权限时会显式报错，不更改数据目录权限。

`env.sh` 在主机本地配置下面的环境变量；可沿用 CTA monitor 已有 webhook 的值，
不得把这些值写入 Git、聊天或命令输出：

- `CTA_DINGTALK_MARKET_WEBHOOK_URL`
- `CTA_DINGTALK_ORDER_WEBHOOK_URL`
- 可选签名密钥：在 TOML 的 `market_secret_env` / `order_secret_env` 指定其环境变量名。

先从独立环境目录验证：

```bash
cd ~/fr_monitor
./fr_monitor --config config/fr_monitor.toml --once --dry-run
./scripts/start_fr_monitor.sh --dry-run
# 经授权启用实际钉钉推送时：
./scripts/start_fr_monitor.sh --execute
./scripts/stop_fr_monitor.sh
```

start/stop 包装只管理独立的 `fr_monitor`。stop 会核对并清理与部署可执行文件绝对路径匹配的
遗留进程，以免 supervisor 已删除注册但真实进程仍存活。命名可由 `FR_MONITOR_PROCESS_NAME`
覆盖。配置变更需要重启 monitor；不需要重启交易栈。
部署后如新增 CPU 绑定或 source IP，必须更新 living ops docs；本实现不指定交易 source IP。
