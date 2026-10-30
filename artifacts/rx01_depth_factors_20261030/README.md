# RX01 全深度因子框架更新

2026-10-30 UTC，JP `binance-cta-perp-rx01`。计算实现提交 `36ce1491`，
最终盘口 band 边界修正 `6f49c3e9`。

本次直接复用 `depth_pub::orderbook::OrderBook`，将 Kafka 保留的盘口消息合成
全档深度 baseline，新增用户提供的 23 个 `depth_*` 公式，再接入现有 CTA Perp
1m/5m/15m 归一化、分位和发布链路。未变更原有 IPC 消息格式。

RX01 安装本地 arbmm release 构建的 15min publisher、15min signal、因子名称校验脚本、
allowlist 和说明文件。上传逐项校验 SHA-256，同目录临时文件校验后原子替换。
`manifest.json` 与 `receipt.json` 记录首次更新的源提交、安装文件和前后哈希，
`boundary_fix_manifest.json` 与 `boundary_fix_receipt.json` 记录最终二进制。
校验脚本来自该提交的 `git show`，没有捎带工作区并行的容量修改。

操作员的 `env.sh`、交易配置、空币种因子表、空发布 universe 哈希保持不变；
`enabled=false`、`factor_rules=[]`、无 Manager 绑定。未启动四项常驻服务，
CPU17 仍预留，10195/19185 仍关闭。发布期间 57 个 CTA/Exec/共享行情受保护进程
的 PID、启动时间和 cwd 保持一致。未改动账户、订单、source IP、ENI 或路由。

`probe.toml` 仅用于 BTCUSDT/ETHUSDT 的独立 Kafka 回放。通过
`--replay-only --core 17` 回放至 Kafka 头，不创建 IPC 发布器，不调用交易接口，
不写入 RX01 正式配置或正式因子历史。使用真实 15min 预热要求：zscore 96/67，
因子分位 192/96，NQ 120/60。回放结果、近期样本及各因子有限值/就绪计数由
`verification.json` 记录；不能据此宣称尚未配置的 RX01 已经启用交易。

完整 Kafka 回放运行于 `36ce1491` 的首次构建。回放期间补测发现，先相减再换算 bp
可能将恰好 1bp 的价格因浮点误差排除；`6f49c3e9` 改为直接比较 band 边界价格，
四项针对性 Rust 测试通过后重新本地 release 构建并替换两个二进制。
验收分别记录回放二进制 SHA-256 与最终安装提交，避免混淆两次产物。

净流量两项严格保留原始 `(bid_net-ask_net)/(bid_net+ask_net)` 和分母大于零条件。
由此产生的 NaN 可能导致 67/96 的 zscore 有效样本不足；验证程序会如实记录
未就绪的净流量因子，不以绝对值分母、补零或较低门槛替代原公式。

验证：Rust 库 1401 通过、3 忽略；CTA Python 108 通过；新增 Python 对照测试 2 通过。
Rust 逐项核对原始 Python 生成的 11 行 × 23 项基准，并验证超过20档的统计、
绝对量增删、快照排除、消息边界、无成交 bar 和 5s→1m→15m 存量/流量汇总。
四个相关入口 cargo check 与 cargo fmt --check 通过，本地两个 release 构建成功。

远端备份和回放留在
`~/binance-cta-perp-rx01/run/depth_factors_20261030/`；临时上传目录验收后移除。
09:05 UTC 验收：BTC/ETH 各195根，最新已完成 bar 为09:00，各22/23项就绪；
`depth_flow_net` 均未达到有效样本门槛。57个受保护进程保持，正式服务未启动。
临时上传目录已移除。后续仍待操作员提供新币种、因子方向与逐组 NQ，再配置和预热正式环境。
