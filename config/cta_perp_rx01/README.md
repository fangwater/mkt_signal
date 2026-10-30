# RX01 CTA Perp 15min 框架

新环境名为 `binance-cta-perp-rx01`，复用 Perp 的 15min 发布器、仓位信号和页面，
按 RX03 的独立因子模式支持标的 × 因子的方向及 NQ 规则。

本目录等待操作员提供新清单：`factor_rules=[]`、币种因子表 `{}`，发布器
`universe.symbols/factors=[]`。未复制 RX03 清单，也未配置 Manager 跟随或开启交易。
空 universe 会阻止因子发布器启动。提供清单后，需要同步填写这三处配置并校验
因子支持情况，再进行历史预热；清单不得通过启动时回退到 RX03 或其他环境补齐。

窗口：NQ 固定 120 根、至少 60 个有效样本，连续 15min 数据对应 30 小时；
因子分位 192 根、至少 96 根，完整窗口两天；zscore 96/67，完整窗口一天。
每组 NQ 配置与 RX03 一致：`0.50`、`0.2/0.8` 使用严格分位比较，`关闭`
跳过该组 NQ 过滤。没有每组规则时保留既有 15min 线性 Q50 规则。

准备命令（默认 dry-run，安装也不启动进程）：

```bash
scripts/deploy_cta_perp_rx01.sh
```

运行环境、Redis 前缀、IPC scope、状态和历史与旧 Spread 及其他 RX 环境独立。
预留 publisher CPU17、Dashboard `127.0.0.1:10195`、Config `127.0.0.1:19185`。
正式发布仍使用本地 arbmm release 构建、上传 SHA-256 校验和环境本地启停脚本。
旧 RX01 Spread 的历史与配置归档单独保留，不作为新 Perp 的状态或预热输入。
