# CTA Perp 15min

源码支持独立的 15min 因子发布器和仓位信号。本目录提供默认关闭交易的配置模板，
沿用 RX04 的 15 币、35 因子、48 个方向组合；上线时按目标策略调整清单与 Manager 绑定。

从 Kafka 的逐笔与盘口数据聚合完整 UTC 15min bar，再计算因子。`model_ts_ms`
是收盘可用时间，`bar_start_ms` 是区间左端；开仓报价取收盘后盘口。
启动时的首个不完整区间会丢弃。

因子归一化与分位按时间跨度换算，NQ 固定保留 120 根：

| 窗口 | 15min 条数 | 最少有效样本 |
| --- | ---: | ---: |
| 归一化 1 天 | 96 | 67（原 5min 的 200 根向上换算） |
| 因子分位 2 天 | 192 | 96 |
| NQ 极值和变化窗口各 120 根（30 小时） | 120 | 60 |

因子公式内部窗口仍按 bar 条数计算，`TP_VPI_006` 沿用 5min 的 280 样本设置。
Kafka 默认回溯 15 天，保留与 5min 模板相同的 1440 根输入预热容量；实际就绪仍要求
公式、归一化及分位窗口样本齐备。NQ 使用本合约每个 15min 区间最后一分钟内的
最后一个有效盘口 mid；缺失样本不推进 NQ 窗口。变化值与线性中位数比较，开多 `>=`、
开空 `<=`，只过滤开仓。未配置逐组规则时，15min 的 NQ 阈值固定为 Q50。
V0 配置 `factor_rules` 后，沿用 RX03 的逐组严格分位比较，支持 `0.50`、
`0.2/0.8` 或 `关闭`；窗口仍为 120 根，不使用线性中位数过滤。

入口：`cta_perp_factor_model_15m_pub`、`cta_perp_15m_signal`。IPC 使用
`model_output/<scope>/cta_perp-binance-futures-15m-<factor>`，默认因子历史目录为
`data/cta_perp_15m/factor_history`。环境目录、Redis 前缀、Manager 虚拟账户和进程名
均需独立；配置服务不允许运行中切换周期。

```bash
cargo build --release --bin cta_perp_factor_model_15m_pub --bin cta_perp_15m_signal
scripts/deploy_cta_perp_15m.sh --env-name binance-cta-perp-15m
```

部署脚本默认仅打印计划；显式 `--execute` 安装文件，也不启动进程。既有环境的
`env.sh`、参数和状态保留。使用各环境自己的 `scripts/start_cta_perp.sh` /
`stop_cta_perp.sh` 管理生命周期，按 `bar_interval=15m` 选择对应入口。
配置服务、历史、回放、仓位页与信号图均支持 15min。此次源码改动未部署到实盘。
