# RX04 CTA Perp 5min

新增 bin：`cta_perp_factor_model_5m_pub`、`cta_perp_5m_signal`。
本目录为用户指定的 15 币、35 因子、48 项有符号绑定；默认 `enabled=false`，
单份金额先沿用 Perp 模板的 200 USDT，尚未部署或绑定 Manager/Exec。

| 环节 | 数据与窗口 | 最少有效样本 |
|---|---|---|
| 因子公式 | 从 Kafka 原始成交/盘口聚合的完整 5min bar；公式窗口按 bar 计 | 各公式自身要求 |
| 因子 z | 288 根（1 天），沿用因果截尾及标准化，clip=3 | 200 根 |
| 因子分位 | 576 根（2 天），负向因子使用 `1-rank(z)`，等价于并列值取中点的 `rank(-z)` | 288 根 |
| NQ 极值 | 本币最近 24 个有效 mid，包含当前价 | 有效 mid 即参与 |
| NQ 门槛 | 最近 24 个有效 buy/sell change 的线性插值 Q50 | 各 12 个 |

NQ 的 mid 取本币 5min 区间最后一分钟内最后一个有效一档盘口中间价。
无该分钟有效盘口时，该根 NQ 缺失，且不推进有效样本窗口。因此 24 根名义上为
120 分钟，有缺口时实际跨度会延长。`buy_chg >= Q50(buy_chg)` 允许开多，
`sell_chg <= Q50(sell_chg)` 允许开空；相等时放行，NQ 不影响持仓及因子退出。

UTC 区间 `[T,T+5min)` 的图表及历史 `bar_start_ms` 是 T；IPC 和历史
`model_ts_ms` 保留可用时间 T+5min，执行报价取收盘后到达的盘口，避免未来数据。
因子/NQ 的历史记录和实时 IPC 携带同一组值与门槛，回放复用同一开平规则。

5min IPC 使用 `model_output/<scope>/cta_perp-binance-futures-5m-<factor>`，
1min 通道仍为原名称。两端设置相同 `CTA_PERP_MODEL_SCOPE=binance_cta_perp_rx04`；
环境目录用独立的 `binance-cta-perp-rx04`，Redis key 自动按该目录隔离。
5min 因子历史默认在 `data/cta_perp_5m/factor_history`。现有 1min NQ 语义不变。

准备部署文件的入口（默认仅打印计划）：

```bash
scripts/deploy_cta_perp_5m.sh --env-name binance-cta-perp-rx04
```

实际发布仍遵守仓库的本地 release 构建、上传校验及环境本地启停流程。
安装脚本把本目录三份配置安装到环境的 `config/`，保留既有配置，并拒绝跨周期覆盖。
`start_cta_perp.sh` / `stop_cta_perp.sh` 根据 `cta_perp.json` 的 `bar_interval`
选择相应 bin；也提供独立的 5min publisher/signal 启停脚本。
配置服务、回放和图表按该字段选择 5min 数据，不支持运行中切换周期。

Kafka 配置预热 5 天，公式就绪仍取决于实际保留的原始行情。本清单 35 个公式
已核对仅依赖价格、成交量及盘口，不依赖成交大小分类阈值。上线前另行配置独立的页面端口
及 Manager 目标；本次代码和离线测试不代表在线行情完整性验收。
