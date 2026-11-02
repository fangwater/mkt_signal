# OKX / Binance CTA Spread

1min 策略族 `cta_cross_spread`，配置 `mode = okex_binance`。OKX 仍通过已有的
原生统一账户 Exec 执行，部署 venue 为 `okex-futures`，本策略只发布该账户的
USDT 永续目标。Binance 使用独立的 `binance-futures` Exec 合约账户。两个账户
分别维护原有账户模式、namespace、执行账本和持久化数据。

因子和 NQ 沿用 Binance 永续的 1min 计算；价差把旧 Spread 的现货价格替换成
OKX 永续价格：`(okex_mid - binance_mid) / okex_mid`。同一分钟末盘口配对，
2880 槽、至少 1440 对，入场使用前一分钟价差分位，NQ 和因子门槛、反转及退出
规则与原 Spread 一致。OKX 行情缺失或未就绪时阻止入场，因子退出仍可执行。
Kafka 的两路 topic 是 `binance-futures` 和 `okex-swap`；不以 `okex-futures`
作为 Kafka topic。这份实现按两路 Kafka 可用的前提准备，尚未进行实盘验收。

方向在 OKX 永续开仓。若开多得到 +2 ETH，目标为 OKX +2 / Binance 0；退出后
目标为 OKX +2 / Binance -2；反向入场得到 -3 ETH 时一次替换为 OKX -3 /
Binance 0。数量均为基础资产数量，由现有 Exec 规则处理合约张数。持续同向信号
保持冻结数量，不累积库存；删除币种会持续发布两腿显式 clear。

每个 symbol × factor 的一份仓位策略一次发布完整两腿：

```json
{
  "strategy_name": "okex_binance_cta_spread_demo_baseline_005",
  "targets": {
    "okex-futures:ETHUSDT": {"qty": 2, "signal": 0},
    "binance-futures:ETHUSDT": {"qty": -2, "signal": 0}
  }
}
```

Manager 将该策略分别绑定至 OKX / Binance 账户，各账户只接收属于自己的腿。
账户份数应配对配置；单独停止一腿会改变跨所净敞口。两个 REST/Exec 目标发布
独立完成并重试，不提供交易所之间的原子执行保证。Manager 在目标归档中冻结
各账户的实际投影与份数，NAV、FIFO、Funding 和交易费用仍逐账户逐市场计算。

两个入口复用原有 Spread 状态机和行情计算：

- `cta_cross_spread_factor_model_1m_pub`，默认配置
  `config/cta_cross_spread_factor_model_1m_pub.toml`。
- `cta_cross_spread_signal`，默认配置 `config/cta_cross_spread.json`。
- 模型服务 `model_output/cta_cross_spread-binance-futures-1m-<factor>`。
- 信号状态 `run/cta_cross_spread_status.json`；持久化目标
  `run/cta_cross_spread_position_flags.json`；历史 `data/cta_cross_spread/`。
- 环境名 `okex-binance-cta-spread-<tag>`；因子绑定键保持
  `<env>:cta_symbol_factors`，与原 Spread 分离。

仓库 BTC/ETH × baseline_005 是关闭发布的接口样例，正式币种、因子和符号应由
研究清单配置。共享配置服务支持该 mode 并禁止保存时改变已有环境的市场含义。
观察和核算使用 Manager 的账户 NAV；原 Binance 现货图表不适用于这个策略。

仅准备文件的入口是 `scripts/deploy_cta_cross_spread.sh`，默认 dry-run，
遵守现有本地构建和发布约束。运行环境内使用
`scripts/start_cta_cross_spread.sh --execute` /
`scripts/stop_cta_cross_spread.sh --execute`。环境需要配置独立
`IPC_NAMESPACE`、Manager gateway、发布凭据和 `CTA_POSITION_STRATEGY_PREFIX`。
本次没有创建线上策略、绑定账户、部署或启动任何进程。
