# OKX / Binance CTA Spread

1min 仓位表 `cta-spread-okx-bn-1min-pos`，由 `cta-factor-spread` 进程维护。OKX 仍通过已有的
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

每个 symbol × factor 保留独立子仓位和单份 `max_pos_u`；发布时按币种、venue
汇总为 `cta-spread-okx-bn-1min-pos` 一份策略，一次发布完整两腿：

```json
{
  "strategy_name": "cta-spread-okx-bn-1min-pos",
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

当前入口为 `cta-factor-spread`，默认配置 `config/cta_factor_spread.toml`。
组合放在 `config/cta_factor_spread/`，`config/cta_factor_spread/example.json` 是关闭
发布的接口样例；`signal.max_pos_u` 独立维护单份 USDT，`signal.spread` 配置
价差过滤阈值。普通因子和 NQ 使用 Binance 永续，价差使用 OKX/Binance 同分钟
末盘口；因子、前一分钟价差与仓位决策在同一进程内衔接。仓位状态位于
`data/cta_factor_spread/positions/cta-spread-okx-bn-1min-pos.json`，不再维护第二份
Redis 因子绑定表或启动 `cta_cross_spread_signal`。旧独立入口已移出构建清单，
后面的部署记录描述历史准备状态；本轮没有部署或迁移线上仓位。

观察和核算使用 Manager 的账户 NAV；原 Binance 现货图表不适用于这个策略。

仅准备文件的入口是 `scripts/deploy_cta_cross_spread.sh`，默认 dry-run，
遵守现有本地构建和发布约束。运行环境内使用
`scripts/start_cta_cross_spread.sh --execute` /
`scripts/stop_cta_cross_spread.sh --execute`。环境需要配置独立
`IPC_NAMESPACE`、Manager gateway 和发布凭据；策略名由市场对和频段确定。
本次没有创建线上策略、绑定账户、部署或启动任何进程。
