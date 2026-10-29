# OKX 统一账户 Exec 的现货与永续

原生 `okex-futures` Exec 同时管理该账户的 USDT 现货和 USDT 永续。
账户仍需为 `acctLv=3`（多币种保证金）或 `4`（组合保证金），永续使用
`net_mode`；本次接入不修改交易所账户模式或自动借币设置。

Manager 可在同一策略里下发两种目标，数量均为标的币数量：

```json
{"strategy_name":"basis","targets":{"BTC":1,"BTCUSDT":-1}}
```

`BTC` 为 BTC/USDT 现货目标，`BTCUSDT` 为 BTC-USDT-SWAP 永续目标。
Batch、POV、CTaker、Chase 和 Grid 共用已有执行路径。现金目标只在现货
reloader 内扩展成 `BTCUSDT`，规则、行情、订单及仓位账本始终保留 venue：
现货为 `okex-margin`，永续为 `okex-futures`。Manager 在同一个 Redis 事务中
写入两个市场的完整目标；省略原有现货目标会清空该市场目标，避免遗留持仓指令。
FIFO 先分市场计算再汇总。

现货沿用统一账户 `tdMode=cross`，并从 SPOT 产品清单获取 `instIdCode`。
市价买单显式设置 `tgtCcy=base_ccy`，现货数量不乘永续合约乘数。
现货仓位来自币种净权益，包含借币本金和利息；能否借币遵循账户设置与交易所约束。
不要让其他环境同时管理同一账户的现货库存。

Manager 每 60 秒分别发布 SPOT 和 SWAP 完整规则，现货只接受 USDT 报价，
数量步长/最小量均为标的币单位，只有 `live` 状态可新下单。Exec 启动前先验证
两个规则缓存，然后分别清理 USDT SWAP/SPOT 普通挂单并查询确认清空。
私有启动查询/撤单绑定 `trade_engine.toml` 的首个交易源 IP，启动不会改动该配置。
Exec 同时订阅两个市场的行情和账户状态；现货、永续快照各自完成后才可初始化其仓位。

部署入口仍为 `scripts/deploy_exec.sh --env-name <env> --venue okex-futures`，
源账户仍以 `okex-futures` 注册到 CTA Manager。部署脚本准备 `spread_pbs/okex-futures`
和 `spread_pbs/okex-margin` 的二进制及启动脚本，但不启动它们。升级需要同时发布
Manager 与 Exec，并确保现货 BBO、成交量及波动率服务可用。
本次代码修改不包含生产部署或实盘验单。

已知边界：`acctLv=4` 虽能通过账户校验，但现有永续固定杠杆初始化仍未绕过
组合保证金限制，失败会阻止相应永续目标激活；本次现货接入不解决该缺口。
OKX 的分钟理论净值等分析功能也不在本次变更范围内。

外部契约：[OKX API](https://www.okx.com/docs-v5/en/)、
[统一账户现货交易说明](https://www.okx.com/en-sg/help/how-can-i-do-spot-trading-with-the-jupyter-notebook)。
