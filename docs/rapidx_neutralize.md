# RapidX Binance 现货保留、合约中性化

`scripts/neutralize_rapidx_binance.py` 是人工运行的一次性工具。默认仅 GET 查询；
只有操作者传入 `--execute` 后才提交合约限价单。不会启动交易进程、买卖现货、
改杠杆、借还币或修改 CTA 的内部 lot 状态。

每币 `现货净量 = balance - debt`，`合约目标 = -现货净量`，
`合约调整量 = -(现货净量 + 当前合约量)`。数量向零取整到 RapidX `sym/info`
规定的步长，遵守 minSize/minNotional，不为凑最低金额扩大敞口。工具要求
Binance NET 模式、contractSize=1、USDT 永续；存在其他合约、显式 RapidX loan、
未识别的资产、未完成订单或 algo/TP-SL 时停止，由操作者先核对。

例如 ETH 现货 `+0.051622673`、合约 `+0.272` 时，按 `0.001` 步长调整为
卖出 `0.323 ETH` 合约，合约变为 `-0.051`，净敞口剩 `+0.000622673 ETH`。
该委托需要穿过合约零仓位，不能使用 reduce-only；完全缩减已有合约的委托才使用它。
这是计算示例，脚本每次重新查询实际账户，不使用文档中的固定数量。

## 操作命令

将该脚本及依赖 `scripts/binance_local_ip.py` 安装到环境的 `scripts/`。
从运行主机进入目标环境后，先读入环境配置，再预演。以下 RX01 命令由操作者运行：

```bash
ssh jp-meta-elvpn
cd ~/binance-cta-rx01
set -a
source ./env.sh
set +a
python3 scripts/neutralize_rapidx_binance.py --env-dir "$PWD" --ioc-fallback
```

预演显示账户净量、目标合约、预计调整量、剩余零头和当前 maker 价格。
确认符合预期后，由操作者执行：

```bash
python3 scripts/neutralize_rapidx_binance.py --env-dir "$PWD" --ioc-fallback --execute
```

默认每币最多两次 post-only（GTX），每次等 20 秒；未完成量先撤单并查询到终态。
带 `--ioc-fallback` 才在最后增加一次 IOC 限价尝试，价格最多偏离当时对手价
2 bps；全程相对该币首个执行盘口中点的价格漂移上限为 10 bps。
这些是价格限制，不包含手续费，也不保证成交或保证最低总成本。
不带 `--ioc-fallback` 为纯 maker；没有市价追单。
单笔上限 1000 USDT，本次累计成交的保守估值上限 2000 USDT；超限会停止。
限价和参数可用 `--help` 查看。行情直接查询 Binance `/fapi/v1/depth`，不经过缓存；
签名请求直接访问 RapidX，source IP 复用环境的既有配置解析器。

操作前及每轮下单前检查该环境交易进程已停、普通和 algo 挂单为空。
持仓必须仅由此次工具调整；其他机器或手工交易造成的持仓变动不能被本机进程检查排除。
同一环境通过文件锁阻止本工具并行运行。account_monitor/persist_manager 应保持运行。

## 中断与复核

每次提交前，先将请求与 clientOrderId fsync 到
`run/rapidx_neutralize.json`。下单超时只查询原单，不重新提交；撤单 ACK 不算终态。
确认终态后，用实际 executedQty 核对交易所合约仓位，匹配后才允许下一单。
每单成交价、手续费、返佣和剩余净量保留在同一文件中。

中断、未知下单结果或仓位未完成同步时，pending 记录阻止继续下单。
由操作者运行以下命令仅查询/撤掉该工具自己的未完成单并复核，不新开订单：

```bash
python3 scripts/neutralize_rapidx_binance.py --env-dir "$PWD" --execute --recover-only
```

仍无法确认原单状态时工具继续停止；不要删除 pending 文件来绕过检查。
成功完成或回收后可以重新预演。完整流程结束再查询所有挂单与实际仓位：
退出码 0 表示执行完成后仅剩零敞口或低于交易所最小规则的零头（dry-run 的 0
只表示预演完成），2 表示仍有可交易差额，1 表示检查失败或结果不确定。
始终查看逐币净量，不能把不同币的 USDT 多空金额相抵当作逐币中性。
借币利息会继续改变净量；本工具是一次性操作，不是持续对冲服务。

## RX01 ETH 只读调查

2026-10-13 从现有 RapidX 账户原始 journal 核对：10 月 12 日 22:13 UTC，
ETH 合约连续 9 笔 BUY，各 0.039，总计 0.351；10 月 13 日 00:38 UTC，
现货/杠杆侧又成交 4 笔 BUY，各 0.04，总计 0.16。停盘快照为现货净量
约 +0.051623、合约 +0.272。该同向敞口有真实成交支持，不是页面符号错误。
是否因旧 CTA lot 退出重复/过量，尚未与完整 lot 记录闭合验证，不能据此断言根因。
旧交易进程已停；该工具独立读取实际账户，不承接旧待对冲队列。

接口依据：[RapidX 下单](https://apidocliquidity.readme.io/reference/place-order)、
[撤单异步语义](https://apidocliquidity.readme.io/reference/cancel-order)、
[订单查询](https://apidocliquidity.readme.io/reference/query-order)、
[交易规则](https://apidocliquidity.readme.io/reference/sym-info)、
[现货资产](https://apidocliquidity.readme.io/reference/get-portfolio-assets-details)、
[Binance 盘口](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book)。
