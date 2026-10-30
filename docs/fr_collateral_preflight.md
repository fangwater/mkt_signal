# FR 启动抵押检查

最后更新：2026-10-30。

FR 的 `start_trade_signal.sh`、`start_fr_pre_trade.sh` 在停止旧进程之前执行
`check_fr_collateral.py --execute`。`start_fr_signal.sh` 转调前者。
`start-jp-meta-fr.sh` 在重启任何组件之前执行同样的检查；其 `--check-only`
仅查询，不修改抵押。

检查读取部署目录的 `env.sh`、`trade_engine.toml local_ips[0]`，以及该环境的六类
Redis 币对列表：`dump_symbols`、`pos_dump_symbols`、`trade_symbols`、
`fwd_trade_symbols`、`bwd_trade_symbols`、`unimmr_close_symbols`。以这些列表的并集
和 USDT 为范围，包含尚无余额的在线币。任何配置、认证、网络或响应字段异常都让启动失败。
同一币只有在交易所支持抵押、用户未开启时才执行补开，之后重新读取交易所状态确认。
默认 CLI 为只读；显式 `--execute` 才能修改抵押。没有跳过启动检查的参数。

| 交易所 | 检查和补开方式 |
| --- | --- |
| Bybit | 校验 UTA 和全仓/组合保证金模式；组合保证金要求现货对冲 ON。读取 `marginCollateral` 与 `collateralSwitch`，逐币设置 ON。USDT/USDC 不提交开关修改。 |
| Gate | 要求跨币种或组合保证金模式，账户未锁定；组合保证金要求现货对冲开启。`is_all_collateral=true` 直接满足抵押配置；自定义模式仅将所需币加入 `enable_list`，保留原有配置。 |
| Bitget | 要求 UTA Advanced 和单向持仓。读取 all/mainstream/custom；仅在必要时将原有抵押币与缺失在线币合并为 custom。mainstream 原有六币全部保留，不替换为仅 FR 币。 |
| OKX | 要求跨币种或组合保证金账户。跨币种模式逐币读取 `collateralEnabled`，以 custom + true 补开。官方将该开关限定在跨币种模式，组合保证金模式不强行套用该字段。 |
| Binance | 按显式 `BINANCE_ACCOUNT_MODE` 分别查询 PAPI 或标准 UM；PAPI 要求账户 NORMAL。没有对应的逐币开关操作，不把标准现货与 UM 余额当成共享抵押。 |

交易所明确不支持抵押的币单独警告，不能通过“开开关”解决。缺失状态字段不算
不支持，必须中止并查明。抵押折算率、额度和 UniMMR 阈值仍由交易所及现有风控处理。
本工具不调整杠杆、账户模式、借贷或资金划转，也不关闭原有抵押币。
POST 失败不自动重试，先回读状态；只有确认所需抵押均已启用才继续启动。

```bash
cd ~/bybit_fr_arb01
python3 scripts/check_fr_collateral.py            # 只读审计
python3 scripts/check_fr_collateral.py --execute  # 补开并回读
```

该检查覆盖上述启动脚本。进程已运行期间热更新币对时，应对更新后的完整列表执行同一
`--execute` 检查；启动检查本身不是定时任务，直接运行二进制也不会调用该 Python 检查。

2026-10-30 已安装启动保护的环境：SG `bybit_fr_arb01`；JP
`binance_fr_arb01/02/03/04`、`bitget_fr_arb01/02/03`、`gate_fr_arb01/02/03/04/05`。
仅更新脚本和依赖，没有重启进程；旧版环境仅插入检查，保留其余现有内容。
正在交易的 10 个环境均用已安装的 `--execute` 路径复核通过。Bybit 额外发现并补开
GRVT、SLX，当前 18 个在线币及 USDT 均通过抵押检查。其余正在交易的 Gate、Bitget
账户均已配置全部抵押，Binance 均为 PAPI NORMAL。
OKX 适配已做离线测试，本次没有运行中的 OKX FR 环境可做实盘验证。
证据及文件哈希见 `artifacts/fr_collateral_preflight_20261030/`；远端备份位于各环境
`run/fr_collateral_preflight_20261030/backup/`，该目录不含 `env.sh`。

官方接口说明：

- [Bybit 抵押信息](https://bybit-exchange.github.io/docs/v5/account/collateral-info)、[设置抵押币](https://bybit-exchange.github.io/docs/v5/account/set-collateral)。
- [Gate Unified API](https://www.gate.com/docs/developers/apiv4/en/unified/)。
- [Bitget 资产及抵押配置](https://www.bitget.com/docs/catalog/account/assets-balance)、[账户设置](https://www.bitget.com/docs/catalog/account/account-settings)。
- [OKX API](https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-collateral-assets)。
- [Binance PAPI 账户信息](https://developers.binance.com/docs/derivatives/portfolio-margin/account/Account-Information)。
