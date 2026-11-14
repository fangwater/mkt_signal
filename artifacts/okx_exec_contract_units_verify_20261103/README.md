# OKX Exec 与跨所仓位表的数量口径核验

2026-11-03 03:00 UTC，只读核验JP `okex_exec_trade01` 的USDT线性永续路径。
合约乘数换算已实现，发生在`exec-pre-trade`生成子订单时；仓位表和Manager始终传递币数量。
本次查询公开合约规格和现有Redis缓存，未提交订单、修改运行配置或启动执行进程。

| 环节 | 数量口径与处理位置 |
| --- | --- |
| 仓位表 | `src/cta_spread/positions.rs:62`，币数量=`USDT额度/入场价格`；退出对冲腿使用相反币数量 |
| Manager | `src/target_proxy.rs:123`（crypto_cta_manager），按venue拆腿、乘跟随份数，保留币数量 |
| 规则发布 | Manager `src/market_rules.rs:488`，读取OKX `ctVal × ctMult`，发布`contract_multiplier` |
| Exec规则加载 | `src/pre_trade/manager_market_rules.rs`，读取`okex_exec_trade01:okex-futures:market_rules`并更新乘数表 |
| 下单换算 | `src/strategy/batch_exec_strategy.rs:921`，子单币数量÷每张币数，再按`lotSz`向下对齐；同时记录实际对应币数量 |
| 交易引擎 | `crates/trade_engine/src/okex.rs:671`，将已经折算的张数写入OKX `sz` |
| 成交回算 | `src/strategy/batch_exec_strategy.rs:3003`，增量成交张数乘回币数量；账户持仓通过Monitor/BasicUmManager按乘数统一为币数量 |

例如2 TRB对应20张（每张0.1 TRB）；2000 DOGE对应2张（每张1000 DOGE）。
仓位表再次除合约乘数会造成重复换算。

本次逐项比对现有缓存与OKX官方`GET /api/v5/public/instruments?instType=SWAP`，
以下8个正式Spread品种及BTC/ETH的`ctVal × ctMult`、`lotSz`、`minSz`、面值币种一致。
公开请求沿用Manager配置的REST源IP，不经过HTTP代理。

| 品种 | 每张币数 | 张数步长 | 对应币数量步长 |
| --- | ---: | ---: | ---: |
| TRB | 0.1 | 1 | 0.1 |
| DOT | 1 | 1 | 1 |
| ADA | 100 | 0.1 | 10 |
| NEAR | 10 | 0.1 | 1 |
| ATOM | 1 | 1 | 1 |
| DOGE | 1000 | 0.01 | 10 |
| ETC | 10 | 0.01 | 0.1 |
| CRV | 1 | 1 | 1 |

原始核验结果、缓存时间与安装二进制SHA-256见`verified.json`；可重现的只读脚本为
`verify_rules.py`。线上`exec-pre-trade`、`trade_engine`、`account_monitor`三个产物与
`artifacts/cta_cross_spread_deploy_20261102/release_manifest.final.json`完全一致。
上述Exec关键源码相对该部署清单记录的`1a10d3b2`无差异。

26项定向测试通过：mkt_signal的8项仓位表测试、13项Manager规则缓存测试、
1项OKX张数回算测试、2项订单计划测试；Manager的1项OKX规则解析测试和1项跨所拆腿测试。
本次没有进行实盘成交验收。

仓位表尚未完成上线：JP没有`cta-prep-binance-futures`或`cta-factor-spread`运行目录；
既有`okex-binance-cta-spread-rx01`只有配置服务，信号enabled=false。
两腿Exec均没有运行`exec-pre-trade`或`trade_engine`，Manager绑定数和实际Exec活跃策略数均0。
因此只能确认仓位表核心源码和数量换算路径，不能声称聚合仓位表已在线运行。

两腿精度尾差仍须覆盖：仓位表保存未量化的币数量，两所Exec独立取整，
未按共同可成交步长或OKX实际成交数量联动。例如103 ADA的理论目标，OKX按10 ADA步长
只能执行100 ADA；Binance若对冲103 ADA，会剩3 ADA净敞口。乘数换算正确并不代表
两腿实际成交天然严格配平；这是上线验收和后续数量对齐需处理的边界。
