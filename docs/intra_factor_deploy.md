# Intra CTA（V007 两市场期现）

最后更新：2026-09-23

本文记录普通 intra CTA（`ArbMode::Cta`，不是 `cta_special`）的 V007 研究契约与
`jp-meta-elvpn:~/binance-cta-rx01` 部署。研究源码位于
`/home/u171/research/crypto_research/CTA_research/version007_long_short_rust_two_exchange/`，
以 `factor_backtest_baseline489_train2024_2026.toml`、`src/signal.rs`、
`src/filters.rs`、`src/engine.rs` 为依据。单所纯合约 CTA 见
`one_exchange_cta_deploy.md`。

rx01 dashboard 使用 `docs/intra_pre_trade_dashboard.html`，在风险和敞口之外只对
`/cta/binance-cta-rx01/` 显示 CTA 规则、交易标的、执行/价差/风险配置以及
最新模型 bar 的方向、分数、阈值和 NQ 状态。配置读取该 env 的 config server
只读 GET 接口，代表 Redis 中当前配置，最多需要 60 秒才会反映到 `trade_signal`。
信号进程异步写入 `run/cta_signal_status.json`，config server 的
`/api/signal-status` 只读接口提供数据和时效状态；方向是模型 vote，不代表已成交
或已通过价差、冷却与 pre-trade 风控。页面通过 `/config/api/` 访问此接口。
`scripts/publish-cta.sh` 同步页面和 config server；要使信号状态生效，还需
发布新 `trade_signal` 并通过 env 内脚本重启信号与 config server。

## 策略契约

- 每个 env 最多一条独立因子规则；`model_service` 指向对应的 1 分钟
  `model_output/intra-binance-futures-1m-*`。不能把不同参数组的账户、订单或状态合并。
- 因子 score 在当前 bar 内纳入 2880 个固定时间槽的滚动窗口（至少 1440 个有效值），
  大于 q0.9 开多、小于 q0.1 开空，严格比较；`trade_sides` 可设 `long`、
  `short`、`both`。发布端输出分位是 `count(score <= current) / valid_count`，
  与开仓所用的线性插值 q0.9/q0.1 阈值不是同一概念。
- NQ 开仓过滤只用 **现货** BBO 的每分钟最后一个有效 mid，不跨空 bar 补价。
  BUY/SELL 分别与最近 1440 根有效 bar 的 rolling min/max 比较；窗口 1440，
  至少 720 个有效变化值；long 使用 `>= q0.5`、short 使用 `<= q0.5`。
  `nq_change_enabled` 可开关。factor 与 NQ 必须以相同 `(symbol, right_edge_ts)`
  合并后才发布；禁止用上一根过滤值替代缺失值。
- `spread=(spot_mid-swap_mid)/spot_mid` 只管现货 maker 报撤：long 报单
  `< q30`、short `> q70`，分别跨过 q50 撤同向未成交单。live 阈值由
  `rolling_metrics` 和 CTA spread mapping 提供；与研究 `signal.rs` 独立计算
  的阈值仍须逐 bar 核对。
- 现货四档 maker 偏移 `[0, 0.0001, 0.0003, 0.0005]`，每档 100 USDT，
  TTL 120 秒；不对缺现货报价的 bar 虚构挂单。同 symbol 的反向信号先撤原方向
  opening maker，等待撤单终态，已有成交的 lot 拦截反向开仓。
- 成交后不挂合约 maker TP，也没有入场硬止损或最长持仓退出。
  合约侧价格达到盈利 `trailing_stop_trigger_step` 后激活只向盈利方向收紧的
  止损；或者因子分位对 long 严格 `< factor_exit_quantile_long`、对 short
  严格 `> factor_exit_quantile_short` 时，按 lot 对合约侧发 taker 退出；
  单 lot 低于合约最小下单量时，同方向、同退出原因且未预留的 lot 合并借量。
  trailing 优先于因子退出；新成交 lot 下一秒起才能退出。
  因子衰减也撤同向未成交的现货开仓 maker；因子退出分位必须分别
  `< 0.9` 和 `> 0.1`。

## 代码与配置

- 因子/NQ publisher：`src/factor_pub/intra_factor_model_1m_pub/`，配置
  `config/intra_factor_model_1m_pub.toml`。本地 publisher 的
  `INTRA_FACTOR_NAMES` 已包含 `TD_TI_008`（共十个 service）；JP 六个 symbol 的
  TLen `factor_plan_1m` 均已加入该因子。选择其他 V007 factor 时，必须先扩展
  publisher 与 TLen factor plan，不能只在 Redis 填写不存在的 service。
- 开仓：`crates/trade_signal/src/cta_config.rs`、`arb_decision.rs`；
  `{env}:cta_rules` 配 model、方向、NQ 开关等，
  `{env}:cta_strategy_params:{open}:{hedge}` 配四档、TTL、因子退出分位和 trailing。
  JSON 规则及 strategy hash 均不接受 V005 的 `take_profit`、
  `reward_risk_ratio`、`max_holding_seconds`；hash 未知字段/格式错误拒绝热重载。
- 平仓：`src/pre_trade/cta_factor_channel.rs` 订阅和热加载 model；
  `src/strategy/intra_trailing_stop.rs` 按现货 lot 身份管理因子/trailing 退出；
  `src/strategy/arb_hedge_strategy.rs` 只在退出触发后对合约发定向 taker。
  `src/strategy/cta_special_strategy.rs` 提供共用的因子衰减方向判定。
- 启动 pre-trade 前必须先写入一条有效 CTA rule；缺失或 `[]` 时拒绝启动，
  防止信号后续热加载但退出订阅仍为空。运行中写入 `[]` 只停新开仓，
  pre-trade 保留已有持仓的因子订阅。
- 配置界面：`scripts/cta_config_server.py`；写入校验：
  `scripts/sync_cta_rules.py`；读回：`scripts/print_cta_rules.py`。
  切换 `rule_id` 或 `model_service` 必须停止信号并人工核对所有未平 lot 后
  重启 pre-trade，不能在已有持仓时热切换到另一条因子。

## 部署与验收

2026-09-23 将 `binance-cta-rx01` 从 `TD_PR_005` 切换至 `TD_TI_008`，交易范围为
BNB、BTC、DOGE、ETH、SOL、XRP 的 Binance margin/futures（RapidX）。使用
`trade_sides=both`、`nq_change_enabled=false`、四档每档 100 USDT、TTL 120 秒、
多空因子退出分位 `0.3/0.7`、移动止损触发/移动档位 `0.05/0.005`。publisher 回放
三天 Kafka 数据，启动超时 5400 秒；必须追上实时、六个 symbol 的模型均 ready，
然后才通过环境内的 `intra_scripts/start_intra_trade_signal.sh` 启动信号。本次于
2026-09-23 06:08 UTC 完成回放，验证六个 symbol 均 ready，06:11 UTC 启动信号；
整个 CTA 栈通过环境内 stop/start 脚本重启并校验发布件 SHA-256。

此版不持久化旧仓的逐笔退出账本。重启只从账户仓位初始化净敞口；重启前的配对仓
不会自动恢复历史移动止损或因子退出，也不会因该退出逻辑被主动平仓。新开仓可能
与同 symbol 的旧账户净仓相互抵消，不应把「旧仓不受退出管理」理解为独立账户持仓。

`factor_backtest_baseline489_train2024_2026.toml` 是 symbol6 搜索配置，
不是已经选定的生产参数组。先选定因子、NQ 开关、退出分位与 trailing 参数，
再逐 bar 核对 factor score、分位、现货 NQ、spread、open/exit/撤单事件。
交易所排队、部分成交、取整、实际费率和 API 延迟不能靠回测预测，须单独验收。
pre-trade 重启时的在途订单仍需人工核对；旧仓的退出 lot 不恢复，不能把新信号
上线等同于对旧仓恢复止损保护。
