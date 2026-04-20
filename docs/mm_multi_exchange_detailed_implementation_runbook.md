# 高频做市多交易所详细实施手册

本文档是 `docs/mm_multi_exchange_development_blueprint.md` 的实现版补充，目标是把开发工作拆到可执行粒度，覆盖：

- Gate / Bitget / Bybit 各自的逐文件 TODO 表
- 每一阶段对应的提交顺序
- 每一步的验证命令和通过标准
- 需要新增的配置样例文件内容

本文档默认你已经接受下面这个核心判断：

- 当前仓库已经有可复用的高频做市骨架
- 不能重写 pre_trade / trade_engine / strategy manager
- 需要补的是 `market_maker runtime + venue adapter + execution/query/private-event support`

---

## 1. 先修正文档和现实之间的差异

在进入分阶段开发前，先明确 3 个当前仓库事实：

### 1.1 `market_maker` 仍是 stub

文件：

- [`src/bin/market_maker.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/bin/market_maker.rs)

现状：

- 只启动 / 等退出
- 不订阅行情
- 不生成 MM 信号

因此：

- 真正第一优先级是把它补成可运行的 MM runtime

### 1.2 `manual_mm_signal` 在脚本里被引用，但 Rust bin 里不存在

现状：

- 脚本里存在：[`mm_scripts/start_manual_mm_signal.sh`](/home/ubuntu/crypto_mkt/mkt_signal/mm_scripts/start_manual_mm_signal.sh)
- 它会尝试启动 `manual_mm_signal`
- 但当前 Rust 二进制入口里没有 `src/bin/manual_mm_signal.rs`
- 仓库里实际存在的是 [`src/bin/manual_signal.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/bin/manual_signal.rs)

因此开发时要做选择：

方案 A，推荐：

- 新增真正的 `src/bin/manual_mm_signal.rs`
- 让它成为 MM 控制面和开发期手工信号入口
- 逐步取代脚本里的历史占位行为

方案 B，过渡方案：

- 先让 `manual_signal` 兼容 MM 用法
- 修改 `mm_scripts/start_manual_mm_signal.sh` 去启动 `manual_signal`
- 等真正 `market_maker` runtime 完成后，再淘汰这条控制链

建议采用方案 A，原因：

- FR / XARB 的 `manual_signal` 和 MM 最终职责不应混淆
- MM 需要自己的配置结构和页面字段

### 1.3 `deploy_mm_account_monitor.sh` 当前不支持 Bybit

文件：

- [`scripts/deploy_mm_account_monitor.sh`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/deploy_mm_account_monitor.sh)

现状：

- 只支持 `okex|binance|gate|bitget`
- 没有 `bybit`

因此：

- 在落地 `bybit_account_monitor` 之前，不能宣称 Bybit 进程组完整
- 文档中的 Bybit 监控进程需要明确标记为待新增

---

## 2. 总体开发顺序

统一顺序：

1. 共享基础层
2. Gate MVP
3. Bitget 执行层 + Bitget MVP
4. Bybit 私有层 + 执行层 + Bybit MVP
5. 三家统一运维、日志、参数中心

原因：

- Gate 最接近闭环
- Bitget 的私有事件已有基础，但执行层未接好
- Bybit 缺口最大，必须放最后

---

## 3. 阶段 0：统一目录与运行约定

这一阶段先不写策略逻辑，先把未来开发和部署约束固定下来。

### 3.1 标准目录

每个 MM 环境目录统一：

- `~/gate_mm_alpha`
- `~/bitget_mm_beta`
- `~/bybit_mm_hf01`

每个目录至少包含：

- `env.sh`
- `trade_engine.toml`
- `config/manual_mm_signal.yaml`
- `mm_scripts/`
- `scripts/`
- 二进制：
  - `pre_trade`
  - `trade_engine`
  - `account_monitor`
  - `manual_mm_signal` 或临时 `manual_signal`
  - 未来 `market_maker`

### 3.2 统一进程名

基于 [`scripts/mm_process_name.sh`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/mm_process_name.sh)：

- `mm_pt_<exchange>_<tag>`
- `mm_te_<exchange>_<tag>`
- `mm_am_<exchange>_<tag>`
- `mm_ms_<exchange>_<tag>`

例如：

- `mm_pt_gate_alpha`
- `mm_te_gate_alpha`
- `mm_am_gate_alpha`
- `mm_ms_gate_alpha`

### 3.3 统一进程管理器

统一使用：

- `pmdaemon`

不要新增新的 `pm2` 依赖。

### 3.4 阶段 0 提交顺序

#### Commit 0.1

目标：文档与命名约定落地

涉及文件：

- `docs/mm_multi_exchange_development_blueprint.md`
- `docs/mm_multi_exchange_detailed_implementation_runbook.md`

通过标准：

- 文档能覆盖所有未来路径
- 文档中不再把 `manual_mm_signal` 当作已存在 bin，而是明确成待新增

---

## 4. 阶段 1：共享基础层改造

这一阶段是三家交易所共用的基础工作。

### 4.1 目标

把当前零散的做市组件组织成真正的 MM runtime。

结果要求：

- 有一个真正的 `market_maker` 主进程，或最少有一个 `manual_mm_signal` 替代 runtime
- 可以订阅公共行情
- 可以为每个 symbol 维护 quote state
- 可以发 `MMOpen/MMCancel/MMHedge`

### 4.2 逐文件 TODO

#### 4.2.1 `src/bin/market_maker.rs`

TODO：

- 从 stub 改成真实启动器
- 读取 `config/manual_mm_signal.yaml`
- 初始化 Redis 参数覆盖
- 初始化行情订阅器
- 初始化 SignalPublisher
- 启动 quote recompute loop
- 注册退出信号

通过标准：

- 进程启动不再只打印“启动完成，等待市场数据”
- 能定期打印 symbol quote summary
- 能向 signal channel 发 MM 相关信号

#### 4.2.2 `src/market_maker/mod.rs`

TODO：

- 增加以下模块导出：
  - `runtime`
  - `state`
  - `quote_engine`
  - `cancel_policy`
  - `venue_adapter`

#### 4.2.3 新增 `src/market_maker/runtime.rs`

TODO：

- 订阅：
  - `dat_pbs/<venue>/ask_bid_spread`
  - `dat_pbs/<venue>/derivatives`
  - 如必要，再订阅 trade / incremental
- 维护 `HashMap<symbol, MmSymbolState>`
- 周期性调用 quote engine
- 生成 `MMOpen`
- 检查现有挂单和 quote 偏离，生成 `MMCancel`
- 处理 fill snapshot，生成 `MMHedge`

关键接口建议：

- `run()`
- `handle_market_data()`
- `recompute_quotes()`
- `emit_open_signals()`
- `emit_cancel_signals()`
- `emit_hedge_signals()`

#### 4.2.4 新增 `src/market_maker/state.rs`

TODO：

定义结构：

- `MmSymbolState`
  - `symbol`
  - `last_bid`
  - `last_ask`
  - `last_mid`
  - `last_fair`
  - `last_volatility`
  - `inventory_qty`
  - `last_open_signal_ts`
  - `last_cancel_signal_ts`
  - `last_hedge_signal_ts`
  - `working_quotes`
  - `stale_since`

- `WorkingQuote`
  - `side`
  - `price`
  - `qty`
  - `client_order_id`
  - `create_ts`
  - `expire_ts`

#### 4.2.5 新增 `src/market_maker/quote_engine.rs`

TODO：

实现：

- fair price 计算
- spread 计算
- inventory skew
- alpha skew
- aggressiveness 调整
- 生成双边或单边 quote levels

优先复用：

- [`open_quote_plan.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/market_maker/open_quote_plan.rs)
- [`hedge_quote_plan.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/market_maker/hedge_quote_plan.rs)
- [`quote_plan_levels.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/market_maker/quote_plan_levels.rs)

#### 4.2.6 新增 `src/market_maker/cancel_policy.rs`

TODO：

定义：

- 什么时候 cancel
- 偏离阈值
- stale quote 阈值
- TTL
- cancel 节流
- 每 symbol 最小 cancel 间隔

#### 4.2.7 新增 `src/market_maker/venue_adapter.rs`

TODO：

抽象：

- tick size
- qty step
- min qty
- min notional
- contract multiplier
- maker tif
- reduce only 语义
- symbol 变换

至少实现：

- `GateMmVenueAdapter`
- `BitgetMmVenueAdapter`
- `BybitMmVenueAdapter`

### 4.3 共享基础层配置文件新增

#### 新增 `config/manual_mm_signal.yaml`

建议内容：

```yaml
venue: gate-futures
symbols:
  - BTCUSDT
  - ETHUSDT
refresh:
  order_interval_ms: 5000
  open_order_ttl_ms: 120000
quote:
  default_order_amount_u: 100.0
  open_orders_per_round: 8
  open_buy_vol_scale: [0.1, 2.0]
  open_sell_vol_scale: [0.1, 2.0]
hedge:
  hedge_orders_per_round: 8
  hedge_vol_multiplier: 2.0
  hedge_price_offset_limit_lower: 0.0003
  hedge_price_offset_limit_upper: 0.005
  hedge_offset_ratio: 1.3
  hedge_window_scale_low: 0.8
  hedge_window_scale_high: 1.3
cancel:
  enable_open_cancel: false
  enable_tlen_cancel: false
  tlen_cancel_freq_ms: 3000
risk:
  enable_volatility_limit: true
  open_volatility_limit: 70
model:
  return_model_service: "-"
  environment_model_service: "-"
  enable_return_score_adjust_hedge: true
  enable_environment_model: true
```

#### 新增 `src/bin/manual_mm_signal.rs`

如果采用推荐方案，需要新增一个专门面向 MM 的手工信号与开发控制二进制。

TODO：

- 读取 `config/manual_mm_signal.yaml`
- 支持通过 web UI / HTTP 直接发 `MMOpen/MMCancel/MMHedge`
- 可选订阅公共行情并展示当前 quote state
- 仅用于开发期和手工测试，不替代未来 `market_maker`

### 4.4 阶段 1 提交顺序

#### Commit 1.1

新增：

- `src/market_maker/runtime.rs`
- `src/market_maker/state.rs`
- `src/market_maker/quote_engine.rs`
- `src/market_maker/cancel_policy.rs`
- `src/market_maker/venue_adapter.rs`
- 更新 `src/market_maker/mod.rs`

验证命令：

```bash
cargo build --release --bin market_maker
```

通过标准：

- 能编译
- 无未使用公共接口泛滥

#### Commit 1.2

修改：

- `src/bin/market_maker.rs`

新增：

- `config/manual_mm_signal.yaml`

验证命令：

```bash
cargo build --release --bin market_maker
cargo run --bin market_maker
```

通过标准：

- 进程能启动
- 能读取配置
- 能进入事件循环

#### Commit 1.3

如果采用推荐方案：

新增：

- `src/bin/manual_mm_signal.rs`

修改：

- `mm_scripts/start_manual_mm_signal.sh`
- `scripts/deploy_mm_signal.sh`

验证命令：

```bash
cargo build --release --bin manual_mm_signal
./mm_scripts/start_manual_mm_signal.sh --help
```

通过标准：

- 脚本不再引用不存在的 bin
- 能明确读取配置文件

---

## 5. 阶段 2：Gate MVP

Gate 是第一条闭环线。

### 5.1 目标

单 symbol 跑通完整闭环：

- 生成 MMOpen
- 真正挂 maker 单
- 成交回灌
- 生成 MMHedge
- query watchdog 正常
- 撤单和状态收敛正常

### 5.2 逐文件 TODO

#### 5.2.1 `src/market_maker/venue_adapter.rs`

Gate 适配器 TODO：

- `TradingVenue::GateFutures` tick / qty / min qty
- maker tif 映射为当前 Gate futures 所需参数
- contract multiplier 读取与 `order_align` 保持一致
- symbol `BTCUSDT -> BTC_USDT` 映射

#### 5.2.2 `src/pre_trade/order_manager.rs`

Gate 相关 TODO：

- 审核 `GateMargin` 和 `GateFutures` 当前构单逻辑
- 确认：
  - `time_in_force = poc` 是否稳定代表 post-only
  - market 与 IOC 语义是否满足 hedge 需要
  - signed size 正负方向是否正确
  - reduce_only 是否需要新增字段
- 对 Gate futures 增加必要注释和测试

建议加测试点：

- buy / sell size 正负号
- market order price 字段是否必须为 `0`
- cancel 参数里使用 `text` 还是 `order_id`

#### 5.2.3 `src/trade_engine/gate_ws.rs`

TODO：

- 审核 login 流程
- 审核 request seq / transport id
- 对失败响应增加明确日志
- 记录 raw channel / req_id / client_order_id 对应关系

#### 5.2.4 `src/trade_engine/gate_query.rs`

TODO：

- 审核 order query 的返回字段覆盖率
- 确认 partial fill / filled / cancelled / rejected 是否都能解析
- query not found 是否能转成 `ORDER_QUERY_NOT_FOUND_MARKER`

#### 5.2.5 `src/parser/gate_account_event_parser.rs`

TODO：

- 审核成交字段映射
- 审核 maker/taker 标记
- 审核 event time / update time
- 审核 client_order_id / text 前缀对齐

#### 5.2.6 `src/strategy/gate_basic_impl.rs`

TODO：

- 确认 `OrderUpdate` 和 `TradeUpdate` 接口字段是否完整
- 如果 Gate 订单事件没有独立成交时间，明确文档化 fallback 逻辑

### 5.3 Gate 阶段提交顺序

#### Commit 2.1

目标：Gate adapter 与 Gate 构单审计

修改：

- `src/market_maker/venue_adapter.rs`
- `src/pre_trade/order_manager.rs`

验证命令：

```bash
cargo build --release --bin pre_trade
cargo test gate -- --nocapture
```

通过标准：

- Gate 分支可编译
- 新增测试通过

#### Commit 2.2

目标：Gate execution/query 稳定化

修改：

- `src/trade_engine/gate_ws.rs`
- `src/trade_engine/gate_query.rs`

验证命令：

```bash
cargo build --release --bin trade_engine
cargo run --bin trade_engine -- --exchange gate
```

通过标准：

- 进程可启动
- 登录成功
- 可看到 query / order service 初始化成功

#### Commit 2.3

目标：Gate account event 闭环审计

修改：

- `src/parser/gate_account_event_parser.rs`
- `src/strategy/gate_basic_impl.rs`

验证命令：

```bash
cargo build --release --bin gate_account_monitor
cargo run --bin gate_account_monitor
```

通过标准：

- 能接私有流
- 无大量 parse error
- 能输出 order/trade 事件日志

#### Commit 2.4

目标：Gate 单 symbol 做市实测

使用目录：

- `~/gate_mm_alpha`

部署命令：

```bash
bash scripts/deploy_mm_pre_trade.sh --exchange gate --env-suffix alpha
bash scripts/deploy_mm_trade_engine.sh --exchange gate --env-suffix alpha
bash scripts/deploy_mm_account_monitor.sh --exchange gate --env-suffix alpha
bash scripts/deploy_mm_config_server.sh --env-name gate_mm_alpha
```

参数命令：

```bash
cd ~/gate_mm_alpha
python scripts/sync_mm_symbol_list.py
python scripts/sync_mm_strategy_params.py
python scripts/sync_mm_risk_params.py
```

启动命令：

```bash
cd ~/gate_mm_alpha
./mm_scripts/start_mm_pre_trade.sh
./mm_scripts/start_mm_trade_engine.sh gate
./scripts/start_account_monitor.sh
./mm_scripts/start_manual_mm_signal.sh
```

通过标准：

- 单 symbol 有 MMOpen 发出
- trade_engine 真正发出订单
- account_monitor 收到该订单的回报
- fill 后 pre_trade 发出 MMHedge
- query watchdog 未反复失控

---

## 6. 阶段 3：Bitget 执行层和 Bitget MVP

### 6.1 目标

Bitget 先补执行协议和查询协议，再把策略 venue 放开。

### 6.2 逐文件 TODO

#### 6.2.1 `src/trade_engine/trade_request.rs`

TODO：

新增枚举值：

- `BitgetUnifiedNewOrder`
- `BitgetUnifiedCancelOrder`
- `BitgetFuturesNewOrder`
- `BitgetFuturesCancelOrder`

同时新增：

- `TryFrom<u32>` 分支
- 对应 request struct
- `to_bytes()` / `create()`

#### 6.2.2 `src/trade_engine/query_request.rs`

TODO：

新增：

- `BitgetFuturesOrderQuery`
- `BitgetUnifiedBalanceSnapshot`
- `BitgetUnifiedPositionsSnapshot`

#### 6.2.3 `src/trade_engine/trade_type_mapping.rs`

TODO：

为 Bitget request type 增加：

- 是否 websocket
- endpoint
- method
- weight
- requires_auth

#### 6.2.4 `src/trade_engine/query_type_mapping.rs`

TODO：

为 Bitget query type 增加：

- endpoint
- method
- weight

#### 6.2.5 新增 `src/trade_engine/bitget_ws.rs`

TODO：

- 建立交易 websocket
- login
- new order
- cancel order
- response correlation
- reconnect
- raw error logging

#### 6.2.6 新增 `src/trade_engine/bitget_query.rs`

TODO：

- order query
- balance snapshot
- position snapshot
- parser 接线

#### 6.2.7 `src/trade_engine/engine.rs`

TODO：

- `Exchange::Bitget` 的 request 消费逻辑接入
- `order_reqs/bitget`
- `query_reqs/bitget`
- 结果回发到统一 response channel

#### 6.2.8 `src/pre_trade/order_manager.rs`

TODO：

新增 venue 分支：

- `TradingVenue::BitgetMargin`
- `TradingVenue::BitgetFutures`

至少支持 futures。

需要处理：

- post only
- tif
- reduce only
- symbol 变换
- price/qty 格式

#### 6.2.9 `src/strategy/mm_open_strategy.rs`

TODO：

- 放开 Bitget venue 限制
- 增加 Bitget order query build 分支

#### 6.2.10 `src/strategy/mm_hedge_strategy.rs`

TODO：

- 放开 Bitget venue 限制
- 增加 Bitget hedge query build 分支

### 6.3 Bitget 阶段提交顺序

#### Commit 3.1

目标：新增 Bitget request / query 协议枚举

修改：

- `src/trade_engine/trade_request.rs`
- `src/trade_engine/query_request.rs`

验证命令：

```bash
cargo build --release --bin trade_engine
```

通过标准：

- 编译通过
- 不影响已有 Binance/OKX/Gate

#### Commit 3.2

目标：补 type mapping

修改：

- `src/trade_engine/trade_type_mapping.rs`
- `src/trade_engine/query_type_mapping.rs`

验证命令：

```bash
cargo test trade_type_mapping query_type_mapping -- --nocapture
```

通过标准：

- endpoint / method / weight 可解析
- 未出现 unreachable 分支误用

#### Commit 3.3

目标：新增 Bitget trade_engine adapter

新增：

- `src/trade_engine/bitget_ws.rs`
- `src/trade_engine/bitget_query.rs`

修改：

- `src/trade_engine/mod.rs`
- `src/trade_engine/engine.rs`

验证命令：

```bash
cargo build --release --bin trade_engine
cargo run --bin trade_engine -- --exchange bitget
```

通过标准：

- 进程启动
- Bitget request 通道注册成功
- 登录或初始化连接成功

#### Commit 3.4

目标：补 Bitget order_manager 构单

修改：

- `src/pre_trade/order_manager.rs`

验证命令：

```bash
cargo build --release --bin pre_trade
```

通过标准：

- Bitget futures 能构造 request bytes
- pre_trade 不再把 Bitget 视为 unsupported venue

#### Commit 3.5

目标：放开 MM strategy Bitget venue

修改：

- `src/strategy/mm_open_strategy.rs`
- `src/strategy/mm_hedge_strategy.rs`

验证命令：

```bash
cargo build --release --bin pre_trade
```

通过标准：

- Bitget venue 不再 panic
- 能进入 MM open / hedge 状态机

#### Commit 3.6

目标：Bitget 单 symbol 实盘 / 仿真验证

部署命令：

```bash
bash scripts/deploy_mm_pre_trade.sh --exchange bitget --env-suffix beta
bash scripts/deploy_mm_trade_engine.sh --exchange bitget --env-suffix beta
bash scripts/deploy_mm_account_monitor.sh --exchange bitget --env-suffix beta
bash scripts/deploy_mm_config_server.sh --env-name bitget_mm_beta
```

启动：

```bash
cd ~/bitget_mm_beta
./mm_scripts/start_mm_pre_trade.sh
./mm_scripts/start_mm_trade_engine.sh bitget
./scripts/start_account_monitor.sh
./mm_scripts/start_manual_mm_signal.sh
```

通过标准：

- 单 symbol 可以挂 maker 单
- 能收到订单回报
- 成交后 hedge 可发
- query watchdog 正常

---

## 7. 阶段 4：Bybit 私有层、执行层和 Bybit MVP

### 7.1 目标

Bybit 先补私有闭环，再补执行，再放开做市策略。

### 7.2 逐文件 TODO

#### 7.2.1 新增 `src/portfolio_margin/bybit_auth.rs`

TODO：

- API key / secret / passphrase（若需要）组织
- ws auth sign
- time skew 处理

#### 7.2.2 新增 `src/portfolio_margin/bybit_user_stream.rs`

TODO：

- private ws 连接
- auth
- order / execution / wallet / position 订阅
- reconnect

#### 7.2.3 新增 `src/parser/bybit_account_event_parser.rs`

TODO：

- bybit 私有事件 -> 统一 basic order msg
- client_order_id 映射
- maker/taker
- filled / partial / cancelled / rejected / expired

#### 7.2.4 新增 `src/strategy/bybit_basic_impl.rs`

TODO：

实现：

- `OrderUpdate for BybitBasicOrderMsg`
- `TradeUpdate for BybitBasicOrderMsg`

#### 7.2.5 新增 `src/bin/bybit_account_monitor.rs`

TODO：

- 初始化 bybit user stream
- 调 parser
- 将事件发布到 account_pubs

#### 7.2.6 `src/trade_engine/trade_request.rs`

TODO：

新增：

- `BybitUnifiedNewOrder`
- `BybitUnifiedCancelOrder`
- `BybitFuturesNewOrder`
- `BybitFuturesCancelOrder`

可先只落 futures。

#### 7.2.7 `src/trade_engine/query_request.rs`

TODO：

新增：

- `BybitFuturesOrderQuery`
- `BybitUnifiedBalanceSnapshot`
- `BybitUnifiedPositionsSnapshot`

#### 7.2.8 `src/trade_engine/trade_type_mapping.rs`

TODO：

为 Bybit request type 增加 endpoint / method / weight / transport。

#### 7.2.9 `src/trade_engine/query_type_mapping.rs`

TODO：

为 Bybit query type 增加 endpoint / method / weight。

#### 7.2.10 新增 `src/trade_engine/bybit_ws.rs`

TODO：

- login
- new order
- cancel order
- response correlation
- reconnect

#### 7.2.11 新增 `src/trade_engine/bybit_query.rs`

TODO：

- order query
- balance snapshot
- positions snapshot

#### 7.2.12 `src/trade_engine/engine.rs`

TODO：

- `Exchange::Bybit` 真正可跑 request / query。

#### 7.2.13 `src/pre_trade/order_manager.rs`

TODO：

新增：

- `TradingVenue::BybitMargin`
- `TradingVenue::BybitFutures`

#### 7.2.14 `src/strategy/mm_open_strategy.rs`

TODO：

- 放开 Bybit venue 限制
- query 分支加入 Bybit

#### 7.2.15 `src/strategy/mm_hedge_strategy.rs`

TODO：

- 放开 Bybit venue 限制
- query 分支加入 Bybit

#### 7.2.16 `scripts/deploy_mm_account_monitor.sh`

TODO：

- 在 usage 和 case 分支中加入 `bybit`
- `BIN_NAME="bybit_account_monitor"`

### 7.3 Bybit 阶段提交顺序

#### Commit 4.1

目标：Bybit 私有 user stream 和 parser

新增：

- `src/portfolio_margin/bybit_auth.rs`
- `src/portfolio_margin/bybit_user_stream.rs`
- `src/parser/bybit_account_event_parser.rs`
- `src/strategy/bybit_basic_impl.rs`
- `src/bin/bybit_account_monitor.rs`

验证命令：

```bash
cargo build --release --bin bybit_account_monitor
cargo run --bin bybit_account_monitor
```

通过标准：

- 能登录 private ws
- 能收到订单 / 成交 / 余额 / 仓位事件
- 无大规模 parse error

#### Commit 4.2

目标：Bybit request / query 协议

修改：

- `src/trade_engine/trade_request.rs`
- `src/trade_engine/query_request.rs`
- `src/trade_engine/trade_type_mapping.rs`
- `src/trade_engine/query_type_mapping.rs`

验证命令：

```bash
cargo build --release --bin trade_engine
```

通过标准：

- 协议枚举和映射完整

#### Commit 4.3

目标：Bybit trade_engine adapter

新增：

- `src/trade_engine/bybit_ws.rs`
- `src/trade_engine/bybit_query.rs`

修改：

- `src/trade_engine/mod.rs`
- `src/trade_engine/engine.rs`

验证命令：

```bash
cargo build --release --bin trade_engine
cargo run --bin trade_engine -- --exchange bybit
```

通过标准：

- 进程可启动
- Bybit request/query 通道可初始化

#### Commit 4.4

目标：Bybit order_manager 构单

修改：

- `src/pre_trade/order_manager.rs`

验证命令：

```bash
cargo build --release --bin pre_trade
```

通过标准：

- 能构造 Bybit futures request bytes

#### Commit 4.5

目标：放开 Bybit MM strategy venue

修改：

- `src/strategy/mm_open_strategy.rs`
- `src/strategy/mm_hedge_strategy.rs`
- `scripts/deploy_mm_account_monitor.sh`

验证命令：

```bash
cargo build --release --bin pre_trade
bash scripts/deploy_mm_account_monitor.sh --help
```

通过标准：

- Bybit venue 不再 panic
- 部署脚本支持 Bybit

#### Commit 4.6

目标：Bybit 单 symbol 闭环验证

部署：

```bash
bash scripts/deploy_mm_pre_trade.sh --exchange bybit --env-suffix hf01
bash scripts/deploy_mm_trade_engine.sh --exchange bybit --env-suffix hf01
bash scripts/deploy_mm_account_monitor.sh --exchange bybit --env-suffix hf01
bash scripts/deploy_mm_config_server.sh --env-name bybit_mm_hf01
```

启动：

```bash
cd ~/bybit_mm_hf01
./mm_scripts/start_mm_pre_trade.sh
./mm_scripts/start_mm_trade_engine.sh bybit
./scripts/start_account_monitor.sh
./mm_scripts/start_manual_mm_signal.sh
```

通过标准：

- 单 symbol 可挂 maker 单
- 私有回报闭环正常
- 成交后 hedge 正常
- query watchdog 正常

---

## 8. 各交易所逐文件 TODO 总表

### 8.1 共享基础层文件总表

必须新增：

- `src/market_maker/runtime.rs`
- `src/market_maker/state.rs`
- `src/market_maker/quote_engine.rs`
- `src/market_maker/cancel_policy.rs`
- `src/market_maker/venue_adapter.rs`
- `config/manual_mm_signal.yaml`
- 建议：`src/bin/manual_mm_signal.rs`

必须修改：

- `src/bin/market_maker.rs`
- `src/market_maker/mod.rs`
- `mm_scripts/start_manual_mm_signal.sh`
- `scripts/deploy_mm_signal.sh`

### 8.2 Gate 文件总表

重点修改：

- `src/market_maker/venue_adapter.rs`
- `src/pre_trade/order_manager.rs`
- `src/trade_engine/gate_ws.rs`
- `src/trade_engine/gate_query.rs`
- `src/parser/gate_account_event_parser.rs`
- `src/strategy/gate_basic_impl.rs`

### 8.3 Bitget 文件总表

必须新增：

- `src/trade_engine/bitget_ws.rs`
- `src/trade_engine/bitget_query.rs`

必须修改：

- `src/trade_engine/trade_request.rs`
- `src/trade_engine/query_request.rs`
- `src/trade_engine/trade_type_mapping.rs`
- `src/trade_engine/query_type_mapping.rs`
- `src/trade_engine/mod.rs`
- `src/trade_engine/engine.rs`
- `src/pre_trade/order_manager.rs`
- `src/strategy/mm_open_strategy.rs`
- `src/strategy/mm_hedge_strategy.rs`
- `src/market_maker/venue_adapter.rs`

### 8.4 Bybit 文件总表

必须新增：

- `src/portfolio_margin/bybit_auth.rs`
- `src/portfolio_margin/bybit_user_stream.rs`
- `src/parser/bybit_account_event_parser.rs`
- `src/strategy/bybit_basic_impl.rs`
- `src/bin/bybit_account_monitor.rs`
- `src/trade_engine/bybit_ws.rs`
- `src/trade_engine/bybit_query.rs`

必须修改：

- `src/trade_engine/trade_request.rs`
- `src/trade_engine/query_request.rs`
- `src/trade_engine/trade_type_mapping.rs`
- `src/trade_engine/query_type_mapping.rs`
- `src/trade_engine/mod.rs`
- `src/trade_engine/engine.rs`
- `src/pre_trade/order_manager.rs`
- `src/strategy/mm_open_strategy.rs`
- `src/strategy/mm_hedge_strategy.rs`
- `scripts/deploy_mm_account_monitor.sh`
- `src/market_maker/venue_adapter.rs`

---

## 9. 标准新增配置样例

### 9.1 `config/manual_mm_signal.yaml`

```yaml
venue: gate-futures
symbols:
  - BTCUSDT
  - ETHUSDT
refresh:
  order_interval_ms: 5000
  open_order_ttl_ms: 120000
quote:
  default_order_amount_u: 100.0
  open_orders_per_round: 8
  open_buy_vol_scale: [0.1, 2.0]
  open_sell_vol_scale: [0.1, 2.0]
hedge:
  hedge_orders_per_round: 8
  hedge_vol_multiplier: 2.0
  hedge_price_offset_limit_lower: 0.0003
  hedge_price_offset_limit_upper: 0.005
  hedge_offset_ratio: 1.3
  hedge_window_scale_low: 0.8
  hedge_window_scale_high: 1.3
cancel:
  enable_open_cancel: false
  enable_tlen_cancel: false
  tlen_cancel_freq_ms: 3000
model:
  return_model_service: "-"
  environment_model_service: "-"
  enable_return_score_adjust_hedge: true
  enable_environment_model: true
risk:
  enable_volatility_limit: true
  open_volatility_limit: 70
```

### 9.2 `trade_engine.toml`

```toml
local_ips = ["172.31.40.177"]
```

Bitget / Bybit 如需多 IP：

```toml
local_ips = ["172.31.40.177", "172.31.40.178"]
```

### 9.3 `env.sh`

Gate 示例：

```bash
export IPC_NAMESPACE="gate_mm_alpha"
export RUST_LOG="info"
export GATE_API_KEY="xxx"
export GATE_API_SECRET="xxx"
```

Bitget 示例：

```bash
export IPC_NAMESPACE="bitget_mm_beta"
export RUST_LOG="info"
export BITGET_API_KEY="xxx"
export BITGET_API_SECRET="xxx"
export BITGET_API_PASSPHRASE="xxx"
```

Bybit 示例：

```bash
export IPC_NAMESPACE="bybit_mm_hf01"
export RUST_LOG="info"
export BYBIT_API_KEY="xxx"
export BYBIT_API_SECRET="xxx"
```

### 9.4 `config/mm_config_server.env`

这是 `deploy_mm_config_server.sh` 会写的文件，建议内容像这样：

```bash
HOST=0.0.0.0
PORT=18121
EXCHANGE=gate
ENV_NAME=gate_mm_alpha
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_DB=0
```

---

## 10. 每个阶段的统一验证清单

### 10.1 编译验证

```bash
cargo build --release --bin pre_trade
cargo build --release --bin trade_engine
cargo build --release --bin gate_account_monitor
cargo build --release --bin bitget_account_monitor
cargo build --release --bin market_maker
```

如 Bybit 完成后：

```bash
cargo build --release --bin bybit_account_monitor
```

通过标准：

- 所有目标二进制可编译
- 无明显未接线的 panic 分支

### 10.2 进程启动验证

```bash
pmdaemon list
pmdaemon logs mm_pt_gate_alpha --follow
pmdaemon logs mm_te_gate_alpha --follow
pmdaemon logs mm_am_gate_alpha --follow
```

通过标准：

- 进程存在
- 无持续重启
- 无 auth fail

### 10.3 订单闭环验证

必须检查：

- open order sent
- order accepted
- account monitor received update
- trade update received
- pre_trade strategy state advanced
- hedge signal emitted
- hedge order sent
- terminal state reached

通过标准：

- 本地策略状态与交易所状态最终一致
- 无 orphan order
- 无 stuck pending cancel

### 10.4 风控验证

必须验证：

- `max_pos_u`
- `max_pending_limit_orders`
- `open_order_rate_limit_per_min`
- `hedge_order_rate_limit_per_min`

通过标准：

- 触发风控时 `pre_trade` 拒绝下单
- 不会让 trade_engine 收到本不该发出的 request

---

## 11. 最终执行顺序摘要

严格按下面顺序开发：

1. 修正 MM runtime / manual_mm_signal 入口
2. 新增共享 `market_maker` 基础模块
3. 新增 `config/manual_mm_signal.yaml`
4. Gate adapter 与 Gate 闭环验证
5. Bitget request/query/engine
6. Bitget order_manager + MM strategy 放开
7. Bitget 单 symbol 验证
8. Bybit private stream + parser + account_monitor
9. Bybit request/query/engine
10. Bybit order_manager + MM strategy 放开
11. Bybit 单 symbol 验证
12. 三家统一文档、日志、参数中心收口

这份手册的目标不是一次讲清概念，而是让你可以按阶段逐步推进，并且每一步都知道：

- 改哪些文件
- 先后顺序是什么
- 怎么验证
- 什么算通过
