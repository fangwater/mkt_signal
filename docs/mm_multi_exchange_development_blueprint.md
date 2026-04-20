# 高频做市多交易所开发总蓝图

本文档是面向当前仓库的一份单文件开发手册，目标是指导你基于现有系统，逐步完成 `Gate / Bitget / Bybit` 三个交易所的高频做市策略接入、运行和运维。

文档覆盖：

- 现有架构与可复用模块 
- 三个交易所的开发顺序与依赖关系 
- 配置文件设计 
- 进程设计与部署目录约定 
- 参数设计与 Redis key 规范 
- 日志设计 
- 进程管理命令 
- 查询 / 验证 / 排障命令 
- 按阶段实施步骤 

本文档的原则不是“重新设计一套新系统”，而是“在当前仓库既有架构上做增量扩展，并保持统一运维方式”。

---

## 1. 总体结论

当前仓库已经具备一套可复用的高频交易系统骨架，真正需要做的是：

1. 把现有做市决策逻辑串成真正的 `market_maker runtime`
2. 复用 `pre_trade -> trade_engine -> account_monitor` 的闭环
3. 按交易所补齐执行协议层、查询层、私有回报层
4. 保持参数管理、部署目录、进程命名、日志方式统一

不要做的事：

- 不要重写一套新的 pre_trade / order_manager / strategy manager
- 不要为每个交易所单独做一套完全分叉的 MM 框架
- 不要把做市决策和执行细节糊在一个进程里

推荐顺序：

1. 共享基础层改造
2. Gate MVP
3. Bitget MVP
4. Bybit MVP

原因：

- Gate 已经最接近能跑通单所做市闭环
- Bitget 已有行情和私有回报基础，主要缺执行协议层
- Bybit 公共行情侧已有基础，但私有回报和执行侧缺口最大

---

## 2. 当前系统架构

### 2.1 现有核心链路

当前生产可复用链路如下：

1. 行情采集进程
   - [`src/bin/dat_pbs.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/bin/dat_pbs.rs)
   - 内部使用 [`src/connection/mkt_manager.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/connection/mkt_manager.rs)

2. 做市信号 / 报价规划逻辑
   - [`src/market_maker/open_quote_plan.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/market_maker/open_quote_plan.rs)
   - [`src/market_maker/hedge_quote_plan.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/market_maker/hedge_quote_plan.rs)
   - [`src/market_maker/quote_plan_levels.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/market_maker/quote_plan_levels.rs)
   - [`src/market_maker/order_align.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/market_maker/order_align.rs)

3. 策略状态机 / 风控 / 订单生命周期管理
   - [`src/bin/pre_trade.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/bin/pre_trade.rs)
   - [`src/strategy/manager.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/manager.rs)
   - [`src/strategy/mm_open_strategy.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_open_strategy.rs)
   - [`src/strategy/mm_hedge_strategy.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs)

4. 执行进程
   - [`src/bin/trade_engine.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/bin/trade_engine.rs)
   - [`src/trade_engine/engine.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/engine.rs)

5. 私有回报 / 账户监控
   - `binance_account_monitor`
   - `okex_account_monitor`
   - `gate_account_monitor`
   - `bitget_account_monitor`

6. 持久化
   - [`src/bin/persist_manager.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/bin/persist_manager.rs)
   - [`src/persist_manager/*`](/home/ubuntu/crypto_mkt/mkt_signal/src/persist_manager)

### 2.2 当前不应作为生产入口的模块

[`src/bin/market_maker.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/bin/market_maker.rs) 目前仍是 stub，只做启动和退出，没有真正跑做市事件循环。

这意味着：

- 当前仓库“有做市逻辑”，但“没有完整做市 runtime”
- 真正先要补的是 `market_maker runtime`

### 2.3 当前交易所支持边界

从代码上看，现有 MM 状态机只显式支持：

- Binance
- OKX / Okex
- Gate

见：

- [`src/strategy/mm_open_strategy.rs#L439`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_open_strategy.rs#L439)

执行协议层当前也只定义了：

- Binance 
- Okex 
- Gate 

见：

- [`src/trade_engine/trade_request.rs#L8`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/trade_request.rs#L8)
- [`src/trade_engine/query_request.rs#L8`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/query_request.rs#L8)

所以 `Bitget / Bybit` 的开发不能直接从策略层开始，而必须先补执行与查询协议层。

---

## 3. 设计原则

### 3.1 分层原则

整个做市系统必须强制分成 5 层：

1. 公共行情层
2. 报价决策层
3. 策略状态机层
4. 执行适配层
5. 私有回报闭环层

每层边界：

- `dat_pbs`：只负责公共行情
- `market_maker`：只负责生成 `MMOpen/MMCancel/MMHedge`
- `pre_trade`：只负责策略状态、风控、订单生命周期、query 回补
- `trade_engine`：只负责下单 / 撤单 / 查询
- `account_monitor`：只负责私有订单 / 成交 / 余额 / 仓位回报

### 3.2 统一进程模型

每个 MM 环境目录必须固定包含下列进程：

1. `manual_mm_signal` 或未来真正的 `market_maker`
2. `pre_trade`
3. `trade_engine`
4. `account_monitor`
5. `mm_config_server`
6. 可选 `persist_manager`
7. 可选 `viz_server`

### 3.3 统一命名与目录约定

MM 环境目录命名必须统一：

- `binance_mm_<suffix>`
- `okex_mm_<suffix>`
- `gate_mm_<suffix>`
- `bybit_mm_<suffix>`
- `bitget_mm_<suffix>`

例如：

- `~/gate_mm_alpha`
- `~/bitget_mm_beta`
- `~/bybit_mm_hf01`

仓库中已有部署脚本按此约定工作，见：

- [`scripts/deploy_mm_pre_trade.sh`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/deploy_mm_pre_trade.sh)
- [`scripts/deploy_mm_trade_engine.sh`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/deploy_mm_trade_engine.sh)
- [`scripts/deploy_mm_account_monitor.sh`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/deploy_mm_account_monitor.sh)

---

## 4. 共享基础层开发顺序

这是三家交易所共用的前置工作，必须先做。

### 4.1 第一步：把 `market_maker` 从 stub 变成真正 runtime

当前文件：

- [`src/bin/market_maker.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/bin/market_maker.rs)

当前状态：

- 只是一个空启动器
- 没有事件循环
- 没有订阅行情
- 没有发 signal

必须新增的模块：

- `src/market_maker/runtime.rs`
- `src/market_maker/state.rs`
- `src/market_maker/quote_engine.rs`
- `src/market_maker/cancel_policy.rs`
- `src/market_maker/venue_adapter.rs`

每个模块职责：

- `runtime.rs`
  - 订阅行情
  - 周期性重算 quote
  - 根据 quote 生成 `MMOpen`
  - 根据偏离和 TTL 生成 `MMCancel`
  - 根据 fill 结果和净仓生成 `MMHedge`

- `state.rs`
  - 按 symbol 维护：
    - 当前 bid/ask
    - 当前 fair
    - 当前 volatility
    - 当前 inventory
    - 当前在挂价格层
    - 上次发信号时间
    - 上次撤单时间

- `quote_engine.rs`
  - fair price
  - spread model
  - inventory skew
  - alpha skew
  - edge 过滤

- `cancel_policy.rs`
  - 价格偏离阈值
  - TTL
  - quote stale 判定
  - cancel 节流

- `venue_adapter.rs`
  - 每个 venue 的：
    - tick size
    - qty step
    - min qty
    - contract multiplier
    - maker tif 映射
    - symbol 映射

### 4.2 第二步：统一 Venue Adapter 接口

建议定义 trait：

- `MmVenueAdapter`
  - `venue()`
  - `normalize_symbol()`
  - `price_tick()`
  - `qty_step()`
  - `min_qty()`
  - `contract_multiplier()`
  - `maker_tif()`
  - `supports_post_only()`
  - `supports_reduce_only()`

### 4.3 第三步：统一配置文件模型

建议未来统一用以下配置文件：

- `config/manual_mm_signal.yaml`
- `trade_engine.toml`
- `env.sh`

当前仓库情况：

- `manual_mm_signal` 启动脚本默认引用 `config/manual_mm_signal.yaml`
- 但仓库里目前没有这个文件

见：

- [`mm_scripts/start_manual_mm_signal.sh`](/home/ubuntu/crypto_mkt/mkt_signal/mm_scripts/start_manual_mm_signal.sh)

所以需要新增一个标准配置文件：

`config/manual_mm_signal.yaml`

建议字段：

```yaml
venue: gate-futures
symbols:
  - BTCUSDT
  - ETHUSDT
order_interval_ms: 5000
open_orders_per_round: 8
open_order_ttl_ms: 120000
quote:
  default_order_amount_u: 100.0
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
model:
  return_model_service: "-"
  environment_model_service: "-"
  enable_return_score_adjust_hedge: true
  enable_environment_model: true
risk:
  enable_volatility_limit: true
  open_volatility_limit: 70
cancel:
  enable_open_cancel: false
  enable_tlen_cancel: false
  tlen_cancel_freq_ms: 3000
```

注意：

- 这个配置文件属于做市决策进程
- 不替代 Redis 参数
- 它是 runtime 启动默认参数，Redis 参数仍可覆盖

### 4.4 第四步：保留现有 pre_trade / strategy 结构，不重写

继续复用：

- [`src/strategy/mm_open_strategy.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_open_strategy.rs)
- [`src/strategy/mm_hedge_strategy.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs)
- [`src/strategy/manager.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/manager.rs)
- [`src/pre_trade/order_manager.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/pre_trade/order_manager.rs)

只做两类改动：

1. 放开新 venue 支持
2. 把 query / cancel / fill 闭环补齐

---

## 5. 配置文件总览

### 5.1 公共行情配置

文件：

- [`config/mkt_cfg.yaml`](/home/ubuntu/crypto_mkt/mkt_signal/config/mkt_cfg.yaml)

当前结构：

```yaml
restart_duration_secs: 1800
primary_local_ip: "172.31.33.133"
secondary_local_ip: "172.31.46.90"
snapshot_requery_time: ""
data_types:
  enable_incremental: true
  enable_trade: true
  enable_kline: false
  enable_derivatives: true
  enable_ask_bid_spread: true
  max_levels_per_msg: 50
```

注意：

- `dat_pbs`、`trade_engine`、`account_monitor` 很多时候不是读仓库根目录 `config/mkt_cfg.yaml`
- 它们优先读 `$HOME/dat_pbs/config/mkt_cfg.yaml`

见：

- [`src/bin/dat_pbs.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/bin/dat_pbs.rs)

因此部署环境需要维护：

- `~/dat_pbs/config/mkt_cfg.yaml`

### 5.2 depth publisher 配置

文件：

- [`config/depth_cfg.yaml`](/home/ubuntu/crypto_mkt/mkt_signal/config/depth_cfg.yaml)

当前结构很简单：

```yaml
depth_levels:
  enable_depth25: true
  enable_depth50: true
push_config:
  min_push_interval_ms: 100
```

### 5.3 trade_engine 专用配置

文件：

- `trade_engine.toml`

部署脚本会写：

```toml
local_ips = ["172.31.40.177"]
```

见：

- [`scripts/deploy_mm_trade_engine.sh`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/deploy_mm_trade_engine.sh)

### 5.4 MM config server

进程：

- `mm_config_server.py`

文件：

- [`scripts/mm_config_server.py`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/mm_config_server.py)

它管理这些配置：

- symbol list
- strategy params
- amount_u overrides
- max_pos_u overrides
- pre-trade risk params
- return-model-score thresholds

---

## 6. Redis 参数设计

### 6.1 Symbol List

Redis key：

- `mm_trade_symbols:{venue}`

例如：

- `mm_trade_symbols:gate-futures`
- `mm_trade_symbols:bitget-futures`
- `mm_trade_symbols:bybit-futures`

写入脚本：

- [`scripts/sync_mm_symbol_list.py`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/sync_mm_symbol_list.py)

查看脚本：

- [`scripts/print_mm_symbol_list.py`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/print_mm_symbol_list.py)

### 6.2 Strategy Params

Redis key：

- `mm_strategy_params_{venue}`

例如：

- `mm_strategy_params_gate-futures`
- `mm_strategy_params_bitget-futures`
- `mm_strategy_params_bybit-futures`

写入脚本：

- [`scripts/sync_mm_strategy_params.py`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/sync_mm_strategy_params.py)

查看脚本：

- [`scripts/print_mm_strategy_params.py`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/print_mm_strategy_params.py)

当前默认策略参数：

- `default_order_amount`
- `order_interval_ms`
- `open_orders_per_round`
- `open_buy_vol_scale`
- `open_sell_vol_scale`
- `hedge_orders_per_round`
- `open_order_timeout`
- `next_query_delay_ms`
- `hedge_vol_multiplier`
- `hedge_price_offset_limit_upper`
- `hedge_price_offset_limit_lower`
- `hedge_offset_ratio`
- `hedge_window_scale_low`
- `hedge_window_scale_high`
- `enable_return_score_adjust_hegde`
- `enable_environment_model`
- `enable_volatility_limit`
- `open_volatility_limit`
- `hedge_aggressive_seq_threshold`
- `prediction_mode`
- `enable_open_cancel`
- `enable_tlen_cancel`
- `tlen_cancel_freq_ms`
- `return_model_service`
- `environment_model_service`
- `signal_cooldown`
- `max_hedge_price_pct_change`

### 6.3 Risk Params

Redis key：

- `<env_name>:<open_venue>:<hedge_venue>:pre_trade_risk_params`

单所 MM 下 open/hedge 一样，例如：

- `gate_mm_alpha:gate-futures:gate-futures:pre_trade_risk_params`
- `bitget_mm_beta:bitget-futures:bitget-futures:pre_trade_risk_params`
- `bybit_mm_hf01:bybit-futures:bybit-futures:pre_trade_risk_params`

写入脚本：

- [`scripts/sync_mm_risk_params.py`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/sync_mm_risk_params.py)

查看脚本：

- [`scripts/print_mm_risk_params.py`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/print_mm_risk_params.py)

当前默认风险参数：

- `max_pos_u`
- `max_symbol_exposure_ratio`
- `max_total_exposure_ratio`
- `max_leverage`
- `max_pending_limit_orders`
- `open_order_rate_limit_per_min`
- `open_order_rate_limit_10s`
- `hedge_order_rate_limit_per_min`
- `hedge_order_rate_limit_10s`

### 6.4 Return Model Score Thresholds

Redis key：

- `return_model_score_thresholds_{venue}`

例如：

- `return_model_score_thresholds_gate-futures`
- `return_model_score_thresholds_bitget-futures`
- `return_model_score_thresholds_bybit-futures`

查看脚本：

- [`scripts/print_mm_return_score_thresholds.py`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/print_mm_return_score_thresholds.py)

操作方向：

- `forward_open`
- `forward_cancel`
- `backward_open`
- `backward_cancel`

### 6.5 Tlen Threshold

Redis key：

- `<venue_prefix>:tlen_threshold`

例如：

- `gate_futures:tlen_threshold`
- `bitget_futures:tlen_threshold`
- `bybit_futures:tlen_threshold`

查看脚本：

- [`mm_scripts/print_mm_tlen_threshold.py`](/home/ubuntu/crypto_mkt/mkt_signal/mm_scripts/print_mm_tlen_threshold.py)

---

## 7. 进程设计

### 7.1 单交易所 MM 标准进程组

每个 `<exchange>_mm_<suffix>` 环境建议固定使用以下进程：

1. `market_maker` 或 `manual_mm_signal`
2. `pre_trade`
3. `trade_engine`
4. `account_monitor`
5. `mm_config_server`
6. 可选 `persist_manager`
7. 可选 `viz_server`

### 7.2 当前已有脚本与默认进程名

#### pre_trade

启动脚本：

- [`mm_scripts/start_mm_pre_trade.sh`](/home/ubuntu/crypto_mkt/mkt_signal/mm_scripts/start_mm_pre_trade.sh)

命名规则：

- `mm_pt_<exchange>_<tag>`

例如：

- `mm_pt_gate_alpha`
- `mm_pt_bitget_beta`
- `mm_pt_bybit_hf01`

#### trade_engine

启动脚本：

- [`mm_scripts/start_mm_trade_engine.sh`](/home/ubuntu/crypto_mkt/mkt_signal/mm_scripts/start_mm_trade_engine.sh)

命名规则：

- `mm_te_<exchange>_<tag>`

例如：

- `mm_te_gate_alpha`
- `mm_te_bitget_beta`
- `mm_te_bybit_hf01`

#### manual_mm_signal

启动脚本：

- [`mm_scripts/start_manual_mm_signal.sh`](/home/ubuntu/crypto_mkt/mkt_signal/mm_scripts/start_manual_mm_signal.sh)

当前命名规则：

- `mm_ms_<exchange>_<tag>`

#### account_monitor

启动脚本：

- [`scripts/start_account_monitor.sh`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/start_account_monitor.sh)

命名规则：

- `mm_am_<exchange>_<tag>`

#### process name 规则实现

文件：

- [`scripts/mm_process_name.sh`](/home/ubuntu/crypto_mkt/mkt_signal/scripts/mm_process_name.sh)

短交易所代号：

- `binance -> bn`
- `okex -> ok`
- `bybit -> bb`
- `bitget -> bg`
- `gate -> gt`

### 7.3 进程管理器现状

当前 MM 脚本主要使用：

- `pmdaemon`

个别旧脚本还在使用：

- `pm2`

建议新的 MM 文档和流程统一使用：

- `pmdaemon`

原因：

- `start_mm_pre_trade.sh`
- `start_mm_trade_engine.sh`
- `start_account_monitor.sh`

都已经偏向 `pmdaemon`

---

## 8. 日志设计

### 8.1 日志原则

每个进程日志必须包含以下维度：

- exchange
- venue
- symbol
- strategy_id
- client_order_id
- exchange_order_id
- request_seq
- signal_ts / create_ts / event_time
- side / qty / price
- reason / error_code / raw_status

### 8.2 必须重点打日志的环节

#### market_maker

- quote recompute
- fair price 变化
- spread 变化
- inventory skew 变化
- cancel trigger
- hedge trigger

#### pre_trade

- signal 收到
- 风控拒绝
- order send
- cancel send
- query watchdog
- delayed query reconcile
- strategy deactivate

#### trade_engine

- raw request send
- raw response receive
- sign / auth error
- reconnect
- rate limit
- local ip binding

#### account_monitor

- login success/fail
- order update
- trade update
- balance update
- position update
- parse error

### 8.3 日志级别建议

- `info`
  - 进程启动
  - 参数快照
  - 每次真实下单 / 撤单
  - 关键状态切换

- `warn`
  - fallback query
  - ws reconnect
  - stale quote
  - cancel reject
  - rate limit

- `error`
  - auth fail
  - request encode/decode fail
  - account event parse fail
  - strategy state inconsistency

- `debug`
  - 高频 quote 明细
  - raw ws payload 采样
  - score / volatility / offset 中间值

---

## 9. Gate 开发顺序

Gate 是第一条线，目标是最快跑通单所 MM 闭环。

### 9.1 Gate 现有基础

已有模块：

- 行情连接：[`src/connection/gate_conn.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/connection/gate_conn.rs)
- 行情解析：[`src/parser/gate_parser.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/parser/gate_parser.rs)
- 私有 user stream：[`src/portfolio_margin/gate_user_stream.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/portfolio_margin/gate_user_stream.rs)
- 账户事件解析：[`src/parser/gate_account_event_parser.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/parser/gate_account_event_parser.rs)
- 执行 ws：[`src/trade_engine/gate_ws.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/gate_ws.rs)
- query：[`src/trade_engine/gate_query.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/gate_query.rs)
- 回报适配：[`src/strategy/gate_basic_impl.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/gate_basic_impl.rs)
- account monitor：[`src/bin/gate_account_monitor.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/bin/gate_account_monitor.rs)

### 9.2 Gate MVP 目标

目标闭环：

1. `market_maker` 根据 Gate 公共行情发 `MMOpen`
2. `pre_trade` 建立 open strategy
3. `trade_engine --exchange gate` 发单
4. `gate_account_monitor` 回灌订单 / 成交
5. `pre_trade` 记录 fill 并发 `MMHedge`
6. `trade_engine --exchange gate` 发 hedge
7. `pre_trade` 完成 query reconcile 和状态闭环

### 9.3 Gate 开发步骤

#### 第 1 步：跑通 Gate 单 symbol 行情与 quote engine

使用：

- `dat_pbs --venue gate-futures`
- 后续新的 `market_maker`

验证：

- BBO 是否连续
- derivatives 是否连续
- 波动率输入是否正常

#### 第 2 步：实现 Gate 单 symbol maker quote

复用：

- [`open_quote_plan.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/market_maker/open_quote_plan.rs)

重点核对：

- tick 对齐
- qty step 对齐
- post-only 语义是否用 `"poc"` 合理

#### 第 3 步：验证 Gate 开单回报闭环

检查：

- `GateUnifiedNewOrderRequest`
- `GateFuturesNewOrderRequest`
- `GateUnifiedCancelOrderRequest`
- `GateFuturesCancelOrderRequest`

位置：

- [`src/pre_trade/order_manager.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/pre_trade/order_manager.rs)
- [`src/trade_engine/trade_request.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/trade_request.rs)

#### 第 4 步：验证 query watchdog

重点场景：

- ws 已发单但没回报
- cancel 已发但未确认
- 部分成交后 query 回补

#### 第 5 步：扩到多 symbol

完成多 symbol 后，再接参数中心。

### 9.4 Gate 关键命令

部署：

```bash
bash scripts/deploy_mm_pre_trade.sh --exchange gate --env-suffix alpha
bash scripts/deploy_mm_trade_engine.sh --exchange gate --env-suffix alpha
bash scripts/deploy_mm_account_monitor.sh --exchange gate --env-suffix alpha
bash scripts/deploy_mm_config_server.sh --env-name gate_mm_alpha
```

启动：

```bash
cd ~/gate_mm_alpha
./mm_scripts/start_mm_pre_trade.sh
./mm_scripts/start_mm_trade_engine.sh gate
./scripts/start_account_monitor.sh
./mm_scripts/start_manual_mm_signal.sh
```

查询日志：

```bash
pmdaemon list
pmdaemon logs mm_pt_gate_alpha --follow
pmdaemon logs mm_te_gate_alpha --follow
pmdaemon logs mm_am_gate_alpha --follow
```

Redis 参数：

```bash
cd ~/gate_mm_alpha
python scripts/sync_mm_symbol_list.py
python scripts/sync_mm_strategy_params.py
python scripts/sync_mm_risk_params.py
python scripts/print_mm_return_score_thresholds.py
python mm_scripts/print_mm_tlen_threshold.py
```

排障脚本：

```bash
python scripts/gate_orders_ws.py
python scripts/gate_unified_assets_ws.py
python scripts/gate_futures_positions.py
python scripts/gate_futures_account.py
python scripts/gate_cancel_all.py --mode futures --settle usdt
```

---

## 10. Bitget 开发顺序

Bitget 是第二条线。

### 10.1 Bitget 现有基础

已有：

- 行情连接：[`src/connection/bitget_conn.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/connection/bitget_conn.rs)
- 行情解析：[`src/parser/bitget_parser.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/parser/bitget_parser.rs)
- 私有 user stream：[`src/portfolio_margin/bitget_user_stream.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/portfolio_margin/bitget_user_stream.rs)
- 账户事件解析：[`src/parser/bitget_account_event_parser.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/parser/bitget_account_event_parser.rs)
- 回报适配：[`src/strategy/bitget_basic_impl.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/bitget_basic_impl.rs)
- account monitor：[`src/bin/bitget_account_monitor.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/bin/bitget_account_monitor.rs)

缺：

- `trade_request` request type
- `query_request` query type
- `trade_engine` 的 bitget 执行与查询 transport
- `order_manager` 的 Bitget 构单分支
- MM strategy venue 放开

### 10.2 Bitget MVP 目标

目标不是先做完整策略，而是：

1. 先让 Bitget 交易执行接口能像 Gate 一样跑起来
2. 再把 MM strategy 放开

### 10.3 Bitget 开发步骤

#### 第 1 步：补执行协议枚举

修改：

- [`src/trade_engine/trade_request.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/trade_request.rs)

新增建议：

- `BitgetUnifiedNewOrder`
- `BitgetUnifiedCancelOrder`
- `BitgetFuturesNewOrder`
- `BitgetFuturesCancelOrder`

如果 Bitget 没有 margin/spot unified 需求，可先只实现 futures 版本。

#### 第 2 步：补查询协议枚举

修改：

- [`src/trade_engine/query_request.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/query_request.rs)

新增建议：

- `BitgetFuturesOrderQuery`
- `BitgetUnifiedBalanceSnapshot`
- `BitgetUnifiedPositionsSnapshot`

#### 第 3 步：补 type mapping

修改：

- [`src/trade_engine/trade_type_mapping.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/trade_type_mapping.rs)
- [`src/trade_engine/query_type_mapping.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/query_type_mapping.rs)

需要定义：

- websocket 还是 rest
- endpoint
- method
- weight
- 是否签名

#### 第 4 步：新增 bitget 执行模块

新增：

- `src/trade_engine/bitget_ws.rs`
- `src/trade_engine/bitget_query.rs`

根据实际需要可再拆：

- `src/trade_engine/bitget_rest.rs`

#### 第 5 步：把 bitget 接到 `TradeEngine`

修改：

- [`src/trade_engine/engine.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/engine.rs)

要求：

- `TradeEngine::run_with_shutdown(exchange=Bitget)` 真正可工作
- 可以收 `order_reqs/bitget`
- 可以收 `query_reqs/bitget`

#### 第 6 步：补 order_manager Bitget 构单

修改：

- [`src/pre_trade/order_manager.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/pre_trade/order_manager.rs)

新增分支：

- `TradingVenue::BitgetMargin`
- `TradingVenue::BitgetFutures`

要明确：

- post only 映射
- reduce only
- tif
- symbol 转换
- contract multiplier

#### 第 7 步：放开 MM strategy venue 限制

修改：

- [`src/strategy/mm_open_strategy.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_open_strategy.rs)
- [`src/strategy/mm_hedge_strategy.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs)

当前 MM open 只允许 Binance/OKX/Gate。

### 10.4 Bitget 关键命令

部署：

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

日志：

```bash
pmdaemon logs mm_pt_bitget_beta --follow
pmdaemon logs mm_te_bitget_beta --follow
pmdaemon logs mm_am_bitget_beta --follow
```

参数：

```bash
cd ~/bitget_mm_beta
python scripts/sync_mm_symbol_list.py
python scripts/sync_mm_strategy_params.py
python scripts/sync_mm_risk_params.py
python scripts/print_mm_return_score_thresholds.py
python mm_scripts/print_mm_tlen_threshold.py
```

建议额外阅读：

- [`docs/bitget_approximation_notes.md`](/home/ubuntu/crypto_mkt/mkt_signal/docs/bitget_approximation_notes.md)

---

## 11. Bybit 开发顺序

Bybit 是第三条线，也是缺口最大的。

### 11.1 Bybit 现有基础

已有：

- 行情连接：[`src/connection/bybit_conn.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/connection/bybit_conn.rs)
- 行情解析：[`src/parser/bybit_parser.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/parser/bybit_parser.rs)

明显缺：

- bybit 私有 user stream
- bybit 账户事件解析
- bybit 基础回报适配
- bybit account monitor
- bybit trade request / query request
- bybit trade_engine 执行 transport

### 11.2 Bybit MVP 目标

目标是：

1. 先补全私有回报闭环
2. 再补 execution
3. 最后放开做市策略

### 11.3 Bybit 开发步骤

#### 第 1 步：新增私有侧

新增：

- `src/portfolio_margin/bybit_auth.rs`
- `src/portfolio_margin/bybit_user_stream.rs`
- `src/parser/bybit_account_event_parser.rs`
- `src/strategy/bybit_basic_impl.rs`
- `src/bin/bybit_account_monitor.rs`

要求：

- 能输出统一的 `OrderUpdate`
- 能输出统一的 `TradeUpdate`
- 能映射余额和仓位

#### 第 2 步：新增 query 协议

修改：

- [`src/trade_engine/query_request.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/query_request.rs)
- [`src/trade_engine/query_type_mapping.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/query_type_mapping.rs)

至少加：

- `BybitFuturesOrderQuery`
- `BybitUnifiedBalanceSnapshot`
- `BybitUnifiedPositionsSnapshot`

#### 第 3 步：新增 trade 协议

修改：

- [`src/trade_engine/trade_request.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/trade_request.rs)
- [`src/trade_engine/trade_type_mapping.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/trade_type_mapping.rs)

至少加：

- `BybitFuturesNewOrder`
- `BybitFuturesCancelOrder`

#### 第 4 步：新增 bybit trade_engine adapter

新增：

- `src/trade_engine/bybit_ws.rs`
- `src/trade_engine/bybit_query.rs`

可选：

- `src/trade_engine/bybit_rest.rs`

#### 第 5 步：接到 TradeEngine 主循环

修改：

- [`src/trade_engine/engine.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/trade_engine/engine.rs)

要求：

- `--exchange bybit` 真正可下单 / 撤单 / 查询

#### 第 6 步：补 order_manager 构单

修改：

- [`src/pre_trade/order_manager.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/pre_trade/order_manager.rs)

新增：

- `TradingVenue::BybitMargin`
- `TradingVenue::BybitFutures`

#### 第 7 步：放开 MM venue 限制

修改：

- [`src/strategy/mm_open_strategy.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_open_strategy.rs)
- [`src/strategy/mm_hedge_strategy.rs`](/home/ubuntu/crypto_mkt/mkt_signal/src/strategy/mm_hedge_strategy.rs)

### 11.4 Bybit 关键命令

部署：

```bash
bash scripts/deploy_mm_pre_trade.sh --exchange bybit --env-suffix hf01
bash scripts/deploy_mm_trade_engine.sh --exchange bybit --env-suffix hf01
bash scripts/deploy_mm_config_server.sh --env-name bybit_mm_hf01
```

等 `bybit_account_monitor` 落地后再加：

```bash
bash scripts/deploy_mm_account_monitor.sh --exchange bybit --env-suffix hf01
```

启动：

```bash
cd ~/bybit_mm_hf01
./mm_scripts/start_mm_pre_trade.sh
./mm_scripts/start_mm_trade_engine.sh bybit
./scripts/start_account_monitor.sh
./mm_scripts/start_manual_mm_signal.sh
```

日志：

```bash
pmdaemon logs mm_pt_bybit_hf01 --follow
pmdaemon logs mm_te_bybit_hf01 --follow
pmdaemon logs mm_am_bybit_hf01 --follow
```

参数：

```bash
cd ~/bybit_mm_hf01
python scripts/sync_mm_symbol_list.py
python scripts/sync_mm_strategy_params.py
python scripts/sync_mm_risk_params.py
python scripts/print_mm_return_score_thresholds.py
python mm_scripts/print_mm_tlen_threshold.py
```

---

## 12. 统一开发批次建议

### 批次 A：共享基础层

目标：

- 让 `market_maker` 真正可发布 MM signals

改动：

- 新增 `runtime/state/quote_engine/cancel_policy/venue_adapter`
- 新增 `config/manual_mm_signal.yaml`
- 让 `manual_mm_signal` 和未来 `market_maker` 共享配置结构

验证：

- 本地只跑 Gate 公共行情
- 看是否持续发 `MMOpen/MMCancel`

### 批次 B：Gate MVP

目标：

- 跑通单 symbol 做市闭环

验证：

- maker quote 能挂上
- fill 能回灌
- hedge 能发出
- query watchdog 正常

### 批次 C：Bitget 执行层

目标：

- `trade_engine --exchange bitget` 真正可用

验证：

- 下单
- 撤单
- 订单查询
- 回报闭环

### 批次 D：Bitget MM

目标：

- 放开 MM strategy venue 并跑单 symbol

### 批次 E：Bybit 私有回报层

目标：

- `bybit_account_monitor` 成型

### 批次 F：Bybit 执行层

目标：

- `trade_engine --exchange bybit` 可用

### 批次 G：Bybit MM

目标：

- Bybit 单 symbol / 多 symbol 做市

---

## 13. 统一查询与运维命令

### 13.1 构建

```bash
cargo build --release --bin pre_trade
cargo build --release --bin trade_engine
cargo build --release --bin gate_account_monitor
cargo build --release --bin bitget_account_monitor
cargo build --release --bin manual_signal
cargo build --release --bin dat_pbs
```

如未来新增：

```bash
cargo build --release --bin bybit_account_monitor
cargo build --release --bin market_maker
```

### 13.2 进程状态

```bash
pmdaemon list
```

### 13.3 查看日志

```bash
pmdaemon logs mm_pt_gate_alpha --follow
pmdaemon logs mm_te_gate_alpha --follow
pmdaemon logs mm_am_gate_alpha --follow
pmdaemon logs mm_ms_gate_alpha --follow
```

按交易所替换即可。

### 13.4 停止进程

```bash
cd ~/gate_mm_alpha
./mm_scripts/stop_mm_pre_trade.sh
./mm_scripts/stop_mm_trade_engine.sh
./scripts/stop_account_monitor.sh
./mm_scripts/stop_manual_mm_signal.sh
```

### 13.5 Symbol / 参数查询

```bash
python scripts/print_mm_symbol_list.py
python scripts/print_mm_strategy_params.py
python scripts/print_mm_risk_params.py
python scripts/print_mm_return_score_thresholds.py
python mm_scripts/print_mm_tlen_threshold.py
```

### 13.6 参数同步

```bash
python scripts/sync_mm_symbol_list.py
python scripts/sync_mm_strategy_params.py
python scripts/sync_mm_risk_params.py
```

### 13.7 通用排障

```bash
python scripts/list_unified_orders.py
python scripts/order_export.py --help
python scripts/cleanup_iceoryx.sh
python scripts/cleanup_iceoryx_dead_nodes.sh
```

### 13.8 公共行情启动

```bash
cargo run --bin dat_pbs -- --venue gate-futures
cargo run --bin dat_pbs -- --venue bitget-futures
cargo run --bin dat_pbs -- --venue bybit-futures
```

### 13.9 执行进程启动

```bash
cargo run --bin trade_engine -- --exchange gate
cargo run --bin trade_engine -- --exchange bitget
cargo run --bin trade_engine -- --exchange bybit
```

---

## 14. 关键风险点

### 14.1 不要先放开策略 venue 限制

必须先有：

- request type
- query type
- trade_engine adapter
- order_manager 构单
- account_monitor 回报

否则策略一放开就会进入“能发 signal 但无法闭环”的坏状态。

### 14.2 不要把执行逻辑塞回 `market_maker`

`market_maker` 只出 signal。

### 14.3 不要跳过 query watchdog

高频做市里，最危险的不是下单失败，而是：

- 下单成功但状态丢失
- cancel 成功但本地没收回报
- 部分成交后本地净仓失真

因此 query / reconcile 是必须保留的主链路，不是 fallback。

### 14.4 先单 symbol，再多 symbol

每个交易所必须按以下顺序验证：

1. 单 symbol
2. 单 symbol 高撤单率
3. 单 symbol 有 fill + hedge
4. 多 symbol 低并发
5. 多 symbol 高频并发

---

## 15. 最终建议

实施顺序必须严格按下面来：

1. 补 `market_maker runtime`
2. Gate 单 symbol
3. Gate 多 symbol
4. Bitget 执行层
5. Bitget 单 symbol
6. Bybit 私有回报层
7. Bybit 执行层
8. Bybit 单 symbol
9. 三家统一参数中心
10. 三家统一日志与排障手册

如果后续要继续扩展文档，建议分支文档再拆：

- `docs/mm_gate_runbook.md`
- `docs/mm_bitget_runbook.md`
- `docs/mm_bybit_runbook.md`
- `docs/mm_parameter_reference.md`

但当前阶段，优先保持这一份总手册为单一事实来源。
