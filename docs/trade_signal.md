# Trade Signal

记录目前在 `SignalPublisher` 通道上使用的主要信号类型，以及上下文载荷的关键字段和落地位置。

## BinSingleForwardArbOpenMT

- 类型：`SignalType::BinSingleForwardArbOpenMT`
- 触发来源：`funding_rate_strategy_mt`（`SIGNAL_CHANNEL_MT_ARBITRAGE_FORWARD`）
- 接收方：`pre_trade` 内的 `BinSingleForwardArbStrategyMT`
- 持久化列族：`signals_bin_single_forward_arb_open_mt`
- 上下文字段：
  - `spot_symbol`（现货 symbol，字符串）
  - `futures_symbol`（对应合约 symbol，字符串）
  - `amount`（下单基础量，`f32`）
  - `side`（买卖方向，序列化为 `u8`）
  - `order_type`（下单方式，序列化为 `u8`）
  - `price` / `price_tick`（价格与精度）
  - `exp_time`（信号过期时间，微秒）
  - `create_ts`（信号创建时间戳，微秒）
  - `open_threshold`（触发此次开仓的 bidask 阈值）
  - `spot_bid0` / `spot_ask0`、`swap_bid0` / `swap_ask0`（当时盘口）
  - `funding_ma` / `predicted_funding_rate` / `loan_rate`（可选资金率指标）

## BinSingleForwardArbOpenMM

- 类型：`SignalType::BinSingleForwardArbOpenMM`
- 触发来源：`funding_rate_strategy_mm`（`SIGNAL_CHANNEL_MM_ARBITRAGE_FORWARD`）
- 接收方：`pre_trade` 内的 `BinSingleForwardArbStrategyMM`
- 持久化列族：`signals_bin_single_forward_arb_open_mm`
- 上下文字段与 `OpenMT` 完全一致，共享序列化格式，便于两个流程复用同一持久化导出脚本。

## BinSingleForwardArbCancelMT

- 类型：`SignalType::BinSingleForwardArbCancelMT`
- 触发来源：`funding_rate_strategy_mt`
- 接收方：`pre_trade`（在每个活跃的 MT 策略上逐一传播）
- 持久化列族：`signals_bin_single_forward_arb_cancel_mt`（策略维度 fan-out 后逐条写入）
- 上下文字段：
  - `spot_symbol` / `futures_symbol`
- `cancel_threshold`（Redis 阈值）
- `spot_bid0` / `spot_ask0`
- `swap_bid0` / `swap_ask0`
- `trigger_ts`（触发时刻，微秒）
- 说明：消费侧会依据 `spot_bid0` 与 `swap_ask0` 重新计算 `bidask_sr`，保证日志与风控判断一致。
- 处理说明：`pre_trade` 会为同一信号下所有活跃策略写入独立的 `SignalRecordMessage`，保留策略 ID 与接收时间戳，以便后续审计。

## BinSingleForwardArbCancelMM

- 类型：`SignalType::BinSingleForwardArbCancelMM`
- 触发来源：`funding_rate_strategy_mm`
- 接收方：`pre_trade`（仅下发至 MM 策略）
- 持久化列族：`signals_bin_single_forward_arb_cancel_mm`
- 上下文字段与 `CancelMT` 相同，帧格式一致。
