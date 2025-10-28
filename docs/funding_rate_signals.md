# Funding Rate Signal 执行逻辑

## 资金费率信号

- 预测资金费率 (`predict_funding_rate`)：对历史资金费率按 `interval` 做滚动均值，并向前偏移 `predict_num`。
- 实时资金费率均值 (`current_fundingRate_ma`)：维护最近 `funding_ma_size`（默认 60）个实时资金费率的简单均值。
- 不同结算频率（4h / 8h）对应的缺省阈值：
  - 8h：`open_upper = 0.00008`、`open_lower = -0.00008`、`close_lower = -0.001`、`close_upper = 0.001`
  - 4h：`open_upper = 0.00004`、`open_lower = -0.00004`、`close_lower = -0.0008`、`close_upper = 0.0008`
  - 启动后会根据 Redis(`binance_forward_arb_params`) 的值热更新。
- 资金费率信号判定：
  1. 若 `predict_funding_rate >= open_upper` → 资金信号 `-1`（期货做空 / 现货做多，需配合价差开仓条件）。
  2. 若 `predict_funding_rate <= open_lower` → 信号 `1`（反向，仅记录日志）。
3. 若 `current_fundingRate_ma < close_lower` → 资金信号 `-2`（触发平仓，需配合价差平仓条件）。
4. 若 `current_fundingRate_ma > close_upper` → 信号 `2`（反向，仅记录日志）。
  5. 预测驱动的开仓与 MA 驱动的平仓互不排斥：系统会分别检查对应的价差信号，只要其中一组资金+价差条件同时满足，就执行相应的开/平仓动作。

## 价差信号

- `bidask_sr = (spot_bid - fut_ask) / spot_bid`
- `askbid_sr = (spot_ask - fut_bid) / spot_ask`
- Redis 提供的价差阈值 (`open_threshold` / `close_threshold` / `askbid_open_threshold` / `askbid_close_threshold`) 会在刷新时加载。
- 条件：
  - `price_open_bidask = bidask_sr <= open_threshold`
  - `price_open_askbid = askbid_sr >= askbid_open_threshold`（配置为 0/NaN 时视为自动满足，现仅用于监控）
  - `price_close_bidask = bidask_sr >= close_threshold`（仅用于辅助观察）
  - `price_close_askbid = askbid_sr >= askbid_open_threshold`
  - `price_close_ready = price_close_askbid`
  - `price_open_ready = price_open_bidask`

## 开仓 / 平仓逻辑

- **开仓（现货做多 / 合约做空）**
  - 资金信号为 `-1`
  - `price_open_ready` 为 `true`（仅由 `price_open_bidask` 决定）
  - 当前持仓 `Flat`
  - 信号节流满足（距离上次信号 >= `min_interval_ms`）
  - 满足时通过 `BinSingleForwardArbOpen` 发布开仓请求，并记录 `last_open_ts`、`position=Opened`
- **平仓（释放现货多头）**
  - 资金信号为 `-2`
  - `price_close_ready` 为 `true`（即 `askbid_sr >= askbid_open_threshold`）
  - 当前持仓 `Opened`
  - 信号节流满足
  - 满足时通过 `BinSingleForwardArbCloseMargin` 发布平仓请求，并记录 `last_close_ts`、`position=Flat`
- 报单模式与多档价差配置的细节整理在《funding_rate_signal_modes.md》。
- 阶梯模式专用：当 `order_mode=ladder` 且实时 `bidask_sr` 高于 `order_ladder_cancel_bidask_threshold` 时，会通过 `BinSingleForwardArbLadderCancel` 下发撤单信号，策略收到后会主动撤销未成交的阶梯挂单。
- 阶梯模式可以指定 `order_ladder_open_bidask_threshold`，若配置则以固定阈值决定价差是否满足开仓（而不再使用 Redis 阈值）。
- 资金信号 `1` / `2` 仅输出调试日志，不会触发委托。

最大杠杆倍数 设置为2

## Resample 调试输出

- Resample 频率固定为 3s。
- 每次 resample 会额外打印两张三线表（通过 `info!` 输出，方便线上直接观察）：
  1. **Resample价差信号**：展示 BidAsk/AskBid 指标与各自阈值，并标记价差是否满足开仓/平仓条件以及当前持仓状态。
  2. **Resample资费信号**：展示预测资金费率、实时 MA60 及对应阈值，并用文字标明当前资金信号解释（`-1 开仓 / -2 平仓 / 0 无`）。
- 实际信号触发仍在实时行情到达时执行，resample 日志用于复盘和排查。

## 参数热更新

- Redis (`binance_forward_arb_params`) 中的以下键会覆盖默认参数：
  - `interval` / `predict_num` / `refresh_secs` / `fetch_secs` 等运行参数；
  - `fr_4h_*`、`fr_8h_*` 用于更新资金费率阈值；
  - `order_ladder_cancel_bidask_threshold`：阶梯模式全局撤单阈值；
  - `order_ladder_open_bidask_threshold`：阶梯模式全局开仓阈值；
  - 价差阈值通过 `binance_arb_price_spread_threshold` HASH 下的每个 symbol 维护。
- 参数变更或结算点触发时，会重算资金阈值并更新调试输出。
