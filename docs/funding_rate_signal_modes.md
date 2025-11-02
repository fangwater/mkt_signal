# Funding Rate 报单模式说明

资金费率策略的下单逻辑通过 `order_mode`、`order_open_range` 以及 `order_close_range` 三个参数控制。所有参数都存放在 Redis `binance_forward_arb_params` 中，可通过 `scripts/sync_binance_forward_arb_params.py` 或 `scripts/sync_self.py` 写入。

## 关键参数

- `order_mode`：`normal`（普通报单）或 `ladder`（阶梯报单）。
- `order_open_range`：数组，表示开仓价相对于现价（spot bid）的偏移比例。
- `order_close_range`：数组，表示平仓价相对于现价（spot ask）的偏移比例。
- 价差阈值统一从 Redis `binance_arb_price_spread_threshold` 中读取：正套使用 `forward_arb_open_tr` / `forward_arb_cancel_tr`，反套使用 `backward_arb_open_tr` / `backward_arb_cancel_tr`。

### 普通模式 (`normal`)

- 仅读取数组的第 0 档（索引 0）。
- 开仓价格：`bid * (1 - order_open_range[0])`。
- 平仓价格：`ask * (1 + order_close_range[0])`。
- 配置兼容旧格式（单个数字会在导入时转换为 `["value"]`），默认值为 `0.0002`。

### 阶梯模式 (`ladder`)

- 开仓：
  - 跳过索引 0，从索引 1 开始逐档生成限价单；举例 `[0.00, 0.01, 0.03, 0.05]` 会产生三档限价单，对应 `1%/3%/5%` 偏移。
  - 如果数组长度不足 2（没有额外档位），会回退到普通模式的 0 档，确保至少生成一张单。
- 平仓：
  - 同样按照索引 1 开始逐档发单，逻辑与开仓保持一致。
  - 所有档位共用 `order_amount_u`，下单数量会按最小下单量和数量步长自动向上取整。
- 撤单：
  - 若实时 `bidask_sr` 超过 Redis 中该 symbol 的 `forward_arb_cancel_tr`，策略会派发 `BinSingleForwardArbCancelMT`（`funding_rate_strategy_mt`）或 `BinSingleForwardArbCancelMM`（`funding_rate_strategy_mm`）信号，`pre_trade` 会根据策略类型撤销对应流程下尚未成交的阶梯挂单。
- 开仓阈值：
  - 阶梯模式始终使用 `forward_arb_open_tr` 判断价差是否满足开仓条件。

## 价格与数量处理

- 价格会根据交易对最小价位 (`price_tick`) 对齐：开仓向下取整，平仓向上取整。
- 数量会对齐到合约/现货的最小数量及数量步长。
- 任何档位在计算后价格或数量非正，即撤销该档位的信号并记录告警日志，不影响其他档位。

## 示例配置

```json
{
  "order_mode": "ladder",
  "order_open_range": "[0.0,0.0001,0.0003,0.0005]",
  "order_close_range": "[0.0,0.0001,0.0003,0.0005]",
  "order_amount_u": "100"
}
```

## 运行提示

1. 更新 Redis 参数后，策略会在下一次热更新周期内自动加载，无需重启。
2. 阶梯模式会一次性发出多条开/平仓信号，策略日志 `open_signals` / `close_signals` 计数会累加每档请求。
3. 阶梯开仓/撤单阈值由 `binance_arb_price_spread_threshold` 中的 per-symbol 阈值控制。
4. 建议使用 `scripts/print_binance_forward_arb_params.py` 校验参数格式。
