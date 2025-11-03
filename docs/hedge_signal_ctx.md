# 对冲信号上下文对比

| 字段 | `BinSingleForwardArbHedgeMTCtx` | `BinSingleForwardArbHedgeMMCtx` | 说明 |
| ---- | -------------------------------- | -------------------------------- | ---- |
| `strategy_id: i32` | ✅ | ✅ | 触发对冲的策略实例 ID，用于在 pre-trade/strategy 管道中精确路由。 |
| `client_order_id: i64` | ✅ | ✅ | 原始 margin 开仓的 `clientOrderId`，策略内部通过它检索订单快照。 |
| `hedge_qty: f64` | ✅ | ✅ | 需要对冲的成交增量。 |
| `hedge_side: Side` | ❌ | ✅ | 对冲方向（BUY/SELL）。MM 限价单需要显式携带，避免再次推导。 |
| `limit_price: f64` | ❌ | ✅ | 资金费率策略根据最新期货盘口计算出的 maker 限价。 |
| `price_tick: f64` | ❌ | ✅ | 合约对应的价格步长，用于在策略侧复核/对齐。 |
| `maker_only: bool` | ❌ | ✅ | 是否要求 GTX（`timeInForce=GTX`）。当前固定为 `true`。 |
| `exp_time: i64` | ❌ | ✅ | 订单保留时长（微秒），来源于配置 `max_hedge_order_keep_s`。 |
| `spot_bid_price: f64` | ❌ | ✅ | 记录触发对冲时的现货最优买价，用于分析盘口环境。 |
| `spot_ask_price: f64` | ❌ | ✅ | 记录触发对冲时的现货最优卖价。 |
| `spot_ts: i64` | ❌ | ✅ | 现货盘口时间戳（微秒）。 |
| `fut_bid_price: f64` | ❌ | ✅ | 记录触发对冲时的合约最优买价。 |
| `fut_ask_price: f64` | ❌ | ✅ | 记录触发对冲时的合约最优卖价（限价参考）。 |
| `fut_ts: i64` | ❌ | ✅ | 合约盘口时间戳（微秒）。 |

### 场景说明

- **MT 流程（`BinSingleForwardArbHedgeMTCtx`）**  
  MT 策略在本地维护 margin 订单的完整信息，收到信号后直接提交 UM 市价单，因此只需要策略 ID + client order id + 对冲数量这三个最小要素。

- **MM 流程（`BinSingleForwardArbHedgeMMCtx`）**  
  资金费率策略会在收到 `ReqBinSingleForwardArbHedgeMM` 后，根据实时盘口计算 maker 限价，并补充 GTX/TIF、方向及完整的现货 / 合约盘口信息，再将 `BinSingleForwardArbHedgeMMCtx` 发布给 pre-trade 和 MM 策略执行。  
  `hedge_side` 会依据原 margin 订单方向推断（买入保证金 → 卖出对冲，卖出保证金 → 买入对冲）。这样 MM 策略无需再次计算价格或依赖外部盘口数据，直接按上下文执行限价对冲，亦可在失败退回时携带原始盘口用于排查。
