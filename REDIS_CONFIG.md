# Funding Rate Strategy - Redis 配置文档

## 策略参数 Key

### `fr_strategy_params` (Hash 类型)

所有下单相关参数集中在一个 Hash 中，由 `params_loader` 定时加载（60秒）。

```bash
# Redis CLI 设置示例
redis-cli

# 设置所有字段
HSET fr_strategy_params \
  mode "MM" \
  order_amount "100.0" \
  price_offsets "[0.0002, 0.0004, 0.0006, 0.0008, 0.001]" \
  open_order_timeout "120" \
  hedge_timeout "30" \
  hedge_price_offset "0.0003" \
  signal_cooldown "5"
```

### 字段说明

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| `mode` | String | 做市模式：`MM`（Maker-Maker）或 `MT`（Maker-Taker） | `"MM"` |
| `order_amount` | String (f32) | 单笔下单量（USDT） | `"100.0"` |
| `price_offsets` | String (JSON) | 开仓挂单档位数组 | `"[0.0002,0.0004,0.0006,0.0008,0.001]"` |
| `open_order_timeout` | String (u64) | 开仓订单超时（秒） | `"120"` |
| `hedge_timeout` | String (u64) | 对冲订单超时（秒） | `"30"` |
| `hedge_price_offset` | String (f64) | 对冲价格偏移（万分之几） | `"0.0003"` |
| `signal_cooldown` | String (u64) | 信号冷却时间（秒） | `"5"` |

## 交易对列表

策略维护两组交易对列表（参考 SymbolList 设计）：
- **平仓列表 (dump_symbols)**: 算法会平仓的交易对
- **建仓列表 (trade_symbols)**: 算法会根据信号建仓的交易对

### Binance UM (合约)

```bash
# 平仓列表
redis-cli SET fr_dump_symbols:binance_um '["BTCUSDT","ETHUSDT"]'

# 建仓列表
redis-cli SET fr_trade_symbols:binance_um '["BTCUSDT","ETHUSDT","SOLUSDT"]'
```

### Binance Margin (现货杠杆)

```bash
# 平仓列表
redis-cli SET fr_dump_symbols:binance_margin '["BTCUSDT","ETHUSDT"]'

# 建仓列表
redis-cli SET fr_trade_symbols:binance_margin '["BTCUSDT","ETHUSDT","SOLUSDT"]'
```

## 价差阈值（TODO）

### `binance_forward_spread_thresholds` (Hash 类型)

TODO: 待实现 SpreadFactor 的 Redis 更新逻辑

## 资金费率阈值（TODO）

### `funding_rate_thresholds` (Hash 类型)

TODO: 待实现 FundingRateFactor 的 Redis 更新逻辑

## 配置更新流程

```
1. Redis 更新配置
   ↓
2. params_loader 每 60 秒自动检测
   ↓
3. 解析参数并更新单例
   ├─ 策略参数 (fr_strategy_params)
   │  - FrDecision::update_order_amount()
   │  - FrDecision::update_price_offsets()
   │  - FrDecision::update_open_order_timeout()
   │  - FrDecision::update_hedge_timeout()
   │  - FrDecision::update_hedge_price_offset()
   │  - FrDecision::update_signal_cooldown()
   │  - SpreadFactor::set_mode()
   │
   └─ 交易对列表 (fr_dump_symbols + fr_trade_symbols)
      - SymbolList::reload_from_redis()
         ├─ Binance UM: dump_symbols + trade_symbols
         └─ Binance Margin: dump_symbols + trade_symbols
   ↓
4. 新配置立即生效
```

## 环境变量

```bash
# Redis 配置
export FUNDING_RATE_REDIS_HOST="127.0.0.1"

# 可选：自定义 Redis Key
export SPREAD_THRESHOLD_REDIS_KEY="binance_forward_spread_thresholds"
export FUNDING_THRESHOLD_REDIS_KEY="funding_rate_thresholds"
export SYMBOLS_REDIS_KEY="fr_trade_symbols:binance_um"
```

## 示例：完整配置

```bash
#!/bin/bash
# 完整 Redis 配置脚本

redis-cli <<EOF
# 1. 策略参数
HSET fr_strategy_params \\
  mode "MM" \\
  order_amount "100.0" \\
  price_offsets "[0.0002, 0.0004, 0.0006, 0.0008, 0.001]" \\
  open_order_timeout "120" \\
  hedge_timeout "30" \\
  hedge_price_offset "0.0003" \\
  signal_cooldown "5"

# 2. Binance UM 交易对列表
SET fr_dump_symbols:binance_um '["BTCUSDT","ETHUSDT"]'
SET fr_trade_symbols:binance_um '["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT"]'

# 3. Binance Margin 交易对列表
SET fr_dump_symbols:binance_margin '["BTCUSDT","ETHUSDT"]'
SET fr_trade_symbols:binance_margin '["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT"]'

# 验证配置
HGETALL fr_strategy_params
GET fr_dump_symbols:binance_um
GET fr_trade_symbols:binance_um
GET fr_dump_symbols:binance_margin
GET fr_trade_symbols:binance_margin
EOF
```

**或者使用 Python 脚本（推荐）：**

```bash
# 同步配置到 Redis（会自动打印结果）
python scripts/sync_fr_strategy_params.py

# 只查看当前配置
python scripts/print_fr_strategy_params.py
```

## 实时查看配置

```bash
# 查看策略参数
redis-cli HGETALL fr_strategy_params

# 查看交易对列表
redis-cli GET fr_dump_symbols:binance_um
redis-cli GET fr_trade_symbols:binance_um
redis-cli GET fr_dump_symbols:binance_margin
redis-cli GET fr_trade_symbols:binance_margin

# 监控配置变化
redis-cli MONITOR

# 或使用 Python 脚本（推荐）
python scripts/print_fr_strategy_params.py
```

## 参数调优建议

### MM 模式（做市双边）
```bash
HSET fr_strategy_params \
  mode "MM" \
  hedge_timeout "30"  # 对冲单有超时
```

### MT 模式（对冲吃单）
```bash
HSET fr_strategy_params \
  mode "MT" \
  hedge_timeout "0"   # 对冲单立即成交
```

### 高频策略
```bash
HSET fr_strategy_params \
  signal_cooldown "3" \
  order_amount "50.0" \
  open_order_timeout "60"
```

### 稳健策略
```bash
HSET fr_strategy_params \
  signal_cooldown "10" \
  order_amount "200.0" \
  open_order_timeout "180"
```
