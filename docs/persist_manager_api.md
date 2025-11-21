# Persist Manager API 使用文档

## 概述

Persist Manager 提供 HTTP API 来查询、导出和删除持久化的交易数据。

**默认端口**: 8088
**基础URL**: `http://localhost:8088`

---

## 一、Signal 信号数据

### 1.1 获取信号列表 (JSON)

```bash
# 获取 ArbOpen 信号（最近100条，降序）
curl "http://localhost:8088/signals/signals_arb_open?limit=100&direction=desc"

# 获取 ArbHedge 信号
curl "http://localhost:8088/signals/signals_arb_hedge?limit=100"

# 获取 ArbCancel 信号
curl "http://localhost:8088/signals/signals_arb_cancel?limit=100"

# 获取 ArbClose 信号
curl "http://localhost:8088/signals/signals_arb_close?limit=100"
```

**查询参数**:
- `limit`: 返回条数上限，默认100，最大1000
- `direction`: 排序方向，`asc` 升序或 `desc` 降序（默认）
- `start`: 起始key（用于分页）
- `start_ts`: 起始时间戳（微秒）
- `end_ts`: 结束时间戳（微秒）

**示例：按时间范围查询**
```bash
# 查询 2025-11-20 17:00:00 到 18:00:00 之间的信号
START_TS=1732122000000000  # 微秒
END_TS=1732125600000000

curl "http://localhost:8088/signals/signals_arb_open?start_ts=${START_TS}&end_ts=${END_TS}&limit=500"
```

### 1.2 获取单个信号 (JSON)

```bash
# 根据 key 获取特定信号
KEY="1732122123456789_123456"
curl "http://localhost:8088/signals/signals_arb_open/${KEY}"
```

### 1.3 导出信号数据 (Parquet)

```bash
# 导出 ArbOpen 信号到 parquet 文件
curl "http://localhost:8088/signals/signals_arb_open/export?limit=1000" \
  -o arb_open_signals.parquet

curl "http://localhost:8088/signals/signals_arb_hedge/export?limit=10000" \
  -o arb_hedge_signals.parquet

# 按时间范围导出
curl "http://localhost:8088/signals/signals_arb_hedge/export?start_ts=${START_TS}&end_ts=${END_TS}&limit=5000" \
  -o arb_hedge_signals.parquet
```

**Parquet导出参数**:
- `limit`: 最大10000条
- 支持 `start_ts`, `end_ts`, `direction` 参数

### 1.4 删除信号数据

```bash
# 删除单个信号
curl -X DELETE "http://localhost:8088/signals/signals_arb_open/${KEY}"

# 批量删除多个信号
curl -X DELETE "http://localhost:8088/signals/signals_arb_open" \
  -H "Content-Type: application/json" \
  -d '{"keys": ["key1", "key2", "key3"]}'
```

---

## 二、Trade Update 成交数据

### 2.1 获取成交列表 (JSON)

```bash
# 获取最近成交记录
curl "http://localhost:8088/trade_updates?limit=100&direction=desc"

# 按时间范围查询
curl "http://localhost:8088/trade_updates?start_ts=${START_TS}&end_ts=${END_TS}&limit=500"
```

**查询参数**: 与 Signal 相同

### 2.2 获取单个成交 (JSON)

```bash
# 根据 key 获取
KEY="1732122123456789"
curl "http://localhost:8088/trade_updates/${KEY}"
```

### 2.3 导出成交数据 (Parquet)

```bash
# 导出到 parquet
curl "http://localhost:8088/trade_updates/export?limit=5000" \
  -o trade_updates.parquet

# 按时间范围导出
curl "http://localhost:8088/trade_updates/export?start_ts=${START_TS}&end_ts=${END_TS}&limit=10000" \
  -o trade_updates_range.parquet
```

### 2.4 删除成交数据

```bash
# 删除单个
curl -X DELETE "http://localhost:8088/trade_updates/${KEY}"

# 批量删除
curl -X DELETE "http://localhost:8088/trade_updates" \
  -H "Content-Type: application/json" \
  -d '{"keys": ["key1", "key2"]}'
```

---

## 三、Order Update 订单更新数据

### 3.1 获取订单更新列表 (JSON)

```bash
# 获取最近订单更新
curl "http://localhost:8088/order_updates?limit=100&direction=desc"

# 按时间范围查询
curl "http://localhost:8088/order_updates?start_ts=${START_TS}&end_ts=${END_TS}&limit=500"
```

### 3.2 获取单个订单更新 (JSON)

```bash
KEY="1732122123456789"
curl "http://localhost:8088/order_updates/${KEY}"
```

### 3.3 导出订单更新数据 (Parquet)

```bash
# 导出到 parquet
curl "http://localhost:8088/order_updates/export?limit=5000" \
  -o order_updates.parquet

# 按时间范围导出
curl "http://localhost:8088/order_updates/export?start_ts=${START_TS}&end_ts=${END_TS}&limit=10000" \
  -o order_updates_range.parquet
```

### 3.4 删除订单更新数据

```bash
# 删除单个
curl -X DELETE "http://localhost:8088/order_updates/${KEY}"

# 批量删除
curl -X DELETE "http://localhost:8088/order_updates" \
  -H "Content-Type: application/json" \
  -d '{"keys": ["key1", "key2"]}'
```

---

## 四、健康检查

```bash
# 检查服务是否运行
curl "http://localhost:8088/health"
# 返回: ok
```

---

## 五、时间戳转换

Persist Manager 使用**微秒级时间戳**。

### Bash 转换示例

```bash
# 当前时间转微秒
NOW_US=$(date +%s)000000
echo "当前时间戳（微秒）: ${NOW_US}"

# 指定时间转微秒 (macOS/Linux)
START_TS=$(date -d "2025-11-20 17:00:00" +%s)000000
END_TS=$(date -d "2025-11-20 18:00:00" +%s)000000

# macOS 格式
START_TS=$(date -j -f "%Y-%m-%d %H:%M:%S" "2025-11-20 17:00:00" +%s)000000
```

### Python 转换示例

```python
from datetime import datetime

# 转换为微秒时间戳
dt = datetime(2025, 11, 20, 17, 0, 0)
ts_us = int(dt.timestamp() * 1_000_000)
print(f"时间戳（微秒）: {ts_us}")

# 微秒转回时间
ts_us = 1732122000000000
dt = datetime.fromtimestamp(ts_us / 1_000_000)
print(f"时间: {dt}")
```

---

## 六、完整使用示例

### 示例1: 导出今日所有 ArbOpen 信号

```bash
#!/bin/bash

# 计算今日零点和当前时间的微秒时间戳
TODAY_START=$(date -d "$(date +%Y-%m-%d) 00:00:00" +%s)000000
NOW=$(date +%s)000000

# 导出数据
curl "http://localhost:8088/signals/signals_arb_open/export?start_ts=${TODAY_START}&end_ts=${NOW}&limit=10000" \
  -o "arb_open_$(date +%Y%m%d).parquet"

echo "导出完成: arb_open_$(date +%Y%m%d).parquet"
```

### 示例2: 查询特定策略ID的成交记录

```bash
# 查询最近1000条成交，然后用jq过滤特定策略
STRATEGY_ID=1590578402

curl -s "http://localhost:8088/trade_updates?limit=1000" | \
  jq ".[] | select(.client_order_id == ${STRATEGY_ID})"
```

### 示例3: 批量清理旧数据

```bash
#!/bin/bash

# 获取7天前的数据key
SEVEN_DAYS_AGO=$(date -d "7 days ago" +%s)000000

# 获取所有旧数据的key
OLD_KEYS=$(curl -s "http://localhost:8088/trade_updates?end_ts=${SEVEN_DAYS_AGO}&limit=1000" | \
  jq -r '.[].key' | jq -R -s -c 'split("\n")[:-1]')

# 批量删除
curl -X DELETE "http://localhost:8088/trade_updates" \
  -H "Content-Type: application/json" \
  -d "{\"keys\": ${OLD_KEYS}}"
```

---

## 七、数据格式说明

### Signal JSON 响应格式
```json
{
  "key": "1732122123456789_123456",
  "ts_us": 1732122123456789,
  "strategy_id": 123456,
  "signal_type": "ArbOpen",
  "context": {
    "opening_leg": {
      "venue": 1,
      "bid0": 0.1104,
      "ask0": 0.1105
    },
    "opening_symbol": "SAGAUSDT",
    "amount": 905.7,
    "side": "Sell",
    "price": 0.1104
  },
  "payload_base64": "...",
  "record_ts_us": 1732122123456789
}
```

### Trade Update JSON 响应格式
```json
{
  "key": "1732122123456789",
  "ts_us": 1732122123456789,
  "event_time": 1732122123456789,
  "trade_time": 1732122123456000,
  "symbol": "SAGAUSDT",
  "trade_id": 12345678,
  "order_id": 1948044069,
  "client_order_id": 6831482218313940993,
  "side": "Sell",
  "price": 0.1104,
  "quantity": 271.9,
  "commission": 0.00012,
  "commission_asset": "USDT",
  "is_maker": true,
  "realized_pnl": 0.0,
  "trading_venue": "BinanceMargin",
  "cumulative_filled_quantity": 271.9,
  "order_status": "PartiallyFilled",
  "payload_base64": "..."
}
```

### Order Update JSON 响应格式
```json
{
  "key": "1732122123456789",
  "ts_us": 1732122123456789,
  "event_time": 1732122123456789,
  "symbol": "SAGAUSDT",
  "order_id": 1948044069,
  "client_order_id": 6831482218313940993,
  "side": "Sell",
  "order_type": "Limit",
  "time_in_force": "GTC",
  "price": 0.1104,
  "quantity": 905.7,
  "cumulative_filled_quantity": 271.9,
  "status": "PartiallyFilled",
  "raw_status": "PARTIALLY_FILLED",
  "execution_type": "Trade",
  "raw_execution_type": "TRADE",
  "trading_venue": "BinanceMargin",
  "payload_base64": "..."
}
```

---

## 八、常见问题

### Q1: 如何分页获取大量数据？

使用返回结果中最后一个记录的 `key` 作为下次请求的 `start` 参数：

```bash
# 第一页
curl "http://localhost:8088/trade_updates?limit=100" > page1.json

# 从第一页获取最后一个key
LAST_KEY=$(jq -r '.[-1].key' page1.json)

# 第二页
curl "http://localhost:8088/trade_updates?limit=100&start=${LAST_KEY}" > page2.json
```

### Q2: Parquet 文件如何读取？

**Python (pandas)**:
```python
import pandas as pd
df = pd.read_parquet('trade_updates.parquet')
print(df.head())
```

**Python (polars)**:
```python
import polars as pl
df = pl.read_parquet('trade_updates.parquet')
print(df.head())
```

### Q3: 如何修改默认端口？

启动 persist_manager 时使用 `--port` 参数：

```bash
./target/release/persist_manager --port 9090
```

---

## 附录：Signal 类型说明

| 类型路径 | 说明 |
|---------|------|
| `signals_arb_open` | 套利开仓信号 |
| `signals_arb_hedge` | 套利对冲信号 |
| `signals_arb_cancel` | 套利撤单信号 |
| `signals_arb_close` | 套利平仓信号 |
