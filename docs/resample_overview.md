# 资金费率 Resample 数据总览

## 1. 数据链路
- `funding_rate_strategy` 每 `resample_ms`（默认 3000ms）采样一轮所有跟踪 symbol 的行情、资金、阈值信息。
- 对每个 symbol 生成一条 `FundingRateArbResampleEntry`，经 bincode 序列化后写入 Iceoryx 服务 `signal_pubs/binance_fr_signal_resample_msg`（payload = `[len_le_u32 | bytes]`）。
- `viz` 侧订阅同一服务，反序列化为结构化数据，落入内存 state，并可按需广播至前端或写盘。

> **说明**：已移除批量 `ResampleBatch` 机制，所有消费者均按 **流式切片** 处理；若需要批量视图，可在内存自行聚合。

## 2. `FundingRateArbResampleEntry` 字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `symbol` | `String` | 交易对（现货符号命名）。 |
| `ts_ms` | `i64` | 该条数据生成时间（毫秒）。 |
| `funding_frequency` | `String` | 推断的资金结算周期（`"4h"`/`"8h"` 等）。 |
| `spot_bid` / `spot_ask` | `Option<f64>` | 最新现货买/卖价，为 0 或缺值时置 `null`。 |
| `fut_bid` / `fut_ask` | `Option<f64>` | 最新 U 本位永续买/卖价，为 0 或缺值时置 `null`。 |
| `bidask_sr` | `Option<f64>` | `(spot_bid - fut_ask) / spot_bid`。 |
| `askbid_sr` | `Option<f64>` | `(spot_ask - fut_bid) / spot_ask`。 |
| `funding_rate` | `Option<f64>` | 最新官方资金费率（无/0 置 `null`）。 |
| `funding_rate_ma` | `Option<f64>` | 策略维护的 MA 序列（默认近 60 次）。 |
| `funding_rate_ma_lower` / `funding_rate_ma_upper` | `Option<f64>` | 平仓阈值：MA 超过上限→平空，跌破下限→平多。 |
| `predicted_rate` | `Option<f64>` | 预测资金费率（向前偏移后的“预测值”）。 |
| `predicted_rate_lower` / `predicted_rate_upper` | `Option<f64>` | 开仓阈值：预测值 ≥ 上限触发做空（现货多 + 合约空），≤ 下限触发反向提示。 |
| `loan_rate_8h` | `Option<f64>` | 借贷利率（8h 等效）。 |
| `bidask_lower` / `bidask_upper` | `Option<f64>` | 价差阈值：小于 `lower` 触发开仓，大于 `upper` 触发平仓。 |
| `askbid_lower` / `askbid_upper` | `Option<f64>` | 反向价差阈值：大于 `lower` 触发开仓，小于 `upper` 触发平仓（若阈值≈0 视为“自动满足”，字段置 `null`）。 |

### 序列化注意
- 数值为 `0.0` 但代表“无效”时会主动转为 `None`，以降低前端判定复杂度。
- 阈值始终发送（来自当前策略内存状态），方便前端做着色和提示。

## 3. 消费端指引

### 3.1 解码流程
1. 读取前 4 字节（`u32` 小端）得到载荷长度 `len`。
2. 截取 `[4, 4+len)` 作为 bincode 数据，反序列化为 `FundingRateArbResampleEntry`。
3. 将 `entry` 写入本地缓存（`HashMap<String, Entry>`），用于构建快照。

### 3.2 前端广播建议
- `viz_server` 推荐维护一个 `HashMap<Symbol, Entry>`，并在收到新条目后广播 JSON：
  ```json
  {
    "type": "fr_resample_entry",
    "ts_ms": 1712132323456,
    "entry": { /* FundingRateArbResampleEntry 转 JSON，字段与上表一致 */ }
  }
  ```
- 可选：周期性（如 3s）打包发送
  ```json
  { "type": "fr_resample_snapshot", "ts_ms": 1712132323456, "entries": [ ... ] }
  ```
  用于前端初次订阅或断线重连后的全量同步。

### 3.3 触发判定辅助
- **价格/价差**：如果 `bidask_sr < bidask_lower` 或 `bidask_sr > bidask_upper` → 价差触发。`askbid_sr` 类似。
- **资金预测**：`predicted_rate ≥ predicted_rate_upper` → 做空信号；`≤ predicted_rate_lower` → 反向提示。
- **资金 MA**：`funding_rate_ma ≥ funding_rate_ma_upper` → 平空；`≤ funding_rate_ma_lower` → 平多。
- 前端可据此决定颜色、排序或告警。

## 4. 可视化要求（对接前端）

| 项目 | 具体要求 |
| --- | --- |
| 表格展示 | 每个 symbol 一行，列包含 `symbol`、`bidask_sr`、`askbid_sr`、`predicted_rate`、`funding_rate_ma`、`funding_rate`、`loan_rate_8h`、`updated_at` 等。 |
| 着色规则 | - `值 < lower` → 背景红色<br>- `值 > upper` → 背景绿色<br>- `lower ≤ 值 ≤ upper` → 背景深色（黑/深灰）<br>- 阈值自身不直接显示，只在鼠标悬停时通过 tooltip 提示 `lower/upper`。 |
| 排序策略 | 以“触发状态”优先（任意指标越界即视为触发），触发项在前；同组内按 `ts_ms` 逆序（最新靠前）。 |
| 交互 | - 全天候自动排序刷新<br>- 顶部状态栏显示 WS 连接状态、最近刷新时间<br>- 支持 Symbol 过滤（输入框）<br>- Hover 提示值与阈值、资金频率等信息 |
| 兼容性 | 优先支持现代浏览器（Chromium/Firefox/Safari）。移动端可退化为列表模式。 |

## 5. 示例 JSON
```json
{
  "type": "fr_resample_entry",
  "ts_ms": 1712132323456,
  "entry": {
    "symbol": "BTCUSDT",
    "ts_ms": 1712132323000,
    "funding_frequency": "8h",
    "spot_bid": 70500.3,
    "spot_ask": 70500.5,
    "fut_bid": 70488.2,
    "fut_ask": 70488.6,
    "bidask_sr": 0.00017,
    "askbid_sr": 0.00032,
    "funding_rate": 0.000092,
    "funding_rate_ma": 0.000084,
    "funding_rate_ma_lower": -0.0008,
    "funding_rate_ma_upper": 0.0008,
    "predicted_rate": 0.00011,
    "predicted_rate_lower": -0.00008,
    "predicted_rate_upper": 0.00008,
    "loan_rate_8h": 0.00014,
    "bidask_lower": -0.0002,
    "bidask_upper": 0.0005,
    "askbid_lower": 0.0012,
    "askbid_upper": 0.0002
  }
}
```

以上结构即前端和第三方系统解析的基准格式，后续若扩展其它 resample 类型，请新增独立文档条目并保持向前兼容。
