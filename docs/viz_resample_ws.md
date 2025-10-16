# Viz Funding Resample WS 设计

## 1. 消息链路
1. `funding_rate_strategy` → Iceoryx `signal_pubs/binance_fr_signal_resample_msg`：写入 `FundingRateArbResampleEntry` 的二进制切片。
2. `viz` → 内部订阅并反序列化，维护 `HashMap<symbol, entry>`。
3. `viz_server` → 将最新 entry/快照编码为 JSON，通过 WebSocket 向前端推送。

> WebSocket URL 示例：`ws://<host>:<port>/ws?stream=fr_resample&token=...`。`stream`/`token` 的解析逻辑可自定义，但建议使用查询参数以兼容现有鉴权实现。

## 2. WebSocket 消息类型

### 2.1 实时切片：`fr_resample_entry`
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
- `ts_ms`：服务器发送时刻，可用于健康检查。
- `entry`：字段与 `FundingRateArbResampleEntry` 一致。
- 前端收到后更新缓存并重新渲染表格。

### 2.2 快照：`fr_resample_snapshot`（可选）
```json
{
  "type": "fr_resample_snapshot",
  "ts_ms": 1712132323456,
  "entries": [ /* FundingRateArbResampleEntry[] */ ]
}
```
- 建议在客户端初次连接或重连时发送一次全量，以避免短时空档。
- 若未实现快照，前端可在连接后等待若干秒以收全流式切片。

### 2.3 心跳与错误
- 心跳：`{"type":"ping","ts_ms":...}`；客户端原样返回 `pong` 或忽略。
- 错误：`{"type":"error","message":"unauthorized"}`，随后服务器主动断开连接。

## 3. 服务端实现要点
- 订阅 Iceoryx 后需在 Tokio 本地任务中循环拉取，失败时回退重试。
- 将反序列化出的 entry 写入 `SharedState`（如 `RwLock<HashMap<String, Entry>>`）。
- 当有订阅者连接时：
  1. 若启用快照，先发送一次 `fr_resample_snapshot`。
  2. 监听广播 channel，实时发送 `fr_resample_entry`。
- 为避免刷屏，可在后台节流日志，将 entry 打印级别控制在 `debug`。

## 4. 前端消费规范
- **排序**：触发信号（任一指标越界）优先，其次按 `ts_ms` 降序。
- **阈值展示**：阈值不直接出现在表格列中，而是在单元格 hover 的 tooltip 中展示 `lower / upper`。
- **颜色**：
  - `< lower` → 背景红。
  - `> upper` → 背景绿。
  - `[lower, upper]` → 背景深色（黑/深灰）。
- **断线重连**：使用 `WebSocket#onclose` → 定时重连；重连后等待快照或连续 entry 以恢复列表。
- **扩展兼容**：留意未来可能新增字段（例如风险敞口、手续费等），前端应采用宽松解析策略（忽略未知字段）。

## 5. 事件流示例
1. 客户端连接 → 服务器返回 `fr_resample_snapshot`（100 条）。
2. 后续每 3s 左右接收到若干 `fr_resample_entry`。
3. 若 10s 未收到数据，可向 `/healthz` 轮询或显示“延迟”提示。
4. 发现 `type="error"` → 展示消息并停止重连。

这一规范旨在确保流式 resample 数据被一致、低延迟地传递至前端；若未来加入多种 resample 类型，可在 `type` 上新增命名并共享上述握手流程。
