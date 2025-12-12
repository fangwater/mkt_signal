# FR 可视化面板文档

## 目标
- 不再在 `fr_signal` 里打印三线表。
- `fr_signal` 周期性发布结构化状态快照到 IceOryx。
- 单一 `fr_visualization` 进程默认订阅 5 个交易所快照，并通过 WebSocket 推送 JSON。
- Web 前端实时订阅 WebSocket，按交易所展示表格。
- Python 后端只负责页面导航/路由，不负责 WS 转发。

---

## 组件与数据流

### 1) fr_signal（每个交易所一个进程）
- 作用：订阅行情/费率 → 计算信号 → 每 10 秒生成快照并发布到 IceOryx。
- 发布通道（按 exchange 隔离）：
  - `fr_signal_state_binance`
  - `fr_signal_state_okex`
  - `fr_signal_state_bybit`
  - `fr_signal_state_bitget`
  - `fr_signal_state_gate`
- 快照结构定义：`src/funding_rate/fr_signal_state.rs`

### 2) fr_visualization（单进程，聚合订阅 + WS 后端）
- 作用：默认订阅上述 5 个通道 → 解码 batch → 转 JSON → 广播给所有 WS 客户端。
- WebSocket 服务：
  - 端口：`--port`（默认 8811）
  - 路径：`/ws`
- 推送 JSON 包含 `"exchange"` 字段用于前端过滤。

### 3) Python Dashboard Server（页面后端）
- 作用：提供导航页与 dashboard 页面；不处理 WS。
- 路由：
  - `/`：导航页，5 个 exchange 入口
  - `/fr_signal/{exchange}`：FR Signal dashboard 页面
- 文件：`scripts/fr_dashboard_server.py`

### 4) Web 前端（FR Signal Dashboard）
- 文件：`docs/fr_signal_dashboard.html`
- 作用：连接同源 `/ws`（或 `?ws=...`）实时订阅；只渲染匹配自己 exchange 的消息。
- exchange 获取方式：
  - 优先 query `?exchange=binance`
  - 或从路径 `/fr_signal/binance` 推断

---

## JSON 协议

`fr_visualization` 每次推送一个 batch（单交易所）：

```json
{
  "exchange": "binance",
  "ts_us": 1733980000000000,
  "entries": [
    {
      "symbol": "IMX-USDT-SWAP",
      "pred_fr_pct": -0.021,
      "fr_ma_pct": -0.031,
      "pred_loan_pct": 0.022,
      "cur_loan_pct": 0.001,
      "fr_plus_pred_loan_pct": 0.006,
      "ma_plus_cur_loan_pct": -0.030,
      "fr_sig": "BwdOpen",
      "spread_sig": "FwdOpen",
      "spread_threshold": 0.00025,
      "spread_used_value": 0.00023,
      "spread_compare_op": "LessThan",
      "spread_type": "BidAsk",
      "spread_bidask": 0.00023,
      "spread_askbid": 0.00041,
      "spread_rate": 0.00019,
      "final_sig": "BwdOpen"
    }
  ]
}
```

说明：
- `*_pct` 字段按旧表展示口径（已乘 100，例如 0.0005 -> 0.05）。
- `spread_threshold/spread_used_value` 无信号时为 `null`。
- `fr_sig/spread_sig/final_sig` 取值：
  `FwdOpen/FwdClose/BwdOpen/BwdClose/FwdCancel/BwdCancel/-`。

---

## 启动方式

### 1) 启动 5 个 fr_signal

```bash
cargo run --bin fr_signal -- --exchange binance
cargo run --bin fr_signal -- --exchange okex
cargo run --bin fr_signal -- --exchange bybit
cargo run --bin fr_signal -- --exchange bitget
cargo run --bin fr_signal -- --exchange gate
```

### 2) 启动 fr_visualization（单进程）

```bash
cargo run --bin fr_visualization
# 或指定端口
cargo run --bin fr_visualization -- --port 8811
```

WebSocket 地址：
- `ws://<host>:8811/ws`（生产环境可由 nginx 代理成 wss）。

### 3) 启动 Python 页面后端

```bash
python3 scripts/fr_dashboard_server.py --bind 0.0.0.0 --port 8900
```

访问：
- 导航页：`http://<host>:8900/`
- 某交易所面板：`http://<host>:8900/fr_signal/binance`

---

## 目录/文件速查

- 快照结构：`src/funding_rate/fr_signal_state.rs`
- 快照生成：`src/funding_rate/decision.rs` 的 `collect_signal_state_entries`
- fr_signal 发布：`src/bin/fr_signal.rs`
- fr_visualization 聚合订阅 + WS：`src/bin/fr_visualization.rs`
- 前端页面：`docs/fr_signal_dashboard.html`
- Python 后端：`scripts/fr_dashboard_server.py`

---

## 常见问题

1) 面板无数据  
- 确认对应 `fr_signal --exchange X` 已启动且能正常拿到行情。  
- 确认 `fr_visualization` 日志里看到订阅的 5 个通道。  
- 等一个 10 秒发布周期。

2) 只看到别的交易所数据  
- 确认访问路径是 `/fr_signal/{exchange}`（前端按路径推断并过滤）。  
- 或手动带 `?exchange=binance`。

