# Viz WebSocket 消息格式

本文汇总 `viz_server` 当前通过 WebSocket 向前端推送的所有 JSON 报文类型及字段含义，方便前后端对齐接口。

## 链路概览
- `viz_server` 启动时会订阅 Iceoryx 中的 `binance_fr_signal_resample_msg`、`pre_trade_positions_resample`、`pre_trade_exposure_resample`、`pre_trade_risk_resample` 等频道，并定时采样账户快照。
- 所有订阅数据被编码为 JSON 字符串，通过同一个 WebSocket 广播给所有连接的客户端。
- WebSocket 地址为 `ws://<host>:<port><ws_path>`（`ws_path` 默认 `/ws`，可在 `config/viz.toml` 的 `http.ws_path` 中修改）。若 `http.auth_token` 配置了非空字符串，客户端需在连接 URL 中附带 `?token=<token>`。

> 当前实现未区分子频道，也没有额外的心跳/错误报文；一旦订阅失败或连接断开，客户端需自行重连。

## 通用约定
- 所有字段均使用 UTF-8 编码的 JSON 文本发送。
- 时间戳单位为毫秒（`ts` 或 `ts_ms`）并使用 64 位整型。
- 未出现的字段应当被视为“未知/留空”，前端解析时请保持容错以便后续扩展。

## 报文类型

当前 `viz_server` 为 relay 模式：仅转发带 `type` 字段的消息，不再生成周期性 Snapshot。

### 1. 资金费率切片 `fr_resample_entry`
- **触发来源**：`signal_pubs/binance_fr_signal_resample_msg` 新数据。
- **判别方式**：`type` 为 `fr_resample_entry`。

示例：
```json
{
  "type": "fr_resample_entry",
  "ts_ms": 1717496400456,
  "entry": {
    "symbol": "BTCUSDT",
    "ts_ms": 1717496400000,
    "funding_frequency": "8h",
    "spot_bid": 70000.4,
    "spot_ask": 70000.6,
    "fut_bid": 69995.3,
    "fut_ask": 69995.7,
    "bidask_sr": 7.3e-5,
    "askbid_sr": 8.5e-5,
    "funding_rate": 0.00012,
    "funding_rate_ma": 0.0001,
    "funding_rate_ma_lower": -0.0008,
    "funding_rate_ma_upper": 0.0008,
    "predicted_rate": 0.00015,
    "predicted_rate_lower": -0.0001,
    "predicted_rate_upper": 0.0001,
    "loan_rate_8h": 0.0002,
    "bidask_lower": -0.0003,
    "bidask_upper": 0.0006,
    "askbid_lower": -0.0002,
    "askbid_upper": 0.0005
  }
}
```

说明：
- `ts_ms`：服务器写入广播的时刻，可用于检测延迟。
- `entry` 字段与 `FundingRateArbResampleEntry` 结构一致；`predicted_*`、`loan_rate_8h` 等字段可能为 `null`。

### 2. 预交易持仓 `pre_trade_positions`
- **触发来源**：`pre_trade_positions_resample`。
- **判别方式**：`type` 为 `pre_trade_positions`。

示例：
```json
{
  "type": "pre_trade_positions",
  "ts_ms": 1717496400789,
  "entry": {
    "ts_ms": 1717496400000,
    "rows": [
      {
        "asset": "BTC",
        "open_qty": 1.2,
        "hedge_qty": -1.0,
        "net_qty": 0.2,
        "net_usdt": 13000
      },
      {
        "asset": "ETH",
        "open_qty": 0.0,
        "hedge_qty": 10.5,
        "net_qty": 10.5,
        "net_usdt": 36750
      }
    ]
  }
}
```

说明：
- `rows[]`：每个资产一行，统一展示 open 和 hedge 两侧的持仓情况
  - `asset`：基础资产名称（如 "BTC", "ETH"）
  - `open_qty`：open 侧净持仓数量（spot 或 um 其中一个）
  - `hedge_qty`：hedge 侧净持仓数量（spot 或 um 其中一个）
  - `net_qty`：净敞口（币计）= open_qty + hedge_qty
  - `net_usdt`：净敞口（U 计）

### 3. 预交易敞口 `pre_trade_exposure`
- **触发来源**：`pre_trade_exposure_resample`。
- **判别方式**：`type` 为 `pre_trade_exposure`。

示例：
```json
{
  "type": "pre_trade_exposure",
  "ts_ms": 1717496400911,
  "entry": {
    "ts_ms": 1717496400000,
    "rows": [
      {
        "asset": "BTC",
        "spot_qty": 1.2,
        "spot_usdt": 84000,
        "um_net_qty": -0.8,
        "um_net_usdt": -56000,
        "exposure_qty": 0.4,
        "exposure_usdt": 28000,
        "is_total": false
      },
      {
        "asset": "TOTAL",
        "spot_qty": null,
        "spot_usdt": null,
        "um_net_qty": null,
        "um_net_usdt": null,
        "exposure_qty": null,
        "exposure_usdt": 28000,
        "is_total": true
      }
    ]
  }
}
```

说明：
- 每行对应 `PreTradeExposureRow`，将现货与合约资产折算为 USDT。
- 总计行（`is_total = true`）仅提供敞口合计，数量字段为空。

### 4. 预交易风险指标 `pre_trade_risk`
- **触发来源**：`pre_trade_risk_resample`。
- **判别方式**：`type` 为 `pre_trade_risk`。

示例：
```json
{
  "type": "pre_trade_risk",
  "ts_ms": 1717496401042,
  "entry": {
    "ts_ms": 1717496400000,
    "total_equity": 120000,
    "total_exposure": 50000,
    "total_position": 60000,
    "spot_equity_usd": 150000,
    "borrowed_usd": 20000,
    "interest_usd": 1000,
    "um_unrealized_usd": -9000,
    "leverage": 0.5,
    "max_leverage": 2.5
  }
}
```

说明：
- `leverage` 按 `total_position / total_equity` 计算，权益为 0 时置为 0。
- `max_leverage` 来源于风险管控配置，帮助判断是否逼近杠杆上限。

## 客户端接入建议
- 连接建立后即可开始接收上述多种报文，需按 `type` 判断具体处理逻辑；对没有 `type` 的报文，可通过是否包含 `account` 字段识别为账户快照。
- 若需要初始全量数据，前端应等待下一次重采样推送（当前实现不提供额外的历史补偿）。
- 建议在客户端记录 `ts_ms` 与本地时间的差值，用于绘制延迟或掉线提示。
