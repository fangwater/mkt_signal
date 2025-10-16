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

### 1. 账户快照 `Snapshot`
- **触发来源**：采样器 `Sampler::build_snapshot()` 每 `sampling.interval_ms` 毫秒生成一次。
- **判别方式**：报文顶层没有 `type` 字段，直接包含以下键值。

示例：
```json
{
  "ts": 1717496400123,
  "account": {
    "total_equity": 123456.78,
    "abs_total_exposure": 3456.7,
    "exposures": [
      {
        "asset": "USDT",
        "spot_total_wallet": 100000,
        "spot_cross_free": 80000,
        "spot_cross_locked": 20000,
        "um_net_position": 0,
        "um_position_initial_margin": 0,
        "um_open_order_initial_margin": 0,
        "exposure": 0
      }
    ]
  },
  "prices": [
    {"symbol": "BTCUSDT", "mark": 70000.5, "index": 69995.1, "ts": 1717496399000}
  ],
  "funding": [
    {"symbol": "BTCUSDT", "funding_rate": 0.0001, "predicted_funding_rate": 0, "next_funding_time": null, "loan_rate_8h": 0}
  ],
  "factors": [
    {"symbol": "BTCUSDT", "basis": 7.7e-5, "open_threshold": 0.0015, "close_threshold": 0.0005}
  ],
  "pnl": {"unrealized": 123.4},
  "stale": {"account": false, "prices": false, "funding": false}
}
```

字段说明：
- `account.total_equity`：账户权益估算。
- `account.abs_total_exposure`：所有资产敞口绝对值之和。
- `account.exposures[]`：`ExposureItem` 结构，逐项列出资产维度的仓位与保证金信息。
- `prices[]`：`PriceItem` 结构，包含标记价/指数价及其时间戳。
- `funding[]`：`FundingItem` 结构，当前实时资金费率及预测值（未接入的值会返回 `0` 或 `null`）。
- `factors[]`：`FactorItem` 结构，利用价格计算得到的基差与阈值。
- `pnl.unrealized`：根据当前标记价估算的未实现盈亏。
- `stale`：简单的过期标记，若某类数据超过 5 秒未更新则为 `true`。

### 2. 资金费率切片 `fr_resample_entry`
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

### 3. 预交易持仓 `pre_trade_positions`
- **触发来源**：`pre_trade_positions_resample`。
- **判别方式**：`type` 为 `pre_trade_positions`。

示例：
```json
{
  "type": "pre_trade_positions",
  "ts_ms": 1717496400789,
  "entry": {
    "ts_ms": 1717496400000,
    "um_positions": [
      {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "position_amount": 0.5,
        "entry_price": 65000,
        "leverage": 10,
        "position_initial_margin": 3250,
        "open_order_initial_margin": 0,
        "unrealized_profit": 2500
      }
    ],
    "spot_balances": [
      {
        "asset": "USDT",
        "total_wallet": 80000,
        "cross_free": 60000,
        "cross_locked": 20000,
        "cross_borrowed": 0,
        "cross_interest": 0,
        "um_wallet": 1000,
        "um_unrealized_pnl": 0
      }
    ]
  }
}
```

说明：
- `um_positions[]`：来自合约账户的逐仓/全仓头寸，`side` 取值为 `LONG`/`SHORT`/`BOTH`。
- `spot_balances[]`：现货资产余额与交叉保证金占用情况，字段直接对应 `PreTradeSpotBalanceRow`，新增 `cross_interest` 表示借贷利息。

### 4. 预交易敞口 `pre_trade_exposure`
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

### 5. 预交易风险指标 `pre_trade_risk`
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
- 若需要初始全量数据，前端应缓存最近一次 `Snapshot` 或自行轮询 REST 备份接口；当前实现不提供额外的历史补偿。
- 建议在客户端记录 `ts_ms` 与本地时间的差值，用于绘制延迟或掉线提示。
