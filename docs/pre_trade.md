# pre_trade 模块说明

`pre_trade` 是一个单线程 Tokio 服务，负责汇聚多个内部信号源：

- 账户监控（Account Monitor）推送的账户变动消息（`account_pubs/<exchange>/pm`）。
- `trade_engine` 写入的订单执行响应（`order_resps/<exchange>`）。
- 策略侧（目前为 `funding_rate_strategy`）发布的交易信号事件。

服务在内部使用 `tokio::sync::mpsc` 合并上述数据源，并在收到事件时输出调试日志（时间戳、事件类型、载荷大小等），为后续补充业务逻辑预留接口。

## 统一的信号事件格式

为了让不同策略之间复用信号管线，`common::signal_event` 定义了统一的帧结构：

```
#[repr(C, align(8))]
struct SignalEventHeader {
    magic: u32,            // 固定为 0x5349474E ('SIGN')
    version: u16,          // 当前版本为 1
    event_type: u16,       // 信号类型枚举（例如 TradeSignal=1）
    payload_len: u32,      // payload 字节长度
    event_ts_ms: i64,      // 策略生成信号的时间戳（毫秒）
    publish_ts_ms: i64,    // 发布端打包时间（毫秒）
    reserved0: u32,
    reserved1: u32,
}
```

- 固定头长度 36 字节，总帧大小上限 1024 字节（`SIGNAL_EVENT_MAX_FRAME`）。
- `encode_frame` / `SignalEvent::parse` 负责序列化与解析；
- `SignalEventType` 扩展时只需在枚举中新增值即可。

当前提供的标准化载荷为 `TradeSignalData`，对应原始套利信号字段，保持与旧版 `TradeEvent` 一致：

```
pub struct TradeSignalData {
    pub symbol: String,
    pub exchange: String,
    pub event_type: String,    // open_long / close_short 等
    pub signal_type: String,   // spread / funding
    pub spread_deviation: f64,
    pub spread_threshold: f64,
    pub funding_rate: f64,
    pub predicted_rate: f64,
    pub loan_rate: f64,
    pub bid_price: f64,
    pub bid_amount: f64,
    pub ask_price: f64,
    pub ask_amount: f64,
    pub timestamp: i64,        // 信号产生时间（毫秒）
}
```

辅助函数 `encode_trade_signal` / `decode_trade_signal` 负责将结构体打包到统一帧或从帧中还原。

`funding_rate_strategy.rs` 已改为使用 `encode_trade_signal` 发布信号帧；`pre_trade` 在订阅策略频道时，通过 `SignalEvent::parse + decode_trade_signal` 完成解析。

## Iceoryx 节点命名

为避免节点名随进程 PID 变化，所有 Iceoryx `Node` 名称均固定在程序中：

- `funding_rate_strategy` 订阅端节点：`funding_rate_strategy`
- `pre_trade` 账户通道：`pre_trade_account[_<label>]`
- `pre_trade` 交易响应通道：`pre_trade_trade_resp[_<label>]`
- `pre_trade` 信号通道：`pre_trade_signal_<channel>`（按频道名进行静态映射，不含进程 ID）

信号频道目前约定常量 `mt_arbitrage`，若新增策略频道，只需在配置中追加频道名即可。

## 配置与运行

默认配置存放在 `config/pre_trade.toml`：

```
[account_stream]
service = "account_pubs/binance/pm"
label = "account_binance"

[trade_engine]
service = "order_resps/binance"
label = "trade_resp_binance"

[signals]
channels = ["mt_arbitrage"]
```

- `label` 字段仅用于生成可读的节点名后缀，可选。
- `channels` 为订阅的信号频道列表，需要与策略发布端一致。

运行命令示例：

```
cargo run --bin pre_trade
```

如需自定义配置路径，可设置 `PRE_TRADE_CFG` 环境变量。

## 调试输出

在 `RUST_LOG=debug` 下，`pre_trade` 会记录每类事件的核心字段：

- 账户消息：事件类型 (`e`)、交易所时间 (`E`)、报文大小。
- 交易响应：请求类型、HTTP 状态、耗费配额等信息。
- 信号事件：频道、事件类型、帧/载荷字节数、事件时间戳、关键业务字段（符号、交易所等）。

这些日志便于后续扩展实际业务处理逻辑时快速定位问题。

