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

[risk_checks.binance_pm_um]

[risk_checks.binance_spot]
```

- `label` 字段仅用于生成可读的节点名后缀，可选。
- `channels` 为订阅的信号频道列表，需要与策略发布端一致。
- `risk_checks.*` 节默认使用内置的 REST 基址（`https://papi.binance.com`）、API Key/Secret 环境变量名称（`BINANCE_API_KEY` / `BINANCE_API_SECRET`）以及 `recvWindow=5000ms`，如需覆盖可在此节内添加同名字段。

运行命令示例：

```
cargo run --bin pre_trade
```

如需自定义配置路径，可设置 `PRE_TRADE_CFG` 环境变量。

## 统一账户初始化与风控快照

自 `pre_trade` 启动起，会先串行完成以下步骤，任何一步出错都会阻止后续 Iceoryx 订阅逻辑：

1. **UM 持仓快照**：`BinancePmUmAccountManager` 调用 `GET /papi/v1/um/account`，解析永续合约持仓（仅保留有仓位或有挂单保证金的符号），并缓存到内存。
2. **现货余额快照**：`BinancePmSpotAccountManager` 调用 `GET /papi/v1/balance`，支持数组或单对象返回两种格式，转换成结构化余额数据。
3. **敞口聚合**：`ExposureManager` 将上述两类快照按基础资产进行汇总，计算现货与合约的综合敞口，并额外提取 USDT 概况。
4. **标记价格表**：`PriceTable` 调用公共接口 `GET https://fapi.binance.com/fapi/v1/premiumIndex`，加载 union(UM 符号 ∪ 现货资产拼接 `USDT`) 的标记价/指数价，用于后续风险评估，随后通过 Iceoryx 订阅 `data_pubs/binance-futures/derivatives` 实时增量更新。

### 依赖的环境变量

两类 REST 请求都需要币安统一账户 API 权限：

- `BINANCE_API_KEY`
- `BINANCE_API_SECRET`

如需使用不同的环境变量名，可在 `risk_checks` 节中分别设置 `api_key_env` / `api_secret_env`。

### 三线表日志输出

初始化成功后，`pre_trade` 会按照三线表样式打印三组日志，方便快速核对持仓与余额（列宽根据内容自适应）：

1. `UM 持仓概览`
   - 列：`Symbol | Side | PosAmt | NetAmt | EntryPx | Lev | PosIM | OpenIM | uPnL`
   - `NetAmt` 会将双向持仓折算为带符号仓位（双向模式 LONG 视为正，SHORT 视为负）。
2. `现货资产概览`
   - 列：`Asset | TotalWallet | CrossAsset | CrossFree | CrossLocked | CrossBorrowed | UMWallet | UMUPNL | CMWallet | CMUPNL`
3. `现货+UM 敞口汇总`
   - 列：`Asset | SpotTotal | SpotFree | SpotLocked | UMNet | UMPosIM | UMOpenIM | Exposure`
   - `Exposure = SpotTotal + UMNet`，用于评估现货与合约头寸的叠加效果。

此外会单独打印 `USDT余额`，展示统一账户中 USDT 的钱包余额、全仓可用与锁定金额、UM/CM 子账户余额等关键字段。

## 敞口计算说明

敞口计算由 `ExposureManager` 实现，核心逻辑如下：

- **归集键**：
  - 现货部分按资产符号（例如 `BTC`、`ETH`）映射。
  - 合约部分会优先匹配现货余额中已出现的资产前缀；找不到时再按常见报价货币（`USDT/BUSD/USDC/FDUSD/BIDR/TRY`）切分，确保符号对齐。
  - USDT 特殊处理，只在现货汇总中展示，不参与敞口表。
- **UM 净仓**：
  - 单向持仓(`BOTH`)直接取 `positionAmt`；
  - 双向 LONG 记正、SHORT 记负（绝对值取自 `positionAmt`）。
- **Exposure 字段**：`spot_total_wallet + um_net_position`，若正值表示现货多头或净多敞口，负值表示整体净空。
- **Margin 信息**：`UMPosIM`、`UMOpenIM` 分别为持仓与挂单的起始保证金，便于衡量杠杆占用。
- **USDT 汇总**：额外记录 `total_wallet_balance`、`cross_margin_free/locked`、`um_wallet_balance`、`cm_wallet_balance` 等指标，方便快读资金余量。
- **标记价格表**：只保留在 UM/现货中出现的资产对应的 `xxxUSDT` 交易对（USDT 自身除外），记录 `markPrice/indexPrice/time` 三项指标，并在订阅 `data_pubs/binance-futures/derivatives` 时基于推送的 mark/index 消息实时刷新。
- **余额/仓位增量**：
  - `balanceUpdate` 事件按资产增减 `delta` 更新现货钱包余额。
  - `ACCOUNT_UPDATE_BALANCE` 事件以绝对值同步钱包/全仓余额。
  - `ACCOUNT_UPDATE_POSITION` 事件刷新 UM 仓位数量与价格，确保与统一账户实时对齐。

## 调试输出

在 `RUST_LOG=debug` 下，`pre_trade` 会记录每类事件的核心字段：

- 账户消息：事件类型 (`e`)、交易所时间 (`E`)、报文大小。
- 交易响应：请求类型、HTTP 状态、耗费配额等信息。
- 信号事件：频道、事件类型、帧/载荷字节数、事件时间戳、关键业务字段（符号、交易所等）。

这些日志便于后续扩展实际业务处理逻辑时快速定位问题。
