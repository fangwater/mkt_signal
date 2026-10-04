# BasicAccountRiskMsg 字段映射

`BasicAccountRiskMsg` 是各 PM / 统一账户 producer 下发的账户级风险快照。

统一约定：

```text
margin_ratio = 1.0 表示强平边界
margin_ratio 越大越安全
```

金额字段统一为 USD 等值 `f64`。没有可靠来源的金额在固定宽度 IPC 中使用
`NaN`，在 JSON 视图中使用 `null`；不得把未知金额当作事实零值。
下面先记录现有 5 个 CEX 的公共风险字段，Hyperliquid 的差异见后文。

## Hyperliquid

Portfolio Margin 的 `margin_ratio` 来自 `spotState.portfolioMarginRatio`，
转换为 `min(0.95 / ratio, 1e12)`；原始比率为零时使用 `1e12`。
`borrowed_usd` 在借贷用户状态和储备估值均有效时，使用各 token 的
`borrow.value * oraclePx` 求和，包含当前应计利息。无法可靠对齐的 USD
权益、初始保证金、维持保证金和名义持仓金额保持未知，不从风险比率反推。

统一借贷余额另用 `BasicBorrowInterestMsg` 发送本金及当前应计利息。
PM spot 的净余额加上同批发布的借贷金额，构成统一 gross wallet，确保公共
`wallet - borrowed - interest` 仍等于交易所净余额。借贷数据独立保留 HTTP
接收时间，60 秒过期后账户快照不可继续维持交易就绪。
完整来源及查询恢复边界见 [Hyperliquid](hyperliquid.md)。

## 字段映射

| RiskMsg 字段 | Binance PM | OKX UA | Gate UA | Bitget UTA | Bybit UTA |
|---|---|---|---|---|---|
| `timestamp` | `/papi/v1/account.updateTime` | `account.uTime` | `unified.assets.t * 1000` | `uTime` / `updatedTime` / `ts` | `updatedTime` |
| `adj_equity_usd` | `accountEquity` | `adjEq` | `e` | `effEquity` | `totalMarginBalance` |
| `actual_equity_usd` | `actualEquity` | `totalEq`，缺失回退 `adjEq` | `e` | `totalEquity` / `accountEquity` | `totalEquity` |
| `maintenance_margin_usd` | `accountMaintMargin` | `mmr` | `abs(b) / (R / 100)` | `mmr` | `totalMaintenanceMargin` |
| `initial_margin_usd` | `accountInitialMargin` | `imr` | `abs(b) / (r / 100)` | `imr` | `totalInitialMargin` |
| `margin_ratio` | `uniMMR` | `mgnRatio` 直接用 | `R / 100` | `effEquity / mmr` | `1 / accountMMRate`，缺失回退 `totalMarginBalance / totalMaintenanceMargin` |
| `available_margin_usd` | 未提供（NaN） | 未提供（NaN） | `unified.assets.a`，直接使用 `total_available_margin` | 未提供（NaN） | 未提供（NaN） |

`available_margin_usd` 是当前 `BasicAccountRiskMsg` 固定宽度 IPC 格式的一部分，
在 `notional_usd` 后传输。所有 producer/consumer 使用同一份当前格式；发布时
需要同步更新账户监控和读取账户风险消息的进程。

## Producer 来源

| 交易所 | 来源 | Scope | 实现位置 |
|---|---|---|---|
| Binance | REST `/papi/v1/account`，5 秒轮询 | `BinanceUnified` | `src/trade_engine/query_parsers/binance_pm_account_risk.rs` |
| OKX | WS `account` 顶层字段 | `OkexUnified` | `src/parser/okex_account_event_parser.rs` |
| Gate | WS `unified.assets` | `GateUnified` | `src/parser/gate_account_event_parser.rs` |
| Bitget | WS `account` 顶层字段 | `BitgetUnified` | `src/parser/bitget_account_event_parser.rs` |
| Bybit | WS `wallet` 顶层字段 | `BybitUnified` | `src/parser/bybit_account_event_parser.rs` |

## 口径说明

- OKX `mgnRatio` 已是账户保证金率，直接作为 `margin_ratio` 使用，不做 `/100`。
- Gate `r` 是初始保证金率，`R` 是维持保证金率，`b` 是 margin balance，`l` 是 liabilities。Gate 的 rate 是百分比，所以 `margin_ratio = R / 100`。
- Gate `unified.assets.a` 是账户可用保证金，不能用 `e - initial_margin` 或
  USDT 钱包余额替代；缺失有效 `t` 的消息不能用于保证金恢复。
  Gate FR 收到有效且不超过 60 秒的快照后，以现有 2,000
  USDT 门槛更新开仓状态；只有快照时间晚于对应保证金拒单，才提前清除账户和
  币种的保证金错误锁。其他拒单锁保持各自规则。REST 在启动以及 WS 数据失效时
  补查，用交易所 `refresh_time` 与 WS 排序，避免旧响应覆盖新状态。
- Bitget 不直接信任 `mgnRatio` 字段，而是用 `effEquity / mmr` 计算统一口径。
- Bybit 优先使用 `accountMMRate` 反推统一口径；缺失时用 `totalMarginBalance / totalMaintenanceMargin` 回退。
