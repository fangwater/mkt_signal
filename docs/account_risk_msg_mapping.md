# BasicAccountRiskMsg 字段映射

`BasicAccountRiskMsg` 是各 PM / 统一账户 producer 下发的账户级风险快照。

统一约定：

```text
margin_ratio = 1.0 表示强平边界
margin_ratio 越大越安全
```

金额字段统一为 USD 等值 `f64`。这里只记录 5 个交易所都能稳定对齐的公共风险字段。

## 字段映射

| RiskMsg 字段 | Binance PM | OKX UA | Gate UA | Bitget UTA | Bybit UTA |
|---|---|---|---|---|---|
| `timestamp` | `/papi/v1/account.updateTime` | `account.uTime` | `unified.assets.t * 1000` | `uTime` / `updatedTime` / `ts` | `updatedTime` |
| `adj_equity_usd` | `accountEquity` | `adjEq` | `e` | `effEquity` | `totalMarginBalance` |
| `actual_equity_usd` | `actualEquity` | `totalEq`，缺失回退 `adjEq` | `e` | `totalEquity` / `accountEquity` | `totalEquity` |
| `maintenance_margin_usd` | `accountMaintMargin` | `mmr` | `abs(b) / (R / 100)` | `mmr` | `totalMaintenanceMargin` |
| `initial_margin_usd` | `accountInitialMargin` | `imr` | `abs(b) / (r / 100)` | `imr` | `totalInitialMargin` |
| `margin_ratio` | `uniMMR` | `mgnRatio` 直接用 | `R / 100` | `effEquity / mmr` | `1 / accountMMRate`，缺失回退 `totalMarginBalance / totalMaintenanceMargin` |

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
- Bitget 不直接信任 `mgnRatio` 字段，而是用 `effEquity / mmr` 计算统一口径。
- Bybit 优先使用 `accountMMRate` 反推统一口径；缺失时用 `totalMarginBalance / totalMaintenanceMargin` 回退。
