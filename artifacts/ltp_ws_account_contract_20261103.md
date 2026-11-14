---
updatedAt: 2026-08-15T06:43:33.000Z
agentTools:
  projectIndex: https://apidocliquidity.readme.io/llms.txt
---

# User Data (orders, trades, assets, positions, accounts)

All push channels are activated automatically after a successful login. No explicit subscription is required.

***

## Orders

Pushed whenever an order changes state (e.g. NEW → OPEN → PARTIALLY\_FILLED → FILLED / CANCELLED). Each push reflects the latest snapshot of that order.

```json
{
    "channel":"Orders",
    "instId":"BINANCE_PERP_ETH_USDT",
    "data":{
        "portfolioId":"1702884522340000",
        "orderId":"1703213979730000",
        "clientOrderId":"1703213979730000",
        "exchangeType":"BINANCE",
        "businessType":"PERP",
        "sym":"BINANCE_PERP_ETH_USDT",
        "limitPrice":"2346",
        "orderQty":"0.01",
        "quoteOrderQty":"0",
        "side":"BUY",
        "exchangeOrderType":"LIMIT",
        "orderType": "DMA",
        "timeInForce":"GTC",
        "executedQty":"0",
        "executedAmount":"0",
        "executedAvgPrice":"0",
        "lastExecutedQty":"0",
        "lastExecutedPrice":"0",
        "lastExecutedAmount":"0",
        "fee":"10",
        "feeCoin":"",
        "rebate":"0.1",
        "rebateCoin":"",
        "orderState":"NEW",
        "updateAt":"1703213979731",
        "createAt":"1703213979731",
        "borrowAmount":"0",
        "borrowAsset":"",
        "reason":"",
        "leverage":"1",
        "positionSide":"NONE",
        "reduceOnly":false,
        "tpTriggerPrice":"",
        "tpTriggerType":"",
        "tpPrice":"",
        "slTriggerPrice":"",
        "slTriggerType":"",
        "slPrice":"",
        "action": "",
        "actionMsg": "",
        "cancelType": "",
        "amendType": ""
    }
}
```

| Field                 | Type    | Remark                                                                                                                                                                                                                                                                                                                                                                                                                          |
| --------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `channel`             | String  | Channel name: `Orders`                                                                                                                                                                                                                                                                                                                                                                                                          |
| `instId`              | String  | Subscribed trading pair identifier, e.g. `BINANCE_SPOT_BTC_USDT`, `BINANCE_PERP_BTC_USDT`                                                                                                                                                                                                                                                                                                                                       |
| `data`                | Object  | Order snapshot                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `>portfolioId`        | String  | Portfolio ID                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `>orderId`            | String  | System-generated unique order ID                                                                                                                                                                                                                                                                                                                                                                                                |
| `>clientOrderId`      | String  | Client-defined order ID (echoed from the place\_order request)                                                                                                                                                                                                                                                                                                                                                                  |
| `>exchangeType`       | String  | Exchange where the order was routed: `BINANCE`, `OKX`, `EDX`                                                                                                                                                                                                                                                                                                                                                                    |
| `>businessType`       | String  | Product type: `SPOT`, `PERP`, `MARGIN`                                                                                                                                                                                                                                                                                                                                                                                          |
| `>sym`                | String  | Trading pair identifier, e.g. `BINANCE_PERP_ETH_USDT`                                                                                                                                                                                                                                                                                                                                                                           |
| `>limitPrice`         | String  | Limit order price. Empty string for market orders.                                                                                                                                                                                                                                                                                                                                                                              |
| `>orderQty`           | String  | Order quantity. For Binance this is the base asset quantity (e.g. ETH in ETH/USDT). For OKX this is the number of contracts. Empty string when `quoteOrderQty` is used instead.                                                                                                                                                                                                                                                 |
| `>quoteOrderQty`      | String  | Order amount in quote asset. Used for spot market buy orders. `"0"` when not applicable; may be non-zero for some exchange-specific order types (e.g. perpetual market orders sized in quote currency)..                                                                                                                                                                                                                        |
| `>side`               | String  | Order direction: `BUY` or `SELL`                                                                                                                                                                                                                                                                                                                                                                                                |
| `>exchangeOrderType`  | String  | Order type sent to the exchange: `LIMIT` or `MARKET`                                                                                                                                                                                                                                                                                                                                                                            |
| `>orderType`          | String  | LTP order routing type. `DMA` means the order is sent directly to the exchange.                                                                                                                                                                                                                                                                                                                                                 |
| `>timeInForce`        | String  | How long the order remains active. `GTC` (Good Till Cancel), `IOC` (Immediate Or Cancel — unfilled portion is cancelled immediately), `FOK` (Fill Or Kill — fully filled or fully cancelled), `GTX` (Post-only, equivalent to GTC + maker-only)                                                                                                                                                                                 |
| `>executedQty`        | String  | Cumulative filled quantity so far                                                                                                                                                                                                                                                                                                                                                                                               |
| `>executedAmount`     | String  | Cumulative filled amount (price × quantity) so far                                                                                                                                                                                                                                                                                                                                                                              |
| `>executedAvgPrice`   | String  | Volume-weighted average fill price so far. `"0"` if no fills yet.                                                                                                                                                                                                                                                                                                                                                               |
| `>lastExecutedQty`    | String  | Quantity filled in the most recent fill event                                                                                                                                                                                                                                                                                                                                                                                   |
| `>lastExecutedPrice`  | String  | Price of the most recent fill                                                                                                                                                                                                                                                                                                                                                                                                   |
| `>lastExecutedAmount` | String  | Amount (price × quantity) of the most recent fill                                                                                                                                                                                                                                                                                                                                                                               |
| `>fee`                | String  | Cumulative trading fee for this order. Positive = fee charged (taker). `"0"` when the order earned a maker rebate instead (see `rebate` field).                                                                                                                                                                                                                                                                                 |
| `>feeCoin`            | String  | Currency of the trading fee. Empty string if no fee was charged.                                                                                                                                                                                                                                                                                                                                                                |
| `>rebate`             | String  | Cumulative maker rebate received. `"0"` for taker orders. For OKX spot: BUY orders return quote currency, SELL orders return base currency. For Binance spot: returns quote currency. For perpetuals: returns USDT.                                                                                                                                                                                                             |
| `>rebateCoin`         | String  | Currency of the rebate. Empty string if no rebate was received.                                                                                                                                                                                                                                                                                                                                                                 |
| `>orderState`         | String  | Current order status: `NEW` (accepted, pending submission to exchange), `OPEN` (resting on exchange order book), `PARTIALLY_FILLED` (partially filled), `FILLED` (fully filled), `CANCELLED` (cancelled), `REJECT` (rejected by exchange), `FAIL` (order placement failed)                                                                                                                                                      |
| `>updateAt`           | String  | Timestamp of this update (milliseconds)                                                                                                                                                                                                                                                                                                                                                                                         |
| `>createAt`           | String  | Timestamp when the order was created (milliseconds)                                                                                                                                                                                                                                                                                                                                                                             |
| `>reason`             | String  | Rejection or failure reason. Empty string if none.                                                                                                                                                                                                                                                                                                                                                                              |
| `>borrowAmount`       | String  | Amount borrowed for this order (margin orders only). `"0"` if not a margin order.                                                                                                                                                                                                                                                                                                                                               |
| `>borrowAsset`        | String  | Asset borrowed (margin orders only). Empty string if none.                                                                                                                                                                                                                                                                                                                                                                      |
| `>leverage`           | String  | Leverage applied to this order. `"1"` for spot.                                                                                                                                                                                                                                                                                                                                                                                 |
| `>positionSide`       | String  | Position side. `NONE` in one-way mode. `LONG` or `SHORT` in hedge mode.                                                                                                                                                                                                                                                                                                                                                         |
| `>reduceOnly`         | Boolean | `true` means this order can only reduce an existing position, not open or increase one.                                                                                                                                                                                                                                                                                                                                         |
| `>tpTriggerPrice`     | String  | Take-profit trigger price. Empty string if not set.                                                                                                                                                                                                                                                                                                                                                                             |
| `>tpTriggerType`      | String  | Price type used to trigger take-profit: `LAST_PRICE` or `MARK_PRICE`. Empty string if not set.                                                                                                                                                                                                                                                                                                                                  |
| `>tpPrice`            | String  | Take-profit limit price. `"0"` means execute at market price. Empty string if not set.                                                                                                                                                                                                                                                                                                                                          |
| `>slTriggerPrice`     | String  | Stop-loss trigger price. Empty string if not set.                                                                                                                                                                                                                                                                                                                                                                               |
| `>slTriggerType`      | String  | Price type used to trigger stop-loss: `LAST_PRICE` or `MARK_PRICE`. Empty string if not set.                                                                                                                                                                                                                                                                                                                                    |
| `>slPrice`            | String  | Stop-loss limit price. `"0"` means execute at market price. Empty string if not set.                                                                                                                                                                                                                                                                                                                                            |
| `>action`             | String  | In-progress action on the order. Empty string when idle. `CANCEL_PENDING` (cancel request submitted), `CANCEL_COMPLETED` (cancel confirmed by exchange), `CANCEL_FAILED` (cancel rejected by exchange), `AMEND_PENDING` (replace request submitted), `AMEND_COMPLETED` (replace confirmed by exchange), `AMEND_FAILED` (replace rejected by exchange)                                                                           |
| `>actionMsg`          | String  | Additional message for the current action. Usually empty.                                                                                                                                                                                                                                                                                                                                                                       |
| `>cancelType`         | String  | Reason the order was cancelled. Empty string if the order has not been cancelled. `USER` (cancelled by user), `MATCH_CANCEL` (cancelled by matching engine), `EXPIRE` (expired per timeInForce policy), `REDUCE_ONLY` (cancelled because it would have increased position size), `LIQUIDATING` (cancelled during account liquidation), `SYSTEM` (cancelled by system), `SYM_DELIST` (cancelled because the symbol was delisted) |
| `>amendType`          | String  | Reason for the most recent order amendment. Empty string if never amended. `USER` (user-initiated replace), `REDUCE_ONLY` (system reduced quantity to comply with reduce-only constraint)                                                                                                                                                                                                                                       |

***

## Trades

Pushed for each individual fill event. A single order may generate multiple Trades pushes if it is filled in multiple transactions.

```json
{
    "channel":"Trades",
    "instId":"BINANCE_PERP_APT_USDT",
    "data":{
        "transactionId": "38132969466022978",
        "portfolioId": "2066376093138754",
        "orderId": "2104172237333826",
        "exchangeType": "BINANCE",
        "businessType": "PERP",
        "sym": "BINANCE_PERP_APT_USDT",
        "side": "BUY",
        "quantity": "4",
        "price": "2.24509925",
        "tradingFee": "0.00089804",
        "tradingFeeCoin": "USDT",
        "rpnl": "0",
        "clientOrderId": "2104172237333826",
        "createAt": "1763977805203",
        "execType": "MAKER"
    }
}
```

| Name               | Type   | Description                                                                                         |
| ------------------ | ------ | --------------------------------------------------------------------------------------------------- |
| `channel`          | String | Channel name: `Trades`                                                                              |
| `instId`           | String | Subscribed trading pair identifier, e.g. `OKX_PERP_BTC_USDT`                                        |
| `data`             | Object | Fill event payload                                                                                  |
| `> transactionId`  | String | Unique identifier for this fill record                                                              |
| `> portfolioId`    | String | Portfolio ID                                                                                        |
| `> orderId`        | String | System-generated order ID this fill belongs to                                                      |
| `> exchangeType`   | String | Exchange where the fill occurred: `BINANCE`, `OKX`, `EDX`                                           |
| `> businessType`   | String | Product type: `SPOT`, `PERP`, `MARGIN`                                                              |
| `> sym`            | String | Trading pair identifier                                                                             |
| `> side`           | String | Fill side: `BUY` or `SELL`                                                                          |
| `> quantity`       | String | Quantity filled in this transaction                                                                 |
| `> price`          | String | Fill price of this transaction                                                                      |
| `> tradingFee`     | String | Fee for this fill. Positive = fee charged (taker). Negative = maker rebate credited to the account. |
| `> tradingFeeCoin` | String | Currency of the fee or rebate.                                                                      |
| `> rpnl`           | String | Realized PnL generated by this fill. `"0"` for opening trades.                                      |
| `> clientOrderId`  | String | Client-defined order ID (echoed from the original order)                                            |
| `> createAt`       | String | Timestamp of this fill (milliseconds)                                                               |
| `> execType`       | String | Liquidity role: `MAKER` (order rested on the book) or `TAKER` (order matched against the book)      |

***

## Assets

Pushed whenever the balance of any coin changes (e.g. after a fill, deposit, withdrawal, or funding fee). Each push covers only the coin(s) whose balance changed.

```json
{
    "channel": "Assets",
    "data": [{
        "portfolioId": "1711075287090000",
        "coin": "USDT",
        "exchangeType": "OKX",
        "businessType": "UNI",
        "balance": "99.012100925",
        "equity": "99.012100925",
        "available": "99.012100925",
        "frozen": "0",
        "overdraw": "0",
        "marginValue": "96.536798401875",
        "debt": "0",
        "virtualBorrow": "0",
        "debtMargin": "0",
        "perpMargin": "0",
        "maxTransferable": "0",
        "equityValue": "0",
        "borrow": "0",
        "upnl":"0",
        "updateAt": "1711955466958",
        "balanceDelta": "0",
        "eventType": "trading",
        "loan": "20",
        "loanValue": "20",
        "netEquity": "109.598539562734535877",
        "netEquityValue": "109.598539562734535877",
        "netMarginValue": "106.858576073666172481"
    }]
}
```

| Field              | Type   | Remark                                                                                                                                                                                         |
| ------------------ | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `channel`          | String | Channel name: `Assets`                                                                                                                                                                         |
| `data`             | Array  | List of coin balance snapshots that changed in this update                                                                                                                                     |
| `>portfolioId`     | String | Portfolio ID                                                                                                                                                                                   |
| `>coin`            | String | Coin symbol, e.g. `USDT`, `BTC`                                                                                                                                                                |
| `>exchangeType`    | String | Exchange this balance belongs to: `BINANCE`, `OKX`, \`EDX\`\`                                                                                                                                  |
| `>businessType`    | String | Account type. Returns `UNI` for all assets in the portfolio.                                                                                                                                   |
| `>balance`         | String | Total balance of this coin including frozen funds                                                                                                                                              |
| `>equity`          | String | Equity = balance + unrealized PnL from open positions                                                                                                                                          |
| `>available`       | String | Amount available for placing new orders or withdrawing                                                                                                                                         |
| `>frozen`          | String | Amount locked due to open orders or pending operations                                                                                                                                         |
| `>overdraw`        | String | Amount overdrawn beyond the available balance                                                                                                                                                  |
| `>debt`            | String | Outstanding borrowed amount not yet repaid (margin accounts)                                                                                                                                   |
| `>marginValue`     | String | Value of this coin counted as collateral margin (in USDT)                                                                                                                                      |
| `>virtualBorrow`   | String | Virtual borrow used by derivatives positions (no actual loan is taken out)                                                                                                                     |
| `>debtMargin`      | String | Margin reserved to cover debt obligations                                                                                                                                                      |
| `>perpMargin`      | String | Margin reserved for open perpetual contract positions                                                                                                                                          |
| `>maxTransferable` | String | Maximum amount that can be transferred out right now                                                                                                                                           |
| `>equityValue`     | String | Equivalent USDT value of this coin's equity                                                                                                                                                    |
| `>borrow`          | String | Amount currently borrowed                                                                                                                                                                      |
| `>upnl`            | String | Unrealized profit and loss from open positions denominated in this coin                                                                                                                        |
| `>updateAt`        | String | Timestamp of this update (milliseconds)                                                                                                                                                        |
| `>balanceDelta`    | String | Balance change that triggered this push. `"0"` when the push is caused by a position or order event with no direct balance movement. Negative value indicates a deduction (e.g. fee charged).. |
| `>eventType`       | String | Event that triggered this push. `trading` indicates a trade-related event.                                                                                                                     |
| `>loanValue`       | String | This field is included only when loan > 0. `loan × indexPrice`                                                                                                                                 |
| `>netEquity`       | String | This field is included only when loan > 0. Asset equity minus loan.                                                                                                                            |
| `>netEquityValue`  | String | This field is included only when loan > 0. `netEquity × indexPrice`                                                                                                                            |
| `>netMarginValue`  | String | This field is included only when loan > 0. `netEquityValue × discountRate`                                                                                                                     |

***

## Positions

Pushed whenever a position changes (e.g. after a fill, funding fee, or mark price update). Each push is a full snapshot of the affected position.

```json
{
    "channel":"Positions",
    "instId":"OKX_PERP_BTC_USDT",
    "data":{
        "portfolioId":"1702884522340000",
        "positionId":"1704179813908000",
        "sym":"OKX_PERP_BTC_USDT",
        "positionSide":"NONE",
        "positionMargin":"226.586",
        "positionMM":"4.53172",
        "positionQty":"1",
        "positionValue":"453.172",
        "entryValue":"453.194",
        "unrealizedPNL":"-0.022",
        "unrealizedPNLRate":"-0.000097088664015851",
        "avgPrice":"45319.4",
        "markPrice":"45317.2",
        "leverage":"2",
        "maxLeverage":"20",
        "riskLevel":"1",
        "fee":"0.0679791",
        "fundingFee":"0",
        "liqPrice":"1.1",
        "createAt":"1704179813908",
        "updateAt":"1704179813908"
    }
}
```

| Field                | Type   | Remark                                                                                                                                                                                                                             |
| -------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `channel`            | String | Channel name: `Positions`                                                                                                                                                                                                          |
| `instId`             | String | Subscribed trading pair identifier, e.g. `OKX_PERP_BTC_USDT`                                                                                                                                                                       |
| `data`               | Object | Position snapshot                                                                                                                                                                                                                  |
| `>portfolioId`       | String | Portfolio ID                                                                                                                                                                                                                       |
| `>positionId`        | String | Unique identifier for this position                                                                                                                                                                                                |
| `>sym`               | String | Trading pair identifier, e.g. `BINANCE_PERP_BTC_USDT`                                                                                                                                                                              |
| `>positionSide`      | String | Number of contracts/units held. In one-way mode (`positionSide: "NONE"`): positive = long, negative = short. In hedge mode (`positionSide: "LONG"` or `"SHORT"`): always positive — the direction is indicated by `positionSide`.. |
| `>positionMargin`    | String | Margin currently allocated to this position                                                                                                                                                                                        |
| `>positionMM`        | String | Maintenance margin required to keep this position open. If actual margin falls below this value, liquidation is triggered.                                                                                                         |
| `>positionQty`       | String | Number of contracts/units held. Positive = long, negative = short.                                                                                                                                                                 |
| `>positionValue`     | String | Current market value of the position (mark price × quantity)                                                                                                                                                                       |
| `>entryValue`        | String | Value at which the position was originally opened (entry price × quantity)                                                                                                                                                         |
| `>unrealizedPNL`     | String | Unrealized profit or loss based on the current mark price                                                                                                                                                                          |
| `>unrealizedPNLRate` | String | Unrealized PnL as a proportion of entry value                                                                                                                                                                                      |
| `>avgPrice`          | String | Volume-weighted average entry price of this position                                                                                                                                                                               |
| `>markPrice`         | String | Current mark price used for PnL calculation and liquidation checks                                                                                                                                                                 |
| `>leverage`          | String | Leverage currently applied to this position                                                                                                                                                                                        |
| `>maxLeverage`       | String | Maximum leverage allowed for this symbol                                                                                                                                                                                           |
| `>riskLevel`         | String | Risk tier (1–5). Higher values indicate stricter margin requirements and higher liquidation risk.                                                                                                                                  |
| `>fee`               | String | Cumulative net trading fee for this position. Positive = net fee paid. Negative = net rebate received (e.g. all fills were maker orders).                                                                                          |
| `>fundingFee`        | String | Cumulative funding fees paid (negative) or received (positive) for this position                                                                                                                                                   |
| `>liqPrice`          | String | Estimated liquidation price. `null` if there is no liquidation risk or the calculated price is outside a reasonable range.                                                                                                         |
| `>createAt`          | String | Timestamp when the position was first opened (milliseconds)                                                                                                                                                                        |
| `>updateAt`          | String | Timestamp of this update (milliseconds)                                                                                                                                                                                            |

***

## MarginCall

Pushed when the account's margin ratio approaches or crosses the liquidation threshold.

```json
{
    "channel":"MarginCall",
    "data":{
        "portfolioId":"1702884522340000",
        "exchangeType":"OKX",
        "margin":"141.598333333333333333",
        "maintainMargin":"4.24795",
        "uniMMR":"-0.220583246036323403",
        "accountStatus":"LIQUIDATED",
        "updateAt":"1703834254274"
    }
}
```

[block:parameters]
{
  "data": {
    "h-0": "Field",
    "h-1": "Type",
    "h-2": "Remark",
    "0-0": "`channel`",
    "0-1": "String",
    "0-2": "Channel name: `MarginCall`",
    "1-0": "`data`",
    "1-1": "Object",
    "1-2": "Margin call snapshot",
    "2-0": "`>portfolioId`",
    "2-1": "String",
    "2-2": "Portfolio ID",
    "3-0": "`>exchangeType`",
    "3-1": "String",
    "3-2": "Exchange this margin status applies to: `BINANCE`, `OKX`, `EDX`",
    "4-0": "`>margin`",
    "4-1": "String",
    "4-2": "Current total margin balance of the account",
    "5-0": "`>maintainMargin`",
    "5-1": "String",
    "5-2": "Minimum margin required across all positions to avoid liquidation",
    "6-0": "`>uniMMR`",
    "6-1": "String",
    "6-2": "`validMargin`/`maintainMargin`.  \nUnified maintenance margin rate, used to measure risk, a forced liquidation is triggered when the value is below 100%..",
    "7-0": "`>accountStatus`",
    "7-1": "String",
    "7-2": "Current account status: `NORMAL` (healthy), `MARGIN_CALL` (approaching liquidation threshold), `LIQUIDATED` (liquidation in progress)",
    "8-0": "`>updateAt`",
    "8-1": "String",
    "8-2": "Timestamp of this update (milliseconds)"
  },
  "cols": 3,
  "rows": 9,
  "align": [
    null,
    null,
    null
  ]
}
[/block]

> 📘 `MarginCall` is the warning. Once liquidation actually starts, the [Liquidation](https://apidocliquidity.readme.io/reference/ws-liquidation) channels report every step of the process and every liquidation order.

<br />

***

## Accounts

Push whenever the account status changes. Each push contains a full snapshot of the account information.

```json
{
    "channel": "Accounts",
    "data": {
        "portfolioId": "2178424043067202",
        "exchangeType": "BINANCE",
        "equity": "135.997795254213646297",
        "marginValue": "126.3772437379809635842714751341",
        "frozenMargin": "7.972976138666666667",
        "perpMargin": "1.306309472",
        "debtMargin": "0",
        "openLossMargin": "0.002286041576",
        "validMargin": "126.3749576964049635842714751341",
        "availableMargin": "118.4019815577382969172714751341",
        "maintainMargin": "1.08491011568",
        "positionValue": "6.53154736",
        "uniMMR": "116.484265258413286347",
        "riskRatio": "0.0086",
        "accountStatus": "NORMAL",
        "upnl": "-0.01245264",
        "perpAvailableMargin": "118.4019815577382969172714751341",
        "createAt": "1782288534085",
        "updateAt": "1782732414466",
        "totalLoanValue": "20",
        "netEquity": "115.997795254213646297",
        "netMarginValue": "106.8772437379809635851964751341",
        "netValidMargin": "106.8749576964049635851964751341",
        "netAvailableMargin": "98.9019815577382969181964751341",
        "loanMargin": "6.666666666666666667",
        "loanMM": "1",
        "ltv": "0.158259202333793917"
    }
}
```

[block:parameters]
{
  "data": {
    "h-0": "Field",
    "h-1": "Type",
    "h-2": "Remark",
    "0-0": "`channel`",
    "0-1": "String",
    "0-2": "Channel name: `Accounts`.",
    "1-0": "`data`",
    "1-1": "Object",
    "1-2": "Account snapshot.",
    "2-0": "`portfolioId`",
    "2-1": "String",
    "2-2": "Portfolio ID",
    "3-0": "`exchangeType`",
    "3-1": "String",
    "3-2": "Exchange this margin status applies to: `BINANCE`, `OKX`, `EDX`",
    "4-0": "`equity`",
    "4-1": "String",
    "4-2": "Total account equity on this venue, valued in USDT-equivalent (including cash balance and P&L).",
    "5-0": "`marginValue`",
    "5-1": "String",
    "5-2": "Total margin value on this venue.",
    "6-0": "`frozenMargin`",
    "6-1": "String",
    "6-2": "Locked margin (e.g. for open orders or positions) and unavailable for new trades or withdrawal.",
    "7-0": "`perpMargin`",
    "7-1": "String",
    "7-2": "Margin allocated specifically to perpetual contracts.",
    "8-0": "`debtMargin`",
    "8-1": "String",
    "8-2": "Margin used to cover borrowing and negative balances.",
    "9-0": "`openLossMargin`",
    "9-1": "String",
    "9-2": "Margin portion reserved to cover current open losses.",
    "10-0": "`validMargin`",
    "10-1": "String",
    "10-2": "`marginValue`-`openLossMargin`",
    "11-0": "`availableMargin`",
    "11-1": "String",
    "11-2": "Margin available for opening new positions or for withdrawal.",
    "12-0": "`maintainMargin`",
    "12-1": "String",
    "12-2": "Minimum margin required across all positions to avoid liquidation.",
    "13-0": "`positionValue`",
    "13-1": "String",
    "13-2": "Notional value of all open positions on this venue.",
    "14-0": "`uniMMR`",
    "14-1": "String",
    "14-2": "`validMargin`/`maintainMargin`.  \nUnified maintenance margin rate, used to measure risk, a forced liquidation is triggered when the value is below 100%.",
    "15-0": "`riskRatio`",
    "15-1": "String",
    "15-2": "1/`uniMMR`.",
    "16-0": "`accountStatus`",
    "16-1": "String",
    "16-2": "Current account status: `NORMAL` (healthy), `MARGIN_CALL` (approaching liquidation threshold), `LIQUIDATED` (liquidation in progress)",
    "17-0": "`upnl`",
    "17-1": "String",
    "17-2": "Unrealized profit and loss of the account in USDT.",
    "18-0": "`perpAvailableMargin`",
    "18-1": "String",
    "18-2": "Available margin for perpetual trading.",
    "19-0": "`createAt`",
    "19-1": "String",
    "19-2": "Timestamp when the position was first opened (milliseconds).",
    "20-0": "`updateAt`",
    "20-1": "String",
    "20-2": "Timestamp of this update (milliseconds).",
    "21-0": "`totalLoanValue`",
    "21-1": "String",
    "21-2": "Total loan value in USDT.  Pushed only if the account has any asset with loan > 0.",
    "22-0": "`netEquity`",
    "22-1": "String",
    "22-2": "Asset equity minus loan.  Pushed only if the account has any asset with loan > 0.",
    "23-0": "`netMarginValue`",
    "23-1": "String",
    "23-2": "`netEquityValue` × `discountRate`. Pushed only if the account has any asset with loan > 0.",
    "24-0": "`netValidMargin`",
    "24-1": "String",
    "24-2": "`netMarginValue` - `openLossMargin`.  Pushed only if the account has any asset with loan > 0.",
    "25-0": "`netAvailableMargin`",
    "25-1": "String",
    "25-2": "`netValidMargin` - `frozenMargin`.  Pushed only if the account has any asset with loan > 0.",
    "26-0": "`loanMargin`",
    "26-1": "String",
    "26-2": "`Σ loan` × `indexPrice` / `loanLeverage`.  Pushed only if the account has any asset with loan > 0.",
    "27-0": "`loanMM`",
    "27-1": "String",
    "27-2": "`Σ loan` × `indexPrice` × `loanMMR`.  Pushed only if the account has any asset with loan > 0.",
    "28-0": "`ltv`",
    "28-1": "String",
    "28-2": "`LTV` = `totalLoanValue` / `VaildMargin`, returns \"0\" when no loan is outstanding.  Pushed only if the account has any asset with loan > 0."
  },
  "cols": 3,
  "rows": 29,
  "align": [
    "left",
    "left",
    "left"
  ]
}
[/block]