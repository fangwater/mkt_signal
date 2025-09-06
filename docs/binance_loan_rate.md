# Binance 借贷利率接入说明（签名接口）

本文档说明 FundingRateManager 如何获取币安跨保证金借贷利率（8h），以及如何本地测试。

## 环境变量

- `BINANCE_API_KEY`: 币安 API Key（只读权限即可）。
- `BINANCE_API_SECRET`: 币安 API Secret。
- `BINANCE_LOAN_RATE_OVERRIDES`（可选）: JSON 对象，覆盖/补充利率，示例：

  ```
  export BINANCE_LOAN_RATE_OVERRIDES='{"BTCUSDT":0.00005, "ETHUSDT":0.00006}'
  ```

优先级：覆盖 > 签名接口 > 默认。默认会为常见交易对（BTC/ETH/BNB/SOL/XRP/ADA/DOGE）设置 0.0001 的 8h 利率。

说明：币安接口返回的是 `dailyInterest`（日利率），系统内部转换为 `loan_rate_8h = dailyInterest / 3`。

### OKX / Bybit 借贷利率

同样支持签名接口获取（不需要 VIP，但需要 API Key/Secret 等）。如未配置，将使用覆盖或默认值。

- OKX：
  - `OKX_API_KEY`, `OKX_API_SECRET`, `OKX_API_PASSPHRASE`
  - `OKX_LOAN_RATE_OVERRIDES`（可选，JSON，以币种为键，如 `{ "BTC": 5e-5 }`）
  - 说明：代码默认请求 `GET /api/v5/asset/interest-rate`，并尝试从返回中解析 `ccy` 与 `dailyInterest/interestRate/rate` 字段。若你在 OKX 账户侧返回结构有差异，可以用覆盖变量修正，或联系我们调整端点。

- Bybit：
  - `BYBIT_API_KEY`, `BYBIT_API_SECRET`
  - `BYBIT_LOAN_RATE_OVERRIDES`（可选，JSON，币种为键）
  - 说明：代码默认请求 `GET /v5/asset/coin/query-info` 并尝试从 `result.list/rows/data` 中解析 `coin` 与 `dailyInterest/interestRate/rate`。如有差异，请用覆盖或联系我们调整。

## 访问的 API

- `GET https://api.binance.com/sapi/v1/margin/crossMarginData`（签名接口）
  - 必需参数：`timestamp`（毫秒）
  - 请求头：`X-MBX-APIKEY: <your-api-key>`
  - 签名方式：`HMAC-SHA256`，签名内容为 query string。
  - 返回中需包含：`coin`、`dailyInterest` 字段。

注意：不同账户/版本返回结构可能有差异，系统做了容错解析；如接口调用失败或数据不完整，将回退至覆盖或默认值。

## 本地测试（Demo）

1. 配置环境变量（如有 Key/Secret）：

   ```bash
   export BINANCE_API_KEY=your_key
   export BINANCE_API_SECRET=your_secret
   # 可选覆盖
   export BINANCE_LOAN_RATE_OVERRIDES='{"BTCUSDT":0.00005, "ETHUSDT":0.00006}'
   ```

2. 运行 Demo：

   ```bash
   cargo run --bin loan_rate_demo -- --exchange binance --symbols BTCUSDT,ETHUSDT,BNBUSDT
   cargo run --bin loan_rate_demo -- --exchange okex --symbols BTC-USDT-SWAP,ETH-USDT-SWAP
   cargo run --bin loan_rate_demo -- --exchange bybit --symbols BTCUSDT,ETHUSDT
   ```

   输出示例：

   ```
   symbol,predicted_funding_rate,loan_rate_8h
   BTCUSDT,0.00012345,0.00005000
   ETHUSDT,0.00006789,0.00006000
   BNBUSDT,0.00001234,0.00010000
   ```

## 与 s.py 的一致性

- 预测逻辑：使用最近 6 期资金费率均值预测下一期（等价于 s.py 的 rolling(6).mean().shift(1)）。
- 利率逻辑：`loan_rate_8h = dailyInterest / 3`，与 s.py 一致。

## 其他注意事项

- WS 订阅的 symbol 大小写不一定一致；查询时系统会进行大小写兜底匹配（原样/大写/小写）。
- 刷新时带有重试与指数退避（最多 3 次），以提高稳定性。
