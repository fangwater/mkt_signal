# CME TAS sparse 1s backtest + ylabel

`cme_tas_backtest` 从 TAS RocksDB 的 `cme_trade` + `cme_quote` 合成稀疏 1 秒 backtest 和 ylabel，写入 ClickHouse。已有的小时 KLL 表不改、不重跑。

**不算 Special。** 每个到期 RIC 自己一条序列，`symbol=CLG24`，不合成主力。盘口或成交都没变的秒不写。

同一张合约的 quote 必须按时间顺序算盘口，不能把一张 RIC 切开。不同到期 RIC 互不依赖，按 `workers` 分片并行扫。默认 32。

## 表

| 表 | 列 |
| --- | --- |
| `baseline.backtest_cme_tas_1s` | `ts, symbol, bid0p, bid0v, ask0p, ask0v, buy_high, sell_low, open, high, low, close, volume, turnover, midp` |
| `baseline.ylabel_cme_tas_{5s,10s,30s,1m,5m}` | `ts, symbol, twap, vwap, midp` |

ylabel 行 `ts=t` 存已闭合桶 `[t-horizon, t)`。只在该桶里有可打印成交或 mid 更新时写。

## 口径

- 秒 `t` 的盘口：严格早于 `t` 的最近有效（未交叉）L1。
- 成交：`[t, t+1)` 的 `cme_trade`。`aggressor=1` 进 `buy_high`，`=2` 进 `sell_low`，`=0` 仍进 OHLC / volume。
- 价量缺失、非正有限，或 `aggressor` 不是 0/1/2 的成交：打 `warn` 后跳过，不退出进程。
- 空秒、以及盘口相对上一根没变且没有成交的秒，跳过。
- 小时 KLL 仍是 `trade_notional_kll_cme_tas_hourly`。

## 运行

```bash
cd ~/fanghaizhou/mkt_signal
cargo run --release -p cme_tas_replay --bin cme_tas_backtest -- \
  --config config/cme_tas_backtest.toml
```

preprocess：[tas_backtest.md](../../preprocess/data_format/lseg/tas_backtest.md)。
