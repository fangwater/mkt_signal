# RocksDB vs summarised 1Min

独立核对工具，**不是** `cme_tas_replay`。Replay 只负责把 TAS 洗进 RocksDB。这个二进制只读库，按 UTC 分钟左端合成 `Open/High/Low/Last/Volume/No. Trades`，去对 summarised 源：

```text
/mnt/hdd-raid5-72t/liang_torch/lseg_data/future/summarised/
  shanghai_evolution_futures_1m_summaries_ric_list_0_sum_2026-01-01_2026-06-01/merged-Data.csv.gz
```

RocksDB 用 secondary 打开，不抢 replay 的锁。

## 比什么

只比有价分钟的 Open/High/Low/Last/Volume。OHLC 只用来自 `cme_trade` 的可打印成交。`cme_special` 单独扫，用来解释 Summary 多出来的量；不要把 Special 加进 Open/High/Low/Last。Volume 必须等于可打印量，或可打印量加 Special 量；剩下的差立刻失败。只有量、OHLC 空的分钟不算有价失败。有价分钟缺一边立刻失败。5 个交易日的 replay 停在 `ADF26`，默认对这一只。

## 运行

等 replay 日志里 `written_trades` 开始涨、目标 RIC 已经出现后再跑：

```bash
cd ~/fanghaizhou/mkt_signal
cargo run --release -p cme_tas_replay --bin cme_tas_vs_summary -- \
  --config config/cme_tas_vs_summary.toml
```
