# RocksDB → 1 分钟 parquet

独立工具 `cme_tas_synth_1min`，**不是** `cme_tas_replay`。从同一座 TAS RocksDB 读 `cme_trade` + `cme_special`，按 UTC 分钟左端合成 1 分钟 bar，写成 parquet，再对 summarised 源做有价分钟校对。

Replay 仍只写 RocksDB。本工具用 secondary 打开，不抢写锁。

## 口径

- OHLC / `volume` / `count` 只来自可打印成交 `cme_trade`。
- `cme_special` 不进 Open/High/Low/Last。量写在 `special_volume` / `special_count`。
- `volume_total` = 可打印量 + Special 量。
- 只有 Special、没有可打印成交的分钟：OHLC 为 null，`volume=0`。
- 对照源是 `future/summarised` 的 `Intraday 1Min`，不是过滤后的 `summary_1m` parquet。
- 有价分钟：Open/High/Low/Last 必须和 Summary 一致；Volume 必须等于可打印量，或可打印量加 Special 量。剩下的差立刻失败。
- `No. Trades` 不是有价 K 线字段，不拿来判失败。

## parquet 列

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `ric` | string | `#RIC` |
| `ts` | int64 | UTC Unix 秒，分钟左端 |
| `ts_utc_ns` | int64 | 同一时刻的纳秒 |
| `date_time` | string | `YYYY-MM-DDTHH:MM:SS.000000000Z` |
| `open` / `high` / `low` / `close` | float64，可空 | 可打印成交 OHLC；`close` 是 Last |
| `volume` | int64 | 可打印成交量 |
| `count` | int32 | 可打印笔数 |
| `special_volume` | int64 | Special 量 |
| `special_count` | int32 | Special 笔数 |
| `volume_total` | int64 | `volume + special_volume` |

## 运行

默认窗口是 `ADF26` 五个芝加哥 17:00 交易日：`20260102`–`20260108`。

```bash
cd ~/fanghaizhou/mkt_signal
cargo run --release -p cme_tas_replay --bin cme_tas_synth_1min -- \
  --config config/cme_tas_synth_1min.toml
```

产物：

```text
/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_synth_1min/ADF26_20260102_20260108.parquet
/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_synth_1min/ADF26_20260102_20260108.compare.json
```

Notebook：preprocess [cme_tas_synth_1min.ipynb](../../preprocess/lseg/cme_tas_synth_1min.ipynb)。

## 对照结论（`ADF26` 五天）

`20260102`–`20260108`：174 分钟两边都有；164 根有价 OHLC 精确；10 根只有 Special 量、OHLC 空；leftover / 缺边都是 0。

**考虑 Special 之后，这次窗口的成交价量完全对上。** 不是整根 Summary 所有列都能还原：`No. Trades` 不判失败；Bid/Ask 不在 `cme_trade` + `cme_special` 里；这只是一只 RIC、五天。口径见 preprocess [tas_synth_1min.md](../../preprocess/data_format/lseg/tas_synth_1min.md)。
