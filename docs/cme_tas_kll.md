# CME TAS hourly notional KLL

`cme_tas_kll` 从 TAS RocksDB 的 `cme_trade` 扫可打印成交，按 UTC 整点做小时 notional KLL，写入 ClickHouse。口径对齐 crypto `tardis_ipc_replay` 的 `kll_only`：同一套 sketch、同一张表结构、RowBinary 插入。

不是 `cme_tas_replay`。Replay 只写 RocksDB。本工具 secondary 打开，不抢写锁。**不算 Special。** `cme_special` 没有价，不能进 notional。

## 口径

- 样本：`price * volume`，只来自 `cme_trade`。
- 空小时不写。
- 迟到成交（时间倒退）丢掉，记 `late_trades`，不进当前小时。
- `venue` 字节固定 `100`，和 crypto 的 Binance futures `1` 分开。
- 表名：`baseline.trade_notional_kll_cme_tas_hourly`。

## 为什么不并发扫

RocksDB 一座库。多 iterator 并发扫同一 CF 会抢 cache / compaction，收益差。默认一条 forward iterator，`readahead_bytes = 16MiB`，`fill_cache = false`。ClickHouse 写入用大 `batch_rows`。

## 运行

```bash
cd ~/fanghaizhou/mkt_signal
cargo run --release -p cme_tas_replay --bin cme_tas_kll -- \
  --config config/cme_tas_kll.toml
```

配置默认扫整座 `cme_trade`（2024 / 2025 / 2026 H1 已写入的 51 根）。`overwrite_existing = true` 会先按窗口删再写。

preprocess 文档：[tas_kll.md](../../preprocess/data_format/lseg/tas_kll.md)。
