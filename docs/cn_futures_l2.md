# Domestic futures L2 replay

`crates/cn_futures_l2` is the only maintained implementation for replaying
Tonglian domestic-futures L2 data and exporting its derived parquet datasets.
It is a workspace crate in `/home/fanghaizhou/mkt_signal`.

The crate replaces the retired root binary `tonglian_baseline_replay`. That
binary wrote independent 5-second and 60-second ClickHouse staging tables and
used `config/tonglian_baseline_*.toml`; its source, binary registration, and six
configuration files have been removed. Do not use or restore that path.

The replacement pipeline writes one production RocksDB and derives these
outputs from it:

- dense contract-level `backtest_1s` parquet;
- contract-level `baseline_data_1min` parquet;
- `ylabel_1min` parquet;
- dominant continuous `baseline_data_1min_hfq` and `ylabel_1min_hfq` parquet.

Run the replay from the workspace root:

```bash
cd /home/fanghaizhou/mkt_signal
cargo run --release -p cn_futures_l2 -- \
  --l2-root /mnt/nfs/30.3_tonglian_comm_l2 \
  --msg-root /mnt/nfs/30.3_tonglian_msg \
  --start 2025-11-01 --end 2025-11-30 \
  --workers 32
```

Export binaries use the same package selector, for example:

```bash
cargo run --release -p cn_futures_l2 --bin export_backtest_1s -- --help
cargo run --release -p cn_futures_l2 --bin export_baseline_1min -- --help
cargo run --release -p cn_futures_l2 --bin export_ylabel_1min -- --help
cargo run --release -p cn_futures_l2 --bin export_hfq_1min -- --help
```

The data contracts remain in the sibling `preprocess` documentation repository:

- `../preprocess/data_format/cn_pipeline.md` from the workspace parent;
- `../preprocess/data_format/cn_l2_rocksdb.md` from the workspace parent;
- `../preprocess/data_format/cn_backtest_1s.md` from the workspace parent;
- `../preprocess/data_format/cn_baseline_1min.md` from the workspace parent;
- `../preprocess/data_format/cn_ylabel_1min.md` from the workspace parent;
- `../preprocess/data_format/cn_hfq_1min.md` from the workspace parent.

Those documents define source precedence, session handling, storage layout,
field semantics, and output paths. Code changes must preserve those contracts.
