# usstock_lseg_mbp_replay

Typed, compact replay of LSEG US-stock Market By Price raw exports.

The pipeline is lossless at the audited MBP semantic level:

- every outer `REFRESH` / `UPDATE` / `STATUS` message is retained;
- all 26 known Summary FIDs use a sparse presence bitmap, including present-but-empty values;
- source enum strings are retained when present;
- ordered `ADD` / `UPDATE` / `DELETE` price-level actions stay atomic with their outer message;
- both historical seven-FID and current eight-FID level layouts are supported;
- no depth truncation, time sampling, trade inference, or Summary-only filtering is performed.

High-frequency decimals and clocks are normalized to integers. RocksDB values use the compact
`mbp-event` binary codec instead of storing CSV cells. Each period is built as
`<period>.building` and atomically published only after source counts match
`merged-Report.csv`.

```bash
cd crates/usstock_lseg_mbp_replay
./decompress.sh
cargo test --release --locked
cargo run --release --locked --bin usstock_lseg_mbp_replay -- \
  --config config.toml --preflight
cargo run --release --locked --bin usstock_lseg_mbp_replay -- \
  --config config.toml
cargo run --release --locked --bin usstock_lseg_mbp_replay -- \
  --config config.toml --verify
```

`reference.py` is the Python correctness baseline. Rust and Python encode the same fixed fixture
into the golden key/value vectors in `tests/fixtures/mbp_small_golden.json`.

The persistent background pipeline builds, replays, then fully verifies all six periods:

```bash
systemd-run --user --unit=usstock-lseg-mbp-replay --collect \
  --working-directory="$PWD" "$PWD/run_pipeline.sh"
journalctl --user -fu usstock-lseg-mbp-replay.service
```

The workspace owns the single `Cargo.lock`. Runtime scripts use the crate-local
`target/` directory by default; set `USSTOCK_MBP_TARGET_DIR` to override it.

Diagnostic parsing never publishes a database:

```bash
cargo run --release --locked --bin usstock_lseg_mbp_replay -- \
  --config config.toml --dry-run \
  --period 2026-07-01_2026-08-14 --max-messages 100000
```
