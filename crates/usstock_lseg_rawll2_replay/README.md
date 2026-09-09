# usstock_lseg_rawll2_replay

Source-exact replay for LSEG US-stock `Legacy Level 2` raw depth messages.
The replay preserves ordered FID patches; it does not synthesize a complete
book or produce sampled depth snapshots.

```bash
cargo run --release --locked -p usstock_lseg_rawll2_replay -- --config config.toml --preflight
cargo run --release --locked -p usstock_lseg_rawll2_replay -- --config config.toml --period 2021-07-01_2022-07-01 --dry-run --max-messages 100000
```
