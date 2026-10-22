# RX01 factor replacement

Target: `jp-meta-elvpn:~/binance-cta-spread-rx01`, following
`virtual03/cta_spread_rx01` into `binance_exec_trade06`.

The requested JSON contains 20 symbols, 15 factors and 169 combinations.
ICX and STORJ are absent from the current execution account's tradable cash
and futures rule snapshots. Following the user's margin-market correction,
the deployed candidate excludes their 16 combinations: 18 symbols, 15 factors,
153 combinations. `requested_factors.json` preserves the original request.

All 15 formulas are independent of trade-size buckets. Six were missing from
the Spread publisher's reviewed exemption list: TD_MT_019, TD_MT_021, TD_TI_038,
baseline_169, factor_trades_045 and factor_trades_049. The implementation now
allows them to compute without amount thresholds.

`warmup_bounds.json` records the actual Rust pipeline's synthetic complete-bar
test, including normalization (1440 slots / 1000 samples) and the downstream
percentile window (2880 slots / 1440 samples). Readiness needs 2439–2499 minutes
(40h39m–41h39m), within 48 hours. This is a sample lower bound; missing real
market observations can delay readiness. The runtime keeps its existing
259200-second Kafka lookback.

Validation: 19 focused Rust publisher tests and 22 focused Python configuration,
history and handoff tests pass. An initial broader Python invocation also found
an existing Perp stop-suite fixture missing its required bar-interval configuration;
the fixture was completed during final workspace checks. Release build succeeded
locally on synchronized `arbmm`.
`manifest.json` records exact artifact hashes; no remote Cargo build was used.

Remote evidence and backups: `run/factor_update_20261022/` under the environment.
The remote-only `before/env.sh` contains credentials and must not be copied into
this repository or printed. The operation used existing environment-local
configuration helpers and saved the pre-change factor revision, parameters,
positions and virtual-account bindings before modification. Temporary scripts,
build logs and duplicate local configuration backups were removed after verification.

At 09:59 UTC, standby PID3973994 started on CPU17 beside live PID330224.
At 10:17 UTC it completed replay and took over shared output/history ownership
using the environment-local stop script with the replacement PID retained.
The process name was persisted in the existing env.sh without replacing any
other settings. At 10:18 UTC the configuration service accepted the factor-table
CAS update. Signal/config/dashboard processes were not restarted.

Final verification: all 153 combinations are ready, all 15 desired strategies
are bound, removed combinations have no directional position, all strategy
market targets match Manager, and no delivery is pending. Publication and
journal checks pass. Config parameters are unchanged. Decode/evaluation/publish
errors are zero. See `verification.json`, `activated.json`, `handoff.json`, and
`installed.json`. All-market Vol continues: futures 524/525 and cash 362/363
symbols ready at the audit. Source IP and network settings are unchanged.

Removing old combinations uses the existing Spread behavior: exit the direction
and preserve the cash/perpetual hedge. It does not reset the position ledger or
liquidate hedged inventory. Existing 10-USDT sizing, entry/exit thresholds,
enabled state, execution template and network assignments are preserved.
