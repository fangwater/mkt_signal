# Gate FR arb04 / arb05 deployment

Host: `jp-meta-elvpn`. Release artifacts were built locally from the synchronized `arbmm` worktree, which contained pre-existing local changes. All seven binaries in each deployment match the local SHA-256 hashes.

- `gate_fr_arb04` → `GateAM36_LTP`: unified / multi_currency; read-only account monitor running; NAV equity is live. The first trades/funding/interest/liquidations sync succeeded with zero records.
- `gate_fr_arb05` → `GateAM05_LTP`: classic; signed account-mode and spot-balance reads succeeded from the trading source and all three NAV REST sources. The requested multi_currency / USDT futures switch returned HTTP 400, `INVALID_PARAM_VALUE`, `account balance must greater than 500 USDT`. NAV registers the actual classic mode and reports unavailable until activation.
- Both NAV entries use `st_ms=1793091610625` (2026-10-27 09:00:10.625 UTC), with the SubAccount ID as the display alias.
- Both trading engines, pre_trade and trade_signal remain stopped. No orders, cancellations or fund transfers were submitted. Viz, Config, dashboard and persist processes are also stopped. Gateway routes were prepared locally and have not been installed.
- Configured trading source is `172.31.35.231` / `52.69.78.134`. The arb04 monitor also has REST sockets from the existing default source `172.31.35.228`; both remain on ens41. No network routing changes were made.

`verification.json` contains sanitized API results, NAV entries, runtime state and artifact hashes; credentials are excluded. `nav_first_sync.log` records the initial bounded history scan.
