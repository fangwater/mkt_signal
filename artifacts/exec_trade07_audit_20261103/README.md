# Exec07 execution-state read-only audit

Observed 2026-11-03 13:48–13:53 UTC on `jp-meta-elvpn`.

- `/exec_trade07/` currently serves `binance_exec_rx03`, loopback Viz 10047 and Config 18167.
- All seven environment processes were running. Viz snapshots and execution order events continued to advance.
- All three Prep02 execution strategies use Grid. Futures settings: maker timeout 10000 ms; max_maker_requotes=null (unlimited); signal=0 for the observed clears. This has no automatic taker fallback after a finite number of attempts.
- Exchange open-order GET and the durable account journal confirm ongoing GTX BUY order placement and cancellation for IOTA. The absence of an order at one instant was between maker attempts.
- BEL completed its observed close at 13:52:13.670 UTC; ZRX completed at 13:52:22.552 UTC.
- At 13:52:59 UTC, IOTA's 1min strategy still had 2499.3 units (about 99 USDT) in an active BUY order. Its 5min and 15min strategies continued to target short positions; this is not an account-wide flat target.
- That latest snapshot also had a new ONT 1min clear of 2213.9 units (about 100 USDT) awaiting submission.
- In the final 20-minute journal window, IOTA had 325 unique orders: 322 cancelled, two filled, one open. Targets changed during that interval; these are not all attempts for one unchanged target.

No live orders, account settings, strategy targets, execution parameters, services, binaries, or configuration were changed. No restart was performed. Repository source files were not changed.

Evidence: `readonly_snapshot_1.json` and `readonly_verification.json`; both contain selected operational/account state and no credentials.
