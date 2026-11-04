# One Manager account, two execution Viz panes

Release `cross01_single_account_20261103T051501Z` on jp-meta. Public alias `okex_binace_cross_1min`,
internal account ID `cta_cross_spread_rx01`. Both execution source IDs and
aliases stay unchanged; the Exec aliases no longer appear as two public accounts.

Account: http://13.115.227.29:4191/manager/account/?source=cta_cross_spread_rx01
Execution Viz: http://13.115.227.29:4191/manager/exec/?group=cross01

Only Manager restarted (PID3538262, CPU24), using the existing API
binary. The only runtime TOML addition is the public-account mapping; saved
aliases, 87 protected process identities, execution configuration,
immutable anchors and bindings are preserved. Trading and signal publication
remain off. No order, position, account-mode or risk mutation was performed.
Actual funding collectors remain disabled. New strategy orders/initial positions
and tested combined PnL/funding are zero. This is not a physical-flatness claim;
the earlier Binance IOTAUSDT -0.1 residual was not traded or seeded.

41 frontend tests and the production build passed. The real browser checked one
account in all three selectors, correct account/config links, independent market
fees, two native WS101 connections, stable frames during refresh, stacked and
side-by-side layouts, mobile width, combined PnL and denied-user permissions.
No browser runtime exceptions were recorded.

The initial static archive omitted the required `manager/` directory, producing
Nginx HTTP500. `static-layout-fix.json` records its correction without another
restart or source-code change. Final file hashes and public HTTP responses were
verified for every HTML/CSS/JS asset. The old archive hash remains in the manifest,
and the remote audit retains `manifest.before-layout-fix.json`.

Receipts: `verification.json`, `browser-verification.json`,
`artifact-verification.json`, `allocation-preflight.log`, `frontend-tests.log`,
`frontend-build.log` and `manifest.json`. `ops.py` now validates the required
static directory and the frontend HTTP route before accepting activation.

Screenshots were inspected, then removed with the upload archive, temporary
browser profiles and owned staging. Browser test sessions and tunnels were closed.
Private rollback configuration and receipts remain at
`~/crypto_cta_manager/maintenance-backups/cross01_single_account_20261103T051501Z/`.
