# cross01 unified execution dashboard

Published 2026-11-03 05:50 UTC on JP as `cross01_unified_viz_20261103T054937Z`.
Entry: http://13.115.227.29:4191/manager/exec/?group=cross01

One public account, `okex_binace_cross_1min`, now has one execution summary,
one table and shared filters. The frontend directly joins the two authenticated
native streams; it contains no iframe. Source, venue, symbol and strategy
identities remain separate before display aggregation. Risk is independent;
combined equity/notionals add and leverage is recalculated. Execution amounts
use absolute totals. Unknown, stale or disconnected input never becomes zero.

Only static frontend assets were switched. API/config hashes and the public
account mapping stayed unchanged. Manager PID3538262 remains on CPU24; all 87
protected process identities were preserved. No trading was started, no order
or position was changed, and actual funding collectors remain disabled.

Evidence:

- `manifest.json`: release, artifact/file/source hashes and candidate receipts.
- `frontend-tests.log`: 52 focused frontend tests passed.
- `frontend-build.log`: successful production Vite/TypeScript build.
- `candidate-browser.json`: Chromium candidate check using the real Nginx API
  and native WebSocket handshakes, plus browser-only synthetic execution events.
- `verification.json`: remote invariants and empty new strategy ledger.
- `live-browser.json`: actual published frontend; one public account, zero
  iframes, one summary, one table, both WS upgrades101, stable refresh,
  denied-user access and no runtime errors.
- `cleanup.json`: exact temporary-file cleanup and final process check.

Fixture equity400, gross200, leverage0.5 and pending40/70 are synthetic browser
values only. They were never sent to a server or exchange. The real execution
snapshots stayed empty throughout; PnL has no orders or initial positions.
This does not establish exchange flatness: the earlier observed Binance
IOTAUSDT -0.1 residual was neither closed nor seeded into the new ledger.

Desktop/mobile and live empty-state screenshots were inspected and then
removed, together with the upload archive and exact remote staging directory.
Temporary browser profiles, tunnels and test sessions were cleaned by the
harness. `ops.py` and `browser.mjs` remain as reproducible audit helpers.

Remote rollback/audit directory:
`~/crypto_cta_manager/maintenance-backups/cross01_unified_viz_20261103T054937Z/`.
The previous release and private configuration backup are retained there/by the
saved webroot pointer. New assets remain in the matching `web-releases/` path.
