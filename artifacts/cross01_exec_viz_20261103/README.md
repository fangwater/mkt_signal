# cross01 execution Viz deployment

Release: `cross01_exec_viz_20261103T041000Z` on jp-meta.

Two ordinary Manager aliases: `okex_exec_cross01` and `binance_exec_cross01`.
Their source IDs stay `okex_exec_trade01` and `binance_exec_spread01`.
The old logical account `cta_cross_spread_rx01` was removed from runtime TOML.
Monitor: http://13.115.227.29:4191/manager/exec/?group=cross01

Only Manager was restarted (PID3427918, CPU24); the existing API artifact was
reused. No execution, risk, source-IP, Nginx or account state was changed.
Trading remains off. Actual funding collectors remain off. The combined strategy
ledger has no orders or initial positions; tested fee-after PnL and funding are 0.
The previously observed physical Binance IOTAUSDT -0.1 residual was neither closed
nor seeded; the empty strategy ledger does not prove physical flatness.

- `manifest.json`: API/frontend hashes and source inventory.
- `verification.json`: live account/config/process/snapshot checks.
- `allocation-preflight.log`: existing five-source immutable allocation validation.
- `artifact-verification.json`: installed frontend bytes, API hash, config and CPU.
- `browser-verification.json`: real native frames and WS101, stable frame polling,
  desktop/mobile layout, combined PnL, funding controls, denied-user access.
- `frontend-tests.log`: 41 passing tests.
- `frontend-build.log`: successful production build.
- `cleanup.json`: exact owned upload staging removed; rollback audit retained.
- `ops.py`, `browser.mjs`: scoped maintenance and browser verification helpers.

The initial browser checks used the wrong account-list route and connection label;
these harness expectations were corrected to `/manager/accounts/` and `实时`.
The final browser run passed without application runtime errors.

Screenshots were inspected and removed. Temporary browser profiles, test sessions,
SSH tunnels and upload archives were removed. Private rollback config and receipts
remain in `~/crypto_cta_manager/maintenance-backups/cross01_exec_viz_20261103T041000Z/`.
