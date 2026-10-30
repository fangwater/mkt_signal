# RX03 symbol × factor NQ deployment

Operator request: pause RX03 trading first, install the supplied 45 rules, and
restart its Config Server and factor publisher. Keep trading paused.

Target: `jp-meta-elvpn`, `/home/ubuntu/binance-cta-perp-rx03`, associated Exec07
`/home/ubuntu/binance_exec_trade07`. Base is synchronized `arbmm` / `709d4369`,
with the task's uncommitted implementation. `source.patch` captures the tracked
runtime changes; the complete rule fixture and regression are in the checkout.

Stopped signal and Exec07 pre-trade through their environment wrappers. Set
`enabled=false`; source-bound RapidX queries found ten ordinary orders and no
algo orders. Cancelled those ten orders with DELETE only and verified both order
lists empty. No flatten or new-order requests were sent. The final account query
still reported eleven nonzero perpetual positions.

Installed locally built release signal/factor binaries, updated Python modules
and the Config JSON editor, verifying hashes before atomic replacement. Preserved
the live 300 USDT amount, entry 0.9/0.1, exit 0.3/0.7, normalization, Kafka
transport and CPU19 binding. Rules alone replace the selected combination and
per-pair NQ settings. Shared trade-size metadata and Redis weights were unchanged.

The initial factor start exposed missing FIL/LINK trade-size metadata. Reviewed
the selected formulas, extended the existing OHLCV/depth-only allowlist by seven
formulas, verified all RX03 formulas are independent of perturbed size buckets,
rebuilt locally and atomically replaced only the factor binary. No fabricated
thresholds were introduced.

Config PID2288047, factor PID2307925, read-only dashboard PID2326926. The dashboard
was also updated/restarted to read canonical rules while signal status is stale.
The five protected Exec07 persist/engine/account-monitor/Viz/Config process
identities stayed unchanged. Signal and pre-trade remain stopped.

Final 10:15 UTC check: Config and panel gateway requests succeeded, raw-array POST
round-tripped all 45 rules, sixteen symbols had fresh factor output and ready NQ,
and 43/45 factor pairs were ready. ALGOUSDT and ZRXUSDT `buy_avg_price` still need
further warm-up samples. Ordinary/algo orders remained zero. No trading resume.

Validation: 13 focused Rust publisher tests, five JSON-rule tests, 21 Perp server
tests and 19 history/panel tests passed. Both affected binaries passed cargo check
and local release builds. Selected Rust formatting, Python compilation and
`git diff --check` passed. Earlier implementation verification also passed the
42 CTA Perp Rust tests and symbol-override regressions.

`installation_final.json` and `manifest_final.json` describe the final eight
installed files. `manifest.json` describes the first package before the formula
allowlist correction. `gateway_check.json` records JSON array save verification;
`verification_final.json` records final live checks. Large upload archives were
removed after installation; binaries remain in the environment and original
versions are backed up remotely.

Recoverable backup: live environment `run/nq_backup_20261028T094509Z`, including
the original configs, original binaries/scripts, pause/order evidence and final
manifests. Restore artifacts/config only while trading remains paused; resuming
signal/Exec or changing Manager strategy bindings requires a separate operation.
No credential file was copied or changed.
