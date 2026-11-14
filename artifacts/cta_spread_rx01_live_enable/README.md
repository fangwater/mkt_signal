# RX01 Spread live publication enabled

Enabled 2026-10-14 04:12:05 UTC at the operator's explicit request.
The existing repaired release binary was retained. Signal PID46021 → PID176562,
CPU0-5; all 44 protected processes and environment/trading bindings unchanged.

Opening the flag alone would have resent the old oversized saved targets. The
46 latest successful 100-USDT entry records were recovered from immutable signal
journals before enabling. Active direction targets become one cash allocation
and zero perpetual; exited targets become the last allocation plus its opposing
hedge. All 46 entries were found. No historical record or fill was rewritten.
Five-factor gross targets at preparation marks: 58,851.68 → 8,876.85 USDT.
The independent inventory binding was preserved and is outside these totals.

`prepare.py` produces `plan.json` without runtime mutations. `apply.py` defaults
to a dry run and requires `--execute` to back up state, stop the signal through
its environment-local wrapper, load the corrected state, start while disabled,
verify 46 ready observations, then enable using the Config API revision guard.
Exec receives normal complete absolute targets and adjusts against factual
positions; no separate account flatten operation was used.

`enabled.json` records all five successful strategy publications.
`verified.json` confirms all 92 Manager/Exec quantities matched, the follow queue
was empty, inventory unchanged and publication healthy. `margin_after.json`
is a read-only exchange query explicitly bound to the current trading IP:
available margin +1,123.01 USDT, frozen margin 18,159.49 USDT on the first check.
Execution was still converging; this is not an assertion of completed positions.

Remote backup: `/home/ubuntu/binance-cta-spread-rx01/run/live_enable_20261014T041023Z/`.
No credential values or environment file contents were exported.
