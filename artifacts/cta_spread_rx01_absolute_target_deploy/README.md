# RX01 Spread absolute-target signal deployment

Completed 2026-10-14 04:01 UTC on `jp-meta-elvpn`, environment
`/home/ubuntu/binance-cta-spread-rx01`. Only `cta_spread_signal` was replaced and
restarted through the existing environment-local script. Publication remains disabled.

- Local synchronized `arbmm` base: `6ff0248e524a673c3ba8524bab8e9a6c6ddbb7e1`.
- Exact signal-source changes: `source.patch`; SHA-256 in `manifest.json`.
- Binary SHA-256: `4069055ff3a20582f727d8cecdc6a14af7d619af7ca12ed625bad765add330b4`.
- PID3132535 → PID46021; affinity CPU0-5 retained.
- 46/46 observations ready, healthy journal, no publication error, 100 USDT allocation.
- All 44 protected processes, 15 configuration/environment hashes and Exec strategy catalog unchanged.
- No existing account positions or business targets were adjusted. JSON restart serialization
  rounded 12 quantity fields by one ULP, at most 2.85e-14 base units.
- Oversized historical state remains; deployment does not make it safe to enable publication.

`before.json`, `after.json`, `verification.json`, `installed.json` and `manifest.json`
contain sanitized evidence. No credentials or environment file contents were exported.
Remote backup: `run/absolute_target_20261014T035311Z/` under the signal environment.

The first wrapper wait timed out after 45 seconds while its stop child was still
finishing. The old process subsequently exited. The same environment-local wrapper
was resumed with a longer timeout and started the new binary. A strict state equality
check then detected only the one-ULP JSON rounding documented above; verified before
finalizing deployment metadata. No additional process restart was needed.

Validation before release: 33 Spread tests, signal cargo check, formatting and release
build passed. Full library tests: 1265 passed, 3 ignored, 3 unrelated factor-config
failures documented in `docs/cta_signal_families.md`.
