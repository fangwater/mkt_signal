# FR exposure board rollout

Verified 2026-11-04 06:32 UTC. Code: `c5f52f669156387d05a197e75d4bb6057d026816` on synchronized `arbmm`.

The permanent read-only check is live at `http://13.115.227.29:4191/delist/`.
It discovers NAV `funding_rate` accounts and checks the account-derived Viz
exposure/risk snapshots, including underlying account-monitor timestamps.
Thresholds: per asset >100 USDT, per-account absolute residual sum >500 USDT,
30 seconds persistence, 10 seconds polling, 90 seconds source freshness.
Missing prices and failed/incomplete coverage remain unknown; complete per-asset
observations can recover that asset independently. Unknown observations cannot
confirm uninterrupted excess. Prior confirmed unknown risk remains identified.
No trading action or external notification is submitted by this module.

## Build and validation

- 1,770 files from the committed `arbmm` tree checked against Git blob IDs inside
  `/home/fanghaizhou/mkt_signal/data/ops/fr_exposure_check_20261104/build-source`.
  This isolated build within the active local worktree preserves concurrent,
  uncommitted Gate ADL changes; those changes are not in this release.
- Local `cargo build --release --locked --bin delist_risk_server` and 40 binary
  tests passed using that source; committed-source formatting and shell checks passed.
- The final release passed `scripts/tests/delist_exposure_browser.py`: real
  authenticated disposable service, Gate04/SG discovery, persistent warning,
  partial-valuation recovery, stale/read failures, automatic refresh, escaping,
  desktop/mobile layout. Existing CTA configuration browser checks also passed.
- The deployed HTML and real 11-account JSON report were replayed locally in
  Chromium on desktop/mobile; no JavaScript errors or whole-page mobile overflow.
- Runtime/cache JSON, source archive and screenshots stay in ignored
  `data/ops/fr_exposure_check_20261104/`, rather than in this report.

## Deployment

- Env: JP `~/delist_risk_server`; no Cargo build on production.
- SCP explicit temporary paths, verify SHA-256, back up old files, atomically
  replace binary/page/wrapper, restart only through the env-local start script.
- Remote wrapper retained its existing differences from repo; only the five new
  exposure arguments were inserted. Private service/auth configuration hashes
  and all previous service arguments were preserved.
- PID `2469971` / CPU `0-5`; 92 protected trading/market-data/CTA processes retained
  their PIDs, executable ownership and CPU masks. No source IP or listener changes.
- Original backup: `~/delist_risk_server/run/fr_exposure_20261104_1cb089b7/`.
  Final release backup/evidence: `~/delist_risk_server/run/fr_exposure_20261104_c5f52f66/`.
- Binary SHA-256: `6315c7bc39ef90e2caf3b9de756814fa69eecad8b700cb24de4e87b111d65e7c`.
- HTML SHA-256: `7e597ef90bf61d29537b577e208dac807fd1bfd1132770a9968f40b2eb27889a`.
- Wrapper SHA-256: `05282a33a7cec43dd67a7c1ee6376a6297c76c2ea7e23d3b1bb0035c25837519`.
- Direct and gateway `/exposures` return 401 without login; no auth bypass added.
- Final report age was 0.6 seconds, NAV catalog current, all
  account-monitor and snapshot timestamps within 90 seconds.

## Observed positions

Values below are a verification snapshot, not current guarantees. Absolute totals
for partial accounts are lower bounds; upstream stablecoin/nontrading-dust filters
also apply. This is not an independent private-API exchange reconciliation.

| FR environment | Risk / data | Absolute residual USDT | Largest signed residual USDT |
| --- | --- | ---: | ---: |
| gate_fr_arb04 | alert / partial | 1854.24 | NAORIS +666.08 |
| bybit_fr_arb01 | unknown / partial | 25.00 | VELO +7.02 |
| gate_fr_arb01 | unknown / partial | 104.06 | ZEREBRO +23.19 |
| gate_fr_arb02 | unknown / partial | 123.89 | AIN +14.94 |
| binance_fr_arb02 | ok / fresh | 18.46 | COTI -4.65 |
| binance_fr_arb03 | ok / fresh | 175.77 | BNB -43.53 |
| binance_fr_arb04 | ok / fresh | 57.27 | SOMI +6.33 |
| bitget_fr_arb01 | ok / fresh | 48.56 | ACH +12.05 |
| bitget_fr_arb02 | ok / fresh | 41.52 | CROSS +6.89 |
| bitget_fr_arb03 | ok / fresh | 51.86 | SAPIEN +8.22 |
| gate_fr_arb03 | ok / fresh | 65.13 | AIN +22.38 |

Gate04 had confirmed asset and account-total excess for over 30 seconds; all seven
fully valued accounts were within monitoring thresholds. Unpriced holdings:

- `gate_fr_arb04`: AMZNX 有持仓但缺少有效价格; CTC 有持仓但缺少有效价格; HOODX 有持仓但缺少有效价格; METAX 有持仓但缺少有效价格
- `bybit_fr_arb01`: EUR 有持仓但缺少有效价格; HFT 有持仓但缺少有效价格
- `gate_fr_arb01`: SLVON 有持仓但缺少有效价格; TSLAX 有持仓但缺少有效价格
- `gate_fr_arb02`: CRCLX 有持仓但缺少有效价格; FTM 有持仓但缺少有效价格; MATIC 有持仓但缺少有效价格; SLVON 有持仓但缺少有效价格; TSLAX 有持仓但缺少有效价格
