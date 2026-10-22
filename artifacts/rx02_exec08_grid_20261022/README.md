# RX02 / Exec08 Grid activation

Verified 2026-10-22 15:06 UTC on jp-meta-elvpn.

- New account: `lgq_ltp_81857`, `binance_exec_trade08`, native STANDARD USD-M.
- Explicitly authorized one POST to enable Multi-Assets Mode; reply code200,
  readback true. One-way positions remain unchanged.
- Follow `virtual02` at multiplier1, binding `lgq_cta_symbol_mean` to existing
  `lgq_grid_prep`: futures 3 levels / 10000ms / unlimited Maker requotes,
  100 USDT hand, Vol multiplier1, 10 USDT target tolerance.
- RX02 keeps 23 symbols, 50 USDT per symbol, both JP/SG publications. RLC is
  still warming and stays clear. Original Exec05 risk limits were copied;
  fees and exchange symbol leverages were preserved.
- Exec08 started at15:00; 8 factual Binance fills all Maker, 5 positions reached
  within tolerance, no open orders/pending publications at verification.
- Grid support was added consistently to Manager, Config and Exec for native
  STANDARD USD-M only. Existing Multi-Assets and market-scope gates remain.
- 1354 Exec library tests passed (3 ignored), 398 Manager tests passed (5
  ignored), 42 Config Python tests passed. A stale RX01 test was changed to
  an explicit fixture so it tests opposite sleeves independently of the live
  universe. No production factor calculation changed in this Grid patch.
- Release artifacts were built locally, SHA-256 checked and atomically installed.
  Manager's first health window expired during normal historical NAV startup;
  verification resumed without reinstall/restart. Binding publication initially
  waited on Multi-Assets Mode, then cleared automatically after authorization.
- CPU, source IP, gateway and other protected process identities were verified.
  Old Exec05 remains stopped. RX02's page now references Exec08.

See `release.json`, `preflight.json`, `installed.json`, `configured.json`,
`multi_assets_verified.json`, `started.json`, and `verification.json`.
Secrets and private env backups remain exclusively on the remote host.
Temporary operation scripts, patches, build/test logs and Python caches were
removed locally after verification; the audit receipts above are retained.

API contract: [Binance Change Multi-Assets Mode](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade#change-multi-assets-mode-trade).

Existing Config URL: http://13.115.227.29:4191/exec_trade08/config/ (Manager login).
