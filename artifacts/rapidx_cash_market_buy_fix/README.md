# RapidX trade06 cash market buy execution repair

Production scope: jp-meta-elvpn `/home/ubuntu/binance_exec_trade06`, RapidX
Binance cash MARGIN and USDT perpetual execution. Spread publication remains
active at 100 USDT per configured factor/symbol. No target, share, leverage,
account mode, trading IP, gateway or credential changes.

The execution producer sent a nonzero base `quantity_qv` and zero
`quote_order_qty_qv` for cash MARKET BUY. The existing correct RapidX serializer
rejected it before exchange submission. Both prepared and byte request paths
now share the cash request builder. Only RapidX cash MARKET BUY converts base
quantity times a fresh ask to quote amount; the internal order ledger stays in
base quantity. Native orders, cash sells/limits and perpetuals retain base qty.
The IPC format already contains the quote field and is unchanged.

The first verified release `20261014T043000Z` ran as PID375418 and demonstrated
actual account convergence: at 04:32 UTC available margin was 10,375.42 USDT,
frozen margin 3,263.82 and debt margin 2,009.62, versus 419.06 / 18,835.27 /
17,583.07 immediately before resumed execution. A subsequent exchange rejection
401122 exposed a symbol quote decimal restriction. The final builder floors
quote amount to the Manager-owned price tick decimal scale, with no new rule
polling or metadata contract. Regression coverage includes 5-place, 2-place
non-power-of-ten ticks and integer-price ticks.

Validation: order manager tests 36 pass; trade_engine tests 357 pass;
exec-pre-trade binary tests 8 pass; final full library tests: 1269 pass, 3 ignored, 3 known unrelated factor
configuration failures (recorded in lib_tests_final.log). Format and exec-pre-trade check pass. Release builds
are local from synchronized arbmm base 6ff0248e524a673c3ba8524bab8e9a6c6ddbb7e1.
The pre-existing native account-query IP diff in src/bin/pre_trade.rs was
preserved and reviewed; its branch is inactive for RapidX. Exact build source
patches and manifests are retained here, not committed automatically.

Deployment uses explicit upload paths, SHA-256 checks, atomic binary replacement,
and only the env-local start_exec_pre_trade.sh wrapper, including its startup
order cancellation gate. The first restart replaced PID3307344 with PID375418;
all other 44 protected process identities/affinities and all 15 configuration
hashes remained identical. No direct writes to Exec RocksDB.

Official contract:
- https://apidocliquidity.readme.io/reference/place-order
- https://apidocliquidity.readme.io/reference/ws-place-order
- https://apidocliquidity.readme.io/docs/error-codes

Final release: `20261014T043600Z`, binary SHA-256
`d65aa0f6262ec37a08f4b5dd4f8436445fd83068316824c969b59b7dabaf7838`,
PID448295, CPU0-5, source IP172.31.35.228. Previous binary and release metadata
retained in remote `run/cash_market_buy_20261014T043600Z/`. Both initial and final
restart evidence show all 44 protected processes/affinities and 15 config hashes
unchanged; Spread remains enabled with healthy journal and no publish error.

At 04:39:21 UTC the ready factual snapshot had 115/115 completed rows: 64
`target_reached`, 51 `exchange_minimum`. Five factors target gross was 8,337.93
USDT, factual strategy gross 8,305.20, absolute per-row residual about61.71 USDT;
all residuals were individually below exchange minimum. SYSTEM_POSITION_CLOSE
residual was 2.66 USDT across14 dust rows. Independent historical inventory
remained 8,153.41 USDT gross. Deduplicated factual account cash+perpetual gross
was13,690.21 USDT: this includes that inventory and nets strategies per symbol,
so it is not the five-factor 100 USDT target count. No old inventory was deleted.
At 04:38:56 UTC the bound account query showed availableMargin11,401.52,
frozenMargin7,185.68 and debtMargin5,909.55, with zero ordinary open orders.
The temporary earlier margin trough is not the final target allocation.
Final snapshot/account evidence is in the Manager checkout's
`artifacts/jp_meta_trade06_convergence_20261014/`.

A second ready snapshot at04:40:10 UTC again had115/115 completed rows. All104
Manager targets (92factor legs plus12inventory legs) matched Exec and the
publication queue was empty. From04:37:20 through04:40:34 no new trade-engine
error line appeared in the bounded live tail. Final running binary checksum
was re-read from /proc/448295/exe and matched the release manifest.
