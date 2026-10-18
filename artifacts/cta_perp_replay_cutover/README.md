# RX02/RX03 silent replay cutover — deployed and verified

The user authorized RX02 v1, RX03 v0, silent historical recovery and final absolute targets, with explicit clears for flat/retired positions. RX01 must remain unaffected.

Completed at 2026-10-15 13:59 UTC. Both publishers caught up and both signals completed explicit silent recovery before publishing final targets. RX02 v1 has 47 current symbol targets; RX03 v0 has 34 factor/symbol targets. All nine retired RX02 strategies have explicit clear vectors (55 targets), independently verified in Exec Redis, and their shares are zero. RX01 configuration and the 44 other protected processes remain unchanged after the separately authorized Exec05 repair.

Manager release `20261015T124324Z` is running with cta_web SHA-256 `3e796fc165531b5ae3ee3a9fd2248b6dd0fcbdf6ee4b2fcb5daab761f540ff87`. Exec05 required a further repair: its fixed 5x initialization exceeded RapidX limits on some newly active symbols. The user explicitly approved lower venue limits and an Exec05 restart. The new pre-trade accepts only explicit lower-limit rejection 401034, then requires exact asynchronous readback. Its matching trade_engine was also necessary because the old engine rejected the cash balance query required by the current runtime, leaving position readiness false. Both are now running, readiness is true, and the final Exec05/07 snapshots contain 36/16 completed execution rows with no unfinished batches. Remaining position differences are reported as exchange minimums, not asserted to be exact zero. The original startup wrapper omissions and corrected retries are retained in the component evidence folders.

`final_target_verification.json`, `verified_targets_snapshot.json`, `final_health.json`, `exec05_leverage_cap/` and `exec05_engine_match/` contain the final evidence. Temporary remote staging and cutover backups are removed after verification; runtime factor history, completed-recovery requests, shadow positions and release metadata remain.

## Scope

- RX02: `position_algorithm=v1`, 47 active symbols, 17 factors, 192 pairs, single entry 300 USDT. Original 48-symbol request remains in `../cta_perp_rx02_replacement/requested_factors.json`. ICXUSDT was SETTLING in the preflight Binance exchangeInfo check, is absent from the current Manager trading-rule cache, and is absent from retained Kafka samples; the three ICX bindings are retained in `rx02/excluded_symbols.json` rather than enabled for new entries.
- RX03: `position_algorithm=v0`, original 14 symbols / 5 factors / 34 pairs, single entry 100 USDT. Existing entry/exit thresholds retained.
- Both: four symbol-level threshold overrides supported; no amount override or max-position sizing.
- RX01, shared market-data producers, Kafka and BBO were not restarted. Exec05 pre-trade and trade_engine were subsequently updated as described above; all other Exec processes were preserved.

## Recovery behavior

Factor history records carry a publisher generation. `recovery.json` identifies that generation, current per-symbol closed minute and whether the consumer has reached a newly sampled Kafka head. Catch-up rechecks high watermarks after each pass, including data produced while replay ran.

An explicit env-local `run/cta_perp_recovery_request.json` selects a generation and history path. Signal starts without publishing any targets, replays history with the same factor mean/threshold/position functions and historical post-bar bid/ask, and waits for every configured symbol and factor to be ready and current. Old strategy/symbol names remain as zero positions for final clears. Publication remains suppressed for malformed history, changed configuration, changed publisher generation, partial lines, stale publisher progress and incomplete factors. On success it atomically saves reconstructed positions, marks the request completed, deduplicates the replay/live boundary and publishes final targets. Ordinary restarts keep existing shadow state and do not request a fresh replay automatically.

Replay starts flat at the earliest ready retained observation. Finite Kafka retention cannot prove a position opened before retained history and never subsequently closed; this is a close reconstruction of available history, not an assertion of complete lifetime strategy state.

The reviewed RX02 factor formulas do not read trade-size bucket fields. Their publisher path uses unclassified OHLCV/depth aggregation, avoiding fabricated thresholds or changes to shared TLen metadata. Unreviewed factor names still require valid amount thresholds. A test varies every small/medium/large input and verifies all 17 factor outputs remain identical.

## Manager contract and deployed release

Exec already understood `qty: "clear"`. The previous Manager release accepted only numeric qty; RX01's six current catalog strategies used numeric targets (including nine numeric zeros), not the string command. The user approved upgrading Manager and the Perp environments together. The isolated Manager build excludes unrelated fee/schema changes and preserves the production baseline source code plus the clear patch.

Manager clear support is deployed: numeric targets preserve their existing representation, explicit clear survives shares scaling, signal filtering, catalog/archive serialization and Exec Redis publication. The frontend and publish client retain the command. The isolated build passed 333 library tests, nine other tests, the actual Redis integration test, Python client tests and frontend tests/build. Initial startup health returned 502 while caches initialized; subsequent health passed with unchanged live TOML. See the independent Manager repository's `artifacts/cta_clear_support/` for release evidence.

New Perp signals emit explicit `clear` and require this Manager release. Rollback must preserve that compatibility; do not resume new signals against the former numeric-only Manager.

RX02's `lgq_cta_symbol_mean` strategy, scoped publish token and virtual02 binding using `lgq_cta_ctaker` were prepared with empty targets before recovery and now carry the verified live v1 targets. RX02 previously had no env publish token; a new token was persisted only in its stopped remote signal env after installation, with a mode-0600 temporary env backup, removed after successful verification. All old strategy bindings remain available for historical attribution with zero shares and explicit clear targets. RX03 retains its existing five bindings.

## Validation

- `cargo test --lib cta_perp::`: 36 passed.
- `cargo test --lib factor_pub::cta_perp_factor_model_1m_pub::`: 8 passed.
- Python CTA Perp suite: 62 passed.
- Node panel tests and desktop/mobile Chromium panel/dashboard checks passed; no JS errors/overflow.
- Full library tests: 1288 passed, 2 pre-existing failures, 3 ignored. Failures are obsolete expected factor lists in `cta_factor_universe::configured_factors_match_raw_formulas_after_warmup` and Spread publisher service-list test.
- Local release build of both binaries passed; source branch arbmm matched fetched origin/arbmm at the manifest commit. Existing unrelated worktree changes were preserved.
- `cargo fmt --check` and `git diff --check` passed.
- `staging_validation.txt` records remote checksum/dry-run/ABI validation.

`deploy_remote.py` defaults to dry-run. Its install action has completed: both signals were stopped before factor/config changes, environment files backed up, exact hashed artifacts installed atomically, each factor table updated with the existing compare-and-set loader, and config/dashboard/factor services restarted. The recover action explicitly requests silent recovery and starts env-local signal wrappers; it completed successfully. See `install.log`, `recover.log`, `before_execution.json` and `during_install.json`.

## Manager binding migration helper

`manager_cutover.py` and `manager_binding_plan.json` are staged in the same independent directory. Their hashes are recorded separately in `manager_cutover_manifest.json`; they are an auxiliary addition, not a rebuilt Perp binary bundle. Remote hashes and the read-only `prepare` dry-run passed. Eleven local safety tests passed (`python3 -m unittest discover -s artifacts/cta_perp_replay_cutover -p test_manager_cutover.py -v`). Preparation completed with zero pending publishes and protected bindings unchanged. Retirement completed with zero pending publishes and protected bindings unchanged.

- `prepare --execute --manager-sha256 <verified-release-sha>` requires stopped RX02/RX03 signal processes and the exact clear-capable Manager binary. It creates the empty `lgq_cta_symbol_mean` strategy as `lgq`, assigns the existing RX02 publish token in memory, then adds its virtual02 binding. Existing shares stay unchanged. Temporary authorized sessions are deleted on exit; strategy and runtime changes go through Manager APIs via Nginx.
- `retire --execute --manager-sha256 <verified-release-sha>` requires completed current-generation recovery, fresh factor/signal status, matching config and all 47 current symbol targets. It compares the final mean table against both signal status and Exec Redis. All nine old strategy names, including the four already at zero shares, must have complete explicit-clear vectors covering their historical shadow symbols. It republishes each old binding through Manager and reads back the exact clear vector from Exec Redis before setting the old shares to zero. Old strategy names and attribution history remain available.
- Both actions reject unexpected virtual02 followers, changed binding plans and pending configuration publications. RX01/RX03 bindings, virtual templates and account-follow relationships are compared before/after. A failed runtime-clear confirmation prevents retirement; tests cover numeric-zero substitution, missing historical symbols, extra followers, operator edits and premature retirement.

The user approved the clear-capable Manager deployment and isolated build. Final execution verification is recorded separately from catalog/Redis publication: both accounts are position-ready and every observed execution row completed, with remaining quantities attributed to exchange minimums.
