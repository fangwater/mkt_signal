# Cross Bybit/OKX Account Mode Verify — Design

**Date:** 2026-05-21
**Status:** Approved (pending spec review)
**Scope:** Pre-flight verification of OKX + Bybit account configurations before launching a `<open>-<hedge>-cross-<suffix>` environment (initial target: `bybit-okex-cross-arb01`).

## 1. Background

The cross trade engine assumes specific account configurations on each exchange:

- **OKX** orders are sent with `tdMode="cross"` and without `posSide` (see `src/trade_engine/okex.rs:391-396`). This requires:
  - `acctLv ∈ {3, 4}` — 跨币种保证金 / 组合保证金 (PM). `acctLv ≤ 2` rejects `tdMode=cross`.
  - `posMode == "net_mode"` — single-side position mode. `long_short_mode` requires `posSide`, which the engine does not send.
- **Bybit** orders carry `isLeverage=1` for spot-margin loans (see `src/trade_engine/bybit.rs:357`). The engine already enforces precheck at startup (`src/trade_engine/bybit_precheck.rs`):
  - `unifiedMarginStatus ≥ 3` — UTA 1.0 / 1.0 Pro / 2.0 / 2.0 Pro.
  - `spotMarginMode == "1"` — spot margin enabled.

OKX's two requirements are NOT auto-checked at engine startup. Misconfigured OKX accounts present as "engine online but every order rejected." A standalone pre-flight script — runnable before first start and on demand — closes that gap and bundles Bybit's existing checks for a single unified view.

The current intra arb environments (`okex-intra-arb01`, `bybit-intra-arb01`) already satisfy these settings. The new cross environment must match.

## 2. Goals

- Single command verifies all 4 mode requirements across OKX + Bybit.
- Dry-run by default; non-zero exit on any FAIL (so it slots into deploy chains and CI).
- Optional `--execute` mode auto-fixes the 3 reversible items (OKX acctLv, OKX posMode, Bybit spotMarginMode); UTA upgrade is irreversible and never auto-executed.
- Reuses existing env-discovery convention (`--env-name <open>-<hedge>-cross-<suffix>` → auto-source `$HOME/<env_name>/env.sh`).
- Output identifies failing checks with the exact API call needed for manual fix.

## 3. Non-Goals

- Per-symbol leverage verification (handled by `set_cross_futures_leverage.py`).
- Per-symbol margin mode (engine always sends `tdMode=cross`; per-symbol overrides not in scope).
- Other exchanges (binance/gate/bitget). The architecture leaves a dispatch slot but only `okex` and `bybit` verifiers ship in v1.
- UTA upgrade automation (single-direction, account-wide, irreversible — out of scope for an unattended script).

## 4. Architecture

### 4.1 File layout

- New: `scripts/verify_cross_account_modes.py`
- Add the file to `CROSS_TOOLS_TO_SYNC` in `scripts/deploy_cross_trade_signal.sh:136-149` so it is rsynced into `<env>/cross_scripts/` during deploy.

### 4.2 Invocation patterns

```bash
# From inside a deployed env dir (CWD-based inference):
cd $HOME/bybit-okex-cross-arb01
./cross_scripts/verify_cross_account_modes.py            # dry-run all checks
./cross_scripts/verify_cross_account_modes.py --execute  # fix fixables
./cross_scripts/verify_cross_account_modes.py --exchange okex  # one side only

# From repo root:
scripts/verify_cross_account_modes.py --env-name bybit-okex-cross-arb01
```

### 4.3 Argument surface

| Flag | Purpose |
|---|---|
| `--env-name <name>` | Explicit env name (matches `^([a-z0-9]+)-([a-z0-9]+)-cross-([a-z0-9_-]+)$`). Otherwise inferred from CWD basename. |
| `--env-dir <path>` | Override env dir; default `$HOME/<env_name>`. |
| `--no-env-sh` | Skip auto-source of env.sh (use ambient env vars only). |
| `--execute` | Apply fixes for fixable FAILs. Default is dry-run. |
| `--exchange <bybit\|okex>` | Limit checks to one exchange (default: both `open_ex` and `hedge_ex`). |
| `--timeout <secs>` | HTTP timeout per request, default 10. |

### 4.4 Components

```
verify_cross_account_modes.py
├── parse_env_name(name)           # → (open_ex, hedge_ex, suffix); validates "<ex>-<ex>-cross-<tag>"
├── load_env_file(env_dir)         # mirror set_cross_futures_leverage.py:148
├── load_required(names)           # mirror set_cross_futures_leverage.py:273
├── CheckResult                    # dataclass: name, status (PASS/FAIL/SKIP), detail, fix_cmd
├── OkexChecker                    # verify acctLv, verify posMode, fix_*, _signed_request helper
├── BybitChecker                   # verify unifiedMarginStatus, verify spotMarginMode, fix_*, _signed_request helper
├── precheck_empty_account(ex)     # before --execute touches acctLv/posMode, fail if positions/orders non-empty
├── render_report(results)         # pretty-prints per-exchange section with icons + summary
└── main()                         # arg parsing, dispatch, exit code
```

The two checkers are independent classes; each owns its REST helper, credential names, and the API paths. No shared state beyond `requests.Session`. Each `Checker.run()` returns a `List[CheckResult]`.

### 4.5 Data flow

```
CLI args
  ↓
parse_env_name → (open_ex, hedge_ex)
  ↓
load_env_file($HOME/<env>/env.sh)   [unless --no-env-sh]
  ↓
For ex in {open_ex, hedge_ex} ∩ requested:
  checker = OkexChecker | BybitChecker
  for check in checker.checks():
     state = check.fetch()                  # GET endpoint
     result = check.compare(state)          # PASS/FAIL
     if result.FAIL and --execute and check.fixable:
        precheck_empty_account(ex)          # for acctLv/posMode
        check.apply_fix()                   # POST endpoint
        result = check.compare(check.fetch())  # re-verify
     yield result
  ↓
render_report(results)
exit 0 if all PASS else 1
```

## 5. API endpoints

### OKX

| Operation | Method | Path | Notes |
|---|---|---|---|
| read acctLv + posMode | GET | `/api/v5/account/config` | already used by `check_okx_collateral.py:203-204` |
| set acctLv | POST | `/api/v5/account/set-account-level` | body `{"acctLv":"3"}`; used by `okx_set_account_level.py:133` |
| set posMode | POST | `/api/v5/account/set-position-mode` | body `{"posMode":"net_mode"}` |
| pre-fix position guard | GET | `/api/v5/account/positions` | abort fix if any `pos != "0"` |
| pre-fix pending-order guard | GET | `/api/v5/trade/orders-pending` | abort fix if `data` non-empty |

### Bybit

| Operation | Method | Path | Notes |
|---|---|---|---|
| read unifiedMarginStatus | GET | `/v5/account/info` | already used by `bybit_precheck.rs:19`, `setup_bybit_spot_margin.py:37` |
| read spotMarginMode | GET | `/v5/spot-margin-trade/state` | already used by `bybit_precheck.rs:20` |
| set spotMarginMode | POST | `/v5/spot-margin-trade/switch-mode` | body `{"spotMarginMode":"1"}`; will fail with explicit error if quiz not completed |
| upgrade UTA | (not executed) | — | print `python3 .../setup_bybit_spot_margin.py --yes` as remediation |

## 6. --execute safety

`posMode` and `acctLv` switches are rejected by OKX if any position or open order exists. The script enforces this client-side before issuing the POST:

1. Call positions endpoint; collect any `instId` with `pos != "0"`.
2. Call orders-pending endpoint; collect any open order.
3. If either set is non-empty:
   - Print the offending instruments / order IDs.
   - Mark the FAIL with `detail="cannot fix: account not empty"`.
   - Print remediation: `./cross_scripts/set_cross_cancel_all.py --execute` then manually flat positions.
   - Skip the POST; exit non-zero.

UTA upgrade (`unifiedMarginStatus`) is one-way and account-wide; the script never auto-executes it regardless of `--execute`. The FAIL detail prints the explicit fix command pointing at `setup_bybit_spot_margin.py --yes`.

Bybit `spotMarginMode` switch requires the user to have completed the Bybit web/app quiz; if the API returns the corresponding error code, the script surfaces the same hint that `setup_bybit_spot_margin.py` does.

## 7. Output contract

```
=== verify cross account modes: bybit-okex-cross-arb01 ===
open  = bybit-futures
hedge = okex-futures

[OKX]
  ✅ acctLv      = 3  (多币种保证金, expected ∈ {3,4})
  ❌ posMode     = "long_short_mode"  (expected "net_mode")
        fix: POST /api/v5/account/set-position-mode {"posMode":"net_mode"}
        cmd: ./cross_scripts/verify_cross_account_modes.py --execute --exchange okex

[Bybit]
  ✅ unifiedMarginStatus = 4  (UTA 2.0, expected ≥ 3)
  ✅ spotMarginMode      = "1"

result: 3 PASS / 1 FAIL
exit 1
```

Each `[<Exchange>]` block lists checks in fixed order. FAIL rows print the API call and the cmd a human can copy to fix.

## 8. Exit codes

- `0` — all PASS (including all FAILs successfully repaired and re-verified under `--execute`).
- `1` — at least one FAIL remains.
- `2` — credential missing, env file unreadable, HTTP error, malformed API response.

## 9. Error handling

- Missing credentials: print `ERROR: 缺少环境变量: BYBIT_API_KEY, OKX_PASSPHRASE` (matches `okx_set_account_level.py:106`).
- HTTP non-2xx: print `ERROR: <METHOD> <PATH> http_status=<code> body=<truncated 256B>`.
- API non-zero `code` (OKX) / `retCode` (Bybit): print `ERROR: <PATH> code=<code> msg=<msg>`.
- Both fatal errors yield exit 2 before any check completes.

## 10. Testing

Per the user's TDD-skip preference for Rust (and explicit "Python 除外"), Python tests are in scope.

Add `tests/test_verify_cross_account_modes.py` covering:
- `parse_env_name` valid cases (`bybit-okex-cross-arb01`, `okex-binance-cross-trade`) and invalid (`bybit-cross-arb01`, `bybit-okex-arb01`).
- `compare_expected` for each of the 4 checks (PASS/FAIL boundaries: acctLv=2/3/4, posMode net/long_short, unifiedMarginStatus=2/3/4, spotMarginMode "0"/"1").
- Fix-cmd string includes correct exchange flag.
- `precheck_empty_account` returns OK for empty fixtures, NOT-OK for fixtures with one position or one pending order.
- No real HTTP — mock the request layer.

## 11. Trade-offs

- **Python over Rust binary** — faster iteration, mirrors existing `set_cross_*.py` family, no engine restart needed to use it. Loses the on-startup auto-check guarantee that `bybit_precheck.rs` already provides; OKX still has no startup check (out of scope for this work).
- **Verify-first, fix-optional** — minimizes blast radius. The fixable items still need `--execute` and pass the empty-account guard, so the dangerous failure modes (mid-switch with positions) are blocked.
- **No leverage check** — keeps script focused; leverage already has dedicated tooling.
- **No checker for binance/gate/bitget** — defers complexity; the dispatch shape (`{exchange → Checker}`) leaves the door open.

## 12. Open questions

None at design time. Implementation plan to follow.
