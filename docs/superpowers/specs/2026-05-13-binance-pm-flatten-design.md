# Binance Portfolio Margin: Flatten & Cancel-All Scripts — Design

Date: 2026-05-13
Scope: binance unified (Portfolio Margin / PAPI) FR arb environments (`binance_fr_arb01..05`)

## Goal

Provide two repo-local Python scripts deployed into each FR env directory so an operator can quickly:
1. Cancel all PM open orders (UM + cross-margin) for a given env, optionally filtered by symbol.
2. Flatten / clean up residual positions in a unified account, with two modes (`align` and `clear`), using API-derived state (not the viz dashboard).

Both scripts are deployed via `scripts/deploy_fr_signal.sh`'s existing `SCRIPTS_TO_SYNC` list and end up at `$HOME/<env_name>/scripts/` on the remote.

## Non-Goals

- Cross-exchange support — these scripts are binance-PAPI only.
- Dashboard scraping — state comes solely from `/papi/v1/balance`, `/papi/v1/um/positionRisk`, `/fapi/v1/exchangeInfo`.
- Wrapper / ssh launcher — operator ssh's in, `cd` to env dir, sources `env.sh`, runs `python3 scripts/<name>.py …`.

## Constraints (carry-overs from prior discussion)

- API keys are IP-restricted to the remote host — these scripts must execute on remote, not locally.
- Repay is the canonical way to extinguish margin borrow legs. Never market-buy back the asset to clear borrow.
- For align mode, "net" follows the FR-arb definition: `net_qty = margin_free + um_positionAmt - margin_borrowed`.
- For unified account dust (UM `qty < minQty`), there is no API closing path; default behavior is to skip with a warning.

---

## Script 1: `scripts/cancel_binance_pm_orders.py`

### Purpose
Cancel open orders on Binance unified account (UM futures and/or cross-margin) for one or more symbols, or all symbols.

### CLI
```
python3 scripts/cancel_binance_pm_orders.py
    [--symbols BTCUSDT,ETHUSDT]    # repeatable comma list; if omitted, scope is all symbols
    [--scope um|margin|both]       # default both
    [--execute]                    # default dry-run
```

### Behavior
1. CWD safety check: `basename(cwd)` must match `^binance_fr_`; else exit 2 with error.
2. Read `BINANCE_API_KEY` / `BINANCE_API_SECRET` from environment (env.sh must be sourced first).
3. Query open orders:
   - UM: `GET /papi/v1/um/openOrders` (no symbol → all)
   - Margin: `GET /papi/v1/margin/openOrders`
4. Filter by `--symbols` if provided. Print plan table:
   ```
   Symbol     | UM open | Margin open
   BTCUSDT    | 3       | 1
   ETHUSDT    | 0       | 2
   ```
5. If `--execute`, for each symbol with ≥1 open order:
   - UM: `DELETE /papi/v1/um/allOpenOrders?symbol=...`
   - Margin: `DELETE /papi/v1/margin/allOpenOrders?symbol=...`
6. Print result table with per-symbol HTTP status.

### Exit codes
- 0: all cancels succeeded (or dry-run completed)
- 1: at least one cancel call returned non-2xx
- 2: precondition / config error (missing credentials, bad CWD, malformed args)

### Self-contained
- No imports from other scripts in this repo.
- Inline HMAC signing, `urllib.request` calls, JSON parsing.

---

## Script 2: `scripts/flatten_binance_pm.py`

### Purpose
For each specified symbol, neutralize the FR-arb position via a three-phase pipeline:
**(R) repay borrowed → (U) UM reduce-only flatten → (D) [clear-mode only] dust → BNB**.

### CLI
```
python3 scripts/flatten_binance_pm.py
    --symbols BTCUSDT,ETHUSDT      # required; comma list of symbol names
    [--mode align|clear]           # default align
    [--execute]                    # default dry-run
```

### Mode semantics
- `align` (default): make `net_qty = 0` via UM futures. Margin `free` and `borrowed` may remain non-zero so long as they balance against UM position. Repay still runs first to extinguish any "禁态" (`free > 0 AND borrowed > 0` for the same asset).
- `clear`: drain the account. UM position → 0 (regardless of net). Then collect remaining margin asset to spot and convert to BNB.

### Data flow

```
[start]
  1. CWD safety check: basename matches ^binance_fr_
  2. Parse --symbols → asset list (BTCUSDT → BTC)
  3. Check BINANCE_API_KEY / SECRET present

[fetch state — single snapshot, no re-fetch later]
  - GET /papi/v1/balance               → per-asset {free, borrowed}
  - GET /papi/v1/um/positionRisk?symbol=X (per symbol)  → positionAmt
  - GET /fapi/v1/exchangeInfo          → stepSize, minQty, minNotional per symbol

[per-symbol planning, pure local]
  free, borrowed, pos = state[symbol]
  repay_amt = min(free, borrowed)
  free_after  = free - repay_amt
  borrow_after = borrowed - repay_amt     # may stay > 0 if free < borrowed
  net_qty = free_after - borrow_after + pos

  if mode == align:
    target_um_delta = -net_qty            # close enough to zero out net
    um_qty = floor_to_step(|target_um_delta|, stepSize)
    if um_qty < minQty:
      um_action = SKIP_DUST (warn)
    else:
      um_action = REDUCE_ONLY MARKET  side = SELL if net_qty>0 else BUY

  if mode == clear:
    target_um_delta = -pos
    um_qty = floor_to_step(|pos|, stepSize)
    same dust handling as align
    dust_plan = collect remaining free_after → spot → dust-convert to BNB

[print plan]  (always; in --execute also serves as audit log)
  Table: symbol | repay | um_side | um_qty | dust_qty | skip_reason

  If dry-run → exit 0

[execute phases]
  Phase R — Repay (sequential across assets, continue-on-error)
    for each asset with repay_amt > 0:
      POST /papi/v1/repayLoan?asset=X&amount=Y
      record success / fail; do NOT abort the run on per-asset fail

  Phase U — UM reduce-only orders (sequential per symbol)
    for each symbol with um_qty >= minQty:
      POST /papi/v1/um/order
        body: symbol, side, type=MARKET, quantity=um_qty, reduceOnly=true
      record success / fail; do NOT abort

  Phase D — Dust convert (clear mode only; runs only after all Phase U succeed for that symbol)
    Compute final dust list = [asset for asset in symbols if free_after > 0 AND um_action succeeded]
    POST /papi/v1/asset-collection?asset=X    (for each)
    POST /sapi/v1/asset/dust  asset=[X1,X2,...]  (single batch call)

[final report]
  Table per symbol:
    repay_done | um_done | dust_done | leftover_borrow | warnings
  Exit code:
    0 — all planned actions succeeded
    1 — at least one phase had ≥1 per-item failure (partial completion)
    2 — precondition / config error
```

### Edge cases

| Case | Default behavior | Override |
|---|---|---|
| `qty < minQty` after step rounding | skip + WARN (no reliable API closing path; accept residual or use Binance UI) | — |
| `notional < MIN_NOTIONAL` but `qty ≥ minQty` | submit with `reduceOnly=true` (binance allows reduce-only to bypass MIN_NOTIONAL) | — |
| `borrowed > free` (can only partially repay) | repay the max (`= free`), record leftover borrow in report, continue subsequent phases | manual intervention later |
| Symbol not in exchangeInfo | skip + WARN | — |
| asset not in `/papi/v1/balance` response | treat as `free=0, borrowed=0` | — |
| UM position absent (no entry returned) | treat as `pos=0` | — |
| Repay endpoint 5xx | record per-asset failure, continue | — |
| Repay endpoint 4xx with `Repay amount exceeds borrowed amount` | trim repay_amt to borrowed and retry once | — |
| asset-collection fails | skip dust step for that asset, WARN | — |

### Failure semantics
- All phases run "best-effort, continue-on-error" within a single run. The final report makes failures visible; operator decides whether to re-run.
- The plan table is the ground truth of intent; the result table is the ground truth of outcome.
- Process never exits between phases (always reaches "final report").

### Safety
- Dry-run by default. `--execute` is required to send any state-changing API call.
- CWD check prevents accidentally running with wrong env's API key (e.g., from a personal home dir).
- No file writes, no caching: each run is self-contained.

### Self-contained
- No imports from other scripts in this repo.
- All HMAC signing, REST calls, exchangeInfo parsing inline.

---

## Deployment

Both files are appended to the `SCRIPTS_TO_SYNC` array in `scripts/deploy_fr_signal.sh` (currently around line 154):

```diff
   SCRIPTS_TO_SYNC=(
     "start_trade_signal.sh"
     "stop_trade_signal.sh"
     "start_fr_signal.sh"
     "stop_fr_signal.sh"
     "flatten_fr_futures_exposure.py"
+    "flatten_binance_pm.py"
+    "cancel_binance_pm_orders.py"
     ...
   )
```

After deploy:
```
scripts/deploy_fr_binance.sh arb03
```
the scripts land at `$HOME/binance_fr_arb03/scripts/{flatten_binance_pm,cancel_binance_pm_orders}.py` (local), and then `fr_remote_sync_env_dir` rsyncs them to `ubuntu@54.64.147.69:~/binance_fr_arb03/scripts/`.

### Operator workflow (typical)
```bash
# 1) cancel all open orders for the relevant symbols
ssh ubuntu@54.64.147.69 \
  'cd ~/binance_fr_arb03 && source env.sh && \
   python3 scripts/cancel_binance_pm_orders.py --symbols BTCUSDT,ETHUSDT --execute'

# 2) dry-run flatten plan
ssh ubuntu@54.64.147.69 \
  'cd ~/binance_fr_arb03 && source env.sh && \
   python3 scripts/flatten_binance_pm.py --symbols BTCUSDT,ETHUSDT'

# 3) execute (align mode default)
ssh ubuntu@54.64.147.69 \
  'cd ~/binance_fr_arb03 && source env.sh && \
   python3 scripts/flatten_binance_pm.py --symbols BTCUSDT,ETHUSDT --execute'

# Or fully drain to BNB:
ssh ubuntu@54.64.147.69 \
  'cd ~/binance_fr_arb03 && source env.sh && \
   python3 scripts/flatten_binance_pm.py --symbols BTCUSDT,ETHUSDT --mode clear --execute'
```

---

## API reference (endpoints used)

| Method | Path | Use |
|---|---|---|
| GET | `/papi/v1/balance` | Read per-asset margin free/borrowed |
| GET | `/papi/v1/um/positionRisk` | Read UM position size & side |
| GET | `/papi/v1/um/openOrders` | List UM open orders (cancel script) |
| GET | `/papi/v1/margin/openOrders` | List margin open orders (cancel script) |
| GET | `/fapi/v1/exchangeInfo` | LOT_SIZE, minQty, minNotional (public, no signing) |
| POST | `/papi/v1/repayLoan` | Repay margin borrow per asset |
| POST | `/papi/v1/um/order` | UM reduce-only flatten order |
| POST | `/papi/v1/asset-collection` | Collect margin asset → spot wallet |
| POST | `/sapi/v1/asset/dust` | Convert spot dust → BNB (batched) |
| DELETE | `/papi/v1/um/allOpenOrders` | Cancel all UM open orders by symbol |
| DELETE | `/papi/v1/margin/allOpenOrders` | Cancel all margin open orders by symbol |

All PAPI/SAPI endpoints require HMAC-SHA256 signing of query string with `BINANCE_API_SECRET`; `X-MBX-APIKEY` header for auth.

---

## Testing

No unit tests — these scripts are thin REST clients with little logic worth mocking. Verification is operator-driven:

1. **Dry-run on live env** (default behavior). Visually confirm the planned actions table matches expected state on the dashboard.
2. **Single-symbol --execute on a low-notional symbol** to validate signing + endpoint paths.
3. **Final report cross-check**: compare against `/papi/v1/balance` + `/papi/v1/um/positionRisk` after the run.

If a regression suite is later desired, the natural seams are:
- `plan_for_symbol(state, mode, spec) -> Plan` — pure function over balance + position + exchangeInfo
- HTTP layer wrapped in a small adapter for swapping a fixture client

But these are not added in v1.

## Out of scope

- Auto-retry with backoff (operator re-runs manually if needed)
- Cron / unattended invocation (always operator-initiated)
- Cross-env batch (one env per invocation; deploy already handles per-env distribution)
- Reconciliation against pre_trade exposure manager (not a goal of this tool)
