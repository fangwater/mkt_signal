# Bybit Intra Non-Online Position Cleanup

This runbook records the cleanup flow used for `bybit-intra-arb01` on the SG host.
It intentionally avoids credentials and account-specific secrets.

## Scope

- Environment: `bybit-intra-arb01`
- Host access: `ssh -i ~/.ssh/aws-sg.pem ubuntu@47.131.162.78`
- Dashboard snapshot:
  `http://47.131.162.78:4191/intra/bybit-intra-arb01/snapshot`
- Redis online symbol keys:
  - `intra_dump_symbols:bybit`
  - `intra_trade_symbols:bybit`
  - `intra_fwd_trade_symbols:bybit`
  - `intra_bwd_trade_symbols:bybit`
  - `intra_unimmr_close_symbols:bybit`
  - `bybit-intra-arb01:intra_unimmr_close_symbols:bybit-margin_bybit-futures`

Treat every command with `--execute` as live trading.

## Inspect Online Symbols

Run on the SG host:

```bash
cd ~/bybit-intra-arb01
python3 - <<'PY'
import json
import os
import redis

r = redis.Redis(
    host=os.environ.get("REDIS_HOST", "127.0.0.1"),
    port=int(os.environ.get("REDIS_PORT", "6379")),
    db=int(os.environ.get("REDIS_DB", "0")),
    password=os.environ.get("REDIS_PASSWORD") or None,
)
keys = [
    "intra_dump_symbols:bybit",
    "intra_trade_symbols:bybit",
    "intra_fwd_trade_symbols:bybit",
    "intra_bwd_trade_symbols:bybit",
    "intra_unimmr_close_symbols:bybit",
    "bybit-intra-arb01:intra_unimmr_close_symbols:bybit-margin_bybit-futures",
]
for key in keys:
    raw = r.get(key)
    values = json.loads(raw.decode()) if raw else []
    print(f"{key}\t{len(values)}\t{values}")
PY
```

Union the trade and close lists to build the online asset set. Normalize symbols
such as `BTCUSDT` to asset `BTC`.

## Compute Cleanup Targets

Fetch the dashboard snapshot locally or on the host:

```bash
curl -fsS --max-time 10 \
  http://47.131.162.78:4191/intra/bybit-intra-arb01/snapshot \
  -o /tmp/bybit-intra-arb01.snapshot.json
```

Select rows from `pre_trade_exposure` where:

- `is_total != true`
- `asset` is not in the online asset set
- `abs(open_usdt) + abs(hedge_usdt)` is material for the first pass, for example
  `>= 1`

Validate candidates against Bybit public `spot` and `linear` instruments before
batching them into cleanup scripts.

## Clear Material Non-Online Positions

Use the deployed Bybit clear script from the env directory. Always dry-run first:

```bash
cd ~/bybit-intra-arb01
python3 scripts/flatten_bybit_pm.py \
  --mode clear \
  --symbols SYMBOL1USDT,SYMBOL2USDT
```

If the plan is correct, execute:

```bash
python3 scripts/flatten_bybit_pm.py \
  --mode clear \
  --execute \
  --symbols SYMBOL1USDT,SYMBOL2USDT
```

Behavior:

- Phase R: `/v5/account/no-convert-repay` for same-coin repay.
- Phase U: Bybit linear market orders with `reduceOnly=true`.
- Phase B: spot buyback for borrow-side residuals, then no-convert repay.
- Phase S: spot selldown for positive wallet residuals.

Expected failure classes:

- `retCode=10006`: Bybit rate limit. Retry same-coin repay later with slower pacing.
- `retCode=170140`: spot order value below lower limit. Leave these for small-balance
  conversion.

## Retry Same-Coin Repay Slowly

For assets with wallet balance that can repay same-coin borrow, retry
`/v5/account/no-convert-repay` with a delay between coins. Do not use
`quick-repayment` unless explicitly accepting cross-coin conversion side effects.

The clear script and `scripts/flatten_bybit_pm.py` already implement the
no-convert-repay call. For a large list, prefer a small helper or manual batches
with several seconds of sleep between calls.

## Clear Futures Dust

After the material pass, fetch a fresh snapshot and find non-online rows with
non-zero `hedge_qty`. These may still be below 1 USDT but above Bybit linear
minimum quantity.

Dry-run and execute another `--mode clear` batch for those symbols. The goal is
to close `hedge_qty` with reduce-only linear orders. Spot failures with
`170140` are expected and should be left for small-balance conversion.

The verification target after this step is:

```text
non_online_hedge_residual_count = 0
```

## Convert Spot Dust

Use `scripts/bybit_small_balance_convert.py`. It wraps Bybit Convert Small
Balances:

- `GET /v5/asset/covert/small-balance-list`
- `POST /v5/asset/covert/get-quote`
- `POST /v5/asset/covert/small-balance-execute`

Dry-run:

```bash
cd ~/bybit-intra-arb01
python3 scripts/bybit_small_balance_convert.py --to-coin USDT --max-batch 20
```

Execute:

```bash
python3 scripts/bybit_small_balance_convert.py \
  --to-coin USDT \
  --max-batch 20 \
  --execute
```

If Bybit returns:

```text
retCode=10005 retMsg=Permission denied, please check your API key permissions.
```

enable the `Convert` permission for the current API key in Bybit API Management,
or create a new key with the existing required permissions plus `Convert`, then
update the env's `BYBIT_API_KEY` and `BYBIT_API_SECRET`.

Do not enable withdrawal permission for this workflow.

## Final Verification

Fetch a fresh dashboard snapshot and verify:

- All non-online rows have `hedge_qty == 0` and `hedge_usdt == 0`.
- Remaining non-online exposure is only spot/margin dust or borrow dust.
- Bybit small-balance conversion either cleaned the eligible residuals or reported
  a permission/eligibility reason.

Example final summary fields:

```text
non_online_hedge_residual_count = 0
non_online_spot_or_borrow_ge_1_count = <remaining spot/margin dust count>
```
