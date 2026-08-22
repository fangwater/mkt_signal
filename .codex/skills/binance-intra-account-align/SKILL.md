---
name: binance-intra-account-align
description: Align Binance account settings and wallet allocation for intra-arb and funding-rate deployments. Use when Codex needs to verify or set online-symbol UM futures leverage, distinguish standard Spot, standard UM, and Portfolio Margin balances, compare Spot versus futures allocation, or transfer USDT between Spot MAIN and UMFUTURE or Portfolio Margin, including moving all remaining Spot USDT.
---

# Binance Account Align

Use this for Binance same-exchange intra environments such as `binance-intra-arb01` and Binance Portfolio Margin FR environments such as `binance_fr_arb04`. The workflow is live-account sensitive: leverage updates and wallet transfers mutate the exchange account.

## Safety

- State the target host, env path, exchange, symbols scope, and whether the next command is read-only or mutating.
- Run dry-run/read-only checks first whenever possible.
- Do not execute `--execute` leverage updates or wallet transfers unless the user has explicitly confirmed the mutation and amount.
- Interpret shorthand amounts such as `10w 156.48` explicitly as `100156.48` and state that interpretation before execution.
- Treat `all` as all currently free units of the named asset, not all account assets. Resolve the exact free balance immediately before submitting.
- Do not print API keys, secrets, or full env files.
- Use the environment-local `env.sh` from the target deployment directory.
- Submit a transfer at most once. If the POST times out or returns an unknown transport result, query balances and transfer history before considering a retry.

## Balance Scopes

Do not infer standard Spot balance from Portfolio Margin data. Query each requested wallet explicitly:

- Standard Spot MAIN: `GET /api/v3/account`; use the asset row's `free` and `locked` fields.
- Portfolio Margin: `GET /papi/v1/balance`; inspect `crossMarginFree`, `crossMarginLocked`, `crossMarginBorrowed`, `crossMarginInterest`, `umWalletBalance`, `umUnrealizedPNL`, and `totalWalletBalance`.
- Standard UM: `GET /fapi/v2/account`; use `totalWalletBalance`, `totalUnrealizedProfit`, and `totalMarginBalance`.

In a Portfolio Margin account, `/papi/v1/balance` and `/api/v3/account` can show separate USDT balances for the same API key. Always query both before saying that standard Spot is empty.

## Online Symbols And Leverage

Use the project script to discover online symbols from Redis and plan leverage changes:

```bash
cd /home/ubuntu/binance-intra-arb01
python3 scripts/set_online_futures_leverage.py --env-name binance-intra-arb01 --leverage 5
```

This is dry-run by default. The script reads the intra Redis JSON-list keys, including:

- `intra_fwd_trade_symbols:binance`
- `intra_bwd_trade_symbols:binance`
- `intra_trade_symbols:binance`
- `intra_dump_symbols:binance`
- `intra_unimmr_close_symbols:binance`
- `<env>:intra_unimmr_close_symbols:binance-margin_binance-futures`

To verify current leverage, query signed `GET /fapi/v2/positionRisk` for the script's symbols and check the `leverage` field. If any symbol is not at target and the user confirms mutation, execute:

```bash
cd /home/ubuntu/binance-intra-arb01
python3 scripts/set_online_futures_leverage.py --env-name binance-intra-arb01 --leverage 5 --execute
```

## Allocation Ratio

For this Binance intra setup, do not calculate spot allocation from only `/api/v3/account` USDT cash. That undercounts spot when the account holds spot base assets.

Use this ratio basis:

- Spot side: total estimated USDT value of all nonzero assets in the Spot MAIN wallet from `GET /api/v3/account`, priced with public ticker prices.
- Futures side: UM futures equity from `GET /fapi/v2/account` field `totalMarginBalance`.
- Total: `spot_est_usdt + futures_totalMarginBalance`.

Cross margin may be zero for this environment even though the open venue name is `binance-margin`; check `/sapi/v1/margin/account` separately if the user is asking about margin wallet state.

Formula:

```text
spot_ratio = spot_est_usdt / (spot_est_usdt + futures_totalMarginBalance)
futures_ratio = futures_totalMarginBalance / (spot_est_usdt + futures_totalMarginBalance)
target_spot_value = total * target_spot_ratio
transfer_delta = target_spot_value - spot_est_usdt
```

Interpret `transfer_delta`:

- `transfer_delta > 0`: move USDT from UM futures to Spot MAIN, type `UMFUTURE_MAIN`.
- `transfer_delta < 0`: move USDT from Spot MAIN to UM futures, type `MAIN_UMFUTURE`.

When spot contains non-USDT assets, transferring USDT changes wallet allocation but does not rebalance spot holdings. Recheck prices and balances immediately before execution.

## Standard Spot And UM Transfer

Use the existing transfer script where available. It is dry-run unless `--execute` is passed:

```bash
source /home/ubuntu/binance-intra-arb01/env.sh
python3 scripts/binance_transfer_std_cash.py --type MAIN_UMFUTURE --asset USDT --amount 3000
```

Some deployed envs may not have this script. Copy the repo version to `/tmp` and set `PYTHONPATH` so it can import deployed script helpers:

```bash
scp scripts/binance_transfer_std_cash.py jp-meta-elvpn:/tmp/binance_transfer_std_cash.py
ssh jp-meta-elvpn "cd /home/ubuntu/binance-intra-arb01 && set -a && source env.sh >/dev/null 2>&1 && set +a && PYTHONPATH=/home/ubuntu/binance-intra-arb01/scripts python3 /tmp/binance_transfer_std_cash.py --type MAIN_UMFUTURE --asset USDT --amount 3000 --execute"
```

After any transfer, run a read-only balance check again and report:

- Spot USDT cash.
- Spot total estimated USDT value.
- Futures wallet balance.
- Futures unrealized PnL.
- Futures `totalMarginBalance`.
- Combined estimated total and spot/futures percentages.

## Standard Spot And Portfolio Margin Transfer

Use the bundled script for Spot MAIN and Portfolio Margin transfers. It is dry-run by default, performs a read-only source-balance precheck, never retries, and performs a post-transfer balance check after success.

```bash
cd /home/ubuntu/binance_fr_arb04
set -a
source env.sh >/dev/null 2>&1
set +a
python3 /home/ubuntu/mkt_signal/.codex/skills/binance-intra-account-align/scripts/binance_pm_transfer.py \
  --type MAIN_PORTFOLIO_MARGIN --asset USDT --amount 100156.48
```

Only add `--execute` after the user confirms the exact amount and direction:

```bash
python3 /home/ubuntu/mkt_signal/.codex/skills/binance-intra-account-align/scripts/binance_pm_transfer.py \
  --type MAIN_PORTFOLIO_MARGIN --asset USDT --amount 100156.48 --execute
```

For an explicit request to move all remaining standard Spot USDT, let the script resolve Spot `free` immediately before submission:

```bash
python3 /home/ubuntu/mkt_signal/.codex/skills/binance-intra-account-align/scripts/binance_pm_transfer.py \
  --type MAIN_PORTFOLIO_MARGIN --asset USDT --all --execute
```

Use `PORTFOLIO_MARGIN_MAIN` for the reverse direction. Do not substitute `MAIN_UMFUTURE`; that moves funds to a standard UM wallet, not Portfolio Margin.

Record every successful `tranId`. In an actively trading FR environment, destination `crossMarginFree` may not rise by the exact transfer amount because orders and fills can immediately move funds between free, locked, and other assets. Confirm the source decrease, report destination `totalWalletBalance`, `crossMarginFree`, and `crossMarginLocked`, and avoid treating a dynamic free-balance delta as transfer failure when Binance returned a successful `tranId`.

## Known Pitfall

If you only sum Spot MAIN USDT cash plus UM futures balance, the result can be far below the exchange UI total net value because Spot MAIN may hold base assets such as `SYN`, `ZAMA`, `TON`, `VET`, or `QNT`. Always compute spot estimated value from all nonzero spot assets when the user asks for a spot/futures percentage.
