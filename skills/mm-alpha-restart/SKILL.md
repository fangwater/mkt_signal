---
name: mm-alpha-restart
description: Use when the user wants to restart an MM alpha env such as `bitget_mm_alpha`, `okex_mm_alpha`, `binance_mm_alpha`, `bybit_mm_alpha`, or `gate_mm_alpha`. Follow the exact stop, cancel, start, and log-check sequence: stop `trade_signal` in the alpha directory, stop `trade_engine` in the alpha directory, run `bash scripts/close_mm_all_um_ws_orders.sh --env-name <env_name> --execute` from the `mkt_signal` repo root, stop `pre_trade` in the alpha directory, start `trade_engine`, verify pmdaemon logs for MM TE/PT under `/home/ubuntu/.pmdaemon/logs` with no obvious errors, then start `trade_signal` and verify PM2 logs.
metadata:
  short-description: Restart an MM alpha env safely
---

# MM Alpha Restart

Use this skill when the user asks to restart a market-making alpha env such as:

- `bitget_mm_alpha`
- `okex_mm_alpha`
- `binance_mm_alpha`
- `bybit_mm_alpha`
- `gate_mm_alpha`

This workflow is order-sensitive. Do not reorder steps unless the user explicitly asks to change the runbook.

## Required paths

- Repo root:
  - `/home/ubuntu/crypto_mkt/mkt_signal`
- Target env dir:
  - `/home/ubuntu/<env_name>`
- pmdaemon logs:
  - `/home/ubuntu/.pmdaemon/logs`

## Command rules

- Run `trade_signal`, `trade_engine`, and `pre_trade` stop/start scripts from the target env directory `/home/ubuntu/<env_name>`.
- Run `bash scripts/close_mm_all_um_ws_orders.sh --env-name <env_name> --execute` from the repo root `/home/ubuntu/crypto_mkt/mkt_signal`.
- `<env_name>` must match the exact target env, for example `bitget_mm_alpha`.
- After starting `trade_engine` and `pre_trade`, verify both process status and recent logs before starting `trade_signal`.
- `trade_signal` log verification uses PM2 logs, not pmdaemon logs.

## Standard sequence

Assume:

- `ENV_NAME=bitget_mm_alpha`
- `ENV_DIR=/home/ubuntu/bitget_mm_alpha`
- `REPO_DIR=/home/ubuntu/crypto_mkt/mkt_signal`

### 1. Stop trade signal

Run in `ENV_DIR`:

```bash
bash scripts/stop_trade_signal.sh
```

### 2. Stop trade engine

Run in `ENV_DIR`:

```bash
bash mm_scripts/stop_mm_trade_engine.sh
```

### 3. Cancel all UM WS orders

Run in `REPO_DIR`:

```bash
bash scripts/close_mm_all_um_ws_orders.sh --env-name bitget_mm_alpha --execute
```

For another env, replace `bitget_mm_alpha` with the actual env name.

### 4. Stop pre-trade

Run in `ENV_DIR`:

```bash
bash mm_scripts/stop_mm_pre_trade.sh
```

### 5. Start trade engine

Run in `ENV_DIR`:

```bash
bash mm_scripts/start_mm_trade_engine.sh
```

### 6. Start pre-trade

Run in `ENV_DIR`:

```bash
bash mm_scripts/start_mm_pre_trade.sh
```

### 7. Verify MM trade engine and pre-trade

Check status:

```bash
pmdaemon list | rg 'mm_(te|pt)_.*alpha'
```

For a specific env, prefer:

```bash
pmdaemon list | rg 'mm_(te|pt)_bitget_alpha'
```

Check recent logs:

```bash
tail -n 80 /home/ubuntu/.pmdaemon/logs/mm_te_bitget_alpha-out.log
tail -n 80 /home/ubuntu/.pmdaemon/logs/mm_te_bitget_alpha-error.log
tail -n 80 /home/ubuntu/.pmdaemon/logs/mm_pt_bitget_alpha-out.log
tail -n 80 /home/ubuntu/.pmdaemon/logs/mm_pt_bitget_alpha-error.log
```1

If the user explicitly wants follow mode, use:

```bash
tail -f /home/ubuntu/.pmdaemon/logs/mm_te_bitget_alpha-out.log
tail -f /home/ubuntu/.pmdaemon/logs/mm_pt_bitget_alpha-out.log
```

Success signals:

- `pmdaemon list` shows both TE and PT as `online`
- `trade_engine` log shows startup complete and exchange private WS login success
- `pre_trade` log shows Redis init, channel init, main loop start, and snapshot/query subscriptions
- error logs do not show new `ERROR`, `panic`, or repeated `failed` messages after restart

### 8. Start trade signal

Run in `ENV_DIR`:

```bash
bash scripts/start_trade_signal.sh
```

### 9. Verify trade signal

Check status:

```bash
npx pm2 status --namespace bitget_mm_alpha
```

Check recent logs:

```bash
npx pm2 logs --namespace bitget_mm_alpha mm_bitget_futures_alpha_trade_signal --lines 80 --nostream
```

Success signals:

- PM2 process is `online`
- recent logs show normal MM decision activity, config reloads, or hedge/query handling
- no new fatal startup error, panic, or crash loop appears after restart

## Practical notes

- The stop scripts may report leaked processes and clean them with `SIGTERM` or `SIGKILL`; that is acceptable if the script exits successfully.
- The UM cancel step is expected to print the current open-order list and cancel results.
- `trade_signal` may emit `WARN` lines for strategy gating such as volatility limits. Those are not restart failures by themselves.
- Prefer checking logs immediately after each start rather than batching all checks at the end.

## Example: `bitget_mm_alpha`

```bash
cd /home/ubuntu/bitget_mm_alpha
bash scripts/stop_trade_signal.sh
bash mm_scripts/stop_mm_trade_engine.sh

cd /home/ubuntu/crypto_mkt/mkt_signal
bash scripts/close_mm_all_um_ws_orders.sh --env-name bitget_mm_alpha --execute

cd /home/ubuntu/bitget_mm_alpha
bash mm_scripts/stop_mm_pre_trade.sh
bash mm_scripts/start_mm_trade_engine.sh
bash mm_scripts/start_mm_pre_trade.sh

tail -n 80 /home/ubuntu/.pmdaemon/logs/mm_te_bitget_alpha-out.log
tail -n 80 /home/ubuntu/.pmdaemon/logs/mm_pt_bitget_alpha-out.log

bash scripts/start_trade_signal.sh
npx pm2 logs --namespace bitget_mm_alpha mm_bitget_futures_alpha_trade_signal --lines 80 --nostream
```
