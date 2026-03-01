---
name: okex-fr-deploy
description: Interactive deployment workflow for okex fr environments with per-command confirmation; includes required env.sh setup (IPC_NAMESPACE + OKX creds + OKEX_LOAN_RATE_URL) and the OKX loan-rate cache Python service.
metadata:
  short-description: Deploy okex fr with per-command confirmation and okx rate cache
---

# OKEx FR deploy (interactive, per-command confirm)

Use this skill when the user asks to deploy an okex fr environment (e.g. okex_fr_trade).
Goal: guide the full deploy sequence and **confirm before every shell command**.

## Rules (must follow)

- **Per-command confirmation**: Before each shell command, show it and ask "Run this command? (yes/no)".
  - Only run after an explicit "yes".
  - If "no", skip and move on or ask for the next action.
- **One command at a time**: Do not chain commands with `&&` or `;`.
- **Manual steps**: For web UI steps (risk params), ask for confirmation before continuing.
- **Env name** must match `okex_fr_<suffix>` and should be lowercase.
- **All deploy commands must pass `--env-name`**: no legacy `trade/test` positional mode.
- **Viz deploy must pass `--port`**: never rely on implicit default port.
- **Env setup first**: `env.sh` must be created and configured before any deploy/start. Require IPC namespace, OKX credentials, and OKEX_LOAN_RATE_URL.
- **Source before every start/stop**: Always `source ./env.sh` immediately before each process start/stop command.
- **Source must be in same shell**: Environment does not persist across commands, so run start/stop via a single `bash -lc` command with `cd`, `source`, and the start/stop script on separate lines (no `&&` / `;`).
- **Always cd to env dir**: Before any start/stop command, `cd ~/<env>` first.
- **Deploy uses escalated permissions**: Run every deploy command with escalated permissions by default.
- **Post start/stop checks**: After every start/stop, run PM2 status and log checks (one command at a time).
- **IPC_NAMESPACE rule**: must equal the env folder name (e.g. `~/okex_fr_trade` => `IPC_NAMESPACE=okex_fr_trade`).
- **OKEX loan rate URL rule**: `OKEX_LOAN_RATE_URL` must match the rate-cache service URL (default `http://127.0.0.1:28901/rates`).

## Port rules (okex)

Default single-env ports:
- config: 18011
- viz: 10011

If deploying multiple okex fr envs or the user provides a different mapping, stop and ask for the exact ports.

## Deployment flow (fixed order, commands must be confirmed)

0) **env.sh (required, before everything else)**
   - Create `~/\<env>/env.sh` manually (do not run any script for this).
   - Must include:
     - `export IPC_NAMESPACE='<env>'` (exactly the env folder name, e.g. `okex_fr_trade`)
     - `export OKX_API_KEY='...'`
     - `export OKX_API_SECRET='...'`
     - `export OKX_PASSPHRASE='...'`
     - `export OKEX_LOAN_RATE_URL='http://127.0.0.1:28901/rates'` (or a user-provided URL)
   - **Allow interactive input for key/secret/passphrase** (if user wants):
     - Prompt for them in shell, then write `env.sh` from those variables.
     - Keep commands one-at-a-time (no `&&` / `;`), and confirm each command before running.
   - Make sure it is readable and sourceable:
     - `cd ~/<env>`
     - `source ./env.sh`
   - Do not proceed until the user confirms `env.sh` is ready and sourced.

1) **Config server**
   - Deploy:
     - `bash scripts/deploy_fr_config_server.sh --env-name <env> --exchange okex --port <config_port> --apply-nginx`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_fr_config_server.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> fr_config_server_<env> --lines 50`
   - Manual: open `http://<host>:4191/fr/<env>/config/`, save Risk Params (open=okex-margin, hedge=okex-futures).
   - Optional check:
     - `cd ~/<env>`
     - `./scripts/print_fr_risk_params.py --open-venue okex-margin --hedge-venue okex-futures`

2) **OKX loan-rate cache service (python)**
   - Start (single command, run from repo or env dir):
     - `npx pm2 start python3 --name okx_rate_cache_<env> --namespace <env> -- scripts/okx_rate_cache_server.py --host 127.0.0.1 --port 28901`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> okx_rate_cache_<env> --lines 50`
   - Optional quick check:
     - `curl http://127.0.0.1:28901/rates`

3) **account_monitor**
   - Deploy:
     - `bash scripts/deploy_account_monitor.sh --exchange okex --env-name <env>`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_account_monitor.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> account_monitor_<env> --lines 50`

4) **trade_engine**
   - Deploy:
     - `bash scripts/deploy_fr_trade_engine.sh --exchange okex --env-name <env>`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_trade_engine.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> trade_engine_<env> --lines 50`

5) **viz_server**
   - Deploy:
     - `bash scripts/deploy_fr_viz_server.sh --env-name <env> --exchange okex --port <viz_port> --apply-nginx`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_fr_viz_server.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> viz_server_<env> --lines 50`

6) **persist_manager**
   - Deploy:
     - `bash scripts/deploy_fr_persist_manager.sh --exchange okex --env-name <env>`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_fr_persist_manager.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> persist_manager_<env> --lines 50`

7) **pre_trade**
   - Deploy:
     - `bash scripts/deploy_fr_pre_trade.sh --exchange okex --env-name <env>`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_fr_pre_trade.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> pre_trade_<env> --lines 50`

8) **trade_signal (deploy only)**
   - Deploy:
     - `bash scripts/deploy_fr_signal.sh --exchange okex --env-name <env>`
   - Do NOT start unless explicitly requested (default: deploy only).

## Inputs to collect before running

- Target env name (e.g. `okex_fr_trade`)
- Target host for config UI (for user to open)
- Confirmed ports (config/viz)
- Explicit `viz_port` (required by `deploy_fr_viz_server.sh`)
- Confirmed OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE are set in env.sh (manual or interactive)
- Confirmed OKEX_LOAN_RATE_URL (default `http://127.0.0.1:28901/rates`) and rate cache port
- Confirm that trade_signal is deploy-only

If any input is missing, ask before proceeding.
