---
name: binance-fr-deploy
description: Interactive deployment workflow for binance fr environments (config/viz/pre_trade/etc.) with per-command confirmation; includes required env.sh setup (IPC_NAMESPACE from env folder name, BINANCE_ACCOUNT_MODE=UNIFIED, BINANCE_API_KEY/SECRET) before any deploy/start.
metadata:
  short-description: Deploy binance fr with per-command confirmation
---

# Binance FR deploy (interactive, per-command confirm)

Use this skill when the user asks to deploy a binance fr environment (e.g. binance_fr_hf02).
Goal: guide the full deploy sequence and **confirm before every shell command**.

## Rules (must follow)

- **Per-command confirmation**: Before each shell command, show it and ask "Run this command? (yes/no)".
  - Only run after an explicit "yes".
  - If "no", skip and move on or ask for the next action.
- **One command at a time**: Do not chain commands with `&&` or `;`.
- **Manual steps**: For web UI steps (risk params), ask for confirmation before continuing.
- **Env name** must match `binance_fr_<suffix>` and should be lowercase.
- **All deploy commands must pass `--env-name`**: no legacy `trade/test` positional mode.
- **Viz deploy must pass `--port`**: never rely on implicit default port.
- **Env setup first**: `env.sh` must be created and configured before any deploy/start. Require IPC namespace, account mode, and API credentials.
- **Source before every start/stop**: Always `source ./env.sh` immediately before each process start/stop command.
- **Source must be in same shell**: Environment does not persist across commands, so run start/stop via a single `bash -lc` command with `cd`, `source`, and the start/stop script on separate lines (no `&&` / `;`).
- **Always cd to env dir**: Before any start/stop command, `cd ~/<env>` first.
- **Deploy uses escalated permissions**: Run every deploy command with escalated permissions by default.
- **Post start/stop checks**: After every start/stop, run PM2 status and log checks (one command at a time).
- **IPC_NAMESPACE rule**: must equal the env folder name (e.g. `~/binance_fr_hf02` => `IPC_NAMESPACE=binance_fr_hf02`).
- **Account mode rule**: set `BINANCE_ACCOUNT_MODE=UNIFIED` unless the user explicitly asks for STANDARD.

## Port rules (binance)

Base envs:
- trade: viz 10031, config 18031
- hf01: viz 10041, config 18041

General rule for hfNN (NN as 2-digit number):
- viz: 10040 + NN
- config: 18040 + NN

If any port conflicts or the user provides a different mapping, stop and ask.

## Deployment flow (fixed order, commands must be confirmed)

0) **env.sh (required, before everything else)**
   - Create `~/\<env>/env.sh` manually (do not run any script for this).
   - Must include:
     - `export IPC_NAMESPACE='<env>'` (exactly the env folder name, e.g. `binance_fr_hf02`)
     - `export BINANCE_ACCOUNT_MODE='UNIFIED'` (fixed default; change only if user explicitly requests STANDARD)
     - `export BINANCE_API_KEY='...'`
     - `export BINANCE_API_SECRET='...'`
   - **Allow interactive input for API/secret** (if user wants):
     - Prompt for key/secret in shell, then write `env.sh` from those variables.
     - Keep commands one-at-a-time (no `&&` / `;`), and confirm each command before running.
   - Make sure it is readable and sourceable:
     - `cd ~/<env>`
     - `source ./env.sh`
   - Do not proceed until the user confirms `env.sh` is ready and sourced.

1) **Config server**
   - Deploy:
     - `bash scripts/deploy_fr_config_server.sh --env-name <env> --exchange binance --port <config_port> --apply-nginx`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_fr_config_server.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> fr_config_server_<env> --lines 50`
   - Manual: open `http://<host>:4191/fr/<env>/config/`, save Risk Params (open=binance-margin, hedge=binance-futures).
   - Optional check:
     - `cd ~/<env>`
     - `./scripts/print_fr_risk_params.py --open-venue binance-margin --hedge-venue binance-futures`

2) **account_monitor**
   - Deploy:
     - `bash scripts/deploy_account_monitor.sh --exchange binance --env-name <env>`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_account_monitor.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> account_monitor_<env> --lines 50`

3) **trade_engine**
   - Deploy:
     - `bash scripts/deploy_fr_trade_engine.sh --exchange binance --env-name <env>`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_trade_engine.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> trade_engine_<env> --lines 50`

4) **viz_server**
   - Deploy:
     - `bash scripts/deploy_fr_viz_server.sh --env-name <env> --exchange binance --port <viz_port> --apply-nginx`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_fr_viz_server.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> viz_server_<env> --lines 50`

5) **persist_manager**
   - Deploy:
     - `bash scripts/deploy_fr_persist_manager.sh --exchange binance --env-name <env>`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_fr_persist_manager.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> persist_manager_<env> --lines 50`

6) **pre_trade**
   - Deploy:
     - `bash scripts/deploy_fr_pre_trade.sh --exchange binance --env-name <env>`
   - Start (single command):
     - `bash -lc 'cd ~/<env>\nsource ./env.sh\n./scripts/start_fr_pre_trade.sh'`
   - Check (PM2 status):
     - `npx pm2 list --namespace <env>`
   - Check (logs):
     - `npx pm2 logs --namespace <env> pre_trade_<env> --lines 50`

7) **trade_signal (deploy only)**
   - Deploy:
     - `bash scripts/deploy_fr_signal.sh --exchange binance --env-name <env>`
   - Do NOT start unless explicitly requested (default: deploy only).

## Inputs to collect before running

- Target env name (e.g. `binance_fr_hf02`)
- Target host for config UI (for user to open)
- Confirmed ports (config/viz)
- Explicit `viz_port` (required by `deploy_fr_viz_server.sh`)
- Confirmed BINANCE_ACCOUNT_MODE (defaults to UNIFIED; STANDARD only if explicitly requested)
- Confirmed BINANCE_API_KEY / BINANCE_API_SECRET are set in env.sh (manual or interactive)
- Confirm that trade_signal is deploy-only

If any input is missing, ask before proceeding.
