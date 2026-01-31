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
- **Env setup first**: `env.sh` must be created and configured before any deploy/start. Require IPC namespace, account mode, and API credentials.
- **IPC_NAMESPACE rule**: must equal the env folder name (e.g. `~/binance_fr_hf02` => `IPC_NAMESPACE=binance_fr_hf02`).
- **Account mode rule**: set `BINANCE_ACCOUNT_MODE=UNIFIED` unless the user explicitly asks for STANDARD.

## Port rules (binance)

Base envs:
- trade: viz 10031, config 18031, persist 19131, manual 8931
- hf01: viz 10041, config 18041, persist 19141, manual 8932

General rule for hfNN (NN as 2-digit number):
- viz: 10040 + NN
- config: 18040 + NN
- persist: 19140 + NN
- manual: 8931 + NN

If any port conflicts or the user provides a different mapping, stop and ask.

## Deployment flow (commands must be confirmed)

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
   - Start:
     - `cd ~/<env>`
     - `./scripts/start_fr_config_server.sh`
   - Manual: open `http://<host>:4191/fr/<env>/config/`, save Risk Params (open=binance-margin, hedge=binance-futures).
   - Optional check:
     - `cd ~/<env>`
     - `./scripts/print_fr_risk_params.py --open-venue binance-margin --hedge-venue binance-futures`

2) **account_monitor**
   - Deploy:
     - `bash scripts/deploy_account_monitor.sh --exchange binance --env-name <env>`
   - Start:
     - `cd ~/<env>`
     - `./scripts/start_account_monitor.sh`

3) **trade_engine**
   - Deploy:
     - `bash scripts/deploy_fr_trade_engine.sh --exchange binance --env-name <env>`
   - Start:
     - `cd ~/<env>`
     - `./scripts/start_trade_engine.sh`

4) **viz_server**
   - Deploy:
     - `bash scripts/deploy_fr_viz_server.sh --env-name <env> --exchange binance --port <viz_port> --apply-nginx`
   - Start:
     - `cd ~/<env>`
     - `./scripts/start_fr_viz_server.sh`

5) **persist_manager**
   - Deploy:
     - `bash scripts/deploy_fr_persist_manager.sh --exchange binance --env-name <env>`
   - Start:
     - `cd ~/<env>`
     - `source ./env.sh`
     - `./scripts/start_fr_persist_manager.sh --port <persist_port>`

6) **manual_signal**
   - Deploy:
     - `bash scripts/deploy_fr_manual_signal.sh --exchange binance --env-name <env> --port <manual_port> --apply-nginx`
   - Start:
     - `cd ~/<env>`
     - `source ./env.sh`
     - `./scripts/start_fr_manual_signal.sh --port <manual_port>`

7) **pre_trade**
   - Deploy:
     - `bash scripts/deploy_fr_pre_trade.sh --exchange binance --env-name <env>`
   - Start:
     - `cd ~/<env>`
     - `source ./env.sh`
     - `./scripts/start_fr_pre_trade.sh`

8) **trade_signal (deploy only)**
   - Deploy:
     - `bash scripts/deploy_fr_signal.sh --exchange binance --env-name <env>`
   - Start (if requested):
     - `cd ~/<env>`
     - `source ./env.sh`
     - `./scripts/start_trade_signal.sh`

## Inputs to collect before running

- Target env name (e.g. `binance_fr_hf02`)
- Target host for config UI (for user to open)
- Confirmed ports (computed from rules or provided by user)
- Confirmed BINANCE_ACCOUNT_MODE (defaults to UNIFIED; STANDARD only if explicitly requested)
- Confirmed BINANCE_API_KEY / BINANCE_API_SECRET are set in env.sh (manual or interactive)

If any input is missing, ask before proceeding.
