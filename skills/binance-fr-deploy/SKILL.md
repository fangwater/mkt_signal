---
name: binance-fr-deploy
description: Interactive deployment workflow for binance fr environments (config/viz/pre_trade/etc.) with per-command confirmation.
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
     - `./scripts/start_fr_persist_manager.sh --port <persist_port>`

6) **manual_signal**
   - Deploy:
     - `bash scripts/deploy_fr_manual_signal.sh --exchange binance --env-name <env> --port <manual_port> --apply-nginx`
   - Start:
     - `cd ~/<env>`
     - `./scripts/start_fr_manual_signal.sh --port <manual_port>`

7) **pre_trade**
   - Deploy:
     - `bash scripts/deploy_fr_pre_trade.sh --exchange binance --env-name <env>`
   - Start:
     - `cd ~/<env>`
     - `./scripts/start_fr_pre_trade.sh`

8) **trade_signal (deploy only)**
   - Deploy:
     - `bash scripts/deploy_fr_signal.sh --exchange binance --env-name <env>`
   - Start (if requested):
     - `cd ~/<env>`
     - `./scripts/start_trade_signal.sh`

## Inputs to collect before running

- Target env name (e.g. `binance_fr_hf02`)
- Target host for config UI (for user to open)
- Confirmed ports (computed from rules or provided by user)

If any input is missing, ask before proceeding.
