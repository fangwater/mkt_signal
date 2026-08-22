---
name: gate-fr-risk-limit-align
description: Align Gate.io funding-rate arb futures leverage and Redis per-symbol max_u caps in mkt_signal. Use when asked to audit Gate FR online symbols, compare current Gate futures position value against exchange risk_limit minus a buffer, lower Gate cross leverage when a position is above that buffered cap, enforce a unified/base max_u ceiling such as 98000, or write gate_fr_* max_pos_u_overrides from exchange risk limits.
---

# Gate FR Risk Limit Align

## Overview

Use this skill for live-risk Gate FR environments such as `gate_fr_arb01`.
The operation combines Gate private futures position reads, optional Gate leverage updates, and Redis writes to per-symbol `max_pos_u_overrides`.

Default buffer: `2000` USDT. The exchange-derived cap is `final risk_limit - 2000`.
If the user provides a unified/base max_u ceiling, the Redis cap to write is:

```text
max_u = min(final risk_limit - buffer, base_max_u)
```

## Safety

- Treat leverage changes and Redis writes as live trading risk.
- Always start with read-only audit output before mutating anything.
- State the target env, symbol scope, buffer, Gate settle currency, Redis key, and which symbols will be skipped.
- Do not change leverage for a symbol unless current `abs(value) > risk_limit - buffer`.
- Do not write a max_u override for a symbol whose Gate futures contract returns `CONTRACT_NOT_FOUND` or another non-2xx response.
- Do not include extra Redis online symbols outside the user's requested scope unless the user asks to process all Redis online symbols.
- A base max_u ceiling is a Redis risk cap, not a reason to lower leverage. If `abs(value) <= risk_limit - buffer` but `abs(value) > base_max_u`, write the capped max_u and report that the current position is already above the new cap; do not flatten or lower leverage unless explicitly requested.

## Redis Keys

For FR env `gate_fr_arb01` with open `gate-margin` and hedge `gate-futures`:

- Online symbol lists:
  - `gate_fr_arb01:fr_dump_symbols:gate-margin_gate-futures`
  - `gate_fr_arb01:fr_trade_symbols:gate-margin_gate-futures`
  - `gate_fr_arb01:fr_fwd_trade_symbols:gate-margin_gate-futures`
  - `gate_fr_arb01:fr_bwd_trade_symbols:gate-margin_gate-futures`
  - `gate_fr_arb01:fr_unimmr_close_symbols:gate-margin_gate-futures`
- Per-symbol max_u override string JSON:
  - `gate_fr_arb01:gate-margin:gate-futures:max_pos_u_overrides`

For another Gate FR env, replace the env prefix while keeping the same venue names unless the env explicitly differs.

## Gate Fields

Read positions from `GET /api/v4/futures/usdt/positions/{CONTRACT}` after sourcing the env's `env.sh`.

Important fields:

- `leverage=0` means Gate cross margin; use `cross_leverage_limit` as the effective cross leverage setting.
- `risk_limit` is the current exchange risk-limit notional for that position tier.
- `leverage_max` is the exchange-returned maximum leverage field for the current state.
- `value` is current position value. Compare `abs(Decimal(value))` to `Decimal(risk_limit) - buffer`.

Normalize symbols like `ZEREBROUSDT` to Gate contracts like `ZEREBRO_USDT`.

## Workflow

1. Check repo state only if files will be edited. For pure ops, avoid repo edits.
2. Source `/home/ubuntu/<env>/env.sh` for `GATE_API_KEY`, `GATE_API_SECRET`, and Redis settings.
3. Read Redis online symbols and compare them with the user's requested symbol list. Report missing and extra symbols.
4. Read the existing JSON at `<env>:gate-margin:gate-futures:max_pos_u_overrides`.
5. For each requested symbol, query the Gate futures position. Build an audit table with symbol, contract, status, margin mode, `cross_leverage_limit`, `leverage_max`, `risk_limit`, exchange cap (`risk_limit - buffer`), final max_u (`min(exchange cap, base_max_u)` when a base cap is set), size, value, and current override.
6. Skip any symbol with a non-2xx Gate position response. If the contract is suspicious, optionally check the public contracts list read-only to find possible naming mismatches, but do not guess a mapping for live writes.
7. For every valid symbol where `abs(value) <= risk_limit - buffer`, leave leverage unchanged and plan a Redis max_u override equal to `min(risk_limit - buffer, base_max_u)` if a base cap is provided, otherwise `risk_limit - buffer`.
8. For every valid symbol where `abs(value) > risk_limit - buffer`, lower Gate cross leverage one step at a time. Send `POST /futures/usdt/positions/{CONTRACT}/leverage?leverage=0&cross_leverage_limit=<target>` with decreasing integer targets until `abs(value) <= new risk_limit - buffer` or target reaches 1.
9. After leverage changes, write one merged JSON object to the max_u override Redis key, preserving existing entries outside the processed scope unless the user explicitly wants replacement. For processed valid symbols, set `SYMBOLUSDT` to the final max_u formula above.
10. Report symbols where `abs(value) > final max_u` after writing. This usually means the base max_u ceiling is below the current position value; the new cap blocks expansion but does not reduce the existing position.
11. Verify by reading the Redis override key and dry-running or GET-reading the changed Gate positions.

## Existing Helpers

Useful repo scripts:

- `scripts/set_online_futures_leverage.py`: loads FR online symbols from Redis and contains Gate signing helpers.
- `scripts/gate_set_futures_leverage.py`: dry-run prints Gate position fields including `lev`, `cross_limit`, `risk_limit`, `lev_max`, `size`, and `value`; `--execute` mutates leverage.
- `scripts/print_fr_max_pos_u.py`: read-only print of FR per-symbol `max_pos_u_overrides`.
- `scripts/arb_per_symbol_overrides.py`: documents and normalizes the JSON override storage model.

Prefer reusing the helpers or their signing/key conventions. For a one-off batch, a short inline Python script is acceptable if it uses `Decimal`, preserves existing JSON entries outside scope, and prints a complete before/after summary.

## Command Patterns

Read-only Redis override verification:

```bash
cd /home/ubuntu/<env>
python3 /home/ubuntu/crypto_mkt/mkt_signal/scripts/print_fr_max_pos_u.py \
  --env-name <env> \
  --open-venue gate-margin \
  --hedge-venue gate-futures
```

Read-only Gate field verification:

```bash
set -a
source /home/ubuntu/<env>/env.sh
set +a
python3 /home/ubuntu/crypto_mkt/mkt_signal/scripts/gate_set_futures_leverage.py \
  --contracts AIN_USDT,HNT_USDT,ZEREBRO_USDT \
  --leverage 5 \
  --settle usdt
```

The second command is read-only only when `--execute` is absent.
