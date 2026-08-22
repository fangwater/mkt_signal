# Gate FR Risk Limit Alignment 2026-06-28

This note records the live risk-limit alignment performed for `gate_fr_arb01`
on `jp-meta-elvpn`. It intentionally avoids credentials and account-specific secrets.

## Scope

- Environment: `gate_fr_arb01`
- Host access: `ssh jp-meta-elvpn`
- Gate settle currency: `usdt`
- Symbol scope: union of the Gate FR online Redis symbol lists
- Buffer: `2000` USDT
- Base `max_u` ceiling: `98000`
- Redis override key:
  `gate_fr_arb01:gate-margin:gate-futures:max_pos_u_overrides`

Treat Gate leverage updates and Redis override writes as live trading changes.

## Formula

For each valid Gate futures contract:

```text
exchange_cap = final_risk_limit - 2000
max_u = min(exchange_cap, 98000)
```

Only lower Gate cross leverage when:

```text
abs(position.value) > risk_limit - 2000
```

Lower cross leverage one integer step at a time, then immediately re-read the
Gate position. Stop as soon as the new `risk_limit - 2000` covers the current
position value.

Do not write a Redis override for a symbol whose Gate futures position endpoint
returns `CONTRACT_NOT_FOUND` or another non-2xx response.

## Initial Audit

Online Redis symbols: 33.

Gate position query results:

```text
valid = 32
failed = 1
exceeded_exchange_cap = 3
```

The failed symbol was:

```text
ANDUSDT contract=AND_USDT status=400 label=CONTRACT_NOT_FOUND
```

Symbols exceeding `risk_limit - 2000` before the fix:

```text
ZEREBROUSDT  value=62855.436   risk_limit=50000  cap=48000  over_by=14855.436
HNTUSDT      value=20206.0794  risk_limit=20000  cap=18000  over_by=2206.0794
AINUSDT      value=19989.229   risk_limit=20000  cap=18000  over_by=1989.229
```

## Live Changes

Each exceeded symbol only needed one cross-leverage step down:

```text
AINUSDT      cross 3 -> 2   risk_limit 20000 -> 50000    cap=48000
HNTUSDT      cross 5 -> 4   risk_limit 20000 -> 30000    cap=28000
ZEREBROUSDT  cross 4 -> 3   risk_limit 50000 -> 100000   cap=98000
```

Redis override changes:

```text
HNTUSDT   38000 -> 28000

Added:
ARUSDT      98000
CCUSDT      98000
CHZUSDT     98000
GRASSUSDT   98000
SUSDT       98000
TSLAXUSDT   98000
```

`ANDUSDT` was skipped and no override was written because Gate returned
`CONTRACT_NOT_FOUND`.

## Verification

Final verification after the leverage changes and Redis write:

```text
leverage_changed = 3
leverage_failures = 0
skipped = 1
above_final_max_u = 0
still_over_exchange_cap = 0
```

This means all valid online Gate FR contracts were within both the exchange
buffer cap and the final Redis `max_u` cap after the fix.
