# Exec Chase

`chase_exec` is a standalone live execution strategy that runs alongside
`batch_exec` inside `exec-pre-trade`. Each release places one level-0
post-only order that follows an opposite-side price anchor through in-place
amendments, and exposure is released by fill water level rather than a batch
timer. It reuses the existing account risk checks, Manager tradability cache,
exec order rate limiter, order queries, orphan reconciliation, uniform
persistence, and position-ledger infrastructure.

## Configuration

Strategy values live in Redis under `chase_exec:<strategy_name>`; the
namespace is fully independent from `batch_exec:*`.

```json
{
  "single_order_usdt": 100.0,
  "max_open_usdt": 200.0,
  "maker_price_anchor": "opposite_best_plus_one_tick",
  "maker_recenter_trigger_bps": 3.0,
  "maker_amend_cooldown_ms": 0,
  "maker_timeout_ms": 60000,
  "target_tolerance_usdt": 10.0,
  "bbo_max_age_ms": 2000,
  "targets": {"BTCUSDT": {"qty": 0.1, "signal": 0}},
  "symbol_overrides": {"ETHUSDT": {"single_order_usdt": 250.0}}
}
```

| Parameter | Meaning |
| --- | --- |
| `single_order_usdt` | Maximum notional released per child order. |
| `max_open_usdt` | Maximum unfilled maker exposure open at any time. |
| `maker_price_anchor` | `own_best` or `opposite_best_plus_one_tick`; the level-0 post-only price each maker child chases. |
| `maker_recenter_trigger_bps` | Anchor movement (bps of the previous anchor) required before a live child is amended. `0` amends whenever the aligned level-0 price actually changes. |
| `maker_amend_cooldown_ms` | Per-child minimum delay between amend requests. |
| `maker_timeout_ms` | Per-child maker lifetime; on expiry the confirmed-unfilled remainder escalates to taker. |
| `target_tolerance_usdt` | Stop once the remaining gap is within this notional tolerance. |
| `bbo_max_age_ms` | Maximum BBO age for releases and recentering; stale quotes pause activity. |

Targets share the `batch_exec` schema: a bare base quantity or
`{"qty": .., "signal": ..}` with `signal` in `[-2, -1, 0, 1, 2]`.
`signal.abs() == 1` selects one-shot taker execution of the remaining gap.
A target of `0` flattens the allocated position, which is how the internal
`SYSTEM_POSITION_CLOSE` strategy works. Symbol overrides replace at least
one field and are validated against the defaults.

## Execution Semantics

- `remaining = target - position`, `uncommitted = remaining - open_unfilled`.
  Maker clips release at most one per clock pass while unfilled maker
  exposure stays below `max_open_usdt`; fills are what re-open the release
  budget. Taker obligations drain before any maker release.
- Each maker child is a level-0 post-only order priced from the configured
  anchor. When the opposite-side anchor moves by at least
  `maker_recenter_trigger_bps`, the child is amended in place
  (`order.modify`) with one amend in flight per child. Amends consume the
  same exec order rate limit as new orders.
- Post-only rejections repost at the fresh BBO without backoff; deterministic
  open rejections apply backoff through `submit_blocked_until_us`. GTX
  cross-cancels and unexpected exchange cancels return the remainder to the
  uncommitted water level and repost as maker on the next pass.
- On `maker_timeout_ms` expiry the child is cancelled and its confirmed
  unfilled remainder becomes a taker obligation that drains as market orders.
- Target cancels, stale-generation remainders, and cancels issued because the
  target no longer needs the committed side are dropped back to the position
  ledger rather than escalated.
- Taker obligations that cannot execute — escalated remainders that floor
  below the venue minimum, or overhang beyond the uncommitted gap — are
  released back to the water level instead of pinning the strategy in
  flight. A gap that ends below the venue minimum completes as
  `exchange_minimum`; within `target_tolerance_usdt` it completes as
  `target_tolerance`.
- Cancels whose publish was never confirmed are retried on every clock pass
  (`cancel_for_target` or `maker_expired` children with no `cancel_requested`
  acknowledgement).

## Position Ledger And Lifecycle

Redis keys: `chase_exec:strategy_names`,
`chase_exec:removed_strategy_names`,
`chase_exec_state:position_allocations`,
`chase_exec_state:leverage_initialized`. The reloader shares
`batch_exec_pubs/reload_notify` with the batch reloader; iceoryx2 gives each
subscriber its own copy, so the two reloaders do not consume each other's
notifications.

- Targets activate only after the per-symbol leverage-init audit confirms the
  configured leverage and the strategy's position allocation is applied.
  Leverage initialization failures block execution and retry each reload.
- The account position is distributed across same-symbol strategies;
  residual account position is assigned to a `SYSTEM_POSITION_CLOSE`
  strategy that flattens it. Untradable symbols reconcile their ledger to
  the account position instead of creating an unexecutable close loop.
- Opposite unexecuted target gaps on the same symbol are netted internally:
  proportional legs book synthetic fills at the current mid (fresh quote
  required), which moves ledger positions without touching the shared
  account position.
- Removing a strategy pauses and cancels all chase strategies on the venue,
  reallocates the removed strategy's position, then deletes the strategy and
  its ledger entry.

## Batch/Chase Coexistence Rule

Both exec families' position ledgers assume exclusive ownership of the
shared account position on a symbol. Running `batch_exec` and `chase_exec`
strategies on the same venue+symbol+account would allocate the same
physical position twice. Each reloader therefore detects the other family's
live strategies and skips position reconcile, internal cross, and
position-close creation for any conflicted symbol, logging a warning on the
transition. Conflicted chase strategies stay unallocated and do not trade.
Do not configure both exec families on the same symbol; there is also no
cross-family internal netting, so opposite targets in different families
would trade externally against each other.

## Observation

Chase strategies publish into the same exec state IPC/JSON rows with
`algorithm: "chase_exec"`. `pending_qty` reports the escalated taker
obligation, `live_order_qty`/`active_batches` report signed unfilled and live
child counts, `completion_reason` is one of `target_reached`,
`target_tolerance`, `exchange_minimum`, or `symbol_not_tradable`.
`remaining_batches` and `estimated_completion_ts_ms` are `0` (fill-driven
release has no schedule to project).

## Verification

Run `cargo check -p mkt_signal --bin exec-pre-trade`,
`cargo test -p mkt_signal --lib`, `cargo test -p viz_common --all-targets`, and
`python3 -m unittest scripts.tests.test_exec_config_server`.
On this host bindgen may need
`BINDGEN_EXTRA_CLANG_ARGS=-I/usr/lib/gcc/x86_64-linux-gnu/13/include`.
No production deployment or exchange order is required for these checks.
