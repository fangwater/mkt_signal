# Exec Chase

`chase_exec` is a standalone live execution strategy that runs alongside
`batch_exec` inside `exec-pre-trade`. Each release places one level-0
post-only order that follows the same-side best price through in-place
amendments, and exposure is released by fill water level rather than a batch
timer. It reuses the existing account risk checks, Manager tradability cache,
exec order rate limiter, order queries, orphan reconciliation, uniform
persistence, and position-ledger infrastructure.

## Configuration

Strategy values live in Redis under `chase_exec:<strategy_name>`; the
namespace is fully independent from `batch_exec:*`.

The Exec Config API selects this namespace with
`execution_family: "chase_exec"` in POST bodies or the matching query
parameter for GET/DELETE. Requests without that field continue to address
`batch_exec`.

Chase is available only for `binance-futures` and `okex-futures`. Both native
exchange backends and the RapidX/LTP backend use in-place amend. Binance
COIN-M is deliberately rejected because this execution path has no supported
modify contract for it.

Before reloading an existing strategy, remove the legacy
`maker_price_anchor` field from its Redis JSON. Chase now fixes this behavior
internally, and strict config parsing rejects the removed field.

```json
{
  "single_order_usdt": 100.0,
  "max_open_usdt": 200.0,
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
| `maker_recenter_trigger_bps` | Own-best movement (bps of the previous anchor) required before a live child is amended. `0` amends whenever the aligned own-best price actually changes. |
| `maker_amend_cooldown_ms` | Per-child minimum delay between amend requests. |
| `maker_timeout_ms` | Per-child maker lifetime; on expiry the confirmed-unfilled remainder escalates to taker. |
| `target_tolerance_usdt` | Stop once the remaining gap is within this notional tolerance. |
| `bbo_max_age_ms` | Maximum BBO age for releases and recentering; stale quotes pause activity. |

Manager stores Chase as its own order-strategy template type and publishes the
strict Chase payload above directly into `chase_exec:*`. Symbol-level template
overrides must stay in the same execution family. A live binding may move
between Batch/POV and Chase through `exec_switch:*`: the source strategy first
freezes its target, cancels all working children, waits for cancellation and
late-fill reconciliation, and publishes its exact per-symbol net allocation.
The destination imports that allocation before it becomes executable. Once the
destination confirms the allocation, the source ledger entry and old config are
removed. This changes ledger ownership without flattening the exchange
position. Other strategies may not keep the same account-symbol in the opposite
family because the two family ledgers still require exclusive symbol ownership.

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
- Each maker child is a level-0 post-only order fixed to the same-side best.
  When that own-best anchor moves by at least
  `maker_recenter_trigger_bps`, the child is amended in place
  (Binance `order.modify`, OKX `amend-order`, RapidX `replace_order`) with one
  amend in flight per child. Amends consume the same exec order rate limit as
  new orders. RapidX modify checks are capped at the venue contract of 300
  requests per minute even when the general Exec rate limit is disabled or
  configured higher. Native OKX amend requests also enforce the venue's
  per-instrument limit of 60 requests per 2 seconds.
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
`chase_exec_state:leverage_initialized`, plus the shared switch state
`exec_switch:strategy_names` / `exec_switch:<strategy_name>`. The reloader shares
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
