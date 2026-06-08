# GENIUSUSDT Binance FR Arb02 Hedge Pending Incident (2026-06-06)

## Scope

This note records the investigation and fix for the `GENIUSUSDT` close/hedge mismatch in
`binance_fr_arb02`.

Operator-reported time:

- `2026-06-06 03:14` Beijing time
- `2026-06-05 19:14` UTC in logs

Primary logs inspected:

- `/home/ubuntu/.pmdaemon/logs/fr_pt_bn_arb02-error.log-20260606`
- `/home/ubuntu/.pmdaemon/logs/fr_te_bn_arb02-error.log-20260606`
- `/home/ubuntu/.pmdaemon/logs/fr_am_bn_arb02-error.log-20260606`
- `/home/ubuntu/.pmdaemon/logs/fr_pt_bn_arb02-out.log-20260606`

No live-impact commands were run during the investigation.

## Impact

Before the close flow, GENIUS was nearly aligned:

```text
spot/margin: +8434.7731 GENIUS
futures:     -8469 GENIUS
net:         -34.2269 GENIUS
```

After the close/hedge sequence around `2026-06-05 19:14 UTC`, the account monitor and exposure
table showed:

```text
spot/margin: +231.0731 GENIUS
futures:     +4840 GENIUS
net:         +5071.0731 GENIUS
```

At roughly `0.426` to `0.430` USDT per GENIUS, this was about `2160` to `2180` USDT of unintended
long exposure.

## Timeline

### Initial state

At `2026-06-05 19:02:08 UTC`, ArbHedge initialized with:

```text
open_pos=8434.77310000
hedge_pos=-8469.00000000
net=-34.22690000
hedge_work_baseline=-34.22690000
```

### Spot close begins

From `19:02:08 UTC`, ArbClose created many Binance Margin `Sell` orders in `231` to `232` GENIUS
chunks. These fills reduced spot/margin inventory from `8434.7731` to `231.0731`.

Each filled close sell produced negative hedge work, so ArbHedge correctly started creating
Binance Futures `Buy` orders to reduce the original futures short.

### Dispatch/query delay

For several hedge orders, local pre_trade considered the order sent, but trade_engine had not yet
processed the new-order request.

Representative examples:

| client_order_id | pre_trade created | Binance query error | orphan handoff | trade_engine async recv delay |
| --- | --- | --- | --- | --- |
| `164254573194641421` | `19:03:06` | `19:03:15`, `-2013 Order does not exist` | `19:03:18` | `15.995s` |
| `164254573194641422` | `19:03:07` | `19:03:17`, `-2013 Order does not exist` | `19:03:19` | `16.369s` |
| `164254573194641423` | `19:03:09` | `19:03:21`, `-2013 Order does not exist` | `19:03:21` | `15.609s` |

Cancel requests in the same window also showed queue delays around `21s` to `28s` and repeated
`-2011 Unknown order sent`.

## Root Cause

The main bug was not simply that orphan released pending. The pending quantity became too large
because orphan handoff removed borrowed hedge work from the ArbHedge outstanding calculation.

ArbHedge computes new pending hedge work from open fills with:

```text
outstanding_before = pending_hedge_queue.net_qty() + borrowed_hedge_qv()
target             = net_qty_queue.net_qty() - hedge_work_baseline
hedge_work_delta   = target - outstanding_before
pending           += hedge_work_delta
```

When a live hedge order was handed to the orphan strategy, ArbHedge removed its `hedge_order_meta`.
That meta was the only place where the order's borrowed quantity was counted. After removal,
`borrowed_hedge_qv()` stopped including the still-live orphaned hedge order.

As a result, later open fills saw an understated `outstanding_before` and recreated pending for
hedge work that had already been borrowed by orphaned hedge orders.

Concrete example from the GENIUS logs:

```text
2026-06-05T19:03:27Z opening Sell filled=231
outstanding_before=-2553
net=-5369.2269
baseline=-34.2269
target=-5335
hedge_work_delta=-2782
```

Only `231` came from this new close fill. The extra roughly `2551` came from already-borrowed hedge
work that had disappeared from `borrowed_hedge_qv()` after orphan handoff.

Another later example:

```text
2026-06-05T19:04:14Z opening Sell filled=231
outstanding_before=-4628
target=-8107
hedge_work_delta=-3479
```

Again, a `231` close fill generated far more pending because orphaned borrowed work was missing
from outstanding.

There was a second related issue at orphan terminal time. The orphan terminal path called
`record_hedge_order_terminal` without the original hedge client order id or original borrowed
metadata, so ArbHedge reconstructed `borrowed_qv` from order side/quantity and used
`bound_open_client_order_id=0`. This turned the release into an unbound pending lot rather than
returning unfilled quantity to the original open-bound lot.

## Fix

The fix keeps orphaned hedge order accounting in ArbHedge until the orphan terminal event closes it.

Code changes:

- Added `orphaned_hedge_order_meta` to `ArbHedgeStrategy`.
- On successful hedge orphan handoff, move metadata from `hedge_order_meta` to
  `orphaned_hedge_order_meta` instead of dropping it.
- Include both live and orphaned hedge metadata in `borrowed_hedge_qv()`, so open-side pending
  deltas remain bounded by actual open work.
- Extended orphan terminal recording to pass the hedge `client_order_id` back to the terminal
  recorder.
- ArbHedge terminal recording now uses original `borrowed_qv`, `order_base_qty`, and
  `bound_open_client_order_id` from `orphaned_hedge_order_meta` when available.

Focused tests added:

- `orphan_handoff_keeps_borrowed_work_in_outstanding`
- `orphan_terminal_uses_original_borrowed_meta_and_bound_open_id`

These tests cover the two failure modes:

- A new open fill after handoff only creates pending for the new open fill, not for already-borrowed
  orphaned hedge work.
- An orphan terminal release uses the original borrowed quantity and returns unfilled pending to the
  original open id.

## Follow-Up Checks

Useful follow-up checks before live deployment:

- Run the focused ArbHedge tests.
- Run `cargo check --bin pre_trade`.
- Monitor `ArbHedgeRecord` for `hedge_work_delta` much larger than `filled_base_qty` after orphan
  handoff. A large delta can be valid only if accumulated outstanding really changed; it should no
  longer be caused by orphaned borrowed work disappearing.
- Monitor trade_engine ingress latency. This bug is fixed at the accounting layer, but the incident
  was triggered by `15s+` new-order dispatch delays and `20s+` cancel dispatch delays.
