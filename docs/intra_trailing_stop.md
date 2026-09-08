# Intra Per-Symbol Protective Taker

The intra Config Server provides a Take Profit / Trailing Stop JSON editor.
It stores a Redis String at
`<env>:<open_venue>:<hedge_venue>:intra_trailing_stop_overrides`:

```json
{"BTCUSDT":{"take_profit":0.005,"reward_risk_ratio":2}}
```

Only these two numeric fields are accepted. Both must be positive, and both
`take_profit` and `take_profit / reward_risk_ratio` must be below 1. Missing
symbols, a missing key, and `{}` disable protective triggers. This configuration
reloads with the pre-trade risk parameters every 60 seconds. Invalid JSON or
parameters reject the refresh, preserving the previous configuration.

Each maker client order ID has an independent in-memory position in
`IntraTrailingBook.positions`. Opening fill deltas update its remaining quantity
and weighted entry price. No account-average entry price is substituted. The
configured symbol also enables incremental maker-fill recording, even when
`ARB_OPEN_PARTIAL_HEDGE` is off, so protection need not await maker terminal.
The initial stop distance is `take_profit / reward_risk_ratio`, excluding fees.
Longs use the opening venue's bid; shorts use its ask. The fixed trailing rule
advances one level for every 0.1% favorable movement from entry, moving the stop
by 0.05% of entry per level. The stop only tightens. Fixed take profit also
triggers a taker hedge on the configured hedge venue.

The existing pre-trade period driver checks the latest BBO on its 20ms cycle
(subject to reactor scheduling). Quotes older than one second do not create new
triggers. This is sampled BBO protection, not an exchange-native stop order.
Once triggered, an exit stays latched and retries its available remainder even
if the quote subsequently recovers or becomes stale. Removing the symbol clears
the protective state; submitted orders still complete normal reconciliation.
Changing TP/RR applies to existing positions without loosening an existing stop.

Model takers reserve all currently eligible pending hedge quantity. Protective
takers reserve only the triggering open ID and can bypass its scheduled hedge
time. Every intra hedge order retains an ordered allocation list. Cumulative
fills debit those allocations once; partial cancellation or rejection restores
each unfilled component to its original ID and entry price. Orphan handoff and
Gate order retry retain the allocation list. Reserved quantities cannot be
hedged again by either trigger. Venue quantity/minimum rules and existing risk
blocks still apply; a small residual is never rounded up into extra exposure.

Position tracking also runs for fills registered with the hedge strategy while
the symbol is disabled, allowing later enabling for those fills. With incremental
maker recording off, such disabled symbols register fills at order terminal.
As with the existing pending
hedge state, this map is not persisted across pre-trade restarts. Startup account
balances do not supply factual per-order entries or historical trailing levels,
so this change does not infer or reconstruct stops for pre-restart holdings.
