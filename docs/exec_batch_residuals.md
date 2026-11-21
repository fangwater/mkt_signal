# Exec batch residual settlement and warning limits

Last updated: 2026-11-15 UTC.

Batch completion uses the complete target minus allocated-position gap. An idle
batch without children or orphan orders can settle as target reached, target
notional tolerance, or below the exchange lot minimum. Release unsent reservations
without changing factual positions, targets, order limits or fees. Keep execution
and query reconciliation active while any child, orphan or batch order identity
still lacks terminal evidence, including a fully accounted fill awaiting its final
acknowledgement. Zero displayed live/pending quantity alone is insufficient proof
that an order can be discarded. The existing idle-settlement fix is `cc742676`.

A sub-minimum unallocated remainder can coexist with a live/cancelling batch.
Such a batch must keep its scheduler awake. Its next-batch deadline therefore
cannot also suppress repeated warnings: using `next_batch_at_us != i64::MAX`
logs once on every check, observed around 48 times/second on el01 trade07.

The minimum-quantity warning has independent per-strategy in-memory state. Emit
its first warning, then at most one summary per 60 seconds with a `suppressed`
count. New targets and changing retry deadlines cannot bypass this budget.
A backwards clock change reanchors the interval. The log budget never changes
completion reasons, reservations, retries, quantities, trading parameters or
terminal-order reconciliation. Other strategies retain independent warnings.

Validation: 86 BatchExec tests and all 1,578 runnable mkt_signal library tests
passed, with 3 existing ignored tests. New regression coverage exercises a 50 Hz
reconciliation loop, summary counts, independent strategies and clock changes;
existing tests retain unknown-order safeguards and verify idle dust settlement,
full target gaps, target transitions and eventual orphan terminal fills.

The el01 incident affects `/home/el01/binance_exec_trade07` (dzy_group0807), native
Binance USD-M. Manager's separate monitor residual calculation is fixed in
`crypto_cta_manager` commit `556b596`. Full release/backup/verification evidence
is recorded independently in that project's `docs/el01_dzy_btc_monitor_20261115.md`.
Only explicitly selected environments receive this replacement; other deployed
accounts and venues retain their existing artifacts.
