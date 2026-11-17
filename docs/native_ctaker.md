# Native CTP account-target ctaker

The default remains `NoStrategy`. An explicit `[ctaker]` in the Trader TOML,
`--market-data-config`, recovered/settled account, ready MD and `--execute` are
all required for target execution. Existing read-only launches stay read-only.
No live environment is enabled by adding this implementation.

Settings require an explicit stable `source_id`, `max_target_lots`,
`max_order_lots`, `quote_max_age_ms` (1–5000), `target_max_age_ms` (1–60000) and
`retry_interval_ms` (100–60000). Lot limits cannot exceed the existing risk
limits. `risk.enable_fak_fok` must already be validated for that venue/account.
Do not copy these controls into a live TOML without choosing account limits.

The local private JSON-line command channel accepts:

```json
{"op":"ctaker_target","target":{"source_id":"explicit-source","batch_id":"manager-batch","target_revision":17,"trading_day":"20261117","expires_at_us":1794877230000000,"net_targets":{"SHFE:cu2612":1}}}
```

This is an expired documentation example, not a live order instruction. The
vector must contain every configured native contract, including explicit zeros.
Contracts retain their exchange, case and delivery month. Another source/day,
unknown contracts, missing zero entries, excess lots, expired/long leases and
old/conflicting revisions are rejected. An identical live batch is idempotent;
a reconnect or rejection requires a new batch/revision. New batches cannot
replace unresolved native orders. Manual submits are blocked in ctaker mode;
operator cancel/recovery remains subject to the existing native core gates.

At each current-generation fresh tick, ctaker compares the signed target with
factual gross long minus short. It first closes available opposite inventory,
then opens only after factual fills have changed the account. Hedged holdings
already at their net target remain hedged. Native core planning chooses exact
close-today/close-yesterday children and retains risk, budget and market-status
ownership. A filled order report without its trade facts remains unresolved.

Orders are integer-lot **limit/FAK at the opposite BBO**. This does not assume a
domestic native market-order capability. Crossed, missing, off-tick, out-of-limit,
wrong-day, future/stale or foreign-generation quotes produce no action. FAK
cancellation does not imply a fill; subsequent attempts wait for resolved
orders and the configured interval. Rejections/uncertain submissions halt
this target, with unknown reservations left under the native core's control.
No automatic order cancellation or new target restoration runs on restart.

Every child `prepared_submit` audit includes its exact native key plus the
frozen `ctaker_target` (source/batch/revision/day/lease/vector) before calling the
SDK. The reliable bounded writer queue and existing asynchronous persistence
remain unchanged; queue/writer failure blocks submission. This does not add a
synchronous disk wait. Abnormal termination may lose unflushed audit records;
recovery must block unresolved outcomes. Local SDK acceptance is never reported
as a factual venue fill. No maker/taker fee classification is invented.

The strategy hooks do bounded CPU work only. There is no HTTP, disk access or
lock inside ctaker. The private command channel is an operator/Exec ingress;
it is **not yet** an authenticated remote Manager execution transport. Manager
netting plans require a separate reviewed delivery adapter and exact native
order/batch evidence joins before automated remote publication/fill allocation.
Do not reuse the read-only sync token as execution authorization.

Compilation may run on the development host. All CTP code/tests, including
SDK-free/FakeTrader tests, run only on the designated external `sh_dev` host.
The initial implementation passed 166 native library tests there (one existing
helper ignored), including native FIFO/offset/uncertain-outcome regressions,
ctaker target gates and actual RocksDB audit persistence. No real order was
submitted by those tests. Both official CTP/Yida feature sets compile locally.
