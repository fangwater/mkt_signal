# Native CTP account-target ctaker

The default remains `NoStrategy`. An explicit `[ctaker]` in the Trader TOML,
`--market-data-config`, recovered/settled account, ready MD and `--execute` are
all required for target execution. Existing read-only launches stay read-only.
No live environment is enabled by adding this implementation.

CTP settings require a stable `source_id`, `quote_max_age_ms` (1–5000),
`target_max_age_ms` (1–60000) and `retry_interval_ms` (100–60000).
The Manager's signed net target is authoritative. CTP target execution derives each
order from that target, factual inventory and exchange-reported contract size
rules. Remove `risk.max_order_lots`, `risk.max_position_lots_per_side` and
`risk.max_working_lots` from the CTP configuration when enabling `[ctaker]`.
`risk.enable_fak_fok` must be enabled for the venue/account.
An optional `contracts` list restricts which native contracts this instance may
trade. The full account vector may include zero entries for other flat contracts.

The local private JSON-line command channel accepts:

```json
{"op":"ctaker_target","target":{"source_id":"explicit-source","batch_id":"manager-batch","target_revision":17,"trading_day":"20261117","expires_at_us":1794877230000000,"net_targets":{"SHFE:cu2612":1}}}
```

This is an expired documentation example, not a live order instruction. The
vector must contain every configured native contract, including explicit zeros.
Contracts retain their exchange, case and delivery month. Another source/day,
unknown contracts, missing zero entries, expired/long leases and
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
If an incoming callback closes the CTP gate before any reservation or SDK
call, the journal and trader log record `ctp_strategy_dispatch_blocked` with
the action's batch, quote generation, gate values, and retry status. A
callback backlog defers the action to a subsequent fresh tick within the
original target lease when the account, market feed, and journal are healthy.
No automatic order cancellation or new target restoration runs on restart.

Every child `prepared_submit` audit includes its exact native key plus the
frozen `ctaker_target` (source/batch/revision/day/lease/vector) before calling the
SDK. The reliable bounded writer queue and existing asynchronous persistence
remain unchanged; queue/writer failure blocks submission. This does not add a
synchronous disk wait. Abnormal termination may lose unflushed audit records;
recovery must block unresolved outcomes. Local SDK acceptance is never reported
as a factual venue fill. No maker/taker fee classification is invented.

## Manager delivery

The CTP Exec I/O worker opens an authenticated, source-scoped gRPC
`ManagerPlans.Watch` subscription. Manager checks the current plan on connect
and after each committed batch or attribution update. While an active plan
awaits current account evidence, Manager retries within that plan's original
lease. An idle subscription waits for changes.

The I/O worker queues new plans for admission inside the existing Exec loop.
It retries a pending plan while its original lease is valid and deduplicates
admitted plans across reconnects. The `ctaker_target_accepted` audit records
admission by Exec. Factual fills and exchange outcomes come from the native
order journal. The JSON `ctaker-plan` GET remains available for diagnostics.
The CTP Trader's `idle_sleep_us` controls the fixed wait after an empty loop
iteration. Zero keeps the loop spinning.

With `--read-only`, the Manager-only ingress can subscribe without a
`[ctaker]` section. It reads the current account target, including a flat
or previously recorded batch, and logs `ctaker_target_observed` inside the
business loop. Observations have a zero execution lease. Executable
subscriptions require a live lease and the existing account and market gates.

Each subscription update includes the batch identity and a machine-readable
plan status. The execution subscriber queues `ready` updates for admission.
Status transitions are emitted as `manager_plan_status` JSON log events.
Idle and read-only statuses use `info`. Blocking statuses such as
`lease_expired` or `account_snapshot_stale` use `warn`. Connection or
protocol errors emit `manager_plan_transport_error`. Deploy the Manager
that emits plan statuses before upgrading its Exec subscribers.

For an account configured to read Manager plans, use the matching
`[ctaker].source_id`, and set `target_max_age_ms = 60000` to admit the full
Manager lease promptly. Add the target reader to the Trader TOML.

```toml
[position_target_ingress]
source_id = "ctp_live01"

[position_target_ingress.manager_plan]
grpc_url = "http://127.0.0.1:18202"
token_env = "CN_CTA_CTP_LIVE01_PLAN_TOKEN"
```

Configure the same account and environment credential in Manager's
`[ctaker_plan_readers]`. Enable `publication.include_trading_day` in the
signal publisher so the batch carries its native trading day. Manager serves
gRPC on the same loopback port as the web API. For cross-host access, forward
only the subscription route from the gateway's private HTTP/2 listener to
Manager's loopback port. For example, in the gateway's private Nginx server
block with HTTP/2 enabled:

```nginx
location = /cn_futures_target.ManagerPlans/Watch {
    grpc_pass grpc://127.0.0.1:18202;
    grpc_read_timeout 1h;
}
```

Set `grpc_url` to the gateway's origin. An `http://` origin uses plaintext
HTTP/2 and `https://` uses TLS. Both carry the source-scoped plan token.

The CTP loop publishes
settled position snapshots every five seconds while the Manager reader is
enabled. The existing journal worker serializes and persists them, and Manager
reads them through its local secondary or the configured replica.

Private Unix socket IPC and mTLS gRPC remain available as Exec target inputs
using the same admission queue. Manager's plan endpoint currently supports
configured CTP sources.

Compilation may run on the development host. All CTP code/tests, including
SDK-free/FakeTrader tests, run only on the designated external `sh_dev` host.
The initial implementation passed 166 native library tests there (one existing
helper ignored), including native FIFO/offset/uncertain-outcome regressions,
ctaker target gates and actual RocksDB audit persistence. No real order was
submitted by those tests. Both official CTP/Yida feature sets compile locally.
