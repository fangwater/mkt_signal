# Native strategy-target Exec

The default remains `NoStrategy`. An explicit `[exec]` in the Trader TOML,
`--market-data-config`, recovered/settled account, ready MD and `--execute` are
all required for target execution. Existing read-only launches stay read-only.
No live environment is enabled by adding this implementation.

CTP settings require a stable `source_id`, `quote_max_age_ms` (1–5000) and
`retry_interval_ms` (100–60000). The optional
`algorithm` (with its parameters) selects how each contract's net gap is
worked (see [Execution algorithms](#execution-algorithms)); it defaults to
`"ctaker"`. The Yida `exec` section accepts the same keys.
Like crypto pre_trade, Exec owns the strategy ledger. Manager delivers every
registered strategy's complete signed target vector; Exec nets them per native
contract, crosses opposite strategy gaps internally, trades only the account
net and attributes its own fills (see [Strategy ledger](#strategy-ledger)).
Orders derive from the net target, factual inventory and exchange-reported
contract size rules. Remove `risk.max_order_lots`, `risk.max_position_lots_per_side` and
`risk.max_working_lots` from the CTP configuration when enabling `[exec]`.
`risk.enable_fak_fok` must be enabled for the venue/account.
There is no contract allowlist: Exec trades every contract of the target, and
every held contract the target omits (account net, working order or a named
strategy's attribution) at an implicit target of zero, like crypto
`SYSTEM_POSITION_CLOSE`. Such contracts are found at admission and by a
once-a-second refresh; an empty roster (every strategy retired in Manager) is a
valid target that flattens the account.
Admission never waits for contract rules or MD subscriptions; Exec requests them
at admission and every second for the standing target, and a contract whose
rules or quotes are still pending waits alone (`contract_rule_missing`).

The local private JSON-line command channel accepts:

```json
{"op":"exec_target","target":{"source_id":"explicit-source","batch_id":"manager-batch","target_revision":17,"strategies":{"cn-futures-01-a-1h":{"SHFE:cu2612":2},"cn-futures-01-b-1h":{"SHFE:cu2612":-1}}}}
```

This is a documentation example, not a live order instruction. Every
strategy carries the same complete native contract vector, including explicit
zeros; `__unallocated__` is reserved. Contracts retain their exchange, case and
delivery month. Another source, unknown contracts, missing zero entries,
mismatched strategy vectors and old/conflicting revisions are rejected.
A target has no lease and no trading day (2026-11-23): it stands until a newer
batch replaces it, across sessions, breaks, MD reconnects and order rejections,
and is executed whenever the account, MD and quote gates allow. An identical
batch is idempotent. Like crypto targets, a newer batch replaces the
current one at any time: in-flight FAK fills are attributed against the new
strategy gaps. Manual submits are blocked in exec mode; operator
cancel/recovery remains subject to the existing native core gates.

At each current-generation fresh tick, Exec first crosses opposite strategy
gaps for that contract at the quote mid, then compares the signed net target
with factual gross long minus short. It first closes available opposite inventory,
then opens only after factual fills have changed the account. Hedged holdings
already at their net target remain hedged. Native core planning chooses exact
close-today/close-yesterday children and retains risk, budget and market-status
ownership. A filled order report without its trade facts remains unresolved.

Orders are integer-lot limits; this does not assume a domestic native
market-order capability. Crossed, missing, off-tick, out-of-limit, wrong-day,
future/stale or foreign-generation quotes produce no action; wrong-day means a
quote whose trading day differs from the account's. FAK cancellation does not
imply a fill. A rejected or withheld order pauses only its contract until the
algorithm's `retry_interval_ms` has passed; other contracts keep trading.
An SDK insert or cancel with an unknown outcome leaves only that order
unconfirmed, with its reservations kept, so only its contract waits; the account
keeps trading. Its venue report settles it; otherwise, 3 s later, one background
`ReqQryOrder` does: a listed order takes the queried state (a listed cancel may
be retried) and an absent insert, which never reached the counter, is released
and retried by the algorithm (`unconfirmed_order_query` /
`unconfirmed_order_resolved` in the trader log). A complete reconnection snapshot
settles them the same way. Yida keeps blocking the account instead. Exec has no
local action-rate limit; the full rejection chain is logged as
`strategy_action_rejected`.

## Execution algorithms

Like crypto, Exec (the counterpart of pre_trade) sits above the execution
algorithms. Exec owns target admission, the strategy ledger, the gates above
and sizing: for the gap's side it computes the lots one order may carry
(closable inventory minus frozen, rule `max_lots`, contract minimum) and the
offset. The per-contract algorithm (`ctp::algo`) then decides price, time in
force and cancel/replace from the contract's live OMS orders, never from a
predicted position. Names and parameters follow the crypto
`ExecutionAlgorithm`:

| `algorithm` | Behaviour |
|---|---|
| `"ctaker"` (default) | Crosses the whole tradable gap at once like the crypto ctaker; domestic venues have no usable market order, so it is a limit/FAK at the opposite touch. Waits for every live order of the contract to resolve and for `retry_interval_ms` between attempts. |
| `"chase"` | Joins the near touch with one Day limit; cancels it when the target side or size changed; once the touch has moved and the order has rested `chase.maker_amend_cooldown_ms` (100–60000, default 500) cancels and re-places it (no native amend); after the gap has stayed open `chase.maker_timeout_ms` (cooldown–3600000, default 5000) cancels and crosses with FAK. |

```toml
[exec]
algorithm = "chase"
chase = { maker_amend_cooldown_ms = 500, maker_timeout_ms = 5000 }
```

Algorithms never touch orders this process did not place (manual orders or a
previous process's orders after restart); the contract waits with
`working_order_unresolved` until those resolve. A new target batch restarts
the chase maker clock and clears the retry interval. A step emits at most the
per-tick action budget; further cancels follow on later ticks. `grid`,
`batch` and `pov` fit the same interface (several `Place` commands per step
against the same live-order view).

Every child order is recorded as a uniform-order lifecycle record right after
the SDK call, in the same loop iteration, so evidence never delays the order;
orders of an exec target batch carry `from_key = exec:<batch_id>`, and the
complete strategy vectors are logged once by `exec_target_accepted`.
`PersistChannel` writes the record in its final crypto store format on the
business thread and publishes it to the separate `persist_manager` process,
which appends it unchanged (see [persistence](cn_order_persistence.md)); a
local publish failure blocks later trading. There is no synchronous disk wait.
Abnormal termination may lose unflushed records; recovery must block
unresolved outcomes. Local SDK acceptance is never reported as a factual venue
fill. No maker/taker fee classification is invented.

## Strategy ledger

The ledger holds signed lots per strategy and native contract. The account net
minus all named strategies is the implicit `__unallocated__` remainder, whose
target is always zero (crypto's `SYSTEM_POSITION_CLOSE`). Strategies that left
the roster but still hold lots are treated as target zero; their closing fills
stay attributed to them, so Manager's two-step retirement (stop, then delete
once the ledger is flat) needs no publisher cooperation. Locked inventory (long
and short at a flat net) stays hedged, as for target contracts.

- **Internal cross**: on a valid fresh book, opposite gaps are paired,
  `__unallocated__` first and then by strategy name, at the bid/ask mid. No
  order is sent; the remaining gaps all share the direction of `net target -
  account net`. Recorded as one internal-cross uniform order (`ttype = 2`) per
  leg, like crypto BatchExec.
- **Fill allocation**: each newly applied live fact of this process's order
  goes to strategies whose gap has the fill direction, by name; any excess
  stays `__unallocated__`. Fills of other sessions' orders or without a target
  for the contract go wholly to `__unallocated__`, as do fills first seen in a
  recovery query. Recorded as one uniform fill per strategy share
  (`from_key` = strategy) next to the fill's single trade update.

Both appear in the trader log. There is no ledger table: a strategy's
position is the signed sum of its uniform fill and cross records, and Exec
rebuilds its ledger from that log read-only at startup. A contract with
nonzero strategy targets or held attribution is subscribed even at a flat
net, because crossing needs its quote. At cutover an empty ledger makes the whole account
`__unallocated__`; crosses then move it to the strategies at the mid.

A legacy holding that already belongs to a strategy is credited at its own
cost before targets flow, with the operator command

```json
{"op":"assign_position","strategy":"cn-futures-01-a-1h","market":"SHFE:cu2612","lots":2,"price":98000.0}
```

`lots` is signed (positive makes the strategy longer) and must fit inside the
current unallocated remainder in that direction, so a holding already crossed
at the mid, or assigned once, is refused instead of counted twice. It is
recorded as one internal-cross uniform order per leg at `price`
(`batch_id = assign_position`), and requires a recovered account and a
configured `[exec]`.

## Manager delivery

The CTP Exec I/O task opens an authenticated, source-scoped gRPC
`ManagerPlans.Watch` subscription. Manager checks the current plan on connect
and after each committed batch. A plan is `ready` whenever the latest batch is a
live batch (published with a native trading day) whose decision time is not in
the future; there is no lease. Manager never waits on the Exec replica,
account snapshots or attribution. An idle subscription waits for changes.

The I/O task queues new plans for admission inside the existing Exec loop.
It retries a pending plan until admitted and deduplicates admitted plans across
reconnects; after an Exec restart the current plan is admitted again, however
old its decision. The `exec_target_accepted` audit records
admission by Exec. Factual fills and exchange outcomes come from the native
order journal. The JSON `exec-plan` GET remains available for diagnostics.
The subscription and any target IPC/gRPC listener run as local tasks of the
business thread's `IoLoop`, driven without blocking every millisecond; there is
no I/O worker thread. The Trader's `poll` mode (`busy` or `sleep`) controls the
empty-iteration policy.

With `--read-only`, the Manager-only ingress can subscribe without a
`[exec]` section. It reads the current account target, including a flat
or previously recorded batch, and logs `exec_target_observed` inside the
business loop. Executable subscriptions require the existing account and
market gates.

Each subscription update includes the batch identity and a machine-readable
plan status. The execution subscriber queues `ready` updates for admission.
Status transitions are emitted as `manager_plan_status` JSON log events.
`ready`, `no_batch` and `observe_only` use `info`; `missing_trading_day` and
`decision_in_future` use `warn`. Connection or
protocol errors emit `manager_plan_transport_error`. Deploy the Manager
that emits plan statuses before upgrading its Exec subscribers.

For an account configured to read Manager plans, use the matching
`[exec].source_id`. Add the target reader to the Trader TOML.

```toml
[position_target_ingress]
source_id = "ctp_live01"

[position_target_ingress.manager_plan]
grpc_url = "http://127.0.0.1:18202"
token_env = "CN_CTA_CTP_LIVE01_PLAN_TOKEN"
```

Configure the same account and environment credential in Manager's
`[exec_plan_readers]`. Enable `publication.include_trading_day` in the
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
enabled. The business loop publishes them to `persist_manager`, and Manager reads
them through its local secondary or the configured replica, together with the
Exec ledger evidence for strategy NAV.

Private Unix socket IPC and mTLS gRPC remain available as Exec target inputs
using the same admission queue. Manager's plan endpoint currently supports
configured CTP sources.

Compilation may run on the development host. All CTP code/tests, including
SDK-free/FakeTrader tests, run only on the designated external `sh_dev` host.
The initial implementation passed 166 native library tests there (one existing
helper ignored). The strategy-ledger and asynchronous persistence changes are
compiled locally with both official CTP/Yida feature sets; the SDK-free ledger
(`cn_futures_target::ledger`) and `persist_manager` tests run locally. The
exec, algo and persist tests in `cn_futures` must still run on `sh_dev`.
