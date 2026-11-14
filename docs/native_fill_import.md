# Native ordinary-fill maintenance

`native_fill_import` is an independent, offline `persist_manager` binary. It
imports audited Binance USD-M ordinary LIMIT/GTX maker and native MARKET/GTC taker REST fills and explicitly
reviewed mobile reduce-only MARKET taker fills, without
publishing IPC, querying an exchange, changing a live order, or using the
forced-close repair identity. Its first reviewed input is the trade03 manifest
and checklist from 2026-10-23. The trade03 manifest's HBAR exclusion is mandatory for trade03 only. Other
source manifests must not contain that account's exclusion; they are validated
against their own binding and same-source historical attribution.
Other order types, external clients, venues, partial exchange-history coverage and replicated
sources are refused rather than guessed.

Mobile inputs must have an original `ios_` client string, `reduceOnly=true`,
MARKET/origType MARKET, GTC, BOTH and factual `maker=false`. They must explicitly
declare `attribution_kind=external_account`,
`inferred_original_from_key=external_account_fill:ios` and
`native_client_order_id=-exchange_order_id`. That from-key field describes a
REST reconstruction, not recovered Exec strategy metadata. No CTA identity is
inferred. The current native codec has only an integer client-ID field, so this
uses the existing negative exchange-order-ID convention; the exact original
string remains in `native_fill_repairs` together with the scheme and a null
strategy identity. Order-ID collisions, existing positive raw cumulative fills,
existing native fills without a matching repair identity and changed audit bytes
refuse the whole batch. Never rewrite a mobile fill as `chase_exec:*`, use a
positive invented Exec client ID, or describe it as an exchange forced close.
UTF-8 alphanumeric venue symbols are accepted for these reviewed external facts.

Build in the maintained Exec checkout:

```sh
cargo test -p persist_common -p persist_manager --features persist_manager/runtime --lib
cargo build --release -p persist_manager --features runtime --bin native_fill_import
```

The binding JSON pins `source_id`, absolute canonical `env_root`,
`db_path = env_root/data/persist_manager`, `db_identity`, `ipc_namespace`,
`venue = 1`, `sync_disabled_evidence_sha256` and `fee_configuration_sha256`.
The operator must obtain those hashes from current live-process sync evidence
and the source's effective fee configuration. It must not contain credentials.
Source identity must be reviewed against the host Manager catalog and the Exec
deployment; the program cannot infer account identity from RocksDB values.
The exact binding, manifest and checklist bytes are hashed into each audit.

```sh
native_fill_import --binding binding.json --manifest fill_identity_manifest.json \
  --checklist missing_fills_checklist.json
```

This default is read-only. Every run decodes all five native matched/unmatched
CFs, rechecks unique same-symbol/client-high-bits attribution, the HBAR exclusion,
and existing repair identities. Any existing positive uniform fill without a
repair identity refuses the entire batch. Positive raw cumulative reports also
refuse the batch unless they meet the reviewed, byte-pinned unmatched-report
exception described below. A current LIMIT identity
must stay positive and `chase_exec:*` / `batch_exec:*` must match unique same-source historical evidence.
Exact decimal arithmetic validates per-order cumulative quantities, quote sums,
commission totals, and manifest counts before f64 conversion for native codecs.

The production serializers have one implementation in
`persist_common::native_codec`; `pre_trade::persist_channel` still performs its
original venue normalization and supplies the clock. The persister and importer
share the same disk BBO insertion. No wire format or production behavior changes.
The importer uses production decoders for existing data and new payload checks.

For each fill, it writes one incremental `uniform_orders` record and one
`trade_updates` cumulative record, preserving the original positive client ID,
trade price, base quantity, factual LIMIT/MARKET type and Maker/Taker role and exchange millisecond
timestamps (converted to microseconds). The receive key is a new repair time,
checked for collisions. Unknown event/signal/submit/local/market times are zero;
BBO is empty and signal BBO absent. This is documented REST reconstruction, not
an assertion that a missing WS packet was recovered. Fill-time status is derived
from exact cumulative quantity; SAND remains PARTIALLY_FILLED at its fill, and
its later REST CANCELED terminal state is retained in the audit. No raw
`order_updates` are synthesized because REST does not prove WS execution type
or event time. Existing zero-quantity NEW records remain unchanged.
The mobile path instead retains its original string in the audit as described
above, writes MARKET and Taker, and keeps ownership explicitly unassigned.

`native_fill_repairs` is the sole repair audit CF. Its `fill:<JSON tuple>` keys
are `(source_id, symbol, 1, exchange_order_id, exchange_trade_id)`, with IDs as
strings. Values retain the complete exchange order/trade JSON including original
BNB commission decimals, inferred attribution evidence, source file hashes,
native key/value bytes and SHA-256, and batch ID. Native CFs have no tradeId or
commission fields; no extra native tail fields are added. Manager continues to
estimate maker fees from its own saved fee rates; BNB is never treated as USDT.
`batch:<digest>` records contain binding, checkpoint path, repair receive time,
input hash, committed state and all affected keys with their previous absence
and new bytes. Native rows, identities and batch record use one synchronous
WriteBatch. A crash yields either the whole committed batch or none of it.
Creating an empty audit CF can precede this commit; it carries no fill state.

Before `--apply`, establish and document a lossless quiescence and drain of this
source's producers using reviewed environment wrappers. The tool refuses writes
while this environment's persister, pre-trade or trade engine is running, takes
the normal primary RocksDB lock, refuses nonempty sync_meta/sync_outbox and
nonempty sync environment settings, and never removes LOCK. Its process check
is an offline precondition, not a durable-drain protocol. Keep producers stopped
through the whole operation; a settled Viz snapshot alone does not prove drained
persistence. If the deployment has no proven drain/retention mechanism, stop
before production apply. Do not stop only the persister while trading continues.

```sh
native_fill_import --binding binding.json --manifest fill_identity_manifest.json \
  --checklist missing_fills_checklist.json --apply --backup /absolute/new-checkpoint
```

Before writing any native row, the tool creates a complete RocksDB checkpoint,
checks its IDENTITY, reopens it read-only and proves the same repair plan. The
checkpoint path must be new and outside the source DB. After commit it verifies
every identity and byte mapping and repeats the plan: zero new fills must remain.
Repeated apply produces no new batch. Keep the checkpoint and input evidence.
The audit is authoritative if the process crashes after commit but before stdout.

```sh
native_fill_import --binding binding.json --manifest fill_identity_manifest.json \
  --checklist missing_fills_checklist.json --rollback BATCH_ID
# After reviewing that dry run, add --apply to roll back under the same offline conditions.
```

Rollback verifies every current key equals its recorded new bytes, deletes only
those keys in one synchronous batch, and retains a `rolled_back` batch audit.
A changed/missing row refuses the entire rollback. Existing and later native
rows survive. Repeated rollback is a no-op. It never restores an old full DB
onto a source with later trading. Replicated sources are unsupported because
replica deletion/rollback propagation is not provided.

Only after import verification and a fresh full-history account FIFO / Exec
reconciliation passes the existing allocation tool gates may Manager allocation
be prepared/applied. This tool does not widen those gates, write PostgreSQL,
change snapshots, or imply that these 24 fills cover every historical gap.

Reviewed unmatched-report completion also accepts per-fill
`existing_raw_trade_records` containing the exact `trade_updates_unmatched` key,
value bytes and SHA-256. Production decoders must prove the same source-bound
client/order identity, venue, side, price, trade time, maker role and exact
cumulative quantity as the signed REST facts. Those bytes must still exist
unchanged on every plan and repeat. For these fills only the missing uniform
record is inserted; the original unmatched report is preserved through import
and rollback. Undeclared positive raw reports, changed references and matched
positive reports continue to refuse. The trade03-only HBAR exclusion remains.
Attribution may use original zero-fill uniform metadata; a unique same-source
symbol/client-high-bits mapping is still required. No live runtime change or
repair IPC is involved.
