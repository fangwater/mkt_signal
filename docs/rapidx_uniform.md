# RapidX standard fills

`persist_common::uniform_fill` defines the current stored uniform tail and the
bounded one-order normalizer. `persist_manager::rapidx_uniform` owns projection
and maintenance. Manager remains a read-only consumer; no runtime dependency on
the Manager checkout is required.

New orders use factual execution prices, base quantities, timestamps and roles.
Lifecycle records supply attribution and a quantity-completeness floor. WS and
REST observations of one transaction produce one fill; conflicting identities,
prices, quantities or roles refuse projection. Missing raw roles remain Unknown.
Late role evidence updates the same projected quantity and advances the rewrite
epoch. Durable dirty-order receipts survive owner restarts. A pending-order count
prevents consumers from publishing partial NAV generations.

Historical normalization preserves already priced lifecycle quantities, prices,
timestamps and attribution. It recovers missing/unpriced quantities and joins
factual roles. Provenance preserves the old logical receive key and timestamp,
including recovered/split identities used by ownership audits. Historical role
absence and native trade-report fallback remain distinct from explicit Unknown.
Original lifecycle bytes remain in `rapidx_uniform_state`; original execution
and role CFs remain unchanged. `uniform_orders` keeps zero-quantity lifecycle
rows and a single active standard-fill projection, without counting both.

## Explicit initialization and historical repair

This is a local closed-store maintenance operation, never a normal startup scan.
The tool refuses a live RocksDB writer and does not stop any process or contact
an exchange. A maintenance window must be arranged separately before using it.
No production repair or deployment was performed as part of implementing it.

Build the owner and tool from the maintained checkout:

```sh
cargo build --release -p persist_manager --features runtime \
  --bin persist_manager --bin rapidx_uniform_repair
```

Preview a closed source with its actual portfolio and exchange, using a **new**
scratch directory outside the source and backup:

```sh
rapidx_uniform_repair --db /absolute/source/persist_manager \
  --portfolio PORTFOLIO_ID --exchange BINANCE \
  --scratch /absolute/maintenance/preview
```

To apply, use a fresh scratch directory and add `--apply --backup
/absolute/maintenance/original-checkpoint`. Before the first overwrite, the tool
verifies source/evidence/plan fingerprints and creates and verifies a native
checkpoint, retaining its source `IDENTITY` and repair receipt. Apply sets an
incomplete marker, writes bounded batches, verifies all resulting uniform bytes,
then commits the complete marker. A completed source is verified without reapply.
An interrupted apply requires the same original `--backup` and a new scratch
directory; its plan is reconstructed and compared with the saved receipt.
Retain the backup for recovery. Preview never overwrites the source.

The same explicit operation initializes an empty RapidX source. Stores without
the receipt retain their existing persistence behavior until explicitly repaired.
Only the updated owner may write a normalized store: running an older owner
would append unnormalized records and violate the receipt's guarantee. The
current implementation requires local persistence with sync publishing disabled.

Manager accepts direct uniform reads only when the receipt matches database
identity, portfolio and exchange, repair is complete, and pending count is zero.
An epoch change invalidates derived FIFO/history caches. Long-window readers
reject a changed epoch rather than mixing the old checkpoint with overwritten
history. Unrepaired sources continue using the established validated recovery
path. Cold FIFO replay still occurs; this change removes repeated cross-CF
evidence reconstruction for repaired sources, not all startup work.
