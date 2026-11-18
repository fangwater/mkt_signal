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
For historical evidence that was never persisted, `--executions-jsonl` accepts
complete, source-scoped RapidX REST execution rows from a verified collection.
Only matched lifecycle orders are projected. Conflicting facts are rejected.
Use the same evidence file when resuming an interrupted apply. To require an
evidence-backed role for every external fill, use `--require-known-liquidity`.
`--unknown-report` records unresolved fills privately for investigation.

An interrupted apply requires the same original `--backup` and a new scratch
directory; its plan is reconstructed and compared with the saved receipt.
Retain the backup for recovery. Preview never overwrites the source.

The same explicit operation initializes an empty RapidX source. Stores without
the receipt retain their existing persistence behavior until explicitly repaired.
Only the updated owner may write a normalized store: running an older owner
would append unnormalized records and violate the receipt's guarantee. The
current implementation requires local persistence with sync publishing disabled.

The repair receipt belongs to the Exec owner. Manager consumes the standard
uniform stream directly. In-place updates publish the generic
`uniform_orders_revision` key in the default CF as an eight-byte big-endian
integer, in the same batch as the changed uniform records. Completed offline
repair also publishes the revision. It lets readers invalidate old FIFO
checkpoints without consulting an Exec repair receipt. Complete and verify
historical rewrites before exposing the repaired store to direct readers.
