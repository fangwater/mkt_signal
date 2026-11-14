# Hyperliquid order-level feed

This crate receives node-side `l4Book` and publishes compact L3 order-book
snapshots and mutations. It is the shared source mirror in `crypto_proxy` and
`mkt_signal`; keep **all crate files identical**, including fixtures and tests.
There is one binary contract, independent of either project's legacy L2 format.

The protocol was checked against `hyperliquid-dex/order_book_server` commit
`8b4f237904f683aca2dba21a07d87e831ead2a97`, especially `server/src/types/mod.rs`,
`types/node_data.rs`, and `listeners/order_book/state.rs`. Fixtures are synthetic
examples of those types, not captures from a running mainnet node.

## Source and scope

Use a non-validating Hyperliquid node plus a compatible order-book server, or a
provider exposing that protocol. The ordinary public WebSocket has no `l4Book`;
public mainnet/testnet API hosts are rejected by configuration validation.
The reference service supports perpetuals, not spot books or untriggered trigger
orders. This collector does not turn L2 snapshots into inferred individual orders.

Native coins (`BTC`, `xyz:TSLA`) remain exact and case-sensitive on the new wire;
no paired-spot filter, collateral assumption, or lossy symbol concatenation is
applied. Explicit coin lists allow process/core sharding without shared hot locks.
HIP-3 coins work when the configured upstream exposes them. This is a compact
order-book contract: it preserves user, oid, optional cloid, side, price, size,
priority anchor, reduce-only/trigger flags and timestamps. It does not archive
the native order-type/trigger-condition strings, all terminal status rows, or
non-book account facts. Existing account/fill pipelines own those facts.

## Hot path

- A single-pass `sonic-rs` serde-seed decoder writes directly into reused typed
  arenas. It neither constructs a JSON DOM nor allocates a String per order.
- Prices and sizes use exact checked unsigned e8 integers. Unsupported precision,
  invalid identities, malformed trailing records, mixed coins and missing opening
  statuses reject the complete frame before any record is published.
- Snapshots retain input order; mutations retain `book_diffs` order. `insertBefore`
  is preserved. New-order status metadata is joined by oid; the diff's resting
  size overrides the status size. Triggered orders use their actual entry time.
- All JSON and book-relevant records are validated before publication. Fixed-width 96-byte
  records are chunked into reusable, bounded buffers. IPC writes directly into
  an exact-length loan without clearing an unused 64 KiB tail.
- The receive/decode/publish path has no per-order task, lock or intermediate
  unbounded queue. One collector uses one ordered upstream at a time. Optional
  source failover always obtains a new complete snapshot rather than merging
  two nodes' order histories.
- The optional consumer uses an oid-to-arena-slot hash map and intrusive per-price
  FIFO links. Insert, `insertBefore`, cancel and size update take expected O(1)
  time without shifting price-level vectors. Freed slots are reused.

Reserve sizes, source-frame bytes, event counts and output chunk sizes are bounded.
Cold-start/first-peak growth is allowed up to the configured bounds; the
allocation benchmark measures repeated warmed frames, not networking/TLS startup.

## Continuity and recovery

The upstream externally tagged JSON shapes are:

```json
{"channel":"l4Book","data":{"Snapshot":{"coin":"BTC","time":1751427259657,"height":100,"levels":[[],[]]}}}
{"channel":"l4Book","data":{"Updates":{"time":1751427260000,"height":105,"order_statuses":[],"book_diffs":[...]}}}
```

`Updates` has **no top-level coin**. The decoder infers it from the records and
checks that every status/diff belongs to the same coin. Empty, unidentifiable
Updates are rejected. A status-only Updates still publishes an empty mutation
batch and advances continuity. A snapshot with zero orders clears the book.

Source heights are sparse per coin: `100 -> 105` is valid. Output adds a contiguous
per-coin batch sequence, the preceding **delivered** height and a random 128-bit
connection epoch. Every chunk of a batch has the same sequence and metadata.
Consumers stage all chunks and apply only complete snapshots/blocks. Missing,
reordered or inconsistent chunks and invalid oid mutations revoke readiness.
Exact duplicate upstream frames at the most recent height are suppressed within
one connection; conflicting/reordered data forces reconnection and resnapshot.
By default source block time must be at most ten seconds old and no more than
five seconds in the future. `--max-source-lag-ms 0` disables that check only for
explicit historical replay/testing; local receipt time alone does not make an
old node snapshot fresh.

Startup, disconnect, parse failure and graceful shutdown emit Reset. A new epoch's
snapshot is independently sufficient to recover if the Reset was missed. Updates
cannot initialize a book. A consumer joining midstream must wait for a snapshot;
`--resnapshot-secs` bounds the periodic refresh interval (default 300 seconds).
Start consumers before collectors when an immediate first snapshot is required.

PUB/IPC queues are bounded and may drop/overwrite for slow readers. Sequence and
chunk checks detect a missed batch on subsequent delivery; they do **not** promise
lossless transport. Consumers must enforce freshness (`OrderBook::is_fresh`) as
well as completeness and request/restart a source to resnapshot when needed.
Quiet per-coin streams cannot prove node health just from their last update.

## Binary contract

Little-endian, no platform padding, market message type **1040**. This new channel
always uses microseconds in both repositories. Legacy proxy millisecond fields
and existing IPC formats are unchanged. Prices/sizes are integers divided by
`100_000_000`; do not reinterpret their bits as f64.

| Header offset | Bytes | Field |
| --- | --- | --- |
| 0 | 4 | Message type = 1040 |
| 4 | 4 | Exact total chunk bytes |
| 8 | 16 | Connection epoch |
| 24 | 8 | Per-coin batch sequence (Reset 0, Snapshot 1) |
| 32 | 8 | Source block height |
| 40 | 8 | Previous delivered source height |
| 48 | 8 | Source block time, microseconds |
| 56 | 8 | Local receipt time, microseconds |
| 64 | 4 | Zero-based chunk index |
| 68 | 4 | Total chunks in this batch |
| 72 | 4 | Records in this chunk |
| 76 | 1 | Snapshot=1, Update=2, Reset=3 |
| 77 | 1 | Reserved, zero |
| 78 | 2 | Native coin byte length |
| 80 | 64 | ASCII native coin, zero-padded |

Each record is 96 bytes immediately following the 144-byte header:

| Record offset | Bytes | Field |
| --- | --- | --- |
| 0 | 8 | oid |
| 8 | 8 | Price, e8 |
| 16 | 8 | Remaining size, e8 |
| 24 | 8 | Original size for Update, e8 |
| 32 | 8 | `insertBefore` oid when present |
| 40 | 8 | Order/entry time, microseconds (New/Snapshot) |
| 48 | 20 | User address bytes |
| 68 | 16 | Optional cloid bytes |
| 84 | 2 | Flags: user=1, cloid=2, anchor=4, reduceOnly=8, isTrigger=16, positionTpSl=32 |
| 86 | 1 | Side: unknown=0, bid=1, ask=2 |
| 87 | 1 | Operation: Snapshot=1, New=2, Update=3, Remove=4 |
| 88 | 8 | Opening status time, microseconds (New only) |

Update/Remove resolve side and standing metadata by oid; their zero side is not
a sell signal. Order size may become zero before Remove. Flags distinguish
absent identities/anchors from zero bytes. `origSz` is validated by the consumer.
The transport carries no implicit asset-universe positions and no protocol
version forks. Use `Header::decode` / `Event::decode`, not Rust struct casts.

## Validation

In `mkt_signal`, run only the focused crate (no trading/CTP code):

```bash
cargo test -p hyperliquid_l4 --features ipc
cargo bench -p hyperliquid_l4 --bench decode
```

The protocol suite includes an independent vector-based FIFO reference, a real
local WebSocket server, interrupted publication, actual iceoryx2 delivery,
decimal/identity validation, malformed tails and complete-batch recovery. The
proxy's focused suite additionally exercises actual ZMQ PUB/SUB delivery against
its separately locked dependency versions. No live orders are involved.

The benchmark reports throughput, per-frame p50/p99 and counted heap allocations
after warm-up, with synthetic fixtures. DOM-only parsing is a separately labeled
reference, not an end-to-end equivalent workload. Network bandwidth, source node
cost, TLS, subscriber lag and production feed bursts require live measurement.
