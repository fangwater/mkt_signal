# Exec POV

Exec supports `algorithm: "batch"` (default) and `algorithm: "pov"` in the
existing `batch_exec:<strategy_name>` configuration. POV controls the quantity
released to the existing BatchExec order lifecycle. It uses the existing
account risk checks, Manager tradability cache, rate limiter, order queries,
orphan reconciliation, position allocation, and persistence.

## Reference And Design

- [Gate spot POV API](https://www.gate.com/docs/developers/apiv4/en/spot/)
  exposes participation rate, validity period, limit and trigger prices.
- [Binance Futures volume participation](https://www.binance.com/en/support/faq/detail/b0b94dcc8eb64c2585763b8747b60702)
  exposes urgency and duration.
- [Binance spot POV](https://www.binance.com/en-KZ/support/faq/detail/b5b7a1b4182b4cd696454da2e0629687)
  adjusts execution speed with market volume.

This implementation runs locally in `exec-pre-trade`; it does not submit
exchange-hosted algorithm orders. Existing child order attribution therefore
continues to identify the CTA strategy. A target remains an absolute signed
position in base quantity, including when crossing through zero.

## Configuration

Example complete strategy value (illustrative prices and sizes, no live writes):

```json
{
  "algorithm": "pov",
  "pov": {
    "participation_rate": 0.10,
    "max_batch_usdt": 300.0,
    "max_carry_usdt": 600.0,
    "volume_stale_ms": 5000,
    "quote_stale_ms": 1000,
    "duration_ms": 3600000,
    "liquidity": "maker_then_taker",
    "limit_price": null
  },
  "single_order_usdt": 100.0,
  "orders_per_batch": 3,
  "max_batch": 20,
  "maker_price_anchor": "own_best",
  "tick_spacing": 1,
  "batch_interval_ms": 500,
  "maker_timeout_ms": 1000,
  "max_maker_requotes": 2,
  "target_tolerance_usdt": 10.0,
  "targets": {"BTCUSDT": {"qty": 0.1, "signal": 0}},
  "symbol_overrides": {"ETHUSDT": {"algorithm": "batch"}}
}
```

| Parameter | Meaning |
| --- | --- |
| `participation_rate` | Fraction in `(0, 1]`, e.g. `0.10` is 10%; continuous rather than fixed urgency buckets. |
| `max_batch_usdt` | Extra batch size cap, converted to base quantity using the current ask. Actual notional depends on child price and fills. |
| `max_carry_usdt` | Unreserved credit cap valued at the latest public trade price; must be at least `max_batch_usdt`. Prevents unlimited catch-up after illiquidity or price blocking. |
| `volume_stale_ms` | Maximum public trade event age at ingestion and order submission. |
| `quote_stale_ms` | Maximum BBO event age at submission. |
| `duration_ms` | Duration from target activation; expiry cancels working orders and leaves remaining position unexecuted. Positive uint32 milliseconds. |
| `liquidity` | `maker_only`, `taker_only`, or `maker_then_taker`. |
| `limit_price` | Optional buy ceiling / sell floor; requires `maker_only`, since the market-order transport cannot enforce a hard cap. Prices are checked on every new or replacement child plan. |

Existing `single_order_usdt`, `orders_per_batch`, price anchor, tick spacing,
batch interval, maker timeout/requote count, and target tolerance still apply.
POV disables the dynamic order enlargement associated with `max_batch`.
`max_batch` is retained in the common config but has no sizing effect in POV.
Signals `+1/-1` select immediate taker execution only under `maker_then_taker`;
explicit `maker_only` and `taker_only` policies take precedence. All modes,
including signal-driven takers, consume the same volume budget.

`algorithm` and `pov` are accepted by the existing authenticated
`POST /api/order-parameters` path, strategy reads, and symbol overrides. A
symbol `pov` override replaces the entire POV object, with missing nested
members filled from POV defaults; it does not merge individual nested fields
with the strategy's POV object. No new Redis keys or internal API versions are
used. Send complete current order parameters when updating them.

## Volume Accounting

`exec-pre-trade` subscribes to `dat_pbs/<venue>/trade`, which is published by
`spread_pbs` when its trade stream is enabled. Its BBO remains
`spread_pbs/<venue>/ask_bid_spread`. The trade subscriber retries when the
publisher is unavailable; batch mode continues independently.
The producer's `data_types.enable_trade` or `SPREAD_PBS_ENABLE_TRADE=true`
enables the trade stream. A Binance futures `bookticker` role alone is
insufficient; the matching `market` or `full` role must also run in the same
IPC namespace. Cross-host BBO forwarding alone does not supply POV volume.

The existing trade parser and account quantity conversion convert venue sizes
to base quantity, including Gate decimal contract sizes and inverse contracts
at the trade price. Positive increasing per-symbol trade IDs and nondecreasing
event timestamps are required. Duplicate, reordered, malformed, future, and
old trades are discarded. The listener also rejects backlog older than 60s.
This matches the Binance/Gate trade feeds; other venues must provide this ID
contract before relying on POV. There is no REST polling or historical volume
bootstrap. IPC loss causes conservative underparticipation.

For each target and strategy, a public trade of base quantity `V` earns
`participation_rate * V` credit. Both aggressor sides count once; the public
tape includes the strategy's own fills. Credit is capped at outstanding batch
reservations plus `max_carry_usdt / trade_price`. Available quantity equals
credit minus all outstanding batch quantities, including orders awaiting
cancellation, unsent child plans, and orphan orders. A confirmed fill reduces
credit and its reservation by the same amount. Requotes reuse their existing
reservation. Removed unfilled batches release only their reservations.

Example: 20 BTC observed at 10% earns 2 BTC. A 2 BTC working batch consumes all
available credit; filling 0.5 BTC leaves 1.5 BTC reserved and zero additional
capacity. Cancellation confirmation makes the unfilled reservation reusable.
Multiple CTA strategies each have their own requested participation rate;
these rates add across the account. This is not an account-wide participation
cap. Account exposure and order-count limits remain shared.

A new batch is bounded by target remainder, normal batch capacity, POV batch
cap, and available volume credit. If that bound cannot satisfy venue minimums,
Exec waits for more volume. It never rounds up a volume budget or merges an
unfunded residual into a requote. Quantity step alignment rounds down.

## Lifecycle And Observation

New targets first cancel/reconcile the old generation, then start with zero
credit at activation. Process restart also starts with zero volume credit;
position and orphan recovery continue through the existing Exec mechanisms.
Changing the algorithm or POV parameters cancels existing batches, discards
credit, and preserves unresolved orders until terminal evidence arrives.
Changing POV parameters retains the original target start time, so an ordinary
parameter refresh does not renew its deadline. A new target generation renews
the duration. Existing fills after a budget reset are debited conservatively.

Stale volume or BBO pauses submission and requests cancellation of resting
orders. Cancellation races may still fill, and those fills remain accounted.
Fresh data resumes execution within the available budget. Expiry does not
force a market sweep; extending `duration_ms` or submitting a new target can
resume execution. Price-blocked maker plans wait for an allowed price.

The existing Exec state IPC/JSON row now includes `algorithm` and `pov` with
status, configured participation, observed base volume, filled quantity,
reserved/available quantity, last trade time, and deadline. `expired` is a POV
status and does not assert that the requested target was achieved. For POV,
`remaining_batches` and `estimated_completion_ts_ms` are zero (unknown), since
future volume is unknown. The common state contract changes in place; rebuild
the producer and viz consumer together.

## Verification

Run `cargo check -p mkt_signal -p viz_server --bin exec-pre-trade --bin viz_server`,
`cargo test -p mkt_signal --lib`, `cargo test -p viz_common`, and
`python3 -m unittest scripts.tests.test_exec_config_server`.
On this host bindgen may need
`BINDGEN_EXTRA_CLANG_ARGS=-I/usr/lib/gcc/x86_64-linux-gnu/13/include`.
No production deployment or exchange order is required for these checks.
