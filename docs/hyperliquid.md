# Hyperliquid Integration

This is an implementation inventory, not a production deployment approval.
No account-mode changes, transfers, or live orders are required to run the unit
tests. Configure production credentials only in the environment's `env.sh`.

## Venues And Market Data

`spread_pbs` accepts `hyperliquid-margin`, `hyperliquid-futures`, and
`hyperliquid-both`. The latter runs both publishers in one process. Deployment
wrappers require an explicit `SPREAD_PBS_CORE`; there is no default live CPU pin.

| Source | Internal output |
| --- | --- |
| `bbo` | Best bid/ask price and size |
| `trades` | Individual public trades |
| `l2Book` | Full book snapshots on the existing incremental IPC channel |
| Perpetual `activeAssetCtx` | Mark price, oracle/index price, funding rate, open interest in base-asset units |

Hyperliquid books are snapshots, not exchange-sequenced deltas. Do not apply
Binance-style sequence-gap rules. Different books/trades within the same
millisecond remain distinct. Context messages without venue timestamps use
second-quantized local receipt time; that is not an exchange event timestamp.

The catalog loads `perpDexs`, `allPerpMetas`, and `spotMeta`. Perpetual coverage
includes active default-DEX and HIP-3 assets. Spot selection currently uses the
USDC spot/default-perpetual intersection, not every listed spot token. Spot wire
indices and HIP-3 asset IDs must come from metadata, never symbol arithmetic.
Internal symbols include collateral and preserve DEX identity, for example
`xyz:FOO` with `USDH` maps to `XYZFOOUSDH`.

Enable trade, incremental, and derivatives in `mkt_cfg.yaml` or through
`SPREAD_PBS_ENABLE_TRADE`, `SPREAD_PBS_ENABLE_INCREMENTAL`, and
`SPREAD_PBS_ENABLE_DERIVATIVES`. `SPREAD_PBS_SYMBOLS` selects internal symbols.

Hyperliquid limits subscriptions to 1000 per public IP, across connections.
The process reserves the combined spot/perpetual and dual-leg budget before
starting sockets, rejects over-capacity refreshes, and retains peak reservations
until shutdown to cover rolling restarts. Unbound legs conservatively share the
explicit source routes. Different local IPs do not prove different public egress.
Other processes and private streams consume the same venue quota and must be
included in the deployment budget. Unfiltered all-perpetual coverage can exceed
one IP's quota.

`SPREAD_PBS_HYPERLIQUID_EGRESS_SHARDS` enables stable wire-coin sharding across
explicit source pairs. For example, using documentation-only addresses:

```bash
SPREAD_PBS_HYPERLIQUID_EGRESS_SHARDS='[{"primary_local_ip":"192.0.2.1","secondary_local_ip":"192.0.2.2"},{"primary_local_ip":"192.0.2.3","secondary_local_ip":"192.0.2.4"}]'
```

Replace these with actual bound local addresses whose public egress independence
has been verified. All addresses must be distinct and non-wildcard. Each coin's
BBO/trades/book/context stays on one shard with two redundant legs. Each venue
retains one IPC publisher per output channel and one shared dedup state; `both`
aggregates its subscription reservations across both venues. A refresh rejects
over-capacity groups before changing active routes. Rolling restarts replace one
leg at a time. Single-symbol book stalls also trigger one affected leg's restart
per health check, with a cooldown and the peer left running. Shard count and
source pairs are startup configuration. Partition
sizes need not be equal, so inspect the logged per-leg counts rather than simply
dividing the total by the number of IPs. This has unit coverage, not a live
multi-egress deployment test.

## Private Account Streams

`hyperliquid_account_monitor` subscribes on two paths to:

- `orderUpdates` and unaggregated `userFills`;
- `spotState` and `allDexsClearinghouseState`;
- `userFundings` and `userNonFundingLedgerUpdates`;
- `userTwapSliceFills` and `userTwapHistory`;
- `userEvents` (venue channel `user`), `notification`, and `webData3`;
- `twapStates` for every DEX and `activeAssetData` for every active perpetual.

Subscriptions require full matching ACK parameters, including coin/DEX for
repeated channel types, and the expected user identity. Channels that omit the
user are bound by the acknowledged per-socket subscription. Startup and
reconnection buffer WS events around HTTP order/history recovery. Private
subscription capacity is checked for both legs too; wildcard or identical IPs
share one quota. This check cannot account for other processes or shared NAT.
A failed
parse or conflicting fact rejects the frame without partially advancing dedup
state. Slice fills share the ordinary fill deduplicator; their TWAP association
is a separate audit fact and does not create a second strategy execution.
TWAP history preserves the venue's seconds-based row `time` separately from
its millisecond `state.timestamp`; slice fill times are milliseconds. History
watermarks must never be used as millisecond fill-pagination cursors.

Balances and positions are applied through complete, scope-matched snapshot
transactions with freshness leases. Every DEX retains its native collateral and
decimal summary fields. A default-DEX query cannot initialize all-DEX positions
or shared-collateral account readiness.

| Account mode | Implemented behavior |
| --- | --- |
| Standard | Separate spot/default-perpetual cash and risk; all-DEX positions and native summaries; HIP-3 opening orders blocked, reduce-only allowed by the execution adapter |
| Unified | Shared token balances and all-DEX state; collateral-specific maintenance ratio; strategy readiness requires fresh complete state and durable facts |
| Portfolio Margin | Native portfolio risk from `spotState.portfolioMarginRatio`; shared balances and complete all-DEX positions; strategy readiness requires fresh state, valid risk, and durable facts |

Account monitor, trade engine and pre_trade discover the actual account rules
from the exchange; no user-configured account-mode switch is required or read.
Strategies continue to consume the existing balance, position and risk IPC
interfaces. An ambiguous `default` response for a user/subaccount fails closed;
the software never overrides exchange truth using a local mode setting.
Runtime mode drift invalidates
readiness and latches execution off. Deprecated DEX abstraction is not supported.
Portfolio Margin polls `borrowLendUserState` and `allBorrowLendReserveStates`
every 30 seconds. Native borrow/supply balances, yearly rates, utilization,
oracle prices, LTV, health, and healthFactor are retained as audit evidence.
PM bootstrap and reconnect also fetch `borrowLendUserState` before accepting
spot snapshots. Borrow `basis` is normalized as principal and `value - basis`
as accrued interest through the existing `BasicBorrowInterestMsg` interface.
When the paired reserve poll is available, account-risk `borrowed_usd` is the
sum of borrow current value (including accrued interest) times each reserve's
`oraclePx`. Missing valuations remain unknown; USD parity is never assumed.
Each spot transaction publishes wallet = net spot total + that exact cached
liability, together with principal and interest. The common wallet-minus-debt
calculation therefore preserves the venue's net balance; it does not subtract
borrowing twice. These normalized wallets are not a separately reported venue
gross balance. Borrow timestamps retain their actual HTTP receipt time: the
adapter does not claim an atomic venue cut across HTTP and websocket responses.
Missing, malformed, future-dated or 60-second-old borrowing rejects PM spot
updates. Snapshot readiness is capped at the borrowing deadline, even when
spot updates continue. Full snapshots explicitly clear repaid/omitted debt.
An isolated PM `spotClearinghouseState` query cannot replace this combined
state and is not advertised as complete; recovery uses the account monitor.
Borrow/lend `healthFactor` is audit-only. The PM trading-risk source is the
venue-calculated `portfolioMarginRatio` in the PM `spotState` snapshot, also used
by the official trading frontend. It already accounts for the exchange's PM
collateral, borrowing and cap rules; the adapter does not independently recreate
that calculation from incomplete borrow/lend responses or hard-coded caps.
The documented PM liquidation boundary is 0.95. The existing account risk IPC
uses a higher-is-safer ratio, so PM publishes:

```text
margin_ratio = min(0.95 / portfolioMarginRatio, 1e12)
portfolioMarginRatio == 0 -> margin_ratio = 1e12
```

The ratio and, when sourced, oracle-valued borrowing are populated. Unavailable
USD equity/margin fields are NaN on the existing fixed-width IPC wire and `null`
in JSON views, not factual zeroes
or invented cross-token totals. Existing `unimmr_trigger_line` and
`unimmr_recover_line` retain their higher-is-safer meaning. For example a venue
ratio of 0.5 maps to 1.9; with a trigger of 2.0 this locks new arb opening.
This mapping is not the raw percentage displayed by Hyperliquid.

PM risk is emitted within the spot snapshot transaction. Perpetual refreshes
cannot overwrite or erase it with a single-DEX value. Both spot and all-DEX
position snapshots must be complete and fresh, and the factual replay must be
durable and from the same monitor epoch, before strategy readiness is granted.
Missing/null/negative/non-finite PM ratios reject the snapshot without partially
applying balances. Risk is not defaulted to healthy when the venue omits the
field. A stale or invalidated snapshot revokes readiness. Existing order/exposure
limits and exchange-side order validation still apply; a current account ratio
does not guarantee acceptance of a future order or reserve borrow liquidity.

Source validation used the official frontend's response parser and ratio
display (`config-CJA34E4O.js` and `index-DjTwPlgb.js`, inspected 2026-09-05).
The adapter depends on API field names, never on these frontend bundle hashes.
A read-only zero-address WS probe confirmed the PM subscription ACK, but that
non-PM address omits the ratio. Tests use explicit PM schema fixtures; no funded
PM account or live order was used for verification.

## Durable Facts And Recovery

Orders, fills, funding, non-funding ledger events, TWAP associations, and TWAP
history use the existing account IPC envelope and stable venue identities.
Supplemental native facts use account event type `4018`, carrying source,
canonical JSON, and an explicitly local microsecond receipt timestamp. Venue
timestamps remain in JSON in their native units. Liquidation summaries and
non-user cancellations are audit-only and never synthesized into fills, fees,
or strategy orders. Changed TWAP/asset/UI/borrow snapshots are audit facts, not
substitutes for the canonical balance/position transactions. Unknown external
order intent is retained as native lifecycle evidence rather than guessed as
GTC; standard order validation still applies.
`pre_trade` waits for an exact-value ACK from `persist_manager` before applying
the fact and advancing its fsynced cursor. The ACK binds account, monitor epoch,
sequence, stable key, and payload digest. A missing persistor keeps readiness
closed; sending an IPC message alone is not considered durable.

`persist_manager` stores these facts in RocksDB `hyperliquid_account_facts`.
With persistence sync enabled, the fact and replication outbox are written in
one synchronous batch. Facts are not added to the order-export Parquet schema
or the time-keyed order-bucket repair job. Enabling sync later does not backfill
facts previously stored without an outbox entry.

HTTP recovery is bounded by venue retention and local safety caps. Fill recovery
checks the documented 10,000 retained-fill boundary. Funding/ledger cold-start
lookback defaults to seven days. TWAP history has no assumed undocumented
retention size; reconnect requires overlap with the prior history watermark.
Cold-start fill recovery at the retention cap fails closed instead of claiming
complete coverage. This is not an account-lifetime archival backfill.

## Runtime Configuration

| Variable | Meaning |
| --- | --- |
| `HYPERLIQUID_ACCOUNT_ADDRESS` | Actual user, subaccount, or vault being observed/traded, not the API-agent wallet |
| `HYPERLIQUID_PRIVATE_KEY` | Signing key for `trade_engine`; account monitor does not require it |
| `HYPERLIQUID_VAULT_ADDRESS` | Required matching order target for subaccounts/vaults; unset for a master user |
| `HYPERLIQUID_TESTNET` | Select testnet consistently across all processes; default false |
| `HYPERLIQUID_INFO_URL`, `HYPERLIQUID_WS_URL` | Optional endpoint overrides; known cross-network combinations rejected |
| `HYPERLIQUID_ACTION_EXPIRES_AFTER_MS` | Signed new-order expiry, default 15000; accepted range 1000 through 60000 |
| `HYPERLIQUID_FACT_CURSOR_PATH` | Optional durable consumer cursor path; default account-specific file under `data/` |
| `HYPERLIQUID_FACT_RECOVERY_LOOKBACK_MS` | Funding/ledger cold-start recovery lookback, default seven days |
| `HYPERLIQUID_PROCESS_FILL_SNAPSHOT` | Default true; false opts into baseline-only startup and omits initial historical fills from the factual output |

The monitor loads source IPs through the existing trade-engine/account-monitor
configuration loader. Snapshot TTL defaults to 60 seconds, state refresh to
30 seconds, and drift checks to 45 seconds. Refresh and drift intervals must be
less than the TTL. Keep fill snapshot processing enabled for historical factual
output. Reconnect snapshots still recover unseen fills in baseline-only mode.

Signed WS `post` supports new orders, cancel-by-cloid, and `orderStatus` queries.
Limit orders use `Alo` (post-only); market orders use price-protected `Ioc`.
Timeouts and ambiguous transport outcomes require query reconciliation, not
blind resubmission. Query transport IDs remain separate from business IDs.

## Shared CEX Execution Behavior

Hyperliquid uses the existing `TradeEngineResponse`, signal throttle, hedge
backoff, and reduce-only order paths. Documented text errors map to stable
internal codes; both per-order error objects and whole-action rejections retain
their error text. Unknown errors remain generic rejections, and transport
ambiguity continues to require order-status reconciliation.

| Rejection | Shared strategy behavior |
| --- | --- |
| Post-only crossing | Existing post-only rejection handling |
| Oracle/reference price bound | Existing price-limit error category, not an account-capacity lock |
| Insufficient perpetual margin | Account-wide new-risk signal throttle; rejected hedge also cancels outstanding ArbOpen orders |
| Insufficient spot balance | Symbol/direction throttle, not an account-wide margin lock |
| OI or position-tier limit | Symbol/direction throttle and hedge retry backoff |
| Tick, minimum notional, reduce-only or trigger validation | Distinct error codes and existing non-retryable error-description interface |
| IOC no match or no market liquidity | Distinct liquidity rejection, not insufficient margin or transport ambiguity |

Capacity rejections reuse the existing two-hour `SIGNAL_THROTTLE_TTL_US`.
They expire as backoff, not as proof that exchange capacity recovered. New orders
still pass snapshot freshness, account ratio, exposure, and exchange checks.
A healthy PM maintenance ratio does not clear an initial-margin or borrowing
capacity rejection. Confirmed non-flipping Hyperliquid perpetual reductions may
bypass the hedge open-failure backoff and are sent with `reduceOnly=true`.
Spot has no equivalent exchange-enforced reduce-only primitive.

The shared strategy `Order` does not expose an independent TIF selector. Like
the existing CEX strategy builders, Hyperliquid retains post-only limits and
protected IOC market intent. Adding adapter-only GTC/FOK selection would not
provide an end-to-end strategy contract and is not claimed here.

## Remaining Coverage

Spot market selection remains the USDC/default-perpetual intersection. Open interest is
published as market message type `1016` on the derivatives IPC channel; consumers
must explicitly opt into this metric instead of interpreting it as price.
Supplemental whole-frame native facts are bounded by the existing 16 KiB account
IPC payload. Oversized frames reject without partial dedup progress; they are
not truncated. No historical recovery is claimed for ephemeral liquidation,
notification, or UI events that the venue does not expose through the existing
recovery endpoints. Full exchange-feature parity and live readiness are not
claimed by this inventory.

## Protocol References

- [WebSocket subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
- [WebSocket POST requests](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/post-requests)
- [Info endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
- [Account abstraction modes](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/account-abstraction-modes)
- [Portfolio margin](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/portfolio-margin)
- [Rate and user limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)
- [Order error responses](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses)
