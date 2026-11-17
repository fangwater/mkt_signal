# Exec contract leverage

## Account max_leverage

Exec account leverage is opt-in per environment's existing Redis hash:
`<env>:<futures-venue>:pre_trade_risk_params`, field `max_leverage`.
For example, `max_leverage=1` caps gross position notional at account net equity.
Missing or zero disables this limit; positive finite values support fractional
multiples. Other environments retain their existing behavior. The risk sync
script preserves this field rather than resetting the account's opt-in.

The Delist CTA Config page (`/delist/cta/config/`) exposes an **账户风控** section.
Read each account's current value and use **保存账户阈值** to write only its
`max_leverage` field. Concurrent changes return a conflict and require reloading.
The account cap is shared across frequencies; separate exchanges have separate
rows and budgets. Page edits join Delist's existing parameter-change journal.
On JP the editable scope is explicitly RX01/RX03; both remain at 1x.

After publishing the supporting binary once, changes hot-load on a persistent
async connection every 250 ms, without restarting Exec. The application log
`Exec account max_leverage applied` confirms activation. Redis failures and
invalid updates retain the last valid limit; an invalid initial value blocks
startup. Deleting the entire risk hash is an error, not a way to disable the cap.

Batch/Grid/POV and Chase check every new child order against factual positions
and all local live/unacknowledged orders, including market and cancel-pending
orders. For each market/symbol, reserve the greater notional after all buys or
all sells; opposing pending orders never cancel each other's reservation.
Sum these amounts across the owned cash and perpetual markets, without netting
spot against futures or one symbol against another. Inverse contracts use their
face value. Equity uses the existing account valuation, including unrealized
PnL and exchange-reported actual equity when present.

An over-cap account may reduce a position, reserving already-live close orders
so that multiple reductions cannot over-close into a new position. Missing
positions, prices or invalid equity block new risk. Lowering the limit affects
new submissions; it does not flatten positions or cancel existing orders.
Price changes and fills already in flight can still move actual leverage.

Two-exchange strategies must configure each execution account separately.
Never add another exchange's equity to this account's budget. Each Exec owns
one exchange; a mixed-exchange runtime fails this check rather than pooling
collateral. Multiple independent processes trading the same account do not
share local pending reservations and require a common account order owner.

On 2026-11-16 this cap was enabled at 1x only for LGQ `binance_exec_rx01`
and `binance_exec_rx03` on JP. Their running executables were SHA-256 checked;
logs confirm `max_leverage=Some(1.0)` and both position snapshots are ready.
Receipt: `data/ops/account-leverage-20261116T041018Z/`; environment-local
backups use `backups/account-leverage-20261116T041018Z/`.

## Exchange contract settings

Native Binance USD-M BatchExec/POV and Chase normally initialize each nonzero
target symbol at 5x before activating it. An exchange rejection keeps that symbol
blocked. Manager target quantities and strategy shares do not change with contract
leverage.

An operator may explicitly allow lower leverage for named contracts in one Exec
environment's private `env.sh`, for example:

```bash
export EXEC_BINANCE_UM_LEVERAGE_OVERRIDES='{"哈基米USDT":3}'
```

The value is a JSON object with normalized USD-M perpetual symbols and integer
leverage from 1 through 5. Unlisted symbols stay at 5x. Invalid configuration
blocks startup. The setting does not apply to COIN-M, RapidX, other venues or
FR leverage guards; there is no automatic fallback after an exchange rejection.

Restart that environment's pre-trade process through its local start/stop
wrappers after changing the setting. Both execution families include the explicit
map in the existing leverage-confirmation scope, so adding, changing or removing
an exception invalidates saved confirmations. Logs report the actual requested
leverage. Native Binance initialization binds the first configured trading IP,
resolves unspecified bindings through local route selection without sending a
packet, and disables HTTP proxies.

On 2026-11-14 the operator allowed the above single-contract exception for el01
`binance_exec_cta2024_902_15` (`cta2024-902`). Binance reported a maximum of 3x
for that symbol and rejected 5x with code `-4028`. Other contracts retain 5x;
the account uses one-way positions and Multi-Assets Mode. See the Manager
repository's `docs/el01_cta2024_902_20261112.md` for activation verification.
