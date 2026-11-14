# Exec contract leverage

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
