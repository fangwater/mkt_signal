# Exec native Binance leverage policy

Native BatchExec/ChaseExec normally initialize contract leverage at 5x.
`EXEC_BINANCE_UM_LEVERAGE_FALLBACK=1` opts one environment into automatic
5x → 4x → 3x fallback. Default remains disabled for other environments.
Only native Binance USD-M STANDARD, one-way accounts are supported. The startup
account-mode gate is reused; no periodic account-mode requests are introduced.
Explicit `EXEC_BINANCE_UM_LEVERAGE_OVERRIDES` always take precedence and exclude
those symbols from automatic management.

New-symbol initialization tries 5x first. An explicit HTTP 400 Binance -4028
(invalid leverage) or -2027 (position cap) allows 4x, then 3x. Auth, clock,
network, rate-limit and -2028 margin errors do not permit further lowering.
POST outcomes are always reconciled with a signed position-risk readback.
A lost response can succeed only when that readback confirms the desired level;
a failed readback or conflicting acknowledgement blocks further changes.
The initialization cache remains a confirmation of the 5x-preferred policy,
not a statement that every symbol is actually 5x. Fallback returns its actual
level to activation logs. Previously confirmed symbols retain their live level
across restarts instead of being reset to 5x.

One async worker per process polls source/venue-scoped BatchExec and ChaseExec
Redis targets, factual positions and complete open USD-M orders every 60 seconds.
It checks the maximum of factual exposure, aggregate original target, and the
account position after all live buys or all live sells. An actual -2027 order
rejection also triggers a check, covering order-price/mark-price boundary cases.
The synchronous rejection callback only records local evidence; it performs
no network or disk I/O. Rejection evidence expires after 120 seconds; continued
cap breaches or fresh rejections keep the issue active.

Before a lowering sequence, the worker refreshes signed account-specific leverage
brackets and open orders. Before each mutation it refreshes holdings and
available collateral. It chooses the highest
permitted lower level whose bracket covers the exposure and requires enough
available initial margin for the complete exposure, with a 1% reserve. It
serializes automatic mutations with leverage initialization and verifies both
the acknowledgement and readback. Insufficient collateral, incomplete evidence,
concurrent level changes and insufficient 3x capacity leave execution blocked
by the exchange's existing risk checks; no targets are resized. It never
lowers below 3x or automatically raises a successful fallback back to 5x.

Every HTTP client explicitly binds the current first configured trading IP,
resolving unspecified bindings locally without sending packets. Proxies and
redirects are disabled. This worker has its own Redis connection and never
holds the target reloader or Manager publication lock while doing exchange I/O.
It does not change account mode, trading egress, targets or order history.

The current Redis diagnostic contract is `<source>:binance-futures:exec_leverage_guard`:
`updated_at_us`, optional source-level `error`, and `symbols` containing factual
`leverage`, `max_notional`, `required_notional` and optional per-symbol `error`.
Exec alone writes it; Manager's `cta_monitor` reads it. Lower levels generate
an order-channel notice, failed checks/margin/cap issues generate alarms, and
updates older than 180 seconds generate a worker-stale alarm. By operator
instruction on 2026-11-15, successful 4x/3x fallback sends one informational
`[通知]` per observed level transition, without periodic repeats or recovery
messages, and does not count as an unresolved fault in heartbeats. Failed
notification delivery still retries until acknowledged. Monitoring state is
in memory, so restarting the monitor observes the current setting once again.
Actual errors retain the existing transition/repeat/recovery policy. Redis
retains diagnostics across restarts. Local rejection
and in-flight initialization evidence is memory-only; startup initialization
retries failures, and the periodic factual/target check rebuilds current risk.

## el01 activation

On 2026-11-15 the operator authorized preferred 5x with 4x/3x fallback.
Enable only `/home/el01/binance_exec_trade07` (`dzy_group0807`) initially.
ONEUSDT was separately approved and changed to 4x with a 200,000 USDT cap;
its previously blocked targets then completed. Other deployments remain opt-out.
Runtime binary, hashes and process verification are recorded in Manager's
`docs/el01_dzy_btc_monitor_20261115.md` and the environment release receipt.
