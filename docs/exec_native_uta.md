# Native UTA Exec: Bybit and Bitget

Both BatchExec and ChaseExec support `bybit-futures` and `bitget-futures` through
the native backend. Each environment owns USDT perpetuals only. Spot, USDC,
COIN-M and RapidX are rejected. Keep separate env directories, Redis prefixes,
IPC namespaces, persistence and listeners for each account and market.

Bybit requires UTA 1.0/2.0, cross or portfolio margin, and one-way positions.
Bitget requires a ready unified/hybrid UTA, basic/advanced/delta account level,
one-way holding mode and crossed USDT symbol margins. Startup observes these
settings; it never changes account/holding mode. Private queries bind the first
explicit `trade_engine.toml local_ips` address with proxies and redirects disabled.
Bybit uses `BYBIT_API_KEY` / `BYBIT_API_SECRET`; Bitget also requires
`BITGET_API_PASSPHRASE` alongside its key and secret.

Manager must publish a complete current `market_rules` cache before startup.
Exec validates market/backend, timestamp, symbol identity and decimal filters
before cancellation. Only Bybit `Trading` and Bitget `online` contracts are
tradable. Missing/inactive symbols block new orders; the existing untradable
position reconciliation handles factual account changes without an impossible
close-order loop. Exec never polls public order rules independently.

Starting exec-pre-trade cancels and confirms all open orders in its USDT market.
Bybit uses linear/USDT cancellation and rechecks until empty, handling the
500-order cancellation limit. Bitget also cancels pending strategy orders
before clearing ordinary USDT orders. Errors, malformed responses or timeout
abort startup. Account collateral/risk remains account-wide; order, fill and
position streams stay within the configured market.

The default leverage is set and read back per symbol, and revalidated after
restart. Bybit portfolio margin bypasses fixed leverage, which is inapplicable
in that mode. ChaseExec uses confirmed cancel/repost for repricing on these
venues; it never overlaps the old maker order with its replacement.

Detailed Bybit execution and Bitget fill subscriptions retain exchange-owned
fills even with nonnumeric client IDs. Only documented execution identities
qualify as forced closes. Bitget joins order execution type to factual fills in
either arrival order; unmatched external fills remain audit evidence. Forced
fills are persisted as unmatched trades and uniform fills attributed to
`exchange_forced_close:<reason>:order=<native-id>:trade=<native-id>`, with no CTA
strategy identity. Funding and liquidation ledger penalties are not fill fees.

Deployment uses the existing scripts, for example:

```bash
scripts/deploy_exec.sh --env-name bybit_exec_trade02 --venue bybit-futures --skip-build
scripts/deploy_exec.sh --env-name bitget_exec_trade01 --venue bitget-futures --skip-build
```

Build release artifacts locally on synchronized `arbmm`, verify checksums on
publication, and choose distinct Viz/Config ports when accounts share a host.
`deploy_exec.sh` does not start processes; `start-exec.sh` never starts trade_signal.
A fresh deployment still needs credentials, explicit trading IPs and Manager
source registration. These changes do not migrate existing intra positions or
start the reserved SG Exec environments.

Authoritative external contracts:
[Bybit account modes](https://bybit-exchange.github.io/docs/v5/account/account-info),
[Bybit cancellation](https://bybit-exchange.github.io/docs/v5/order/cancel-all),
[Bybit execution identities](https://bybit-exchange.github.io/docs/v5/enum),
[Bitget account settings](https://www.bitget.com/docs/catalog/account/account-settings),
[Bitget order management](https://www.bitget.com/docs/catalog/trading/order-management),
[Bitget strategy orders](https://www.bitget.com/docs/catalog/trading/strategy-trading),
[Bitget detailed fills](https://www.bitget.com/docs/uta/websocket/private/Fill-Channel).
