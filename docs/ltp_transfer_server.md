# LTP Transfer Server

Independent Rust service and React / Ant Design console, reached through the
existing Delist login at `/delist/transfers/`. Delist owns authentication; the
transfer service owns account discovery, previews, execution and durable receipts.
No transfer code or LTP credentials are loaded into `delist_risk_server`.

## Runtime

- Host: `jp-meta-elvpn`; environment: `~/ltp_transfer_server`.
- Binary / Cargo package: `ltp_transfer_server`.
- Listener: `127.0.0.1:8788`; existing public Nginx port `4191`.
- CPU: housekeeping `0-5`; signed LTP HTTPS source: `172.31.35.228` on `ens41`.
- Configuration: `config/ltp_transfer_server.toml`.
- Dedicated credentials: `config/credentials.env`, owner-only mode `600`, with
  `LTP_TRANSFER_API_KEY` and `LTP_TRANSFER_API_SECRET`. Never use an exchange-native
  key or silently fall back to a strategy's trading credentials.
- Audit store: `data/transfers/*.json`, owner-only files, fsync before submission.
- Log files: `logs/out.log` and `logs/error.log`; stop preserves a copy under `run/`.

The child wrapper loads credentials after supervisor startup, so the dedicated
credentials do not enter the PM2 process registry. No credential reaches the
browser, static bundle, deployment manifest or application log.

Use only environment-local wrappers:

```bash
cd ~/ltp_transfer_server
./scripts/start_ltp_transfer_server.sh
./scripts/stop_ltp_transfer_server.sh
```

## Account scope and execution

The installed key currently discovers Binance Classic DMA `81857` and OKX
multi-currency DMA `84219`. It has no visible LTP Funding Account or RapidX
Portfolio. The server displays the current API-discovered scope rather than an
operator-maintained list; frozen accounts are excluded. It supports the
documented Binance Classic/PM and OKX wallet selections. A main-user key can also
discover Funding and RapidX Portfolio accounts. Other DMA wallet modes remain
unavailable until their balance and enum mappings are implemented.

Account IDs and amounts remain decimal strings in the web API. Portfolio
transfer limits come from `maxTransferable`; DMA balances conservatively use the
smaller of `available` and `withdrawAvailable`. Network availability comes from
one direction-specific LTP currencies query with both `fromAccount` and
`toAccount`, using its outgoing `withdrawEnable` flag. The incoming-query
`depositEnable` field does not gate an outgoing query.
Preview checks account scope, wallet selection, amount,
precision, network minimums and fresh transferable funds. Execute repeats the
checks before reserving its audit record.

The preview expires after 120 seconds. Its UUID is the `clientOrderId` of the
single LTP submission. A replay returns the same durable attempt, including
after restart. A source account with `submitting`, `pending` or `unknown` status
cannot submit a replacement. Request timeouts and unclassified exchange errors
remain unresolved until factual query evidence settles them. Never delete an
unresolved audit record or replace its client ID to retry a transfer.

An accepted request is not proof of completion. Final records retain actual
received amount, network fee and actual rapid-transfer flag when LTP returns
them. `rapidTransfer=true` is a preference, with standard transfer fallback;
Funding routes are not eligible. The server never places trades, borrows,
repays, changes account mode, or calls the external-address withdrawal API.

Read-only account APIs can return history outside the key's current account
assignment. Remote history is therefore filtered so both endpoints must be in
the current discovered account scope. Locally created audit records remain
available for reconciliation. A failed history request is shown as unavailable
alongside the local evidence; it must not be described as an empty exchange
history.

## Authentication and API

Nginx authenticates the whole `/delist/transfers/` subtree through an internal
Delist subrequest. The independent server also verifies the real session cookie
with Delist on every protected request. Actor identity comes from that verifier's
response. Writes require one exact configured same-origin `Origin` header;
browser-supplied actor and original-method headers cannot grant access. Private
responses are not cached. The backend has no public listener or permissive CORS.

All paths below are relative to `/delist/transfers/`:

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `api/catalog` | Current key-scoped accounts and currency metadata |
| GET | `api/balances` | One selected account/wallet's transferable balances |
| GET | `api/routes` | Available currencies and networks for the selected route |
| POST | `api/transfers/preview` | Validate and prepare; moves no funds |
| POST | `api/transfers` | Submit one prepared preview |
| GET | `api/transfers` | Current-scope LTP history and durable local attempts |
| GET | `api/transfers/:client_order_id` | Query the original operation |

For a definitive execute rejection before submission, the server returns
`submitted:false` together with the matching preview ID. All other missing or
failed responses require reconciliation. Browser storage retains an attempted
preview ID across reloads; only GET requests are automatically repeated.

## Build, verify and publish

```bash
cargo fmt -p ltp_transfer_server --check
cargo test -p ltp_transfer_server
npm ci --prefix web/ltp_transfer/frontend --no-audit --no-fund
npm run build --prefix web/ltp_transfer/frontend
cargo build --release -p ltp_transfer_server
python scripts/deploy_ltp_transfer_server.py
python scripts/deploy_ltp_transfer_server.py --execute --skip-build
```

The deploy tool defaults to dry-run. Execution verifies synchronized `arbmm`,
publishes only local release artifacts using SCP and SHA-256 verification, and
atomically installs them. It preserves existing private configuration. It adds
only the dedicated Nginx include and Delist navigation link, retaining existing
live page differences and gateway locations. It starts or restarts only this
environment through its wrappers; Delist and trading processes are not restarted.
Backups and a deployment receipt live under `run/deploy_<UTC>/`; the local receipt
and hashes live under `artifacts/ltp_transfer_server/`.

Unit/integration tests use a local simulated LTP and login verifier, covering
authentication, CSRF, decimal precision, fresh-balance checks, replay protection,
ambiguous submission, evidence identity, corruption and exclusive store ownership.
The frontend mock browser checks are documented in `web/ltp_transfer/README.md`.
Production acceptance must never move money merely to test deployment.

Authoritative contracts: [API Management](https://apidocliquidity.readme.io/docs/api-management),
[Transfer](https://apidocliquidity.readme.io/reference/asset-transfer-between-accounts-support-innercross-exchange),
[Account Types](https://apidocliquidity.readme.io/docs/account-types), and
[Rapid Transfer](https://apidocliquidity.readme.io/docs/rapidtransfer).
