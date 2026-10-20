# Delist Risk Server

最后更新：2026-10-20 UTC

Authenticated HTTP service for trading-book delist / opening-restriction risk on
Binance, Bitget, Gate, and Bybit. Official market snapshots plus announcement LLM
extracts land in one book. Full dump, grouped by exchange.

This is a **risk hint**, not a product timetable. Spot, margin and borrowing
cutoffs associated with a margin/spot delist use `{exchange}-margin`.
Futures / perpetual use `{exchange}-futures` or
`{exchange}-coin-futures`.

## Public URL

On `jp-meta-elvpn`, nginx port **4191** reverse-proxies `/delist/` to
`127.0.0.1:8787`.

```text
http://<jp-host>:4191/delist/
http://<jp-host>:4191/delist/healthz
http://<jp-host>:4191/delist/venues
http://<jp-host>:4191/delist/risk
http://<jp-host>:4191/delist/accounts
http://<jp-host>:4191/delist/removal-candidates
http://<jp-host>:4191/delist/removals
http://<jp-host>:4191/delist/dump-candidates
http://<jp-host>:4191/delist/dump-transitions
http://<jp-host>:4191/delist/flatten-candidates
http://<jp-host>:4191/delist/flatten-executions
http://<jp-host>:4191/delist/status
```

Loopback (same host):

```text
http://127.0.0.1:8787/healthz
```

All page and data endpoints require the `delist_session` cookie. Visit
`/delist/` to log in; credentials never belong in URLs. Manual flatten retains
its separate operation token in addition to the login session.

## Login and FR panel access

Delist is the entry point for the nine JP FR environments: Binance arb02/03/04,
Gate arb01/02/03, and Bitget arb01/02/03. After login, use the account row's Viz
or Config link. `/delist/open?env=<slug>&panel=viz|config` grants that environment
to the current session and redirects to its existing `/fr/<slug>/` route.
The launch link must originate from the delist board. A direct panel URL without
a grant redirects to the board, including when the user has already logged in.
Refreshes and navigation within an opened environment keep working.

Sessions are random, server-owned tokens with a 12-hour absolute lifetime;
cookies use HttpOnly, SameSite=Strict, Path=/, and Secure for an HTTPS origin.
Logout revokes the session and its grants; restarting delist also clears them.
Password verification uses PBKDF2-HMAC-SHA256 with a random salt and 600,000
iterations. Login attempts are limited per proxy-observed client IP. Mutating
requests and websocket handshakes require the configured same-origin header.
Neither shared passwords nor password hashes are checked into Git.

Bootstrap the private, mode-0600 credential file interactively:

```bash
python3 scripts/configure_delist_access.py --username <user> \
  --public-origin http://<jp-host>:4191 --output config/delist_access.json
```

`start_delist_risk_server.sh` passes `--access-config` (override with
`DELIST_ACCESS_CONFIG`). Missing or invalid credentials prevent startup.
The backend defaults to `127.0.0.1:8787`; `public_origin` must match the browser's
scheme, host and port. Provision the private file before the first authenticated
release; publishing must preserve it.

Nginx checks the same session and per-environment grant for all seven FR
locations: static Viz, Config with/without trailing slash, `/ws`, `/fr_ws`,
`/healthz`, and `/snapshot`. Config subpaths inherit the gate. Internal auth
request headers are set by Nginx, and its verify route cannot be called publicly.
Only actual loopback GET requests to the nine `/snapshot` URLs bypass browser
login, preserving automatic delist position checks; this exception grants no
configuration write or other panel access.

`scripts/protect_delist_routes.py` renders an in-place site patch and refuses
partial route inventories. Install `config/nginx_delist_access.conf` and
`config/nginx_delist_fr_gate.conf` under `/etc/nginx/snippets/` with names
`delist_access.conf` and `delist_fr_gate.conf`, validate the rendered site with
`nginx -t`, then atomically install and reload. The general mapping-only Nginx
setup wrapper refuses to overwrite a site containing these access gates.

`scripts/delist_fr_port_guard.sh` blocks non-loopback IPv4/IPv6 connections to
the 27 Config/Viz/FR dashboard backend ports and delist's 8787. The systemd unit
`scripts/systemd/delist-fr-port-guard.service` reapplies the dedicated INPUT chain
after reboot. Install the script at `/usr/local/sbin/` and the unit at
`/etc/systemd/system/`, then enable/start the unit. This avoids restarting any
trading or dashboard processes. To roll back the port rules explicitly, use
the script's `--remove`; stopping the unit alone deliberately leaves rules in
place. Retain access gates and port rules together: removing only the port
guard exposes the existing wildcard-bound FR backend listeners.

## Current JP deployment

On 2026-10-20 06:40 UTC, published the authenticated gateway from `arbmm`
commit `1abb12cc` after locally building and verifying the release. Binary
SHA-256: `cdea480dfda4cdc20378f8eab77026e508c19a31e31fa1b39e12def8b4c2920a`.
Credentials were provisioned in `config/delist_access.json` with mode 0600;
only a salted password hash is stored. The backend now listens on loopback,
the nine FR route groups use Nginx access checks, and the IPv4/IPv6 direct-port
guard is enabled persistently. No FR process was restarted: all 72 observed
FR process PIDs remained unchanged. Delist retains CPU mask `0-5,48-95`.

Browser verification covered all nine Viz/Config panels, all 18 websocket
handshakes, Config API reads, all anonymous entry surfaces, spoofed loopback
headers, denied cross-origin writes, and logout revocation. A login alone does
not grant a panel until its delist entry link is used. All 28 backend ports
reject external access; local position snapshot reads still work. PostgreSQL,
NAV and both Redis sources remain healthy, with no snapshot/dump errors or
flatten candidates. Validation passed 1323 Rust tests (3 ignored), 3 route
renderer tests, formatting and frontend syntax checks. The release manifest
and browser evidence live under
`artifacts/delist_risk_server/access_20261020T063845Z/`; remote rollback files
are under `run/access_20261020T063845Z/before/`. The follow-up frontend patch
keeps the logout button visible on mobile.

On 2026-10-20 06:20 UTC, republished the lending-only classification fix to
`/home/ubuntu/delist_risk_server` on `jp-meta-elvpn`. Built locally from the
synchronized `arbmm` worktree at `bc7223fe` with the current delist changes;
uploaded the release and frontend to temporary paths, verified SHA-256, and
atomically installed both before restarting through the existing environment
start script. Release SHA-256:
`f80f6b8f968741119305299fb9d5b67ae5364c9f1841d9fae98711c7fba05af4`.
Frontend SHA-256:
`d0179b769d5296af79d2f21b49b43b1b76264f055f080bfc77754fb0447a7e12`.
Validation passed 1320 library/service tests with 3 ignored, Rust formatting,
and frontend JavaScript syntax checks.

All three Gate accounts now report `tone=ok`, `risk_n=0`: the five ARIA/BANK
lending-only hits are excluded without changing their 68/68/45-symbol trading
universes or account configuration. All 147 explicit delist/disable-open events
returned before deployment remain present. Cached extractions are filtered on
read; no forced model re-extraction was requested. The public page and health
endpoint return HTTP 200; PostgreSQL, NAV and JP/SG Redis are healthy, with
`degraded=false`. Both automatic dump and flatten candidate lists are empty.
The service and SG tunnel retain CPU mask `0-5,48-95`; credentials, startup
scripts, listeners and network configuration are unchanged. Deployment evidence
is in `artifacts/delist_risk_server/20261020T061825Z/`; the remote environment's
`run/deploy_20261020T061825Z/` holds the previous binary/page, risk-book snapshot,
before/after checks and startup log for rollback.

On 2026-10-20 03:48 UTC, the operator requested removal of all currently
flagged Binance FR symbols after a direct Binance account audit. The
environment-local Config APIs removed VELODROMEUSDT from arb02; HOTUSDT,
IOTXUSDT, THEUSDT, TSTUSDT, VELODROMEUSDT and XVGUSDT from arb03; and the same
set except XVGUSDT from arb04. Matching entries were removed from the forward,
dump and UniMMR close lists. Backward lists were empty, and position-dump lists
were empty or absent. Remaining account universes are 68, 71 and 42 symbols
respectively. Config read-back and running trade_signal logs confirmed the
updates; `/accounts` reports `tone=ok` and `risk_n=0` for all Binance accounts.
The exchange-wide risk declarations remain available.

All flagged balances and positions were zero except arb04 VELODROME:
0.097775 tokens in the margin wallet and a 3-token perpetual short, worth
approximately 0.0035 and 0.1065 USDT respectively. Those dust positions were
retained; this operation submitted no orders and restarted no processes.
Before/after lists and API responses are backed up in each environment under
`run/remove_delist_symbols_20261020T034829Z.json`. This explicit manual removal
does not change the automatic removal policy or CPU/source-IP allocations.

On 2026-10-19 02:08 UTC, `/delist/` returned HTTP 502 because neither the
backend nor its SG Redis tunnel was running after the October 18 host reboots.
Both were restored through the environment-local
`/home/ubuntu/delist_risk_server/scripts/start_delist_risk_server.sh`, using
the existing binary and configuration. No reboot persistence was added; include
this script in host recovery. The public page and health endpoint returned
HTTP 200, with `ok=true`, `postgres=true`, `degraded=false`, fourteen monitored
accounts, and healthy JP/SG Redis. Both processes had zero restarts and CPU mask
`0-5,48-95`; source IPs and listeners were unchanged. The existing automatic
risk policy resumed and added arb04 VELODROMEUSDT to `fr_dump_symbols`, removing
it from `fr_fwd_trade_symbols`; the scan reported no flatten candidates.

Republished on 2026-10-14 22:46 UTC in `/home/ubuntu/delist_risk_server` on
`jp-meta-elvpn` from the synchronized local `arbmm` worktree at `6ff0248e`
with current local fixes. The Binance margin catalog now requires both active
spot status and `isMarginTradingAllowed=true`. The exact locally built release
was uploaded, SHA-256 checked and atomically installed before the environment
start script restarted the service. Artifact SHA-256:
`8c31f9e3e3fe53b99efd640d2cdba2d8750b64ec666a0f5e1a0ccae6314aa702`.
Existing configuration and frontend were retained. Live `/healthz` reports
`ok=true`, `postgres=true`, `degraded=false`; `binance_fr_arb04` reports margin
catalog removals for HOTUSDT, THEUSDT, IOTXUSDT and TSTUSDT. Their active futures
legs still prevent whole-pair Redis removal under the existing removal policy.
After arb04 signal resumed, HOTUSDT margin-order submissions still returned
HTTP 400 / code 51023 (`Trading pair does not exist.`). Catalog risk recognition
does not by itself remove an active futures/unsupported margin pair or suppress
every strategy submission; this release does not change that removal policy.
The process inherits CPU mask `0-5,48-95`; network configuration is unchanged.
Build and runtime verification are recorded under
`artifacts/binance_fr_arb04_republish/20261014T2227Z/`.

On 2026-10-15 02:30 UTC, the operator explicitly requested removal of HOTUSDT
and THEUSDT from arb04. The environment-local FR Config API removed only these
symbols from `fwd_trade_symbols` and `unimmr_close_symbols` (46 to 44 each).
The signal process confirmed hot reload at 02:30:52 UTC. The account universe
now has 45 symbols; the remaining margin catalog risk hits are IOTXUSDT and
TSTUSDT. This manual configuration change does not alter the shared automatic
removal policy. Before/after lists are backed up in the arb04 environment at
`run/remove_hot_the_20261015T0230Z.json`.

The following is the historical 2026-10-05 deployment audit. Verified then in
`/home/ubuntu/delist_risk_server` on
`jp-meta-elvpn`: the Rust release from `a69dcd84` and the launch-script fix from
`74060ee3` are published. Artifact SHA-256:
`e5d655dd78433a1510ef484b8184c5b4bc1d06a0e8009c0346599df65e61d862`.
Frontend SHA-256:
`f7159a48b03e02aa9a77e6f4d458b7fd8056a07792f14e648a2ebdf7e11311d9`.

The board shows each exchange's latest analysed announcement above its account
table. All fourteen account rows stay expanded and display **进入看板** and
**进入配置** side by side. Live verification covered 1365px, 800px, 390px and
320px layouts, exact Viz/config destinations, and page refreshes and filters.
Thirteen public Viz entries return HTTP 200; Binance Exec's `/exec_trade01/`
requires an existing Manager session and its upstream page/health both return
HTTP 200. Service and SG tunnel are online with no supervisor restarts;
PostgreSQL and both Redis sites are healthy.

At 00:44 UTC, the existing enabled NAV strategy `gate_fr_arb03` was missing
from the monitor because the `delist` service user lacked its strategy read
grant. The single grant was added to `nav_user_strategy_grants`; the next
60-second catalog refresh picked it up without restarting either service.
`/accounts` now contains fourteen accounts, including all three Gate FR
accounts. `gate_fr_arb03` loads 45 online symbols and 45 UniMMR close symbols,
has healthy Redis/catalog coverage, and currently has no risk hits.

All three SG Bybit accounts load their Redis universes and have catalog
coverage (3, 13, and 6 symbols). Twelve recent Bybit announcements have successful
persisted extractions. The 2026-10-05 UTC snapshot contains all eleven catalogs
and 13,442 symbols. After the corrected launch, Gate connects once and reuses
previous successful extractions; the repeated `101657` announcement generated
no new model call during verification. The service retains CPU affinity 0–5
and its existing default network route.

## Cadence

| Source | Interval | Notes |
| --- | --- | --- |
| Announcements (Binance CMS delisting catalog, Bitget `symbol_delisting`, Bybit `delistings`) | **24h** | List discovery plus article detail; raw JSON stored in Postgres |
| Gate announcement WS | persistent | Push stream rather than a polling check; reconnects on drop |
| Official snapshots (Gate `delisting_time` / `in_delisting`, Bitget `offTime`, Binance SAPI if keys, futures schedule) | **24h** | Replaces that source in the book |
| Complete public product catalogs | **24h**, plus **00:00 UTC** | Drives current listing state and confirmed Redis removal; the midnight fetch is persisted to Postgres |
| NAV strategy catalog | **60s** | Sole account inventory for the board and account controls; the last successful catalog is retained on failure |
| FR `fr_unimmr_close_symbols` watch | **60s** | Redis-only read; an empty or missing key on any `funding_rate` account is a finding |

LLM extraction reuses successful results for the same exchange, announcement ID,
title, and body. Replayed publication timestamps do not trigger another request.
Content fingerprints persist in `llm_status`; existing successful rows recover
fingerprints from their stored original announcement. Concurrent duplicates
share an extraction, at most two model requests run together, and failed
attempts wait at least five minutes before retrying. Explicit `--force-llm-ids`
is the only way to force unchanged successful announcements on startup.

Gate WebSocket and daily announcement extraction run independently of the
60-second account/position timers. Model latency cannot cancel the connection
or stop its heartbeat. LLM / fetch failures never block the other source;
reasons are queryable at `/status`.

## Venues

| venue | Meaning |
| --- | --- |
| `binance-margin` / `bitget-margin` / `gate-margin` / `bybit-margin` | Spot, margin, loan |
| `binance-futures` / `bitget-futures` / `gate-futures` / `bybit-futures` | USDT-M perpetual / delivery |
| `binance-coin-futures` / `bitget-coin-futures` / `gate-coin-futures` | Coin-M |

`BINANCE_API_KEY` / `BINANCE_API_SECRET` are required for Binance spot/margin
SAPI (`delist-schedule`, `asset/tags`). Without them, `binance-margin` still
gets LLM extracts from CMS announcements; official SAPI rows show as fetch
failures in `/status`. Bitget, Gate, and Bybit public APIs need no key.

Bybit discovery uses `GET /v5/announcements/index?locale=en-US&type=delistings`;
complete bodies come from the official article page's `__NEXT_DATA__`.
The lookback is at least 37 days. Both discovery and article-detail failures
are visible in `/status`. Spot catalogs use `category=spot` without pagination;
USDT perpetual catalogs use every `category=linear` page, exclude USDC/delivery
contracts, and flag upcoming `deliveryTime` as pending. Bybit accounts are only
covered after all their venue catalogs have loaded successfully.

Official contracts: [announcements](https://bybit-exchange.github.io/docs/v5/announcement),
[instruments](https://bybit-exchange.github.io/docs/v5/market/instrument).

## Endpoints

### `GET /healthz`

Process liveness plus book size.

```json
{
  "ok": true,
  "updated_ms": 1787800000000,
  "events": 31,
  "announcements": 12,
  "degraded": false,
  "postgres": true
}
```

`degraded` is true when any fetch/LLM source last failed.

### `GET /venues`

Summary per venue, grouped by exchange. Same filters as `/risk`.

```bash
curl -sS 'http://<host>:4191/delist/venues'
curl -sS 'http://<host>:4191/delist/venues?exchange=gate'
```

```json
{
  "ok": true,
  "as_of_ms": 1787800000000,
  "venues": {
    "gate": [
      {
        "venue": "gate-margin",
        "exchange": "gate",
        "abnormal": true,
        "count": 26,
        "next_utc": "2026-09-02T03:00:00Z"
      }
    ]
  }
}
```

### `GET /risk`

Full dump. Events keep the same fields across exchanges, but are grouped under
`exchanges.binance` / `exchanges.bitget` / `exchanges.gate` / `exchanges.bybit`.

| Query | Default | Notes |
| --- | --- | --- |
| `venue` | all | optional filter, e.g. `binance-margin` |
| `exchange` | all | optional filter: `binance` / `bitget` / `gate` / `bybit` |
| `days` | 30 | Horizon for dated events |
| `include_past` | false | Include events older than 7 days |

```bash
curl -sS 'http://<host>:4191/delist/risk'
curl -sS 'http://<host>:4191/delist/risk?exchange=binance'
curl -sS 'http://<host>:4191/delist/risk?venue=gate-margin'
curl -sS 'http://<host>:4191/delist/risk?days=30&include_past=true'
```

```json
{
  "ok": true,
  "as_of_ms": 1787800000000,
  "abnormal": true,
  "count": 2,
  "exchanges": {
    "binance": {
      "exchange": "binance",
      "abnormal": true,
      "count": 1,
      "items": [
        {
          "exchange": "binance",
          "venue": "binance-futures",
          "action": "delist",
          "utc": "2026-08-26T09:00:00Z",
          "status": "due",
          "assets": ["ICX", "SCRT", "STORJ"],
          "symbols": [],
          "note": "Binance Futures will close positions, automatically settle, and delist the contracts after settlement.",
          "source": "llm_extract",
          "title": "Binance Will Delist ICX, SCRT, STORJ on 2026-09-03",
          "url": "https://www.binance.com/en/support/announcement/detail/d72915ed7a60473b92f0818d959a227a",
          "announcement_id": "d72915ed7a60473b92f0818d959a227a",
          "listing": "pending"
        }
      ]
    },
    "gate": {
      "exchange": "gate",
      "abnormal": true,
      "count": 1,
      "items": [
        {
          "exchange": "gate",
          "venue": "gate-margin",
          "action": "delist",
          "utc": "2026-09-02T03:00:00Z",
          "status": "upcoming",
          "assets": [],
          "symbols": ["TRCUSDT"],
          "note": "tradable",
          "source": "gate_market",
          "title": "gate spot delisting_time",
          "url": "",
          "announcement_id": "gate_market"
        }
      ]
    }
  }
}
```

`abnormal` is true when any returned item is not `past`. Event `status`:

- `upcoming` — `utc` still in the future
- `due` — `utc` within the last 7 days
- `past` — older than 7 days (hidden unless `include_past=true`)
- `unknown` — no usable `utc`

`action`: `delist`, `disable_open`, `disable_margin`, `disable_loan`.
Monitoring/watchlist tags, trading-bot removals, and other risk labels are excluded from the risk book.

Standalone lending/borrowing suspensions, collateral eligibility changes and
mandatory loan repayments do not count as delisting. `disable_margin` and
`disable_loan` appear in risk queries only when the same source announcement
also contains an explicit `delist` on the same exchange and venue for the
affected assets/pairs. Pair-only removals remain pair-only. The association is
checked before date/horizon filters so an early borrowing cutoff is retained
even when the actual delist is further away. Actual margin-pair trading removal
is still `delist`, including when ordinary spot trading continues.

This rule is enforced when reading the book, including cached model outputs;
it does not require deleting extraction history or forcing model retries.
For example, Gate article `102264` (ARIA/BANK lending cessation and automatic
loan repayment, with spot trading unaffected) produces no account delist hit.
The extraction prompt also explicitly excludes this case.

`listing` comes from public exchangeInfo / instrument catalogs (no API key):

Binance `binance-margin` requires both an active spot status and
`isMarginTradingAllowed=true`. A spot pair that remains `TRADING` after margin
removal is reported as unavailable on the margin leg. Missing or malformed
permission fields reject the catalog refresh and retain the last complete
snapshot; they do not synthesize mass delistings. The July 2026 HOT/THE margin
removal is one example of this distinction.

- `listed` — the pair is still in the live book
- `pending` — still in the book, but official `offTime` / `delisting_time` / `in_delisting` / nearby `deliveryDate` is set
- `delisted` — the pair is absent or inactive on that venue, including margin permission removal
- `unknown` — that venue catalog has not been fetched yet

Spot pair removals such as `SUI/BNB` are stored only as `symbols=["SUIBNB"]`. They do not mark `SUI` / `SUIUSDT` as delisted.

### `GET /accounts`

Per NAV-configured strategy: Redis online universe ∩ `/risk`. Same filters as
`/risk`. `delist_risk_server` does not keep a separate handwritten account
list. It reads `slug`, display alias, host, strategy kind, and exchange from
`/nav-api/strategies`; a strategy added to NAV appears here after the next
catalog refresh. The NAV `delist` user must also have a strategy grant for the
new slug; otherwise NAV omits it from the authenticated response and `/accounts`.
For a newly registered monitored account, include the `delist` user's strategy
read grant in onboarding and verify that exact slug through `/delist/accounts`
after the next 60-second refresh. A successful `nav_strategies` source status
only confirms a successful authenticated fetch, not access to every enabled
NAV strategy.

The response field `nav_accounts_current` is `true` only after the latest NAV
catalog request succeeds. It is independent from the per-site `redis` health
fields.

```bash
curl -sS 'http://<host>:4191/delist/accounts'
```

`tone`: `ok` / `risk` / `error` / `uncovered`. Pair-only notices (`SUIBNB`) do not flag `SUIUSDT`.

For Binance, Bitget, and Gate, every Redis universe symbol is also compared to
the latest public exchange catalog. A `catalog_removed` hit means that the
pair is no longer present or tradable at that venue; it is a risk hit even when
the service has no matching delist announcement.

The HTML board at `/delist/` uses this endpoint. Style matches crypto NAV manager.
`latest_announcements` contains the latest LLM analysis attempt for each exchange:
title, source URL, publication and check timestamps, success/failure state, last
successful analysis time, and the stored structured `actions`. Selection uses
analysis time, not publication order. These actions use the same lifecycle
qualification as `/risk`, independently of its date/exchange query filters,
and retain elapsed deadlines and events beyond its horizon. An empty action
list after successful analysis means no qualifying lifecycle actions; a failed
attempt is shown separately and any retained actions are labelled with their
last successful analysis time.

Each exchange displays this announcement above its always-visible account table.
The board shows human-readable action/market labels, assets/pairs, UTC deadlines,
analysis and publication times, and a link to the original article. Every account
has side-by-side **进入看板** (Viz) and **进入配置** links in the **导航** column.
Configuration uses the authoritative NAV `config_url`, including SG URLs and
the Exec Manager account page. JP Binance Exec Viz uses the existing
`/exec_tradeNN/` Manager gateway and requires a Manager session. Account
rows remain expanded through refreshes and strategy filters. Rendering reuses
persisted extractions and never requests additional LLM analysis.

Strategies whose exchange or strategy kind has no delist control rule remain
visible with `tone=uncovered`. If a NAV refresh fails, the board retains the
last successful catalog, `/status` marks `nav_strategies` failed, and automatic
Redis/flatten mutations pause until a fresh catalog succeeds.

Every `funding_rate` account also reports its `fr_unimmr_close_symbols` Redis
list: `unimmr_close_n` is the parsed symbol count and `unimmr_empty` is `true`
when the key is missing or parses to no symbols (a site Redis read failure
leaves both unset). An empty list upgrades the account to `tone=risk` unless
the tone is already `error`. This is a dedicated watch, not a delist event:
when UniMMR drops below the trigger line the env would have no algorithmic
close candidates.

A background scan runs every `DELIST_UNIMMR_LIST_INTERVAL_SECS` (60s). On a
confirmed empty transition it pushes a `critical` Telegram (`UniMMR平仓列表为空`),
repeats every hour while the list stays empty, and sends one `info`
`UniMMR平仓列表恢复` when the list becomes non-empty again. Telegram requires
`PRE_TRADE_NOTIFICATION_URL`; without it the finding is still visible in
`/accounts` and `/status` (source `fr_unimmr_close_list`).

### `GET /removal-candidates`

Returns the Redis symbols eligible for automatic removal. A symbol is eligible
only when every venue used by that account is `delisted` in one complete,
successful Binance/Bitget/Gate/Bybit catalog refresh and a fresh account snapshot
shows both FR legs below 1 USDT. A missing margin or futures leg keeps the
symbol in dump while another account venue remains listed. Market-making
accounts use their futures venue. CTA maps are excluded. The audit `venues`
field records every venue that confirmed the removal.

### `GET /removals`

Returns the latest automatic Redis removal audit rows from local Postgres. The
service inserts a `pending` row before changing Redis and updates it to `success`
or `failed` afterward. `changes` records Redis key names and item counts; process
logs also include the audit ID, account, symbol, and result.

Redis lists are updated with a compare-and-set Lua script. If a config service
changes any involved key after it was read, the removal aborts and retries on a
later catalog refresh. PostgreSQL unavailability also blocks the mutation.

### `GET /dump-candidates`

Returns funding-rate symbols that have a dated upcoming/due `disable_open` or
`delist` event and a fresh account `/snapshot`. The affected venue leg must have
an absolute position of at least `DELIST_POSITION_RISK_THRESHOLD_USDT` (50 USDT
by default). This local snapshot scan runs every 60 seconds and does not poll an
exchange announcement or instrument API.

With `DELIST_AUTO_DUMP_POSITION_RISK=1`, the service atomically removes each
candidate from both `fr_fwd_trade_symbols` and `fr_bwd_trade_symbols` and adds it
to `fr_dump_symbols`. It does not submit or cancel orders. Missing/stale
snapshots, PostgreSQL failure, missing Redis keys, or concurrent Redis changes
block the update.

### `GET /dump-transitions`

Returns the latest automatic open-to-dump audit rows from PostgreSQL table
`redis_symbol_dump_audit`, including the triggering event, snapshot timestamp,
both leg notionals, applied threshold, Redis key changes, and result.

### Final-24-hour flatten

`GET /flatten-candidates` returns positioned FR symbols whose actual `delist`
deadline is within the next 24 hours. The decision value is the larger absolute
USDT notional of the margin and futures legs. The response includes the latest
audit state for each account/symbol/deadline.

The response also includes `position_statuses` for every dated upcoming/due FR
delist risk found in a fresh account snapshot. `closed=true` requires the symbol
to be present in that account's `fr_dump_symbols` and requires both
`abs(open_usdt) < 100` and `abs(hedge_usdt) < 100`. The board keeps the delist
declaration visible and marks such a row as `声明要下架，但已平仓完毕`.

With `DELIST_AUTO_FLATTEN_POSITION_RISK=1`, positions at or below 1000 USDT are
first moved to `dump` and removed from both open lists, then the service runs the
fixed account-local exchange script with `--symbols SYMBOL --mode clear
--execute`. Positions above 1000 USDT are never run automatically: one
`manual_required` audit is created and the existing local notification service
pushes a Telegram alert. Script, Redis, PostgreSQL, timeout, and notification
errors remain visible in `delist_flatten_audit`; a failed automatic execution is
not retried automatically. If a newer position snapshot still shows exposure
after a successful script run, the service marks the source degraded and the
page exposes the manual action instead of repeating live orders automatically.

The page displays final-window position and execution state. `POST /flatten`
backs its manual button. It accepts only `account_slug` and `symbol`, requires a
Bearer token matching `DELIST_FLATTEN_API_TOKEN`, reloads a fresh snapshot,
rechecks the deadline and position, and verifies the account against the current
NAV catalog before resolving the fixed exchange-specific executable. It cannot
accept a command or filesystem path. A successful execution cannot run again
against a snapshot captured before that execution.

`GET /flatten-executions` returns the latest PostgreSQL execution rows. The
`trigger` is `auto`, `manual`, or `manual_required`; `status` is `running`,
`success`, `failed`, or `manual_required`.

### `GET /announcements`

Recently seen announcement metadata (not full bodies). Full raw payloads live
in Postgres `announcements.raw`.

### `GET /status`

Last success time and last error for every fetch / LLM source. Use this when
the book looks empty or stale.

```bash
curl -sS 'http://<host>:4191/delist/status'
```

```json
{
  "ok": true,
  "as_of_ms": 1787800000000,
  "degraded": true,
  "postgres": true,
  "sources": [
    {
      "source": "binance_cms",
      "kind": "fetch",
      "ok": true,
      "last_success_ms": 1787800000000,
      "last_attempt_ms": 1787800000000
    },
    {
      "source": "binance_spot_delist",
      "kind": "fetch",
      "ok": false,
      "last_success_ms": null,
      "last_attempt_ms": 1787800000000,
      "last_error": "missing BINANCE_API_KEY",
      "last_error_ms": 1787800000000
    },
    {
      "source": "llm",
      "kind": "llm",
      "ok": false,
      "last_attempt_ms": 1787800100000,
      "last_error": "llm backup also failed id=...: 401 Unauthorized"
    }
  ],
  "llm_failures": [
    {
      "exchange": "binance",
      "announcement_id": "abc",
      "title": "...",
      "ok": false,
      "last_attempt_ms": 1787800100000,
      "last_error": "401 Unauthorized"
    }
  ]
}
```

Source names:

- fetch: `nav_strategies`, `binance_cms`, `bitget_announcements`, `gate_market`,
  `bitget_instrument_offtime`, `binance_spot_delist`, `binance_margin_delist`,
  `exchange_info`, `bybit_announcements`, `bybit_article_detail`, `schedule:bybit-futures`, `schedule:binance-futures`, `schedule:binance-coin-futures`,
  `schedule:gate-futures`, `schedule:bitget-futures`, `schedule:bitget-coin-futures`
- check: `fr_unimmr_close_list` — lists the empty FR `unimmr_close_symbols`
  accounts or site read errors in `last_error`
- ws: `gate_ws`
- llm: `llm` plus per-announcement rows in `llm_failures`

## Postgres

Database `delist_risk` on `127.0.0.1:5432` stores:

- `announcements` — raw fetched announcement JSON, `first_fetched_ms`,
  `last_fetched_ms`
- `source_status` — last success / last error per source
- `llm_status` — last LLM extract status and input fingerprint per announcement
- `exchange_symbol_snapshot_runs` — one daily run at `00:00 UTC`, including
  completion state, venue count, symbol count, and failure text
- `exchange_symbol_snapshots` — complete daily symbol rows for Binance, Bitget,
  and Gate Spot, USDT-M, and Coin-M, plus Bybit Spot and USDT perpetuals; stores both the exchange symbol and its
  normalized lookup symbol
- `redis_symbol_dump_audit` — position-aware FR open-to-dump operations and
  failures, with the exact event, position snapshot, threshold, and Redis key
  changes
- `delist_flatten_audit` — final-window manual-required decisions and automatic
  or operator-triggered clear executions, including exit status and bounded
  stdout/stderr

The daily snapshot is transactional and date-idempotent. All eleven public
catalogs must succeed before rows are committed. On restart, the service fills
the current UTC date only when no successful snapshot exists; the normal run at
midnight is not overwritten.

Restart recovery loads `announcements`, `source_status`, and `llm_status` back
into memory. Daily symbol snapshots remain historical query data in Postgres.

The in-memory JSON book (`data/delist_risk.json`) is a secondary cache.

## PM2

```bash
# from the repo (builds, scp to jp, creates PG, patches /delist/ in place, starts pm2)
scripts/deploy_delist_risk_server.sh

# on jp
cd ~/delist_risk_server
$EDITOR config/delist_risk_server.env   # LLM / Binance keys; never commit
./scripts/start_delist_risk_server.sh
./scripts/stop_delist_risk_server.sh
npx pm2 logs --namespace delist_risk_server delist_risk_server
```

Deploy never overwrites an existing `config/delist_risk_server.env`.

jp-meta **4191 is a shared public front**. Deploy only upserts `location /delist/`
into the live `crypto_proxy_4191.conf`. It does **not** regenerate the whole
server from `nginx_locations.txt` (that previously dropped `/manager/` and
other hand-maintained routes). `scripts/setup_nginx_4191.sh` now refuses to
overwrite a 4191 conf that already has `/manager/` unless the mapping also
contains `/manager/` or `FORCE_NGINX_REWRITE=1`.

Environment (see `config/delist_risk_server.env.example`):

| Variable | Purpose |
| --- | --- |
| `DELIST_BIND` | default `0.0.0.0:8787` |
| `DELIST_PG_URL` | `postgres://delist_risk:...@127.0.0.1:5432/delist_risk` |
| `DELIST_REDIS_URL` | JP Redis, default `redis://127.0.0.1:6379/0` |
| `DELIST_SG_REDIS_URL` | SG Bybit Redis through a local SSH tunnel, `redis://127.0.0.1:16379/0` |
| `DELIST_SG_REDIS_SSH_HOST` | SSH target for the SG tunnel, default `sg` |
| `DELIST_ANNOUNCEMENT_INTERVAL_SECS` | default `86400` |
| `DELIST_OFFICIAL_INTERVAL_SECS` | default `86400` |
| `DELIST_LISTING_INTERVAL_SECS` | default `86400`; a separate catalog fetch runs at `00:00 UTC` |
| `DELIST_NAV_STRATEGIES_URL` | NAV account source, default `http://127.0.0.1:4191/nav-api/strategies` |
| `DELIST_NAV_STRATEGY_INTERVAL_SECS` | NAV account refresh interval, default `60` |
| `DELIST_UNIMMR_LIST_INTERVAL_SECS` | FR `unimmr_close_symbols` empty-list watch, default `60` |
| `DELIST_AUTO_REMOVE_REDIS` | `1` enables audited confirmed-delisting removal; default `0` |
| `DELIST_AUTO_FLATTEN_POSITION_RISK` | `1` enables real final-window automatic clear; default `0` |
| `DELIST_FLATTEN_WINDOW_HOURS` | actual delist window, default `24` |
| `DELIST_FLATTEN_MANUAL_THRESHOLD_USDT` | larger leg above this value is manual-only, default `1000` |
| `DELIST_FLATTEN_ENV_ROOT` | account deployment parent, default `/home/ubuntu` |
| `DELIST_FLATTEN_TIMEOUT_SECS` | one script timeout, default `300` |
| `DELIST_FLATTEN_API_TOKEN` | required Bearer token for the manual page action |
| `PRE_TRADE_NOTIFICATION_URL` / `NOTIFICATION_API_TOKEN` | existing local notify endpoint and optional token; required for automatic flatten |
| `DELIST_LLM_API_URL` / `DELIST_LLM_API_KEY` / `DELIST_LLM_MODEL` | OpenAI Responses compatible |
| `DELIST_LLM_BACKUP_*` | optional backup endpoint |
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | optional SAPI snapshots |

### SG Bybit universe

Bybit books run on SG. Its Redis stays loopback-only; do not expose port 6379
or point `DELIST_SG_REDIS_URL` at a public IP. On the JP delist host, add the
following to the environment-local `config/delist_risk_server.env` and restart
the service:

```text
DELIST_SG_REDIS_URL=redis://127.0.0.1:16379/0
DELIST_SG_REDIS_SSH_HOST=sg
DELIST_SG_REDIS_TUNNEL_PORT=16379
```

`start_delist_risk_server.sh` removes the previous app before creating the
tunnel: PM2 `--namespace` deletion also selects other processes in that
namespace, so app cleanup after tunnel creation would immediately delete the
new tunnel. The script owns a PM2 SSH tunnel from JP loopback port
`16379` to SG loopback port `6379`. The SSH connection uses the existing `sg`
control-plane target; the source data remains on SG and is never exposed to the
public network.

## Local run

```bash
cargo run --release --bin delist_risk_server -- \
  --bind 127.0.0.1:8787 \
  --postgres 'postgres://delist_risk:@127.0.0.1:5432/delist_risk'
```
