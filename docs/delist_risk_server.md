# Delist Risk Server

最后更新：2026-11-04 06:32 UTC（11个FR只读敞口检查已上线，保留当前CTA与下架监控）

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
http://<jp-host>:4191/delist/exposures
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

## FR exposure checks

Deployment verified 2026-11-04 06:32 UTC on JP: source `c5f52f66`, Delist PID `2469971`,
CPU `0-5`, all 11 current FR accounts discovered. Binary SHA-256:
`6315c7bc39ef90e2caf3b9de756814fa69eecad8b700cb24de4e87b111d65e7c`.
Service/auth configuration and previous automatic-action arguments were unchanged;
92 protected trading/market-data/CTA processes retained their PIDs and affinities.
Loopback and gateway `/exposures` both returned 401 without a login. The deployed
HTML and live report were replayed in Chromium at desktop/mobile sizes. Gate04
had at least 1854.24 USDT absolute residual exposure at verification; the other
seven fully valued accounts were within monitoring thresholds. Gate01, Gate02,
Bybit and Gate04 had unpriced holdings and were explicitly marked incomplete.
These are a point-in-time observation, not a guarantee about later positions.
See `../artifacts/fr_exposure_check_20261104/README.md` and the private runtime
backup `~/delist_risk_server/run/fr_exposure_20261104_c5f52f66/`.

The board includes a permanent, read-only FR exposure section. `/exposures`
returns the cached report under the same login protection. Account discovery
uses the current NAV catalog's `funding_rate` entries, including Gate arb04 and
SG Bybit; directional CTA/Exec targets do not enter this check.

Every 10 seconds, the service concurrently reads each account's existing
`/fr/<env>/snapshot` through the loopback gateway (five-second request timeout).
It sums **absolute per-asset net exposures** within each account; different
assets and accounts cannot cancel each other's risk. The signed `TOTAL` row is
ignored. Two-leg quantities/valuations, the largest residual, pending hedge
quantity, timestamps and all nonzero snapshot positions are available on the
board. Mobile summaries are stacked; detailed position tables scroll locally.

Default monitoring thresholds are **100 USDT per asset**, **500 USDT per account**,
and **30 seconds** of observed excess before a persistent warning. Initial excess
is shown immediately as pending. These are display thresholds, independent of
trading, delisting, automatic dump and flatten thresholds. The collector itself
neither changes Redis lists nor submits orders; the independently configured FR
DingTalk channel below consumes its observations.

The exposure entry, risk entry and every underlying account-monitor timestamp
must be no older than 90 seconds and at most five seconds in the future.
Missing/invalid/duplicate data, nonzero holdings with zero valuation, unknown
hosts and failed/stale account discovery cannot become healthy zero exposure.
Partial valuations show a lower bound. Failed/incomplete refreshes retain prior
breach evidence for unknown assets and account totals. A fresh valid valuation
can clear that asset's own breach even when another asset is unpriced; clearing
an account-total breach requires complete coverage. Unknown observations interrupt
pending confirmation. A stopped collector
invalidates results after three poll intervals. The same cached snapshot cannot
advance breach confirmation; different assets have separate observation timers.
Restarting the service restarts the observation window.

This uses account-monitor-derived snapshots, **not independent exchange REST
reconciliation**. Cash stablecoins and small nontrading holdings filtered by
`pre_trade`'s existing `min_non_trading_position_usdt` setting are outside the
snapshot totals. Missing upstream positions cannot be reconstructed here.
The board explicitly identifies this scope; unpriced holdings that are present
in a snapshot are listed as incomplete instead of being treated as zero.

The wrapper accepts `DELIST_EXPOSURE_INTERVAL_SECS`,
`DELIST_EXPOSURE_MAX_AGE_SECS`, `DELIST_EXPOSURE_PERSISTENCE_SECS`,
`DELIST_EXPOSURE_ASSET_LIMIT_USDT` and `DELIST_EXPOSURE_ACCOUNT_LIMIT_USDT`.
The identical report is atomically saved beside the risk book as
`data/fr_exposure_check.json` for private operator diagnostics. The protected API
also reevaluates freshness at request time; an old file is historical evidence.

Validation: `cargo test --bin delist_risk_server` and
`scripts/tests/delist_exposure_browser.py --delist-binary <binary> --browser-binary <chromium>`.
The browser test runs a disposable authenticated service with synthetic NAV and
account snapshots; it checks Gate04/SG discovery, persistent warnings, recovery,
stale/read-failure retention, escaping, automatic refresh and desktop/mobile UI.

## Independent FR DingTalk

The FR channel uses its own robot URL and signing secret, independently of CTA.
The environment-local wrapper reads `config/fr_dingtalk.env` (owned by the service
user, mode 0600), containing `FR_DINGTALK_WEBHOOK_URL` and optional
`FR_DINGTALK_SECRET`. A populated URL enables `--fr-notify` unless
`DELIST_FR_NOTIFY_ENABLED=0`. Credentials are never arguments, API fields or logs.
`--fr-notify-check` validates configuration only; `--fr-notify-send-check` sends
one explicit setup test and exits. No extra scheduler or GPT model is introduced.

- Every actual Luna announcement processing attempt queues its outcome, including
  a successful analysis with no delisting action or a failed attempt. Primary /
  fallback calls belong to one processing attempt. Successful unchanged-article
  cache reuse does not send again. Queuing precedes the durable success marker.
- The existing 24-hour announcement scan queues a summary heartbeat after it
  completes. It states actual Luna successes/failures, coverage/data gaps, risk
  transitions, listing changes and recorded operations since the prior summary.
  It never claims that Luna ran when only cached results were used. A 23-hour
  guard prevents restart-driven heartbeat repetition.
- Complete official product catalogs retain a durable baseline and report
  newly/recurrently tradable and removed/inactive symbols. The initial baseline
  and an absent venue never manufacture mass listing/delisting notices. No new
  symbols are automatically added to strategies.
- UniMMR uses the live account state and configured trigger/recovery lines;
  the gap between those lines is unknown for recovery. Confirmed asset/account
  exposure, incomplete data, provider failures and empty UniMMR close lists send
  once on first detection and once on confirmed recovery. There is no periodic
  repeat or severity-change repeat. Unknown/missing/stale data never recover a
  prior issue. A fully closed asset requires complete fresh coverage to recover.
- Actual successful dump/list-removal mutations and completed flatten-script
  outcomes queue operation logs. The notification module itself submits no order
  and does not change trading settings. It reports these service operations,
  rather than claiming to audit every external operator or trading log.

`data/fr_notifications.json` holds the private atomic outbox, acknowledged issue
states, product baseline and a bounded processing/operation journal. Pending sends
retry with bounded backoff; HTTP success plus DingTalk `errcode=0` is required for
acknowledgement. Risk messages precede announcement backlog. Messages are spaced
four seconds apart to respect one robot's rate limit. Ordinary restarts preserve
delivery history. As with any webhook without receiver idempotency, a crash or
lost response at the acceptance/acknowledgement boundary can still duplicate a
delivery; it does not cause periodic resends of acknowledged issues.

Protected `GET /notifications` exposes enablement, pending count, acknowledged
delivery count, last send/error, last Luna processing and last summary enqueue.
The board displays channel status next to the exposure check. When FR DingTalk is
enabled, UniMMR-list alerts use this channel instead of the former Telegram watch.
Other pre-existing Telegram flatten notifications keep their own behavior.

## Login and FR panel access

Delist is the entry point for JP FR environments: Binance arb02/03/04,
Gate arb01/02/03/04, and Bitget arb01/02/03, plus the active SG
`bybit_fr_arb01` after its 2026-10-23 trading cutover. After login, use the account row's Viz
or Config link. `/delist/open?env=<slug>&panel=viz|config` grants that environment
to the current session and redirects to its existing `/fr/<slug>/` route.
Gate arb04 Config listens on port 20024; its Viz and trading services were started
on 2026-11-03. See `docs/core_allocation.md` for the live deployment record.
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
Only actual loopback GET requests to the allowlisted FR `/snapshot` URLs bypass browser
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

On 2026-10-23 at 10:39–10:49 UTC, the operator authorized a complete reset of
this account. Signal, pre_trade and trade_engine were stopped through env-local
scripts; all 13 active orders were canceled, all 21 perpetual positions were
closed with reduce-only market orders, and spot balances/liabilities were cleared
using market buys/sells plus explicit no-convert repayment. HOME's first market
buy was partially filled and canceled by the venue; its remaining debt was
bought back and repaid in a subsequent pass. The final exchange snapshot before
restart has zero perpetual positions, zero open orders and zero borrowing.
Only spot amounts below each market's minimum order value remain, totaling
approximately 52.15 USDT, as accepted by the operator.

Both the forward and UniMMR close lists now contain only the 19 requested symbols
listed below; backward, dump and pos_dump are empty. Live trading was restored
at 10:45–10:46 UTC: trade_engine PID1603071, pre_trade PID1603176 and signal
PID1603903, on cores 15/14/13. Account monitor PID1581248 remained on core 12;
all eight FR components and 14 unrelated processes were verified. Signal dry-run
is disabled. Existing operator changes to risk settings were preserved:
max_leverage=1.5, UniMMR lock/recover=5.0/5.1, force-close/recover=4.5/4.6.

The displayed UniMMR of 4.79 during the stop was the last pre_trade snapshot:
about 193673 USD of gross borrowing still required about 11560 USD maintenance
margin despite small net positions. After repayment, exchange initial margin,
maintenance margin, MMR and LTV are all zero. The refreshed snapshot has zero
borrow and derivative exposure; the infinite margin ratio serializes as null.
No current source IP or NAV accounting identity changed. Reset evidence is in
`data/ops/bybit_fr_reset_20261023/` and SG
`~/bybit_fr_arb01/run/reset_20261023/`.

The following records describe the initial cutover and its earlier preparation.

On 2026-10-23 at 10:15 UTC, SG `bybit_fr_arb01` completed the authorized live
cutover. All eight components run; the old intra signal, pre_trade, trade engine
and account monitor are stopped. Old Config, Viz and persist_manager remain
for historical access, with the original persistence identity and port 6351.
The new FR store is local and separate; no new collector endpoint was allocated.
NAV atomically disabled `bybit-intra-arb01` and enabled `bybit_fr_arb01` with
accounting start `2026-10-23T10:03:29.591Z`; existing user grants were preserved.
Delist's NAV catalog includes the new FR and excludes the old intra; its JP
loopback snapshot route serves fresh Bybit FR exposure.

The forward list is the operator's 19 symbols; the backward list is empty:
`QTUMUSDT WOOUSDT SNXUSDT COMPUSDT DYDXUSDT VELOUSDT MERLUSDT FLOWUSDT EGLDUSDT
BASEDUSDT BILLUSDT WETUSDT EDGEUSDT BSBUSDT RECALLUSDT HUSDT COOKIEUSDT GRVTUSDT SLXUSDT`.
Dump contains 27 symbols: all nonzero old perpetual positions, spot net holdings
worth at least 1 USDT, and the previous dump list. Sub-dollar spot dust in
COMP/EGLD/H/COOKIE does not block the new forward list. UniMMR close candidates
cover the union of 46 forward/dump symbols. Two residual SAHARA spot orders were
canceled after stopping intra and the account was verified clear of open orders
before FR startup; legacy positions were entrusted to normal dump decisions.

Live Binance FR arb02 parameters were copied through the FR Config API:
MM mode, 100 USDT order amount, 5000 USDT per-symbol cap, 2.1 leverage cap,
UniMMR lock/recover 3.0/3.1 and force-close/recover 2.8/2.9, plus its funding
thresholds and spread-quantile mapping. The disabled Binance model service name
was cleared. Bybit's existing rolling spread quantiles gained 25 without changing
other factors or windows; runtime then loaded 184 spread entries for all 46
symbols with zero missing references. A no-emit check preceded the final live
signal; observed execution records include spot ETHFI/ZK closes and ZK perpetual
hedges. The new RocksDB WAL is writing, all process/thread core masks match the
SG inventory, both private API consumers bind 172.31.7.124, and 14 unrelated
market-data/MM/bridge processes retained their identities and affinity.

Known limitations found during startup: `PRE_TRADE_NOTIFICATION_URL` was missing
and has been added from the existing account, but neither SG nor the reference
JP host currently runs the configured loopback 18100 notification service, so
alert delivery fails. EUR has no Bybit linear perpetual and HFT linear is Closed (its spot market
remains Trading). The derivatives-only valuation therefore lacks marks for their
roughly 0.0054/0.1115 USDT historical dust. The concentration-to-pos_dump guard
preserves its state and skips the entire scan when any position lacks a mark;
per-symbol, leverage and UniMMR controls still run. The operator accepted leaving
these immaterial balances in place; no dust cleanup orders were submitted. BRETT/TAIKO dump symbols lack borrowing
rates; runtime evaluates readiness per symbol and bypasses the rate gate for dump
closes, so this does not block the 19 requested forward symbols. Evidence and
sanitized checks are in `data/ops/bybit_fr_cutover_20261023/`; SG config backups and
verification are in `~/bybit_fr_arb01/run/cutover_20261023/`.

The following preparation record describes the state before this cutover.

On 2026-10-23, published Bybit FR position handling and prepared
`/home/ubuntu/bybit_fr_arb01` on SG without switching the active intra strategy.
The local synchronized `arbmm` release includes the current delist changes;
1357 library tests (3 ignored), 6 service tests and 3 route tests passed.
Release SHA-256: `271b7d24a49fd160659b271a371cc113b521f95c793352e50143c08b6c4ace9a`.
The exact artifact was uploaded, checksum-verified and atomically installed.
Only delist and its environment-managed SSH tunnel restarted; all 72 existing
JP FR process PIDs and all seven SG intra01 process PIDs were retained.
Delist and its tunnel use CPU mask `0-5` on the current JP host.

Bybit funding-rate accounts now participate in position-to-dump transitions,
confirmed removal after both catalogs delist the symbol and both legs fall
below 1 USDT, and the existing final-24-hour flatten policy. Existing Bybit
intra/MM accounts retain observation-only delist behavior. SG flatten runs
`python3 <env>/scripts/flatten_bybit_pm.py --symbols SYMBOL --mode clear --execute`
via configured SSH; account/venue/slug validation fixes the destination script,
and a remote `timeout` bounds execution even if the SSH client disconnects.
Credentials are read by the SG script from its own env.sh; they are never sent
in command arguments or copied to JP. A real JP-to-SG BTCUSDT **dry-run**
verified Bybit authentication and source IP `172.31.7.124`; no order or repayment
was submitted. Missing/stale FR exposure snapshots prevent automatic handling.

SG has all eight FR components deployed. Config (20041), Viz (20141), and
FR dashboard (20151) run on `0-7`, bind only loopback, and pass HTTP checks.
Account monitor, pre_trade, trade_engine, trade_signal and persist_manager
remain stopped. FR dashboard successfully fetched Bybit lending rates through
the order NIC. A signal dry-run entered `FundingArb` but could not initialize
without pre_trade's query IPC; it was stopped. Full trading startup remains a
cutover check, not a completed smoke test. The original intra strategy is live.

The new namespace is `bybit_fr_arb01`; symbol/risk/funding keys and RocksDB are
separate from intra. All five new FR lists are empty. The prepared risk config
retains intra's 8000 USDT per-symbol cap and 1.5 leverage limit; common execution
settings are copied into the separate Bybit FR parameter hash with MT mode.
FR funding defaults plus compatible intra 4h overrides are staged; unqualified
intra close-threshold keys are excluded. `run/prepared_config.json` records the
old lists as a reference. Re-read live positions and lists at cutover.

NAV has a disabled `bybit_fr_arb01` funding-rate record and a `delist` grant,
with its own storage schema. Keeping `enabled=false` excludes the staged account
from NAV polling, totals and delist's active account catalog. At cutover, finalize
the new symbol/UniMMR lists, establish fresh FR exposure, set the new accounting
start time and switch NAV enablement/grants consistently with retiring intra;
do not count the same exchange wallet under both active records.

The JP access allowlist includes the new env. SG's new FR routes allow only
loopback, and JP's corresponding routes use existing session/grant gates and
proxy through `127.0.0.1:14191` on the managed SG SSH tunnel. Public anonymous
JP requests redirect to delist login, public SG requests are denied, and the JP
loopback snapshot returns the new environment's empty snapshot without falling
back to intra exposure. Rollback assets are in JP
`run/bybit_fr_prepare_20261023/before/`; SG preparation and Nginx backup are in
`~/bybit_fr_prepare_20261023/`. Local evidence is under
`data/ops/bybit_fr_prepare_20261023/`. Both hosts also retain the scoped route
snippet at `/etc/nginx/snippets/bybit_fr_arb01.conf`, referenced by an `external:`
row in `~/nginx_locations.txt`, so a future mapping render retains the restrictions.

On 2026-10-22 13:15 UTC, published the final-window dust fix to
`/home/ubuntu/delist_risk_server` on `jp-meta-elvpn`. The local release was
built from synchronized `arbmm` at `4cf16d17` with the three delist source
changes; 42 delist library tests, 6 service tests, and formatting passed.
Release SHA-256:
`5ef488a32e3a2f1a313c8f861779291a4e23c9f0c0592a83cbfa6cde44dbb44b`.
The uploaded artifact was checksum-verified and atomically installed before
restarting through the environment-local start script. All 72 observed FR
process PIDs were unchanged; delist retained CPU mask `0-5,48-95`, its loopback
listener, credentials, frontend and existing control settings.

MITOUSDT was removed from arb02's dump/UniMMR lists and arb03's forward/UniMMR
lists at the operator's request. The initial arb02 removal was undone by the
old final-window scan because it treated 0.57199 MITO (about 0.006 USDT) as a
remaining position. Both margin and futures legs below 1 USDT now share the
existing removal dust threshold and do not become flatten candidates or get
added back to dump by final-window handling. After publication, arb02's
re-added dump entry was removed again. Direct Binance queries confirmed zero
MITO/PTB futures positions and open orders in both accounts; the arb02 cash
dust was retained, with no borrow or locked balance. PTBUSDT was absent from
both accounts' lists. This cleanup submitted no orders. The previous binary
and deployment evidence are under `run/dust_fix_20261022T131446Z/`; each
account's `run/remove_delist_symbols_20261022T131620Z.json` records the latest
account audit and Config API before/after state. Local evidence is under
`artifacts/delist_mito_cleanup_20261022/`.
Three complete scans after the final removal kept both accounts free of
MITOUSDT/PTBUSDT in all five symbol lists, with zero flatten candidates and
zero scan mutations. Persisted source statuses were all healthy and there
were no failed LLM analyses. The account universes are now 67 and 70 symbols.

On 2026-10-20 06:53 UTC, removed the retired account labelled `bybit cta`
(`bybit-intra-arb02`, historically registered as `intra_exchange`) from delist
monitoring by revoking only the NAV `delist` user's grant for that slug. The
next catalog refresh drops the account without a code exclusion or restart;
the nine FR accounts retain their grants. Historical strategy data is retained.
The grant backup and verification live under
`run/remove_bybit_cta_20261020T065320Z/` in the delist deployment directory.

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
FR links use the central delist session/grant gateway, including SG FR.
Other strategies use the authoritative NAV `config_url`, including SG URLs and
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

A background scan runs every `DELIST_UNIMMR_LIST_INTERVAL_SECS` (60s). The independent
FR DingTalk channel sends the first confirmed empty-list alert and its recovery;
read failures do not recover it. Without FR DingTalk, the legacy Telegram watch
uses `PRE_TRADE_NOTIFICATION_URL` and also sends only first/recovery, with no hourly
repeat. Findings remain visible in `/accounts` and `/status` (source
`fr_unimmr_close_list`) independently of delivery availability.

### `GET /removal-candidates`

Returns the Redis symbols eligible for automatic removal. A symbol is eligible
only when every venue used by that account is `delisted` in one complete,
successful Binance/Bitget/Gate/Bybit catalog refresh and a fresh account snapshot
shows both FR legs below 1 USDT. A missing margin or futures leg keeps the
symbol in dump while another account venue remains listed. Market-making
accounts use their futures venue. Bybit automatic removal applies only to
funding-rate accounts; Bybit intra/MM and CTA maps are excluded. The audit `venues`
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

Both legs strictly below 1 USDT are excluded from flatten candidates, using
the same dust threshold as confirmed symbol removal. Final-window handling
therefore does not re-add these residuals to dump or report a completed
automatic flatten as unresolved solely because of dust. A position of exactly
1 USDT on either leg remains a candidate; offsetting legs are checked
individually rather than by their net exposure.

The response also includes `position_statuses` for every dated upcoming/due FR
delist risk found in a fresh account snapshot. `closed=true` requires the symbol
to be present in that account's `fr_dump_symbols` and requires both
`abs(open_usdt) < 100` and `abs(hedge_usdt) < 100`. The board keeps the delist
declaration visible and marks such a row as `声明要下架，但已平仓完毕`.

With `DELIST_AUTO_FLATTEN_POSITION_RISK=1`, candidate positions from 1 through
1000 USDT are first moved to `dump` and removed from both open lists, then the
service runs the
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

## FR Redis configuration backups

On 2026-11-02 05:19 UTC, the JP service was updated to create and upload these
archives. The private rclone remote targets the Tokyo COS bucket and keeps the
existing Google Drive remote separate. The initial run produced verified
receipts for ten NAV FR environments (nine JP and one SG); one object from
each site was independently read back and SHA-256 checked, and anonymous
reads of those two objects returned 403. The installed server artifact SHA-256
was `d880efdf0a0a818885e4ecf00c6b92019cda0ef464c21d20a1316d490d3efbf2`.
The prior binary, startup script and configuration were preserved under
`~/delist_risk_server/run/cos-backup-deploy-20261102T051943Z/`.

On 2026-11-02 05:35 UTC, the JP server was updated with COS retention.
The locally built artifact SHA-256 is
`c82f2e9b26a1385fbf807d434a05b881ced6967279ec700fdd3846ad3f6ab27c`;
the previous binary is `run/delist-before-retention-20261102`. After restart,
retention reported success for all ten FR environments.

The server reads the current NAV catalog and creates one daily archive per
`funding_rate` environment on JP or SG. Each archive contains the selected
Redis configuration, the environment's `env.sh`, `trade_engine.toml` (or
`trade engine.toml`), and regular files under its `config/` directory, excluding
historical `.bak`, `.bk`, and `.backup` files and their dated variants.
It reads JP files locally and SG files through the SSH host configured by
`DELIST_SG_REDIS_SSH_HOST` (default `sg`). It uses the configured JP Redis and
the existing SG Redis tunnel, and takes each environment's selected keys in a
single read-only Redis transaction. No Redis replication or restore command is
used. On startup it backs up the current UTC day if needed; it checks again
every five minutes, including after a failed attempt or NAV outage. A fresh
NAV catalog is required; unsupported hosts, unavailable Redis or SSH access,
missing required files, and symlinks in the selected files are failures rather
than silently skipped.

The default path is `data/redis_backups/<jp|sg>/<env>/YYYY-MM-DD.tar.gz`,
relative to the deployed delist server directory. Each environment retains its
latest 30 successful daily archives by default, configurable with
`DELIST_REDIS_BACKUP_DIR` and `DELIST_REDIS_BACKUP_KEEP`. Set
`DELIST_SKIP_REDIS_BACKUP=1` to disable the task. Successful snapshots are
published only after gzip, tar, metadata, and per-key SHA-256 validation. Older
archives are deleted only after the current archive is validated; when COS is
enabled, they are retained until the corresponding uploads are verified. The archive
directory is private (0700) and files are mode 0600. Archives contain
credentials from `env.sh` and possibly `config/`; they are not encrypted.
The JP environment parent defaults to `/home/ubuntu` and can be changed via
`DELIST_REDIS_BACKUP_ENV_ROOT`.

`config.json` identifies the environment, site, exchange, capture time and every
selected key; missing keys are recorded explicitly. Present strings and hashes
have a readable value, remaining TTL, and a `keys/*.dump` Redis DUMP payload
with its SHA-256. Environment files are under `files/`; their original modes,
sizes and SHA-256 values are recorded in `config.json`. File capture and the
Redis transaction are sequential, so changes made during a capture are not a
single cross-system point-in-time snapshot. The key set covers FR symbol lists (including position dump),
per-environment risk and funding parameters, per-symbol overrides, and the
corresponding shared FR strategy, rolling-parameter and spread configuration.
Shared venue keys are marked `shared`; restoring one affects every strategy that
uses it. A snapshot is rejected if risk parameters, shared strategy parameters,
or all three open/dump lists are absent. The backup cannot establish that the
captured values are correct or protect against a compromised host; arrange an
independent copy if protection against host loss or tampering is required.

Optional COS upload uses a private, service-user-owned rclone configuration
(`0600`) and a configured `remote:bucket/prefix`. The deployment-specific
destination is `tencent-fr:jp-fr-config-1307417638/fr-config-backups` in the
Tokyo region. Set `DELIST_REDIS_BACKUP_COS_TARGET` and
`DELIST_REDIS_BACKUP_COS_CONFIG` in the environment-local config. The existing
Google Drive rclone configuration is not changed. For Tencent COS, use the
`TencentCOS` S3 provider and endpoint `cos.ap-tokyo.myqcloud.com`; do not put
credentials in repo files, command arguments or logs. The credential helper
`scripts/configure_fr_cos_rclone.sh` accepts both values interactively without
terminal echo and creates a separate private configuration.

After a validated local backup, rclone uploads to
`<prefix>/<jp|sg>/<env>/<YYYY-MM-DD>-<sha256>.tar.gz`. It does not overwrite
an existing differing object, and reads the exact uploaded object back to check
its SHA-256 before recording a private local receipt. Failed or interrupted
uploads are retried every five minutes, including older pending archives; local
retention does not delete an archive until it has been verified for the current
destination. COS retention keeps the latest `DELIST_REDIS_BACKUP_KEEP` daily
archives and the same number of independently timestamped manual archives per
site/environment (30 of each by default), and only deletes matching dated archives
after verifying the current day's uploaded object. Other object names and
future-dated objects are ignored. `/status` reports `cos_fr_retention_<env>`
and `cos_fr_backup_<env>` separately from local backup status. The COS bucket must
be private: archives include account credentials and are not client-side
encrypted. COS availability and policy are external dependencies.

The Redis user needs `MULTI`, `EXEC`, `DUMP`, `PTTL`, `GET`, and `HGETALL` for
these keys. `/status` exposes `redis_fr_backup_catalog` and
`redis_fr_backup_<env>` success/error states. No live restore is performed:
inspect the archive and reconcile its values, shared keys, TTLs, positions and
orders before any separately authorized restoration.

## CTA combinations, shared factors and Exec mapping

Delist is the common navigation entry for CTA Prep and CTA Spread. The page
always displays **CTA Prep 统一配置** and **CTA Spread 统一配置**, including when
no signal is running or one catalog is missing. They open the same editor with
an explicit worker selection:

| Entry | URL |
|---|---|
| CTA Prep | `/cta/cta-prep-binance-futures/config/?worker=cta-prep-binance-futures` |
| CTA Spread | `/cta/cta-prep-binance-futures/config/?worker=cta-factor-spread` |

The editor reads that selection on initial navigation and includes a return
link to `/delist/`. `/cta/cta-factor-spread/config/` is a gateway redirect to
the Spread selection in this editor. The old Prep and Spread editable config
locations return 410. The unified gateways were already cut over before the 2026-11-03 current-worker monitoring publish; this publish did not change Nginx routes.

The service independently inventories `cta-prep-binance-futures` and
`cta-factor-spread`. `DELIST_CTA_FACTOR_ENV` / `--cta-factor-env` and
`DELIST_CTA_SPREAD_FACTOR_ENV` / `--cta-spread-factor-env` select their environment
directories; defaults are the corresponding names beneath
`<redis-backup-env-root>`. It reads each worker's TOML, combination directory and
`state.json`, reusing the factor core's validation without Kafka or IPC.
`/cta-strategies` exposes one current `factor_processes` array, with worker and
combination configuration URLs. A missing Spread catalog does not discard the
Prep inventory or either navigation link. The shared editor must be configured
with both catalogs and the gateway's configure-permission scope must cover all
affected Prep and Spread accounts.

The current page reads six Prep combinations (Prep01/Prep02 at 1min, 5min and
15min) and Spread01 from the two shared workers. At the 2026-11-03 15:36 UTC
verification, Prep had 404 unique jobs and Spread had 50. These are observations,
not hard-coded inventories.
The authoritative combination format keeps the four columns `标的`, `nq`,
`符号`, `因子`, with top-level `venue`, `interval` and signal controls. NQ
dependencies are derived from those rows. Every CTA configuration link points
to `/cta/cta-prep-binance-futures/config/?worker=<worker>#<combination>`; no per-signal factor
Redis key or old editable Config URL is used by the new configuration flow.
Counts come from the deployed nonzero references, rather than hard-coded RX
lists. Deleting a combination or its last reference changes the next read.
Each process is shown once with its configured combinations, active-plan match,
replay and snapshot freshness. A missing configuration, missing runtime status,
stale status, and a plan waiting to become active remain distinct from a live
signal's enabled flag. Configuration alone is not evidence of a running process.

The delist page discovers current combinations from each shared worker's plan
and matches their Manager strategy identities to its runtime `position_tables`.
Retired `binance-cta-perp-rx*` directories are not part of live monitoring.
The worker heartbeat controls freshness: a 15-minute table legitimately retains
its decision between bars. Missing or mismatched tables still report an error.
It combines the current published Manager strategy identifiers with
the JP Manager catalog's current account bindings and virtual-account follows.
This supports multiple Exec sources per combination. Links come from each source's
configured Manager gateway prefix; account identity is the Manager source ID,
not its display alias or a guessed Exec number. The endpoint is read-only and
does not change Manager, Redis, positions, or orders. CTA checking does not
enable the FR-specific automatic removal or flatten actions.

The strategy universe combines its own configured factor references and runtime
targets, including symbols retained for position handling. Another combination's
symbols are not inherited.
The existing delist book and Binance futures listing catalog provide risk
matches. The page reports an unavailable Manager, missing gateway, ambiguous
strategy name, unsynchronized followed binding, and worker state older than
three minutes. A stale runtime snapshot is explicitly marked; it is not proof of
current trading or that every position has been included. CTA counts are included
in the page summary.

Spread uses the shared Spread worker's current tables, Manager strategy identity
and Exec bindings. Each leg has an explicit market scope;
Binance cash delisting is checked independently of Binance perpetuals. OKX
listing coverage remains explicitly unavailable until its risk catalog is
connected; known Binance hits are retained. CTA monitoring never invokes FR
automatic removal or flatten actions.

`config/cta_manager_reader.env` is a service-user-owned 0600 file containing
`DELIST_CTA_JP_DATABASE_URL`; its role can only SELECT the mapping tables. The
Manager configuration file is read locally for source IDs and gateway paths.
Database reads use a read-only repeatable-read transaction. The service checks
JP only; the existing SG connection for FR remains independent. On 2026-11-02,
read-only verification found RX01/15m → Exec06, RX02/1m → Exec08,
RX03/1m → Exec07, and RX04/5m → Exec05. These are a snapshot, not static
configuration; Manager changes appear on the next page refresh. The 2026-11-02
binary SHA-256 was `b559a14451555dd7a8fb31d3b403b73f3e993e0757902f393d7c65b520542f59`;
rollback copies are in `run/cta-mapping-deploy-20261102/` on JP.

## CTA configuration backups and manual backups

JP CTA Prep and Spread signal environments are discovered independently of NAV and backed
up individually. The startup pass and subsequent five-minute retries create one
archive per UTC day under `data/cta_backups/jp/<signal>/YYYY-MM-DD.tar.gz`;
midnight UTC is 08:00 Shanghai time. The optional COS target is configured by
`DELIST_CTA_BACKUP_COS_TARGET`; the configured Tokyo destination is
`tencent-fr:jp-fr-config-1307417638/lgq-cta-backup`. The existing private rclone
config and `DELIST_REDIS_BACKUP_KEEP` are shared with FR.

Each CTA archive contains the signal `env.sh`, regular files under `config/`,
available release metadata, the runtime status and, when readable, position
flags. When the shared factor catalog contains that signal's combination, the
archive also includes the worker TOML, all combination JSON files, available
`env.sh` and release metadata, a resolved
merged plan, and the available runtime state under
`files/factor/<worker>/`. Saving the complete shared catalog
preserves other strategies' dependencies during a reviewed restoration. Factor
history, Kafka records and a fabricated factor Redis key are not captured.
Missing shared configuration/status or a config/active-plan mismatch is a
completeness warning; existing archives without the shared dependency are also
labelled incomplete. No existing archive is overwritten. Manifest checks reject
missing, duplicate or corrupt shared-factor members.
It includes each dynamically associated JP Exec's `env.sh`,
`trade_engine.toml`, configuration directory and release metadata. A
repeatable-read Manager snapshot contains the associated virtual accounts,
bindings, follows, position and order strategies, source metadata, leverage,
grants, and user IDs/names; user password hashes are excluded. The shared
`cta-manager.toml` is included and can reference other accounts. Redis DUMP
payloads cover each associated Exec's strategy
indexes/configurations, risk parameters, algorithm-switch state, leverage-init
state and position-allocation evidence;
missing selected keys are recorded. `.bk`, `.bak`, `.backup` and dated variants
are excluded. All captured file and dump payloads have SHA-256 checksums in the
archive manifest. The files can contain account credentials and Manager tokens;
local archives are 0600 in private directories and the COS bucket must remain
private. These archives are not client-side encrypted.

Spread archives use `cta-factor-spread` dependencies and the corresponding
Spread runtime status, position flags and ownership ledger. Exec Redis captures
follow each source's configured venue, including native OKX unified cash and
perpetual scopes; Binance and OKX keys are not mixed. An unregistered Spread
combination is reported as incomplete rather than associated with another
combination by guessing its symbols.

The FR and CTA rows provide an authenticated, same-origin **立即备份** action.
Each click captures a new `YYYY-MM-DDTHHMMSSnnnnnnnnnZ.tar.gz` version, uploads
it and verifies the exact cloud object by reading it back and hashing it. The
page reports failure or completeness warnings. Daily and manual versions each
retain their own 30 most recent objects by default, so manual backups do not
evict daily backups. A backup or read-back failure retains unverified local
archives for retry and prevents cloud cleanup. Retention is scoped to the exact
site/environment and destination prefix. `/status` includes
`cta_backup_<env>`, `cta_backup_quality_<env>`, `cos_cta_backup_<env>` and
`cos_cta_retention_<env>`; the quality entry reports absent Exec Redis keys or
differences between the unified combination and the signal's active snapshot.
Signal snapshots written before this cutover are explicitly identified.

On 2026-11-02, the JP service was updated with this backup and manual action;
the final server SHA-256 is
`fb7f148b01716e53851397e8891a924a3b8c4be171167c0bb0ee170ec7fcda8f`.
The first four CTA daily archives were uploaded and read back with matching
hashes. At the initial verification, RX01–RX03's factor Redis keys and Exec06–08's
`binance-futures:pre_trade_risk_params` keys were absent; RX02's runtime factor
table was empty and RX03's runtime table differed from its disk file. Both
available tables were retained. The pre-trade logs independently showed failed
risk-parameter refreshes for Exec06–08. Existing startup-table logs and older
snapshots are historical evidence, not proof of the latest active risk values.
At 06:49:43 UTC, after explicitly choosing each process's startup-log values,
the operator authorized restoration of the three missing Exec risk hashes:
pending order limit 10, buy/sell limits 10 each, 400 orders/minute and 300
orders/10 seconds. A WATCH/MULTI transaction created only absent keys and
verified every field; all three pre-trade PIDs remained unchanged and no later
missing-risk refresh warnings appeared during the next refresh cycle. The audit
is in `run/exec-risk-restore-20261102-evidence/`. New timestamped CTA manual
archives were uploaded and verified for RX01–RX03, each containing the restored
hash. All three restored DUMP payloads were independently downloaded and
restored into an isolated Redis, and their five fields matched the selected
values. Backup quality now reports the newest verified archive, so the old
daily snapshots do not continue to report the resolved missing risk hashes.
Their factor-key/runtime-table warnings remain. The initial RX01/RX04
archives had a false difference warning caused by integer versus floating-point
JSON representation; originals were preserved under
`run/cta-factor-compare-fix-20261102/` and as COS objects, and new daily
archives were generated after the comparison fix. The current CTA archives
and their completeness warnings must be reviewed before any live recovery.
The final archives additionally cover Chase allocation, leverage-init and
execution-family switch evidence; the preceding cloud-verified archives are
preserved under `run/cta-exec-state-fix-20261102/` and in COS. RX03's runtime
factor table passed the deployed publisher's universe validation, while its
disk factor table did not; this is evidence to review, not authorization to
write either table into Redis or resume a trading process.

On 2026-11-02, the operator subsequently chose to repopulate the missing factor
keys from running snapshots. RX01's 38-symbol and RX03's 16-symbol tables were
each unchanged from the preceding verified manual archive, matched their live
rule configurations, and were within the deployed publisher's symbol/factor
universe. A WATCH/MULTI transaction created both keys only while absent, and
read-back matched the captured tables. The signal process identities were
unchanged. These tables represent the running rule-derived state, not evidence
of the original lost Redis values. The private audit is in
`run/cta-factor-restore-20261102-evidence/` on JP. RX02's running table was
empty and its missing key was not restored; its disk table is a separate,
unverified candidate. The snapshots and current Redis state still require
independent review before live recovery.

Redis, files and the Manager database are sampled sequentially, so the package
is not a single cross-system transaction. Position flags, allocation ledgers,
and targets are point-in-time evidence and must not be blindly replayed into a
live account. Runtime binaries, market-data history, exchange positions and
open orders are not backed up. Restoring trading requires the matching software
and independent reconciliation of actual positions, orders and account access;
the archive alone is not an automatic live-trading restore.

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
| `DELIST_BIND` | default `127.0.0.1:8787` |
| `DELIST_PG_URL` | `postgres://delist_risk:...@127.0.0.1:5432/delist_risk` |
| `DELIST_REDIS_URL` | JP Redis, default `redis://127.0.0.1:6379/0` |
| `DELIST_SG_REDIS_URL` | SG Bybit Redis through a local SSH tunnel, `redis://127.0.0.1:16379/0` |
| `DELIST_SG_REDIS_SSH_HOST` | SSH target for the SG tunnel, default `sg` |
| `DELIST_SG_WEB_TUNNEL_PORT` | Optional JP loopback port forwarding to SG loopback 4191; deployed `14191` |
| `DELIST_SG_FLATTEN_SSH_HOST` | Explicit SSH target enabling SG Bybit FR flatten; deployed `sg` |
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
DELIST_SG_WEB_TUNNEL_PORT=14191
DELIST_SG_FLATTEN_SSH_HOST=sg
```

`start_delist_risk_server.sh` removes the previous app before creating the
tunnel: PM2 `--namespace` deletion also selects other processes in that
namespace, so app cleanup after tunnel creation would immediately delete the
new tunnel. The script owns a PM2 SSH tunnel from JP loopback port
`16379` to SG loopback port `6379`. The SSH connection uses the existing `sg`
control-plane target; the source data remains on SG and is never exposed to the
public network. The optional web forward exposes SG 4191 only on JP loopback
14191; authenticated JP FR routes use it for Config, Viz, websocket and snapshot
traffic. Bind SG backend ports to loopback and restrict SG FR gateway locations
to loopback so the JP authorization gate cannot be bypassed.

## Local run

```bash
cargo run --release --bin delist_risk_server -- \
  --bind 127.0.0.1:8787 \
  --postgres 'postgres://delist_risk:@127.0.0.1:5432/delist_risk'
```

2026-11-03 15:33 UTC current-worker monitoring publish: binary SHA-256
`a3096488dfad952bfc06f049731ba222ee8f3bd3edfb0688f29764bf0bfa97f5`, PID580273,
CPU `0-5,48-95`. Configuration/auth hashes and existing automatic-action switches
were retained. The local arbmm release build, 26 binary tests, browser checks and
read-only production mapping checks passed. The seven tables have no mapping
errors; retired RX01/03/04 no longer contribute stale warnings. Backup and
verification: `~/delist_risk_server/run/current_cta_mapping_20261103T1533Z/` and
`artifacts/delist_strategy_alerts_20261103/`. Monitoring uses the shared workers;
legacy signal-directory archive controls are not shown for these current tables.
