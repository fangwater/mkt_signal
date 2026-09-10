# Delist Risk Server

Public HTTP service for upcoming delist / margin / loan / futures-off risk on
Binance, Bitget, and Gate. Official market snapshots plus announcement LLM
extracts land in one book. Full dump, grouped by exchange.

This is a **risk hint**, not a product timetable. Spot and loan both count as
`{exchange}-margin`. Futures / perpetual use `{exchange}-futures` or
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

No API token. Do not put secrets in query strings.

## Cadence

| Source | Interval | Notes |
| --- | --- | --- |
| Announcements (Binance CMS delisting catalog, Bitget `symbol_delisting`) | **24h** | List discovery plus article detail; raw JSON stored in Postgres |
| Gate announcement WS | persistent | Push stream rather than a polling check; reconnects on drop |
| Official snapshots (Gate `delisting_time` / `in_delisting`, Bitget `offTime`, Binance SAPI if keys, futures schedule) | **24h** | Replaces that source in the book |
| Complete public product catalogs | **24h**, plus **00:00 UTC** | Drives current listing state and confirmed Redis removal; the midnight fetch is persisted to Postgres |

LLM extract runs only on **new** announcements. LLM / fetch failures never
block the other source. Reasons are queryable at `/status`.

## Venues

| venue | Meaning |
| --- | --- |
| `binance-margin` / `bitget-margin` / `gate-margin` | Spot, margin, loan |
| `binance-futures` / `bitget-futures` / `gate-futures` | USDT-M perpetual / delivery |
| `binance-coin-futures` / `bitget-coin-futures` / `gate-coin-futures` | Coin-M |

`BINANCE_API_KEY` / `BINANCE_API_SECRET` are required for Binance spot/margin
SAPI (`delist-schedule`, `asset/tags`). Without them, `binance-margin` still
gets LLM extracts from CMS announcements; official SAPI rows show as fetch
failures in `/status`. Bitget and Gate public market APIs need no key.

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
`exchanges.binance` / `exchanges.bitget` / `exchanges.gate`.

| Query | Default | Notes |
| --- | --- | --- |
| `venue` | all | optional filter, e.g. `binance-margin` |
| `exchange` | all | optional filter: `binance` / `bitget` / `gate` |
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

`listing` comes from public exchangeInfo / instrument catalogs (no API key):

- `listed` — the pair is still in the live book
- `pending` — still in the book, but official `offTime` / `delisting_time` / `in_delisting` / nearby `deliveryDate` is set
- `delisted` — the pair is gone from that venue catalog (already off)
- `unknown` — that venue catalog has not been fetched yet

Spot pair removals such as `SUI/BNB` are stored only as `symbols=["SUIBNB"]`. They do not mark `SUI` / `SUIUSDT` as delisted.

### `GET /accounts`

Per mounted book: Redis online universe ∩ `/risk`. Same filters as `/risk`.

```bash
curl -sS 'http://<host>:4191/delist/accounts'
```

`tone`: `ok` / `risk` / `error` / `uncovered`. Pair-only notices (`SUIBNB`) do not flag `SUIUSDT`.

For Binance, Bitget, and Gate, every Redis universe symbol is also compared to
the latest public exchange catalog. A `catalog_removed` hit means that the
pair is no longer present or tradable at that venue; it is a risk hit even when
the service has no matching delist announcement.

The HTML board at `/delist/` uses this endpoint. Style matches crypto NAV manager.

### `GET /removal-candidates`

Returns the Redis symbols eligible for automatic removal. A symbol is eligible
only when every venue used by that account is `delisted` in one complete,
successful Binance/Bitget/Gate catalog refresh and a fresh account snapshot
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
rechecks the deadline and position, and resolves the executable from the fixed
mounted-account map. It cannot accept a command or filesystem path. A successful
execution cannot run again against a snapshot captured before that execution.

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

- fetch: `binance_cms`, `bitget_announcements`, `gate_market`,
  `bitget_instrument_offtime`, `binance_spot_delist`, `binance_margin_delist`,
  `exchange_info`, `schedule:binance-futures`, `schedule:binance-coin-futures`,
  `schedule:gate-futures`, `schedule:bitget-futures`, `schedule:bitget-coin-futures`
- ws: `gate_ws`
- llm: `llm` plus per-announcement rows in `llm_failures`

## Postgres

Database `delist_risk` on `127.0.0.1:5432` stores:

- `announcements` — raw fetched announcement JSON, `first_fetched_ms`,
  `last_fetched_ms`
- `source_status` — last success / last error per source
- `llm_status` — last LLM extract result per announcement
- `exchange_symbol_snapshot_runs` — one daily run at `00:00 UTC`, including
  completion state, venue count, symbol count, and failure text
- `exchange_symbol_snapshots` — complete daily symbol rows for Binance, Bitget,
  and Gate Spot, USDT-M, and Coin-M; stores both the exchange symbol and its
  normalized lookup symbol
- `redis_symbol_dump_audit` — position-aware FR open-to-dump operations and
  failures, with the exact event, position snapshot, threshold, and Redis key
  changes
- `delist_flatten_audit` — final-window manual-required decisions and automatic
  or operator-triggered clear executions, including exit status and bounded
  stdout/stderr

The daily snapshot is transactional and date-idempotent. All nine public
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

`start_delist_risk_server.sh` owns a PM2 SSH tunnel from JP loopback port
`16379` to SG loopback port `6379`. The SSH connection uses the existing `sg`
control-plane target; the source data remains on SG and is never exposed to the
public network.

## Local run

```bash
cargo run --release --bin delist_risk_server -- \
  --bind 127.0.0.1:8787 \
  --postgres 'postgres://delist_risk:@127.0.0.1:5432/delist_risk'
```
