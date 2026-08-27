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
| Announcements (Binance CMS delisting catalog, Bitget `symbol_delisting`) | **1h** | Raw JSON stored in Postgres |
| Gate announcement WS | persistent | Incremental; reconnects on drop |
| Official snapshots (Gate `delisting_time` / `in_delisting`, Bitget `offTime`, Binance SAPI if keys, futures schedule) | **3h** | Replaces that source in the book |

LLM extract runs only on **new** announcements. LLM / fetch failures never
block the other source. Reasons are queryable at `/status`.

## Venues

| venue | Meaning |
| --- | --- |
| `binance-margin` / `bitget-margin` / `gate-margin` | Spot, margin, loan |
| `binance-futures` / `bitget-futures` / `gate-futures` | USDT-M perpetual / delivery |
| `binance-coin-futures` / `bitget-coin-futures` | Coin-M |

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

`action`: `delist`, `disable_margin`, `disable_loan`, `monitoring`, `other`.

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
  `binance_monitoring`, `exchange_info`, `schedule:binance-futures`, `schedule:binance-coin-futures`,
  `schedule:gate-futures`, `schedule:bitget-futures`, `schedule:bitget-coin-futures`
- ws: `gate_ws`
- llm: `llm` plus per-announcement rows in `llm_failures`

## Postgres

Database `delist_risk` on `127.0.0.1:5432`. Restart recovery reads:

- `announcements` — raw fetched announcement JSON, `first_fetched_ms`,
  `last_fetched_ms`
- `source_status` — last success / last error per source
- `llm_status` — last LLM extract result per announcement

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
| `DELIST_ANNOUNCEMENT_INTERVAL_SECS` | default `3600` |
| `DELIST_OFFICIAL_INTERVAL_SECS` | default `10800` |
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
