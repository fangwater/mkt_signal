# Instrument snapshots

`scripts/instrument_snapshot_sync.py` captures public exchange contract and
instrument rules through `jp-meta-elvpn`, retains the raw and normalized
snapshot locally, and loads it into PostgreSQL on `el_dev`.

The default command is a dry run:

```bash
python3 scripts/instrument_snapshot_sync.py sync
```

Run the complete workflow explicitly:

```bash
python3 scripts/instrument_snapshot_sync.py sync --execute
```

The default scope is Binance, Bitget, Bybit, Gate, and OKX across spot,
margin, and futures. Restrict a run when needed:

```bash
python3 scripts/instrument_snapshot_sync.py sync \
  --exchanges binance,okx \
  --market-types futures \
  --execute
```

By default, `effective_from` is the capture timestamp. For a separately
verified historical effective time, pass an explicit UTC timestamp:

```bash
python3 scripts/instrument_snapshot_sync.py sync \
  --effective-from 2026-08-01T00:00:00Z \
  --execute
```

Do not assign an old effective time merely because the backtest starts then.
The API response proves only the rules observed at collection time unless a
separate exchange record establishes earlier effectivity.

## Local archive

Snapshots are written below `data/instrument_snapshots/`, which is ignored by
Git. Each directory contains:

- `manifest.json`: the complete normalized snapshot and capture provenance.
- `normalized_instruments.jsonl`: one normalized instrument per line.
- `raw/<source>/page-NNN.json`: exact public REST response bytes.
- `SHA256SUMS`: hashes for the manifest, normalized rows, and raw responses.
- `snapshot.tar.gz` and its sidecar hash: the transferred archive.

`price_tick_raw` and `qty_step_raw` retain the exchange decimal text. The
corresponding `*_integer` and `*_scale` fields encode the exact value as
`integer * 10^-scale`; for example, `0.25` is `(25, 2)` and `5` is `(5, 0)`.
When an API exposes only precision, the derived value is retained but its
source is explicitly `precision_derived` rather than `exchange`.

## PostgreSQL

The loader creates the `market_metadata` schema in the existing
`crypto_cta_manager` database. It reads the connection URL only on `el_dev`
from `~/.config/crypto-cta-manager/database.env`; the URL is never transferred
or printed.

Tables are:

- `instrument_snapshot_runs`: immutable complete manifests.
- `instrument_snapshot_scopes`: complete exchange/market validity intervals.
- `instrument_raw_responses`: exact response text, parsed JSON, and hashes.
- `instrument_rules`: exact-decimal normalized rules for each snapshot.

`instrument_rule_history` joins rules to the scope interval. Select one
frozen scope for a backtest start time before reading its rules:

```sql
WITH selected_scope AS (
    SELECT snapshot_id
    FROM market_metadata.instrument_snapshot_scopes
    WHERE exchange = 'binance'
      AND market_type = 'futures'
      AND effective_from <= TIMESTAMPTZ '2026-08-14T00:00:00Z'
      AND (effective_to IS NULL
           OR TIMESTAMPTZ '2026-08-14T00:00:00Z' < effective_to)
)
SELECT rules.*
FROM market_metadata.instrument_rules AS rules
JOIN selected_scope USING (snapshot_id)
WHERE rules.exchange = 'binance'
  AND rules.market_type = 'futures';
```

If no scope covers the backtest start, fail the run instead of falling back to
today's rules.
