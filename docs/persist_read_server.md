# persist_read_server

`persist_read_server` is a read-only HTTP service for persisted order records. It opens the primary RocksDB as a Secondary instance, catches up with the primary before each request, and returns Arrow IPC stream or Parquet bytes without writing export files on the server.

## Start

Use the unified persist config to manage the central recorder, read server, and config UI together:

```bash
./scripts/start_persist_sync_collector.sh
./scripts/stop_persist_sync_collector.sh

# Optional local UI for editing persist.toml and viewing per-source gRPC heartbeats.
./scripts/start_persist_config_server.sh
./scripts/stop_persist_config_server.sh
```

`start_persist_sync_collector.sh` always starts `persist_sync_collector`. If the same `persist.toml` contains `[read_server]` and `read_server.enabled` is not `false`, it also starts `persist_read_server` as a second PM2 process in the same namespace.

The read server can still be started alone for debugging:

```bash
./scripts/start_persist_read_server.sh
```

By default the scripts use `persist.toml` next to the deployed binaries, or `config/persist.toml` when run from the source tree. A config path argument still overrides the default, and `PERSIST_CONFIG` can point both processes at the same file.

## Config

```toml
# Single persist config for the central recorder and local read server.
# persist_sync_collector consumes the top-level fields; persist_read_server consumes [read_server].

center_db = "data/persist_sync_center"
batch_records = 1000
batch_bytes = 4194304
reconnect_delay_ms = 1000
status_path = "data/persist_sync_status.json"
sync_writes = false

repair_enabled = true
repair_interval_secs = 1800
repair_lookback_hours = 24
repair_bucket_us = 60000000

# Add one source per remote persist manager.
# read_server.source_id is the default source when requests omit source_id.
[[sources]]
id = "default"
url = "http://127.0.0.1:50051"

[read_server]
enabled = true
bind = "0.0.0.0:8822"

# Defaults to top-level center_db when omitted.
# primary_dir = "data/persist_sync_center"
secondary_dir = "data/persist_read_server_secondary"

# Optional for a single [[sources]] entry. Requests can override this with source_id.
# If omitted and the request also omits source_id, /v1/schema and /v1/read return 400.
source_id = "default"

max_concurrent = 8

# Server-side RocksDB scan/decode batch size only.
# This does not split the HTTP response; each /v1/read request still returns one full Arrow/Parquet body.
batch_rows = 50000

# Hard per-request time-window limit. For larger analysis ranges, use the Python SDK
# read_*_range(..., window_sec=...) helpers to split requests by time.
max_window_sec = 3600
max_result_rows = 5000000
request_timeout_sec = 120

default_format = "arrow_ipc"
enable_parquet = true

allowed_tables = [
  "uniform_orders",
  "order_updates_unmatched",
  "trade_updates_unmatched",
]
```

`status_path` is written by `persist_sync_collector` and read by `scripts/persist_config_server.py` for heartbeat visualization. `read_server.primary_dir` defaults to top-level `center_db`. `secondary_dir` must be different from `primary_dir`; it stores Secondary instance metadata, not a full data copy. The central DB stores source-specific column families like `source_id__uniform_orders`. Requests may pass `source_id`; if they omit it, `read_server.source_id` is used as the fallback. For a single `[[sources]]` entry the fallback can be inferred. With multiple sources, either set `read_server.source_id` as the default or pass `source_id` on each schema/read request.

## Config UI

Start the local Python UI when you want browser-based config edits and heartbeat status:

```bash
./scripts/start_persist_config_server.sh
# open http://127.0.0.1:8830
```

The UI edits the same `persist.toml` and writes a timestamped `.bak` before saving. Heartbeats are read from `status_path`; each gRPC request or response updates the per-source status and recent event table.

## API

### `GET /healthz`

Returns `{"ok": true}`.

### `GET /v1/schema?table=uniform_orders&source_id=default`

Returns allowed columns and output formats for a table. `source_id` is optional when `read_server.source_id` is configured.

### `GET /v1/read`

Example:

```bash
curl -o orders.arrow \
  'http://127.0.0.1:8822/v1/read?table=uniform_orders&source_id=default&start_us=1770000000000000&end_us=1770003600000000&columns=ts_us,symbol,client_order_id,status,price&format=arrow_ipc'
```

Parameters:

- `table`: `uniform_orders`, `order_updates_unmatched`, or `trade_updates_unmatched` by default.
- `source_id`: optional source id. Falls back to `read_server.source_id`; if neither is set, the request returns 400.
- `start_us`: inclusive lower bound, microseconds.
- `end_us`: exclusive upper bound, microseconds.
- `columns`: optional comma-separated column list.
- `format`: `arrow_ipc` or `parquet`. Defaults to config `default_format`.

Concurrency uses `try_acquire`; if all permits are in use, the service returns `429 Too Many Requests` immediately and does not queue.

## Python SDK

Use `scripts/persist_read_client.py` from notebooks or analysis scripts:

```python
from scripts.persist_read_client import PersistReadClient

client = PersistReadClient("http://127.0.0.1:8822", timeout_sec=120)

schema = client.schema("uniform_orders", source_id="bybit-intra-arb01")
print(schema.columns)

df = client.read_pandas(
    "uniform_orders",
    start="2026-05-31 09:30:00",
    end="2026-05-31 10:30:00",
    source_id="bybit-intra-arb01",
)
```

For windows longer than the server limit, let the client split requests and concatenate Arrow tables before converting to pandas:

```python
df = client.read_pandas_range(
    "uniform_orders",
    start="2026-05-31 09:30:00",
    end="2026-05-31 12:30:00",
    source_id="bybit-intra-arb01",
    window_sec=3600,
)
```

`columns` is optional; omitted means all columns. Naive time strings are interpreted in `Asia/Shanghai` by default. Set `tz="UTC"` on the client or one request to interpret naive strings as UTC:

```python
client_utc = PersistReadClient("http://127.0.0.1:8822", tz="UTC")
df = client.read_pandas("uniform_orders", "2026-05-31 01:30:00", "2026-05-31 02:30:00", tz="UTC")
```

The SDK uses the Python standard library for HTTP. `pyarrow` is required for Arrow decoding, and `pandas` is required only for DataFrame conversion or Parquet reads.

CLI examples:

```bash
python3 scripts/persist_read_client.py --base-url http://127.0.0.1:8822 schema --table uniform_orders --source-id bybit-intra-arb01
python3 scripts/persist_read_client.py --base-url http://127.0.0.1:8822 read \
  --table uniform_orders \
  --start "2026-05-31 09:30:00" \
  --end "2026-05-31 10:30:00" \
  --source-id bybit-intra-arb01 \
  --format arrow_ipc \
  --out orders.arrow
```

## First-Version Notes

The service reuses the existing persisted-record decoder and builds the response DataFrame directly, without an intermediate Parquet encode/decode roundtrip. RocksDB scans are decoded in `batch_rows` chunks and appended into the final result, so it no longer keeps a second full-sized KV vector for the whole request. It still returns one in-memory response body; a later version can reduce peak memory further with true chunked HTTP streaming and column-aware decoding.
