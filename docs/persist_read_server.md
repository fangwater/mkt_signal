# persist_read_server

`persist_read_server` is a read-only HTTP service for persisted order records. It opens the primary RocksDB as a Secondary instance, catches up with the primary before each request, and returns Arrow IPC stream or Parquet bytes without writing export files on the server.

## Start

Use the unified persist config to manage the central recorder and read server together:

```bash
./scripts/start_persist_sync_collector.sh config/persist_read_server.toml
./scripts/stop_persist_sync_collector.sh
```

`start_persist_sync_collector.sh` always starts `persist_sync_collector`. If the same TOML contains `[read_server]` and `read_server.enabled` is not `false`, it also starts `persist_read_server` as a second PM2 process in the same namespace.

The read server can still be started alone for debugging:

```bash
./scripts/start_persist_read_server.sh config/persist_read_server.toml
```

## Config

```toml
center_db = "data/persist_sync_center"
batch_records = 1000
batch_bytes = 4194304
reconnect_delay_ms = 1000
sync_writes = false

[[sources]]
id = "default"
url = "http://127.0.0.1:50051"

[read_server]
enabled = true
bind = "0.0.0.0:8822"
secondary_dir = "data/persist_read_server_secondary"
source_id = "default"

max_concurrent = 8
batch_rows = 50000
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

`read_server.primary_dir` defaults to top-level `center_db`. `secondary_dir` must be different from `primary_dir`; it stores Secondary instance metadata, not a full data copy. The central DB stores source-specific column families like `source_id__uniform_orders`, so `read_server.source_id` selects which source the API reads. For a single `[[sources]]` entry it can be inferred; for multiple sources it must be set explicitly.

## API

### `GET /healthz`

Returns `{"ok": true}`.

### `GET /v1/schema?table=uniform_orders`

Returns allowed columns and output formats for a table.

### `GET /v1/read`

Example:

```bash
curl -o orders.arrow \
  'http://127.0.0.1:8822/v1/read?table=uniform_orders&start_us=1770000000000000&end_us=1770003600000000&columns=ts_us,symbol,client_order_id,status,price&format=arrow_ipc'
```

Parameters:

- `table`: `uniform_orders`, `order_updates_unmatched`, or `trade_updates_unmatched` by default.
- `start_us`: inclusive lower bound, microseconds.
- `end_us`: exclusive upper bound, microseconds.
- `columns`: optional comma-separated column list.
- `format`: `arrow_ipc` or `parquet`. Defaults to config `default_format`.

Concurrency uses `try_acquire`; if all permits are in use, the service returns `429 Too Many Requests` immediately and does not queue.

## Python

Arrow IPC stream:

```python
import pyarrow as pa
import pyarrow.ipc as ipc
import requests

r = requests.get(url, params=params, timeout=120)
r.raise_for_status()
table = ipc.open_stream(pa.BufferReader(r.content)).read_all()
df = table.to_pandas()
```

Parquet:

```python
from io import BytesIO
import pandas as pd
import requests

r = requests.get(url, params={**params, "format": "parquet"}, timeout=120)
r.raise_for_status()
df = pd.read_parquet(BytesIO(r.content))
```

## First-Version Notes

The service reuses the existing persisted-record decoder and builds the response DataFrame directly, without an intermediate Parquet encode/decode roundtrip. RocksDB scans are decoded in `batch_rows` chunks and appended into the final result, so it no longer keeps a second full-sized KV vector for the whole request. It still returns one in-memory response body; a later version can reduce peak memory further with true chunked HTTP streaming and column-aware decoding.
