# Local ClickHouse

## Installation

The local ClickHouse installation is outside this repository:

- Version: `26.3.12.3` official static build
- Root: `/mnt/Data/fanghz/clickhouse`
- Binary: `/mnt/Data/fanghz/clickhouse/clickhouse`
- Config: `/mnt/Data/fanghz/clickhouse/config/config.xml`
- Data: `/mnt/Data/fanghz/clickhouse/data`
- Logs: `/mnt/Data/fanghz/clickhouse/log`
- PID file: `/mnt/Data/fanghz/clickhouse/run/clickhouse-server.pid`

The server binds only to `127.0.0.1` and uses dedicated ports to avoid an
existing listener on the default interserver port:

- HTTP: `18123`
- Native TCP: `19000`
- Interserver HTTP: `19009`

## Lifecycle

Start the local daemon:

```bash
/mnt/Data/fanghz/clickhouse/clickhouse server \
  --daemon \
  --config-file=/mnt/Data/fanghz/clickhouse/config/config.xml \
  --pid-file=/mnt/Data/fanghz/clickhouse/run/clickhouse-server.pid
```

Connect with the native client:

```bash
/mnt/Data/fanghz/clickhouse/clickhouse client --host 127.0.0.1 --port 19000
```

Check the running instance:

```bash
/mnt/Data/fanghz/clickhouse/clickhouse client \
  --host 127.0.0.1 \
  --port 19000 \
  --query 'SELECT version(), currentDatabase()'
```

Stop the daemon:

```bash
kill "$(cat /mnt/Data/fanghz/clickhouse/run/clickhouse-server.pid)"
```

Inspect startup and server errors:

```bash
tail -n 120 /mnt/Data/fanghz/clickhouse/log/clickhouse-server.log
tail -n 120 /mnt/Data/fanghz/clickhouse/log/clickhouse-server.err.log
```
