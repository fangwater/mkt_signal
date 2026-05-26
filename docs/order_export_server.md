# order_export_server

每个 env 一只的 HTTP 服务：
- 每小时 `HH:01` 自动跑一次 `export_window_to_dir`，把上一整小时窗口的三张 parquet 落到本地 cache
- 同时暴露 `/export` 按指定 start/end 现场导出，tar.gz 直接回流

二进制：`src/bin/order_export_server.rs`
依赖：read-only 打开 `<base_dir>/data/persist_manager`，所以可与 `persist_manager` 同机运行。

---

## 启动

```bash
./order_export_server \
  --bind 0.0.0.0 \
  --port 8821 \
  --base-dir /home/ubuntu/<env_name> \
  --retention-hours 3
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `--bind` | `0.0.0.0` | 监听 IP |
| `--port` | `8821` | 监听端口 |
| `--base-dir` | `cwd` | env 根目录；如果省略则用当前工作目录 |
| `--persist-dir` | `<base_dir>/data/persist_manager` | RocksDB 路径 |
| `--cache-dir` | `<base_dir>/data/order_export_cache` | 自动 snapshot 输出根目录 |
| `--retention-hours` | `3` | snapshot 保留小时数；过期自动清 |

PM2 形式启动用 `scripts/start_order_export_server.sh`，env 通过 `env.sh` 注入：

```
ORDER_EXPORT_SERVER_BIND=0.0.0.0
ORDER_EXPORT_SERVER_PORT=8821
ORDER_EXPORT_SERVER_RETENTION_HOURS=3
```

进程名：`<env_name>_order_export_server`，namespace：`<env_name>`。

---

## 自动调度

- 触发点：每小时 `HH:01:00` UTC（避开整点写库高峰）
- 窗口：`[上一整点, 本整点)`，举例 `01:01` 跑 `[00:00, 01:00)`
- 落盘：`cache_dir/<start>__<end>/` 三张 parquet：
  - `order_updates_unmatched.parquet`
  - `trade_updates_unmatched.parquet`
  - `uniform_orders.parquet`
- 时间戳格式：`YYYYMMDDTHHMMSS.000000Z`（UTC）
- 写入用 `<cache_dir>/.tmp.<name>/` → 成功后 `rename` 上去，下游 reader 不会读到半成品
- 过期清理：`end_ts <= now - retention_hours` 的目录被删

---

## HTTP API

所有响应都是 JSON 或 `application/octet-stream`/`application/gzip`，不需要鉴权（部署时靠 nginx/防火墙隔离）。

### `GET /healthz`

```
{"ok": true}
```

### `GET /latest`

返回 `end_ts` 最新的一份 snapshot 元数据：

```json
{
  "name": "20260526T000000.000000Z__20260526T010000.000000Z",
  "start_ts": "20260526T000000.000000Z",
  "end_ts":   "20260526T010000.000000Z",
  "files": [
    "order_updates_unmatched.parquet",
    "trade_updates_unmatched.parquet",
    "uniform_orders.parquet"
  ]
}
```

无 snapshot 时返回 `404 no snapshots available`。

### `GET /snapshots`

返回 cache 里所有 snapshot 的列表（结构同 `/latest`，未排序）。

### `GET /snapshots/:name/:file`

下载某个 cache snapshot 中的单个 parquet：

```bash
curl -OJ "http://host:8821/snapshots/20260526T000000.000000Z__20260526T010000.000000Z/uniform_orders.parquet"
```

- `name` 必须形如 `<start>__<end>` 且两端都能解析成时间戳
- `file` 必须在白名单：`order_updates_unmatched.parquet` / `trade_updates_unmatched.parquet` / `uniform_orders.parquet`

非法名 → `400`；文件不在 → `404`。

### `GET /export?start=...&end=...`

按任意 `start`/`end` 现场导出，**不落 cache**，tar.gz 流式返回。

- `start` / `end`：RFC3339（如 `2026-05-26T00:00:00Z`）
- 校验：`end > start`，两端都需 ≥ unix epoch
- 服务端：read-only 打开 RocksDB → 导出到 `<cache_dir>/.export.<pid>.<name>/` → tar.gz 进内存 → 删 tmp → 回 body
- 响应头：
  - `Content-Type: application/gzip`
  - `Content-Disposition: attachment; filename="order_export_<start>__<end>.tar.gz"`
- 解压结构：

  ```
  <start>__<end>/
    ├─ order_updates_unmatched.parquet
    ├─ trade_updates_unmatched.parquet
    └─ uniform_orders.parquet
  ```

示例：

```bash
curl -OJ "http://host:8821/export?start=2026-05-26T00:00:00Z&end=2026-05-26T01:00:00Z"
tar xzf order_export_20260526T000000.000000Z__20260526T010000.000000Z.tar.gz
```

> 注意：窗口可以任意大小（10 分钟 / 6 小时都行），但 RocksDB 扫描时间和 tar.gz body 大小会跟着涨；本接口同步阻塞，**不要拿来跑跨多天的大窗口**，那种场景另起 `order_export` 离线工具。

---

## 部署

本地 build + 推远端：

```bash
./scripts/deploy_order_export_server.sh \
  --upstream-port 8821 \
  --apply-nginx \
  --nginx-prefix /order_export/<env_name>/
```

- `--apply-nginx` 会把 `prefix → http://127.0.0.1:8821/` upsert 到远端 nginx mapping 并 reload
- 远端目标 host 由 `FR_DEPLOY_HOST` / `FR_REMOTE_HOME` / `FR_REMOTE_ENV_DIR` 控制
- 不带 `--apply-nginx` 只 rsync binary 和 start/stop 脚本

启停：

```bash
./start_order_export_server.sh   # PM2 拉起，自动 source env.sh
./stop_order_export_server.sh
```

---

## 故障排查

| 现象 | 处理 |
|---|---|
| `/latest` 一直 404 | 看进程是否启过整点；`ls <cache_dir>` 是否有 `<start>__<end>/` 目录；查 `pm2 logs` 找 `export tick failed` |
| `/export` 报 `invalid start` | RFC3339 必须带时区，比如 `Z` 或 `+00:00`；不带时区会拒掉 |
| `/export` 返回 500 + `failed to open RocksDB` | persist_manager 没起 / `--persist-dir` 路径错 |
| snapshot 目录残留 `.tmp.xxx` | 进程上次启动时崩在 export 中；下次启动会扫一遍 `.tmp.*` 全删 |
| cache 一直涨 | `--retention-hours` 太大；或 sweep 报错（查日志 `sweep remove ... failed`） |
| 整点 01 分没跑 | 排查 `pm2 status` 是否在线；`previous export tick still running, skipping` 表示上一轮没跑完，正常 |

---

## 关联文件

- 实现：`src/bin/order_export_server.rs`
- 导出逻辑：`src/persist_manager/exporter.rs::export_window_to_dir`
- 启停脚本：`scripts/start_order_export_server.sh` / `scripts/stop_order_export_server.sh`
- 部署：`scripts/deploy_order_export_server.sh`
- 离线版（一次性导出，不起 server）：`src/bin/order_export.rs`
