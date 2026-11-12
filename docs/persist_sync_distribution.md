# Persist Sync Distribution

最后更新: 2026-11-10 08:28 UTC。

Global allocation table for `persist_manager` gRPC sync sources across JP, HK, and SG.

## Strategy

Public endpoints must stay inside `6300-6400`. The dedicated 30-port pool for persist sync is:

- `6340-6359`
- `6380-6389`

Known non-persist ports in this range remain excluded: `6300`, `6322`, `6327`, `6338`, `6360`, `6370`, and `6379`.

Rules:

- Only explicitly requested envs get concrete allocations.
- Unconfigured envs must fail closed. Do not infer ports from suffixes.
- Bybit intra arb01 is fixed on SG `6351` and should not be moved.
- Binance intra arb02 is fixed on JP `6358` and should not be moved.
- Public URL format: `http://<host>:<port>`.
- Local bind format: `127.0.0.1:<bind_port>`.
- Bind formula: `bind_port = 50000 + public_port - 6300`.
- Nginx stream mapping format: `<public_port> <bind_host>:<bind_port>`.
- `source_id` defaults to the env directory basename.
- SG persist traffic uses the 行情网卡 EIP `47.128.92.224`; keep the 下单网卡/SSH EIP
  `47.131.162.78` out of collector URLs.

Regions:

| Region | Host | Base URL |
| --- | --- | --- |
| jp | `54.64.147.69` | `http://54.64.147.69` |
| hk | `47.238.128.48` | `http://47.238.128.48` |
| sg | `47.128.92.224` | `http://47.128.92.224` |

## Allocations

| Port | Region | Strategy | Env | Source ID | Public URL | Local Bind | Status |
| ---: | --- | --- | --- | --- | --- | --- | --- |
| 6340 | jp | fr | `gate_fr_arb01` | `gate_fr_arb01` | `http://54.64.147.69:6340` | `127.0.0.1:50040` | planned |
| 6341 | jp | fr | `bitget_fr_arb02` | `bitget_fr_arb02` | `http://54.64.147.69:6341` | `127.0.0.1:50041` | planned |
| 6351 | sg | intra | `bybit-intra-arb01` | `bybit-intra-arb01` | `http://47.128.92.224:6351` | `127.0.0.1:50051` | fixed |
| 6352 | sg | mm | `bybit_mm_alpha` | `bybit_mm_alpha` | `http://47.128.92.224:6352` | `127.0.0.1:50052` | fixed |
| 6353 | sg | intra | `bybit-intra-arb02` | `bybit-intra-arb02` | `http://47.128.92.224:6353` | `127.0.0.1:50053` | fixed |
| 6354 | hk | intra | `okex-intra-arb01` | `okex-intra-arb01` | `http://47.238.128.48:6354` | `127.0.0.1:50054` | planned |
| 6355 | jp | mm | `okex_mm_83777` | `okex_mm_83777` | `http://127.0.0.1:6355` | `127.0.0.1:50055` | fixed |
| 6359 | jp | cta | `binance-cta-rx01` | `binance-cta-rx01` | `http://127.0.0.1:6359` | `127.0.0.1:50059` | fixed |
| 6380 | jp | fr | `gate_fr_arb03` | `gate_fr_arb03` | `http://127.0.0.1:6380` | `127.0.0.1:50080` | planned |
| 6381 | jp | fr | `bitget_fr_arb03` | `bitget_fr_arb03` | `http://127.0.0.1:6381` | `127.0.0.1:50081` | planned |
| 6382 | jp | fr | `bitget_fr_arb01` | `bitget_fr_arb01_20261110` | `http://127.0.0.1:6382` | `127.0.0.1:50082` | bind active; forwarding reserved |

## Setup Notes

On 2026-11-10, JP `bitget_fr_arb01` was restored as a fresh book using the same
exchange account. Its new local RocksDB has a distinct identity and no imported
orders. The new source is `bitget_fr_arb01_20261110`, listening on loopback
`127.0.0.1:50082`; port `6382` is reserved, without Nginx forwarding. The existing
collector and retired `bitget_fr_arb01` source/cursor remain untouched and its
old `50057` endpoint remains unavailable, so fresh records cannot be appended
under the historical source. A read-only gRPC verification against an isolated
scratch center returned zero missing/mismatched records and made no repairs.
The first listener attempt encountered transient port occupancy; an env-local
persist-manager restart restored it. Recovery initially ran read-only; trading
was enabled at 08:21:50 UTC after the operator-approved 20,000 USDT transfer to
UTA. The new source/bind, historical source, collector and port reservation
were unchanged. Evidence: `data/ops/bitget_fr_arb01_restore_20261110/`.

On 2026-11-09, JP `bitget_fr_arb01` was retired. All eight environment
processes stopped, its `6357` Nginx stream forwarding was removed, and its
`6357`/`50057` allocation was released. The original 187 order-store files
were archived as tar.gz and copied to the local machine with archive and
per-file SHA-256 verification. The center collector's historical source entry
is retained to preserve its existing RocksDB column families; its retired
`127.0.0.1:50057` endpoint is unavailable. The collector was not restarted.
Evidence: `data/ops/bitget_fr_arb01_retire_20261109/`.

On 2026-11-06 07:26 UTC, `okex_mm_83777` switched to an independently verified
OKX subaccount in observation mode. The operator requested deleting its old
local observation RocksDB; the store was recreated with a new RocksDB identity.
Its source ID, bind and port remain unchanged. The center collector still does
not include this source, so no center cursor or shared store was reset.

On 2026-11-06, the stopped JP `okex_mm_alpha` slot was replaced by
`okex_mm_83777`. The new environment has a separate source ID and order store;
the old order archive remains historical evidence. Its persist manager is now
running on `127.0.0.1:50055`; the new source has not been added to the center
collector.

On 2026-10-08, JP `gate-intra-arb01` and `bitget-intra-arb01` were archived and
retired. Their public ports `6343`/`6344`, local binds `50043`/`50044`, Nginx
forwarding, and distribution allocations were removed. Cross arb01 remains on
`6345`/`50045`; no trading process was started during retirement.

Removing the two sources from the center collector configuration caused its
restart to fail with `Column families not opened`: the center RocksDB retains
eight historical column families for those sources, while the collector opens
only families derived from its current source list. The runtime
`~/persist_center/persist.toml` was restored to its original 14-source list and
only the history collector was restarted to restore collection for other
sources. The two retired endpoints remain in that runtime list and are
unavailable; the repository template omits them. No collector code fix was
deployed. Center historical data and the running read server were preserved.

Use the table-driven helper from the env directory. It fails if the env is not listed in `config/persist_sync_distribution.toml`.

```bash
./scripts/configure_persist_sync_source.sh
```

For deployed intra envs, the compatibility wrapper is:

```bash
./intra_scripts/configure_intra_persist_sync_source.sh
```

The helper writes the matching values into `env.sh`:

```bash
export PERSIST_SYNC_SOURCE_ID="<source_id>"
export PERSIST_SYNC_BIND="<local_bind>"
```

It also writes the nginx stream mapping:

```text
<port> <local_bind>
```

Restart only that env's `persist_manager` after updating `env.sh`.
