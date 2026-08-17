# Persist Sync Distribution

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
| 6355 | jp | mm | `okex_mm_alpha` | `okex_mm_alpha` | `http://127.0.0.1:6355` | `127.0.0.1:50055` | fixed |

## Setup Notes

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
