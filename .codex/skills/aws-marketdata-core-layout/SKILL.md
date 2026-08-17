---
name: aws-marketdata-core-layout
description: AWS market-data and trading-host dual-ENI layouts for mkt_signal. Use when operating, auditing, or documenting the JP market-data host or the SG execution host, especially spread_pbs/depth_pub CPU bindings, account_monitor and trade_engine source-IP isolation, default-route migration, source-policy routing, service listeners, cross-host endpoint changes, or ENA IRQ affinity.
---

# AWS Marketdata Core Layout

## Host Variants

Identify the target host before applying any layout. For the SG execution host reached through
the `sg` SSH alias, read [references/sg-dual-eni-layout.md](references/sg-dual-eni-layout.md) in
full before auditing or changing networking, application binds, remote endpoints, IRQs, or trading
startup. The SG reference records the applied layout, current startup gates, and historical audit;
always re-check live state.

The remainder of this file describes the current JP market-data-host layout.

## Scope

Use this skill when operating directly on the current AWS market-data host.
Run deployment, restart, and verification commands locally on the host that owns the runtime
directories under `~/spread_pbs`, `~/depth_pub`, `~/rolling_metrics`, and
`~/trade_flow_feature`.

Before a live operation, run `hostname -f` and verify that the expected runtime
directory exists. Do not infer host identity from a historical hostname or SSH
alias. If the runtime directory is absent, stop and locate the current
market-data host instead of applying this layout to another machine.

This layout pins market-data processes to dedicated cores where possible.
`lscpu -e=CPU,NODE,SOCKET,CORE,CACHE,ONLINE` on the market-data host currently shows CPUs
0-47 online and a single visible L3 cache id (`...:0`) for all online CPUs.
Treat CPU0-5 as housekeeping/general OS capacity and market-data cores as
explicit per-process bindings.

- 5 `spread_pbs` processes:
  - Binance split into 2 processes: `binance-margin` and `binance-futures`.
  - Gate, OKEx, and Bitget each run as one `*-both` process.
- 2 `depth_pub` processes:
  - Binance runs one `binance-both` depth publisher on CPU14.
  - OKEx runs one `okex-both` depth publisher on CPU13 for model input.
  - Bitget and Gate depth publishers are not running in the current
    layout.
- Model-input auxiliary processes also run on housekeeping/general cores:
  - OKEx margin/futures rolling metrics: `rm_ok_mg_ok_fu`.
  - OKEx margin trade-flow feature publisher: `tff_ok_mg`.
  - OKEx futures trade-flow feature publisher: `tff_ok_fu`.

## Core Map

| CPU | Process | Venue dir | Env override | pmdaemon name |
| --- | --- | --- | --- | --- |
| 8 | `spread_pbs` | `~/spread_pbs/binance-margin` | `SPREAD_PBS_CORE=8` | `spp_bn_mg` |
| 9 | `spread_pbs` | `~/spread_pbs/binance-futures` | `SPREAD_PBS_CORE=9` | `spp_bn_fu` |
| 10 | `spread_pbs` | `~/spread_pbs/gate-both` | `SPREAD_PBS_CORE=10` | `spp_gt_bo` |
| 11 | `spread_pbs` | `~/spread_pbs/bitget-both` | `SPREAD_PBS_CORE=11` | `spp_bg_bo` |
| 12 | `spread_pbs` | `~/spread_pbs/okex-both` | `SPREAD_PBS_CORE=12`; source OKX credentials, then unset `IPC_NAMESPACE` | `spp_ok_bo` |
| 13 | `depth_pub` | `~/depth_pub/okex-both` | `DEPTH_PUB_CORE=13` | `dp_ok_both` |
| 14 | `depth_pub` | `~/depth_pub/binance-both` | `DEPTH_PUB_CORE=14` | `dp_bn_both` |

Treat the table as authoritative for the current market-data host unless the user explicitly
updates the topology. Do not assume CPU13 is Bitget depth on this host; it is
currently OKEx `depth_pub`.

## Dual ENI Network Lanes

Use these lanes on the current host:

| Lane | Interface / ENI | Purpose | Routing |
| --- | --- | --- | --- |
| Order/control | `ens41` / `eni-046788fa6ed28d9ab` | All `trade_engine`, account/user streams, and constrained market-data feeds | Source-policy table 100; DHCP main route metric 100 |
| Market data | `ens42` / `eni-06e24eee2c44d856e` | Eligible public `spread_pbs` feeds | Source-policy table 101, metric 200 |

The following public/private mappings were verified through IMDSv2 and outbound
source tests on 2026-08-04. Re-read IMDS before changing them; bind private IPs
to Linux, never public/EIP addresses directly.

| Interface | Private IPv4 | Public IPv4 |
| --- | --- | --- |
| `ens41` | `172.31.35.228` | `13.115.227.29` |
| `ens41` | `172.31.35.229` | `52.193.90.33` |
| `ens41` | `172.31.35.230` | `54.238.72.43` |
| `ens41` | `172.31.35.231` | `52.69.78.134` |
| `ens41` | `172.31.35.232` | `54.238.97.67` |
| `ens41` | `172.31.35.233` | `54.64.165.84` |
| `ens41` | `172.31.35.234` | `54.64.228.233` |
| `ens42` | `172.31.46.90` | `52.69.209.108` |
| `ens42` | `172.31.46.91` | `54.199.82.56` |
| `ens42` | `172.31.46.92` | `52.192.54.88` |
| `ens42` | `172.31.46.93` | `18.181.48.65` |

Keep the current `spread_pbs` source assignments explicit:

| Venue directory | Primary IP | Secondary IP | Lane |
| --- | --- | --- | --- |
| `binance-futures` | `172.31.46.90` | `172.31.46.91` | Market data |
| `gate-both` | `172.31.46.90` | `172.31.46.91` | Market data |
| `bitget-both` | `172.31.46.90` | `172.31.46.91` | Market data |
| `binance-margin` | `172.31.46.90` | `172.31.46.91` | Market data |
| `okex-both` | `172.31.46.90` | `172.31.46.91` | Market data |

OKEx `depth_pub` does not open exchange network connections. It consumes the
local `dat_pbs/okex-{margin,futures}` IPC services, so it has no ENI source-IP
assignment to migrate.

Other market-data egress and relay processes use the following assignments:

| Process | Network assignment | Runtime management |
| --- | --- | --- |
| `bridge_sg_model_sender/ipc_bridge` | Outgoing ZMQ TCP source `172.31.46.90` via top-level `zmq_source_ip` | `pmdaemon` process `bridge_sg_model_sender` |
| `spread_bbo_zmq_pub` (`binance-futures`) | Listen on `172.31.46.90:6320`; peers connect through the EIP associated with that private address | PM2 process `sbbzp_bn_fu` in namespace `spread_bbo_zmq_pub`, CPU `5` |
| `rclone-gdrive` | Google Drive HTTPS source `172.31.46.92` via `rclone --bind` | systemd `rclone-gdrive.service`; drop-in `/etc/systemd/system/rclone-gdrive.service.d/ens42-bind.conf` |
| `persist_sync_collector` SG sources | Collector connects to `127.0.0.1:50551-50553`; nginx binds `172.31.46.93` and proxies to the SG `行情网卡` EIP `47.128.92.224:6351-6353` | PM2 `persist_center_persist_sync_collector`; nginx config `/etc/nginx/stream-enabled/persist_center_sg_ens42.conf` |

`172.31.46.93` is assigned to non-exchange service egress through explicit
per-service binds. Review existing consumers before adding another service to it.

Manage the BBO relay with the deployed
`scripts/start_spread_bbo_zmq_pub.sh` and
`scripts/stop_spread_bbo_zmq_pub.sh`. The scripts source the deployment
`env.sh`, clean up legacy `pmdaemon` or leaked processes, and never require a
hand-written PM2 JSON file.

Do not leave these values at `0.0.0.0`. The current default route selects
`ens41`, but that is an implicit dependency on route metrics. The deployed
`spread_pbs` must prefer `./config/mkt_cfg.yaml` from its venue CWD before
the shared `~/spread_pbs/config/mkt_cfg.yaml` fallback.

Keep table 100 populated with a connected `172.31.32.0/20` route, an
`ens41` default route via `172.31.32.1` with `on-link: true`, and source
rules for `172.31.35.228` through `.234`. Keep table 101 populated with the
same connected route, an `ens42` default route, and source rules for
`172.31.46.90` through `.93`. Keep reverse-path filtering in loose mode
(`rp_filter=2`) for this multihomed, same-subnet layout. Verify every source
with `ip route get <remote> from <private-ip>`; when external observation is
authorized, also use
`curl --interface <private-ip> https://checkip.amazonaws.com`.

Restart one publisher at a time and verify its PID, CPU, and established socket
source addresses before continuing. The start-script process matcher must use
the exact `comm` value `spread_pbs`; matching any command line containing
the string would also stop the Binance Futures `spread_bbo_zmq_pub` sidecar.
After restarting Binance Futures, confirm the sidecar still runs on CPU5 and
listens on port 6320.

Both ENA devices expose 16 combined queues on NUMA node 0. The current
dedicated IRQ layout is:

| Interface | Traffic lane | ENA Tx/Rx IRQ CPU | Persistent unit |
| --- | --- | --- | --- |
| `ens41` | Order/control and non-exchange service traffic | `46` | `pin-aws-ena-irq@ens41.service` |
| `ens42` | Eligible public market data | `47` | `pin-aws-ena-irq@ens42.service` |

All 16 Tx/Rx IRQs of an interface intentionally share its one dedicated CPU.
Do not pin user-space processes to cores 46 or 47. The kernel default IRQ set
remains `0-5`; these two explicit ENA overrides are persisted by systemd, and
`irqbalance` must remain inactive.

The tracked deployment resources are:

- `scripts/pin_aws_ena_irq_affinity.sh`
- `scripts/systemd/pin-aws-ena-irq@.service`
- `scripts/systemd/pin-aws-ena-irq-ens41.default`
- `scripts/systemd/pin-aws-ena-irq-ens42.default`

Deploy them without restarting application processes:

```bash
sudo install -m 0755 scripts/pin_aws_ena_irq_affinity.sh /usr/local/sbin/
sudo install -m 0644 scripts/systemd/pin-aws-ena-irq@.service /etc/systemd/system/
sudo install -m 0644 scripts/systemd/pin-aws-ena-irq-ens41.default /etc/default/pin-aws-ena-irq-ens41
sudo install -m 0644 scripts/systemd/pin-aws-ena-irq-ens42.default /etc/default/pin-aws-ena-irq-ens42
sudo systemctl daemon-reload
sudo systemctl enable --now pin-aws-ena-irq@ens41.service pin-aws-ena-irq@ens42.service
```

Verify every IRQ's configured and effective affinity in `/proc/irq/<irq>/`;
for this layout both values must be 46 for `ens41` and 47 for `ens42`.
To roll back to the kernel housekeeping set, disable both units and run the
deployed script for each interface with `--cpus 0-5 --execute`.

Check `ethtool -c` on both interfaces independently; a newly attached ENI
does not inherit the existing interface's interrupt-coalescing settings.
Coalescing is deliberately unchanged by the IRQ operation and must be tuned
only as a separate measured HFT change.

## Model Input Auxiliaries

OKEx model-input support is deployed under:

| pmdaemon name | Binary | Venue dir | Key config |
| --- | --- | --- | --- |
| `rm_ok_mg_ok_fu` | `rolling_metrics` | `~/rolling_metrics/okex-margin-okex-futures` | Redis params key `rolling_metrics_params_okex-margin_okex-futures`; output key `rolling_metrics_thresholds_okex-margin_okex-futures` |
| `tff_ok_mg` | `trade_flow_feature_pub` | `~/trade_flow_feature/okex-margin` | `config/trade_flow_feature_pub.yaml` uses `depth_channel: "none"` |
| `tff_ok_fu` | `trade_flow_feature_pub` | `~/trade_flow_feature/okex-futures` | `config/trade_flow_feature_pub.yaml` uses `depth_channel: "depth25"` and subscribes `depth_pubs/okex-futures/depth25` |

The OKEx rolling Redis params were copied from
`rolling_metrics_params_bitget-margin_bitget-futures`, but the output hash key
must remain OKEx-specific. Do not copy Bitget's `output_hash_key` verbatim.

The OKEx futures trade-flow feature publisher requires
`okex-futures:amount-thresholds`. On 2026-06-26 this key was initialized from
the OKEx rolling symbol set with 211 fields and the same default futures
threshold shape used by Bitget/Gate futures:
`medium_notional_threshold=1.0`, `large_notional_threshold=1000.0`.

## Deployment Notes

Check worktree state before changing repo scripts:

```bash
git status --short
git diff --stat
```

Deploy the selected venues from the repo checkout:

```bash
cd ~/spread_pbs/<venue> && ./scripts/start_spread_pbs.sh
cd ~/depth_pub/binance-both && ./scripts/start_depth_pub.sh
cd ~/trade_flow_feature/okex-futures && ./scripts/start_trade_flow_feature_pub.sh
```

The live host may not use the active repo checkout for execution. It uses
deployed runtime directories under
`~/spread_pbs`, `~/depth_pub`, `~/rolling_metrics`, and
`~/trade_flow_feature`. The spread/depth start scripts read per-venue `env.sh`
files:

- `~/spread_pbs/<venue>/env.sh` with `export SPREAD_PBS_CORE='<cpu>'`
- `~/depth_pub/<venue>/env.sh` with `export DEPTH_PUB_CORE='<cpu>'`

Write or preserve only the relevant core override in each deployed venue. Do
not hard-code credentials in repo files. OKEx `spread_pbs` needs
`OKX_API_KEY`, `OKX_API_SECRET`, and `OKX_PASSPHRASE` for SBE handshake.
`~/spread_pbs/okex-both/env.sh` sources `~/okex-intra-arb01/env.sh` for the
three OKX credentials, immediately unsets `IPC_NAMESPACE`, and then sets
`SPREAD_PBS_CORE=12`. The publisher must remain in the default namespace with
the other market-data publishers; the sourced trading namespace is not part of
the SBE authentication configuration.

## Startup Order

Start each process from its deployed venue directory:

```bash
cd ~/spread_pbs/binance-margin && ./scripts/start_spread_pbs.sh
cd ~/spread_pbs/binance-futures && ./scripts/start_spread_pbs.sh
cd ~/spread_pbs/gate-both && ./scripts/start_spread_pbs.sh
cd ~/spread_pbs/bitget-both && ./scripts/start_spread_pbs.sh
cd ~/spread_pbs/okex-both && ./scripts/start_spread_pbs.sh
cd ~/depth_pub/okex-both && ./scripts/start_depth_pub.sh
cd ~/depth_pub/binance-both && ./scripts/start_depth_pub.sh
pmdaemon delete rm_ok_mg_ok_fu >/dev/null 2>&1 || true
cd ~/rolling_metrics/okex-margin-okex-futures && source ./env.sh && pmdaemon start -n rm_ok_mg_ok_fu --cwd ~/rolling_metrics/okex-margin-okex-futures -e RUST_LOG=info,rolling_metrics=info,mkt_signal=info ~/rolling_metrics/okex-margin-okex-futures/rolling_metrics -- --open-venue okex-margin --hedge-venue okex-futures
cd ~/trade_flow_feature/okex-margin && source ./env.sh && ./scripts/start_trade_flow_feature_pub.sh
cd ~/trade_flow_feature/okex-futures && source ./env.sh && ./scripts/start_trade_flow_feature_pub.sh
```

Before starting a `*-both` `spread_pbs`, stop conflicting single-side processes for the same exchange. The start script checks for conflicts, but do not rely on it as the only guard when operating live deployments.

## Verification

After startup, verify the market-data process names exist and are pinned to the
expected live cores:

```bash
pmdaemon list | grep -E "spp_|dp_"
ps -eo pid,psr,comm,args | grep -E "spread_pbs|depth_pub" | grep -v grep
pmdaemon list | grep -E "rm_ok_mg_ok_fu|tff_ok_mg|tff_ok_fu"
ps -eo pid,psr,comm,args | grep -E "rolling_metrics|trade_flow_feature_pub" | grep -E "okex|ok_" | grep -v grep
```

Expected process names:

- `spp_bn_mg`
- `spp_bn_fu`
- `spp_gt_bo`
- `spp_bg_bo`
- `spp_ok_bo`
- `dp_ok_both`
- `dp_bn_both`
- `rm_ok_mg_ok_fu`
- `tff_ok_mg`
- `tff_ok_fu`

If a process is missing or on a different CPU, inspect that venue's `env.sh` first, then restart only that venue.

For OKEx futures trade-flow feature, the startup log must include both:

- `Subscribed to trade channel: dat_pbs/okex-futures/trade`
- `Subscribed to depth channel: depth_pubs/okex-futures/depth25`
