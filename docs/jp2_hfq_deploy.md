# JP2 HFQ Deploy Notes

Target host: `ubuntu@52.68.224.23` / `ip-172-31-33-150`.

This document records the current JP2 access, dual-public-IP routing, and HFQ
CPU isolation layout for the 8-vCPU AWS host.

## Access

SSH key:

```text
aws-jp-aws-hfq.pem
```

Primary SSH target:

```bash
ssh -i aws-jp-aws-hfq.pem ubuntu@52.68.224.23
```

Observed on 2026-06-09:

- Primary public IP: `52.68.224.23`
- Secondary public IP: `52.69.78.134`
- Primary private IP: `172.31.33.150`
- Secondary private IP: `172.31.33.151`
- NIC: `ens5`
- Primary gateway: `172.31.32.1`
- Subnet: `172.31.32.0/20`

## Dual Public IP Routing

JP2 uses source-based routing so traffic sourced from the secondary private IP
egresses via the second public IP.

Required behavior:

```bash
curl -4 --interface 172.31.33.150 ifconfig.me/ip
# 52.68.224.23

curl -4 --interface 172.31.33.151 ifconfig.me/ip
# 52.69.78.134
```

Persistent config lives in:

```bash
/etc/netplan/50-cloud-init.yaml
```

Key routing policy:

```text
from 172.31.33.151/32 lookup 10001
```

Key table `10001` routes:

```text
172.31.32.0/20 dev ens5 src 172.31.33.151
default via 172.31.32.1 dev ens5 src 172.31.33.151
```

Important: do not reuse the older `172.31.7.124` / `172.31.0.1` examples on
this host. JP2 is on the `172.31.32.0/20` subnet.

## Hardware Shape

Observed on 2026-06-09:

- CPU model: `Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz`
- Logical CPUs: `0-7`
- Physical cores exposed: `8`
- SMT: not exposed / not supported at runtime
- `Thread(s) per core`: `1`
- Socket(s): `1`
- NUMA: one node, `0-7`

Topology summary:

```text
CPU(s): 8
Thread(s) per core): 1
Core(s) per socket: 8
Socket(s): 1
```

Runtime SMT control state:

```text
/sys/devices/system/cpu/smt/control -> notsupported
```

That means there are no visible sibling threads to disable in the running
kernel. We still stage `nosmt=force` in GRUB so the boot policy stays explicit.

## Kernel Isolation

Housekeeping CPUs:

```text
0-1
```

Isolated CPUs:

```text
2-7
```

GRUB drop-in:

```bash
/etc/default/grub.d/99-cpu-isolation.cfg
```

Configured kernel arguments:

```text
nosmt=force isolcpus=nohz,domain,managed_irq,2-7 nohz_full=2-7 rcu_nocbs=2-7 irqaffinity=0-1
```

JP2 was rebooted on 2026-06-09 and the running kernel picked up the isolation settings.

Post-reboot verified runtime:

```bash
cat /sys/devices/system/cpu/isolated
# 2-7

cat /sys/devices/system/cpu/nohz_full
# 2-7

cat /proc/irq/default_smp_affinity
# 03 or zero-padded equivalent
```

## IRQ Policy

`irqbalance` remains enabled, but it is configured not to move IRQs onto
isolated CPUs.

File:

```bash
/etc/default/irqbalance
```

Pinned exclusion:

```text
IRQBALANCE_BANNED_CPULIST=2-7
```

Operational intent:

```text
0-1  OS, SSH, IRQs, softirq, logs, Redis/Nginx, dashboards, pmdaemon/PM2
2-7  HFQ hot path only
```

## Base Services

Configured on 2026-06-09:

- `redis-server` installed and enabled.
- `nginx` installed and enabled.
- `libnginx-mod-stream` installed for TCP stream proxying.
- Redis listens locally on `127.0.0.1:6379`.
- Nginx HTTP/WebSocket proxy listens on public `0.0.0.0:4191`.
- Nginx stream proxy listens on public `0.0.0.0:4190` and forwards to `127.0.0.1:6379`.

Mapping files:

```bash
/home/ubuntu/nginx_locations.txt
/home/ubuntu/nginx_streams.txt
```

Validation:

```bash
redis-cli -h 127.0.0.1 -p 6379 ping
# PONG

redis-cli -h 127.0.0.1 -p 4190 ping
# PONG

sudo nginx -t
# successful
```

Both public IPs accepted TCP connections on `4190` and `4191` after setup.

## Deploying Repo Envs To JP2

Existing deploy scripts default to the older JP host. To target JP2, override
the remote host and key explicitly.

Example:

```bash
export FR_DEPLOY_HOST=ubuntu@52.68.224.23
export FR_DEPLOY_KEY=$PWD/aws-jp-aws-hfq.pem
```

Then run the normal deploy wrapper, for example:

```bash
bash scripts/deploy_fr_binance.sh arb01
bash scripts/deploy_intra_binance_std.sh arb01
```

These wrappers still build locally, create `$HOME/<env-name>/`, and rsync to
the remote host. They do not start processes remotely.

## Binance Intra Arb01 Local Core Allocation

Use the two housekeeping CPUs for OS work only. Bind hot services explicitly
using the existing env vars:

```bash
export TRADE_ENGINE_CORE=<core>
export TRADE_ENGINE_IPC_CORE=<core>
export PRE_TRADE_CORE=<core>
export TRADE_SIGNAL_CORE=<core>
export SPREAD_PBS_CORE=<core>
```

For local Binance spot/futures intra arb01, do not run `depth_pub` unless the
strategy is explicitly changed to consume it. Run two single-sided `spread_pbs`
processes instead of one `binance-both` process, so margin and futures market
data each get one isolated core.

Current 6-core layout:

```text
2  spread_pbs binance-margin
3  spread_pbs binance-futures
4  trade_signal
5  pre_trade
6  trade_engine main
7  trade_engine IPC thread
```

The two order-path cores are `TRADE_ENGINE_CORE` and
`TRADE_ENGINE_IPC_CORE`. There is not currently a separate per-venue
spot/futures order-core setting in the Binance `trade_engine` launcher.

Keep notebooks, config servers, bulk persistence, and ad hoc shell work on
housekeeping CPUs unless they are intentionally part of the hot path.


## Runtime Processes Verified On 2026-06-09

Market data uses two single-sided `spread_pbs` processes and intentionally does
not run `depth_pub`:

```text
spp_bn_mg  /home/ubuntu/spread_pbs/binance-margin/spread_pbs --venue binance-margin --core 2
spp_bn_fu  /home/ubuntu/spread_pbs/binance-futures/spread_pbs --venue binance-futures --core 3
```

Rolling metrics runs without explicit CPU binding:

```text
rm_bn_mg_bn_fu  /home/ubuntu/rolling_metrics/binance-margin-binance-futures/rolling_metrics --open-venue binance-margin --hedge-venue binance-futures
```

Redis params for rolling metrics were written to:

```text
rolling_metrics_params_binance-margin_binance-futures
```

Rolling output was verified in:

```text
rolling_metrics_thresholds_binance-margin_binance-futures
```

Trade-flow volatility publishers run in vol-only mode without explicit CPU
binding. Their configs set `data_source.depth_channel: "none"`, so they do not
open `depth_pubs/<venue>/depth25` and only publish `factor_pub/<venue>/rl_vol`:

```text
tff_bn_mg  /home/ubuntu/trade_flow_feature/binance-margin/trade_flow_feature_pub --venue binance-margin
tff_bn_fu  /home/ubuntu/trade_flow_feature/binance-futures/trade_flow_feature_pub --venue binance-futures
```

The deployed `trade_flow_feature_pub` binary was built from a code path that
supports `depth_channel=none`; full `trade_flow_feature` publishing remains
enabled for the default `depth25`/`depth50` configs.

## Next Step

Before declaring CPU isolation active, reboot JP2 and re-run:

```bash
cat /proc/cmdline
cat /sys/devices/system/cpu/isolated
cat /sys/devices/system/cpu/nohz_full
cat /proc/irq/default_smp_affinity
lscpu
```
