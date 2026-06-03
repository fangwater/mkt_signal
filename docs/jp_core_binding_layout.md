# JP Core Binding Layout

Target host: `ubuntu@54.64.147.69` / `ip-172-31-33-133`.

This document records the agreed JP CPU isolation and core binding layout for the
32-vCPU AWS trading host.

## Hardware Shape

Observed on 2026-06-03:

- CPU: AMD EPYC 9R14
- Logical CPUs: `0-31`
- SMT: disabled/not exposed (`Thread(s) per core: 1`)
- NUMA: one node, `0-31`
- L3 cache islands:
  - `0-7`: L3#0, 32 MiB
  - `8-15`: L3#1, 32 MiB
  - `16-23`: L3#2, 32 MiB
  - `24-31`: L3#3, 32 MiB

Use complete L3 islands as the basic scheduling boundary. Do not use a
`0-5` housekeeping / `6-31` isolated split on this host, because CPUs `6-7`
share L3#0 with housekeeping CPUs.

## Kernel Isolation

Housekeeping CPUs:

```text
0-7
```

Isolated CPUs:

```text
8-31
```

The JP GRUB drop-in is:

```bash
/etc/default/grub.d/99-cpu-isolation.cfg
```

Expected kernel arguments:

```text
isolcpus=nohz,domain,managed_irq,8-31 nohz_full=8-31 rcu_nocbs=8-31 irqaffinity=0-7
```

The running kernel only picks this up after reboot.

Post-reboot pass criteria:

```bash
cat /sys/devices/system/cpu/isolated
# 8-31

cat /sys/devices/system/cpu/nohz_full
# 8-31

cat /proc/irq/default_smp_affinity
# ff or zero-padded equivalent
```

## Network And IRQ Policy

Network interrupts are not placed next to market-data parsing. Keep kernel
network work on housekeeping CPUs and keep user-space market-data parsing on
the isolated market-data island.

Policy:

```text
0-7    NIC IRQ, softirq, OS, SSH, pmdaemon/PM2, Redis/Nginx, dashboards, Jupyter, non-critical jobs
8-15   market-data parsing and publishing
16-31  signal, pre-trade, trade-engine, factors, cross-exchange hot paths
```

Runtime IRQ/RPS/XPS changes can be made without reboot, but the boot-time
`irqaffinity=0-7` default requires reboot. If static IRQ affinity is used,
`irqbalance` must either be disabled or configured so it cannot move IRQs onto
`8-31`.

## Market-Data Island

Assume all four exchanges deploy `both` market-data paths. Bind one
`spread_pbs` and one `depth_pub` per exchange to CPUs `8-15`:

```text
8   okex    spread_pbs both
9   okex    depth_pub both

10  binance spread_pbs both
11  binance depth_pub both

12  gate    spread_pbs both
13  gate    depth_pub both

14  bitget  spread_pbs both
15  bitget  depth_pub both
```

Use `SPREAD_PBS_CORE` and `DEPTH_PUB_CORE` in the corresponding deployed
`env.sh` files or start wrappers.

## Strategy Layout

The remaining isolated CPUs are assigned to hot strategy work.

```text
16  binance-intra-arb01 trade_engine main
17  binance-intra-arb01 trade_engine ipc
18  binance-intra-arb01 pre_trade
19  binance-intra-arb01 trade_signal

20  bitget-intra-arb01 trade_engine main
21  bitget-intra-arb01 trade_engine ipc
22  bitget-intra-arb01 pre_trade
23  bitget-intra-arb01 trade_signal

24  binance-futures fusion factor

25  bitget-gate-cross-arb01 trade_engine bitget
26  bitget-gate-cross-arb01 trade_engine gate
27  bitget-gate-cross-arb01 pre_trade
28  bitget-gate-cross-arb01 trade_signal / decision core 1
29  bitget-gate-cross-arb01 trade_signal / decision core 2

30  spare / failover / temporary profiling
31  spare / failover / emergency capacity
```

OKEX intra arb01 and Gate intra arb01 are intentionally not assigned isolated
hot-path cores in this layout. After kernel isolation is active, ordinary
unbound services should remain on housekeeping CPUs unless a wrapper,
`taskset`, cpuset, or explicit affinity setting places them elsewhere.

## Environment Variables

Use these existing variables where supported by the deployed start scripts:

```bash
export TRADE_ENGINE_CORE=<core>
export TRADE_ENGINE_IPC_CORE=<core>
export PRE_TRADE_CORE=<core>
export TRADE_SIGNAL_CORE=<core>
export SPREAD_PBS_CORE=<core>
export DEPTH_PUB_CORE=<core>
```

Keep dashboards, config servers, rolling metrics, notebooks, ad hoc searches,
and bulk persistence off `8-31` unless they are explicitly part of the planned
hot path.
