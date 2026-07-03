# Current Host CPU Layout

This document records the current CPU affinity plan for the local 48-online-core
host. It was refreshed against live `/proc` affinity and runtime `env.sh` files
on 2026-07-03.

## Kernel Layout

```text
online CPUs      0-47
offline CPUs     48-95
housekeeping     0-5
isolated/nohz    6-47
irq/kthread      0-5
```

Many helper processes show `Cpus_allowed_list=0-5,48-95`. Because `48-95` are
offline, those processes effectively run on housekeeping cores `0-5`.

## Current Layout

```text
0-5    housekeeping: OS, SSH, Redis/Nginx, PM2, VSCode/Codex, config servers,
       dashboards, Jupyter, FR helpers, rolling_metrics, trade_flow_feature
6      fusion_factor_pub binance-futures
7      fusion_factor_pub okex-futures
8      spread_pbs binance-margin
9      spread_pbs binance-futures
10     spread_pbs gate-both
11     spread_pbs bitget-both
12     spread_pbs okex-both
13     depth_pub okex-both
14     depth_pub binance-both
15     pinned persist_manager pool
16     binance-intra-arb01 account_monitor
17     binance-intra-arb01 trade_signal
18     binance-intra-arb01 pre_trade
19     binance-intra-arb01 trade_engine main thread
20     binance-intra-arb01 trade_engine IPC/other thread
21     spare
22     gate-intra-arb01 account_monitor
23     gate-intra-arb01 trade_signal
24     gate-intra-arb01 pre_trade
25     gate-intra-arb01 trade_engine main thread
26     gate-intra-arb01 trade_engine IPC/other thread
27     binance_mm_alpha account_monitor
28     binance_mm_alpha trade_signal
29     binance_mm_alpha pre_trade
30     binance_mm_alpha trade_engine main thread
31     binance_mm_alpha trade_engine IPC/other thread
32     spare
33     bitget-intra-arb01 account_monitor
34     bitget-intra-arb01 trade_signal
35     bitget-intra-arb01 pre_trade
36     bitget-intra-arb01 trade_engine main thread
37     bitget-intra-arb01 trade_engine IPC/other thread
38     bitget-gate-cross-arb01 account monitors, open and hedge, persisted
39     bitget-gate-cross-arb01 trade_signal, persisted
40     bitget-gate-cross-arb01 pre_trade, persisted
41     bitget-gate-cross-arb01 open trade_engine main thread, persisted
42     bitget-gate-cross-arb01 open trade_engine IPC/other thread, persisted
43     bitget-gate-cross-arb01 hedge trade_engine main thread, persisted
44     bitget-gate-cross-arb01 hedge trade_engine IPC/other thread, persisted
45     spare
46     spare
47     model_pub for all deployed binance-futures and okex-swap model services
```

The `bitget-gate-cross-arb01` block is persisted in
`/home/ubuntu/bitget-gate-cross-arb01/env.sh`; no live cross processes were
running when this document was refreshed.

## Persisted Runtime Env

Market data and factor/model publishers:

```text
/home/ubuntu/fusion_factor/binance-futures/env.sh             FUSION_FACTOR_CORE=6
/home/ubuntu/fusion_factor/okex-futures/env.sh                FUSION_FACTOR_CORE=7
/home/ubuntu/spread_pbs/binance-margin/env.sh                 SPREAD_PBS_CORE=8
/home/ubuntu/spread_pbs/binance-futures/env.sh                SPREAD_PBS_CORE=9
/home/ubuntu/spread_pbs/gate-both/env.sh                      SPREAD_PBS_CORE=10
/home/ubuntu/spread_pbs/bitget-both/env.sh                    SPREAD_PBS_CORE=11
/home/ubuntu/spread_pbs/okex-both/env.sh                      SPREAD_PBS_CORE=12
/home/ubuntu/depth_pub/okex-both/env.sh                       DEPTH_PUB_CORE=13
/home/ubuntu/depth_pub/binance-both/env.sh                    DEPTH_PUB_CORE=14
/home/ubuntu/model_pub/*/env.sh                               MODEL_PUB_CORE=47
```

Hot-path strategy environments:

```text
/home/ubuntu/binance-intra-arb01/env.sh
  ACCOUNT_MONITOR_CORE=16
  TRADE_SIGNAL_CORE=17
  PRE_TRADE_CORE=18
  TRADE_ENGINE_CORE=19
  TRADE_ENGINE_IPC_CORE=20
  PERSIST_MANAGER_CORE=15

/home/ubuntu/gate-intra-arb01/env.sh
  ACCOUNT_MONITOR_CORE=22
  TRADE_SIGNAL_CORE=23
  PRE_TRADE_CORE=24
  TRADE_ENGINE_CORE=25
  TRADE_ENGINE_IPC_CORE=26
  PERSIST_MANAGER_CORE=15

/home/ubuntu/binance_mm_alpha/env.sh
  ACCOUNT_MONITOR_CORE=27
  TRADE_SIGNAL_CORE=28
  PRE_TRADE_CORE=29
  TRADE_ENGINE_CORE=30
  TRADE_ENGINE_IPC_CORE=31
  PERSIST_MANAGER_CORE=15

/home/ubuntu/bitget-intra-arb01/env.sh
  ACCOUNT_MONITOR_CORE=33
  TRADE_SIGNAL_CORE=34
  PRE_TRADE_CORE=35
  TRADE_ENGINE_CORE=36
  TRADE_ENGINE_IPC_CORE=37
  PERSIST_MANAGER_CORE=15

/home/ubuntu/bitget-gate-cross-arb01/env.sh
  ACCOUNT_MONITOR_OPEN_CORE=38
  ACCOUNT_MONITOR_HEDGE_CORE=38
  TRADE_SIGNAL_CORE=39
  PRE_TRADE_CORE=40
  TRADE_ENGINE_OPEN_CORE=41
  TRADE_ENGINE_OPEN_IPC_CORE=42
  TRADE_ENGINE_HEDGE_CORE=43
  TRADE_ENGINE_HEDGE_IPC_CORE=44
  PERSIST_MANAGER_CORE=15
```

FR persist managers for `binance_fr_arb01`, `binance_fr_arb02`,
`binance_fr_arb03`, `binance_fr_arb04`, `bitget_fr_arb02`, `gate_fr_arb01`,
and `gate_fr_arb02` are configured for core `15`.

`okex_fr_arb01` and `okex-intra-arb01` persist managers were not pinned when
this document was refreshed; they were running on the housekeeping set.

## Operational Notes

- Live `taskset` changes do not update process argv. Trust
  `Cpus_allowed_list` in `/proc/<pid>/status` or
  `/proc/<pid>/task/<tid>/status` for actual affinity.
- `trade_engine` uses split thread affinity: the main TID is pinned to the
  main core and the secondary IPC/worker TID is pinned to the IPC core.
- Core `15` is the pinned persist-manager pool for active FR/intra/MM
  environments and bitget-gate-cross-arb01. The cross hot path is a `38-44`
  block, with its manager on core `15`.
- Keep unpinned helpers on housekeeping `0-5` unless they are intentionally
  assigned into one of the spare isolated cores.

## Verification

Check per-core load across all online CPUs:

```bash
ps -Leo psr,pcpu --no-headers |
  awk '{a[$1]+=$2} END{for (c=0;c<=47;c++) printf "%s %.1f\n", c, a[c]+0}' |
  sort -n
```

Check live `/home/ubuntu` runtime affinities:

```bash
for d in /proc/[0-9]*; do
  pid=${d##*/}
  cmd=$(tr '\0' ' ' < "$d/cmdline" 2>/dev/null || true)
  case "$cmd" in
    *"/home/ubuntu/"*)
      allowed=$(awk '/^Cpus_allowed_list:/{print $2}' "$d/status" 2>/dev/null || true)
      comm=$(cat "$d/comm" 2>/dev/null || true)
      printf '%-10s %-18s %-8s %s\n' "$allowed" "$comm" "$pid" "$cmd"
      ;;
  esac
done | sort -V
```
