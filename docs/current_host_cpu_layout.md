# Current Host CPU Layout

This document records the current CPU affinity plan for the local 32-logical-core
host used by the active Binance/Bitget intra and model/factor pipeline.

## Target Layout

```text
0-5    housekeeping: OS, SSH, Redis/Nginx, config servers, dashboards, Jupyter
6      fusion_factor for binance-futures
7      model_pub for binance-futures 30s and 1min models
8      spread_pbs binance-margin
9      spread_pbs binance-futures
10     spread_pbs gate-both
11     depth_pub binance-both
12     spread_pbs bitget-both
13     depth_pub bitget-both
14     depth_pub gate-both
15     persist_manager only
16     binance-intra-arb01 account_monitor
17     binance-intra-arb01 trade_signal
18     binance-intra-arb01 pre_trade
19     binance-intra-arb01 trade_engine main
20     binance-intra-arb01 trade_engine IPC/other threads
21     bitget-intra-arb01 account_monitor
22     bitget-intra-arb01 trade_signal
23     bitget-intra-arb01 pre_trade
24     bitget-intra-arb01 trade_engine main
25     bitget-intra-arb01 trade_engine IPC/other threads
26-31  overflow/helper; do not include 15 in helper cpusets
```

## Persisted Runtime Env

`/home/ubuntu/fusion_factor/binance-futures/env.sh`:

```bash
export FUSION_FACTOR_CORE=6
```

`/home/ubuntu/model_pub/binance-futures-mid-chg-30s/env.sh` and
`/home/ubuntu/model_pub/binance-futures-mid-chg-1m/env.sh`:

```bash
export MODEL_PUB_CORE=7
```

`/home/ubuntu/binance-intra-arb01/env.sh`:

```bash
export ACCOUNT_MONITOR_CORE='16'
export TRADE_SIGNAL_CORE='17'
export PRE_TRADE_CORE='18'
export TRADE_ENGINE_CORE='19'
export TRADE_ENGINE_IPC_CORE='20'
```

`/home/ubuntu/bitget-intra-arb01/env.sh`:

```bash
export ACCOUNT_MONITOR_CORE='21'
export TRADE_SIGNAL_CORE='22'
export PRE_TRADE_CORE='23'
export TRADE_ENGINE_CORE='24'
export TRADE_ENGINE_IPC_CORE='25'
export PERSIST_MANAGER_CORE='15'
```

FR persist managers are also configured for core 15:

```text
/home/ubuntu/binance_fr_arb01/env.sh
/home/ubuntu/binance_fr_arb02/env.sh
/home/ubuntu/binance_fr_arb03/env.sh
/home/ubuntu/bitget_fr_arb02/env.sh
/home/ubuntu/gate_fr_arb01/env.sh
```

Each should contain:

```bash
export PERSIST_MANAGER_CORE='15'
```

## Operational Notes

- `binance-intra-arb02` hot path is not assigned in this layout.
- `binance-intra-arb01` lightweight config/viz processes should stay on
  housekeeping cores.
- Core 15 is reserved for persist managers. Helper/overflow cpusets should use
  `26-31` or another explicitly approved range, not `15`.
- Live `taskset` changes do not update process argv. Trust
  `Cpus_allowed_list` in `/proc/<pid>/status` or `/proc/<pid>/task/<tid>/status`
  for the actual affinity.

## Verification

Check single-core pinned business processes:

```bash
for pid in $(pgrep -f '/home/ubuntu/(fusion_factor|model_pub|binance-intra-arb01|bitget-intra-arb01)' || true); do
  comm=$(cat /proc/$pid/comm 2>/dev/null || true)
  cpus=$(awk '/^Cpus_allowed_list:/{print $2}' /proc/$pid/status 2>/dev/null || true)
  psr=$(ps -p "$pid" -o psr= 2>/dev/null | tr -d ' ')
  args=$(ps -p "$pid" -o args= 2>/dev/null)
  printf '%-7s %-18s psr=%-3s allowed=%-10s %s\n' "$pid" "$comm" "$psr" "$cpus" "$args"
done
```

Check whether non-target `/home/ubuntu` business threads still allow core 6 or
7:

```bash
for f in /proc/[0-9]*/task/[0-9]*/status; do
  allowed=$(awk '/^Cpus_allowed_list:/{print $2}' "$f" 2>/dev/null || true)
  case ",$allowed," in
    *,6,*|*,7,*|*,6-7,*)
      pid=${f#/proc/}; pid=${pid%%/*}
      cmd=$(tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null || true)
      if [[ "$cmd" == *"/home/ubuntu/"* ]]; then
        echo "$f allowed=$allowed $cmd"
      fi
      ;;
  esac
done
```
