# HFT Host Tuning Checklist

Use these items after the base GRUB isolation workflow. Apply only the changes that fit the host, kernel, exchange workload, and rollback tolerance.

## CPU And Scheduler

- Priority order:
  - Must do: GRUB isolation, explicit process pinning, post-reboot verification.
  - Usually do: CPU governor `performance`, THP disabled, swap off or very low swappiness, IRQ policy documented.
  - Test before applying broadly: C-state limits, turbo policy, SMT disablement, NIC offload changes, RPS/XPS rewrites.
- Kernel args:
  - `isolcpus=nohz,domain,managed_irq,<isolated>`
  - `nohz_full=<isolated>`
  - `rcu_nocbs=<isolated>`
  - `irqaffinity=<housekeeping>`
- For the known HK 32-vCPU split:
  - housekeeping `0-5`
  - isolated `6-31`
  - expected IRQ mask `3f` or `0000003f`
- For the current HK 16-vCPU OKEX intra plus OKEX spread_pbs split requiring 6 independent physical cores:
  - housekeeping `0-3`
  - `okex-intra-arb01` hot path `4,6,8,10`
  - OKEX `spread_pbs` `12,14`
  - isolated hot path total `4,6,8,10,12,14`
  - reserved/offline siblings `5,7,9,11,13,15`
  - expected IRQ mask `000f` or `f`
- Optional kernel args after testing:
  - `nosoftlockup` for hosts where isolated busy loops trigger false soft-lockup warnings.
  - `processor.max_cstate=1` and `intel_idle.max_cstate=1` only when latency benefit is worth power/thermal cost.
- CPU governor:
  - Prefer `performance` for trading hosts.
  - Verify with `cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor`.
- Turbo:
  - Decide per host. Turbo may improve latency but can increase jitter under thermal pressure.
- SMT:
  - Decide per strategy. For ultra-hot paths, avoid sharing a physical core with unrelated work. Pin sibling hyperthreads deliberately or disable SMT at BIOS/kernel level if testing supports it.
  - For the HK 32-vCPU / 6-housekeeping plan, verify thread siblings before accepting `0-5` plus `6-31`. If `0-5` have siblings inside `6-31`, treat those sibling CPUs as reserved/non-hot-path or change the split.
  - For the current HK 16-vCPU host, sibling pairs are adjacent. If OKEX intra plus OKEX spread_pbs need six independent physical cores, use one logical CPU from six physical cores (`4,6,8,10,12,14`) and keep their siblings (`5,7,9,11,13,15`) offline or unused.

## IRQ And Network

- Keep default IRQ affinity on housekeeping CPUs with `irqaffinity=<housekeeping>`.
- If using static IRQ placement, stop/disable `irqbalance` and document the manual policy.
- With only 6 housekeeping CPUs on a 32-vCPU host, check housekeeping saturation after market open and under persistence bursts. Move non-critical services off the host or reduce log/persist load if CPUs `0-5` are saturated.
- Verify IRQs:
  - `cat /proc/interrupts`
  - `cat /proc/irq/default_smp_affinity`
  - `cat /proc/irq/<irq>/smp_affinity_list`
- Place NIC RX/TX IRQs on housekeeping CPUs or on dedicated non-strategy cores. Avoid sharing latency-critical strategy cores with noisy NIC IRQs unless the design intentionally co-locates packet handling and user-space polling.
- Review RPS/XPS:
  - `/sys/class/net/<iface>/queues/rx-*/rps_cpus`
  - `/sys/class/net/<iface>/queues/tx-*/xps_cpus`
- Optional NIC offload checks:
  - `ethtool -k <iface>`
  - Disable only after workload testing; some offloads reduce CPU load while others add jitter.

## Memory And NUMA

- Transparent huge pages:
  - Usually disable for latency-sensitive trading processes:
    - `echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled`
    - `echo never | sudo tee /sys/kernel/mm/transparent_hugepage/defrag`
  - Persist with a systemd unit if needed.
- Swap:
  - Keep swap disabled or swappiness very low for live trading hosts.
  - Verify with `swapon --show` and `sysctl vm.swappiness`.
- NUMA:
  - Inspect `lscpu -e` and `numactl --hardware`.
  - Pin process CPU and memory to the same NUMA node with `numactl --cpunodebind` and `--membind` when applicable.

## Processes

- Pin latency-critical binaries explicitly:
  - `taskset -c 8-15 <cmd>`
  - `numactl --physcpubind=8-15 --membind=<node> <cmd>`
  - systemd: `CPUAffinity=8-15`
- Pin non-critical services to housekeeping CPUs when possible:
  - account monitors
  - persist managers
  - dashboards
  - log shippers
  - cron/system maintenance jobs
- Verify live placement:
  - `ps -eo pid,psr,pcpu,pmem,comm,args`
  - `taskset -pc <pid>`
  - `systemd-cgls` and `systemctl show <unit> -p CPUAffinity`

## Filesystem And Logging

- Avoid heavy synchronous logging on strategy hot paths.
- Put RocksDB/persistence work on housekeeping or dedicated cores if it competes with order path latency.
- Keep disk/NVMe IRQs off isolated strategy cores unless intentionally dedicated.

## Validation

- Confirm kernel args after reboot, not before.
- Compare pre/post latency metrics under live-like load:
  - p50/p99/p999 order-path latency
  - websocket decode-to-signal latency
  - signal-to-order-send latency
  - scheduling jitter from `cyclictest` if installed
- Watch for regressions:
  - lost connectivity from IRQ misplacement
  - overloaded housekeeping CPUs
  - thermal throttling
  - starved RCU/kworker activity
  - process pinning drift after restarts

## Rollback

- Remove the GRUB drop-in, run `update-grub`, reboot, and verify isolation is gone.
- Re-enable `irqbalance` if it was disabled and no static IRQ plan is maintained.
- Restore THP/governor/C-state settings to previous values if latency or stability worsens.
