---
name: hft-host-tuning
description: Use when auditing, deploying, configuring, or verifying low-latency trading host tuning for HFT systems, especially Linux CPU isolation via GRUB, isolated versus housekeeping cores, nohz_full, rcu_nocbs, IRQ affinity, irqbalance, CPU governor, THP, NUMA, NIC IRQ/RPS/XPS, process pinning, and post-reboot validation on AWS or bare-metal trading machines.
---

# HFT Host Tuning

Use this skill for low-latency trading host preparation. Treat remote hosts as production-risk: prefer read-only audit first, write explicit config second, then require reboot and post-reboot verification.

## Safety Rules

- State the target host, environment, and whether the command only observes or mutates state before running remote commands.
- Do not change live host tuning implicitly. Generate config first; apply only when the user confirms the target core split.
- Keep one clear housekeeping CPU set for the OS, IRQs, RCU callbacks, logging, SSH, and non-latency services.
- Keep latency-sensitive binaries pinned to isolated CPUs using service wrappers, `taskset`, `numactl`, systemd `CPUAffinity=`, or pmdaemon/PM2 launch config.
- Do not disable `irqbalance` blindly. If using static IRQ affinity, stop/disable it intentionally and verify NIC/NVMe IRQs.
- Reboot is required for GRUB kernel parameters to take effect. Do not declare success before post-reboot checks pass.

## Known SG Pattern

Existing SG host pattern:

- Host: `ubuntu@47.131.162.78`
- Housekeeping CPUs: `0-7`
- Isolated CPUs: `8-15`
- Drop-in: `/etc/default/grub.d/99-cpu-isolation.cfg`
- Runtime kernel args:
  - `isolcpus=nohz,domain,managed_irq,8-15`
  - `nohz_full=8-15`
  - `rcu_nocbs=8-15`
  - `irqaffinity=0-7`
- Runtime verification:
  - `/sys/devices/system/cpu/isolated` -> `8-15`
  - `/sys/devices/system/cpu/nohz_full` -> `8-15`
  - `/proc/irq/default_smp_affinity` -> mask for housekeeping CPUs, e.g. `00ff` for `0-7`

## Known HK 32-CPU Pattern

For a 32-vCPU HK trading host where only 6 CPUs are public/housekeeping:

- Housekeeping CPUs: `0-5`
- Isolated CPUs: `6-31`
- Expected kernel args:
  - `isolcpus=nohz,domain,managed_irq,6-31`
  - `nohz_full=6-31`
  - `rcu_nocbs=6-31`
  - `irqaffinity=0-5`
- Expected default IRQ affinity mask: low 6 bits, commonly `3f` or zero-padded as `0000003f`.

Use this exact split only after confirming the host exposes CPUs `0-31` and checking SMT sibling placement:

```bash
cat /sys/devices/system/cpu/possible
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE
for f in /sys/devices/system/cpu/cpu*/topology/thread_siblings_list; do echo "$f $(cat "$f")"; done | sort -V
```

If SMT is enabled and housekeeping CPUs `0-5` have siblings such as `16-21`, do not run latency-critical processes on those sibling CPUs. Prefer either making both sibling threads housekeeping/reserved, disabling SMT, or choosing the isolated hot-path set from physical cores that do not share with housekeeping CPUs.

## Known HK 16-CPU OKEX-Intra Pattern

For the current HK host `el-cc-okx-srv01`, observed topology is 16 logical CPUs, 8 physical cores, SMT sibling pairs `0-1`, `2-3`, `4-5`, `6-7`, `8-9`, `10-11`, `12-13`, `14-15`. When `okex-intra-arb01` plus the two OKEX `spread_pbs` processes need 6 independent physical cores, use 2 full physical cores for housekeeping and one logical thread from each remaining physical core for the hot path:

- Housekeeping CPUs: `0-3`
- Isolated hot-path CPUs: `4,6,8,10,12,14`
- Reserved/offline sibling CPUs: `5,7,9,11,13,15`
- `okex-intra-arb01` mapping:
  - `TRADE_ENGINE_CORE=4`
  - `TRADE_ENGINE_IPC_CORE=6`
  - `PRE_TRADE_CORE=8`
  - `TRADE_SIGNAL_CORE=10`
- OKEX `spread_pbs` mapping:
  - `okex-margin`: `SPREAD_PBS_CORE=12`
  - `okex-futures`: `SPREAD_PBS_CORE=14`
- Expected kernel args:
  - `isolcpus=nohz,domain,managed_irq,4,6,8,10,12,14`
  - `nohz_full=4,6,8,10,12,14`
  - `rcu_nocbs=4,6,8,10,12,14`
  - `irqaffinity=0-3`

This layout preserves six independent physical cores for latency-sensitive OKEX intra and OKEX spread publishing while keeping housekeeping on two complete physical cores. Do not pin latency-critical work to `5,7,9,11,13,15`; either offline them with CPU hotplug or leave them unused through cpuset/taskset policy.

```bash
for c in 5 7 9 11 13 15; do echo 0 | sudo tee /sys/devices/system/cpu/cpu$c/online; done
```

## Workflow

1. Audit current state.

Use `scripts/audit_hft_host.sh` locally on the target host, or run it over SSH without writing files when possible.

```bash
bash scripts/audit_hft_host.sh
```

For remote ad hoc checks, collect at minimum:

```bash
hostname
uname -a
cat /proc/cmdline
cat /sys/devices/system/cpu/possible
cat /sys/devices/system/cpu/online
cat /sys/devices/system/cpu/isolated 2>/dev/null || true
cat /sys/devices/system/cpu/nohz_full 2>/dev/null || true
cat /proc/irq/default_smp_affinity
systemctl is-active irqbalance 2>/dev/null || true
```

2. Choose the CPU split.

For a 16-vCPU trading host, use this default unless topology or workload says otherwise:

- Housekeeping: `0-7`
- Isolated: `8-15`

For the HK 32-vCPU host requested by the operator:

- Housekeeping: `0-5`
- Isolated: `6-31`

For the current HK 16-vCPU OKEX intra plus OKEX spread_pbs case requiring 6 independent physical cores:

- Housekeeping: `0-3`
- Isolated hot path: `4,6,8,10,12,14`
- `okex-intra-arb01`: `4,6,8,10`
- OKEX `spread_pbs`: `12,14`
- Reserved/offline siblings: `5,7,9,11,13,15`

For other hosts, inspect topology first:

```bash
lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE,MAXMHZ,MINMHZ
```

Keep enough housekeeping cores for network IRQs, disk IRQs, system daemons, account monitors, persistence, and dashboards. Put latency-critical order path or market-data hot loops on isolated cores.

3. Generate GRUB CPU isolation config.

Use the renderer script; dry-run is default:

```bash
bash scripts/render_grub_cpu_isolation.sh --housekeeping 0-7 --isolated 8-15
```

HK 32-vCPU dry-run:

```bash
bash scripts/render_grub_cpu_isolation.sh --housekeeping 0-5 --isolated 6-31
```

HK 16-vCPU OKEX intra/spread dry-run:

```bash
bash scripts/render_grub_cpu_isolation.sh --housekeeping 0-3 --isolated 4,6,8,10,12,14
```

Apply only after confirming the split:

```bash
sudo bash scripts/render_grub_cpu_isolation.sh --housekeeping 0-7 --isolated 8-15 --apply --update-grub
```

HK 32-vCPU apply:

```bash
sudo bash scripts/render_grub_cpu_isolation.sh --housekeeping 0-5 --isolated 6-31 --apply --update-grub
```

HK 16-vCPU OKEX intra/spread apply:

```bash
sudo bash scripts/render_grub_cpu_isolation.sh --housekeeping 0-3 --isolated 4,6,8,10,12,14 --apply --update-grub
```

Expected drop-in:

```bash
GRUB_CMDLINE_LINUX_DEFAULT="$GRUB_CMDLINE_LINUX_DEFAULT isolcpus=nohz,domain,managed_irq,8-15 nohz_full=8-15 rcu_nocbs=8-15 irqaffinity=0-7"
```

HK 32-vCPU expected drop-in:

```bash
GRUB_CMDLINE_LINUX_DEFAULT="$GRUB_CMDLINE_LINUX_DEFAULT isolcpus=nohz,domain,managed_irq,6-31 nohz_full=6-31 rcu_nocbs=6-31 irqaffinity=0-5"
```

HK 16-vCPU OKEX intra/spread expected drop-in:

```bash
GRUB_CMDLINE_LINUX_DEFAULT="$GRUB_CMDLINE_LINUX_DEFAULT isolcpus=nohz,domain,managed_irq,4,6,8,10,12,14 nohz_full=4,6,8,10,12,14 rcu_nocbs=4,6,8,10,12,14 irqaffinity=0-3"
```

4. Reboot.

The GRUB parameters do not affect the running kernel until reboot:

```bash
sudo reboot
```

5. Verify after reboot.

Run:

```bash
bash scripts/audit_hft_host.sh
```

Minimum pass criteria:

- `/proc/cmdline` contains the expected `isolcpus`, `nohz_full`, `rcu_nocbs`, and `irqaffinity`.
- `/sys/devices/system/cpu/isolated` equals the isolated set.
- `/sys/devices/system/cpu/nohz_full` equals the isolated set.
- `/proc/irq/default_smp_affinity` maps to housekeeping CPUs.
- Latency-critical processes are pinned to isolated CPUs.
- No unexpected high-CPU daemon, IRQ, or kernel work is running on isolated CPUs.

## Extra Tuning

When the user asks what else can be done for low-latency host tuning, read [host-tuning-checklist.md](references/host-tuning-checklist.md). It covers CPU governor, C-states, THP, swap, NUMA, IRQ placement, RPS/XPS, NIC offloads, process pinning, logging, and rollback.

## Rollback

To remove GRUB isolation, delete or rename `/etc/default/grub.d/99-cpu-isolation.cfg`, run `sudo update-grub`, reboot, then verify `/sys/devices/system/cpu/isolated` is empty. Keep this as an explicit operator action.
