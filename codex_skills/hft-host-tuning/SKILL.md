---
name: hft-host-tuning
description: Use when auditing, deploying, configuring, or verifying low-latency trading host tuning for HFT/HFQ systems, especially Linux CPU isolation via GRUB, isolated versus housekeeping cores, nohz_full, rcu_nocbs, IRQ affinity, irqbalance, CPU governor, THP, NUMA, NIC IRQ/RPS/XPS, process pinning, and post-reboot validation on AWS or bare-metal trading machines.
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

## Known JP2 8-CPU HFQ Pattern

For JP2 host `ubuntu@52.68.224.23` / `ip-172-31-33-150`, verified on 2026-06-09:

- Public IPs:
  - primary: `52.68.224.23`
  - secondary: `52.69.78.134`
- Private IPs:
  - primary: `172.31.33.150`
  - secondary: `172.31.33.151`
- NIC: `ens5`
- Gateway: `172.31.32.1`
- Subnet: `172.31.32.0/20`
- CPU: `Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz`
- Logical CPUs: `0-7`
- Physical cores exposed: `8`
- NUMA: one node, `0-7`
- SMT runtime state: not exposed / not supported (`Thread(s) per core: 1`, `/sys/devices/system/cpu/smt/control -> notsupported`)

This host is not a "disable half the sibling threads" case. The running kernel
already exposes one thread per core. Still stage `nosmt=force` in GRUB so the
boot policy remains explicit and future instance-shape changes do not silently
re-enable sibling threads.

Target split:

- Housekeeping CPUs: `0-1`
- Isolated CPUs: `2-7`

Persistent files:

- GRUB drop-in: `/etc/default/grub.d/99-cpu-isolation.cfg`
- irqbalance config: `/etc/default/irqbalance`
- netplan source-routing config: `/etc/netplan/50-cloud-init.yaml`

Configured kernel args:

- `nosmt=force`
- `isolcpus=nohz,domain,managed_irq,2-7`
- `nohz_full=2-7`
- `rcu_nocbs=2-7`
- `irqaffinity=0-1`

irqbalance policy:

- Keep `irqbalance` active.
- Set `IRQBALANCE_BANNED_CPULIST=2-7` so IRQs stay on housekeeping CPUs.

Verified post-reboot runtime on 2026-06-09:

- `/proc/cmdline` contains `nosmt=force isolcpus=nohz,domain,managed_irq,2-7 nohz_full=2-7 rcu_nocbs=2-7 irqaffinity=0-1`
- `/sys/devices/system/cpu/isolated` -> `2-7`
- `/sys/devices/system/cpu/nohz_full` -> `2-7`
- `/proc/irq/default_smp_affinity` -> `03`

Source-routing verification:

- `curl -4 --interface 172.31.33.150 ifconfig.me/ip` -> `52.68.224.23`
- `curl -4 --interface 172.31.33.151 ifconfig.me/ip` -> `52.69.78.134`

Base services verified on 2026-06-09:

- `redis-server` active on local `127.0.0.1:6379`.
- `nginx` active.
- `libnginx-mod-stream` installed.
- Public `4191` is the HTTP/WebSocket reverse-proxy port driven by `/home/ubuntu/nginx_locations.txt`.
- Public `4190` is the TCP stream proxy driven by `/home/ubuntu/nginx_streams.txt`; current mapping forwards `4190 -> redis://127.0.0.1:6379/0`.
- `redis-cli -h 127.0.0.1 -p 6379 ping` and `redis-cli -h 127.0.0.1 -p 4190 ping` both returned `PONG`.
- Both public IPs accepted TCP connections on `4190` and `4191`.

JP2 Binance intra arb01 local 6-core binding suggestion:

- `2`: `spread_pbs` for `binance-margin`
- `3`: `spread_pbs` for `binance-futures`
- `4`: `trade_signal`
- `5`: `pre_trade`
- `6`: `trade_engine` main (`TRADE_ENGINE_CORE`)
- `7`: `trade_engine` IPC thread (`TRADE_ENGINE_IPC_CORE`)

This local Binance intra layout intentionally does not run `depth_pub`; deploy
single-sided `spread_pbs` processes for margin and futures rather than one
`binance-both` process when dedicating one core to each market side.

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

For the JP2 8-vCPU HFQ host:

- Housekeeping: `0-1`
- Isolated: `2-7`

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

JP2 8-vCPU HFQ dry-run:

```bash
bash scripts/render_grub_cpu_isolation.sh --housekeeping 0-1 --isolated 2-7
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

JP2 8-vCPU HFQ apply:

```bash
sudo bash scripts/render_grub_cpu_isolation.sh --housekeeping 0-1 --isolated 2-7 --apply --update-grub
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

JP2 8-vCPU HFQ expected drop-in:

```bash
GRUB_CMDLINE_LINUX_DEFAULT="$GRUB_CMDLINE_LINUX_DEFAULT isolcpus=nohz,domain,managed_irq,2-7 nohz_full=2-7 rcu_nocbs=2-7 irqaffinity=0-1 nosmt=force"
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

When the user asks what else can be done for HFT/HFQ tuning, read [host-tuning-checklist.md](references/host-tuning-checklist.md). It covers CPU governor, C-states, THP, swap, NUMA, IRQ placement, RPS/XPS, NIC offloads, process pinning, logging, and rollback.

## Rollback

To remove GRUB isolation, delete or rename `/etc/default/grub.d/99-cpu-isolation.cfg`, run `sudo update-grub`, reboot, then verify `/sys/devices/system/cpu/isolated` is empty. Keep this as an explicit operator action.
