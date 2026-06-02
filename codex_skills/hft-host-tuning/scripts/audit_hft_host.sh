#!/usr/bin/env bash
set -euo pipefail

section() {
  printf '\n==== %s ====\n' "$1"
}

run() {
  printf '$ %s\n' "$*"
  "$@" 2>&1 || true
}

read_file() {
  local path="$1"
  printf '%s: ' "$path"
  if [ -r "$path" ]; then
    cat "$path"
  else
    printf 'not-readable\n'
  fi
}

section "host"
run hostname
run date -Is
run uname -a

section "kernel command line"
read_file /proc/cmdline
printf '\n'
if [ -r /proc/cmdline ]; then
  tr ' ' '\n' </proc/cmdline | grep -E '^(isolcpus|nohz_full|rcu_nocbs|irqaffinity|processor.max_cstate|intel_idle.max_cstate|nosoftlockup|panic)=' || true
fi

section "cpu topology"
run nproc
run lscpu
run lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE,MAXMHZ,MINMHZ
read_file /sys/devices/system/cpu/possible
read_file /sys/devices/system/cpu/online
read_file /sys/devices/system/cpu/isolated
read_file /sys/devices/system/cpu/nohz_full

section "cpu governor"
if compgen -G '/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor' >/dev/null; then
  for f in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    read_file "$f"
  done
else
  printf 'no cpufreq scaling_governor files found\n'
fi

section "irq"
read_file /proc/irq/default_smp_affinity
run systemctl is-active irqbalance
run systemctl is-enabled irqbalance
run sh -c "grep -E 'CPU|eth|ens|ena|nvme|mlx|ixgbe|i40e|virtio' /proc/interrupts | sed -n '1,80p'"

section "network queues"
for dev in /sys/class/net/*; do
  [ -d "$dev" ] || continue
  iface="$(basename "$dev")"
  [ "$iface" = "lo" ] && continue
  printf '\n-- %s --\n' "$iface"
  run ip -brief link show "$iface"
  for f in "$dev"/queues/rx-*/rps_cpus "$dev"/queues/tx-*/xps_cpus; do
    [ -r "$f" ] && read_file "$f"
  done
done

section "memory"
read_file /sys/kernel/mm/transparent_hugepage/enabled
read_file /sys/kernel/mm/transparent_hugepage/defrag
run swapon --show
run sysctl vm.swappiness
run numactl --hardware

section "process placement sample"
run ps -eo pid,psr,pcpu,pmem,comm,args --sort=-pcpu

section "grub snippets"
for f in /etc/default/grub /etc/default/grub.d/*.cfg; do
  [ -r "$f" ] || continue
  printf '\n-- %s --\n' "$f"
  grep -nE 'GRUB_CMDLINE|isolcpus|nohz_full|rcu_nocbs|irqaffinity' "$f" || true
done
