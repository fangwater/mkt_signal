#!/usr/bin/env bash
set -euo pipefail

IFACE=""
CPUS=""
EXECUTE=0
STOP_IRQBALANCE=0

usage() {
  cat <<'USAGE'
Usage:
  scripts/pin_aws_ena_irq_affinity.sh --iface <interface> --cpus <cpu-list> [--stop-irqbalance] [--execute]

Default is dry-run. Add --execute to write /proc/irq/*/smp_affinity_list.

Examples:
  scripts/pin_aws_ena_irq_affinity.sh --iface ens41 --cpus 45
  scripts/pin_aws_ena_irq_affinity.sh --iface ens41 --cpus 45 --stop-irqbalance --execute
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --iface)
      IFACE="${2:-}"
      shift 2
      ;;
    --cpus)
      CPUS="${2:-}"
      shift 2
      ;;
    --stop-irqbalance)
      STOP_IRQBALANCE=1
      shift
      ;;
    --execute)
      EXECUTE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$IFACE" || -z "$CPUS" ]]; then
  echo "[ERROR] --iface and --cpus are required" >&2
  usage >&2
  exit 1
fi

if [[ ! "$CPUS" =~ ^[0-9,-]+$ ]]; then
  echo "[ERROR] invalid --cpus value: $CPUS" >&2
  exit 1
fi

normalize_cpu_list() {
  awk -v value="$1" '
    BEGIN {
      count = split(value, chunks, ",")
      max_cpu = -1
      for (i = 1; i <= count; i++) {
        if (chunks[i] !~ /^[0-9]+(-[0-9]+)?$/) {
          exit 1
        }

        parts = split(chunks[i], bounds, "-")
        if (parts == 1) {
          first = last = bounds[1] + 0
        } else if (parts == 2) {
          first = bounds[1] + 0
          last = bounds[2] + 0
        } else {
          exit 1
        }
        if (first > last) {
          exit 1
        }
        for (cpu = first; cpu <= last; cpu++) {
          selected[cpu] = 1
          if (cpu > max_cpu) max_cpu = cpu
        }
      }
      separator = ""
      for (cpu = 0; cpu <= max_cpu; cpu++) {
        if (selected[cpu]) {
          printf "%s%d", separator, cpu
          separator = ","
        }
      }
      print ""
    }
  '
}

REQUESTED_CPUS="$(normalize_cpu_list "$CPUS")" || {
  echo "[ERROR] invalid --cpus range: $CPUS" >&2
  exit 1
}

IFS=',' read -r -a REQUESTED_CPU_ARRAY <<<"$REQUESTED_CPUS"
for cpu in "${REQUESTED_CPU_ARRAY[@]}"; do
  cpu_dir="/sys/devices/system/cpu/cpu${cpu}"
  if [[ ! -d "$cpu_dir" ]] || [[ -f "$cpu_dir/online" && "$(<"$cpu_dir/online")" != "1" ]]; then
    echo "[ERROR] requested CPU is not online: $cpu" >&2
    exit 1
  fi
done

if ! [[ -d "/sys/class/net/$IFACE" ]]; then
  echo "[ERROR] interface not found: $IFACE" >&2
  exit 1
fi

mapfile -t IRQS < <(awk -v iface="$IFACE" '
  index($0, iface "-Tx-Rx-") {
    gsub(":", "", $1)
    print $1
  }
' /proc/interrupts)

if [[ "${#IRQS[@]}" -eq 0 ]]; then
  echo "[ERROR] no Tx-Rx IRQs found for interface: $IFACE" >&2
  exit 1
fi

run_root() {
  if [[ "$EUID" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

echo "[INFO] interface=$IFACE cpus=$CPUS execute=$EXECUTE stop_irqbalance=$STOP_IRQBALANCE"
echo "[INFO] IRQs: ${IRQS[*]}"

if [[ "$STOP_IRQBALANCE" -eq 1 ]]; then
  if command -v systemctl >/dev/null 2>&1; then
    if systemctl is-active --quiet irqbalance; then
      if [[ "$EXECUTE" -eq 1 ]]; then
        echo "[INFO] disabling irqbalance"
        run_root systemctl disable --now irqbalance
      else
        echo "[DRY-RUN] would disable irqbalance"
      fi
    else
      echo "[INFO] irqbalance is not active"
    fi
  else
    echo "[WARN] systemctl not available; skip irqbalance"
  fi
fi

for irq in "${IRQS[@]}"; do
  current="$(cat "/proc/irq/$irq/smp_affinity_list" 2>/dev/null || echo "?")"
  if [[ "$EXECUTE" -eq 1 ]]; then
    printf '%s\n' "$CPUS" | run_root tee "/proc/irq/$irq/smp_affinity_list" >/dev/null
    updated="$(cat "/proc/irq/$irq/smp_affinity_list" 2>/dev/null || echo "?")"
    effective="$(cat "/proc/irq/$irq/effective_affinity_list" 2>/dev/null || echo "?")"
    updated_normalized="$(normalize_cpu_list "$updated" 2>/dev/null || true)"
    effective_normalized="$(normalize_cpu_list "$effective" 2>/dev/null || true)"
    effective_is_subset=1
    IFS=',' read -r -a EFFECTIVE_CPU_ARRAY <<<"$effective_normalized"
    for cpu in "${EFFECTIVE_CPU_ARRAY[@]}"; do
      if [[ ",$REQUESTED_CPUS," != *",$cpu,"* ]]; then
        effective_is_subset=0
        break
      fi
    done
    if [[ "$updated_normalized" != "$REQUESTED_CPUS" || -z "$effective_normalized" || "$effective_is_subset" -ne 1 ]]; then
      echo "[ERROR] irq=$irq requested=$CPUS configured=$updated effective=$effective" >&2
      exit 1
    fi
    echo "[SET] irq=$irq $current -> configured=$updated effective=$effective"
  else
    echo "[DRY-RUN] irq=$irq $current -> $CPUS"
  fi
done

echo "[INFO] matching /proc/interrupts rows:"
awk -v iface="$IFACE" 'index($0, iface "-Tx-Rx-") { print }' /proc/interrupts
