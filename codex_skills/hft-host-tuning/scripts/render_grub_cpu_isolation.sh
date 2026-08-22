#!/usr/bin/env bash
set -euo pipefail

housekeeping=""
isolated=""
output="/tmp/99-cpu-isolation.cfg"
apply=0
run_update_grub=0

usage() {
  cat <<'EOF'
Usage:
  render_grub_cpu_isolation.sh --housekeeping 0-7 --isolated 8-15 [options]

Options:
  --housekeeping CPUS   CPUs for OS, IRQs, RCU, daemons, e.g. 0-7
  --isolated CPUS       CPUs dedicated to latency-sensitive processes, e.g. 8-15
  --output PATH         Dry-run output path, default /tmp/99-cpu-isolation.cfg
  --apply               Install to /etc/default/grub.d/99-cpu-isolation.cfg
  --update-grub         Run update-grub after --apply
  -h, --help            Show this help

Dry-run is default. Applying requires root or sudo.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --housekeeping)
      housekeeping="${2:-}"
      shift 2
      ;;
    --isolated)
      isolated="${2:-}"
      shift 2
      ;;
    --output)
      output="${2:-}"
      shift 2
      ;;
    --apply)
      apply=1
      shift
      ;;
    --update-grub)
      run_update_grub=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ -z "$housekeeping" ] || [ -z "$isolated" ]; then
  usage >&2
  exit 2
fi

validate_cpu_list() {
  local value="$1"
  if ! printf '%s' "$value" | grep -Eq '^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$'; then
    printf 'invalid CPU list: %s\n' "$value" >&2
    exit 2
  fi
}

validate_cpu_list "$housekeeping"
validate_cpu_list "$isolated"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

cat >"$tmp" <<EOF
# CPU isolation for low-latency trading: keep $housekeeping as housekeeping, dedicate $isolated.
# - isolcpus with managed_irq covers managed IRQs such as AWS Nitro ENA/NVMe.
# - nohz_full drops the periodic scheduler tick on isolated CPUs when possible.
# - rcu_nocbs offloads RCU callbacks to housekeeping CPUs.
# - irqaffinity sets default IRQ affinity for IRQs registered at boot.
GRUB_CMDLINE_LINUX_DEFAULT="\$GRUB_CMDLINE_LINUX_DEFAULT isolcpus=nohz,domain,managed_irq,$isolated nohz_full=$isolated rcu_nocbs=$isolated irqaffinity=$housekeeping"
EOF

if [ "$apply" -eq 0 ]; then
  install -m 0644 "$tmp" "$output"
  printf 'wrote dry-run config to %s\n' "$output"
  cat "$output"
  exit 0
fi

target="/etc/default/grub.d/99-cpu-isolation.cfg"
install_cmd=(install -m 0644 "$tmp" "$target")
if [ "$(id -u)" -ne 0 ]; then
  install_cmd=(sudo install -m 0644 "$tmp" "$target")
fi
"${install_cmd[@]}"
printf 'installed %s\n' "$target"

if [ "$run_update_grub" -eq 1 ]; then
  if [ "$(id -u)" -ne 0 ]; then
    sudo update-grub
  else
    update-grub
  fi
  printf 'update-grub complete; reboot is required for changes to take effect\n'
else
  printf 'run sudo update-grub, then reboot for changes to take effect\n'
fi
