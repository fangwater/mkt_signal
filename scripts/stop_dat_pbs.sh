#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  stop_dat_pbs.sh [--root <path>] (--exchange <exchange> | <exchange> | <venue...>)

Examples:
  ./scripts/stop_dat_pbs.sh --exchange bybit
  ./scripts/stop_dat_pbs.sh bybit
  ./scripts/stop_dat_pbs.sh bybit-futures bybit-margin
  ./scripts/stop_dat_pbs.sh --root "$HOME/dat_pbs" --venue bybit-futures

Notes:
  - Exchange expands to default venues:
      okex    -> okex-futures okex-margin
      binance -> binance-futures binance-margin
      bybit   -> bybit-futures bybit-margin
      bitget  -> bitget-futures bitget-margin
      gate    -> gate-futures gate-margin
  - Default deploy root is $HOME/dat_pbs.
  - Each venue is stopped by entering <root>/<venue>/ and invoking ./scripts/stop_dat_pbs.sh.
EOF
}

KNOWN_EXCHANGES=("okex" "binance" "bybit" "bitget" "gate")
KNOWN_VENUES=(
  "okex-futures" "okex-margin"
  "binance-futures" "binance-margin"
  "bybit-futures" "bybit-margin"
  "bitget-futures" "bitget-margin"
  "gate-futures" "gate-margin"
)

is_known_exchange() {
  local value="${1,,}"
  for exchange in "${KNOWN_EXCHANGES[@]}"; do
    if [[ "$value" == "$exchange" ]]; then
      return 0
    fi
  done
  return 1
}

is_known_venue() {
  local value="${1,,}"
  for venue in "${KNOWN_VENUES[@]}"; do
    if [[ "$value" == "$venue" ]]; then
      return 0
    fi
  done
  return 1
}

default_venues_for_exchange() {
  local exchange="${1,,}"
  case "$exchange" in
    okex) echo "okex-futures okex-margin" ;;
    binance) echo "binance-futures binance-margin" ;;
    bybit) echo "bybit-futures bybit-margin" ;;
    bitget) echo "bitget-futures bitget-margin" ;;
    gate) echo "gate-futures gate-margin" ;;
    *)
      echo ""
      return 1
      ;;
  esac
}

TARGET_ROOT="${DAT_PBS_ROOT:-$HOME/dat_pbs}"
EXCHANGE=""
VENUES=()
POSITIONAL=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root|--dir)
      TARGET_ROOT="${2:-}"
      if [[ -z "$TARGET_ROOT" ]]; then
        echo "[ERROR] --root/--dir 需要一个路径" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      if [[ -z "$EXCHANGE" ]]; then
        echo "[ERROR] --exchange 需要一个值" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    --venue)
      venue="${2:-}"
      if [[ -z "$venue" ]]; then
        echo "[ERROR] --venue 需要一个值" >&2
        usage >&2
        exit 1
      fi
      VENUES+=("${venue,,}")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$EXCHANGE" && ${#POSITIONAL[@]} -gt 0 ]]; then
  if is_known_exchange "${POSITIONAL[0]}"; then
    EXCHANGE="${POSITIONAL[0]}"
    POSITIONAL=("${POSITIONAL[@]:1}")
  fi
fi

if [[ ${#VENUES[@]} -eq 0 && ${#POSITIONAL[@]} -gt 0 ]]; then
  for venue in "${POSITIONAL[@]}"; do
    VENUES+=("${venue,,}")
  done
fi

if [[ ${#VENUES[@]} -eq 0 && -n "$EXCHANGE" ]]; then
  read -r -a VENUES <<<"$(default_venues_for_exchange "$EXCHANGE")"
fi

if [[ ${#VENUES[@]} -eq 0 ]]; then
  echo "[ERROR] 必须提供 --exchange 或至少一个 venue" >&2
  usage >&2
  exit 1
fi

for venue in "${VENUES[@]}"; do
  if ! is_known_venue "$venue"; then
    echo "[ERROR] 不支持的 venue: $venue" >&2
    exit 1
  fi
done

for venue in "${VENUES[@]}"; do
  venue_dir="${TARGET_ROOT%/}/${venue}"
  stop_script="$venue_dir/scripts/stop_dat_pbs.sh"
  if [[ ! -x "$stop_script" ]]; then
    echo "[ERROR] 未找到可执行停止脚本: $stop_script" >&2
    echo "[HINT] 先确认该 venue 已部署到 ${TARGET_ROOT%/}/${venue}" >&2
    exit 1
  fi

  echo "[INFO] 停止 dat_pbs venue=${venue}"
  (
    cd "$venue_dir"
    ./scripts/stop_dat_pbs.sh
  )
  sleep 1
done

echo ""
echo "[INFO] Stopped venues: ${VENUES[*]}"
echo "[INFO] root_dir: ${TARGET_ROOT%/}"
