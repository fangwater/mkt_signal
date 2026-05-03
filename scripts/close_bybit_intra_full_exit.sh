#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_ENV_NAME="bybit-intra-arb01"

usage() {
  cat <<EOF
Usage:
  scripts/close_bybit_intra_full_exit.sh [--execute] [--symbol BTC]
  scripts/close_bybit_intra_full_exit.sh --full-exit [--execute]

Notes:
  - Convenience entry for Bybit intra arb, default env is ${DEFAULT_ENV_NAME}
  - Default mode: cancel all orders, then adjust futures so hedge_qty -> -open_qty
  - With --full-exit: cancel all orders, then close both spot and futures legs to zero
  - Default is dry-run; add --execute to actually cancel and submit orders
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

has_env_selector=0
has_mode=0
pass_args=()
for arg in "$@"; do
  case "$arg" in
    --env-name|--env-name=*|--env-dir|--env-dir=*)
      has_env_selector=1
      pass_args+=("$arg")
      ;;
    --mode|--mode=*)
      has_mode=1
      pass_args+=("$arg")
      ;;
    --full-exit)
      has_mode=1
      pass_args+=(--mode full-exit)
      ;;
    *)
      pass_args+=("$arg")
      ;;
  esac
done

mode_args=()
if [[ "$has_mode" -eq 0 ]]; then
  mode_args=(--mode align-futures-to-spot)
fi

if [[ "$has_env_selector" -eq 1 ]]; then
  exec "$SCRIPT_DIR/close_intra_full_exit.sh" "${pass_args[@]}" "${mode_args[@]}"
fi

exec "$SCRIPT_DIR/close_intra_full_exit.sh" --env-name "$DEFAULT_ENV_NAME" "${pass_args[@]}" "${mode_args[@]}"
