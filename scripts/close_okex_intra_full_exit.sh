#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_ENV_NAME="okex-intra-arb01"

usage() {
  cat <<EOF
用法:
  scripts/close_okex_intra_full_exit.sh [--execute] [--symbol BTC]
  scripts/close_okex_intra_full_exit.sh [--execute] [--skip-assets BNB,SOL]
  scripts/close_okex_intra_full_exit.sh --full-exit [--execute]

说明:
  - 这是 OKX 同所期现盘的便捷入口，默认对准 ${DEFAULT_ENV_NAME}
  - 默认只清净敞口：撤现货/合约委托，然后按短板清掉多出来的一侧
  - 加 --full-exit 才会把现货腿和合约腿都清到 0
  - 默认 dry-run；加 --execute 才会真实撤单 + 下市价单
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
  mode_args=(--mode align-exposure)
fi

if [[ "$has_env_selector" -eq 1 ]]; then
  exec "$SCRIPT_DIR/close_intra_full_exit.sh" "${pass_args[@]}" "${mode_args[@]}"
fi

exec "$SCRIPT_DIR/close_intra_full_exit.sh" --env-name "$DEFAULT_ENV_NAME" "${pass_args[@]}" "${mode_args[@]}"
