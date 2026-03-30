#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  scripts/close_binance_fr_all_orders.sh --env-name binance_fr_trade01 [--env-name binance_fr_trade02 ...] [--scope <um|margin|both>] [--execute]
  scripts/close_binance_fr_all_orders.sh trade01 trade02 trade03 --execute

Notes:
  - Default env names are built as: binance_fr_<suffix>
  - For each env dir, this script sources $HOME/<env-name>/env.sh
  - It then runs scripts/binance_cancel_all_unified_open_orders.py
  - Default is dry-run; add --execute to actually cancel all open orders
EOF
}

ENV_NAMES=()
SUFFIXES=()
SCOPE="both"
EXECUTE=0
PYTHON_BIN="${PYTHON_BIN:-python3}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-name)
      if [[ -z "${2:-}" ]]; then
        echo "[ERROR] --env-name requires a value" >&2
        exit 1
      fi
      ENV_NAMES+=("${2}")
      shift 2
      ;;
    --scope)
      if [[ -z "${2:-}" ]]; then
        echo "[ERROR] --scope requires a value" >&2
        exit 1
      fi
      SCOPE="${2,,}"
      shift 2
      ;;
    --python)
      if [[ -z "${2:-}" ]]; then
        echo "[ERROR] --python requires a value" >&2
        exit 1
      fi
      PYTHON_BIN="${2}"
      shift 2
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
      SUFFIXES+=("$1")
      shift
      ;;
  esac
done

case "$SCOPE" in
  um|margin|both)
    ;;
  *)
    echo "[ERROR] invalid --scope: $SCOPE" >&2
    exit 1
    ;;
esac

if [[ "${#ENV_NAMES[@]}" -eq 0 && "${#SUFFIXES[@]}" -eq 0 ]]; then
  echo "[ERROR] provide at least one --env-name or suffix" >&2
  usage >&2
  exit 1
fi

for suffix in "${SUFFIXES[@]}"; do
  suffix_lc="$(echo "$suffix" | tr 'A-Z' 'a-z')"
  ENV_NAMES+=("binance_fr_${suffix_lc}")
done

CANCEL_SCRIPT="${ROOT_DIR}/scripts/binance_cancel_all_unified_open_orders.py"
if [[ ! -f "$CANCEL_SCRIPT" ]]; then
  echo "[ERROR] cancel script not found: $CANCEL_SCRIPT" >&2
  exit 1
fi

exit_code=0
for env_name in "${ENV_NAMES[@]}"; do
  env_dir="$HOME/$env_name"
  env_file="$env_dir/env.sh"
  if [[ ! -f "$env_file" ]]; then
    echo "[ERROR] env file not found: $env_file" >&2
    exit_code=1
    continue
  fi

  echo "[INFO] env_name=$env_name env_file=$env_file scope=$SCOPE execute=$EXECUTE"
  cmd=(bash -lc "source $(printf '%q' "$env_file") && cd $(printf '%q' "$ROOT_DIR") && exec $(printf '%q' "$PYTHON_BIN") $(printf '%q' "$CANCEL_SCRIPT") --scope $(printf '%q' "$SCOPE")")
  if [[ "$EXECUTE" -eq 1 ]]; then
    cmd[2]+=" --execute"
  fi
  if ! "${cmd[@]}"; then
    exit_code=1
  fi
done

exit "$exit_code"
