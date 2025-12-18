#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
用法: xarb_scripts/stop_xarb_monitors.sh [open|hedge|all]

说明:
  - 会基于部署目录名推断 open/hedge exchange（目录名需形如 <open>-<hedge>-xarb-...）
  - 默认 all：停止两个 PM2 进程
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

MODE="${1:-all}"
case "$MODE" in
  open|hedge|all) ;;
  *)
    echo "[ERROR] 未知参数: $MODE"
    usage
    exit 1
    ;;
esac

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
}

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

OPEN_EXCHANGE=""
HEDGE_EXCHANGE=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$ ]]; then
  OPEN_EXCHANGE="$(normalize_exchange "${BASH_REMATCH[1]}")"
  HEDGE_EXCHANGE="$(normalize_exchange "${BASH_REMATCH[2]}")"
fi

if [[ -z "$OPEN_EXCHANGE" || -z "$HEDGE_EXCHANGE" || "$OPEN_EXCHANGE" == "$HEDGE_EXCHANGE" ]]; then
  echo "[ERROR] 无法从目录名推断 open/hedge (dir=$dir_name)，期望 <open>-<hedge>-xarb-..."
  exit 1
fi

stop_one() {
  local side="$1"
  local pm2_name="xarb_am_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_${side}"
  echo "[INFO] Stopping $pm2_name"
  npx pm2 delete "$pm2_name" >/dev/null 2>&1 || true
}

case "$MODE" in
  open)
    stop_one "open"
    ;;
  hedge)
    stop_one "hedge"
    ;;
  all)
    stop_one "open"
    stop_one "hedge"
    ;;
esac

