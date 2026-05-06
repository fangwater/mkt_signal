#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INNER_SCRIPT="$ROOT_DIR/scripts/spread_pbs/deploy_spread_pbs.sh"

if [[ ! -f "$INNER_SCRIPT" ]]; then
  echo "[ERROR] 未找到 spread_pbs 部署脚本: $INNER_SCRIPT" >&2
  exit 1
fi

exec bash "$INNER_SCRIPT" "$@"
