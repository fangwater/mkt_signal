#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENUE_DIR_REGEX='^[a-z0-9]+-(futures|margin|spot|swap|perp|perpetual)$'

usage() {
  cat <<'USAGE'
Usage:
  stop_trade_flow_feature_pub.sh

Behavior:
  - 必须在单个 venue 部署目录下执行（例如 ~/trade_flow_feature/binance-futures）。
  - venue 由当前目录名自动推断。
  - 删除 pmdaemon 进程名: trade_flow_feature_pub_<venue>
  - 可用 PMDAEMON_BIN 覆盖二进制名（默认 pmdaemon）

Examples:
  cd ~/trade_flow_feature/binance-futures
  ./scripts/stop_trade_flow_feature_pub.sh
USAGE
}

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 不支持参数: $*" >&2
      usage >&2
      exit 1
      ;;
  esac
fi

venue="$(basename "${BASE_DIR}" | tr '[:upper:]' '[:lower:]')"
if [[ ! "$venue" =~ $VENUE_DIR_REGEX ]]; then
  echo "[ERROR] 当前目录无法推断 venue: ${BASE_DIR}" >&2
  echo "[ERROR] 期望目录名形如 <exchange>-<market>，例如 binance-futures" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

name="trade_flow_feature_pub_${venue}"

echo "[INFO] Stopping ${name}"
if "${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1; then
  echo "[INFO] Stopped ${name}"
else
  echo "[WARN] ${name} not found"
fi

echo ""
echo "[INFO] Stopped: ${name}"
echo "Venue: ${venue}"
echo "Status: ${PMDAEMON[*]} list"
