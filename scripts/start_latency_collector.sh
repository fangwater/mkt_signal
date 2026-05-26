#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  start_latency_collector.sh [--instance <name>] [--out-dir <path>] [--stats-secs <n>]

Defaults:
  --instance   sg
  --out-dir    <BASE_DIR>/data/latency_collector
  --stats-secs 60

PM2 app:        latency_collector-<instance>
PM2 namespace:  latency_collector
USAGE
}

INSTANCE="sg"
OUT_DIR=""
STATS_SECS="60"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance)  INSTANCE="${2:-}"; shift 2;;
    --instance=*) INSTANCE="${1#--instance=}"; shift;;
    --out-dir)   OUT_DIR="${2:-}"; shift 2;;
    --out-dir=*) OUT_DIR="${1#--out-dir=}"; shift;;
    --stats-secs) STATS_SECS="${2:-}"; shift 2;;
    --stats-secs=*) STATS_SECS="${1#--stats-secs=}"; shift;;
    -h|--help)   usage; exit 0;;
    *) echo "[ERROR] unsupported argument: $1" >&2; usage >&2; exit 1;;
  esac
done

if [[ -z "$INSTANCE" ]]; then
  echo "[ERROR] --instance must not be empty" >&2; exit 1
fi

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="${BASE_DIR}/data/latency_collector"
fi
if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="${BASE_DIR}/${OUT_DIR}"
fi
mkdir -p "$OUT_DIR"

BIN_CANDIDATES=(
  "${BASE_DIR}/latency_collector"
  "${BASE_DIR}/target/release/latency_collector"
)
BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then BIN_PATH="$cand"; break; fi
done
if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] latency_collector binary not found in ${BASE_DIR} or target/release/" >&2
  exit 1
fi

if ! command -v npx >/dev/null 2>&1; then
  echo "[ERROR] npx not found; PM2 scripts require npx pm2" >&2
  exit 1
fi

NAMESPACE="latency_collector"
APP_NAME="latency_collector-${INSTANCE}"

npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
sleep 1

echo "[INFO] starting ${APP_NAME} (namespace=${NAMESPACE})"
echo "[INFO] bin=${BIN_PATH}"
echo "[INFO] out_dir=${OUT_DIR}"
echo "[INFO] stats_secs=${STATS_SECS}"

(
  cd "$BASE_DIR"
  RUST_LOG="info" npx pm2 start "$BIN_PATH" \
    --name "$APP_NAME" \
    --namespace "$NAMESPACE" \
    --interpreter none \
    -- \
    --out-dir "$OUT_DIR" \
    --stats-secs "$STATS_SECS"
)

echo "[INFO] status: npx pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] logs:   npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
