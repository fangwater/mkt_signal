#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  stop_latency_collector.sh [--instance <name>]

Defaults:
  --instance sg

Stops PM2 app:  latency_collector-<instance>
PM2 namespace:  latency_collector
USAGE
}

INSTANCE="sg"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance)  INSTANCE="${2:-}"; shift 2;;
    --instance=*) INSTANCE="${1#--instance=}"; shift;;
    -h|--help)   usage; exit 0;;
    *) echo "[ERROR] unsupported argument: $1" >&2; usage >&2; exit 1;;
  esac
done

if [[ -z "$INSTANCE" ]]; then
  echo "[ERROR] --instance must not be empty" >&2; exit 1
fi

if ! command -v npx >/dev/null 2>&1; then
  echo "[ERROR] npx not found; PM2 scripts require npx pm2" >&2
  exit 1
fi

NAMESPACE="latency_collector"
APP_NAME="latency_collector-${INSTANCE}"

echo "[INFO] stopping ${APP_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
echo "[INFO] stopped."
