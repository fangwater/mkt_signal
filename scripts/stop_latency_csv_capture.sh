#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  stop_latency_csv_capture.sh --config <config.toml>

Behavior:
  - Stops the latency_csv_capture PM2 process named by the TOML config.
  - PM2 name defaults to latency_csv_capture-<instance>, or config key pm2_name.

Example:
  ./scripts/stop_latency_csv_capture.sh --config config/latency_csv_capture_sg.toml
USAGE
}

CONFIG_PATH=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH="${2:-}"
      if [[ -z "$CONFIG_PATH" ]]; then
        echo "[ERROR] --config requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --config=*)
      CONFIG_PATH="${1#--config=}"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unsupported argument: $1 (allowed: --config <config.toml>)" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$CONFIG_PATH" ]]; then
  echo "[ERROR] config is required: --config <config.toml>" >&2
  usage >&2
  exit 1
fi

if [[ "$CONFIG_PATH" != /* ]]; then
  CONFIG_PATH="${BASE_DIR}/${CONFIG_PATH}"
fi
CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "[ERROR] config not found: $CONFIG_PATH" >&2
  exit 1
fi

toml_get() {
  local key="$1"
  awk -v key="$key" '
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      line = $0
      sub(/^[^=]*=/, "", line)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
      if (line ~ /^".*"$/) {
        sub(/^"/, "", line)
        sub(/"$/, "", line)
      }
      print line
      exit
    }
  ' "$CONFIG_PATH"
}

if ! command -v npx >/dev/null 2>&1; then
  echo "[ERROR] npx not found; PM2 scripts require npx pm2" >&2
  exit 1
fi

INSTANCE="$(toml_get instance)"
if [[ -z "$INSTANCE" ]]; then
  INSTANCE="$(basename "$CONFIG_PATH" .toml)"
fi
NAMESPACE="$(toml_get pm2_namespace)"
if [[ -z "$NAMESPACE" ]]; then
  NAMESPACE="latency_csv_capture"
fi
APP_NAME="$(toml_get pm2_name)"
if [[ -z "$APP_NAME" ]]; then
  APP_NAME="latency_csv_capture-${INSTANCE}"
fi

echo "[INFO] Stopping latency_csv_capture instance=${INSTANCE} app=${APP_NAME} namespace=${NAMESPACE}"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
echo "[INFO] Stopped."
