#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage: stop_predict_file.sh --config <config.toml>

Stops the predict_file PM2 process named by TOML.
USAGE
}

CONFIG_PATH=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) CONFIG_PATH="${2:-}"; shift 2 ;;
    --config=*) CONFIG_PATH="${1#--config=}"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[ERROR] unsupported argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done
[[ -n "$CONFIG_PATH" ]] || { echo "[ERROR] --config is required" >&2; exit 1; }
[[ "$CONFIG_PATH" = /* ]] || CONFIG_PATH="${BASE_DIR}/${CONFIG_PATH}"
CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"
[[ -f "$CONFIG_PATH" ]] || { echo "[ERROR] config not found: $CONFIG_PATH" >&2; exit 1; }

toml_get() {
  awk -v key="$1" '
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      line = $0; sub(/^[^=]*=/, "", line); gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
      if (line ~ /^".*"$/) { sub(/^"/, "", line); sub(/"$/, "", line) }
      print line; exit
    }' "$CONFIG_PATH"
}

command -v npx >/dev/null 2>&1 || { echo "[ERROR] npx/PM2 is required" >&2; exit 1; }
INSTANCE="$(toml_get instance)"; [[ -n "$INSTANCE" ]] || INSTANCE="$(basename "$CONFIG_PATH" .toml)"
NAMESPACE="$(toml_get pm2_namespace)"; [[ -n "$NAMESPACE" ]] || NAMESPACE="predict_file"
APP_NAME="$(toml_get pm2_name)"; [[ -n "$APP_NAME" ]] || APP_NAME="predict_file-${INSTANCE}"

echo "[INFO] Stopping predict_file app=${APP_NAME} namespace=${NAMESPACE}"
npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
echo "[INFO] Stopped."
