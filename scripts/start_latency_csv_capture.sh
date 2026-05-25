#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  start_latency_csv_capture.sh --config <config.toml>

Behavior:
  - Starts latency_csv_capture via PM2.
  - Runtime parameters come from TOML config only.
  - PM2 name defaults to latency_csv_capture-<instance>, or config key pm2_name.

Example:
  ./scripts/start_latency_csv_capture.sh --config config/latency_csv_capture_sg.toml
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

BIN_CANDIDATES=(
  "${BASE_DIR}/latency_csv_capture"
  "${BASE_DIR}/target/release/latency_csv_capture"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] latency_csv_capture binary not found. Build/deploy first." >&2
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
RUST_LOG_VAL="$(toml_get rust_log)"
if [[ -z "$RUST_LOG_VAL" ]]; then
  RUST_LOG_VAL="info"
fi
OUT_DIR="$(toml_get out_dir)"
if [[ -n "$OUT_DIR" && "$OUT_DIR" != /* ]]; then
  mkdir -p "${BASE_DIR}/${OUT_DIR}"
elif [[ -n "$OUT_DIR" ]]; then
  mkdir -p "$OUT_DIR"
fi

npx pm2 delete "$APP_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
sleep 1

echo "[INFO] Starting latency_csv_capture instance=${INSTANCE} app=${APP_NAME} namespace=${NAMESPACE}"
echo "[INFO] config=${CONFIG_PATH}"
if [[ -n "$OUT_DIR" ]]; then
  echo "[INFO] out_dir=${OUT_DIR}"
fi
(
  cd "$BASE_DIR"
  RUST_LOG="$RUST_LOG_VAL" npx pm2 start "$BIN_PATH" \
    --name "$APP_NAME" \
    --namespace "$NAMESPACE" \
    --interpreter none \
    -- \
    --config "$CONFIG_PATH"
)

echo "[INFO] Started: npx pm2 status --namespace ${NAMESPACE} ${APP_NAME}"
echo "[INFO] Logs:    npx pm2 logs --namespace ${NAMESPACE} ${APP_NAME}"
