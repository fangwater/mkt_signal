#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  start_model_score_rolling.sh [--model-name <name>]

Behavior:
  - Run in deploy dir (for example: ~/model_score_rolling/<model_name>)
  - If --model-name is omitted, infer from deploy dir name
  - Start with pmdaemon process name: model_score_rolling_<model_name>

Examples:
  cd ~/model_score_rolling/binance-futures-mm-xgb-test
  ./scripts/model_score_rolling/start_model_score_rolling.sh
  ./scripts/model_score_rolling/start_model_score_rolling.sh --model-name binance-futures-mm-xgb-test
USAGE
}

MODEL_NAME=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --model-name)
      MODEL_NAME="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$MODEL_NAME" ]]; then
  MODEL_NAME="$(basename "$BASE_DIR")"
fi
if [[ -z "$MODEL_NAME" ]]; then
  echo "[ERROR] cannot infer model_name from base dir: $BASE_DIR" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/model_score_rolling"
  "${BASE_DIR}/target/release/model_score_rolling"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done
if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] model_score_rolling binary not found. Build/deploy first." >&2
  exit 1
fi

PROC_NAME="model_score_rolling_${MODEL_NAME}"
RUST_LOG_VAL="${RUST_LOG:-info,model_score_rolling=info,mkt_signal=info}"
CFG_FILE="$(mktemp)"
trap 'rm -f "$CFG_FILE" >/dev/null 2>&1 || true' EXIT

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

json_name="$(json_escape "$PROC_NAME")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_model="$(json_escape "$MODEL_NAME")"
json_rust_log="$(json_escape "$RUST_LOG_VAL")"

cat >"$CFG_FILE" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": ["${json_model}"],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}"
      }
    }
  ]
}
JSON

echo "[INFO] restarting ${PROC_NAME}"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$CFG_FILE" start --name "$PROC_NAME"

echo "[INFO] started: ${PROC_NAME}"
echo "[INFO] model: ${MODEL_NAME}"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
