#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  start_model_pub.sh --model <model_name>

Behavior:
  - 使用 pmdaemon 启动 model_pub 进程
  - 进程名: model_pub_<model_name>
  - 可用 PMDAEMON_BIN 覆盖二进制名（默认 pmdaemon）

Examples:
  ./scripts/start_model_pub.sh --model binance-futures-mm-xgb-test
USAGE
}

MODEL_NAME=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --model)
      MODEL_NAME="${2:-}"
      if [[ -z "$MODEL_NAME" ]]; then
        echo "[ERROR] --model 需要一个值" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$MODEL_NAME" ]]; then
  echo "[ERROR] 必须提供 --model" >&2
  usage >&2
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
  "${BASE_DIR}/model_pub"
  "${BASE_DIR}/target/release/model_pub"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] model_pub binary not found. Build first with: cargo build --release --bin model_pub" >&2
  exit 1
fi

if [[ ! -f "${BASE_DIR}/config/model_pub.toml" ]]; then
  echo "[ERROR] config not found: ${BASE_DIR}/config/model_pub.toml" >&2
  exit 1
fi

name="model_pub_${MODEL_NAME}"
rust_log="${RUST_LOG:-info}"
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

json_name="$(json_escape "$name")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_model="$(json_escape "$MODEL_NAME")"
json_rust_log="$(json_escape "$rust_log")"

cat >"$cfg_file" <<JSON
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

echo "[INFO] Restarting ${name}"
"${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$name"

echo ""
echo "[INFO] Started: ${name}"
echo "Model: ${MODEL_NAME}"
echo "Config: ${BASE_DIR}/config/model_pub.toml"
echo "Logs: ${PMDAEMON[*]} logs ${name} --follow"
echo "Status: ${PMDAEMON[*]} list"
