#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  start_model_1m_pub.sh

Behavior:
  - 必须在 model_1m_pub 部署目录下执行（例如 ~/model_1m_pub/binance-futures-mid-re-1m）
  - model_name 由当前目录名自动推断
  - 默认 warming 目录: ./history_ylabel（必须已存在）
  - 使用 pmdaemon 启动进程名: model_1m_pub_<model_name>
  - 若 env.sh 设置 MODEL_1M_PUB_CORE=<N>，则传给 binary 做主线程绑核
  - 可用 PMDAEMON_BIN 覆盖二进制名（默认 pmdaemon）

Examples:
  cd ~/model_1m_pub/binance-futures-mid-re-1m
  ./scripts/start_model_1m_pub.sh
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

MODEL_NAME="$(basename "${BASE_DIR}")"
if [[ -z "$MODEL_NAME" ]]; then
  echo "[ERROR] 无法从目录名推断 model_name: ${BASE_DIR}" >&2
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
  "${BASE_DIR}/model_1m_pub"
  "${BASE_DIR}/target/release/model_1m_pub"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] model_1m_pub binary not found. Build first with: cargo build --release --bin model_1m_pub --features model-ort" >&2
  exit 1
fi

if [[ ! -f "${BASE_DIR}/config/model_1m_pub.toml" ]]; then
  echo "[ERROR] config not found: ${BASE_DIR}/config/model_1m_pub.toml" >&2
  exit 1
fi

WARMING_DIR="${BASE_DIR}/history_ylabel"
if [[ ! -d "$WARMING_DIR" ]]; then
  echo "[ERROR] warming dir not found: ${WARMING_DIR}" >&2
  exit 1
fi

ONNX_LIB_CANDIDATES=(
  "${BASE_DIR}/third_party/onnxruntime/linux-x86_64/lib"
  "${HOME}/crypto_mkt/mkt_signal/third_party/onnxruntime/linux-x86_64/lib"
  "${HOME}/mkt_signal/third_party/onnxruntime/linux-x86_64/lib"
)

ONNX_LIB_DIR=""
for cand in "${ONNX_LIB_CANDIDATES[@]}"; do
  if [[ -d "$cand" ]]; then
    ONNX_LIB_DIR="$cand"
    break
  fi
done

if [[ -z "$ONNX_LIB_DIR" ]]; then
  echo "[ERROR] ONNX runtime lib dir not found" >&2
  exit 1
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

extra_args_json=""
if [[ -n "${MODEL_1M_PUB_CORE:-}" ]]; then
  if [[ ! "$MODEL_1M_PUB_CORE" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] MODEL_1M_PUB_CORE 必须为单个整数 (got: $MODEL_1M_PUB_CORE)" >&2
    exit 1
  fi
  extra_args_json=", \"--core\", \"${MODEL_1M_PUB_CORE}\""
  echo "[INFO] core bind ${MODEL_1M_PUB_CORE} (from $ENV_FILE:MODEL_1M_PUB_CORE)"
fi

name="model_1m_pub_${MODEL_NAME}"
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
json_warming_dir="$(json_escape "$WARMING_DIR")"
json_rust_log="$(json_escape "$rust_log")"
json_ld_library_path="$(json_escape "${ONNX_LIB_DIR}:${BASE_DIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}")"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": ["${json_model}", "--warming-dir", "${json_warming_dir}"${extra_args_json}],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}",
        "LD_LIBRARY_PATH": "${json_ld_library_path}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting ${name}"
STOP_SCRIPT="${SCRIPT_DIR}/stop_model_1m_pub.sh"
if [[ ! -x "$STOP_SCRIPT" ]]; then
  echo "[ERROR] stop script not found or not executable: $STOP_SCRIPT" >&2
  exit 1
fi
"$STOP_SCRIPT"
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$name"

echo ""
echo "[INFO] Started: ${name}"
echo "Model: ${MODEL_NAME}"
echo "Config: ${BASE_DIR}/config/model_1m_pub.toml"
echo "Factor plan: factor_plan_1m"
echo "Warming dir: ${WARMING_DIR}"
echo "Logs: ${PMDAEMON[*]} logs ${name} --follow"
echo "Status: ${PMDAEMON[*]} list"
