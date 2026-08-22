#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  start_depth_pub_general.sh

Behavior:
  - 在部署目录执行（例如 ~/depth_pub/general）。
  - pm2 进程名 dp_general，namespace depth_pub。
  - env.sh 可 export DEPTH_PUB_CORE、BINANCE_SBE_API_KEY / BINANCE_API_KEY。
  - 不含 Bybit（Bybit 在 sg）。
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

if command -v pm2 >/dev/null 2>&1; then
  PM2=(pm2)
elif command -v npx >/dev/null 2>&1; then
  PM2=(npx pm2)
else
  echo "[ERROR] pm2/npx not found" >&2
  exit 1
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/depth_pub_general"
  "${SCRIPT_DIR}/../depth_pub_general"
  "${SCRIPT_DIR}/../target/release/depth_pub_general"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] depth_pub_general binary not found. Build: cargo build --release --bin depth_pub_general" >&2
  exit 1
fi

PROC_NAME="dp_general"
NAMESPACE="depth_pub"
rust_log="${RUST_LOG:-info}"

if [[ -f "$BASE_DIR/env.sh" ]]; then
  # shellcheck disable=SC1090
  set -a; source "$BASE_DIR/env.sh"; set +a
fi

ARGS=()
if [[ -n "${DEPTH_PUB_CORE:-}" ]]; then
  if [[ ! "$DEPTH_PUB_CORE" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] DEPTH_PUB_CORE 必须为单个整数 (got: $DEPTH_PUB_CORE)" >&2
    exit 1
  fi
  ARGS+=(--core "$DEPTH_PUB_CORE")
  echo "[INFO] core bind ${DEPTH_PUB_CORE} (from $BASE_DIR/env.sh:DEPTH_PUB_CORE)"
fi

STOP_SCRIPT="${SCRIPT_DIR}/stop_depth_pub_general.sh"
if [[ ! -x "$STOP_SCRIPT" ]]; then
  echo "[ERROR] stop script not found or not executable: $STOP_SCRIPT" >&2
  exit 1
fi
"$STOP_SCRIPT"

echo "[INFO] Starting ${PROC_NAME} (namespace=${NAMESPACE})"
export RUST_LOG="${rust_log}"
"${PM2[@]}" start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  --cwd "$BASE_DIR" \
  -- \
  "${ARGS[@]}"

echo ""
echo "[INFO] Started: ${PROC_NAME}"
echo "Logs: ${PM2[*]} logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: ${PM2[*]} status --namespace ${NAMESPACE}"
