#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

ensure_pmdaemon() {
  if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
    echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
    echo "[HINT] install with: cargo install pmdaemon" >&2
    exit 1
  fi
}

BIN_CANDIDATES=(
  "${BASE_DIR}/persist_manager"
  "${SCRIPT_DIR}/persist_manager"
  "${BASE_DIR}/target/release/persist_manager"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] persist_manager binary not found. Build/deploy first."
  exit 1
fi

usage() {
  cat <<'USAGE'
用法: xarb_scripts/start_xarb_persist_manager.sh

说明:
  - 需要在部署目录存在 env.sh（包含 IPC_NAMESPACE），否则 persist_manager 会 panic。
  - 会基于部署目录名推断 open/hedge exchange（目录名需形如 <open>-<hedge>-xarb-...）。
  - 会以 pmdaemon 启动 1 个进程：persist_manager_xarb_<open>_<hedge>
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -gt 0 ]]; then
  echo "[ERROR] 不支持参数：$*"
  usage
  exit 1
fi

ensure_pmdaemon

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[ERROR] 未找到 env.sh：${ENV_FILE}"
  echo "[ERROR] 请先生成并配置：scripts/deploy_setup_env_xarb.sh --env-name $(basename "${BASE_DIR}") --open-venue <...> --hedge-venue <...>"
  exit 1
fi

if [[ -z "${IPC_NAMESPACE:-}" ]]; then
  echo "[ERROR] IPC_NAMESPACE 未设置（请 source env.sh）"
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

OPEN_EXCHANGE=""
HEDGE_EXCHANGE=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$ ]]; then
  OPEN_EXCHANGE="${BASH_REMATCH[1]}"
  HEDGE_EXCHANGE="${BASH_REMATCH[2]}"
fi

if [[ "$OPEN_EXCHANGE" == "okx" ]]; then
  OPEN_EXCHANGE="okex"
fi
if [[ "$HEDGE_EXCHANGE" == "okx" ]]; then
  HEDGE_EXCHANGE="okex"
fi

if [[ -z "$OPEN_EXCHANGE" || -z "$HEDGE_EXCHANGE" ]]; then
  echo "[ERROR] 无法从目录名推断 open/hedge (dir=$dir_name)，期望 <open>-<hedge>-xarb-..."
  exit 1
fi

PROC_NAME="${PMDAEMON_NAME:-persist_manager_xarb_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}}"
RUST_LOG="${RUST_LOG:-info}"

mkdir -p "${BASE_DIR}/data/persist_manager" >/dev/null 2>&1 || true

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_name="$(json_escape "$PROC_NAME")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_rust_log="$(json_escape "$RUST_LOG")"
json_ipc_ns="$(json_escape "$IPC_NAMESPACE")"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": [],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}",
        "IPC_NAMESPACE": "${json_ipc_ns}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting ${PROC_NAME}"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo ""
echo "[INFO] Started persist_manager"
echo "Process: ${PROC_NAME}"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
