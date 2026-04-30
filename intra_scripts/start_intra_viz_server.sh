#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

BIN_CANDIDATES=(
  "${BASE_DIR}/viz_server"
  "${SCRIPT_DIR}/viz_server"
  "${BASE_DIR}/target/release/viz_server"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then BIN_PATH="$cand"; break; fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] viz_server binary not found. Deploy/build first."; exit 1
fi

usage() {
  cat <<'EOF'
用法: intra_scripts/start_intra_viz_server.sh [--cfg config/viz.toml]

说明:
  - 同所期现：按目录名 <exchange>-intra-<tag> 推断
  - 进程名: intra_viz_<exchange>_<env>
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage; exit 0
fi

CFG_PATH=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cfg) CFG_PATH="${2:-}"; shift 2 ;;
    *) echo "[ERROR] 未知参数: $1"; usage; exit 1 ;;
  esac
done

if [[ -z "$CFG_PATH" ]]; then
  CFG_PATH="${VIZ_CFG:-config/viz.toml}"
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"
EXCHANGE=""
ENV_TAG="intra"
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
  ENV_TAG="${BASH_REMATCH[2]//-/_}"
elif [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
fi
if [[ "$EXCHANGE" == "okx" ]]; then EXCHANGE="okex"; fi
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 无法从目录名推断 exchange (dir=$dir_name)"
  exit 1
fi

PROC_NAME="intra_viz_${EXCHANGE}_${ENV_TAG}"
RUST_LOG="${RUST_LOG:-info}"

if [[ ! -f "$BASE_DIR/$CFG_PATH" ]]; then
  echo "[ERROR] viz config not found: $BASE_DIR/$CFG_PATH"
  echo "[ERROR] 建议先部署：scripts/deploy_intra_viz_server.sh --exchange $EXCHANGE"
  exit 1
fi

if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2; exit 1
fi

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }
shell_quote() { printf '%q' "$1"; }

json_name="$(json_escape "$PROC_NAME")"
json_shell="$(json_escape "/bin/bash")"
json_base="$(json_escape "$BASE_DIR")"
json_cfg="$(json_escape "$CFG_PATH")"
json_rust_log="$(json_escape "$RUST_LOG")"
json_ipc_ns="$(json_escape "${IPC_NAMESPACE:-}")"
cmd="if [[ -f $(shell_quote "$ENV_FILE") ]]; then source $(shell_quote "$ENV_FILE"); fi; exec $(shell_quote "$BIN_PATH")"
json_cmd="$(json_escape "$cmd")"

cat >"$cfg_file" <<CFG
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_shell}",
      "args": ["-lc", "${json_cmd}"],
      "cwd": "${json_base}",
      "env": {
        "VIZ_CFG": "${json_cfg}",
        "RUST_LOG": "${json_rust_log}",
        "IPC_NAMESPACE": "${json_ipc_ns}"
      }
    }
  ]
}
CFG

echo "[INFO] Restarting ${PROC_NAME} (cfg=${CFG_PATH})"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] Started viz_server (${PROC_NAME})"
echo "[INFO] Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
