#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

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
  "${BASE_DIR}/trade_engine"
  "${SCRIPT_DIR}/trade_engine"
  "${BASE_DIR}/target/release/trade_engine"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] trade_engine binary not found. Build/deploy first."
  exit 1
fi

usage() {
  cat <<'USAGE'
用法: intra_scripts/start_intra_trade_engine.sh

说明:
  - 同所期现：从目录名 <exchange>-intra-<tag> 推断 exchange / env_tag
  - 启动 1 个 pmdaemon 进程：intra_te_<exchange>_<env> -> trade_engine --exchange <exchange>
  - 若存在 env.sh，会自动 source（用于 API credentials 等）
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -gt 0 ]]; then
  echo "[ERROR] 不支持的参数: $*"; usage; exit 1
fi

ensure_pmdaemon

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
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi

case "$EXCHANGE" in
  binance|okex|bybit|bitget|gate) ;;
  *)
    echo "[ERROR] 无法从目录名推断 exchange (dir=$dir_name)，期望 <exchange>-intra-<tag>"
    exit 1
    ;;
esac

RUST_LOG="${RUST_LOG:-info}"
IPC_NS="${IPC_NAMESPACE:-}"
PROC_NAME="intra_te_${EXCHANGE}_${ENV_TAG}"

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }
shell_quote() { printf '%q' "$1"; }

json_name="$(json_escape "$PROC_NAME")"
json_shell="$(json_escape "/bin/bash")"
json_base="$(json_escape "$BASE_DIR")"
json_rust_log="$(json_escape "$RUST_LOG")"
json_ipc_ns="$(json_escape "$IPC_NS")"
cmd="if [[ -f $(shell_quote "$ENV_FILE") ]]; then source $(shell_quote "$ENV_FILE"); fi; exec $(shell_quote "$BIN_PATH") --exchange $(shell_quote "$EXCHANGE")"
json_cmd="$(json_escape "$cmd")"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_shell}",
      "args": ["-lc", "${json_cmd}"],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}",
        "IPC_NAMESPACE": "${json_ipc_ns}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting $PROC_NAME (exchange=$EXCHANGE)"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] Started: $PROC_NAME"
echo "[INFO] Logs: ${PMDAEMON[*]} logs $PROC_NAME --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
