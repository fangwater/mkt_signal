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
用法: xarb_scripts/start_xarb_trade_engine.sh

说明:
  - 会基于部署目录名推断 open/hedge exchange（目录名需形如 <open>-<hedge>-xarb-...）
  - 将以 pmdaemon 启动两个进程：
      xarb_te_<open>_<hedge>_open   -> trade_engine --exchange <open>
      xarb_te_<open>_<hedge>_hedge  -> trade_engine --exchange <hedge>
  - 若存在 env.sh，会自动 source（用于 API credentials 等）
  - trade_engine 的本地 IP 从 /home/<user>/config/mkt_cfg.yaml 读取
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -gt 0 ]]; then
  echo "[ERROR] 不支持的参数: $*"
  usage
  exit 1
fi

ensure_pmdaemon

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[WARN] 未找到 env.sh：${ENV_FILE}"
  echo "[WARN] 若需要凭证，请先生成并配置：scripts/deploy_setup_env_xarb.sh --env-name $(basename "${BASE_DIR}") --open-venue <...> --hedge-venue <...>"
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

case "$OPEN_EXCHANGE" in
  binance|okex|bybit|bitget|gate) ;;
  *)
    echo "[ERROR] 无法从目录名推断 open exchange (dir=$dir_name)，期望 <open>-<hedge>-xarb-..."
    exit 1
    ;;
esac
case "$HEDGE_EXCHANGE" in
  binance|okex|bybit|bitget|gate) ;;
  *)
    echo "[ERROR] 无法从目录名推断 hedge exchange (dir=$dir_name)，期望 <open>-<hedge>-xarb-..."
    exit 1
    ;;
esac
if [[ "$OPEN_EXCHANGE" == "$HEDGE_EXCHANGE" ]]; then
  echo "[ERROR] xarb 需要跨所：open=$OPEN_EXCHANGE hedge=$HEDGE_EXCHANGE"
  exit 1
fi

RUST_LOG="${RUST_LOG:-info}"
IPC_NS="${IPC_NAMESPACE:-}"

TMP_CFGS=()
cleanup_tmp_cfgs() {
  if [[ ${#TMP_CFGS[@]} -gt 0 ]]; then
    rm -f "${TMP_CFGS[@]}" >/dev/null 2>&1 || true
  fi
}
trap cleanup_tmp_cfgs EXIT

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

start_one() {
  local side="$1"
  local exchange="$2"
  local proc_name="xarb_te_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_${side}"

  local cfg_file
  cfg_file="$(mktemp)"
  TMP_CFGS+=("$cfg_file")

  local json_name json_bin json_base json_exchange json_rust_log json_ipc_ns
  json_name="$(json_escape "$proc_name")"
  json_bin="$(json_escape "$BIN_PATH")"
  json_base="$(json_escape "$BASE_DIR")"
  json_exchange="$(json_escape "$exchange")"
  json_rust_log="$(json_escape "$RUST_LOG")"
  json_ipc_ns="$(json_escape "$IPC_NS")"

  cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": ["--exchange", "${json_exchange}"],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}",
        "IPC_NAMESPACE": "${json_ipc_ns}"
      }
    }
  ]
}
JSON

  echo "[INFO] Restarting $proc_name (exchange=$exchange)"
  "${PMDAEMON[@]}" delete "$proc_name" >/dev/null 2>&1 || true
  "${PMDAEMON[@]}" --config "$cfg_file" start --name "$proc_name"
}

start_one "open" "$OPEN_EXCHANGE"
sleep 0.5
start_one "hedge" "$HEDGE_EXCHANGE"

echo "[INFO] Started:"
echo "  - xarb_te_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_open"
echo "  - xarb_te_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_hedge"
echo "[INFO] Logs: ${PMDAEMON[*]} logs xarb_te_${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_open --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
