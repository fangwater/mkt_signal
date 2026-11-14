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
  "${SCRIPT_DIR}/../target/release/persist_manager"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] persist_manager binary not found. Build first with: cargo build --release --bin persist_manager" >&2
  exit 1
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
else
  echo "[ERROR] env.sh not found: ${ENV_FILE}" >&2
  echo "[ERROR] Run: scripts/deploy_setup_env.sh [trade|test] --exchange <...> (and source env.sh)" >&2
  exit 1
fi

usage() {
  cat <<'USAGE'
Usage:
  scripts/start_fr_persist_manager.sh [--exchange <binance|okex|bybit|bitget|gate>]

Notes:
  - 可选：env.sh 设置 PERSIST_MANAGER_CORE=<N> 绑定 persist_manager 单线程 runtime。
USAGE
}

EXCHANGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] Unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"
if [[ -z "$EXCHANGE" ]]; then
  case "$dir_name" in
    okex_fr_*|*okex*|*OKEX*) EXCHANGE="okex" ;;
    binance_fr_*|*binance*|*BINANCE*) EXCHANGE="binance" ;;
    bybit_fr_*|*bybit*|*BYBIT*) EXCHANGE="bybit" ;;
    bitget_fr_*|*bitget*|*BITGET*) EXCHANGE="bitget" ;;
    gate_fr_*|*gate*|*GATE*) EXCHANGE="gate" ;;
    *)
      echo "[ERROR] Failed to infer exchange from dir: ${dir_name}" >&2
      exit 1
      ;;
  esac
fi

EXCHANGE="$(echo "$EXCHANGE" | tr 'A-Z' 'a-z')"
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi

if [[ -z "${IPC_NAMESPACE:-}" ]]; then
  echo "[ERROR] IPC_NAMESPACE is not set; run: source ${ENV_FILE}" >&2
  exit 1
fi

ensure_pmdaemon

short_exchange() {
  case "${1,,}" in
    binance) echo "bn" ;;
    okex) echo "ok" ;;
    bybit) echo "bb" ;;
    bitget) echo "bg" ;;
    gate) echo "gt" ;;
    *)
      echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2
      ;;
  esac
}

env_tag="fr"
if [[ "$dir_lc" =~ ^[a-z0-9]+[-_]fr[-_](.+)$ ]]; then
  env_tag="$(echo "${BASH_REMATCH[1]}" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
fi
if [[ -z "$env_tag" ]]; then
  env_tag="fr"
fi

PROC_NAME="${PMDAEMON_NAME:-fr_pm_$(short_exchange "$EXCHANGE")_${env_tag}}"
LEGACY_PROC_NAME="persist_manager_${dir_tag}"
RUST_LOG="${RUST_LOG:-info}"

# Leave the RocksDB path absent on first start so RocksDbStore can initialize
# its column families instead of trying to open an empty directory.
mkdir -p "${BASE_DIR}/data" >/dev/null 2>&1 || true

core_args=()
if [[ -n "${PERSIST_MANAGER_CORE:-}" ]]; then
  if [[ ! "$PERSIST_MANAGER_CORE" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] PERSIST_MANAGER_CORE 必须为单个整数 (got: $PERSIST_MANAGER_CORE)" >&2
    exit 1
  fi
  core_args=(--core "$PERSIST_MANAGER_CORE")
  echo "[INFO] core bind ${PERSIST_MANAGER_CORE} (from $ENV_FILE:PERSIST_MANAGER_CORE)"
fi

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

shell_quote() {
  printf '%q' "$1"
}

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_name="$(json_escape "$PROC_NAME")"
json_shell="$(json_escape "/bin/bash")"
json_base="$(json_escape "$BASE_DIR")"
json_rust_log="$(json_escape "$RUST_LOG")"
json_ipc_ns="$(json_escape "$IPC_NAMESPACE")"
json_persist_sync_source_id="$(json_escape "${PERSIST_SYNC_SOURCE_ID:-}")"
json_persist_sync_bind="$(json_escape "${PERSIST_SYNC_BIND:-}")"
cmd="if [[ -f $(shell_quote "$ENV_FILE") ]]; then source $(shell_quote "$ENV_FILE"); fi; exec $(shell_quote "$BIN_PATH")"
for arg in "${core_args[@]}"; do
  cmd+=" $(shell_quote "$arg")"
done
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
        "IPC_NAMESPACE": "${json_ipc_ns}",
        "PERSIST_SYNC_SOURCE_ID": "${json_persist_sync_source_id}",
        "PERSIST_SYNC_BIND": "${json_persist_sync_bind}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting ${PROC_NAME}"
STOP_SCRIPT="${SCRIPT_DIR}/stop_fr_persist_manager.sh"
if [[ ! -x "$STOP_SCRIPT" ]]; then
  echo "[ERROR] stop script not found or not executable: $STOP_SCRIPT" >&2
  exit 1
fi
"$STOP_SCRIPT" --exchange "$EXCHANGE"
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo ""
echo "[INFO] Started persist_manager"
echo "Process: ${PROC_NAME}"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
