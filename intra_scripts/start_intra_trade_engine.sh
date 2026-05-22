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
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

# CPU core binding lookup (optional, per-host table at ~/.mkt_signal_cores.sh).
# 不存在或没匹配条目 → 不传 --core，binary 自然跳过绑核。
CORE_BIND_TABLE="${MKT_CORE_BIND_TABLE:-$HOME/.mkt_signal_cores.sh}"
if [[ -f "$CORE_BIND_TABLE" ]]; then
  # shellcheck disable=SC1090
  source "$CORE_BIND_TABLE"
fi
core_args=()
if declare -p MKT_CORE_BINDINGS >/dev/null 2>&1; then
  _bind_key="${dir_name}:trade_engine_${EXCHANGE}"
  if [[ -n "${MKT_CORE_BINDINGS[$_bind_key]:-}" ]]; then
    core_args+=(--core "${MKT_CORE_BINDINGS[$_bind_key]}")
    echo "[INFO] core bind ${MKT_CORE_BINDINGS[$_bind_key]} (main thread, table=$CORE_BIND_TABLE key=$_bind_key)"
  fi
  _bind_key_ipc="${dir_name}:trade_engine_${EXCHANGE}_ipc"
  if [[ -n "${MKT_CORE_BINDINGS[$_bind_key_ipc]:-}" ]]; then
    core_args+=(--ipc-core "${MKT_CORE_BINDINGS[$_bind_key_ipc]}")
    echo "[INFO] core bind ${MKT_CORE_BINDINGS[$_bind_key_ipc]} (te-ipc thread, key=$_bind_key_ipc)"
  fi
fi

find_running_pids() {
  local exchange_arg="--exchange ${EXCHANGE}"
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v base_dir="$BASE_DIR" -v exchange_arg="$exchange_arg" '
      index($0, base_dir "/") > 0 && index($0, "trade_engine") > 0 && index($0, exchange_arg) > 0 {
        print $1
      }
    '
  )
  if [[ ${#pids[@]} -gt 0 ]]; then
    printf '%s\n' "${pids[@]}"
  fi
}

cleanup_leaked() {
  mapfile -t leaked_pids < <(find_running_pids || true)
  if [[ ${#leaked_pids[@]} -eq 0 ]]; then
    return
  fi
  echo "[WARN] Found pre-existing process(es) before start: ${leaked_pids[*]}"
  echo "[INFO] Sending SIGTERM"
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true

  deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(find_running_pids || true)
    if [[ ${#leaked_pids[@]} -eq 0 ]]; then
      break
    fi
    sleep 1
  done

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[WARN] SIGTERM timeout, sending SIGKILL: ${leaked_pids[*]}"
    kill -9 "${leaked_pids[@]}" >/dev/null 2>&1 || true
    sleep 1
    mapfile -t leaked_pids < <(find_running_pids || true)
  fi

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[ERROR] Failed to kill pre-existing process(es): ${leaked_pids[*]}" >&2
    exit 1
  fi
}

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }
shell_quote() { printf '%q' "$1"; }

json_name="$(json_escape "$PROC_NAME")"
json_shell="$(json_escape "/bin/bash")"
json_base="$(json_escape "$BASE_DIR")"
json_rust_log="$(json_escape "$RUST_LOG")"
json_ipc_ns="$(json_escape "$IPC_NS")"
te_args=(--exchange "$EXCHANGE")
if [[ ${#core_args[@]} -gt 0 ]]; then
  te_args+=("${core_args[@]}")
fi
cmd="if [[ -f $(shell_quote "$ENV_FILE") ]]; then source $(shell_quote "$ENV_FILE"); fi; exec $(shell_quote "$BIN_PATH")"
for arg in "${te_args[@]}"; do
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
        "IPC_NAMESPACE": "${json_ipc_ns}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting $PROC_NAME (exchange=$EXCHANGE)"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
cleanup_leaked
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] Started: $PROC_NAME"
echo "[INFO] Logs: ${PMDAEMON[*]} logs $PROC_NAME --follow"
echo "[INFO] Status: ${PMDAEMON[*]} list"
