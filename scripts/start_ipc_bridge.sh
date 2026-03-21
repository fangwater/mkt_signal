#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  start_ipc_bridge.sh [--cfg <path>]

Behavior:
  - Run inside a bridge deploy directory, e.g. ~/bridge_jp
  - Default process name: bridge_<env>
  - Default config path: ./config/ipc_bridge.yaml
  - Managed by pmdaemon, with leaked-process cleanup aligned to current scripts
USAGE
}

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="$(echo "${dir_name}" | tr 'A-Z' 'a-z')"

infer_env_from_dir() {
  local name="${1,,}"
  if [[ "$name" =~ ^bridge[_-]([a-z0-9][a-z0-9_-]*)$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

ENV_TAG=""
if ! ENV_TAG="$(infer_env_from_dir "$dir_lc")"; then
  echo "[ERROR] not a bridge env dir: ${dir_name} (expect bridge_jp / bridge_hk)" >&2
  exit 1
fi

CFG_PATH="${BASE_DIR}/config/ipc_bridge.yaml"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cfg)
      CFG_PATH="${2:-}"
      if [[ -z "$CFG_PATH" ]]; then
        echo "[ERROR] --cfg requires a value" >&2
        exit 1
      fi
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

if [[ "$CFG_PATH" != /* ]]; then
  CFG_PATH="${BASE_DIR}/${CFG_PATH#./}"
fi

if [[ ! -f "$CFG_PATH" ]]; then
  echo "[ERROR] bridge config not found: $CFG_PATH" >&2
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
  "${BASE_DIR}/ipc_bridge"
  "${BASE_DIR}/target/release/ipc_bridge"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] ipc_bridge binary not found. Build/deploy first." >&2
  exit 1
fi

PROC_NAME="bridge_${ENV_TAG}"
LEGACY_PROC_NAME="ipc_bridge_${dir_lc}"
RUST_LOG_VAL="${RUST_LOG:-info}"
IPC_NS_VAL="${IPC_NAMESPACE:-}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

find_running_pids() {
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v cfg_arg="--cfg ${CFG_PATH}" -v base_dir="$BASE_DIR" '
      index($0, "ipc_bridge") > 0 &&
      index($0, cfg_arg) > 0 &&
      index($0, base_dir) > 0 &&
      index($0, "start_ipc_bridge.sh") == 0 &&
      index($0, "stop_ipc_bridge.sh") == 0 {
        print $1
      }
    '
  )

  if [[ ${#pids[@]} -gt 0 ]]; then
    printf '%s\n' "${pids[@]}"
  fi
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

cfg_file="$(mktemp)"
cleanup() { rm -f "$cfg_file" >/dev/null 2>&1 || true; }
trap cleanup EXIT

json_name="$(json_escape "$PROC_NAME")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_cfg="$(json_escape "$CFG_PATH")"
json_rust_log="$(json_escape "$RUST_LOG_VAL")"
json_ipc_ns="$(json_escape "$IPC_NS_VAL")"

if [[ -z "$IPC_NS_VAL" ]]; then
  # Global services `dat_pbs/*` and `bridge/*` do not require IPC_NAMESPACE.
  # Business channels like `order_reqs/*` and `order_resps/*` are namespaced and will panic
  # inside ipc_bridge if IPC_NAMESPACE is not set.
  if rg -q "endpoint:\\s*\"(order_reqs/|order_resps/|signal_pubs/|persist_pubs/|account_pubs/|viz_pubs/)\"" "$CFG_PATH" 2>/dev/null; then
    echo "[ERROR] IPC_NAMESPACE is required by this bridge config, but is not set." >&2
    echo "[HINT] place an env file at: $ENV_FILE (e.g. copy xarb env.sh) and re-run." >&2
    exit 1
  fi
fi

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": ["--cfg", "${json_cfg}"],
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
"${PMDAEMON[@]}" delete "$LEGACY_PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true

mapfile -t leaked_pids < <(find_running_pids || true)
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  echo "[WARN] Found leaked process before start: ${leaked_pids[*]}"
  echo "[INFO] Sending SIGTERM to leaked process(es)"
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
    echo "[ERROR] Failed to kill leaked process(es): ${leaked_pids[*]}" >&2
    exit 1
  fi

  echo "[INFO] Leaked process cleanup done"
fi

"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] Started: ${PROC_NAME}"
echo "Config: ${CFG_PATH}"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
