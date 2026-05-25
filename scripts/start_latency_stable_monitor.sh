#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  start_latency_stable_monitor.sh [--cfg <path>] [--core <N>]

Behavior:
  - Run inside a latency_stable_monitor deploy directory, e.g. ~/latency_stable_monitor_sg
  - Default process name: latency_stable_monitor_<env>
  - Default config path: ./config/latency_stable_monitor.yaml
  - Managed by pmdaemon
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
  if [[ "$name" =~ ^latency_stable_monitor[_-]([a-z0-9][a-z0-9_-]*)$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

ENV_TAG=""
if ! ENV_TAG="$(infer_env_from_dir "$dir_lc")"; then
  echo "[ERROR] not a latency monitor env dir: ${dir_name} (expect latency_stable_monitor_<tag>, e.g. latency_stable_monitor_sg)" >&2
  exit 1
fi

CFG_PATH="${BASE_DIR}/config/latency_stable_monitor.yaml"
CORE_ARG=()

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
    --core)
      if [[ -z "${2:-}" ]]; then
        echo "[ERROR] --core requires a value" >&2
        exit 1
      fi
      CORE_ARG=("--core" "$2")
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
  echo "[ERROR] latency monitor config not found: $CFG_PATH" >&2
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
  "${BASE_DIR}/latency_stable_monitor"
  "${BASE_DIR}/target/release/latency_stable_monitor"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] latency_stable_monitor binary not found. Build/deploy first." >&2
  exit 1
fi

PROC_NAME="latency_stable_monitor_${ENV_TAG}"
RUST_LOG_VAL="${RUST_LOG:-info}"
IPC_NS_VAL="${IPC_NAMESPACE:-}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"

find_running_pids() {
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" && "$pid" != "$$" && "$pid" != "$PPID" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v cfg_arg="--cfg ${CFG_PATH}" -v base_dir="$BASE_DIR" '
      index($0, "latency_stable_monitor") > 0 &&
      index($0, cfg_arg) > 0 &&
      index($0, base_dir) > 0 &&
      index($0, "awk -v ") == 0 &&
      index($0, "start_latency_stable_monitor.sh") == 0 &&
      index($0, "stop_latency_stable_monitor.sh") == 0 {
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

args_json="[\"--cfg\", \"${json_cfg}\""
if [[ ${#CORE_ARG[@]} -gt 0 ]]; then
  args_json+=", \"--core\", \"$(json_escape "${CORE_ARG[1]}")\""
fi
args_json+="]"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": ${args_json},
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
  fi
fi

"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo "[INFO] Started: ${PROC_NAME}"
echo "Config: ${CFG_PATH}"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
