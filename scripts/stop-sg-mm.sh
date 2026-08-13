#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_HOST="${SG_MM_STOP_HOST:-ubuntu@47.131.162.78}"
SSH_KEY="${SG_MM_STOP_KEY:-$ROOT_DIR/aws-sg.pem}"
ENV_NAME=""
ALL_ENVS=0
CHECK_ONLY=0

usage() {
  cat <<'USAGE'
Usage: scripts/stop-sg-mm.sh (--env-name <name> | --all) [options]

Options:
  --host <ssh-host>    SSH target (default: ubuntu@47.131.162.78)
  --key <identity>     SSH private key (default: ./aws-sg.pem)
  --env-name <name>   Exactly bybit_mm_beta or bybit_mm_alpha
  --all                Stop beta, then alpha
  --check-only        Validate target and show matching processes; change nothing
  -h, --help           Show this help

Live stop order:
  1. trade_engine, followed by an executable-level stopped check
  2. cancel every Bybit linear order with --execute, then verify open count is 0
  3. trade_signal and optional manual_mm_signal
  4. pre_trade
  5. account_monitor
  6. config_server
  7. persist_manager
  8. viz_server

Cancellation runs from the selected environment directory as:
  bash scripts/close_mm_all_um_ws_orders.sh --env-name <env> \
    --env-dir <dir> --execute
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) SSH_HOST="${2:-}"; shift 2 ;;
    --key) SSH_KEY="${2:-}"; shift 2 ;;
    --env-name) ENV_NAME="${2:-}"; shift 2 ;;
    --all) ALL_ENVS=1; shift ;;
    --check-only) CHECK_ONLY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[ERROR] unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ "$ALL_ENVS" -eq 1 && -n "$ENV_NAME" ]]; then
  echo "[ERROR] use either --env-name or --all, not both" >&2
  exit 2
fi
if [[ "$ALL_ENVS" -eq 0 && -z "$ENV_NAME" ]]; then
  echo "[ERROR] one of --env-name or --all is required" >&2
  usage >&2
  exit 2
fi
if [[ -n "$ENV_NAME" ]]; then
  case "$ENV_NAME" in
    bybit_mm_beta|bybit_mm_alpha) ;;
    *) echo "[ERROR] unsupported SG MM environment: $ENV_NAME" >&2; exit 2 ;;
  esac
fi
if [[ -z "$SSH_HOST" || "$SSH_HOST" == -* ]]; then
  echo "[ERROR] invalid SSH host: $SSH_HOST" >&2
  exit 2
fi
if [[ ! -r "$SSH_KEY" ]]; then
  echo "[ERROR] SSH identity is not readable: $SSH_KEY" >&2
  exit 2
fi
SSH_KEY="$(readlink -f -- "$SSH_KEY")"

if [[ "$ALL_ENVS" -eq 1 ]]; then
  for env_name in bybit_mm_beta bybit_mm_alpha; do
    child_args=(
      --host "$SSH_HOST"
      --key "$SSH_KEY"
      --env-name "$env_name"
    )
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
      child_args+=(--check-only)
    fi
    "$ROOT_DIR/scripts/stop-sg-mm.sh" "${child_args[@]}"
  done
  exit 0
fi

if ! command -v ssh >/dev/null 2>&1; then
  echo "[ERROR] required command not found: ssh" >&2
  exit 1
fi

SSH=(ssh -i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=15)
REMOTE_HOME="$("${SSH[@]}" "$SSH_HOST" 'printf "%s\n" "$HOME"')"
if [[ "$REMOTE_HOME" != /* || "$REMOTE_HOME" == *$'\n'* ]]; then
  echo "[ERROR] invalid remote home returned by $SSH_HOST: $REMOTE_HOME" >&2
  exit 1
fi
REMOTE_DIR="${REMOTE_HOME}/${ENV_NAME}"
REMOTE_REAL="$("${SSH[@]}" "$SSH_HOST" "readlink -f -- '$REMOTE_DIR'")"
if [[ "$REMOTE_REAL" != "$REMOTE_DIR" ]]; then
  echo "[ERROR] remote target mismatch: expected=$REMOTE_DIR resolved=$REMOTE_REAL" >&2
  exit 1
fi

echo "[INFO] stop target host=$SSH_HOST exchange=bybit env=$ENV_NAME dir=$REMOTE_DIR"
"${SSH[@]}" "$SSH_HOST" bash -s -- "$REMOTE_DIR" "$CHECK_ONLY" <<'REMOTE_STOP'
set -euo pipefail

target="$1"
check_only="$2"
scripts_dir="$target/scripts"
mm_scripts_dir="$target/mm_scripts"
cancel_script="$scripts_dir/close_mm_all_um_ws_orders.sh"

if [[ ! -d "$target" || "$(readlink -f -- "$target")" != "$target" ]]; then
  echo "[ERROR] invalid remote target: $target" >&2
  exit 1
fi
case "$(basename "$target")" in
  bybit_mm_beta|bybit_mm_alpha) ;;
  *) echo "[ERROR] unsupported remote target: $target" >&2; exit 1 ;;
esac

required_files=(
  "$target/env.sh"
  "$target/trade_engine.toml"
  "$scripts_dir/mm_process_name.sh"
  "$scripts_dir/process_match_lib.sh"
  "$scripts_dir/stop_trade_signal.sh"
  "$scripts_dir/stop_account_monitor.sh"
  "$scripts_dir/stop_mm_config_server.sh"
  "$scripts_dir/bybit_cancel_all_um_orders.py"
  "$scripts_dir/binance_local_ip.py"
  "$mm_scripts_dir/stop_mm_trade_engine.sh"
  "$mm_scripts_dir/stop_mm_pre_trade.sh"
  "$mm_scripts_dir/stop_mm_persist_manager.sh"
  "$mm_scripts_dir/stop_mm_viz_server.sh"
  "$cancel_script"
)
for required_file in "${required_files[@]}"; do
  if [[ ! -f "$required_file" ]]; then
    echo "[ERROR] required remote file not found: $required_file" >&2
    exit 1
  fi
done
for required_command in bash python3 pmdaemon npx ps readlink grep sleep; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "[ERROR] required remote command not found: $required_command" >&2
    exit 1
  fi
done

if ! (
  set +u
  set -a
  # shellcheck disable=SC1090
  source "$target/env.sh" >/dev/null 2>&1
  set +a
  [[ -n "${BYBIT_API_KEY:-}" && -n "${BYBIT_API_SECRET:-}" ]]
); then
  echo "[ERROR] env.sh must provide BYBIT_API_KEY and BYBIT_API_SECRET" >&2
  exit 1
fi

target_executables=(
  "$target/trade_signal"
  "$target/manual_mm_signal"
  "$target/account_monitor"
  "$target/bybit_account_monitor"
  "$target/account_monitor_bybit"
  "$target/viz_server"
  "$target/pre_trade"
  "$target/trade_engine"
  "$target/persist_manager"
)
config_server="$scripts_dir/mm_config_server.py"

find_running_targets() {
  local pid=""
  local comm=""
  local exe=""
  local args=""
  local target_exe=""

  while read -r pid comm; do
    [[ -n "$pid" ]] || continue
    case "$comm" in
      trade_signal|manual_mm_signa|account_monitor|*_account*|viz_server|pre_trade|trade_engine|persist_manager) ;;
      *) continue ;;
    esac
    exe="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
    exe="${exe% (deleted)}"
    for target_exe in "${target_executables[@]}"; do
      if [[ "$exe" == "$target_exe" ]]; then
        args="$(ps -p "$pid" -o args= 2>/dev/null || true)"
        echo "pid=$pid exe=$exe args=$args"
        break
      fi
    done
  done < <(ps -eo pid=,comm=)

  while read -r pid args; do
    [[ -n "$pid" ]] || continue
    if [[ "$args" == *"$config_server"* ]]; then
      echo "pid=$pid config_server=$args"
    fi
  done < <(ps -eo pid=,args=)
  return 0
}

exact_executable_running() {
  local expected="$1"
  local expected_comm="${expected##*/}"
  local pid=""
  local comm=""
  local exe=""
  expected_comm="${expected_comm:0:15}"
  while read -r pid comm; do
    [[ -n "$pid" ]] || continue
    [[ "$comm" == "$expected_comm" ]] || continue
    exe="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
    exe="${exe% (deleted)}"
    if [[ "$exe" == "$expected" ]]; then
      return 0
    fi
  done < <(ps -eo pid=,comm=)
  return 1
}

echo "[INFO] remote preflight passed"
running_before="$(find_running_targets)"
if [[ -n "$running_before" ]]; then
  echo "[INFO] matching processes before stop:"
  printf '%s\n' "$running_before"
else
  echo "[INFO] no matching processes are currently running"
fi
if [[ "$check_only" == "1" ]]; then
  echo "[INFO] check-only complete; no process or order changes were made"
  exit 0
fi

run_step() {
  local description="$1"
  shift
  echo
  echo "[STEP] $description"
  "$@"
}

cd "$target"
env_basename="$(basename "$target")"
echo "[WARN] LIVE stop begins: env=$env_basename exchange=bybit order_scope=all_linear"
run_step "stop trade_engine" bash "$mm_scripts_dir/stop_mm_trade_engine.sh"
if exact_executable_running "$target/trade_engine"; then
  echo "[ERROR] trade_engine is still running; refusing to cancel while it can place orders" >&2
  exit 1
fi
echo "[INFO] trade_engine confirmed stopped"

cancel_args=(
  --env-name "$env_basename"
  --env-dir "$target"
  --require-local-address
)
echo
echo "[STEP] cancel all Bybit linear open orders"
set +e
cancel_output="$(bash "$cancel_script" "${cancel_args[@]}" --execute 2>&1)"
cancel_status=$?
set -e
if [[ -n "$cancel_output" ]]; then
  printf '%s\n' "$cancel_output"
fi
if [[ "$cancel_status" -ne 0 ]]; then
  echo "[ERROR] cancel script failed with status=$cancel_status; stopping sequence" >&2
  exit "$cancel_status"
fi
if grep -Eiq '(^|[[:space:]])(WARN:|WARNING:|ERROR:|\[WARN\]|\[WARNING\]|\[ERROR\])' <<<"$cancel_output"; then
  echo "[ERROR] cancel script reported a warning/error; stopping sequence" >&2
  exit 1
fi

orders_empty=0
for attempt in 1 2 3; do
  echo "[INFO] verifying Bybit open linear orders are empty (attempt $attempt/3)"
  set +e
  verify_output="$(bash "$cancel_script" "${cancel_args[@]}" 2>&1)"
  verify_status=$?
  set -e
  if [[ -n "$verify_output" ]]; then
    printf '%s\n' "$verify_output"
  fi
  if [[ "$verify_status" -ne 0 ]] || \
     grep -Eiq '(^|[[:space:]])(WARN:|WARNING:|ERROR:|\[WARN\]|\[WARNING\]|\[ERROR\])' <<<"$verify_output"; then
    echo "[ERROR] unable to verify Bybit open orders after cancellation" >&2
    exit 1
  fi
  if grep -Fxq '[bybit] open linear orders: 0' <<<"$verify_output"; then
    orders_empty=1
    break
  fi
  if [[ "$attempt" -lt 3 ]]; then
    sleep 1
  fi
done
if [[ "$orders_empty" -ne 1 ]]; then
  echo "[ERROR] Bybit open linear orders remain after cancellation; stopping sequence" >&2
  exit 1
fi
echo "[INFO] all Bybit linear open orders confirmed empty"

run_step "stop trade_signal" bash "$scripts_dir/stop_trade_signal.sh" bybit
if [[ -x "$mm_scripts_dir/stop_manual_mm_signal.sh" || -f "$mm_scripts_dir/stop_manual_mm_signal.sh" ]]; then
  run_step "stop manual_mm_signal" bash "$mm_scripts_dir/stop_manual_mm_signal.sh"
fi
run_step "stop pre_trade" bash "$mm_scripts_dir/stop_mm_pre_trade.sh"
run_step "stop account_monitor" bash "$scripts_dir/stop_account_monitor.sh"
run_step "stop config_server" bash "$scripts_dir/stop_mm_config_server.sh"
run_step "stop persist_manager" bash "$mm_scripts_dir/stop_mm_persist_manager.sh"
run_step "stop viz_server" bash "$mm_scripts_dir/stop_mm_viz_server.sh" --exchange bybit

remaining="$(find_running_targets)"
if [[ -n "$remaining" ]]; then
  echo "[ERROR] matching processes remain after stop:" >&2
  printf '%s\n' "$remaining" >&2
  exit 1
fi
echo
echo "[INFO] stop complete: env=$env_basename orders_empty=true processes_stopped=true"
REMOTE_STOP
