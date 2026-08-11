#!/usr/bin/env bash
set -euo pipefail

SSH_HOST="${FR_STOP_HOST:-jp-meta-elvpn}"
ENV_NAME="${FR_STOP_ENV:-}"
CHECK_ONLY=0

usage() {
  cat <<'USAGE'
Usage: scripts/stop-jp-meta-fr.sh --env-name <name> [options]

Options:
  --host <ssh-host>    SSH config host (default: jp-meta-elvpn)
  --env-name <name>   Binance/Gate FR environment (required)
  --check-only        Validate the target and show matching processes only
  -h, --help          Show this help

Live stop order:
  1. trade_engine
  2. cancel all PM futures and spot/unified open orders, then verify empty
  3. trade_signal
  4. pre_trade
  5. account_monitor
  6. config_server
  7. persist_manager
  8. viz_server
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      SSH_HOST="${2:-}"
      shift 2
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --check-only)
      CHECK_ONLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! "$ENV_NAME" =~ ^(binance|gate)_fr_[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "[ERROR] env-name must match binance_fr_<suffix> or gate_fr_<suffix>: $ENV_NAME" >&2
  exit 2
fi
EXCHANGE="${BASH_REMATCH[1]}"
if [[ -z "$SSH_HOST" || "$SSH_HOST" == -* ]]; then
  echo "[ERROR] invalid SSH host: $SSH_HOST" >&2
  exit 2
fi
if ! command -v ssh >/dev/null 2>&1; then
  echo "[ERROR] required command not found: ssh" >&2
  exit 1
fi

SSH=(ssh -o BatchMode=yes -o ConnectTimeout=15)
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

echo "[INFO] stop target host=$SSH_HOST exchange=$EXCHANGE env=$ENV_NAME dir=$REMOTE_DIR"
"${SSH[@]}" "$SSH_HOST" bash -s -- "$REMOTE_DIR" "$EXCHANGE" "$CHECK_ONLY" <<'REMOTE_STOP'
set -euo pipefail

target="$1"
exchange="$2"
check_only="$3"
scripts_dir="$target/scripts"
cancel_script="$scripts_dir/cancel_${exchange}_pm_orders.py"

if [[ ! -d "$target" || "$(readlink -f -- "$target")" != "$target" ]]; then
  echo "[ERROR] invalid remote target: $target" >&2
  exit 1
fi
if [[ "$(basename "$target")" != "${exchange}_fr_"* ]]; then
  echo "[ERROR] exchange/target mismatch: exchange=$exchange target=$target" >&2
  exit 1
fi

required_files=(
  "$target/env.sh"
  "$scripts_dir/process_match_lib.sh"
  "$scripts_dir/stop_trade_engine.sh"
  "$cancel_script"
  "$scripts_dir/stop_trade_signal.sh"
  "$scripts_dir/stop_fr_pre_trade.sh"
  "$scripts_dir/stop_account_monitor.sh"
  "$scripts_dir/stop_fr_config_server.sh"
  "$scripts_dir/stop_fr_persist_manager.sh"
  "$scripts_dir/stop_fr_viz_server.sh"
)
for required_file in "${required_files[@]}"; do
  if [[ ! -f "$required_file" ]]; then
    echo "[ERROR] required remote file not found: $required_file" >&2
    exit 1
  fi
done
for required_command in python3 pmdaemon npx ps readlink grep; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "[ERROR] required remote command not found: $required_command" >&2
    exit 1
  fi
done

case "$exchange" in
  binance)
    api_key_name="BINANCE_API_KEY"
    api_secret_name="BINANCE_API_SECRET"
    ;;
  gate)
    api_key_name="GATE_API_KEY"
    api_secret_name="GATE_API_SECRET"
    ;;
  *)
    echo "[ERROR] unsupported exchange: $exchange" >&2
    exit 1
    ;;
esac
if ! (
  set +u
  set -a
  # shellcheck disable=SC1090
  source "$target/env.sh" >/dev/null 2>&1
  set +a
  [[ -n "${!api_key_name:-}" && -n "${!api_secret_name:-}" ]]
); then
  echo "[ERROR] $target/env.sh does not provide $api_key_name and $api_secret_name" >&2
  exit 1
fi

target_executables=(
  "$target/trade_signal"
  "$target/account_monitor"
  "$target/binance_account_monitor"
  "$target/gate_account_monitor"
  "$target/viz_server"
  "$target/pre_trade"
  "$target/trade_engine"
  "$target/persist_manager"
)
config_server="$scripts_dir/fr_config_server.py"

find_running_targets() {
  local pid=""
  local exe=""
  local args=""
  local target_exe=""

  while read -r pid; do
    [[ -n "$pid" ]] || continue
    exe="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
    exe="${exe% (deleted)}"
    for target_exe in "${target_executables[@]}"; do
      if [[ "$exe" == "$target_exe" ]]; then
        args="$(ps -p "$pid" -o args= 2>/dev/null || true)"
        echo "pid=$pid exe=$exe args=$args"
        break
      fi
    done
  done < <(ps -eo pid=)

  while read -r pid args; do
    if [[ "$args" == *"$config_server"* ]]; then
      echo "pid=$pid config_server=$args"
    fi
  done < <(ps -eo pid=,args=)
  return 0
}

exact_executable_running() {
  local expected="$1"
  local pid=""
  local exe=""
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    exe="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
    exe="${exe% (deleted)}"
    if [[ "$exe" == "$expected" ]]; then
      return 0
    fi
  done < <(ps -eo pid=)
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
echo "[WARN] LIVE stop begins: env=$(basename "$target") exchange=$exchange order_scope=both"
run_step "stop trade_engine" bash "$scripts_dir/stop_trade_engine.sh" "$exchange"
if exact_executable_running "$target/trade_engine"; then
  echo "[ERROR] trade_engine is still running; refusing to cancel while it can place orders" >&2
  exit 1
fi
echo "[INFO] trade_engine confirmed stopped"

echo
echo "[STEP] cancel all PM futures and spot/unified open orders"
set +e
cancel_output="$(python3 "$cancel_script" --scope both --execute 2>&1)"
cancel_status=$?
set -e
if [[ -n "$cancel_output" ]]; then
  printf '%s\n' "$cancel_output"
fi
if [[ "$cancel_status" -ne 0 ]]; then
  echo "[ERROR] cancel script failed with status=$cancel_status; stopping sequence" >&2
  exit "$cancel_status"
fi
if grep -Eq '^(\[WARN\]|\[ERROR\]|WARN:)' <<<"$cancel_output"; then
  echo "[ERROR] cancel script reported an API warning/error; stopping sequence" >&2
  exit 1
fi

orders_empty=0
for attempt in 1 2 3; do
  echo "[INFO] verifying open orders are empty (attempt $attempt/3)"
  set +e
  verify_output="$(python3 "$cancel_script" --scope both 2>&1)"
  verify_status=$?
  set -e
  if [[ -n "$verify_output" ]]; then
    printf '%s\n' "$verify_output"
  fi
  if [[ "$verify_status" -ne 0 ]] || grep -Eq '^(\[WARN\]|\[ERROR\]|WARN:)' <<<"$verify_output"; then
    echo "[ERROR] unable to verify open orders after cancellation" >&2
    exit 1
  fi
  if grep -Fq '[plan] no open orders in scope. Nothing to do.' <<<"$verify_output"; then
    orders_empty=1
    break
  fi
  if [[ "$attempt" -lt 3 ]]; then
    sleep 1
  fi
done
if [[ "$orders_empty" -ne 1 ]]; then
  echo "[ERROR] open orders remain after cancellation; stopping sequence" >&2
  exit 1
fi
echo "[INFO] all PM futures and spot/unified open orders confirmed empty"

run_step "stop trade_signal" bash "$scripts_dir/stop_trade_signal.sh" "$exchange"
run_step "stop pre_trade" bash "$scripts_dir/stop_fr_pre_trade.sh"
run_step "stop account_monitor" bash "$scripts_dir/stop_account_monitor.sh"
run_step "stop config_server" bash "$scripts_dir/stop_fr_config_server.sh"
run_step "stop persist_manager" bash "$scripts_dir/stop_fr_persist_manager.sh" --exchange "$exchange"
run_step "stop viz_server" bash "$scripts_dir/stop_fr_viz_server.sh" --exchange "$exchange"

remaining="$(find_running_targets)"
if [[ -n "$remaining" ]]; then
  echo "[ERROR] matching processes remain after stop:" >&2
  printf '%s\n' "$remaining" >&2
  exit 1
fi
echo
echo "[INFO] stop complete: env=$(basename "$target") orders_empty=true processes_stopped=true"
REMOTE_STOP
