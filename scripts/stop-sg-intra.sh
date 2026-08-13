#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_HOST="${SG_INTRA_STOP_HOST:-ubuntu@47.131.162.78}"
SSH_KEY="${SG_INTRA_STOP_KEY:-$ROOT_DIR/aws-sg.pem}"
ENV_NAME=""
ALL_ENVS=0
CHECK_ONLY=0

usage() {
  cat <<'USAGE'
Usage: scripts/stop-sg-intra.sh (--env-name <name> | --all) [options]

Options:
  --host <ssh-host>    SSH target (default: ubuntu@47.131.162.78)
  --key <identity>     SSH private key (default: ./aws-sg.pem)
  --env-name <name>   Exactly bybit-intra-arb01 or bybit-intra-arb02
  --all                Stop arb01, then arb02
  --check-only        Validate target and show matching processes; change nothing
  -h, --help           Show this help

Live stop order:
  1. trade_engine, followed by an executable-level stopped check
  2. cancel and verify all Bybit linear and every spot order-filter order
  3. trade_signal
  4. pre_trade
  5. account_monitor
  6. config_server
  7. persist_manager
  8. viz_server

Cancellation runs from the selected environment directory as:
  python3 scripts/cancel_bybit_pm_orders.py --scope both \
    --spot-order-filters all --execute
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
    bybit-intra-arb01|bybit-intra-arb02) ;;
    *) echo "[ERROR] unsupported SG Intra environment: $ENV_NAME" >&2; exit 2 ;;
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
  for env_name in bybit-intra-arb01 bybit-intra-arb02; do
    child_args=(
      --host "$SSH_HOST"
      --key "$SSH_KEY"
      --env-name "$env_name"
    )
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
      child_args+=(--check-only)
    fi
    "$ROOT_DIR/scripts/stop-sg-intra.sh" "${child_args[@]}"
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
intra_scripts_dir="$target/intra_scripts"
cancel_script="$scripts_dir/cancel_bybit_pm_orders.py"

if [[ ! -d "$target" || "$(readlink -f -- "$target")" != "$target" ]]; then
  echo "[ERROR] invalid remote target: $target" >&2
  exit 1
fi
case "$(basename "$target")" in
  bybit-intra-arb01|bybit-intra-arb02) ;;
  *) echo "[ERROR] unsupported remote target: $target" >&2; exit 1 ;;
esac

required_files=(
  "$target/env.sh"
  "$scripts_dir/process_match_lib.sh"
  "$cancel_script"
  "$scripts_dir/stop_intra_config_server.sh"
  "$intra_scripts_dir/stop_intra_trade_engine.sh"
  "$intra_scripts_dir/stop_intra_trade_signal.sh"
  "$intra_scripts_dir/stop_intra_pre_trade.sh"
  "$intra_scripts_dir/stop_intra_monitors.sh"
  "$intra_scripts_dir/stop_intra_persist_manager.sh"
  "$intra_scripts_dir/stop_intra_viz_server.sh"
)
for required_file in "${required_files[@]}"; do
  if [[ ! -f "$required_file" ]]; then
    echo "[ERROR] required remote file not found: $required_file" >&2
    exit 1
  fi
done
for required_command in bash python3 pmdaemon npx ps readlink grep; do
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
  echo "[ERROR] $target/env.sh does not provide BYBIT_API_KEY and BYBIT_API_SECRET" >&2
  exit 1
fi

target_executables=(
  "$target/trade_signal"
  "$target/account_monitor_bybit"
  "$target/bybit_account_monitor"
  "$target/account_monitor"
  "$target/viz_server"
  "$target/pre_trade"
  "$target/trade_engine"
  "$target/persist_manager"
)
config_server="$scripts_dir/intra_config_server.py"

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
    [[ -n "$pid" ]] || continue
    if [[ "$args" == *"$config_server"* ]]; then
      echo "pid=$pid config_server=$args"
    fi
  done < <(ps -eo pid=,args=)
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
echo "[WARN] LIVE stop begins: env=$(basename "$target") exchange=bybit order_scope=linear_and_all_spot_filters"
run_step "stop trade_engine" bash "$intra_scripts_dir/stop_intra_trade_engine.sh"
if exact_executable_running "$target/trade_engine"; then
  echo "[ERROR] trade_engine is still running; refusing to cancel while it can place orders" >&2
  exit 1
fi
echo "[INFO] trade_engine confirmed stopped"

echo
echo "[STEP] cancel and verify all Bybit linear and spot open orders"
set +e
cancel_output="$(python3 "$cancel_script" \
  --scope both \
  --spot-order-filters all \
  --execute 2>&1)"
cancel_status=$?
set -e
if [[ -n "$cancel_output" ]]; then
  printf '%s\n' "$cancel_output"
fi
if [[ "$cancel_status" -ne 0 ]]; then
  echo "[ERROR] cancel script failed with status=$cancel_status; stopping sequence" >&2
  exit "$cancel_status"
fi
if grep -Eq '(\[WARN\]|WARN:|\[ERROR\])' <<<"$cancel_output"; then
  echo "[ERROR] cancel script reported a warning/error; stopping sequence" >&2
  exit 1
fi
if ! grep -Eq '(Verification passed: no residual active orders in scope\.|\[plan\] no open orders in scope\. Nothing to do\.)' <<<"$cancel_output"; then
  echo "[ERROR] cancel script did not confirm an empty order scope" >&2
  exit 1
fi
echo "[INFO] all Bybit linear and spot open orders confirmed empty"

run_step "stop trade_signal" bash "$intra_scripts_dir/stop_intra_trade_signal.sh"
run_step "stop pre_trade" bash "$intra_scripts_dir/stop_intra_pre_trade.sh"
run_step "stop account_monitor" bash "$intra_scripts_dir/stop_intra_monitors.sh"
run_step "stop config_server" bash "$scripts_dir/stop_intra_config_server.sh"
run_step "stop persist_manager" bash "$intra_scripts_dir/stop_intra_persist_manager.sh"
run_step "stop viz_server" bash "$intra_scripts_dir/stop_intra_viz_server.sh"

remaining="$(find_running_targets)"
if [[ -n "$remaining" ]]; then
  echo "[ERROR] matching processes remain after stop:" >&2
  printf '%s\n' "$remaining" >&2
  exit 1
fi
echo
echo "[INFO] stop complete: env=$(basename "$target") orders_empty=true processes_stopped=true"
REMOTE_STOP
