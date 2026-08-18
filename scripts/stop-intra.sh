#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/intra_orchestration_lib.sh
source "$ROOT_DIR/scripts/intra_orchestration_lib.sh"

SSH_KEY=""
ENV_NAME=""
ALL_ENVS=0
CHECK_ONLY=0

usage() {
  cat <<'USAGE'
Usage: scripts/stop-intra.sh (--env-name <name> | --all) [options]

Options:
  --key <identity>     Optional Bybit/SG SSH identity override
  --env-name <name>   One supported Intra environment
  --all                Stop every supported Intra environment
  --check-only        Validate target and show matching processes; change nothing
  -h, --help           Show this help

Supported environments:
  bybit-intra-arb01, bybit-intra-arb02 -> SG
  okex-intra-arb01, binance-intra-arb01 -> jp-meta-elvpn

Live stop order:
  1. trade_engine, followed by an executable-level stopped check
  2. cancel and verify every futures and spot/margin order for the exchange
  3. trade_signal
  4. pre_trade
  5. account_monitor
  6. config_server
  7. persist_manager
  8. viz_server

The exchange is inferred from env-name. Bybit targets SG; OKX and Binance
always target the jp-meta-elvpn SSH alias.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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
  intra_configure_env "$ENV_NAME"
fi
if [[ -n "$SSH_KEY" ]]; then
  if [[ ! -r "$SSH_KEY" ]]; then
    echo "[ERROR] SSH identity is not readable: $SSH_KEY" >&2
    exit 2
  fi
  SSH_KEY="$(readlink -f -- "$SSH_KEY")"
fi
if [[ -n "$ENV_NAME" ]]; then
  intra_validate_explicit_key "$SSH_KEY"
fi

if [[ "$ALL_ENVS" -eq 1 ]]; then
  for env_name in "${INTRA_ORCHESTRATION_ENVS[@]}"; do
    child_args=(--env-name "$env_name")
    if [[ -n "$SSH_KEY" && "$env_name" == bybit-* ]]; then
      child_args+=(--key "$SSH_KEY")
    fi
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
      child_args+=(--check-only)
    fi
    "$ROOT_DIR/scripts/stop-intra.sh" "${child_args[@]}"
  done
  exit 0
fi

if ! command -v ssh >/dev/null 2>&1; then
  echo "[ERROR] required command not found: ssh" >&2
  exit 1
fi

intra_configure_transport "$ROOT_DIR" "$SSH_KEY"
SSH=("${INTRA_SSH[@]}")
REMOTE_HOME="$("${SSH[@]}" "$INTRA_SSH_HOST" 'printf "%s\n" "$HOME"')"
if [[ "$REMOTE_HOME" != /* || "$REMOTE_HOME" == *$'\n'* ]]; then
  echo "[ERROR] invalid remote home returned by $INTRA_SSH_HOST: $REMOTE_HOME" >&2
  exit 1
fi
REMOTE_DIR="${REMOTE_HOME}/${ENV_NAME}"
REMOTE_REAL="$("${SSH[@]}" "$INTRA_SSH_HOST" "readlink -f -- '$REMOTE_DIR'")"
if [[ "$REMOTE_REAL" != "$REMOTE_DIR" ]]; then
  echo "[ERROR] remote target mismatch: expected=$REMOTE_DIR resolved=$REMOTE_REAL" >&2
  exit 1
fi

echo "[INFO] stop target host=$INTRA_SSH_HOST exchange=$INTRA_EXCHANGE env=$ENV_NAME dir=$REMOTE_DIR"
intra_remote_bash \
  "$REMOTE_DIR" "$ENV_NAME" "$INTRA_EXCHANGE" \
  "$INTRA_ACCOUNT_MONITOR_DEST" "$INTRA_ACCOUNT_MONITOR_BIN" \
  "$CHECK_ONLY" <<'REMOTE_STOP'
set -euo pipefail

target="$1"
env_name="$2"
exchange="$3"
account_monitor_dest="$4"
account_monitor_bin="$5"
check_only="$6"
scripts_dir="$target/scripts"
intra_scripts_dir="$target/intra_scripts"

if [[ ! -d "$target" || "$(readlink -f -- "$target")" != "$target" ]]; then
  echo "[ERROR] invalid remote target: $target" >&2
  exit 1
fi
if [[ "$(basename "$target")" != "$env_name" || "$env_name" != "${exchange}-intra-"* ]]; then
  echo "[ERROR] exchange/target mismatch: exchange=$exchange env=$env_name target=$target" >&2
  exit 1
fi

case "$exchange" in
  bybit)
    cancel_script="$scripts_dir/cancel_bybit_pm_orders.py"
    cancel_args=(--scope both --spot-order-filters all)
    cancel_scope="linear_and_all_spot_filters"
    ;;
  okex)
    cancel_script="$scripts_dir/cancel_okex_pm_orders.py"
    cancel_args=(--scope both)
    cancel_scope="swap_and_margin_spot"
    ;;
  binance)
    cancel_script="$scripts_dir/cancel_binance_std_orders.py"
    cancel_args=(--scope both)
    cancel_scope="standard_um_and_spot"
    ;;
  *)
    echo "[ERROR] unsupported exchange: $exchange" >&2
    exit 1
    ;;
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
if [[ "$exchange" == "binance" ]]; then
  required_files+=(
    "$scripts_dir/binance_cancel_all_std_spot_orders.py"
    "$scripts_dir/binance_cancel_all_std_um_ws_orders.py"
    "$scripts_dir/binance_local_ip.py"
  )
fi
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

(
  set +u
  set -a
  # shellcheck disable=SC1090
  source "$target/env.sh" >/dev/null 2>&1
  set +a
  case "$exchange" in
    bybit)
      [[ -n "${BYBIT_API_KEY:-}" && -n "${BYBIT_API_SECRET:-}" ]]
      ;;
    okex)
      [[ -n "${OKX_API_KEY:-}" && -n "${OKX_API_SECRET:-}" && -n "${OKX_PASSPHRASE:-}" ]]
      ;;
    binance)
      [[ -n "${BINANCE_API_KEY:-}" && -n "${BINANCE_API_SECRET:-}" ]]
      [[ "${BINANCE_ACCOUNT_MODE:-}" == "STANDARD" ]]
      ;;
  esac
) || {
  echo "[ERROR] $target/env.sh does not provide valid $exchange credentials/account mode" >&2
  exit 1
}

target_executables=(
  "$target/trade_signal"
  "$target/$account_monitor_dest"
  "$target/$account_monitor_bin"
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
  "$@" </dev/null
}

cd "$target"
echo "[WARN] LIVE stop begins: env=$(basename "$target") exchange=$exchange order_scope=$cancel_scope"
run_step "stop trade_engine" bash "$intra_scripts_dir/stop_intra_trade_engine.sh"
if exact_executable_running "$target/trade_engine"; then
  echo "[ERROR] trade_engine is still running; refusing to cancel while it can place orders" >&2
  exit 1
fi
echo "[INFO] trade_engine confirmed stopped"

echo
echo "[STEP] cancel all $exchange futures and spot/margin open orders"
set +e
cancel_output="$(python3 "$cancel_script" "${cancel_args[@]}" --execute </dev/null 2>&1)"
cancel_status=$?
set -e
if [[ -n "$cancel_output" ]]; then
  printf '%s\n' "$cancel_output"
fi
if [[ "$cancel_status" -ne 0 ]]; then
  echo "[ERROR] cancel script failed with status=$cancel_status; stopping sequence" >&2
  exit "$cancel_status"
fi
if grep -Eq '(\[WARN\]|WARN:|\[ERROR\]|ERROR:)' <<<"$cancel_output"; then
  echo "[ERROR] cancel script reported a warning/error; stopping sequence" >&2
  exit 1
fi

echo
echo "[STEP] verify $exchange futures and spot/margin order scopes are empty"
set +e
verify_output="$(python3 "$cancel_script" "${cancel_args[@]}" </dev/null 2>&1)"
verify_status=$?
set -e
if [[ -n "$verify_output" ]]; then
  printf '%s\n' "$verify_output"
fi
if [[ "$verify_status" -ne 0 ]]; then
  echo "[ERROR] post-cancel verification failed with status=$verify_status; stopping sequence" >&2
  exit "$verify_status"
fi
if grep -Eq '(\[WARN\]|WARN:|\[ERROR\]|ERROR:)' <<<"$verify_output"; then
  echo "[ERROR] post-cancel verification reported a warning/error; stopping sequence" >&2
  exit 1
fi
case "$exchange" in
  bybit|okex)
    if ! grep -Fq '[plan] no open orders in scope. Nothing to do.' <<<"$verify_output"; then
      echo "[ERROR] post-cancel verification found residual or unconfirmed orders" >&2
      exit 1
    fi
    ;;
  binance)
    if ! grep -Fq '[plan] symbols=0 open_orders=0 execute=False' <<<"$verify_output" || \
       ! grep -Fq '[plan] no open UM futures orders found' <<<"$verify_output"; then
      echo "[ERROR] post-cancel verification did not confirm empty Binance Spot and UM scopes" >&2
      exit 1
    fi
    ;;
esac
echo "[INFO] all $exchange futures and spot/margin open orders confirmed empty"

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
