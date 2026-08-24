#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib/ssh_remote_bash.sh
source "$SCRIPT_DIR/lib/ssh_remote_bash.sh"

SSH_HOST="${EXEC_START_HOST:-cta_exec}"
ENV_NAME="${EXEC_START_ENV:-}"
VENUE="${EXEC_START_VENUE:-}"
CHECK_ONLY=0
STARTUP_WAIT_SECONDS="${EXEC_START_WAIT_SECONDS:-15}"
STARTUP_SETTLE_SECONDS="${EXEC_START_SETTLE_SECONDS:-3}"
LOG_LINES="${EXEC_START_LOG_LINES:-80}"

usage() {
  cat <<'USAGE'
Usage: scripts/start-exec.sh --env-name <nameNN> [options]

Options:
  --host <ssh-host>     SSH config host (default: cta_exec)
  --env-name <nameNN>   Exec environment (required), e.g. binance_exec_trade01
  --venue <name>        Venue (binance-futures, binance-coin-futures or okex-futures)
  --check-only          Validate the target and show process state without starting
  -h, --help            Show this help

Live start order, with a health check after every step:
  1. persist_manager
  2. trade_engine
  3. account_monitor
  4. exec-pre-trade
  5. viz_server
  6. config_server

trade_signal is never started. The script refuses to begin when trade_signal
is running and verifies that it remains stopped at the end.
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
    --venue)
      VENUE="${2:-}"
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

if [[ ! "$ENV_NAME" =~ ^(binance|okex)_exec_[a-z0-9][a-z0-9_-]*([0-9]{2})$ ]]; then
  echo "[ERROR] env-name must match binance_exec_<suffixNN> or okex_exec_<suffixNN>: $ENV_NAME" >&2
  exit 2
fi
INFERRED_EXCHANGE="${BASH_REMATCH[1]}"
INFERRED_VENUE="${INFERRED_EXCHANGE}-futures"
VENUE="${VENUE:-$INFERRED_VENUE}"
case "$VENUE" in
  binance-futures|binance-coin-futures) VENUE_EXCHANGE="binance" ;;
  okex-futures) VENUE_EXCHANGE="okex" ;;
  *) echo "[ERROR] unsupported venue: $VENUE" >&2; exit 2 ;;
esac
if [[ "$VENUE_EXCHANGE" != "$INFERRED_EXCHANGE" ]]; then
  echo "[ERROR] venue/env-name mismatch: venue=$VENUE env-name=$ENV_NAME" >&2
  exit 2
fi
EXCHANGE="$INFERRED_EXCHANGE"
if [[ -z "$SSH_HOST" || "$SSH_HOST" == -* ]]; then
  echo "[ERROR] invalid SSH host: $SSH_HOST" >&2
  exit 2
fi

require_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "[ERROR] $name must be a positive integer: $value" >&2
    exit 2
  fi
}

require_positive_integer EXEC_START_WAIT_SECONDS "$STARTUP_WAIT_SECONDS"
require_positive_integer EXEC_START_SETTLE_SECONDS "$STARTUP_SETTLE_SECONDS"
require_positive_integer EXEC_START_LOG_LINES "$LOG_LINES"

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

echo "[INFO] start target host=$SSH_HOST exchange=$EXCHANGE venue=$VENUE env=$ENV_NAME dir=$REMOTE_DIR"
ssh_remote_bash SSH "$SSH_HOST" \
  "$REMOTE_DIR" \
  "$EXCHANGE" \
  "$VENUE" \
  "$CHECK_ONLY" \
  "$STARTUP_WAIT_SECONDS" \
  "$STARTUP_SETTLE_SECONDS" \
  "$LOG_LINES" <<'REMOTE_START'
set -euo pipefail

target="$1"
exchange="$2"
venue="$3"
check_only="$4"
startup_wait_seconds="$5"
startup_settle_seconds="$6"
log_lines="$7"
if [[ -d "$HOME/.local/bin" ]]; then
  export PATH="$HOME/.local/bin:$PATH"
fi
scripts_dir="$target/scripts"

if [[ ! -d "$target" || "$(readlink -f -- "$target")" != "$target" ]]; then
  echo "[ERROR] invalid remote target: $target" >&2
  exit 1
fi
if [[ "$(basename "$target")" != "${exchange}_exec_"* ]]; then
  echo "[ERROR] exchange/target mismatch: exchange=$exchange target=$target" >&2
  exit 1
fi
if [[ "$venue" != "${exchange}-futures" ]]; then
  echo "[ERROR] venue/exchange mismatch: venue=$venue exchange=$exchange" >&2
  exit 1
fi

required_files=(
  "$target/env.sh"
  "$target/config/exec_viz.toml"
  "$target/config/exec_config_server.env"
  "$scripts_dir/exec_config_server.py"
  "$scripts_dir/process_match_lib.sh"
)
required_executables=(
  "$target/exec-pre-trade"
  "$target/trade_signal"
  "$target/viz_server"
  "$target/persist_manager"
  "$target/trade_engine"
  "$target/account_monitor"
  "$scripts_dir/start_exec_persist_manager.sh"
  "$scripts_dir/stop_exec_persist_manager.sh"
  "$scripts_dir/start_exec_trade_engine.sh"
  "$scripts_dir/stop_exec_trade_engine.sh"
  "$scripts_dir/start_account_monitor.sh"
  "$scripts_dir/stop_account_monitor.sh"
  "$scripts_dir/start_exec_pre_trade.sh"
  "$scripts_dir/stop_exec_pre_trade.sh"
  "$scripts_dir/start_exec_viz_server.sh"
  "$scripts_dir/stop_exec_viz_server.sh"
  "$scripts_dir/start_exec_config_server.sh"
  "$scripts_dir/stop_exec_config_server.sh"
)
for required_file in "${required_files[@]}"; do
  if [[ ! -f "$required_file" ]]; then
    echo "[ERROR] required remote file not found: $required_file" >&2
    exit 1
  fi
done
for required_executable in "${required_executables[@]}"; do
  if [[ ! -x "$required_executable" ]]; then
    echo "[ERROR] required remote executable not found: $required_executable" >&2
    exit 1
  fi
done
for required_command in bash curl pmdaemon ps readlink sed grep ss tail sleep awk; do
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
  [[ "${EXEC_VENUE:-${VENUE:-}}" == "$venue" ]]
  [[ "${IPC_NAMESPACE:-}" == "$(basename "$target")" ]]
  case "$exchange" in
    binance)
      [[ -n "${BINANCE_API_KEY:-}" && -n "${BINANCE_API_SECRET:-}" ]]
      ;;
    okex)
      [[ -n "${OKX_API_KEY:-}" && -n "${OKX_API_SECRET:-}" && -n "${OKX_PASSPHRASE:-}" ]]
      ;;
  esac
); then
  echo "[ERROR] env.sh must provide matching EXEC_VENUE/IPC_NAMESPACE and ${exchange} credentials" >&2
  exit 1
fi

env_basename="$(basename "$target")"
dir_tag="$(printf '%s' "${env_basename,,}" | sed 's/[^a-z0-9_-]/_/g')"
env_tag="${env_basename#${exchange}_exec_}"
env_tag="$(printf '%s' "$env_tag" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
if [[ -z "$env_tag" ]]; then
  echo "[ERROR] failed to derive environment tag from: $env_basename" >&2
  exit 1
fi
case "$exchange" in
  binance) short_exchange="bn" ;;
  okex) short_exchange="ok" ;;
  *)
    echo "[ERROR] unsupported exchange: $exchange" >&2
    exit 1
    ;;
esac

config_server_script="$scripts_dir/exec_config_server.py"
config_server_port="$(
  unset PORT
  # shellcheck disable=SC1090
  source "$target/config/exec_config_server.env"
  printf '%s\n' "${PORT:-}"
)"
if [[ ! "$config_server_port" =~ ^[1-9][0-9]{0,4}$ ]] || ((10#$config_server_port > 65535)); then
  echo "[ERROR] invalid PORT in $target/config/exec_config_server.env: $config_server_port" >&2
  exit 1
fi

labels=(
  persist_manager
  trade_engine
  account_monitor
  exec-pre-trade
  viz_server
)
binaries=(
  "$target/persist_manager"
  "$target/trade_engine"
  "$target/account_monitor"
  "$target/exec-pre-trade"
  "$target/viz_server"
)
process_names=(
  "exec_pm_${dir_tag}"
  "exec_te_${dir_tag}"
  "exec_am_${short_exchange}_${env_tag}"
  "exec_pt_${dir_tag}"
  "exec_vz_${dir_tag}"
)
start_scripts=(
  "$scripts_dir/start_exec_persist_manager.sh"
  "$scripts_dir/start_exec_trade_engine.sh"
  "$scripts_dir/start_account_monitor.sh"
  "$scripts_dir/start_exec_pre_trade.sh"
  "$scripts_dir/start_exec_viz_server.sh"
)
signal_labels=(trade_signal)
signal_binaries=("$target/trade_signal")

find_exact_pids() {
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
      printf '%s\n' "$pid"
    fi
  done < <(ps -eo pid=,comm=)
}

find_config_server_pids() {
  local pid=""
  local args=""

  while read -r pid args; do
    [[ -n "$pid" ]] || continue
    if [[ "$args" == *"$config_server_script"* ]]; then
      printf '%s\n' "$pid"
    fi
  done < <(ps -eo pid=,args=)
}

config_server_port_is_listening() {
  ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "(:|\\])${config_server_port}\$"
}

config_server_is_healthy() {
  local http_code=""

  config_server_port_is_listening || return 1
  http_code="$(
    curl --silent --show-error \
      --output /dev/null \
      --write-out '%{http_code}' \
      --max-time 2 \
      "http://127.0.0.1:${config_server_port}/healthz" 2>/dev/null || true
  )"
  [[ "$http_code" == "200" ]]
}

print_process_state() {
  local index=""
  local pids=()

  for index in "${!labels[@]}"; do
    mapfile -t pids < <(find_exact_pids "${binaries[$index]}")
    if [[ "${#pids[@]}" -eq 0 ]]; then
      echo "[STATE] ${labels[$index]} stopped"
    else
      echo "[STATE] ${labels[$index]} running pids=${pids[*]}"
    fi
  done

  mapfile -t pids < <(find_config_server_pids)
  if [[ "${#pids[@]}" -eq 0 ]]; then
    if config_server_port_is_listening; then
      echo "[STATE] config_server stopped but port=${config_server_port} is occupied"
    else
      echo "[STATE] config_server stopped (port=${config_server_port})"
    fi
  elif config_server_is_healthy; then
    echo "[STATE] config_server running pids=${pids[*]} port=${config_server_port} http=200"
  else
    echo "[STATE] config_server unhealthy pids=${pids[*]} port=${config_server_port}"
  fi

  for index in "${!signal_labels[@]}"; do
    mapfile -t pids < <(find_exact_pids "${signal_binaries[$index]}")
    if [[ "${#pids[@]}" -eq 0 ]]; then
      if [[ -e "${signal_binaries[$index]}" ]]; then
        echo "[STATE] ${signal_labels[$index]} stopped (required)"
      else
        echo "[STATE] ${signal_labels[$index]} not deployed (treated as stopped)"
      fi
    else
      echo "[STATE] ${signal_labels[$index]} running pids=${pids[*]} (not allowed)"
    fi
  done
}

require_signals_stopped() {
  local index=""
  local pids=()

  for index in "${!signal_labels[@]}"; do
    mapfile -t pids < <(find_exact_pids "${signal_binaries[$index]}")
    if [[ "${#pids[@]}" -ne 0 ]]; then
      echo "[ERROR] ${signal_labels[$index]} is running (pids=${pids[*]}); refusing to start the Exec stack" >&2
      exit 3
    fi
  done
}

stop_existing_config_server() {
  local max_attempts=$((startup_wait_seconds * 5))
  local attempt=""
  local pids=()

  mapfile -t pids < <(find_config_server_pids)
  if [[ "${#pids[@]}" -gt 0 ]]; then
    echo "[INFO] stopping existing config_server instance(s): ${pids[*]}"
  else
    echo "[INFO] config_server has no existing process"
  fi

  bash "$scripts_dir/stop_exec_config_server.sh" </dev/null
  for ((attempt = 1; attempt <= max_attempts; attempt += 1)); do
    mapfile -t pids < <(find_config_server_pids)
    if [[ "${#pids[@]}" -eq 0 ]]; then
      echo "[INFO] existing config_server instance(s) stopped"
      return 0
    fi
    sleep 0.2
  done

  echo "[ERROR] config_server did not stop after ${startup_wait_seconds}s: ${pids[*]}" >&2
  return 1
}

wait_for_config_server() {
  local max_attempts=$((startup_wait_seconds * 5))
  local attempt=""
  local pids=()

  for ((attempt = 1; attempt <= max_attempts; attempt += 1)); do
    mapfile -t pids < <(find_config_server_pids)
    if [[ "${#pids[@]}" -eq 1 ]] && config_server_is_healthy; then
      printf '%s\n' "${pids[0]}"
      return 0
    fi
    if [[ "${#pids[@]}" -gt 1 ]]; then
      echo "[ERROR] config_server has multiple processes: ${pids[*]}" >&2
      return 1
    fi
    sleep 0.2
  done

  echo "[ERROR] config_server did not become healthy on port ${config_server_port} within ${startup_wait_seconds}s" >&2
  return 1
}

require_same_config_server() {
  local expected_pid="$1"
  local pids=()

  mapfile -t pids < <(find_config_server_pids)
  if [[ "${#pids[@]}" -ne 1 ]]; then
    echo "[ERROR] config_server is not running as exactly one process after startup: ${pids[*]:-none}" >&2
    return 1
  fi
  if [[ "${pids[0]}" != "$expected_pid" ]]; then
    echo "[ERROR] config_server restarted during health checks: initial_pid=$expected_pid current_pid=${pids[0]}" >&2
    return 1
  fi
  if ! config_server_is_healthy; then
    echo "[ERROR] config_server process=$expected_pid is live but port=${config_server_port} is unhealthy" >&2
    return 1
  fi
}

start_and_verify_config_server() {
  local pid=""

  echo
  echo "[STEP] start and verify config_server"
  stop_existing_config_server
  bash "$scripts_dir/start_exec_config_server.sh" </dev/null
  pid="$(wait_for_config_server)"
  echo "[INFO] config_server is live pid=$pid port=${config_server_port} http=200"

  sleep "$startup_settle_seconds"
  require_same_config_server "$pid"
  echo "[INFO] config_server survived ${startup_settle_seconds}s stability check"
  echo "[INFO] config_server health check passed pid=$pid port=${config_server_port}"
}

stop_existing_instances() {
  local binary="$1"
  local label="$2"
  local max_attempts=$((startup_wait_seconds * 5))
  local attempt=""
  local pids=()

  mapfile -t pids < <(find_exact_pids "$binary")
  if [[ "${#pids[@]}" -eq 0 ]]; then
    echo "[INFO] $label has no existing instance to stop"
    return 0
  fi

  echo "[INFO] stopping existing $label instance(s): ${pids[*]}"
  kill "${pids[@]}" >/dev/null 2>&1 || true
  for ((attempt = 1; attempt <= max_attempts; attempt += 1)); do
    mapfile -t pids < <(find_exact_pids "$binary")
    if [[ "${#pids[@]}" -eq 0 ]]; then
      echo "[INFO] existing $label instance(s) stopped"
      return 0
    fi
    sleep 0.2
  done

  echo "[WARN] $label did not stop after ${startup_wait_seconds}s; sending SIGKILL to exact target pids=${pids[*]}"
  kill -9 "${pids[@]}" >/dev/null 2>&1 || true
  sleep 1
  mapfile -t pids < <(find_exact_pids "$binary")
  if [[ "${#pids[@]}" -ne 0 ]]; then
    echo "[ERROR] failed to stop existing $label instance(s): ${pids[*]}" >&2
    return 1
  fi
  echo "[INFO] existing $label instance(s) stopped after SIGKILL"
}

wait_for_single_pid() {
  local binary="$1"
  local label="$2"
  local max_attempts=$((startup_wait_seconds * 5))
  local attempt=""
  local pids=()

  for ((attempt = 1; attempt <= max_attempts; attempt += 1)); do
    mapfile -t pids < <(find_exact_pids "$binary")
    if [[ "${#pids[@]}" -eq 1 ]]; then
      printf '%s\n' "${pids[0]}"
      return 0
    fi
    if [[ "${#pids[@]}" -gt 1 ]]; then
      echo "[ERROR] $label has multiple processes: ${pids[*]}" >&2
      return 1
    fi
    sleep 0.2
  done

  echo "[ERROR] $label did not become live within ${startup_wait_seconds}s" >&2
  return 1
}

require_same_single_pid() {
  local binary="$1"
  local label="$2"
  local expected_pid="$3"
  local pids=()

  mapfile -t pids < <(find_exact_pids "$binary")
  if [[ "${#pids[@]}" -ne 1 ]]; then
    echo "[ERROR] $label is not running as exactly one process after startup: ${pids[*]:-none}" >&2
    return 1
  fi
  if [[ "${pids[0]}" != "$expected_pid" ]]; then
    echo "[ERROR] $label restarted during health checks: initial_pid=$expected_pid current_pid=${pids[0]}" >&2
    return 1
  fi
}

check_recent_logs() {
  local label="$1"
  local process_name="$2"
  local log_output=""
  local stream_output=""
  local display_output=""
  local clean_output=""
  local log_status=0
  local logs_found=0
  local stream=""
  local log_path=""
  local error_pattern='(^|[^[:alnum:]_])(ERROR|FATAL)([^[:alnum:]_]|$)|(^|[[:space:]])Error:|panicked at|fatal runtime error|[Ss]egmentation fault|[Cc]ore dumped|[Aa]ddress already in use|[Ss]tartup failed|[Ff]ailed to start|[Ee]xited unexpectedly|[Pp]rocess .*crash'

  for stream in out error; do
    log_path="$HOME/.pmdaemon/logs/${process_name}-${stream}.log"
    echo "[LOG] $label $stream: tail -n $log_lines $log_path"
    if [[ ! -f "$log_path" ]]; then
      echo "[LOG] file not present: $log_path"
      continue
    fi
    logs_found=1
    set +e
    stream_output="$(tail -n "$log_lines" -- "$log_path" 2>&1)"
    log_status=$?
    set -e
    if [[ "$log_status" -ne 0 ]]; then
      echo "[ERROR] failed to tail $label $stream log (status=$log_status)" >&2
      return 1
    fi
    if [[ -n "$stream_output" ]]; then
      display_output="$(printf '%s\n' "$stream_output" | sed -E \
        -e 's/("(KEY|SIGN|API_KEY|API_SECRET|SECRET|SIGNATURE|TOKEN|AUTHORIZATION)"[[:space:]]*:[[:space:]]*")[^"]*"/\1<redacted>"/gI' \
        -e "s/(first4=')[^']*(' last4=')[^']*'/\1<redacted>\2<redacted>'/g" \
        -e 's/(Authorization:[[:space:]]*(Bearer|Basic)[[:space:]]+)[^[:space:]]+/\1<redacted>/gI')"
      printf '%s\n' "$display_output"
      log_output+="${display_output}"$'\n'
    else
      echo "[LOG] file is empty"
    fi
  done
  if [[ "$logs_found" -ne 1 ]]; then
    echo "[ERROR] no pmdaemon log file found for $label process=$process_name" >&2
    return 1
  fi

  clean_output="$(printf '%s\n' "$log_output" | sed -E $'s/\033\\[[0-9;]*[[:alpha:]]//g')"
  if grep -Eq "$error_pattern" <<<"$clean_output"; then
    echo "[ERROR] $label recent logs contain a startup error signature" >&2
    printf '%s\n' "$clean_output" | grep -E "$error_pattern" >&2 || true
    return 1
  fi
  echo "[INFO] $label recent logs contain no fatal/error startup signature"
}

run_start_script() {
  local label="$1"
  local script="$2"

  case "$label" in
    persist_manager|trade_engine|account_monitor|viz_server)
      bash "$script" </dev/null
      ;;
    exec-pre-trade)
      bash "$script" --venue "$venue" </dev/null
      ;;
    *)
      echo "[ERROR] unsupported component: $label" >&2
      return 1
      ;;
  esac
}

start_and_verify() {
  local index="$1"
  local label="${labels[$index]}"
  local binary="${binaries[$index]}"
  local process_name="${process_names[$index]}"
  local start_script="${start_scripts[$index]}"
  local pid=""

  echo
  echo "[STEP] start and verify $label"
  stop_existing_instances "$binary" "$label"
  run_start_script "$label" "$start_script"
  pid="$(wait_for_single_pid "$binary" "$label")"
  echo "[INFO] $label is live pid=$pid exe=$binary"

  sleep "$startup_settle_seconds"
  require_same_single_pid "$binary" "$label" "$pid"
  echo "[INFO] $label survived ${startup_settle_seconds}s stability check"

  check_recent_logs "$label" "$process_name"
  require_same_single_pid "$binary" "$label" "$pid"
  echo "[INFO] $label health check passed pid=$pid"
}

echo "[INFO] remote preflight passed"
print_process_state
require_signals_stopped
if [[ "$check_only" == "1" ]]; then
  echo "[INFO] check-only complete; no process was started or restarted"
  exit 0
fi

cd "$target"
echo "[WARN] LIVE start begins: env=$env_basename venue=$venue signal_processes=stopped"
for index in "${!labels[@]}"; do
  start_and_verify "$index"
done
start_and_verify_config_server

require_signals_stopped
echo
echo "[INFO] final process state"
print_process_state
echo "[INFO] start complete: env=$env_basename config_server_started=true signal_processes_started=false"
REMOTE_START
