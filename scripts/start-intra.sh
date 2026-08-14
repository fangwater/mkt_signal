#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/intra_orchestration_lib.sh
source "$ROOT_DIR/scripts/intra_orchestration_lib.sh"

SSH_KEY=""
ENV_NAME=""
ALL_ENVS=0
CHECK_ONLY=0
STARTUP_WAIT_SECONDS="${INTRA_START_WAIT_SECONDS:-15}"
STARTUP_SETTLE_SECONDS="${INTRA_START_SETTLE_SECONDS:-2}"

usage() {
  cat <<'USAGE'
Usage: scripts/start-intra.sh (--env-name <name> | --all) [options]

Options:
  --key <identity>     Optional Bybit/SG SSH identity override
  --env-name <name>   One supported Intra environment
  --all                Start every supported Intra environment
  --check-only        Validate target and show process state; start nothing
  -h, --help           Show this help

Supported environments:
  bybit-intra-arb01, bybit-intra-arb02 -> SG
  okex-intra-arb01, binance-intra-arb01 -> jp-meta-elvpn

Live start order, with a stability check after every step:
  1. config_server
  2. viz_server
  3. persist_manager
  4. trade_engine
  5. pre_trade
  6. account_monitor

trade_signal is never started. The script refuses to begin when trade_signal is
running and verifies that it remains stopped at the end. Normal start requires
the selected environment's base stack to be fully stopped.
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
    "$ROOT_DIR/scripts/start-intra.sh" "${child_args[@]}"
  done
  exit 0
fi

require_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "[ERROR] $name must be a positive integer: $value" >&2
    exit 2
  fi
}
require_positive_integer INTRA_START_WAIT_SECONDS "$STARTUP_WAIT_SECONDS"
require_positive_integer INTRA_START_SETTLE_SECONDS "$STARTUP_SETTLE_SECONDS"

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

echo "[INFO] start target host=$INTRA_SSH_HOST exchange=$INTRA_EXCHANGE env=$ENV_NAME dir=$REMOTE_DIR"
"${SSH[@]}" "$INTRA_SSH_HOST" bash -s -- \
  "$REMOTE_DIR" \
  "$ENV_NAME" \
  "$INTRA_EXCHANGE" \
  "$INTRA_ACCOUNT_MONITOR_DEST" \
  "$INTRA_ACCOUNT_MONITOR_BIN" \
  "$CHECK_ONLY" \
  "$INTRA_CONFIG_PORT" \
  "$INTRA_VIZ_PORT" \
  "$STARTUP_WAIT_SECONDS" \
  "$STARTUP_SETTLE_SECONDS" <<'REMOTE_START'
set -euo pipefail

target="$1"
env_name="$2"
exchange="$3"
account_monitor_dest="$4"
account_monitor_bin="$5"
check_only="$6"
config_port="$7"
viz_port="$8"
startup_wait_seconds="$9"
startup_settle_seconds="${10}"
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

required_files=(
  "$target/env.sh"
  "$target/config/viz.toml"
  "$target/config/intra_config_server.env"
  "$scripts_dir/intra_config_server.py"
  "$scripts_dir/process_match_lib.sh"
)
required_executables=(
  "$target/trade_signal"
  "$target/$account_monitor_dest"
  "$target/viz_server"
  "$target/pre_trade"
  "$target/trade_engine"
  "$target/persist_manager"
  "$scripts_dir/start_intra_config_server.sh"
  "$scripts_dir/stop_intra_config_server.sh"
  "$intra_scripts_dir/start_intra_viz_server.sh"
  "$intra_scripts_dir/stop_intra_viz_server.sh"
  "$intra_scripts_dir/start_intra_persist_manager.sh"
  "$intra_scripts_dir/stop_intra_persist_manager.sh"
  "$intra_scripts_dir/start_intra_trade_engine.sh"
  "$intra_scripts_dir/stop_intra_trade_engine.sh"
  "$intra_scripts_dir/start_intra_pre_trade.sh"
  "$intra_scripts_dir/stop_intra_pre_trade.sh"
  "$intra_scripts_dir/start_intra_monitors.sh"
  "$intra_scripts_dir/stop_intra_monitors.sh"
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
for required_command in bash pmdaemon npx ps readlink awk grep ss sleep; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "[ERROR] required remote command not found: $required_command" >&2
    exit 1
  fi
done

configured_port="$(
  unset PORT
  # shellcheck disable=SC1090
  source "$target/config/intra_config_server.env"
  printf '%s\n' "${PORT:-}"
)"
if [[ "$configured_port" != "$config_port" ]]; then
  echo "[ERROR] config server port mismatch: expected=$config_port configured=${configured_port:-<empty>}" >&2
  exit 1
fi
configured_viz_port="$(
  awk '
    /^\[servers\.http\]$/ { in_http = 1; next }
    /^\[/ { in_http = 0 }
    in_http && /^[[:space:]]*port[[:space:]]*=/ {
      sub(/^[^=]*=[[:space:]]*/, "")
      sub(/[[:space:]]*#.*/, "")
      gsub(/[[:space:]]/, "")
      print
      exit
    }
  ' "$target/config/viz.toml"
)"
if [[ "$configured_viz_port" != "$viz_port" ]]; then
  echo "[ERROR] viz server port mismatch: expected=$viz_port configured=${configured_viz_port:-<empty>}" >&2
  exit 1
fi

(
  set +u
  set -a
  # shellcheck disable=SC1090
  source "$target/env.sh" >/dev/null 2>&1
  set +a
  [[ -n "${IPC_NAMESPACE:-}" ]] || exit 1
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
    *)
      exit 1
      ;;
  esac
) || {
  echo "[ERROR] env.sh is missing IPC namespace, $exchange credentials, or required account mode" >&2
  exit 1
}

labels=(
  viz_server
  persist_manager
  trade_engine
  pre_trade
  account_monitor
)
binaries=(
  "$target/viz_server"
  "$target/persist_manager"
  "$target/trade_engine"
  "$target/pre_trade"
  "$target/$account_monitor_dest"
)
start_scripts=(
  "$intra_scripts_dir/start_intra_viz_server.sh"
  "$intra_scripts_dir/start_intra_persist_manager.sh"
  "$intra_scripts_dir/start_intra_trade_engine.sh"
  "$intra_scripts_dir/start_intra_pre_trade.sh"
  "$intra_scripts_dir/start_intra_monitors.sh"
)
trade_signal_binary="$target/trade_signal"
legacy_account_monitor_binary="$target/$account_monitor_bin"
config_server_script="$scripts_dir/intra_config_server.py"

find_exact_pids() {
  local expected="$1"
  local pid=""
  local exe=""
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    exe="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
    exe="${exe% (deleted)}"
    if [[ "$exe" == "$expected" ]]; then
      printf '%s\n' "$pid"
    fi
  done < <(ps -eo pid=)
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

config_port_is_listening() {
  ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "(:|\\])${config_port}\$"
}

viz_port_is_listening() {
  ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "(:|\\])${viz_port}\$"
}

print_process_state() {
  local index=""
  local pids=()
  mapfile -t pids < <(find_config_server_pids)
  if [[ "${#pids[@]}" -eq 1 ]] && config_port_is_listening; then
    echo "[STATE] config_server running pid=${pids[0]} port=$config_port"
  elif [[ "${#pids[@]}" -eq 0 ]]; then
    echo "[STATE] config_server stopped port=$config_port"
  else
    echo "[STATE] config_server unhealthy pids=${pids[*]:-none} port=$config_port"
  fi
  for index in "${!labels[@]}"; do
    mapfile -t pids < <(find_exact_pids "${binaries[$index]}")
    if [[ "${#pids[@]}" -eq 0 ]]; then
      echo "[STATE] ${labels[$index]} stopped"
    else
      echo "[STATE] ${labels[$index]} running pids=${pids[*]}"
    fi
  done
  mapfile -t pids < <(find_exact_pids "$legacy_account_monitor_binary")
  if [[ "${#pids[@]}" -ne 0 ]]; then
    echo "[STATE] legacy account_monitor running pids=${pids[*]} path=$legacy_account_monitor_binary"
  fi
  mapfile -t pids < <(find_exact_pids "$trade_signal_binary")
  if [[ "${#pids[@]}" -eq 0 ]]; then
    echo "[STATE] trade_signal stopped (required)"
  else
    echo "[STATE] trade_signal running pids=${pids[*]} (not allowed)"
  fi
}

require_trade_signal_stopped() {
  local pids=()
  mapfile -t pids < <(find_exact_pids "$trade_signal_binary")
  if [[ "${#pids[@]}" -ne 0 ]]; then
    echo "[ERROR] trade_signal is already running (pids=${pids[*]}); refusing to start the base Intra stack" >&2
    exit 3
  fi
}

require_all_stopped() {
  local index=""
  local pids=()
  local found=0
  mapfile -t pids < <(find_config_server_pids)
  if [[ "${#pids[@]}" -ne 0 ]]; then
    echo "[ERROR] config_server is already running: ${pids[*]}" >&2
    found=1
  fi
  if config_port_is_listening; then
    echo "[ERROR] config port $config_port is already occupied" >&2
    found=1
  fi
  if viz_port_is_listening; then
    echo "[ERROR] viz port $viz_port is already occupied" >&2
    found=1
  fi
  for index in "${!labels[@]}"; do
    mapfile -t pids < <(find_exact_pids "${binaries[$index]}")
    if [[ "${#pids[@]}" -ne 0 ]]; then
      echo "[ERROR] ${labels[$index]} is already running: ${pids[*]}" >&2
      found=1
    fi
  done
  mapfile -t pids < <(find_exact_pids "$legacy_account_monitor_binary")
  if [[ "${#pids[@]}" -ne 0 ]]; then
    echo "[ERROR] legacy account_monitor is already running: ${pids[*]} path=$legacy_account_monitor_binary" >&2
    found=1
  fi
  if [[ "$found" -ne 0 ]]; then
    echo "[ERROR] start requires a fully stopped target; run stop-intra.sh first" >&2
    exit 3
  fi
}

wait_for_config_server() {
  local max_attempts=$((startup_wait_seconds * 5))
  local attempt=""
  local pids=()
  for ((attempt = 1; attempt <= max_attempts; attempt += 1)); do
    mapfile -t pids < <(find_config_server_pids)
    if [[ "${#pids[@]}" -eq 1 ]] && config_port_is_listening; then
      printf '%s\n' "${pids[0]}"
      return 0
    fi
    if [[ "${#pids[@]}" -gt 1 ]]; then
      echo "[ERROR] config_server has multiple processes: ${pids[*]}" >&2
      return 1
    fi
    sleep 0.2
  done
  echo "[ERROR] config_server did not become live on port $config_port within ${startup_wait_seconds}s" >&2
  return 1
}

start_and_verify_config_server() {
  local initial_pid=""
  local pids=()
  echo
  echo "[STEP] start and verify config_server"
  bash "$scripts_dir/start_intra_config_server.sh"
  initial_pid="$(wait_for_config_server)"
  sleep "$startup_settle_seconds"
  mapfile -t pids < <(find_config_server_pids)
  if [[ "${#pids[@]}" -ne 1 || "${pids[0]}" != "$initial_pid" ]] || ! config_port_is_listening; then
    echo "[ERROR] config_server failed its ${startup_settle_seconds}s stability check" >&2
    return 1
  fi
  echo "[INFO] config_server health check passed pid=$initial_pid port=$config_port"
}

wait_for_binary() {
  local expected="$1"
  local label="$2"
  local max_attempts=$((startup_wait_seconds * 5))
  local attempt=""
  local pids=()
  for ((attempt = 1; attempt <= max_attempts; attempt += 1)); do
    mapfile -t pids < <(find_exact_pids "$expected")
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
  echo "[ERROR] $label did not start within ${startup_wait_seconds}s" >&2
  return 1
}

start_and_verify_binary() {
  local label="$1"
  local binary="$2"
  local start_script="$3"
  local initial_pid=""
  local pids=()
  echo
  echo "[STEP] start and verify $label"
  bash "$start_script"
  initial_pid="$(wait_for_binary "$binary" "$label")"
  sleep "$startup_settle_seconds"
  mapfile -t pids < <(find_exact_pids "$binary")
  if [[ "${#pids[@]}" -ne 1 || "${pids[0]}" != "$initial_pid" ]]; then
    echo "[ERROR] $label failed its ${startup_settle_seconds}s stability check" >&2
    return 1
  fi
  if [[ "$label" == "viz_server" ]] && ! viz_port_is_listening; then
    echo "[ERROR] viz_server is live but port=$viz_port is not listening" >&2
    return 1
  fi
  echo "[INFO] $label health check passed pid=$initial_pid"
}

echo "[INFO] remote preflight passed"
print_process_state
require_trade_signal_stopped
if [[ "$check_only" == "1" ]]; then
  echo "[INFO] check-only complete; no process was started"
  exit 0
fi

require_all_stopped
cd "$target"
echo "[WARN] LIVE start begins: env=$(basename "$target") exchange=$exchange trade_signal=stopped"
start_and_verify_config_server
for index in "${!labels[@]}"; do
  start_and_verify_binary \
    "${labels[$index]}" \
    "${binaries[$index]}" \
    "${start_scripts[$index]}"
done

require_trade_signal_stopped
echo
echo "[INFO] final process state"
print_process_state
echo "[INFO] start complete: env=$(basename "$target") components=6 trade_signal_started=false persist_manager=included"
REMOTE_START
