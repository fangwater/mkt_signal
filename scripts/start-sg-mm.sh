#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_HOST="${SG_MM_START_HOST:-ubuntu@47.131.162.78}"
SSH_KEY="${SG_MM_START_KEY:-$ROOT_DIR/aws-sg.pem}"
ENV_NAME=""
ALL_ENVS=0
CHECK_ONLY=0
STARTUP_WAIT_SECONDS="${SG_MM_START_WAIT_SECONDS:-15}"
STARTUP_SETTLE_SECONDS="${SG_MM_START_SETTLE_SECONDS:-2}"

usage() {
  cat <<'USAGE'
Usage: scripts/start-sg-mm.sh (--env-name <name> | --all) [options]

Options:
  --host <ssh-host>    SSH target (default: ubuntu@47.131.162.78)
  --key <identity>     SSH private key (default: ./aws-sg.pem)
  --env-name <name>   Exactly bybit_mm_beta or bybit_mm_alpha
  --all                Start beta, then alpha
  --check-only        Validate target and show process state; start nothing
  -h, --help           Show this help

Live start order, with a stability check after every step:
  1. config_server
  2. viz_server
  3. persist_manager
  4. trade_engine
  5. pre_trade
  6. account_monitor
  7. trade_signal (always last)

manual_mm_signal is not part of the standard Bybit MM deployment and is never
started. Normal start requires the selected environment's stack to be fully
stopped.
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
    "$ROOT_DIR/scripts/start-sg-mm.sh" "${child_args[@]}"
  done
  exit 0
fi

case "$ENV_NAME" in
  bybit_mm_beta) CONFIG_PORT=18141; VIZ_PORT=10241 ;;
  bybit_mm_alpha) CONFIG_PORT=18142; VIZ_PORT=10242 ;;
esac

require_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "[ERROR] $name must be a positive integer: $value" >&2
    exit 2
  fi
}
require_positive_integer SG_MM_START_WAIT_SECONDS "$STARTUP_WAIT_SECONDS"
require_positive_integer SG_MM_START_SETTLE_SECONDS "$STARTUP_SETTLE_SECONDS"

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

echo "[INFO] start target host=$SSH_HOST exchange=bybit env=$ENV_NAME dir=$REMOTE_DIR"
"${SSH[@]}" "$SSH_HOST" bash -s -- \
  "$REMOTE_DIR" \
  "$CHECK_ONLY" \
  "$CONFIG_PORT" \
  "$VIZ_PORT" \
  "$STARTUP_WAIT_SECONDS" \
  "$STARTUP_SETTLE_SECONDS" <<'REMOTE_START'
set -euo pipefail

target="$1"
check_only="$2"
config_port="$3"
viz_port="$4"
startup_wait_seconds="$5"
startup_settle_seconds="$6"
scripts_dir="$target/scripts"
mm_scripts_dir="$target/mm_scripts"

if [[ ! -d "$target" || "$(readlink -f -- "$target")" != "$target" ]]; then
  echo "[ERROR] invalid remote target: $target" >&2
  exit 1
fi
case "$(basename "$target"):$config_port:$viz_port" in
  bybit_mm_beta:18141:10241|bybit_mm_alpha:18142:10242) ;;
  *) echo "[ERROR] target/port mismatch: target=$target config=$config_port viz=$viz_port" >&2; exit 1 ;;
esac

required_files=(
  "$target/env.sh"
  "$target/trade_engine.toml"
  "$target/config/viz.toml"
  "$target/config/mm_config_server.env"
  "$scripts_dir/mm_config_server.py"
  "$scripts_dir/mm_process_name.sh"
  "$scripts_dir/process_match_lib.sh"
)
required_executables=(
  "$target/trade_signal"
  "$target/account_monitor"
  "$target/viz_server"
  "$target/pre_trade"
  "$target/trade_engine"
  "$target/persist_manager"
  "$scripts_dir/start_mm_config_server.sh"
  "$scripts_dir/stop_mm_config_server.sh"
  "$scripts_dir/start_account_monitor.sh"
  "$scripts_dir/stop_account_monitor.sh"
  "$scripts_dir/start_trade_signal.sh"
  "$scripts_dir/stop_trade_signal.sh"
  "$mm_scripts_dir/start_mm_viz_server.sh"
  "$mm_scripts_dir/stop_mm_viz_server.sh"
  "$mm_scripts_dir/start_mm_persist_manager.sh"
  "$mm_scripts_dir/stop_mm_persist_manager.sh"
  "$mm_scripts_dir/start_mm_trade_engine.sh"
  "$mm_scripts_dir/stop_mm_trade_engine.sh"
  "$mm_scripts_dir/start_mm_pre_trade.sh"
  "$mm_scripts_dir/stop_mm_pre_trade.sh"
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
for required_command in bash curl pmdaemon npx ps readlink awk grep ss sleep; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "[ERROR] required remote command not found: $required_command" >&2
    exit 1
  fi
done

configured_port="$(
  unset PORT
  # shellcheck disable=SC1090
  source "$target/config/mm_config_server.env"
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

if ! (
  set +u
  set -a
  # shellcheck disable=SC1090
  source "$target/env.sh" >/dev/null 2>&1
  set +a
  [[ -n "${IPC_NAMESPACE:-}" ]]
  [[ -n "${BYBIT_API_KEY:-}" && -n "${BYBIT_API_SECRET:-}" ]]
); then
  echo "[ERROR] env.sh must provide IPC_NAMESPACE, BYBIT_API_KEY, and BYBIT_API_SECRET" >&2
  exit 1
fi

labels=(
  viz_server
  persist_manager
  trade_engine
  pre_trade
  account_monitor
  trade_signal
)
binaries=(
  "$target/viz_server"
  "$target/persist_manager"
  "$target/trade_engine"
  "$target/pre_trade"
  "$target/account_monitor"
  "$target/trade_signal"
)
manual_signal_binary="$target/manual_mm_signal"
config_server_script="$scripts_dir/mm_config_server.py"

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

port_is_listening() {
  local port="$1"
  ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "(:|\\])${port}\$"
}

config_server_is_healthy() {
  local http_code=""
  port_is_listening "$config_port" || return 1
  http_code="$(
    curl --silent --show-error \
      --output /dev/null \
      --write-out '%{http_code}' \
      --max-time 2 \
      "http://127.0.0.1:${config_port}/healthz" 2>/dev/null || true
  )"
  [[ "$http_code" == "200" ]]
}

print_process_state() {
  local index=""
  local pids=()
  mapfile -t pids < <(find_config_server_pids)
  if [[ "${#pids[@]}" -eq 1 ]] && config_server_is_healthy; then
    echo "[STATE] config_server running pid=${pids[0]} port=$config_port http=200"
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
  mapfile -t pids < <(find_exact_pids "$manual_signal_binary")
  if [[ "${#pids[@]}" -eq 0 ]]; then
    echo "[STATE] manual_mm_signal stopped or not deployed"
  else
    echo "[STATE] manual_mm_signal running pids=${pids[*]} (not allowed)"
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
  if port_is_listening "$config_port"; then
    echo "[ERROR] config port $config_port is already occupied" >&2
    found=1
  fi
  if port_is_listening "$viz_port"; then
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
  mapfile -t pids < <(find_exact_pids "$manual_signal_binary")
  if [[ "${#pids[@]}" -ne 0 ]]; then
    echo "[ERROR] manual_mm_signal is already running: ${pids[*]}" >&2
    found=1
  fi
  if [[ "$found" -ne 0 ]]; then
    echo "[ERROR] start requires a fully stopped target; run stop-sg-mm.sh first" >&2
    exit 3
  fi
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
  echo "[ERROR] config_server did not become healthy on port $config_port within ${startup_wait_seconds}s" >&2
  return 1
}

start_and_verify_config_server() {
  local initial_pid=""
  local pids=()
  echo
  echo "[STEP] start and verify config_server"
  bash "$scripts_dir/start_mm_config_server.sh"
  initial_pid="$(wait_for_config_server)"
  sleep "$startup_settle_seconds"
  mapfile -t pids < <(find_config_server_pids)
  if [[ "${#pids[@]}" -ne 1 || "${pids[0]}" != "$initial_pid" ]] || ! config_server_is_healthy; then
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

run_start_script() {
  local label="$1"
  case "$label" in
    viz_server) bash "$mm_scripts_dir/start_mm_viz_server.sh" --exchange bybit ;;
    persist_manager) bash "$mm_scripts_dir/start_mm_persist_manager.sh" ;;
    trade_engine) bash "$mm_scripts_dir/start_mm_trade_engine.sh" bybit ;;
    pre_trade) bash "$mm_scripts_dir/start_mm_pre_trade.sh" ;;
    account_monitor) bash "$scripts_dir/start_account_monitor.sh" ;;
    trade_signal) bash "$scripts_dir/start_trade_signal.sh" bybit ;;
    *) echo "[ERROR] unsupported start label: $label" >&2; return 1 ;;
  esac
}

start_and_verify_binary() {
  local label="$1"
  local binary="$2"
  local initial_pid=""
  local pids=()
  echo
  echo "[STEP] start and verify $label"
  run_start_script "$label"
  initial_pid="$(wait_for_binary "$binary" "$label")"
  sleep "$startup_settle_seconds"
  mapfile -t pids < <(find_exact_pids "$binary")
  if [[ "${#pids[@]}" -ne 1 || "${pids[0]}" != "$initial_pid" ]]; then
    echo "[ERROR] $label failed its ${startup_settle_seconds}s stability check" >&2
    return 1
  fi
  if [[ "$label" == "viz_server" ]] && ! port_is_listening "$viz_port"; then
    echo "[ERROR] viz_server is live but port=$viz_port is not listening" >&2
    return 1
  fi
  echo "[INFO] $label health check passed pid=$initial_pid"
}

echo "[INFO] remote preflight passed"
print_process_state
if [[ "$check_only" == "1" ]]; then
  echo "[INFO] check-only complete; no process was started"
  exit 0
fi

require_all_stopped
cd "$target"
echo "[WARN] LIVE start begins: env=$(basename "$target") exchange=bybit"
start_and_verify_config_server
for index in "${!labels[@]}"; do
  start_and_verify_binary "${labels[$index]}" "${binaries[$index]}"
done

echo
echo "[INFO] start complete: env=$(basename "$target") components=7 trade_signal=last persist_manager=included"
REMOTE_START
