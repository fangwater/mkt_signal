#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib/ssh_remote_bash.sh
source "$SCRIPT_DIR/lib/ssh_remote_bash.sh"

SSH_HOST="${EXEC_STOP_HOST:-cta_exec}"
ENV_NAME="${EXEC_STOP_ENV:-}"
VENUE="${EXEC_STOP_VENUE:-}"
CHECK_ONLY=0

usage() {
  cat <<'USAGE'
Usage: scripts/stop-exec.sh --env-name <nameNN> [options]

Options:
  --host <ssh-host>     SSH config host (default: cta_exec)
  --env-name <nameNN>   Exec environment (required), e.g. binance_exec_trade01
  --venue <name>        Venue (binance-futures, binance-coin-futures or okex-futures)
  --check-only          Validate the target and show matching processes only
  -h, --help            Show this help

Live stop order:
  1. trade_engine
  2. exec-pre-trade
  3. trade_signal (if present)
  4. account_monitor
  5. persist_manager
  6. viz_server
  7. config_server
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

echo "[INFO] stop target host=$SSH_HOST exchange=$EXCHANGE venue=$VENUE env=$ENV_NAME dir=$REMOTE_DIR"
ssh_remote_bash SSH "$SSH_HOST" "$REMOTE_DIR" "$EXCHANGE" "$VENUE" "$CHECK_ONLY" <<'REMOTE_STOP'
set -euo pipefail

target="$1"
exchange="$2"
venue="$3"
check_only="$4"
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
  "$scripts_dir/process_match_lib.sh"
  "$scripts_dir/stop_exec_trade_engine.sh"
  "$scripts_dir/stop_exec_pre_trade.sh"
  "$scripts_dir/stop_exec_trade_signal.sh"
  "$scripts_dir/stop_account_monitor.sh"
  "$scripts_dir/stop_exec_persist_manager.sh"
  "$scripts_dir/stop_exec_viz_server.sh"
  "$scripts_dir/stop_exec_config_server.sh"
)
for required_file in "${required_files[@]}"; do
  if [[ ! -f "$required_file" ]]; then
    echo "[ERROR] required remote file not found: $required_file" >&2
    exit 1
  fi
done
for required_command in bash pmdaemon ps readlink awk grep; do
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
); then
  echo "[ERROR] env.sh must provide matching EXEC_VENUE=$venue" >&2
  exit 1
fi

target_executables=(
  "$target/exec-pre-trade"
  "$target/trade_signal"
  "$target/account_monitor"
  "$target/binance_account_monitor"
  "$target/okex_account_monitor"
  "$target/viz_server"
  "$target/trade_engine"
  "$target/persist_manager"
)
config_server="$scripts_dir/exec_config_server.py"

find_running_targets() {
  local pid=""
  local exe=""
  local args=""
  local target_exe=""

  while read -r pid comm; do
    [[ -n "$pid" ]] || continue
    case "$comm" in
      exec-pre-trade|trade_signal|account_monitor|*_account*|viz_server|trade_engine|persist_manager) ;;
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

echo "[INFO] remote preflight passed"
running_before="$(find_running_targets)"
if [[ -n "$running_before" ]]; then
  echo "[INFO] matching processes before stop:"
  printf '%s\n' "$running_before"
else
  echo "[INFO] no matching processes are currently running"
fi
if [[ "$check_only" == "1" ]]; then
  echo "[INFO] check-only complete; no process changes were made"
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
env_basename="$(basename "$target")"
echo "[WARN] LIVE stop begins: env=$env_basename venue=$venue"
run_step "stop trade_engine" bash "$scripts_dir/stop_exec_trade_engine.sh"
run_step "stop exec-pre-trade" bash "$scripts_dir/stop_exec_pre_trade.sh"
run_step "stop trade_signal" bash "$scripts_dir/stop_exec_trade_signal.sh"
run_step "stop account_monitor" bash "$scripts_dir/stop_account_monitor.sh"
run_step "stop persist_manager" bash "$scripts_dir/stop_exec_persist_manager.sh"
run_step "stop viz_server" bash "$scripts_dir/stop_exec_viz_server.sh"
run_step "stop config_server" bash "$scripts_dir/stop_exec_config_server.sh"

remaining="$(find_running_targets)"
if [[ -n "$remaining" ]]; then
  echo "[ERROR] matching processes remain after stop:" >&2
  printf '%s\n' "$remaining" >&2
  exit 1
fi
echo
echo "[INFO] stop complete: env=$env_basename processes_stopped=true"
REMOTE_STOP
