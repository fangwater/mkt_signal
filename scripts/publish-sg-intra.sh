#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_HOST="${SG_INTRA_PUBLISH_HOST:-ubuntu@47.131.162.78}"
SSH_KEY="${SG_INTRA_PUBLISH_KEY:-$ROOT_DIR/aws-sg.pem}"
ENV_NAME=""
ALL_ENVS=0
CHECK_ONLY=0
SKIP_BUILD=0

usage() {
  cat <<'USAGE'
Usage: scripts/publish-sg-intra.sh (--env-name <name> | --all) [options]

Options:
  --host <ssh-host>    SSH target (default: ubuntu@47.131.162.78)
  --key <identity>     SSH private key (default: ./aws-sg.pem)
  --env-name <name>   Exactly bybit-intra-arb01 or bybit-intra-arb02
  --all                Publish arb01, then arb02
  --check-only        Verify that all publish targets are stopped; change nothing
  --skip-build        Reuse binaries built by update-sg-intra.sh
  -h, --help           Show this help

Unless --check-only or --skip-build is used, every required release binary is
built before the first SSH call. Files are staged, SHA-256 checked, rechecked
against running processes, and atomically replaced. Environment configuration,
credentials, data, and logs are never uploaded or replaced.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) SSH_HOST="${2:-}"; shift 2 ;;
    --key) SSH_KEY="${2:-}"; shift 2 ;;
    --env-name) ENV_NAME="${2:-}"; shift 2 ;;
    --all) ALL_ENVS=1; shift ;;
    --check-only) CHECK_ONLY=1; shift ;;
    --skip-build) SKIP_BUILD=1; shift ;;
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
  if [[ "$CHECK_ONLY" -eq 0 && "$SKIP_BUILD" -eq 0 ]]; then
    "$ROOT_DIR/scripts/build-sg-intra-binaries.sh"
  fi
  for env_name in bybit-intra-arb01 bybit-intra-arb02; do
    child_args=(
      --host "$SSH_HOST"
      --key "$SSH_KEY"
      --env-name "$env_name"
    )
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
      child_args+=(--check-only)
    else
      child_args+=(--skip-build)
    fi
    "$ROOT_DIR/scripts/publish-sg-intra.sh" "${child_args[@]}"
  done
  exit 0
fi

if [[ "$CHECK_ONLY" -eq 0 && "$SKIP_BUILD" -eq 0 ]]; then
  "$ROOT_DIR/scripts/build-sg-intra-binaries.sh"
fi

for command_name in ssh scp sha256sum awk mktemp readlink; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "[ERROR] required command not found: $command_name" >&2
    exit 1
  fi
done

SSH=(ssh -i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=15)
SCP=(scp -i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=15)

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

check_remote_stopped() {
  "${SSH[@]}" "$SSH_HOST" bash -s -- "$REMOTE_DIR" <<'REMOTE_CHECK'
set -euo pipefail
target="$1"
if [[ ! -d "$target" || "$(readlink -f -- "$target")" != "$target" ]]; then
  echo "[ERROR] invalid remote environment: $target" >&2
  exit 1
fi
case "$(basename "$target")" in
  bybit-intra-arb01|bybit-intra-arb02) ;;
  *) echo "[ERROR] unsupported remote environment: $target" >&2; exit 1 ;;
esac

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
config_server="$target/scripts/intra_config_server.py"
found=0

while read -r pid; do
  [[ -n "$pid" ]] || continue
  exe="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
  exe="${exe% (deleted)}"
  for target_exe in "${target_executables[@]}"; do
    if [[ "$exe" == "$target_exe" ]]; then
      args="$(ps -p "$pid" -o args= 2>/dev/null || true)"
      echo "[WARN] running publish target pid=$pid exe=$exe args=$args"
      found=1
      break
    fi
  done
done < <(ps -eo pid=)

while read -r pid args; do
  [[ -n "$pid" ]] || continue
  if [[ "$args" == *"$config_server"* ]]; then
    echo "[WARN] running publish target pid=$pid config_server=$args"
    found=1
  fi
done < <(ps -eo pid=,args=)

if [[ "$found" -ne 0 ]]; then
  echo "[ERROR] publish aborted: not all target processes are stopped" >&2
  exit 3
fi
echo "[INFO] all publish target processes are stopped"
REMOTE_CHECK
}

echo "[INFO] publish target host=$SSH_HOST env=$ENV_NAME dir=$REMOTE_DIR"
check_remote_stopped
if [[ "$CHECK_ONLY" -eq 1 ]]; then
  echo "[INFO] check-only complete; no files or processes were changed"
  exit 0
fi

LOCAL_RELATIVE=(
  "target/release/trade_signal"
  "target/release/bybit_account_monitor"
  "target/release/viz_server"
  "target/release/pre_trade"
  "target/release/trade_engine"
  "target/release/persist_manager"
  "scripts/intra_config_server.py"
  "scripts/arb_per_symbol_overrides.py"
  "intra_scripts/sync_intra_risk_params.py"
  "intra_scripts/sync_intra_strategy_params.py"
  "intra_scripts/sync_intra_funding_thresholds.py"
  "intra_scripts/sync_intra_symbol_lists.py"
  "intra_scripts/sync_intra_spread_thresholds.py"
  "scripts/rolling_metrics/sync_rolling_metrics_params.py"
  "scripts/start_intra_config_server.sh"
  "scripts/stop_intra_config_server.sh"
  "scripts/process_match_lib.sh"
  "scripts/cancel_bybit_pm_orders.py"
  "intra_scripts/intra_monitor_process_lib.sh"
  "intra_scripts/start_intra_monitors.sh"
  "intra_scripts/stop_intra_monitors.sh"
  "intra_scripts/start_intra_trade_signal.sh"
  "intra_scripts/stop_intra_trade_signal.sh"
  "intra_scripts/start_intra_trade_engine.sh"
  "intra_scripts/stop_intra_trade_engine.sh"
  "intra_scripts/start_intra_pre_trade.sh"
  "intra_scripts/stop_intra_pre_trade.sh"
  "intra_scripts/start_intra_persist_manager.sh"
  "intra_scripts/stop_intra_persist_manager.sh"
  "intra_scripts/start_intra_viz_server.sh"
  "intra_scripts/stop_intra_viz_server.sh"
)
UPLOAD_NAMES=(
  "trade_signal"
  "bybit_account_monitor"
  "viz_server"
  "pre_trade"
  "trade_engine"
  "persist_manager"
  "intra_config_server.py"
  "arb_per_symbol_overrides.py"
  "sync_intra_risk_params.py"
  "sync_intra_strategy_params.py"
  "sync_intra_funding_thresholds.py"
  "sync_intra_symbol_lists.py"
  "sync_intra_spread_thresholds.py"
  "sync_rolling_metrics_params.py"
  "start_intra_config_server.sh"
  "stop_intra_config_server.sh"
  "process_match_lib.sh"
  "cancel_bybit_pm_orders.py"
  "intra_monitor_process_lib.sh"
  "start_intra_monitors.sh"
  "stop_intra_monitors.sh"
  "start_intra_trade_signal.sh"
  "stop_intra_trade_signal.sh"
  "start_intra_trade_engine.sh"
  "stop_intra_trade_engine.sh"
  "start_intra_pre_trade.sh"
  "stop_intra_pre_trade.sh"
  "start_intra_persist_manager.sh"
  "stop_intra_persist_manager.sh"
  "start_intra_viz_server.sh"
  "stop_intra_viz_server.sh"
)
LOCAL_PATHS=()
for relative_path in "${LOCAL_RELATIVE[@]}"; do
  local_path="$ROOT_DIR/$relative_path"
  if [[ ! -f "$local_path" ]]; then
    echo "[ERROR] local publish file not found: $local_path" >&2
    exit 1
  fi
  LOCAL_PATHS+=("$local_path")
done

MANIFEST="$(mktemp)"
REMOTE_STAGE=""
cleanup() {
  rm -f "$MANIFEST" >/dev/null 2>&1 || true
  if [[ -n "$REMOTE_STAGE" ]]; then
    "${SSH[@]}" "$SSH_HOST" bash -s -- "$REMOTE_DIR" "$REMOTE_STAGE" <<'REMOTE_CLEANUP' >/dev/null 2>&1 || true
set -euo pipefail
target="$1"
stage="$2"
case "$stage" in
  "$target"/.publish-sg-intra.*) ;;
  *) exit 1 ;;
esac
if [[ -d "$stage" ]]; then
  find "$stage" -mindepth 1 -maxdepth 1 -type f -delete
  rmdir "$stage" 2>/dev/null || true
fi
REMOTE_CLEANUP
  fi
}
trap cleanup EXIT

for index in "${!LOCAL_PATHS[@]}"; do
  read -r file_hash _ < <(sha256sum "${LOCAL_PATHS[$index]}")
  printf '%s  %s\n' "$file_hash" "${UPLOAD_NAMES[$index]}" >>"$MANIFEST"
done

REMOTE_STAGE="$("${SSH[@]}" "$SSH_HOST" "mktemp -d '$REMOTE_DIR/.publish-sg-intra.XXXXXX'")"
case "$REMOTE_STAGE" in
  "$REMOTE_DIR"/.publish-sg-intra.*) ;;
  *) echo "[ERROR] invalid remote staging path: $REMOTE_STAGE" >&2; exit 1 ;;
esac

echo "[INFO] SCP ${#LOCAL_PATHS[@]} files to $SSH_HOST:$REMOTE_STAGE"
"${SCP[@]}" "${LOCAL_PATHS[@]}" "${SSH_HOST}:${REMOTE_STAGE}/"
"${SCP[@]}" "$MANIFEST" "${SSH_HOST}:${REMOTE_STAGE}/SHA256SUMS"
"${SSH[@]}" "$SSH_HOST" "cd '$REMOTE_STAGE' && sha256sum -c SHA256SUMS"

echo "[INFO] rechecking target processes before replacement"
check_remote_stopped

"${SSH[@]}" "$SSH_HOST" bash -s -- "$REMOTE_DIR" "$REMOTE_STAGE" <<'REMOTE_PUBLISH'
set -euo pipefail
target="$1"
stage="$2"
case "$stage" in
  "$target"/.publish-sg-intra.*) ;;
  *) echo "[ERROR] invalid staging path: $stage" >&2; exit 1 ;;
esac
[[ "$(readlink -f -- "$target")" == "$target" ]]
[[ -d "$target/scripts" && -d "$target/intra_scripts" ]]
[[ -f "$stage/SHA256SUMS" ]]

publish_file() {
  local upload_name="$1"
  local destination="$2"
  local source="$stage/$upload_name"
  local expected=""
  local actual=""

  expected="$(awk -v name="$upload_name" '$2 == name { print $1 }' "$stage/SHA256SUMS")"
  [[ -n "$expected" && -f "$source" ]]
  actual="$(sha256sum "$source" | awk '{print $1}')"
  [[ "$actual" == "$expected" ]]
  chmod 755 "$source"
  mv -f "$source" "$target/$destination"
  actual="$(sha256sum "$target/$destination" | awk '{print $1}')"
  [[ "$actual" == "$expected" ]]
  echo "[INFO] published $destination sha256=$actual"
}

publish_file trade_signal trade_signal
publish_file bybit_account_monitor account_monitor_bybit
publish_file viz_server viz_server
publish_file pre_trade pre_trade
publish_file trade_engine trade_engine
publish_file persist_manager persist_manager
publish_file intra_config_server.py scripts/intra_config_server.py
publish_file arb_per_symbol_overrides.py scripts/arb_per_symbol_overrides.py
publish_file sync_intra_risk_params.py scripts/sync_intra_risk_params.py
publish_file sync_intra_strategy_params.py scripts/sync_intra_strategy_params.py
publish_file sync_intra_funding_thresholds.py scripts/sync_intra_funding_thresholds.py
publish_file sync_intra_symbol_lists.py scripts/sync_intra_symbol_lists.py
publish_file sync_intra_spread_thresholds.py scripts/sync_intra_spread_thresholds.py
publish_file sync_rolling_metrics_params.py scripts/sync_rolling_metrics_params.py
publish_file start_intra_config_server.sh scripts/start_intra_config_server.sh
publish_file stop_intra_config_server.sh scripts/stop_intra_config_server.sh
publish_file process_match_lib.sh scripts/process_match_lib.sh
publish_file cancel_bybit_pm_orders.py scripts/cancel_bybit_pm_orders.py
publish_file intra_monitor_process_lib.sh intra_scripts/intra_monitor_process_lib.sh
publish_file start_intra_monitors.sh intra_scripts/start_intra_monitors.sh
publish_file stop_intra_monitors.sh intra_scripts/stop_intra_monitors.sh
publish_file start_intra_trade_signal.sh intra_scripts/start_intra_trade_signal.sh
publish_file stop_intra_trade_signal.sh intra_scripts/stop_intra_trade_signal.sh
publish_file start_intra_trade_engine.sh intra_scripts/start_intra_trade_engine.sh
publish_file stop_intra_trade_engine.sh intra_scripts/stop_intra_trade_engine.sh
publish_file start_intra_pre_trade.sh intra_scripts/start_intra_pre_trade.sh
publish_file stop_intra_pre_trade.sh intra_scripts/stop_intra_pre_trade.sh
publish_file start_intra_persist_manager.sh intra_scripts/start_intra_persist_manager.sh
publish_file stop_intra_persist_manager.sh intra_scripts/stop_intra_persist_manager.sh
publish_file start_intra_viz_server.sh intra_scripts/start_intra_viz_server.sh
publish_file stop_intra_viz_server.sh intra_scripts/stop_intra_viz_server.sh

rm -f "$stage/SHA256SUMS"
rmdir "$stage"
REMOTE_PUBLISH

REMOTE_STAGE=""
echo "[INFO] publish complete: $SSH_HOST:$REMOTE_DIR"
