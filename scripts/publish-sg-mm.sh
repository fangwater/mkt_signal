#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_HOST="${SG_MM_PUBLISH_HOST:-ubuntu@47.131.162.78}"
SSH_KEY="${SG_MM_PUBLISH_KEY:-$ROOT_DIR/aws-sg.pem}"
ENV_NAME=""
ALL_ENVS=0
CHECK_ONLY=0
SKIP_BUILD=0

usage() {
  cat <<'USAGE'
Usage: scripts/publish-sg-mm.sh (--env-name <name> | --all) [options]

Options:
  --host <ssh-host>    SSH target (default: ubuntu@47.131.162.78)
  --key <identity>     SSH private key (default: ./aws-sg.pem)
  --env-name <name>   Exactly bybit_mm_beta or bybit_mm_alpha
  --all                Publish beta, then alpha
  --check-only        Verify that all publish targets are stopped; change nothing
  --skip-build        Reuse binaries built by update-sg-mm.sh
  -h, --help           Show this help

Unless --check-only or --skip-build is used, every required release binary is
built before the first SSH call. Files are staged, SHA-256 checked, rechecked
against running processes, and atomically replaced. Environment configuration,
credentials, trade_engine.toml, data, logs, and nginx mappings are never
uploaded or replaced.
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
  if [[ "$CHECK_ONLY" -eq 0 && "$SKIP_BUILD" -eq 0 ]]; then
    "$ROOT_DIR/scripts/build-sg-mm-binaries.sh"
  fi
  for env_name in bybit_mm_beta bybit_mm_alpha; do
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
    "$ROOT_DIR/scripts/publish-sg-mm.sh" "${child_args[@]}"
  done
  exit 0
fi

if [[ "$CHECK_ONLY" -eq 0 && "$SKIP_BUILD" -eq 0 ]]; then
  "$ROOT_DIR/scripts/build-sg-mm-binaries.sh"
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
  bybit_mm_beta|bybit_mm_alpha) ;;
  *) echo "[ERROR] unsupported remote environment: $target" >&2; exit 1 ;;
esac

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
config_server="$target/scripts/mm_config_server.py"
found=0

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
      echo "[WARN] running publish target pid=$pid exe=$exe args=$args"
      found=1
      break
    fi
  done
done < <(ps -eo pid=,comm=)

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

echo "[INFO] publish target host=$SSH_HOST exchange=bybit env=$ENV_NAME dir=$REMOTE_DIR"
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
  "scripts/mm_config_server.py"
  "scripts/mm_process_name.sh"
  "scripts/process_match_lib.sh"
  "scripts/start_mm_config_server.sh"
  "scripts/stop_mm_config_server.sh"
  "scripts/start_account_monitor.sh"
  "scripts/stop_account_monitor.sh"
  "scripts/start_trade_signal.sh"
  "scripts/stop_trade_signal.sh"
  "scripts/close_mm_all_um_ws_orders.sh"
  "scripts/bybit_cancel_all_um_orders.py"
  "scripts/binance_local_ip.py"
  "scripts/print_mm_risk_params.py"
  "scripts/sync_mm_risk_params.py"
  "scripts/print_mm_strategy_params.py"
  "scripts/sync_mm_strategy_params.py"
  "scripts/print_mm_amount_u.py"
  "scripts/sync_mm_amount_u.py"
  "scripts/print_mm_symbol_list.py"
  "scripts/sync_mm_symbol_list.py"
  "mm_scripts/start_mm_viz_server.sh"
  "mm_scripts/stop_mm_viz_server.sh"
  "mm_scripts/start_mm_persist_manager.sh"
  "mm_scripts/stop_mm_persist_manager.sh"
  "mm_scripts/start_mm_trade_engine.sh"
  "mm_scripts/stop_mm_trade_engine.sh"
  "mm_scripts/start_mm_pre_trade.sh"
  "mm_scripts/stop_mm_pre_trade.sh"
  "mm_scripts/stop_manual_mm_signal.sh"
)
UPLOAD_NAMES=(
  "trade_signal"
  "bybit_account_monitor"
  "viz_server"
  "pre_trade"
  "trade_engine"
  "persist_manager"
  "mm_config_server.py"
  "mm_process_name.sh"
  "process_match_lib.sh"
  "start_mm_config_server.sh"
  "stop_mm_config_server.sh"
  "start_account_monitor.sh"
  "stop_account_monitor.sh"
  "start_trade_signal.sh"
  "stop_trade_signal.sh"
  "close_mm_all_um_ws_orders.sh"
  "bybit_cancel_all_um_orders.py"
  "binance_local_ip.py"
  "print_mm_risk_params.py"
  "sync_mm_risk_params.py"
  "print_mm_strategy_params.py"
  "sync_mm_strategy_params.py"
  "print_mm_amount_u.py"
  "sync_mm_amount_u.py"
  "print_mm_symbol_list.py"
  "sync_mm_symbol_list.py"
  "start_mm_viz_server.sh"
  "stop_mm_viz_server.sh"
  "start_mm_persist_manager.sh"
  "stop_mm_persist_manager.sh"
  "start_mm_trade_engine.sh"
  "stop_mm_trade_engine.sh"
  "start_mm_pre_trade.sh"
  "stop_mm_pre_trade.sh"
  "stop_manual_mm_signal.sh"
)
DESTINATIONS=(
  "trade_signal"
  "account_monitor"
  "viz_server"
  "pre_trade"
  "trade_engine"
  "persist_manager"
  "scripts/mm_config_server.py"
  "scripts/mm_process_name.sh"
  "scripts/process_match_lib.sh"
  "scripts/start_mm_config_server.sh"
  "scripts/stop_mm_config_server.sh"
  "scripts/start_account_monitor.sh"
  "scripts/stop_account_monitor.sh"
  "scripts/start_trade_signal.sh"
  "scripts/stop_trade_signal.sh"
  "scripts/close_mm_all_um_ws_orders.sh"
  "scripts/bybit_cancel_all_um_orders.py"
  "scripts/binance_local_ip.py"
  "scripts/print_mm_risk_params.py"
  "scripts/sync_mm_risk_params.py"
  "scripts/print_mm_strategy_params.py"
  "scripts/sync_mm_strategy_params.py"
  "scripts/print_mm_amount_u.py"
  "scripts/sync_mm_amount_u.py"
  "scripts/print_mm_symbol_list.py"
  "scripts/sync_mm_symbol_list.py"
  "mm_scripts/start_mm_viz_server.sh"
  "mm_scripts/stop_mm_viz_server.sh"
  "mm_scripts/start_mm_persist_manager.sh"
  "mm_scripts/stop_mm_persist_manager.sh"
  "mm_scripts/start_mm_trade_engine.sh"
  "mm_scripts/stop_mm_trade_engine.sh"
  "mm_scripts/start_mm_pre_trade.sh"
  "mm_scripts/stop_mm_pre_trade.sh"
  "mm_scripts/stop_manual_mm_signal.sh"
)

if [[ "${#LOCAL_RELATIVE[@]}" -ne "${#UPLOAD_NAMES[@]}" ||
      "${#LOCAL_RELATIVE[@]}" -ne "${#DESTINATIONS[@]}" ]]; then
  echo "[ERROR] internal publish manifest length mismatch" >&2
  exit 1
fi

LOCAL_PATHS=()
declare -A SEEN_UPLOAD_NAMES=()
for index in "${!LOCAL_RELATIVE[@]}"; do
  local_path="$ROOT_DIR/${LOCAL_RELATIVE[$index]}"
  upload_name="${UPLOAD_NAMES[$index]}"
  if [[ ! -f "$local_path" ]]; then
    echo "[ERROR] local publish file not found: $local_path" >&2
    exit 1
  fi
  if [[ "${local_path##*/}" != "$upload_name" ]]; then
    echo "[ERROR] upload name must match local basename: path=$local_path upload=$upload_name" >&2
    exit 1
  fi
  if [[ -n "${SEEN_UPLOAD_NAMES[$upload_name]:-}" ]]; then
    echo "[ERROR] duplicate upload name in publish manifest: $upload_name" >&2
    exit 1
  fi
  SEEN_UPLOAD_NAMES[$upload_name]=1
  LOCAL_PATHS+=("$local_path")
done

MANIFEST="$(mktemp)"
PUBLISH_MAP="$(mktemp)"
REMOTE_STAGE=""
cleanup() {
  rm -f "$MANIFEST" "$PUBLISH_MAP" >/dev/null 2>&1 || true
  if [[ -n "$REMOTE_STAGE" ]]; then
    "${SSH[@]}" "$SSH_HOST" bash -s -- "$REMOTE_DIR" "$REMOTE_STAGE" <<'REMOTE_CLEANUP' >/dev/null 2>&1 || true
set -euo pipefail
target="$1"
stage="$2"
case "$stage" in
  "$target"/.publish-sg-mm.*) ;;
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
  printf '%s\t%s\n' "${UPLOAD_NAMES[$index]}" "${DESTINATIONS[$index]}" >>"$PUBLISH_MAP"
done

REMOTE_STAGE="$("${SSH[@]}" "$SSH_HOST" "mktemp -d '$REMOTE_DIR/.publish-sg-mm.XXXXXX'")"
case "$REMOTE_STAGE" in
  "$REMOTE_DIR"/.publish-sg-mm.*) ;;
  *) echo "[ERROR] invalid remote staging path: $REMOTE_STAGE" >&2; exit 1 ;;
esac

echo "[INFO] SCP ${#LOCAL_PATHS[@]} files to $SSH_HOST:$REMOTE_STAGE"
"${SCP[@]}" "${LOCAL_PATHS[@]}" "${SSH_HOST}:${REMOTE_STAGE}/"
"${SCP[@]}" "$MANIFEST" "${SSH_HOST}:${REMOTE_STAGE}/SHA256SUMS"
"${SCP[@]}" "$PUBLISH_MAP" "${SSH_HOST}:${REMOTE_STAGE}/PUBLISH_MAP"
"${SSH[@]}" "$SSH_HOST" "cd '$REMOTE_STAGE' && sha256sum -c SHA256SUMS"

echo "[INFO] rechecking target processes before replacement"
check_remote_stopped

"${SSH[@]}" "$SSH_HOST" bash -s -- "$REMOTE_DIR" "$REMOTE_STAGE" <<'REMOTE_PUBLISH'
set -euo pipefail
target="$1"
stage="$2"
case "$stage" in
  "$target"/.publish-sg-mm.*) ;;
  *) echo "[ERROR] invalid staging path: $stage" >&2; exit 1 ;;
esac
[[ "$(readlink -f -- "$target")" == "$target" ]]
[[ -d "$target/scripts" && -d "$target/mm_scripts" ]]
[[ -f "$stage/SHA256SUMS" && -f "$stage/PUBLISH_MAP" ]]

while IFS=$'\t' read -r upload_name destination; do
  [[ -n "$upload_name" && -n "$destination" ]]
  case "$destination" in
    scripts/*|mm_scripts/*) ;;
    trade_signal|account_monitor|viz_server|pre_trade|trade_engine|persist_manager) ;;
    *) echo "[ERROR] invalid publish destination: $destination" >&2; exit 1 ;;
  esac
  if [[ "$destination" == *".."* || "$destination" == /* ]]; then
    echo "[ERROR] unsafe publish destination: $destination" >&2
    exit 1
  fi

  source="$stage/$upload_name"
  expected="$(awk -v name="$upload_name" '$2 == name { print $1 }' "$stage/SHA256SUMS")"
  [[ -n "$expected" && -f "$source" ]]
  actual="$(sha256sum "$source" | awk '{print $1}')"
  [[ "$actual" == "$expected" ]]
  chmod 755 "$source"
  mv -f "$source" "$target/$destination"
  actual="$(sha256sum "$target/$destination" | awk '{print $1}')"
  [[ "$actual" == "$expected" ]]
  echo "[INFO] published $destination sha256=$actual"
done <"$stage/PUBLISH_MAP"

rm -f "$stage/SHA256SUMS" "$stage/PUBLISH_MAP"
rmdir "$stage"
REMOTE_PUBLISH

REMOTE_STAGE=""
echo "[INFO] publish complete: $SSH_HOST:$REMOTE_DIR"
