#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_HOST="${EXEC_PUBLISH_HOST:-cta_exec}"
ENV_NAME="${EXEC_PUBLISH_ENV:-}"
VENUE="${EXEC_PUBLISH_VENUE:-}"
CHECK_ONLY=0
SKIP_BUILD=0

usage() {
  cat <<'USAGE'
Usage: scripts/publish-exec.sh --env-name <nameNN> [options]

Options:
  --host <ssh-host>     SSH config host (default: cta_exec)
  --env-name <nameNN>   Exec environment (required), e.g. binance_exec_trade01
  --venue <name>        Venue (binance-futures or okex-futures; inferred from env-name)
  --check-only          Only verify that publish target processes are stopped
  --skip-build          Reuse already-built release binaries
  -h, --help            Show this help

Unless --check-only or --skip-build is used, all required release binaries are
built before the first remote process check.
The publish aborts before SCP when any target process is running. It checks
again immediately before replacing files to close the check/upload race.
It never starts or stops a process and never rewrites env.sh or operator-owned
config under the remote Exec directory.
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
    --skip-build)
      SKIP_BUILD=1
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
INSTANCE_SUFFIX="${BASH_REMATCH[2]}"
INSTANCE_INDEX=$((10#$INSTANCE_SUFFIX))
if ((INSTANCE_INDEX < 1 || INSTANCE_INDEX > 99)); then
  echo "[ERROR] instance number must be 01..99: $ENV_NAME" >&2
  exit 2
fi
INFERRED_VENUE="${INFERRED_EXCHANGE}-futures"
VENUE="${VENUE:-$INFERRED_VENUE}"
if [[ "$VENUE" != "binance-futures" && "$VENUE" != "okex-futures" ]]; then
  echo "[ERROR] venue must be binance-futures or okex-futures: $VENUE" >&2
  exit 2
fi
if [[ "$VENUE" != "$INFERRED_VENUE" ]]; then
  echo "[ERROR] venue/env-name mismatch: venue=$VENUE env-name=$ENV_NAME" >&2
  exit 2
fi
EXCHANGE="$INFERRED_EXCHANGE"
if [[ -z "$SSH_HOST" || "$SSH_HOST" == -* ]]; then
  echo "[ERROR] invalid SSH host: $SSH_HOST" >&2
  exit 2
fi

ACCOUNT_MONITOR_BIN="${EXCHANGE}_account_monitor"

if [[ "$CHECK_ONLY" -eq 0 && "$SKIP_BUILD" -eq 0 ]]; then
  if ! command -v cargo >/dev/null 2>&1; then
    echo "[ERROR] required command not found: cargo" >&2
    exit 1
  fi
  echo "[INFO] building Exec release binaries for exchange=$EXCHANGE venue=$VENUE"
  (
    cd "$ROOT_DIR"
    cargo build --release -p mkt_signal \
      --bin exec-pre-trade \
      --bin trade_engine \
      --bin "$ACCOUNT_MONITOR_BIN"
    cargo build --release -p trade_signal --bin trade_signal
    cargo build --release -p viz_server --bin viz_server
    cargo build --release -p persist_manager --features runtime --bin persist_manager
  )
fi

SSH=(ssh -o BatchMode=yes -o ConnectTimeout=15)
SCP=(scp -o BatchMode=yes -o ConnectTimeout=15)

for command_name in ssh scp sha256sum awk mktemp; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "[ERROR] required command not found: $command_name" >&2
    exit 1
  fi
done

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
  local output=""
  local status=0

  set +e
  output="$("${SSH[@]}" "$SSH_HOST" bash -s -- "$REMOTE_DIR" "$EXCHANGE" <<'REMOTE_CHECK'
set -uo pipefail
target="$1"
exchange="$2"
if [[ ! -d "$target" ]]; then
  echo "[ERROR] remote environment does not exist: $target" >&2
  exit 1
fi
if [[ "$(basename "$target")" != "${exchange}_exec_"* ]]; then
  echo "[ERROR] exchange/target mismatch: exchange=$exchange target=$target" >&2
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
config_server="$target/scripts/exec_config_server.py"
found=0

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
  echo "[WARN] publish aborted: not all target processes are stopped" >&2
  exit 3
fi
echo "[INFO] all publish target processes are stopped"
REMOTE_CHECK
)"
  status=$?
  set -e

  if [[ -n "$output" ]]; then
    printf '%s\n' "$output"
  fi
  return "$status"
}

echo "[INFO] publish target host=$SSH_HOST exchange=$EXCHANGE venue=$VENUE env=$ENV_NAME dir=$REMOTE_DIR"
if check_remote_stopped; then
  :
else
  check_status=$?
  exit "$check_status"
fi
if [[ "$CHECK_ONLY" -eq 1 ]]; then
  echo "[INFO] check-only complete; no files were uploaded or replaced"
  exit 0
fi

LOCAL_RELATIVE=(
  "target/release/exec-pre-trade"
  "target/release/trade_signal"
  "target/release/${ACCOUNT_MONITOR_BIN}"
  "target/release/viz_server"
  "target/release/trade_engine"
  "target/release/persist_manager"
  "scripts/exec_config_server.py"
  "scripts/exec_config_client.py"
  "scripts/mm_process_name.sh"
  "scripts/process_match_lib.sh"
  "scripts/start_exec_config_server.sh"
  "scripts/stop_exec_config_server.sh"
  "scripts/start_exec_viz_server.sh"
  "scripts/stop_exec_viz_server.sh"
  "scripts/start_exec_persist_manager.sh"
  "scripts/stop_exec_persist_manager.sh"
  "scripts/start_exec_trade_engine.sh"
  "scripts/stop_exec_trade_engine.sh"
  "scripts/start_exec_pre_trade.sh"
  "scripts/stop_exec_pre_trade.sh"
  "scripts/start_exec_trade_signal.sh"
  "scripts/stop_exec_trade_signal.sh"
  "scripts/start_account_monitor.sh"
  "scripts/stop_account_monitor.sh"
  "scripts/start_trade_engine.sh"
  "scripts/stop_trade_engine.sh"
  "scripts/start_fr_persist_manager.sh"
  "scripts/stop_fr_persist_manager.sh"
  "scripts/print_exec_risk_params.py"
  "scripts/sync_exec_risk_params.py"
  "scripts/print_exec_max_pos_u.py"
  "scripts/sync_exec_max_pos_u.py"
)
UPLOAD_NAMES=(
  "exec-pre-trade"
  "trade_signal"
  "$ACCOUNT_MONITOR_BIN"
  "viz_server"
  "trade_engine"
  "persist_manager"
  "exec_config_server.py"
  "exec_config_client.py"
  "mm_process_name.sh"
  "process_match_lib.sh"
  "start_exec_config_server.sh"
  "stop_exec_config_server.sh"
  "start_exec_viz_server.sh"
  "stop_exec_viz_server.sh"
  "start_exec_persist_manager.sh"
  "stop_exec_persist_manager.sh"
  "start_exec_trade_engine.sh"
  "stop_exec_trade_engine.sh"
  "start_exec_pre_trade.sh"
  "stop_exec_pre_trade.sh"
  "start_exec_trade_signal.sh"
  "stop_exec_trade_signal.sh"
  "start_account_monitor.sh"
  "stop_account_monitor.sh"
  "start_trade_engine.sh"
  "stop_trade_engine.sh"
  "start_fr_persist_manager.sh"
  "stop_fr_persist_manager.sh"
  "print_exec_risk_params.py"
  "sync_exec_risk_params.py"
  "print_exec_max_pos_u.py"
  "sync_exec_max_pos_u.py"
)
DESTINATIONS=(
  "exec-pre-trade"
  "trade_signal"
  "account_monitor"
  "viz_server"
  "trade_engine"
  "persist_manager"
  "scripts/exec_config_server.py"
  "scripts/exec_config_client.py"
  "scripts/mm_process_name.sh"
  "scripts/process_match_lib.sh"
  "scripts/start_exec_config_server.sh"
  "scripts/stop_exec_config_server.sh"
  "scripts/start_exec_viz_server.sh"
  "scripts/stop_exec_viz_server.sh"
  "scripts/start_exec_persist_manager.sh"
  "scripts/stop_exec_persist_manager.sh"
  "scripts/start_exec_trade_engine.sh"
  "scripts/stop_exec_trade_engine.sh"
  "scripts/start_exec_pre_trade.sh"
  "scripts/stop_exec_pre_trade.sh"
  "scripts/start_exec_trade_signal.sh"
  "scripts/stop_exec_trade_signal.sh"
  "scripts/start_account_monitor.sh"
  "scripts/stop_account_monitor.sh"
  "scripts/start_trade_engine.sh"
  "scripts/stop_trade_engine.sh"
  "scripts/start_fr_persist_manager.sh"
  "scripts/stop_fr_persist_manager.sh"
  "scripts/print_exec_risk_params.py"
  "scripts/sync_exec_risk_params.py"
  "scripts/print_exec_max_pos_u.py"
  "scripts/sync_exec_max_pos_u.py"
)

case "$EXCHANGE" in
  binance)
    LOCAL_RELATIVE+=(
      "scripts/binance_cancel_all_std_um_ws_orders.py"
      "scripts/binance_cancel_all_unified_open_orders.py"
      "scripts/binance_local_ip.py"
      "scripts/sell_margin_spot.py"
    )
    UPLOAD_NAMES+=(
      "binance_cancel_all_std_um_ws_orders.py"
      "binance_cancel_all_unified_open_orders.py"
      "binance_local_ip.py"
      "sell_margin_spot.py"
    )
    DESTINATIONS+=(
      "scripts/binance_cancel_all_std_um_ws_orders.py"
      "scripts/binance_cancel_all_unified_open_orders.py"
      "scripts/binance_local_ip.py"
      "scripts/sell_margin_spot.py"
    )
    ;;
  okex)
    LOCAL_RELATIVE+=("scripts/okx_swap_open_orders.py")
    UPLOAD_NAMES+=("okx_swap_open_orders.py")
    DESTINATIONS+=("scripts/okx_swap_open_orders.py")
    ;;
esac

if [[ "${#LOCAL_RELATIVE[@]}" -ne "${#UPLOAD_NAMES[@]}" ||
      "${#LOCAL_RELATIVE[@]}" -ne "${#DESTINATIONS[@]}" ]]; then
  echo "[ERROR] internal publish manifest length mismatch" >&2
  exit 1
fi

LOCAL_PATHS=()
declare -A SEEN_UPLOAD_NAMES=()
for index in "${!LOCAL_RELATIVE[@]}"; do
  local_path="${ROOT_DIR}/${LOCAL_RELATIVE[$index]}"
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
  "$target"/.publish-exec.*) ;;
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
read -r map_hash _ < <(sha256sum "$PUBLISH_MAP")
printf '%s  PUBLISH_MAP\n' "$map_hash" >>"$MANIFEST"

REMOTE_STAGE="$("${SSH[@]}" "$SSH_HOST" "mktemp -d '$REMOTE_DIR/.publish-exec.XXXXXX'")"
case "$REMOTE_STAGE" in
  "$REMOTE_DIR"/.publish-exec.*) ;;
  *)
    echo "[ERROR] invalid remote staging path: $REMOTE_STAGE" >&2
    exit 1
    ;;
esac

echo "[INFO] SCP ${#LOCAL_PATHS[@]} files to $SSH_HOST:$REMOTE_STAGE"
"${SCP[@]}" "${LOCAL_PATHS[@]}" "${SSH_HOST}:${REMOTE_STAGE}/"
"${SCP[@]}" "$PUBLISH_MAP" "${SSH_HOST}:${REMOTE_STAGE}/PUBLISH_MAP"
"${SCP[@]}" "$MANIFEST" "${SSH_HOST}:${REMOTE_STAGE}/SHA256SUMS"
"${SSH[@]}" "$SSH_HOST" "cd '$REMOTE_STAGE' && sha256sum -c SHA256SUMS"

echo "[INFO] rechecking target processes before replacement"
if check_remote_stopped; then
  :
else
  check_status=$?
  exit "$check_status"
fi

"${SSH[@]}" "$SSH_HOST" bash -s -- "$REMOTE_DIR" "$REMOTE_STAGE" <<'REMOTE_PUBLISH'
set -euo pipefail
target="$1"
stage="$2"
case "$stage" in
  "$target"/.publish-exec.*) ;;
  *)
    echo "[ERROR] invalid staging path: $stage" >&2
    exit 1
    ;;
esac
[[ "$(readlink -f -- "$target")" == "$target" ]]
[[ -f "$stage/SHA256SUMS" ]]
[[ -f "$stage/PUBLISH_MAP" ]]

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
  mkdir -p "$(dirname "$target/$destination")"
  chmod 755 "$source"
  mv -f "$source" "$target/$destination"
  actual="$(sha256sum "$target/$destination" | awk '{print $1}')"
  [[ "$actual" == "$expected" ]]
  echo "[INFO] published $destination sha256=$actual"
}

while IFS=$'\t' read -r upload_name destination; do
  [[ -n "$upload_name" && -n "$destination" ]] || {
    echo "[ERROR] invalid empty entry in PUBLISH_MAP" >&2
    exit 1
  }
  case "$upload_name" in
    */*|*..*)
      echo "[ERROR] invalid publish upload name: $upload_name" >&2
      exit 1
      ;;
  esac
  case "$destination" in
    /*|*..*)
      echo "[ERROR] invalid publish destination: $destination" >&2
      exit 1
      ;;
  esac
  publish_file "$upload_name" "$destination"
done <"$stage/PUBLISH_MAP"

rm -f "$stage/PUBLISH_MAP" "$stage/SHA256SUMS"
rmdir "$stage"
REMOTE_PUBLISH

REMOTE_STAGE=""
echo "[INFO] publish complete: $SSH_HOST:$REMOTE_DIR"
