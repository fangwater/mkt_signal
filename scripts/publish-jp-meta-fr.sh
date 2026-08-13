#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_HOST="${FR_PUBLISH_HOST:-jp-meta-elvpn}"
ENV_NAME="${FR_PUBLISH_ENV:-binance_fr_arb03}"
EXCHANGE="${FR_PUBLISH_EXCHANGE:-}"
CHECK_ONLY=0
SKIP_BUILD=0

usage() {
  cat <<'USAGE'
Usage: scripts/publish-jp-meta-fr.sh [options]

Options:
  --host <ssh-host>    SSH config host (default: jp-meta-elvpn)
  --env-name <name>   Binance/Gate FR environment (default: binance_fr_arb03)
  --exchange <name>   Exchange (binance or gate; inferred from env-name)
  --check-only        Only verify that publish target processes are stopped
  --skip-build        Reuse binaries built by update-jp-meta-fr.sh
  -h, --help          Show this help

Unless --check-only or --skip-build is used, all required release binaries are
built before the first remote process check.
The publish aborts before SCP when any target process is running. It checks
again immediately before replacing files to close the check/upload race.
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
    --exchange)
      EXCHANGE="${2:-}"
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
      echo "[ERROR] Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! "$ENV_NAME" =~ ^(binance|gate)_fr_[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "[ERROR] env-name must match binance_fr_<suffix> or gate_fr_<suffix>: $ENV_NAME" >&2
  exit 2
fi
INFERRED_EXCHANGE="${BASH_REMATCH[1]}"
EXCHANGE="${EXCHANGE,,}"
if [[ -z "$EXCHANGE" ]]; then
  EXCHANGE="$INFERRED_EXCHANGE"
fi
case "$EXCHANGE" in
  binance|gate) ;;
  *)
    echo "[ERROR] exchange must be binance or gate: $EXCHANGE" >&2
    exit 2
    ;;
esac
if [[ "$EXCHANGE" != "$INFERRED_EXCHANGE" ]]; then
  echo "[ERROR] exchange/env-name mismatch: exchange=$EXCHANGE env-name=$ENV_NAME" >&2
  exit 2
fi
if [[ -z "$SSH_HOST" || "$SSH_HOST" == -* ]]; then
  echo "[ERROR] invalid SSH host: $SSH_HOST" >&2
  exit 2
fi

if [[ "$CHECK_ONLY" -eq 0 && "$SKIP_BUILD" -eq 0 ]]; then
  "$ROOT_DIR/scripts/build-jp-meta-binaries.sh" --exchange "$EXCHANGE"
fi

SSH=(ssh -o BatchMode=yes)
SCP=(scp -o BatchMode=yes)

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
  output="$("${SSH[@]}" "$SSH_HOST" bash -s -- "$REMOTE_DIR" <<'REMOTE_CHECK'
set -uo pipefail
target="$1"
if [[ ! -d "$target" ]]; then
  echo "[ERROR] remote environment does not exist: $target" >&2
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
config_server="$target/scripts/fr_config_server.py"
found=0

while read -r pid comm; do
  case "$comm" in
    trade_signal|account_monitor|*_account*|viz_server|pre_trade|trade_engine|persist_manager) ;;
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

while IFS= read -r process_row; do
  if [[ "$process_row" == *"$config_server"* ]]; then
    echo "[WARN] running publish target $process_row"
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

echo "[INFO] publish target host=$SSH_HOST exchange=$EXCHANGE env=$ENV_NAME dir=$REMOTE_DIR"
if check_remote_stopped; then
  :
else
  check_status=$?
  exit "$check_status"
fi
if [[ "$CHECK_ONLY" -eq 1 ]]; then
  exit 0
fi

ACCOUNT_MONITOR_BIN="${EXCHANGE}_account_monitor"
LOCAL_RELATIVE=(
  "target/release/trade_signal"
  "target/release/${ACCOUNT_MONITOR_BIN}"
  "target/release/viz_server"
  "target/release/pre_trade"
  "target/release/trade_engine"
  "target/release/persist_manager"
  "scripts/fr_config_server.py"
  "scripts/print_fr_risk_params.py"
  "scripts/sync_fr_risk_params.py"
  "scripts/start_trade_engine.sh"
)
UPLOAD_NAMES=(
  "trade_signal"
  "$ACCOUNT_MONITOR_BIN"
  "viz_server"
  "pre_trade"
  "trade_engine"
  "persist_manager"
  "fr_config_server.py"
  "print_fr_risk_params.py"
  "sync_fr_risk_params.py"
  "start_trade_engine.sh"
)
LOCAL_PATHS=()
for relative_path in "${LOCAL_RELATIVE[@]}"; do
  local_path="${ROOT_DIR}/${relative_path}"
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
  "$target"/.publish-jp-meta-fr.*) ;;
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

REMOTE_STAGE="$("${SSH[@]}" "$SSH_HOST" "mktemp -d '$REMOTE_DIR/.publish-jp-meta-fr.XXXXXX'")"
case "$REMOTE_STAGE" in
  "$REMOTE_DIR"/.publish-jp-meta-fr.*) ;;
  *)
    echo "[ERROR] invalid remote staging path: $REMOTE_STAGE" >&2
    exit 1
    ;;
esac

echo "[INFO] SCP ${#LOCAL_PATHS[@]} files to $SSH_HOST:$REMOTE_STAGE"
"${SCP[@]}" "${LOCAL_PATHS[@]}" "${SSH_HOST}:${REMOTE_STAGE}/"
"${SCP[@]}" "$MANIFEST" "${SSH_HOST}:${REMOTE_STAGE}/SHA256SUMS"
"${SSH[@]}" "$SSH_HOST" "cd '$REMOTE_STAGE' && sha256sum -c SHA256SUMS"

echo "[INFO] rechecking target processes before replacement"
if check_remote_stopped; then
  :
else
  check_status=$?
  exit "$check_status"
fi

"${SSH[@]}" "$SSH_HOST" bash -s -- "$REMOTE_DIR" "$REMOTE_STAGE" "$ACCOUNT_MONITOR_BIN" <<'REMOTE_PUBLISH'
set -euo pipefail
target="$1"
stage="$2"
account_monitor_upload="$3"
case "$stage" in
  "$target"/.publish-jp-meta-fr.*) ;;
  *)
    echo "[ERROR] invalid staging path: $stage" >&2
    exit 1
    ;;
esac
[[ "$(readlink -f -- "$target")" == "$target" ]]
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
publish_file "$account_monitor_upload" account_monitor
publish_file viz_server viz_server
publish_file pre_trade pre_trade
publish_file trade_engine trade_engine
publish_file persist_manager persist_manager
publish_file fr_config_server.py scripts/fr_config_server.py
publish_file print_fr_risk_params.py scripts/print_fr_risk_params.py
publish_file sync_fr_risk_params.py scripts/sync_fr_risk_params.py
publish_file start_trade_engine.sh scripts/start_trade_engine.sh

rm -f "$stage/SHA256SUMS"
rmdir "$stage"
REMOTE_PUBLISH

REMOTE_STAGE=""
echo "[INFO] publish complete: $SSH_HOST:$REMOTE_DIR"
