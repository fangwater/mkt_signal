#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_NAME="rolling_metrics"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"
DEPLOY_ROOT_NAME="rolling_metrics"

usage() {
  cat <<'EOF'
Usage:
  scripts/rolling_metrics/deploy_rolling_metrics.sh --open-venue <venue> --hedge-venue <venue> [--dir <path>]

Description:
  - Build rolling_metrics and deploy to:
      $HOME/rolling_metrics/<open-venue>-<hedge-venue>
    (or to --dir if specified)
  - Pair placement follows the public market-data topology:
      local HK : okex/bybit pairs, plus pairs with binance-futures mirrored to HK
      remote JP: binance/bitget/gate pairs
  - Process management uses pmdaemon via:
      ./scripts/rolling_metrics/start_rolling_metrics.sh
      ./scripts/rolling_metrics/stop_rolling_metrics.sh
  - Unified scripts layout: deploy dir contains `scripts/` only.
  - Remote mode still builds locally, stages under $HOME/rolling_metrics, then rsyncs
    to ${FR_DEPLOY_HOST:-ubuntu@54.64.147.69}:${FR_REMOTE_HOME:-/home/ubuntu}/rolling_metrics.

Examples:
  scripts/rolling_metrics/deploy_rolling_metrics.sh --open-venue binance-margin --hedge-venue binance-futures
  scripts/rolling_metrics/deploy_rolling_metrics.sh --open-venue okex-futures --hedge-venue binance-futures
  scripts/rolling_metrics/deploy_rolling_metrics.sh --open-venue okex-futures --hedge-venue bybit-futures
  scripts/rolling_metrics/deploy_rolling_metrics.sh --open-venue bitget-futures --hedge-venue gate-futures
  scripts/rolling_metrics/deploy_rolling_metrics.sh --open-venue okex-futures --hedge-venue binance-futures --dir "$HOME/rolling_metrics/okex-futures-binance-futures"
EOF
}

normalize_venue() {
  local v="${1,,}"
  if [[ "$v" == okx-* ]]; then
    v="okex-${v#okx-}"
  fi
  echo "$v"
}

validate_venue() {
  local v="${1,,}"
  if [[ ! "$v" =~ ^[a-z0-9]+-(margin|futures|spot|swap|perp|perpetual)$ ]]; then
    echo "[ERROR] invalid venue: $1 (expect <exchange>-<margin|futures|spot|swap>)" >&2
    exit 1
  fi
}

exchange_of_venue() {
  echo "${1%%-*}"
}

is_hk_available_venue() {
  local venue="${1,,}"
  local exchange
  exchange="$(exchange_of_venue "$venue")"
  case "$exchange" in
    okex|bybit)
      return 0
      ;;
    binance)
      if [[ "$venue" == "binance-futures" ]]; then
        return 0
      fi
      ;;
  esac
  return 1
}

is_jp_available_venue() {
  local exchange
  exchange="$(exchange_of_venue "${1,,}")"
  case "$exchange" in
    binance|bitget|gate)
      return 0
      ;;
  esac
  return 1
}

deploy_location_for_pair() {
  local open_venue="$1"
  local hedge_venue="$2"
  if is_jp_available_venue "$open_venue" && is_jp_available_venue "$hedge_venue"; then
    echo "remote"
    return 0
  fi
  if is_hk_available_venue "$open_venue" && is_hk_available_venue "$hedge_venue"; then
    echo "local"
    return 0
  fi
  echo ""
  return 1
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

OPEN_VENUE=""
HEDGE_VENUE=""
TARGET_DIR=""
TARGET_DIR_OVERRIDE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --open-venue)
      OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge-venue)
      HEDGE_VENUE="${2:-}"
      shift 2
      ;;
    --dir)
      TARGET_DIR="${2:-}"
      TARGET_DIR_OVERRIDE=1
      shift 2
      ;;
    *)
      echo "[ERROR] unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] --open-venue and --hedge-venue are required" >&2
  usage >&2
  exit 1
fi

OPEN_VENUE="$(normalize_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(normalize_venue "$HEDGE_VENUE")"
validate_venue "$OPEN_VENUE"
validate_venue "$HEDGE_VENUE"

if [[ -z "$TARGET_DIR" ]]; then
  TARGET_DIR="$HOME/$DEPLOY_ROOT_NAME/${OPEN_VENUE}-${HEDGE_VENUE}"
fi

DEPLOY_LOCATION="$(deploy_location_for_pair "$OPEN_VENUE" "$HEDGE_VENUE" || true)"
if [[ -z "$DEPLOY_LOCATION" ]]; then
  echo "[ERROR] cannot choose rolling_metrics deploy host for open=${OPEN_VENUE} hedge=${HEDGE_VENUE}" >&2
  echo "[ERROR] available local HK venues: okex-*, bybit-*, binance-futures" >&2
  echo "[ERROR] available remote JP venues: binance-*, bitget-*, gate-*" >&2
  exit 1
fi

if [[ "$DEPLOY_LOCATION" == "remote" && "$TARGET_DIR_OVERRIDE" == "1" ]]; then
  echo "[ERROR] --dir override is not compatible with remote rolling_metrics deploy" >&2
  echo "[ERROR] remote rsync staging is fixed at \$HOME/$DEPLOY_ROOT_NAME/${OPEN_VENUE}-${HEDGE_VENUE}" >&2
  exit 1
fi

echo "[INFO] build ${BIN_NAME} (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] deploy ${BIN_NAME} to ${TARGET_DIR}"
mkdir -p "$TARGET_DIR"
mkdir -p "$TARGET_DIR/scripts"

SYNC_FILES=(
  "scripts/process_match_lib.sh"
  "scripts/rolling_metrics/deploy_rolling_metrics.sh"
  "scripts/rolling_metrics/start_rolling_metrics.sh"
  "scripts/rolling_metrics/stop_rolling_metrics.sh"
  "scripts/rolling_metrics/print_rolling_metrics_thresholds.py"
  "scripts/rolling_metrics/print_rolling_metrics_params.py"
  "scripts/rolling_metrics/sync_rolling_metrics_params.py"
  "docs/rolling_metrics_migration.md"
)

for file in "${SYNC_FILES[@]}"; do
  src="$ROOT_DIR/$file"
  if [[ ! -f "$src" ]]; then
    continue
  fi
  dest_dir="$TARGET_DIR/$(dirname "$file")"
  mkdir -p "$dest_dir"
  rsync -a "$src" "$dest_dir/"
  chmod +x "$dest_dir/$(basename "$file")" 2>/dev/null || true
done

# Atomic binary replacement to avoid Text file busy.
tmp_bin="$TARGET_DIR/${BIN_NAME}.new"
cp "$BIN_PATH" "$tmp_bin"
chmod +x "$tmp_bin"
mv -f "$tmp_bin" "$TARGET_DIR/$BIN_NAME"

if [[ "$DEPLOY_LOCATION" == "remote" ]]; then
  # shellcheck source=../lib/fr_remote_deploy.sh
  source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"
  fr_remote_init_ssh "$ROOT_DIR"
  fr_remote_sync_path "$DEPLOY_ROOT_NAME/${OPEN_VENUE}-${HEDGE_VENUE}"
fi

echo "[INFO] deploy finished: $TARGET_DIR"
echo "[INFO] venues: open=${OPEN_VENUE} hedge=${HEDGE_VENUE}"
if [[ "$DEPLOY_LOCATION" == "local" ]]; then
  echo "[INFO] location: local"
  echo "[INFO] start: cd $TARGET_DIR && ./scripts/rolling_metrics/start_rolling_metrics.sh"
  echo "[INFO] stop:  cd $TARGET_DIR && ./scripts/rolling_metrics/stop_rolling_metrics.sh"
  echo "[INFO] logs:  pmdaemon logs rm_<open_tag>_<hedge_tag> --follow"
else
  echo "[INFO] location: remote ${FR_DEPLOY_HOST}:${FR_REMOTE_HOME}/$DEPLOY_ROOT_NAME/${OPEN_VENUE}-${HEDGE_VENUE}"
  echo "[INFO] start: ssh ${FR_DEPLOY_HOST} 'cd ${FR_REMOTE_HOME}/$DEPLOY_ROOT_NAME/${OPEN_VENUE}-${HEDGE_VENUE} && ./scripts/rolling_metrics/start_rolling_metrics.sh'"
  echo "[INFO] stop:  ssh ${FR_DEPLOY_HOST} 'cd ${FR_REMOTE_HOME}/$DEPLOY_ROOT_NAME/${OPEN_VENUE}-${HEDGE_VENUE} && ./scripts/rolling_metrics/stop_rolling_metrics.sh'"
  echo "[INFO] logs:  ssh ${FR_DEPLOY_HOST} 'pmdaemon logs rm_<open_tag>_<hedge_tag> --follow'"
fi
