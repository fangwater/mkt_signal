#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="rolling_metrics"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'EOF'
Usage:
  scripts/deploy_rolling_metrics.sh --open-venue <venue> --hedge-venue <venue> [--dir <path>]

Description:
  - Build rolling_metrics and deploy to:
      $HOME/rolling_metrics/<open-venue>-<hedge-venue>
    (or to --dir if specified)
  - Process management uses pmdaemon via:
      ./scripts/start_rolling_metrics.sh
      ./scripts/stop_rolling_metrics.sh
  - Unified scripts layout: deploy dir contains `scripts/` only
    (no split between xarb_scripts/fr scripts for rolling_metrics).

Examples:
  scripts/deploy_rolling_metrics.sh --open-venue binance-margin --hedge-venue binance-futures
  scripts/deploy_rolling_metrics.sh --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_rolling_metrics.sh --open-venue okex-futures --hedge-venue binance-futures --dir "$HOME/rolling_metrics/okex-futures-binance-futures"
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

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

OPEN_VENUE=""
HEDGE_VENUE=""
TARGET_DIR=""

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
  TARGET_DIR="$HOME/rolling_metrics/${OPEN_VENUE}-${HEDGE_VENUE}"
fi

echo "[INFO] build ${BIN_NAME} (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] deploy ${BIN_NAME} to ${TARGET_DIR}"
mkdir -p "$TARGET_DIR"
mkdir -p "$TARGET_DIR/scripts"

SYNC_FILES=(
  "scripts/deploy_rolling_metrics.sh"
  "scripts/start_rolling_metrics.sh"
  "scripts/stop_rolling_metrics.sh"
  "scripts/print_fr_rolling_metrics_thresholds.py"
  "scripts/print_fr_rolling_metrics_params.py"
  "scripts/sync_fr_rolling_metrics_params.py"
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

echo "[INFO] deploy finished: $TARGET_DIR"
echo "[INFO] venues: open=${OPEN_VENUE} hedge=${HEDGE_VENUE}"
echo "[INFO] start: cd $TARGET_DIR && ./scripts/start_rolling_metrics.sh"
echo "[INFO] stop:  cd $TARGET_DIR && ./scripts/stop_rolling_metrics.sh"
echo "[INFO] logs:  pmdaemon logs rolling_metrics_<open>_<hedge> --follow"
