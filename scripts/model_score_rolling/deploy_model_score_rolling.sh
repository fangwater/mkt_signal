#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_NAME="model_score_rolling"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'USAGE'
Usage:
  scripts/model_score_rolling/deploy_model_score_rolling.sh --target <model_name> [--dir <path>]

Behavior:
  - Build model_score_rolling
  - Deploy to $HOME/model_score_rolling/<model_name> (or --dir)
  - Copy management scripts

Examples:
  scripts/model_score_rolling/deploy_model_score_rolling.sh --target binance-futures-mm-xgb-test
  scripts/model_score_rolling/deploy_model_score_rolling.sh --target gate-futures-xxx --dir "$HOME/model_score_rolling/gate-futures-xxx"
USAGE
}

MODEL_NAME=""
TARGET_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      MODEL_NAME="${2:-}"
      shift 2
      ;;
    --dir)
      TARGET_DIR="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$MODEL_NAME" ]]; then
  echo "[ERROR] --target <model_name> is required" >&2
  usage >&2
  exit 1
fi

if [[ -z "$TARGET_DIR" ]]; then
  TARGET_DIR="$HOME/model_score_rolling/${MODEL_NAME}"
fi

echo "[INFO] build ${BIN_NAME} (release)"
cargo build --release --bin "$BIN_NAME"

echo "[INFO] deploy ${BIN_NAME} to ${TARGET_DIR}"
mkdir -p "$TARGET_DIR"
mkdir -p "$TARGET_DIR/scripts/model_score_rolling"

# atomic binary replacement
TMP_BIN="$TARGET_DIR/${BIN_NAME}.new"
cp "$BIN_PATH" "$TMP_BIN"
chmod +x "$TMP_BIN"
mv -f "$TMP_BIN" "$TARGET_DIR/$BIN_NAME"

SYNC_FILES=(
  "scripts/model_score_rolling/deploy_model_score_rolling.sh"
  "scripts/model_score_rolling/start_model_score_rolling.sh"
  "scripts/model_score_rolling/stop_model_score_rolling.sh"
  "scripts/model_score_rolling/print_model_score_rolling_params.py"
  "scripts/model_score_rolling/sync_model_score_rolling_params.py"
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

echo "[INFO] deploy done"
echo "[INFO] target_dir: $TARGET_DIR"
echo "[INFO] model_name: $MODEL_NAME"
echo "[INFO] start: cd $TARGET_DIR && ./scripts/model_score_rolling/start_model_score_rolling.sh"
echo "[INFO] stop:  cd $TARGET_DIR && ./scripts/model_score_rolling/stop_model_score_rolling.sh"
