#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="latency_csv_capture"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'USAGE'
Usage:
  scripts/deploy_latency_csv_capture.sh --config <config.toml> [--target-dir <path>] [--scripts-only|--bin-only]

Behavior:
  - Builds and packages the lightweight client-side latency CSV capture tool.
  - Runtime parameters come from the TOML config in the package.
  - This does not deploy or package latency_stable_monitor.
  - Default target dir:
      ./dist/latency_csv_capture_<instance>

Package contents:
  - latency_csv_capture
  - config/<config basename>.toml
  - scripts/start_latency_csv_capture.sh
  - scripts/stop_latency_csv_capture.sh
  - README.md

Examples:
  bash scripts/deploy_latency_csv_capture.sh --config config/latency_csv_capture_sg.toml
  bash scripts/deploy_latency_csv_capture.sh --config config/latency_csv_capture_sg.toml --target-dir /tmp/latency_csv_capture_sg
USAGE
}

create_zip_package() {
  local src_dir="$1"
  local zip_path="$2"
  local parent_dir base_name
  parent_dir="$(cd "$(dirname "$src_dir")" && pwd)"
  base_name="$(basename "$src_dir")"

  if command -v zip >/dev/null 2>&1; then
    (cd "$parent_dir" && zip -qr "$zip_path" "$base_name")
    return 0
  fi

  if command -v python3 >/dev/null 2>&1; then
    python3 - "$src_dir" "$zip_path" <<'PYZIP'
import sys
import zipfile
from pathlib import Path
src = Path(sys.argv[1]).resolve()
zip_path = Path(sys.argv[2]).resolve()
with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
    for path in src.rglob('*'):
        if path.is_file():
            zf.write(path, path.relative_to(src.parent))
PYZIP
    return 0
  fi

  echo "[ERROR] neither zip nor python3 found; cannot create $zip_path" >&2
  return 1
}

toml_get() {
  local file="$1"
  local key="$2"
  awk -v key="$key" '
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      line = $0
      sub(/^[^=]*=/, "", line)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
      if (line ~ /^".*"$/) {
        sub(/^"/, "", line)
        sub(/"$/, "", line)
      }
      print line
      exit
    }
  ' "$file"
}

CONFIG_PATH=""
TARGET_DIR=""
DO_BUILD=1
DO_SCRIPTS=1
ONLY_MODE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH="${2:-}"
      if [[ -z "$CONFIG_PATH" ]]; then
        echo "[ERROR] --config requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --config=*)
      CONFIG_PATH="${1#--config=}"
      shift
      ;;
    --target-dir)
      TARGET_DIR="${2:-}"
      if [[ -z "$TARGET_DIR" ]]; then
        echo "[ERROR] --target-dir requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --scripts-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only conflicts with --bin-only" >&2
        exit 1
      fi
      ONLY_MODE="scripts"
      DO_BUILD=0
      DO_SCRIPTS=1
      shift
      ;;
    --bin-only)
      if [[ -n "$ONLY_MODE" ]]; then
        echo "[ERROR] --scripts-only conflicts with --bin-only" >&2
        exit 1
      fi
      ONLY_MODE="bin"
      DO_BUILD=1
      DO_SCRIPTS=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unsupported argument: $1 (allowed: --config, --target-dir, --scripts-only, --bin-only)" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$CONFIG_PATH" ]]; then
  echo "[ERROR] config is required: --config <config.toml>" >&2
  usage >&2
  exit 1
fi
if [[ "$CONFIG_PATH" != /* ]]; then
  CONFIG_PATH="$ROOT_DIR/$CONFIG_PATH"
fi
CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"
if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "[ERROR] config not found: $CONFIG_PATH" >&2
  exit 1
fi

INSTANCE="$(toml_get "$CONFIG_PATH" instance)"
if [[ -z "$INSTANCE" ]]; then
  INSTANCE="$(basename "$CONFIG_PATH" .toml)"
fi
CONFIG_BASENAME="$(basename "$CONFIG_PATH")"
OUT_DIR_CFG="$(toml_get "$CONFIG_PATH" out_dir)"

if [[ -z "$TARGET_DIR" ]]; then
  TARGET_DIR="$ROOT_DIR/dist/latency_csv_capture_${INSTANCE}"
fi
ZIP_PATH="${TARGET_DIR%/}.zip"

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] build $BIN_NAME (release)"
  cargo build --release --bin "$BIN_NAME"
fi

mkdir -p "$TARGET_DIR"
rm -f "$ZIP_PATH"
if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] deploy $BIN_NAME to $TARGET_DIR"
  cp "$BIN_PATH" "$TARGET_DIR/"
  chmod +x "$TARGET_DIR/$BIN_NAME"
fi

mkdir -p "$TARGET_DIR/config"
cp "$CONFIG_PATH" "$TARGET_DIR/config/$CONFIG_BASENAME"

if [[ "$DO_SCRIPTS" -eq 1 ]]; then
  mkdir -p "$TARGET_DIR/scripts"
  for script in start_latency_csv_capture.sh stop_latency_csv_capture.sh; do
    cp "$ROOT_DIR/scripts/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  done
  cat >"$TARGET_DIR/README.md" <<'README'
# latency_csv_capture client package (__INSTANCE__)

This package subscribes to latency_stable_monitor ZMQ PUB and writes hourly CSV slices.
All runtime parameters are in the TOML config:

- `config/__CONFIG_BASENAME__`

Start:

```bash
./scripts/start_latency_csv_capture.sh --config config/__CONFIG_BASENAME__
```

Stop:

```bash
./scripts/stop_latency_csv_capture.sh --config config/__CONFIG_BASENAME__
```

Important config keys:

- `ip` / `port`: ZMQ publisher address, unless `endpoint` is set.
- `out_dir`: root directory for CSV files.
- `pm2_name`: PM2 process name, e.g. `latency_csv_capture-__INSTANCE__`.
- `include_spread_net`: false means spread_pbs writes only spread_e2e rows.

CSV files are written as `<out_dir>/<topic>/<YYYYMMDD_HH>.csv`.
README
  sed -i \
    -e "s/__INSTANCE__/${INSTANCE}/g" \
    -e "s/__CONFIG_BASENAME__/${CONFIG_BASENAME}/g" \
    "$TARGET_DIR/README.md"
fi

if [[ -n "$OUT_DIR_CFG" ]]; then
  if [[ "$OUT_DIR_CFG" == /* ]]; then
    mkdir -p "$TARGET_DIR/data"
  else
    mkdir -p "$TARGET_DIR/$OUT_DIR_CFG"
  fi
fi

create_zip_package "$TARGET_DIR" "$ZIP_PATH"

echo "[INFO] latency CSV capture package ready: $TARGET_DIR"
echo "[INFO] zip:   $ZIP_PATH"
echo "[INFO] start: cd $TARGET_DIR && ./scripts/start_latency_csv_capture.sh --config config/$CONFIG_BASENAME"
echo "[INFO] stop:  cd $TARGET_DIR && ./scripts/stop_latency_csv_capture.sh --config config/$CONFIG_BASENAME"
