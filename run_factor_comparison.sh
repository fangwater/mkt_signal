#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_OUTPUT="/tmp/factor_test_output.json"

OUTPUT_PATH="$DEFAULT_OUTPUT"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CARGO_BIN="${CARGO_BIN:-cargo}"
RUST_PROFILE="${RUST_PROFILE:-}"
SKIP_RUST=0
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage:
  ./run_factor_comparison.sh [options]

Options:
  --output <path>       Rust factor_test output json path (default: /tmp/factor_test_output.json)
  --python <bin>        Python interpreter (default: $PYTHON_BIN or python3)
  --cargo <bin>         Cargo binary (default: $CARGO_BIN or cargo)
  --profile <name>      Cargo profile passed to `cargo run --profile`
  --skip-rust           Skip Rust generation and only run Python compare
  --dry-run             Print commands only, do not execute
  -h, --help            Show this help

Notes:
  - compare_factor_test.py reads /tmp/factor_test_output.json by default.
  - If --output is not /tmp/factor_test_output.json, this script will copy it to that path
    before running compare_factor_test.py.
EOF
}

log() {
  printf '[factor-compare] %s\n' "$*" >&2
}

run_cmd() {
  log "+ $*"
  if [[ "$DRY_RUN" -eq 0 ]]; then
    "$@"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)
      [[ $# -ge 2 ]] || { echo "missing value for --output" >&2; exit 2; }
      OUTPUT_PATH="$2"
      shift 2
      ;;
    --python)
      [[ $# -ge 2 ]] || { echo "missing value for --python" >&2; exit 2; }
      PYTHON_BIN="$2"
      shift 2
      ;;
    --cargo)
      [[ $# -ge 2 ]] || { echo "missing value for --cargo" >&2; exit 2; }
      CARGO_BIN="$2"
      shift 2
      ;;
    --profile)
      [[ $# -ge 2 ]] || { echo "missing value for --profile" >&2; exit 2; }
      RUST_PROFILE="$2"
      shift 2
      ;;
    --skip-rust)
      SKIP_RUST=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

cd "$ROOT_DIR"

if [[ ! -f "compare_factor_test.py" ]]; then
  echo "compare_factor_test.py not found in $ROOT_DIR" >&2
  exit 1
fi

if [[ "$SKIP_RUST" -eq 0 ]]; then
  log "step 1/2: generate Rust factor output"
  if [[ -n "$RUST_PROFILE" ]]; then
    run_cmd "$CARGO_BIN" run --profile "$RUST_PROFILE" --bin factor_test -- --output "$OUTPUT_PATH"
  else
    run_cmd "$CARGO_BIN" run --bin factor_test -- --output "$OUTPUT_PATH"
  fi
else
  log "step 1/2: skip Rust generation (--skip-rust)"
fi

if [[ "$DRY_RUN" -eq 0 && ! -f "$OUTPUT_PATH" ]]; then
  echo "factor output json not found: $OUTPUT_PATH" >&2
  exit 1
fi

if [[ "$OUTPUT_PATH" != "$DEFAULT_OUTPUT" ]]; then
  log "copy output to default compare path: $DEFAULT_OUTPUT"
  run_cmd cp -f "$OUTPUT_PATH" "$DEFAULT_OUTPUT"
fi

log "step 2/2: run Python comparison via compare_factor_test.py"
run_cmd "$PYTHON_BIN" compare_factor_test.py

log "done: output=$OUTPUT_PATH compare_input=$DEFAULT_OUTPUT"
