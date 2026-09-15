#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENUE_DIR_REGEX='^([a-z0-9]+-(futures|margin|spot|swap|perp|perpetual)|(binance|bitget)-coin-futures)$'

usage() {
  cat <<'USAGE'
Usage:
  start_intra_factor_model_1m_pub.sh

Starts one venue-local intra_factor_model_1m_pub under pmdaemon.
The current directory must be a deployed venue directory, for example:
  ~/intra_factor_model_1m/binance-futures
USAGE
}

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unsupported arguments: $*" >&2
      usage >&2
      exit 1
      ;;
  esac
fi

venue="$(basename "${BASE_DIR}" | tr '[:upper:]' '[:lower:]')"
if [[ ! "$venue" =~ $VENUE_DIR_REGEX ]]; then
  echo "[ERROR] cannot infer venue from deployment directory: ${BASE_DIR}" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  exit 1
fi

BIN_PATH="${BASE_DIR}/intra_factor_model_1m_pub"
CONFIG_PATH="${BASE_DIR}/config/intra_factor_model_1m_pub.toml"
if [[ ! -x "$BIN_PATH" ]]; then
  echo "[ERROR] binary not executable: $BIN_PATH" >&2
  exit 1
fi
if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "[ERROR] config not found: $CONFIG_PATH" >&2
  exit 1
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

extra_args_json=""
if [[ -n "${INTRA_FACTOR_MODEL_1M_CORE:-}" ]]; then
  if [[ ! "$INTRA_FACTOR_MODEL_1M_CORE" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] INTRA_FACTOR_MODEL_1M_CORE must be an integer" >&2
    exit 1
  fi
  extra_args_json=", \"--core\", \"${INTRA_FACTOR_MODEL_1M_CORE}\""
fi

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

name="intra_factor_model_1m_pub_${venue}"
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "$(json_escape "$name")",
      "script": "$(json_escape "$BIN_PATH")",
      "args": ["--venue", "$(json_escape "$venue")", "--config", "config/intra_factor_model_1m_pub.toml"${extra_args_json}],
      "cwd": "$(json_escape "$BASE_DIR")",
      "env": {"RUST_LOG": "$(json_escape "${RUST_LOG:-info}")"}
    }
  ]
}
JSON

"${SCRIPT_DIR}/stop_intra_factor_model_1m_pub.sh"
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$name"

echo "[INFO] started ${name} in ${BASE_DIR}"
