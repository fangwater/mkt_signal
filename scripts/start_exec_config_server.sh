#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${BASE_DIR}/env.sh"
CFG_ENV_FILE="${BASE_DIR}/config/exec_config_server.env"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi
if [[ -f "$CFG_ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$CFG_ENV_FILE"
fi

APP_SCRIPT="${SCRIPT_DIR}/exec_config_server.py"
if [[ ! -f "$APP_SCRIPT" ]]; then
  echo "[ERROR] exec_config_server.py not found: $APP_SCRIPT" >&2
  exit 1
fi

dir_name="$(basename "$BASE_DIR")"
dir_tag="$(echo "${dir_name,,}" | sed 's/[^a-z0-9_-]/_/g')"
ENV_NAME="${ENV_NAME:-$dir_name}"
VENUE="${VENUE:-${EXEC_VENUE:-}}"
if [[ -z "$VENUE" ]]; then
  echo "[ERROR] VENUE or EXEC_VENUE is required" >&2
  exit 1
fi

BIND="${BIND:-127.0.0.1}"
PORT="${PORT:-18161}"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
APP_NAME="${PMDAEMON_NAME:-exec_cfg_${dir_tag}}"

if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x "${HOME}/.venvs/default/bin/python" ]]; then
    PYTHON_BIN="${HOME}/.venvs/default/bin/python"
  elif [[ -x "/home/ubuntu/jupyter_env/bin/python" ]]; then
    PYTHON_BIN="/home/ubuntu/jupyter_env/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
command -v "$PMDAEMON_BIN" >/dev/null 2>&1 || { echo "[ERROR] pmdaemon not found: ${PMDAEMON_BIN}" >&2; exit 1; }
"$PYTHON_BIN" -c "import redis" >/dev/null 2>&1 || {
  echo "[ERROR] Python redis package is missing for ${PYTHON_BIN}" >&2
  echo "[HINT] ${PYTHON_BIN} -m pip install redis" >&2
  exit 1
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file"' EXIT
json_name="$(json_escape "$APP_NAME")"
json_python="$(json_escape "$PYTHON_BIN")"
json_base="$(json_escape "$BASE_DIR")"
json_script="$(json_escape "$APP_SCRIPT")"
json_bind="$(json_escape "$BIND")"
json_port="$(json_escape "$PORT")"
json_redis="$(json_escape "$REDIS_URL")"
json_env_name="$(json_escape "$ENV_NAME")"
json_venue="$(json_escape "$VENUE")"
cat >"$cfg_file" <<JSON
{"apps":[{"name":"${json_name}","script":"${json_python}","args":["${json_script}","--bind","${json_bind}","--port","${json_port}","--redis-url","${json_redis}","--env-name","${json_env_name}","--venue","${json_venue}"],"cwd":"${json_base}"}]}
JSON

echo "[INFO] Starting exec_config_server env=${ENV_NAME} venue=${VENUE} port=${PORT}"
PMDAEMON_NAME="$APP_NAME" "${SCRIPT_DIR}/stop_exec_config_server.sh"
"$PMDAEMON_BIN" --config "$cfg_file" start --name "$APP_NAME"

echo "[INFO] Started: ${PMDAEMON_BIN} logs ${APP_NAME} --follow"
