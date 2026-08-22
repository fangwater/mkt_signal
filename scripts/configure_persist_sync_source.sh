#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  scripts/configure_persist_sync_source.sh [options]

Options:
  --env-name <name>             Env name to resolve. Default: current env dir name.
  --table <path>                Distribution table. Default: config/persist_sync_distribution.toml.
  --mapping-file <path>         Nginx stream mapping file. Default: $HOME/nginx_streams.txt.
  --env-file <path>             Env file to update. Default: <env dir>/env.sh.
  --no-env                      Only update nginx stream mapping.
  --no-nginx                    Only update env file.

The env must be explicitly present in config/persist_sync_distribution.toml.
Unconfigured envs fail closed; this script never infers a port from suffixes.
USAGE
}

ENV_NAME=""
TABLE_FILE="${PERSIST_SYNC_DISTRIBUTION_TABLE:-}"
MAPPING_FILE="${HOME}/nginx_streams.txt"
ENV_FILE="${BASE_DIR}/env.sh"
UPDATE_ENV="1"
UPDATE_NGINX="1"

shell_quote() { printf '%q' "$1"; }

normalize_env_name() {
  basename "$BASE_DIR" | tr 'A-Z' 'a-z'
}

find_table_file() {
  if [[ -n "$TABLE_FILE" ]]; then
    echo "$TABLE_FILE"
    return
  fi
  local candidates=(
    "${BASE_DIR}/config/persist_sync_distribution.toml"
    "${BASE_DIR}/../config/persist_sync_distribution.toml"
    "${SCRIPT_DIR}/../config/persist_sync_distribution.toml"
    "$(pwd)/config/persist_sync_distribution.toml"
  )
  local cand
  for cand in "${candidates[@]}"; do
    if [[ -f "$cand" ]]; then
      echo "$cand"
      return
    fi
  done
  echo "${BASE_DIR}/config/persist_sync_distribution.toml"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-name) ENV_NAME="${2:-}"; shift 2 ;;
    --table) TABLE_FILE="${2:-}"; shift 2 ;;
    --mapping-file) MAPPING_FILE="${2:-}"; shift 2 ;;
    --env-file) ENV_FILE="${2:-}"; shift 2 ;;
    --no-env) UPDATE_ENV="0"; shift ;;
    --no-nginx) UPDATE_NGINX="0"; shift ;;
    --source-id|--bind|--listen)
      echo "[ERROR] $1 is no longer accepted; configure the source in config/persist_sync_distribution.toml" >&2
      exit 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[ERROR] Unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="$(normalize_env_name)"
fi
ENV_NAME="$(printf '%s' "$ENV_NAME" | tr 'A-Z' 'a-z')"
TABLE_FILE="$(find_table_file)"

if [[ "$UPDATE_ENV" != "1" && "$UPDATE_NGINX" != "1" ]]; then
  echo "[ERROR] nothing to do: both --no-env and --no-nginx were set" >&2
  exit 1
fi
if [[ ! -f "$TABLE_FILE" ]]; then
  echo "[ERROR] persist sync distribution table not found: $TABLE_FILE" >&2
  exit 1
fi

pybin="${PYTHON_BIN:-python3}"
lookup_output="$("$pybin" - "$TABLE_FILE" "$ENV_NAME" <<'PY'
import shlex
import sys
from pathlib import Path
from urllib.parse import urlparse
try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib
    except ModuleNotFoundError:
        print('[ERROR] Python tomllib/tomli is required to read persist_sync_distribution.toml', file=sys.stderr)
        raise SystemExit(1)

table_path = Path(sys.argv[1])
env_name = sys.argv[2]
data = tomllib.loads(table_path.read_text())
for row in data.get('allocations', []):
    if str(row.get('env', '')).lower() == env_name or str(row.get('source_id', '')).lower() == env_name:
        public_url = str(row.get('public_url', ''))
        public_host = urlparse(public_url).hostname or ''
        port = str(row['port'])
        listen = f'{public_host}:{port}' if public_host in {'127.0.0.1', 'localhost'} else port
        fields = {
            'SOURCE_ID': row['source_id'],
            'BIND': row['bind'],
            'LISTEN': listen,
            'LISTEN_PORT': port,
            'PUBLIC_URL': public_url,
            'REGION': row.get('region', ''),
            'STRATEGY': row.get('strategy', ''),
            'STATUS': row.get('status', ''),
        }
        for key, value in fields.items():
            print(f'{key}={shlex.quote(str(value))}')
        raise SystemExit(0)
print(f'[ERROR] env is not allocated in {table_path}: {env_name}', file=sys.stderr)
raise SystemExit(2)
PY
)"
eval "$lookup_output"

upsert_env_block() {
  local tmp begin end
  begin="# BEGIN managed: persist sync source"
  end="# END managed: persist sync source"
  mkdir -p "$(dirname "$ENV_FILE")"
  if [[ ! -f "$ENV_FILE" ]]; then
    : > "$ENV_FILE"
    chmod 600 "$ENV_FILE" 2>/dev/null || true
  fi
  if grep -Fqx "$begin" "$ENV_FILE" && ! grep -Fqx "$end" "$ENV_FILE"; then
    echo "[ERROR] env file has begin marker but missing end marker: $ENV_FILE" >&2
    exit 1
  fi
  tmp="$(mktemp)"
  awk -v begin="$begin" \
      -v end="$end" \
      -v source_id="$SOURCE_ID" \
      -v bind="$BIND" '
    function emit() {
      print begin
      print "export PERSIST_SYNC_SOURCE_ID=\"" source_id "\""
      print "export PERSIST_SYNC_BIND=\"" bind "\""
      print end
    }
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end { in_block = 0; emit(); next }
    in_block { next }
    { print }
    END { if (!replaced) { print ""; emit() } }
  ' "$ENV_FILE" > "$tmp"
  mv "$tmp" "$ENV_FILE"
  chmod 600 "$ENV_FILE" 2>/dev/null || true
}

upsert_stream_mapping() {
  local tmp begin end
  begin="# BEGIN managed: persist sync source ${SOURCE_ID}"
  end="# END managed: persist sync source ${SOURCE_ID}"
  mkdir -p "$(dirname "$MAPPING_FILE")"
  if [[ ! -f "$MAPPING_FILE" ]]; then
    cat >"$MAPPING_FILE" <<'MAP'
# Each line: <listen> <upstream>. For nginx stream (TCP) proxying.
# gRPC clients should use http://<host>:<listen> for plaintext tonic endpoints.
MAP
  fi
  if grep -Fqx "$begin" "$MAPPING_FILE" && ! grep -Fqx "$end" "$MAPPING_FILE"; then
    echo "[ERROR] mapping file has begin marker but missing end marker: $MAPPING_FILE" >&2
    exit 1
  fi
  tmp="$(mktemp)"
  awk -v begin="$begin" \
      -v end="$end" \
      -v listen="$LISTEN" \
      -v listen_port="$LISTEN_PORT" \
      -v upstream="$BIND" '
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end {
      in_block = 0
      print begin
      print "# persist_manager gRPC sync source"
      print listen " " upstream
      print end
      next
    }
    in_block { next }
    {
      first = $1
      if (first == listen) { next }
      if (first == listen_port) { next }
      if (first == "0.0.0.0:" listen_port) { next }
      if (first == "127.0.0.1:" listen_port) { next }
      if (first == "localhost:" listen_port) { next }
      print
    }
    END {
      if (!replaced) {
        print ""
        print begin
        print "# persist_manager gRPC sync source"
        print listen " " upstream
        print end
      }
    }
  ' "$MAPPING_FILE" > "$tmp"
  mv "$tmp" "$MAPPING_FILE"
}

if [[ "$UPDATE_ENV" == "1" ]]; then
  upsert_env_block
  echo "[INFO] env updated: $ENV_FILE"
  echo "[INFO] PERSIST_SYNC_SOURCE_ID=$SOURCE_ID"
  echo "[INFO] PERSIST_SYNC_BIND=$BIND"
fi

if [[ "$UPDATE_NGINX" == "1" ]]; then
  upsert_stream_mapping
  echo "[INFO] nginx stream mapping updated: $MAPPING_FILE"
  echo "[INFO] collector endpoint: ${PUBLIC_URL:-http://<host>:${LISTEN}}"
fi

setup_script="${BASE_DIR}/scripts/setup_nginx_stream_4190.sh"
if [[ "$UPDATE_NGINX" == "1" ]]; then
  if [[ -f "$setup_script" ]]; then
    echo "[INFO] To apply nginx stream config:"
    echo "       cd $(shell_quote "$BASE_DIR") && MAPPING_FILE=$(shell_quote "$MAPPING_FILE") bash ./scripts/setup_nginx_stream_4190.sh"
  else
    echo "[WARN] setup script missing in deploy dir: $setup_script"
  fi
fi
