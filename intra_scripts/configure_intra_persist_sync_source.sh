#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  intra_scripts/configure_intra_persist_sync_source.sh [options]

Options:
  --source-id <id>              Stable source id. Default: current env dir name.
  --bind <host:port>            persist_manager gRPC bind. Default: 127.0.0.1:50051.
  --listen <port|ip:port>       Public nginx stream listen. Default: derived from env suffix.
  --mapping-file <path>         nginx stream mapping file. Default: $HOME/nginx_streams.txt.
  --env-file <path>             env file to update. Default: <env dir>/env.sh.
  --no-env                      Only update nginx stream mapping.
  --no-nginx                    Only update env file.

This script is intentionally non-sudo. It enables persist_manager's gRPC sync
source through env.sh and writes an nginx stream mapping. Run the printed sudo
apply command separately.
EOF
}

SOURCE_ID=""
BIND="127.0.0.1:50051"
LISTEN=""
MAPPING_FILE="${HOME}/nginx_streams.txt"
ENV_FILE="${BASE_DIR}/env.sh"
UPDATE_ENV="1"
UPDATE_NGINX="1"

shell_quote() {
  printf '%q' "$1"
}

normalize_env_name() {
  basename "$BASE_DIR" | tr 'A-Z' 'a-z'
}

default_listen_for_env() {
  local env_name suffix
  env_name="$(normalize_env_name)"
  if [[ "$env_name" =~ ^[a-z0-9]+[-_]intra[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
    suffix="${BASH_REMATCH[1]//_/-}"
  else
    suffix="arb01"
  fi
  case "$suffix" in
    trade) echo "6350" ;;
    arb01) echo "6351" ;;
    arb02) echo "6352" ;;
    arb03) echo "6353" ;;
    *) echo "6351" ;;
  esac
}

validate_addr() {
  local name="$1" value="$2"
  if [[ ! "$value" =~ ^([^:[:space:]]+|\[[0-9a-fA-F:]+\]):[0-9]+$ ]]; then
    echo "[ERROR] ${name} must be host:port, got: ${value}" >&2
    exit 1
  fi
}

validate_listen() {
  local value="$1"
  if [[ "$value" =~ ^[0-9]+$ ]]; then
    return 0
  fi
  if [[ "$value" =~ ^([^:[:space:]]+|\[[0-9a-fA-F:]+\]):[0-9]+$ ]]; then
    return 0
  fi
  echo "[ERROR] --listen must be port or host:port, got: ${value}" >&2
  exit 1
}

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
    cat >"$MAPPING_FILE" <<'EOF'
# Each line: <listen> <upstream>. For nginx stream (TCP) proxying.
# gRPC clients should use http://<host>:<listen> for plaintext tonic endpoints.
EOF
  fi
  if grep -Fqx "$begin" "$MAPPING_FILE" && ! grep -Fqx "$end" "$MAPPING_FILE"; then
    echo "[ERROR] mapping file has begin marker but missing end marker: $MAPPING_FILE" >&2
    exit 1
  fi
  tmp="$(mktemp)"
  awk -v begin="$begin" \
      -v end="$end" \
      -v listen="$LISTEN" \
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

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-id) SOURCE_ID="${2:-}"; shift 2 ;;
    --bind) BIND="${2:-}"; shift 2 ;;
    --listen) LISTEN="${2:-}"; shift 2 ;;
    --mapping-file) MAPPING_FILE="${2:-}"; shift 2 ;;
    --env-file) ENV_FILE="${2:-}"; shift 2 ;;
    --no-env) UPDATE_ENV="0"; shift ;;
    --no-nginx) UPDATE_NGINX="0"; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "[ERROR] Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$SOURCE_ID" ]]; then
  SOURCE_ID="$(normalize_env_name)"
fi
if [[ -z "$LISTEN" ]]; then
  LISTEN="$(default_listen_for_env)"
fi

if [[ "$UPDATE_ENV" != "1" && "$UPDATE_NGINX" != "1" ]]; then
  echo "[ERROR] nothing to do: both --no-env and --no-nginx were set" >&2
  exit 1
fi

validate_addr "--bind" "$BIND"
validate_listen "$LISTEN"

if [[ "$UPDATE_ENV" == "1" ]]; then
  upsert_env_block
  echo "[INFO] env updated: $ENV_FILE"
  echo "[INFO] PERSIST_SYNC_SOURCE_ID=$SOURCE_ID"
  echo "[INFO] PERSIST_SYNC_BIND=$BIND"
fi

if [[ "$UPDATE_NGINX" == "1" ]]; then
  upsert_stream_mapping
  echo "[INFO] nginx stream mapping updated: $MAPPING_FILE"
  echo "[INFO] collector endpoint: http://<host>:${LISTEN##*:}"
fi

setup_script="${BASE_DIR}/scripts/setup_nginx_stream_4190.sh"
if [[ "$UPDATE_NGINX" == "1" ]]; then
  if [[ -f "$setup_script" ]]; then
    echo "[INFO] To apply nginx stream config:"
    echo "       cd $(shell_quote "$BASE_DIR") && MAPPING_FILE=$(shell_quote "$MAPPING_FILE") bash ./scripts/setup_nginx_stream_4190.sh"
  else
    echo "[WARN] setup script missing in deploy dir: $setup_script"
    echo "[WARN] Sync scripts first, or run from repo root:"
    echo "       MAPPING_FILE=$(shell_quote "$MAPPING_FILE") bash scripts/setup_nginx_stream_4190.sh"
  fi
fi
