#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage: setup_predict_file_nginx.sh --config <config.toml>
                                  [--nginx-prefix /predict/<instance>]
                                  [--nginx-port 4191]
                                  [--nginx-mapping-file $HOME/nginx_locations.txt]
                                  [--apply-nginx]

Writes a managed Nginx reverse-proxy block for the predict_file dashboard.
--apply-nginx validates and reloads the Nginx proxy on the selected port.
USAGE
}

CONFIG_PATH=""
NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE="${HOME}/nginx_locations.txt"
APPLY_NGINX=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) CONFIG_PATH="${2:-}"; shift 2 ;;
    --config=*) CONFIG_PATH="${1#--config=}"; shift ;;
    --nginx-prefix) NGINX_PREFIX="${2:-}"; shift 2 ;;
    --nginx-prefix=*) NGINX_PREFIX="${1#--nginx-prefix=}"; shift ;;
    --nginx-port) NGINX_PORT="${2:-}"; shift 2 ;;
    --nginx-mapping-file) NGINX_MAPPING_FILE="${2:-}"; shift 2 ;;
    --apply-nginx) APPLY_NGINX=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[ERROR] unsupported argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

[[ -n "$CONFIG_PATH" ]] || { echo "[ERROR] --config is required" >&2; exit 1; }
[[ "$CONFIG_PATH" = /* ]] || CONFIG_PATH="${ROOT_DIR}/${CONFIG_PATH}"
CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"
[[ -f "$CONFIG_PATH" ]] || { echo "[ERROR] config not found: $CONFIG_PATH" >&2; exit 1; }

toml_get() {
  awk -v key="$1" '
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      line = $0; sub(/^[^=]*=/, "", line); gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
      if (line ~ /^".*"$/) { sub(/^"/, "", line); sub(/"$/, "", line) }
      print line; exit
    }' "$CONFIG_PATH"
}

INSTANCE="$(toml_get instance)"
[[ -n "$INSTANCE" ]] || INSTANCE="$(basename "$CONFIG_PATH" .toml)"
HTTP_PORT="$(toml_get http_port)"
[[ "$HTTP_PORT" =~ ^[1-9][0-9]*$ ]] || { echo "[ERROR] config http_port must be a positive integer" >&2; exit 1; }
[[ -n "$NGINX_PREFIX" ]] || NGINX_PREFIX="/predict/${INSTANCE}"
[[ "$NGINX_PREFIX" = /* ]] || { echo "[ERROR] --nginx-prefix must start with /" >&2; exit 1; }
BASE_PREFIX="${NGINX_PREFIX%/}"
PROXY_PREFIX="${BASE_PREFIX}/"
BEGIN_MARKER="# BEGIN managed: predict file ${BASE_PREFIX}"
END_MARKER="# END managed: predict file ${BASE_PREFIX}"

if [[ ! -f "$NGINX_MAPPING_FILE" ]]; then
  if [[ -f "${ROOT_DIR}/config/nginx_locations.txt" ]]; then
    mkdir -p "$(dirname "$NGINX_MAPPING_FILE")"
    cp "${ROOT_DIR}/config/nginx_locations.txt" "$NGINX_MAPPING_FILE"
  else
    echo "[ERROR] nginx mapping file is missing: $NGINX_MAPPING_FILE" >&2
    exit 1
  fi
fi

if grep -Fqx "$BEGIN_MARKER" "$NGINX_MAPPING_FILE" && ! grep -Fqx "$END_MARKER" "$NGINX_MAPPING_FILE"; then
  echo "[ERROR] incomplete managed Nginx block: $BEGIN_MARKER" >&2
  exit 1
fi

tmp="$(mktemp)"
awk -v begin="$BEGIN_MARKER" -v end="$END_MARKER" -v base="$BASE_PREFIX" -v prefix="$PROXY_PREFIX" -v port="$HTTP_PORT" '
  BEGIN { in_block = 0; replaced = 0 }
  $0 == begin { in_block = 1; replaced = 1; next }
  in_block && $0 == end {
    in_block = 0
    print begin
    print "# predict_file model and Binance futures mid-price dashboard (HTTP)"
    print base " http://127.0.0.1:" port "/"
    print prefix " http://127.0.0.1:" port "/"
    print end
    next
  }
  in_block { next }
  {
    if (substr($0, 1, length(base)) == base && substr($0, length(base) + 1, 1) ~ /[[:space:]]/) next
    if (substr($0, 1, length(prefix)) == prefix && substr($0, length(prefix) + 1, 1) ~ /[[:space:]]/) next
    print
  }
  END {
    if (!replaced) {
      print ""
      print begin
      print "# predict_file model and Binance futures mid-price dashboard (HTTP)"
      print base " http://127.0.0.1:" port "/"
      print prefix " http://127.0.0.1:" port "/"
      print end
    }
  }
' "$NGINX_MAPPING_FILE" >"$tmp"
mv "$tmp" "$NGINX_MAPPING_FILE"

echo "[INFO] Nginx mapping updated: ${NGINX_MAPPING_FILE}"
echo "[INFO] Dashboard proxy: http://<host>:${NGINX_PORT}${PROXY_PREFIX} -> http://127.0.0.1:${HTTP_PORT}/"

if [[ "$APPLY_NGINX" -eq 1 ]]; then
  echo "[INFO] Validating and reloading Nginx on port ${NGINX_PORT}"
  PORT="$NGINX_PORT" MAPPING_FILE="$NGINX_MAPPING_FILE" "${ROOT_DIR}/scripts/setup_nginx_4191.sh"
fi
