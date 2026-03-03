#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

TARGET_DIR="${HOME}/persist_exporter_public"
BIND="0.0.0.0"
PORT="10331"

NGINX_PREFIX="/persist_admin"
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="1"

usage() {
  cat <<'EOF'
Usage:
  scripts/deploy_persist_admin_server.sh [--target <path>]
                                         [--bind 0.0.0.0] [--port 10331]
                                         [--nginx-prefix /persist_admin]
                                         [--nginx-port 4191]
                                         [--nginx-mapping-file $HOME/nginx_locations.txt]
                                         [--apply-nginx|--no-apply-nginx]

Notes:
  - Deploy persist_admin_server into target dir (default: ~/persist_exporter_public).
  - By default it updates nginx mapping and applies nginx immediately.
  - nginx mapping block is managed and idempotent.
EOF
}

upsert_main_nginx_mapping() {
  local main_file begin_marker end_marker tmp

  if [[ -z "${NGINX_MAPPING_FILE}" ]]; then
    NGINX_MAPPING_FILE="$HOME/nginx_locations.txt"
  fi
  main_file="${NGINX_MAPPING_FILE}"

  if [[ ! -f "$main_file" ]]; then
    if [[ -f "${ROOT_DIR}/config/nginx_locations.txt" ]]; then
      mkdir -p "$(dirname "$main_file")" >/dev/null 2>&1 || true
      cp "${ROOT_DIR}/config/nginx_locations.txt" "$main_file"
      echo "[INFO] Initialized nginx mapping file: $main_file (from ${ROOT_DIR}/config/nginx_locations.txt)"
    else
      echo "[ERROR] Missing nginx mapping file: $main_file" >&2
      echo "[ERROR] Also missing template: ${ROOT_DIR}/config/nginx_locations.txt" >&2
      exit 1
    fi
  fi

  if [[ "${NGINX_PREFIX}" != /* ]]; then
    echo "[ERROR] --nginx-prefix must start with /: ${NGINX_PREFIX}" >&2
    exit 1
  fi

  local base_prefix="${NGINX_PREFIX%/}"
  local proxy_prefix="${base_prefix}/"

  begin_marker="# BEGIN managed: persist admin ${base_prefix}"
  end_marker="# END managed: persist admin ${base_prefix}"

  if grep -Fqx "$begin_marker" "$main_file" && ! grep -Fqx "$end_marker" "$main_file"; then
    echo "[ERROR] nginx_locations.txt has begin marker but missing end marker:" >&2
    echo "        ${begin_marker}" >&2
    echo "        (please fix the block manually and retry)" >&2
    exit 1
  fi

  tmp="$(mktemp)"
  awk -v begin="$begin_marker" \
      -v end="$end_marker" \
      -v prefix="$base_prefix" \
      -v proxy_prefix="$proxy_prefix" \
      -v port="$PORT" '
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end {
        in_block = 0;
        print begin;
        print "# persist admin server (HTTP UI + API)";
        print prefix " http://127.0.0.1:" port "/";
        print proxy_prefix " http://127.0.0.1:" port "/";
        print end;
        next
    }
    in_block { next }
    {
        if (substr($0, 1, length(prefix)) == prefix && substr($0, length(prefix) + 1, 1) ~ /[[:space:]]/) {
            next
        }
        if (substr($0, 1, length(prefix) + 1) == (prefix "/") && substr($0, length(prefix) + 2, 1) ~ /[[:space:]]/) {
            next
        }
        print
    }
    END {
        if (!replaced) {
            print "";
            print begin;
            print "# persist admin server (HTTP UI + API)";
            print prefix " http://127.0.0.1:" port "/";
            print proxy_prefix " http://127.0.0.1:" port "/";
            print end;
        }
    }
  ' "$main_file" >"$tmp"
  mv "$tmp" "$main_file"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      TARGET_DIR="${2:-}"
      shift 2
      ;;
    --bind)
      BIND="${2:-0.0.0.0}"
      shift 2
      ;;
    --port)
      PORT="${2:-10331}"
      shift 2
      ;;
    --nginx-prefix)
      NGINX_PREFIX="${2:-/persist_admin}"
      shift 2
      ;;
    --nginx-port)
      NGINX_PORT="${2:-4191}"
      shift 2
      ;;
    --nginx-mapping-file)
      NGINX_MAPPING_FILE="${2:-}"
      shift 2
      ;;
    --apply-nginx)
      APPLY_NGINX="1"
      shift
      ;;
    --no-apply-nginx)
      APPLY_NGINX="0"
      shift
      ;;
    *)
      echo "[ERROR] unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ! "$PORT" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --port must be numeric: $PORT" >&2
  exit 1
fi

echo "[INFO] building persist_admin_server (release)..."
cargo build --release --bin persist_admin_server

mkdir -p "${TARGET_DIR}" "${TARGET_DIR}/config" "${TARGET_DIR}/logs"

cp -f "${ROOT_DIR}/target/release/persist_admin_server" "${TARGET_DIR}/persist_admin_server"
chmod +x "${TARGET_DIR}/persist_admin_server"

if [[ ! -f "${TARGET_DIR}/config/persist_auto_exporter.toml" ]]; then
  cp -f "${ROOT_DIR}/config/persist_auto_exporter.toml" "${TARGET_DIR}/config/persist_auto_exporter.toml"
  echo "[INFO] copied default config to ${TARGET_DIR}/config/persist_auto_exporter.toml"
fi

cp -f "${ROOT_DIR}/scripts/start_persist_admin_server.sh" "${TARGET_DIR}/start_persist_admin_server.sh"
cp -f "${ROOT_DIR}/scripts/stop_persist_admin_server.sh" "${TARGET_DIR}/stop_persist_admin_server.sh"
chmod +x "${TARGET_DIR}/start_persist_admin_server.sh" "${TARGET_DIR}/stop_persist_admin_server.sh"

cat <<EOF > "${TARGET_DIR}/config/persist_admin_server.env"
PERSIST_ADMIN_BIND=${BIND}:${PORT}
EOF

upsert_main_nginx_mapping
if [[ "${APPLY_NGINX}" == "1" ]]; then
  echo "[INFO] Applying nginx config (PORT=${NGINX_PORT}, MAPPING_FILE=${NGINX_MAPPING_FILE:-$HOME/nginx_locations.txt})"
  (
    cd "$ROOT_DIR"
    PORT="${NGINX_PORT}" MAPPING_FILE="${NGINX_MAPPING_FILE}" ./scripts/setup_nginx_4191.sh
  )
fi

echo "[INFO] deployed persist_admin_server to ${TARGET_DIR}"
echo "[INFO] start: cd ${TARGET_DIR} && ./start_persist_admin_server.sh"
echo "[INFO] stop:  cd ${TARGET_DIR} && ./stop_persist_admin_server.sh"
echo "[INFO] nginx prefix: ${NGINX_PREFIX} (port ${NGINX_PORT})"
