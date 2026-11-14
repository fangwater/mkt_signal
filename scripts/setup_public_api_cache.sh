#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONF_SOURCE="${ROOT_DIR}/config/nginx/bitget_public_api_cache.conf"
SNIPPET_SOURCE="${ROOT_DIR}/config/nginx/bitget_public_cache.inc"
CONF_TARGET="/etc/nginx/conf.d/bitget_public_api_cache.conf"
SNIPPET_TARGET="/etc/nginx/snippets/bitget_public_cache.inc"
CACHE_DIR="/var/cache/nginx/public_api/bitget"
PORT="28902"
EXECUTE=0

usage() {
    cat <<'EOF'
Usage: scripts/setup_public_api_cache.sh [--execute]

Installs the host-local Bitget public REST proxy cache on 127.0.0.1:28902.
Without --execute, prints the target settings and makes no changes.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --execute)
            EXECUTE=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "[ERROR] unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

for source_file in "$CONF_SOURCE" "$SNIPPET_SOURCE"; do
    if [[ ! -f "$source_file" ]]; then
        echo "[ERROR] missing source file: $source_file" >&2
        exit 1
    fi
done

echo "[PLAN] host=$(hostname -f)"
echo "[PLAN] listen=127.0.0.1:${PORT} upstream=https://api.bitget.com"
echo "[PLAN] conf=${CONF_TARGET} snippet=${SNIPPET_TARGET}"
echo "[PLAN] cache_dir=${CACHE_DIR} source_ip=system-default"

if [[ "$EXECUTE" -ne 1 ]]; then
    echo "[DRY-RUN] no files changed; rerun with --execute"
    exit 0
fi

for command_name in nginx systemctl install ss date; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "[ERROR] required command not found: $command_name" >&2
        exit 1
    fi
done

listener="$(ss -lntpH "sport = :${PORT}" 2>/dev/null || true)"
if [[ -n "$listener" && "$listener" != *nginx* ]]; then
    echo "[ERROR] 127.0.0.1:${PORT} is already owned by a non-nginx process" >&2
    echo "$listener" >&2
    exit 1
fi

if [[ "$(id -u)" -eq 0 ]]; then
    SUDO=()
else
    SUDO=(sudo)
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
conf_backup=""
snippet_backup=""
if [[ -e "$CONF_TARGET" ]]; then
    conf_backup="${CONF_TARGET}.bak.${stamp}"
    "${SUDO[@]}" cp -a -- "$CONF_TARGET" "$conf_backup"
fi
if [[ -e "$SNIPPET_TARGET" ]]; then
    snippet_backup="${SNIPPET_TARGET}.bak.${stamp}"
    "${SUDO[@]}" cp -a -- "$SNIPPET_TARGET" "$snippet_backup"
fi

rollback() {
    if [[ -n "$conf_backup" ]]; then
        "${SUDO[@]}" cp -a -- "$conf_backup" "$CONF_TARGET"
    else
        "${SUDO[@]}" rm -f -- "$CONF_TARGET"
    fi
    if [[ -n "$snippet_backup" ]]; then
        "${SUDO[@]}" cp -a -- "$snippet_backup" "$SNIPPET_TARGET"
    else
        "${SUDO[@]}" rm -f -- "$SNIPPET_TARGET"
    fi
}

"${SUDO[@]}" install -d -o www-data -g www-data -m 0750 "$CACHE_DIR"
"${SUDO[@]}" install -m 0644 "$SNIPPET_SOURCE" "$SNIPPET_TARGET"
"${SUDO[@]}" install -m 0644 "$CONF_SOURCE" "$CONF_TARGET"

if ! "${SUDO[@]}" nginx -t; then
    echo "[ERROR] nginx validation failed; restoring previous configuration" >&2
    rollback
    "${SUDO[@]}" nginx -t
    exit 1
fi

if ! "${SUDO[@]}" systemctl reload nginx; then
    echo "[ERROR] nginx reload failed; restoring previous configuration" >&2
    rollback
    "${SUDO[@]}" nginx -t
    "${SUDO[@]}" systemctl reload nginx
    exit 1
fi

echo "[OK] Bitget public API cache is listening on 127.0.0.1:${PORT}"
