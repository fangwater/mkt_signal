#!/usr/bin/env bash
# Install nginx (Debian/Ubuntu) and configure a reverse proxy on port 4191.
# (Renamed from setup_nginx_4911.sh; now defaults to 4191.)
# Supports multiple path→upstream mappings for HTTP/WebSocket, and optional static directory hosting.

set -euo pipefail

PORT="${PORT:-4191}"
DEFAULT_UPSTREAM="${UPSTREAM:-http://127.0.0.1:3000}"
SERVER_NAME="${SERVER_NAME:-_}"
# Prefer a host-local mapping file so this script can run outside the repo.
MAPPING_FILE="${MAPPING_FILE:-$HOME/nginx_locations.txt}"
KEEP_OLD_PORTS="${KEEP_OLD_PORTS:-0}"
SITE_NAME="${SITE_NAME:-crypto_proxy_${PORT}}"
CONF_PATH="${CONF_PATH:-/etc/nginx/sites-available/${SITE_NAME}.conf}"

need_sudo() {
    if [ "$(id -u)" -ne 0 ]; then
        echo "sudo"
    else
        echo ""
    fi
}

SUDO="$(need_sudo)"

if ! command -v nginx >/dev/null 2>&1; then
    ${SUDO} apt-get update
    DEBIAN_FRONTEND=noninteractive ${SUDO} apt-get install -y nginx
fi

expand_static_dir() {
    local raw="$1"
    local dir="$raw"

    # Allow writing $HOME or ~ in mapping file, expand to absolute path here.
    if [[ "$dir" == "~/"* ]]; then
        dir="${HOME}/${dir#~/}"
    elif [[ "$dir" == "~" ]]; then
        dir="${HOME}"
    elif [[ "$dir" == "\$HOME/"* ]]; then
        dir="${HOME}/${dir#\$HOME/}"
    elif [[ "$dir" == "\$HOME" ]]; then
        dir="${HOME}"
    elif [[ "$dir" == "\${HOME}/"* ]]; then
        dir="${HOME}/${dir#\${HOME}/}"
    elif [[ "$dir" == "\${HOME}" ]]; then
        dir="${HOME}"
    fi

    echo "$dir"
}

locations() {
    if [ -f "${MAPPING_FILE}" ]; then
        while IFS= read -r line; do
            # skip blanks/comments
            [[ -z "${line// }" || "${line}" =~ ^# ]] && continue
            path="$(echo "${line}" | awk '{print $1}')"
            upstream="$(echo "${line}" | awk '{print $2}')"
            if [ -z "${path}" ] || [ -z "${upstream}" ]; then
                echo "忽略无效行: ${line}" >&2
                continue
            fi

            # static directory (served by nginx directly)
            # Mapping format:
            #   /xarb/okex-binance/ static:/abs/path/to/www/
            # Note: path and directory are recommended to end with '/'.
            if [[ "${upstream}" == static:* ]]; then
                dir="$(expand_static_dir "${upstream#static:}")"
                if [[ -z "${dir}" ]]; then
                    echo "忽略无效 static 映射（目录为空）: ${line}" >&2
                    continue
                fi
                if [[ "${dir}" != /* ]]; then
                    echo "忽略无效 static 映射（需绝对路径）: ${line}" >&2
                    continue
                fi
                # normalize trailing slash
                [[ "${dir}" != */ ]] && dir="${dir}/"

                # Normalize to a single trailing slash, and always redirect the no-slash variant
                # to keep relative URLs (e.g. ./ws, ./assets/...) stable.
                if [[ "${path}" != "/" ]]; then
                    base_path="${path%/}"
                    path="${base_path}/"
                    cat <<EOF
    location = ${base_path} {
        return 301 ${path};
    }

EOF
                fi

                # SPA-friendly: fallback to index.html under the same prefix.
                index_uri="${path}index.html"
                cat <<EOF
    location ${path} {
        alias ${dir};
        index index.html;
        try_files \$uri \$uri/ ${index_uri};
        add_header Cache-Control "no-cache";
    }

EOF
                continue
            fi

            # nginx does not accept ws:// or wss:// in proxy_pass; WebSocket still uses HTTP(S)
            upstream="${upstream/#ws:\/\//http:\/\/}"
            upstream="${upstream/#wss:\/\//https:\/\/}"
            cat <<EOF
    location ${path} {
        proxy_pass ${upstream};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }

EOF
        done < "${MAPPING_FILE}"
    else
        cat <<EOF
    location / {
        proxy_pass ${DEFAULT_UPSTREAM};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }
EOF
    fi
}

cat <<EOF | ${SUDO} tee "${CONF_PATH}" >/dev/null
server {
    listen ${PORT};
    server_name ${SERVER_NAME};

$(locations)
}
EOF

${SUDO} ln -sf "${CONF_PATH}" "/etc/nginx/sites-enabled/${SITE_NAME}.conf"

if [[ "${KEEP_OLD_PORTS}" != "1" ]]; then
    # Disable the previously used port to avoid confusion.
    ${SUDO} rm -f /etc/nginx/sites-enabled/crypto_proxy_4911.conf
fi

if [ -L /etc/nginx/sites-enabled/default ]; then
    ${SUDO} rm /etc/nginx/sites-enabled/default
fi

${SUDO} nginx -t
if command -v systemctl >/dev/null 2>&1; then
    ${SUDO} systemctl reload nginx
else
    ${SUDO} nginx -s reload
fi

echo "Nginx configured: listen ${PORT}, mappings from ${MAPPING_FILE}, server_name ${SERVER_NAME}"
