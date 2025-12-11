#!/usr/bin/env bash
# Install nginx (Debian/Ubuntu) and configure a reverse proxy on port 4911.
# Supports multiple path→upstream mappings for HTTP/WebSocket。

set -euo pipefail

PORT="${PORT:-4911}"
DEFAULT_UPSTREAM="${UPSTREAM:-http://127.0.0.1:3000}"
SERVER_NAME="${SERVER_NAME:-_}"
MAPPING_FILE="${MAPPING_FILE:-config/nginx_locations.txt}"
SITE_NAME="crypto_proxy_${PORT}"
CONF_PATH="/etc/nginx/sites-available/${SITE_NAME}.conf"

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

if [ -L /etc/nginx/sites-enabled/default ]; then
    ${SUDO} rm /etc/nginx/sites-enabled/default
fi

${SUDO} nginx -t
if command -v systemctl >/dev/null 2>&1; then
    ${SUDO} systemctl reload nginx
else
    ${SUDO} nginx -s reload
fi

echo "Nginx configured: listen ${PORT}, proxy_pass ${UPSTREAM}, server_name ${SERVER_NAME}"
