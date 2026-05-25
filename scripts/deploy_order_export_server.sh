#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/${USER}/order_export_server}"
SKIP_LOCAL=0
SKIP_REMOTE=0
APPLY_NGINX=0
UPSTREAM_PORT="${ORDER_EXPORT_SERVER_PORT:-8821}"
NGINX_PREFIX=""

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_order_export_server.sh
    [--skip-local] [--skip-remote]
    [--apply-nginx] [--upstream-port 8821] [--nginx-prefix /order_export/<env>/]

说明:
  - 默认: cargo install 到本地 /home/$USER/order_export_server,
          然后 rsync 推送到 $FR_DEPLOY_HOST:$FR_REMOTE_HOME/$FR_REMOTE_ENV_DIR/.
  - --skip-local : 跳过本地 cargo install (复用现有 binary).
  - --skip-remote: 只装本地, 不推远端.
  - --apply-nginx: rsync 后 upsert nginx_locations.txt + 远端 reload nginx
                   (走 scripts/lib/fr_remote_deploy.sh 现成机制).
  - --upstream-port: 远端 server 监听端口 (写进 nginx mapping); 默认 8821 或 ORDER_EXPORT_SERVER_PORT.
  - --nginx-prefix : nginx URL 前缀; 默认 /order_export/<FR_REMOTE_ENV_DIR>/.
  - 远端 host/key 通过 FR_DEPLOY_HOST / FR_DEPLOY_KEY / FR_REMOTE_HOME 控制,
    部署的 env 目录通过 FR_REMOTE_ENV_DIR 指定.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-local)    SKIP_LOCAL=1; shift ;;
    --skip-remote)   SKIP_REMOTE=1; shift ;;
    --apply-nginx)   APPLY_NGINX=1; shift ;;
    --upstream-port) UPSTREAM_PORT="${2:-}"; shift 2 ;;
    --nginx-prefix)  NGINX_PREFIX="${2:-}"; shift 2 ;;
    -h|--help)       usage; exit 0 ;;
    *)               echo "[ERROR] unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ $SKIP_LOCAL -eq 0 ]]; then
  echo "[INFO] installing order_export_server into ${INSTALL_ROOT}"
  cargo install --path "${BASE_DIR}" --bin order_export_server --root "${INSTALL_ROOT}" --force --locked
  echo "[INFO] local binary at ${INSTALL_ROOT}/bin/order_export_server"
else
  echo "[INFO] --skip-local: leaving ${INSTALL_ROOT}/bin/order_export_server untouched"
fi

if [[ $SKIP_REMOTE -eq 1 ]]; then
  echo "[INFO] --skip-remote: not pushing to remote"
  exit 0
fi

# shellcheck source=lib/fr_remote_deploy.sh
source "${BASE_DIR}/scripts/lib/fr_remote_deploy.sh"

REMOTE_ENV_DIR="${FR_REMOTE_ENV_DIR:-}"
if [[ -z "${REMOTE_ENV_DIR}" ]]; then
  echo "[ERROR] FR_REMOTE_ENV_DIR is required (env directory name, e.g. binance_fr_trade01)" >&2
  exit 1
fi

if [[ $APPLY_NGINX -eq 1 ]]; then
  fr_remote_init "${BASE_DIR}" "${REMOTE_ENV_DIR}"
  fr_remote_fetch_nginx_mapping "${BASE_DIR}"
else
  fr_remote_init_ssh "${BASE_DIR}"
fi

LOCAL_BIN="${INSTALL_ROOT}/bin/order_export_server"
if [[ ! -f "${LOCAL_BIN}" ]]; then
  echo "[ERROR] local binary missing: ${LOCAL_BIN}" >&2
  echo "[ERROR] run without --skip-local first, or build it manually." >&2
  exit 1
fi

OPTS="$(_fr_ssh_opts)"
REMOTE_DEST_DIR="${FR_REMOTE_HOME}/${REMOTE_ENV_DIR}"

echo "[INFO] mkdir -p remote: ${FR_DEPLOY_HOST}:${REMOTE_DEST_DIR}"
# shellcheck disable=SC2086
ssh ${OPTS} "${FR_DEPLOY_HOST}" "mkdir -p ${REMOTE_DEST_DIR}"

echo "[INFO] rsync ${LOCAL_BIN} -> ${FR_DEPLOY_HOST}:${REMOTE_DEST_DIR}/order_export_server"
# shellcheck disable=SC2086
rsync -a --human-readable --info=stats1 \
  -e "ssh ${OPTS}" \
  "${LOCAL_BIN}" \
  "${FR_DEPLOY_HOST}:${REMOTE_DEST_DIR}/order_export_server"

for sh in start_order_export_server.sh stop_order_export_server.sh; do
  echo "[INFO] rsync scripts/${sh} -> ${FR_DEPLOY_HOST}:${REMOTE_DEST_DIR}/${sh}"
  # shellcheck disable=SC2086
  rsync -a --human-readable --info=stats1 \
    -e "ssh ${OPTS}" \
    "${BASE_DIR}/scripts/${sh}" \
    "${FR_DEPLOY_HOST}:${REMOTE_DEST_DIR}/${sh}"
done

if [[ $APPLY_NGINX -eq 1 ]]; then
  if [[ -z "$NGINX_PREFIX" ]]; then
    NGINX_PREFIX="/order_export/${REMOTE_ENV_DIR}/"
  fi
  if [[ "$NGINX_PREFIX" != /* ]]; then
    echo "[ERROR] --nginx-prefix must start with /: ${NGINX_PREFIX}" >&2
    exit 1
  fi
  if [[ "$NGINX_PREFIX" != */ ]]; then
    NGINX_PREFIX="${NGINX_PREFIX}/"
  fi
  base_prefix="${NGINX_PREFIX%/}"

  echo "[INFO] upsert nginx mapping: ${NGINX_PREFIX} -> http://127.0.0.1:${UPSTREAM_PORT}/"

  tmp="$(mktemp)"
  awk -v begin="# BEGIN managed: order_export_server ${base_prefix}" \
      -v end="# END managed: order_export_server ${base_prefix}" \
      -v base_prefix="$base_prefix" \
      -v prefix="$NGINX_PREFIX" \
      -v port="$UPSTREAM_PORT" '
    function emit() {
      print begin
      print "# order_export_server (HTTP, per-env)"
      print base_prefix " http://127.0.0.1:" port "/"
      print prefix " http://127.0.0.1:" port "/"
      print end
    }
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end { in_block = 0; emit(); next }
    in_block { next }
    {
      if (substr($0, 1, length(base_prefix)) == base_prefix && substr($0, length(base_prefix) + 1, 1) ~ /[[:space:]]/) {
        next
      }
      if (substr($0, 1, length(prefix)) == prefix && substr($0, length(prefix) + 1, 1) ~ /[[:space:]]/) {
        next
      }
      print
    }
    END { if (!replaced) { print ""; emit() } }
  ' "$FR_NGINX_STAGING" > "$tmp"
  mv "$tmp" "$FR_NGINX_STAGING"

  fr_remote_apply_nginx "${REMOTE_ENV_DIR}"
fi

cat <<EOF
[INFO] done.
[INFO] local : ${INSTALL_ROOT}/bin/order_export_server
[INFO] remote: ssh -i ${FR_DEPLOY_KEY:-${BASE_DIR}/aws-jp-srv-1.pem} \\
           ${FR_DEPLOY_HOST:-ubuntu@54.64.147.69} \\
           'cd ${FR_REMOTE_HOME}/${REMOTE_ENV_DIR} && ./start_order_export_server.sh'
[INFO] env.sh keys (optional):
  ORDER_EXPORT_SERVER_BIND=0.0.0.0
  ORDER_EXPORT_SERVER_PORT=${UPSTREAM_PORT}
  ORDER_EXPORT_SERVER_RETENTION_HOURS=3
EOF
if [[ $APPLY_NGINX -eq 1 ]]; then
  echo "[INFO] nginx : http://${FR_DEPLOY_HOST#*@}:${FR_NGINX_PORT}${NGINX_PREFIX}latest"
fi
