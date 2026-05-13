#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/${USER}/order_export}"
SKIP_LOCAL=0
SKIP_REMOTE=0

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_order_export.sh [--skip-local] [--skip-remote]

说明:
  - 默认: cargo install 到本地 /home/$USER/order_export,
          然后 rsync 推送到 $FR_DEPLOY_HOST:/home/ubuntu/order_export/bin/.
  - --skip-local : 跳过本地 cargo install (复用现有 binary).
  - --skip-remote: 只装本地, 不推远端.
  - 远端 host/key 通过 scripts/lib/fr_remote_deploy.sh 的环境变量控制
    (FR_DEPLOY_HOST / FR_DEPLOY_KEY / FR_REMOTE_HOME).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-local)  SKIP_LOCAL=1; shift ;;
    --skip-remote) SKIP_REMOTE=1; shift ;;
    -h|--help)     usage; exit 0 ;;
    *)             echo "[ERROR] unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ $SKIP_LOCAL -eq 0 ]]; then
  echo "[INFO] installing order_export into ${INSTALL_ROOT}"
  cargo install --path "${BASE_DIR}" --bin order_export --root "${INSTALL_ROOT}" --force --locked
  echo "[INFO] local binary at ${INSTALL_ROOT}/bin/order_export"
else
  echo "[INFO] --skip-local: leaving ${INSTALL_ROOT}/bin/order_export untouched"
fi

if [[ $SKIP_REMOTE -eq 0 ]]; then
  # shellcheck source=lib/fr_remote_deploy.sh
  source "${BASE_DIR}/scripts/lib/fr_remote_deploy.sh"
  fr_remote_init_ssh "${BASE_DIR}"

  LOCAL_BIN="${INSTALL_ROOT}/bin/order_export"
  if [[ ! -f "${LOCAL_BIN}" ]]; then
    echo "[ERROR] local binary missing: ${LOCAL_BIN}" >&2
    echo "[ERROR] run without --skip-local first, or build it manually." >&2
    exit 1
  fi

  OPTS="$(_fr_ssh_opts)"
  REMOTE_BIN_DIR="${FR_REMOTE_HOME}/order_export/bin"

  echo "[INFO] mkdir -p remote: ${FR_DEPLOY_HOST}:${REMOTE_BIN_DIR}"
  # shellcheck disable=SC2086
  ssh ${OPTS} "${FR_DEPLOY_HOST}" "mkdir -p ${REMOTE_BIN_DIR}"

  echo "[INFO] rsync ${LOCAL_BIN} -> ${FR_DEPLOY_HOST}:${REMOTE_BIN_DIR}/order_export"
  # shellcheck disable=SC2086
  rsync -a --human-readable --info=stats1 \
    -e "ssh ${OPTS}" \
    "${LOCAL_BIN}" \
    "${FR_DEPLOY_HOST}:${REMOTE_BIN_DIR}/order_export"
else
  echo "[INFO] --skip-remote: not pushing to remote"
fi

cat <<EOF
[INFO] done.
[INFO] local : ${INSTALL_ROOT}/bin/order_export
[INFO] remote: ssh -i ${FR_DEPLOY_KEY:-${BASE_DIR}/aws-jp-srv-1.pem} \\
           ${FR_DEPLOY_HOST:-ubuntu@54.64.147.69} \\
           '/home/ubuntu/order_export/bin/order_export --date YYYY-MM-DD'
[INFO] env once:
  export ORDER_EXPORT_BASE_DIR=/home/\$USER
[INFO] example:
  cd ~/binance-intra-arb01
  ${INSTALL_ROOT}/bin/order_export --date 2026-05-12
EOF
