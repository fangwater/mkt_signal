#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=scripts/deploy_xarb_lib.sh
source "$ROOT_DIR/scripts/deploy_xarb_lib.sh"
xarb_preparse_remote_args "$@"
set -- "${XARB_FORWARD_ARGS[@]}"
if [[ -n "${XARB_REMOTE_HOST}" ]]; then
  xarb_remote_maybe_sync_repo "$ROOT_DIR"
  xarb_remote_exec "scripts/$(basename "${BASH_SOURCE[0]}")" "$@"
  exit $?
fi

usage() {
  cat <<'EOF'
用法: scripts/deploy_xarb_order_query.sh [trade|test] --open-venue <okex-futures> --hedge-venue <binance-futures>
                                       [--env-name okex-binance-xarb-trade]
                                       [--port 18080]
                                       [--nginx-prefix /xarb/okex-binance/order_query]
                                       [--nginx-port 4191]
                                       [--nginx-mapping-file $HOME/nginx_locations.txt]
                                       [--apply-nginx]
      scripts/deploy_xarb_order_query.sh --remote-host awsjp [--remote-repo <path>] [--remote-sync] [...]

说明:
  - 部署 Python webserver：order_query 到 $HOME/<open>-<hedge>-xarb-<trade|test>/（或 --env-name 指定）。
  - 同步文件到目标目录：
      order_query/ (后端+前端)
      scripts/export_all.sh
      scripts/export_xarb_symbol_data.py
      xarb_scripts/start_xarb_order_query.sh
      xarb_scripts/stop_xarb_order_query.sh
      scripts/setup_nginx_4191.sh
  - 会把 nginx 映射写入 $HOME/nginx_locations.txt（幂等更新；可用 --nginx-mapping-file 覆盖）。
    这样可以避免用“只有 order_query 的映射文件”去重建 nginx 站点，导致其他路径（如静态面板）被覆盖而 404。

示例:
  scripts/deploy_xarb_order_query.sh trade --open-venue okex-futures --hedge-venue binance-futures --port 18080

远程模式（可选）:
  --remote-host <ssh_host>        在远端部署（用于目标机为 awsjp 的场景）
  --remote-repo <path>            远端仓库目录（默认 $HOME/crypto_mkt/mkt_signal）
  --remote-sync                   先 rsync 本地仓库到远端（默认关闭）
  --remote-tty                    远端执行分配 TTY（需要 sudo 输密码时用）
  --remote-nice <n>               远端执行优先级（默认 10）
  --remote-ionice/--remote-no-ionice  远端使用 ionice 降低 IO 优先级（默认开启）
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_TYPE="trade"
ENV_NAME=""
OPEN_VENUE=""
HEDGE_VENUE=""
PORT="18080"
NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="0"

normalize_venue() {
  echo "${1,,}"
}

ensure_futures_venue() {
  local v
  v="$(normalize_venue "$1")"
  if [[ -z "$v" || "$v" != *-futures ]]; then
    echo "[ERROR] xarb 只支持 futures：venue 必须以 -futures 结尾: $1"
    exit 1
  fi
  echo "$v"
}

upsert_main_nginx_mapping() {
  local main_file begin_marker end_marker tmp
  if [[ -z "${NGINX_MAPPING_FILE}" ]]; then
    NGINX_MAPPING_FILE="$HOME/nginx_locations.txt"
  fi
  main_file="${NGINX_MAPPING_FILE}"

  if [[ ! -f "$main_file" ]]; then
    # Bootstrap from repo template (first-time setup on a fresh host)
    if [[ -f "${ROOT_DIR}/config/nginx_locations.txt" ]]; then
      mkdir -p "$(dirname "$main_file")" >/dev/null 2>&1 || true
      cp "${ROOT_DIR}/config/nginx_locations.txt" "$main_file"
      echo "[INFO] 初始化 nginx 映射文件: $main_file (from ${ROOT_DIR}/config/nginx_locations.txt)"
    else
      echo "[ERROR] 未找到 nginx 映射文件: $main_file" >&2
      echo "[ERROR] 且未找到模板: ${ROOT_DIR}/config/nginx_locations.txt" >&2
      exit 1
    fi
  fi
  if [[ "${NGINX_PREFIX}" != /* ]]; then
    echo "[ERROR] --nginx-prefix 必须以 / 开头: ${NGINX_PREFIX}" >&2
    exit 1
  fi

  begin_marker="# BEGIN managed: xarb order_query ${NGINX_PREFIX}"
  end_marker="# END managed: xarb order_query ${NGINX_PREFIX}"

  if grep -Fqx "$begin_marker" "$main_file" && ! grep -Fqx "$end_marker" "$main_file"; then
    echo "[ERROR] nginx_locations.txt 存在 begin marker 但缺少 end marker，拒绝自动修改：" >&2
    echo "        ${begin_marker}" >&2
    echo "        （请手动补齐/删除该段后重试）" >&2
    exit 1
  fi

  tmp="$(mktemp)"
  awk -v begin="$begin_marker" \
      -v end="$end_marker" \
      -v prefix="$NGINX_PREFIX" \
      -v port="$PORT" '
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end {
        in_block = 0;
        print begin;
        print "# xarb order_query (HTTP)";
        print "# upstream 末尾带 /，用于将前缀 " prefix "(/) strip 掉并转发给后端的 /";
        print prefix " http://127.0.0.1:" port "/";
        print prefix "/ http://127.0.0.1:" port "/";
        print end;
        next
    }
    in_block { next }
    {
        # 去重：如果历史上有人手动把映射写进主文件（不在 managed block 内），这里会把旧行删掉，避免 nginx duplicate location。
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
            print "# xarb order_query (HTTP)";
            print "# upstream 末尾带 /，用于将前缀 " prefix "(/) strip 掉并转发给后端的 /";
            print prefix " http://127.0.0.1:" port "/";
            print prefix "/ http://127.0.0.1:" port "/";
            print end;
        }
    }
  ' "$main_file" >"$tmp"
  mv "$tmp" "$main_file"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    trade|test)
      ENV_TYPE="$1"
      shift
      ;;
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --open-venue)
      OPEN_VENUE="${2:-}"
      shift 2
      ;;
    --hedge-venue)
      HEDGE_VENUE="${2:-}"
      shift 2
      ;;
    --port)
      PORT="${2:-18080}"
      shift 2
      ;;
    --nginx-prefix)
      NGINX_PREFIX="${2:-}"
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
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 需要 --open-venue 与 --hedge-venue"
  usage
  exit 1
fi

OPEN_VENUE="$(ensure_futures_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(ensure_futures_venue "$HEDGE_VENUE")"
if [[ "$OPEN_VENUE" == "$HEDGE_VENUE" ]]; then
  echo "[ERROR] xarb 需要跨所：open=$OPEN_VENUE hedge=$HEDGE_VENUE"
  exit 1
fi

OPEN_EXCHANGE="${OPEN_VENUE%%-*}"
HEDGE_EXCHANGE="${HEDGE_VENUE%%-*}"
if [[ "$OPEN_EXCHANGE" == "okx" ]]; then
  OPEN_EXCHANGE="okex"
fi
if [[ "$HEDGE_EXCHANGE" == "okx" ]]; then
  HEDGE_EXCHANGE="okex"
fi

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-xarb-${ENV_TYPE}"
fi

TARGET_DIR="$HOME/${ENV_NAME}"

if [[ -z "$NGINX_PREFIX" ]]; then
  NGINX_PREFIX="/xarb/${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}/order_query"
fi
if [[ -z "${NGINX_MAPPING_FILE}" ]]; then
  NGINX_MAPPING_FILE="$HOME/nginx_locations.txt"
fi

echo "[INFO] 部署 order_query 到 $TARGET_DIR"
mkdir -p "$TARGET_DIR"
mkdir -p "$TARGET_DIR/order_query" "$TARGET_DIR/scripts" "$TARGET_DIR/xarb_scripts" "$TARGET_DIR/config" "$TARGET_DIR/data/order_query" >/dev/null 2>&1 || true

rsync -a --delete "$ROOT_DIR/order_query/" "$TARGET_DIR/order_query/"

EXTRA_FILES=(
  "scripts/export_all.sh"
  "scripts/export_xarb_symbol_data.py"
  "scripts/export_symbol_data_v2.py"
  "scripts/setup_nginx_4191.sh"
  "xarb_scripts/start_xarb_order_query.sh"
  "xarb_scripts/stop_xarb_order_query.sh"
)

for file in "${EXTRA_FILES[@]}"; do
  SRC_PATH="$ROOT_DIR/$file"
  if [[ -f "$SRC_PATH" ]]; then
    DEST_DIR="$TARGET_DIR/$(dirname "$file")"
    mkdir -p "$DEST_DIR"
    rsync -a "$SRC_PATH" "$DEST_DIR/"
    chmod +x "$DEST_DIR/$(basename "$file")" 2>/dev/null || true
  else
    echo "[WARN] missing: $SRC_PATH"
  fi
done

upsert_main_nginx_mapping

if [[ "${APPLY_NGINX}" == "1" ]]; then
  echo "[INFO] 应用 nginx 配置 (PORT=${NGINX_PORT}, MAPPING_FILE=${NGINX_MAPPING_FILE})"
  (
    cd "$ROOT_DIR"
    PORT="${NGINX_PORT}" MAPPING_FILE="${NGINX_MAPPING_FILE}" scripts/setup_nginx_4191.sh
  )
fi

echo ""
echo "[INFO] order_query 部署完成: $TARGET_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && PYTHON_BIN=/home/ubuntu/jupyter_env/bin/python ./xarb_scripts/start_xarb_order_query.sh --port ${PORT} --api-base http://127.0.0.1:8089"
echo "[INFO] 停止: cd $TARGET_DIR && ./xarb_scripts/stop_xarb_order_query.sh"
echo ""
echo "[INFO] nginx 主映射已更新: ${NGINX_MAPPING_FILE}"
echo "[INFO] 如需在 nginx(${NGINX_PORT}) 上暴露该页面，请用主映射重建/重载 nginx："
echo "       cd ${ROOT_DIR} && PORT=${NGINX_PORT} MAPPING_FILE=${NGINX_MAPPING_FILE} scripts/setup_nginx_4191.sh"
