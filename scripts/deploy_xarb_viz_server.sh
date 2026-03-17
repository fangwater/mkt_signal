#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="viz_server"
BIN_PATH=""

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
用法: scripts/deploy_xarb_viz_server.sh --open-venue <okex-futures> --hedge-venue <binance-futures>
                                       [--env-suffix xarb-trade]
                                       [--env-name okex-binance-xarb-trade]
                                       [--bind 0.0.0.0] [--ws-path /ws]
                                       [--namespace <IPC_NAMESPACE>]
                                       [--nginx-prefix /xarb/<env-name>]
                                       [--nginx-port 4191]
                                       [--nginx-mapping-file $HOME/nginx_locations.txt]
                                       [--apply-nginx]
                                       [--resample-suffix <suffix>] [--instance-label <label>]
                                       [--jobs <n>] [--cargo-target-dir <path>]
      scripts/deploy_xarb_viz_server.sh --remote-host awsjp [--remote-repo <path>] [--remote-sync] [...]

说明:
  - 构建并部署 viz_server 到 xarb 环境目录 $HOME/<open>-<hedge>-<env_suffix>/（默认 env_suffix=xarb-trade，可通过 --env-suffix / --env-name 指定）。
  - 会在目标目录生成/覆盖：config/viz.toml（仅开启 pre_trade resample 订阅，单端口）。
  - port 由 env-suffix 硬编码映射决定（xarb-trade → 10111），不支持的 env-suffix 会报错退出。
  - namespaces 字段对应 IceOryx 的 IPC_NAMESPACE（即 pre_trade 发布时使用的命名空间前缀）。
      - 若目标目录已存在 env.sh，则优先读取其中的 IPC_NAMESPACE
      - 否则可用 --namespace 显式指定；若不指定则按默认规则推断
  - 可选写入 nginx mapping（默认路径 /xarb/<env-name>/），包含静态 dashboard、ws、healthz、snapshot。
  - 可选 --resample-suffix：订阅带后缀的两路通道（pre_trade_exposure_<suffix>, pre_trade_risk_<suffix>）

示例:
  scripts/deploy_xarb_viz_server.sh --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_xarb_viz_server.sh --env-name okex-binance-xarb-trade --open-venue okex-futures --hedge-venue binance-futures
  scripts/deploy_xarb_viz_server.sh --open-venue okex-futures --hedge-venue binance-futures --resample-suffix okex_binance_xarb

启动/停止（在目标目录）:
  source ./env.sh  # 可选，但推荐
  ./xarb_scripts/start_xarb_viz_server.sh
  ./xarb_scripts/stop_xarb_viz_server.sh

远程模式（可选）:
  --remote-host <ssh_host>        在远端编译并部署（避免本机编译）
  --remote-repo <path>            远端仓库目录（默认 $HOME/crypto_mkt/mkt_signal）
  --remote-sync                   先 rsync 本地仓库到远端（默认关闭）
  --remote-cargo-target-dir <p>   远端 cargo target 目录（默认 $HOME/.cache/mkt_signal/cargo_target_xarb）
  --remote-nice <n>               远端执行优先级（默认 10）
  --remote-ionice/--remote-no-ionice  远端使用 ionice 降低 IO 优先级（默认开启）
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

ENV_SUFFIX="xarb-trade"
ENV_NAME=""
OPEN_VENUE=""
HEDGE_VENUE=""

BIND="0.0.0.0"
WS_PATH="/ws"

IPC_NS_OVERRIDE=""
RESAMPLE_SUFFIX=""
INSTANCE_LABEL="xarb"
CARGO_TARGET_DIR_OVERRIDE=""
BUILD_JOBS=""
NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="0"
if [[ -n "${XARB_REMOTE_RUN:-}" ]]; then
  BUILD_JOBS="1"
fi

normalize_env_name() {
  echo "$1" | tr 'A-Z' 'a-z'
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
  local static_prefix="${base_prefix}/"
  local ws_path="${WS_PATH}"
  local ws_location="${base_prefix}${ws_path}"
  local health_location="${base_prefix}/healthz"
  local snapshot_location="${base_prefix}/snapshot"
  local static_dir="${TARGET_DIR}/www/"

  begin_marker="# BEGIN managed: xarb viz ${base_prefix}"
  end_marker="# END managed: xarb viz ${base_prefix}"

  if grep -Fqx "$begin_marker" "$main_file" && ! grep -Fqx "$end_marker" "$main_file"; then
    echo "[ERROR] nginx_locations.txt has begin marker but missing end marker:" >&2
    echo "        ${begin_marker}" >&2
    exit 1
  fi

  tmp="$(mktemp)"
  awk -v begin="$begin_marker" \
      -v end="$end_marker" \
      -v prefix="$base_prefix" \
      -v static_prefix="$static_prefix" \
      -v static_dir="$static_dir" \
      -v ws_location="$ws_location" \
      -v health_location="$health_location" \
      -v snapshot_location="$snapshot_location" \
      -v port="$PORT" \
      -v ws_path="$ws_path" '
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end {
        in_block = 0;
        print begin;
        print "# xarb pre_trade dashboard (static) + viz_server (WS/healthz/snapshot)";
        print static_prefix " static:" static_dir;
        print ws_location " http://127.0.0.1:" port ws_path;
        print health_location " http://127.0.0.1:" port "/healthz";
        print snapshot_location " http://127.0.0.1:" port "/snapshot";
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
            print "# xarb pre_trade dashboard (static) + viz_server (WS/healthz/snapshot)";
            print static_prefix " static:" static_dir;
            print ws_location " http://127.0.0.1:" port ws_path;
            print health_location " http://127.0.0.1:" port "/healthz";
            print snapshot_location " http://127.0.0.1:" port "/snapshot";
            print end;
        }
    }
  ' "$main_file" >"$tmp"
  mv "$tmp" "$main_file"
}

normalize_venue() {
  echo "${1,,}"
}

ensure_xarb_venue() {
  local v
  v="$(normalize_venue "$1")"
  if [[ -z "$v" || ! "$v" =~ ^[a-z0-9]+-(margin|futures|spot|swap|perp|perpetual)$ ]]; then
    echo "[ERROR] 非法 xarb venue: $1"
    exit 1
  fi
  echo "$v"
}

infer_pair_from_name() {
  local name="${1,,}"
  local open_ex=""
  local hedge_ex=""

  if [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$ ]]; then
    open_ex="${BASH_REMATCH[1]}"
    hedge_ex="${BASH_REMATCH[2]}"
  fi

  if [[ "$open_ex" == "okx" ]]; then
    open_ex="okex"
  fi
  if [[ "$hedge_ex" == "okx" ]]; then
    hedge_ex="okex"
  fi

  if [[ -n "$open_ex" && -n "$hedge_ex" ]]; then
    echo "${open_ex},${hedge_ex}"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    trade|test)
      echo "[ERROR] 不再支持 trade/test 位置参数，请使用 --env-suffix 或 --env-name"
      usage
      exit 1
      ;;
    --env-suffix)
      ENV_SUFFIX="${2:-xarb-trade}"
      shift 2
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
    --bind)
      BIND="${2:-0.0.0.0}"
      shift 2
      ;;
    --port)
      echo "[ERROR] --port 已移除，port 由 env-suffix 硬编码映射决定"
      exit 1
      ;;
    --ws-path)
      WS_PATH="${2:-/ws}"
      shift 2
      ;;
    --namespace)
      IPC_NS_OVERRIDE="${2:-}"
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
    --resample-suffix)
      RESAMPLE_SUFFIX="${2:-}"
      shift 2
      ;;
    --instance-label)
      INSTANCE_LABEL="${2:-xarb}"
      shift 2
      ;;
    --jobs)
      BUILD_JOBS="${2:-}"
      shift 2
      ;;
    --cargo-target-dir)
      CARGO_TARGET_DIR_OVERRIDE="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] 未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

# ── env-suffix → port 硬编码映射 ──
resolve_port_by_suffix() {
  case "$1" in
    xarb-trade) echo "10111" ;;
    xarb-trade01) echo "10151" ;;
    xarb-trade02) echo "10152" ;;
    xarb-trade03) echo "10153" ;;
    *) echo "[ERROR] 未知的 env-suffix: $1，无法确定 port" >&2; exit 1 ;;
  esac
}
PORT="$(resolve_port_by_suffix "$ENV_SUFFIX")"

if [[ -n "$ENV_NAME" && ( -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ) ]]; then
  if inferred="$(infer_pair_from_name "$ENV_NAME")" && [[ -n "$inferred" ]]; then
    if [[ "${inferred%%,*}" == "${inferred##*,}" ]]; then
      OPEN_VENUE="${OPEN_VENUE:-${inferred%%,*}-margin}"
      HEDGE_VENUE="${HEDGE_VENUE:-${inferred##*,}-futures}"
    else
      OPEN_VENUE="${OPEN_VENUE:-${inferred%%,*}-futures}"
      HEDGE_VENUE="${HEDGE_VENUE:-${inferred##*,}-futures}"
    fi
  fi
fi

if [[ -z "$OPEN_VENUE" || -z "$HEDGE_VENUE" ]]; then
  echo "[ERROR] 需要 --open-venue 与 --hedge-venue，或提供可推断的 --env-name"
  usage
  exit 1
fi

ENV_NAME="$(normalize_env_name "$ENV_NAME")"
OPEN_VENUE="$(ensure_xarb_venue "$OPEN_VENUE")"
HEDGE_VENUE="$(ensure_xarb_venue "$HEDGE_VENUE")"
if [[ "$OPEN_VENUE" == "$HEDGE_VENUE" ]]; then
  echo "[ERROR] xarb open/hedge venue 不能完全相同：open=$OPEN_VENUE hedge=$HEDGE_VENUE"
  exit 1
fi

OPEN_EXCHANGE="${OPEN_VENUE%%-*}"
HEDGE_EXCHANGE="${HEDGE_VENUE%%-*}"
if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${OPEN_EXCHANGE}-${HEDGE_EXCHANGE}-${ENV_SUFFIX}"
fi

if [[ "$WS_PATH" != /* ]]; then
  WS_PATH="/$WS_PATH"
fi

TARGET_DIR="$HOME/${ENV_NAME}"
mkdir -p "$TARGET_DIR"

if [[ -z "$NGINX_PREFIX" ]]; then
  NGINX_PREFIX="/xarb/${ENV_NAME}"
fi
if [[ -z "$NGINX_MAPPING_FILE" ]]; then
  NGINX_MAPPING_FILE="$HOME/nginx_locations.txt"
fi

IPC_NAMESPACE=""
ENV_FILE="$TARGET_DIR/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  IPC_NAMESPACE="${IPC_NAMESPACE:-}"
fi
IPC_NAMESPACE="${IPC_NS_OVERRIDE:-${IPC_NAMESPACE:-}}"

if [[ -z "$IPC_NAMESPACE" ]]; then
  ENV_SUFFIX_NS="${ENV_SUFFIX//-/_}"
  IPC_NAMESPACE="${OPEN_EXCHANGE}_${HEDGE_EXCHANGE}_${ENV_SUFFIX_NS}"
  echo "[WARN] 未读取到 $ENV_FILE 的 IPC_NAMESPACE，且未指定 --namespace；使用默认推断: $IPC_NAMESPACE"
  echo "[WARN] 建议先生成 env.sh：scripts/deploy_setup_env_xarb.sh --env-name ${ENV_NAME} --open-venue ${OPEN_VENUE} --hedge-venue ${HEDGE_VENUE}"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
CARGO_TARGET_DIR_EFFECTIVE="$(xarb_effective_cargo_target_dir "$ROOT_DIR" "$CARGO_TARGET_DIR_OVERRIDE")"
(
  cd "$ROOT_DIR"
  CARGO_TARGET_DIR="$CARGO_TARGET_DIR_EFFECTIVE" \
    cargo build --release --bin "$BIN_NAME" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
)
BIN_PATH="$(xarb_bin_path_release "$CARGO_TARGET_DIR_EFFECTIVE" "$BIN_NAME")"

mkdir -p "$TARGET_DIR/config" "$TARGET_DIR/xarb_scripts"
echo "[INFO] 生成 viz 配置: $TARGET_DIR/config/viz.toml (namespaces=[\"$IPC_NAMESPACE\"] port=${PORT})"

emit_server_block() {
  local bind="$1"
  local port="$2"
  local ws_path="$3"

  cat << EOF
[[servers]]
namespaces = ["$IPC_NAMESPACE"]
[servers.http]
bind = "$bind"
port = $port
ws_path = "$ws_path"
[servers.pre_trade]
enabled = true
EOF

  local exposure_ch="pre_trade_exposure"
  local risk_ch="pre_trade_risk"
  if [[ -n "$RESAMPLE_SUFFIX" ]]; then
    exposure_ch="pre_trade_exposure_${RESAMPLE_SUFFIX}"
    risk_ch="pre_trade_risk_${RESAMPLE_SUFFIX}"
  fi

  # Default: only subscribe exposure/risk (dashboard does not use aggregated positions).
  cat << EOF
[[servers.pre_trade.instances]]
label = "$INSTANCE_LABEL"
exposure_channel = "$exposure_ch"
risk_channel = "$risk_ch"
EOF

}

emit_server_block "$BIND" "$PORT" "$WS_PATH" > "$TARGET_DIR/config/viz.toml"

mkdir -p "$TARGET_DIR/www"
if [[ -f "$ROOT_DIR/docs/xarb_pre_trade_dashboard.html" ]]; then
  cp "$ROOT_DIR/docs/xarb_pre_trade_dashboard.html" "$TARGET_DIR/www/xarb_pre_trade_dashboard.html"

  DASHBOARD_SRC="$ROOT_DIR/docs/xarb_pre_trade_dashboard.html"
  if [[ "$OPEN_VENUE" == "binance-margin" && "$HEDGE_VENUE" == "binance-futures" && -f "$ROOT_DIR/docs/xarb_binance_std_pre_trade_dashboard.html" ]]; then
    DASHBOARD_SRC="$ROOT_DIR/docs/xarb_binance_std_pre_trade_dashboard.html"
    cp "$ROOT_DIR/docs/xarb_binance_std_pre_trade_dashboard.html" "$TARGET_DIR/www/xarb_binance_std_pre_trade_dashboard.html"
    echo "[INFO] 已同步 Binance std 专用 dashboard: $TARGET_DIR/www/xarb_binance_std_pre_trade_dashboard.html"
  fi

  cp "$DASHBOARD_SRC" "$TARGET_DIR/www/pre_trade_dashboard.html"
  cp "$DASHBOARD_SRC" "$TARGET_DIR/www/index.html"
  echo "[INFO] 已同步 dashboard: $TARGET_DIR/www/pre_trade_dashboard.html"
elif [[ -f "$ROOT_DIR/docs/pre_trade_dashboard.html" ]]; then
  cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
  cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
  echo "[WARN] 缺少 xarb dashboard，回退到通用版: $TARGET_DIR/www/pre_trade_dashboard.html"
fi

EXTRA_FILES=(
  "xarb_scripts/start_xarb_viz_server.sh"
  "xarb_scripts/stop_xarb_viz_server.sh"
  "scripts/setup_nginx_4191.sh"
)
for file in "${EXTRA_FILES[@]}"; do
  SRC="$ROOT_DIR/$file"
  if [[ -f "$SRC" ]]; then
    DEST_DIR="$TARGET_DIR/$(dirname "$file")"
    mkdir -p "$DEST_DIR"
    rsync -a "$SRC" "$DEST_DIR/"
    chmod +x "$TARGET_DIR/$file" 2>/dev/null || true
  fi
done

upsert_main_nginx_mapping
if [[ "${APPLY_NGINX}" == "1" ]]; then
  echo "[INFO] Applying nginx config (PORT=${NGINX_PORT}, MAPPING_FILE=${NGINX_MAPPING_FILE})"
  (
    cd "$ROOT_DIR"
    PORT="$NGINX_PORT" MAPPING_FILE="$NGINX_MAPPING_FILE" ./scripts/setup_nginx_4191.sh
  )
fi

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR"
if ! xarb_atomic_install "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"; then
  exit 2
fi

echo "[INFO] 部署完成: $TARGET_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && ./xarb_scripts/start_xarb_viz_server.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./xarb_scripts/stop_xarb_viz_server.sh"
echo "[INFO] dashboard nginx: ${NGINX_PREFIX}/"
echo "[INFO] dashboard(静态): $TARGET_DIR/www/"
