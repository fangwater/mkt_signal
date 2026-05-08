#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="viz_server"

# shellcheck source=scripts/deploy_intra_lib.sh
source "$ROOT_DIR/scripts/deploy_intra_lib.sh"

usage() {
  cat <<'EOF'
用法: scripts/deploy_intra_viz_server.sh --exchange <binance>
                                         [--env-suffix intra-trade]
                                         [--env-name binance-intra-trade]
                                         [--bind 0.0.0.0] [--ws-path /ws]
                                         [--namespace <IPC_NAMESPACE>]
                                         [--nginx-prefix /intra/<env-name>]
                                         [--nginx-port 4191]
                                         [--nginx-mapping-file $HOME/nginx_locations.txt]
                                         [--apply-nginx]
                                         [--resample-suffix <suffix>] [--instance-label <label>]
                                         [--jobs <n>] [--cargo-target-dir <path>]

说明:
  - 同所期现：构建并部署 viz_server 到 $HOME/<exchange>-<env_suffix>/
  - 在目标目录生成 config/viz.toml（订阅 pre_trade resample exposure/risk 通道）
  - port 由 (exchange, env-suffix) 硬编码 2D 映射决定（见 resolve_port）

启动/停止（在目标目录）:
  source ./env.sh
  ./intra_scripts/start_intra_viz_server.sh
  ./intra_scripts/stop_intra_viz_server.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage; exit 0
fi

ENV_SUFFIX="intra-trade"
ENV_NAME=""
EXCHANGE=""
BIND="0.0.0.0"
WS_PATH="/ws"
IPC_NS_OVERRIDE=""
RESAMPLE_SUFFIX=""
INSTANCE_LABEL="intra"
CARGO_TARGET_DIR_OVERRIDE=""
BUILD_JOBS=""
NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-suffix) ENV_SUFFIX="${2:-intra-trade}"; shift 2 ;;
    --env-name)   ENV_NAME="${2:-}"; shift 2 ;;
    --exchange)   EXCHANGE="${2:-}"; shift 2 ;;
    --bind)       BIND="${2:-0.0.0.0}"; shift 2 ;;
    --port)
      echo "[ERROR] --port 已移除，port 由 env-suffix 硬编码映射决定"
      exit 1 ;;
    --ws-path)    WS_PATH="${2:-/ws}"; shift 2 ;;
    --namespace)  IPC_NS_OVERRIDE="${2:-}"; shift 2 ;;
    --nginx-prefix) NGINX_PREFIX="${2:-}"; shift 2 ;;
    --nginx-port)   NGINX_PORT="${2:-4191}"; shift 2 ;;
    --nginx-mapping-file) NGINX_MAPPING_FILE="${2:-}"; shift 2 ;;
    --apply-nginx) APPLY_NGINX="1"; shift ;;
    --resample-suffix) RESAMPLE_SUFFIX="${2:-}"; shift 2 ;;
    --instance-label)  INSTANCE_LABEL="${2:-intra}"; shift 2 ;;
    --jobs)        BUILD_JOBS="${2:-}"; shift 2 ;;
    --cargo-target-dir) CARGO_TARGET_DIR_OVERRIDE="${2:-}"; shift 2 ;;
    *)
      echo "[ERROR] 未知参数: $1"; usage; exit 1 ;;
  esac
done

# (exchange, env-suffix) → port 硬编码映射
# 矩阵布局，每个 exchange 独占一个 decade，避免多交易所同 tier 撞口
#             arb01    arb02    arb03
#   okex      10171    10172    10173
#   bybit     10174    10175    10176
#   gate      10177    10178    10179
#   binance   10180    10181    10182
#   bitget    10183    10184    10185
#   intra-trade(legacy 单交易所兼容): 10131
resolve_port() {
  local exchange="$1" suffix="$2"
  case "$suffix" in
    intra-trade) echo "10131"; return ;;
  esac
  case "$exchange:$suffix" in
    okex:intra-arb01)    echo "10171" ;;
    okex:intra-arb02)    echo "10172" ;;
    okex:intra-arb03)    echo "10173" ;;
    bybit:intra-arb01)   echo "10174" ;;
    bybit:intra-arb02)   echo "10175" ;;
    bybit:intra-arb03)   echo "10176" ;;
    gate:intra-arb01)    echo "10177" ;;
    gate:intra-arb02)    echo "10178" ;;
    gate:intra-arb03)    echo "10179" ;;
    binance:intra-arb01) echo "10180" ;;
    binance:intra-arb02) echo "10181" ;;
    binance:intra-arb03) echo "10182" ;;
    bitget:intra-arb01)  echo "10183" ;;
    bitget:intra-arb02)  echo "10184" ;;
    bitget:intra-arb03)  echo "10185" ;;
    *) echo "[ERROR] 未知的 (exchange, env-suffix): ($exchange, $suffix)" >&2; exit 1 ;;
  esac
}

if [[ -n "$ENV_NAME" && -z "$EXCHANGE" ]]; then
  if [[ "${ENV_NAME,,}" =~ ^([a-z0-9]+)[-_]intra[-_][a-z0-9][a-z0-9_-]*$ ]]; then
    EXCHANGE="${BASH_REMATCH[1]}"
  fi
fi

if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] 需要 --exchange，或使用 --env-name <exchange>-intra-<tag>"
  usage; exit 1
fi

EXCHANGE="$(intra_ensure_exchange "$EXCHANGE")"

if [[ -z "$ENV_NAME" ]]; then
  ENV_NAME="${EXCHANGE}-${ENV_SUFFIX}"
fi
ENV_NAME="${ENV_NAME,,}"

PORT="$(resolve_port "$EXCHANGE" "$ENV_SUFFIX")"

if [[ "$WS_PATH" != /* ]]; then WS_PATH="/$WS_PATH"; fi

TARGET_DIR="$HOME/${ENV_NAME}"
mkdir -p "$TARGET_DIR"

if [[ -z "$NGINX_PREFIX" ]]; then
  NGINX_PREFIX="/intra/${ENV_NAME}"
fi
if [[ -z "$NGINX_MAPPING_FILE" ]]; then
  NGINX_MAPPING_FILE="$HOME/nginx_locations.txt"
fi

upsert_main_nginx_mapping() {
  local main_file begin_marker end_marker tmp
  main_file="${NGINX_MAPPING_FILE}"
  if [[ ! -f "$main_file" ]]; then
    if [[ -f "${ROOT_DIR}/config/nginx_locations.txt" ]]; then
      mkdir -p "$(dirname "$main_file")" >/dev/null 2>&1 || true
      cp "${ROOT_DIR}/config/nginx_locations.txt" "$main_file"
      echo "[INFO] Initialized nginx mapping file: $main_file"
    else
      echo "[ERROR] Missing nginx mapping file: $main_file" >&2
      exit 1
    fi
  fi
  if [[ "${NGINX_PREFIX}" != /* ]]; then
    echo "[ERROR] --nginx-prefix must start with /: ${NGINX_PREFIX}" >&2; exit 1
  fi
  local base_prefix="${NGINX_PREFIX%/}"
  local static_prefix="${base_prefix}/"
  local ws_location="${base_prefix}${WS_PATH}"
  local health_location="${base_prefix}/healthz"
  local snapshot_location="${base_prefix}/snapshot"
  # 写字面 $HOME 让 setup_nginx_4191.sh 在目标机上展开（远端部署 $HOME 与本机不同）
  local static_dir="\$HOME/${ENV_NAME}/www/"

  begin_marker="# BEGIN managed: intra viz ${base_prefix}"
  end_marker="# END managed: intra viz ${base_prefix}"
  if grep -Fqx "$begin_marker" "$main_file" && ! grep -Fqx "$end_marker" "$main_file"; then
    echo "[ERROR] nginx_locations.txt has begin marker but missing end marker: ${begin_marker}" >&2
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
      -v ws_path="$WS_PATH" '
    BEGIN { in_block = 0; replaced = 0 }
    $0 == begin { in_block = 1; replaced = 1; next }
    in_block && $0 == end {
        in_block = 0;
        print begin;
        print "# intra pre_trade dashboard (static) + viz_server (WS/healthz/snapshot)";
        print static_prefix " static:" static_dir;
        print ws_location " http://127.0.0.1:" port ws_path;
        print health_location " http://127.0.0.1:" port "/healthz";
        print snapshot_location " http://127.0.0.1:" port "/snapshot";
        print end;
        next
    }
    in_block { next }
    {
        if (substr($0, 1, length(prefix)) == prefix && substr($0, length(prefix) + 1, 1) ~ /[[:space:]]/) { next }
        if (substr($0, 1, length(prefix) + 1) == (prefix "/") && substr($0, length(prefix) + 2, 1) ~ /[[:space:]]/) { next }
        print
    }
    END {
        if (!replaced) {
            print "";
            print begin;
            print "# intra pre_trade dashboard (static) + viz_server (WS/healthz/snapshot)";
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
  IPC_NAMESPACE="${EXCHANGE}_${ENV_SUFFIX_NS}"
  echo "[WARN] 未读取到 IPC_NAMESPACE 且未指定 --namespace；使用默认推断: $IPC_NAMESPACE"
  echo "[WARN] 建议先生成 env.sh: scripts/deploy_setup_env_intra.sh --env-name ${ENV_NAME} --exchange ${EXCHANGE}"
fi

echo "[INFO] 构建 $BIN_NAME (release)"
CARGO_TARGET_DIR_EFFECTIVE="$(intra_effective_cargo_target_dir "$ROOT_DIR" "$CARGO_TARGET_DIR_OVERRIDE")"
(
  cd "$ROOT_DIR"
  CARGO_TARGET_DIR="$CARGO_TARGET_DIR_EFFECTIVE" \
    cargo build --release --bin "$BIN_NAME" ${BUILD_JOBS:+--jobs "$BUILD_JOBS"}
)
BIN_PATH="$(intra_bin_path_release "$CARGO_TARGET_DIR_EFFECTIVE" "$BIN_NAME")"

mkdir -p "$TARGET_DIR/config" "$TARGET_DIR/intra_scripts"
echo "[INFO] 生成 viz 配置: $TARGET_DIR/config/viz.toml (namespaces=[\"$IPC_NAMESPACE\"] port=${PORT})"

emit_server_block() {
  local exposure_ch="pre_trade_exposure"
  local risk_ch="pre_trade_risk"
  if [[ -n "$RESAMPLE_SUFFIX" ]]; then
    exposure_ch="pre_trade_exposure_${RESAMPLE_SUFFIX}"
    risk_ch="pre_trade_risk_${RESAMPLE_SUFFIX}"
  fi

  cat <<EOF
[[servers]]
namespaces = ["$IPC_NAMESPACE"]
[servers.http]
bind = "$BIND"
port = $PORT
ws_path = "$WS_PATH"
[servers.pre_trade]
enabled = true
[[servers.pre_trade.instances]]
label = "$INSTANCE_LABEL"
exposure_channel = "$exposure_ch"
risk_channel = "$risk_ch"
EOF
}

emit_server_block > "$TARGET_DIR/config/viz.toml"

mkdir -p "$TARGET_DIR/www"
# 优先使用同所专用 dashboard（如有），否则回退通用
if [[ -f "$ROOT_DIR/docs/intra_pre_trade_dashboard.html" ]]; then
  cp "$ROOT_DIR/docs/intra_pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
  cp "$ROOT_DIR/docs/intra_pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
  echo "[INFO] 已同步 dashboard: $TARGET_DIR/www/pre_trade_dashboard.html"
elif [[ -f "$ROOT_DIR/docs/xarb_binance_std_pre_trade_dashboard.html" && "$EXCHANGE" == "binance" ]]; then
  cp "$ROOT_DIR/docs/xarb_binance_std_pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
  cp "$ROOT_DIR/docs/xarb_binance_std_pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
  echo "[INFO] 已同步 binance 同所 dashboard"
elif [[ -f "$ROOT_DIR/docs/pre_trade_dashboard.html" ]]; then
  cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
  cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
  echo "[WARN] 缺少 intra dashboard，回退到通用版"
fi

EXTRA_FILES=(
  "intra_scripts/start_intra_viz_server.sh"
  "intra_scripts/stop_intra_viz_server.sh"
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
if ! intra_atomic_install "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"; then
  exit 2
fi

echo "[INFO] 部署完成: $TARGET_DIR"
echo "[INFO] 启动: cd $TARGET_DIR && ./intra_scripts/start_intra_viz_server.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./intra_scripts/stop_intra_viz_server.sh"
echo "[INFO] dashboard nginx: ${NGINX_PREFIX}/"
echo "[INFO] dashboard(静态): $TARGET_DIR/www/"
