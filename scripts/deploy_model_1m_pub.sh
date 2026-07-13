#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="model_1m_pub"
BIN_PATH="$ROOT_DIR/target/release/$BIN_NAME"

usage() {
  cat <<'USAGE'
Usage:
  deploy_model_1m_pub.sh [--target <model_name>]

Behavior:
  - 不传 --target 时使用默认 1m model_name
  - 部署目录: ~/model_1m_pub/<model_name>/
  - 输出 service 固定改写为 model_output/<model_name>

Examples:
  bash scripts/deploy_model_1m_pub.sh --target binance-futures-mid-re-1m
  bash scripts/deploy_model_1m_pub.sh
USAGE
}

MODEL_NAME=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      MODEL_NAME="${2:-}"
      if [[ -z "$MODEL_NAME" ]]; then
        echo "[ERROR] --target 需要一个值" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 未知参数: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$MODEL_NAME" ]]; then
  MODEL_NAME="binance-futures-mid-re-1m"
  echo "[INFO] 未指定 --target，使用默认 model_name: $MODEL_NAME"
fi

TARGET_DIR="$HOME/model_1m_pub/${MODEL_NAME}"

echo "[INFO] 构建 $BIN_NAME (release)"
cargo build --release --bin "$BIN_NAME" --features model-ort

echo "[INFO] 部署 $BIN_NAME 到 $TARGET_DIR (model_name=$MODEL_NAME)"
mkdir -p "$TARGET_DIR"
cp "$BIN_PATH" "$TARGET_DIR/$BIN_NAME"
chmod +x "$TARGET_DIR/$BIN_NAME"

mkdir -p "$TARGET_DIR/scripts"
for script in start_model_1m_pub.sh stop_model_1m_pub.sh process_match_lib.sh; do
  if [[ -f "$ROOT_DIR/scripts/$script" ]]; then
    rsync -a "$ROOT_DIR/scripts/$script" "$TARGET_DIR/scripts/"
    chmod +x "$TARGET_DIR/scripts/$script"
  fi
done

mkdir -p "$TARGET_DIR/config"
if [[ -f "$ROOT_DIR/config/model_1m_pub.toml" ]]; then
  escaped_model_name="${MODEL_NAME//\\/\\\\}"
  escaped_model_name="${escaped_model_name//&/\\&}"
  sed \
    -e "s|{model_name}|${escaped_model_name}|g" \
    -e "s|^output_service = .*|output_service = \"model_output/${escaped_model_name}\"|" \
    "$ROOT_DIR/config/model_1m_pub.toml" >"$TARGET_DIR/config/model_1m_pub.toml"
fi

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] target_dir: $TARGET_DIR"
echo "[INFO] model_name: $MODEL_NAME"
echo "[INFO] input_service: fusion_factor_1m/binance-futures"
echo "[INFO] factor_plan: factor_plan_1m"
echo "[INFO] 启动: cd $TARGET_DIR && ./scripts/start_model_1m_pub.sh"
echo "[INFO] 停止: cd $TARGET_DIR && ./scripts/stop_model_1m_pub.sh"
