#!/usr/bin/env bash
set -Eeuo pipefail

# binance-intra-arb01 实盘发布脚本。环境、进程名和二进制路径全部固定。
readonly ENV_NAME="binance-intra-arb01"
readonly REPO_DIR="/home/ubuntu/mkt_signal"
readonly ENV_DIR="/home/ubuntu/binance-intra-arb01"
readonly PERSIST_CENTER_DIR="/home/ubuntu/persist_center"
readonly RELEASE_DIR="${REPO_DIR}/target/release"
readonly INTRA_SCRIPTS_DIR="${ENV_DIR}/intra_scripts"
readonly CANCEL_SCRIPT="${ENV_DIR}/scripts/cancel_binance_std_orders.py"

readonly TRADE_ENGINE_PROC="intra_te_binance_arb01"
readonly PRE_TRADE_PROC="intra_pt_binance_arb01"
readonly PROCESS_WAIT_SECONDS=30
readonly STARTUP_SETTLE_SECONDS=5
readonly LOG_TIMEOUT_SECONDS=60
readonly LOG_LINES=80

CURRENT_STEP="初始化"

usage() {
  cat <<'USAGE'
用法: scripts/republish_binance_intra_arb01.sh

固定目标: /home/ubuntu/binance-intra-arb01

流程:
  1. 确认中心 persist_sync_collector/persist_read_server 与本次 release 完全一致。
  2. 停止 trade_engine。
  3. 执行 Binance STANDARD Spot + UM 撤单。
  4. 再执行一次 dry-run，并自动确认 Spot 与 UM 均为零挂单。
  5. 停止 pre_trade 和 trade_signal。
  6. 从 /home/ubuntu/mkt_signal/target/release 直接 cp 覆盖三个二进制，不保留备份。
  7. 启动 trade_engine，检查进程和日志，人工确认后继续。
  8. 启动 pre_trade，检查进程和日志，人工确认后结束。

最终状态:
  - trade_engine: 运行
  - pre_trade: 运行
  - trade_signal: 保持停止，不重新启动
USAGE
}

binary_pattern() {
  local binary="$1"
  printf '^%s/%s([[:space:]]|$)' "$ENV_DIR" "$binary"
}

binary_pids() {
  local binary="$1"
  pgrep -f "$(binary_pattern "$binary")" 2>/dev/null || true
}

binary_running() {
  local binary="$1"
  pgrep -f "$(binary_pattern "$binary")" >/dev/null 2>&1
}

print_process_states() {
  local binary pids rendered
  echo "[状态] 当前目标进程:"
  for binary in trade_engine pre_trade trade_signal; do
    pids="$(binary_pids "$binary")"
    if [[ -n "$pids" ]]; then
      rendered="${pids//$'\n'/,}"
      echo "  - ${binary}: 运行中 (pid=${rendered})"
    else
      echo "  - ${binary}: 已停止"
    fi
  done
}

recovery_hint() {
  cat <<EOF
[提示] 脚本不会自动回滚二进制，也不会自动启动 trade_signal。
[提示] 如需人工恢复，请在确认风险后使用环境本地脚本:
  cd ${ENV_DIR}
  ./intra_scripts/start_intra_trade_engine.sh
  ./intra_scripts/start_intra_pre_trade.sh
EOF
}

die() {
  local message="$1"
  echo "[失败] ${message}" >&2
  echo "[失败] 当前步骤: ${CURRENT_STEP}" >&2
  print_process_states >&2
  recovery_hint >&2
  exit 1
}

on_error() {
  local exit_code=$?
  local line="${1:-未知}"
  trap - ERR
  set +e
  echo "[失败] 命令执行失败，退出码=${exit_code}，行号=${line}" >&2
  echo "[失败] 当前步骤: ${CURRENT_STEP}" >&2
  print_process_states >&2
  recovery_hint >&2
  exit "$exit_code"
}
trap 'on_error "$LINENO"' ERR

require_command() {
  local command_name="$1"
  command -v "$command_name" >/dev/null 2>&1 || die "缺少命令: ${command_name}"
}

preflight() {
  CURRENT_STEP="发布前预检"

  [[ "$(basename "$ENV_DIR")" == "$ENV_NAME" ]] || die "目标环境目录名不匹配"
  [[ -d "$REPO_DIR" ]] || die "仓库目录不存在: ${REPO_DIR}"
  [[ -d "$ENV_DIR" ]] || die "目标环境目录不存在: ${ENV_DIR}"
  [[ -w "$ENV_DIR" ]] || die "目标环境目录不可写: ${ENV_DIR}"
  [[ -f "${ENV_DIR}/env.sh" ]] || die "目标环境缺少 env.sh"

  local command_name
  for command_name in cp cmp grep pgrep python3 timeout pmdaemon npx; do
    require_command "$command_name"
  done

  local script_path
  for script_path in \
    "${INTRA_SCRIPTS_DIR}/stop_intra_trade_engine.sh" \
    "${INTRA_SCRIPTS_DIR}/start_intra_trade_engine.sh" \
    "${INTRA_SCRIPTS_DIR}/stop_intra_pre_trade.sh" \
    "${INTRA_SCRIPTS_DIR}/start_intra_pre_trade.sh" \
    "${INTRA_SCRIPTS_DIR}/stop_intra_trade_signal.sh"; do
    [[ -x "$script_path" ]] || die "脚本不存在或不可执行: ${script_path}"
  done
  [[ -f "$CANCEL_SCRIPT" ]] || die "撤单脚本不存在: ${CANCEL_SCRIPT}"

  local binary
  for binary in trade_engine pre_trade trade_signal; do
    [[ -x "${RELEASE_DIR}/${binary}" ]] || die "release 二进制不存在或不可执行: ${RELEASE_DIR}/${binary}"
  done

  [[ -d "$PERSIST_CENTER_DIR" ]] || die "中心持久化目录不存在: ${PERSIST_CENTER_DIR}"
  for binary in persist_sync_collector persist_read_server; do
    [[ -x "${RELEASE_DIR}/${binary}" ]] || die "release 二进制不存在或不可执行: ${RELEASE_DIR}/${binary}"
    [[ -x "${PERSIST_CENTER_DIR}/${binary}" ]] || die "中心二进制不存在或不可执行: ${PERSIST_CENTER_DIR}/${binary}"
    cmp -s -- "${RELEASE_DIR}/${binary}" "${PERSIST_CENTER_DIR}/${binary}" || \
      die "中心 ${binary} 与本次 release 不一致；请先更新并重启 persist_center"
  done

  echo "[通过] 发布前预检完成"
}

confirm_live_operation() {
  CURRENT_STEP="实盘操作确认"
  [[ -r /dev/tty ]] || die "需要交互式终端确认实盘操作"

  echo ""
  echo "[警告] 即将操作实盘环境 ${ENV_NAME}: 停进程、撤销全部 STANDARD Spot/UM 挂单并覆盖二进制。"
  printf '[确认] 请输入完整环境名 %s 继续: ' "$ENV_NAME" >/dev/tty
  local reply
  read -r reply </dev/tty || die "无法读取实盘确认"
  [[ "$reply" == "$ENV_NAME" ]] || die "环境名确认不匹配，已终止"
}

wait_for_stopped() {
  local binary="$1"
  local deadline=$((SECONDS + PROCESS_WAIT_SECONDS))

  while ((SECONDS < deadline)); do
    if ! binary_running "$binary"; then
      echo "[通过] ${binary} 已停止"
      return 0
    fi
    sleep 1
  done
  die "等待 ${binary} 停止超时"
}

wait_for_running() {
  local binary="$1"
  local deadline=$((SECONDS + PROCESS_WAIT_SECONDS))

  while ((SECONDS < deadline)); do
    if binary_running "$binary"; then
      echo "[通过] ${binary} 已启动，pid=$(binary_pids "$binary" | tr '\n' ',')"
      return 0
    fi
    sleep 1
  done
  die "等待 ${binary} 启动超时"
}

stop_component() {
  local binary="$1"
  local stop_script="$2"

  CURRENT_STEP="停止 ${binary}"
  echo ""
  echo "[步骤] ${CURRENT_STEP}"
  "$stop_script"
  wait_for_stopped "$binary"
}

cancel_all_orders() {
  CURRENT_STEP="执行 Binance STANDARD 全量撤单"
  echo ""
  echo "[步骤] ${CURRENT_STEP}"
  python3 "$CANCEL_SCRIPT" --execute
}

verify_no_open_orders() {
  CURRENT_STEP="dry-run 确认 Spot 与 UM 均无挂单"
  echo ""
  echo "[步骤] ${CURRENT_STEP}"

  local output
  if ! output="$(python3 "$CANCEL_SCRIPT" 2>&1)"; then
    printf '%s\n' "$output"
    die "撤单后的 dry-run 查询失败"
  fi
  printf '%s\n' "$output"

  local spot_zero='[plan] symbols=0 open_orders=0 execute=False'
  local um_zero='[plan] no open UM futures orders found'
  grep -Fq "$spot_zero" <<<"$output" || die "dry-run 仍检测到 STANDARD Spot 挂单"
  grep -Fq "$um_zero" <<<"$output" || die "dry-run 仍检测到 STANDARD UM 挂单"

  echo "[通过] 第二次 dry-run 已确认 STANDARD Spot 与 UM 均为零挂单"
}

replace_binaries() {
  CURRENT_STEP="直接覆盖 release 二进制"
  echo ""
  echo "[步骤] ${CURRENT_STEP}（不创建备份）"

  local binary source_path target_path
  for binary in trade_engine pre_trade trade_signal; do
    binary_running "$binary" && die "${binary} 仍在运行，拒绝覆盖"
    source_path="${RELEASE_DIR}/${binary}"
    target_path="${ENV_DIR}/${binary}"
    echo "[复制] ${source_path} -> ${target_path}"
    cp -f -- "$source_path" "$target_path"
    [[ -x "$target_path" ]] || die "覆盖后目标不可执行: ${target_path}"
    cmp -s -- "$source_path" "$target_path" || die "覆盖后内容校验失败: ${binary}"
  done

  echo "[通过] 三个二进制已直接覆盖且内容一致"
}

confirm_logs() {
  local label="$1"
  [[ -r /dev/tty ]] || die "需要交互式终端确认 ${label} 日志"

  printf '[确认] 请确认以上 %s 日志无启动异常；输入 yes 继续: ' "$label" >/dev/tty
  local reply
  read -r reply </dev/tty || die "无法读取 ${label} 日志确认"
  [[ "$reply" == "yes" ]] || die "未确认 ${label} 日志，停止后续启动"
}

start_and_check_pmdaemon_component() {
  local binary="$1"
  local label="$2"
  local process_name="$3"
  local start_script="$4"
  local started_at

  CURRENT_STEP="启动并检查 ${label}"
  echo ""
  echo "[步骤] ${CURRENT_STEP}"
  started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  "$start_script"
  wait_for_running "$binary"

  sleep "$STARTUP_SETTLE_SECONDS"
  binary_running "$binary" || die "${label} 启动后在稳定观察期内退出"

  echo ""
  echo "[日志] ${label} 启动时间（UTC）: ${started_at}"
  echo "[日志] 请重点检查该时间之后的日志；以下显示最近 ${LOG_LINES} 行。"
  if ! timeout "${LOG_TIMEOUT_SECONDS}s" pmdaemon logs "$process_name" --lines "$LOG_LINES"; then
    die "读取 ${label} 日志失败或超时"
  fi

  binary_running "$binary" || die "${label} 在日志检查期间退出"
  confirm_logs "$label"
}

main() {
  case "${1:-}" in
    -h|--help)
      usage
      exit 0
      ;;
    "") ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
  [[ $# -eq 0 ]] || {
    usage >&2
    exit 2
  }

  preflight
  confirm_live_operation
  cd "$ENV_DIR"

  stop_component trade_engine "${INTRA_SCRIPTS_DIR}/stop_intra_trade_engine.sh"
  cancel_all_orders
  verify_no_open_orders

  stop_component pre_trade "${INTRA_SCRIPTS_DIR}/stop_intra_pre_trade.sh"
  stop_component trade_signal "${INTRA_SCRIPTS_DIR}/stop_intra_trade_signal.sh"
  replace_binaries

  start_and_check_pmdaemon_component \
    trade_engine \
    trade_engine \
    "$TRADE_ENGINE_PROC" \
    "${INTRA_SCRIPTS_DIR}/start_intra_trade_engine.sh"

  start_and_check_pmdaemon_component \
    pre_trade \
    pre_trade \
    "$PRE_TRADE_PROC" \
    "${INTRA_SCRIPTS_DIR}/start_intra_pre_trade.sh"

  CURRENT_STEP="确认最终状态"
  binary_running trade_engine || die "最终检查失败: trade_engine 未运行"
  binary_running pre_trade || die "最终检查失败: pre_trade 未运行"
  if binary_running trade_signal; then
    die "最终检查失败: trade_signal 不应启动"
  fi

  echo ""
  echo "[完成] ${ENV_NAME} 二进制发布完成"
  print_process_states
  echo "[完成] trade_signal 按要求保持停止"
}

main "$@"
