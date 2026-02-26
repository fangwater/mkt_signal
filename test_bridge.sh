#!/bin/bash
# Bridge 本机测试脚本
#
# 测试流程:
#   Node A 端: demo_bridge_pub -> iceoryx "order_reqs/binance"
#              -> bridge_a 读取 -> ZMQ PUSH -> bridge_b ZMQ PULL
#              -> iceoryx "order_reqs/binance" (Node B 命名空间)
#              -> demo_bridge_sub 收到消息
#
# 使用方法:
#   chmod +x test_bridge.sh && ./test_bridge.sh
#
# 需要先 cargo build: cargo build --bin ipc_bridge --bin demo_bridge_pub --bin demo_bridge_sub

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="$SCRIPT_DIR/target/debug"
CFG_A="$SCRIPT_DIR/config/bridge_test_node_a.yaml"
CFG_B="$SCRIPT_DIR/config/bridge_test_node_b.yaml"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    [ -n "$PID_BRIDGE_A" ] && kill "$PID_BRIDGE_A" 2>/dev/null || true
    [ -n "$PID_BRIDGE_B" ] && kill "$PID_BRIDGE_B" 2>/dev/null || true
    [ -n "$PID_SUB" ] && kill "$PID_SUB" 2>/dev/null || true
    wait 2>/dev/null || true
    echo -e "${GREEN}Done.${NC}"
}
trap cleanup EXIT

echo -e "${YELLOW}=== Building binaries ===${NC}"
cargo build --bin ipc_bridge --bin demo_bridge_pub --bin demo_bridge_sub 2>&1

# 检查二进制
for bin in ipc_bridge demo_bridge_pub demo_bridge_sub; do
    if [ ! -f "$BIN_DIR/$bin" ]; then
        echo -e "${RED}ERROR: $BIN_DIR/$bin not found${NC}"
        exit 1
    fi
done

echo ""
echo -e "${YELLOW}=== Step 1: Start Bridge A (node_a, bind :15555) ===${NC}"
IPC_NAMESPACE=test_a RUST_LOG=info "$BIN_DIR/ipc_bridge" --cfg "$CFG_A" &
PID_BRIDGE_A=$!
sleep 1

echo -e "${YELLOW}=== Step 2: Start Bridge B (node_b, bind :15556) ===${NC}"
IPC_NAMESPACE=test_b RUST_LOG=info "$BIN_DIR/ipc_bridge" --cfg "$CFG_B" &
PID_BRIDGE_B=$!
sleep 1

echo ""
echo -e "${YELLOW}=== Step 3: Start Subscriber on Node B side ===${NC}"
echo -e "  (subscribes to iceoryx 'order_reqs/binance' — the service bridge_b publishes to)"
# Bridge B 收到 ZMQ 消息后会 publish 到本地 iceoryx "order_reqs/binance"
# 注意: bridge 的 iceoryx 模块用 open_or_create 不带 namespace，
# 但 route 的 to.service 会经过 build_service_name
# 对于 "order_reqs/binance"，不以 dat_pbs/ 开头，所以会加 IPC_NAMESPACE 前缀
IPC_NAMESPACE=test_b "$BIN_DIR/demo_bridge_sub" --service "test_b/order_reqs/binance" --timeout-s 20 &
PID_SUB=$!
sleep 1

echo ""
echo -e "${YELLOW}=== Step 4: Publish messages on Node A side ===${NC}"
echo -e "  (publishes to iceoryx 'order_reqs/binance' — bridge_a subscribes to this)"
# Bridge A 的 outgoing route: from.service = "order_reqs/binance"
# build_service_name("order_reqs/binance") => "test_a/order_reqs/binance"
IPC_NAMESPACE=test_a "$BIN_DIR/demo_bridge_pub" --service "test_a/order_reqs/binance" --count 10 --interval-ms 300

echo ""
echo -e "${YELLOW}=== Waiting for subscriber to finish ===${NC}"
wait "$PID_SUB" 2>/dev/null || true

echo ""
echo -e "${GREEN}=== Test complete ===${NC}"
