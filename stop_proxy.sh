#!/bin/bash

# 停止脚本 - 按顺序停止三个进程
# 候选的exchange list 
exchange_list=(
    "binance"
    "binance-futures"
    "okex-swap"
    "okex"
    "bybit"
    "bybit-spot"
)

target_exchange=$1

if [ -z "$target_exchange" ]; then
    echo "请提供一个交易所名称作为参数"
    exit 1
fi

if ! echo "${exchange_list[@]}" | grep -wq "$target_exchange"; then
    echo "无效的交易所名称: $target_exchange"
    exit 1
fi

# 停止脚本 
# pm2 进程名
PM2_NAME="mkt_subscriber_${target_exchange}"

# 停止现有进程
echo "停止现有进程..."
pm2 delete "$PM2_NAME" 2>/dev/null

# 确认是否停止成功
if [ $? -eq 0 ]; then
    echo "停止 $PM2_NAME 完成"
else
    echo "停止 $PM2_NAME 失败"
fi

echo "停止 $PM2_NAME 完成"

# 启动脚本 - 按顺序启动三个进程

# 如果logs不存在，先创建
# if [ ! -d "./logs" ]; then
#     mkdir -p "./logs"
# fi

# stop_process() {
#     local name=$1
#     local signal=$2
#     local pids=$(pgrep -f "$name")
    
#     if [ -n "$pids" ]; then
#         echo "向 $name 发送 $signal 信号..."
#         pkill -$signal -f "$name"
#         return 0
#     fi
#     return 1
# }

# stop_mkt_subscriber() {
#     stop_process "mkt_subscriber --config ${target_exchange}_mkt_cfg.yaml --kafka_producer_cfg_path kafka_cfg_producer.json" TERM
# }

# force_kill() {
#     local name=$1
#     local pids=$(pgrep -f "$name")
    
#     if [ -n "$pids" ]; then
#         echo "强制停止 $name..."
#         pkill -9 -f "$name"
#     fi
# }

# # 检查进程是否运行（带参数检查）
# check_process() {
#     local process_name=$1
#     local config_pattern="${target_exchange}_mkt_cfg.yaml"
#     local pids=$(pgrep -f "$process_name --config $config_pattern --kafka_producer_cfg_path kafka_cfg_producer.json")
    
#     if [ -n "$pids" ]; then
#         echo "$process_name (${target_exchange}) 正在运行 (PID: $pids)"
#         return 0
#     else
#         echo "$process_name (${target_exchange}) 未运行"
#         return 1
#     fi
# }

# # 强制停止特定进程（带参数）
# force_kill_specific() {
#     local process_name=$1
#     local config_pattern="${target_exchange}_mkt_cfg.yaml"
#     local pids=$(pgrep -f "$process_name --config $config_pattern --kafka_producer_config kafka_cfg_producer.json")
    
#     if [ -n "$pids" ]; then
#         echo "强制停止 $process_name (${target_exchange})..."
#         pkill -9 -f "$process_name --config $config_pattern"
#         return 0
#     fi
#     return 1
# }

# # 主停止流程 - 按指定顺序
# echo "开始停止 ${target_exchange} 相关进程..."
# stop_mkt_subscriber && sleep 2

# # 检查并强制停止残留进程（带参数检查）
# echo "检查残留进程..."
# for process in "mkt_subscriber"; do
#     if check_process "$process"; then
#         echo "$process (${target_exchange}) 仍然在运行，尝试强制停止..."
#         force_kill_specific "$process"
#     fi
# done

# # 最终检查
# echo "最终状态检查:"
# for process in "mkt_subscriber"; do
#     if check_process "$process"; then
#         echo "警告: $process (${target_exchange}) 可能未完全停止"
#     else
#         echo "$process (${target_exchange}) 已停止"
#     fi
# done

# echo "停止脚本执行完成"