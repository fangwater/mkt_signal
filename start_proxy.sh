#!/bin/bash

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

# pm2 进程名
PM2_NAME="mkt_proxy_${target_exchange}"

# 停止现有进程
echo "停止现有进程..."
pm2 delete "$PM2_NAME" 2>/dev/null

# 启动顺序函数
start_mkt_proxy() {
    echo "启动 mkt_proxy..."
    pm2 start ./mkt_proxy \
        --name "$PM2_NAME" \
        -- \
        --config ${target_exchange}_mkt_cfg.yaml
    # 设置日志路径
    sleep 1
    pm2 describe "$PM2_NAME"
    echo "服务启动完成，日志可用 pm2 logs $PM2_NAME 查看"
    pm2 logs "$PM2_NAME" --lines 20
}

start_mkt_proxy

echo ""
echo "服务启动完成，日志可用 pm2 logs $PM2_NAME 查看"