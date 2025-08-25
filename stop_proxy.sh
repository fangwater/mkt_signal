#!/bin/bash

# 停止脚本 - 停止指定交易所的所有代理
# 候选的exchange list 
exchange_list=(
    "binance"
    "okex"
    "bybit"
)

exchange=$1

if [ -z "$exchange" ]; then
    echo "请提供一个交易所名称作为参数"
    echo "支持的交易所: ${exchange_list[*]}"
    exit 1
fi

if ! echo "${exchange_list[@]}" | grep -wq "$exchange"; then
    echo "无效的交易所名称: $exchange"
    echo "支持的交易所: ${exchange_list[*]}"
    exit 1
fi

# 根据交易所获取对应的合约和现货配置
get_exchange_configs() {
    local exchange=$1
    case $exchange in
        "binance")
            echo "binance binance-futures"
            ;;
        "okex")
            echo "okex okex-swap"
            ;;
        "bybit")
            echo "bybit bybit-spot"
            ;;
        *)
            echo "未知交易所: $exchange"
            exit 1
            ;;
    esac
}

# 停止单个代理的函数
stop_single_proxy() {
    local config=$1
    local pm2_name="crypto_proxy_${config}"
    
    echo "停止 ${config} 代理..."
    
    pm2 delete "$pm2_name" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "✓ ${config} 代理停止成功"
    else
        echo "✗ ${config} 代理停止失败（可能本来就没运行）"
    fi
}

# 主停止函数
stop_exchange_proxies() {
    local exchange=$1
    local configs=$(get_exchange_configs "$exchange")
    
    echo "开始停止 ${exchange} 交易所的代理服务..."
    echo "将停止以下配置: $configs"
    echo ""
    
    for config in $configs; do
        stop_single_proxy "$config"
        sleep 1
    done
    
    echo ""
    echo "${exchange} 交易所所有代理停止完成！"
    echo ""
    echo "查看状态: pm2 status"
}

stop_exchange_proxies "$exchange"
