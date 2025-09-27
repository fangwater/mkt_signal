#!/bin/bash

# 候选的exchange list 
exchange_list=(
    "binance"
    "okex"
    "bybit"
)

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

# 启动单个代理的函数
start_single_proxy() {
    local config=$1
    local pm2_name="mkt_pub_${config}"
    local bin_path="${DIR}/mkt_pub"
    local env_file="${DIR}/env.sh"

    echo "启动 ${config} 代理..."

    # 停止现有进程
    npx pm2 delete "$pm2_name" 2>/dev/null

    if [[ -f "$env_file" ]]; then
        # shellcheck disable=SC1090
        source "$env_file"
    fi

    # 启动新进程
    npx pm2 start "$bin_path" \
        --name "$pm2_name" \
        -- \
        --exchange "$config"
        
    if [ $? -eq 0 ]; then
        echo "✓ ${config} 代理启动成功"
    else
        echo "✗ ${config} 代理启动失败"
        return 1
    fi
}

# 主启动函数
start_exchange_proxies() {
    local exchange=$1
    local configs=$(get_exchange_configs "$exchange")
    
    echo "开始启动 ${exchange} 交易所的代理服务..."
    echo "将启动以下配置: $configs"
    echo ""
    
    for config in $configs; do
        start_single_proxy "$config"
        sleep 2
    done
    
    echo ""
    echo "${exchange} 交易所所有代理启动完成！"
    echo ""
    echo "查看日志命令:"
    for config in $configs; do
        echo "  npx pm2 logs mkt_pub_${config}"
    done
    
    echo ""
    echo "查看状态: npx pm2 status"
}

start_exchange_proxies "$exchange"
