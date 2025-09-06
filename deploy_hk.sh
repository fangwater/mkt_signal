#!/bin/bash

# 设置错误时立即退出
set -e

# 设置SSH超时时间
SSH_TIMEOUT=10

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# 检查命令执行状态
check_status() {
    if [ $? -eq 0 ]; then
        log "✅ $1 成功"
    else
        log "❌ $1 失败"
        exit 1
    fi
}

#在两台机器上部署行情stream进程的二进制

# 1、在当前机器上，编译rust项目，获得stream二进制文件
log "开始编译项目..."
cargo build --release -j 2
check_status "项目编译"

ip_list=(68.64.176.133)
user=root
exec_dir=/$user/crypto_mkt

# 检查SSH连接
log "检查SSH连接..."
for ip in $primary_ip $secondary_ip; do
    ssh -o ConnectTimeout=$SSH_TIMEOUT $user@$ip "echo 'SSH连接成功'" > /dev/null 2>&1
    check_status "SSH连接到 $ip"
done

# 检查exec_dir目录是否存在
for ip in $primary_ip $secondary_ip; do
    ssh -o ConnectTimeout=$SSH_TIMEOUT $user@$ip "if [ ! -d $exec_dir ]; then mkdir -p $exec_dir; fi"
    check_status "检查目录在 $ip"
done

# 部署mkt_signal二进制文件和配置文件
log "开始部署mkt_signal二进制文件..."
for ip in ${ip_list[@]}; do
    log "部署到服务器 $ip..."

    scp -o ConnectTimeout=$SSH_TIMEOUT target/release/mkt_signal $user@$ip:$exec_dir
    check_status "二进制文件传输到 $ip"
    
    ssh -o ConnectTimeout=$SSH_TIMEOUT $user@$ip "chmod 755 $exec_dir/mkt_signal"
    check_status "设置文件权限在 $ip"
done

log "开始部署配置文件..."
for ip in ${ip_list[@]}; do
    log "部署配置文件到服务器 $ip..."
    scp -o ConnectTimeout=$SSH_TIMEOUT mkt_cfg.yaml $user@$ip:$exec_dir
    check_status "配置文件传输到 $ip"
done

log "部署完成！"







