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
cargo build --release
check_status "项目编译"

# 2、给定两台机器的ip，并指定primary_ip和secondary_ip
primary_ip=38.55.198.59
secondary_ip=68.64.176.133

# 检查SSH连接
log "检查SSH连接..."
for ip in $primary_ip $secondary_ip; do
    ssh -o ConnectTimeout=$SSH_TIMEOUT root@$ip "echo 'SSH连接成功'" > /dev/null 2>&1
    check_status "SSH连接到 $ip"
done

# 3 确定两台机器上，都有crypto_mkt用户
log "创建目录并设置权限..."
for ip in $primary_ip $secondary_ip; do
    log "处理服务器 $ip..."
    
    # 检查crypto_mkt用户是否存在
    ssh -o ConnectTimeout=$SSH_TIMEOUT root@$ip "id crypto_mkt" > /dev/null 2>&1 || {
        log "创建crypto_mkt用户..."
        ssh -o ConnectTimeout=$SSH_TIMEOUT root@$ip "useradd -m crypto_mkt"
    }
    check_status "服务器 $ip 目录设置"
done

# 4、将stream二进制文件拷贝到两台机器上, 并设置权限
log "开始部署二进制文件..."
for ip in $primary_ip $secondary_ip; do
    log "部署到服务器 $ip..."
    scp -o ConnectTimeout=$SSH_TIMEOUT target/release/crypto_proxy root@$ip:/home/crypto_mkt
    check_status "文件传输到 $ip"
    
    ssh -o ConnectTimeout=$SSH_TIMEOUT root@$ip "chmod 755 /home/crypto_mkt/crypto_proxy"
    check_status "设置文件权限在 $ip"
done

log "部署完成！"







