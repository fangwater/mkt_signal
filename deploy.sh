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
primary_ip=103.141.183.151 
secondary_ip=103.141.183.152
user=el02
exec_dir=/home/$user/crypto_mkt

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
    ssh -o ConnectTimeout=$SSH_TIMEOUT $user@$ip "chown -R $user:$user $exec_dir"
    check_status "设置目录权限在 $ip"
done

# 将mkt_proxy二进制文件拷贝到两台机器上, 并设置权限
log "开始部署二进制文件..."
for ip in $primary_ip $secondary_ip; do
    log "部署到服务器 $ip..."

    scp -o ConnectTimeout=$SSH_TIMEOUT target/release/mkt_proxy $user@$ip:$exec_dir
    check_status "文件传输到 $ip"
    
    ssh -o ConnectTimeout=$SSH_TIMEOUT $user@$ip "chmod 755 $exec_dir/mkt_proxy"
    check_status "设置文件权限在 $ip"
done

log "开始部署start_proxy.sh和stop_proxy.sh文件..."
for ip in $primary_ip $secondary_ip; do
    log "部署到服务器 $ip..."
    scp -o ConnectTimeout=$SSH_TIMEOUT start_proxy.sh stop_proxy.sh $user@$ip:$exec_dir
    ssh -o ConnectTimeout=$SSH_TIMEOUT $user@$ip "chmod +x $exec_dir/start_proxy.sh $exec_dir/stop_proxy.sh"
    check_status "文件传输到 $ip"
done

log "部署完成！"







