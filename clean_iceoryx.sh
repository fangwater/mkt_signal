#!/bin/bash

# Iceoryx2 清理脚本
# 用于解决 "ServiceInCorruptedState" 错误

echo "========================================="
echo "开始清理 Iceoryx2 服务..."
echo "========================================="

# 1. 杀死所有相关进程
echo "步骤 1: 终止所有相关进程..."
pkill -9 -f iceoryx 2>/dev/null
pkill -9 -f demo_send 2>/dev/null
pkill -9 -f demo_subscribe 2>/dev/null
pkill -9 -f trade_engine 2>/dev/null
pkill -9 -f crypto_proxy 2>/dev/null

# 给进程一点时间完全退出
sleep 1

# 2. 清理共享内存段
echo "步骤 2: 清理共享内存..."
ipcs -m | grep $USER | awk '{print $2}' | while read id; do
    [ -n "$id" ] && ipcrm -m $id 2>/dev/null
done

# 3. 清理信号量
echo "步骤 3: 清理信号量..."
ipcs -s | grep $USER | awk '{print $2}' | while read id; do
    [ -n "$id" ] && ipcrm -s $id 2>/dev/null
done

# 4. 清理消息队列
echo "步骤 4: 清理消息队列..."
ipcs -q | grep $USER | awk '{print $2}' | while read id; do
    [ -n "$id" ] && ipcrm -q $id 2>/dev/null
done

# 5. 清理 iceoryx2 临时文件
echo "步骤 5: 清理临时文件..."
rm -rf /tmp/iceoryx2* 2>/dev/null
rm -rf /dev/shm/iceoryx2* 2>/dev/null
rm -rf /dev/shm/*iox2* 2>/dev/null

# 如果有权限，使用 sudo 清理系统级文件
if [ "$EUID" -eq 0 ] || sudo -n true 2>/dev/null; then
    echo "步骤 6: 使用 sudo 清理系统级文件..."
    sudo rm -rf /tmp/iceoryx2* 2>/dev/null
    sudo rm -rf /dev/shm/iceoryx2* 2>/dev/null
    sudo rm -rf /dev/shm/*iox2* 2>/dev/null
else
    echo "步骤 6: 跳过系统级文件清理（需要 sudo 权限）"
fi

# 7. 验证清理结果
echo ""
echo "========================================="
echo "清理完成！检查状态："
echo "========================================="

# 检查是否还有相关进程
remaining_procs=$(pgrep -f "iceoryx|demo_send|demo_subscribe|trade_engine" 2>/dev/null)
if [ -z "$remaining_procs" ]; then
    echo "✓ 所有进程已终止"
else
    echo "⚠ 警告：仍有进程运行："
    echo "$remaining_procs"
fi

# 检查共享内存
remaining_shm=$(ipcs -m | grep $USER | wc -l)
if [ "$remaining_shm" -eq 0 ]; then
    echo "✓ 共享内存已清理"
else
    echo "⚠ 警告：还有 $remaining_shm 个共享内存段"
fi

# 检查临时文件
if [ ! -d /tmp/iceoryx2 ] && [ -z "$(ls /dev/shm/*iox* 2>/dev/null)" ]; then
    echo "✓ 临时文件已清理"
else
    echo "⚠ 警告：可能还有临时文件残留"
fi

echo ""
echo "========================================="
echo "清理完成！现在可以重新启动服务了"
echo "========================================="
echo ""
echo "使用方法："
echo "  RUST_LOG=debug cargo run --bin demo_send_um_market_short"
echo ""