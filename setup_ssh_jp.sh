#!/bin/bash 

# SSH配置脚本 - 日本服务器 
# 使用方法: ./setup_ssh_jp.sh 

set -e 

# 配置信息 
PEM_FILE="/Users/fanghaizhou/project/mkt_signal/aws-jp-srv-1.pem" 
SSH_CONFIG_FILE="$HOME/.ssh/config" 
HOST_ALIAS="jp-server" 
HOST_IP="54.64.147.69" 
USER="ubuntu" 

echo "开始配置 SSH 免密登录..." 

# 1. 检查 PEM 文件是否存在 
if [ ! -f "$PEM_FILE" ]; then 
    echo "❌ PEM 文件不存在: $PEM_FILE" 
    exit 1  
fi 

# 2. 设置 PEM 文件权限
echo "设置 PEM 文件权限..."
chmod 400 "$PEM_FILE"
echo "✅ PEM 文件权限已设置为 400"

# 3. 创建 .ssh 目录（如果不存在）
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"

# 4. 备份现有 SSH 配置（如果存在）
if [ -f "$SSH_CONFIG_FILE" ]; then
    BACKUP_FILE="$SSH_CONFIG_FILE.backup.$(date +%Y%m%d_%H%M%S)"
    cp "$SSH_CONFIG_FILE" "$BACKUP_FILE"
    echo "✅ 已备份现有配置到: $BACKUP_FILE"
fi

# 5. 检查配置是否已存在
if grep -q "Host $HOST_ALIAS" "$SSH_CONFIG_FILE" 2>/dev/null; then
    echo "⚠️  配置 '$HOST_ALIAS' 已存在，是否覆盖? (y/n)"
    read -r answer
    if [ "$answer" != "y" ]; then
        echo "跳过配置更新"
    else
        # 删除旧配置
        sed -i.tmp "/Host $HOST_ALIAS/,/^$/d" "$SSH_CONFIG_FILE"
    fi
fi

# 6. 添加新的 SSH 配置
echo "" >> "$SSH_CONFIG_FILE"
echo "# 日本服务器配置" >> "$SSH_CONFIG_FILE"
echo "Host $HOST_ALIAS" >> "$SSH_CONFIG_FILE"
echo "    HostName $HOST_IP" >> "$SSH_CONFIG_FILE"
echo "    User $USER" >> "$SSH_CONFIG_FILE"
echo "    IdentityFile $PEM_FILE" >> "$SSH_CONFIG_FILE"
echo "    Port 22" >> "$SSH_CONFIG_FILE"
echo "    StrictHostKeyChecking no" >> "$SSH_CONFIG_FILE"
echo "    UserKnownHostsFile /dev/null" >> "$SSH_CONFIG_FILE"
echo "" >> "$SSH_CONFIG_FILE"

echo "✅ SSH 配置已添加到 $SSH_CONFIG_FILE"

# 7. 测试连接
echo ""
echo "正在测试 SSH 连接..."
if ssh -o ConnectTimeout=5 $HOST_ALIAS "echo '✅ SSH 连接成功!'" 2>/dev/null; then
    echo ""
    echo "========================================="
    echo "配置完成！现在你可以使用以下命令连接："
    echo ""
    echo "  ssh $HOST_ALIAS"
    echo ""
    echo "或者直接使用 PEM 文件："
    echo "  ssh -i $PEM_FILE $USER@$HOST_IP"
    echo ""
    echo "SCP 示例："
    echo "  scp file.txt $HOST_ALIAS:/path/to/destination/"
    echo "========================================="
else
    echo ""
    echo "⚠️  SSH 连接测试失败，请检查："
    echo "1. PEM 文件路径是否正确"
    echo "2. 服务器 IP 是否正确"
    echo "3. 用户名是否正确"
    echo "4. 服务器是否在线"
    echo ""
    echo "你仍然可以尝试手动连接："
    echo "  ssh -i $PEM_FILE $USER@$HOST_IP"
fi