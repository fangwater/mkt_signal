#!/bin/bash
# Filename: pm2_log_set.sh
# Description: 自动配置 PM2 日志轮转策略，支持普通用户和root用户
# Created: 2025-07-15
# Updated: 2025-09-23
# 
# 使用说明:
#   1. 普通用户模式 (推荐): ./pm2_log_set.sh
#   2. 全局模式 (需root): sudo ./pm2_log_set.sh (需手动设置INSTALL_MODE="global")
# 
# 功能特点:
#   - 默认仅对当前用户生效，无需root权限
#   - 自动安装pm2-logrotate模块
#   - 配置日志大小限制、保留天数、轮转计划
#   - 支持日志压缩选项

# =============================================================================
# 配置参数 (可根据需要修改)
# =============================================================================
MAX_SIZE="100M"          # 单日志文件最大尺寸 (10M, 100K, 1G)
LOG_RETAIN_DAYS=3       # 保留日志文件天数
CRON_SCHEDULE="0 0 * * *" # 每日轮转计划 (crontab格式)
ENABLE_COMPRESS=false    # 是否压缩旧日志 true/false
INSTALL_MODE="user"      # 安装模式: "user" 仅对当前用户生效, "global" 全局生效(需root权限)

# =============================================================================
# 函数定义
# =============================================================================

# 检查命令是否存在
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# 检查当前用户是否有权限执行操作
check_permissions() {
    if [[ $(id -u) -eq 0 ]]; then
        echo "[INFO] 以root身份运行，可使用全局模式"
    elif [[ "$INSTALL_MODE" == "global" ]]; then
        echo "[ERROR] 全局模式需要root权限! 请使用 sudo 运行或设置 INSTALL_MODE=\"user\"" >&2
        exit 1
    else
        echo "[INFO] 以普通用户身份运行，仅对当前用户 $(whoami) 生效"
    fi
}

# 检查PM2是否安装
check_pm2_installed() {
    if npx pm2 --version >/dev/null 2>&1; then
        echo "[SUCCESS] PM2已安装: $(npx pm2 --version)"
    else
        echo "[ERROR] PM2未安装!" >&2
        if [[ "$INSTALL_MODE" == "user" ]]; then
            echo "[提示] 对于用户模式，可以执行: npm install pm2" >&2
            echo "[提示] 或全局安装: npm install -g pm2" >&2
        else
            echo "[提示] 对于全局模式，请执行: sudo npm install -g pm2" >&2
        fi
        exit 1
    fi
}

# 安装日志模块
install_logrotate_module() {
    echo "[STEP] 安装PM2日志轮转模块..."
    
    # 检查模块是否已安装
    if npx pm2 describe pm2-logrotate >/dev/null 2>&1; then
        echo "[INFO] pm2-logrotate 模块已安装"
    else
        echo "[INFO] 正在安装 pm2-logrotate 模块..."
        if [[ "$INSTALL_MODE" == "global" && $(id -u) -eq 0 ]]; then
            npx pm2 install pm2-logrotate
        else
            npx pm2 install pm2-logrotate
        fi
        
        if [ $? -eq 0 ]; then
            echo "[SUCCESS] pm2-logrotate 模块安装成功"
            npx pm2 save
        else
            echo "[ERROR] pm2-logrotate 模块安装失败" >&2
            exit 1
        fi
    fi
}

# 配置日志参数
configure_log_settings() {
    echo "[STEP] 应用日志策略:"
    npx pm2 set pm2-logrotate:max_size "$MAX_SIZE"
    echo "  - 设置单文件上限: $MAX_SIZE"
    
    npx pm2 set pm2-logrotate:retain "$LOG_RETAIN_DAYS"
    echo "  - 设置保留天数: $LOG_RETAIN_DAYS 天"
    
    npx pm2 set pm2-logrotate:rotateInterval "$CRON_SCHEDULE"
    echo "  - 设置轮转计划: $CRON_SCHEDULE (crontab)"
    
    npx pm2 set pm2-logrotate:compress "$ENABLE_COMPRESS"
    echo "  - 压缩旧日志: $ENABLE_COMPRESS"
    
    npx pm2 set pm2-logrotate:rotateModule true
    echo "  - 应用到所有PM2进程"
    
    # 保存配置
    npx pm2 save
    echo "  - 配置已保存"
}


# =============================================================================
# 主执行流程
# =============================================================================

# Step 1: 验证依赖和权限
echo "====== PM2 日志配置脚本 ======"
check_pm2_installed
check_permissions

# Step 2: 安装模块
install_logrotate_module

# Step 3: 应用配置
configure_log_settings

# Step 4: 结果验证
echo ""
echo "====== 配置完成 ======"
echo "[SUCCESS] PM2 日志轮转配置已完成!"
echo ""
echo "[验证] 当前日志轮转设置:"
npx pm2 config | grep pm2-logrotate || echo "[WARNING] 无法获取配置信息"
echo ""
echo "[用法] 配置仅对当前用户 $(whoami) 的 PM2 实例生效"
echo "[提示] 可以使用 'npx pm2 logs' 命令查看日志"
echo "[提示] 日志文件位置: ~/.pm2/logs/"
echo "[提示] 要查看模块状态: npx pm2 describe pm2-logrotate"