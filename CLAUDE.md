# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是一个用 Rust 编写的加密货币市场数据代理服务，连接多个交易所（币安、OKEx、Bybit）并通过 ZeroMQ 转发市场数据。系统处理 WebSocket 连接，具有特定交易所的心跳机制，并提供高吞吐量的市场数据流。

## 核心架构

- **连接管理**: `/src/connection/` - 各交易所特定的 WebSocket 连接处理器，包含自定义心跳逻辑
- **市场数据处理**: `/src/proxy.rs` - 聚合增量和交易数据的中央代理
- **ZeroMQ 转发**: `/src/forwarder.rs` - 通过 ZeroMQ socket 处理市场数据分发
- **配置管理**: `/src/cfg.rs` - 基于 YAML 的多交易所配置管理
- **消息解析**: `/src/parser/` - 各交易所特定的消息格式处理

## 开发命令

### 构建和运行
```bash
# 构建发布版本
cargo build --release

# 使用特定交易所配置运行
./target/release/mkt_proxy --config binance-futures_mkt_cfg.yaml
```

### 进程管理（通过 PM2）
```bash
# 启动指定交易所的代理
./start_proxy.sh <exchange>

# 停止指定交易所的代理
./stop_proxy.sh <exchange>

# 支持的交易所: binance, binance-futures, okex-swap, okex, bybit, bybit-spot
```

### 部署
```bash
# 部署到生产服务器
./deploy_hk.sh  # 部署到香港服务器
./deploy_jp.sh  # 部署到日本服务器
```

## 配置系统

系统使用 YAML 配置文件（`mkt_cfg.yaml`）包含交易所特定的代理设置：
- **主备设置**: `is_primary` 标志用于高可用性
- **重启调度**: `restart_duration_secs` 用于定期连接刷新
- **快照管理**: `snapshot_requery_time` 用于深度快照同步
- **ZeroMQ 端点**: 交易所特定的 IPC 路径和网络地址

## 交易所特定实现

每个交易所都需要自定义的 WebSocket 处理，因为心跳协议不同：
- **币安**: 服务器发起的 ping，需要回显 payload（3分钟间隔）
- **OKEx**: 客户端发起的 ping/pong，30秒超时逻辑
- **Bybit**: 固定20秒客户端心跳，带请求/响应跟踪

## 主要功能

- **高可用性**: 主备节点配置，自动故障转移
- **定时重启**: 可配置重启间隔，维护连接稳定性
- **市场数据类型**: 处理增量更新和交易数据流
- **快照修正**: 定期深度快照查询，确保数据一致性（仅币安）
- **进程隔离**: 每个交易所独立进程，故障隔离