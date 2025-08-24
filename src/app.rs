use crate::cfg::Config;
use crate::connection::mkt_manager::MktDataConnectionManager;
use crate::connection::kline_manager::KlineDataConnectionManager;
use crate::connection::derivatives_metrics_manager::DerivativesMetricsDataConnectionManager;
use crate::forwarder::ZmqForwarder;
use crate::proxy::Proxy;
use crate::restart_checker::RestartChecker;

use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, interval};
use tokio_util::sync::CancellationToken;
use tokio::signal;
use bytes::Bytes;
use log::{info, error};
use anyhow::Result;

pub struct CryptoProxyApp {
    // 连接管理器（根据配置选择性初始化）
    mkt_manager: Option<MktDataConnectionManager>,
    kline_manager: Option<KlineDataConnectionManager>,
    derivatives_manager: Option<DerivativesMetricsDataConnectionManager>,
    
    // 代理
    proxy_handle: Option<JoinHandle<()>>,
    
    // 信号和通道管理
    global_shutdown_tx: watch::Sender<bool>,
    proxy_shutdown_tx: watch::Sender<bool>,
    cancellation_token: CancellationToken,
    
    // 统一广播通道
    unified_tx: broadcast::Sender<Bytes>,
    
    // 重启检查器
    restart_checker: RestartChecker,
    
    // 配置
    config: &'static Config,
}

impl CryptoProxyApp {
    pub async fn new(config: &'static Config) -> Result<Self> {
        info!("Initializing CryptoProxyApp for exchange: {}", config.get_exchange());
        
        // 创建通道
        let (global_shutdown_tx, _) = watch::channel(false);
        let (proxy_shutdown_tx, _) = watch::channel(false);
        let (unified_tx, _) = broadcast::channel(8192);
        let cancellation_token = CancellationToken::new();
        
        // 创建重启检查器
        let restart_checker = RestartChecker::new(config.is_primary, config.restart_duration_secs);
        
        // 所有管理器都强制启动，直接传递统一的广播发送器
        info!("Initializing market data manager");
        let mkt_manager = Some(MktDataConnectionManager::new(config, &global_shutdown_tx, unified_tx.clone()).await);
        
        info!("Initializing kline data manager");
        let kline_manager = Some(KlineDataConnectionManager::new(config, &global_shutdown_tx, unified_tx.clone()).await);
        
        // 只为衍生品交易所初始化 derivatives metrics manager
        let derivatives_manager = match config.get_exchange().as_str() {
            "binance-futures" | "okex-swap" | "bybit" => {
                info!("Initializing derivatives metrics manager for {}", config.get_exchange());
                Some(DerivativesMetricsDataConnectionManager::new(config, &global_shutdown_tx, unified_tx.clone()).await)
            },
            _ => {
                info!("Skipping derivatives metrics manager for spot exchange: {}", config.get_exchange());
                None
            }
        };
        
        Ok(Self {
            mkt_manager,
            kline_manager,
            derivatives_manager,
            proxy_handle: None,
            global_shutdown_tx,
            proxy_shutdown_tx,
            cancellation_token,
            unified_tx,
            restart_checker,
            config,
        })
    }
    
    pub async fn run(mut self) -> Result<()> {
        info!("Starting CryptoProxyApp...");
        
        // 设置信号处理
        self.setup_signal_handlers().await;
        
        // 启动所有服务
        self.start_services().await?;
        
        // 主事件循环
        self.main_event_loop().await
    }
    
    async fn setup_signal_handlers(&self) {
        let token_ctrl_c = self.cancellation_token.clone();
        let token_sigterm = self.cancellation_token.clone();
        
        // 处理 Ctrl+C 信号
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.expect("Failed to wait for Ctrl+C");
            info!("SIGINT (Ctrl+C) received");
            token_ctrl_c.cancel();
        });
        
        // 处理 SIGTERM 信号
        tokio::spawn(async move {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
            sigterm.recv().await.expect("Failed to wait for SIGTERM");
            info!("SIGTERM received");
            token_sigterm.cancel();
        });
    }
    
    async fn start_services(&mut self) -> Result<()> {
        info!("Starting all services...");
        
        // 启动代理
        self.start_proxy().await?;
        
        // 启动所有连接
        self.start_all_connections().await;
        
        info!("All services started successfully");
        Ok(())
    }
    
    async fn start_proxy(&mut self) -> Result<()> {
        info!("Starting proxy...");
        
        let proxy_shutdown_rx = self.proxy_shutdown_tx.subscribe();
        let tp_reset_notify = self.get_tp_reset_notify();
        let unified_rx = self.unified_tx.subscribe();
        let forwarder = ZmqForwarder::new(self.config)?;
        
        let proxy_handle = tokio::spawn(async move {
            let mut proxy = Proxy::new(forwarder, unified_rx, proxy_shutdown_rx, tp_reset_notify);
            proxy.run().await;
        });
        
        self.proxy_handle = Some(proxy_handle);
        info!("Proxy started successfully");
        Ok(())
    }
    
    async fn start_all_connections(&mut self) {
        info!("Starting all connections...");
        
        if let Some(ref mut manager) = self.mkt_manager {
            info!("Starting market data connections");
            manager.start_all_connections().await;
        }
        
        if let Some(ref mut manager) = self.kline_manager {
            info!("Starting kline data connections");
            manager.start_all_kline_connections().await;
        }
        
        if let Some(ref mut manager) = self.derivatives_manager {
            info!("Starting derivatives metrics connections");
            manager.start_all_derivatives_connections().await;
        }
        
        info!("All connections started");
    }
    
    async fn main_event_loop(&mut self) -> Result<()> {
        let mut next_restart_instant = self.restart_checker.get_next_restart_instant();
        info!("Next restart instant: {:?}", next_restart_instant);
        
        let mut log_interval = interval(Duration::from_secs(3));
        info!("Exchange: {}", self.config.get_exchange());
        
        while !self.cancellation_token.is_cancelled() {
            tokio::select! {
                _ = log_interval.tick() => {
                    let now_instant = Instant::now();
                    info!("{}", self.format_status_table(
                        next_restart_instant.duration_since(now_instant).as_secs()
                    ));
                }
                _ = tokio::time::sleep_until(next_restart_instant) => {
                    next_restart_instant += Duration::from_secs(self.restart_checker.restart_duration_secs) * 2;
                    if let Err(e) = self.perform_restart("scheduled").await {
                        error!("Failed to perform scheduled restart: {}", e);
                    }
                }
            }
        }
        
        info!("Shutdown signal received");
        self.shutdown().await
    }
    
    async fn perform_restart(&mut self, reason: &str) -> Result<()> {
        info!("Triggering {} restart...", reason);
        
        // 发送关闭信号给所有任务
        let _ = self.global_shutdown_tx.send(true);
        
        // 关闭所有连接
        self.shutdown_all_connections().await?;
        
        // 重置关闭信号
        let _ = self.global_shutdown_tx.send(false);
        
        // 更新订阅消息
        self.update_all_subscribe_msgs().await?;
        
        // 重新启动所有连接
        self.start_all_connections().await;
        info!("Restarting all connections successfully");
        
        Ok(())
    }
    
    async fn shutdown_all_connections(&mut self) -> Result<()> {
        if let Some(ref mut manager) = self.mkt_manager {
            if let Err(e) = manager.shutdown(&self.global_shutdown_tx).await {
                error!("Failed to shutdown MktConnectionManager: {}", e);
                return Err(anyhow::anyhow!("MktConnectionManager shutdown failed"));
            }
        }
        
        if let Some(ref mut manager) = self.kline_manager {
            if let Err(e) = manager.shutdown(&self.global_shutdown_tx).await {
                error!("Failed to shutdown KlineConnectionManager: {}", e);
                return Err(anyhow::anyhow!("KlineConnectionManager shutdown failed"));
            }
        }
        
        if let Some(ref mut manager) = self.derivatives_manager {
            if let Err(e) = manager.shutdown(&self.global_shutdown_tx).await {
                error!("Failed to shutdown DerivativesMetricsConnectionManager: {}", e);
                return Err(anyhow::anyhow!("DerivativesMetricsConnectionManager shutdown failed"));
            }
        }
        
        info!("All connection managers shutdown successfully");
        Ok(())
    }
    
    async fn update_all_subscribe_msgs(&mut self) -> Result<()> {
        if let Some(ref mut manager) = self.mkt_manager {
            if let Err(e) = manager.update_subscribe_msgs().await {
                error!("Failed to update subscribe msgs for MktConnectionManager: {}", e);
                return Err(anyhow::anyhow!("MktConnectionManager update_subscribe_msgs failed"));
            }
        }
        
        if let Some(ref mut manager) = self.kline_manager {
            if let Err(e) = manager.update_subscribe_msgs().await {
                error!("Failed to update subscribe msgs for KlineConnectionManager: {}", e);
                return Err(anyhow::anyhow!("KlineConnectionManager update_subscribe_msgs failed"));
            }
        }
        
        if let Some(ref mut manager) = self.derivatives_manager {
            if let Err(e) = manager.update_subscribe_msgs().await {
                error!("Failed to update subscribe msgs for DerivativesMetricsConnectionManager: {}", e);
                return Err(anyhow::anyhow!("DerivativesMetricsConnectionManager update_subscribe_msgs failed"));
            }
        }
        
        info!("Update subscribe msgs successfully");
        Ok(())
    }
    
    async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down application...");
        
        // 关闭所有连接
        self.shutdown_all_connections().await?;
        
        // 关闭代理
        let _ = self.proxy_shutdown_tx.send(true);
        if let Some(handle) = self.proxy_handle.take() {
            let _ = handle.await;
        }
        
        info!("Application shutdown completed");
        Ok(())
    }
    
    fn get_tp_reset_notify(&self) -> std::sync::Arc<tokio::sync::Notify> {
        // 优先从市场数据管理器获取
        if let Some(ref manager) = self.mkt_manager {
            return manager.get_tp_reset_notify();
        }
        
        // 如果没有市场数据管理器，创建一个新的
        std::sync::Arc::new(tokio::sync::Notify::new())
    }
    
    fn format_status_table(&self, next_restart: u64) -> String {
        format!("\n\
             |-------------------------|-------------|\n\
             | Next Restart            | {:>10}s     |\n\
             |-------------------------|-------------|",
            next_restart
        )
    }
}