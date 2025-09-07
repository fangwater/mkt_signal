use crate::cfg::Config;
use crate::connection::mkt_manager::{MktManager, MessageQueues};
use crate::iceoryx_forwarder::IceOryxForwarder;
use crate::proxy::MpscProxy;

use tokio::sync::{watch, mpsc};
use tokio::time::{Duration, Instant, interval};
use tokio_util::sync::CancellationToken;
use tokio::signal;
use log::{info, error};
use anyhow::Result;
use bytes::Bytes;
use tokio::task::JoinHandle;

pub struct MktSignalApp {
    // 信号和通道管理
    global_shutdown_tx: watch::Sender<bool>,
    proxy_shutdown_tx: watch::Sender<bool>,
    cancellation_token: CancellationToken,
    
    // 代理（不再使用句柄，直接保存proxy）
    proxy: Option<MpscProxy>,
    proxy_handle: Option<JoinHandle<()>>, // 后台运行的 proxy 任务句柄
    
    // 主备两个市场数据管理器
    primary_mkt_manager: Option<MktManager>,
    secondary_mkt_manager: Option<MktManager>,
    
    // 重启检查器
    
    // 主备管理器的下次重启时间
    next_primary_restart: Instant,
    next_secondary_restart: Instant,
    
    // 消息队列发送端（用于重启时复用）
    incremental_tx: mpsc::UnboundedSender<Bytes>,
    trade_tx: mpsc::UnboundedSender<Bytes>,
    kline_tx: mpsc::UnboundedSender<Bytes>,
    derivatives_tx: mpsc::UnboundedSender<Bytes>,
    signal_tx: mpsc::UnboundedSender<Bytes>,
    ask_bid_spread_tx: mpsc::UnboundedSender<Bytes>,
    
    // 配置
    config: &'static Config,
}

impl MktSignalApp {
    pub async fn new(config: &'static Config) -> Result<Self> {
        info!("Initializing MktSignalApp for exchange: {}", config.get_exchange());
        
        // 创建通道
        let (global_shutdown_tx, _) = watch::channel(false);
        let (proxy_shutdown_tx, _) = watch::channel(false);
        let cancellation_token = CancellationToken::new();
        
        // 创建重启检查器
        
        // 计算主备的初始重启时间
        // 主：从现在开始等待一个完整的重启周期
        // 备：错开半个周期
        let restart_duration = Duration::from_secs(config.restart_duration_secs);
        let next_primary_restart = Instant::now() + restart_duration;
        let next_secondary_restart = Instant::now() + restart_duration / 2;
        
        // 创建消息队列（这些通道将被所有管理器共享）
        let message_queues = MessageQueues::new();
        
        Ok(Self {
            global_shutdown_tx,
            proxy_shutdown_tx,
            cancellation_token,
            proxy: None,
            proxy_handle: None,
            primary_mkt_manager: None,
            secondary_mkt_manager: None,
            next_primary_restart,
            next_secondary_restart,
            incremental_tx: message_queues.incremental_tx,
            trade_tx: message_queues.trade_tx,
            kline_tx: message_queues.kline_tx,
            derivatives_tx: message_queues.derivatives_tx,
            signal_tx: message_queues.signal_tx,
            ask_bid_spread_tx: message_queues.ask_bid_spread_tx,
            config,
        })
    }
    
    pub async fn run(mut self) -> Result<()> {
        info!("Starting MktSignalApp...");
        
        // 设置信号处理
        self.setup_signal_handlers().await;
        
        // 初始化并启动所有服务
        self.initialize_and_start().await?;
        
        // 同时运行 proxy 与主事件循环；proxy 仅在收到 shutdown 时退出
        let proxy = self.proxy.take();
        if let Some(mut proxy) = proxy {
            tokio::select! {
                _ = proxy.run() => {
                    // 一般只有在收到 shutdown 才会返回
                    info!("Proxy exited; initiating shutdown");
                    self.cancellation_token.cancel();
                    self.main_event_loop().await
                }
                result = self.main_event_loop() => {
                    result
                }
            }
        } else {
            log::warn!("Proxy was not created; running without proxy");
            self.main_event_loop().await
        }
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
    
    async fn initialize_and_start(&mut self) -> Result<()> {
        info!("Initializing and starting all services...");
        
        // 创建主市场数据管理器
        let primary_mkt_manager = MktManager::new(
            self.config,
            &self.global_shutdown_tx,
            self.incremental_tx.clone(),
            self.trade_tx.clone(),
            self.kline_tx.clone(),
            self.derivatives_tx.clone(),
            self.signal_tx.clone(),
            self.ask_bid_spread_tx.clone(),
            self.config.primary_local_ip.clone(),
        ).await;
        
        // 创建备市场数据管理器（使用相同的tx通道）
        let secondary_mkt_manager = MktManager::new(
            self.config,
            &self.global_shutdown_tx,
            self.incremental_tx.clone(),
            self.trade_tx.clone(),
            self.kline_tx.clone(),
            self.derivatives_tx.clone(),
            self.signal_tx.clone(),
            self.ask_bid_spread_tx.clone(),
            self.config.secondary_local_ip.clone(),
        ).await;
        
        // 启动代理（只在初始化时创建一次）
        if self.proxy.is_none() {
            // 创建接收端（只在第一次初始化时创建）
            let (incremental_tx, incremental_rx) = mpsc::unbounded_channel();
            let (trade_tx, trade_rx) = mpsc::unbounded_channel();
            let (kline_tx, kline_rx) = mpsc::unbounded_channel();
            let (derivatives_tx, derivatives_rx) = mpsc::unbounded_channel();
            let (signal_tx, signal_rx) = mpsc::unbounded_channel();
            let (ask_bid_spread_tx, ask_bid_spread_rx) = mpsc::unbounded_channel();
            
            // 更新发送端
            self.incremental_tx = incremental_tx;
            self.trade_tx = trade_tx;
            self.kline_tx = kline_tx;
            self.derivatives_tx = derivatives_tx;
            self.signal_tx = signal_tx;
            self.ask_bid_spread_tx = ask_bid_spread_tx;
            
            let proxy_shutdown_rx = self.proxy_shutdown_tx.subscribe();
            let tp_reset_notify = primary_mkt_manager.get_tp_reset_notify();
            let forwarder = IceOryxForwarder::new(self.config)?;
            
            let proxy = MpscProxy::new(
                forwarder,
                incremental_rx,
                trade_rx,
                kline_rx,
                derivatives_rx,
                signal_rx,
                ask_bid_spread_rx,
                proxy_shutdown_rx,
                tp_reset_notify
            );
            
            self.proxy = Some(proxy);
            info!("Proxy created successfully");
        }
        
        // 保存两个mkt_manager
        self.primary_mkt_manager = Some(primary_mkt_manager);
        self.secondary_mkt_manager = Some(secondary_mkt_manager);
        
        // 启动主备两个管理器的所有连接
        if let Some(ref mut manager) = self.primary_mkt_manager {
            info!("Starting PRIMARY market data connections...");
            manager.start_all_connections().await;
        }
        
        if let Some(ref mut manager) = self.secondary_mkt_manager {
            info!("Starting SECONDARY market data connections...");
            manager.start_all_connections().await;
        }
        
        info!("All services started successfully");
        Ok(())
    }
    
   async fn main_event_loop(&mut self) -> Result<()> {
        info!("Primary restart scheduled at: {:?}", self.next_primary_restart);
        info!("Secondary restart scheduled at: {:?}", self.next_secondary_restart);
        
        let mut log_interval = interval(Duration::from_secs(3));
        info!("Exchange: {}", self.config.get_exchange());
        
        while !self.cancellation_token.is_cancelled() {
            let now = Instant::now();
            let time_to_primary = self.next_primary_restart.saturating_duration_since(now).as_secs();
            let time_to_secondary = self.next_secondary_restart.saturating_duration_since(now).as_secs();
            
            tokio::select! {
                _ = log_interval.tick() => {
                    info!("{}", self.format_status_table_dual(time_to_primary, time_to_secondary));
                }
                _ = tokio::time::sleep_until(self.next_primary_restart) => {
                    info!("执行主管理器定时重启");
                    if let Err(e) = self.restart_primary().await {
                        error!("Failed to restart primary manager: {}", e);
                    }
                    // 更新下次主管理器重启时间
                    self.next_primary_restart = Instant::now() + Duration::from_secs(self.config.restart_duration_secs);
                }
                _ = tokio::time::sleep_until(self.next_secondary_restart) => {
                    info!("执行备管理器定时重启");
                    if let Err(e) = self.restart_secondary().await {
                        error!("Failed to restart secondary manager: {}", e);
                    }
                    // 更新下次备管理器重启时间
                    self.next_secondary_restart = Instant::now() + Duration::from_secs(self.config.restart_duration_secs);
                }
            }
        }
        
        info!("Shutdown signal received");
        self.shutdown().await
    }
    
    async fn restart_primary(&mut self) -> Result<()> {
        info!("正在重启主管理器...");
        
        // 关闭当前主管理器
        if let Some(mut manager) = self.primary_mkt_manager.take() {
            if let Err(e) = manager.shutdown(&self.global_shutdown_tx).await {
                error!("Failed to shutdown PRIMARY MktManager: {}", e);
            }
        }
        
        // 等待短暂时间确保清理完成
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // 创建新的主管理器（复用相同的通道）
        let primary_mkt_manager = MktManager::new(
            self.config,
            &self.global_shutdown_tx,
            self.incremental_tx.clone(),
            self.trade_tx.clone(),
            self.kline_tx.clone(),
            self.derivatives_tx.clone(),
            self.signal_tx.clone(),
            self.ask_bid_spread_tx.clone(),
            self.config.primary_local_ip.clone(),
        ).await;
        
        // 启动主管理器
        self.primary_mkt_manager = Some(primary_mkt_manager);
        if let Some(ref mut manager) = self.primary_mkt_manager {
            info!("Starting PRIMARY market data connections...");
            manager.start_all_connections().await;
        }
        
        info!("主管理器重启成功");
        Ok(())
    }
    
    async fn restart_secondary(&mut self) -> Result<()> {
        info!("正在重启备管理器...");
        
        // 关闭当前备管理器
        if let Some(mut manager) = self.secondary_mkt_manager.take() {
            if let Err(e) = manager.shutdown(&self.global_shutdown_tx).await {
                error!("Failed to shutdown SECONDARY MktManager: {}", e);
            }
        }
        
        // 等待短暂时间确保清理完成
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // 创建新的备管理器（复用相同的通道）
        let secondary_mkt_manager = MktManager::new(
            self.config,
            &self.global_shutdown_tx,
            self.incremental_tx.clone(),
            self.trade_tx.clone(),
            self.kline_tx.clone(),
            self.derivatives_tx.clone(),
            self.signal_tx.clone(),
            self.ask_bid_spread_tx.clone(),
            self.config.secondary_local_ip.clone(),
        ).await;
        
        // 启动备管理器
        self.secondary_mkt_manager = Some(secondary_mkt_manager);
        if let Some(ref mut manager) = self.secondary_mkt_manager {
            info!("Starting SECONDARY market data connections...");
            manager.start_all_connections().await;
        }
        
        info!("备管理器重启成功");
        Ok(())
    }
    
    async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down application...");
        
        // 关闭主市场数据管理器
        if let Some(mut manager) = self.primary_mkt_manager.take() {
            if let Err(e) = manager.shutdown(&self.global_shutdown_tx).await {
                error!("Failed to shutdown PRIMARY MktManager: {}", e);
            }
        }
        
        // 关闭备市场数据管理器
        if let Some(mut manager) = self.secondary_mkt_manager.take() {
            if let Err(e) = manager.shutdown(&self.global_shutdown_tx).await {
                error!("Failed to shutdown SECONDARY MktManager: {}", e);
            }
        }
        
        // 关闭代理
        let _ = self.proxy_shutdown_tx.send(true);
        // 等待 proxy 任务结束（如果存在）
        if let Some(handle) = self.proxy_handle.take() {
            if let Err(e) = handle.await {
                error!("Proxy task join error: {:?}", e);
            }
        }

        info!("Application shutdown completed");
        Ok(())
    }
    
    fn format_status_table_dual(&self, primary_restart: u64, secondary_restart: u64) -> String {
        format!("\n\
             |-------------------------|-------------|\n\
             | Primary Next Restart    | {:>10}s |\n\
             | Secondary Next Restart  | {:>10}s |\n\
             |-------------------------|-------------|",
            primary_restart,
            secondary_restart
        )
    }
}
