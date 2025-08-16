use crate::cfg::Config;
use crate::sub_msg::SubscribeMsgs;
use crate::parser::default_parser::{Parser};
use crate::parser::binance_parser::{BinanceSignalParser, BinanceTradeParser, BinanceSnapshotParser, BinanceIncParser};
use crate::parser::okex_parser::{OkexSignalParser, OkexTradeParser, OkexIncParser};
use crate::parser::bybit_parser::{BybitSignalParser, BybitTradeParser, BybitIncParser};
use crate::connection::connection::construct_connection;
use crate::connection::binance_conn::BinanceFuturesSnapshotQuery;
use tokio::sync::{broadcast, watch, Notify};
use tokio::task::JoinSet;
use bytes::Bytes;
use log::{info, error};
use std::sync::Arc;
use tokio::time::{Duration, Instant};
use chrono::{Utc, TimeDelta, NaiveTime};

//订阅逐笔行情，orderbook增量消息，通过parser处理后转发

pub fn next_target_instant(time_str: &str) -> Instant {
    if time_str == "--:--:--" {
        log::warn!("Using fallback time + 30 seconds from now");
        return Instant::now() + Duration::from_secs(5);
    }

    // 解析时间字符串
    let target_time = match NaiveTime::parse_from_str(time_str, "%H:%M:%S") {
        Ok(time) => time,
        Err(_) => {
            log::warn!("Invalid time format '{}', using 00:00:00", time_str);
            NaiveTime::from_hms_opt(0, 0, 0).unwrap()
        }
    };

    // 获取当前UTC时间
    let now = Utc::now();
    let now_naive = now.naive_utc();
    
    // 构造今天的目标时间
    let today_target = now.date_naive().and_time(target_time);
    
    // 计算目标时间（如果已过今天就取明天）
    let target = if now_naive < today_target {
        today_target
    } else {
        today_target + TimeDelta::days(1)
    };
    
    // 计算需要等待的时间长度
    let sleep_duration = target - now_naive;
    
    // 转换为std::time::Duration
    let sleep_std = sleep_duration.to_std().unwrap();
    
    // 返回目标时刻的tokio Instant
    Instant::now() + sleep_std
}

pub struct MktDataConnectionManager {
    cfg: Config, //进程基本参数
    subscribe_msgs: SubscribeMsgs, //所有的订阅消息
    mkt_tx: broadcast::Sender<Bytes>, //行情消息转发通道（包含signal）
    global_shutdown_rx: watch::Receiver<bool>, //全局关闭信号
    tp_reset_notify: Arc<Notify>, //tp重置消息通知
    join_set: JoinSet<()>, //任务集合
}


impl MktDataConnectionManager {
    pub async fn new(cfg: &Config, global_shutdown: &watch::Sender<bool>) -> Self {
        let (mkt_tx, _) = broadcast::channel(1000);
        let subscribe_msgs = SubscribeMsgs::new(&cfg).await;
        Self {
            cfg: cfg.clone(),
            subscribe_msgs : subscribe_msgs,
            mkt_tx: mkt_tx,
            global_shutdown_rx: global_shutdown.subscribe(),
            tp_reset_notify: Arc::new(Notify::new()),
            join_set: JoinSet::new(),
        }   
    }
    pub fn get_tp_reset_notify(&self) -> Arc<Notify> {
        self.tp_reset_notify.clone()
    }

    pub fn notify_tp_reset(&self) {
        self.tp_reset_notify.notify_waiters();
    }

    pub async fn update_subscribe_msgs(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        //获取之前的活跃symbol
        let prev_symbols = self.subscribe_msgs.get_active_symbols();
        let subscribe_msgs = SubscribeMsgs::new(&self.cfg).await;
        self.subscribe_msgs = subscribe_msgs;
        SubscribeMsgs::compare_symbol_set(&prev_symbols, &self.subscribe_msgs.get_active_symbols());
        Ok(())
    }

    
    pub async fn start_snapshot_task(&mut self) {
        let exchange = self.cfg.get_exchange().clone();
        let is_primary = self.cfg.is_primary;
        
        // Only start snapshot task for Binance exchanges and primary nodes
        if (exchange != "binance-futures" && exchange != "binance") || !is_primary {
            info!("跳过快照任务 - 非币安交易所或非主节点");
            return;
        }
        
        let snapshot_requery_time = match self.cfg.snapshot_requery_time.as_ref() {
            Some(time) if !time.is_empty() => time.clone(),
            _ => {
                info!("快照查询已禁用（snapshot_requery_time 为空或未配置）");
                return;
            }
        };
        
        let cfg_clone = self.cfg.clone();
        let mut global_shutdown_rx = self.global_shutdown_rx.clone();
        let mkt_tx = self.mkt_tx.clone();
        
        self.join_set.spawn(async move {
            let mut next_snapshot_query_instant = next_target_instant(&snapshot_requery_time);
            
            info!("币安快照任务已启动，下次查询时间: {:?}", next_snapshot_query_instant);
            
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(next_snapshot_query_instant) => {
                        info!("在 {:?} 查询深度快照", next_snapshot_query_instant);
                        
                        let mkt_tx_for_snapshot = mkt_tx.clone();
                        let exchange_for_snapshot = exchange.clone();
                        let cfg_for_snapshot = cfg_clone.clone();
                        tokio::spawn(async move {
                            // Create intermediate channel for snapshot data
                            let (snapshot_raw_tx, mut snapshot_raw_rx) = broadcast::channel(100);
                            let parser = BinanceSnapshotParser::new();
                            
                            // Start snapshot fetching task
                            let snapshot_tx_for_fetcher = snapshot_raw_tx.clone();
                            let exchange_for_fetcher = exchange_for_snapshot.clone();
                            let cfg_for_fetcher = cfg_for_snapshot.clone();
                            tokio::spawn(async move {
                                let symbols = match cfg_for_fetcher.get_symbols().await {
                                    Ok(symbols) => symbols,
                                    Err(e) => {
                                        error!("获取快照查询符号失败: {}", e);
                                        return;
                                    }
                                };
                                BinanceFuturesSnapshotQuery::start_fetching_depth(exchange_for_fetcher.as_str(), symbols, snapshot_tx_for_fetcher).await;
                                info!("为 {} 成功查询深度快照", exchange_for_fetcher);
                            });
                            
                            // Parse snapshot data and forward to mkt_tx
                            while let Ok(snapshot_data) = snapshot_raw_rx.recv().await {
                                let _parsed_count = parser.parse(snapshot_data, &mkt_tx_for_snapshot);
                            }
                        });
                        
                        next_snapshot_query_instant += Duration::from_secs(24 * 60 * 60);
                    }
                    _ = global_shutdown_rx.changed() => {
                        if *global_shutdown_rx.borrow() {
                            info!("币安快照任务关闭");
                            break;
                        }
                    }
                }
            }
        });
    }

    pub async fn start_all_connections(&mut self) {
        // 1. 启动所有增量连接
        for i in 0..self.subscribe_msgs.get_inc_subscribe_msg_len() {
            let exchange = self.cfg.get_exchange().clone();
            let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
            let subscribe_msg = self.subscribe_msgs.get_inc_subscribe_msg(i).clone();
            
            // Create inc parser based on exchange (static dispatch for performance)
            match exchange.as_str() {
                "binance-futures" | "binance" => {
                    let parser = BinanceIncParser::new();
                    self.spawn_mkt_connection_typed(exchange, url, subscribe_msg, format!("inc msg batch {}", i), parser).await;
                }
                "bybit" | "bybit-spot" => {
                    let parser = BybitIncParser::new();
                    self.spawn_mkt_connection_typed(exchange, url, subscribe_msg, format!("inc msg batch {}", i), parser).await;
                }
                "okex-swap" | "okex" => {
                    let parser = OkexIncParser::new();
                    self.spawn_mkt_connection_typed(exchange, url, subscribe_msg, format!("inc msg batch {}", i), parser).await;
                }
                _ => {
                    panic!("Unsupported exchange for trade parser: {}", exchange);
                }
            }
        }
    
        // 2. 启动所有交易连接
        for i in 0..self.subscribe_msgs.get_trade_subscribe_msg_len() {
            let exchange = self.cfg.get_exchange().clone();
            let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
            let subscribe_msg = self.subscribe_msgs.get_trade_subscribe_msg(i).clone();
            
            // Create trade parser based on exchange (static dispatch for performance)
            match exchange.as_str() {
                "binance-futures" | "binance" => {
                    let parser = BinanceTradeParser::new();
                    self.spawn_mkt_connection_typed(exchange, url, subscribe_msg, format!("trade msg batch {}", i), parser).await;
                }
                "bybit" | "bybit-spot" => {
                    let parser = BybitTradeParser::new();
                    self.spawn_mkt_connection_typed(exchange, url, subscribe_msg, format!("trade msg batch {}", i), parser).await;
                }
                "okex-swap" | "okex" => {
                    let parser  = OkexTradeParser::new();
                    self.spawn_mkt_connection_typed(exchange, url, subscribe_msg, format!("trade msg batch {}", i), parser).await;
                }
                _ => {
                    error!("Unsupported exchange for trade parser: {}", exchange);
                    continue;
                }
            };
        }
        
        self.notify_tp_reset();
        
        // 3、启动独立的时间信号源连接
        let exchange = self.cfg.get_exchange().clone();

        // 如果是币安且为主机，额外启动快照query
        let url = crate::sub_msg::SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
        let signal_subscribe_msg = self.subscribe_msgs.get_time_signal_subscribe_msg();
        
        // Create signal parser based on exchange
        let signal_parser: Box<dyn Parser> = match exchange.as_str() {
            "binance-futures" | "binance" => Box::new(BinanceSignalParser::new(false)),
            "okex-swap" | "okex" => Box::new(OkexSignalParser::new(false)),
            "bybit" | "bybit-spot" => Box::new(BybitSignalParser::new(false)),
            _ => {
                error!("Unsupported exchange for signal parser: {}", exchange);
                return;
            }
        };
        
        self.spawn_mkt_connection(exchange, url, signal_subscribe_msg, "time signal".to_string(), signal_parser).await;

        // 4. 启动币安快照查询任务（仅主节点且为币安交易所）
        self.start_snapshot_task().await;
    
        log::info!("All connections started...");
    }
    pub async fn shutdown(&mut self, global_shutdown: &watch::Sender<bool>) -> Result<(), Box<dyn std::error::Error>>{
        // 发送全局关闭信号
        if let Err(e) = global_shutdown.send(true) {
            error!("Failed to shutdown MktConnectionManager: {}", e);
        }
        // 在drop时，等待所有任务完成
        let mut join_set = std::mem::take(&mut self.join_set); // 拿走 join_set，避免借用问题
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(_) => log::debug!("Task completed successfully"),
                Err(e) => error!("Task failed: {:?}", e),
            }
        }
        log::info!("All tasks completed");
        Ok(())
    }
    pub fn get_mkt_tx(&self) -> broadcast::Sender<Bytes> {
        self.mkt_tx.clone()
    }

    // 泛型版本的连接函数，避免动态分发开销
    async fn spawn_mkt_connection_typed<P>(&mut self, exchange: String, url: String, subscribe_msg: serde_json::Value, description: String, parser: P) 
    where 
        P: Parser + Send + 'static,
    {
        let mkt_tx = self.mkt_tx.clone();
        let global_shutdown_rx = self.global_shutdown_rx.clone();
        
        self.join_set.spawn(async move {
            // Create intermediate channel for raw WebSocket data
            let (raw_tx, mut raw_rx) = broadcast::channel(1000);
            
            // Spawn WebSocket connection task
            let ws_global_shutdown_rx = global_shutdown_rx.clone();
            let ws_exchange = exchange.clone();
            let ws_url = url.clone();
            let ws_subscribe_msg = subscribe_msg.clone();
            let ws_description = description.clone();
            
            tokio::spawn(async move {
                let mut connection = match construct_connection(
                    ws_exchange, 
                    ws_url, 
                    ws_subscribe_msg, 
                    raw_tx, 
                    ws_global_shutdown_rx
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to create connection for {}: {}", ws_description, e);
                        return;
                    }
                };
                if let Err(e) = connection.start_ws().await {
                    error!("Connection failed for {}: {}", ws_description, e);
                } else {
                    info!("Connection closed for {}", ws_description);
                }
            });
            
            // Spawn parser task (静态分发，无虚函数开销)
            let mut shutdown_rx = global_shutdown_rx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg_result = raw_rx.recv() => {
                            match msg_result {
                                Ok(raw_msg) => {
                                    // 静态分发调用，编译时确定具体类型
                                    let _parsed_count = parser.parse(raw_msg, &mkt_tx);
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    info!("Raw message channel closed for {}", description);
                                    break;
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => {
                                    error!("Market data parser lagged for {}", description);
                                    continue;
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                info!("Market data parser task shutdown for {}", description);
                                break;
                            }
                        }
                    }
                }
            });
        });
    }

    async fn spawn_mkt_connection(&mut self, exchange: String, url: String, subscribe_msg: serde_json::Value, description: String, parser: Box<dyn Parser>) {
        let mkt_tx = self.mkt_tx.clone();
        let global_shutdown_rx = self.global_shutdown_rx.clone();
        
        self.join_set.spawn(async move {
            // Create intermediate channel for raw WebSocket data
            let (raw_tx, mut raw_rx) = broadcast::channel(1000);
            
            // Spawn WebSocket connection task
            let ws_global_shutdown_rx = global_shutdown_rx.clone();
            let ws_exchange = exchange.clone();
            let ws_url = url.clone();
            let ws_subscribe_msg = subscribe_msg.clone();
            let ws_description = description.clone();
            
            tokio::spawn(async move {
                let mut connection = match construct_connection(
                    ws_exchange, 
                    ws_url, 
                    ws_subscribe_msg, 
                    raw_tx, 
                    ws_global_shutdown_rx
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to create connection for {}: {}", ws_description, e);
                        return;
                    }
                };
                if let Err(e) = connection.start_ws().await {
                    error!("Connection failed for {}: {}", ws_description, e);
                } else {
                    info!("Connection closed for {}", ws_description);
                }
            });
            
            // Spawn parser task
            let mut shutdown_rx = global_shutdown_rx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg_result = raw_rx.recv() => {
                            match msg_result {
                                Ok(raw_msg) => {
                                    let _parsed_count = parser.parse(raw_msg, &mkt_tx);
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    info!("Raw message channel closed for {}", description);
                                    break;
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => {
                                    error!("Market data parser lagged for {}", description);
                                    continue;
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                info!("Market data parser task shutdown for {}", description);
                                break;
                            }
                        }
                    }
                }
            });
        });
    }
}