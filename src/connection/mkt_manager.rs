use crate::cfg::Config;
use crate::sub_msg::SubscribeMsgs;
use crate::parser::default_parser::{Parser, DefaultTradeParser, DefaultIncParser};
use crate::parser::binance_parser::{BinanceSignalParser, BinanceTradeParser};
use crate::parser::okex_parser::{OkexSignalParser, OkexTradeParser};
use crate::parser::bybit_parser::{BybitSignalParser, BybitTradeParser};
use crate::connection::connection::construct_connection;
use tokio::sync::{broadcast, watch, Notify};
use tokio::task::JoinSet;
use bytes::Bytes;
use log::{info, error};
use std::sync::Arc;

//订阅逐笔行情，orderbook增量消息，通过parser处理后转发
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

    
    pub async fn start_all_connections(&mut self) {
        // 1. 启动所有增量连接
        for i in 0..self.subscribe_msgs.get_inc_subscribe_msg_len() {
            let exchange = self.cfg.get_exchange().clone();
            let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
            let subscribe_msg = self.subscribe_msgs.get_inc_subscribe_msg(i).clone();
            let parser: Box<dyn Parser> = Box::new(DefaultIncParser::new());
            
            self.spawn_mkt_connection(exchange, url, subscribe_msg, format!("inc msg batch {}", i), parser).await;
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
                    let parser = OkexTradeParser::new();
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