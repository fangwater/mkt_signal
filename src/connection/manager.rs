use crate::cfg::Config;
use crate::sub_msg::SubscribeMsgs;
use crate::connection::connection::{MktConnection, MktConnectionHandler};
use crate::connection::binance_conn::BinanceConnection;
use crate::connection::okex_conn::OkexConnection;
use crate::connection::bybit_conn::BybitConnection;
use tokio::sync::{broadcast, watch, Notify};
use tokio::task::JoinSet;
use bytes::Bytes;
use log::{info, error};
use std::sync::Arc;

pub struct MktConnectionManager {
    cfg: Config, //进程基本参数
    subscribe_msgs: SubscribeMsgs, //所有的订阅消息
    inc_tx: broadcast::Sender<Bytes>, //行情消息转发通道
    trade_tx: broadcast::Sender<Bytes>, //行情消息转发通道
    global_shutdown_rx: watch::Receiver<bool>, //全局关闭信号
    tp_reset_notify: Arc<Notify>, //tp重置消息通知
    join_set: JoinSet<()>, //任务集合
}


impl MktConnectionManager {
    fn construct_connection(exchange: String, url: String, subscribe_msg: serde_json::Value, tx: broadcast::Sender<Bytes>, global_shutdown_rx: watch::Receiver<bool>) -> anyhow::Result<Box<dyn MktConnectionHandler>> {
        match exchange.as_str() {
            "binance-futures" => {
                let base_connection = MktConnection::new(url, subscribe_msg, tx.clone(), global_shutdown_rx);
                let connection: BinanceConnection = BinanceConnection::new(base_connection);
                Ok(Box::new(connection))
            }
            "binance" => {
                let base_connection = MktConnection::new(url, subscribe_msg, tx.clone(), global_shutdown_rx);   
                let connection: BinanceConnection = BinanceConnection::new(base_connection);
                Ok(Box::new(connection))
            }
            "okex-swap" => {
                let base_connection = MktConnection::new(url, subscribe_msg, tx.clone(), global_shutdown_rx);
                let connection: OkexConnection = OkexConnection::new(base_connection);
                Ok(Box::new(connection))
            }
            "okex" => {
                let base_connection = MktConnection::new(url, subscribe_msg, tx.clone(), global_shutdown_rx);
                let connection: OkexConnection = OkexConnection::new(base_connection);
                Ok(Box::new(connection))
            }
            "bybit" => {
                let base_connection = MktConnection::new(url, subscribe_msg, tx.clone(), global_shutdown_rx);
                let connection: BybitConnection = BybitConnection::new(base_connection);
                Ok(Box::new(connection))
            }
            "bybit-spot" => {
                let base_connection = MktConnection::new(url, subscribe_msg, tx.clone(), global_shutdown_rx);
                let connection: BybitConnection = BybitConnection::new(base_connection);
                Ok(Box::new(connection))
            }
            _ => panic!("Unsupported exchange: {}", exchange),
        }
    }
    pub async fn new(cfg: &Config, global_shutdown: &watch::Sender<bool>) -> Self {
        let (inc_tx, _) = broadcast::channel(1000);
        let (trade_tx, _) = broadcast::channel(1000);
        let subscribe_msgs = SubscribeMsgs::new(&cfg).await;
        Self {
            cfg: cfg.clone(),
            subscribe_msgs : subscribe_msgs,
            inc_tx: inc_tx,
            trade_tx: trade_tx,
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
        self.notify_tp_reset();
        // 1. 启动所有增量连接
        for i in 0..self.subscribe_msgs.get_inc_subscribe_msg_len() {
            let exchange = self.cfg.get_exchange().clone();
            let url = self.cfg.get_exchange_url().unwrap();
            let subscribe_msg = self.subscribe_msgs.get_inc_subscribe_msg(i).clone();
            let tx = self.inc_tx.clone();
            let global_shutdown_rx = self.global_shutdown_rx.clone();
            self.join_set.spawn(async move {
                let mut connection = match Self::construct_connection(
                    exchange, 
                    url, 
                    subscribe_msg, 
                    tx, 
                    global_shutdown_rx
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to create connection: {}", e);
                        return;
                    }
                };
                if let Err(e) = connection.start_ws().await {
                    error!("Connection failed: {}", e);
                }else{
                    info!("Connection closed. finished task for inc msg batch: {}", i);
                }
            });
        }
    
        // 2. 启动所有交易连接
        for i in 0..self.subscribe_msgs.get_trade_subscribe_msg_len() {
            let exchange = self.cfg.get_exchange().clone();
            let url = self.cfg.get_exchange_url().unwrap();
            let subscribe_msg = self.subscribe_msgs.get_trade_subscribe_msg(i).clone();
            let tx = self.trade_tx.clone();
            let global_shutdown_rx = self.global_shutdown_rx.clone();
            
            self.join_set.spawn(async move {
                let mut connection = match Self::construct_connection(
                    exchange, 
                    url, 
                    subscribe_msg, 
                    tx, 
                    global_shutdown_rx
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to create connection: {}", e);
                        return;
                    }
                };
                if let Err(e) = connection.start_ws().await {
                    error!("Connection failed: {}", e);
                }else{
                    info!("Connection closed. finished task for trade msg batch: {}", i);
                }
            });
        }
    
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
    pub fn get_inc_tx(&self) -> broadcast::Sender<Bytes> {
        self.inc_tx.clone()
    }
    pub fn get_trade_tx(&self) -> broadcast::Sender<Bytes> {
        self.trade_tx.clone()
    }
}