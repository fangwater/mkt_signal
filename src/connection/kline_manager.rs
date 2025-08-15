use crate::cfg::Config;
use crate::sub_msg::SubscribeMsgs;
use crate::sub_msg::DerivativesMetricsSubscribeMsgs;
use crate::connection::connection::{MktConnection, MktConnectionHandler};
use crate::connection::binance_conn::BinanceConnection;
use crate::connection::okex_conn::OkexConnection;
use crate::connection::bybit_conn::BybitConnection;
use crate::connection::mkt_manager::construct_connection;
use tokio::sync::{broadcast, watch, Notify};
use tokio::task::JoinSet;
use bytes::Bytes;
use log::{info, error};
use std::sync::Arc;

//订阅
pub struct MktDataConnectionManager {
    cfg: Config, //进程基本参数
    subscribe_msgs: SubscribeMsgs, //所有的订阅消息
    inc_tx: broadcast::Sender<Bytes>, //行情消息转发通道
    trade_tx: broadcast::Sender<Bytes>, //行情消息转发通道
    global_shutdown_rx: watch::Receiver<bool>, //全局关闭信号
    tp_reset_notify: Arc<Notify>, //tp重置消息通知
    join_set: JoinSet<()>, //任务集合
}

pub struct KlineDataConnectionManager {
    cfg: Config,
    subscribe_msgs: SubscribeMsgs,
    kline_tx: broadcast::Sender<Bytes>,
    global_shutdown_rx: watch::Receiver<bool>,
    join_set: JoinSet<()>,
}

impl KlineDataConnectionManager {
    pub async fn new(cfg: &Config, global_shutdown: &watch::Sender<bool>) -> Self {
        let (kline_tx, _) = broadcast::channel(1000);
        let subscribe_msgs = SubscribeMsgs::new(&cfg).await;
        Self {
            cfg: cfg.clone(),
            subscribe_msgs,
            kline_tx,
            global_shutdown_rx: global_shutdown.subscribe(),
            join_set: JoinSet::new(),
        }   
    }

    pub async fn start_all_kline_connections(&mut self) {
        let kline_msg_len = self.subscribe_msgs.get_kline_subscribe_msg_len();
        for i in 0..kline_msg_len {
            let exchange = self.cfg.get_exchange().clone();
            let url = crate::sub_msg::SubscribeMsgs::get_exchange_kline_data_url(&exchange).to_string();
            let subscribe_msg = self.subscribe_msgs.get_kline_subscribe_msg(i).clone();
            let tx = self.kline_tx.clone();
            let global_shutdown_rx = self.global_shutdown_rx.clone();
            
            self.join_set.spawn(async move {
                let mut connection = match construct_connection(
                    exchange, 
                    url, 
                    subscribe_msg, 
                    tx, 
                    global_shutdown_rx
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Failed to create kline connection: {}", e);
                        return;
                    }
                };
                if let Err(e) = connection.start_ws().await {
                    error!("Kline connection failed: {}", e);
                } else {
                    info!("Kline connection closed. finished task for kline msg batch: {}", i);
                }
            });
        }
        log::info!("All kline connections started...");
    }

    pub async fn shutdown(&mut self, global_shutdown: &watch::Sender<bool>) -> Result<(), Box<dyn std::error::Error>>{
        if let Err(e) = global_shutdown.send(true) {
            error!("Failed to shutdown KlineDataConnectionManager: {}", e);
        }
        let mut join_set = std::mem::take(&mut self.join_set);
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(_) => log::debug!("Kline task completed successfully"),
                Err(e) => error!("Kline task failed: {:?}", e),
            }
        }
        log::info!("All kline tasks completed");
        Ok(())
    }

    pub fn get_kline_tx(&self) -> broadcast::Sender<Bytes> {
        self.kline_tx.clone()
    }

    pub async fn update_subscribe_msgs(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let prev_symbols = self.subscribe_msgs.get_active_symbols();
        let subscribe_msgs = SubscribeMsgs::new(&self.cfg).await;
        self.subscribe_msgs = subscribe_msgs;
        SubscribeMsgs::compare_symbol_set(&prev_symbols, &self.subscribe_msgs.get_active_symbols());
        Ok(())
    }
}

pub struct DerivativesMetricsDataConnectionManager {
    cfg: Config, //进程基本参数
    subscribe_msgs: DerivativesMetricsSubscribeMsgs, //衍生品指标相关消息
    kline_tx: broadcast::Sender<Bytes>, //kline消息转发通道
    global_shutdown_rx: watch::Receiver<bool>, //全局关闭信号
    join_set: JoinSet<()>, //任务集合
}




impl MktDataConnectionManager {
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

    pub async fn start_time_signal_connection(&mut self){
        let exchange = self.cfg.get_exchange().clone();
        let url = self.cfg.get_exchange_url().unwrap();
    }
    
    pub async fn start_all_connections(&mut self) {
        self.notify_tp_reset();
        // 启动一个独立的时间信号源，不同交易所不同



        // 1. 启动所有增量连接
        for i in 0..self.subscribe_msgs.get_inc_subscribe_msg_len() {
            let exchange = self.cfg.get_exchange().clone();
            let url = self.cfg.get_exchange_url().unwrap();
            let subscribe_msg = self.subscribe_msgs.get_inc_subscribe_msg(i).clone();
            let tx = self.inc_tx.clone();
            let global_shutdown_rx = self.global_shutdown_rx.clone();
            self.join_set.spawn(async move {
                let mut connection = match construct_connection(
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
                let mut connection = match construct_connection(
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