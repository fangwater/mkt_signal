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