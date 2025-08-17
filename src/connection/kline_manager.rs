use crate::cfg::Config;
use crate::sub_msg::SubscribeMsgs;
use crate::parser::binance_parser::BinanceKlineParser;
use crate::parser::okex_parser::OkexKlineParser;
use crate::parser::bybit_parser::BybitKlineParser;
use crate::parser::default_parser::Parser;
use crate::connection::connection::construct_connection;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinSet;
use bytes::Bytes;
use log::{info, error};

pub struct KlineDataConnectionManager {
    cfg: Config,
    subscribe_msgs: SubscribeMsgs,
    kline_tx: broadcast::Sender<Bytes>,
    global_shutdown_rx: watch::Receiver<bool>,
    join_set: JoinSet<()>,
}

impl KlineDataConnectionManager {
    pub async fn new(cfg: &Config, global_shutdown: &watch::Sender<bool>, kline_tx: broadcast::Sender<Bytes>) -> Self {
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
            
            self.spawn_kline_connection(exchange, url, subscribe_msg, format!("kline batch {}", i)).await;
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


    /// 根据交易所类型构造 Kline parser
    async fn construct_kline_parser(&self, exchange: &str) -> Result<Box<dyn Parser>, Box<dyn std::error::Error>> {
        match exchange {
            "binance-futures" | "binance" => {
                Ok(Box::new(BinanceKlineParser::new()))
            }
            "bybit" | "bybit-spot" => {
                Ok(Box::new(BybitKlineParser::new()))
            }
            "okex-swap" | "okex" => {
                Ok(Box::new(OkexKlineParser::new()))
            }
            _ => {
                error!("Unsupported exchange for kline: {}", exchange);
                Err(format!("Unsupported exchange: {}", exchange).into())
            }
        }
    }

    async fn spawn_kline_connection(&mut self, exchange: String, url: String, subscribe_msg: serde_json::Value, description: String) {
        let kline_tx = self.kline_tx.clone();
        let global_shutdown_rx = self.global_shutdown_rx.clone();
        
        // Create parser before moving into the async block
        let parser = match self.construct_kline_parser(&exchange).await {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to create kline parser for {}: {}", description, e);
                return;
            }
        };
        
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
                                    let _parsed_count = parser.parse(raw_msg, &kline_tx);
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    info!("Raw message channel closed for {}", description);
                                    break;
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => {
                                    error!("Kline parser lagged for {}", description);
                                    continue;
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                info!("Kline parser task shutdown for {}", description);
                                break;
                            }
                        }
                    }
                }
            });
        });
    }

    pub async fn update_subscribe_msgs(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let prev_symbols = self.subscribe_msgs.get_active_symbols();
        let subscribe_msgs = SubscribeMsgs::new(&self.cfg).await;
        self.subscribe_msgs = subscribe_msgs;
        SubscribeMsgs::compare_symbol_set(&prev_symbols, &self.subscribe_msgs.get_active_symbols());
        Ok(())
    }
}