use crate::cfg::Config;
use crate::parser::binance_parser::BinanceDerivativesMetricsParser;
use crate::parser::okex_parser::OkexDerivativesMetricsParser;
use crate::parser::bybit_parser::BybitDerivativesMetricsParser;
use crate::parser::default_parser::Parser;
use crate::sub_msg::DerivativesMetricsSubscribeMsgs;
use crate::connection::connection::construct_connection;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinSet;
use bytes::Bytes;
use log::{info, error};

//订阅衍生品相关数据
pub struct DerivativesMetricsDataConnectionManager {
    cfg: Config,
    subscribe_msgs: DerivativesMetricsSubscribeMsgs,
    metrics_tx: broadcast::Sender<Bytes>,
    global_shutdown_rx: watch::Receiver<bool>,
    join_set: JoinSet<()>,
}

impl DerivativesMetricsDataConnectionManager {
    pub async fn new(cfg: &Config, global_shutdown: &watch::Sender<bool>, metrics_tx: broadcast::Sender<Bytes>) -> Self {
        let subscribe_msgs = DerivativesMetricsSubscribeMsgs::new(&cfg).await;
        Self {
            cfg: cfg.clone(),
            subscribe_msgs,
            metrics_tx,
            global_shutdown_rx: global_shutdown.subscribe(),
            join_set: JoinSet::new(),
        }   
    }

    pub async fn start_all_derivatives_connections(&mut self) {
        let exchange_msgs = self.subscribe_msgs.exchange_msgs.clone();
        match exchange_msgs {
            crate::sub_msg::ExchangePerpsSubscribeMsgs::Binance(binance_msgs) => {
                self.start_binance_connections(&binance_msgs).await;
            }
            crate::sub_msg::ExchangePerpsSubscribeMsgs::Okex(okex_msgs) => {
                self.start_okex_connections(&okex_msgs).await;
            }
            crate::sub_msg::ExchangePerpsSubscribeMsgs::Bybit(bybit_msgs) => {
                self.start_bybit_connections(&bybit_msgs).await;
            }
        }
        log::info!("All derivatives metrics connections started...");
    }

    async fn start_binance_connections(&mut self, msgs: &crate::sub_msg::BinancePerpsSubscribeMsgs) {
        let exchange = self.cfg.get_exchange().clone();
        let url = crate::sub_msg::BinancePerpsSubscribeMsgs::WS_URL.to_string();
        
        info!("Starting Binance derivatives connections for exchange: {}", exchange);
        info!("Binance derivatives WebSocket URL: {}", url);
        info!("Initializing {} Binance derivatives streams", 2);
        
        info!("Starting Binance mark price stream (全市场标记价格、资金费率、指数价格)");
        self.spawn_connection(exchange.clone(), url.clone(), msgs.mark_price_stream_for_all_market.clone(), "binance mark price".to_string()).await;
        
        info!("Starting Binance liquidation orders stream (强平信息)");
        self.spawn_connection(exchange.clone(), url.clone(), msgs.liquidation_orders_msg.clone(), "binance liquidation orders".to_string()).await;
        
        info!("Binance derivatives connections initialization completed");
    }

    async fn start_okex_connections(&mut self, msgs: &crate::sub_msg::OkexPerpsSubscribeMsgs) {
        let exchange = self.cfg.get_exchange().clone();
        let url = crate::sub_msg::OkexPerpsSubscribeMsgs::WS_URL.to_string();
        
        info!("Starting OKEx derivatives connections for exchange: {}", exchange);
        info!("OKEx derivatives WebSocket URL: {}", url);
        info!("Initializing {} OKEx unified derivatives streams (包含标记价格、指数价格、资金费率、强平信息)", msgs.unified_perps_msgs.len());
        
        for (i, unified_msg) in msgs.unified_perps_msgs.iter().enumerate() {
            self.spawn_connection(
                exchange.clone(), 
                url.clone(), 
                unified_msg.clone(), 
                format!("okex unified derivatives batch {}", i)
            ).await;
        }
        
        info!("OKEx derivatives connections initialization completed");
    }

    async fn start_bybit_connections(&mut self, msgs: &crate::sub_msg::BybitPerpsSubscribeMsgs) {
        let exchange = self.cfg.get_exchange().clone();
        let url = crate::sub_msg::BybitPerpsSubscribeMsgs::WS_URL.to_string();
        
        let total_streams = msgs.ticker_stream_msgs.len() + msgs.liquidation_orders_msgs.len();
        info!("Starting Bybit derivatives connections for exchange: {}", exchange);
        info!("Bybit derivatives WebSocket URL: {}", url);
        info!("Initializing {} Bybit derivatives streams total", total_streams);
        
        info!("Starting {} Bybit ticker streams (标记价格、指数价格、资金费率)", msgs.ticker_stream_msgs.len());
        for (i, ticker_msg) in msgs.ticker_stream_msgs.iter().enumerate() {
            self.spawn_connection(exchange.clone(), url.clone(), ticker_msg.clone(), format!("bybit ticker batch {}", i)).await;
        }
        
        info!("Starting {} Bybit liquidation streams (强平信息)", msgs.liquidation_orders_msgs.len());
        for (i, liquidation_msg) in msgs.liquidation_orders_msgs.iter().enumerate() {
            self.spawn_connection(exchange.clone(), url.clone(), liquidation_msg.clone(), format!("bybit liquidation batch {}", i)).await;
        }
        
        info!("Bybit derivatives connections initialization completed");
    }
    async fn construct_parser(&self, exchange: &str) -> Result<Box<dyn Parser>, Box<dyn std::error::Error>> {
        // 获取 symbol set
        let symbols = self.subscribe_msgs.get_active_symbols();
        
        match exchange {
            "binance-futures" => {
                Ok(Box::new(BinanceDerivativesMetricsParser::new(symbols.clone())))
            }
            "bybit" => {
                Ok(Box::new(BybitDerivativesMetricsParser::new()))
            }
            "okex-swap" => {
                Ok(Box::new(OkexDerivativesMetricsParser::new(symbols.clone())))
            }
            _ => {
                panic!("Unsupported exchange for derivatives metrics: {}", exchange);
            }
        }
    }

    async fn spawn_connection(&mut self, exchange: String, url: String, subscribe_msg: serde_json::Value, description: String) {
        let metrics_tx = self.metrics_tx.clone();
        let global_shutdown_rx = self.global_shutdown_rx.clone();
        
        info!("Creating derivatives connection: {} (exchange: {})", description, exchange);
        // info!("Subscription message: {}", serde_json::to_string(&subscribe_msg).unwrap_or_else(|_| "invalid json".to_string()));
        // Create parser before moving into the async block
        let parser = match self.construct_parser(&exchange).await {
            Ok(p) => {
                info!("Parser created successfully for {}", description);
                p
            },
            Err(e) => {
                error!("Failed to create parser for {}: {}", description, e);
                return;
            }
        };
        
        info!("Spawning connection task for {}", description);
        let task_description = description.clone();
        let ws_description = description.clone();
        self.join_set.spawn(async move {
            info!("Connection task started for {}", task_description);
            
            // Create intermediate channel for raw WebSocket data
            let (raw_tx, mut raw_rx) = broadcast::channel(8192);
            info!("Created raw message channel (capacity: 1000) for {}", task_description);
            
            // Spawn WebSocket connection task
            let ws_global_shutdown_rx = global_shutdown_rx.clone();
            let ws_exchange = exchange.clone();
            let ws_url = url.clone();
            let ws_subscribe_msg = subscribe_msg.clone();
            
            info!("Creating WebSocket connection task for {}", ws_description);
            tokio::spawn(async move {
                info!("WebSocket connection task starting for {}", ws_description);
                let mut connection = match construct_connection(
                    ws_exchange, 
                    ws_url, 
                    ws_subscribe_msg, 
                    raw_tx, 
                    ws_global_shutdown_rx
                ) {
                    Ok(c) => {
                        info!("WebSocket connection constructed successfully for {}", ws_description);
                        c
                    },
                    Err(e) => {
                        error!("Failed to create connection for {}: {}", ws_description, e);
                        return;
                    }
                };
                
                info!("Starting WebSocket for {}", ws_description);
                if let Err(e) = connection.start_ws().await {
                    error!("Connection failed for {}: {}", ws_description, e);
                } else {
                    info!("Connection closed for {}", ws_description);
                }
            });
            
            // Spawn parser task
            let mut shutdown_rx = global_shutdown_rx.clone();
            let parser_description = task_description.clone();
            info!("Creating parser task for {}", parser_description);
            tokio::spawn(async move {
                info!("Parser task started for {}", parser_description);
                
                loop {
                    tokio::select! {
                        msg_result = raw_rx.recv() => {
                            match msg_result {
                                Ok(raw_msg) => {
                                    let _parsed_count = parser.parse(raw_msg, &metrics_tx);
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    info!("Raw message channel closed for {}", parser_description);
                                    break;
                                }
                                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                    error!("Parser lagged for {} (skipped {} messages)", parser_description, skipped);
                                    continue;
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                info!("Parser task shutdown for {}", parser_description);
                                break;
                            }
                        }
                    }
                }
                info!("Parser task completed for {}", parser_description);
            });
            
            info!("All tasks spawned successfully for {}", task_description);
        });
        
        info!("Connection spawning completed for {}", description);
    }

    pub async fn shutdown(&mut self, global_shutdown: &watch::Sender<bool>) -> Result<(), Box<dyn std::error::Error>>{
        if let Err(e) = global_shutdown.send(true) {
            error!("Failed to shutdown DerivativesMetricsDataConnectionManager: {}", e);
        }
        let mut join_set = std::mem::take(&mut self.join_set);
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(_) => log::debug!("Derivatives metrics task completed successfully"),
                Err(e) => error!("Derivatives metrics task failed: {:?}", e),
            }
        }
        log::info!("All derivatives metrics tasks completed");
        Ok(())
    }


    pub async fn update_subscribe_msgs(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let prev_symbols = self.subscribe_msgs.get_active_symbols().clone();
        let subscribe_msgs = DerivativesMetricsSubscribeMsgs::new(&self.cfg).await;
        let new_symbols = subscribe_msgs.get_active_symbols().clone();
        self.subscribe_msgs = subscribe_msgs;
        crate::sub_msg::SubscribeMsgs::compare_symbol_set(&prev_symbols, &new_symbols);
        Ok(())
    }
}



