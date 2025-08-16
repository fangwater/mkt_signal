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

//订阅衍生品相关数据
pub struct DerivativesMetricsDataConnectionManager {
    cfg: Config,
    subscribe_msgs: DerivativesMetricsSubscribeMsgs,
    metrics_tx: broadcast::Sender<Bytes>,
    global_shutdown_rx: watch::Receiver<bool>,
    join_set: JoinSet<()>,
}

impl DerivativesMetricsDataConnectionManager {
    pub async fn new(cfg: &Config, global_shutdown: &watch::Sender<bool>) -> Self {
        let (metrics_tx, _) = broadcast::channel(1000);
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
        
        self.spawn_connection(exchange.clone(), url.clone(), msgs.mark_price_stream_for_all_market.clone(), "binance mark price".to_string()).await;
        self.spawn_connection(exchange.clone(), url.clone(), msgs.liquidation_orders_msg.clone(), "binance liquidation orders".to_string()).await;
    }

    async fn start_okex_connections(&mut self, msgs: &crate::sub_msg::OkexPerpsSubscribeMsgs) {
        let exchange = self.cfg.get_exchange().clone();
        let url = crate::sub_msg::OkexPerpsSubscribeMsgs::WS_URL.to_string();
        
        for (i, mark_price_msg) in msgs.mark_price_msgs.iter().enumerate() {
            self.spawn_connection(exchange.clone(), url.clone(), mark_price_msg.clone(), format!("okex mark price batch {}", i)).await;
        }
        
        for (i, index_price_msg) in msgs.index_price_msgs.iter().enumerate() {
            self.spawn_connection(exchange.clone(), url.clone(), index_price_msg.clone(), format!("okex index price batch {}", i)).await;
        }
        
        for (i, funding_rate_msg) in msgs.funding_rate_msgs.iter().enumerate() {
            self.spawn_connection(exchange.clone(), url.clone(), funding_rate_msg.clone(), format!("okex funding rate batch {}", i)).await;
        }
        
        self.spawn_connection(exchange.clone(), url.clone(), msgs.liquidation_orders_msg.clone(), "okex liquidation orders".to_string()).await;
    }

    async fn start_bybit_connections(&mut self, msgs: &crate::sub_msg::BybitPerpsSubscribeMsgs) {
        let exchange = self.cfg.get_exchange().clone();
        let url = crate::sub_msg::BybitPerpsSubscribeMsgs::WS_URL.to_string();
        
        for (i, ticker_msg) in msgs.ticker_stream_msgs.iter().enumerate() {
            self.spawn_connection(exchange.clone(), url.clone(), ticker_msg.clone(), format!("bybit ticker batch {}", i)).await;
        }
        
        for (i, liquidation_msg) in msgs.liquidation_orders_msgs.iter().enumerate() {
            self.spawn_connection(exchange.clone(), url.clone(), liquidation_msg.clone(), format!("bybit liquidation batch {}", i)).await;
        }
    }

    async fn spawn_connection(&mut self, exchange: String, url: String, subscribe_msg: serde_json::Value, description: String) {
        let tx = self.metrics_tx.clone();
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
                    error!("Failed to create connection for {}: {}", description, e);
                    return;
                }
            };
            if let Err(e) = connection.start_ws().await {
                error!("Connection failed for {}: {}", description, e);
            } else {
                info!("Connection closed for {}", description);
            }
        });
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

    pub fn get_metrics_tx(&self) -> broadcast::Sender<Bytes> {
        self.metrics_tx.clone()
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



