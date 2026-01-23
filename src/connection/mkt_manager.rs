use crate::cfg::Config;
use crate::common::exchange::Exchange;
use crate::connection::connection::construct_connection_with_ip;
use crate::parser::binance_parser::{
    BinanceAskBidSpreadParser, BinanceDerivativesMetricsParser, BinanceIncParser,
    BinanceKlineParser, BinanceSignalParser, BinanceTradeParser,
};
use crate::parser::bitget_parser::{
    BitgetDerivativesMetricsParser, BitgetIncParser, BitgetSignalParser, BitgetTradeParser,
};
use crate::parser::bybit_parser::{
    BybitAskBidSpreadParser, BybitDerivativesMetricsParser, BybitIncParser, BybitKlineParser,
    BybitSignalParser, BybitTradeParser,
};
use crate::parser::default_parser::Parser;
use crate::parser::gate_parser::{
    GateDerivativesMetricsParser, GateKlineParser, GateSignalParser, GateTickerParser,
};
use crate::parser::okex_parser::{
    OkexAskBidSpreadParser, OkexDerivativesMetricsParser, OkexIncParser, OkexKlineParser,
    OkexSignalParser, OkexTradeParser,
};
use crate::signal::common::TradingVenue;
use crate::sub_msg::{DerivativesMetricsSubscribeMsgs, SubscribeMsgs};
use bytes::Bytes;
use log::{debug, error, info};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch, Notify};
use tokio::task::JoinSet;

const BINANCE_SPOT_WS_URL: &str = "wss://stream.binance.com:9443/ws";

// 消息队列结构，每种数据类型对应一个mpsc channel
pub struct MessageQueues {
    pub incremental_tx: mpsc::UnboundedSender<Bytes>,
    pub incremental_rx: mpsc::UnboundedReceiver<Bytes>,

    pub trade_tx: mpsc::UnboundedSender<Bytes>,
    pub trade_rx: mpsc::UnboundedReceiver<Bytes>,

    pub kline_tx: mpsc::UnboundedSender<Bytes>,
    pub kline_rx: mpsc::UnboundedReceiver<Bytes>,

    pub derivatives_tx: mpsc::UnboundedSender<Bytes>,
    pub derivatives_rx: mpsc::UnboundedReceiver<Bytes>,

    pub signal_tx: mpsc::UnboundedSender<Bytes>,
    pub signal_rx: mpsc::UnboundedReceiver<Bytes>,

    pub ask_bid_spread_tx: mpsc::UnboundedSender<Bytes>,
    pub ask_bid_spread_rx: mpsc::UnboundedReceiver<Bytes>,
}

impl MessageQueues {
    pub fn new() -> Self {
        let (incremental_tx, incremental_rx) = mpsc::unbounded_channel();
        let (trade_tx, trade_rx) = mpsc::unbounded_channel();
        let (kline_tx, kline_rx) = mpsc::unbounded_channel();
        let (derivatives_tx, derivatives_rx) = mpsc::unbounded_channel();
        let (signal_tx, signal_rx) = mpsc::unbounded_channel();
        let (ask_bid_spread_tx, ask_bid_spread_rx) = mpsc::unbounded_channel();

        Self {
            incremental_tx,
            incremental_rx,
            trade_tx,
            trade_rx,
            kline_tx,
            kline_rx,
            derivatives_tx,
            derivatives_rx,
            signal_tx,
            signal_rx,
            ask_bid_spread_tx,
            ask_bid_spread_rx,
        }
    }
}

// 统一的信号管理器
pub struct MktManager {
    cfg: Config,
    subscribe_msgs: SubscribeMsgs,
    derivatives_subscribe_msgs: Option<DerivativesMetricsSubscribeMsgs>,
    global_shutdown_rx: watch::Receiver<bool>,
    tp_reset_notify: Arc<Notify>,
    join_set: JoinSet<()>,
    local_ip: String, // 绑定的本地IP地址

    // 各种数据类型的mpsc发送器
    incremental_tx: mpsc::UnboundedSender<Bytes>,
    trade_tx: mpsc::UnboundedSender<Bytes>,
    kline_tx: mpsc::UnboundedSender<Bytes>,
    derivatives_tx: mpsc::UnboundedSender<Bytes>,
    signal_tx: mpsc::UnboundedSender<Bytes>,
    ask_bid_spread_tx: mpsc::UnboundedSender<Bytes>,
}

impl MktManager {
    pub async fn new(
        cfg: &Config,
        global_shutdown: &watch::Sender<bool>,
        subscribe_msgs: SubscribeMsgs,
        derivatives_subscribe_msgs: Option<DerivativesMetricsSubscribeMsgs>,
        incremental_tx: mpsc::UnboundedSender<Bytes>,
        trade_tx: mpsc::UnboundedSender<Bytes>,
        kline_tx: mpsc::UnboundedSender<Bytes>,
        derivatives_tx: mpsc::UnboundedSender<Bytes>,
        signal_tx: mpsc::UnboundedSender<Bytes>,
        ask_bid_spread_tx: mpsc::UnboundedSender<Bytes>,
        local_ip: String,
    ) -> Self {
        Self {
            cfg: cfg.clone(),
            subscribe_msgs,
            derivatives_subscribe_msgs,
            global_shutdown_rx: global_shutdown.subscribe(),
            tp_reset_notify: Arc::new(Notify::new()),
            join_set: JoinSet::new(),
            local_ip,
            incremental_tx,
            trade_tx,
            kline_tx,
            derivatives_tx,
            signal_tx,
            ask_bid_spread_tx,
        }
    }

    pub fn get_tp_reset_notify(&self) -> Arc<Notify> {
        self.tp_reset_notify.clone()
    }

    pub fn notify_tp_reset(&self) {
        self.tp_reset_notify.notify_waiters();
    }

    pub async fn start_all_connections(&mut self) {
        info!("Starting all connections with data type configuration:");
        info!(
            "  - Incremental: {}",
            self.cfg.data_types.enable_incremental
        );
        info!("  - Trade: {}", self.cfg.data_types.enable_trade);
        info!("  - Kline: {}", self.cfg.data_types.enable_kline);
        info!(
            "  - Derivatives: {}",
            self.cfg.data_types.enable_derivatives
        );
        info!(
            "  - Ask/Bid Spread: {}",
            self.cfg.data_types.enable_ask_bid_spread
        );

        // 1. 启动增量连接（如果启用）
        if self.cfg.data_types.enable_incremental {
            self.start_incremental_connections().await;
        }

        // 2. 启动交易连接（如果启用）
        if self.cfg.data_types.enable_trade {
            self.start_trade_connections().await;
        }

        // 3. 启动K线连接（如果启用）
        if self.cfg.data_types.enable_kline {
            self.start_kline_connections().await;
        }

        // 4. 启动衍生品连接（如果启用且为期货交易所）
        if self.cfg.data_types.enable_derivatives && self.derivatives_subscribe_msgs.is_some() {
            let count = self
                .derivatives_subscribe_msgs
                .as_ref()
                .map(|d| d.get_active_symbols().len())
                .unwrap_or(0);
            info!("Derivatives enabled; active symbols={}", count);
            self.start_derivatives_connections().await;
        }

        // 5. 启动买卖价差连接（如果启用）
        if self.cfg.data_types.enable_ask_bid_spread {
            self.start_ask_bid_spread_connections().await;
        }

        self.notify_tp_reset();

        // 5. 始终启动时间信号源连接
        self.start_signal_connection().await;

        info!("All connections started...");
    }

    async fn start_incremental_connections(&mut self) {
        let exchange = self.cfg.get_exchange();
        let max_levels = self.cfg.data_types.max_levels_per_msg;

        for i in 0..self.subscribe_msgs.get_inc_subscribe_msg_len() {
            let subscribe_msg = self.subscribe_msgs.get_inc_subscribe_msg(i).clone();
            let tx = self.incremental_tx.clone();

            match exchange {
                Exchange::Binance => {
                    match self.cfg.venue {
                        TradingVenue::BinanceMargin => {
                            let url = BINANCE_SPOT_WS_URL.to_string();
                            let parser = BinanceIncParser::spot_incremental(max_levels);
                            self.spawn_connection_with_mpsc(
                                exchange,
                                url,
                                subscribe_msg,
                                format!("inc msg batch {}", i),
                                parser,
                                tx,
                            )
                            .await;
                        }
                        _ => {
                            let url =
                                SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                            let parser = BinanceIncParser::futures_incremental(max_levels);
                            self.spawn_connection_with_mpsc(
                                exchange,
                                url,
                                subscribe_msg,
                                format!("inc msg batch {}", i),
                                parser,
                                tx,
                            )
                            .await;
                        }
                    }
                }
                Exchange::Bybit => {
                    let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                    let parser = BybitIncParser::with_max_levels(max_levels);
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("inc msg batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                Exchange::Okex => {
                    let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                    let parser = OkexIncParser::with_max_levels(max_levels);
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("inc msg batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                Exchange::Bitget => {
                    let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                    let parser = BitgetIncParser::with_max_levels(max_levels);
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("inc msg batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                _ => {
                    panic!("Unsupported exchange for inc parser: {}", exchange);
                }
            }
        }

        if self.subscribe_msgs.get_depth_subscribe_msg_len() > 0 {
            self.start_depth_snapshot_connections().await;
        }
    }

    async fn start_depth_snapshot_connections(&mut self) {
        let exchange = self.cfg.get_exchange();
        if exchange != Exchange::Binance {
            return;
        }

        let max_levels = self.cfg.data_types.max_levels_per_msg;

        for i in 0..self.subscribe_msgs.get_depth_subscribe_msg_len() {
            let subscribe_msg = self.subscribe_msgs.get_depth_subscribe_msg(i).clone();
            let tx = self.incremental_tx.clone();

            match self.cfg.venue {
                TradingVenue::BinanceMargin => {
                    let url = BINANCE_SPOT_WS_URL.to_string();
                    let parser = BinanceIncParser::spot_snapshot(max_levels);
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url,
                        subscribe_msg,
                        format!("depth snapshot batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                _ => {
                    let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                    let parser = BinanceIncParser::futures_snapshot(max_levels);
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url,
                        subscribe_msg,
                        format!("depth snapshot batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
            }
        }
    }

    async fn start_trade_connections(&mut self) {
        let exchange = self.cfg.get_exchange();

        for i in 0..self.subscribe_msgs.get_trade_subscribe_msg_len() {
            let subscribe_msg = self.subscribe_msgs.get_trade_subscribe_msg(i).clone();
            let tx = self.trade_tx.clone();

            match exchange {
                Exchange::Binance => {
                    match self.cfg.venue {
                        TradingVenue::BinanceMargin => {
                            let url = BINANCE_SPOT_WS_URL.to_string();
                            let parser = BinanceTradeParser::new();
                            self.spawn_connection_with_mpsc(
                                exchange,
                                url,
                                subscribe_msg,
                                format!("trade msg batch {}", i),
                                parser,
                                tx,
                            )
                            .await;
                        }
                        _ => {
                            let url =
                                SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                            let parser = BinanceTradeParser::new();
                            self.spawn_connection_with_mpsc(
                                exchange,
                                url,
                                subscribe_msg,
                                format!("trade msg batch {}", i),
                                parser,
                                tx,
                            )
                            .await;
                        }
                    }
                }
                Exchange::Bybit => {
                    let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                    let parser = BybitTradeParser::new();
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("trade msg batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                Exchange::Okex => {
                    let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                    let parser = OkexTradeParser::new();
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("trade msg batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                Exchange::Bitget => {
                    let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                    let parser = BitgetTradeParser::new();
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("trade msg batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                _ => {
                    error!("Unsupported exchange for trade parser: {}", exchange);
                }
            }
        }
    }

    async fn start_kline_connections(&mut self) {
        let exchange = self.cfg.get_exchange();
        let url = if exchange == Exchange::Binance && self.cfg.venue == TradingVenue::BinanceMargin
        {
            BINANCE_SPOT_WS_URL.to_string()
        } else {
            crate::sub_msg::SubscribeMsgs::get_exchange_kline_data_url(&exchange).to_string()
        };

        for i in 0..self.subscribe_msgs.get_kline_subscribe_msg_len() {
            let subscribe_msg = self.subscribe_msgs.get_kline_subscribe_msg(i).clone();
            let tx = self.kline_tx.clone();

            match exchange {
                Exchange::Binance => {
                    let parser = BinanceKlineParser::new();
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("kline batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                Exchange::Bybit => {
                    let parser = BybitKlineParser::new();
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("kline batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                Exchange::Okex => {
                    let parser = OkexKlineParser::new();
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("kline batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                Exchange::Gate => {
                    // Gate K线：使用 only_closed=true 只处理已完结K线
                    let parser = GateKlineParser::new(true);
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("kline batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                _ => {
                    error!("Unsupported exchange for kline parser: {}", exchange);
                }
            }
        }
    }

    async fn start_ask_bid_spread_connections(&mut self) {
        let exchange = self.cfg.get_exchange();

        for i in 0..self.subscribe_msgs.get_ask_bid_spread_subscribe_msg_len() {
            let subscribe_msg = self
                .subscribe_msgs
                .get_ask_bid_spread_subscribe_msg(i)
                .clone();
            let tx = self.ask_bid_spread_tx.clone();

            match exchange {
                Exchange::Binance => {
                    match self.cfg.venue {
                        TradingVenue::BinanceMargin => {
                            let url = BINANCE_SPOT_WS_URL.to_string();
                            let parser = BinanceAskBidSpreadParser::new();
                            self.spawn_connection_with_mpsc(
                                exchange,
                                url,
                                subscribe_msg,
                                format!("ask_bid_spread batch {}", i),
                                parser,
                                tx,
                            )
                            .await;
                        }
                        _ => {
                            let url =
                                SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                            let parser = BinanceAskBidSpreadParser::new();
                            self.spawn_connection_with_mpsc(
                                exchange,
                                url,
                                subscribe_msg,
                                format!("ask_bid_spread batch {}", i),
                                parser,
                                tx,
                            )
                            .await;
                        }
                    }
                }
                Exchange::Bybit => {
                    let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                    let parser = BybitAskBidSpreadParser::new();
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("ask_bid_spread batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                Exchange::Okex => {
                    let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                    let parser = OkexAskBidSpreadParser::new();
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("ask_bid_spread batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                Exchange::Gate => {
                    let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
                    // Gate ticker 包含最优买卖价
                    let parser = GateTickerParser::new();
                    log::info!(
                        "Gate ask_bid_spread subscribe batch {} payload={}",
                        i,
                        subscribe_msg
                    );
                    self.spawn_connection_with_mpsc(
                        exchange,
                        url.clone(),
                        subscribe_msg,
                        format!("ask_bid_spread batch {}", i),
                        parser,
                        tx,
                    )
                    .await;
                }
                _ => {
                    error!(
                        "Unsupported exchange for ask_bid_spread parser: {}",
                        exchange
                    );
                }
            }
        }
    }

    async fn start_derivatives_connections(&mut self) {
        if let Some(derivatives_msgs) = self.derivatives_subscribe_msgs.clone() {
            let exchange_msgs = &derivatives_msgs.exchange_msgs;
            match exchange_msgs {
                crate::sub_msg::ExchangePerpsSubscribeMsgs::Binance(binance_msgs) => {
                    self.start_binance_derivatives_connections(&binance_msgs)
                        .await;
                }
                crate::sub_msg::ExchangePerpsSubscribeMsgs::Okex(okex_msgs) => {
                    self.start_okex_derivatives_connections(&okex_msgs).await;
                }
                crate::sub_msg::ExchangePerpsSubscribeMsgs::Bybit(bybit_msgs) => {
                    self.start_bybit_derivatives_connections(&bybit_msgs).await;
                }
                crate::sub_msg::ExchangePerpsSubscribeMsgs::Bitget(bitget_msgs) => {
                    self.start_bitget_derivatives_connections(&bitget_msgs)
                        .await;
                }
                crate::sub_msg::ExchangePerpsSubscribeMsgs::Gate(gate_msgs) => {
                    self.start_gate_derivatives_connections(&gate_msgs).await;
                }
            }
        }
    }

    async fn start_binance_derivatives_connections(
        &mut self,
        msgs: &crate::sub_msg::BinancePerpsSubscribeMsgs,
    ) {
        let exchange = self.cfg.get_exchange();
        let url = crate::sub_msg::BinancePerpsSubscribeMsgs::WS_URL.to_string();
        let tx = self.derivatives_tx.clone();
        let symbols = self
            .derivatives_subscribe_msgs
            .as_ref()
            .unwrap()
            .get_active_symbols();

        info!("Starting Binance derivatives connections");
        let parser1 = BinanceDerivativesMetricsParser::new(symbols.clone());
        let parser2 = BinanceDerivativesMetricsParser::new(symbols.clone());

        self.spawn_connection_with_mpsc(
            exchange,
            url.clone(),
            msgs.mark_price_stream_for_all_market.clone(),
            "binance mark price".to_string(),
            parser1,
            tx.clone(),
        )
        .await;
        self.spawn_connection_with_mpsc(
            exchange,
            url,
            msgs.liquidation_orders_msg.clone(),
            "binance liquidation orders".to_string(),
            parser2,
            tx,
        )
        .await;
    }

    async fn start_okex_derivatives_connections(
        &mut self,
        msgs: &crate::sub_msg::OkexPerpsSubscribeMsgs,
    ) {
        let exchange = self.cfg.get_exchange();
        let url = crate::sub_msg::OkexPerpsSubscribeMsgs::WS_URL.to_string();
        let tx = self.derivatives_tx.clone();
        let symbols = self
            .derivatives_subscribe_msgs
            .as_ref()
            .unwrap()
            .get_active_symbols()
            .clone();

        info!(
            "Starting OKEx derivatives connections ({} batches, symbols={})",
            msgs.unified_perps_msgs.len(),
            symbols.len()
        );
        for (i, unified_msg) in msgs.unified_perps_msgs.iter().enumerate() {
            let parser = OkexDerivativesMetricsParser::new(symbols.clone());
            self.spawn_connection_with_mpsc(
                exchange,
                url.clone(),
                unified_msg.clone(),
                format!("okex unified derivatives batch {}", i),
                parser,
                tx.clone(),
            )
            .await;
        }
    }

    async fn start_bybit_derivatives_connections(
        &mut self,
        msgs: &crate::sub_msg::BybitPerpsSubscribeMsgs,
    ) {
        let exchange = self.cfg.get_exchange();
        let url = crate::sub_msg::BybitPerpsSubscribeMsgs::WS_URL.to_string();
        let tx = self.derivatives_tx.clone();

        info!("Starting Bybit derivatives connections");

        // 处理ticker消息
        for (i, ticker_msg) in msgs.ticker_stream_msgs.iter().enumerate() {
            let parser = BybitDerivativesMetricsParser::new();
            self.spawn_connection_with_mpsc(
                exchange,
                url.clone(),
                ticker_msg.clone(),
                format!("bybit ticker batch {}", i),
                parser,
                tx.clone(),
            )
            .await;
        }

        // 处理强平消息
        for (i, liquidation_msg) in msgs.liquidation_orders_msgs.iter().enumerate() {
            let parser = BybitDerivativesMetricsParser::new();
            self.spawn_connection_with_mpsc(
                exchange,
                url.clone(),
                liquidation_msg.clone(),
                format!("bybit liquidation batch {}", i),
                parser,
                tx.clone(),
            )
            .await;
        }
    }

    async fn start_bitget_derivatives_connections(
        &mut self,
        msgs: &crate::sub_msg::BitgetPerpsSubscribeMsgs,
    ) {
        let exchange = self.cfg.get_exchange();
        let url = crate::sub_msg::BitgetPerpsSubscribeMsgs::WS_URL.to_string();
        let tx = self.derivatives_tx.clone();

        info!("Starting Bitget derivatives connections");

        // 处理 ticker 消息（包含买卖价、资金费率等）
        for (i, ticker_msg) in msgs.ticker_stream_msgs.iter().enumerate() {
            let parser = BitgetDerivativesMetricsParser::new();
            self.spawn_connection_with_mpsc(
                exchange,
                url.clone(),
                ticker_msg.clone(),
                format!("bitget ticker batch {}", i),
                parser,
                tx.clone(),
            )
            .await;
        }
    }

    async fn start_gate_derivatives_connections(
        &mut self,
        msgs: &crate::sub_msg::GatePerpsSubscribeMsgs,
    ) {
        let exchange = self.cfg.get_exchange();
        let url = crate::sub_msg::GatePerpsSubscribeMsgs::WS_URL.to_string();
        let tx = self.derivatives_tx.clone();

        info!("Starting Gate derivatives connections (tickers)");

        for (i, ticker_msg) in msgs.ticker_stream_msgs.iter().enumerate() {
            let parser = GateDerivativesMetricsParser::new();
            log::info!(
                "Gate derivatives subscribe batch {} payload={}",
                i,
                ticker_msg
            );
            self.spawn_connection_with_mpsc(
                exchange,
                url.clone(),
                ticker_msg.clone(),
                format!("gate futures ticker batch {}", i),
                parser,
                tx.clone(),
            )
            .await;
        }
    }

    async fn start_signal_connection(&mut self) {
        let exchange = self.cfg.get_exchange();
        let url = SubscribeMsgs::get_exchange_mkt_data_url(&exchange).to_string();
        let signal_subscribe_msg = self.subscribe_msgs.get_time_signal_subscribe_msg();
        let tx = self.signal_tx.clone();

        let signal_parser: Box<dyn Parser> = match exchange {
            Exchange::Binance => Box::new(BinanceSignalParser::new(false)),
            Exchange::Okex => Box::new(OkexSignalParser::new(false)),
            Exchange::Bybit => Box::new(BybitSignalParser::new(false)),
            Exchange::Bitget => Box::new(BitgetSignalParser::new()),
            Exchange::Gate => Box::new(GateSignalParser::new(false)),
        };

        self.spawn_connection_with_mpsc_dyn(
            exchange,
            url,
            signal_subscribe_msg,
            "time signal".to_string(),
            signal_parser,
            tx,
        )
        .await;
    }

    pub async fn shutdown(
        &mut self,
        global_shutdown: &watch::Sender<bool>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Err(e) = global_shutdown.send(true) {
            error!("Failed to shutdown MktManager: {}", e);
        }

        let mut join_set = std::mem::take(&mut self.join_set);
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(_) => log::debug!("Task completed successfully"),
                Err(e) => error!("Task failed: {:?}", e),
            }
        }
        log::info!("All tasks completed");
        Ok(())
    }

    // 使用mpsc的连接函数（静态分发）
    async fn spawn_connection_with_mpsc<P>(
        &mut self,
        exchange: Exchange,
        url: String,
        subscribe_msg: serde_json::Value,
        description: String,
        parser: P,
        tx: mpsc::UnboundedSender<Bytes>,
    ) where
        P: Parser + Send + 'static,
    {
        let global_shutdown_rx = self.global_shutdown_rx.clone();
        let local_ip = self.local_ip.clone();

        self.join_set.spawn(async move {
            let (raw_tx, mut raw_rx) = broadcast::channel(8192);

            let ws_global_shutdown_rx = global_shutdown_rx.clone();
            let ws_exchange = exchange;
            let ws_url = url.clone();
            let ws_subscribe_msg = subscribe_msg.clone();
            let ws_description = description.clone();
            let ws_local_ip = local_ip.clone();

            tokio::spawn(async move {
                let mut connection = match construct_connection_with_ip(
                    ws_exchange,
                    ws_url,
                    ws_subscribe_msg,
                    raw_tx,
                    ws_global_shutdown_rx,
                    ws_local_ip
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
                    debug!("Connection closed for {}", ws_description);
                }
            });

            let mut shutdown_rx = global_shutdown_rx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg_result = raw_rx.recv() => {
                            match msg_result {
                                Ok(raw_msg) => {
                                    let _ = parser.parse(raw_msg, &tx);
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    info!("Raw message channel closed for {}", description);
                                    break;
                                }
                                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                    if !*shutdown_rx.borrow() {
                                        error!("Parser lagged for {} (skipped {} messages)", description, skipped);
                                    }
                                    continue;
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                debug!("Parser task shutdown for {}", description);
                                break;
                            }
                        }
                    }
                }
            });
        });
    }

    // 使用mpsc的连接函数（动态分发，仅用于signal）
    async fn spawn_connection_with_mpsc_dyn(
        &mut self,
        exchange: Exchange,
        url: String,
        subscribe_msg: serde_json::Value,
        description: String,
        parser: Box<dyn Parser>,
        tx: mpsc::UnboundedSender<Bytes>,
    ) {
        let global_shutdown_rx = self.global_shutdown_rx.clone();
        let local_ip = self.local_ip.clone();

        self.join_set.spawn(async move {
            let (raw_tx, mut raw_rx) = broadcast::channel(8192);

            let ws_global_shutdown_rx = global_shutdown_rx.clone();
            let ws_exchange = exchange;
            let ws_url = url.clone();
            let ws_subscribe_msg = subscribe_msg.clone();
            let ws_description = description.clone();
            let ws_local_ip = local_ip.clone();

            tokio::spawn(async move {
                let mut connection = match construct_connection_with_ip(
                    ws_exchange,
                    ws_url,
                    ws_subscribe_msg,
                    raw_tx,
                    ws_global_shutdown_rx,
                    ws_local_ip
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
                    debug!("Connection closed for {}", ws_description);
                }
            });

            let mut shutdown_rx = global_shutdown_rx.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg_result = raw_rx.recv() => {
                            match msg_result {
                                Ok(raw_msg) => {
                                    let _ = parser.parse(raw_msg, &tx);
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    info!("Raw message channel closed for {}", description);
                                    break;
                                }
                                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                    if !*shutdown_rx.borrow() {
                                        error!("Parser lagged for {} (skipped {} messages)", description, skipped);
                                    }
                                    continue;
                                }
                            }
                        }
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                debug!("Parser task shutdown for {}", description);
                                break;
                            }
                        }
                    }
                }
            });
        });
    }
}
