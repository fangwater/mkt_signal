use crate::cfg::Config;
use crate::common::exchange::Exchange;
use crate::signal::common::TradingVenue;
use serde_json::Value;
use std::collections::HashSet;

fn bitget_inst_type_for_venue(venue: TradingVenue) -> &'static str {
    match venue {
        TradingVenue::BitgetFutures => "USDT-FUTURES",
        TradingVenue::BitgetMargin => "SPOT",
        _ => "USDT-FUTURES",
    }
}

fn construct_subscribe_message(
    exchange: &Exchange,
    venue: TradingVenue,
    symbols: &[String],
    channel: &str,
) -> Value {
    match exchange {
        Exchange::Binance => {
            let params: Vec<String> = symbols
                .iter()
                .map(|symbol| format!("{}@{}", symbol.to_lowercase(), channel))
                .collect();
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": params,
                "id": 1,
            })
        }
        Exchange::Okex => {
            let args: Vec<Value> = symbols
                .iter()
                .map(|symbol| {
                    serde_json::json!({
                        "channel": channel,
                        "instId": symbol
                    })
                })
                .collect();
            serde_json::json!({
                "op": "subscribe",
                "args": args
            })
        }
        Exchange::Bybit => {
            let args: Vec<String> = symbols
                .iter()
                .map(|symbol| format!("{}.{}", channel, symbol))
                .collect();
            serde_json::json!({
                "op": "subscribe",
                "args": args
            })
        }
        Exchange::Hyperliquid => {
            let mut requests = Vec::new();

            if channel == "allMids" {
                requests.push(serde_json::json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "allMids",
                    }
                }));
            } else {
                for symbol in symbols {
                    let coin = hyperliquid_coin_from_internal(symbol);
                    let subscription = match channel {
                        "l2Book" => Some(serde_json::json!({
                            "type": "l2Book",
                            "coin": coin,
                        })),
                        "trades" => Some(serde_json::json!({
                            "type": "trades",
                            "coin": coin,
                        })),
                        "bbo" => Some(serde_json::json!({
                            "type": "bbo",
                            "coin": coin,
                        })),
                        "candle" => Some(serde_json::json!({
                            "type": "candle",
                            "coin": coin,
                            "interval": "1m",
                        })),
                        _ => None,
                    };

                    if let Some(subscription) = subscription {
                        requests.push(serde_json::json!({
                            "method": "subscribe",
                            "subscription": subscription,
                        }));
                    }
                }
            }

            if requests.is_empty() {
                panic!("unsupported hyperliquid channel: {}", channel);
            }

            if requests.len() == 1 {
                requests.remove(0)
            } else {
                Value::Array(requests)
            }
        }
        Exchange::Bitget => {
            // Bitget v2 API 格式
            let inst_type = bitget_inst_type_for_venue(venue);
            let args: Vec<Value> = symbols
                .iter()
                .map(|symbol| {
                    serde_json::json!({
                        "instType": inst_type,
                        "channel": channel,
                        "instId": symbol
                    })
                })
                .collect();
            serde_json::json!({
                "op": "subscribe",
                "args": args
            })
        }
        Exchange::Gate => {
            // Gate.io API 格式
            // 现货: spot.xxx, 合约: futures.xxx
            let channel_prefix = "futures"; // Default to futures for now
            let payload: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            serde_json::json!({
                "time": timestamp,
                "channel": format!("{}.{}", channel_prefix, channel),
                "event": "subscribe",
                "payload": payload
            })
        }
    }
}

//市场高频数据的订阅消息
//包含一个信号，用于切分数据
//其次是增量行情快照数据和逐笔成交数据
#[derive(Clone)]
pub struct SubscribeMsgs {
    active_symbols: HashSet<String>,              //当前所有u本位符号
    inc_subscribe_msgs: Vec<serde_json::Value>,   //增量orderbook
    depth_subscribe_msgs: Vec<serde_json::Value>, //有限档深度快照（binance-futures: depth20@100ms, binance-margin: depth20/SBE）
    trade_subscribe_msgs: Vec<serde_json::Value>, //逐笔成交
    kline_subscribe_msgs: Vec<serde_json::Value>, //k线
    signal_subscribe_msg: serde_json::Value,      //只需要一个，实际是和btc深度有关的某个行情
    ask_bid_spread_msgs: Vec<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct BinancePerpsSubscribeMsgs {
    pub mark_price_stream_for_all_market: serde_json::Value, // 币安的markprice订阅全市场，包含资金费率，指数价格等信息
    pub liquidation_orders_msg: serde_json::Value,           //强平信息
}

impl BinancePerpsSubscribeMsgs {
    pub const WS_URL: &'static str = "wss://fstream.binance.com/ws";
    pub async fn new() -> Self {
        Self {
            mark_price_stream_for_all_market: serde_json::json!({
                "method": "SUBSCRIBE",
                "params": ["!markPrice@arr@1s"],
                "id": 1,
            }),
            liquidation_orders_msg: serde_json::json!({
                "method": "SUBSCRIBE",
                "params": ["!forceOrder@arr"],
                "id": 1,
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OkexPerpsSubscribeMsgs {
    pub mark_price_msgs: Vec<serde_json::Value>, // 标记价格订阅
    pub index_tickers_msgs: Vec<serde_json::Value>, // 指数价格订阅
    pub funding_rate_msgs: Vec<serde_json::Value>, // 资金费率订阅
    pub liquidation_orders_msgs: Vec<serde_json::Value>, // 强平信息订阅
}

impl OkexPerpsSubscribeMsgs {
    pub const WS_URL: &'static str = "wss://ws.okx.com:8443/ws/v5/public";
    pub async fn new(cfg: &Config) -> Self {
        let symbols: Vec<String> = cfg.get_symbols().await.unwrap();
        let batch_size = cfg.get_batch_size();
        let mut mark_price_msgs = Vec::new();
        let mut index_tickers_msgs = Vec::new();
        let mut funding_rate_msgs = Vec::new();
        let mut liquidation_orders_msgs = Vec::new();

        // 为每个批次创建独立的订阅消息（拆分 mark/index/funding）
        for chunk in symbols.chunks(batch_size) {
            // 标记价格订阅
            let mut mark_args = Vec::new();
            for symbol in chunk {
                mark_args.push(serde_json::json!({
                    "channel": "mark-price",
                    "instId": symbol
                }));
            }
            mark_price_msgs.push(serde_json::json!({
                "op": "subscribe",
                "args": mark_args
            }));

            // 指数价格订阅（使用USD作为基准）
            let mut index_args = Vec::new();
            for symbol in chunk {
                // 从USDT永续合约符号转换为USD指数符号 (如：BTC-USDT-SWAP -> BTC-USD)
                let index_symbol = symbol
                    .replace("-USDT-SWAP", "-USD")
                    .replace("-USDT", "-USD");
                index_args.push(serde_json::json!({
                    "channel": "index-tickers",
                    "instId": index_symbol
                }));
            }
            index_tickers_msgs.push(serde_json::json!({
                "op": "subscribe",
                "args": index_args
            }));

            // 资金费率订阅
            let mut funding_args = Vec::new();
            for symbol in chunk {
                funding_args.push(serde_json::json!({
                    "channel": "funding-rate",
                    "instId": symbol
                }));
            }
            funding_rate_msgs.push(serde_json::json!({
                "op": "subscribe",
                "args": funding_args
            }));
        }

        // 强平信息只需要订阅一次
        liquidation_orders_msgs.push(serde_json::json!({
            "op": "subscribe",
            "args": [serde_json::json!({
                "channel": "liquidation-orders",
                "instType": "SWAP"
            })]
        }));

        Self {
            mark_price_msgs,
            index_tickers_msgs,
            funding_rate_msgs,
            liquidation_orders_msgs,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BybitPerpsSubscribeMsgs {
    pub ticker_stream_msgs: Vec<serde_json::Value>, //bybit的标记价格、指数价格、资金费率都来自ticker stream
    pub liquidation_orders_msgs: Vec<serde_json::Value>, //强平信息
}

impl BybitPerpsSubscribeMsgs {
    pub const WS_URL: &'static str = "wss://stream.bybit.com/v5/public/linear";
    pub async fn new(cfg: &Config) -> Self {
        let symbols: Vec<String> = cfg.get_symbols().await.unwrap();
        let batch_size = cfg.get_batch_size();
        let mut ticker_stream_msgs = Vec::new();
        let mut liquidation_orders_msgs = Vec::new();
        let exchange = cfg.get_exchange();
        for chunk in symbols.chunks(batch_size) {
            ticker_stream_msgs.push(construct_subscribe_message(
                &exchange, cfg.venue, chunk, "tickers",
            ));
            liquidation_orders_msgs.push(construct_subscribe_message(
                &exchange,
                cfg.venue,
                chunk,
                "allLiquidation",
            ));
        }
        Self {
            ticker_stream_msgs: ticker_stream_msgs,
            liquidation_orders_msgs: liquidation_orders_msgs,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BitgetPerpsSubscribeMsgs {
    pub ticker_stream_msgs: Vec<serde_json::Value>, // Bitget ticker 包含买卖价、资金费率等
}

impl BitgetPerpsSubscribeMsgs {
    pub const WS_URL: &'static str = "wss://ws.bitget.com/v2/ws/public";
    // Bitget 建议单连接不超过50个频道
    pub const MAX_CHANNELS_PER_CONNECTION: usize = 50;

    pub async fn new(cfg: &Config) -> Self {
        let symbols: Vec<String> = cfg.get_symbols().await.unwrap();
        // 使用 Bitget 特定的 batch size，不超过50
        let batch_size = cfg.get_batch_size().min(Self::MAX_CHANNELS_PER_CONNECTION);
        let mut ticker_stream_msgs = Vec::new();
        let exchange = cfg.get_exchange();

        for chunk in symbols.chunks(batch_size) {
            ticker_stream_msgs.push(construct_subscribe_message(
                &exchange,
                TradingVenue::BitgetFutures,
                chunk,
                "ticker",
            ));
        }

        Self { ticker_stream_msgs }
    }
}

#[derive(Debug, Clone)]
pub struct GatePerpsSubscribeMsgs {
    pub ticker_stream_msgs: Vec<serde_json::Value>, // Gate futures.tickers 含 mark/index/funding
}

impl GatePerpsSubscribeMsgs {
    pub const WS_URL: &'static str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
    pub const MAX_CHANNELS_PER_CONNECTION: usize = 100;

    pub async fn new(cfg: &Config) -> Self {
        let symbols: Vec<String> = cfg.get_symbols().await.unwrap();
        let batch_size = cfg.get_batch_size().min(Self::MAX_CHANNELS_PER_CONNECTION);
        let mut ticker_stream_msgs = Vec::new();
        let exchange = cfg.get_exchange();

        for chunk in symbols.chunks(batch_size) {
            ticker_stream_msgs.push(construct_subscribe_message(
                &exchange, cfg.venue, chunk, "tickers",
            ));
        }

        Self { ticker_stream_msgs }
    }
}

#[derive(Debug, Clone)]
pub struct HyperliquidPerpsSubscribeMsgs {
    pub active_asset_ctx_msgs: Vec<serde_json::Value>,
}

impl HyperliquidPerpsSubscribeMsgs {
    pub const WS_URL: &'static str = "wss://api.hyperliquid.xyz/ws";

    pub async fn new(cfg: &Config) -> Self {
        let symbols: Vec<String> = cfg.get_symbols().await.unwrap();
        let batch_size = cfg.get_batch_size();
        let mut active_asset_ctx_msgs = Vec::new();

        for chunk in symbols.chunks(batch_size) {
            let mut requests = Vec::new();
            for symbol in chunk {
                requests.push(serde_json::json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "activeAssetCtx",
                        "coin": hyperliquid_coin_from_internal(symbol),
                    }
                }));
            }

            let payload = if requests.len() == 1 {
                requests.remove(0)
            } else {
                Value::Array(requests)
            };

            active_asset_ctx_msgs.push(payload);
        }

        Self {
            active_asset_ctx_msgs,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExchangePerpsSubscribeMsgs {
    Binance(BinancePerpsSubscribeMsgs),
    Okex(OkexPerpsSubscribeMsgs),
    Bybit(BybitPerpsSubscribeMsgs),
    Bitget(BitgetPerpsSubscribeMsgs),
    Gate(GatePerpsSubscribeMsgs),
    Hyperliquid(HyperliquidPerpsSubscribeMsgs),
}

//衍生品(永续合约)的额外消息
#[derive(Clone)]
pub struct DerivativesMetricsSubscribeMsgs {
    pub active_symbols: HashSet<String>, //当前所有u本位永续合约的symbol
    pub exchange_msgs: ExchangePerpsSubscribeMsgs,
}

impl SubscribeMsgs {
    pub fn get_time_signal_subscribe_msg(&self) -> serde_json::Value {
        self.signal_subscribe_msg.clone()
    }

    pub fn get_kline_subscribe_msg(&self, index: usize) -> &serde_json::Value {
        &self.kline_subscribe_msgs[index]
    }

    pub fn get_kline_subscribe_msg_len(&self) -> usize {
        self.kline_subscribe_msgs.len()
    }

    pub fn get_active_symbols(&self) -> HashSet<String> {
        self.active_symbols.clone()
    }

    pub fn get_inc_subscribe_msg_len(&self) -> usize {
        self.inc_subscribe_msgs.len()
    }

    pub fn get_depth_subscribe_msg_len(&self) -> usize {
        self.depth_subscribe_msgs.len()
    }

    pub fn get_depth_subscribe_msg(&self, index: usize) -> &serde_json::Value {
        &self.depth_subscribe_msgs[index]
    }

    pub fn get_trade_subscribe_msg_len(&self) -> usize {
        self.trade_subscribe_msgs.len()
    }

    pub fn get_ask_bid_spread_subscribe_msg_len(&self) -> usize {
        self.ask_bid_spread_msgs.len()
    }

    pub fn get_ask_bid_spread_subscribe_msg(&self, index: usize) -> &serde_json::Value {
        &self.ask_bid_spread_msgs[index]
    }

    pub fn get_inc_subscribe_msg(&self, index: usize) -> &serde_json::Value {
        &self.inc_subscribe_msgs[index]
    }

    pub fn get_trade_subscribe_msg(&self, index: usize) -> &serde_json::Value {
        &self.trade_subscribe_msgs[index]
    }

    pub fn compare_symbol_set(prev_symbols: &HashSet<String>, new_symbols: &HashSet<String>) {
        println!("Updating symbols (current: {} symbols)", prev_symbols.len());

        let new_set: HashSet<String> = new_symbols.iter().map(|s| s.clone()).collect();
        let old_set = &prev_symbols;

        let added_count = new_set.difference(old_set).count();
        let removed_count = old_set.difference(&new_set).count();

        if added_count > 0 || removed_count > 0 {
            println!("Symbol changes:\n");
            if added_count > 0 {
                println!(
                    "  Added ({}): {:?}",
                    added_count,
                    new_set.difference(old_set).collect::<Vec<_>>()
                );
            } else {
                println!("  No new symbols added");
            }
            if removed_count > 0 {
                println!(
                    "  Removed ({}): {:?}",
                    removed_count,
                    old_set.difference(&new_set).collect::<Vec<_>>()
                );
            } else {
                println!("  No symbols removed");
            }
        } else {
            println!("No symbol changes");
        }
    }

    fn get_inc_channel(exchange: &Exchange, venue: TradingVenue) -> String {
        match exchange {
            Exchange::Binance => match venue {
                TradingVenue::BinanceMargin => "depth@100ms".to_string(),
                _ => "depth@100ms".to_string(),
            },
            Exchange::Okex => "books".to_string(),
            Exchange::Bybit => "orderbook.500".to_string(),
            Exchange::Bitget => "books".to_string(),
            Exchange::Gate => panic!("Gate.io does not support incremental orderbook"),
            Exchange::Hyperliquid => "l2Book".to_string(),
        }
    }

    /// 获取有限档深度快照 channel（目前只有 binance 支持）
    fn get_depth_channel(exchange: &Exchange, venue: TradingVenue) -> Option<String> {
        match exchange {
            Exchange::Binance => match venue {
                TradingVenue::BinanceMargin => Some("depth20@100ms".to_string()),
                _ => Some("depth20@100ms".to_string()),
            },
            _ => None, // 其他交易所暂不支持
        }
    }

    fn get_kline_channel(exchange: &Exchange) -> String {
        match exchange {
            Exchange::Binance => "kline_1m".to_string(),
            Exchange::Okex => "candle1m".to_string(),
            Exchange::Bybit => "kline.1".to_string(),
            Exchange::Bitget => "candle1m".to_string(),
            Exchange::Gate => "candlesticks".to_string(),
            Exchange::Hyperliquid => "candle".to_string(),
        }
    }

    fn get_trade_channel(exchange: &Exchange) -> String {
        match exchange {
            Exchange::Binance => "trade".to_string(),
            Exchange::Okex => "trades".to_string(),
            Exchange::Bybit => "publicTrade".to_string(),
            Exchange::Bitget => "trade".to_string(),
            Exchange::Gate => "trades".to_string(),
            Exchange::Hyperliquid => "trades".to_string(),
        }
    }

    fn get_ask_bid_spread_channel(exchange: &Exchange, venue: TradingVenue) -> String {
        match exchange {
            Exchange::Binance => match venue {
                TradingVenue::BinanceMargin => "bookTicker".to_string(),
                _ => "bookTicker".to_string(),
            },
            Exchange::Okex => "bbo-tbt".to_string(),
            Exchange::Bybit => "orderbook.1".to_string(),
            Exchange::Bitget => "books1".to_string(),
            Exchange::Gate => "book_ticker".to_string(),
            Exchange::Hyperliquid => "bbo".to_string(),
        }
    }
}

impl SubscribeMsgs {
    pub fn get_exchange_mkt_data_url(exchange: &Exchange) -> &'static str {
        match exchange {
            //币安u本位期货合约和现货
            Exchange::Binance => "wss://fstream.binance.com/ws",
            //OKEXu本位期货合约和现货
            Exchange::Okex => "wss://ws.okx.com:8443/ws/v5/public",
            //Bybitu本位期货合约和现货
            Exchange::Bybit => "wss://stream.bybit.com/v5/public/linear",
            //Gate.io USDT 永续（futures 专用 endpoint）
            Exchange::Gate => "wss://fx-ws.gateio.ws/v4/ws/usdt",
            //Bitget
            Exchange::Bitget => "wss://ws.bitget.com/v2/ws/public",
            Exchange::Hyperliquid => "wss://api.hyperliquid.xyz/ws",
        }
    }

    pub fn get_exchange_kline_data_url(exchange: &Exchange) -> &'static str {
        match exchange {
            //币安u本位期货合约和现货
            Exchange::Binance => "wss://fstream.binance.com/ws",
            //OKEXu本位期货合约和现货
            Exchange::Okex => "wss://ws.okx.com:8443/ws/v5/business",
            //Bybitu本位期货合约和现货
            Exchange::Bybit => "wss://stream.bybit.com/v5/public/linear",
            //Gate.io 现货和USDT合约 (K线也使用同一URL)
            Exchange::Gate => "wss://fx-ws.gateio.ws/v4/ws/usdt",
            //Bitget
            Exchange::Bitget => "wss://ws.bitget.com/v2/ws/public",
            Exchange::Hyperliquid => "wss://api.hyperliquid.xyz/ws",
        }
    }

    fn get_signal_subscribe_message(exchange: &Exchange, venue: TradingVenue) -> serde_json::Value {
        match exchange {
            Exchange::Binance => {
                serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": ["btcusdt@depth5@100ms"],
                    "id": 1,
                })
            }
            Exchange::Okex => {
                serde_json::json!({
                    "op": "subscribe",
                    "args": [serde_json::json!({
                        "channel": "books5",
                        "instId": "BTC-USDT-SWAP"
                    })]
                })
            }
            Exchange::Bybit => {
                serde_json::json!({
                    "op": "subscribe",
                    "args": ["orderbook.rpi.BTCUSDT"]
                })
            }
            Exchange::Bitget => {
                let inst_type = bitget_inst_type_for_venue(venue);
                serde_json::json!({
                    "op": "subscribe",
                    "args": [serde_json::json!({
                        "instType": inst_type,
                        "channel": "ticker",
                        "instId": "BTCUSDT"
                    })]
                })
            }
            Exchange::Gate => {
                // Gate.io 合约 ticker 作为信号源
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                serde_json::json!({
                    "time": timestamp,
                    "channel": "futures.tickers",
                    "event": "subscribe",
                    "payload": ["BTC_USDT"]
                })
            }
            Exchange::Hyperliquid => {
                serde_json::json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "allMids"
                    }
                })
            }
        }
    }

    pub async fn new(cfg: &Config) -> Self {
        let symbols: Vec<String> = cfg.get_symbols().await.unwrap();
        let batch_size = cfg.get_batch_size();
        let mut inc_subscribe_msgs = Vec::new();
        let mut depth_subscribe_msgs = Vec::new();
        let mut trade_subscribe_msgs = Vec::new();
        let mut kline_subscribe_msgs = Vec::new();
        let mut ask_bid_spread_msgs = Vec::new();
        let exchange = cfg.get_exchange();
        let inc_channel = if cfg.data_types.enable_incremental {
            Some(SubscribeMsgs::get_inc_channel(&exchange, cfg.venue))
        } else {
            None
        };
        let depth_channel = if cfg.data_types.enable_incremental {
            SubscribeMsgs::get_depth_channel(&exchange, cfg.venue)
        } else {
            None
        };
        let trade_channel = if cfg.data_types.enable_trade {
            Some(SubscribeMsgs::get_trade_channel(&exchange))
        } else {
            None
        };
        let kline_channel = if cfg.data_types.enable_kline {
            Some(SubscribeMsgs::get_kline_channel(&exchange))
        } else {
            None
        };
        let best_price_spread_channel = if cfg.data_types.enable_ask_bid_spread {
            Some(SubscribeMsgs::get_ask_bid_spread_channel(
                &exchange, cfg.venue,
            ))
        } else {
            None
        };
        for chunk in symbols.chunks(batch_size) {
            if let Some(ref ch) = inc_channel {
                inc_subscribe_msgs
                    .push(construct_subscribe_message(&exchange, cfg.venue, chunk, ch));
            }
            if let Some(ref ch) = depth_channel {
                depth_subscribe_msgs
                    .push(construct_subscribe_message(&exchange, cfg.venue, chunk, ch));
            }
            if let Some(ref ch) = trade_channel {
                trade_subscribe_msgs.push(construct_subscribe_message(
                    &exchange,
                    cfg.venue,
                    chunk,
                    ch,
                ));
            }
            if let Some(ref ch) = kline_channel {
                kline_subscribe_msgs.push(construct_subscribe_message(
                    &exchange,
                    cfg.venue,
                    chunk,
                    ch,
                ));
            }
            if let Some(ref ch) = best_price_spread_channel {
                ask_bid_spread_msgs.push(construct_subscribe_message(
                    &exchange,
                    cfg.venue,
                    chunk,
                    ch,
                ));
            }
        }
        Self {
            active_symbols: symbols.iter().map(|s| s.clone()).collect(),
            inc_subscribe_msgs,
            depth_subscribe_msgs,
            trade_subscribe_msgs,
            kline_subscribe_msgs,
            signal_subscribe_msg: SubscribeMsgs::get_signal_subscribe_message(&exchange, cfg.venue),
            ask_bid_spread_msgs,
        }
    }
}

impl DerivativesMetricsSubscribeMsgs {
    pub async fn new(cfg: &Config) -> Self {
        let symbols: Vec<String> = cfg.get_symbols().await.unwrap();
        let exchange = cfg.get_exchange();
        let exchange_msgs = match exchange {
            Exchange::Binance => {
                ExchangePerpsSubscribeMsgs::Binance(BinancePerpsSubscribeMsgs::new().await)
            }
            Exchange::Okex => {
                ExchangePerpsSubscribeMsgs::Okex(OkexPerpsSubscribeMsgs::new(cfg).await)
            }
            Exchange::Bybit => {
                ExchangePerpsSubscribeMsgs::Bybit(BybitPerpsSubscribeMsgs::new(cfg).await)
            }
            Exchange::Bitget => {
                ExchangePerpsSubscribeMsgs::Bitget(BitgetPerpsSubscribeMsgs::new(cfg).await)
            }
            Exchange::Gate => {
                ExchangePerpsSubscribeMsgs::Gate(GatePerpsSubscribeMsgs::new(cfg).await)
            }
            Exchange::Hyperliquid => ExchangePerpsSubscribeMsgs::Hyperliquid(
                HyperliquidPerpsSubscribeMsgs::new(cfg).await,
            ),
        };

        Self {
            active_symbols: symbols.iter().map(|s| s.clone()).collect(),
            exchange_msgs,
        }
    }

    pub fn get_active_symbols(&self) -> &HashSet<String> {
        &self.active_symbols
    }
}
