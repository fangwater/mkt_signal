use crate::cfg::Config;
use crate::common::exchange::Exchange;
use crate::signal::common::TradingVenue;
use log::warn;
use serde_json::Value;
use std::collections::HashSet;

const BINANCE_SPOT_WS_URL: &str = "wss://stream.binance.com:9443/ws";
const BINANCE_FUTURES_PUBLIC_WS_URL: &str = "wss://fstream.binance.com/public/ws";
const BINANCE_FUTURES_MARKET_WS_URL: &str = "wss://fstream.binance.com/market/ws";
const BYBIT_SPOT_PUBLIC_WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";
const BYBIT_LINEAR_PUBLIC_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const BYBIT_SPOT_SBE_WS_URL: &str = "wss://stream.bybit.com/v5/public-sbe/spot";
const BYBIT_LINEAR_SBE_WS_URL: &str = "wss://stream.bybit.com/v5/public-sbe/linear";
const ASTER_SPOT_WS_URL: &str = "wss://sstream.asterdex.com/ws";
const ASTER_FUTURES_WS_URL: &str = "wss://fstream.asterdex.com/ws";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceFuturesWsRoute {
    Public,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceFuturesStreamKind {
    Depth,
    BookTicker,
    Trade,
    Kline,
}

fn bitget_inst_type_for_venue(venue: TradingVenue) -> &'static str {
    match venue {
        TradingVenue::BitgetFutures => "USDT-FUTURES",
        TradingVenue::BitgetMargin => "SPOT",
        _ => "USDT-FUTURES",
    }
}

fn gate_channel_prefix_for_venue(venue: TradingVenue) -> &'static str {
    match venue {
        TradingVenue::GateMargin => "spot",
        TradingVenue::GateFutures => "futures",
        _ => "futures",
    }
}

fn bitget_derivatives_supported_for_venue(venue: TradingVenue) -> bool {
    venue == TradingVenue::BitgetFutures
}

fn hyperliquid_coin_from_internal(symbol: &str) -> String {
    let normalized = symbol.trim().to_uppercase().replace(['/', '-', '_'], "");
    normalized
        .strip_suffix("USDC")
        .filter(|coin| !coin.is_empty())
        .unwrap_or(normalized.as_str())
        .to_string()
}

fn construct_subscribe_message(
    exchange: &Exchange,
    venue: TradingVenue,
    symbols: &[String],
    channel: &str,
) -> Value {
    match exchange {
        Exchange::Binance | Exchange::Aster => {
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
            let channel_prefix = gate_channel_prefix_for_venue(venue);
            let channel_name = format!("{}.{}", channel_prefix, channel);
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Gate order_book_update 的 payload 与其他频道不同：
            // - spot:    ["BTC_USDT", "100ms"]
            // - futures: ["BTC_USDT", "100ms", "100"]
            // 且按 symbol 逐条订阅，不能用 ["BTC_USDT", "ETH_USDT", ...] 这种批量格式。
            if channel == "order_book_update" {
                let mut per_symbol_msgs = Vec::new();
                for symbol in symbols {
                    let payload = match venue {
                        TradingVenue::GateMargin => {
                            serde_json::json!([symbol.as_str(), "100ms"])
                        }
                        TradingVenue::GateFutures => {
                            serde_json::json!([symbol.as_str(), "100ms", "100"])
                        }
                        _ => serde_json::json!([symbol.as_str(), "100ms", "100"]),
                    };
                    per_symbol_msgs.push(serde_json::json!({
                        "time": timestamp,
                        "channel": channel_name,
                        "event": "subscribe",
                        "payload": payload
                    }));
                }
                if per_symbol_msgs.len() == 1 {
                    per_symbol_msgs.remove(0)
                } else {
                    Value::Array(per_symbol_msgs)
                }
            } else {
                let payload: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
                serde_json::json!({
                    "time": timestamp,
                    "channel": channel_name,
                    "event": "subscribe",
                    "payload": payload
                })
            }
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
    pub const WS_URL: &'static str = BINANCE_FUTURES_MARKET_WS_URL;
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

/// Aster 衍生品订阅消息 — 协议与 Binance 同构，只换 WS URL
#[derive(Debug, Clone)]
pub struct AsterPerpsSubscribeMsgs {
    pub mark_price_stream_for_all_market: serde_json::Value,
    pub liquidation_orders_msg: serde_json::Value,
}

impl AsterPerpsSubscribeMsgs {
    pub const WS_URL: &'static str = ASTER_FUTURES_WS_URL;
    pub async fn new() -> Self {
        let inner = BinancePerpsSubscribeMsgs::new().await;
        Self {
            mark_price_stream_for_all_market: inner.mark_price_stream_for_all_market,
            liquidation_orders_msg: inner.liquidation_orders_msg,
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
    pub const WS_URL: &'static str = BYBIT_LINEAR_PUBLIC_WS_URL;
    pub async fn new(cfg: &Config) -> Self {
        if cfg.venue != TradingVenue::BybitFutures {
            warn!(
                "bybit derivatives metrics are only supported on bybit-futures; current venue={} will skip derivatives subscriptions",
                cfg.venue.data_pub_slug()
            );
            return Self {
                ticker_stream_msgs: Vec::new(),
                liquidation_orders_msgs: Vec::new(),
            };
        }

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
            ticker_stream_msgs,
            liquidation_orders_msgs,
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
        if !bitget_derivatives_supported_for_venue(cfg.venue) {
            warn!(
                "bitget derivatives metrics are only supported on bitget-futures; current venue={} will skip derivatives subscriptions",
                cfg.venue.data_pub_slug()
            );
            return Self {
                ticker_stream_msgs: Vec::new(),
            };
        }

        let symbols: Vec<String> = cfg.get_symbols().await.unwrap();
        // 使用 Bitget 特定的 batch size，不超过50
        let batch_size = cfg.get_batch_size().min(Self::MAX_CHANNELS_PER_CONNECTION);
        let mut ticker_stream_msgs = Vec::new();
        let exchange = cfg.get_exchange();

        for chunk in symbols.chunks(batch_size) {
            ticker_stream_msgs.push(construct_subscribe_message(
                &exchange, cfg.venue, chunk, "ticker",
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
        if cfg.venue != TradingVenue::GateFutures {
            warn!(
                "gate derivatives metrics are only supported on gate-futures; current venue={} will skip derivatives subscriptions",
                cfg.venue.data_pub_slug()
            );
            return Self {
                ticker_stream_msgs: Vec::new(),
            };
        }

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
    Aster(AsterPerpsSubscribeMsgs),
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

        let new_set: HashSet<String> = new_symbols.iter().cloned().collect();
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

    fn get_inc_channel(exchange: &Exchange, _venue: TradingVenue) -> Option<String> {
        match exchange {
            Exchange::Binance | Exchange::Aster => Some("depth@100ms".to_string()),
            Exchange::Okex => Some("books".to_string()),
            // Bybit V5 public spot/linear docs currently expose orderbook depths such as 1/50/200/1000.
            Exchange::Bybit => Some("orderbook.1000".to_string()),
            Exchange::Bitget => Some("books".to_string()),
            Exchange::Gate => Some("order_book_update".to_string()),
            Exchange::Hyperliquid => Some("l2Book".to_string()),
        }
    }

    /// 获取有限档深度快照 channel（目前只有 binance / aster 支持）
    fn get_depth_channel(exchange: &Exchange, _venue: TradingVenue) -> Option<String> {
        match exchange {
            Exchange::Binance | Exchange::Aster => Some("depth20@100ms".to_string()),
            _ => None, // 其他交易所暂不支持
        }
    }

    fn get_kline_channel(exchange: &Exchange) -> String {
        match exchange {
            Exchange::Binance | Exchange::Aster => "kline_1m".to_string(),
            Exchange::Okex => "candle1m".to_string(),
            Exchange::Bybit => "kline.1".to_string(),
            Exchange::Bitget => "candle1m".to_string(),
            Exchange::Gate => "candlesticks".to_string(),
            Exchange::Hyperliquid => "candle".to_string(),
        }
    }

    fn get_trade_channel(exchange: &Exchange) -> String {
        match exchange {
            Exchange::Binance | Exchange::Aster => "trade".to_string(),
            Exchange::Okex => "trades".to_string(),
            Exchange::Bybit => "publicTrade".to_string(),
            Exchange::Bitget => "trade".to_string(),
            Exchange::Gate => "trades".to_string(),
            Exchange::Hyperliquid => "trades".to_string(),
        }
    }

    fn get_ask_bid_spread_channel(exchange: &Exchange, venue: TradingVenue) -> String {
        match exchange {
            Exchange::Binance | Exchange::Aster => "bookTicker".to_string(),
            Exchange::Okex => "bbo-tbt".to_string(),
            Exchange::Bybit => {
                if Self::bybit_ask_bid_uses_sbe() {
                    "ob.rpi.1.sbe".to_string()
                } else {
                    "orderbook.1".to_string()
                }
            }
            Exchange::Bitget => "books1".to_string(),
            Exchange::Gate => match venue {
                TradingVenue::GateMargin => "tickers".to_string(),
                _ => "book_ticker".to_string(),
            },
            Exchange::Hyperliquid => "bbo".to_string(),
        }
    }
}

impl SubscribeMsgs {
    fn env_ws_override(name: &str) -> Option<String> {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    fn env_flag(name: &str) -> bool {
        std::env::var(name)
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    }

    fn bybit_ask_bid_uses_sbe() -> bool {
        Self::env_flag("BYBIT_USE_SBE_BBO")
            || Self::env_ws_override("BYBIT_SBE_WS_URL").is_some()
            || Self::env_ws_override("BYBIT_SPOT_SBE_WS_URL").is_some()
            || Self::env_ws_override("BYBIT_LINEAR_SBE_WS_URL").is_some()
    }

    fn get_bybit_public_ws_url_with_venue(venue: TradingVenue) -> &'static str {
        match venue {
            TradingVenue::BybitMargin => BYBIT_SPOT_PUBLIC_WS_URL,
            _ => BYBIT_LINEAR_PUBLIC_WS_URL,
        }
    }

    pub fn get_bybit_ask_bid_spread_url_with_venue(venue: TradingVenue) -> String {
        if !Self::bybit_ask_bid_uses_sbe() {
            return Self::get_bybit_public_ws_url_with_venue(venue).to_string();
        }

        if let Some(url) = Self::env_ws_override("BYBIT_SBE_WS_URL") {
            return url;
        }

        match venue {
            TradingVenue::BybitMargin => Self::env_ws_override("BYBIT_SPOT_SBE_WS_URL")
                .unwrap_or_else(|| BYBIT_SPOT_SBE_WS_URL.to_string()),
            _ => Self::env_ws_override("BYBIT_LINEAR_SBE_WS_URL")
                .unwrap_or_else(|| BYBIT_LINEAR_SBE_WS_URL.to_string()),
        }
    }

    pub fn get_binance_ws_url_with_route(
        venue: TradingVenue,
        route: BinanceFuturesWsRoute,
    ) -> &'static str {
        match venue {
            TradingVenue::BinanceMargin => BINANCE_SPOT_WS_URL,
            _ => match route {
                BinanceFuturesWsRoute::Public => BINANCE_FUTURES_PUBLIC_WS_URL,
                BinanceFuturesWsRoute::Market => BINANCE_FUTURES_MARKET_WS_URL,
            },
        }
    }

    pub fn get_binance_ws_url_for_stream_kind(
        venue: TradingVenue,
        stream_kind: BinanceFuturesStreamKind,
    ) -> &'static str {
        let route = match stream_kind {
            BinanceFuturesStreamKind::Depth
            | BinanceFuturesStreamKind::BookTicker
            | BinanceFuturesStreamKind::Trade => BinanceFuturesWsRoute::Public,
            BinanceFuturesStreamKind::Kline => BinanceFuturesWsRoute::Market,
        };
        Self::get_binance_ws_url_with_route(venue, route)
    }

    /// Aster 的所有公共行情流走单一 endpoint，按 venue 区分现货 / 合约。
    pub fn get_aster_ws_url_with_venue(venue: TradingVenue) -> &'static str {
        match venue {
            TradingVenue::AsterMargin => ASTER_SPOT_WS_URL,
            _ => ASTER_FUTURES_WS_URL,
        }
    }

    pub fn get_exchange_mkt_data_url_with_venue(
        exchange: &Exchange,
        venue: TradingVenue,
    ) -> &'static str {
        match exchange {
            //币安u本位期货合约和现货
            Exchange::Binance => {
                Self::get_binance_ws_url_with_route(venue, BinanceFuturesWsRoute::Market)
            }
            //Aster：与币安协议同构，但所有流走单一 endpoint
            Exchange::Aster => Self::get_aster_ws_url_with_venue(venue),
            //OKEXu本位期货合约和现货
            Exchange::Okex => "wss://ws.okx.com:8443/ws/v5/public",
            //Bybitu本位期货合约和现货
            Exchange::Bybit => Self::get_bybit_public_ws_url_with_venue(venue),
            //Gate.io: 现货与合约分不同 endpoint
            Exchange::Gate => match venue {
                TradingVenue::GateMargin => "wss://api.gateio.ws/ws/v4/",
                _ => "wss://fx-ws.gateio.ws/v4/ws/usdt",
            },
            //Bitget
            Exchange::Bitget => "wss://ws.bitget.com/v2/ws/public",
            Exchange::Hyperliquid => "wss://api.hyperliquid.xyz/ws",
        }
    }

    pub fn get_exchange_mkt_data_url(exchange: &Exchange) -> &'static str {
        Self::get_exchange_mkt_data_url_with_venue(exchange, TradingVenue::GateFutures)
    }

    pub fn get_exchange_kline_data_url_with_venue(
        exchange: &Exchange,
        venue: TradingVenue,
    ) -> &'static str {
        match exchange {
            //币安u本位期货合约和现货
            Exchange::Binance => {
                Self::get_binance_ws_url_with_route(venue, BinanceFuturesWsRoute::Market)
            }
            //Aster：单一 endpoint
            Exchange::Aster => Self::get_aster_ws_url_with_venue(venue),
            //OKEXu本位期货合约和现货
            Exchange::Okex => "wss://ws.okx.com:8443/ws/v5/business",
            //Bybitu本位期货合约和现货
            Exchange::Bybit => Self::get_bybit_public_ws_url_with_venue(venue),
            //Gate.io: 现货与合约分不同 endpoint
            Exchange::Gate => match venue {
                TradingVenue::GateMargin => "wss://api.gateio.ws/ws/v4/",
                _ => "wss://fx-ws.gateio.ws/v4/ws/usdt",
            },
            //Bitget
            Exchange::Bitget => "wss://ws.bitget.com/v2/ws/public",
            Exchange::Hyperliquid => "wss://api.hyperliquid.xyz/ws",
        }
    }

    pub fn get_exchange_kline_data_url(exchange: &Exchange) -> &'static str {
        Self::get_exchange_kline_data_url_with_venue(exchange, TradingVenue::GateFutures)
    }

    fn get_signal_subscribe_message(exchange: &Exchange, venue: TradingVenue) -> serde_json::Value {
        match exchange {
            Exchange::Binance | Exchange::Aster => {
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
                    "args": ["orderbook.1.BTCUSDT"]
                })
            }
            Exchange::Bitget => {
                let inst_type = bitget_inst_type_for_venue(venue);
                serde_json::json!({
                    "op": "subscribe",
                    "args": [serde_json::json!({
                        "instType": inst_type,
                        "channel": "books1",
                        "instId": "BTCUSDT"
                    })]
                })
            }
            Exchange::Gate => {
                let channel_prefix = gate_channel_prefix_for_venue(venue);
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                serde_json::json!({
                    "time": timestamp,
                    "channel": format!("{}.tickers", channel_prefix),
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
            let ch = SubscribeMsgs::get_inc_channel(&exchange, cfg.venue);
            if ch.is_none() {
                warn!(
                    "incremental orderbook is not supported for exchange={} venue={}; ignore data_types.enable_incremental=true",
                    exchange,
                    cfg.venue.data_pub_slug()
                );
            }
            ch
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
                trade_subscribe_msgs
                    .push(construct_subscribe_message(&exchange, cfg.venue, chunk, ch));
            }
            if let Some(ref ch) = kline_channel {
                kline_subscribe_msgs
                    .push(construct_subscribe_message(&exchange, cfg.venue, chunk, ch));
            }
            if let Some(ref ch) = best_price_spread_channel {
                ask_bid_spread_msgs
                    .push(construct_subscribe_message(&exchange, cfg.venue, chunk, ch));
            }
        }
        Self {
            active_symbols: symbols.iter().cloned().collect(),
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
            Exchange::Aster => {
                ExchangePerpsSubscribeMsgs::Aster(AsterPerpsSubscribeMsgs::new().await)
            }
        };

        Self {
            active_symbols: symbols.iter().cloned().collect(),
            exchange_msgs,
        }
    }

    pub fn get_active_symbols(&self) -> &HashSet<String> {
        &self.active_symbols
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_margin_order_book_update_subscribe_is_per_symbol_with_interval() {
        let symbols = vec!["BTC_USDT".to_string(), "ETH_USDT".to_string()];
        let msg = construct_subscribe_message(
            &Exchange::Gate,
            TradingVenue::GateMargin,
            &symbols,
            "order_book_update",
        );

        let arr = msg
            .as_array()
            .expect("gate margin order_book_update should generate message array");
        assert_eq!(arr.len(), 2);

        for item in arr {
            assert_eq!(
                item.get("channel").and_then(|v| v.as_str()),
                Some("spot.order_book_update")
            );
            let payload = item
                .get("payload")
                .and_then(|v| v.as_array())
                .expect("payload should be array");
            assert_eq!(payload.len(), 2);
            assert!(payload[0].as_str().unwrap_or("").ends_with("_USDT"));
            assert_eq!(payload[1].as_str(), Some("100ms"));
        }
    }

    #[test]
    fn gate_futures_order_book_update_subscribe_has_level_param() {
        let symbols = vec!["BTC_USDT".to_string()];
        let msg = construct_subscribe_message(
            &Exchange::Gate,
            TradingVenue::GateFutures,
            &symbols,
            "order_book_update",
        );

        assert_eq!(
            msg.get("channel").and_then(|v| v.as_str()),
            Some("futures.order_book_update")
        );
        let payload = msg
            .get("payload")
            .and_then(|v| v.as_array())
            .expect("payload should be array");
        assert_eq!(payload.len(), 3);
        assert_eq!(payload[0].as_str(), Some("BTC_USDT"));
        assert_eq!(payload[1].as_str(), Some("100ms"));
        assert_eq!(payload[2].as_str(), Some("100"));
    }

    #[test]
    fn binance_futures_stream_kinds_match_new_url_split() {
        assert_eq!(
            SubscribeMsgs::get_binance_ws_url_for_stream_kind(
                TradingVenue::BinanceFutures,
                BinanceFuturesStreamKind::Depth,
            ),
            BINANCE_FUTURES_PUBLIC_WS_URL
        );
        assert_eq!(
            SubscribeMsgs::get_binance_ws_url_for_stream_kind(
                TradingVenue::BinanceFutures,
                BinanceFuturesStreamKind::BookTicker,
            ),
            BINANCE_FUTURES_PUBLIC_WS_URL
        );
        assert_eq!(
            SubscribeMsgs::get_binance_ws_url_for_stream_kind(
                TradingVenue::BinanceFutures,
                BinanceFuturesStreamKind::Trade,
            ),
            BINANCE_FUTURES_PUBLIC_WS_URL
        );
        assert_eq!(
            SubscribeMsgs::get_binance_ws_url_for_stream_kind(
                TradingVenue::BinanceFutures,
                BinanceFuturesStreamKind::Kline,
            ),
            BINANCE_FUTURES_MARKET_WS_URL
        );
        assert_eq!(
            BinancePerpsSubscribeMsgs::WS_URL,
            BINANCE_FUTURES_MARKET_WS_URL
        );
    }

    #[test]
    fn binance_margin_keeps_spot_ws_url_for_all_routes() {
        assert_eq!(
            SubscribeMsgs::get_binance_ws_url_with_route(
                TradingVenue::BinanceMargin,
                BinanceFuturesWsRoute::Public,
            ),
            BINANCE_SPOT_WS_URL
        );
        assert_eq!(
            SubscribeMsgs::get_binance_ws_url_with_route(
                TradingVenue::BinanceMargin,
                BinanceFuturesWsRoute::Market,
            ),
            BINANCE_SPOT_WS_URL
        );
    }

    #[test]
    fn bybit_margin_uses_spot_public_ws_urls() {
        assert_eq!(
            SubscribeMsgs::get_exchange_mkt_data_url_with_venue(
                &Exchange::Bybit,
                TradingVenue::BybitMargin,
            ),
            BYBIT_SPOT_PUBLIC_WS_URL
        );
        assert_eq!(
            SubscribeMsgs::get_exchange_kline_data_url_with_venue(
                &Exchange::Bybit,
                TradingVenue::BybitMargin,
            ),
            BYBIT_SPOT_PUBLIC_WS_URL
        );
    }

    #[test]
    fn bybit_futures_uses_linear_public_ws_urls() {
        assert_eq!(
            SubscribeMsgs::get_exchange_mkt_data_url_with_venue(
                &Exchange::Bybit,
                TradingVenue::BybitFutures,
            ),
            BYBIT_LINEAR_PUBLIC_WS_URL
        );
        assert_eq!(
            SubscribeMsgs::get_exchange_kline_data_url_with_venue(
                &Exchange::Bybit,
                TradingVenue::BybitFutures,
            ),
            BYBIT_LINEAR_PUBLIC_WS_URL
        );
    }

    #[test]
    fn bybit_signal_uses_standard_orderbook_topic() {
        let msg = SubscribeMsgs::get_signal_subscribe_message(
            &Exchange::Bybit,
            TradingVenue::BybitFutures,
        );
        let args = msg
            .get("args")
            .and_then(|value| value.as_array())
            .expect("bybit signal args should be an array");

        assert_eq!(args.len(), 1);
        assert_eq!(args[0].as_str(), Some("orderbook.1.BTCUSDT"));
    }

    #[test]
    fn bybit_ask_bid_uses_legacy_topic_by_default() {
        assert_eq!(
            SubscribeMsgs::get_ask_bid_spread_channel(&Exchange::Bybit, TradingVenue::BybitFutures),
            "orderbook.1"
        );

        let msg = construct_subscribe_message(
            &Exchange::Bybit,
            TradingVenue::BybitFutures,
            &["BTCUSDT".to_string()],
            "orderbook.1",
        );
        let args = msg
            .get("args")
            .and_then(|value| value.as_array())
            .expect("bybit ask/bid args should be an array");

        assert_eq!(args.len(), 1);
        assert_eq!(args[0].as_str(), Some("orderbook.1.BTCUSDT"));
    }

    #[test]
    fn bybit_ask_bid_uses_public_ws_urls_by_default() {
        assert_eq!(
            SubscribeMsgs::get_bybit_ask_bid_spread_url_with_venue(TradingVenue::BybitMargin),
            BYBIT_SPOT_PUBLIC_WS_URL
        );
        assert_eq!(
            SubscribeMsgs::get_bybit_ask_bid_spread_url_with_venue(TradingVenue::BybitFutures),
            BYBIT_LINEAR_PUBLIC_WS_URL
        );
    }

    #[test]
    fn bybit_incremental_uses_supported_depth_topic() {
        assert_eq!(
            SubscribeMsgs::get_inc_channel(&Exchange::Bybit, TradingVenue::BybitFutures),
            Some("orderbook.1000".to_string())
        );
    }

    #[test]
    fn bitget_signal_uses_books1_topic() {
        let msg = SubscribeMsgs::get_signal_subscribe_message(
            &Exchange::Bitget,
            TradingVenue::BitgetFutures,
        );
        let args = msg
            .get("args")
            .and_then(|value| value.as_array())
            .expect("bitget signal args should be an array");

        assert_eq!(args.len(), 1);
        let arg = args[0]
            .as_object()
            .expect("bitget signal arg should be an object");
        assert_eq!(
            arg.get("instType").and_then(|value| value.as_str()),
            Some("USDT-FUTURES")
        );
        assert_eq!(
            arg.get("channel").and_then(|value| value.as_str()),
            Some("books1")
        );
        assert_eq!(
            arg.get("instId").and_then(|value| value.as_str()),
            Some("BTCUSDT")
        );
    }

    #[test]
    fn bitget_derivatives_are_futures_only() {
        assert!(bitget_derivatives_supported_for_venue(
            TradingVenue::BitgetFutures
        ));
        assert!(!bitget_derivatives_supported_for_venue(
            TradingVenue::BitgetMargin
        ));
    }
}
