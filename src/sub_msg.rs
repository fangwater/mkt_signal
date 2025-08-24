use serde_json::Value;
use crate::cfg::Config;
use std::collections::HashSet;

fn construct_subscribe_message(exchange: &str, symbols: &[String], channel: &str) -> Value {
    match exchange {
        "binance-futures" | "binance" => {
            let params: Vec<String> = symbols.iter()
                .map(|symbol| format!("{}@{}", symbol.to_lowercase(), channel))
                .collect();
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": params,
                "id": 1,
            })
        },
        "okex-swap" | "okex" => {
            let args: Vec<Value> = symbols.iter()
                .map(|symbol| serde_json::json!({
                    "channel": channel,
                    "instId": symbol
                }))
                .collect();
            serde_json::json!({
                "op": "subscribe",
                "args": args
            })
        },
        "bybit" | "bybit-spot" => {
            let args: Vec<String> = symbols.iter()
                .map(|symbol| format!("{}.{}",channel,symbol))
                .collect();
            serde_json::json!({
                "op": "subscribe",
                "args": args
            })
        },
        _ => panic!("Unsupported exchange: {}", exchange)
    }
}

//市场高频数据的订阅消息
//包含一个信号，用于切分数据
//其次是增量行情快照数据和逐笔成交数据
pub struct SubscribeMsgs {
    active_symbols: HashSet<String>,//当前所有u本位符号
    inc_subscribe_msgs: Vec<serde_json::Value>,//增量orderbook
    trade_subscribe_msgs: Vec<serde_json::Value>, //逐笔成交
    kline_subscribe_msgs: Vec<serde_json::Value>, //k线
    signal_subscribe_msg: serde_json::Value, //只需要一个，实际是和btc深度有关的某个行情
}

#[derive(Debug, Clone)]
pub struct BinancePerpsSubscribeMsgs {
    pub mark_price_stream_for_all_market: serde_json::Value, // 币安的markprice订阅全市场，包含资金费率，指数价格等信息
    pub liquidation_orders_msg: serde_json::Value,//强平信息
}

impl BinancePerpsSubscribeMsgs {
    pub const WS_URL: &'static str = "wss://fstream.binance.com/ws"; 
    pub async fn new() -> Self {
        Self {
            mark_price_stream_for_all_market : serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": ["!markPrice@arr@1s"],
                    "id": 1,
                }), 
            liquidation_orders_msg : serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": ["!forceOrder@arr"],
                    "id": 1,
                }), 
        }
    }
}

#[derive(Debug, Clone)]
pub struct OkexPerpsSubscribeMsgs {
    pub unified_perps_msgs: Vec<serde_json::Value>, //统一的衍生品订阅消息，包含标记价格、指数价格、资金费率、强平信息
}

impl OkexPerpsSubscribeMsgs {
    pub const WS_URL: &'static str = "wss://ws.okx.com:8443/ws/v5/public"; 
    pub async fn new(cfg: &Config) -> Self {
        let symbols: Vec<String> = cfg.get_symbols().await.unwrap();
        let batch_size = cfg.get_batch_size();
        let mut unified_perps_msgs = Vec::new();
        
        // 为每个批次创建统一的订阅消息，包含4种数据类型
        for chunk in symbols.chunks(batch_size) {
            let mut args = Vec::new();
            
            // 添加标记价格订阅
            for symbol in chunk {
                args.push(serde_json::json!({
                    "channel": "mark-price",
                    "instId": symbol
                }));
            }
            
            // 添加指数价格订阅（使用USD作为基准）
            for symbol in chunk {
                // 从USDT永续合约符号转换为USD指数符号 (如：BTC-USDT-SWAP -> BTC-USD)
                let index_symbol = symbol.replace("-USDT-SWAP", "-USD").replace("-USDT", "-USD");
                args.push(serde_json::json!({
                    "channel": "index-tickers", 
                    "instId": index_symbol
                }));
            }
            
            // 添加资金费率订阅
            for symbol in chunk {
                args.push(serde_json::json!({
                    "channel": "funding-rate",
                    "instId": symbol
                }));
            }
            
            // 为每个批次添加强平信息（只需要一次，但每个批次都包含）
            args.push(serde_json::json!({
                "channel": "liquidation-orders",
                "instType": "SWAP"
            }));
            
            // 创建统一的订阅消息
            unified_perps_msgs.push(serde_json::json!({
                "op": "subscribe",
                "args": args
            }));
        }
        
        Self {
            unified_perps_msgs
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
        for chunk in symbols.chunks(batch_size) {
            ticker_stream_msgs.push(construct_subscribe_message(cfg.get_exchange().as_str(), chunk, "tickers"));
            liquidation_orders_msgs.push(construct_subscribe_message(cfg.get_exchange().as_str(), chunk, "allLiquidation"));
        }
        Self {
            ticker_stream_msgs : ticker_stream_msgs,
            liquidation_orders_msgs : liquidation_orders_msgs
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExchangePerpsSubscribeMsgs {
    Binance(BinancePerpsSubscribeMsgs),
    Okex(OkexPerpsSubscribeMsgs),
    Bybit(BybitPerpsSubscribeMsgs),
}

//衍生品(永续合约)的额外消息
pub struct DerivativesMetricsSubscribeMsgs {
    pub active_symbols: HashSet<String>, //当前所有u本位永续合约的symbol
    pub exchange_msgs: ExchangePerpsSubscribeMsgs,
}


impl SubscribeMsgs {
    pub fn get_time_signal_subscribe_msg(&self) -> serde_json::Value{
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

    pub fn get_trade_subscribe_msg_len(&self) -> usize {
        self.trade_subscribe_msgs.len()
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
                println!("  Added ({}): {:?}", 
                    added_count,
                    new_set.difference(old_set).collect::<Vec<_>>()
                );
            }else{
                println!("  No new symbols added");
            }
            if removed_count > 0 {
                println!("  Removed ({}): {:?}",
                    removed_count,
                    old_set.difference(&new_set).collect::<Vec<_>>()
                );
            }else{
                println!("  No symbols removed");
            }
        }else{
            println!("No symbol changes");
        }
    }

    fn get_inc_channel(exchange: &str) -> String {
        match exchange {
            "binance-futures" => "depth@0ms".to_string(),
            "binance" => "depth@100ms".to_string(),
            "okex-swap" => "books".to_string(),
            "okex" => "books".to_string(),
            "bybit" => "orderbook.500".to_string(),
            "bybit-spot" => "orderbook.200".to_string(),
            _ => panic!("Unsupported exchange: {}", exchange)
        }
    }
    fn get_kline_channel(exchange: &str) -> String {
        match exchange {
            "binance-futures" | "binance" => "kline_1m".to_string(),
            "okex-swap" | "okex" => "candle1D".to_string(),
            "bybit" | "bybit-spot" => "kline.1".to_string(),
            _ => panic!("Unsupported exchange: {}", exchange)
        }
    }
    
    fn get_trade_channel(exchange: &str) -> String {
        match exchange {
            "binance-futures" | "binance" => "trade".to_string(),
            "okex-swap" | "okex" => "trades".to_string(),
            "bybit" | "bybit-spot" => "publicTrade".to_string(),
            _ => panic!("Unsupported exchange: {}", exchange)
        }
    }
       
}


impl SubscribeMsgs {
    pub fn get_exchange_mkt_data_url(exchange: &str) -> &'static str {
        match exchange {
            //币安u本位期货合约
            "binance-futures" => "wss://fstream.binance.com/ws",
            //币安u本位期货合约对应的现货
            "binance" => "wss://data-stream.binance.vision/ws",
            //OKEXu本位期货合约
            "okex-swap" => "wss://ws.okx.com:8443/ws/v5/public",
            //OKEXu本位期货合约对应的现货
            "okex" => "wss://ws.okx.com:8443/ws/v5/public",
            //Bybitu本位期货合约
            "bybit" => "wss://stream.bybit.com/v5/public/linear",
            //Bybitu本位期货合约对应的现货
            "bybit-spot" => "wss://stream.bybit.com/v5/public/spot",
            _ => panic!("Unsupported exchange: {}", exchange)
        }
    }

    pub fn get_exchange_kline_data_url(exchange: &str) -> &'static str {
        match exchange {
            //币安u本位期货合约
            "binance-futures" => "wss://fstream.binance.com/ws",
            //币安u本位期货合约对应的现货
            "binance" => "wss://data-stream.binance.vision/ws",
            //OKEXu本位期货合约
            "okex-swap" => "wss://ws.okx.com:8443/ws/v5/business",
            //OKEXu本位期货合约对应的现货
            "okex" => "wss://ws.okx.com:8443/ws/v5/business",
            //Bybitu本位期货合约
            "bybit" => "wss://stream.bybit.com/v5/public/linear",
            //Bybitu本位期货合约对应的现货
            "bybit-spot" => "wss://stream.bybit.com/v5/public/spot",
            _ => panic!("Unsupported exchange: {}", exchange)
        }
    }

    fn get_signal_subscribe_message(exchange: &str) -> serde_json::Value{
        match exchange {
            "binance-futures" => {
                serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": ["btcusdt@depth5@100ms"],
                    "id": 1,
                })
            },
            "binance" => {
                serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": ["btcusdt@depth@100ms"],
                    "id": 1,
                })
            },
            "okex-swap" => {
                serde_json::json!({
                    "op": "subscribe",
                    "args": serde_json::json!({
                        "channel": "books5",
                        "instId": "BTC-USDT-SWAP"
                    })
                })
            },
            "okex" => {
                serde_json::json!({
                    "op": "subscribe",
                    "args": serde_json::json!({
                        "channel": "books5",
                        "instId": "BTC-USDT"
                    })
                })
            },

            "bybit" | "bybit-spot" => {
                serde_json::json!({
                    "op": "subscribe",
                    "args": ["orderbook.rpi.BTCUSDT"]
                })
            },
            _ => panic!("Unsupported exchange: {}", exchange)
        }
    }
    pub async fn new(cfg: &Config) -> Self {
        let symbols: Vec<String> = cfg.get_symbols().await.unwrap();
        let batch_size = cfg.get_batch_size();
        let mut inc_subscribe_msgs = Vec::new();
        let mut trade_subscribe_msgs = Vec::new();
        let mut kline_subscribe_msgs = Vec::new();
        let exchange = cfg.get_exchange();
        let inc_channel = SubscribeMsgs::get_inc_channel(&exchange);
        let trade_channel = SubscribeMsgs::get_trade_channel(&exchange);
        let kline_channel = SubscribeMsgs::get_kline_channel(&exchange);
        for chunk in symbols.chunks(batch_size) {
            inc_subscribe_msgs.push(construct_subscribe_message(&exchange, chunk, &inc_channel));
            trade_subscribe_msgs.push(construct_subscribe_message(&exchange, chunk, &trade_channel));
            kline_subscribe_msgs.push(construct_subscribe_message(&exchange, chunk, &kline_channel));
        }
        Self {             
            active_symbols: symbols.iter().map(|s| s.clone()).collect(),
            inc_subscribe_msgs,
            trade_subscribe_msgs,
            kline_subscribe_msgs,
            signal_subscribe_msg: SubscribeMsgs::get_signal_subscribe_message(&exchange)
        }
    }
}


impl DerivativesMetricsSubscribeMsgs {
    pub async fn new(cfg: &Config) -> Self {
        let symbols: Vec<String> = cfg.get_symbols().await.unwrap();
        let exchange = cfg.get_exchange();
        let exchange_msgs = match exchange.as_str() {
            "binance-futures" => {
                ExchangePerpsSubscribeMsgs::Binance(BinancePerpsSubscribeMsgs::new().await)
            },
            "okex-swap"=> {
                ExchangePerpsSubscribeMsgs::Okex(OkexPerpsSubscribeMsgs::new(cfg).await)
            },
            "bybit"=> {
                ExchangePerpsSubscribeMsgs::Bybit(BybitPerpsSubscribeMsgs::new(cfg).await)
            },
            _ => panic!("Unsupported exchange: {}", exchange)
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
