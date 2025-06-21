use serde_json::Value;
use crate::cfg::Config;
use std::collections::HashSet;

pub struct SubscribeMsgs {
    active_symbols: HashSet<String>,
    inc_subscribe_msgs: Vec<serde_json::Value>,
    trade_subscribe_msgs: Vec<serde_json::Value>,
}

impl SubscribeMsgs {
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
    
    fn get_trade_channel(exchange: &str) -> String {
        match exchange {
            "binance-futures" => "trade".to_string(),
            "binance" => "trade".to_string(),
            "okex-swap" => "trades".to_string(),
            "okex" => "trades".to_string(),
            "bybit" => "publicTrade".to_string(),
            "bybit-spot" => "publicTrade".to_string(),
            _ => panic!("Unsupported exchange: {}", exchange)
        }
    }
       
}

impl SubscribeMsgs {
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
    pub async fn new(cfg: &Config) -> Self {
        let symbols = cfg.get_symbols().await.unwrap();
        let batch_size = cfg.get_batch_size();
        let mut inc_subscribe_msgs = Vec::new();
        let mut trade_subscribe_msgs = Vec::new();
        let exchange = cfg.get_exchange();
        let inc_channel = SubscribeMsgs::get_inc_channel(&exchange);
        let trade_channel = SubscribeMsgs::get_trade_channel(&exchange);
        for chunk in symbols.chunks(batch_size) {
            inc_subscribe_msgs.push(SubscribeMsgs::construct_subscribe_message(&exchange, chunk, &inc_channel));
            trade_subscribe_msgs.push(SubscribeMsgs::construct_subscribe_message(&exchange, chunk, &trade_channel));
        }
        Self { 
            inc_subscribe_msgs,
            trade_subscribe_msgs,
            active_symbols: symbols.iter().map(|s| s.clone()).collect(),
        }
    }
}
