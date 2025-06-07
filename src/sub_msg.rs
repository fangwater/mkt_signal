use serde_json::Value;
use crate::cfg::Config;

pub struct SubscribeMsgs {
    inc_subscribe_msgs: Vec<serde_json::Value>,
    trade_subscribe_msgs: Vec<serde_json::Value>,
}

impl SubscribeMsgs {
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
}

impl SubscribeMsgs {
    fn construct_subscribe_message(exchange: &str, symbols: &[String], channel: &str) -> Value {
        match exchange {
            "binance-futures" => {
                let params: Vec<String> = symbols.iter()
                    .map(|symbol| format!("{}@{}", symbol.to_lowercase(), channel))
                    .collect();
                serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": params,
                    "id": 1,
                })
            },
            "okex-swap" => {
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
            "bybit" => {
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
        for chunk in symbols.chunks(batch_size) {
            inc_subscribe_msgs.push(SubscribeMsgs::construct_subscribe_message(&cfg.get_exchange(), chunk, "inc"));
            trade_subscribe_msgs.push(SubscribeMsgs::construct_subscribe_message(&cfg.get_exchange(), chunk, "trade"));
        }
        Self { 
            inc_subscribe_msgs,
            trade_subscribe_msgs,
        }
    }
}
