use serde_json::Value;
use crate::args::Args;

pub struct SubscribeMsgs {
    exchange: String,
    subscribe_msgs: Vec<serde_json::Value>
}

impl SubscribeMsgs {
    fn construct_subscribe_message(&self, symbols: &[String], channel: &str) -> Value {
        match self.exchange.as_str() {
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
    fn new(args: &Args) -> Self {
        let symbols = args.get_symbols().unwrap();
        let batch_size = args.get_batch_size();
        let mut subscribe_msgs = Vec::new();
        for chunk in symbols.chunks(batch_size) {
            subscribe_msgs.push(self.construct_subscribe_message(chunk, "trade"));
            subscribe_msgs.push(self.construct_subscribe_message(chunk, "inc"));
        }
        Self { exchange: args.get_exchange(), subscribe_msgs }
    }
}


struct SubscribClient {
    tx : broadcast::Sender<Bytes>, //每个batch copy一个tx， 从这个tx发送
    ws_url: String,
    subscribe_msgs: SubscribeMsgs,
    ws_handlers: Vec<Box<dyn WebSocketHandler>>,
    unix_socket_handler: Box<dyn UnixDomainSocketHandler>,
}

impl SubscribClient {
    fn new(args: &Args) -> Self {
        Self {
            tx: broadcast::channel::<Bytes>(1000),
            ws_url: args.get_exchange_url().unwrap(),
            subscribe_msgs: SubscribeMsgs::new(args),
            ws_handlers: Vec::new(),
            unix_socket_handler: Box::new(UnixDomainSocketHandler::new()),
        }
    }
}