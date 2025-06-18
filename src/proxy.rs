///proxy 从tokio的broadcast 通过forwarder转发到tcp和ipc
use crate::forwarder::ZmqForwarder;
use bytes::Bytes;
use std::time::Duration;
use tokio::time::interval;
use tokio::sync::broadcast;
use crate::parser::default_parser::Parser;
use tokio::sync::watch;
use std::sync::Arc;
use tokio::sync::Notify;

//proxy需要异步运行，因此需要实现send trait
pub struct Proxy {
    forwarder: ZmqForwarder, 
    inc_rx : broadcast::Receiver<Bytes>,
    trade_rx : broadcast::Receiver<Bytes>,
    binance_snapshot_rx : broadcast::Receiver<Bytes>,
    inc_parser : Box<dyn Parser>,
    trade_parser : Box<dyn Parser>,
    proxy_shutdown: watch::Receiver<bool>,
    inc_count: u64,
    trade_count: u64,
    tp_reset_notify: Arc<Notify>,
}


impl Proxy {
    pub fn new(forwarder: ZmqForwarder, inc_rx: broadcast::Receiver<Bytes>, trade_rx: broadcast::Receiver<Bytes>, binance_snapshot_rx: broadcast::Receiver<Bytes>, proxy_shutdown: watch::Receiver<bool>, tp_reset_notify: Arc<Notify>) -> Self {
        use crate::parser::default_parser::{DefaultIncParser, DefaultTradeParser};
        Self { 
            forwarder, 
            inc_rx, 
            trade_rx,
            binance_snapshot_rx,
            inc_parser: Box::new(DefaultIncParser::new()),
            trade_parser: Box::new(DefaultTradeParser::new()),
            proxy_shutdown: proxy_shutdown,
            inc_count: 0,
            trade_count: 0,
            tp_reset_notify: tp_reset_notify,
        }
    }

    pub async fn run(&mut self) {
        let mut stats_timer = interval(Duration::from_secs(3));
        // 跳过第一次立即触发
        stats_timer.tick().await;
        
        loop {
            tokio::select! {
                _ = self.proxy_shutdown.changed() => {
                    if *self.proxy_shutdown.borrow() {
                        break;
                    }
                }
                _ = self.tp_reset_notify.notified() => {
                    log::info!("Sending tp reset message...");
                    match self.forwarder.send_tp_reset_msg().await {
                        true => {
                            log::info!("Sent tp reset message successfully");
                        }
                        false => {
                            log::error!("Failed to send tp reset message");
                            break;
                        }
                    }
                }
                msg = self.inc_rx.recv() => {
                    if let Ok(msg) = msg {
                        self.inc_count += 1;
                        if let Some(parsed_msg) = self.inc_parser.parse(msg) {
                            self.forwarder.send_msg(parsed_msg.to_bytes()).await;
                        }
                    }
                }
                msg = self.trade_rx.recv() => {
                    if let Ok(msg) = msg {
                        self.trade_count += 1;
                        if let Some(parsed_msg) = self.trade_parser.parse(msg) {
                            self.forwarder.send_msg(parsed_msg.to_bytes()).await;
                        }
                    }
                }
                msg = self.binance_snapshot_rx.recv() => {
                    if let Ok(msg) = msg {
                        //因为已经构造成MktMsg，所以直接发送
                        self.forwarder.send_msg(msg).await;
                    }
                }
                _ = stats_timer.tick() => {
                    self.forwarder.log_stats();
                    log::info!("inc_count: {}, trade_count: {}", self.inc_count, self.trade_count);
                    self.inc_count = 0;
                    self.trade_count = 0;
                }
            }
        }
        log::info!("Proxy stopped gracefully");
    }
}