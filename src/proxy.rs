///proxy 从tokio的broadcast 通过forwarder转发到tcp和ipc
use crate::forwarder::ZmqForwarder;
use bytes::Bytes;
use std::time::Duration;
use tokio::time::interval;
use tokio::sync::broadcast;
use tokio::sync::watch;
use std::sync::Arc;
use tokio::sync::Notify;

//proxy需要异步运行，因此需要实现send trait
pub struct Proxy {
    forwarder: ZmqForwarder, 
    out_rx : broadcast::Receiver<Bytes>, //消息的输出通道
    proxy_shutdown: watch::Receiver<bool>,
    tp_reset_notify: Arc<Notify>,
}


impl Proxy {
    pub fn new(forwarder: ZmqForwarder, out_rx: broadcast::Receiver<Bytes>, proxy_shutdown: watch::Receiver<bool>, tp_reset_notify: Arc<Notify>) -> Self {
        Self { 
            forwarder, 
            inc_rx, 
            trade_rx,
            binance_snapshot_rx,
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
            }
        }
        log::info!("Proxy stopped gracefully");
    }
}