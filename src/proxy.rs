///proxy 从tokio的mpsc 通过forwarder转发到tcp和ipc
use crate::iceoryx_forwarder::IceOryxForwarder;
use bytes::Bytes;
use std::time::Duration;
use tokio::time::interval;
use tokio::sync::{mpsc, broadcast, watch};
use std::sync::Arc;
use tokio::sync::Notify;
// 新的使用mpsc的Proxy
pub struct MpscProxy {
    forwarder: IceOryxForwarder,
    incremental_rx: mpsc::UnboundedReceiver<Bytes>,
    trade_rx: mpsc::UnboundedReceiver<Bytes>,
    kline_rx: mpsc::UnboundedReceiver<Bytes>,
    derivatives_rx: mpsc::UnboundedReceiver<Bytes>,
    signal_rx: mpsc::UnboundedReceiver<Bytes>,
    ask_bid_spread_rx: mpsc::UnboundedReceiver<Bytes>,
    proxy_shutdown: watch::Receiver<bool>,
    tp_reset_notify: Arc<Notify>,
}

impl MpscProxy {
    pub fn new(
        forwarder: IceOryxForwarder,
        incremental_rx: mpsc::UnboundedReceiver<Bytes>,
        trade_rx: mpsc::UnboundedReceiver<Bytes>,
        kline_rx: mpsc::UnboundedReceiver<Bytes>,
        derivatives_rx: mpsc::UnboundedReceiver<Bytes>,
        signal_rx: mpsc::UnboundedReceiver<Bytes>,
        ask_bid_spread_rx: mpsc::UnboundedReceiver<Bytes>,
        proxy_shutdown: watch::Receiver<bool>,
        tp_reset_notify: Arc<Notify>,
    ) -> Self {
        Self {
            forwarder,
            incremental_rx,
            trade_rx,
            kline_rx,
            derivatives_rx,
            signal_rx,
            ask_bid_spread_rx,
            proxy_shutdown,
            tp_reset_notify,
        }
    }

    pub async fn run(&mut self) {
        let mut stats_timer = interval(Duration::from_secs(3));
        // 跳过第一次立即触发
        stats_timer.tick().await;
        
        let mut msg_count = 0u64;
        let mut last_log_count = 0u64;
        
        loop {
            tokio::select! {
                _ = stats_timer.tick() => {
                    self.forwarder.log_stats();
                    let msgs_per_sec = (msg_count - last_log_count) / 3;
                    if msgs_per_sec > 0 {
                        log::info!("Messages processed: {} msgs/sec", msgs_per_sec);
                    }
                    last_log_count = msg_count;
                }
                _ = self.proxy_shutdown.changed() => {
                    if *self.proxy_shutdown.borrow() {
                        break;
                    }
                }
                _ = self.tp_reset_notify.notified() => {
                    log::info!("Sending tp reset message...");
                    if self.forwarder.send_tp_reset_msg().await {
                        log::info!("Sent tp reset message successfully");
                    } else {
                        // 不退出，记录错误并继续运行，避免 proxy 提前终止
                        log::error!("Failed to send tp reset message");
                    }
                }
                // 接收增量数据
                Some(msg) = self.incremental_rx.recv() => {
                    self.forwarder.send_msg(msg).await;
                    msg_count += 1;
                }
                // 接收交易数据
                Some(msg) = self.trade_rx.recv() => {
                    self.forwarder.send_msg(msg).await;
                    msg_count += 1;
                }
                // 接收K线数据
                Some(msg) = self.kline_rx.recv() => {
                    self.forwarder.send_msg(msg).await;
                    msg_count += 1;
                }
                // 接收衍生品数据
                Some(msg) = self.derivatives_rx.recv() => {
                    self.forwarder.send_msg(msg).await;
                    msg_count += 1;
                }
                // 接收信号数据
                Some(msg) = self.signal_rx.recv() => {
                    self.forwarder.send_msg(msg).await;
                    msg_count += 1;
                }
                // 接收买卖价差数据
                Some(msg) = self.ask_bid_spread_rx.recv() => {
                    self.forwarder.send_msg(msg).await;
                    msg_count += 1;
                }
            }
        }
        log::info!("MpscProxy stopped gracefully. Total messages processed: {}", msg_count);
    }
}
