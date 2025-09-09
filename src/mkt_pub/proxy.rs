///proxy 从tokio的mpsc 通过forwarder转发到tcp和ipc
use crate::iceoryx_forwarder::IceOryxForwarder;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::{mpsc, watch};
use tokio::time::interval;
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

    // per-channel counters for rate diagnostics
    inc_cnt: u64,
    trade_cnt: u64,
    kline_cnt: u64,
    der_cnt: u64,
    signal_cnt: u64,
    spread_cnt: u64,
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
            inc_cnt: 0,
            trade_cnt: 0,
            kline_cnt: 0,
            der_cnt: 0,
            signal_cnt: 0,
            spread_cnt: 0,
        }
    }

    pub async fn run(&mut self) {
        let mut stats_timer = interval(Duration::from_secs(3));
        // 跳过第一次立即触发
        stats_timer.tick().await;

        let mut msg_count = 0u64;
        let mut last_log_count = 0u64;
        // snapshots for per-channel rate
        let mut last_inc = 0u64;
        let mut last_trade = 0u64;
        let mut last_kline = 0u64;
        let mut last_der = 0u64;
        let mut last_signal = 0u64;
        let mut last_spread = 0u64;

        loop {
            tokio::select! {
                _ = stats_timer.tick() => {
                    self.forwarder.log_stats();
                    let window_secs = 3u64;
                    let msgs_per_sec = (msg_count - last_log_count) / window_secs;
                    let inc_ps = (self.inc_cnt - last_inc) / window_secs;
                    let trade_ps = (self.trade_cnt - last_trade) / window_secs;
                    let kline_ps = (self.kline_cnt - last_kline) / window_secs;
                    let der_ps = (self.der_cnt - last_der) / window_secs;
                    let signal_ps = (self.signal_cnt - last_signal) / window_secs;
                    let spread_ps = (self.spread_cnt - last_spread) / window_secs;
                    log::debug!(
                        "Ingress rates: total={} msg/s | inc={} trade={} kline={} derivatives={} spread={} signal={} msg/s",
                        msgs_per_sec, inc_ps, trade_ps, kline_ps, der_ps, spread_ps, signal_ps
                    );
                    last_log_count = msg_count;
                    last_inc = self.inc_cnt;
                    last_trade = self.trade_cnt;
                    last_kline = self.kline_cnt;
                    last_der = self.der_cnt;
                    last_signal = self.signal_cnt;
                    last_spread = self.spread_cnt;
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
                    self.inc_cnt += 1;
                }
                // 接收交易数据
                Some(msg) = self.trade_rx.recv() => {
                    self.forwarder.send_msg(msg).await;
                    msg_count += 1;
                    self.trade_cnt += 1;
                }
                // 接收K线数据
                Some(msg) = self.kline_rx.recv() => {
                    self.forwarder.send_msg(msg).await;
                    msg_count += 1;
                    self.kline_cnt += 1;
                }
                // 接收衍生品数据
                Some(msg) = self.derivatives_rx.recv() => {
                    self.forwarder.send_msg(msg).await;
                    msg_count += 1;
                    self.der_cnt += 1;
                }
                // 接收信号数据
                Some(msg) = self.signal_rx.recv() => {
                    self.forwarder.send_msg(msg).await;
                    msg_count += 1;
                    self.signal_cnt += 1;
                }
                // 接收买卖价差数据
                Some(msg) = self.ask_bid_spread_rx.recv() => {
                    self.forwarder.send_msg(msg).await;
                    msg_count += 1;
                    self.spread_cnt += 1;
                }
            }
        }
        log::info!(
            "MpscProxy stopped gracefully. Total messages processed: {}",
            msg_count
        );
    }
}
