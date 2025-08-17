///proxy 从tokio的broadcast 通过forwarder转发到tcp和ipc
use crate::forwarder::ZmqForwarder;
use crate::mkt_msg::MktMsgType;
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
            out_rx,
            proxy_shutdown,
            tp_reset_notify,
        }
    }

    /// 根据消息类型确定topic
    fn get_topic_from_msg_type(msg_type: u32) -> &'static str {
        // 先尝试转换为枚举类型
        let mkt_msg_type = match msg_type {
            1111 => MktMsgType::TimeSignal,
            1001 => MktMsgType::TradeInfo,
            1003 => MktMsgType::OrderBookSnapshot,
            1004 => MktMsgType::ParseredOrderBookSnapshot,
            1005 => MktMsgType::OrderBookInc,
            1006 => MktMsgType::ParseredOrderbookInc,
            1007 => MktMsgType::SymbolAdd,
            1008 => MktMsgType::SymbolDel,
            1009 => MktMsgType::TpReset,
            1010 => MktMsgType::Kline,
            1011 => MktMsgType::MarkPrice,
            1012 => MktMsgType::IndexPrice,
            1013 => MktMsgType::LiquidationOrder,
            1014 => MktMsgType::FundingRate,
            _ => MktMsgType::Error,
        };

        // 根据枚举类型确定topic
        match mkt_msg_type {
            // market_data - 市场数据（深度、交易）
            MktMsgType::TimeSignal |
            MktMsgType::TradeInfo |
            MktMsgType::OrderBookSnapshot |
            MktMsgType::ParseredOrderBookSnapshot |
            MktMsgType::OrderBookInc |
            MktMsgType::ParseredOrderbookInc |
            MktMsgType::SymbolAdd |
            MktMsgType::SymbolDel |
            MktMsgType::TpReset => "market_data",
            
            // kline_data - K线数据
            MktMsgType::Kline => "kline_data",
            
            // derivatives_metrics - 衍生品指标
            MktMsgType::MarkPrice |
            MktMsgType::IndexPrice |
            MktMsgType::LiquidationOrder |
            MktMsgType::FundingRate => "derivatives_metrics",
            
            // 默认归类到market_data
            MktMsgType::Error => "market_data",
        }
    }

    /// 从bytes消息中提取消息类型
    fn extract_msg_type(msg: &Bytes) -> u32 {
        if msg.len() >= 4 {
            u32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]])
        } else {
            2222 // Error type
        }
    }

    pub async fn run(&mut self) {
        let mut stats_timer = interval(Duration::from_secs(3));
        // 跳过第一次立即触发
        stats_timer.tick().await;
        
        loop {
            tokio::select! {
                _ = stats_timer.tick() => {
                    self.forwarder.log_stats();
                }
                _ = self.proxy_shutdown.changed() => {
                    if *self.proxy_shutdown.borrow() {
                        break;
                    }
                }
                _ = self.tp_reset_notify.notified() => {
                    log::info!("Sending tp reset message...");
                    match self.forwarder.send_tp_reset_msg("market_data").await {
                        true => {
                            log::info!("Sent tp reset message successfully");
                        }
                        false => {
                            log::error!("Failed to send tp reset message");
                            break;
                        }
                    }
                }
                msg = self.out_rx.recv() => {
                    if let Ok(msg) = msg {
                        // 从消息中提取类型并确定topic
                        let msg_type = Self::extract_msg_type(&msg);
                        let topic = Self::get_topic_from_msg_type(msg_type);
                        self.forwarder.send_msg(msg, topic).await;
                    }
                }
            }
        }
        log::info!("Proxy stopped gracefully");
    }
}