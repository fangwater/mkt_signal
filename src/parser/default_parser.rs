use crate::mkt_msg::{MktMsg, MktMsgType};
use bytes::Bytes;
use tokio::sync::mpsc;

/// Parser trait - 直接使用mpsc发送解析结果
pub trait Parser: Send {
    /// 解析消息并通过mpsc发送
    /// 返回成功发送的消息数量
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize;
}

#[derive(Clone)]
pub struct DefaultTradeParser;

impl DefaultTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for DefaultTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 不做任何解析，直接转发，仅标记msg的type
        let mkt_msg = MktMsg::create(MktMsgType::TradeInfo, msg);
        let msg_bytes = mkt_msg.to_bytes();
        
        match tx.send(msg_bytes) {
            Ok(_) => 1,
            Err(_) => 0,
        }
    }
}

#[derive(Clone)]
pub struct DefaultIncParser;

impl DefaultIncParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for DefaultIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 不做任何解析，直接转发，仅标记msg的type
        let mkt_msg = MktMsg::create(MktMsgType::OrderBookInc, msg);
        let msg_bytes = mkt_msg.to_bytes();
        
        match tx.send(msg_bytes) {
            Ok(_) => 1,
            Err(_) => 0,
        }
    }
}