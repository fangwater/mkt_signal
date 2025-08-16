use crate::mkt_msg::{MktMsg, MktMsgType};
use bytes::Bytes;
use tokio::sync::broadcast;

///需要根据不同交易所实现，因此是一个纯粹的trait
pub trait Parser: Send {
    ///有可能消息被drop，也可能一个消息产生多个消息，所以返回一个count, 表示产生了多少消息
    ///参数需要输入一个broadcast::Sender<Bytes>，表示输出到这个
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize;
}

pub struct DefaultTradeParser;

impl DefaultTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for DefaultTradeParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        //不做任何行为，直接转发，仅标记msg的type
        let mkt_msg = MktMsg::create(MktMsgType::TradeInfo, msg);
        let msg_bytes = mkt_msg.to_bytes();
        
        if let Err(_) = sender.send(msg_bytes) {
            return 0;
        }
        1
    }
}

pub struct DefaultIncParser;

impl DefaultIncParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for DefaultIncParser {
    fn parse(&self, msg: Bytes, sender: &broadcast::Sender<Bytes>) -> usize {
        //不做任何行为，直接转发，仅标记msg的type
        let mkt_msg = MktMsg::create(MktMsgType::OrderBookInc, msg);
        let msg_bytes = mkt_msg.to_bytes();
        
        if let Err(_) = sender.send(msg_bytes) {
            return 0;
        }
        1
    }
}

