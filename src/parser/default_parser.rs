use crate::mkt_msg::{MktMsg, MktMsgType};
use bytes::Bytes;
///需要根据不同交易所实现，因此是一个纯粹的trait
pub trait Parser: Send {
    ///有可能消息被drop，因此需要返回Option
    fn parse(&self, msg: Bytes) -> Option<MktMsg>;
}

pub struct DefaultTradeParser;

impl DefaultTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for DefaultTradeParser {
    fn parse(&self, msg: Bytes) -> Option<MktMsg> {
        //不做任何行为，直接转发，仅标记msg的type
        Some(MktMsg::create(MktMsgType::TradeInfo, msg))
    }
}

pub struct DefaultIncParser;

impl DefaultIncParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for DefaultIncParser {
    fn parse(&self, msg: Bytes) -> Option<MktMsg> {
        //不做任何行为，直接转发，仅标记msg的type
        Some(MktMsg::create(MktMsgType::OrderBookInc, msg))
    }
}
