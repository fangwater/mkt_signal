use bytes::{Bytes, BufMut, BytesMut};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum MktMsgType {
    TimeSignal = 1111,//btc的Partial Book Depth 100ms 推送一次，作为collect的信号
    TradeInfo = 1001,
    ParseredTradeInfo = 1002,
    OrderBookSnapshot = 1003,
    ParseredOrderBookSnapshot = 1004,
    OrderBookInc = 1005,
    ParseredOrderbookInc = 1006,
    SymbolAdd = 1007,
    SymbolDel = 1008,
    TpReset = 1009,
    Kline = 1010,
    MarkPrice = 1011,
    LiquidationOrder = 1012,
    Error = 1013,
}

#[allow(dead_code)]
pub struct MktMsg {
    pub msg_type: MktMsgType,
    pub msg_length: u32,
    pub data: Bytes
}   

#[repr(u32)]
#[derive(Debug, Clone, Copy)]
enum SignalSource {
    pubError = 0,
    Ipc = 1,
    Tcp = 2,
}

pub struct SignalMsg {
    pub msg_type: MktMsgType,
    pub source: SignalSource,
    pub timestamp: i64
}

impl SignalMsg {
    /// 创建一个时间信号消息
    pub fn create(src: SignalSource, tp: i64) -> Self {
        Self {
            msg_type : MktMsgType::TimeSignal,
            source: src,
            timestamp: tp,
        }
    }
    /// 将消息转换为字节数组
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.source as u32);
        buf.put_i64_le(self.timestamp);
        buf.freeze()  
    }
}


impl MktMsg {
    /// 从bytes创建消息
    pub fn create(msg_type: MktMsgType, data: Bytes) -> Self {
        Self {
            msg_type,
            msg_length : data.len() as u32,
            data,
        }
    }

    /// 创建SymbolDel消息
    // pub fn symbol_del(symbol: impl AsRef<str>) -> Self {
    //     Self::create(MktMsgType::SymbolDel, Bytes::from(symbol.as_ref().to_string()))
    // }

    /// 创建错误消息
    // pub fn error(msg: impl AsRef<str>) -> Self {
    //     Self::create(MktMsgType::Error, Bytes::from(msg.as_ref().to_string()))
    // }

    /// 创建TP Reset消息
    pub fn tp_reset() -> Self {
        Self::create(MktMsgType::TpReset, Bytes::new())
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8 + self.data.len());
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.msg_length as u32);
        buf.put(self.data.clone());
        buf.freeze()
    }
}