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
    IndexPrice = 1012,
    LiquidationOrder = 1013,
    FundingRate = 1014,
    Error = 2222,
}

#[allow(dead_code)]
pub struct MktMsg {
    pub msg_type: MktMsgType,
    pub msg_length: u32,
    pub data: Bytes
}   

#[repr(u32)]
#[derive(Debug, Clone, Copy)]
pub enum SignalSource {
    Error = 0,
    Ipc = 1,
    Tcp = 2,
}

pub struct SignalMsg {
    pub msg_type: MktMsgType,
    pub source: SignalSource,
    pub timestamp: i64
}

pub struct KlineMsg {
    pub msg_type: MktMsgType,
    pub symbol_length: u32,
    pub symbol: String,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub close_price: f64,
    pub volume: f64,
    pub timestamp: i64,
}

pub struct FundingRateMsg {
    pub msg_type: MktMsgType,
    pub symbol_length: u32,
    pub symbol: String,
    pub funding_rate: f64,
    pub next_funding_time: i64,
    pub timestamp: i64,
}

pub struct MarkPriceMsg {
    pub msg_type: MktMsgType,
    pub symbol_length: u32,
    pub symbol: String,
    pub mark_price: f64,
    pub timestamp: i64,
}

pub struct IndexPriceMsg {
    pub msg_type: MktMsgType,
    pub symbol_length: u32,
    pub symbol: String,
    pub index_price: f64,
    pub timestamp: i64,
}
/// 对永续合约来说, 预估结算

impl FundingRateMsg {
    /// 创建一个费率信息消息
    pub fn create(src: )
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

impl KlineMsg {
    /// Create a kline message
    pub fn create(
        symbol: String,
        open_price: f64,
        high_price: f64,
        low_price: f64,
        close_price: f64,
        volume: f64,
        timestamp: i64,
    ) -> Self {
        let symbol_length = symbol.len() as u32;
        Self {
            msg_type: MktMsgType::Kline,
            symbol_length,
            symbol,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            timestamp,
        }
    }

    /// Convert message to bytes
    pub fn to_bytes(&self) -> Bytes {
        // Calculate total size: msg_type(4) + symbol_length(4) + symbol + 5*f64(8*5) + timestamp(8)
        let total_size = 4 + 4 + self.symbol_length as usize + 5 * 8 + 8;
        let mut buf = BytesMut::with_capacity(total_size);
        
        // Write header
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.symbol_length);
        
        // Write symbol
        buf.put(self.symbol.as_bytes());
        
        // Write OHLCV data
        buf.put_f64_le(self.open_price);
        buf.put_f64_le(self.high_price);
        buf.put_f64_le(self.low_price);
        buf.put_f64_le(self.close_price);
        buf.put_f64_le(self.volume);
        
        // Write timestamp
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