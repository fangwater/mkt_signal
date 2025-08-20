use bytes::{Bytes, BufMut, BytesMut};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum MktMsgType {
    TimeSignal = 1111,//btc的Partial Book Depth 100ms 推送一次，作为collect的信号
    TradeInfo = 1001,
    OrderBookInc = 1005,
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
/// 对永续合约来说, 币安的预估结算没有意义，不需要考虑Estimated Settle Price字段

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Level {
    pub price: f64,
    pub amount: f64,
}

impl Level {
    pub fn new(price_str: &str, amount_str: &str) -> Self {
        let price = price_str.parse::<f64>().unwrap_or(0.0);
        let amount = amount_str.parse::<f64>().unwrap_or(0.0);
        Self { price, amount }
    }
    
    pub fn from_values(price: f64, amount: f64) -> Self {
        Self { price, amount }
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct IncMsg {
    pub msg_type: MktMsgType,
    pub symbol_length: u32,
    pub symbol: String,
    pub first_update_id: i64,
    pub final_update_id: i64,
    pub timestamp: i64,
    // 8字节对齐的字段
    pub is_snapshot: bool,
    // 在Rust中，我们使用数组来表示padding
    pub padding: [u8; 7],
    pub bids_count: u32,
    pub asks_count: u32,
    // 存储所有档位数据，bids在前，asks在后
    pub levels: Vec<Level>,
}

impl IncMsg {
    /// Create an incremental orderbook message
    pub fn create(
        symbol: String,
        first_update_id: i64,
        final_update_id: i64,
        timestamp: i64,
        is_snapshot: bool,
        bids_count: u32,
        asks_count: u32,
    ) -> Self {
        let symbol_length = symbol.len() as u32;
        let total_levels = (bids_count + asks_count) as usize;
        let levels = vec![Level::from_values(0.0, 0.0); total_levels];
        
        Self {
            msg_type: MktMsgType::OrderBookInc,
            symbol_length,
            symbol,
            first_update_id,
            final_update_id,
            timestamp,
            is_snapshot,
            padding: [0u8; 7],
            bids_count,
            asks_count,
            levels,
        }
    }

    /// Set a bid level
    pub fn set_bid_level(&mut self, index: usize, level: Level) {
        if index < self.bids_count as usize && index < self.levels.len() {
            self.levels[index] = level;
        }
    }

    /// Set an ask level  
    pub fn set_ask_level(&mut self, index: usize, level: Level) {
        let ask_start = self.bids_count as usize;
        let ask_index = ask_start + index;
        if index < self.asks_count as usize && ask_index < self.levels.len() {
            self.levels[ask_index] = level;
        }
    }

    /// Convert message to bytes (C++ compatible layout)
    pub fn to_bytes(&self) -> Bytes {
        // Calculate total size: 
        // msg_type(4) + symbol_length(4) + symbol + first_update_id(8) + final_update_id(8) + timestamp(8) +
        // is_snapshot(1) + padding(7) + bids_count(4) + asks_count(4) + levels(levels.len() * 16)
        let levels_size = self.levels.len() * std::mem::size_of::<Level>();
        let total_size = 4 + 4 + self.symbol_length as usize + 8 + 8 + 8 + 1 + 7 + 4 + 4 + levels_size;
        let mut buf = BytesMut::with_capacity(total_size);
        
        // Write header
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.symbol_length);
        
        // Write symbol
        buf.put(self.symbol.as_bytes());
        
        // Write orderbook data
        buf.put_i64_le(self.first_update_id);
        buf.put_i64_le(self.final_update_id);
        buf.put_i64_le(self.timestamp);
        
        // Write is_snapshot with 8-byte alignment (1 byte + 7 bytes padding)
        buf.put_u8(if self.is_snapshot { 1 } else { 0 });
        buf.put(&self.padding[..]);  // 7 bytes padding
        
        // Write counts
        buf.put_u32_le(self.bids_count);
        buf.put_u32_le(self.asks_count);
        
        // Write levels (price and amount pairs, 16 bytes each)
        for level in &self.levels {
            buf.put_f64_le(level.price);
            buf.put_f64_le(level.amount);
        }
        
        buf.freeze()
    }

    /// Get the total size of the message
    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        4 + 4 + self.symbol_length as usize + 8 + 8 + 8 + 8 + 4 + 4 + (self.levels.len() * 16)
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct TradeMsg {
    pub msg_type: MktMsgType,
    pub symbol_length: u32,
    pub symbol: String,
    pub id: i64,
    pub timestamp: i64,
    // 8字节对齐的字段
    pub side: char,
    // 在Rust中，我们使用数组来表示padding
    pub padding: [u8; 7],
    pub price: f64,
    pub amount: f64,
}

pub struct LiquidationMsg{
    pub msg_type: MktMsgType,
    pub symbol_length: u32,
    pub symbol: String,
    pub liquidation_side: char,
    pub executed_qty: f64,
    pub price: f64,
    pub timestamp: i64,
}

impl TradeMsg {
    /// Create a trade message with proper byte alignment
    pub fn create(
        symbol: String,
        id: i64,
        timestamp: i64,
        side: char,
        price: f64,
        amount: f64,
    ) -> Self {
        let symbol_length = symbol.len() as u32;
        Self {
            msg_type: MktMsgType::TradeInfo,
            symbol_length,
            symbol,
            id,
            timestamp,
            side,
            padding: [0u8; 7], // 7字节填充，确保8字节对齐
            price,
            amount,
        }
    }

    /// Convert message to bytes with proper alignment
    pub fn to_bytes(&self) -> Bytes {
        // Calculate total size: 
        // msg_type(4) + symbol_length(4) + symbol + id(8) + timestamp(8) + 
        // side(1) + padding(7) + price(8) + amount(8)
        let total_size = 4 + 4 + self.symbol_length as usize + 8 + 8 + 1 + 7 + 8 + 8;
        let mut buf = BytesMut::with_capacity(total_size);
        
        // Write header
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.symbol_length);
        
        // Write symbol
        buf.put(self.symbol.as_bytes());
        
        // Write trade data
        buf.put_i64_le(self.id);
        buf.put_i64_le(self.timestamp);
        
        // Write side with 8-byte alignment (side + 7 bytes padding)
        buf.put_u8(self.side as u8);
        buf.put(&self.padding[..]);  // 7 bytes padding
        
        // Write price and amount (both 8 bytes, naturally aligned)
        buf.put_f64_le(self.price);
        buf.put_f64_le(self.amount);
        
        buf.freeze()
    }

    /// Get the total aligned size of the message
    #[allow(dead_code)]
    pub fn aligned_size(&self) -> usize {
        4 + 4 + self.symbol_length as usize + 8 + 8 + 8 + 8 + 8  // Last 8 includes side+padding as one 8-byte unit
    }
}

impl LiquidationMsg {
    /// Create a liquidation message
    pub fn create(
        symbol: String,
        liquidation_side: char,
        executed_qty: f64,
        price: f64,
        timestamp: i64,
    ) -> Self {
        let symbol_length = symbol.len() as u32;
        Self {
            msg_type: MktMsgType::LiquidationOrder,
            symbol_length,
            symbol,
            liquidation_side,
            executed_qty,
            price,
            timestamp,
        }
    }

    /// Convert message to bytes
    pub fn to_bytes(&self) -> Bytes {
        // Calculate total size: msg_type(4) + symbol_length(4) + symbol + liquidation_side(1) + executed_qty(8) + price(8) + timestamp(8)
        let total_size = 4 + 4 + self.symbol_length as usize + 1 + 8 + 8 + 8;
        let mut buf = BytesMut::with_capacity(total_size);
        
        // Write header
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.symbol_length);
        
        // Write symbol
        buf.put(self.symbol.as_bytes());
        
        // Write liquidation data
        buf.put_u8(self.liquidation_side as u8);
        buf.put_f64_le(self.executed_qty);
        buf.put_f64_le(self.price);
        buf.put_i64_le(self.timestamp);
        
        buf.freeze()
    }
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

impl FundingRateMsg {
    /// Create a funding rate message
    pub fn create(
        symbol: String,
        funding_rate: f64,
        next_funding_time: i64,
        timestamp: i64,
    ) -> Self {
        let symbol_length = symbol.len() as u32;
        Self {
            msg_type: MktMsgType::FundingRate,
            symbol_length,
            symbol,
            funding_rate,
            next_funding_time,
            timestamp,
        }
    }

    /// Convert message to bytes
    pub fn to_bytes(&self) -> Bytes {
        // Calculate total size: msg_type(4) + symbol_length(4) + symbol + funding_rate(8) + next_funding_time(8) + timestamp(8)
        let total_size = 4 + 4 + self.symbol_length as usize + 8 + 8 + 8;
        let mut buf = BytesMut::with_capacity(total_size);
        
        // Write header
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.symbol_length);
        
        // Write symbol
        buf.put(self.symbol.as_bytes());
        
        // Write funding rate data
        buf.put_f64_le(self.funding_rate);
        buf.put_i64_le(self.next_funding_time);
        buf.put_i64_le(self.timestamp);
        
        buf.freeze()
    }
}

impl MarkPriceMsg {
    /// Create a mark price message
    pub fn create(
        symbol: String,
        mark_price: f64,
        timestamp: i64,
    ) -> Self {
        let symbol_length = symbol.len() as u32;
        Self {
            msg_type: MktMsgType::MarkPrice,
            symbol_length,
            symbol,
            mark_price,
            timestamp,
        }
    }

    /// Convert message to bytes
    pub fn to_bytes(&self) -> Bytes {
        // Calculate total size: msg_type(4) + symbol_length(4) + symbol + mark_price(8) + timestamp(8)
        let total_size = 4 + 4 + self.symbol_length as usize + 8 + 8;
        let mut buf = BytesMut::with_capacity(total_size);
        
        // Write header
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.symbol_length);
        
        // Write symbol
        buf.put(self.symbol.as_bytes());
        
        // Write mark price data
        buf.put_f64_le(self.mark_price);
        buf.put_i64_le(self.timestamp);
        
        buf.freeze()
    }
}

impl IndexPriceMsg {
    /// Create an index price message
    pub fn create(
        symbol: String,
        index_price: f64,
        timestamp: i64,
    ) -> Self {
        let symbol_length = symbol.len() as u32;
        Self {
            msg_type: MktMsgType::IndexPrice,
            symbol_length,
            symbol,
            index_price,
            timestamp,
        }
    }

    /// Convert message to bytes
    pub fn to_bytes(&self) -> Bytes {
        // Calculate total size: msg_type(4) + symbol_length(4) + symbol + index_price(8) + timestamp(8)
        let total_size = 4 + 4 + self.symbol_length as usize + 8 + 8;
        let mut buf = BytesMut::with_capacity(total_size);
        
        // Write header
        buf.put_u32_le(self.msg_type as u32);
        buf.put_u32_le(self.symbol_length);
        
        // Write symbol
        buf.put(self.symbol.as_bytes());
        
        // Write index price data
        buf.put_f64_le(self.index_price);
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