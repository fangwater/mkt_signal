use anyhow::Result;
use bytes::{Buf, Bytes};
use hex;

/// 消息类型枚举（与mkt_signal保持一致）
#[repr(u32)]
pub enum MktMsgType {
    TimeSignal = 1111,
    TradeInfo = 1001,
    OrderBookInc = 1005,
    TpReset = 1009,
    Kline = 1010,
    MarkPrice = 1011,
    IndexPrice = 1012,
    LiquidationOrder = 1013,
    FundingRate = 1014,
    AskBidSpread = 1015,
    Error = 2222,
}

/// 买卖价差消息
#[derive(Debug, Clone)]
pub struct AskBidSpreadMsg {
    pub symbol: String,
    pub timestamp: i64,
    pub bid_price: f64,
    pub bid_amount: f64,
    pub ask_price: f64,
    pub ask_amount: f64,
}

/// 资金费率消息
#[derive(Debug, Clone)]
pub struct FundingRateMsg {
    pub symbol: String,
    pub funding_rate: f64,
    pub next_funding_time: i64,
    pub timestamp: i64,
}

/// 标记价格消息
#[derive(Debug, Clone)]
pub struct MarkPriceMsg {
    pub symbol: String,
    pub mark_price: f64,
    pub timestamp: i64,
}

/// 指数价格消息
#[derive(Debug, Clone)]
pub struct IndexPriceMsg {
    pub symbol: String,
    pub index_price: f64,
    pub timestamp: i64,
}

/// 解析买卖价差消息
pub fn parse_ask_bid_spread(data: &[u8]) -> Result<AskBidSpreadMsg> {
    if data.len() < 8 {
        anyhow::bail!("Message too short");
    }

    let mut cursor = Bytes::copy_from_slice(data);

    // 读取消息类型（4字节）
    let msg_type = cursor.get_u32_le();
    if msg_type != MktMsgType::AskBidSpread as u32 {
        anyhow::bail!("Invalid message type: {}", msg_type);
    }

    // 读取symbol长度（4字节）
    let symbol_len = cursor.get_u32_le() as usize;

    // 读取symbol
    if cursor.len() < symbol_len {
        anyhow::bail!("Invalid symbol length");
    }
    let symbol_bytes = cursor.copy_to_bytes(symbol_len);
    let symbol = String::from_utf8(symbol_bytes.to_vec())?;

    // 读取时间戳（8字节）
    let timestamp = cursor.get_i64_le();

    // 读取价格和数量（各8字节）
    let bid_price = cursor.get_f64_le();
    let bid_amount = cursor.get_f64_le();
    let ask_price = cursor.get_f64_le();
    let ask_amount = cursor.get_f64_le();

    Ok(AskBidSpreadMsg {
        symbol,
        timestamp,
        bid_price,
        bid_amount,
        ask_price,
        ask_amount,
    })
}

/// 解析资金费率消息
pub fn parse_funding_rate(data: &[u8]) -> Result<FundingRateMsg> {
    if data.len() < 8 {
        anyhow::bail!("Message too short");
    }

    let mut cursor = Bytes::copy_from_slice(data);

    // 读取消息类型（4字节）
    let msg_type = cursor.get_u32_le();
    if msg_type != MktMsgType::FundingRate as u32 {
        anyhow::bail!("Invalid message type: {}", msg_type);
    }

    // 读取symbol长度（4字节）
    let symbol_len = cursor.get_u32_le() as usize;

    // 读取symbol
    if cursor.len() < symbol_len {
        anyhow::bail!("Invalid symbol length");
    }
    let symbol_bytes = cursor.copy_to_bytes(symbol_len);
    let symbol = String::from_utf8(symbol_bytes.to_vec())?;

    // 读取资金费率（8字节）
    let funding_rate = cursor.get_f64_le();

    // 读取下次资金费时间（8字节）
    let next_funding_time = cursor.get_i64_le();

    // 读取时间戳（8字节）
    let timestamp = cursor.get_i64_le();

    Ok(FundingRateMsg {
        symbol,
        funding_rate,
        next_funding_time,
        timestamp,
    })
}

/// 解析标记价格消息
pub fn parse_mark_price(data: &[u8]) -> Result<MarkPriceMsg> {
    if data.len() < 12 {
        panic!(
            "Mark price message too short (len={}) raw={}",
            data.len(),
            hex::encode(data)
        );
    }

    let mut cursor = Bytes::copy_from_slice(data);

    // 读取消息类型（4字节）
    let msg_type = cursor.get_u32_le();
    if msg_type != MktMsgType::MarkPrice as u32 {
        anyhow::bail!("Invalid message type: {}", msg_type);
    }

    // 读取symbol长度（4字节）
    if cursor.len() < 4 {
        panic!(
            "Mark price missing symbol length (remaining={}) raw={}",
            cursor.len(),
            hex::encode(data)
        );
    }
    let symbol_len = cursor.get_u32_le() as usize;

    // 读取symbol
    if cursor.len() < symbol_len {
        panic!(
            "Mark price payload too short for symbol: need {} have {} raw={}",
            symbol_len,
            cursor.len(),
            hex::encode(data)
        );
    }
    let symbol_bytes = cursor.copy_to_bytes(symbol_len);
    let symbol = String::from_utf8(symbol_bytes.to_vec())?;

    // 读取标记价格（8字节）
    if cursor.len() < 8 {
        panic!(
            "Mark price payload missing price field (remaining={}) raw={}",
            cursor.len(),
            hex::encode(data)
        );
    }
    let mark_price = cursor.get_f64_le();

    // 读取时间戳（8字节）
    if cursor.len() < 8 {
        panic!(
            "Mark price payload missing timestamp field (remaining={}) raw={}",
            cursor.len(),
            hex::encode(data)
        );
    }
    let timestamp = cursor.get_i64_le();

    Ok(MarkPriceMsg {
        symbol,
        mark_price,
        timestamp,
    })
}

/// 解析指数价格消息
pub fn parse_index_price(data: &[u8]) -> Result<IndexPriceMsg> {
    if data.len() < 12 {
        panic!(
            "Index price message too short (len={}) raw={}",
            data.len(),
            hex::encode(data)
        );
    }

    let mut cursor = Bytes::copy_from_slice(data);

    let msg_type = cursor.get_u32_le();
    if msg_type != MktMsgType::IndexPrice as u32 {
        anyhow::bail!("Invalid message type: {}", msg_type);
    }

    if cursor.len() < 4 {
        panic!(
            "Index price missing symbol length (remaining={}) raw={}",
            cursor.len(),
            hex::encode(data)
        );
    }
    let symbol_len = cursor.get_u32_le() as usize;

    if cursor.len() < symbol_len {
        panic!(
            "Index price payload too short for symbol: need {} have {} raw={}",
            symbol_len,
            cursor.len(),
            hex::encode(data)
        );
    }

    let symbol_bytes = cursor.copy_to_bytes(symbol_len);
    let symbol = String::from_utf8(symbol_bytes.to_vec())?;

    if cursor.len() < 8 {
        panic!(
            "Index price payload missing price field (remaining={}) raw={}",
            cursor.len(),
            hex::encode(data)
        );
    }
    let index_price = cursor.get_f64_le();
    if cursor.len() < 8 {
        panic!(
            "Index price payload missing timestamp field (remaining={}) raw={}",
            cursor.len(),
            hex::encode(data)
        );
    }
    let timestamp = cursor.get_i64_le();

    Ok(IndexPriceMsg {
        symbol,
        index_price,
        timestamp,
    })
}

/// 获取消息类型
pub fn get_msg_type(data: &[u8]) -> Option<MktMsgType> {
    if data.len() < 4 {
        return None;
    }

    let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

    match msg_type {
        1111 => Some(MktMsgType::TimeSignal),
        1001 => Some(MktMsgType::TradeInfo),
        1005 => Some(MktMsgType::OrderBookInc),
        1009 => Some(MktMsgType::TpReset),
        1010 => Some(MktMsgType::Kline),
        1011 => Some(MktMsgType::MarkPrice),
        1012 => Some(MktMsgType::IndexPrice),
        1013 => Some(MktMsgType::LiquidationOrder),
        1014 => Some(MktMsgType::FundingRate),
        1015 => Some(MktMsgType::AskBidSpread),
        2222 => Some(MktMsgType::Error),
        _ => None,
    }
}
