use crate::common::mkt_msg::MktMsgType;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::signal::common::TradingVenue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct TradeTick {
    pub symbol: String,
    pub trade_id: i64,
    pub timestamp_us: i64,
    pub timestamp_ms: i64,
    pub side: TradeSide,
    pub price: f64,
    pub amount: f64,
}

pub fn parse_trade(data: &[u8], venue: TradingVenue) -> Option<TradeTick> {
    if data.len() < 8 {
        return None;
    }

    let msg_type = u32::from_le_bytes(data[0..4].try_into().ok()?);
    if msg_type != MktMsgType::TradeInfo as u32 {
        return None;
    }

    let symbol_len = u32::from_le_bytes(data[4..8].try_into().ok()?) as usize;
    let min_len = 8 + symbol_len + 8 + 8 + 8 + 8;
    if data.len() < min_len {
        return None;
    }

    let symbol_raw = std::str::from_utf8(&data[8..8 + symbol_len]).ok()?;
    let symbol = normalize_symbol_for_venue(symbol_raw, venue);
    let mut offset = 8 + symbol_len;

    let trade_id = i64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let raw_timestamp = i64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    let timestamp_us = normalize_trade_timestamp_to_us(raw_timestamp);
    let timestamp_ms = timestamp_us / 1_000;
    offset += 8;

    let side = match data[offset] as char {
        'B' | 'b' => TradeSide::Buy,
        'S' | 's' => TradeSide::Sell,
        _ => return None,
    };
    offset += 8; // side + padding

    let price = f64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
    offset += 8;
    let amount = f64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);

    if !price.is_finite()
        || !amount.is_finite()
        || price <= 0.0
        || amount <= 0.0
        || timestamp_us <= 0
    {
        return None;
    }

    Some(TradeTick {
        symbol,
        trade_id,
        timestamp_us,
        timestamp_ms,
        side,
        price,
        amount,
    })
}

fn normalize_trade_timestamp_to_us(timestamp: i64) -> i64 {
    if timestamp <= 0 {
        return timestamp;
    }
    if timestamp >= 1_000_000_000_000_000 {
        timestamp
    } else if timestamp >= 1_000_000_000_000 {
        timestamp.saturating_mul(1_000)
    } else {
        timestamp
    }
}
