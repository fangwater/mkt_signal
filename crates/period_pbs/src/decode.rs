use crate::period::normalize_timestamp_ms;
use anyhow::{bail, ensure, Result};

const MKT_MSG_TYPE_TRADE_INFO: u32 = 1001;
const MKT_MSG_TYPE_ORDER_BOOK_INC: u32 = 1005;

const HEADER_LEN: usize = 8;
const TRADE_FIXED_LEN: usize = 8 + 8 + 8 + 8 + 8;
const INC_FIXED_LEN: usize = 8 + 8 + 8 + 8 + 4 + 4;

#[derive(Debug, Clone, Copy)]
pub struct LevelRecord {
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub symbol: String,
    pub timestamp: i64,
    pub timestamp_ms: i64,
    pub side: char,
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Clone)]
pub struct IncRecord {
    pub symbol: String,
    pub first_update_id: i64,
    pub final_update_id: i64,
    pub timestamp: i64,
    pub timestamp_ms: i64,
    pub is_snapshot: bool,
    pub is_last: bool,
    pub chunk_index: u8,
    pub bids: Vec<LevelRecord>,
    pub asks: Vec<LevelRecord>,
}

#[derive(Debug, Clone)]
pub enum DecodedMarketMsg {
    Trade(TradeRecord),
    Incremental(IncRecord),
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketMsgKind {
    Trade,
    Incremental,
    Other,
}

pub fn peek_market_msg(payload: &[u8]) -> Result<(MarketMsgKind, &str)> {
    ensure!(payload.len() >= 4, "payload too short for msg_type");
    let kind = match read_u32_le(payload, 0)? {
        MKT_MSG_TYPE_TRADE_INFO => MarketMsgKind::Trade,
        MKT_MSG_TYPE_ORDER_BOOK_INC => MarketMsgKind::Incremental,
        _ => return Ok((MarketMsgKind::Other, "")),
    };
    let (symbol, _) = read_symbol_ref(payload)?;
    Ok((kind, symbol))
}

pub fn decode_market_msg(payload: &[u8]) -> Result<DecodedMarketMsg> {
    ensure!(payload.len() >= 4, "payload too short for msg_type");
    let msg_type = read_u32_le(payload, 0)?;
    match msg_type {
        x if x == MKT_MSG_TYPE_TRADE_INFO => decode_trade(payload).map(DecodedMarketMsg::Trade),
        x if x == MKT_MSG_TYPE_ORDER_BOOK_INC => {
            decode_incremental(payload).map(DecodedMarketMsg::Incremental)
        }
        _ => Ok(DecodedMarketMsg::Other),
    }
}

pub fn decode_market_msg_with_symbol(
    payload: &[u8],
    canonical_symbol: &str,
) -> Result<DecodedMarketMsg> {
    ensure!(payload.len() >= 4, "payload too short for msg_type");
    let msg_type = read_u32_le(payload, 0)?;
    match msg_type {
        x if x == MKT_MSG_TYPE_TRADE_INFO => {
            decode_trade_with_symbol(payload, canonical_symbol).map(DecodedMarketMsg::Trade)
        }
        x if x == MKT_MSG_TYPE_ORDER_BOOK_INC => {
            decode_incremental_with_symbol(payload, canonical_symbol)
                .map(DecodedMarketMsg::Incremental)
        }
        _ => Ok(DecodedMarketMsg::Other),
    }
}

pub fn decode_trade(payload: &[u8]) -> Result<TradeRecord> {
    let (symbol, off) = read_symbol(payload)?;
    decode_trade_after_symbol(payload, symbol, off)
}

pub fn decode_trade_with_symbol(payload: &[u8], canonical_symbol: &str) -> Result<TradeRecord> {
    let (_, off) = read_symbol_ref(payload)?;
    decode_trade_after_symbol(payload, canonical_symbol.to_string(), off)
}

fn decode_trade_after_symbol(
    payload: &[u8],
    symbol: String,
    mut off: usize,
) -> Result<TradeRecord> {
    ensure!(
        payload.len() >= off + TRADE_FIXED_LEN,
        "trade payload too short: len={} need={}",
        payload.len(),
        off + TRADE_FIXED_LEN
    );
    let _id = read_i64_le(payload, off)?;
    off += 8;
    let timestamp = read_i64_le(payload, off)?;
    off += 8;
    let side = payload[off] as char;
    off += 8;
    let price = read_f64_le(payload, off)?;
    off += 8;
    let amount = read_f64_le(payload, off)?;
    Ok(TradeRecord {
        symbol,
        timestamp,
        timestamp_ms: normalize_timestamp_ms(timestamp),
        side,
        price,
        amount,
    })
}

pub fn decode_incremental(payload: &[u8]) -> Result<IncRecord> {
    let (symbol, off) = read_symbol(payload)?;
    decode_incremental_after_symbol(payload, symbol, off)
}

pub fn decode_incremental_with_symbol(payload: &[u8], canonical_symbol: &str) -> Result<IncRecord> {
    let (_, off) = read_symbol_ref(payload)?;
    decode_incremental_after_symbol(payload, canonical_symbol.to_string(), off)
}

fn decode_incremental_after_symbol(
    payload: &[u8],
    symbol: String,
    mut off: usize,
) -> Result<IncRecord> {
    ensure!(
        payload.len() >= off + INC_FIXED_LEN,
        "incremental payload too short: len={} need={}",
        payload.len(),
        off + INC_FIXED_LEN
    );
    let first_update_id = read_i64_le(payload, off)?;
    off += 8;
    let final_update_id = read_i64_le(payload, off)?;
    off += 8;
    let timestamp = read_i64_le(payload, off)?;
    off += 8;
    let is_snapshot = payload[off] != 0;
    let is_last = payload[off + 1] != 0;
    let chunk_index = payload[off + 2];
    off += 8;
    let bids_count = read_u32_le(payload, off)? as usize;
    off += 4;
    let asks_count = read_u32_le(payload, off)? as usize;
    off += 4;
    let levels_len = (bids_count + asks_count) * 16;
    ensure!(
        payload.len() >= off + levels_len,
        "incremental levels truncated: len={} need={}",
        payload.len(),
        off + levels_len
    );

    let mut bids = Vec::with_capacity(bids_count);
    for _ in 0..bids_count {
        let price = read_f64_le(payload, off)?;
        off += 8;
        let amount = read_f64_le(payload, off)?;
        off += 8;
        bids.push(LevelRecord { price, amount });
    }

    let mut asks = Vec::with_capacity(asks_count);
    for _ in 0..asks_count {
        let price = read_f64_le(payload, off)?;
        off += 8;
        let amount = read_f64_le(payload, off)?;
        off += 8;
        asks.push(LevelRecord { price, amount });
    }

    Ok(IncRecord {
        symbol,
        first_update_id,
        final_update_id,
        timestamp,
        timestamp_ms: normalize_timestamp_ms(timestamp),
        is_snapshot,
        is_last,
        chunk_index,
        bids,
        asks,
    })
}

fn read_symbol(payload: &[u8]) -> Result<(String, usize)> {
    let (symbol, off) = read_symbol_ref(payload)?;
    Ok((symbol.to_string(), off))
}

fn read_symbol_ref(payload: &[u8]) -> Result<(&str, usize)> {
    ensure!(
        payload.len() >= HEADER_LEN,
        "payload too short for symbol header"
    );
    let symbol_len = read_u32_le(payload, 4)? as usize;
    ensure!(
        payload.len() >= HEADER_LEN + symbol_len,
        "payload too short for symbol: len={} symbol_len={}",
        payload.len(),
        symbol_len
    );
    let symbol = std::str::from_utf8(&payload[HEADER_LEN..HEADER_LEN + symbol_len])?;
    Ok((symbol, HEADER_LEN + symbol_len))
}

fn read_u32_le(payload: &[u8], off: usize) -> Result<u32> {
    let bytes = payload
        .get(off..off + 4)
        .ok_or_else(|| anyhow::anyhow!("u32 out of bounds at {}", off))?;
    Ok(u32::from_le_bytes(bytes.try_into()?))
}

fn read_i64_le(payload: &[u8], off: usize) -> Result<i64> {
    let bytes = payload
        .get(off..off + 8)
        .ok_or_else(|| anyhow::anyhow!("i64 out of bounds at {}", off))?;
    Ok(i64::from_le_bytes(bytes.try_into()?))
}

fn read_f64_le(payload: &[u8], off: usize) -> Result<f64> {
    let bytes = payload
        .get(off..off + 8)
        .ok_or_else(|| anyhow::anyhow!("f64 out of bounds at {}", off))?;
    Ok(f64::from_le_bytes(bytes.try_into()?))
}

pub fn validate_trade_side(side: char) -> Result<()> {
    if matches!(side, 'B' | 'S') {
        Ok(())
    } else {
        bail!("unexpected trade side {:?}", side)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_trade_payload() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(MKT_MSG_TYPE_TRADE_INFO).to_le_bytes());
        payload.extend_from_slice(&(7u32).to_le_bytes());
        payload.extend_from_slice(b"BTCUSDT");
        payload.extend_from_slice(&42i64.to_le_bytes());
        payload.extend_from_slice(&1_704_067_200_123_456i64.to_le_bytes());
        payload.push(b'B');
        payload.extend_from_slice(&[0u8; 7]);
        payload.extend_from_slice(&100.0f64.to_le_bytes());
        payload.extend_from_slice(&0.5f64.to_le_bytes());

        assert_eq!(
            peek_market_msg(&payload).expect("peek trade"),
            (MarketMsgKind::Trade, "BTCUSDT")
        );

        let trade = decode_trade(&payload).expect("decode trade");
        assert_eq!(trade.symbol, "BTCUSDT");
        assert_eq!(trade.timestamp, 1_704_067_200_123_456);
        assert_eq!(trade.timestamp_ms, 1_704_067_200_123);
        assert_eq!(trade.side, 'B');
        assert_eq!(trade.price, 100.0);
        assert_eq!(trade.amount, 0.5);
    }

    #[test]
    fn decodes_trade_with_canonical_symbol_override() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(MKT_MSG_TYPE_TRADE_INFO).to_le_bytes());
        payload.extend_from_slice(&(12u32).to_le_bytes());
        payload.extend_from_slice(b"1000PUMPUSDT");
        payload.extend_from_slice(&42i64.to_le_bytes());
        payload.extend_from_slice(&1_704_067_200_123_456i64.to_le_bytes());
        payload.push(b'B');
        payload.extend_from_slice(&[0u8; 7]);
        payload.extend_from_slice(&100.0f64.to_le_bytes());
        payload.extend_from_slice(&0.5f64.to_le_bytes());

        let trade = decode_trade_with_symbol(&payload, "PUMPUSDT").expect("decode mapped trade");
        assert_eq!(trade.symbol, "PUMPUSDT");
        assert_eq!(trade.timestamp_ms, 1_704_067_200_123);
    }

    #[test]
    fn decodes_incremental_payload() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(MKT_MSG_TYPE_ORDER_BOOK_INC).to_le_bytes());
        payload.extend_from_slice(&(7u32).to_le_bytes());
        payload.extend_from_slice(b"BTCUSDT");
        payload.extend_from_slice(&1i64.to_le_bytes());
        payload.extend_from_slice(&2i64.to_le_bytes());
        payload.extend_from_slice(&1_704_067_200_010i64.to_le_bytes());
        payload.push(0);
        payload.push(1);
        payload.push(0);
        payload.extend_from_slice(&[0u8; 5]);
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.extend_from_slice(&99.0f64.to_le_bytes());
        payload.extend_from_slice(&1.0f64.to_le_bytes());
        payload.extend_from_slice(&101.0f64.to_le_bytes());
        payload.extend_from_slice(&2.0f64.to_le_bytes());

        assert_eq!(
            peek_market_msg(&payload).expect("peek inc"),
            (MarketMsgKind::Incremental, "BTCUSDT")
        );

        let inc = decode_incremental(&payload).expect("decode inc");
        assert_eq!(inc.symbol, "BTCUSDT");
        assert_eq!(inc.timestamp, 1_704_067_200_010);
        assert!(inc.is_last);
        assert_eq!(inc.bids[0].price, 99.0);
        assert_eq!(inc.asks[0].amount, 2.0);
    }
}
