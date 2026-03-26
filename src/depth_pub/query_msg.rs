use anyhow::{anyhow, Result};
use std::io::{Read, Write};
use std::path::PathBuf;

pub const DEPTH_QUERY_PAYLOAD: usize = 256;
pub const DEPTH_QUERY_SOCKET_PREFIX: &str = "/tmp/mkt_signal_depth_query";
const HEADER_LEN: usize = 2;
const FRAME_LEN: usize = 2;
const BATCH_FIXED_LEN: usize = 12;
const TOP5_MAX_LEVELS: usize = 5;
const TOP5_LEVEL_BYTES: usize = 16; // tick_index(i64) + tlen(f64)
const TOP5_FIXED_LEN: usize = 12; // ts(i64) + bid_count(u8) + ask_count(u8) + reserved(u16)

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthQueryType {
    LoadTlenSingle = 1,
    LoadTlenBatch = 2,
    Top5PriceTlen = 3,
    Stats = 4,
}

impl DepthQueryType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::LoadTlenSingle),
            2 => Some(Self::LoadTlenBatch),
            3 => Some(Self::Top5PriceTlen),
            4 => Some(Self::Stats),
            _ => None,
        }
    }
}

pub const RESP_STATUS_OK: u8 = 0;
pub const RESP_STATUS_BAD_REQUEST: u8 = 1;
pub const RESP_STATUS_SYMBOL_MISSING: u8 = 2;
pub const RESP_STATUS_BOOK_INVALID: u8 = 3;
pub const RESP_STATUS_UNSUPPORTED_TYPE: u8 = 4;
pub const RESP_STATUS_PAYLOAD_TOO_LARGE: u8 = 5;
pub const RESP_STATUS_DEPTH_DISABLED: u8 = 6;

/// tlen 查询返回值语义（适用于单价与批量查询）
/// - `-1.0`: 查询输入非法或上下文非法（例如 symbol 无效、tick_index 无效、订单簿不可用）
/// - `0.0`: 查询合法，但该价格档位当前无量
/// - `>0.0`: 查询合法，且档位上存在对应量
pub const TLEN_QUERY_AMOUNT_INVALID: f64 = -1.0;
pub const TLEN_QUERY_AMOUNT_EMPTY: f64 = 0.0;

pub fn resp_status_name(status: u8) -> &'static str {
    match status {
        RESP_STATUS_OK => "ok",
        RESP_STATUS_BAD_REQUEST => "bad_request",
        RESP_STATUS_SYMBOL_MISSING => "symbol_missing",
        RESP_STATUS_BOOK_INVALID => "book_invalid",
        RESP_STATUS_UNSUPPORTED_TYPE => "unsupported_type",
        RESP_STATUS_PAYLOAD_TOO_LARGE => "payload_too_large",
        RESP_STATUS_DEPTH_DISABLED => "depth_disabled",
        _ => "unknown",
    }
}

pub fn build_depth_query_socket_path(venue: &str) -> PathBuf {
    let sanitized: String = venue
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    PathBuf::from(format!("{}_{}.sock", DEPTH_QUERY_SOCKET_PREFIX, sanitized))
}

pub fn price_to_tick_index(price: f64, tick: f64) -> Option<i64> {
    if !price.is_finite() || !tick.is_finite() || tick <= 0.0 {
        return None;
    }

    let steps = price / tick;
    if !steps.is_finite() {
        return None;
    }

    Some(steps.round() as i64)
}

pub fn tick_index_to_price(tick_index: i64, tick: f64) -> Option<f64> {
    if !tick.is_finite() || tick <= 0.0 {
        return None;
    }

    let price = tick_index as f64 * tick;
    if price.is_finite() {
        Some(price)
    } else {
        None
    }
}

pub fn write_depth_query_frame<W: Write>(writer: &mut W, payload: &[u8]) -> Result<()> {
    if payload.is_empty() {
        return Err(anyhow!("depth query frame payload is empty"));
    }
    if payload.len() > u16::MAX as usize {
        return Err(anyhow!(
            "depth query frame payload too large: {}",
            payload.len()
        ));
    }

    writer.write_all(&(payload.len() as u16).to_le_bytes())?;
    writer.write_all(payload)?;
    writer.flush()?;
    Ok(())
}

pub fn read_depth_query_frame<R: Read>(reader: &mut R, max_payload: usize) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; FRAME_LEN];
    reader.read_exact(&mut len_buf)?;
    let payload_len = u16::from_le_bytes(len_buf) as usize;
    if payload_len == 0 {
        return Err(anyhow!("depth query frame payload is empty"));
    }
    if payload_len > max_payload {
        return Err(anyhow!(
            "depth query frame payload too large: {} > {}",
            payload_len,
            max_payload
        ));
    }

    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload)?;
    Ok(payload)
}

#[derive(Debug, Clone)]
pub struct DepthQueryHeader {
    pub query_type: u8,
    pub symbol: String,
    pub payload_offset: usize,
}

impl DepthQueryHeader {
    pub fn parse(payload: &[u8]) -> Result<Self> {
        if payload.len() < HEADER_LEN {
            return Err(anyhow!(
                "depth query payload too short: {} < {}",
                payload.len(),
                HEADER_LEN
            ));
        }

        let query_type = payload[0];
        let symbol_len = payload[1] as usize;
        let symbol_end = HEADER_LEN + symbol_len;
        if symbol_len == 0 || symbol_end > payload.len() {
            return Err(anyhow!("invalid depth query symbol length: {}", symbol_len));
        }

        let symbol = std::str::from_utf8(&payload[HEADER_LEN..symbol_end])
            .map_err(|err| anyhow!("depth query symbol not utf8: {err}"))?
            .to_string();

        Ok(Self {
            query_type,
            symbol,
            payload_offset: symbol_end,
        })
    }

    pub fn write(
        buf: &mut [u8; DEPTH_QUERY_PAYLOAD],
        query_type: u8,
        symbol: &str,
    ) -> Result<usize> {
        let symbol_len = symbol.len();
        if symbol_len == 0 {
            return Err(anyhow!("depth query symbol is empty"));
        }
        if symbol_len > (DEPTH_QUERY_PAYLOAD - HEADER_LEN) {
            return Err(anyhow!("depth query symbol too long: {}", symbol_len));
        }

        buf[0] = query_type;
        buf[1] = symbol_len as u8;
        buf[HEADER_LEN..HEADER_LEN + symbol_len].copy_from_slice(symbol.as_bytes());

        Ok(HEADER_LEN + symbol_len)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DepthQueryLoadTlenSingleReq {
    pub timestamp_us: i64,
    pub tick_index: i64,
}

impl DepthQueryLoadTlenSingleReq {
    pub const REQ_LEN: usize = 16;

    pub fn from_payload(payload: &[u8]) -> Result<Self> {
        if payload.len() < Self::REQ_LEN {
            return Err(anyhow!(
                "depth query load_tlen single req too short: {} < {}",
                payload.len(),
                Self::REQ_LEN
            ));
        }

        let timestamp_us = i64::from_le_bytes(payload[0..8].try_into()?);
        let tick_index = i64::from_le_bytes(payload[8..16].try_into()?);
        Ok(Self {
            timestamp_us,
            tick_index,
        })
    }

    pub fn write_to(&self, payload: &mut [u8]) -> Result<usize> {
        if payload.len() < Self::REQ_LEN {
            return Err(anyhow!(
                "depth query load_tlen single req write overflow: {} < {}",
                payload.len(),
                Self::REQ_LEN
            ));
        }
        payload[0..8].copy_from_slice(&self.timestamp_us.to_le_bytes());
        payload[8..16].copy_from_slice(&self.tick_index.to_le_bytes());
        Ok(Self::REQ_LEN)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DepthQueryLoadTlenSingleResp {
    pub timestamp_us: i64,
    /// 查询语义见 `TLEN_QUERY_AMOUNT_INVALID` / `TLEN_QUERY_AMOUNT_EMPTY`
    pub amount: f64,
}

impl DepthQueryLoadTlenSingleResp {
    pub const RESP_LEN: usize = 16;

    pub fn from_payload(payload: &[u8]) -> Result<Self> {
        if payload.len() < Self::RESP_LEN {
            return Err(anyhow!(
                "depth query load_tlen single resp too short: {} < {}",
                payload.len(),
                Self::RESP_LEN
            ));
        }

        let timestamp_us = i64::from_le_bytes(payload[0..8].try_into()?);
        let amount = f64::from_le_bytes(payload[8..16].try_into()?);
        Ok(Self {
            timestamp_us,
            amount,
        })
    }

    pub fn write_to(&self, payload: &mut [u8]) -> Result<usize> {
        if payload.len() < Self::RESP_LEN {
            return Err(anyhow!(
                "depth query load_tlen single resp write overflow: {} < {}",
                payload.len(),
                Self::RESP_LEN
            ));
        }
        payload[0..8].copy_from_slice(&self.timestamp_us.to_le_bytes());
        payload[8..16].copy_from_slice(&self.amount.to_le_bytes());
        Ok(Self::RESP_LEN)
    }
}

#[derive(Debug, Clone)]
pub struct DepthQueryLoadTlenBatchReq {
    pub timestamp_us: i64,
    pub tick_indices: Vec<i64>,
}

impl DepthQueryLoadTlenBatchReq {
    pub fn from_payload(payload: &[u8]) -> Result<Self> {
        if payload.len() < BATCH_FIXED_LEN {
            return Err(anyhow!(
                "depth query load_tlen batch req too short: {} < {}",
                payload.len(),
                BATCH_FIXED_LEN
            ));
        }

        let timestamp_us = i64::from_le_bytes(payload[0..8].try_into()?);
        let tick_index_count = u16::from_le_bytes(payload[8..10].try_into()?) as usize;
        if tick_index_count == 0 {
            return Err(anyhow!(
                "depth query load_tlen batch req empty tick_indices"
            ));
        }

        let required = BATCH_FIXED_LEN + tick_index_count * 8;
        if payload.len() < required {
            return Err(anyhow!(
                "depth query load_tlen batch req truncated: {} < {}",
                payload.len(),
                required
            ));
        }

        let mut tick_indices = Vec::with_capacity(tick_index_count);
        let mut offset = BATCH_FIXED_LEN;
        for _ in 0..tick_index_count {
            tick_indices.push(i64::from_le_bytes(payload[offset..offset + 8].try_into()?));
            offset += 8;
        }

        Ok(Self {
            timestamp_us,
            tick_indices,
        })
    }

    pub fn write_to(payload: &mut [u8], timestamp_us: i64, tick_indices: &[i64]) -> Result<usize> {
        if tick_indices.is_empty() {
            return Err(anyhow!(
                "depth query load_tlen batch req empty tick_indices"
            ));
        }
        if tick_indices.len() > u16::MAX as usize {
            return Err(anyhow!(
                "depth query load_tlen batch req too many tick_indices"
            ));
        }

        let required = BATCH_FIXED_LEN + tick_indices.len() * 8;
        if payload.len() < required {
            return Err(anyhow!(
                "depth query load_tlen batch req write overflow: {} < {}",
                payload.len(),
                required
            ));
        }

        payload[0..8].copy_from_slice(&timestamp_us.to_le_bytes());
        payload[8..10].copy_from_slice(&(tick_indices.len() as u16).to_le_bytes());
        payload[10..12].fill(0);

        let mut offset = BATCH_FIXED_LEN;
        for tick_index in tick_indices {
            payload[offset..offset + 8].copy_from_slice(&tick_index.to_le_bytes());
            offset += 8;
        }
        Ok(required)
    }
}

#[derive(Debug, Clone)]
pub struct DepthQueryLoadTlenBatchResp {
    pub timestamp_us: i64,
    /// 与请求 tick_indices 一一对应；每个元素语义同 `DepthQueryLoadTlenSingleResp::amount`
    pub amounts: Vec<f64>,
}

impl DepthQueryLoadTlenBatchResp {
    pub fn from_payload(payload: &[u8]) -> Result<Self> {
        if payload.len() < BATCH_FIXED_LEN {
            return Err(anyhow!(
                "depth query load_tlen batch resp too short: {} < {}",
                payload.len(),
                BATCH_FIXED_LEN
            ));
        }

        let timestamp_us = i64::from_le_bytes(payload[0..8].try_into()?);
        let count = u16::from_le_bytes(payload[8..10].try_into()?) as usize;
        if count == 0 {
            return Err(anyhow!("depth query load_tlen batch resp empty amounts"));
        }

        let required = BATCH_FIXED_LEN + count * 8;
        if payload.len() < required {
            return Err(anyhow!(
                "depth query load_tlen batch resp truncated: {} < {}",
                payload.len(),
                required
            ));
        }

        let mut amounts = Vec::with_capacity(count);
        let mut offset = BATCH_FIXED_LEN;
        for _ in 0..count {
            amounts.push(f64::from_le_bytes(payload[offset..offset + 8].try_into()?));
            offset += 8;
        }

        Ok(Self {
            timestamp_us,
            amounts,
        })
    }

    pub fn write_to(payload: &mut [u8], timestamp_us: i64, amounts: &[f64]) -> Result<usize> {
        if amounts.is_empty() {
            return Err(anyhow!("depth query load_tlen batch resp empty amounts"));
        }
        if amounts.len() > u16::MAX as usize {
            return Err(anyhow!("depth query load_tlen batch resp too many amounts"));
        }

        let required = BATCH_FIXED_LEN + amounts.len() * 8;
        if payload.len() < required {
            return Err(anyhow!(
                "depth query load_tlen batch resp write overflow: {} < {}",
                payload.len(),
                required
            ));
        }

        payload[0..8].copy_from_slice(&timestamp_us.to_le_bytes());
        payload[8..10].copy_from_slice(&(amounts.len() as u16).to_le_bytes());
        payload[10..12].fill(0);

        let mut offset = BATCH_FIXED_LEN;
        for amount in amounts {
            payload[offset..offset + 8].copy_from_slice(&amount.to_le_bytes());
            offset += 8;
        }
        Ok(required)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DepthQueryTop5PriceTlenReq {
    pub timestamp_us: i64,
}

impl DepthQueryTop5PriceTlenReq {
    pub const REQ_LEN: usize = 8;

    pub fn from_payload(payload: &[u8]) -> Result<Self> {
        if payload.len() < Self::REQ_LEN {
            return Err(anyhow!(
                "depth query top5 price+tlen req too short: {} < {}",
                payload.len(),
                Self::REQ_LEN
            ));
        }
        Ok(Self {
            timestamp_us: i64::from_le_bytes(payload[0..8].try_into()?),
        })
    }

    pub fn write_to(&self, payload: &mut [u8]) -> Result<usize> {
        if payload.len() < Self::REQ_LEN {
            return Err(anyhow!(
                "depth query top5 price+tlen req write overflow: {} < {}",
                payload.len(),
                Self::REQ_LEN
            ));
        }
        payload[0..8].copy_from_slice(&self.timestamp_us.to_le_bytes());
        Ok(Self::REQ_LEN)
    }
}

#[derive(Debug, Clone)]
pub struct DepthQueryTop5PriceTlenResp {
    pub timestamp_us: i64,
    pub bids: Vec<(i64, f64)>,
    pub asks: Vec<(i64, f64)>,
}

impl DepthQueryTop5PriceTlenResp {
    pub fn from_payload(payload: &[u8]) -> Result<Self> {
        if payload.len() < TOP5_FIXED_LEN {
            return Err(anyhow!(
                "depth query top5 price+tlen resp too short: {} < {}",
                payload.len(),
                TOP5_FIXED_LEN
            ));
        }

        let timestamp_us = i64::from_le_bytes(payload[0..8].try_into()?);
        let bid_count = payload[8] as usize;
        let ask_count = payload[9] as usize;
        if bid_count > TOP5_MAX_LEVELS || ask_count > TOP5_MAX_LEVELS {
            return Err(anyhow!(
                "depth query top5 price+tlen resp invalid level count: bid={} ask={}",
                bid_count,
                ask_count
            ));
        }

        let required = TOP5_FIXED_LEN + (bid_count + ask_count) * TOP5_LEVEL_BYTES;
        if payload.len() < required {
            return Err(anyhow!(
                "depth query top5 price+tlen resp truncated: {} < {}",
                payload.len(),
                required
            ));
        }

        let mut offset = TOP5_FIXED_LEN;
        let mut bids = Vec::with_capacity(bid_count);
        for _ in 0..bid_count {
            let tick_index = i64::from_le_bytes(payload[offset..offset + 8].try_into()?);
            let tlen = f64::from_le_bytes(payload[offset + 8..offset + 16].try_into()?);
            bids.push((tick_index, tlen));
            offset += TOP5_LEVEL_BYTES;
        }

        let mut asks = Vec::with_capacity(ask_count);
        for _ in 0..ask_count {
            let tick_index = i64::from_le_bytes(payload[offset..offset + 8].try_into()?);
            let tlen = f64::from_le_bytes(payload[offset + 8..offset + 16].try_into()?);
            asks.push((tick_index, tlen));
            offset += TOP5_LEVEL_BYTES;
        }

        Ok(Self {
            timestamp_us,
            bids,
            asks,
        })
    }

    pub fn write_to(
        payload: &mut [u8],
        timestamp_us: i64,
        bids: &[(i64, f64)],
        asks: &[(i64, f64)],
    ) -> Result<usize> {
        if bids.len() > TOP5_MAX_LEVELS || asks.len() > TOP5_MAX_LEVELS {
            return Err(anyhow!(
                "depth query top5 price+tlen resp level overflow: bid={} ask={}",
                bids.len(),
                asks.len()
            ));
        }

        let required = TOP5_FIXED_LEN + (bids.len() + asks.len()) * TOP5_LEVEL_BYTES;
        if payload.len() < required {
            return Err(anyhow!(
                "depth query top5 price+tlen resp write overflow: {} < {}",
                payload.len(),
                required
            ));
        }

        payload[0..8].copy_from_slice(&timestamp_us.to_le_bytes());
        payload[8] = bids.len() as u8;
        payload[9] = asks.len() as u8;
        payload[10..12].fill(0);

        let mut offset = TOP5_FIXED_LEN;
        for (tick_index, tlen) in bids {
            payload[offset..offset + 8].copy_from_slice(&tick_index.to_le_bytes());
            payload[offset + 8..offset + 16].copy_from_slice(&tlen.to_le_bytes());
            offset += TOP5_LEVEL_BYTES;
        }
        for (tick_index, tlen) in asks {
            payload[offset..offset + 8].copy_from_slice(&tick_index.to_le_bytes());
            payload[offset + 8..offset + 16].copy_from_slice(&tlen.to_le_bytes());
            offset += TOP5_LEVEL_BYTES;
        }
        Ok(required)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let mut buf = [0u8; DEPTH_QUERY_PAYLOAD];
        let offset =
            DepthQueryHeader::write(&mut buf, DepthQueryType::LoadTlenSingle as u8, "BTCUSDT")
                .unwrap();
        assert_eq!(offset, 9);

        let parsed = DepthQueryHeader::parse(&buf).unwrap();
        assert_eq!(parsed.query_type, DepthQueryType::LoadTlenSingle as u8);
        assert_eq!(parsed.symbol, "BTCUSDT");
        assert_eq!(parsed.payload_offset, 9);
    }

    #[test]
    fn single_req_resp_roundtrip() {
        let mut req_buf = [0u8; 16];
        let req = DepthQueryLoadTlenSingleReq {
            timestamp_us: 123,
            tick_index: 4565,
        };
        req.write_to(&mut req_buf).unwrap();
        let parsed_req = DepthQueryLoadTlenSingleReq::from_payload(&req_buf).unwrap();
        assert_eq!(parsed_req.timestamp_us, 123);
        assert_eq!(parsed_req.tick_index, 4565);

        let mut resp_buf = [0u8; 16];
        let resp = DepthQueryLoadTlenSingleResp {
            timestamp_us: 123,
            amount: 3.5,
        };
        resp.write_to(&mut resp_buf).unwrap();
        let parsed_resp = DepthQueryLoadTlenSingleResp::from_payload(&resp_buf).unwrap();
        assert_eq!(parsed_resp.timestamp_us, 123);
        assert_eq!(parsed_resp.amount, 3.5);
    }

    #[test]
    fn batch_req_resp_roundtrip() {
        let mut req_buf = [0u8; 64];
        let req_len =
            DepthQueryLoadTlenBatchReq::write_to(&mut req_buf, 555, &[1010, 1020, 1030]).unwrap();
        let parsed_req = DepthQueryLoadTlenBatchReq::from_payload(&req_buf[..req_len]).unwrap();
        assert_eq!(parsed_req.timestamp_us, 555);
        assert_eq!(parsed_req.tick_indices, vec![1010, 1020, 1030]);

        let mut resp_buf = [0u8; 64];
        let resp_len =
            DepthQueryLoadTlenBatchResp::write_to(&mut resp_buf, 555, &[1.0, 2.0, 3.0]).unwrap();
        let parsed_resp = DepthQueryLoadTlenBatchResp::from_payload(&resp_buf[..resp_len]).unwrap();
        assert_eq!(parsed_resp.timestamp_us, 555);
        assert_eq!(parsed_resp.amounts, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn batch_req_rejects_truncated_payload() {
        let mut payload = [0u8; BATCH_FIXED_LEN + 8];
        payload[0..8].copy_from_slice(&777_i64.to_le_bytes());
        payload[8..10].copy_from_slice(&(2_u16).to_le_bytes());
        payload[10..12].fill(0);
        payload[12..20].copy_from_slice(&101.0_f64.to_le_bytes());

        let err = DepthQueryLoadTlenBatchReq::from_payload(&payload).unwrap_err();
        assert!(
            err.to_string().contains("truncated"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn top5_req_resp_roundtrip() {
        let mut req_buf = [0u8; 16];
        let req = DepthQueryTop5PriceTlenReq { timestamp_us: 999 };
        req.write_to(&mut req_buf).unwrap();
        let parsed_req = DepthQueryTop5PriceTlenReq::from_payload(&req_buf).unwrap();
        assert_eq!(parsed_req.timestamp_us, 999);

        let mut resp_buf = [0u8; 256];
        let resp_len = DepthQueryTop5PriceTlenResp::write_to(
            &mut resp_buf,
            999,
            &[(1010, 1.2), (1005, 2.3)],
            &[(1015, 1.1)],
        )
        .unwrap();
        let parsed_resp = DepthQueryTop5PriceTlenResp::from_payload(&resp_buf[..resp_len]).unwrap();
        assert_eq!(parsed_resp.timestamp_us, 999);
        assert_eq!(parsed_resp.bids, vec![(1010, 1.2), (1005, 2.3)]);
        assert_eq!(parsed_resp.asks, vec![(1015, 1.1)]);
    }

}
