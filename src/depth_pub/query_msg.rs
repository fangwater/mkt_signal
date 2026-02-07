use anyhow::{anyhow, Result};

pub const DEPTH_QUERY_PAYLOAD: usize = 256;
const HEADER_LEN: usize = 2;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthQueryType {
    LoadTlen = 1,
    TopOfBook = 3,
    Stats = 4,
}

impl DepthQueryType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::LoadTlen),
            3 => Some(Self::TopOfBook),
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
pub struct DepthQueryLoadTlenCtx {
    pub timestamp_us: i64,
    pub price: f64,
}

impl DepthQueryLoadTlenCtx {
    pub const REQ_LEN: usize = 16;
    pub const RESP_LEN: usize = 16; // timestamp_us + amount

    pub fn from_payload(payload: &[u8]) -> Result<Self> {
        if payload.len() < Self::REQ_LEN {
            return Err(anyhow!(
                "depth query load_tlen ctx too short: {} < {}",
                payload.len(),
                Self::REQ_LEN
            ));
        }

        let timestamp_us = i64::from_le_bytes(payload[0..8].try_into()?);
        let price = f64::from_le_bytes(payload[8..16].try_into()?);
        Ok(Self {
            timestamp_us,
            price,
        })
    }

    pub fn write_response(&self, payload: &mut [u8], amount: f64) -> Result<usize> {
        if payload.len() < Self::RESP_LEN {
            return Err(anyhow!(
                "depth query load_tlen resp too short: {} < {}",
                payload.len(),
                Self::RESP_LEN
            ));
        }
        payload[0..8].copy_from_slice(&self.timestamp_us.to_le_bytes());
        payload[8..16].copy_from_slice(&amount.to_le_bytes());
        Ok(Self::RESP_LEN)
    }
}
