use super::mkt_msg::MktMsgType;
use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone, PartialEq)]
pub struct OpenInterestMsg {
    pub symbol: String,
    /// Base-asset units, not USD notional or contract count.
    pub open_interest: f64,
    pub timestamp: i64,
}

impl OpenInterestMsg {
    pub fn create(symbol: String, open_interest: f64, timestamp: i64) -> Self {
        Self {
            symbol,
            open_interest,
            timestamp,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut output = BytesMut::with_capacity(24 + self.symbol.len());
        output.put_u32_le(MktMsgType::OpenInterest as u32);
        output.put_u32_le(self.symbol.len() as u32);
        output.put_slice(self.symbol.as_bytes());
        output.put_f64_le(self.open_interest);
        output.put_i64_le(self.timestamp);
        output.freeze()
    }

    pub fn from_bytes(mut data: &[u8]) -> Result<Self> {
        if data.len() < 24 || data.get_u32_le() != MktMsgType::OpenInterest as u32 {
            bail!("invalid open-interest message");
        }
        let len = data.get_u32_le() as usize;
        if len == 0 || data.len() < 16 || len > data.len() - 16 {
            bail!("truncated open-interest symbol");
        }
        let symbol = std::str::from_utf8(&data[..len])?.to_string();
        data.advance(len);
        let open_interest = data.get_f64_le();
        let timestamp = data.get_i64_le();
        if !open_interest.is_finite() || open_interest < 0.0 || timestamp < 0 {
            bail!("invalid open-interest fields");
        }
        Ok(Self {
            symbol,
            open_interest,
            timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hyperliquid_open_interest_roundtrip_and_truncation() {
        let msg = OpenInterestMsg::create("BTCUSDC".into(), 123.456, 1_700_000_000_000_000);
        let bytes = msg.to_bytes();
        assert_eq!(OpenInterestMsg::from_bytes(&bytes).unwrap(), msg);
        for end in 0..bytes.len() {
            assert!(OpenInterestMsg::from_bytes(&bytes[..end]).is_err());
        }
    }
}
