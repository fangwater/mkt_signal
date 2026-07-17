use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rolling_common::kll_quantile::FrozenKllSketch;

use super::mkt_msg::MktMsgType;

pub const TRADE_NOTIONAL_KLL_MSG_TYPE: u32 = MktMsgType::TradeNotionalKll as u32;
pub const TRADE_NOTIONAL_KLL_MAX_BYTES: usize = 131_072;

#[derive(Debug, Clone, PartialEq)]
pub struct TradeNotionalKllMsg {
    pub symbol: String,
    pub venue: u8,
    pub hour_start_ms: i64,
    pub sketch: FrozenKllSketch,
}

impl TradeNotionalKllMsg {
    pub fn to_bytes(&self) -> Result<Bytes> {
        if self.symbol.is_empty() {
            bail!("trade notional KLL symbol must not be empty");
        }
        let level_count = u32::try_from(self.sketch.levels.len())
            .map_err(|_| anyhow::anyhow!("too many KLL levels"))?;
        let mut total_bytes = 4 + 4 + self.symbol.len() + 1 + 8 + 4 + 8 + 4;
        for values in &self.sketch.levels {
            total_bytes = total_bytes
                .checked_add(4 + 4 + values.len().saturating_mul(8))
                .ok_or_else(|| anyhow::anyhow!("trade notional KLL payload size overflow"))?;
        }
        if total_bytes > TRADE_NOTIONAL_KLL_MAX_BYTES {
            bail!(
                "trade notional KLL payload {} exceeds max {}",
                total_bytes,
                TRADE_NOTIONAL_KLL_MAX_BYTES
            );
        }

        let mut buf = BytesMut::with_capacity(total_bytes);
        buf.put_u32_le(TRADE_NOTIONAL_KLL_MSG_TYPE);
        buf.put_u32_le(self.symbol.len() as u32);
        buf.put(self.symbol.as_bytes());
        buf.put_u8(self.venue);
        buf.put_i64_le(self.hour_start_ms);
        buf.put_u32_le(self.sketch.level_capacity as u32);
        buf.put_u64_le(self.sketch.sample_count as u64);
        buf.put_u32_le(level_count);
        for (level, values) in self.sketch.levels.iter().enumerate() {
            buf.put_u32_le(level as u32);
            buf.put_u32_le(values.len() as u32);
            for value in values {
                buf.put_f64_le(*value);
            }
        }
        Ok(buf.freeze())
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut cursor = Bytes::copy_from_slice(data);
        if cursor.remaining() < 4 + 4 {
            bail!("trade notional KLL payload too short");
        }
        let msg_type = cursor.get_u32_le();
        if msg_type != TRADE_NOTIONAL_KLL_MSG_TYPE {
            bail!("invalid trade notional KLL message type {}", msg_type);
        }
        let symbol_len = cursor.get_u32_le() as usize;
        if symbol_len == 0 || cursor.remaining() < symbol_len + 1 + 8 + 4 + 8 + 4 {
            bail!("trade notional KLL payload truncated");
        }
        let symbol = String::from_utf8(cursor.copy_to_bytes(symbol_len).to_vec())?;
        let venue = cursor.get_u8();
        let hour_start_ms = cursor.get_i64_le();
        let level_capacity = cursor.get_u32_le() as usize;
        let sample_count = cursor.get_u64_le() as usize;
        let level_count = cursor.get_u32_le() as usize;
        let mut levels = Vec::with_capacity(level_count);
        for expected_level in 0..level_count {
            if cursor.remaining() < 8 {
                bail!("trade notional KLL level header truncated");
            }
            let level = cursor.get_u32_le() as usize;
            if level != expected_level {
                bail!("trade notional KLL levels must be contiguous from zero");
            }
            let value_count = cursor.get_u32_le() as usize;
            let value_bytes = value_count
                .checked_mul(8)
                .ok_or_else(|| anyhow::anyhow!("trade notional KLL value size overflow"))?;
            if cursor.remaining() < value_bytes {
                bail!("trade notional KLL values truncated");
            }
            let mut values = Vec::with_capacity(value_count);
            for _ in 0..value_count {
                values.push(cursor.get_f64_le());
            }
            levels.push(values);
        }
        Ok(Self {
            symbol,
            venue,
            hour_start_ms,
            sketch: FrozenKllSketch {
                level_capacity,
                sample_count,
                levels,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::TradeNotionalKllMsg;
    use rolling_common::kll_quantile::StreamingKllSketch;

    #[test]
    fn roundtrip_preserves_hourly_kll_levels() {
        let mut sketch = StreamingKllSketch::new();
        for value in 1..2_000 {
            sketch.insert(value as f64);
        }
        let original = TradeNotionalKllMsg {
            symbol: "BTCUSDT".to_string(),
            venue: 1,
            hour_start_ms: 1_700_000_000_000,
            sketch: sketch.freeze(),
        };
        let decoded = TradeNotionalKllMsg::from_bytes(&original.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, original);
    }
}
