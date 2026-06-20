use bytes::{Buf, BufMut, Bytes, BytesMut};

use order_common::TradingVenue;

fn to_fraction(value: f64) -> Option<(i64, i64)> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    let mut denom: i64 = 1;
    let mut scaled = value;
    for _ in 0..9 {
        let rounded = scaled.round();
        if (scaled - rounded).abs() < 1e-9 {
            return Some((rounded as i64, denom));
        }
        scaled *= 10.0;
        denom = denom.saturating_mul(10);
    }
    None
}

pub fn align_price_floor(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return price;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((price * tick_den as f64) + 1e-9).floor() as i128;
        let aligned_units = (units / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((price / tick) + 1e-9).floor();
    scaled * tick
}

pub fn align_price_ceil(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return price;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((price * tick_den as f64) - 1e-9).ceil() as i128;
        let aligned_units = ((units + tick_num - 1) / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((price / tick) - 1e-9).ceil();
    scaled * tick
}

/// 交易腿信息（不包含序列化）
#[derive(Debug, Clone, Copy)]
pub struct TradingLeg {
    pub venue: u8, // TradingVenue as u8
    pub bid0: f64,
    pub ask0: f64,
    pub ts: i64, // last quote event time (µs)
}

impl TradingLeg {
    pub fn new(venue: TradingVenue, bid0: f64, ask0: f64, ts: i64) -> Self {
        Self {
            venue: venue.to_u8(),
            bid0,
            ask0,
            ts,
        }
    }

    pub fn get_venue(&self) -> Option<TradingVenue> {
        TradingVenue::from_u8(self.venue)
    }
}

/// Trait for signal serialization/deserialization
pub trait SignalBytes: Sized {
    /// Serialize the signal to bytes
    fn to_bytes(&self) -> Bytes;

    /// Serialize the signal into an existing buffer.
    ///
    /// Hot paths should override this to avoid allocating a temporary `Bytes`.
    fn write_to(&self, buf: &mut BytesMut) {
        let bytes = self.to_bytes();
        buf.put_slice(bytes.as_ref());
    }

    /// Deserialize the signal from bytes
    fn from_bytes(bytes: Bytes) -> Result<Self, String>;
}

pub struct SignalSliceReader<'a> {
    raw: &'a [u8],
    offset: usize,
}

impl<'a> SignalSliceReader<'a> {
    pub fn new(raw: &'a [u8]) -> Self {
        Self { raw, offset: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.raw.len().saturating_sub(self.offset)
    }

    fn ensure(&self, len: usize, label: &str) -> Result<(), String> {
        if self.remaining() < len {
            return Err(format!(
                "Not enough bytes for {label}: need {len}, have {}",
                self.remaining()
            ));
        }
        Ok(())
    }

    pub fn read_u8(&mut self, label: &str) -> Result<u8, String> {
        self.ensure(1, label)?;
        let value = self.raw[self.offset];
        self.offset += 1;
        Ok(value)
    }

    pub fn read_i32_le(&mut self, label: &str) -> Result<i32, String> {
        self.ensure(4, label)?;
        let value = i32::from_le_bytes(
            self.raw[self.offset..self.offset + 4]
                .try_into()
                .map_err(|_| format!("Invalid bytes for {label}"))?,
        );
        self.offset += 4;
        Ok(value)
    }

    pub fn read_u32_le(&mut self, label: &str) -> Result<u32, String> {
        self.ensure(4, label)?;
        let value = u32::from_le_bytes(
            self.raw[self.offset..self.offset + 4]
                .try_into()
                .map_err(|_| format!("Invalid bytes for {label}"))?,
        );
        self.offset += 4;
        Ok(value)
    }

    pub fn read_i64_le(&mut self, label: &str) -> Result<i64, String> {
        self.ensure(8, label)?;
        let value = i64::from_le_bytes(
            self.raw[self.offset..self.offset + 8]
                .try_into()
                .map_err(|_| format!("Invalid bytes for {label}"))?,
        );
        self.offset += 8;
        Ok(value)
    }

    pub fn read_u64_le(&mut self, label: &str) -> Result<u64, String> {
        self.ensure(8, label)?;
        let value = u64::from_le_bytes(
            self.raw[self.offset..self.offset + 8]
                .try_into()
                .map_err(|_| format!("Invalid bytes for {label}"))?,
        );
        self.offset += 8;
        Ok(value)
    }

    pub fn read_f64_le(&mut self, label: &str) -> Result<f64, String> {
        self.ensure(8, label)?;
        let value = f64::from_le_bytes(
            self.raw[self.offset..self.offset + 8]
                .try_into()
                .map_err(|_| format!("Invalid bytes for {label}"))?,
        );
        self.offset += 8;
        Ok(value)
    }

    pub fn read_fixed_bytes(&mut self, label: &str) -> Result<[u8; 32], String> {
        let len = self.read_u8(label)? as usize;
        if len > 32 {
            return Err(format!("Invalid array length: {len}"));
        }
        self.ensure(len, "array data")?;

        let mut arr = [0u8; 32];
        arr[..len].copy_from_slice(&self.raw[self.offset..self.offset + len]);
        self.offset += len;
        Ok(arr)
    }

    pub fn read_bytes(&mut self, len: usize, label: &str) -> Result<&'a [u8], String> {
        self.ensure(len, label)?;
        let bytes = &self.raw[self.offset..self.offset + len];
        self.offset += len;
        Ok(bytes)
    }

    pub fn read_trading_leg(
        &mut self,
        with_ts: bool,
        label: &str,
    ) -> Result<(TradingLeg, [u8; 32]), String> {
        let venue = self.read_u8(label)?;
        let bid0 = self.read_f64_le(label)?;
        let ask0 = self.read_f64_le(label)?;
        let ts = if with_ts { self.read_i64_le(label)? } else { 0 };
        let symbol = self.read_fixed_bytes(label)?;
        Ok((
            TradingLeg {
                venue,
                bid0,
                ask0,
                ts,
            },
            symbol,
        ))
    }

    pub fn finish_exact(&self, label: &str) -> Result<(), String> {
        if self.remaining() != 0 {
            return Err(format!("Unexpected trailing bytes for {label}"));
        }
        Ok(())
    }
}

/// Helper functions for byte serialization
pub mod bytes_helper {
    use super::*;

    pub fn fixed_bytes_len(bytes: &[u8; 32]) -> usize {
        bytes.iter().position(|&b| b == 0).unwrap_or(32)
    }

    pub fn fixed_bytes_from_str(value: &str) -> [u8; 32] {
        let mut out = [0u8; 32];
        let bytes = value.as_bytes();
        let len = bytes.len().min(32);
        out[..len].copy_from_slice(&bytes[..len]);
        out
    }

    pub fn fixed_bytes_eq_ignore_ascii_case(bytes: &[u8; 32], value: &str) -> bool {
        let len = fixed_bytes_len(bytes);
        let value = value.as_bytes();
        len == value.len() && bytes[..len].eq_ignore_ascii_case(value)
    }

    /// Write a fixed-size byte array (for symbol storage)
    pub fn write_fixed_bytes(buf: &mut BytesMut, bytes: &[u8; 32]) {
        // Find the actual length (until first zero or full length)
        let len = fixed_bytes_len(bytes);
        buf.put_u8(len as u8);
        buf.put_slice(&bytes[..len]);
    }

    /// Read a fixed-size byte array
    pub fn read_fixed_bytes(bytes: &mut Bytes) -> Result<[u8; 32], String> {
        if bytes.remaining() < 1 {
            return Err("Not enough bytes for array length".to_string());
        }
        let len = bytes.get_u8() as usize;
        if len > 32 {
            return Err(format!("Invalid array length: {}", len));
        }
        if bytes.remaining() < len {
            return Err(format!(
                "Not enough bytes for array data: need {}, have {}",
                len,
                bytes.remaining()
            ));
        }

        let mut arr = [0u8; 32];
        bytes.copy_to_slice(&mut arr[..len]);
        Ok(arr)
    }

    /// Write an optional f64
    pub fn write_option_f64(buf: &mut BytesMut, value: f64) {
        // Use 0.0 to represent None (since we agreed to use 0 for no value)
        buf.put_f64_le(value);
    }

    /// Read an optional f64
    pub fn read_option_f64(bytes: &mut Bytes) -> Result<f64, String> {
        if bytes.remaining() < 8 {
            return Err("Not enough bytes for f64".to_string());
        }
        Ok(bytes.get_f64_le())
    }
}

#[cfg(test)]
mod tests {
    use order_common::TradingVenue;

    #[test]
    fn trading_venue_supports_pre_trade_stack_for_binance_okx_bybit_bitget_gate_only() {
        for venue in [
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
            TradingVenue::OkexMargin,
            TradingVenue::OkexFutures,
            TradingVenue::BybitMargin,
            TradingVenue::BybitFutures,
            TradingVenue::BitgetMargin,
            TradingVenue::BitgetFutures,
            TradingVenue::GateMargin,
            TradingVenue::GateFutures,
        ] {
            assert!(
                venue.supports_pre_trade_stack(),
                "{venue:?} should be supported"
            );
        }

        for venue in [
            TradingVenue::HyperliquidMargin,
            TradingVenue::HyperliquidFutures,
        ] {
            assert!(
                !venue.supports_pre_trade_stack(),
                "{venue:?} should remain unsupported"
            );
        }
    }
}
