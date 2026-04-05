use crate::pre_trade::order_manager::Side;
use crate::signal::common::{bytes_helper, SignalBytes, TradingLeg};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ArbCancelReason {
    Spread = 1,
    Tlen = 2,
}

impl ArbCancelReason {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Spread),
            2 => Some(Self::Tlen),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Generic arbitrage cancel signal context
#[derive(Debug, Clone)]
pub struct ArbCancelCtx {
    /// Opening leg
    pub opening_leg: TradingLeg,

    /// Opening leg symbol
    pub opening_symbol: [u8; 32],

    /// Hedging leg
    pub hedging_leg: TradingLeg,

    /// Hedging leg symbol
    pub hedging_symbol: [u8; 32],

    /// Trigger timestamp
    pub trigger_ts: i64,

    /// From key length
    pub from_key_len: u32,

    /// From key bytes
    pub from_key: Vec<u8>,

    /// Cancel reason.
    pub reason: u8,

    /// Cancel direction for spread arb.
    pub side: u8,

    /// Optional precise strategy target; 0 means broadcast-by-symbol routing.
    pub strategy_id: i32,
}

/// Market maker cancel signal context
#[derive(Debug, Clone)]
pub struct MmCancelCtx {
    /// Single leg (MM only has one leg)
    pub opening_leg: TradingLeg,

    /// Leg symbol
    pub opening_symbol: [u8; 32],

    /// Cancel side
    pub side: u8,

    /// Trigger timestamp
    pub trigger_ts: i64,

    /// From key length
    pub from_key_len: u32,

    /// From key bytes
    pub from_key: Vec<u8>,

    /// Optional precise strategy target; 0 means broadcast-by-side legacy behavior.
    pub strategy_id: i32,

    /// Optional precise client order target; 0 means unspecified.
    pub client_order_id: i64,
}

fn set_symbol(target: &mut [u8; 32], symbol: &str) {
    let bytes = symbol.as_bytes();
    let len = bytes.len().min(32);
    target[..len].copy_from_slice(&bytes[..len]);
}

fn get_symbol(source: &[u8; 32]) -> String {
    let end = source.iter().position(|&b| b == 0).unwrap_or(32);
    String::from_utf8_lossy(&source[..end]).to_string()
}

fn write_leg(buf: &mut BytesMut, leg: &TradingLeg, symbol: &[u8; 32]) {
    buf.put_u8(leg.venue);
    buf.put_f64_le(leg.bid0);
    buf.put_f64_le(leg.ask0);
    buf.put_i64_le(leg.ts);
    bytes_helper::write_fixed_bytes(buf, symbol);
}

fn read_leg(
    bytes: &mut Bytes,
    with_ts: bool,
    label: &str,
) -> Result<(TradingLeg, [u8; 32]), String> {
    let need = if with_ts { 1 + 8 + 8 + 8 } else { 1 + 8 + 8 };
    if bytes.remaining() < need {
        return Err(format!("Not enough bytes for {}", label));
    }
    let venue = bytes.get_u8();
    let bid0 = bytes.get_f64_le();
    let ask0 = bytes.get_f64_le();
    let ts = if with_ts {
        if bytes.remaining() < 8 {
            return Err(format!("Not enough bytes for {} ts", label));
        }
        bytes.get_i64_le()
    } else {
        0
    };
    let symbol = bytes_helper::read_fixed_bytes(bytes)?;
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

impl ArbCancelCtx {
    /// Create new arbitrage cancel context
    pub fn new() -> Self {
        Self {
            opening_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
                ts: 0,
            },
            opening_symbol: [0u8; 32],
            hedging_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
                ts: 0,
            },
            hedging_symbol: [0u8; 32],
            trigger_ts: 0,
            from_key_len: 0,
            from_key: Vec::new(),
            reason: ArbCancelReason::Spread.to_u8(),
            side: Side::Buy.to_u8(),
            strategy_id: 0,
        }
    }

    /// Set opening leg symbol
    pub fn set_opening_symbol(&mut self, symbol: &str) {
        set_symbol(&mut self.opening_symbol, symbol);
    }

    /// Get opening leg symbol
    pub fn get_opening_symbol(&self) -> String {
        get_symbol(&self.opening_symbol)
    }

    /// Set hedging leg symbol
    pub fn set_hedging_symbol(&mut self, symbol: &str) {
        set_symbol(&mut self.hedging_symbol, symbol);
    }

    /// Get hedging leg symbol
    pub fn get_hedging_symbol(&self) -> String {
        get_symbol(&self.hedging_symbol)
    }

    /// Set from key bytes (updates length)
    pub fn set_from_key(&mut self, from_key: Vec<u8>) {
        self.from_key_len = from_key.len() as u32;
        self.from_key = from_key;
    }

    pub fn set_reason(&mut self, reason: ArbCancelReason) {
        self.reason = reason.to_u8();
    }

    pub fn get_reason(&self) -> ArbCancelReason {
        ArbCancelReason::from_u8(self.reason).unwrap_or(ArbCancelReason::Spread)
    }

    pub fn set_side(&mut self, side: Side) {
        self.side = side.to_u8();
    }

    pub fn get_side(&self) -> Side {
        Side::from_u8(self.side).expect("ArbCancelCtx side must be valid")
    }

    pub fn set_target_strategy(&mut self, strategy_id: i32) {
        self.strategy_id = strategy_id.max(0);
    }
}

impl MmCancelCtx {
    /// Create new market maker cancel context
    pub fn new() -> Self {
        Self {
            opening_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
                ts: 0,
            },
            opening_symbol: [0u8; 32],
            side: 0,
            trigger_ts: 0,
            from_key_len: 0,
            from_key: Vec::new(),
            strategy_id: 0,
            client_order_id: 0,
        }
    }

    /// Set opening leg symbol
    pub fn set_opening_symbol(&mut self, symbol: &str) {
        set_symbol(&mut self.opening_symbol, symbol);
    }

    /// Get opening leg symbol
    pub fn get_opening_symbol(&self) -> String {
        get_symbol(&self.opening_symbol)
    }

    /// Set cancel side
    pub fn set_side(&mut self, side: Side) {
        self.side = side.to_u8();
    }

    /// Get cancel side
    pub fn get_side(&self) -> Side {
        Side::from_u8(self.side).expect("MmCancelCtx side must be valid")
    }

    /// Set from key bytes (updates length)
    pub fn set_from_key(&mut self, from_key: Vec<u8>) {
        self.from_key_len = from_key.len() as u32;
        self.from_key = from_key;
    }

    pub fn set_target_strategy(&mut self, strategy_id: i32, client_order_id: i64) {
        self.strategy_id = strategy_id.max(0);
        self.client_order_id = client_order_id.max(0);
    }
}

impl SignalBytes for ArbCancelCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Opening leg
        write_leg(&mut buf, &self.opening_leg, &self.opening_symbol);

        // Hedging leg
        write_leg(&mut buf, &self.hedging_leg, &self.hedging_symbol);

        // Trigger timestamp
        buf.put_i64_le(self.trigger_ts);

        let from_key_len = self.from_key.len() as u32;
        buf.put_u32_le(from_key_len);
        buf.put_slice(&self.from_key);
        buf.put_u8(self.reason);
        buf.put_u8(self.side);
        buf.put_i32_le(self.strategy_id);

        buf.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, String> {
        let mut bytes = bytes;

        // Opening leg
        let (opening_leg, opening_symbol) = read_leg(&mut bytes, true, "opening leg")?;

        // Hedging leg
        let (hedging_leg, hedging_symbol) = read_leg(&mut bytes, true, "hedging leg")?;

        if bytes.remaining() < 8 + 4 {
            return Err(
                "Not enough bytes for ArbCancelCtx trigger timestamp / from_key_len".to_string(),
            );
        }
        let trigger_ts = bytes.get_i64_le();
        let from_key_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < from_key_len {
            return Err(format!(
                "Not enough bytes for from_key: need {}, have {}",
                from_key_len,
                bytes.remaining()
            ));
        }
        let from_key = bytes.copy_to_bytes(from_key_len).to_vec();
        if bytes.remaining() != 1 + 1 + 4 {
            return Err("Unexpected trailing bytes for ArbCancelCtx".to_string());
        }
        let reason = bytes.get_u8();
        let side = bytes.get_u8();
        if Side::from_u8(side).is_none() {
            return Err(format!("Invalid ArbCancelCtx side: {}", side));
        }
        let strategy_id = bytes.get_i32_le();
        if bytes.remaining() != 0 {
            return Err("Unexpected trailing bytes for ArbCancelCtx".to_string());
        }

        Ok(ArbCancelCtx {
            opening_leg,
            opening_symbol,
            hedging_leg,
            hedging_symbol,
            trigger_ts,
            from_key_len: from_key_len as u32,
            from_key,
            reason,
            side,
            strategy_id,
        })
    }
}

impl SignalBytes for MmCancelCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Opening leg
        write_leg(&mut buf, &self.opening_leg, &self.opening_symbol);

        // Cancel side
        buf.put_u8(self.side);

        // Trigger timestamp
        buf.put_i64_le(self.trigger_ts);

        let from_key_len = self.from_key.len() as u32;
        buf.put_u32_le(from_key_len);
        buf.put_slice(&self.from_key);
        buf.put_i32_le(self.strategy_id);
        buf.put_i64_le(self.client_order_id);

        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        // Opening leg
        let (opening_leg, opening_symbol) = read_leg(&mut bytes, true, "opening leg")?;

        if bytes.remaining() < 1 + 8 + 4 {
            return Err("Not enough bytes for cancel side / trigger timestamp".to_string());
        }
        let side = bytes.get_u8();
        if Side::from_u8(side).is_none() {
            return Err(format!("Invalid MmCancelCtx side: {}", side));
        }

        // Trigger timestamp + from_key_len
        let trigger_ts = bytes.get_i64_le();
        let from_key_len = bytes.get_u32_le() as usize;

        if bytes.remaining() < from_key_len {
            return Err(format!(
                "Not enough bytes for from_key: need {}, have {}",
                from_key_len,
                bytes.remaining()
            ));
        }
        let from_key = bytes.copy_to_bytes(from_key_len).to_vec();

        let (strategy_id, client_order_id) = if bytes.remaining() == 0 {
            (0, 0)
        } else if bytes.remaining() == 12 {
            (bytes.get_i32_le(), bytes.get_i64_le())
        } else {
            return Err("Unexpected trailing bytes for MmCancelCtx".to_string());
        };

        Ok(MmCancelCtx {
            opening_leg,
            opening_symbol,
            side,
            trigger_ts,
            from_key_len: from_key_len as u32,
            from_key,
            strategy_id,
            client_order_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::common::{TradingLeg, TradingVenue};

    #[test]
    fn arb_cancel_ctx_roundtrip_with_from_key() {
        let mut ctx = ArbCancelCtx::new();
        ctx.opening_leg = TradingLeg::new(TradingVenue::BinanceMargin, 100.0, 100.1, 123);
        ctx.set_opening_symbol("BTCUSDT");
        ctx.hedging_leg = TradingLeg::new(TradingVenue::BinanceFutures, 100.2, 100.3, 456);
        ctx.set_hedging_symbol("BTCUSDT");
        ctx.trigger_ts = 789;
        ctx.set_from_key(b"fk".to_vec());
        ctx.set_side(Side::Sell);

        let parsed = ArbCancelCtx::from_bytes(ctx.to_bytes()).expect("roundtrip should succeed");
        assert_eq!(parsed.get_opening_symbol(), "BTCUSDT");
        assert_eq!(parsed.get_hedging_symbol(), "BTCUSDT");
        assert_eq!(parsed.trigger_ts, 789);
        assert_eq!(parsed.from_key, b"fk");
        assert_eq!(parsed.get_side(), Side::Sell);
    }

    #[test]
    fn mm_cancel_ctx_roundtrip_with_precise_target() {
        let mut ctx = MmCancelCtx::new();
        ctx.opening_leg = TradingLeg::new(TradingVenue::BinanceMargin, 100.0, 100.1, 123);
        ctx.set_opening_symbol("BTCUSDT");
        ctx.set_side(Side::Buy);
        ctx.trigger_ts = 789;
        ctx.set_from_key(b"fk".to_vec());
        ctx.set_target_strategy(42, 42001);

        let parsed = MmCancelCtx::from_bytes(ctx.to_bytes()).expect("roundtrip should succeed");
        assert_eq!(parsed.get_opening_symbol(), "BTCUSDT");
        assert_eq!(parsed.get_side(), Side::Buy);
        assert_eq!(parsed.trigger_ts, 789);
        assert_eq!(parsed.from_key, b"fk");
        assert_eq!(parsed.strategy_id, 42);
        assert_eq!(parsed.client_order_id, 42001);
    }
}
