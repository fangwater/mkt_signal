use crate::signal::common::{TradingLeg, SignalBytes, bytes_helper};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Generic arbitrage cancel signal context
#[derive(Debug, Clone, Copy)]
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
}

impl ArbCancelCtx {
    /// Create new arbitrage cancel context
    pub fn new() -> Self {
        Self {
            opening_leg: TradingLeg { venue: 0, bid0: 0.0, ask0: 0.0 },
            opening_symbol: [0u8; 32],
            hedging_leg: TradingLeg { venue: 0, bid0: 0.0, ask0: 0.0 },
            hedging_symbol: [0u8; 32],
            trigger_ts: 0,
        }
    }

    /// Set opening leg symbol
    pub fn set_opening_symbol(&mut self, symbol: &str) {
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        self.opening_symbol[..len].copy_from_slice(&bytes[..len]);
    }

    /// Get opening leg symbol
    pub fn get_opening_symbol(&self) -> String {
        let end = self.opening_symbol.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.opening_symbol[..end]).to_string()
    }

    /// Set hedging leg symbol
    pub fn set_hedging_symbol(&mut self, symbol: &str) {
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        self.hedging_symbol[..len].copy_from_slice(&bytes[..len]);
    }

    /// Get hedging leg symbol
    pub fn get_hedging_symbol(&self) -> String {
        let end = self.hedging_symbol.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.hedging_symbol[..end]).to_string()
    }
}

impl SignalBytes for ArbCancelCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Opening leg
        buf.put_u8(self.opening_leg.venue);
        buf.put_f64_le(self.opening_leg.bid0);
        buf.put_f64_le(self.opening_leg.ask0);
        bytes_helper::write_fixed_bytes(&mut buf, &self.opening_symbol);

        // Hedging leg
        buf.put_u8(self.hedging_leg.venue);
        buf.put_f64_le(self.hedging_leg.bid0);
        buf.put_f64_le(self.hedging_leg.ask0);
        bytes_helper::write_fixed_bytes(&mut buf, &self.hedging_symbol);

        // Trigger timestamp
        buf.put_i64_le(self.trigger_ts);

        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        // Opening leg
        if bytes.remaining() < 1 + 8 + 8 {
            return Err("Not enough bytes for opening leg".to_string());
        }
        let opening_venue = bytes.get_u8();
        let opening_bid0 = bytes.get_f64_le();
        let opening_ask0 = bytes.get_f64_le();
        let opening_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        // Hedging leg
        if bytes.remaining() < 1 + 8 + 8 {
            return Err("Not enough bytes for hedging leg".to_string());
        }
        let hedging_venue = bytes.get_u8();
        let hedging_bid0 = bytes.get_f64_le();
        let hedging_ask0 = bytes.get_f64_le();
        let hedging_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        // Trigger timestamp
        if bytes.remaining() < 8 {
            return Err("Not enough bytes for trigger timestamp".to_string());
        }
        let trigger_ts = bytes.get_i64_le();

        Ok(Self {
            opening_leg: TradingLeg {
                venue: opening_venue,
                bid0: opening_bid0,
                ask0: opening_ask0,
            },
            opening_symbol,
            hedging_leg: TradingLeg {
                venue: hedging_venue,
                bid0: hedging_bid0,
                ask0: hedging_ask0,
            },
            hedging_symbol,
            trigger_ts,
        })
    }
}