use crate::pre_trade::order_manager::Side;
use crate::signal::common::{bytes_helper, SignalBytes, TradingLeg};
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

        buf.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, String> {
        fn detect_legacy_format(
            bytes: &Bytes,
            base_offset: usize,
            tail_len: usize,
        ) -> Option<bool> {
            let total = bytes.len();
            let mut new_match = false;
            let mut old_match = false;

            let open_len_idx_new = base_offset + 25;
            if total > open_len_idx_new {
                let open_len = bytes[open_len_idx_new] as usize;
                if open_len <= 32 {
                    let hedge_len_idx = base_offset + 51 + open_len;
                    if total > hedge_len_idx {
                        let hedge_len = bytes[hedge_len_idx] as usize;
                        if hedge_len <= 32 {
                            let expected = base_offset + 52 + open_len + hedge_len + tail_len;
                            if total == expected {
                                new_match = true;
                            }
                        }
                    }
                }
            }

            let open_len_idx_old = base_offset + 17;
            if total > open_len_idx_old {
                let open_len = bytes[open_len_idx_old] as usize;
                if open_len <= 32 {
                    let hedge_len_idx = base_offset + 35 + open_len;
                    if total > hedge_len_idx {
                        let hedge_len = bytes[hedge_len_idx] as usize;
                        if hedge_len <= 32 {
                            let expected = base_offset + 36 + open_len + hedge_len + tail_len;
                            if total == expected {
                                old_match = true;
                            }
                        }
                    }
                }
            }

            match (new_match, old_match) {
                (true, false) => Some(false),
                (false, true) => Some(true),
                (true, true) => Some(false),
                _ => None,
            }
        }

        fn parse(mut bytes: Bytes, with_ts: bool) -> Result<ArbCancelCtx, String> {
            // Opening leg
            let (opening_leg, opening_symbol) = read_leg(&mut bytes, with_ts, "opening leg")?;

            // Hedging leg
            let (hedging_leg, hedging_symbol) = read_leg(&mut bytes, with_ts, "hedging leg")?;

            // Trigger timestamp
            if bytes.remaining() < 8 {
                return Err("Not enough bytes for trigger timestamp".to_string());
            }
            let trigger_ts = bytes.get_i64_le();

            Ok(ArbCancelCtx {
                opening_leg,
                opening_symbol,
                hedging_leg,
                hedging_symbol,
                trigger_ts,
            })
        }

        let format = detect_legacy_format(&bytes, 0, 8);
        match format {
            Some(true) => parse(bytes, false),
            Some(false) => parse(bytes, true),
            None => parse(bytes.clone(), true).or_else(|_| parse(bytes, false)),
        }
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

        if bytes.remaining() != 0 {
            return Err("Unexpected trailing bytes for MmCancelCtx".to_string());
        }

        Ok(MmCancelCtx {
            opening_leg,
            opening_symbol,
            side,
            trigger_ts,
            from_key_len: from_key_len as u32,
            from_key,
        })
    }
}
