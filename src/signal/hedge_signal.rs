use crate::signal::common::{TradingLeg, Side, SignalBytes, bytes_helper};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Maker hedge signal context (for MM strategy - limit order hedging)
#[derive(Debug, Clone, Copy)]
pub struct MakerHedgeCtx {
    /// Strategy ID that needs hedging
    pub strategy_id: i32,

    /// Client order ID for tracking
    pub client_order_id: i64,

    /// Hedge quantity
    pub hedge_qty: f64,

    /// Hedge side (Buy/Sell) - stored as u8
    pub hedge_side: u8,

    /// Limit price for maker order
    pub limit_price: f64,

    /// Price tick size
    pub price_tick: f64,

    /// Whether to use post-only order
    pub maker_only: bool,

    /// Order expiration time (microseconds)
    pub exp_time: i64,

    /// Hedging leg market data
    pub hedging_leg: TradingLeg,

    /// Hedging leg symbol
    pub hedging_symbol: [u8; 32],

    /// Market data timestamp
    pub market_ts: i64,
}

impl MakerHedgeCtx {
    /// Create new maker hedge context
    pub fn new() -> Self {
        Self {
            strategy_id: 0,
            client_order_id: 0,
            hedge_qty: 0.0,
            hedge_side: 0,
            limit_price: 0.0,
            price_tick: 0.0,
            maker_only: false,
            exp_time: 0,
            hedging_leg: TradingLeg { venue: 0, bid0: 0.0, ask0: 0.0 },
            hedging_symbol: [0u8; 32],
            market_ts: 0,
        }
    }

    /// Set hedging symbol
    pub fn set_hedging_symbol(&mut self, symbol: &str) {
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        self.hedging_symbol[..len].copy_from_slice(&bytes[..len]);
    }

    /// Get hedging symbol
    pub fn get_hedging_symbol(&self) -> String {
        let end = self.hedging_symbol.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.hedging_symbol[..end]).to_string()
    }

    /// Get Side enum
    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.hedge_side)
    }

    /// Set Side
    pub fn set_side(&mut self, side: Side) {
        self.hedge_side = side.to_u8();
    }
}

/// Taker hedge signal context (for MT strategy - market order hedging)
#[derive(Debug, Clone, Copy)]
pub struct TakerHedgeCtx {
    /// Strategy ID that needs hedging
    pub strategy_id: i32,

    /// Client order ID for tracking
    pub client_order_id: i64,

    /// Hedge quantity
    pub hedge_qty: f64,

    /// Hedge side (Buy/Sell) - stored as u8
    pub hedge_side: u8,

    /// Hedging leg market data
    pub hedging_leg: TradingLeg,

    /// Hedging leg symbol
    pub hedging_symbol: [u8; 32],

    /// Market data timestamp
    pub market_ts: i64,
}

impl TakerHedgeCtx {
    /// Create new taker hedge context
    pub fn new() -> Self {
        Self {
            strategy_id: 0,
            client_order_id: 0,
            hedge_qty: 0.0,
            hedge_side: 0,
            hedging_leg: TradingLeg { venue: 0, bid0: 0.0, ask0: 0.0 },
            hedging_symbol: [0u8; 32],
            market_ts: 0,
        }
    }

    /// Set hedging symbol
    pub fn set_hedging_symbol(&mut self, symbol: &str) {
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        self.hedging_symbol[..len].copy_from_slice(&bytes[..len]);
    }

    /// Get hedging symbol
    pub fn get_hedging_symbol(&self) -> String {
        let end = self.hedging_symbol.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.hedging_symbol[..end]).to_string()
    }

    /// Get Side enum
    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.hedge_side)
    }

    /// Set Side
    pub fn set_side(&mut self, side: Side) {
        self.hedge_side = side.to_u8();
    }
}
impl SignalBytes for MakerHedgeCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i32_le(self.strategy_id);
        buf.put_i64_le(self.client_order_id);
        buf.put_f64_le(self.hedge_qty);
        buf.put_u8(self.hedge_side);
        buf.put_f64_le(self.limit_price);
        buf.put_f64_le(self.price_tick);
        buf.put_u8(if self.maker_only { 1 } else { 0 });
        buf.put_i64_le(self.exp_time);

        // Hedging leg market data
        buf.put_u8(self.hedging_leg.venue);
        buf.put_f64_le(self.hedging_leg.bid0);
        buf.put_f64_le(self.hedging_leg.ask0);
        bytes_helper::write_fixed_bytes(&mut buf, &self.hedging_symbol);

        buf.put_i64_le(self.market_ts);

        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 4 + 8 + 8 + 1 + 8 + 8 + 1 + 8 {
            return Err("Not enough bytes for maker hedge basic fields".to_string());
        }

        let strategy_id = bytes.get_i32_le();
        let client_order_id = bytes.get_i64_le();
        let hedge_qty = bytes.get_f64_le();
        let hedge_side = bytes.get_u8();
        let limit_price = bytes.get_f64_le();
        let price_tick = bytes.get_f64_le();
        let maker_only = bytes.get_u8() != 0;
        let exp_time = bytes.get_i64_le();

        // Hedging leg market data
        if bytes.remaining() < 1 + 8 + 8 {
            return Err("Not enough bytes for hedging leg".to_string());
        }
        let hedging_venue = bytes.get_u8();
        let hedging_bid0 = bytes.get_f64_le();
        let hedging_ask0 = bytes.get_f64_le();
        let hedging_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for market_ts".to_string());
        }
        let market_ts = bytes.get_i64_le();

        Ok(Self {
            strategy_id,
            client_order_id,
            hedge_qty,
            hedge_side,
            limit_price,
            price_tick,
            maker_only,
            exp_time,
            hedging_leg: TradingLeg {
                venue: hedging_venue,
                bid0: hedging_bid0,
                ask0: hedging_ask0,
            },
            hedging_symbol,
            market_ts,
        })
    }
}

impl SignalBytes for TakerHedgeCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i32_le(self.strategy_id);
        buf.put_i64_le(self.client_order_id);
        buf.put_f64_le(self.hedge_qty);
        buf.put_u8(self.hedge_side);

        // Hedging leg market data
        buf.put_u8(self.hedging_leg.venue);
        buf.put_f64_le(self.hedging_leg.bid0);
        buf.put_f64_le(self.hedging_leg.ask0);
        bytes_helper::write_fixed_bytes(&mut buf, &self.hedging_symbol);

        buf.put_i64_le(self.market_ts);

        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 4 + 8 + 8 + 1 {
            return Err("Not enough bytes for taker hedge basic fields".to_string());
        }

        let strategy_id = bytes.get_i32_le();
        let client_order_id = bytes.get_i64_le();
        let hedge_qty = bytes.get_f64_le();
        let hedge_side = bytes.get_u8();

        // Hedging leg market data
        if bytes.remaining() < 1 + 8 + 8 {
            return Err("Not enough bytes for hedging leg".to_string());
        }
        let hedging_venue = bytes.get_u8();
        let hedging_bid0 = bytes.get_f64_le();
        let hedging_ask0 = bytes.get_f64_le();
        let hedging_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;

        if bytes.remaining() < 8 {
            return Err("Not enough bytes for market_ts".to_string());
        }
        let market_ts = bytes.get_i64_le();

        Ok(Self {
            strategy_id,
            client_order_id,
            hedge_qty,
            hedge_side,
            hedging_leg: TradingLeg {
                venue: hedging_venue,
                bid0: hedging_bid0,
                ask0: hedging_ask0,
            },
            hedging_symbol,
            market_ts,
        })
    }
}
