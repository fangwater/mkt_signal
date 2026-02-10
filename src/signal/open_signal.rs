use crate::common::tick_math::QuantizedValue;
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::common::TradingLeg;
use crate::signal::common::{bytes_helper, SignalBytes};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Generic arbitrage open signal context
#[derive(Debug, Clone)]
pub struct ArbOpenCtx {
    /// Opening leg (active leg)
    pub opening_leg: TradingLeg,

    /// Opening leg symbol - using fixed size array to avoid heap allocation
    pub opening_symbol: [u8; 32], // 32 bytes should be enough for symbol

    /// Hedging leg (passive leg)
    pub hedging_leg: TradingLeg,

    /// Hedging leg symbol
    pub hedging_symbol: [u8; 32],

    /// Trade side (for opening leg) - stored as u8
    pub side: u8,

    /// Order type - stored as u8
    pub order_type: u8,

    /// Price tick/count encoding (price = tick * count)
    pub price_qv: QuantizedValue,

    /// Amount tick/count encoding (amount = tick * count)
    pub amount_qv: QuantizedValue,

    /// Order expiration time (microseconds)
    pub exp_time: i64,

    /// Creation timestamp (microseconds)
    pub create_ts: i64,

    /// Price offset from best bid/ask for limit order placement
    pub price_offset: f64,

    /// Spread rate between opening and hedging legs
    pub spread_rate: f64,

    /// Hedge timeout (microseconds)
    pub hedge_timeout_us: i64,

    /// From key length
    pub from_key_len: u32,

    /// From key bytes
    pub from_key: Vec<u8>,
}

/// Market maker open signal context
#[derive(Debug, Clone)]
pub struct MmOpenCtx {
    /// Single leg (MM only has one leg)
    pub opening_leg: TradingLeg,

    /// Leg symbol - using fixed size array to avoid heap allocation
    pub opening_symbol: [u8; 32],

    /// Amount tick/count encoding (amount = tick * count)
    pub amount_qv: QuantizedValue,

    /// Trade side (for opening leg) - stored as u8
    pub side: u8,

    /// Order type - stored as u8
    pub order_type: u8,

    /// Price tick/count encoding (price = tick * count)
    pub price_qv: QuantizedValue,

    /// Order expiration time (microseconds)
    pub exp_time: i64,

    /// Creation timestamp (microseconds)
    pub create_ts: i64,

    /// Price offset from best bid/ask for limit order placement
    pub price_offset: f64,

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

impl ArbOpenCtx {
    /// Create new arbitrage open context
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
            side: 0,
            order_type: 0,
            price_qv: QuantizedValue::zero(),
            amount_qv: QuantizedValue::zero(),
            exp_time: 0,
            create_ts: 0,
            price_offset: 0.0,
            spread_rate: 0.0,
            hedge_timeout_us: 0,
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

    /// Set hedging leg symbol
    pub fn set_hedging_symbol(&mut self, symbol: &str) {
        set_symbol(&mut self.hedging_symbol, symbol);
    }

    /// Get hedging leg symbol
    pub fn get_hedging_symbol(&self) -> String {
        get_symbol(&self.hedging_symbol)
    }

    /// Get Side enum
    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.side)
    }

    /// Set Side
    pub fn set_side(&mut self, side: Side) {
        self.side = side.to_u8();
    }

    /// Get OrderType enum
    pub fn get_order_type(&self) -> Option<OrderType> {
        OrderType::from_u8(self.order_type)
    }

    /// Set OrderType
    pub fn set_order_type(&mut self, order_type: OrderType) {
        self.order_type = order_type.to_u8();
    }

    /// Set from key bytes (updates length)
    pub fn set_from_key(&mut self, from_key: Vec<u8>) {
        self.from_key_len = from_key.len() as u32;
        self.from_key = from_key;
    }

    pub fn set_price_with_tick_floor(&mut self, price: f64, preferred_tick: f64) -> bool {
        let fallback = !(preferred_tick.is_finite() && preferred_tick > 0.0);
        self.price_qv = QuantizedValue::encode_floor(price, preferred_tick)
            .unwrap_or_else(QuantizedValue::zero);
        fallback
    }

    pub fn set_amount_with_tick_floor(&mut self, amount: f64, preferred_tick: f64) -> bool {
        let fallback = !(preferred_tick.is_finite() && preferred_tick > 0.0);
        self.amount_qv = QuantizedValue::encode_floor(amount, preferred_tick)
            .unwrap_or_else(QuantizedValue::zero);
        fallback
    }

    pub fn price_value(&self) -> f64 {
        self.price_qv.get_val()
    }

    pub fn amount_value(&self) -> f64 {
        self.amount_qv.get_val()
    }

    pub fn price_count(&self) -> i64 {
        self.price_qv.get_count()
    }

    pub fn amount_count(&self) -> i64 {
        self.amount_qv.get_count()
    }

    pub fn set_amount_from_value_floor(&mut self, amount: f64) {
        self.amount_qv.set_count_floor_from_val(amount);
    }
}

impl MmOpenCtx {
    /// Create new market maker open context
    pub fn new() -> Self {
        Self {
            opening_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
                ts: 0,
            },
            opening_symbol: [0u8; 32],
            amount_qv: QuantizedValue::zero(),
            side: 0,
            order_type: 0,
            price_qv: QuantizedValue::zero(),
            exp_time: 0,
            create_ts: 0,
            price_offset: 0.0,
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

    /// Get Side enum
    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.side)
    }

    /// Set Side
    pub fn set_side(&mut self, side: Side) {
        self.side = side.to_u8();
    }

    /// Get OrderType enum
    pub fn get_order_type(&self) -> Option<OrderType> {
        OrderType::from_u8(self.order_type)
    }

    /// Set OrderType
    pub fn set_order_type(&mut self, order_type: OrderType) {
        self.order_type = order_type.to_u8();
    }

    /// Set from key bytes (updates length)
    pub fn set_from_key(&mut self, from_key: Vec<u8>) {
        self.from_key_len = from_key.len() as u32;
        self.from_key = from_key;
    }

    pub fn set_price_with_tick_floor(&mut self, price: f64, preferred_tick: f64) -> bool {
        let fallback = !(preferred_tick.is_finite() && preferred_tick > 0.0);
        self.price_qv = QuantizedValue::encode_floor(price, preferred_tick)
            .unwrap_or_else(QuantizedValue::zero);
        fallback
    }

    pub fn set_amount_with_tick_floor(&mut self, amount: f64, preferred_tick: f64) -> bool {
        let fallback = !(preferred_tick.is_finite() && preferred_tick > 0.0);
        self.amount_qv = QuantizedValue::encode_floor(amount, preferred_tick)
            .unwrap_or_else(QuantizedValue::zero);
        fallback
    }

    pub fn price_value(&self) -> f64 {
        self.price_qv.get_val()
    }

    pub fn amount_value(&self) -> f64 {
        self.amount_qv.get_val()
    }

    pub fn price_count(&self) -> i64 {
        self.price_qv.get_count()
    }

    pub fn amount_count(&self) -> i64 {
        self.amount_qv.get_count()
    }
}

impl SignalBytes for ArbOpenCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Opening leg
        write_leg(&mut buf, &self.opening_leg, &self.opening_symbol);

        // Hedging leg
        write_leg(&mut buf, &self.hedging_leg, &self.hedging_symbol);

        // Trade parameters
        buf.put_u8(self.side);
        buf.put_u8(self.order_type);
        let (price_tick_i64, price_tick_exp) = self.price_qv.get_tick_parts();
        let (amount_tick_i64, amount_tick_exp) = self.amount_qv.get_tick_parts();
        buf.put_i64_le(price_tick_i64);
        buf.put_i32_le(price_tick_exp);
        buf.put_i64_le(self.price_qv.get_count());
        buf.put_i64_le(amount_tick_i64);
        buf.put_i32_le(amount_tick_exp);
        buf.put_i64_le(self.amount_qv.get_count());
        buf.put_i64_le(self.exp_time);
        buf.put_i64_le(self.create_ts);
        buf.put_f64_le(self.price_offset);
        buf.put_f64_le(self.spread_rate);
        buf.put_i64_le(self.hedge_timeout_us);

        let from_key_len = self.from_key.len() as u32;
        buf.put_u32_le(from_key_len);
        buf.put_slice(&self.from_key);

        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        const TAIL_LEN: usize = 1 + 1 + 8 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 8 + 4;

        // Opening leg
        let (opening_leg, opening_symbol) = read_leg(&mut bytes, true, "opening leg")?;

        // Hedging leg
        let (hedging_leg, hedging_symbol) = read_leg(&mut bytes, true, "hedging leg")?;

        // Trade parameters + from_key_len
        if bytes.remaining() < TAIL_LEN {
            return Err("Not enough bytes for trade parameters".to_string());
        }
        let side = bytes.get_u8();
        let order_type = bytes.get_u8();
        let price_tick_i64 = bytes.get_i64_le();
        let price_tick_exp = bytes.get_i32_le();
        let price_count = bytes.get_i64_le();
        let amount_tick_i64 = bytes.get_i64_le();
        let amount_tick_exp = bytes.get_i32_le();
        let amount_count = bytes.get_i64_le();
        let exp_time = bytes.get_i64_le();
        let create_ts = bytes.get_i64_le();
        let price_offset = bytes.get_f64_le();
        let spread_rate = bytes.get_f64_le();
        let hedge_timeout_us = bytes.get_i64_le();
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
            return Err("Unexpected trailing bytes for ArbOpenCtx".to_string());
        }

        Ok(ArbOpenCtx {
            opening_leg,
            opening_symbol,
            hedging_leg,
            hedging_symbol,
            side,
            order_type,
            price_qv: QuantizedValue::from_parts(price_tick_i64, price_tick_exp, price_count),
            amount_qv: QuantizedValue::from_parts(amount_tick_i64, amount_tick_exp, amount_count),
            exp_time,
            create_ts,
            price_offset,
            spread_rate,
            hedge_timeout_us,
            from_key_len: from_key.len() as u32,
            from_key,
        })
    }
}

impl SignalBytes for MmOpenCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Opening leg
        write_leg(&mut buf, &self.opening_leg, &self.opening_symbol);

        // Trade parameters
        buf.put_u8(self.side);
        buf.put_u8(self.order_type);
        let (price_tick_i64, price_tick_exp) = self.price_qv.get_tick_parts();
        let (amount_tick_i64, amount_tick_exp) = self.amount_qv.get_tick_parts();
        buf.put_i64_le(price_tick_i64);
        buf.put_i32_le(price_tick_exp);
        buf.put_i64_le(self.price_qv.get_count());
        buf.put_i64_le(amount_tick_i64);
        buf.put_i32_le(amount_tick_exp);
        buf.put_i64_le(self.amount_qv.get_count());
        buf.put_i64_le(self.exp_time);
        buf.put_i64_le(self.create_ts);
        buf.put_f64_le(self.price_offset);

        let from_key_len = self.from_key.len() as u32;
        buf.put_u32_le(from_key_len);
        buf.put_slice(&self.from_key);

        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        const TAIL_LEN: usize = 1 + 1 + 8 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4;
        // Opening leg
        let (opening_leg, opening_symbol) = read_leg(&mut bytes, true, "opening leg")?;

        // Trade parameters + from_key_len
        if bytes.remaining() < TAIL_LEN {
            return Err("Not enough bytes for trade parameters".to_string());
        }
        let side = bytes.get_u8();
        let order_type = bytes.get_u8();
        let price_tick_i64 = bytes.get_i64_le();
        let price_tick_exp = bytes.get_i32_le();
        let price_count = bytes.get_i64_le();
        let amount_tick_i64 = bytes.get_i64_le();
        let amount_tick_exp = bytes.get_i32_le();
        let amount_count = bytes.get_i64_le();
        let exp_time = bytes.get_i64_le();
        let create_ts = bytes.get_i64_le();
        let price_offset = bytes.get_f64_le();
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
            return Err("Unexpected trailing bytes for MmOpenCtx".to_string());
        }

        Ok(MmOpenCtx {
            opening_leg,
            opening_symbol,
            amount_qv: QuantizedValue::from_parts(amount_tick_i64, amount_tick_exp, amount_count),
            side,
            order_type,
            price_qv: QuantizedValue::from_parts(price_tick_i64, price_tick_exp, price_count),
            exp_time,
            create_ts,
            price_offset,
            from_key_len: from_key_len as u32,
            from_key,
        })
    }
}
