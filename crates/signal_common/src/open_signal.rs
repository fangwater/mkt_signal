use crate::common::TradingLeg;
use crate::common::{bytes_helper, SignalBytes};
use crate::tick_math::QuantizedValue;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use order_common::{OrderType, Side};

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

#[derive(Debug, Clone, Copy)]
pub struct ArbOpenCtxView<'a> {
    pub opening_leg: TradingLeg,
    pub opening_symbol: [u8; 32],
    pub hedging_leg: TradingLeg,
    pub hedging_symbol: [u8; 32],
    pub side: u8,
    pub order_type: u8,
    pub price_qv: QuantizedValue,
    pub amount_qv: QuantizedValue,
    pub exp_time: i64,
    pub create_ts: i64,
    pub price_offset: f64,
    pub spread_rate: f64,
    pub hedge_timeout_us: i64,
    pub from_key_len: u32,
    pub from_key: &'a [u8],
}

#[derive(Debug, Clone, Copy)]
pub struct MmOpenCtxView<'a> {
    pub opening_leg: TradingLeg,
    pub opening_symbol: [u8; 32],
    pub amount_qv: QuantizedValue,
    pub side: u8,
    pub order_type: u8,
    pub price_qv: QuantizedValue,
    pub exp_time: i64,
    pub create_ts: i64,
    pub price_offset: f64,
    pub from_key_len: u32,
    pub from_key: &'a [u8],
}

fn set_symbol(target: &mut [u8; 32], symbol: &str) {
    // 清零再写，避免上次写入的尾部残留（更短的新值会保留旧字节，触发 get_symbol 读到错误后缀）
    target.fill(0);
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

fn read_u8(raw: &[u8], offset: &mut usize, label: &str) -> Result<u8, String> {
    if raw.len() < *offset + 1 {
        return Err(format!("Not enough bytes for {}", label));
    }
    let value = raw[*offset];
    *offset += 1;
    Ok(value)
}

fn read_i32_le(raw: &[u8], offset: &mut usize, label: &str) -> Result<i32, String> {
    if raw.len() < *offset + 4 {
        return Err(format!("Not enough bytes for {}", label));
    }
    let value = i32::from_le_bytes(
        raw[*offset..*offset + 4]
            .try_into()
            .map_err(|_| format!("Invalid bytes for {}", label))?,
    );
    *offset += 4;
    Ok(value)
}

fn read_u32_le(raw: &[u8], offset: &mut usize, label: &str) -> Result<u32, String> {
    if raw.len() < *offset + 4 {
        return Err(format!("Not enough bytes for {}", label));
    }
    let value = u32::from_le_bytes(
        raw[*offset..*offset + 4]
            .try_into()
            .map_err(|_| format!("Invalid bytes for {}", label))?,
    );
    *offset += 4;
    Ok(value)
}

fn read_i64_le(raw: &[u8], offset: &mut usize, label: &str) -> Result<i64, String> {
    if raw.len() < *offset + 8 {
        return Err(format!("Not enough bytes for {}", label));
    }
    let value = i64::from_le_bytes(
        raw[*offset..*offset + 8]
            .try_into()
            .map_err(|_| format!("Invalid bytes for {}", label))?,
    );
    *offset += 8;
    Ok(value)
}

fn read_f64_le(raw: &[u8], offset: &mut usize, label: &str) -> Result<f64, String> {
    if raw.len() < *offset + 8 {
        return Err(format!("Not enough bytes for {}", label));
    }
    let value = f64::from_le_bytes(
        raw[*offset..*offset + 8]
            .try_into()
            .map_err(|_| format!("Invalid bytes for {}", label))?,
    );
    *offset += 8;
    Ok(value)
}

fn read_fixed_symbol(raw: &[u8], offset: &mut usize, label: &str) -> Result<[u8; 32], String> {
    let len = read_u8(raw, offset, label)? as usize;
    if len > 32 {
        return Err(format!("Invalid array length: {}", len));
    }
    if raw.len() < *offset + len {
        return Err(format!(
            "Not enough bytes for array data: need {}, have {}",
            len,
            raw.len().saturating_sub(*offset)
        ));
    }
    let mut arr = [0u8; 32];
    arr[..len].copy_from_slice(&raw[*offset..*offset + len]);
    *offset += len;
    Ok(arr)
}

fn read_leg_ref(
    raw: &[u8],
    offset: &mut usize,
    with_ts: bool,
    label: &str,
) -> Result<(TradingLeg, [u8; 32]), String> {
    let venue = read_u8(raw, offset, label)?;
    let bid0 = read_f64_le(raw, offset, label)?;
    let ask0 = read_f64_le(raw, offset, label)?;
    let ts = if with_ts {
        read_i64_le(raw, offset, label)?
    } else {
        0
    };
    let symbol = read_fixed_symbol(raw, offset, label)?;
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

impl Default for ArbOpenCtx {
    fn default() -> Self {
        Self::new()
    }
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

impl Default for MmOpenCtx {
    fn default() -> Self {
        Self::new()
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

impl<'a> ArbOpenCtxView<'a> {
    pub fn from_bytes(raw: &'a [u8]) -> Result<Self, String> {
        let mut offset = 0usize;
        let (opening_leg, opening_symbol) = read_leg_ref(raw, &mut offset, true, "opening leg")?;
        let (hedging_leg, hedging_symbol) = read_leg_ref(raw, &mut offset, true, "hedging leg")?;

        let side = read_u8(raw, &mut offset, "side")?;
        let order_type = read_u8(raw, &mut offset, "order_type")?;
        let price_tick_i64 = read_i64_le(raw, &mut offset, "price_tick_i64")?;
        let price_tick_exp = read_i32_le(raw, &mut offset, "price_tick_exp")?;
        let price_count = read_i64_le(raw, &mut offset, "price_count")?;
        let amount_tick_i64 = read_i64_le(raw, &mut offset, "amount_tick_i64")?;
        let amount_tick_exp = read_i32_le(raw, &mut offset, "amount_tick_exp")?;
        let amount_count = read_i64_le(raw, &mut offset, "amount_count")?;
        let exp_time = read_i64_le(raw, &mut offset, "exp_time")?;
        let create_ts = read_i64_le(raw, &mut offset, "create_ts")?;
        let price_offset = read_f64_le(raw, &mut offset, "price_offset")?;
        let spread_rate = read_f64_le(raw, &mut offset, "spread_rate")?;
        let hedge_timeout_us = read_i64_le(raw, &mut offset, "hedge_timeout_us")?;
        let from_key_len = read_u32_le(raw, &mut offset, "from_key_len")?;
        let from_key_len_usize = from_key_len as usize;
        if raw.len() < offset + from_key_len_usize {
            return Err(format!(
                "Not enough bytes for from_key: need {}, have {}",
                from_key_len_usize,
                raw.len().saturating_sub(offset)
            ));
        }
        let from_key = &raw[offset..offset + from_key_len_usize];
        offset += from_key_len_usize;
        if offset != raw.len() {
            return Err("Unexpected trailing bytes for ArbOpenCtx".to_string());
        }

        Ok(Self {
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
            from_key_len,
            from_key,
        })
    }

    pub fn get_opening_symbol(&self) -> String {
        get_symbol(&self.opening_symbol)
    }

    pub fn get_hedging_symbol(&self) -> String {
        get_symbol(&self.hedging_symbol)
    }

    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.side)
    }

    pub fn get_order_type(&self) -> Option<OrderType> {
        OrderType::from_u8(self.order_type)
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

    pub fn to_owned_with_symbols(&self, opening_symbol: &str, hedging_symbol: &str) -> ArbOpenCtx {
        let mut ctx = ArbOpenCtx {
            opening_leg: self.opening_leg,
            opening_symbol: [0u8; 32],
            hedging_leg: self.hedging_leg,
            hedging_symbol: [0u8; 32],
            side: self.side,
            order_type: self.order_type,
            price_qv: self.price_qv,
            amount_qv: self.amount_qv,
            exp_time: self.exp_time,
            create_ts: self.create_ts,
            price_offset: self.price_offset,
            spread_rate: self.spread_rate,
            hedge_timeout_us: self.hedge_timeout_us,
            from_key_len: self.from_key_len,
            from_key: self.from_key.to_vec(),
        };
        ctx.set_opening_symbol(opening_symbol);
        ctx.set_hedging_symbol(hedging_symbol);
        ctx
    }
}

impl<'a> MmOpenCtxView<'a> {
    pub fn from_bytes(raw: &'a [u8]) -> Result<Self, String> {
        let mut offset = 0usize;
        let (opening_leg, opening_symbol) = read_leg_ref(raw, &mut offset, true, "opening leg")?;

        let side = read_u8(raw, &mut offset, "side")?;
        let order_type = read_u8(raw, &mut offset, "order_type")?;
        let price_tick_i64 = read_i64_le(raw, &mut offset, "price_tick_i64")?;
        let price_tick_exp = read_i32_le(raw, &mut offset, "price_tick_exp")?;
        let price_count = read_i64_le(raw, &mut offset, "price_count")?;
        let amount_tick_i64 = read_i64_le(raw, &mut offset, "amount_tick_i64")?;
        let amount_tick_exp = read_i32_le(raw, &mut offset, "amount_tick_exp")?;
        let amount_count = read_i64_le(raw, &mut offset, "amount_count")?;
        let exp_time = read_i64_le(raw, &mut offset, "exp_time")?;
        let create_ts = read_i64_le(raw, &mut offset, "create_ts")?;
        let price_offset = read_f64_le(raw, &mut offset, "price_offset")?;
        let from_key_len = read_u32_le(raw, &mut offset, "from_key_len")?;
        let from_key_len_usize = from_key_len as usize;
        if raw.len() < offset + from_key_len_usize {
            return Err(format!(
                "Not enough bytes for from_key: need {}, have {}",
                from_key_len_usize,
                raw.len().saturating_sub(offset)
            ));
        }
        let from_key = &raw[offset..offset + from_key_len_usize];
        offset += from_key_len_usize;
        if offset != raw.len() {
            return Err("Unexpected trailing bytes for MmOpenCtx".to_string());
        }

        Ok(Self {
            opening_leg,
            opening_symbol,
            amount_qv: QuantizedValue::from_parts(amount_tick_i64, amount_tick_exp, amount_count),
            side,
            order_type,
            price_qv: QuantizedValue::from_parts(price_tick_i64, price_tick_exp, price_count),
            exp_time,
            create_ts,
            price_offset,
            from_key_len,
            from_key,
        })
    }

    pub fn get_opening_symbol(&self) -> String {
        get_symbol(&self.opening_symbol)
    }

    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.side)
    }

    pub fn get_order_type(&self) -> Option<OrderType> {
        OrderType::from_u8(self.order_type)
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

    pub fn to_owned_with_symbol(&self, opening_symbol: &str) -> MmOpenCtx {
        let mut ctx = MmOpenCtx {
            opening_leg: self.opening_leg,
            opening_symbol: [0u8; 32],
            amount_qv: self.amount_qv,
            side: self.side,
            order_type: self.order_type,
            price_qv: self.price_qv,
            exp_time: self.exp_time,
            create_ts: self.create_ts,
            price_offset: self.price_offset,
            from_key_len: self.from_key_len,
            from_key: self.from_key.to_vec(),
        };
        ctx.set_opening_symbol(opening_symbol);
        ctx
    }
}

impl SignalBytes for ArbOpenCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.write_to(&mut buf);
        buf.freeze()
    }

    fn write_to(&self, buf: &mut BytesMut) {
        // Opening leg
        write_leg(buf, &self.opening_leg, &self.opening_symbol);

        // Hedging leg
        write_leg(buf, &self.hedging_leg, &self.hedging_symbol);

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
        self.write_to(&mut buf);
        buf.freeze()
    }

    fn write_to(&self, buf: &mut BytesMut) {
        // Opening leg
        write_leg(buf, &self.opening_leg, &self.opening_symbol);

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

#[cfg(test)]
mod tests {
    use super::*;
    use order_common::TradingVenue;

    fn qv(tick_i64: i64, tick_exp: i32, count: i64) -> QuantizedValue {
        QuantizedValue::from_parts(tick_i64, tick_exp, count)
    }

    #[test]
    fn arb_open_ctx_view_matches_owned_parse() {
        let mut ctx = ArbOpenCtx::new();
        ctx.opening_leg = TradingLeg::new(TradingVenue::BinanceMargin, 1.0, 1.1, 101);
        ctx.hedging_leg = TradingLeg::new(TradingVenue::BinanceFutures, 2.0, 2.1, 202);
        ctx.set_opening_symbol("BTCUSDT");
        ctx.set_hedging_symbol("BTCUSDT");
        ctx.set_side(Side::Buy);
        ctx.set_order_type(OrderType::Limit);
        ctx.price_qv = qv(1, -2, 12345);
        ctx.amount_qv = qv(1, -4, 6789);
        ctx.exp_time = 333;
        ctx.create_ts = 444;
        ctx.price_offset = 0.25;
        ctx.spread_rate = 0.003;
        ctx.hedge_timeout_us = 555;
        ctx.set_from_key(b"arb-from-key".to_vec());

        let bytes = ctx.to_bytes();
        let mut written = BytesMut::new();
        ctx.write_to(&mut written);
        assert_eq!(written.as_ref(), bytes.as_ref());

        let owned = ArbOpenCtx::from_bytes(bytes.clone()).expect("owned parse");
        let view = ArbOpenCtxView::from_bytes(bytes.as_ref()).expect("view parse");

        assert_eq!(view.opening_leg.venue, owned.opening_leg.venue);
        assert_eq!(view.hedging_leg.ts, owned.hedging_leg.ts);
        assert_eq!(view.get_opening_symbol(), owned.get_opening_symbol());
        assert_eq!(view.get_hedging_symbol(), owned.get_hedging_symbol());
        assert_eq!(view.side, owned.side);
        assert_eq!(view.order_type, owned.order_type);
        assert_eq!(view.price_qv, owned.price_qv);
        assert_eq!(view.amount_qv, owned.amount_qv);
        assert_eq!(view.from_key, owned.from_key.as_slice());
        assert_eq!(
            view.to_owned_with_symbols("BTCUSDT", "BTCUSDT").from_key,
            owned.from_key
        );
    }

    #[test]
    fn mm_open_ctx_view_matches_owned_parse() {
        let mut ctx = MmOpenCtx::new();
        ctx.opening_leg = TradingLeg::new(TradingVenue::GateFutures, 3.0, 3.1, 303);
        ctx.set_opening_symbol("ETHUSDT");
        ctx.set_side(Side::Sell);
        ctx.set_order_type(OrderType::Limit);
        ctx.price_qv = qv(1, -2, 23456);
        ctx.amount_qv = qv(1, -3, 789);
        ctx.exp_time = 666;
        ctx.create_ts = 777;
        ctx.price_offset = -0.5;
        ctx.set_from_key(b"mm-from-key".to_vec());

        let bytes = ctx.to_bytes();
        let mut written = BytesMut::new();
        ctx.write_to(&mut written);
        assert_eq!(written.as_ref(), bytes.as_ref());

        let owned = MmOpenCtx::from_bytes(bytes.clone()).expect("owned parse");
        let view = MmOpenCtxView::from_bytes(bytes.as_ref()).expect("view parse");

        assert_eq!(view.opening_leg.venue, owned.opening_leg.venue);
        assert_eq!(view.opening_leg.ts, owned.opening_leg.ts);
        assert_eq!(view.get_opening_symbol(), owned.get_opening_symbol());
        assert_eq!(view.side, owned.side);
        assert_eq!(view.order_type, owned.order_type);
        assert_eq!(view.price_qv, owned.price_qv);
        assert_eq!(view.amount_qv, owned.amount_qv);
        assert_eq!(view.from_key, owned.from_key.as_slice());
        assert_eq!(
            view.to_owned_with_symbol("ETHUSDT").from_key,
            owned.from_key
        );
    }
}
