use crate::common::TradingLeg;
use crate::common::{bytes_helper, SignalBytes};
use crate::tick_math::QuantizedValue;
use bytes::{BufMut, Bytes, BytesMut};
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

pub const MM_OPEN_BATCH_VERSION: u8 = 1;
pub const MM_OPEN_BATCH_LEVEL_ENCODED_LEN: usize = 1 + 8 + 8 + 8;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MmOpenBatchLevel {
    pub side: u8,
    pub price_count: i64,
    pub amount_count: i64,
    pub price_offset: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct MmOpenBatchCtx<'a> {
    pub opening_leg: TradingLeg,
    pub opening_symbol: [u8; 32],
    pub order_type: u8,
    pub price_tick_i64: i64,
    pub price_tick_exp: i32,
    pub amount_tick_i64: i64,
    pub amount_tick_exp: i32,
    pub exp_time: i64,
    pub create_ts: i64,
    pub from_key: &'a [u8],
    pub levels: &'a [MmOpenBatchLevel],
}

#[derive(Debug, Clone, Copy)]
pub struct MmOpenBatchCtxView<'a> {
    pub opening_leg: TradingLeg,
    pub opening_symbol: [u8; 32],
    pub order_type: u8,
    pub price_tick_i64: i64,
    pub price_tick_exp: i32,
    pub amount_tick_i64: i64,
    pub amount_tick_exp: i32,
    pub exp_time: i64,
    pub create_ts: i64,
    pub from_key_len: u32,
    pub from_key: &'a [u8],
    level_count: usize,
    levels_raw: &'a [u8],
}

pub struct MmOpenBatchLevelIter<'a> {
    batch: MmOpenBatchCtxView<'a>,
    index: usize,
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
    buf.put_f64_le(leg.bid_qty0);
    buf.put_f64_le(leg.ask0);
    buf.put_f64_le(leg.ask_qty0);
    buf.put_i64_le(leg.ts);
    bytes_helper::write_fixed_bytes(buf, symbol);
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
    let bid_qty0 = read_f64_le(raw, offset, label)?;
    let ask0 = read_f64_le(raw, offset, label)?;
    let ask_qty0 = read_f64_le(raw, offset, label)?;
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
            bid_qty0,
            ask0,
            ask_qty0,
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
                bid_qty0: 0.0,
                ask0: 0.0,
                ask_qty0: 0.0,
                ts: 0,
            },
            opening_symbol: [0u8; 32],
            hedging_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                bid_qty0: 0.0,
                ask0: 0.0,
                ask_qty0: 0.0,
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
                bid_qty0: 0.0,
                ask0: 0.0,
                ask_qty0: 0.0,
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

impl<'a> MmOpenBatchCtx<'a> {
    fn shared_encoded_len(&self) -> Result<usize, String> {
        let symbol_len = bytes_helper::fixed_bytes_len(&self.opening_symbol);
        let _ = u32::try_from(self.from_key.len())
            .map_err(|_| format!("MMOpenBatch from_key too large: {}", self.from_key.len()))?;

        // version + leg fields + symbol length + order/tick/time fields + two u32 lengths
        let fixed_len = 1usize + 41 + 1 + 1 + 12 + 12 + 8 + 8 + 4 + 4;
        fixed_len
            .checked_add(symbol_len)
            .and_then(|len| len.checked_add(self.from_key.len()))
            .ok_or_else(|| "MMOpenBatch encoded length overflow".to_string())
    }

    pub fn encoded_len(&self) -> Result<usize, String> {
        let _ = u32::try_from(self.levels.len())
            .map_err(|_| format!("MMOpenBatch has too many levels: {}", self.levels.len()))?;
        self.shared_encoded_len()?
            .checked_add(
                self.levels
                    .len()
                    .checked_mul(MM_OPEN_BATCH_LEVEL_ENCODED_LEN)
                    .ok_or_else(|| "MMOpenBatch level length overflow".to_string())?,
            )
            .ok_or_else(|| "MMOpenBatch encoded length overflow".to_string())
    }

    pub fn max_levels_for_encoded_len(&self, max_encoded_len: usize) -> usize {
        let Ok(shared_len) = self.shared_encoded_len() else {
            return 0;
        };
        if shared_len > max_encoded_len {
            return 0;
        }
        ((max_encoded_len - shared_len) / MM_OPEN_BATCH_LEVEL_ENCODED_LEN).min(u32::MAX as usize)
    }

    pub fn write_to(&self, buf: &mut BytesMut) -> Result<(), String> {
        if self.levels.is_empty() {
            return Err("MMOpenBatch must contain at least one level".to_string());
        }
        let level_count = u32::try_from(self.levels.len())
            .map_err(|_| format!("MMOpenBatch has too many levels: {}", self.levels.len()))?;
        let from_key_len = u32::try_from(self.from_key.len())
            .map_err(|_| format!("MMOpenBatch from_key too large: {}", self.from_key.len()))?;
        let _ = self.encoded_len()?;

        buf.put_u8(MM_OPEN_BATCH_VERSION);
        write_leg(buf, &self.opening_leg, &self.opening_symbol);
        buf.put_u8(self.order_type);
        buf.put_i64_le(self.price_tick_i64);
        buf.put_i32_le(self.price_tick_exp);
        buf.put_i64_le(self.amount_tick_i64);
        buf.put_i32_le(self.amount_tick_exp);
        buf.put_i64_le(self.exp_time);
        buf.put_i64_le(self.create_ts);
        buf.put_u32_le(level_count);
        buf.put_u32_le(from_key_len);
        buf.put_slice(self.from_key);
        for level in self.levels {
            buf.put_u8(level.side);
            buf.put_i64_le(level.price_count);
            buf.put_i64_le(level.amount_count);
            buf.put_f64_le(level.price_offset);
        }
        Ok(())
    }
}

impl<'a> MmOpenBatchCtxView<'a> {
    pub fn from_bytes(raw: &'a [u8]) -> Result<Self, String> {
        let mut offset = 0usize;
        let version = read_u8(raw, &mut offset, "MMOpenBatch version")?;
        if version != MM_OPEN_BATCH_VERSION {
            return Err(format!(
                "Unsupported MMOpenBatch version: {version}, expected {MM_OPEN_BATCH_VERSION}"
            ));
        }
        let (opening_leg, opening_symbol) =
            read_leg_ref(raw, &mut offset, true, "MMOpenBatch opening leg")?;
        let order_type = read_u8(raw, &mut offset, "MMOpenBatch order_type")?;
        let price_tick_i64 = read_i64_le(raw, &mut offset, "MMOpenBatch price_tick_i64")?;
        let price_tick_exp = read_i32_le(raw, &mut offset, "MMOpenBatch price_tick_exp")?;
        let amount_tick_i64 = read_i64_le(raw, &mut offset, "MMOpenBatch amount_tick_i64")?;
        let amount_tick_exp = read_i32_le(raw, &mut offset, "MMOpenBatch amount_tick_exp")?;
        let exp_time = read_i64_le(raw, &mut offset, "MMOpenBatch exp_time")?;
        let create_ts = read_i64_le(raw, &mut offset, "MMOpenBatch create_ts")?;
        let level_count = read_u32_le(raw, &mut offset, "MMOpenBatch level_count")? as usize;
        if level_count == 0 {
            return Err("MMOpenBatch must contain at least one level".to_string());
        }
        let from_key_len = read_u32_le(raw, &mut offset, "MMOpenBatch from_key_len")?;
        let from_key_len_usize = from_key_len as usize;
        if raw.len() < offset + from_key_len_usize {
            return Err(format!(
                "Not enough bytes for MMOpenBatch from_key: need {}, have {}",
                from_key_len_usize,
                raw.len().saturating_sub(offset)
            ));
        }
        let from_key = &raw[offset..offset + from_key_len_usize];
        offset += from_key_len_usize;

        let levels_len = level_count
            .checked_mul(MM_OPEN_BATCH_LEVEL_ENCODED_LEN)
            .ok_or_else(|| "MMOpenBatch level length overflow".to_string())?;
        if raw.len() != offset.saturating_add(levels_len) {
            return Err(format!(
                "MMOpenBatch length mismatch: expected {}, got {}",
                offset.saturating_add(levels_len),
                raw.len()
            ));
        }
        let levels_raw = &raw[offset..];

        Ok(Self {
            opening_leg,
            opening_symbol,
            order_type,
            price_tick_i64,
            price_tick_exp,
            amount_tick_i64,
            amount_tick_exp,
            exp_time,
            create_ts,
            from_key_len,
            from_key,
            level_count,
            levels_raw,
        })
    }

    pub fn get_order_type(&self) -> Option<OrderType> {
        OrderType::from_u8(self.order_type)
    }

    pub fn len(&self) -> usize {
        self.level_count
    }

    pub fn is_empty(&self) -> bool {
        self.level_count == 0
    }

    pub fn levels(&self) -> MmOpenBatchLevelIter<'a> {
        MmOpenBatchLevelIter {
            batch: *self,
            index: 0,
        }
    }
}

impl<'a> Iterator for MmOpenBatchLevelIter<'a> {
    type Item = MmOpenCtxView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.batch.level_count {
            return None;
        }
        let offset = self.index * MM_OPEN_BATCH_LEVEL_ENCODED_LEN;
        let raw = &self.batch.levels_raw[offset..offset + MM_OPEN_BATCH_LEVEL_ENCODED_LEN];
        self.index += 1;

        let side = raw[0];
        let price_count = i64::from_le_bytes(raw[1..9].try_into().expect("validated batch level"));
        let amount_count =
            i64::from_le_bytes(raw[9..17].try_into().expect("validated batch level"));
        let price_offset =
            f64::from_le_bytes(raw[17..25].try_into().expect("validated batch level"));
        Some(MmOpenCtxView {
            opening_leg: self.batch.opening_leg,
            opening_symbol: self.batch.opening_symbol,
            amount_qv: QuantizedValue::from_parts(
                self.batch.amount_tick_i64,
                self.batch.amount_tick_exp,
                amount_count,
            ),
            side,
            order_type: self.batch.order_type,
            price_qv: QuantizedValue::from_parts(
                self.batch.price_tick_i64,
                self.batch.price_tick_exp,
                price_count,
            ),
            exp_time: self.batch.exp_time,
            create_ts: self.batch.create_ts,
            price_offset,
            from_key_len: self.batch.from_key_len,
            from_key: self.batch.from_key,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batch.level_count.saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MmOpenBatchLevelIter<'_> {}

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

    fn from_bytes(bytes: Bytes) -> Result<Self, String> {
        Self::from_slice(bytes.as_ref())
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

    fn from_bytes(bytes: Bytes) -> Result<Self, String> {
        Self::from_slice(bytes.as_ref())
    }
}

impl ArbOpenCtx {
    pub fn from_slice(raw: &[u8]) -> Result<Self, String> {
        let view = ArbOpenCtxView::from_bytes(raw)?;
        Ok(Self {
            opening_leg: view.opening_leg,
            opening_symbol: view.opening_symbol,
            hedging_leg: view.hedging_leg,
            hedging_symbol: view.hedging_symbol,
            side: view.side,
            order_type: view.order_type,
            price_qv: view.price_qv,
            amount_qv: view.amount_qv,
            exp_time: view.exp_time,
            create_ts: view.create_ts,
            price_offset: view.price_offset,
            spread_rate: view.spread_rate,
            hedge_timeout_us: view.hedge_timeout_us,
            from_key_len: view.from_key.len() as u32,
            from_key: view.from_key.to_vec(),
        })
    }
}

impl MmOpenCtx {
    pub fn from_slice(raw: &[u8]) -> Result<Self, String> {
        let view = MmOpenCtxView::from_bytes(raw)?;
        Ok(Self {
            opening_leg: view.opening_leg,
            opening_symbol: view.opening_symbol,
            amount_qv: view.amount_qv,
            side: view.side,
            order_type: view.order_type,
            price_qv: view.price_qv,
            exp_time: view.exp_time,
            create_ts: view.create_ts,
            price_offset: view.price_offset,
            from_key_len: view.from_key.len() as u32,
            from_key: view.from_key.to_vec(),
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
        ctx.opening_leg =
            TradingLeg::new_with_qty(TradingVenue::BinanceMargin, 1.0, 11.0, 1.1, 12.0, 101);
        ctx.hedging_leg =
            TradingLeg::new_with_qty(TradingVenue::BinanceFutures, 2.0, 21.0, 2.1, 22.0, 202);
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
        assert_eq!(view.opening_leg.bid_qty0, 11.0);
        assert_eq!(view.opening_leg.ask_qty0, 12.0);
        assert_eq!(view.hedging_leg.ts, owned.hedging_leg.ts);
        assert_eq!(view.hedging_leg.bid_qty0, 21.0);
        assert_eq!(view.hedging_leg.ask_qty0, 22.0);
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
        ctx.opening_leg =
            TradingLeg::new_with_qty(TradingVenue::GateFutures, 3.0, 31.0, 3.1, 32.0, 303);
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
        assert_eq!(view.opening_leg.bid_qty0, 31.0);
        assert_eq!(view.opening_leg.ask_qty0, 32.0);
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

    #[test]
    fn mm_open_batch_view_yields_legacy_level_views_without_allocating_contexts() {
        let opening_leg =
            TradingLeg::new_with_qty(TradingVenue::GateFutures, 3.0, 31.0, 3.1, 32.0, 303);
        let opening_symbol = bytes_helper::fixed_bytes_from_str("ETHUSDT");
        let levels = [
            MmOpenBatchLevel {
                side: Side::Buy.to_u8(),
                price_count: 30_000,
                amount_count: 11,
                price_offset: 0.001,
            },
            MmOpenBatchLevel {
                side: Side::Sell.to_u8(),
                price_count: 31_000,
                amount_count: 12,
                price_offset: 0.002,
            },
        ];
        let batch = MmOpenBatchCtx {
            opening_leg,
            opening_symbol,
            order_type: OrderType::Limit.to_u8(),
            price_tick_i64: 1,
            price_tick_exp: -4,
            amount_tick_i64: 1,
            amount_tick_exp: -3,
            exp_time: 666,
            create_ts: 777,
            from_key: b"mm-batch",
            levels: &levels,
        };
        let mut encoded = BytesMut::new();
        batch.write_to(&mut encoded).expect("encode batch");
        assert_eq!(encoded.len(), batch.encoded_len().expect("encoded len"));

        let view = MmOpenBatchCtxView::from_bytes(&encoded).expect("decode batch");
        assert_eq!(view.len(), 2);
        assert_eq!(view.get_order_type(), Some(OrderType::Limit));
        assert_eq!(view.from_key, b"mm-batch");
        let decoded = view.levels().collect::<Vec<_>>();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].get_side(), Some(Side::Buy));
        assert_eq!(decoded[0].price_count(), 30_000);
        assert_eq!(decoded[0].amount_count(), 11);
        assert_eq!(decoded[0].price_offset, 0.001);
        assert_eq!(decoded[1].get_side(), Some(Side::Sell));
        assert_eq!(decoded[1].price_count(), 31_000);
        assert_eq!(decoded[1].amount_count(), 12);
        assert_eq!(decoded[1].from_key, b"mm-batch");
        assert_eq!(decoded[1].opening_leg.bid_qty0, 31.0);
        assert_eq!(decoded[1].get_opening_symbol(), "ETHUSDT");
    }

    #[test]
    fn mm_open_batch_level_matches_legacy_mm_open_context() {
        let mut legacy = MmOpenCtx::new();
        legacy.opening_leg =
            TradingLeg::new_with_qty(TradingVenue::OkexFutures, 9.0, 10.0, 9.1, 11.0, 123);
        legacy.set_opening_symbol("BTC-USDT-SWAP");
        legacy.set_side(Side::Sell);
        legacy.set_order_type(OrderType::Limit);
        legacy.price_qv = qv(5, -2, 182);
        legacy.amount_qv = qv(1, -3, 17);
        legacy.exp_time = 456;
        legacy.create_ts = 789;
        legacy.price_offset = 0.003;
        legacy.set_from_key(b"legacy-equivalent".to_vec());

        let (price_tick_i64, price_tick_exp) = legacy.price_qv.get_tick_parts();
        let (amount_tick_i64, amount_tick_exp) = legacy.amount_qv.get_tick_parts();
        let levels = [MmOpenBatchLevel {
            side: legacy.side,
            price_count: legacy.price_count(),
            amount_count: legacy.amount_count(),
            price_offset: legacy.price_offset,
        }];
        let batch = MmOpenBatchCtx {
            opening_leg: legacy.opening_leg,
            opening_symbol: legacy.opening_symbol,
            order_type: legacy.order_type,
            price_tick_i64,
            price_tick_exp,
            amount_tick_i64,
            amount_tick_exp,
            exp_time: legacy.exp_time,
            create_ts: legacy.create_ts,
            from_key: &legacy.from_key,
            levels: &levels,
        };
        let mut encoded = BytesMut::new();
        batch.write_to(&mut encoded).expect("encode batch");
        let batch_level = MmOpenBatchCtxView::from_bytes(&encoded)
            .expect("decode batch")
            .levels()
            .next()
            .expect("batch level");
        let legacy_bytes = legacy.to_bytes();
        let legacy_view = MmOpenCtxView::from_bytes(&legacy_bytes).expect("decode legacy");

        assert_eq!(batch_level.opening_leg.venue, legacy_view.opening_leg.venue);
        assert_eq!(batch_level.opening_leg.bid0, legacy_view.opening_leg.bid0);
        assert_eq!(
            batch_level.opening_leg.bid_qty0,
            legacy_view.opening_leg.bid_qty0
        );
        assert_eq!(batch_level.opening_leg.ask0, legacy_view.opening_leg.ask0);
        assert_eq!(
            batch_level.opening_leg.ask_qty0,
            legacy_view.opening_leg.ask_qty0
        );
        assert_eq!(batch_level.opening_leg.ts, legacy_view.opening_leg.ts);
        assert_eq!(batch_level.opening_symbol, legacy_view.opening_symbol);
        assert_eq!(batch_level.side, legacy_view.side);
        assert_eq!(batch_level.order_type, legacy_view.order_type);
        assert_eq!(batch_level.price_qv, legacy_view.price_qv);
        assert_eq!(batch_level.amount_qv, legacy_view.amount_qv);
        assert_eq!(batch_level.exp_time, legacy_view.exp_time);
        assert_eq!(batch_level.create_ts, legacy_view.create_ts);
        assert_eq!(batch_level.price_offset, legacy_view.price_offset);
        assert_eq!(batch_level.from_key, legacy_view.from_key);
    }

    #[test]
    fn mm_open_batch_capacity_respects_exact_payload_limit() {
        let levels = vec![
            MmOpenBatchLevel {
                side: Side::Buy.to_u8(),
                price_count: 1,
                amount_count: 1,
                price_offset: 0.0,
            };
            64
        ];
        let template = MmOpenBatchCtx {
            opening_leg: TradingLeg::new_with_qty(
                TradingVenue::BinanceFutures,
                1.0,
                2.0,
                1.1,
                3.0,
                4,
            ),
            opening_symbol: bytes_helper::fixed_bytes_from_str("BTCUSDT"),
            order_type: OrderType::Limit.to_u8(),
            price_tick_i64: 1,
            price_tick_exp: -2,
            amount_tick_i64: 1,
            amount_tick_exp: -4,
            exp_time: 5,
            create_ts: 6,
            from_key: b"batch-capacity",
            levels: &[],
        };
        let capacity = template.max_levels_for_encoded_len(1_000);
        assert!(capacity > 0);
        assert!(capacity < levels.len());

        let fitting = MmOpenBatchCtx {
            levels: &levels[..capacity],
            ..template
        };
        assert!(fitting.encoded_len().expect("fitting len") <= 1_000);
        let overflowing = MmOpenBatchCtx {
            levels: &levels[..capacity + 1],
            ..template
        };
        assert!(overflowing.encoded_len().expect("overflowing len") > 1_000);
    }

    #[test]
    fn mm_open_batch_view_rejects_invalid_version_and_lengths() {
        let levels = [MmOpenBatchLevel {
            side: Side::Buy.to_u8(),
            price_count: 1,
            amount_count: 1,
            price_offset: 0.0,
        }];
        let batch = MmOpenBatchCtx {
            opening_leg: TradingLeg::new_with_qty(
                TradingVenue::BinanceFutures,
                1.0,
                2.0,
                1.1,
                3.0,
                4,
            ),
            opening_symbol: bytes_helper::fixed_bytes_from_str("BTCUSDT"),
            order_type: OrderType::Limit.to_u8(),
            price_tick_i64: 1,
            price_tick_exp: -2,
            amount_tick_i64: 1,
            amount_tick_exp: -4,
            exp_time: 5,
            create_ts: 6,
            from_key: b"batch",
            levels: &levels,
        };
        let mut encoded = BytesMut::new();
        batch.write_to(&mut encoded).expect("encode batch");

        let mut invalid_version = encoded.to_vec();
        invalid_version[0] = MM_OPEN_BATCH_VERSION + 1;
        assert!(MmOpenBatchCtxView::from_bytes(&invalid_version).is_err());

        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(MmOpenBatchCtxView::from_bytes(&trailing).is_err());
        assert!(MmOpenBatchCtxView::from_bytes(&encoded[..encoded.len() - 1]).is_err());
    }
}
