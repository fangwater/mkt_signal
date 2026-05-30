use crate::common::tick_math::QuantizedValue;
use crate::pre_trade::order_manager::Side;
use crate::signal::common::{bytes_helper, SignalBytes, TradingLeg, TradingVenue};
use bytes::{Buf, BufMut, Bytes, BytesMut};

const QV_BYTES_LEN: usize = 8 + 4 + 8;

fn fixed_symbol_to_string(symbol: &[u8; 32]) -> String {
    let end = symbol.iter().position(|&b| b == 0).unwrap_or(32);
    String::from_utf8_lossy(&symbol[..end]).to_string()
}

fn set_fixed_symbol(dst: &mut [u8; 32], symbol: &str) {
    *dst = [0u8; 32];
    let bytes = symbol.as_bytes();
    let len = bytes.len().min(32);
    dst[..len].copy_from_slice(&bytes[..len]);
}

fn write_qv(buf: &mut BytesMut, qv: &QuantizedValue) {
    let (tick_i64, tick_exp) = qv.get_tick_parts();
    buf.put_i64_le(tick_i64);
    buf.put_i32_le(tick_exp);
    buf.put_i64_le(qv.get_count());
}

fn read_qv(bytes: &mut Bytes, field: &str) -> Result<QuantizedValue, String> {
    if bytes.remaining() < QV_BYTES_LEN {
        return Err(format!("Not enough bytes for {field}"));
    }
    let tick_i64 = bytes.get_i64_le();
    let tick_exp = bytes.get_i32_le();
    let count = bytes.get_i64_le();
    Ok(QuantizedValue::from_parts(tick_i64, tick_exp, count))
}

#[derive(Debug, Clone)]
pub struct ExecRequestCtx {
    pub exec_leg: TradingLeg,
    pub exec_symbol: [u8; 32],
    pub side: u8,
    pub amount_qv: QuantizedValue,
    pub close_ts: i64,
    pub create_ts: i64,
    pub price_hint: f64,
    pub from_key_len: u32,
    pub from_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ExecCtx {
    pub strategy_id: i32,
    pub exec_side: u8,
    pub exec_leg: TradingLeg,
    pub exec_symbol: [u8; 32],
    pub price_qv: QuantizedValue,
    pub amount_qv: QuantizedValue,
    pub price_offset: f64,
    pub signal_ts: i64,
    pub exp_time: i64,
    pub request_seq: u64,
    pub from_key_len: u32,
    pub from_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ExecPositionTargetCtx {
    pub exec_venue: u8,
    pub exec_symbol: [u8; 32],
    pub target_qty: f64,
    pub generation_time: i64,
    pub from_key_len: u32,
    pub from_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ExecSignalQueryMsg {
    pub strategy_id: i32,
    pub exec_venue: u8,
    pub symbol: [u8; 32],
    pub due_exec_qty: f64,
    pub pending_exec_qty: f64,
    pub symbol_exposure_u: f64,
    pub weighted_exec_price: f64,
    pub request_seq: u64,
}

impl Default for ExecRequestCtx {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for ExecPositionTargetCtx {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecRequestCtx {
    pub fn new() -> Self {
        Self {
            exec_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
                ts: 0,
            },
            exec_symbol: [0u8; 32],
            side: 0,
            amount_qv: QuantizedValue::zero(),
            close_ts: 0,
            create_ts: 0,
            price_hint: 0.0,
            from_key_len: 0,
            from_key: Vec::new(),
        }
    }

    pub fn set_exec_symbol(&mut self, symbol: &str) {
        set_fixed_symbol(&mut self.exec_symbol, symbol);
    }

    pub fn get_exec_symbol(&self) -> String {
        fixed_symbol_to_string(&self.exec_symbol)
    }

    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.side)
    }

    pub fn set_side(&mut self, side: Side) {
        self.side = side.to_u8();
    }

    pub fn set_amount_with_tick_floor(&mut self, amount: f64, preferred_tick: f64) -> bool {
        let fallback = !(preferred_tick.is_finite() && preferred_tick > 0.0);
        self.amount_qv = QuantizedValue::encode_floor(amount, preferred_tick)
            .unwrap_or_else(QuantizedValue::zero);
        fallback
    }

    pub fn amount_value(&self) -> f64 {
        self.amount_qv.get_val()
    }

    pub fn set_from_key(&mut self, from_key: Vec<u8>) {
        self.from_key_len = from_key.len() as u32;
        self.from_key = from_key;
    }
}

impl ExecPositionTargetCtx {
    pub fn new() -> Self {
        Self {
            exec_venue: 0,
            exec_symbol: [0u8; 32],
            target_qty: 0.0,
            generation_time: 0,
            from_key_len: 0,
            from_key: Vec::new(),
        }
    }

    pub fn set_exec_symbol(&mut self, symbol: &str) {
        set_fixed_symbol(&mut self.exec_symbol, symbol);
    }

    pub fn get_exec_symbol(&self) -> String {
        fixed_symbol_to_string(&self.exec_symbol)
    }

    pub fn set_exec_venue(&mut self, venue: TradingVenue) {
        self.exec_venue = venue.to_u8();
    }

    pub fn get_exec_venue(&self) -> Option<TradingVenue> {
        TradingVenue::from_u8(self.exec_venue)
    }

    pub fn set_from_key(&mut self, from_key: Vec<u8>) {
        self.from_key_len = from_key.len() as u32;
        self.from_key = from_key;
    }
}

impl Default for ExecCtx {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecCtx {
    pub fn new() -> Self {
        Self {
            strategy_id: 0,
            exec_side: 0,
            exec_leg: TradingLeg {
                venue: 0,
                bid0: 0.0,
                ask0: 0.0,
                ts: 0,
            },
            exec_symbol: [0u8; 32],
            price_qv: QuantizedValue::zero(),
            amount_qv: QuantizedValue::zero(),
            price_offset: 0.0,
            signal_ts: 0,
            exp_time: 0,
            request_seq: 0,
            from_key_len: 0,
            from_key: Vec::new(),
        }
    }

    pub fn set_exec_symbol(&mut self, symbol: &str) {
        set_fixed_symbol(&mut self.exec_symbol, symbol);
    }

    pub fn get_exec_symbol(&self) -> String {
        fixed_symbol_to_string(&self.exec_symbol)
    }

    pub fn get_side(&self) -> Option<Side> {
        Side::from_u8(self.exec_side)
    }

    pub fn set_side(&mut self, side: Side) {
        self.exec_side = side.to_u8();
    }

    pub fn set_from_key(&mut self, from_key: Vec<u8>) {
        self.from_key_len = from_key.len() as u32;
        self.from_key = from_key;
    }

    pub fn price_value(&self) -> f64 {
        self.price_qv.get_val()
    }

    pub fn amount_value(&self) -> f64 {
        self.amount_qv.get_val()
    }

    pub fn is_taker(&self) -> bool {
        self.exp_time == 0
    }
}

impl ExecSignalQueryMsg {
    pub fn new(
        strategy_id: i32,
        exec_venue: TradingVenue,
        symbol: &str,
        due_exec_qty: f64,
        pending_exec_qty: f64,
        symbol_exposure_u: f64,
        weighted_exec_price: f64,
        request_seq: u64,
    ) -> Self {
        let mut symbol_bytes = [0u8; 32];
        let bytes = symbol.as_bytes();
        let len = bytes.len().min(32);
        symbol_bytes[..len].copy_from_slice(&bytes[..len]);
        Self {
            strategy_id,
            exec_venue: exec_venue as u8,
            symbol: symbol_bytes,
            due_exec_qty,
            pending_exec_qty,
            symbol_exposure_u,
            weighted_exec_price,
            request_seq,
        }
    }

    pub fn get_symbol(&self) -> String {
        fixed_symbol_to_string(&self.symbol)
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i32_le(self.strategy_id);
        buf.put_u8(self.exec_venue);
        bytes_helper::write_fixed_bytes(&mut buf, &self.symbol);
        buf.put_f64_le(self.due_exec_qty);
        buf.put_f64_le(self.pending_exec_qty);
        buf.put_f64_le(self.symbol_exposure_u);
        buf.put_f64_le(self.weighted_exec_price);
        buf.put_u64_le(self.request_seq);
        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 4 + 1 {
            return Err("insufficient bytes for ExecSignalQueryMsg header".to_string());
        }
        let strategy_id = bytes.get_i32_le();
        let exec_venue = bytes.get_u8();
        let symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
        if bytes.remaining() < 8 * 5 {
            return Err(
                "insufficient bytes for due/pending/symbol_exposure/weighted_price/request_seq"
                    .to_string(),
            );
        }
        let due_exec_qty = bytes.get_f64_le();
        let pending_exec_qty = bytes.get_f64_le();
        let symbol_exposure_u = bytes.get_f64_le();
        let weighted_exec_price = bytes.get_f64_le();
        let request_seq = bytes.get_u64_le();
        Ok(Self {
            strategy_id,
            exec_venue,
            symbol,
            due_exec_qty,
            pending_exec_qty,
            symbol_exposure_u,
            weighted_exec_price,
            request_seq,
        })
    }
}

impl SignalBytes for ExecPositionTargetCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(self.exec_venue);
        bytes_helper::write_fixed_bytes(&mut buf, &self.exec_symbol);
        buf.put_f64_le(self.target_qty);
        buf.put_i64_le(self.generation_time);
        buf.put_u32_le(self.from_key.len() as u32);
        buf.put_slice(&self.from_key);
        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 1 {
            return Err("Not enough bytes for ExecPositionTargetCtx venue".to_string());
        }
        let exec_venue = bytes.get_u8();
        let exec_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
        if bytes.remaining() < 8 + 8 + 4 {
            return Err("Not enough bytes for ExecPositionTargetCtx tail".to_string());
        }
        let target_qty = bytes.get_f64_le();
        let generation_time = bytes.get_i64_le();
        let from_key_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < from_key_len {
            return Err(format!(
                "Not enough bytes for ExecPositionTargetCtx from_key: need {}, have {}",
                from_key_len,
                bytes.remaining()
            ));
        }
        let from_key = bytes.copy_to_bytes(from_key_len).to_vec();
        if bytes.remaining() != 0 {
            return Err("Unexpected trailing bytes for ExecPositionTargetCtx".to_string());
        }
        Ok(Self {
            exec_venue,
            exec_symbol,
            target_qty,
            generation_time,
            from_key_len: from_key.len() as u32,
            from_key,
        })
    }
}

impl SignalBytes for ExecRequestCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(self.exec_leg.venue);
        buf.put_f64_le(self.exec_leg.bid0);
        buf.put_f64_le(self.exec_leg.ask0);
        buf.put_i64_le(self.exec_leg.ts);
        bytes_helper::write_fixed_bytes(&mut buf, &self.exec_symbol);
        buf.put_u8(self.side);
        write_qv(&mut buf, &self.amount_qv);
        buf.put_i64_le(self.close_ts);
        buf.put_i64_le(self.create_ts);
        buf.put_f64_le(self.price_hint);
        buf.put_u32_le(self.from_key.len() as u32);
        buf.put_slice(&self.from_key);
        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 1 + 8 + 8 + 8 {
            return Err("Not enough bytes for ExecRequestCtx leg".to_string());
        }
        let venue = bytes.get_u8();
        let bid0 = bytes.get_f64_le();
        let ask0 = bytes.get_f64_le();
        let ts = bytes.get_i64_le();
        let exec_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
        if bytes.remaining() < 1 {
            return Err("Not enough bytes for ExecRequestCtx side".to_string());
        }
        let side = bytes.get_u8();
        let amount_qv = read_qv(&mut bytes, "ExecRequestCtx amount_qv")?;
        if bytes.remaining() < 8 + 8 + 8 + 4 {
            return Err("Not enough bytes for ExecRequestCtx tail".to_string());
        }
        let close_ts = bytes.get_i64_le();
        let create_ts = bytes.get_i64_le();
        let price_hint = bytes.get_f64_le();
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
            return Err("Unexpected trailing bytes for ExecRequestCtx".to_string());
        }
        Ok(Self {
            exec_leg: TradingLeg {
                venue,
                bid0,
                ask0,
                ts,
            },
            exec_symbol,
            side,
            amount_qv,
            close_ts,
            create_ts,
            price_hint,
            from_key_len: from_key.len() as u32,
            from_key,
        })
    }
}

impl SignalBytes for ExecCtx {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i32_le(self.strategy_id);
        buf.put_u8(self.exec_side);
        buf.put_u8(self.exec_leg.venue);
        buf.put_f64_le(self.exec_leg.bid0);
        buf.put_f64_le(self.exec_leg.ask0);
        buf.put_i64_le(self.exec_leg.ts);
        bytes_helper::write_fixed_bytes(&mut buf, &self.exec_symbol);
        write_qv(&mut buf, &self.price_qv);
        write_qv(&mut buf, &self.amount_qv);
        buf.put_f64_le(self.price_offset);
        buf.put_i64_le(self.signal_ts);
        buf.put_i64_le(self.exp_time);
        buf.put_u64_le(self.request_seq);
        buf.put_u32_le(self.from_key.len() as u32);
        buf.put_slice(&self.from_key);
        buf.freeze()
    }

    fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 4 + 1 + 1 + 8 + 8 + 8 {
            return Err("Not enough bytes for ExecCtx basic fields".to_string());
        }
        let strategy_id = bytes.get_i32_le();
        let exec_side = bytes.get_u8();
        let venue = bytes.get_u8();
        let bid0 = bytes.get_f64_le();
        let ask0 = bytes.get_f64_le();
        let ts = bytes.get_i64_le();
        let exec_symbol = bytes_helper::read_fixed_bytes(&mut bytes)?;
        let price_qv = read_qv(&mut bytes, "ExecCtx price_qv")?;
        let amount_qv = read_qv(&mut bytes, "ExecCtx amount_qv")?;
        if bytes.remaining() < 8 + 8 + 8 + 8 + 4 {
            return Err("Not enough bytes for ExecCtx tail".to_string());
        }
        let price_offset = bytes.get_f64_le();
        let signal_ts = bytes.get_i64_le();
        let exp_time = bytes.get_i64_le();
        let request_seq = bytes.get_u64_le();
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
            return Err("Unexpected trailing bytes for ExecCtx".to_string());
        }
        Ok(Self {
            strategy_id,
            exec_side,
            exec_leg: TradingLeg {
                venue,
                bid0,
                ask0,
                ts,
            },
            exec_symbol,
            price_qv,
            amount_qv,
            price_offset,
            signal_ts,
            exp_time,
            request_seq,
            from_key_len: from_key.len() as u32,
            from_key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecCtx, ExecPositionTargetCtx, ExecRequestCtx, ExecSignalQueryMsg};
    use crate::common::tick_math::QuantizedValue;
    use crate::pre_trade::order_manager::Side;
    use crate::signal::common::{SignalBytes, TradingLeg, TradingVenue};

    #[test]
    fn exec_request_roundtrip() {
        let mut ctx = ExecRequestCtx::new();
        ctx.exec_leg = TradingLeg::new(TradingVenue::BinanceFutures, 100.0, 100.1, 9);
        ctx.set_exec_symbol("BTCUSDT");
        ctx.set_side(Side::Buy);
        ctx.amount_qv = QuantizedValue::from_parts(1, -3, 250);
        ctx.close_ts = 123;
        ctx.create_ts = 99;
        ctx.price_hint = 100.05;
        ctx.set_from_key(b"exec_req".to_vec());
        let parsed = ExecRequestCtx::from_bytes(ctx.to_bytes()).unwrap();
        assert_eq!(parsed.get_exec_symbol(), "BTCUSDT");
        assert_eq!(parsed.get_side(), Some(Side::Buy));
        assert_eq!(parsed.amount_qv.get_count(), 250);
        assert_eq!(parsed.close_ts, 123);
        assert_eq!(parsed.from_key, b"exec_req");
    }

    #[test]
    fn exec_position_target_roundtrip() {
        let mut ctx = ExecPositionTargetCtx::new();
        ctx.set_exec_venue(TradingVenue::BinanceFutures);
        ctx.set_exec_symbol("BTCUSDT");
        ctx.target_qty = -0.25;
        ctx.generation_time = 123;
        ctx.set_from_key(b"target_pos".to_vec());
        let parsed = ExecPositionTargetCtx::from_bytes(ctx.to_bytes()).unwrap();
        assert_eq!(parsed.get_exec_venue(), Some(TradingVenue::BinanceFutures));
        assert_eq!(parsed.get_exec_symbol(), "BTCUSDT");
        assert_eq!(parsed.target_qty, -0.25);
        assert_eq!(parsed.generation_time, 123);
        assert_eq!(parsed.from_key, b"target_pos");
    }

    #[test]
    fn exec_reply_roundtrip() {
        let mut ctx = ExecCtx::new();
        ctx.strategy_id = 42;
        ctx.exec_leg = TradingLeg::new(TradingVenue::BinanceFutures, 100.0, 100.1, 9);
        ctx.set_exec_symbol("ETHUSDT");
        ctx.set_side(Side::Sell);
        ctx.price_qv = QuantizedValue::from_parts(1, -2, 9999);
        ctx.amount_qv = QuantizedValue::from_parts(1, -3, 500);
        ctx.price_offset = 0.001;
        ctx.signal_ts = 1;
        ctx.exp_time = 2;
        ctx.request_seq = 7;
        ctx.set_from_key(b"exec".to_vec());
        let parsed = ExecCtx::from_bytes(ctx.to_bytes()).unwrap();
        assert_eq!(parsed.strategy_id, 42);
        assert_eq!(parsed.get_exec_symbol(), "ETHUSDT");
        assert_eq!(parsed.get_side(), Some(Side::Sell));
        assert_eq!(parsed.request_seq, 7);
    }

    #[test]
    fn exec_query_roundtrip() {
        let msg = ExecSignalQueryMsg::new(
            42,
            TradingVenue::BinanceFutures,
            "BTCUSDT",
            1.5,
            2.0,
            1000.0,
            101.0,
            9,
        );
        let parsed = ExecSignalQueryMsg::from_bytes(msg.to_bytes()).unwrap();
        assert_eq!(parsed.strategy_id, 42);
        assert_eq!(parsed.get_symbol(), "BTCUSDT");
        assert_eq!(parsed.exec_venue, TradingVenue::BinanceFutures as u8);
        assert_eq!(parsed.request_seq, 9);
    }
}
