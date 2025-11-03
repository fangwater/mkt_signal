use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::pre_trade::order_manager::Side;

#[derive(Debug, Clone)]
pub struct ReqBinSingleForwardArbHedgeMM {
    pub strategy_id: i32,
    pub client_order_id: i64,
    pub event_time: i64,
    pub symbol: String,
    pub cumulative_filled_qty: f64,
    pub last_executed_qty: f64,
    pub order_quantity: f64,
    pub hedge_side: Side,
}

impl ReqBinSingleForwardArbHedgeMM {
    pub fn new(
        strategy_id: i32,
        client_order_id: i64,
        event_time: i64,
        symbol: String,
        cumulative_filled_qty: f64,
        last_executed_qty: f64,
        order_quantity: f64,
        hedge_side: Side,
    ) -> Self {
        Self {
            strategy_id,
            client_order_id,
            event_time,
            symbol,
            cumulative_filled_qty,
            last_executed_qty,
            order_quantity,
            hedge_side,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i32_le(self.strategy_id);
        buf.put_i64_le(self.client_order_id);
        buf.put_i64_le(self.event_time);
        buf.put_u32_le(self.symbol.len() as u32);
        buf.put_slice(self.symbol.as_bytes());
        buf.put_f64_le(self.cumulative_filled_qty);
        buf.put_f64_le(self.last_executed_qty);
        buf.put_f64_le(self.order_quantity);
        buf.put_u8(self.hedge_side.to_u8());
        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self, String> {
        if bytes.remaining() < 4 {
            return Err("insufficient bytes for strategy_id".into());
        }
        let strategy_id = bytes.get_i32_le();

        if bytes.remaining() < 8 {
            return Err("insufficient bytes for client_order_id".into());
        }
        let client_order_id = bytes.get_i64_le();

        if bytes.remaining() < 8 {
            return Err("insufficient bytes for event_time".into());
        }
        let event_time = bytes.get_i64_le();

        if bytes.remaining() < 4 {
            return Err("insufficient bytes for symbol length".into());
        }
        let symbol_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < symbol_len + 8 * 3 {
            return Err("insufficient bytes for symbol or quantities".into());
        }
        let symbol = String::from_utf8(bytes.copy_to_bytes(symbol_len).to_vec())
            .map_err(|e| format!("invalid symbol utf8: {e}"))?;

        let cumulative_filled_qty = bytes.get_f64_le();
        let last_executed_qty = bytes.get_f64_le();
        let order_quantity = bytes.get_f64_le();
        if bytes.remaining() < 1 {
            return Err("insufficient bytes for hedge_side".into());
        }
        let hedge_side =
            Side::from_u8(bytes.get_u8()).ok_or_else(|| "invalid hedge_side value".to_string())?;

        Ok(Self {
            strategy_id,
            client_order_id,
            event_time,
            symbol,
            cumulative_filled_qty,
            last_executed_qty,
            order_quantity,
            hedge_side,
        })
    }
}
