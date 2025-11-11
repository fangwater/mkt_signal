use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const MARGIN_EXECUTION_RECORD_CHANNEL: &str = "account_execution/binance_margin";
pub const UM_EXECUTION_RECORD_CHANNEL: &str = "account_execution/binance_um";

#[derive(Debug, Clone)]
pub struct ExecutionRecordMessage {
    pub recv_ts_us: i64,
    pub event_time: i64,
    pub transaction_time: i64,
    pub order_id: i64,
    pub trade_id: i64,
    pub order_creation_time: i64,
    pub working_time: i64,
    pub update_id: i64,
    pub symbol: String,
    pub client_order_id: i64,
    pub client_order_id_str: Option<String>,
    pub strategy_id: i32,
    pub side: char,
    pub is_maker: bool,
    pub is_working: bool,
    pub price: f64,
    pub quantity: f64,
    pub last_executed_quantity: f64,
    pub cumulative_filled_quantity: f64,
    pub last_executed_price: f64,
    pub commission_amount: f64,
    pub commission_asset: String,
    pub cumulative_quote: f64,
    pub last_quote: f64,
    pub quote_order_quantity: f64,
    pub order_type: String,
    pub time_in_force: String,
    pub execution_type: String,
    pub order_status: String,
}

impl ExecutionRecordMessage {
    pub fn new(
        recv_ts_us: i64,
        event_time: i64,
        transaction_time: i64,
        order_id: i64,
        trade_id: i64,
        order_creation_time: i64,
        working_time: i64,
        update_id: i64,
        symbol: String,
        client_order_id: i64,
        client_order_id_str: Option<String>,
        strategy_id: i32,
        side: char,
        is_maker: bool,
        is_working: bool,
        price: f64,
        quantity: f64,
        last_executed_quantity: f64,
        cumulative_filled_quantity: f64,
        last_executed_price: f64,
        commission_amount: f64,
        commission_asset: String,
        cumulative_quote: f64,
        last_quote: f64,
        quote_order_quantity: f64,
        order_type: String,
        time_in_force: String,
        execution_type: String,
        order_status: String,
    ) -> Self {
        Self {
            recv_ts_us,
            event_time,
            transaction_time,
            order_id,
            trade_id,
            order_creation_time,
            working_time,
            update_id,
            symbol,
            client_order_id,
            client_order_id_str,
            strategy_id,
            side,
            is_maker,
            is_working,
            price,
            quantity,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            commission_amount,
            commission_asset,
            cumulative_quote,
            last_quote,
            quote_order_quantity,
            order_type,
            time_in_force,
            execution_type,
            order_status,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(512);
        buf.put_i64_le(self.recv_ts_us);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.transaction_time);
        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.trade_id);
        buf.put_i64_le(self.order_creation_time);
        buf.put_i64_le(self.working_time);
        buf.put_i64_le(self.update_id);

        put_string(&mut buf, &self.symbol);
        buf.put_i64_le(self.client_order_id);
        put_opt_string(&mut buf, self.client_order_id_str.as_ref());
        buf.put_i32_le(self.strategy_id);

        buf.put_i8(self.side as i8);
        buf.put_u8(self.is_maker as u8);
        buf.put_u8(self.is_working as u8);
        buf.put_f64_le(self.price);
        buf.put_f64_le(self.quantity);
        buf.put_f64_le(self.last_executed_quantity);
        buf.put_f64_le(self.cumulative_filled_quantity);
        buf.put_f64_le(self.last_executed_price);
        buf.put_f64_le(self.commission_amount);
        put_string(&mut buf, &self.commission_asset);
        buf.put_f64_le(self.cumulative_quote);
        buf.put_f64_le(self.last_quote);
        buf.put_f64_le(self.quote_order_quantity);
        put_string(&mut buf, &self.order_type);
        put_string(&mut buf, &self.time_in_force);
        put_string(&mut buf, &self.execution_type);
        put_string(&mut buf, &self.order_status);

        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self> {
        if bytes.len() < 15 * 8 {
            return Err(anyhow!("execution record payload too short"));
        }

        let recv_ts_us = bytes.get_i64_le();
        let event_time = bytes.get_i64_le();
        let transaction_time = bytes.get_i64_le();
        let order_id = bytes.get_i64_le();
        let trade_id = bytes.get_i64_le();
        let order_creation_time = bytes.get_i64_le();
        let working_time = bytes.get_i64_le();
        let update_id = bytes.get_i64_le();

        let symbol = take_string(&mut bytes)?;
        let client_order_id = bytes.get_i64_le();
        let client_order_id_str = take_opt_string(&mut bytes)?;
        let strategy_id = bytes.get_i32_le();

        let side = bytes.get_i8() as u8 as char;
        let is_maker = bytes.get_u8() != 0;
        let is_working = bytes.get_u8() != 0;
        let price = bytes.get_f64_le();
        let quantity = bytes.get_f64_le();
        let last_executed_quantity = bytes.get_f64_le();
        let cumulative_filled_quantity = bytes.get_f64_le();
        let last_executed_price = bytes.get_f64_le();
        let commission_amount = bytes.get_f64_le();
        let commission_asset = take_string(&mut bytes)?;
        let cumulative_quote = bytes.get_f64_le();
        let last_quote = bytes.get_f64_le();
        let quote_order_quantity = bytes.get_f64_le();
        let order_type = take_string(&mut bytes)?;
        let time_in_force = take_string(&mut bytes)?;
        let execution_type = take_string(&mut bytes)?;
        let order_status = take_string(&mut bytes)?;

        Ok(Self {
            recv_ts_us,
            event_time,
            transaction_time,
            order_id,
            trade_id,
            order_creation_time,
            working_time,
            update_id,
            symbol,
            client_order_id,
            client_order_id_str,
            strategy_id,
            side,
            is_maker,
            is_working,
            price,
            quantity,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            commission_amount,
            commission_asset,
            cumulative_quote,
            last_quote,
            quote_order_quantity,
            order_type,
            time_in_force,
            execution_type,
            order_status,
        })
    }
}

fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_u32_le(value.len() as u32);
    buf.put_slice(value.as_bytes());
}

fn put_opt_string(buf: &mut BytesMut, value: Option<&String>) {
    if let Some(v) = value {
        buf.put_u32_le(v.len() as u32);
        buf.put_slice(v.as_bytes());
    } else {
        buf.put_u32_le(0);
    }
}

fn take_string(bytes: &mut Bytes) -> Result<String> {
    if bytes.remaining() < 4 {
        return Err(anyhow!("not enough bytes for string length"));
    }
    let len = bytes.get_u32_le() as usize;
    if bytes.remaining() < len {
        return Err(anyhow!("string length mismatch"));
    }
    let data = bytes.copy_to_bytes(len);
    Ok(String::from_utf8(data.to_vec())?)
}

fn take_opt_string(bytes: &mut Bytes) -> Result<Option<String>> {
    if bytes.remaining() < 4 {
        return Err(anyhow!("not enough bytes for opt string length"));
    }
    let len = bytes.get_u32_le() as usize;
    if len == 0 {
        return Ok(None);
    }
    if bytes.remaining() < len {
        return Err(anyhow!("opt string length mismatch"));
    }
    let data = bytes.copy_to_bytes(len);
    Ok(Some(String::from_utf8(data.to_vec())?))
}
