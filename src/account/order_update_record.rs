use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const UM_ORDER_UPDATE_RECORD_CHANNEL: &str = "account_execution/binance_um";

#[derive(Debug, Clone)]
pub struct OrderUpdateRecordMessage {
    pub recv_ts_us: i64,
    pub account_recv_ts_us: i64,
    pub event_time: i64,
    pub transaction_time: i64,
    pub order_id: i64,
    pub trade_id: i64,
    pub strategy_id: i64,
    pub derived_strategy_id: i64,
    pub symbol: String,
    pub client_order_id: i64,
    pub client_order_id_str: Option<String>,
    pub side: char,
    pub position_side: char,
    pub is_maker: bool,
    pub reduce_only: bool,
    pub price: f64,
    pub quantity: f64,
    pub average_price: f64,
    pub stop_price: f64,
    pub last_executed_quantity: f64,
    pub cumulative_filled_quantity: f64,
    pub last_executed_price: f64,
    pub commission_amount: f64,
    pub buy_notional: f64,
    pub sell_notional: f64,
    pub realized_profit: f64,
    pub order_type: String,
    pub time_in_force: String,
    pub execution_type: String,
    pub order_status: String,
    pub commission_asset: String,
    pub strategy_type: String,
    pub business_unit: String,
}

impl OrderUpdateRecordMessage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        recv_ts_us: i64,
        account_recv_ts_us: i64,
        event_time: i64,
        transaction_time: i64,
        order_id: i64,
        trade_id: i64,
        strategy_id: i64,
        derived_strategy_id: i64,
        symbol: String,
        client_order_id: i64,
        client_order_id_str: Option<String>,
        side: char,
        position_side: char,
        is_maker: bool,
        reduce_only: bool,
        price: f64,
        quantity: f64,
        average_price: f64,
        stop_price: f64,
        last_executed_quantity: f64,
        cumulative_filled_quantity: f64,
        last_executed_price: f64,
        commission_amount: f64,
        buy_notional: f64,
        sell_notional: f64,
        realized_profit: f64,
        order_type: String,
        time_in_force: String,
        execution_type: String,
        order_status: String,
        commission_asset: String,
        strategy_type: String,
        business_unit: String,
    ) -> Self {
        Self {
            recv_ts_us,
            account_recv_ts_us,
            event_time,
            transaction_time,
            order_id,
            trade_id,
            strategy_id,
            derived_strategy_id,
            symbol,
            client_order_id,
            client_order_id_str,
            side,
            position_side,
            is_maker,
            reduce_only,
            price,
            quantity,
            average_price,
            stop_price,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            commission_amount,
            buy_notional,
            sell_notional,
            realized_profit,
            order_type,
            time_in_force,
            execution_type,
            order_status,
            commission_asset,
            strategy_type,
            business_unit,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(1024);
        buf.put_i64_le(self.recv_ts_us);
        buf.put_i64_le(self.account_recv_ts_us);
        buf.put_i64_le(self.event_time);
        buf.put_i64_le(self.transaction_time);
        buf.put_i64_le(self.order_id);
        buf.put_i64_le(self.trade_id);
        buf.put_i64_le(self.strategy_id);
        buf.put_i64_le(self.derived_strategy_id);

        put_string(&mut buf, &self.symbol);
        buf.put_i64_le(self.client_order_id);
        put_opt_string(&mut buf, self.client_order_id_str.as_ref());

        buf.put_i8(self.side as i8);
        buf.put_i8(self.position_side as i8);
        buf.put_u8(self.is_maker as u8);
        buf.put_u8(self.reduce_only as u8);

        buf.put_f64_le(self.price);
        buf.put_f64_le(self.quantity);
        buf.put_f64_le(self.average_price);
        buf.put_f64_le(self.stop_price);
        buf.put_f64_le(self.last_executed_quantity);
        buf.put_f64_le(self.cumulative_filled_quantity);
        buf.put_f64_le(self.last_executed_price);
        buf.put_f64_le(self.commission_amount);
        buf.put_f64_le(self.buy_notional);
        buf.put_f64_le(self.sell_notional);
        buf.put_f64_le(self.realized_profit);

        put_string(&mut buf, &self.order_type);
        put_string(&mut buf, &self.time_in_force);
        put_string(&mut buf, &self.execution_type);
        put_string(&mut buf, &self.order_status);
        put_string(&mut buf, &self.commission_asset);
        put_string(&mut buf, &self.strategy_type);
        put_string(&mut buf, &self.business_unit);

        buf.freeze()
    }

    pub fn from_bytes(mut bytes: Bytes) -> Result<Self> {
        if bytes.len() < 8 * 8 {
            return Err(anyhow!("order update payload too short"));
        }

        let recv_ts_us = bytes.get_i64_le();
        let account_recv_ts_us = bytes.get_i64_le();
        let event_time = bytes.get_i64_le();
        let transaction_time = bytes.get_i64_le();
        let order_id = bytes.get_i64_le();
        let trade_id = bytes.get_i64_le();
        let strategy_id = bytes.get_i64_le();
        let derived_strategy_id = bytes.get_i64_le();

        let symbol = take_string(&mut bytes)?;
        let client_order_id = bytes.get_i64_le();
        let client_order_id_str = take_opt_string(&mut bytes)?;

        let side = bytes.get_i8() as u8 as char;
        let position_side = bytes.get_i8() as u8 as char;
        let is_maker = bytes.get_u8() != 0;
        let reduce_only = bytes.get_u8() != 0;

        let price = bytes.get_f64_le();
        let quantity = bytes.get_f64_le();
        let average_price = bytes.get_f64_le();
        let stop_price = bytes.get_f64_le();
        let last_executed_quantity = bytes.get_f64_le();
        let cumulative_filled_quantity = bytes.get_f64_le();
        let last_executed_price = bytes.get_f64_le();
        let commission_amount = bytes.get_f64_le();
        let buy_notional = bytes.get_f64_le();
        let sell_notional = bytes.get_f64_le();
        let realized_profit = bytes.get_f64_le();

        let order_type = take_string(&mut bytes)?;
        let time_in_force = take_string(&mut bytes)?;
        let execution_type = take_string(&mut bytes)?;
        let order_status = take_string(&mut bytes)?;
        let commission_asset = take_string(&mut bytes)?;
        let strategy_type = take_string(&mut bytes)?;
        let business_unit = take_string(&mut bytes)?;

        Ok(Self {
            recv_ts_us,
            account_recv_ts_us,
            event_time,
            transaction_time,
            order_id,
            trade_id,
            strategy_id,
            derived_strategy_id,
            symbol,
            client_order_id,
            client_order_id_str,
            side,
            position_side,
            is_maker,
            reduce_only,
            price,
            quantity,
            average_price,
            stop_price,
            last_executed_quantity,
            cumulative_filled_quantity,
            last_executed_price,
            commission_amount,
            buy_notional,
            sell_notional,
            realized_profit,
            order_type,
            time_in_force,
            execution_type,
            order_status,
            commission_asset,
            strategy_type,
            business_unit,
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
