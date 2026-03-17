use anyhow::{anyhow, Context, Result};
use bytes::{Buf, Bytes};
use polars::prelude::ParquetWriter;
use polars::prelude::*;

use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};

#[derive(Debug, Clone, Copy)]
pub(crate) struct RangeFilter {
    start_ts: Option<u64>,
    end_ts: Option<u64>,
}

impl RangeFilter {
    pub(crate) fn all() -> Self {
        Self {
            start_ts: None,
            end_ts: None,
        }
    }

    pub(crate) fn from_bounds(start_ts: u64, end_ts_inclusive: u64) -> Self {
        Self {
            start_ts: Some(start_ts),
            end_ts: Some(end_ts_inclusive),
        }
    }

    fn contains(&self, ts: u64) -> bool {
        if let Some(start) = self.start_ts {
            if ts < start {
                return false;
            }
        }
        if let Some(end) = self.end_ts {
            if ts > end {
                return false;
            }
        }
        true
    }
}

pub(crate) fn build_parquet_trade_updates(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    let mut key_col = Vec::with_capacity(entries.len());
    let mut ts_col = Vec::with_capacity(entries.len());
    let mut event_time_col = Vec::with_capacity(entries.len());
    let mut trade_time_col = Vec::with_capacity(entries.len());
    let mut symbol_col = Vec::with_capacity(entries.len());
    let mut order_id_col = Vec::with_capacity(entries.len());
    let mut client_order_id_col = Vec::with_capacity(entries.len());
    let mut side_col = Vec::with_capacity(entries.len());
    let mut price_col = Vec::with_capacity(entries.len());
    let mut is_maker_col = Vec::with_capacity(entries.len());
    let mut venue_col = Vec::with_capacity(entries.len());
    let mut cumulative_col = Vec::with_capacity(entries.len());
    let mut status_col: Vec<Option<String>> = Vec::with_capacity(entries.len());

    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes)?;
        let ts_us = parse_simple_key(&key)?;
        if !range.contains(ts_us) {
            continue;
        }
        let record = decode_trade_record(&value_bytes)?;
        let DecodedTradeRecord {
            event_time,
            trade_time,
            symbol,
            order_id,
            client_order_id,
            side,
            price,
            is_maker,
            trading_venue,
            cumulative_filled_quantity,
            order_status,
        } = record;
        key_col.push(key);
        ts_col.push(ts_us as i64);
        event_time_col.push(event_time);
        trade_time_col.push(trade_time);
        symbol_col.push(symbol);
        order_id_col.push(order_id);
        client_order_id_col.push(client_order_id);
        side_col.push(side);
        price_col.push(price);
        is_maker_col.push(is_maker);
        venue_col.push(trading_venue);
        cumulative_col.push(cumulative_filled_quantity);
        status_col.push(order_status.clone());
    }

    let mut df = DataFrame::new(vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_col),
        Series::new("event_time".into(), event_time_col),
        Series::new("trade_time".into(), trade_time_col),
        Series::new("symbol".into(), symbol_col),
        Series::new("order_id".into(), order_id_col),
        Series::new("client_order_id".into(), client_order_id_col),
        Series::new("side".into(), side_col),
        Series::new("price".into(), price_col),
        Series::new("is_maker".into(), is_maker_col),
        Series::new("trading_venue".into(), venue_col),
        Series::new("cumulative_filled_quantity".into(), cumulative_col),
        Series::new("order_status".into(), status_col.as_slice()),
    ])?;

    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(&mut df)?;
    Ok(buf)
}

pub(crate) fn build_parquet_order_updates(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    let mut key_col = Vec::with_capacity(entries.len());
    let mut ts_col = Vec::with_capacity(entries.len());
    let mut event_time_col = Vec::with_capacity(entries.len());
    let mut symbol_col = Vec::with_capacity(entries.len());
    let mut order_id_col = Vec::with_capacity(entries.len());
    let mut client_order_id_col = Vec::with_capacity(entries.len());
    let mut client_order_id_str_col = Vec::with_capacity(entries.len());
    let mut side_col = Vec::with_capacity(entries.len());
    let mut order_type_col = Vec::with_capacity(entries.len());
    let mut tif_col = Vec::with_capacity(entries.len());
    let mut price_col = Vec::with_capacity(entries.len());
    let mut qty_col = Vec::with_capacity(entries.len());
    let mut cumulative_col = Vec::with_capacity(entries.len());
    let mut status_col = Vec::with_capacity(entries.len());
    let mut raw_status_col = Vec::with_capacity(entries.len());
    let mut exec_type_col = Vec::with_capacity(entries.len());
    let mut raw_exec_type_col = Vec::with_capacity(entries.len());
    let mut venue_col = Vec::with_capacity(entries.len());

    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes)?;
        let ts_us = parse_simple_key(&key)?;
        if !range.contains(ts_us) {
            continue;
        }
        let record = decode_order_record(&value_bytes)?;
        let DecodedOrderRecord {
            event_time,
            symbol,
            order_id,
            client_order_id,
            client_order_id_str,
            side,
            order_type,
            time_in_force,
            price,
            quantity,
            cumulative_filled_quantity,
            status,
            raw_status,
            execution_type,
            raw_execution_type,
            trading_venue,
        } = record;
        key_col.push(key);
        ts_col.push(ts_us as i64);
        event_time_col.push(event_time);
        symbol_col.push(symbol);
        order_id_col.push(order_id);
        client_order_id_col.push(client_order_id);
        client_order_id_str_col.push(client_order_id_str);
        side_col.push(side);
        order_type_col.push(order_type);
        tif_col.push(time_in_force);
        price_col.push(price);
        qty_col.push(quantity);
        cumulative_col.push(cumulative_filled_quantity);
        status_col.push(status);
        raw_status_col.push(raw_status);
        exec_type_col.push(execution_type);
        raw_exec_type_col.push(raw_execution_type);
        venue_col.push(trading_venue);
    }

    let mut df = DataFrame::new(vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_col),
        Series::new("event_time".into(), event_time_col),
        Series::new("symbol".into(), symbol_col),
        Series::new("order_id".into(), order_id_col),
        Series::new("client_order_id".into(), client_order_id_col),
        Series::new(
            "client_order_id_str".into(),
            client_order_id_str_col.as_slice(),
        ),
        Series::new("side".into(), side_col),
        Series::new("order_type".into(), order_type_col),
        Series::new("time_in_force".into(), tif_col),
        Series::new("price".into(), price_col),
        Series::new("quantity".into(), qty_col),
        Series::new("cumulative_filled_quantity".into(), cumulative_col),
        Series::new("status".into(), status_col),
        Series::new("raw_status".into(), raw_status_col),
        Series::new("execution_type".into(), exec_type_col),
        Series::new("raw_execution_type".into(), raw_exec_type_col),
        Series::new("trading_venue".into(), venue_col),
    ])?;

    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(&mut df)?;
    Ok(buf)
}

pub(crate) fn build_parquet_uniform_orders(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    let mut key_col = Vec::with_capacity(entries.len());
    let mut ts_col = Vec::with_capacity(entries.len());
    let mut recv_ts_col = Vec::with_capacity(entries.len());
    let mut symbol_col = Vec::with_capacity(entries.len());
    let mut create_ts_col = Vec::with_capacity(entries.len());
    let mut update_ts_col = Vec::with_capacity(entries.len());
    let mut signal_ts_col = Vec::with_capacity(entries.len());
    let mut client_order_id_col = Vec::with_capacity(entries.len());
    let mut venue_col = Vec::with_capacity(entries.len());
    let mut order_type_col = Vec::with_capacity(entries.len());
    let mut side_col = Vec::with_capacity(entries.len());
    let mut price_col = Vec::with_capacity(entries.len());
    let mut price_offset_col = Vec::with_capacity(entries.len());
    let mut amount_init_col = Vec::with_capacity(entries.len());
    let mut amount_update_col = Vec::with_capacity(entries.len());
    let mut status_col = Vec::with_capacity(entries.len());
    let mut from_key_col = Vec::with_capacity(entries.len());
    let mut from_key_hex_col = Vec::with_capacity(entries.len());

    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes)?;
        let ts_us = parse_simple_key(&key)?;
        if !range.contains(ts_us) {
            continue;
        }

        let record = decode_uniform_order_record(&value_bytes)?;
        let DecodedUniformOrderRecord {
            recv_ts_us,
            symbol,
            create_ts,
            update_ts,
            signal_ts,
            client_order_id,
            trading_venue,
            order_type,
            side,
            price,
            price_offset,
            amount_init,
            amount_update,
            status,
            from_key,
            from_key_hex,
        } = record;

        key_col.push(key);
        ts_col.push(ts_us as i64);
        recv_ts_col.push(recv_ts_us);
        symbol_col.push(symbol);
        create_ts_col.push(create_ts);
        update_ts_col.push(update_ts);
        signal_ts_col.push(signal_ts);
        client_order_id_col.push(client_order_id);
        venue_col.push(trading_venue);
        order_type_col.push(order_type);
        side_col.push(side);
        price_col.push(price);
        price_offset_col.push(price_offset);
        amount_init_col.push(amount_init);
        amount_update_col.push(amount_update);
        status_col.push(status);
        from_key_col.push(from_key);
        from_key_hex_col.push(from_key_hex);
    }

    let mut df = DataFrame::new(vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_col),
        Series::new("recv_ts_us".into(), recv_ts_col),
        Series::new("symbol".into(), symbol_col),
        Series::new("create_ts".into(), create_ts_col),
        Series::new("update_ts".into(), update_ts_col),
        Series::new("signal_ts".into(), signal_ts_col),
        Series::new("client_order_id".into(), client_order_id_col),
        Series::new("trading_venue".into(), venue_col),
        Series::new("order_type".into(), order_type_col),
        Series::new("side".into(), side_col),
        Series::new("price".into(), price_col),
        Series::new("price_offset".into(), price_offset_col),
        Series::new("amount_init".into(), amount_init_col),
        Series::new("amount_update".into(), amount_update_col),
        Series::new("status".into(), status_col),
        Series::new("from_key".into(), from_key_col),
        Series::new("from_key_hex".into(), from_key_hex_col),
    ])?;

    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(&mut df)?;
    Ok(buf)
}

#[derive(Debug)]
struct DecodedTradeRecord {
    event_time: i64,
    trade_time: i64,
    symbol: String,
    order_id: i64,
    client_order_id: i64,
    side: String,
    price: f64,
    is_maker: bool,
    trading_venue: String,
    cumulative_filled_quantity: f64,
    order_status: Option<String>,
}

#[derive(Debug)]
struct DecodedOrderRecord {
    event_time: i64,
    symbol: String,
    order_id: i64,
    client_order_id: i64,
    client_order_id_str: Option<String>,
    side: String,
    order_type: String,
    time_in_force: String,
    price: f64,
    quantity: f64,
    cumulative_filled_quantity: f64,
    status: String,
    raw_status: String,
    execution_type: String,
    raw_execution_type: String,
    trading_venue: String,
}

#[derive(Debug)]
struct DecodedUniformOrderRecord {
    recv_ts_us: i64,
    symbol: String,
    create_ts: i64,
    update_ts: i64,
    signal_ts: i64,
    client_order_id: i64,
    trading_venue: String,
    order_type: String,
    side: String,
    price: f64,
    price_offset: f64,
    amount_init: f64,
    amount_update: f64,
    status: String,
    from_key: String,
    from_key_hex: String,
}

fn decode_trade_record(bytes: &[u8]) -> Result<DecodedTradeRecord> {
    let mut cursor = Bytes::copy_from_slice(bytes);
    let _recv_ts_us = read_i64(&mut cursor, "trade update recv_ts_us")?;
    let event_time = read_i64(&mut cursor, "trade update event_time")?;
    let trade_time = read_i64(&mut cursor, "trade update trade_time")?;
    let symbol = read_string(&mut cursor)?;
    let order_id = read_i64(&mut cursor, "trade update order_id")?;
    let client_order_id = read_i64(&mut cursor, "trade update client_order_id")?;
    let side_raw = read_u8(&mut cursor, "trade update side")?;
    let price = read_f64(&mut cursor, "trade update price")?;
    let is_maker = read_u8(&mut cursor, "trade update is_maker")? != 0;
    let trading_venue = read_u8(&mut cursor, "trade update trading_venue")?;
    let cumulative_filled_quantity = read_f64(&mut cursor, "trade update cumulative_qty")?;
    let has_status = read_u8(&mut cursor, "trade update status flag")?;
    let order_status = if has_status != 0 {
        let status_code = read_u8(&mut cursor, "trade update status value")?;
        order_status_from_u8(status_code).map(|s| s.as_str().to_string())
    } else {
        None
    };

    let side_str = Side::from_u8(side_raw)
        .map(|s| s.as_str().to_string())
        .unwrap_or_else(|| format!("Side({side_raw})"));
    let venue_str = TradingVenue::from_u8(trading_venue)
        .map(|v| v.as_str().to_string())
        .unwrap_or_else(|| format!("Venue({trading_venue})"));

    Ok(DecodedTradeRecord {
        event_time,
        trade_time,
        symbol,
        order_id,
        client_order_id,
        side: side_str,
        price,
        is_maker,
        trading_venue: venue_str,
        cumulative_filled_quantity,
        order_status,
    })
}

fn decode_order_record(bytes: &[u8]) -> Result<DecodedOrderRecord> {
    let mut cursor = Bytes::copy_from_slice(bytes);
    let _recv_ts_us = read_i64(&mut cursor, "order update recv_ts_us")?;
    let event_time = read_i64(&mut cursor, "order update event_time")?;
    let symbol = read_string(&mut cursor)?;
    let order_id = read_i64(&mut cursor, "order update order_id")?;
    let client_order_id = read_i64(&mut cursor, "order update client_order_id")?;
    let client_order_id_str = read_opt_string(&mut cursor)?;
    let side_raw = read_u8(&mut cursor, "order update side")?;
    let order_type_raw = read_u8(&mut cursor, "order update order_type")?;
    let tif_raw = read_u8(&mut cursor, "order update time_in_force")?;
    let price = read_f64(&mut cursor, "order update price")?;
    let quantity = read_f64(&mut cursor, "order update quantity")?;
    let cumulative_filled_quantity = read_f64(&mut cursor, "order update cumulative_qty")?;
    let status_raw = read_u8(&mut cursor, "order update status")?;
    let raw_status = read_string(&mut cursor)?;
    let execution_type_raw = read_u8(&mut cursor, "order update execution_type")?;
    let raw_execution_type = read_string(&mut cursor)?;
    let trading_venue_raw = read_u8(&mut cursor, "order update trading_venue")?;

    let side = Side::from_u8(side_raw)
        .map(|s| s.as_str().to_string())
        .unwrap_or_else(|| format!("Side({side_raw})"));
    let order_type = OrderType::from_u8(order_type_raw)
        .map(|t| t.as_str().to_string())
        .unwrap_or_else(|| format!("Type({order_type_raw})"));
    let time_in_force = time_in_force_from_u8(tif_raw)
        .map(|t| t.as_str().to_string())
        .unwrap_or_else(|| format!("TIF({tif_raw})"));
    let status = order_status_from_u8(status_raw)
        .map(|s| s.as_str().to_string())
        .unwrap_or_else(|| format!("Status({status_raw})"));
    let execution_type = execution_type_from_u8(execution_type_raw)
        .map(|e| e.as_str().to_string())
        .unwrap_or_else(|| format!("ExecType({execution_type_raw})"));
    let trading_venue = TradingVenue::from_u8(trading_venue_raw)
        .map(|v| v.as_str().to_string())
        .unwrap_or_else(|| format!("Venue({trading_venue_raw})"));

    Ok(DecodedOrderRecord {
        event_time,
        symbol,
        order_id,
        client_order_id,
        client_order_id_str,
        side,
        order_type,
        time_in_force,
        price,
        quantity,
        cumulative_filled_quantity,
        status,
        raw_status,
        execution_type,
        raw_execution_type,
        trading_venue,
    })
}

fn decode_uniform_order_record(bytes: &[u8]) -> Result<DecodedUniformOrderRecord> {
    let mut cursor = Bytes::copy_from_slice(bytes);
    let recv_ts_us = read_i64(&mut cursor, "uniform order recv_ts_us")?;

    let symbol_len = read_u16(&mut cursor, "uniform order symbol_len")? as usize;
    let symbol = read_bytes_as_string(&mut cursor, symbol_len, "uniform order symbol")?;

    let create_ts = read_i64(&mut cursor, "uniform order create_ts")?;
    let update_ts = read_i64(&mut cursor, "uniform order update_ts")?;
    let signal_ts = read_i64(&mut cursor, "uniform order signal_ts")?;

    let client_order_id = read_i64(&mut cursor, "uniform order client_order_id")?;

    let venue_raw = read_u8(&mut cursor, "uniform order venue")?;
    let order_type_raw = read_u8(&mut cursor, "uniform order order_type")?;
    let side_raw = read_u8(&mut cursor, "uniform order side")?;

    let price = read_f64(&mut cursor, "uniform order price")?;
    let price_offset = read_f64(&mut cursor, "uniform order price_offset")?;
    let amount_init = read_f64(&mut cursor, "uniform order amount_init")?;
    let amount_update = read_f64(&mut cursor, "uniform order amount_update")?;

    let status_raw = read_u8(&mut cursor, "uniform order status")?;

    let from_key_len = read_u32(&mut cursor, "uniform order from_key_len")? as usize;
    if cursor.remaining() < from_key_len {
        return Err(anyhow!(
            "payload too short to read uniform order from_key (need {from_key_len}, have {})",
            cursor.remaining()
        ));
    }
    let from_key_bytes = cursor.copy_to_bytes(from_key_len);
    let from_key = String::from_utf8_lossy(from_key_bytes.as_ref()).into_owned();
    let from_key_hex = hex::encode(from_key_bytes.as_ref());

    if cursor.has_remaining() {
        return Err(anyhow!(
            "uniform order payload has {} trailing bytes",
            cursor.remaining()
        ));
    }

    let trading_venue = TradingVenue::from_u8(venue_raw)
        .map(|v| v.as_str().to_string())
        .unwrap_or_else(|| format!("Venue({venue_raw})"));
    let order_type = OrderType::from_u8(order_type_raw)
        .map(|v| v.as_str().to_string())
        .unwrap_or_else(|| format!("Type({order_type_raw})"));
    let side = Side::from_u8(side_raw)
        .map(|v| v.as_str().to_string())
        .unwrap_or_else(|| format!("Side({side_raw})"));
    let status = OrderStatus::from_u8(status_raw)
        .map(|v| v.as_str().to_string())
        .unwrap_or_else(|| format!("Status({status_raw})"));

    Ok(DecodedUniformOrderRecord {
        recv_ts_us,
        symbol,
        create_ts,
        update_ts,
        signal_ts,
        client_order_id,
        trading_venue,
        order_type,
        side,
        price,
        price_offset,
        amount_init,
        amount_update,
        status,
        from_key,
        from_key_hex,
    })
}

fn read_bytes_as_string(cursor: &mut Bytes, len: usize, field: &str) -> Result<String> {
    if cursor.remaining() < len {
        return Err(anyhow!(
            "payload too short to read {field} (need {len}, have {})",
            cursor.remaining()
        ));
    }

    let bytes = cursor.copy_to_bytes(len);
    Ok(String::from_utf8_lossy(bytes.as_ref()).into_owned())
}

fn read_string(cursor: &mut Bytes) -> Result<String> {
    if cursor.remaining() < 4 {
        return Err(anyhow!("payload too short to read string length"));
    }
    let len = cursor.get_u32_le() as usize;
    if cursor.remaining() < len {
        return Err(anyhow!(
            "payload too short to read string data (need {len}, have {})",
            cursor.remaining()
        ));
    }
    let bytes = cursor.copy_to_bytes(len);
    Ok(String::from_utf8(bytes.to_vec())?)
}

fn read_opt_string(cursor: &mut Bytes) -> Result<Option<String>> {
    if !cursor.has_remaining() {
        return Err(anyhow!("payload too short to read string flag"));
    }
    let flag = cursor.get_u8();
    if flag == 0 {
        return Ok(None);
    }
    read_string(cursor).map(Some)
}

fn read_i64(cursor: &mut Bytes, field: &str) -> Result<i64> {
    if cursor.remaining() < 8 {
        return Err(anyhow!("payload too short to read {field}"));
    }
    Ok(cursor.get_i64_le())
}

fn read_u16(cursor: &mut Bytes, field: &str) -> Result<u16> {
    if cursor.remaining() < 2 {
        return Err(anyhow!("payload too short to read {field}"));
    }
    Ok(cursor.get_u16_le())
}

fn read_u32(cursor: &mut Bytes, field: &str) -> Result<u32> {
    if cursor.remaining() < 4 {
        return Err(anyhow!("payload too short to read {field}"));
    }
    Ok(cursor.get_u32_le())
}

fn read_f64(cursor: &mut Bytes, field: &str) -> Result<f64> {
    if cursor.remaining() < 8 {
        return Err(anyhow!("payload too short to read {field}"));
    }
    Ok(cursor.get_f64_le())
}

fn read_u8(cursor: &mut Bytes, field: &str) -> Result<u8> {
    if !cursor.has_remaining() {
        return Err(anyhow!("payload too short to read {field}"));
    }
    Ok(cursor.get_u8())
}

fn parse_simple_key(key: &str) -> Result<u64> {
    key.parse::<u64>()
        .with_context(|| format!("invalid key format: {}", key))
}

fn time_in_force_from_u8(value: u8) -> Option<TimeInForce> {
    match value {
        0 => Some(TimeInForce::GTC),
        1 => Some(TimeInForce::IOC),
        2 => Some(TimeInForce::FOK),
        3 => Some(TimeInForce::GTX),
        _ => None,
    }
}

fn execution_type_from_u8(value: u8) -> Option<ExecutionType> {
    match value {
        0 => Some(ExecutionType::New),
        1 => Some(ExecutionType::Canceled),
        2 => Some(ExecutionType::Replaced),
        3 => Some(ExecutionType::Rejected),
        4 => Some(ExecutionType::Trade),
        5 => Some(ExecutionType::Expired),
        6 => Some(ExecutionType::TradePrevention),
        _ => None,
    }
}

fn order_status_from_u8(value: u8) -> Option<OrderStatus> {
    match value {
        0 => Some(OrderStatus::New),
        1 => Some(OrderStatus::PartiallyFilled),
        2 => Some(OrderStatus::Filled),
        3 => Some(OrderStatus::Canceled),
        4 => Some(OrderStatus::Expired),
        5 => Some(OrderStatus::ExpiredInMatch),
        _ => None,
    }
}
