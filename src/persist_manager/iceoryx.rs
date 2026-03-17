use anyhow::{Context, Result};
use bytes::{Buf, Bytes};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;

use crate::common::iceoryx_publisher::SIGNAL_PAYLOAD;
use crate::common::ipc_service_name::build_service_name;

const NODE_PREFIX: &str = "persist_record_";

pub fn create_record_subscriber(
    channel: &str,
) -> Result<Subscriber<ipc::Service, [u8; SIGNAL_PAYLOAD], ()>> {
    let node_name = format!("{}{}", NODE_PREFIX, sanitize_suffix(channel));
    let service_name = build_service_name(&format!("persist_pubs/{}", channel));

    let node = NodeBuilder::new()
        .name(&NodeName::new(&node_name)?)
        .create::<ipc::Service>()
        .with_context(|| format!("failed to create iceoryx node {}", node_name))?;

    let service = node
        .service_builder(&ServiceName::new(&service_name)?)
        .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
        .max_publishers(1)
        .max_subscribers(32)
        .history_size(128)
        .subscriber_max_buffer_size(256)
        .open_or_create()
        .with_context(|| format!("failed to open service {}", service_name))?;

    let subscriber = service
        .subscriber_builder()
        .create()
        .with_context(|| format!("failed to create subscriber {}", service_name))?;

    Ok(subscriber)
}

pub fn trim_trade_update_payload(payload: &[u8]) -> Bytes {
    let used = trade_update_used_len(payload).unwrap_or(payload.len());
    Bytes::copy_from_slice(&payload[..used])
}

pub fn trim_order_update_payload(payload: &[u8]) -> Bytes {
    let used = order_update_used_len(payload).unwrap_or(payload.len());
    Bytes::copy_from_slice(&payload[..used])
}

pub fn trim_uniform_order_payload(payload: &[u8]) -> Bytes {
    let used = uniform_order_used_len(payload).unwrap_or(payload.len());
    Bytes::copy_from_slice(&payload[..used])
}

fn trade_update_used_len(payload: &[u8]) -> Option<usize> {
    // Format must match pre_trade::persist_channel::serialize_trade_update
    let mut cursor = Bytes::copy_from_slice(payload);

    skip_i64(&mut cursor)?; // recv_ts_us
    skip_i64(&mut cursor)?; // event_time
    skip_i64(&mut cursor)?; // trade_time
    skip_string(&mut cursor)?; // symbol
    skip_i64(&mut cursor)?; // order_id
    skip_i64(&mut cursor)?; // client_order_id
    skip_u8(&mut cursor)?; // side
    skip_f64(&mut cursor)?; // price
    skip_u8(&mut cursor)?; // is_maker
    skip_u8(&mut cursor)?; // trading_venue
    skip_f64(&mut cursor)?; // cumulative_filled_quantity

    let has_status = read_u8(&mut cursor)?;
    if has_status != 0 {
        skip_u8(&mut cursor)?; // status
    }

    Some(payload.len().saturating_sub(cursor.remaining()))
}

fn order_update_used_len(payload: &[u8]) -> Option<usize> {
    // Format must match pre_trade::persist_channel::serialize_order_update
    let mut cursor = Bytes::copy_from_slice(payload);

    skip_i64(&mut cursor)?; // recv_ts_us
    skip_i64(&mut cursor)?; // event_time
    skip_string(&mut cursor)?; // symbol
    skip_i64(&mut cursor)?; // order_id
    skip_i64(&mut cursor)?; // client_order_id
    skip_opt_string(&mut cursor)?; // client_order_id_str
    skip_u8(&mut cursor)?; // side
    skip_u8(&mut cursor)?; // order_type
    skip_u8(&mut cursor)?; // time_in_force
    skip_f64(&mut cursor)?; // price
    skip_f64(&mut cursor)?; // quantity
    skip_f64(&mut cursor)?; // cumulative_filled_quantity
    skip_u8(&mut cursor)?; // status
    skip_string(&mut cursor)?; // raw_status
    skip_u8(&mut cursor)?; // execution_type
    skip_string(&mut cursor)?; // raw_execution_type
    skip_u8(&mut cursor)?; // trading_venue

    Some(payload.len().saturating_sub(cursor.remaining()))
}

fn uniform_order_used_len(payload: &[u8]) -> Option<usize> {
    // Format must match pre_trade::persist_channel::serialize_uniform_order
    let mut cursor = Bytes::copy_from_slice(payload);

    skip_i64(&mut cursor)?; // recv_ts_us

    let symbol_len = read_u16(&mut cursor)? as usize;
    if cursor.remaining() < symbol_len {
        return None;
    }
    cursor.advance(symbol_len);

    skip_i64(&mut cursor)?; // create_ts
    skip_i64(&mut cursor)?; // update_ts
    skip_i64(&mut cursor)?; // signal_ts

    skip_i64(&mut cursor)?; // client_order_id

    skip_u8(&mut cursor)?; // venue
    skip_u8(&mut cursor)?; // ttype
    skip_u8(&mut cursor)?; // side

    skip_f64(&mut cursor)?; // price
    skip_f64(&mut cursor)?; // price_offset
    skip_f64(&mut cursor)?; // amount_init
    skip_f64(&mut cursor)?; // amount_update

    skip_u8(&mut cursor)?; // status

    let from_key_len = read_u32(&mut cursor)? as usize;
    if cursor.remaining() < from_key_len {
        return None;
    }
    cursor.advance(from_key_len);

    Some(payload.len().saturating_sub(cursor.remaining()))
}

fn skip_string(cursor: &mut Bytes) -> Option<()> {
    let len = read_u32(cursor)? as usize;
    if cursor.remaining() < len {
        return None;
    }
    cursor.advance(len);
    Some(())
}

fn skip_opt_string(cursor: &mut Bytes) -> Option<()> {
    let flag = read_u8(cursor)?;
    if flag == 0 {
        return Some(());
    }
    skip_string(cursor)
}

fn skip_i64(cursor: &mut Bytes) -> Option<()> {
    if cursor.remaining() < 8 {
        return None;
    }
    cursor.advance(8);
    Some(())
}

fn skip_f64(cursor: &mut Bytes) -> Option<()> {
    if cursor.remaining() < 8 {
        return None;
    }
    cursor.advance(8);
    Some(())
}

fn skip_u8(cursor: &mut Bytes) -> Option<()> {
    if cursor.remaining() < 1 {
        return None;
    }
    cursor.advance(1);
    Some(())
}

fn read_u8(cursor: &mut Bytes) -> Option<u8> {
    if cursor.remaining() < 1 {
        return None;
    }
    Some(cursor.get_u8())
}

fn read_u32(cursor: &mut Bytes) -> Option<u32> {
    if cursor.remaining() < 4 {
        return None;
    }
    Some(cursor.get_u32_le())
}

fn read_u16(cursor: &mut Bytes) -> Option<u16> {
    if cursor.remaining() < 2 {
        return None;
    }
    Some(cursor.get_u16_le())
}

fn sanitize_suffix(raw: &str) -> std::borrow::Cow<'_, str> {
    if raw.chars().all(is_valid_node_char) {
        return std::borrow::Cow::Borrowed(raw);
    }
    let sanitized: String = raw
        .chars()
        .map(|c| if is_valid_node_char(c) { c } else { '_' })
        .collect();
    std::borrow::Cow::Owned(sanitized)
}

fn is_valid_node_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}
