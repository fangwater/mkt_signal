//! Current production native persistence codec. Inputs are already normalized by
//! the producer. No clocks, IPC publishers, or market metadata are accessed here.
use crate::{SignalBbo, UnifiedOrderRecord, UNIFORM_ORDER_TAIL_BINARY_LEN};
use bytes::{BufMut, Bytes, BytesMut};
use order_common::TradeUpdate;
fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_u32_le(value.len() as u32);
    buf.put_slice(value.as_bytes());
}
pub fn serialize_trade_update(
    trade: &dyn TradeUpdate,
    recv_ts_us: i64,
    normalized_symbol: &str,
    normalized_cum_qty: f64,
) -> Bytes {
    let mut buf = BytesMut::with_capacity(512);
    let venue = trade.trading_venue();
    // 接收时间戳（在发布时记录）
    buf.put_i64_le(recv_ts_us);

    // 基础时间戳
    buf.put_i64_le(trade.event_time());
    buf.put_i64_le(trade.trade_time());

    // 交易对符号
    put_string(&mut buf, normalized_symbol);

    // ID 字段
    buf.put_i64_le(trade.order_id());
    buf.put_i64_le(trade.client_order_id());

    // 方向
    buf.put_u8(trade.side() as u8);

    // 价格
    buf.put_f64_le(trade.price());

    // Maker/Taker
    buf.put_u8(trade.is_maker() as u8);

    // 交易所类型
    buf.put_u8(venue as u8);

    // 累计成交量
    buf.put_f64_le(normalized_cum_qty);

    // 订单状态（可选）
    if let Some(status) = trade.order_status() {
        buf.put_u8(1); // has value
        buf.put_u8(status as u8);
    } else {
        buf.put_u8(0); // no value
    }

    buf.freeze()
}

pub fn serialize_uniform_order(
    record: &UnifiedOrderRecord,
    recv_ts_us: i64,
    normalized_symbol: &str,
    normalized_amount_init: f64,
    normalized_amount_update: f64,
) -> Bytes {
    let mut buf = BytesMut::with_capacity(512);
    buf.put_i64_le(recv_ts_us);

    let symbol_len = normalized_symbol.len() as u16;
    buf.put_u16_le(symbol_len);
    buf.put_slice(normalized_symbol.as_bytes());

    buf.put_i64_le(record.create_ts);
    buf.put_i64_le(record.update_ts);
    buf.put_i64_le(record.signal_ts);
    buf.put_i64_le(record.submit_ts);
    buf.put_i64_le(record.local_ts);
    buf.put_i64_le(record.mkt_ts);

    buf.put_i64_le(record.client_order_id);

    buf.put_u8(record.venue);
    buf.put_u8(record.ttype);
    buf.put_u8(record.side);

    buf.put_f64_le(record.price);
    buf.put_f64_le(record.price_offset);
    buf.put_f64_le(normalized_amount_init);
    buf.put_f64_le(normalized_amount_update);

    buf.put_u8(record.status);

    let from_key_len = record.from_key.len() as u32;
    buf.put_u32_le(from_key_len);
    buf.put_slice(&record.from_key);

    buf.put_slice(&SignalBbo::encode_optional(record.signal_bbo));
    buf.put_u8(record.fill_liquidity as u8);

    buf.freeze()
}

pub fn append_bbo_spread(payload: Bytes, bbo_spread: &str) -> Bytes {
    let bbo_bytes = bbo_spread.as_bytes();
    let len = bbo_bytes.len().min(u16::MAX as usize);
    let signal_start = payload.len().saturating_sub(UNIFORM_ORDER_TAIL_BINARY_LEN);
    let mut out = BytesMut::with_capacity(payload.len() + 2 + len);
    out.extend_from_slice(&payload[..signal_start]);
    out.put_u16_le(len as u16);
    out.extend_from_slice(&bbo_bytes[..len]);
    out.extend_from_slice(&payload[signal_start..]);
    out.freeze()
}
