//! Current production native persistence codec. Inputs are already normalized by
//! the producer. No clocks, IPC publishers, or market metadata are accessed here.
use crate::record_extension::ExtensionRef;
use crate::{SignalBbo, UnifiedOrderRecord, UniformOrderRef, UNIFORM_ORDER_TAIL_BINARY_LEN};
use bytes::{BufMut, Bytes, BytesMut};
use order_common::TradeUpdate;
fn put_string(buf: &mut impl BufMut, value: &str) {
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
    write_trade_update(
        &mut buf,
        trade,
        recv_ts_us,
        normalized_symbol,
        normalized_cum_qty,
    );
    buf.freeze()
}

/// Writes one trade update into a caller-owned buffer (no allocation), followed
/// by the optional cross-market extension when the adapter reports one.
pub fn write_trade_update(
    buf: &mut impl BufMut,
    trade: &dyn TradeUpdate,
    recv_ts_us: i64,
    normalized_symbol: &str,
    normalized_cum_qty: f64,
) {
    // 接收时间戳（在发布时记录）
    buf.put_i64_le(recv_ts_us);
    // 基础时间戳
    buf.put_i64_le(trade.event_time());
    buf.put_i64_le(trade.trade_time());
    // 交易对符号
    put_string(buf, normalized_symbol);
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
    buf.put_u8(trade.trading_venue() as u8);
    // 累计成交量
    buf.put_f64_le(normalized_cum_qty);
    // 订单状态（可选）
    if let Some(status) = trade.order_status() {
        buf.put_u8(1); // has value
        buf.put_u8(status as u8);
    } else {
        buf.put_u8(0); // no value
    }
    ExtensionRef::of_trade(trade).write(buf);
}

/// Stored uniform-order layout: received time, the record, the BBO spread
/// text, signal BBO, fill role, then the optional extension. Writes into a
/// caller-owned buffer without allocating.
pub fn write_stored_uniform_order(
    buf: &mut impl BufMut,
    received_at_us: i64,
    bbo_spread: &str,
    record: &UniformOrderRef<'_>,
) {
    buf.put_i64_le(received_at_us);
    let symbol = &record.symbol[..record.symbol.len().min(u16::MAX as usize)];
    buf.put_u16_le(symbol.len() as u16);
    buf.put_slice(symbol);
    for ts in [
        record.create_ts,
        record.update_ts,
        record.signal_ts,
        record.submit_ts,
        record.local_ts,
        record.mkt_ts,
        record.client_order_id,
    ] {
        buf.put_i64_le(ts);
    }
    buf.put_u8(record.venue);
    buf.put_u8(record.ttype);
    buf.put_u8(record.side);
    for value in [
        record.price,
        record.price_offset,
        record.amount_init,
        record.amount_update,
    ] {
        buf.put_f64_le(value);
    }
    buf.put_u8(record.status);
    buf.put_u32_le(record.from_key.len() as u32);
    buf.put_slice(record.from_key);
    let spread = &bbo_spread.as_bytes()[..bbo_spread.len().min(u16::MAX as usize)];
    buf.put_u16_le(spread.len() as u16);
    buf.put_slice(spread);
    buf.put_slice(&SignalBbo::encode_optional(record.signal_bbo));
    buf.put_u8(record.fill_liquidity as u8);
    record.extension.write(buf);
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

/// One stored trade update, as written by [`write_trade_update`].
#[derive(Debug, Clone, PartialEq)]
pub struct TradeUpdateRecord {
    pub recv_ts_us: i64,
    pub event_time: i64,
    pub trade_time: i64,
    pub symbol: String,
    pub order_id: i64,
    pub client_order_id: i64,
    pub side: u8,
    pub price: f64,
    pub is_maker: bool,
    pub venue: u8,
    pub cumulative_filled_quantity: f64,
    pub order_status: Option<u8>,
    pub extension: Option<crate::record_extension::RecordExtension>,
}

pub fn decode_trade_update(payload: &[u8]) -> anyhow::Result<TradeUpdateRecord> {
    use anyhow::{bail, ensure, Context};
    fn take<'a>(input: &mut &'a [u8], n: usize) -> anyhow::Result<&'a [u8]> {
        ensure!(input.len() >= n, "truncated trade update");
        let (head, tail) = input.split_at(n);
        *input = tail;
        Ok(head)
    }
    let mut input = payload;
    let i64v = |input: &mut &[u8]| -> anyhow::Result<i64> {
        Ok(i64::from_le_bytes(take(input, 8)?.try_into()?))
    };
    let f64v = |input: &mut &[u8]| -> anyhow::Result<f64> {
        Ok(f64::from_le_bytes(take(input, 8)?.try_into()?))
    };
    let recv_ts_us = i64v(&mut input)?;
    let event_time = i64v(&mut input)?;
    let trade_time = i64v(&mut input)?;
    let len = u32::from_le_bytes(take(&mut input, 4)?.try_into()?) as usize;
    let symbol = std::str::from_utf8(take(&mut input, len)?)
        .context("trade update symbol is not UTF-8")?
        .to_owned();
    let order_id = i64v(&mut input)?;
    let client_order_id = i64v(&mut input)?;
    let side = take(&mut input, 1)?[0];
    let price = f64v(&mut input)?;
    let is_maker = take(&mut input, 1)?[0] != 0;
    let venue = take(&mut input, 1)?[0];
    let cumulative_filled_quantity = f64v(&mut input)?;
    let order_status = match take(&mut input, 1)?[0] {
        0 => None,
        1 => Some(take(&mut input, 1)?[0]),
        flag => bail!("invalid trade update status flag {flag}"),
    };
    let extension = crate::record_extension::RecordExtension::read(&mut input)?;
    ensure!(input.is_empty(), "trailing bytes in trade update");
    Ok(TradeUpdateRecord {
        recv_ts_us,
        event_time,
        trade_time,
        symbol,
        order_id,
        client_order_id,
        side,
        price,
        is_maker,
        venue,
        cumulative_filled_quantity,
        order_status,
        extension,
    })
}
