use anyhow::{anyhow, Context, Result};
use bytes::{Buf, Bytes};
use log::warn;
use polars::prelude::ParquetWriter;
use polars::prelude::*;

use order_common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use order_common::{OrderType, Side};
use persist_common::{
    FillLiquidity, OrderQueuePositionRecord, SignalBbo, SIGNAL_BBO_BINARY_LEN,
    UNIFORM_ORDER_TAIL_BINARY_LEN,
};

/// Minimal factual order observation for read-only health checks. Uses the same
/// persisted codecs as exports, including terminal unmatched exchange evidence.
#[derive(Debug, Clone)]
pub struct OrderHealthObservation {
    pub venue: String,
    pub symbol: String,
    pub client_order_id: i64,
    pub observed_us: i64,
    pub status: Option<String>,
    pub uniform: bool,
}

/// Reopens read-only on every scan so live WAL updates are visible. A capped
/// scan is an error, never an apparently healthy incomplete order history.
pub fn read_order_health_observations(
    path: &std::path::Path,
    since_us: i64,
    max_records_per_cf: usize,
) -> Result<Vec<OrderHealthObservation>> {
    use rocksdb::{Direction, IteratorMode, Options, DB};
    anyhow::ensure!(path.is_dir(), "order store is not a directory");
    anyhow::ensure!(
        since_us >= 0 && max_records_per_cf > 0,
        "invalid order scan bounds"
    );
    let options = Options::default();
    let names = DB::list_cf(&options, path).context("list order store column families")?;
    let requested = [
        "uniform_orders",
        "order_updates",
        "trade_updates",
        "order_updates_unmatched",
        "trade_updates_unmatched",
    ];
    for name in requested {
        anyhow::ensure!(
            names.iter().any(|n| n == name),
            "missing order column family {name}"
        );
    }
    let db = DB::open_cf_for_read_only(&options, path, names, false)
        .context("open order store read-only")?;
    let start = format!("{since_us:020}");
    let mut observations = Vec::new();
    for name in requested {
        let cf = db
            .cf_handle(name)
            .context("order column family disappeared")?;
        for (index, item) in db
            .iterator_cf(cf, IteratorMode::From(start.as_bytes(), Direction::Forward))
            .enumerate()
        {
            anyhow::ensure!(index < max_records_per_cf, "order scan limit reached in {name}; increase max_order_records or shorten lookback");
            let (key, value) = item.context("read order event")?;
            let ts = i64::try_from(parse_simple_key(std::str::from_utf8(&key)?)?)?;
            let observation = match name {
                "uniform_orders" => {
                    let r = decode_uniform_order_record(&value)
                        .context("decode uniform order health record")?;
                    OrderHealthObservation {
                        venue: r.trading_venue,
                        symbol: r.symbol,
                        client_order_id: r.client_order_id,
                        observed_us: ts.max(r.recv_ts_us).max(r.update_ts),
                        status: Some(r.status),
                        uniform: true,
                    }
                }
                "order_updates" | "order_updates_unmatched" => {
                    let r = decode_order_record(&value).context("decode order health update")?;
                    OrderHealthObservation {
                        venue: r.trading_venue,
                        symbol: r.symbol,
                        client_order_id: r.client_order_id,
                        observed_us: ts,
                        status: Some(r.status),
                        uniform: false,
                    }
                }
                _ => {
                    let r = decode_trade_record(&value).context("decode trade health update")?;
                    OrderHealthObservation {
                        venue: r.trading_venue,
                        symbol: r.symbol,
                        client_order_id: r.client_order_id,
                        observed_us: ts,
                        status: r.order_status,
                        uniform: false,
                    }
                }
            };
            observations.push(observation);
        }
    }
    Ok(observations)
}

#[derive(Debug, Clone, Copy)]
pub struct RangeFilter {
    start_ts: Option<u64>,
    end_ts: Option<u64>,
}

impl RangeFilter {
    pub fn all() -> Self {
        Self {
            start_ts: None,
            end_ts: None,
        }
    }

    pub fn from_bounds(start_ts: u64, end_ts_inclusive: u64) -> Self {
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

pub fn build_order_queue_positions_df(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<DataFrame> {
    let mut key_col = Vec::with_capacity(entries.len());
    let mut ts_col = Vec::with_capacity(entries.len());
    let mut recv_ts_col = Vec::with_capacity(entries.len());
    let mut account_id_col = Vec::with_capacity(entries.len());
    let mut venue_col = Vec::with_capacity(entries.len());
    let mut action_col = Vec::with_capacity(entries.len());
    let mut create_tp_col = Vec::with_capacity(entries.len());
    let mut update_tp_col = Vec::with_capacity(entries.len());
    let mut local_tp_col = Vec::with_capacity(entries.len());
    let mut client_order_id_col = Vec::with_capacity(entries.len());
    let mut tlen_col = Vec::with_capacity(entries.len());
    let mut backlen_col = Vec::with_capacity(entries.len());
    let mut inpos_col = Vec::with_capacity(entries.len());
    let mut dropped = 0usize;

    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes)?;
        let ts_us = parse_prefixed_key(&key)?;
        if !range.contains(ts_us) {
            continue;
        }
        let record = match OrderQueuePositionRecord::from_bytes(&value_bytes) {
            Ok(record) => record,
            Err(_) => {
                dropped += 1;
                continue;
            }
        };
        key_col.push(key);
        ts_col.push(ts_us as i64);
        recv_ts_col.push(record.recv_ts_us);
        account_id_col.push(record.account_id);
        venue_col.push(venue_name(record.venue));
        action_col.push(record.action.as_str().to_string());
        create_tp_col.push(record.create_tp);
        update_tp_col.push(record.update_tp);
        local_tp_col.push(record.local_tp);
        client_order_id_col.push(record.client_order_id);
        tlen_col.push(record.tlen);
        backlen_col.push(record.backlen);
        inpos_col.push(record.inpos);
    }

    if dropped > 0 {
        warn!("order queue position: dropped {dropped} undecodable records");
    }

    Ok(DataFrame::new(vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_col),
        Series::new("recv_ts_us".into(), recv_ts_col),
        Series::new("account_id".into(), account_id_col),
        Series::new("trading_venue".into(), venue_col),
        Series::new("action".into(), action_col),
        Series::new("create_tp".into(), create_tp_col),
        Series::new("update_tp".into(), update_tp_col),
        Series::new("local_tp".into(), local_tp_col),
        Series::new("client_order_id".into(), client_order_id_col),
        Series::new("tlen".into(), tlen_col),
        Series::new("backlen".into(), backlen_col),
        Series::new("inpos".into(), inpos_col),
    ])?)
}

pub fn build_parquet_order_queue_positions(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    let mut df = build_order_queue_positions_df(entries, range)?;
    dataframe_to_parquet_bytes(&mut df)
}

pub fn build_trade_updates_df(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<DataFrame> {
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

    Ok(DataFrame::new(vec![
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
    ])?)
}

pub fn build_parquet_trade_updates(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    let mut df = build_trade_updates_df(entries, range)?;
    dataframe_to_parquet_bytes(&mut df)
}

pub fn build_order_updates_df(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<DataFrame> {
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

    Ok(DataFrame::new(vec![
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
    ])?)
}

pub fn build_parquet_order_updates(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    let mut df = build_order_updates_df(entries, range)?;
    dataframe_to_parquet_bytes(&mut df)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniformOrderExportOptions {
    pub include_signal_hedge_bbo: bool,
}

impl UniformOrderExportOptions {
    pub const fn without_signal_hedge_bbo() -> Self {
        Self {
            include_signal_hedge_bbo: false,
        }
    }
}

impl Default for UniformOrderExportOptions {
    fn default() -> Self {
        Self {
            include_signal_hedge_bbo: true,
        }
    }
}

pub fn build_uniform_orders_df(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<DataFrame> {
    build_uniform_orders_df_with_options(entries, range, UniformOrderExportOptions::default())
}

pub fn build_uniform_orders_df_with_options(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
    options: UniformOrderExportOptions,
) -> Result<DataFrame> {
    let mut key_col = Vec::with_capacity(entries.len());
    let mut ts_col = Vec::with_capacity(entries.len());
    let mut recv_ts_col = Vec::with_capacity(entries.len());
    let mut symbol_col = Vec::with_capacity(entries.len());
    let mut create_ts_col = Vec::with_capacity(entries.len());
    let mut update_ts_col = Vec::with_capacity(entries.len());
    let mut signal_ts_col = Vec::with_capacity(entries.len());
    let mut submit_ts_col = Vec::with_capacity(entries.len());
    let mut local_ts_col = Vec::with_capacity(entries.len());
    let mut mkt_ts_col = Vec::with_capacity(entries.len());
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
    let mut fill_liquidity_col = Vec::with_capacity(entries.len());
    let mut signal_open_venue_col: Vec<Option<String>> = Vec::with_capacity(entries.len());
    let mut signal_open_ts_col = Vec::with_capacity(entries.len());
    let mut signal_open_bid_price_col = Vec::with_capacity(entries.len());
    let mut signal_open_bid_qty_col = Vec::with_capacity(entries.len());
    let mut signal_open_ask_price_col = Vec::with_capacity(entries.len());
    let mut signal_open_ask_qty_col = Vec::with_capacity(entries.len());
    let mut signal_hedge_venue_col: Vec<Option<String>> = Vec::with_capacity(entries.len());
    let mut signal_hedge_ts_col = Vec::with_capacity(entries.len());
    let mut signal_hedge_bid_price_col = Vec::with_capacity(entries.len());
    let mut signal_hedge_bid_qty_col = Vec::with_capacity(entries.len());
    let mut signal_hedge_ask_price_col = Vec::with_capacity(entries.len());
    let mut signal_hedge_ask_qty_col = Vec::with_capacity(entries.len());
    let mut bbo_spread_col = Vec::with_capacity(entries.len());

    let mut dropped = 0usize;
    for (key_bytes, value_bytes) in entries {
        let key = match String::from_utf8(key_bytes) {
            Ok(k) => k,
            Err(err) => {
                warn!("uniform order: drop record with non-utf8 key: {err}");
                dropped += 1;
                continue;
            }
        };
        let ts_us = match parse_simple_key(&key) {
            Ok(v) => v,
            Err(err) => {
                warn!("uniform order: drop record key={key} unparsable: {err:#}");
                dropped += 1;
                continue;
            }
        };
        if !range.contains(ts_us) {
            continue;
        }

        let record = match decode_uniform_order_record(&value_bytes) {
            Ok(r) => r,
            Err(err) => {
                warn!(
                    "uniform order: drop record key={key} payload_len={} decode failed: {err:#}",
                    value_bytes.len()
                );
                dropped += 1;
                continue;
            }
        };
        let DecodedUniformOrderRecord {
            recv_ts_us,
            symbol,
            create_ts,
            update_ts,
            signal_ts,
            submit_ts,
            local_ts,
            mkt_ts,
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
            signal_bbo,
            fill_liquidity,
            bbo_spread,
        } = record;

        key_col.push(key);
        ts_col.push(ts_us as i64);
        recv_ts_col.push(recv_ts_us);
        symbol_col.push(symbol);
        create_ts_col.push(create_ts);
        update_ts_col.push(update_ts);
        signal_ts_col.push(signal_ts);
        submit_ts_col.push(submit_ts);
        local_ts_col.push(local_ts);
        mkt_ts_col.push(mkt_ts);
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
        fill_liquidity_col.push(fill_liquidity.map(FillLiquidity::as_str));
        let signal_open = signal_bbo.and_then(|value| value.open);
        signal_open_venue_col.push(signal_open.map(|leg| venue_name(leg.venue)));
        signal_open_ts_col.push(signal_open.map(|leg| leg.ts));
        signal_open_bid_price_col.push(signal_open.map(|leg| leg.bid_price));
        signal_open_bid_qty_col.push(signal_open.map(|leg| leg.bid_qty));
        signal_open_ask_price_col.push(signal_open.map(|leg| leg.ask_price));
        signal_open_ask_qty_col.push(signal_open.map(|leg| leg.ask_qty));

        let signal_hedge = signal_bbo.and_then(|value| value.hedge);
        signal_hedge_venue_col.push(signal_hedge.map(|leg| venue_name(leg.venue)));
        signal_hedge_ts_col.push(signal_hedge.map(|leg| leg.ts));
        signal_hedge_bid_price_col.push(signal_hedge.map(|leg| leg.bid_price));
        signal_hedge_bid_qty_col.push(signal_hedge.map(|leg| leg.bid_qty));
        signal_hedge_ask_price_col.push(signal_hedge.map(|leg| leg.ask_price));
        signal_hedge_ask_qty_col.push(signal_hedge.map(|leg| leg.ask_qty));
        bbo_spread_col.push(bbo_spread);
    }

    if dropped > 0 {
        warn!("uniform order: dropped {dropped} undecodable records");
    }

    let mut df = DataFrame::new(vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_col),
        Series::new("recv_ts_us".into(), recv_ts_col),
        Series::new("symbol".into(), symbol_col),
        Series::new("create_ts".into(), create_ts_col),
        Series::new("update_ts".into(), update_ts_col),
        Series::new("signal_ts".into(), signal_ts_col),
        Series::new("submit_ts".into(), submit_ts_col),
        Series::new("local_ts".into(), local_ts_col),
        Series::new("mkt_ts".into(), mkt_ts_col),
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
        Series::new("fill_liquidity".into(), fill_liquidity_col),
        Series::new("bbo_spread".into(), bbo_spread_col),
        Series::new("signal_open_venue".into(), signal_open_venue_col.as_slice()),
        Series::new("signal_open_ts".into(), signal_open_ts_col.as_slice()),
        Series::new(
            "signal_open_bid_price".into(),
            signal_open_bid_price_col.as_slice(),
        ),
        Series::new(
            "signal_open_bid_qty".into(),
            signal_open_bid_qty_col.as_slice(),
        ),
        Series::new(
            "signal_open_ask_price".into(),
            signal_open_ask_price_col.as_slice(),
        ),
        Series::new(
            "signal_open_ask_qty".into(),
            signal_open_ask_qty_col.as_slice(),
        ),
        Series::new(
            "signal_hedge_venue".into(),
            signal_hedge_venue_col.as_slice(),
        ),
        Series::new("signal_hedge_ts".into(), signal_hedge_ts_col.as_slice()),
        Series::new(
            "signal_hedge_bid_price".into(),
            signal_hedge_bid_price_col.as_slice(),
        ),
        Series::new(
            "signal_hedge_bid_qty".into(),
            signal_hedge_bid_qty_col.as_slice(),
        ),
        Series::new(
            "signal_hedge_ask_price".into(),
            signal_hedge_ask_price_col.as_slice(),
        ),
        Series::new(
            "signal_hedge_ask_qty".into(),
            signal_hedge_ask_qty_col.as_slice(),
        ),
    ])?;

    if !options.include_signal_hedge_bbo {
        for column in [
            "signal_hedge_venue",
            "signal_hedge_ts",
            "signal_hedge_bid_price",
            "signal_hedge_bid_qty",
            "signal_hedge_ask_price",
            "signal_hedge_ask_qty",
        ] {
            let _ = df.drop_in_place(column)?;
        }
    }

    Ok(df)
}

pub fn build_parquet_uniform_orders(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    build_parquet_uniform_orders_with_options(entries, range, UniformOrderExportOptions::default())
}

pub fn build_parquet_uniform_orders_with_options(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
    options: UniformOrderExportOptions,
) -> Result<Vec<u8>> {
    let mut df = build_uniform_orders_df_with_options(entries, range, options)?;
    dataframe_to_parquet_bytes(&mut df)
}

fn dataframe_to_parquet_bytes(df: &mut DataFrame) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(df)?;
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
    submit_ts: i64,
    local_ts: i64,
    mkt_ts: i64,
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
    signal_bbo: Option<SignalBbo>,
    fill_liquidity: Option<FillLiquidity>,
    bbo_spread: String,
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

    // The current base layout includes submit_ts/local_ts/mkt_ts.
    let submit_ts = read_i64(&mut cursor, "uniform order submit_ts")?;
    let local_ts = read_i64(&mut cursor, "uniform order local_ts")?;
    let mkt_ts = read_i64(&mut cursor, "uniform order mkt_ts")?;

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

    let bbo_spread = if cursor.has_remaining() {
        let bbo_len = read_u16(&mut cursor, "uniform order bbo_spread_len")? as usize;
        read_bytes_as_string(&mut cursor, bbo_len, "uniform order bbo_spread")?
    } else {
        String::new()
    };

    let signal_bbo = if cursor.has_remaining() {
        if !matches!(
            cursor.remaining(),
            SIGNAL_BBO_BINARY_LEN | UNIFORM_ORDER_TAIL_BINARY_LEN
        ) {
            return Err(anyhow!(
                "uniform order tail must contain signal_bbo and optional fill_liquidity, got {} bytes",
                cursor.remaining()
            ));
        }
        let bytes = cursor.copy_to_bytes(SIGNAL_BBO_BINARY_LEN);
        SignalBbo::decode_optional(bytes.as_ref()).map_err(|err| anyhow!(err))?
    } else {
        None
    };

    let fill_liquidity = if cursor.has_remaining() {
        let code = read_u8(&mut cursor, "uniform order fill_liquidity")?;
        Some(
            FillLiquidity::from_u8(code)
                .ok_or_else(|| anyhow!("invalid fill_liquidity: {code}"))?,
        )
    } else {
        None
    };

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
        submit_ts,
        local_ts,
        mkt_ts,
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
        signal_bbo,
        fill_liquidity,
        bbo_spread,
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

fn venue_name(raw: u8) -> String {
    TradingVenue::from_u8(raw)
        .map(|venue| venue.as_str().to_string())
        .unwrap_or_else(|| format!("Venue({raw})"))
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

fn parse_prefixed_key(key: &str) -> Result<u64> {
    key.split(':')
        .next()
        .unwrap_or_default()
        .parse::<u64>()
        .with_context(|| format!("invalid timestamp-prefixed key: {key}"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use persist_common::{OrderQueuePositionAction, SignalBboLeg};

    #[test]
    fn order_queue_positions_df_decodes_lifecycle_snapshot() {
        let venue = TradingVenue::BinanceFutures;
        let record = OrderQueuePositionRecord {
            recv_ts_us: 1_005,
            account_id: "binance-intra-arb01".to_string(),
            venue: venue.to_u8(),
            action: OrderQueuePositionAction::PartiallyFilled,
            create_tp: 900,
            update_tp: 1_000,
            local_tp: 1_002,
            client_order_id: 42,
            tlen: 12.5,
            backlen: 4.0,
            inpos: 8.5,
        };
        let df = build_order_queue_positions_df(
            vec![(
                b"00000000000000001000:02:000000000000002a:00000000000003e8:02".to_vec(),
                record.to_bytes().unwrap(),
            )],
            &RangeFilter::all(),
        )
        .unwrap();

        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column("ts_us").unwrap().i64().unwrap().get(0),
            Some(1_000)
        );
        assert_eq!(
            df.column("account_id").unwrap().str().unwrap().get(0),
            Some("binance-intra-arb01")
        );
        assert_eq!(
            df.column("trading_venue").unwrap().str().unwrap().get(0),
            Some(venue.as_str())
        );
        assert_eq!(
            df.column("action").unwrap().str().unwrap().get(0),
            Some("partially_filled")
        );
        assert_eq!(
            df.column("client_order_id").unwrap().i64().unwrap().get(0),
            Some(42)
        );
        assert_eq!(df.column("tlen").unwrap().f64().unwrap().get(0), Some(12.5));
        assert_eq!(
            df.column("backlen").unwrap().f64().unwrap().get(0),
            Some(4.0)
        );
        assert_eq!(df.column("inpos").unwrap().f64().unwrap().get(0), Some(8.5));
    }

    fn uniform_payload(signal_tail: Option<Option<SignalBbo>>) -> Vec<u8> {
        let symbol = b"BTCUSDT";
        let from_key = b"decision";
        let bbo_spread = b"event-bbo";
        let mut buf = BytesMut::new();
        buf.put_i64_le(10);
        buf.put_u16_le(symbol.len() as u16);
        buf.put_slice(symbol);
        buf.put_i64_le(11);
        buf.put_i64_le(12);
        buf.put_i64_le(13);
        buf.put_i64_le(14);
        buf.put_i64_le(15);
        buf.put_i64_le(16);
        buf.put_i64_le(17);
        buf.put_u8(TradingVenue::BinanceMargin.to_u8());
        buf.put_u8(OrderType::Limit.to_u8());
        buf.put_u8(Side::Buy.to_u8());
        buf.put_f64_le(100.0);
        buf.put_f64_le(0.25);
        buf.put_f64_le(2.0);
        buf.put_f64_le(1.0);
        buf.put_u8(OrderStatus::New.to_u8());
        buf.put_u32_le(from_key.len() as u32);
        buf.put_slice(from_key);
        buf.put_u16_le(bbo_spread.len() as u16);
        buf.put_slice(bbo_spread);
        if let Some(signal_bbo) = signal_tail {
            buf.put_slice(&SignalBbo::encode_optional(signal_bbo));
        }
        buf.to_vec()
    }

    #[test]
    fn order_health_reader_sees_live_wal_without_writing_primary_and_rejects_caps() {
        use rocksdb::{Options, DB};
        let path = std::env::temp_dir().join(format!(
            "fr-health-reader-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let names = [
            "uniform_orders",
            "order_updates",
            "trade_updates",
            "order_updates_unmatched",
            "trade_updates_unmatched",
        ];
        let db = DB::open_cf(&options, &path, names).unwrap();
        let cf = db.cf_handle("uniform_orders").unwrap();
        db.put_cf(cf, format!("{:020}", 1000), uniform_payload(None))
            .unwrap();
        let sequence = db.latest_sequence_number();
        let observations = read_order_health_observations(&path, 0, 100).unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].client_order_id, 17);
        assert_eq!(observations[0].status.as_deref(), Some("NEW"));
        assert_eq!(db.latest_sequence_number(), sequence);
        db.put_cf(cf, format!("{:020}", 2000), uniform_payload(None))
            .unwrap();
        assert_eq!(
            read_order_health_observations(&path, 0, 100).unwrap().len(),
            2
        );
        assert!(read_order_health_observations(&path, 0, 1).is_err());
        assert_eq!(
            read_order_health_observations(&path, 1500, 100)
                .unwrap()
                .len(),
            1
        );
        db.put_cf(cf, format!("{:020}", 3000), b"corrupt").unwrap();
        assert!(read_order_health_observations(&path, 0, 100).is_err());
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn uniform_orders_df_decodes_signal_bbo_and_falls_back_to_null() {
        let open_venue = TradingVenue::BinanceMargin;
        let hedge_venue = TradingVenue::BinanceFutures;
        let signal_bbo = SignalBbo::new(
            Some(SignalBboLeg::new(
                open_venue.to_u8(),
                101,
                100.0,
                2.5,
                100.1,
                3.5,
            )),
            Some(SignalBboLeg::new(
                hedge_venue.to_u8(),
                102,
                99.9,
                4.5,
                100.0,
                5.5,
            )),
        );
        let df = build_uniform_orders_df(
            vec![
                (b"1000".to_vec(), uniform_payload(None)),
                (b"1001".to_vec(), uniform_payload(Some(signal_bbo))),
            ],
            &RangeFilter::all(),
        )
        .unwrap();

        assert_eq!(df.height(), 2);
        let open_venue_col = df.column("signal_open_venue").unwrap().str().unwrap();
        let open_bid_qty_col = df.column("signal_open_bid_qty").unwrap().f64().unwrap();
        let hedge_ts_col = df.column("signal_hedge_ts").unwrap().i64().unwrap();
        let hedge_ask_qty_col = df.column("signal_hedge_ask_qty").unwrap().f64().unwrap();

        assert_eq!(open_venue_col.get(0), None);
        assert_eq!(open_bid_qty_col.get(0), None);
        assert_eq!(hedge_ts_col.get(0), None);
        assert_eq!(open_venue_col.get(1), Some(open_venue.as_str()));
        assert_eq!(open_bid_qty_col.get(1), Some(2.5));
        assert_eq!(hedge_ts_col.get(1), Some(102));
        assert_eq!(hedge_ask_qty_col.get(1), Some(5.5));
    }

    #[test]
    fn uniform_orders_df_can_omit_signal_hedge_bbo_columns() {
        let df = build_uniform_orders_df_with_options(
            vec![(b"1000".to_vec(), uniform_payload(None))],
            &RangeFilter::all(),
            UniformOrderExportOptions::without_signal_hedge_bbo(),
        )
        .unwrap();

        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 29);
        assert!(df.column("signal_open_venue").is_ok());
        for column in [
            "signal_hedge_venue",
            "signal_hedge_ts",
            "signal_hedge_bid_price",
            "signal_hedge_bid_qty",
            "signal_hedge_ask_price",
            "signal_hedge_ask_qty",
        ] {
            assert!(df.column(column).is_err(), "{column} should be omitted");
        }
    }

    #[test]
    fn uniform_export_preserves_each_fill_role_and_historical_absence() {
        let mut entries = vec![(b"1000".to_vec(), uniform_payload(None))];
        for (offset, role) in [
            FillLiquidity::Unknown,
            FillLiquidity::Maker,
            FillLiquidity::Taker,
        ]
        .into_iter()
        .enumerate()
        {
            let mut payload = uniform_payload(Some(None));
            payload.push(role as u8);
            let decoded = decode_uniform_order_record(&payload).unwrap();
            assert_eq!(decoded.fill_liquidity, Some(role));
            assert_eq!(decoded.amount_update, 1.0);
            entries.push(((1001 + offset).to_string().into_bytes(), payload));
        }
        let df = build_uniform_orders_df(entries, &RangeFilter::all()).unwrap();
        assert_eq!(df.height(), 4);
        let roles = df.column("fill_liquidity").unwrap().str().unwrap();
        assert_eq!(roles.get(0), None);
        assert_eq!(roles.get(1), Some("unknown"));
        assert_eq!(roles.get(2), Some("maker"));
        assert_eq!(roles.get(3), Some("taker"));

        let mut invalid = uniform_payload(Some(None));
        invalid.push(3);
        assert!(decode_uniform_order_record(&invalid).is_err());
        invalid.push(0);
        assert!(decode_uniform_order_record(&invalid).is_err());
    }
}
