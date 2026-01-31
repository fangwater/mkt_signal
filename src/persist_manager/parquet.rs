use anyhow::{anyhow, Context, Result};
use bytes::{Buf, Bytes};
use polars::prelude::ParquetWriter;
use polars::prelude::*;

use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{ExecutionType, OrderStatus, SignalBytes, TimeInForce, TradingVenue};
use crate::signal::hedge_signal::ArbHedgeCtx;
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::record::SignalRecordMessage;

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

fn parse_key(key: &str) -> Result<(u64, i32)> {
    let mut parts = key.splitn(2, '_');
    let ts_str = parts
        .next()
        .ok_or_else(|| anyhow!("invalid key format: missing timestamp"))?;
    let strategy_str = parts
        .next()
        .ok_or_else(|| anyhow!("invalid key format: missing strategy id"))?;
    let ts_us = ts_str
        .parse::<u64>()
        .with_context(|| format!("invalid timestamp part: {}", ts_str))?;
    let strategy_id = strategy_str
        .parse::<i32>()
        .with_context(|| format!("invalid strategy id part: {}", strategy_str))?;
    Ok((ts_us, strategy_id))
}

pub(crate) fn build_parquet_open(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    let mut key_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut ts_us_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut strategy_id_col: Vec<i32> = Vec::with_capacity(entries.len());
    let mut create_ts_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut opening_venue_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut opening_symbol_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut opening_bid0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut opening_ask0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut opening_ts_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut hedging_venue_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut hedging_symbol_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut hedging_bid0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut hedging_ask0_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut hedging_ts_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut amount_col: Vec<f32> = Vec::with_capacity(entries.len());
    let mut side_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut order_type_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut price_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut price_tick_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut exp_time_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut price_offset_col: Vec<f64> = Vec::with_capacity(entries.len());
    let mut hedge_timeout_us_col: Vec<i64> = Vec::with_capacity(entries.len());
    let mut funding_ma_col: Vec<Option<f64>> = Vec::with_capacity(entries.len());
    let mut predicted_funding_rate_col: Vec<Option<f64>> = Vec::with_capacity(entries.len());
    let mut loan_rate_col: Vec<Option<f64>> = Vec::with_capacity(entries.len());

    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes)?;
        let (ts_us, strategy_id) = parse_key(&key)?;
        if !range.contains(ts_us) {
            continue;
        }
        let record = SignalRecordMessage::from_bytes(Bytes::from(value_bytes))?;
        let ctx = ArbOpenCtx::from_bytes(Bytes::from(record.context.clone()))
            .map_err(|err| anyhow!("failed to decode ArbOpenCtx: {err}"))?;

        key_col.push(key);
        ts_us_col.push(ts_us as i64);
        strategy_id_col.push(strategy_id);
        create_ts_col.push(ctx.create_ts);
        opening_venue_col.push(
            TradingVenue::from_u8(ctx.opening_leg.venue)
                .map(|v| v.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        opening_symbol_col.push(ctx.get_opening_symbol());
        opening_bid0_col.push(ctx.opening_leg.bid0);
        opening_ask0_col.push(ctx.opening_leg.ask0);
        opening_ts_col.push(ctx.opening_leg.ts);
        hedging_venue_col.push(
            TradingVenue::from_u8(ctx.hedging_leg.venue)
                .map(|v| v.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        hedging_symbol_col.push(ctx.get_hedging_symbol());
        hedging_bid0_col.push(ctx.hedging_leg.bid0);
        hedging_ask0_col.push(ctx.hedging_leg.ask0);
        hedging_ts_col.push(ctx.hedging_leg.ts);
        amount_col.push(ctx.amount);
        side_col.push(
            ctx.get_side()
                .map(|s| s.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        order_type_col.push(
            ctx.get_order_type()
                .map(|t| t.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        price_col.push(ctx.price);
        price_tick_col.push(ctx.price_tick);
        exp_time_col.push(ctx.exp_time);
        price_offset_col.push(ctx.price_offset);
        hedge_timeout_us_col.push(ctx.hedge_timeout_us);
        funding_ma_col.push(if ctx.funding_ma != 0.0 {
            Some(ctx.funding_ma)
        } else {
            None
        });
        predicted_funding_rate_col.push(if ctx.predicted_funding_rate != 0.0 {
            Some(ctx.predicted_funding_rate)
        } else {
            None
        });
        loan_rate_col.push(if ctx.loan_rate != 0.0 {
            Some(ctx.loan_rate)
        } else {
            None
        });
    }

    let mut df = DataFrame::new(vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_us_col),
        Series::new("strategy_id".into(), strategy_id_col),
        Series::new("create_ts".into(), create_ts_col),
        Series::new("opening_venue".into(), opening_venue_col),
        Series::new("opening_symbol".into(), opening_symbol_col),
        Series::new("opening_bid0".into(), opening_bid0_col),
        Series::new("opening_ask0".into(), opening_ask0_col),
        Series::new("opening_ts".into(), opening_ts_col),
        Series::new("hedging_venue".into(), hedging_venue_col),
        Series::new("hedging_symbol".into(), hedging_symbol_col),
        Series::new("hedging_bid0".into(), hedging_bid0_col),
        Series::new("hedging_ask0".into(), hedging_ask0_col),
        Series::new("hedging_ts".into(), hedging_ts_col),
        Series::new("amount".into(), amount_col),
        Series::new("side".into(), side_col),
        Series::new("order_type".into(), order_type_col),
        Series::new("price".into(), price_col),
        Series::new("price_tick".into(), price_tick_col),
        Series::new("exp_time".into(), exp_time_col),
        Series::new("price_offset".into(), price_offset_col),
        Series::new("hedge_timeout_us".into(), hedge_timeout_us_col),
        Series::new("funding_ma".into(), funding_ma_col.as_slice()),
        Series::new(
            "predicted_funding_rate".into(),
            predicted_funding_rate_col.as_slice(),
        ),
        Series::new("loan_rate".into(), loan_rate_col.as_slice()),
    ])?;

    let mut buffer = Vec::new();
    ParquetWriter::new(&mut buffer).finish(&mut df)?;
    Ok(buffer)
}

pub(crate) fn build_parquet_cancel(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    let mut key_col = Vec::with_capacity(entries.len());
    let mut ts_us_col = Vec::with_capacity(entries.len());
    let mut strategy_id_col = Vec::with_capacity(entries.len());
    let mut opening_venue_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut opening_symbol_col = Vec::with_capacity(entries.len());
    let mut opening_bid0_col = Vec::with_capacity(entries.len());
    let mut opening_ask0_col = Vec::with_capacity(entries.len());
    let mut opening_ts_col = Vec::with_capacity(entries.len());
    let mut hedging_venue_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut hedging_symbol_col = Vec::with_capacity(entries.len());
    let mut hedging_bid0_col = Vec::with_capacity(entries.len());
    let mut hedging_ask0_col = Vec::with_capacity(entries.len());
    let mut hedging_ts_col = Vec::with_capacity(entries.len());
    let mut trigger_ts_col = Vec::with_capacity(entries.len());

    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes)?;
        let (ts_us, strategy_id) = parse_key(&key)?;
        if !range.contains(ts_us) {
            continue;
        }
        let record = SignalRecordMessage::from_bytes(Bytes::from(value_bytes))?;
        let ctx = ArbCancelCtx::from_bytes(Bytes::from(record.context.clone()))
            .map_err(|err| anyhow!("failed to decode ArbCancelCtx: {err}"))?;

        key_col.push(key);
        ts_us_col.push(ts_us as i64);
        strategy_id_col.push(strategy_id);
        opening_venue_col.push(
            TradingVenue::from_u8(ctx.opening_leg.venue)
                .map(|v| v.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        opening_symbol_col.push(ctx.get_opening_symbol());
        opening_bid0_col.push(ctx.opening_leg.bid0);
        opening_ask0_col.push(ctx.opening_leg.ask0);
        opening_ts_col.push(ctx.opening_leg.ts);
        hedging_venue_col.push(
            TradingVenue::from_u8(ctx.hedging_leg.venue)
                .map(|v| v.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        hedging_symbol_col.push(ctx.get_hedging_symbol());
        hedging_bid0_col.push(ctx.hedging_leg.bid0);
        hedging_ask0_col.push(ctx.hedging_leg.ask0);
        hedging_ts_col.push(ctx.hedging_leg.ts);
        trigger_ts_col.push(ctx.trigger_ts);
    }

    let columns = vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_us_col),
        Series::new("strategy_id".into(), strategy_id_col),
        Series::new("opening_venue".into(), opening_venue_col),
        Series::new("opening_symbol".into(), opening_symbol_col),
        Series::new("opening_bid0".into(), opening_bid0_col),
        Series::new("opening_ask0".into(), opening_ask0_col),
        Series::new("opening_ts".into(), opening_ts_col),
        Series::new("hedging_venue".into(), hedging_venue_col),
        Series::new("hedging_symbol".into(), hedging_symbol_col),
        Series::new("hedging_bid0".into(), hedging_bid0_col),
        Series::new("hedging_ask0".into(), hedging_ask0_col),
        Series::new("hedging_ts".into(), hedging_ts_col),
        Series::new("trigger_ts".into(), trigger_ts_col),
    ];

    let mut df = DataFrame::new(columns)?;
    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(&mut df)?;
    Ok(buf)
}

pub(crate) fn build_parquet_hedge(
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    range: &RangeFilter,
) -> Result<Vec<u8>> {
    let mut key_col = Vec::with_capacity(entries.len());
    let mut ts_us_col = Vec::with_capacity(entries.len());
    let mut strategy_id_col = Vec::with_capacity(entries.len());
    let mut record_ts_col = Vec::with_capacity(entries.len());
    let mut ctx_strategy_id_col = Vec::with_capacity(entries.len());
    let mut client_order_id_col = Vec::with_capacity(entries.len());
    let mut hedge_qty_col = Vec::with_capacity(entries.len());
    let mut hedge_side_col = Vec::with_capacity(entries.len());
    let mut limit_price_col = Vec::with_capacity(entries.len());
    let mut price_tick_col = Vec::with_capacity(entries.len());
    let mut maker_only_col = Vec::with_capacity(entries.len());
    let mut exp_time_col = Vec::with_capacity(entries.len());
    let mut opening_venue_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut opening_symbol_col = Vec::with_capacity(entries.len());
    let mut opening_bid0_col = Vec::with_capacity(entries.len());
    let mut opening_ask0_col = Vec::with_capacity(entries.len());
    let mut opening_ts_col = Vec::with_capacity(entries.len());
    let mut hedging_venue_col: Vec<String> = Vec::with_capacity(entries.len());
    let mut hedging_symbol_col = Vec::with_capacity(entries.len());
    let mut hedging_bid0_col = Vec::with_capacity(entries.len());
    let mut hedging_ask0_col = Vec::with_capacity(entries.len());
    let mut hedging_ts_col = Vec::with_capacity(entries.len());
    let mut market_ts_col = Vec::with_capacity(entries.len());
    let mut price_offset_col = Vec::with_capacity(entries.len());

    for (key_bytes, value_bytes) in entries {
        let key = String::from_utf8(key_bytes)?;
        let (ts_us, strategy_id) = parse_key(&key)?;
        if !range.contains(ts_us) {
            continue;
        }
        let record = SignalRecordMessage::from_bytes(Bytes::from(value_bytes))?;
        let ctx = ArbHedgeCtx::from_bytes(Bytes::from(record.context.clone()))
            .map_err(|err| anyhow!("failed to decode ArbHedgeCtx: {err}"))?;

        key_col.push(key);
        ts_us_col.push(ts_us as i64);
        strategy_id_col.push(strategy_id);
        record_ts_col.push(record.timestamp_us);
        ctx_strategy_id_col.push(ctx.strategy_id);
        client_order_id_col.push(ctx.client_order_id);
        hedge_qty_col.push(ctx.hedge_qty);
        hedge_side_col.push(
            ctx.get_side()
                .map(|s| s.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        limit_price_col.push(ctx.limit_price);
        price_tick_col.push(ctx.price_tick);
        maker_only_col.push(ctx.maker_only);
        exp_time_col.push(ctx.exp_time);
        opening_venue_col.push(
            TradingVenue::from_u8(ctx.opening_leg.venue)
                .map(|v| v.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        opening_symbol_col.push(ctx.get_opening_symbol());
        opening_bid0_col.push(ctx.opening_leg.bid0);
        opening_ask0_col.push(ctx.opening_leg.ask0);
        opening_ts_col.push(ctx.opening_leg.ts);
        hedging_venue_col.push(
            TradingVenue::from_u8(ctx.hedging_leg.venue)
                .map(|v| v.as_str().to_string())
                .unwrap_or("Unknown".to_string()),
        );
        hedging_symbol_col.push(ctx.get_hedging_symbol());
        hedging_bid0_col.push(ctx.hedging_leg.bid0);
        hedging_ask0_col.push(ctx.hedging_leg.ask0);
        hedging_ts_col.push(ctx.hedging_leg.ts);
        market_ts_col.push(ctx.market_ts);
        price_offset_col.push(ctx.price_offset);
    }

    let columns = vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_us_col),
        Series::new("strategy_id".into(), strategy_id_col),
        Series::new("record_ts_us".into(), record_ts_col),
        Series::new("ctx_strategy_id".into(), ctx_strategy_id_col),
        Series::new("client_order_id".into(), client_order_id_col),
        Series::new("hedge_qty".into(), hedge_qty_col),
        Series::new("hedge_side".into(), hedge_side_col),
        Series::new("limit_price".into(), limit_price_col),
        Series::new("price_tick".into(), price_tick_col),
        Series::new("maker_only".into(), maker_only_col),
        Series::new("exp_time".into(), exp_time_col),
        Series::new("opening_venue".into(), opening_venue_col),
        Series::new("opening_symbol".into(), opening_symbol_col),
        Series::new("opening_bid0".into(), opening_bid0_col),
        Series::new("opening_ask0".into(), opening_ask0_col),
        Series::new("opening_ts".into(), opening_ts_col),
        Series::new("hedging_venue".into(), hedging_venue_col),
        Series::new("hedging_symbol".into(), hedging_symbol_col),
        Series::new("hedging_bid0".into(), hedging_bid0_col),
        Series::new("hedging_ask0".into(), hedging_ask0_col),
        Series::new("hedging_ts".into(), hedging_ts_col),
        Series::new("market_ts".into(), market_ts_col),
        Series::new("price_offset".into(), price_offset_col),
    ];

    let mut df = DataFrame::new(columns)?;
    let mut buf = Vec::new();
    ParquetWriter::new(&mut buf).finish(&mut df)?;
    Ok(buf)
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
    let mut trade_id_col = Vec::with_capacity(entries.len());
    let mut order_id_col = Vec::with_capacity(entries.len());
    let mut client_order_id_col = Vec::with_capacity(entries.len());
    let mut side_col = Vec::with_capacity(entries.len());
    let mut price_col = Vec::with_capacity(entries.len());
    let mut qty_col = Vec::with_capacity(entries.len());
    let mut commission_col = Vec::with_capacity(entries.len());
    let mut commission_asset_col = Vec::with_capacity(entries.len());
    let mut is_maker_col = Vec::with_capacity(entries.len());
    let mut realized_col = Vec::with_capacity(entries.len());
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
            trade_id,
            order_id,
            client_order_id,
            side,
            price,
            quantity,
            commission,
            commission_asset,
            is_maker,
            realized_pnl,
            trading_venue,
            cumulative_filled_quantity,
            order_status,
        } = record;
        key_col.push(key);
        ts_col.push(ts_us as i64);
        event_time_col.push(event_time);
        trade_time_col.push(trade_time);
        symbol_col.push(symbol);
        trade_id_col.push(trade_id);
        order_id_col.push(order_id);
        client_order_id_col.push(client_order_id);
        side_col.push(side);
        price_col.push(price);
        qty_col.push(quantity);
        commission_col.push(commission);
        commission_asset_col.push(commission_asset);
        is_maker_col.push(is_maker);
        realized_col.push(realized_pnl);
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
        Series::new("trade_id".into(), trade_id_col),
        Series::new("order_id".into(), order_id_col),
        Series::new("client_order_id".into(), client_order_id_col),
        Series::new("side".into(), side_col),
        Series::new("price".into(), price_col),
        Series::new("quantity".into(), qty_col),
        Series::new("commission".into(), commission_col),
        Series::new("commission_asset".into(), commission_asset_col),
        Series::new("is_maker".into(), is_maker_col),
        Series::new("realized_pnl".into(), realized_col),
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
    let mut last_exec_qty_col = Vec::with_capacity(entries.len());
    let mut cumulative_col = Vec::with_capacity(entries.len());
    let mut status_col = Vec::with_capacity(entries.len());
    let mut raw_status_col = Vec::with_capacity(entries.len());
    let mut exec_type_col = Vec::with_capacity(entries.len());
    let mut raw_exec_type_col = Vec::with_capacity(entries.len());
    let mut venue_col = Vec::with_capacity(entries.len());
    let mut avg_price_col = Vec::with_capacity(entries.len());
    let mut last_price_col = Vec::with_capacity(entries.len());
    let mut business_unit_col = Vec::with_capacity(entries.len());

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
            last_executed_qty,
            cumulative_filled_quantity,
            status,
            raw_status,
            execution_type,
            raw_execution_type,
            trading_venue,
            average_price,
            last_executed_price,
            business_unit,
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
        last_exec_qty_col.push(last_executed_qty);
        cumulative_col.push(cumulative_filled_quantity);
        status_col.push(status);
        raw_status_col.push(raw_status);
        exec_type_col.push(execution_type);
        raw_exec_type_col.push(raw_execution_type);
        venue_col.push(trading_venue);
        avg_price_col.push(average_price);
        last_price_col.push(last_executed_price);
        business_unit_col.push(business_unit);
    }

    let mut df = DataFrame::new(vec![
        Series::new("key".into(), key_col),
        Series::new("ts_us".into(), ts_col),
        Series::new("event_time".into(), event_time_col),
        Series::new("symbol".into(), symbol_col),
        Series::new("order_id".into(), order_id_col),
        Series::new("client_order_id".into(), client_order_id_col),
        Series::new("client_order_id_str".into(), client_order_id_str_col.as_slice()),
        Series::new("side".into(), side_col),
        Series::new("order_type".into(), order_type_col),
        Series::new("time_in_force".into(), tif_col),
        Series::new("price".into(), price_col),
        Series::new("quantity".into(), qty_col),
        Series::new("last_executed_qty".into(), last_exec_qty_col),
        Series::new("cumulative_filled_quantity".into(), cumulative_col),
        Series::new("status".into(), status_col),
        Series::new("raw_status".into(), raw_status_col),
        Series::new("execution_type".into(), exec_type_col),
        Series::new("raw_execution_type".into(), raw_exec_type_col),
        Series::new("trading_venue".into(), venue_col),
        Series::new("average_price".into(), avg_price_col.as_slice()),
        Series::new("last_executed_price".into(), last_price_col.as_slice()),
        Series::new("business_unit".into(), business_unit_col.as_slice()),
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
    trade_id: i64,
    order_id: i64,
    client_order_id: i64,
    side: String,
    price: f64,
    quantity: f64,
    commission: f64,
    commission_asset: String,
    is_maker: bool,
    realized_pnl: f64,
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
    last_executed_qty: f64,
    cumulative_filled_quantity: f64,
    status: String,
    raw_status: String,
    execution_type: String,
    raw_execution_type: String,
    trading_venue: String,
    average_price: Option<f64>,
    last_executed_price: Option<f64>,
    business_unit: Option<String>,
}

fn decode_trade_record(bytes: &[u8]) -> Result<DecodedTradeRecord> {
    let mut cursor = Bytes::copy_from_slice(bytes);
    let _recv_ts_us = read_i64(&mut cursor, "trade update recv_ts_us")?;
    let event_time = read_i64(&mut cursor, "trade update event_time")?;
    let trade_time = read_i64(&mut cursor, "trade update trade_time")?;
    let symbol = read_string(&mut cursor)?;
    let trade_id = read_i64(&mut cursor, "trade update trade_id")?;
    let order_id = read_i64(&mut cursor, "trade update order_id")?;
    let client_order_id = read_i64(&mut cursor, "trade update client_order_id")?;
    let side_raw = read_u8(&mut cursor, "trade update side")?;
    let price = read_f64(&mut cursor, "trade update price")?;
    let quantity = read_f64(&mut cursor, "trade update quantity")?;
    let commission = read_f64(&mut cursor, "trade update commission")?;
    let commission_asset = read_string(&mut cursor)?;
    let is_maker = read_u8(&mut cursor, "trade update is_maker")? != 0;
    let realized_pnl = read_f64(&mut cursor, "trade update realized_pnl")?;
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
        trade_id,
        order_id,
        client_order_id,
        side: side_str,
        price,
        quantity,
        commission,
        commission_asset,
        is_maker,
        realized_pnl,
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
    let last_executed_qty = read_f64(&mut cursor, "order update last_exec_qty")?;
    let cumulative_filled_quantity = read_f64(&mut cursor, "order update cumulative_qty")?;
    let status_raw = read_u8(&mut cursor, "order update status")?;
    let raw_status = read_string(&mut cursor)?;
    let execution_type_raw = read_u8(&mut cursor, "order update execution_type")?;
    let raw_execution_type = read_string(&mut cursor)?;
    let trading_venue_raw = read_u8(&mut cursor, "order update trading_venue")?;
    let average_price = read_opt_f64(&mut cursor)?;
    let last_executed_price = read_opt_f64(&mut cursor)?;
    let business_unit = read_opt_string(&mut cursor)?;

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
        last_executed_qty,
        cumulative_filled_quantity,
        status,
        raw_status,
        execution_type,
        raw_execution_type,
        trading_venue,
        average_price,
        last_executed_price,
        business_unit,
    })
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

fn read_opt_f64(cursor: &mut Bytes) -> Result<Option<f64>> {
    if !cursor.has_remaining() {
        return Err(anyhow!("payload too short to read f64 flag"));
    }
    let flag = cursor.get_u8();
    if flag == 0 {
        return Ok(None);
    }
    if cursor.remaining() < 8 {
        return Err(anyhow!("payload too short to read f64 value"));
    }
    Ok(Some(cursor.get_f64_le()))
}

fn read_i64(cursor: &mut Bytes, field: &str) -> Result<i64> {
    if cursor.remaining() < 8 {
        return Err(anyhow!("payload too short to read {field}"));
    }
    Ok(cursor.get_i64_le())
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
        4 => Some(OrderStatus::Rejected),
        5 => Some(OrderStatus::Expired),
        6 => Some(OrderStatus::Triggered),
        _ => None,
    }
}
