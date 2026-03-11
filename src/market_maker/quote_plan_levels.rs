use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::tick_math::QuantizedValue;
use crate::market_maker::order_align::{align_order_for_venue, min_qty_symbol_key};
use crate::pre_trade::order_manager::Side;
use crate::signal::common::{align_price_ceil, align_price_floor, TradingVenue};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;

#[derive(Debug, Clone, Copy)]
pub struct QuotePlanLevelSpec {
    pub side: Side,
    pub side_level_index: usize,
    pub offset: f64,
    pub base_price: f64,
}

#[derive(Debug, Clone)]
pub struct QuotePlanLevel {
    pub level_index: usize,
    pub side_level_index: usize,
    pub side: Side,
    pub offset: f64,
    pub base_price: f64,
    pub raw_price: f64,
    pub raw_qty: f64,
    pub aligned_price: f64,
    pub aligned_qty: f64,
    pub price_tick_count: i64,
    pub qty_tick_count: i64,
}

pub fn build_quote_plan_levels(
    venue: TradingVenue,
    symbol: &str,
    order_amount_u: f64,
    specs: &[QuotePlanLevelSpec],
    table: &VenueMinQtyTable,
) -> Result<(f64, f64, Vec<QuotePlanLevel>), String> {
    if symbol.trim().is_empty() {
        return Err("symbol is empty".to_string());
    }
    if !(order_amount_u.is_finite() && order_amount_u > 0.0) {
        return Err(format!(
            "invalid order_amount_u={} (must be finite and >0)",
            order_amount_u
        ));
    }
    if specs.is_empty() {
        return Err("empty quote plan specs".to_string());
    }

    let trade_symbol = normalize_symbol_for_venue(symbol, venue);
    if trade_symbol.is_empty() {
        return Err(format!("normalized symbol is empty: {}", symbol));
    }

    let symbol_key = min_qty_symbol_key(venue, &trade_symbol);
    let price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
    let qty_tick = table.step_size(&symbol_key).unwrap_or(0.0);
    if price_tick <= 0.0 || qty_tick <= 0.0 {
        return Err(format!(
            "missing tick for {} (price_tick={}, qty_tick={})",
            symbol_key, price_tick, qty_tick
        ));
    }

    let mut levels = Vec::with_capacity(specs.len());
    for (idx, spec) in specs.iter().copied().enumerate() {
        if !spec.base_price.is_finite() || spec.base_price <= 0.0 {
            return Err(format!(
                "invalid base_price symbol={} idx={} side_idx={} base_price={}",
                trade_symbol, idx, spec.side_level_index, spec.base_price
            ));
        }
        if !spec.offset.is_finite() || spec.offset < 0.0 {
            return Err(format!(
                "invalid offset symbol={} idx={} side_idx={} offset={}",
                trade_symbol, idx, spec.side_level_index, spec.offset
            ));
        }

        let raw_price = match spec.side {
            Side::Sell => spec.base_price * (1.0 + spec.offset),
            Side::Buy => spec.base_price * (1.0 - spec.offset),
        };
        if !raw_price.is_finite() || raw_price <= 0.0 {
            return Err(format!(
                "invalid raw_price symbol={} side={:?} idx={} side_idx={} raw_price={}",
                trade_symbol, spec.side, idx, spec.side_level_index, raw_price
            ));
        }

        let aligned_raw_price = match spec.side {
            Side::Sell => align_price_ceil(raw_price, price_tick),
            Side::Buy => align_price_floor(raw_price, price_tick),
        };
        if !aligned_raw_price.is_finite() || aligned_raw_price <= 0.0 {
            return Err(format!(
                "aligned raw_price invalid symbol={} side={:?} idx={} side_idx={} aligned_raw_price={}",
                trade_symbol, spec.side, idx, spec.side_level_index, aligned_raw_price
            ));
        }

        let raw_qty = order_amount_u / aligned_raw_price;
        if !raw_qty.is_finite() || raw_qty <= 0.0 {
            return Err(format!(
                "invalid raw_qty symbol={} side={:?} idx={} side_idx={} raw_qty={}",
                trade_symbol, spec.side, idx, spec.side_level_index, raw_qty
            ));
        }

        let (aligned_qty, aligned_price) =
            align_order_for_venue(venue, &symbol_key, raw_qty, aligned_raw_price, table)?;

        let Some(price_qv) = QuantizedValue::encode_floor(aligned_price, price_tick) else {
            return Err(format!(
                "invalid aligned_price quantization symbol={} side={:?} idx={} side_idx={} aligned_price={}",
                trade_symbol, spec.side, idx, spec.side_level_index, aligned_price
            ));
        };
        let Some(amount_qv) = QuantizedValue::encode_floor(aligned_qty, qty_tick) else {
            return Err(format!(
                "invalid aligned_qty quantization symbol={} side={:?} idx={} side_idx={} aligned_qty={}",
                trade_symbol, spec.side, idx, spec.side_level_index, aligned_qty
            ));
        };
        if price_qv.get_count() <= 0 || amount_qv.get_count() <= 0 {
            return Err(format!(
                "non-positive aligned qv symbol={} side={:?} idx={} side_idx={} price_cnt={} qty_cnt={}",
                trade_symbol,
                spec.side,
                idx,
                spec.side_level_index,
                price_qv.get_count(),
                amount_qv.get_count()
            ));
        }

        let aligned_price_v = price_qv.get_val();
        let aligned_qty_v = amount_qv.get_val();
        let offset = ((aligned_price_v - spec.base_price) / spec.base_price).abs();
        levels.push(QuotePlanLevel {
            level_index: idx,
            side_level_index: spec.side_level_index,
            side: spec.side,
            offset,
            base_price: spec.base_price,
            raw_price,
            raw_qty,
            aligned_price: aligned_price_v,
            aligned_qty: aligned_qty_v,
            price_tick_count: price_qv.get_count(),
            qty_tick_count: amount_qv.get_count(),
        });
    }

    Ok((price_tick, qty_tick, levels))
}
