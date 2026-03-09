use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::tick_math::QuantizedValue;
use crate::funding_rate::common::Quote;
use crate::market_maker::order_align::{align_order_for_venue, min_qty_symbol_key};
use crate::pre_trade::order_manager::Side;
use crate::signal::common::{align_price_ceil, align_price_floor, TradingVenue};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;

#[derive(Debug, Clone, Copy)]
pub struct MmVolatilityBand {
    pub mid_price: f64,
    pub lower_price: f64,
    pub upper_price: f64,
    pub volatility: f64,
}

#[derive(Debug, Clone)]
pub struct MmOpenPlanLevel {
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

#[derive(Debug, Clone)]
pub struct MmOpenQuotePlan {
    pub symbol: String,
    pub quote: Quote,
    pub now_us: i64,
    pub exp_time_us: i64,
    pub order_amount_u: f64,
    pub orders_per_round: u32,
    pub price_tick: f64,
    pub qty_tick: f64,
    pub band: MmVolatilityBand,
    pub levels: Vec<MmOpenPlanLevel>,
}

fn build_level_prices(mid: f64, vol: f64, levels_per_side: usize) -> Vec<(Side, usize, f64)> {
    if levels_per_side == 0 {
        return Vec::new();
    }
    if !mid.is_finite() || mid <= 0.0 || !vol.is_finite() || vol < 0.0 {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(levels_per_side.saturating_mul(2));
    let step = vol / levels_per_side as f64;
    for side_level_index in 1..=levels_per_side {
        let offset = step * side_level_index as f64;
        let sell_price = mid * (1.0 + offset);
        let buy_price = mid * (1.0 - offset);
        out.push((Side::Sell, side_level_index, sell_price));
        out.push((Side::Buy, side_level_index, buy_price));
    }

    out
}

pub fn build_default_hedge_offsets(orders_per_round: u32) -> Vec<f64> {
    // Keep the legacy hedge responder behavior with deterministic offsets.
    // This list is consumed after open fills; it keeps per-side level count.
    let levels = orders_per_round.max(1) as usize;
    (1..=levels).map(|idx| 0.0002 * idx as f64).collect()
}

#[allow(clippy::too_many_arguments)]
pub fn build_mm_open_quote_plan(
    venue: TradingVenue,
    symbol: &str,
    quote: Quote,
    order_amount_u: f64,
    orders_per_round: u32,
    open_order_ttl_us: i64,
    volatility: f64,
    now_us: i64,
    table: &VenueMinQtyTable,
) -> Result<MmOpenQuotePlan, String> {
    if symbol.trim().is_empty() {
        return Err("symbol is empty".to_string());
    }
    if quote.bid <= 0.0 || quote.ask <= 0.0 || quote.bid >= quote.ask {
        return Err(format!(
            "invalid quote bid={} ask={} symbol={}",
            quote.bid, quote.ask, symbol
        ));
    }
    if !(order_amount_u.is_finite() && order_amount_u > 0.0) {
        return Err(format!(
            "invalid order_amount_u={} (must be finite and >0)",
            order_amount_u
        ));
    }
    if orders_per_round == 0 {
        return Err("orders_per_round must be > 0".to_string());
    }

    let trade_symbol = normalize_symbol_for_venue(symbol, venue);
    if trade_symbol.is_empty() {
        return Err(format!("normalized symbol is empty: {}", symbol));
    }

    let symbol_key_for_tick = min_qty_symbol_key(venue, &trade_symbol);
    let price_tick = table.price_tick(&symbol_key_for_tick).unwrap_or(0.0);
    let qty_tick = table.step_size(&symbol_key_for_tick).unwrap_or(0.0);
    if price_tick <= 0.0 || qty_tick <= 0.0 {
        return Err(format!(
            "missing tick for {} (price_tick={}, qty_tick={})",
            symbol_key_for_tick, price_tick, qty_tick
        ));
    }

    let mid_price = (quote.bid + quote.ask) * 0.5;
    if !mid_price.is_finite() || mid_price <= 0.0 {
        return Err(format!(
            "invalid mid_price symbol={} bid={} ask={}",
            trade_symbol, quote.bid, quote.ask
        ));
    }

    if !volatility.is_finite() {
        return Err(format!(
            "invalid volatility symbol={} volatility={}",
            trade_symbol, volatility
        ));
    }
    let vol = volatility;
    let lower_price = mid_price * (1.0 - vol);
    let upper_price = mid_price * (1.0 + vol);
    let level_points = build_level_prices(mid_price, vol, orders_per_round as usize);
    if level_points.is_empty() {
        return Err(format!(
            "empty level prices symbol={} mid={:.8} lower={:.8} upper={:.8} levels_per_side={}",
            trade_symbol, mid_price, lower_price, upper_price, orders_per_round
        ));
    }

    let mut levels = Vec::with_capacity(level_points.len());
    for (idx, (side, side_level_index, raw_price)) in level_points.iter().copied().enumerate() {
        if !raw_price.is_finite() || raw_price <= 0.0 {
            return Err(format!(
                "invalid raw_price symbol={} side={:?} idx={} side_idx={} raw_price={}",
                trade_symbol, side, idx, side_level_index, raw_price
            ));
        }

        let aligned_raw_price = match side {
            Side::Sell => align_price_ceil(raw_price, price_tick),
            Side::Buy => align_price_floor(raw_price, price_tick),
        };
        if !aligned_raw_price.is_finite() || aligned_raw_price <= 0.0 {
            return Err(format!(
                "aligned raw_price invalid symbol={} side={:?} idx={} side_idx={} aligned_raw_price={}",
                trade_symbol, side, idx, side_level_index, aligned_raw_price
            ));
        }
        let raw_qty = order_amount_u / aligned_raw_price;
        if !raw_qty.is_finite() || raw_qty <= 0.0 {
            return Err(format!(
                "invalid raw_qty symbol={} side={:?} idx={} side_idx={} raw_qty={}",
                trade_symbol, side, idx, side_level_index, raw_qty
            ));
        }

        let (aligned_qty, aligned_price) = align_order_for_venue(
            venue,
            &symbol_key_for_tick,
            raw_qty,
            aligned_raw_price,
            table,
        )?;

        let Some(price_qv) = QuantizedValue::encode_floor(aligned_price, price_tick) else {
            return Err(format!(
                "invalid aligned_price quantization symbol={} side={:?} idx={} side_idx={} aligned_price={}",
                trade_symbol, side, idx, side_level_index, aligned_price
            ));
        };
        let Some(amount_qv) = QuantizedValue::encode_floor(aligned_qty, qty_tick) else {
            return Err(format!(
                "invalid aligned_qty quantization symbol={} side={:?} idx={} side_idx={} aligned_qty={}",
                trade_symbol, side, idx, side_level_index, aligned_qty
            ));
        };
        if price_qv.get_count() <= 0 || amount_qv.get_count() <= 0 {
            return Err(format!(
                "non-positive aligned qv symbol={} side={:?} idx={} side_idx={} price_cnt={} qty_cnt={}",
                trade_symbol,
                side,
                idx,
                side_level_index,
                price_qv.get_count(),
                amount_qv.get_count()
            ));
        }

        let aligned_price_v = price_qv.get_val();
        let aligned_qty_v = amount_qv.get_val();
        let offset = ((aligned_price_v - mid_price) / mid_price).abs();
        levels.push(MmOpenPlanLevel {
            level_index: idx,
            side_level_index,
            side,
            offset,
            base_price: mid_price,
            raw_price,
            raw_qty,
            aligned_price: aligned_price_v,
            aligned_qty: aligned_qty_v,
            price_tick_count: price_qv.get_count(),
            qty_tick_count: amount_qv.get_count(),
        });
    }

    if levels.is_empty() {
        return Err(format!(
            "empty levels after alignment symbol={} levels_per_side={}",
            trade_symbol, orders_per_round
        ));
    }
    if levels.len() != level_points.len() {
        return Err(format!(
            "aligned levels count mismatch symbol={} expected={} actual={}",
            trade_symbol,
            level_points.len(),
            levels.len()
        ));
    }

    let exp_time_us = if open_order_ttl_us > 0 {
        now_us.saturating_add(open_order_ttl_us)
    } else {
        0
    };

    Ok(MmOpenQuotePlan {
        symbol: trade_symbol,
        quote,
        now_us,
        exp_time_us,
        order_amount_u,
        orders_per_round,
        price_tick,
        qty_tick,
        band: MmVolatilityBand {
            mid_price,
            lower_price,
            upper_price,
            volatility: vol,
        },
        levels,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_prices_cover_both_sides_with_per_side_count() {
        let points = build_level_prices(1.0, 0.0008, 8);
        assert_eq!(points.len(), 16);

        assert_eq!(points[0].0, Side::Sell);
        assert_eq!(points[0].1, 1);
        assert!((points[0].2 - 1.0001).abs() < 1e-12);
        assert_eq!(points[1].0, Side::Buy);
        assert_eq!(points[1].1, 1);
        assert!((points[1].2 - 0.9999).abs() < 1e-12);

        assert_eq!(points[14].0, Side::Sell);
        assert_eq!(points[14].1, 8);
        assert!((points[14].2 - 1.0008).abs() < 1e-12);
        assert_eq!(points[15].0, Side::Buy);
        assert_eq!(points[15].1, 8);
        assert!((points[15].2 - 0.9992).abs() < 1e-12);
    }
}
