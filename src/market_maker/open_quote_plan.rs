use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::tick_math::QuantizedValue;
use crate::funding_rate::common::Quote;
use crate::market_maker::order_align::{align_order_for_venue, min_qty_symbol_key};
use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;
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

pub fn resolve_mm_volatility(volatility_raw: Option<f64>, factor_ready: bool) -> f64 {
    let raw = volatility_raw
        .filter(|v| v.is_finite() && *v >= 0.0)
        .unwrap_or(0.0);
    if factor_ready {
        raw.clamp(0.0, 0.95)
    } else {
        0.0
    }
}

fn build_level_prices(lower: f64, upper: f64, levels: usize) -> Vec<f64> {
    if levels == 0 {
        return Vec::new();
    }
    if !lower.is_finite() || !upper.is_finite() {
        return Vec::new();
    }
    if levels == 1 {
        return vec![(lower + upper) * 0.5];
    }

    let width = upper - lower;
    if width.abs() <= f64::EPSILON {
        return vec![(lower + upper) * 0.5; levels];
    }

    let step = width / ((levels - 1) as f64);
    (0..levels).map(|i| lower + (i as f64) * step).collect()
}

fn resolve_side(level_price: f64, mid_price: f64, idx: usize) -> Side {
    if level_price < mid_price {
        Side::Buy
    } else if level_price > mid_price {
        Side::Sell
    } else if idx % 2 == 0 {
        Side::Buy
    } else {
        Side::Sell
    }
}

pub fn build_mm_from_key(
    now_us: i64,
    orders_per_round: u32,
    level_idx: usize,
    band: MmVolatilityBand,
) -> String {
    format!(
        "{now_us}:mm_k={}:idx={}:mid={:.8}:vol={:.8}:band=[{:.8},{:.8}]",
        orders_per_round,
        level_idx,
        band.mid_price,
        band.volatility,
        band.lower_price,
        band.upper_price
    )
}

pub fn build_default_hedge_offsets(orders_per_round: u32) -> Vec<f64> {
    // Keep the legacy hedge responder behavior with deterministic offsets.
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

    let vol = if volatility.is_finite() {
        volatility.clamp(0.0, 0.95)
    } else {
        0.0
    };
    let lower_price = mid_price * (1.0 - vol);
    let upper_price = mid_price * (1.0 + vol);
    let level_prices = build_level_prices(lower_price, upper_price, orders_per_round as usize);
    if level_prices.is_empty() {
        return Err(format!(
            "empty level prices symbol={} mid={:.8} lower={:.8} upper={:.8} levels={}",
            trade_symbol, mid_price, lower_price, upper_price, orders_per_round
        ));
    }

    let mut levels = Vec::with_capacity(level_prices.len());
    for (idx, raw_price) in level_prices.iter().copied().enumerate() {
        if !raw_price.is_finite() || raw_price <= 0.0 {
            continue;
        }
        let side = resolve_side(raw_price, mid_price, idx);
        let raw_qty = order_amount_u / raw_price;
        if !raw_qty.is_finite() || raw_qty <= 0.0 {
            continue;
        }

        let (aligned_qty, aligned_price) =
            align_order_for_venue(venue, &symbol_key_for_tick, raw_qty, raw_price, table)?;

        let Some(price_qv) = QuantizedValue::encode_floor(aligned_price, price_tick) else {
            continue;
        };
        let Some(amount_qv) = QuantizedValue::encode_floor(aligned_qty, qty_tick) else {
            continue;
        };
        if price_qv.get_count() <= 0 || amount_qv.get_count() <= 0 {
            continue;
        }

        let aligned_price_v = price_qv.get_val();
        let aligned_qty_v = amount_qv.get_val();
        let offset = ((aligned_price_v - mid_price) / mid_price).abs();
        levels.push(MmOpenPlanLevel {
            level_index: idx,
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
            "empty levels after alignment symbol={} levels={}",
            trade_symbol, orders_per_round
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
