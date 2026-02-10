use crate::market_maker::hedge_scale::{scale_offsets_by_inventory, HedgeOffsetScaleInput};
use crate::market_maker::order_align::{align_order_for_venue, min_qty_symbol_key};
use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;
use crate::signal::hedge_signal::{MmHedgeCtx, MmHedgeSignalQueryMsg};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use crate::{common::time_util::get_timestamp_us, funding_rate::common::Quote};
use log::info;

pub struct MmHedgeBuildInput<'a> {
    pub venue: TradingVenue,
    pub symbol: &'a str,
    pub quote: Quote,
    pub order_amount_u: f64,
    pub offsets: &'a [f64],
    pub next_query_delay_ms: u64,
    pub from_key: Vec<u8>,
}

pub fn build_mm_hedge_ctx(
    input: MmHedgeBuildInput,
    table: &VenueMinQtyTable,
    query: &MmHedgeSignalQueryMsg,
) -> Result<MmHedgeCtx, String> {
    let symbol = input.symbol.to_uppercase();
    if symbol.is_empty() {
        return Err("empty symbol".to_string());
    }

    let net_qty = query.net_qty;
    if net_qty.abs() <= 1e-12 {
        return Err("net_qty is zero, skip hedge".to_string());
    }
    let side = if net_qty >= 0.0 {
        Side::Sell
    } else {
        Side::Buy
    };

    let base_price = match side {
        Side::Buy => input.quote.bid,
        Side::Sell => input.quote.ask,
    };
    if base_price <= 0.0 {
        return Err("invalid base price".to_string());
    }

    let symbol_key = min_qty_symbol_key(input.venue, &symbol);
    let price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
    let qty_tick = table.step_size(&symbol_key).unwrap_or(0.0);
    if price_tick <= 0.0 || qty_tick <= 0.0 {
        return Err(format!(
            "missing tick for {} (price_tick={}, qty_tick={})",
            symbol_key, price_tick, qty_tick
        ));
    }

    let price_tick_exp = tick_to_exp(price_tick).ok_or("price_tick is not power-of-10")?;
    let qty_tick_exp = tick_to_exp(qty_tick).ok_or("qty_tick is not power-of-10")?;

    let offset_abs: Vec<f64> = input.offsets.iter().map(|v| v.abs()).collect();
    if offset_abs.is_empty() {
        return Err("empty offsets".to_string());
    }
    let base_offset_min = offset_abs
        .iter()
        .copied()
        .fold(f64::INFINITY, |a, b| a.min(b));
    let base_offset_max = offset_abs
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, |a, b| a.max(b));

    let scaled = scale_offsets_by_inventory(HedgeOffsetScaleInput {
        net_qty_base: net_qty,
        hedge_bid0: input.quote.bid,
        hedge_ask0: input.quote.ask,
        symbol_exposure_u: query.symbol_exposure_u,
        final_offset_min: base_offset_min,
        final_offset_max: base_offset_max,
    });

    info!(
        "MMHedge query->scale: symbol={} side={:?} net_qty_base={:.8} bid0={:.8} ask0={:.8} inv_notional={:.8} symbol_exposure_u={:.8} scale={:.8} offset_in=[{:.8},{:.8}] offset_out=[{:.8},{:.8}]",
        symbol,
        side,
        net_qty,
        input.quote.bid,
        input.quote.ask,
        scaled.inv_notional,
        query.symbol_exposure_u,
        scaled.scale,
        base_offset_min,
        base_offset_max,
        scaled.offset_min_scaled,
        scaled.offset_max_scaled,
    );

    let denom = (base_offset_max - base_offset_min).abs();
    let mapped_offsets: Vec<f64> = offset_abs
        .iter()
        .map(|offset| {
            if denom <= 1e-12 {
                scaled.offset_min_scaled
            } else {
                let t = ((*offset - base_offset_min) / denom).clamp(0.0, 1.0);
                scaled.offset_min_scaled + t * (scaled.offset_max_scaled - scaled.offset_min_scaled)
            }
        })
        .collect();

    let mut price_list: Vec<i32> = Vec::with_capacity(mapped_offsets.len());
    let mut amount_list: Vec<i64> = Vec::with_capacity(mapped_offsets.len());
    for offset in mapped_offsets {
        let raw_price = match side {
            Side::Buy => base_price * (1.0 - offset),
            Side::Sell => base_price * (1.0 + offset),
        };
        if raw_price <= 0.0 {
            continue;
        }
        let raw_qty = input.order_amount_u / raw_price;
        let (aligned_qty, aligned_price) =
            align_order_for_venue(input.venue, &symbol_key, raw_qty, raw_price, table)?;

        let price_level = (aligned_price / price_tick).round() as i32;
        let amount_level = (aligned_qty / qty_tick).round() as i64;
        if price_level == 0 || amount_level == 0 {
            continue;
        }
        price_list.push(price_level);
        amount_list.push(amount_level);
    }

    info!(
        "MMHedge ctx levels: symbol={} levels={} price_tick_exp={} qty_tick_exp={} price_list={:?} amount_list={:?}",
        symbol,
        price_list.len(),
        price_tick_exp,
        qty_tick_exp,
        price_list,
        amount_list,
    );

    if price_list.is_empty() || amount_list.is_empty() {
        return Err("empty price/amount list after alignment".to_string());
    }

    let now = get_timestamp_us();
    let mut ctx = MmHedgeCtx::new();
    ctx.opening_leg = crate::signal::common::TradingLeg::new(
        input.venue,
        input.quote.bid,
        input.quote.ask,
        input.quote.ts,
    );
    ctx.set_opening_symbol(&symbol);
    ctx.price_tick_exp = price_tick_exp;
    ctx.qty_tick_exp = qty_tick_exp;
    ctx.price_list = price_list;
    ctx.amount_list = amount_list;
    ctx.signal_ts = now;
    ctx.next_query_ts = now + (input.next_query_delay_ms as i64) * 1000;
    ctx.set_from_key(input.from_key);
    Ok(ctx)
}

fn tick_to_exp(tick: f64) -> Option<i32> {
    if tick <= 0.0 {
        return None;
    }
    let mut exp: i32 = 0;
    let mut val = tick;
    while val < 1.0 {
        val *= 10.0;
        exp -= 1;
        if exp < -12 {
            break;
        }
    }
    while val >= 10.0 {
        val /= 10.0;
        exp += 1;
        if exp > 12 {
            break;
        }
    }
    if (val - 1.0).abs() <= 1e-9 {
        Some(exp)
    } else {
        None
    }
}
