use crate::common::tick_math::QuantizedValue;
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
    info!(
        "MMHedgeQuerySummary {{\"symbol\":\"{}\",\"side\":\"{}\",\"net_qty_base\":{:.8},\"hedge_bid0\":{:.8},\"hedge_ask0\":{:.8},\"inv_notional\":{:.8},\"symbol_exposure_u\":{:.8},\"scale\":{:.8},\"offset_min_in\":{:.8},\"offset_max_in\":{:.8},\"offset_min_out\":{:.8},\"offset_max_out\":{:.8}}}",
        symbol,
        side.as_str(),
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

    let mut price_qv_list: Vec<QuantizedValue> = Vec::with_capacity(mapped_offsets.len());
    let mut amount_qv_list: Vec<QuantizedValue> = Vec::with_capacity(mapped_offsets.len());
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

        let Some(price_qv) = QuantizedValue::encode_floor(aligned_price, price_tick) else {
            continue;
        };
        let Some(amount_qv) = QuantizedValue::encode_floor(aligned_qty, qty_tick) else {
            continue;
        };
        if price_qv.get_count() <= 0 || amount_qv.get_count() <= 0 {
            continue;
        }
        price_qv_list.push(price_qv);
        amount_qv_list.push(amount_qv);
    }

    let price_values: Vec<f64> = price_qv_list.iter().map(QuantizedValue::get_val).collect();
    let amount_values: Vec<f64> = amount_qv_list.iter().map(QuantizedValue::get_val).collect();
    info!(
        "MMHedge ctx levels: symbol={} levels={} price_values={:?} amount_values={:?}",
        symbol,
        price_qv_list.len(),
        price_values,
        amount_values,
    );
    info!(
        "MMHedgeCtxSummary {{\"symbol\":\"{}\",\"side\":\"{}\",\"levels\":{},\"next_query_delay_ms\":{},\"signal_ts\":{}}}",
        symbol,
        side.as_str(),
        price_qv_list.len(),
        input.next_query_delay_ms,
        get_timestamp_us(),
    );

    if price_qv_list.is_empty() || amount_qv_list.is_empty() {
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
    ctx.price_qv_list = price_qv_list;
    ctx.amount_qv_list = amount_qv_list;
    ctx.signal_ts = now;
    ctx.next_query_ts = now + (input.next_query_delay_ms as i64) * 1000;
    ctx.set_from_key(input.from_key);
    Ok(ctx)
}
