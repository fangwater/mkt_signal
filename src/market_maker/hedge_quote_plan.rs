use crate::common::time_util::get_timestamp_us;
use crate::funding_rate::common::{build_decision_from_key_base, Quote};
use crate::funding_rate::factor_value_hub::FactorValueHub;
use crate::market_maker::hedge_scale::{scale_offsets_by_inventory, HedgeOffsetScaleInput};
use crate::market_maker::quote_plan_levels::{
    build_quote_plan_levels, QuotePlanLevel, QuotePlanLevelSpec,
};
use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;
use crate::signal::hedge_signal::{MmHedgeCtx, MmHedgeSignalQueryMsg};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use log::info;

pub struct MmHedgeBuildInput<'a> {
    pub venue: TradingVenue,
    pub symbol: &'a str,
    pub quote: Quote,
    pub volatility: f64,
    pub signal: f64,
    pub enable_return_score_adjust_hedge: bool,
    pub hedge_vol_multiplier: f64,
    pub hedge_offset_ratio: f64,
    pub order_amount_u: f64,
    pub hedge_orders_per_round: u32,
    pub offset_low: f64,
    pub offset_high_limit: f64,
    pub next_query_delay_ms: u64,
}

#[derive(Debug, Clone)]
pub struct MmHedgeQuotePlan {
    pub venue: TradingVenue,
    pub symbol: String,
    pub quote: Quote,
    pub now_us: i64,
    pub next_query_ts: i64,
    pub side: Side,
    pub signal: f64,
    pub effective_signal: f64,
    pub clipped_signal: f64,
    pub normalized_signal: f64,
    pub volatility: f64,
    pub bound: f64,
    pub mapped_offset: f64,
    pub final_offset: f64,
    pub order_amount_u: f64,
    pub hedge_orders_per_round: u32,
    pub price_tick: f64,
    pub qty_tick: f64,
    pub levels: Vec<QuotePlanLevel>,
}

pub fn resolve_mm_hedge_signal_inputs(
    factor_value_hub: &mut FactorValueHub,
    model_service: &str,
    symbol: &str,
    venue: TradingVenue,
) -> Result<(f64, f64), String> {
    let score_lookup = factor_value_hub.lookup_model_output_score(model_service, symbol, venue);
    let signal = score_lookup
        .score
        .filter(|v| v.is_finite())
        .ok_or_else(|| {
            format!(
                "return_score unavailable service={} note={}",
                score_lookup.service_name, score_lookup.note
            )
        })?;
    let factor_lookup = factor_value_hub.lookup_target_factor_value(symbol, venue);
    let volatility = factor_lookup
        .target_factor_value
        .filter(|v| v.is_finite())
        .ok_or_else(|| {
            format!(
                "missing or invalid volatility factor key={} note={}",
                factor_lookup.key, factor_lookup.note
            )
        })?;
    Ok((signal, volatility))
}

fn build_mm_hedge_from_key(now_us: i64, signal: f64, volatility: f64) -> Vec<u8> {
    build_decision_from_key_base(now_us, Some(signal), None, Some(volatility), None, None)
        .into_bytes()
}

fn resolve_score_adjust_factor(
    mapped_offset: f64,
    neutral_offset: f64,
    enable_return_score_adjust_hedge: bool,
) -> Result<f64, String> {
    if !enable_return_score_adjust_hedge {
        return Ok(1.0);
    }
    if !(neutral_offset.is_finite() && neutral_offset > 0.0) {
        return Err(format!("invalid neutral_offset={}", neutral_offset));
    }
    let factor = mapped_offset / neutral_offset;
    if !(factor.is_finite() && factor > 0.0) {
        return Err(format!(
            "invalid score_adjust_factor mapped_offset={} neutral_offset={} factor={}",
            mapped_offset, neutral_offset, factor
        ));
    }
    Ok(factor)
}

fn normalize_signal(signal: f64, bound: f64) -> (f64, f64) {
    let s_clipped = signal.clamp(-bound, bound);
    let n = ((s_clipped + bound) / (2.0 * bound + 1e-10)).clamp(0.0, 1.0);
    (s_clipped, n)
}

fn map_offset_from_signal(
    side: Side,
    signal: f64,
    volatility: f64,
    multiplier: f64,
    low: f64,
    high: f64,
) -> Result<(f64, f64, f64, f64), String> {
    if !(volatility.is_finite() && volatility > 0.0) {
        return Err(format!("invalid volatility={}", volatility));
    }
    if !(multiplier.is_finite() && multiplier > 0.0) {
        return Err(format!("invalid hedge_vol_multiplier={}", multiplier));
    }

    let low = low.max(0.0);
    let high = high.max(low);
    let bound = volatility * multiplier;
    if !(bound.is_finite() && bound > 0.0) {
        return Err(format!(
            "invalid bound volatility={} multiplier={} bound={}",
            volatility, multiplier, bound
        ));
    }

    let (s_clipped, n) = normalize_signal(signal, bound);
    let offset_high = bound.clamp(low, high);
    let offset = match side {
        Side::Sell => low + n * (offset_high - low),
        Side::Buy => offset_high - n * (offset_high - low),
    };
    Ok((offset, bound, s_clipped, n))
}

fn build_linear_offsets(low: f64, high: f64, levels: usize) -> Vec<f64> {
    if levels == 0 {
        return Vec::new();
    }
    if levels == 1 {
        return vec![high.max(low)];
    }

    let start = low.max(0.0);
    let end = high.max(start);
    let step = (end - start) / (levels - 1) as f64;
    (0..levels).map(|idx| start + step * idx as f64).collect()
}

pub fn build_mm_hedge_quote_plan(
    input: MmHedgeBuildInput,
    table: &VenueMinQtyTable,
    query: &MmHedgeSignalQueryMsg,
) -> Result<MmHedgeQuotePlan, String> {
    let now_us = get_timestamp_us();
    let symbol = input.symbol.to_uppercase();
    if symbol.is_empty() {
        return Err("empty symbol".to_string());
    }

    let net_qty = query.net_qty;
    if net_qty.abs() <= 1e-12 {
        return Err("net_qty is zero, skip hedge".to_string());
    }
    if input.hedge_orders_per_round == 0 {
        return Err("hedge_orders_per_round must be > 0".to_string());
    }
    if !(input.hedge_offset_ratio.is_finite() && input.hedge_offset_ratio > 0.0) {
        return Err(format!(
            "invalid hedge_offset_ratio={}",
            input.hedge_offset_ratio
        ));
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
    if !base_price.is_finite() || base_price <= 0.0 {
        return Err("invalid base price".to_string());
    }

    let (mapped_offset, bound, clipped_signal, normalized_signal) = map_offset_from_signal(
        side,
        input.signal,
        input.volatility,
        input.hedge_vol_multiplier,
        input.offset_low,
        input.offset_high_limit,
    )?;
    let (neutral_offset, _, _, _) = map_offset_from_signal(
        side,
        0.0,
        input.volatility,
        input.hedge_vol_multiplier,
        input.offset_low,
        input.offset_high_limit,
    )?;
    let score_adjust_factor = resolve_score_adjust_factor(
        mapped_offset,
        neutral_offset,
        input.enable_return_score_adjust_hedge,
    )?;
    let adjusted_offset = neutral_offset * score_adjust_factor;
    let inventory_scale_result = scale_offsets_by_inventory(HedgeOffsetScaleInput {
        net_qty_base: net_qty,
        hedge_bid0: input.quote.bid,
        hedge_ask0: input.quote.ask,
        symbol_exposure_u: query.symbol_exposure_u,
        final_offset_min: input.offset_low,
        final_offset_max: input.offset_high_limit,
    });
    let final_offset = (adjusted_offset * inventory_scale_result.scale * input.hedge_offset_ratio)
        .clamp(
            input.offset_low.max(0.0),
            input.offset_high_limit.max(input.offset_low.max(0.0)),
        );
    let offsets = build_linear_offsets(
        input.offset_low,
        final_offset,
        input.hedge_orders_per_round as usize,
    );
    if offsets.is_empty() {
        return Err("empty offsets after signal mapping".to_string());
    }

    let specs: Vec<QuotePlanLevelSpec> = offsets
        .into_iter()
        .enumerate()
        .map(|(idx, offset)| QuotePlanLevelSpec {
            side,
            side_level_index: idx + 1,
            offset,
            base_price,
        })
        .collect();
    let (price_tick, qty_tick, levels) =
        build_quote_plan_levels(input.venue, &symbol, input.order_amount_u, &specs, table)?;
    if levels.is_empty() {
        return Err("empty levels after alignment".to_string());
    }

    info!(
        "MMHedge query->scale: symbol={} side={:?} net_qty_base={:.8} bid0={:.8} ask0={:.8} signal={:.8} enable_return_score_adjust_hedge={} clipped_signal={:.8} normalized_signal={:.8} volatility={:.8} hedge_vol_multiplier={:.8} bound={:.8} offset_low={:.8} offset_high_limit={:.8} neutral_offset={:.8} mapped_offset={:.8} score_adjust_factor={:.8} inv_notional={:.8} inventory_scale={:.8} hedge_offset_ratio={:.8} final_offset={:.8}",
        symbol,
        side,
        net_qty,
        input.quote.bid,
        input.quote.ask,
        input.signal,
        input.enable_return_score_adjust_hedge,
        clipped_signal,
        normalized_signal,
        input.volatility,
        input.hedge_vol_multiplier,
        bound,
        input.offset_low,
        input.offset_high_limit,
        neutral_offset,
        mapped_offset,
        score_adjust_factor,
        inventory_scale_result.inv_notional,
        inventory_scale_result.scale,
        input.hedge_offset_ratio,
        final_offset,
    );
    info!(
        "MMHedgeQuerySummary {{\"symbol\":\"{}\",\"side\":\"{}\",\"net_qty_base\":{:.8},\"hedge_bid0\":{:.8},\"hedge_ask0\":{:.8},\"signal\":{:.8},\"enable_return_score_adjust_hedge\":{},\"clipped_signal\":{:.8},\"normalized_signal\":{:.8},\"volatility\":{:.8},\"hedge_vol_multiplier\":{:.8},\"bound\":{:.8},\"offset_low\":{:.8},\"offset_high_limit\":{:.8},\"neutral_offset\":{:.8},\"mapped_offset\":{:.8},\"score_adjust_factor\":{:.8},\"symbol_exposure_u\":{:.8},\"inv_notional\":{:.8},\"inventory_scale\":{:.8},\"hedge_offset_ratio\":{:.8},\"final_offset\":{:.8}}}",
        symbol,
        side.as_str(),
        net_qty,
        input.quote.bid,
        input.quote.ask,
        input.signal,
        input.enable_return_score_adjust_hedge,
        clipped_signal,
        normalized_signal,
        input.volatility,
        input.hedge_vol_multiplier,
        bound,
        input.offset_low,
        input.offset_high_limit,
        neutral_offset,
        mapped_offset,
        score_adjust_factor,
        query.symbol_exposure_u,
        inventory_scale_result.inv_notional,
        inventory_scale_result.scale,
        input.hedge_offset_ratio,
        final_offset,
    );

    Ok(MmHedgeQuotePlan {
        venue: input.venue,
        symbol,
        quote: input.quote,
        now_us,
        next_query_ts: now_us + (input.next_query_delay_ms as i64) * 1000,
        side,
        signal: input.signal,
        effective_signal: input.signal,
        clipped_signal,
        normalized_signal,
        volatility: input.volatility,
        bound,
        mapped_offset,
        final_offset,
        order_amount_u: input.order_amount_u,
        hedge_orders_per_round: input.hedge_orders_per_round,
        price_tick,
        qty_tick,
        levels,
    })
}

pub fn build_mm_hedge_ctx(
    input: MmHedgeBuildInput,
    table: &VenueMinQtyTable,
    query: &MmHedgeSignalQueryMsg,
) -> Result<MmHedgeCtx, String> {
    let plan = build_mm_hedge_quote_plan(input, table, query)?;
    let mut ctx = MmHedgeCtx::new();
    ctx.opening_leg = crate::signal::common::TradingLeg::new(
        plan.venue,
        plan.quote.bid,
        plan.quote.ask,
        plan.quote.ts,
    );
    ctx.set_opening_symbol(&plan.symbol);
    for level in &plan.levels {
        let Some(price_qv) = crate::common::tick_math::QuantizedValue::encode_floor(
            level.aligned_price,
            plan.price_tick,
        ) else {
            continue;
        };
        let Some(amount_qv) = crate::common::tick_math::QuantizedValue::encode_floor(
            level.aligned_qty,
            plan.qty_tick,
        ) else {
            continue;
        };
        if price_qv.get_count() <= 0 || amount_qv.get_count() <= 0 {
            continue;
        }
        ctx.price_qv_list.push(price_qv);
        ctx.amount_qv_list.push(amount_qv);
    }
    if ctx.price_qv_list.is_empty() || ctx.amount_qv_list.is_empty() {
        return Err("empty price/amount list after alignment".to_string());
    }
    info!(
        "MMHedge ctx levels: symbol={} levels={} price_values={:?} amount_values={:?}",
        plan.symbol,
        ctx.price_qv_list.len(),
        ctx.price_qv_list
            .iter()
            .map(crate::common::tick_math::QuantizedValue::get_val)
            .collect::<Vec<_>>(),
        ctx.amount_qv_list
            .iter()
            .map(crate::common::tick_math::QuantizedValue::get_val)
            .collect::<Vec<_>>(),
    );
    info!(
        "MMHedgeCtxSummary {{\"symbol\":\"{}\",\"side\":\"{}\",\"levels\":{},\"next_query_delay_ms\":{},\"signal_ts\":{}}}",
        plan.symbol,
        plan.side.as_str(),
        ctx.price_qv_list.len(),
        (plan.next_query_ts - plan.now_us) / 1000,
        plan.now_us,
    );
    ctx.signal_ts = plan.now_us;
    ctx.next_query_ts = plan.next_query_ts;
    ctx.set_from_key(build_mm_hedge_from_key(
        plan.now_us,
        plan.signal,
        plan.volatility,
    ));
    Ok(ctx)
}

#[cfg(test)]
mod tests {
    use super::{map_offset_from_signal, resolve_score_adjust_factor};
    use crate::market_maker::hedge_scale::{scale_offsets_by_inventory, HedgeOffsetScaleInput};
    use crate::pre_trade::order_manager::Side;

    #[test]
    fn short_signal_increases_offset_monotonically() {
        let (offset_lo, _, _, _) =
            map_offset_from_signal(Side::Sell, -1.0, 0.01, 2.0, 0.001, 0.03).unwrap();
        let (offset_hi, _, _, _) =
            map_offset_from_signal(Side::Sell, 1.0, 0.01, 2.0, 0.001, 0.03).unwrap();
        assert!(offset_hi > offset_lo);
    }

    #[test]
    fn long_signal_decreases_offset_monotonically() {
        let (offset_lo, _, _, _) =
            map_offset_from_signal(Side::Buy, -1.0, 0.01, 2.0, 0.001, 0.03).unwrap();
        let (offset_hi, _, _, _) =
            map_offset_from_signal(Side::Buy, 1.0, 0.01, 2.0, 0.001, 0.03).unwrap();
        assert!(offset_hi < offset_lo);
    }

    #[test]
    fn inventory_scale_uses_abs_net_qty() {
        let positive = scale_offsets_by_inventory(HedgeOffsetScaleInput {
            net_qty_base: 10.0,
            hedge_bid0: 100.0,
            hedge_ask0: 101.0,
            symbol_exposure_u: 1_000.0,
            final_offset_min: 0.0003,
            final_offset_max: 0.005,
        });
        let negative = scale_offsets_by_inventory(HedgeOffsetScaleInput {
            net_qty_base: -10.0,
            hedge_bid0: 100.0,
            hedge_ask0: 101.0,
            symbol_exposure_u: 1_000.0,
            final_offset_min: 0.0003,
            final_offset_max: 0.005,
        });

        assert!(positive.scale > 0.0);
        assert_eq!(positive.scale, negative.scale);
        assert_eq!(positive.inv_notional, negative.inv_notional);
    }

    #[test]
    fn disable_return_score_adjust_hedge_uses_identity_factor() {
        assert_eq!(resolve_score_adjust_factor(0.002, 0.001, false).unwrap(), 1.0);
        assert_eq!(resolve_score_adjust_factor(0.004, 0.002, true).unwrap(), 2.0);
    }
}
