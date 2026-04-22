use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::funding_rate::common::{build_decision_from_key_base, Quote};
use crate::funding_rate::factor_value_hub::FactorValueHub;
use crate::market_maker::quote_plan_levels::{
    build_quote_plan_levels, QuotePlanLevel, QuotePlanLevelSpec,
};
use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;
use crate::signal::hedge_signal::{MmHedgeCtx, MmHedgeSignalQueryMsg};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use log::{debug, warn};
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const MM_NEUTRAL_SIGNAL: f64 = 0.0;
const MM_NEUTRAL_SIGNAL_QUANTILE: f64 = 0.5;
const MISSING_HEDGE_SCORE_LOG_INTERVAL_SECS: u64 = 30;

thread_local! {
    static MM_HEDGE_MISSING_SCORE_LAST_LOG_AT: RefCell<HashMap<String, Instant>> =
        RefCell::new(HashMap::new());
}

pub struct MmHedgeBuildInput<'a> {
    pub venue: TradingVenue,
    pub symbol: &'a str,
    pub quote: Quote,
    pub volatility: f64,
    pub signal: f64,
    pub signal_qtl: Option<f64>,
    pub enable_return_score_adjust_hedge: bool,
    pub hedge_vol_multiplier: f64,
    pub hedge_offset_ratio: f64,
    pub order_amount_u: f64,
    pub hedge_orders_per_round: u32,
    pub offset_low: f64,
    pub offset_high_limit: f64,
    pub hedge_window_scale_low: f64,
    pub hedge_window_scale_high: f64,
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
    pub signal_qtl: Option<f64>,
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

fn next_aligned_query_ts_us(now_us: i64, interval_ms: u64) -> i64 {
    let now_ms = now_us.max(0).div_euclid(1_000);
    let interval_ms = i64::try_from(interval_ms).unwrap_or(i64::MAX);
    let remainder = now_ms.rem_euclid(interval_ms);
    let delay_ms = if remainder == 0 {
        interval_ms
    } else {
        interval_ms - remainder
    };
    now_ms.saturating_add(delay_ms).saturating_mul(1_000)
}

pub fn resolve_mm_hedge_signal_inputs(
    factor_value_hub: &mut FactorValueHub,
    model_service: &str,
    symbol: &str,
    venue: TradingVenue,
    enable_return_score_adjust_hedge: bool,
) -> Result<(f64, Option<f64>, f64), String> {
    let score_lookup = factor_value_hub.lookup_model_output_score(model_service, symbol, venue);
    let factor_lookup =
        factor_value_hub.lookup_factor_value_with_last_valid_fallback(symbol, venue);
    let volatility = factor_lookup
        .target_factor_value
        .filter(|v| v.is_finite())
        .ok_or_else(|| {
            format!(
                "missing or invalid volatility factor key={} note={}",
                factor_lookup.key, factor_lookup.note
            )
        })?;
    let signal = if enable_return_score_adjust_hedge {
        if score_lookup.score.filter(|v| v.is_finite()).is_none()
            && should_log_missing_hedge_score(symbol, &score_lookup.service_name, &score_lookup.note)
        {
            warn!(
                "MmDecision: MMHedge missing return_score, fallback to neutral symbol={} venue={:?} service={} note={} volatility={:.8} signal_qtl={:.2}",
                symbol,
                venue,
                score_lookup.service_name,
                score_lookup.note,
                volatility,
                MM_NEUTRAL_SIGNAL_QUANTILE
            );
        }
        resolve_mm_hedge_effective_signal(
            enable_return_score_adjust_hedge,
            score_lookup.score,
            Some(volatility),
            &score_lookup.service_name,
            &score_lookup.note,
        )?
    } else {
        MM_NEUTRAL_SIGNAL
    };
    let signal_qtl = resolve_mm_hedge_signal_quantile(
        enable_return_score_adjust_hedge,
        score_lookup.score,
        score_lookup.score_quantile,
        Some(volatility),
    );
    Ok((signal, signal_qtl, volatility))
}

fn should_log_missing_hedge_score(symbol: &str, service_name: &str, note: &str) -> bool {
    let now = Instant::now();
    let key = format!("{symbol}|{service_name}|{note}");
    MM_HEDGE_MISSING_SCORE_LAST_LOG_AT.with(|last_log_at| {
        let mut last_log_at = last_log_at.borrow_mut();
        match last_log_at.get(&key) {
            Some(last)
                if now.duration_since(*last)
                    < Duration::from_secs(MISSING_HEDGE_SCORE_LOG_INTERVAL_SECS) =>
            {
                false
            }
            _ => {
                last_log_at.insert(key, now);
                true
            }
        }
    })
}

fn resolve_mm_hedge_effective_signal(
    enable_return_score_adjust_hedge: bool,
    score: Option<f64>,
    volatility: Option<f64>,
    service_name: &str,
    note: &str,
) -> Result<f64, String> {
    if !enable_return_score_adjust_hedge {
        return Ok(MM_NEUTRAL_SIGNAL);
    }
    if volatility.filter(|v| v.is_finite()).is_some() && score.filter(|v| v.is_finite()).is_none() {
        return Ok(MM_NEUTRAL_SIGNAL);
    }
    score.filter(|v| v.is_finite()).ok_or_else(|| {
        format!(
            "return_score unavailable service={} note={}",
            service_name, note
        )
    })
}

fn resolve_mm_hedge_signal_quantile(
    enable_return_score_adjust_hedge: bool,
    score: Option<f64>,
    score_quantile: Option<f64>,
    volatility: Option<f64>,
) -> Option<f64> {
    if !enable_return_score_adjust_hedge {
        return Some(MM_NEUTRAL_SIGNAL_QUANTILE);
    }
    if volatility.filter(|v| v.is_finite()).is_some() && score.filter(|v| v.is_finite()).is_none() {
        return Some(MM_NEUTRAL_SIGNAL_QUANTILE);
    }
    score_quantile.filter(|v| v.is_finite())
}

fn build_mm_hedge_from_key(now_us: i64, signal_qtl: Option<f64>, volatility: f64) -> Vec<u8> {
    build_decision_from_key_base(now_us, signal_qtl, None, Some(volatility), None, None)
        .into_bytes()
}

fn normalize_signal_legacy(signal: f64, bound: f64) -> (f64, f64) {
    let s_clipped = signal.clamp(-bound, bound);
    let n = ((s_clipped + bound) / (2.0 * bound + 1e-10)).clamp(0.0, 1.0);
    (s_clipped, n)
}

fn map_offset_from_signal_legacy(
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

    let (s_clipped, n) = normalize_signal_legacy(signal, bound);
    let offset_high = bound.clamp(low, high);
    let offset = match side {
        Side::Sell => low + n * (offset_high - low),
        Side::Buy => offset_high - n * (offset_high - low),
    };
    Ok((offset, bound, s_clipped, n))
}

struct MmHedgeOffsetPlan {
    bound: f64,
    clipped_signal: f64,
    normalized_signal: f64,
    mapped_offset: f64,
    adjusted_offset: f64,
    final_offset: f64,
    exposure_offset_factor: f64,
    offsets: Vec<f64>,
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

fn build_scaled_split_offsets(
    center_offset: f64,
    levels: usize,
    final_low: f64,
    final_high: f64,
    hedge_window_scale_low: f64,
    hedge_window_scale_high: f64,
) -> Vec<f64> {
    if levels == 0 {
        return Vec::new();
    }

    let final_low = final_low.max(0.0);
    let final_high = final_high.max(final_low);
    let center_offset = center_offset.clamp(final_low, final_high);
    if levels == 1 {
        return vec![center_offset];
    }

    let start = center_offset * hedge_window_scale_low;
    let end = center_offset * hedge_window_scale_high;
    let step = (end - start) / (levels - 1) as f64;
    (0..levels)
        .map(|idx| (start + step * idx as f64).clamp(final_low, final_high))
        .collect()
}

fn build_quantile_offset_plan(
    side: Side,
    signal: f64,
    signal_qtl: f64,
    volatility: f64,
    multiplier: f64,
    low: f64,
    high: f64,
    net_qty: f64,
    mid_price: f64,
    max_exposure_abs: f64,
    hedge_offset_ratio: f64,
    split_count: usize,
    hedge_window_scale_low: f64,
    hedge_window_scale_high: f64,
) -> Result<MmHedgeOffsetPlan, String> {
    if !(signal_qtl.is_finite() && (0.0..=1.0).contains(&signal_qtl)) {
        return Err(format!("invalid signal_qtl={}", signal_qtl));
    }
    if !(volatility.is_finite() && volatility > 0.0) {
        return Err(format!("invalid volatility={}", volatility));
    }
    if !(multiplier.is_finite() && multiplier > 0.0) {
        return Err(format!("invalid hedge_vol_multiplier={}", multiplier));
    }
    if !(hedge_offset_ratio.is_finite() && hedge_offset_ratio > 0.0) {
        return Err(format!("invalid hedge_offset_ratio={}", hedge_offset_ratio));
    }
    if !hedge_window_scale_low.is_finite() || !hedge_window_scale_high.is_finite() {
        return Err(format!(
            "invalid hedge_window_scale low={} high={}",
            hedge_window_scale_low, hedge_window_scale_high
        ));
    }
    if hedge_window_scale_low <= 0.0 || hedge_window_scale_high < hedge_window_scale_low {
        return Err(format!(
            "hedge_window_scale must satisfy 0<low<=high, got low={} high={}",
            hedge_window_scale_low, hedge_window_scale_high
        ));
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

    let offset_high = bound.clamp(low, high);
    let (clipped_signal, _) = normalize_signal_legacy(signal, bound);
    let normalized_signal = signal_qtl;
    let raw_offset = match side {
        Side::Sell => low + normalized_signal * (offset_high - low),
        Side::Buy => offset_high - normalized_signal * (offset_high - low),
    };

    let net_amount_u = net_qty.abs() * mid_price.max(0.0);
    let exposure_ratio = if max_exposure_abs > 0.0 {
        (net_amount_u / max_exposure_abs).clamp(0.0, 3.0)
    } else {
        3.0
    };
    let exposure_offset_factor = 1.0 - exposure_ratio / 6.0;
    let adjusted_offset = low + exposure_offset_factor * (raw_offset - low);
    let center_offset = (low + hedge_offset_ratio * (adjusted_offset - low)).clamp(low, high);
    let offsets = build_scaled_split_offsets(
        center_offset,
        split_count,
        low,
        high,
        hedge_window_scale_low,
        hedge_window_scale_high,
    );
    let final_offset = offsets.last().copied().unwrap_or(center_offset);

    Ok(MmHedgeOffsetPlan {
        bound,
        clipped_signal,
        normalized_signal,
        mapped_offset: raw_offset,
        adjusted_offset: center_offset,
        final_offset,
        exposure_offset_factor,
        offsets,
    })
}

fn build_legacy_offset_plan(
    side: Side,
    signal: f64,
    volatility: f64,
    multiplier: f64,
    low: f64,
    high: f64,
    net_qty: f64,
    mid_price: f64,
    max_exposure_abs: f64,
    hedge_offset_ratio: f64,
    split_count: usize,
) -> Result<MmHedgeOffsetPlan, String> {
    let (mapped_offset, bound, clipped_signal, normalized_signal) =
        map_offset_from_signal_legacy(side, signal, volatility, multiplier, low, high)?;
    let net_amount_u = net_qty.abs() * mid_price.max(0.0);
    let inventory_scale = if max_exposure_abs > 0.0 {
        1.0 / (1.0 + net_amount_u / max_exposure_abs)
    } else {
        1.0
    };
    let adjusted_offset = (mapped_offset * inventory_scale * hedge_offset_ratio)
        .clamp(low.max(0.0), high.max(low.max(0.0)));
    let offsets = build_linear_offsets(low, adjusted_offset, split_count);

    Ok(MmHedgeOffsetPlan {
        bound,
        clipped_signal,
        normalized_signal,
        mapped_offset,
        adjusted_offset,
        final_offset: adjusted_offset,
        exposure_offset_factor: inventory_scale,
        offsets,
    })
}

pub fn build_mm_hedge_quote_plan(
    input: MmHedgeBuildInput,
    table: &VenueMinQtyTable,
    query: &MmHedgeSignalQueryMsg,
) -> Result<MmHedgeQuotePlan, String> {
    let now_us = get_timestamp_us();
    let symbol = normalize_symbol_for_internal(input.symbol);
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

    let mid_price = (input.quote.bid + input.quote.ask) * 0.5;
    let split_count = input.hedge_orders_per_round as usize;
    let offset_plan = match input.signal_qtl {
        Some(signal_qtl) => build_quantile_offset_plan(
            side,
            input.signal,
            signal_qtl,
            input.volatility,
            input.hedge_vol_multiplier,
            input.offset_low,
            input.offset_high_limit,
            net_qty,
            mid_price,
            query.symbol_exposure_u,
            input.hedge_offset_ratio,
            split_count,
            input.hedge_window_scale_low,
            input.hedge_window_scale_high,
        )?,
        None => build_legacy_offset_plan(
            side,
            input.signal,
            input.volatility,
            input.hedge_vol_multiplier,
            input.offset_low,
            input.offset_high_limit,
            net_qty,
            mid_price,
            query.symbol_exposure_u,
            input.hedge_offset_ratio,
            split_count,
        )?,
    };
    if offset_plan.offsets.is_empty() {
        return Err("empty offsets after signal mapping".to_string());
    }
    let signal_qtl_log = input
        .signal_qtl
        .map(|value| format!("{:.8}", value))
        .unwrap_or_else(|| "null".to_string());

    let specs: Vec<QuotePlanLevelSpec> = offset_plan
        .offsets
        .iter()
        .copied()
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

    debug!(
        "MMHedge query->scale: symbol={} side={:?} net_qty_base={:.8} bid0={:.8} ask0={:.8} signal={:.8} signal_qtl={} enable_return_score_adjust_hedge={} clipped_signal={:.8} normalized_signal={:.8} volatility={:.8} hedge_vol_multiplier={:.8} bound={:.8} offset_low={:.8} offset_high_limit={:.8} hedge_window_scale_low={:.8} hedge_window_scale_high={:.8} mapped_offset={:.8} adjusted_offset={:.8} symbol_exposure_u={:.8} exposure_offset_factor={:.8} hedge_offset_ratio={:.8} final_offset={:.8}",
        symbol,
        side,
        net_qty,
        input.quote.bid,
        input.quote.ask,
        input.signal,
        signal_qtl_log,
        input.enable_return_score_adjust_hedge,
        offset_plan.clipped_signal,
        offset_plan.normalized_signal,
        input.volatility,
        input.hedge_vol_multiplier,
        offset_plan.bound,
        input.offset_low,
        input.offset_high_limit,
        input.hedge_window_scale_low,
        input.hedge_window_scale_high,
        offset_plan.mapped_offset,
        offset_plan.adjusted_offset,
        query.symbol_exposure_u,
        offset_plan.exposure_offset_factor,
        input.hedge_offset_ratio,
        offset_plan.final_offset,
    );
    debug!(
        "MMHedgeQuerySummary {{\"symbol\":\"{}\",\"side\":\"{}\",\"net_qty_base\":{:.8},\"hedge_bid0\":{:.8},\"hedge_ask0\":{:.8},\"signal\":{:.8},\"signal_qtl\":{},\"enable_return_score_adjust_hedge\":{},\"clipped_signal\":{:.8},\"normalized_signal\":{:.8},\"volatility\":{:.8},\"hedge_vol_multiplier\":{:.8},\"bound\":{:.8},\"offset_low\":{:.8},\"offset_high_limit\":{:.8},\"hedge_window_scale_low\":{:.8},\"hedge_window_scale_high\":{:.8},\"mapped_offset\":{:.8},\"adjusted_offset\":{:.8},\"symbol_exposure_u\":{:.8},\"exposure_offset_factor\":{:.8},\"hedge_offset_ratio\":{:.8},\"final_offset\":{:.8}}}",
        symbol,
        side.as_str(),
        net_qty,
        input.quote.bid,
        input.quote.ask,
        input.signal,
        signal_qtl_log,
        input.enable_return_score_adjust_hedge,
        offset_plan.clipped_signal,
        offset_plan.normalized_signal,
        input.volatility,
        input.hedge_vol_multiplier,
        offset_plan.bound,
        input.offset_low,
        input.offset_high_limit,
        input.hedge_window_scale_low,
        input.hedge_window_scale_high,
        offset_plan.mapped_offset,
        offset_plan.adjusted_offset,
        query.symbol_exposure_u,
        offset_plan.exposure_offset_factor,
        input.hedge_offset_ratio,
        offset_plan.final_offset,
    );

    Ok(MmHedgeQuotePlan {
        venue: input.venue,
        symbol,
        quote: input.quote,
        now_us,
        next_query_ts: next_aligned_query_ts_us(now_us, input.next_query_delay_ms),
        side,
        signal: input.signal,
        signal_qtl: input.signal_qtl,
        effective_signal: input.signal,
        clipped_signal: offset_plan.clipped_signal,
        normalized_signal: offset_plan.normalized_signal,
        volatility: input.volatility,
        bound: offset_plan.bound,
        mapped_offset: offset_plan.mapped_offset,
        final_offset: offset_plan.final_offset,
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
        ctx.price_offsets.push(level.offset);
    }
    if ctx.price_qv_list.is_empty() || ctx.amount_qv_list.is_empty() || ctx.price_offsets.is_empty()
    {
        return Err("empty price/amount list after alignment".to_string());
    }
    debug!(
        "MMHedge ctx levels: symbol={} levels={} price_values={:?} amount_values={:?} offsets={:?}",
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
        ctx.price_offsets,
    );
    ctx.signal_ts = plan.now_us;
    ctx.next_query_ts = plan.next_query_ts;
    ctx.set_from_key(build_mm_hedge_from_key(
        plan.now_us,
        plan.signal_qtl,
        plan.volatility,
    ));
    Ok(ctx)
}

#[cfg(test)]
mod tests {
    use super::{
        build_quantile_offset_plan, map_offset_from_signal_legacy, next_aligned_query_ts_us,
        resolve_mm_hedge_effective_signal, resolve_mm_hedge_signal_quantile,
    };
    use crate::market_maker::hedge_scale::{scale_offsets_by_inventory, HedgeOffsetScaleInput};
    use crate::pre_trade::order_manager::Side;

    #[test]
    fn legacy_short_signal_increases_offset_monotonically() {
        let (offset_lo, _, _, _) =
            map_offset_from_signal_legacy(Side::Sell, -1.0, 0.01, 2.0, 0.001, 0.03).unwrap();
        let (offset_hi, _, _, _) =
            map_offset_from_signal_legacy(Side::Sell, 1.0, 0.01, 2.0, 0.001, 0.03).unwrap();
        assert!(offset_hi > offset_lo);
    }

    #[test]
    fn legacy_long_signal_decreases_offset_monotonically() {
        let (offset_lo, _, _, _) =
            map_offset_from_signal_legacy(Side::Buy, -1.0, 0.01, 2.0, 0.001, 0.03).unwrap();
        let (offset_hi, _, _, _) =
            map_offset_from_signal_legacy(Side::Buy, 1.0, 0.01, 2.0, 0.001, 0.03).unwrap();
        assert!(offset_hi < offset_lo);
    }

    #[test]
    fn disabled_return_score_adjust_hedge_uses_neutral_quantile() {
        assert_eq!(
            resolve_mm_hedge_signal_quantile(false, Some(0.12), Some(0.91), Some(0.02)),
            Some(0.5)
        );
        assert_eq!(
            resolve_mm_hedge_signal_quantile(false, None, None, Some(0.02)),
            Some(0.5)
        );
    }

    #[test]
    fn enabled_return_score_adjust_hedge_preserves_quantile() {
        assert_eq!(
            resolve_mm_hedge_signal_quantile(true, Some(0.12), Some(0.91), Some(0.02)),
            Some(0.91)
        );
        assert_eq!(
            resolve_mm_hedge_signal_quantile(true, Some(0.12), None, Some(0.02)),
            None
        );
    }

    #[test]
    fn enabled_return_score_adjust_hedge_uses_neutral_when_score_missing_and_volatility_ready() {
        assert_eq!(
            resolve_mm_hedge_effective_signal(
                true,
                None,
                Some(0.02),
                "model_output/test",
                "missing"
            )
            .unwrap(),
            0.0
        );
        assert_eq!(
            resolve_mm_hedge_signal_quantile(true, None, None, Some(0.02)),
            Some(0.5)
        );
    }

    #[test]
    fn next_query_ts_aligns_to_half_minute_boundary() {
        assert_eq!(next_aligned_query_ts_us(29_999_000, 30_000), 30_000_000);
        assert_eq!(next_aligned_query_ts_us(30_000_000, 30_000), 60_000_000);
        assert_eq!(next_aligned_query_ts_us(30_001_000, 30_000), 60_000_000);
    }

    #[test]
    fn next_query_ts_aligns_to_minute_boundary() {
        assert_eq!(next_aligned_query_ts_us(59_999_000, 60_000), 60_000_000);
        assert_eq!(next_aligned_query_ts_us(60_000_000, 60_000), 120_000_000);
    }

    #[test]
    fn quantile_sell_offset_increases_with_quantile() {
        let plan_lo = build_quantile_offset_plan(
            Side::Sell,
            0.5,
            0.1,
            0.01,
            2.0,
            0.001,
            0.03,
            10.0,
            100.0,
            1_000.0,
            1.0,
            8,
            0.8,
            1.3,
        )
        .unwrap();
        let plan_hi = build_quantile_offset_plan(
            Side::Sell,
            0.5,
            0.9,
            0.01,
            2.0,
            0.001,
            0.03,
            10.0,
            100.0,
            1_000.0,
            1.0,
            8,
            0.8,
            1.3,
        )
        .unwrap();
        assert!(plan_hi.mapped_offset > plan_lo.mapped_offset);
    }

    #[test]
    fn quantile_buy_offset_decreases_with_quantile() {
        let plan_lo = build_quantile_offset_plan(
            Side::Buy,
            0.5,
            0.1,
            0.01,
            2.0,
            0.001,
            0.03,
            -10.0,
            100.0,
            1_000.0,
            1.0,
            8,
            0.8,
            1.3,
        )
        .unwrap();
        let plan_hi = build_quantile_offset_plan(
            Side::Buy,
            0.5,
            0.9,
            0.01,
            2.0,
            0.001,
            0.03,
            -10.0,
            100.0,
            1_000.0,
            1.0,
            8,
            0.8,
            1.3,
        )
        .unwrap();
        assert!(plan_hi.mapped_offset < plan_lo.mapped_offset);
    }

    #[test]
    fn quantile_inventory_scaling_reduces_offset_as_exposure_grows() {
        let light = build_quantile_offset_plan(
            Side::Sell,
            0.5,
            0.9,
            0.01,
            2.0,
            0.001,
            0.03,
            2.0,
            100.0,
            1_000.0,
            1.0,
            8,
            0.8,
            1.3,
        )
        .unwrap();
        let heavy = build_quantile_offset_plan(
            Side::Sell,
            0.5,
            0.9,
            0.01,
            2.0,
            0.001,
            0.03,
            20.0,
            100.0,
            1_000.0,
            1.0,
            8,
            0.8,
            1.3,
        )
        .unwrap();
        assert!(heavy.adjusted_offset < light.adjusted_offset);
        assert!(heavy.exposure_offset_factor < light.exposure_offset_factor);
    }

    #[test]
    fn quantile_split_ratios_expand_final_offset_range() {
        let tight = build_quantile_offset_plan(
            Side::Sell,
            0.5,
            0.9,
            0.01,
            2.0,
            0.001,
            0.03,
            10.0,
            100.0,
            1_000.0,
            1.0,
            8,
            0.9,
            1.0,
        )
        .unwrap();
        let wide = build_quantile_offset_plan(
            Side::Sell,
            0.5,
            0.9,
            0.01,
            2.0,
            0.001,
            0.03,
            10.0,
            100.0,
            1_000.0,
            1.0,
            8,
            0.8,
            1.3,
        )
        .unwrap();
        assert!(wide.final_offset > tight.final_offset);
        assert!(wide.offsets.first().unwrap() < tight.offsets.first().unwrap());
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
    fn disabled_return_score_adjust_skips_missing_score() {
        assert_eq!(
            resolve_mm_hedge_effective_signal(false, None, None, "model_output/test", "missing")
                .unwrap(),
            0.0
        );
        assert!(resolve_mm_hedge_effective_signal(
            true,
            None,
            None,
            "model_output/test",
            "missing"
        )
        .unwrap_err()
        .contains("return_score unavailable"));
    }
}
