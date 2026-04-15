use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::funding_rate::common::Quote;
use crate::market_maker::quote_plan_levels::{
    build_quote_plan_levels, QuotePlanLevel, QuotePlanLevelSpec,
};
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

pub type MmOpenPlanLevel = QuotePlanLevel;

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

fn build_level_specs(
    bid: f64,
    ask: f64,
    vol: f64,
    levels_per_side: usize,
    buy_vol_scale: [f64; 2],
    sell_vol_scale: [f64; 2],
) -> Vec<QuotePlanLevelSpec> {
    fn build_side_offsets(vol: f64, levels: usize, scale: [f64; 2]) -> Vec<f64> {
        if levels == 0 {
            return Vec::new();
        }
        let start = scale[0] * vol;
        let end = scale[1] * vol;
        if levels == 1 {
            return vec![end];
        }
        let step = (end - start) / (levels - 1) as f64;
        (0..levels).map(|idx| start + step * idx as f64).collect()
    }

    if levels_per_side == 0 {
        return Vec::new();
    }
    if !bid.is_finite()
        || bid <= 0.0
        || !ask.is_finite()
        || ask <= 0.0
        || bid >= ask
        || !vol.is_finite()
        || vol < 0.0
        || !buy_vol_scale[0].is_finite()
        || !buy_vol_scale[1].is_finite()
        || !sell_vol_scale[0].is_finite()
        || !sell_vol_scale[1].is_finite()
        || buy_vol_scale[0] < 0.0
        || buy_vol_scale[1] < buy_vol_scale[0]
        || sell_vol_scale[0] < 0.0
        || sell_vol_scale[1] < sell_vol_scale[0]
    {
        return Vec::new();
    }

    let sell_offsets = build_side_offsets(vol, levels_per_side, sell_vol_scale);
    let buy_offsets = build_side_offsets(vol, levels_per_side, buy_vol_scale);
    if sell_offsets.len() != levels_per_side || buy_offsets.len() != levels_per_side {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(levels_per_side.saturating_mul(2));
    for side_level_index in 1..=levels_per_side {
        let sell_offset = sell_offsets[side_level_index - 1];
        out.push(QuotePlanLevelSpec {
            side: Side::Sell,
            side_level_index,
            offset: sell_offset,
            base_price: ask,
        });
        let buy_offset = buy_offsets[side_level_index - 1];
        out.push(QuotePlanLevelSpec {
            side: Side::Buy,
            side_level_index,
            offset: buy_offset,
            base_price: bid,
        });
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
    open_buy_vol_scale: [f64; 2],
    open_sell_vol_scale: [f64; 2],
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

    let mid_price = (quote.bid + quote.ask) * 0.5;
    if !mid_price.is_finite() || mid_price <= 0.0 {
        return Err(format!(
            "invalid mid_price symbol={} bid={} ask={}",
            symbol, quote.bid, quote.ask
        ));
    }

    if !volatility.is_finite() {
        return Err(format!(
            "invalid volatility symbol={} volatility={}",
            symbol, volatility
        ));
    }
    if !open_buy_vol_scale[0].is_finite()
        || !open_buy_vol_scale[1].is_finite()
        || !open_sell_vol_scale[0].is_finite()
        || !open_sell_vol_scale[1].is_finite()
    {
        return Err(format!(
            "invalid open vol scale range symbol={} buy={:?} sell={:?}",
            symbol, open_buy_vol_scale, open_sell_vol_scale
        ));
    }
    if open_buy_vol_scale[0] < 0.0
        || open_buy_vol_scale[1] < open_buy_vol_scale[0]
        || open_sell_vol_scale[0] < 0.0
        || open_sell_vol_scale[1] < open_sell_vol_scale[0]
    {
        return Err(format!(
            "open vol scale range must satisfy 0<=low<=high symbol={} buy={:?} sell={:?}",
            symbol, open_buy_vol_scale, open_sell_vol_scale
        ));
    }
    let vol = volatility;
    let lower_price = quote.bid * (1.0 - (open_buy_vol_scale[1] * vol));
    let upper_price = quote.ask * (1.0 + (open_sell_vol_scale[1] * vol));
    let specs = build_level_specs(
        quote.bid,
        quote.ask,
        vol,
        orders_per_round as usize,
        open_buy_vol_scale,
        open_sell_vol_scale,
    );
    if specs.is_empty() {
        return Err(format!(
            "empty level prices symbol={} bid={:.8} ask={:.8} lower={:.8} upper={:.8} levels_per_side={} buy={:?} sell={:?}",
            symbol,
            quote.bid,
            quote.ask,
            lower_price,
            upper_price,
            orders_per_round,
            open_buy_vol_scale,
            open_sell_vol_scale
        ));
    }
    let (price_tick, qty_tick, levels) =
        build_quote_plan_levels(venue, symbol, order_amount_u, &specs, table)?;

    if levels.is_empty() {
        return Err(format!(
            "empty levels after alignment symbol={} levels_per_side={}",
            symbol, orders_per_round
        ));
    }
    if levels.len() != specs.len() {
        return Err(format!(
            "aligned levels count mismatch symbol={} expected={} actual={}",
            symbol,
            specs.len(),
            levels.len()
        ));
    }

    let exp_time_us = if open_order_ttl_us > 0 {
        now_us.saturating_add(open_order_ttl_us)
    } else {
        0
    };

    Ok(MmOpenQuotePlan {
        symbol: normalize_symbol_for_internal(symbol),
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
        let specs = build_level_specs(0.99, 1.01, 0.0008, 8, [0.0, 1.0], [0.0, 1.0]);
        assert_eq!(specs.len(), 16);

        assert_eq!(specs[0].side, Side::Sell);
        assert_eq!(specs[0].side_level_index, 1);
        assert!((specs[0].base_price - 1.01).abs() < 1e-12);
        assert!((specs[0].offset - 0.0).abs() < 1e-12);
        assert_eq!(specs[1].side, Side::Buy);
        assert_eq!(specs[1].side_level_index, 1);
        assert!((specs[1].base_price - 0.99).abs() < 1e-12);
        assert!((specs[1].offset - 0.0).abs() < 1e-12);

        assert_eq!(specs[14].side, Side::Sell);
        assert_eq!(specs[14].side_level_index, 8);
        assert!((specs[14].base_price - 1.01).abs() < 1e-12);
        assert!((specs[14].offset - 0.0008).abs() < 1e-12);
        assert_eq!(specs[15].side, Side::Buy);
        assert_eq!(specs[15].side_level_index, 8);
        assert!((specs[15].base_price - 0.99).abs() < 1e-12);
        assert!((specs[15].offset - 0.0008).abs() < 1e-12);
    }

    #[test]
    fn zero_vol_still_anchors_sell_to_ask_and_buy_to_bid() {
        let specs = build_level_specs(100.0, 100.1, 0.0, 2, [0.2, 0.8], [0.3, 0.9]);
        assert_eq!(specs.len(), 4);
        assert_eq!(specs[0].side, Side::Sell);
        assert!((specs[0].base_price - 100.1).abs() < 1e-12);
        assert!((specs[0].offset - 0.0).abs() < 1e-12);
        assert_eq!(specs[1].side, Side::Buy);
        assert!((specs[1].base_price - 100.0).abs() < 1e-12);
        assert!((specs[1].offset - 0.0).abs() < 1e-12);
        assert_eq!(specs[2].side, Side::Sell);
        assert!((specs[2].base_price - 100.1).abs() < 1e-12);
        assert!((specs[2].offset - 0.0).abs() < 1e-12);
        assert_eq!(specs[3].side, Side::Buy);
        assert!((specs[3].base_price - 100.0).abs() < 1e-12);
        assert!((specs[3].offset - 0.0).abs() < 1e-12);
    }

    #[test]
    fn custom_vol_scale_ranges_apply_per_side() {
        let specs = build_level_specs(100.0, 101.0, 0.01, 3, [0.2, 0.6], [0.1, 0.9]);
        assert_eq!(specs.len(), 6);
        assert_eq!(specs[0].side, Side::Sell);
        assert!((specs[0].offset - 0.001).abs() < 1e-12);
        assert_eq!(specs[1].side, Side::Buy);
        assert!((specs[1].offset - 0.002).abs() < 1e-12);
        assert_eq!(specs[4].side, Side::Sell);
        assert!((specs[4].offset - 0.009).abs() < 1e-12);
        assert_eq!(specs[5].side, Side::Buy);
        assert!((specs[5].offset - 0.006).abs() < 1e-12);
    }
}
