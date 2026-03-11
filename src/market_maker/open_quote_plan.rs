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

fn build_level_specs(mid: f64, vol: f64, levels_per_side: usize) -> Vec<QuotePlanLevelSpec> {
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
        out.push(QuotePlanLevelSpec {
            side: Side::Sell,
            side_level_index,
            offset,
            base_price: mid,
        });
        out.push(QuotePlanLevelSpec {
            side: Side::Buy,
            side_level_index,
            offset,
            base_price: mid,
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
    let vol = volatility;
    let lower_price = mid_price * (1.0 - vol);
    let upper_price = mid_price * (1.0 + vol);
    let specs = build_level_specs(mid_price, vol, orders_per_round as usize);
    if specs.is_empty() {
        return Err(format!(
            "empty level prices symbol={} mid={:.8} lower={:.8} upper={:.8} levels_per_side={}",
            symbol, mid_price, lower_price, upper_price, orders_per_round
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
        symbol: symbol.to_uppercase(),
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
        let specs = build_level_specs(1.0, 0.0008, 8);
        assert_eq!(specs.len(), 16);

        assert_eq!(specs[0].side, Side::Sell);
        assert_eq!(specs[0].side_level_index, 1);
        assert!((specs[0].offset - 0.0001).abs() < 1e-12);
        assert_eq!(specs[1].side, Side::Buy);
        assert_eq!(specs[1].side_level_index, 1);
        assert!((specs[1].offset - 0.0001).abs() < 1e-12);

        assert_eq!(specs[14].side, Side::Sell);
        assert_eq!(specs[14].side_level_index, 8);
        assert!((specs[14].offset - 0.0008).abs() < 1e-12);
        assert_eq!(specs[15].side, Side::Buy);
        assert_eq!(specs[15].side_level_index, 8);
        assert!((specs[15].offset - 0.0008).abs() < 1e-12);
    }
}
