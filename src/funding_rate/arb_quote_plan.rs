use crate::market_maker::quote_plan_levels::{
    build_quote_plan_levels, QuotePlanLevel, QuotePlanLevelSpec,
};
use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;
use crate::signal::venue_min_qty_table::VenueMinQtyTable;

use super::common::Quote;

#[derive(Debug, Clone)]
pub struct ArbOpenQuotePlan {
    pub side: Side,
    pub inner_price: f64,
    pub outer_price: f64,
    pub price_tick: f64,
    pub qty_tick: f64,
    pub levels: Vec<QuotePlanLevel>,
}

pub fn build_arb_level_specs(
    side: Side,
    inner_price: f64,
    volatility: f64,
    level_count: usize,
) -> Vec<QuotePlanLevelSpec> {
    if level_count == 0 || !inner_price.is_finite() || inner_price <= 0.0 {
        return Vec::new();
    }
    if !volatility.is_finite() || volatility < 0.0 {
        return Vec::new();
    }
    if level_count == 1 || volatility <= 0.0 {
        return vec![QuotePlanLevelSpec {
            side,
            side_level_index: 1,
            offset: 0.0,
            base_price: inner_price,
        }];
    }

    let step = volatility / (level_count - 1) as f64;
    (0..level_count)
        .map(|idx| QuotePlanLevelSpec {
            side,
            side_level_index: idx + 1,
            offset: step * idx as f64,
            base_price: inner_price,
        })
        .collect()
}

pub fn build_arb_open_quote_plan(
    venue: TradingVenue,
    symbol: &str,
    quote: Quote,
    order_amount_u: f64,
    orders_per_round: u32,
    side: Side,
    volatility: f64,
    table: &VenueMinQtyTable,
) -> Result<ArbOpenQuotePlan, String> {
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
    if !volatility.is_finite() || volatility < 0.0 {
        return Err(format!(
            "invalid volatility symbol={} volatility={}",
            symbol, volatility
        ));
    }

    let inner_price = match side {
        Side::Buy => quote.bid,
        Side::Sell => quote.ask,
    };
    let outer_price = match side {
        Side::Buy => inner_price * (1.0 - volatility),
        Side::Sell => inner_price * (1.0 + volatility),
    };
    let specs = build_arb_level_specs(side, inner_price, volatility, orders_per_round as usize);
    if specs.is_empty() {
        return Err(format!(
            "empty arb level specs symbol={} side={:?} inner={:.8} volatility={:.8} levels={}",
            symbol, side, inner_price, volatility, orders_per_round
        ));
    }

    let (price_tick, qty_tick, levels) =
        build_quote_plan_levels(venue, symbol, order_amount_u, &specs, table)?;
    if levels.is_empty() {
        return Err(format!(
            "empty levels after alignment symbol={} side={:?} levels={}",
            symbol, side, orders_per_round
        ));
    }

    Ok(ArbOpenQuotePlan {
        side,
        inner_price,
        outer_price,
        price_tick,
        qty_tick,
        levels,
    })
}
