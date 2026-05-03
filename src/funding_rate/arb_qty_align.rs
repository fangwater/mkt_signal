use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;
use crate::signal::common::{align_price_ceil, align_price_floor};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;

use super::common::Quote;

pub fn min_qty_symbol_key(venue: TradingVenue, trade_symbol: &str) -> String {
    match venue {
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => trade_symbol
            .to_uppercase()
            .replace("-SWAP", "")
            .replace('-', ""),
        TradingVenue::GateMargin | TradingVenue::GateFutures => trade_symbol
            .to_uppercase()
            .replace(['_', '-'], ""),
        _ => trade_symbol.to_uppercase(),
    }
}

pub fn venue_qty_is_contracts(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures | TradingVenue::OkexFutures | TradingVenue::GateFutures
    )
}

pub fn contract_qty_multiplier(
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    symbol_key: &str,
) -> Option<f64> {
    match venue {
        TradingVenue::BinanceFutures => Some(1.0),
        TradingVenue::OkexFutures | TradingVenue::GateFutures => table
            .contract_multiplier_opt(symbol_key)
            .filter(|v| v.is_finite() && *v > 0.0),
        _ => Some(1.0),
    }
}

pub fn base_step_size(
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    trade_symbol: &str,
) -> (Option<f64>, Option<f64>) {
    let symbol_key = min_qty_symbol_key(venue, trade_symbol);
    let step = table.step_size(&symbol_key);
    let min_qty = table.min_qty(&symbol_key);

    if venue_qty_is_contracts(venue) {
        let mult = contract_qty_multiplier(table, venue, &symbol_key);
        let step_base = step.zip(mult).map(|(s, m)| s * m);
        let min_qty_base = min_qty.zip(mult).map(|(q, m)| q * m);
        (step_base, min_qty_base)
    } else {
        (step, min_qty)
    }
}

pub fn convert_aligned_base_qty_to_open_venue_qty(
    table: &VenueMinQtyTable,
    open_venue: TradingVenue,
    open_symbol: &str,
    open_price: f64,
    aligned_base_qty: f64,
) -> f64 {
    if aligned_base_qty <= 0.0 || open_price <= 0.0 {
        return 0.0;
    }

    let symbol_key = min_qty_symbol_key(open_venue, open_symbol);

    let qty_multiplier = if venue_qty_is_contracts(open_venue) {
        let Some(multiplier) = contract_qty_multiplier(table, open_venue, &symbol_key) else {
            return 0.0;
        };
        multiplier
    } else {
        1.0
    };

    let mut venue_qty = aligned_base_qty / qty_multiplier;
    let step = table.step_size(&symbol_key).unwrap_or(0.0);
    if step > 0.0 {
        venue_qty = align_price_floor(venue_qty, step);
        if venue_qty <= 0.0 {
            venue_qty = step;
        }
    }

    if let Some(min_qty) = table.min_qty(&symbol_key) {
        if min_qty > 0.0 && venue_qty < min_qty {
            venue_qty = min_qty;
        }
    }

    if open_venue.is_futures() {
        if let Some(min_notional) = table.min_notional(&symbol_key) {
            if min_notional > 0.0 {
                let required_base_qty = min_notional / open_price;
                let required_venue_qty = required_base_qty / qty_multiplier;
                if venue_qty + 1e-12 < required_venue_qty {
                    venue_qty = if step > 0.0 {
                        align_price_ceil(required_venue_qty, step)
                    } else {
                        required_venue_qty
                    };
                }
            }
        }
    }

    venue_qty
}

pub fn convert_order_amount_to_aligned_base_qty(
    order_amount: f32,
    open_table: &VenueMinQtyTable,
    open_venue: TradingVenue,
    open_symbol: &str,
    hedge_table: &VenueMinQtyTable,
    hedge_venue: TradingVenue,
    hedge_symbol: &str,
    _open_quote: &Quote,
    hedge_quote: &Quote,
    open_price: f64,
    open_side: Side,
) -> f64 {
    if !(order_amount > 0.0) || open_price <= 0.0 {
        return 0.0;
    }

    let raw_base_qty = order_amount as f64 / open_price;
    let (open_base_step, open_min_base_qty) = base_step_size(open_table, open_venue, open_symbol);
    let (hedge_base_step, hedge_min_base_qty) =
        base_step_size(hedge_table, hedge_venue, hedge_symbol);

    let align_step = open_base_step
        .unwrap_or(0.0)
        .max(hedge_base_step.unwrap_or(0.0));
    if align_step <= 0.0 {
        return raw_base_qty;
    }

    let mut base_qty = align_price_floor(raw_base_qty, align_step);
    if base_qty <= 0.0 {
        base_qty = align_step;
    }

    let mut required_min_base = align_step;
    if let Some(v) = open_min_base_qty {
        if v > required_min_base {
            required_min_base = v;
        }
    }
    if let Some(v) = hedge_min_base_qty {
        if v > required_min_base {
            required_min_base = v;
        }
    }

    let open_symbol_key = min_qty_symbol_key(open_venue, open_symbol);
    let hedge_symbol_key = min_qty_symbol_key(hedge_venue, hedge_symbol);
    if let Some(min_notional) = open_table.min_notional(&open_symbol_key) {
        if min_notional > 0.0 && open_price > 0.0 {
            required_min_base = required_min_base.max(min_notional / open_price);
        }
    }
    let hedge_side = if open_side == Side::Buy {
        Side::Sell
    } else {
        Side::Buy
    };
    let hedge_price_for_min_notional = match hedge_side {
        Side::Buy => hedge_quote.ask,
        Side::Sell => hedge_quote.bid,
    };
    if let Some(min_notional) = hedge_table.min_notional(&hedge_symbol_key) {
        if min_notional > 0.0 && hedge_price_for_min_notional > 0.0 {
            required_min_base = required_min_base.max(min_notional / hedge_price_for_min_notional);
        }
    }

    if base_qty + 1e-12 < required_min_base {
        base_qty = align_price_ceil(required_min_base, align_step);
    }

    base_qty
}
