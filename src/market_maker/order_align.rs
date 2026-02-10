use crate::signal::common::{align_price_ceil, align_price_floor, TradingVenue};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;

pub fn min_qty_symbol_key(venue: TradingVenue, symbol: &str) -> String {
    match venue {
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
            symbol.to_uppercase().replace("-SWAP", "").replace('-', "")
        }
        TradingVenue::GateMargin | TradingVenue::GateFutures => {
            symbol.to_uppercase().replace('_', "").replace('-', "")
        }
        _ => symbol.to_uppercase(),
    }
}

pub fn is_futures_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures
            | TradingVenue::OkexFutures
            | TradingVenue::BybitFutures
            | TradingVenue::BitgetFutures
            | TradingVenue::GateFutures
    )
}

pub fn ensure_supported_mm_open_venue(venue: TradingVenue) -> Result<(), String> {
    let _ = venue;
    Ok(())
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

pub fn align_order_with_table(
    symbol_key: &str,
    raw_qty: f64,
    raw_price: f64,
    table: &VenueMinQtyTable,
    enforce_min_notional: bool,
) -> Result<(f64, f64), String> {
    if raw_qty <= 0.0 {
        return Err(format!(
            "symbol={} raw qty invalid raw_qty={}",
            symbol_key, raw_qty
        ));
    }
    if raw_price <= 0.0 {
        return Err(format!(
            "symbol={} raw price invalid raw_price={}",
            symbol_key, raw_price
        ));
    }

    let price_tick = table.price_tick(symbol_key).unwrap_or(0.0);
    let price = if price_tick > 0.0 {
        align_price_floor(raw_price, price_tick)
    } else {
        raw_price
    };
    if price <= 0.0 {
        return Err(format!("symbol={} aligned price invalid", symbol_key));
    }

    let step = table.step_size(symbol_key).unwrap_or(0.0);
    let mut qty = if step > 0.0 {
        align_price_floor(raw_qty, step)
    } else {
        raw_qty
    };

    if let Some(min_qty) = table.min_qty(symbol_key) {
        if min_qty > 0.0 && qty < min_qty {
            qty = min_qty;
        }
    }

    if enforce_min_notional {
        if let Some(min_notional) = table.min_notional(symbol_key) {
            if min_notional > 0.0 {
                let required_qty = min_notional / price;
                if qty < required_qty {
                    qty = if step > 0.0 {
                        align_price_ceil(required_qty, step)
                    } else {
                        required_qty
                    };
                }
            }
        }
    }

    if qty <= 0.0 {
        return Err(format!("symbol={} aligned qty invalid", symbol_key));
    }

    Ok((qty, price))
}

pub fn align_order_for_venue(
    venue: TradingVenue,
    symbol_key: &str,
    raw_qty_base: f64,
    raw_price: f64,
    table: &VenueMinQtyTable,
) -> Result<(f64, f64), String> {
    let enforce_min_notional = is_futures_venue(venue);
    let raw_qty = if venue_qty_is_contracts(venue) {
        let contract_size = contract_qty_multiplier(table, venue, symbol_key).ok_or_else(|| {
            format!(
                "symbol={} missing {:?} contract multiplier, cannot convert base qty",
                symbol_key, venue
            )
        })?;
        raw_qty_base / contract_size
    } else {
        raw_qty_base
    };

    align_order_with_table(symbol_key, raw_qty, raw_price, table, enforce_min_notional)
}
