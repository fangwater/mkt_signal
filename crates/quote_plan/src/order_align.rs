use crate::common::{
    align_price_ceil, align_price_floor, is_futures_venue, min_qty_symbol_key as common_min_qty_symbol_key,
    venue_qty_is_contracts, MinQtyLookup, Venue,
};

pub fn min_qty_symbol_key<V>(venue: V, symbol: &str) -> String
where
    V: Into<Venue>,
{
    common_min_qty_symbol_key(venue.into(), symbol)
}

pub fn ensure_supported_mm_open_venue<V>(_venue: V) -> Result<(), String>
where
    V: Into<Venue>,
{
    Ok(())
}

pub fn contract_qty_multiplier<T, V>(table: &T, venue: V, symbol_key: &str) -> Option<f64>
where
    T: MinQtyLookup + ?Sized,
    V: Into<Venue>,
{
    match venue.into() {
        Venue::BinanceFutures => Some(1.0),
        Venue::OkexFutures | Venue::GateFutures => table
            .contract_multiplier_opt(symbol_key)
            .filter(|v| v.is_finite() && *v > 0.0),
        _ => Some(1.0),
    }
}

pub fn align_final_order_qty(raw_qty: f64, step: f64, min_qty: f64) -> (f64, f64) {
    if !raw_qty.is_finite() || raw_qty <= 0.0 {
        return (0.0, 0.0);
    }
    let mut aligned_qty = if step.is_finite() && step > 0.0 {
        align_price_floor(raw_qty, step)
    } else {
        raw_qty
    };
    if min_qty.is_finite() && min_qty > 0.0 && aligned_qty + 1e-12 < min_qty {
        aligned_qty = 0.0;
    }
    let dropped_qty = if raw_qty > aligned_qty { raw_qty - aligned_qty } else { 0.0 };
    (aligned_qty, dropped_qty)
}

pub fn align_order_with_table<T>(
    symbol_key: &str,
    raw_qty: f64,
    raw_price: f64,
    table: &T,
    enforce_min_notional: bool,
) -> Result<(f64, f64), String>
where
    T: MinQtyLookup + ?Sized,
{
    if raw_qty <= 0.0 {
        return Err(format!("symbol={} raw qty invalid raw_qty={}", symbol_key, raw_qty));
    }
    if raw_price <= 0.0 {
        return Err(format!("symbol={} raw price invalid raw_price={}", symbol_key, raw_price));
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
    let mut qty = if step > 0.0 { align_price_floor(raw_qty, step) } else { raw_qty };
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
                    qty = if step > 0.0 { align_price_ceil(required_qty, step) } else { required_qty };
                }
            }
        }
    }
    if qty <= 0.0 {
        return Err(format!("symbol={} aligned qty invalid", symbol_key));
    }
    Ok((qty, price))
}

pub fn align_order_for_venue<T, V>(
    venue: V,
    symbol_key: &str,
    raw_qty_base: f64,
    raw_price: f64,
    table: &T,
) -> Result<(f64, f64), String>
where
    T: MinQtyLookup + ?Sized,
    V: Into<Venue> + Copy,
{
    let venue_qp = venue.into();
    let enforce_min_notional = is_futures_venue(venue_qp);
    let raw_qty = if venue_qty_is_contracts(venue_qp) {
        let contract_size = contract_qty_multiplier(table, venue_qp, symbol_key).ok_or_else(|| {
            format!(
                "symbol={} missing {:?} contract multiplier, cannot convert base qty",
                symbol_key, venue_qp
            )
        })?;
        raw_qty_base / contract_size
    } else {
        raw_qty_base
    };
    align_order_with_table(symbol_key, raw_qty, raw_price, table, enforce_min_notional)
}
