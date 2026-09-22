use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::symbol_mapper::create_symbol_mapper;
use crate::pre_trade::symbol_util::is_exposure_exempt_asset;
use anyhow::{Context, Result};
use runtime_common::time_util::get_timestamp_us;
use serde::Serialize;
use std::fs;
use std::path::Path;

#[derive(Serialize)]
struct ExecutionSymbolStatus {
    symbol: String,
    venue: String,
    account_signed_qty: f64,
    account_notional: Option<f64>,
    tracked_signed_qty: f64,
    lot_count: usize,
    latest_quantile: Option<f64>,
    latest_model_ts_ms: i64,
}

#[derive(Serialize)]
struct AccountSummary {
    total_equity_usdt: f64,
    total_exposure_usdt: f64,
    total_position_usdt: f64,
    spot_equity_usdt: f64,
    um_unrealized_usdt: f64,
    borrowed_usdt: f64,
    interest_usdt: f64,
    long_notional_usdt: f64,
    short_notional_usdt: f64,
    net_notional_usdt: f64,
    leverage: f64,
    max_leverage: f64,
}

#[derive(Serialize)]
struct AssetExposure {
    asset: String,
    net_qty: f64,
    net_usdt: f64,
}

#[derive(Serialize)]
struct ExecutionStatus {
    updated_ts_us: i64,
    account: AccountSummary,
    exposures: Vec<AssetExposure>,
    symbols: Vec<ExecutionSymbolStatus>,
}

pub fn write_cta_special_execution_status(path: &Path) -> Result<()> {
    let mut snapshots = MonitorChannel::instance()
        .strategy_mgr()
        .borrow()
        .cta_special_snapshots();
    snapshots.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    let symbols = snapshots
        .into_iter()
        .map(|snapshot| {
            let account_signed_qty =
                MonitorChannel::instance().get_position_qty(&snapshot.symbol, snapshot.venue);
            let mark_price = MonitorChannel::instance().mark_price_for_symbol(&snapshot.symbol);
            ExecutionSymbolStatus {
                account_signed_qty,
                account_notional: estimated_account_notional(account_signed_qty, mark_price),
                symbol: snapshot.symbol,
                venue: snapshot.venue.data_pub_slug().to_string(),
                tracked_signed_qty: snapshot.tracked_signed_qty,
                lot_count: snapshot.lot_count,
                latest_quantile: snapshot.latest_quantile,
                latest_model_ts_ms: snapshot.latest_model_ts_ms,
            }
        })
        .collect();
    let exposure_rows = asset_net_exposure_rows();
    let payload = serde_json::to_vec_pretty(&ExecutionStatus {
        updated_ts_us: get_timestamp_us(),
        account: collect_account_summary(&exposure_rows),
        exposures: collect_asset_exposures(exposure_rows),
        symbols,
    })?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create CTA special status dir {}", parent.display()))?;
    }
    let temp = path.with_extension("json.tmp");
    fs::write(&temp, payload)
        .with_context(|| format!("write CTA special execution status {}", temp.display()))?;
    fs::rename(&temp, path).with_context(|| {
        format!(
            "replace CTA special execution status {} -> {}",
            temp.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn asset_net_exposure_rows() -> Vec<(String, f64, f64)> {
    let mon = MonitorChannel::instance();
    let (exposures, _, _, _, _) = mon.basic_state_snapshot();
    let price_snapshot = mon.price_table().borrow().snapshot();
    let price_mapper = create_symbol_mapper(mon.mark_price_exchange());
    let mut rows = Vec::new();
    for (asset, (open_qty, hedge_qty)) in exposures {
        let net_qty = open_qty + hedge_qty;
        if net_qty.abs() <= 1e-12 || is_exposure_exempt_asset(&asset) {
            continue;
        }
        let symbol = price_mapper.asset_to_price_symbol(&asset);
        let mark = price_snapshot
            .get(&symbol)
            .map(|entry| entry.mark_price)
            .filter(|price| price.is_finite() && *price > 0.0);
        let Some(mark) = mark else { continue };
        rows.push((asset, net_qty, net_qty * mark));
    }
    rows.sort_by(|left, right| right.2.abs().total_cmp(&left.2.abs()));
    rows
}

fn collect_account_summary(exposure_rows: &[(String, f64, f64)]) -> AccountSummary {
    let mon = MonitorChannel::instance();
    let (_, total_equity, abs_total_exposure, total_position, um_unrealized) =
        mon.basic_state_snapshot();
    let usdt_snap = mon
        .usdt_snapshot_for_venue(mon.open_venue())
        .unwrap_or_default();
    let (mut long_notional, mut short_notional) = (0.0_f64, 0.0_f64);
    for (_, _, net_usdt) in exposure_rows {
        if *net_usdt > 0.0 {
            long_notional += *net_usdt;
        } else {
            short_notional += -*net_usdt;
        }
    }
    let leverage = if total_equity.abs() <= f64::EPSILON {
        0.0
    } else {
        total_position / total_equity
    };
    AccountSummary {
        total_equity_usdt: total_equity,
        total_exposure_usdt: abs_total_exposure,
        total_position_usdt: total_position,
        spot_equity_usdt: total_equity - um_unrealized,
        um_unrealized_usdt: um_unrealized,
        borrowed_usdt: usdt_snap.borrowed,
        interest_usdt: usdt_snap.cumulative_interest,
        long_notional_usdt: long_notional,
        short_notional_usdt: short_notional,
        net_notional_usdt: long_notional - short_notional,
        leverage,
        max_leverage: PreTradeParamsLoader::instance().max_leverage(),
    }
}

fn collect_asset_exposures(exposure_rows: Vec<(String, f64, f64)>) -> Vec<AssetExposure> {
    exposure_rows
        .into_iter()
        .map(|(asset, net_qty, net_usdt)| AssetExposure {
            asset,
            net_qty,
            net_usdt,
        })
        .collect()
}

fn estimated_account_notional(qty: f64, mark_price: Option<f64>) -> Option<f64> {
    if !qty.is_finite() {
        return None;
    }
    if qty == 0.0 {
        return Some(0.0);
    }
    let mark_price = mark_price.filter(|price| price.is_finite() && *price > 0.0)?;
    Some(qty * mark_price)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_notional_preserves_position_direction() {
        assert_eq!(estimated_account_notional(2.0, Some(50.0)), Some(100.0));
        assert_eq!(estimated_account_notional(-2.0, Some(50.0)), Some(-100.0));
        assert_eq!(estimated_account_notional(0.0, None), Some(0.0));
        assert_eq!(estimated_account_notional(2.0, None), None);
    }
}
