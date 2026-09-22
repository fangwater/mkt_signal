use crate::pre_trade::monitor_channel::MonitorChannel;
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
struct ExecutionStatus {
    updated_ts_us: i64,
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
    let payload = serde_json::to_vec_pretty(&ExecutionStatus {
        updated_ts_us: get_timestamp_us(),
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
