use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use log::info;

use super::order_update::{CF_ORDER_UPDATE, CF_ORDER_UPDATE_UNMATCHED};
use super::parquet::{
    build_parquet_cancel, build_parquet_hedge, build_parquet_open,
    build_parquet_order_updates, build_parquet_trade_updates, RangeFilter,
};
use super::signal::{CF_ARB_CANCEL, CF_ARB_CLOSE, CF_ARB_HEDGE, CF_ARB_OPEN};
use super::storage::RocksDbStore;
use super::trade_update::{CF_TRADE_UPDATE, CF_TRADE_UPDATE_UNMATCHED};

fn write_parquet(output_dir: &Path, name: &str, bytes: Vec<u8>) -> Result<()> {
    let path = output_dir.join(name);
    fs::write(&path, &bytes).with_context(|| format!("failed to write {}", path.display()))?;
    info!("exported {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

pub fn export_all_to_dir(store: &RocksDbStore, output_dir: &Path) -> Result<()> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output_dir {}", output_dir.display()))?;

    let range = RangeFilter::all();

    let entries = store.scan(CF_ARB_OPEN, None, false, None)?;
    let parquet = build_parquet_open(entries, &range)?;
    write_parquet(output_dir, "signals_arb_open.parquet", parquet)?;

    let entries = store.scan(CF_ARB_HEDGE, None, false, None)?;
    let parquet = build_parquet_hedge(entries, &range)?;
    write_parquet(output_dir, "signals_arb_hedge.parquet", parquet)?;

    let entries = store.scan(CF_ARB_CANCEL, None, false, None)?;
    let parquet = build_parquet_cancel(entries, &range)?;
    write_parquet(output_dir, "signals_arb_cancel.parquet", parquet)?;

    let entries = store.scan(CF_ARB_CLOSE, None, false, None)?;
    let parquet = build_parquet_open(entries, &range)?;
    write_parquet(output_dir, "signals_arb_close.parquet", parquet)?;

    let entries = store.scan(CF_ORDER_UPDATE, None, false, None)?;
    let parquet = build_parquet_order_updates(entries, &range)?;
    write_parquet(output_dir, "order_updates.parquet", parquet)?;

    let entries = store.scan(CF_ORDER_UPDATE_UNMATCHED, None, false, None)?;
    let parquet = build_parquet_order_updates(entries, &range)?;
    write_parquet(output_dir, "order_updates_unmatched.parquet", parquet)?;

    let entries = store.scan(CF_TRADE_UPDATE, None, false, None)?;
    let parquet = build_parquet_trade_updates(entries, &range)?;
    write_parquet(output_dir, "trade_updates.parquet", parquet)?;

    let entries = store.scan(CF_TRADE_UPDATE_UNMATCHED, None, false, None)?;
    let parquet = build_parquet_trade_updates(entries, &range)?;
    write_parquet(output_dir, "trade_updates_unmatched.parquet", parquet)?;

    Ok(())
}
