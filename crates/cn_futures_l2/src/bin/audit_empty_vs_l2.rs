//! For empty 1min rows with no book, look up L2 second-end depths in that minute.

use anyhow::{Context, Result};
use chrono::{Datelike, TimeZone, Timelike};
use chrono_tz::Asia::Shanghai;
use cn_futures_l2::codec::{
    decode_depth, decode_key, decode_trade, encode_key, product_cf_name, KIND_DEPTH, KIND_TRADE,
};
use cn_futures_l2::db::{open_rocksdb_read_only, DEFAULT_ROCKSDB_DIR};
use cn_futures_l2::export_1min::read_day_parquet;
use cn_futures_l2::export_1s::book_valid;
use cn_futures_l2::universe::product_id;
use rocksdb::{Direction, IteratorMode};
use std::path::PathBuf;

fn main() -> Result<()> {
    let db = open_rocksdb_read_only(DEFAULT_ROCKSDB_DIR.as_ref())?;
    let paths = [
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min/ccfx/IF/20240102.parquet",
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min/xzce/CY/20240102.parquet",
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min/xsge/RB/20240102.parquet",
    ];
    for path in paths {
        let rows = read_day_parquet(&PathBuf::from(path))?;
        eprintln!("=== {path} rows={} ===", rows.len());
        let mut checked = 0u32;
        for row in &rows {
            if row.volume > 0.0 || row.book.is_some() {
                continue;
            }
            let local = Shanghai.timestamp_opt(row.ts, 0).single().context("ts")?;
            // Prefer post-close IF minutes, otherwise first no-book empties.
            let interesting = local.hour() == 15 && local.minute() >= 1 && local.minute() <= 24;
            if !interesting && checked >= 6 {
                continue;
            }
            if interesting || checked < 6 {
                inspect(&db, row)?;
                checked += 1;
            }
            if checked >= 12 {
                break;
            }
        }
    }
    Ok(())
}

fn inspect(
    db: &rocksdb::DBWithThreadMode<rocksdb::MultiThreaded>,
    row: &cn_futures_l2::baseline_1min::BaselineMinute,
) -> Result<()> {
    let local = Shanghai.timestamp_opt(row.ts, 0).single().context("ts")?;
    let instrument = row.contract_id.clone();
    let product = product_id(&instrument).unwrap_or_else(|| instrument.to_ascii_uppercase());
    let cf_name = product_cf_name(local.year(), &product)?;
    let Some(cf) = db.cf_handle(&cf_name) else {
        eprintln!(
            "  {} {} missing cf {cf_name}",
            instrument,
            local.format("%H:%M")
        );
        return Ok(());
    };
    let lo_ns = (row.ts as u64) * 1_000_000_000;
    let hi_ns = lo_ns + 60 * 1_000_000_000;
    let lookback = 6 * 3600 * 1_000_000_000u64;

    let mut in_minute = 0u32;
    let mut invalid_in_minute = 0u32;
    let mut prev: Option<(i64, bool, Option<f64>, Option<f64>)> = None;
    let mut next: Option<(i64, bool, Option<f64>, Option<f64>)> = None;
    let depth_start = encode_key(KIND_DEPTH, &instrument, lo_ns.saturating_sub(lookback), 0)?;
    for item in db.iterator_cf(&cf, IteratorMode::From(&depth_start, Direction::Forward)) {
        let (key, value) = item?;
        let (kind, id, ts_ns, _) = decode_key(&key)?;
        if kind != KIND_DEPTH || id != instrument {
            break;
        }
        let rec = decode_depth(&value)?;
        let ts_sec = (ts_ns / 1_000_000_000) as i64;
        let bid = rec.bid_prices[0];
        let ask = rec.ask_prices[0];
        let valid = book_valid(
            bid.unwrap_or(f64::NAN),
            rec.bid_sizes[0].unwrap_or(f64::NAN),
            ask.unwrap_or(f64::NAN),
            rec.ask_sizes[0].unwrap_or(f64::NAN),
        );
        if ts_ns < lo_ns {
            prev = Some((ts_sec, valid, bid, ask));
            continue;
        }
        if ts_ns >= hi_ns {
            next = Some((ts_sec, valid, bid, ask));
            break;
        }
        in_minute += 1;
        if !valid {
            invalid_in_minute += 1;
        }
    }

    let mut trades = 0u32;
    let trade_start = encode_key(KIND_TRADE, &instrument, lo_ns, 0)?;
    for item in db.iterator_cf(&cf, IteratorMode::From(&trade_start, Direction::Forward)) {
        let (key, value) = item?;
        let (kind, id, ts_ns, _) = decode_key(&key)?;
        if kind != KIND_TRADE || id != instrument || ts_ns >= hi_ns {
            break;
        }
        let _ = decode_trade(&value)?;
        trades += 1;
    }

    let fmt_nb = |item: Option<(i64, bool, Option<f64>, Option<f64>)>, later: bool| {
        item.map(|(ts, ok, bid, ask)| {
            let gap = if later { ts - row.ts } else { row.ts - ts };
            format!("gap={gap}s valid={ok} bid={bid:?} ask={ask:?}")
        })
        .unwrap_or_else(|| "none".into())
    };
    eprintln!(
        "  {} {} vol={} L2_depth_in_min={} invalid={} trades={} prev=[{}] next=[{}]",
        instrument,
        local.format("%H:%M"),
        row.volume,
        in_minute,
        invalid_in_minute,
        trades,
        fmt_nb(prev, false),
        fmt_nb(next, true),
    );
    Ok(())
}
