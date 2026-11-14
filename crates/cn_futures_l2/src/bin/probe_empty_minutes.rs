//! Break empty 1min rows into: no-trade-with-book vs densified no-book.

use anyhow::Result;
use chrono::{TimeZone, Timelike};
use chrono_tz::Asia::Shanghai;
use cn_futures_l2::export_1min::read_day_parquet;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

fn list_sample(root: &Path, files_per_product: usize) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for exchange in fs::read_dir(root)? {
        let exchange_path = exchange?.path();
        if !exchange_path.is_dir() {
            continue;
        }
        let Some(exchange) = exchange_path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if exchange.starts_with('_') {
            continue;
        }
        let mut products: Vec<PathBuf> = fs::read_dir(&exchange_path)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_dir())
            .collect();
        products.sort();
        for product_dir in products {
            let mut files: Vec<PathBuf> = fs::read_dir(&product_dir)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
                .collect();
            files.sort();
            if files.is_empty() {
                continue;
            }
            let n = files.len();
            let mut picked = vec![files[0].clone()];
            if n > 1 {
                picked.push(files[n / 3].clone());
                picked.push(files[2 * n / 3].clone());
                picked.push(files[n - 1].clone());
            }
            picked.truncate(files_per_product);
            out.extend(picked);
        }
    }
    Ok(out)
}

fn main() -> Result<()> {
    let root = PathBuf::from(std::env::args().nth(1).unwrap_or_else(|| {
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min".into()
    }));
    let files = list_sample(&root, 4)?;
    let mut rows = 0u64;
    let mut vol0 = 0u64;
    let mut vol0_book = 0u64;
    let mut vol0_nobook = 0u64;
    let mut vol_pos_nobook = 0u64;
    let mut hour_nobook = [0u64; 24];
    let mut by_contract: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new();
    for path in &files {
        let day = read_day_parquet(path)?;
        for row in day {
            rows += 1;
            let has_book = row.book.is_some();
            let empty = row.volume <= 0.0;
            let entry = by_contract
                .entry(row.contract_id.clone())
                .or_insert((0, 0, 0));
            entry.0 += 1;
            if empty {
                vol0 += 1;
                entry.1 += 1;
                if has_book {
                    vol0_book += 1;
                } else {
                    vol0_nobook += 1;
                    entry.2 += 1;
                    let hour = Shanghai
                        .timestamp_opt(row.ts, 0)
                        .single()
                        .map(|t| t.hour() as usize)
                        .unwrap_or(99);
                    if hour < 24 {
                        hour_nobook[hour] += 1;
                    }
                }
            } else if !has_book {
                vol_pos_nobook += 1;
            }
        }
    }
    eprintln!(
        "files={} rows={} vol0={} ({:.2}%) vol0_with_book={} ({:.2}%) vol0_no_book={} ({:.2}%) vol>0_no_book={}",
        files.len(),
        rows,
        vol0,
        100.0 * vol0 as f64 / rows.max(1) as f64,
        vol0_book,
        100.0 * vol0_book as f64 / rows.max(1) as f64,
        vol0_nobook,
        100.0 * vol0_nobook as f64 / rows.max(1) as f64,
        vol_pos_nobook,
    );
    eprintln!("no-book empty by Shanghai hour:");
    for (hour, count) in hour_nobook.iter().enumerate() {
        if *count > 0 {
            eprintln!("  {hour:02}:00  {count}");
        }
    }
    let mut ranked: Vec<_> = by_contract
        .into_iter()
        .map(|(id, (n, empty, nobook))| (empty as f64 / n.max(1) as f64, n, empty, nobook, id))
        .collect();
    ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    eprintln!("highest empty-rate contracts (need >=200 rows):");
    for (rate, n, empty, nobook, id) in ranked.iter().filter(|r| r.1 >= 200).take(15) {
        eprintln!(
            "  {id} rows={n} empty={empty} ({:.1}%) no_book={nobook}",
            100.0 * rate
        );
    }
    ranked.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    eprintln!("lowest empty-rate contracts (need >=200 rows):");
    for (rate, n, empty, nobook, id) in ranked.iter().filter(|r| r.1 >= 200).take(10) {
        eprintln!(
            "  {id} rows={n} empty={empty} ({:.1}%) no_book={nobook}",
            100.0 * rate
        );
    }
    Ok(())
}
