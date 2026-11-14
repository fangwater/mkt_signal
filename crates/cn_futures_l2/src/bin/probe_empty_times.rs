//! Print local times of empty minutes for a few files.

use anyhow::Result;
use chrono::{TimeZone, Timelike};
use chrono_tz::Asia::Shanghai;
use cn_futures_l2::export_1min::read_day_parquet;
use std::collections::BTreeMap;
use std::path::PathBuf;

fn main() -> Result<()> {
    for path in std::env::args().skip(1) {
        let rows = read_day_parquet(&PathBuf::from(&path))?;
        let mut empty_hm: BTreeMap<(u32, u32), (u64, u64)> = BTreeMap::new();
        let mut n = 0u64;
        let mut vol0 = 0u64;
        for row in &rows {
            n += 1;
            if row.volume > 0.0 {
                continue;
            }
            vol0 += 1;
            let local = Shanghai.timestamp_opt(row.ts, 0).single().unwrap();
            let key = (local.hour(), local.minute());
            let entry = empty_hm.entry(key).or_insert((0, 0));
            entry.0 += 1;
            if row.book.is_none() {
                entry.1 += 1;
            }
        }
        eprintln!("{path} rows={n} vol0={vol0}");
        for ((h, m), (all, nobook)) in empty_hm {
            if all > 0 {
                eprintln!("  {h:02}:{m:02} empty={all} no_book={nobook}");
            }
        }
    }
    Ok(())
}
