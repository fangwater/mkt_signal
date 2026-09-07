//! Inspect one hfq 1min day: volume, book, and causal P[t] counts.

use anyhow::Result;
use cn_futures_l2::export_1min::read_day_parquet;
use cn_futures_l2::ylabel_1m::causal_prices_from_minutes;
use std::path::PathBuf;

fn main() -> Result<()> {
    for path in std::env::args().skip(1) {
        let rows = read_day_parquet(&PathBuf::from(&path))?;
        let mut vol0 = 0u64;
        let mut book = 0u64;
        let mut twap = 0u64;
        let mut vwap = 0u64;
        let mut sample = None;
        for row in &rows {
            if row.volume <= 0.0 {
                vol0 += 1;
            }
            if row.book.is_some() {
                book += 1;
            }
            if row.twap.filter(|px| *px > 0.0).is_some() {
                twap += 1;
            }
            if row.vwap.filter(|px| *px > 0.0).is_some() {
                vwap += 1;
            }
            if sample.is_none() {
                sample = Some((
                    row.contract_id.clone(),
                    row.ts,
                    row.close,
                    row.vwap,
                    row.volume,
                    row.book.is_some(),
                ));
            }
        }
        let prices = causal_prices_from_minutes(&rows);
        eprintln!(
            "{} rows={} vol0={} book={} twap={} vwap={} causal_pt={} sample={sample:?}",
            path,
            rows.len(),
            vol0,
            book,
            twap,
            vwap,
            prices.len()
        );
    }
    Ok(())
}
