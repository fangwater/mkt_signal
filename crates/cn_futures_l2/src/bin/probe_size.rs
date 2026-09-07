//! Print size-bucket sums for a few 1min files.

use anyhow::Result;
use cn_futures_l2::export_1min::read_day_parquet;
use std::path::PathBuf;

fn main() -> Result<()> {
    for path in [
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min/xsge/RB/20240102.parquet",
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min_hfq/xsge/RB/20240102.parquet",
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min_hfq/ccfx/IF/20240102.parquet",
    ] {
        let rows = read_day_parquet(&PathBuf::from(path))?;
        let mut large = 0.0;
        let mut medium = 0.0;
        let mut small = 0.0;
        let mut amount = 0.0;
        let mut n_pos = 0u64;
        for row in &rows {
            large += row.large_order;
            medium += row.medium_order;
            small += row.small_order;
            amount += row.amount;
            if row.large_order + row.medium_order + row.small_order > 0.0 {
                n_pos += 1;
            }
        }
        let bucket = large + medium + small;
        eprintln!(
            "{} rows={} with_size={n_pos} large={large:.0} med={medium:.0} small={small:.0} amount={amount:.0} bucket/amount={:.4}",
            path.rsplit('/')
                .take(3)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("/"),
            rows.len(),
            if amount > 0.0 { bucket / amount } else { 0.0 }
        );
    }
    Ok(())
}
