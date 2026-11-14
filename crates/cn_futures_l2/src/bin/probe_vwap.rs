//! One-off: print close vs vwap for a few rewritten 1min files.

use anyhow::Result;
use cn_futures_l2::export_1min::read_day_parquet;
use std::path::PathBuf;

fn main() -> Result<()> {
    let paths = [
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min/ccfx/IF/20240102.parquet",
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min/xsge/RB/20240102.parquet",
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min/xdce/I/20240102.parquet",
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min/ccfx/T/20240102.parquet",
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min/xsie/EC/20230821.parquet",
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min_hfq/xsie/EC/20230821.parquet",
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min_hfq/xsie/EC/20240513.parquet",
        "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/ylabel_1min/xsie/EC/20230821.parquet",
    ];
    for path in paths {
        let rows = read_day_parquet(&PathBuf::from(path))?;
        let mut n = 0u64;
        let mut traded = 0u64;
        let mut ratio_sum = 0.0;
        let mut ratio_max = 0.0f64;
        let mut sample = None;
        for row in &rows {
            n += 1;
            if row.volume <= 0.0 {
                continue;
            }
            let Some(close) = row.close.filter(|px| *px > 0.0) else {
                continue;
            };
            let Some(vwap) = row.vwap.filter(|px| *px > 0.0) else {
                continue;
            };
            traded += 1;
            let ratio = vwap / close;
            ratio_sum += ratio;
            ratio_max = ratio_max.max(ratio.max(1.0 / ratio));
            if sample.is_none() {
                sample = Some((close, vwap, row.amount, row.volume));
            }
        }
        let mean = if traded == 0 {
            0.0
        } else {
            ratio_sum / traded as f64
        };
        eprintln!(
            "{} rows={n} traded={traded} mean_vwap/close={mean:.4} max_abs_ratio={ratio_max:.4} sample={sample:?}",
            path.rsplit('/')
                .take(3)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("/")
        );
    }
    Ok(())
}
