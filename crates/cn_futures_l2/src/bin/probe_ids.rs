//! Print contract_id set in a 1min parquet.

use anyhow::Result;
use cn_futures_l2::export_1min::read_day_parquet;
use std::collections::BTreeSet;
use std::path::PathBuf;

fn main() -> Result<()> {
    for path in std::env::args().skip(1) {
        let rows = read_day_parquet(&PathBuf::from(&path))?;
        let mut ids = BTreeSet::new();
        for row in &rows {
            ids.insert(row.contract_id.clone());
        }
        eprintln!("{} n={} ids={:?}", path, rows.len(), ids);
    }
    Ok(())
}
