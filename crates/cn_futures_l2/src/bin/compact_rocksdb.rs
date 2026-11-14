//! Run a full-range manual compaction for every CN L2 column family.

use anyhow::{bail, Context, Result};
use clap::Parser;
use cn_futures_l2::db::{open_rocksdb, L2Db, DEFAULT_ROCKSDB_DIR};
use rocksdb::Options;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(about = "Compact every column family in the CN L2 RocksDB")]
struct Args {
    #[arg(long, default_value = DEFAULT_ROCKSDB_DIR)]
    rocksdb_dir: PathBuf,
}

fn cf_size(db: &L2Db, name: &str) -> Result<u64> {
    let cf = db
        .cf_handle(name)
        .with_context(|| format!("column family {name:?} disappeared"))?;
    Ok(db
        .property_int_value_cf(&cf, "rocksdb.total-sst-files-size")
        .with_context(|| format!("read SST size for column family {name:?}"))?
        .unwrap_or(0))
}

fn background_errors(db: &L2Db, name: &str) -> Result<u64> {
    let cf = db
        .cf_handle(name)
        .with_context(|| format!("column family {name:?} disappeared"))?;
    Ok(db
        .property_int_value_cf(&cf, "rocksdb.background-errors")
        .with_context(|| format!("read background errors for column family {name:?}"))?
        .unwrap_or(0))
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.rocksdb_dir.is_dir() {
        bail!(
            "rocksdb_dir {} does not exist or is not a directory",
            args.rocksdb_dir.display()
        );
    }

    let mut names = L2Db::list_cf(&Options::default(), &args.rocksdb_dir)
        .with_context(|| format!("list column families {}", args.rocksdb_dir.display()))?;
    names.sort();
    let db = open_rocksdb(&args.rocksdb_dir)?;
    let total_before = names.iter().try_fold(0_u64, |total, name| {
        Ok::<_, anyhow::Error>(total + cf_size(&db, name)?)
    })?;

    eprintln!(
        "compact start db={} column_families={} total_sst_bytes={total_before}",
        args.rocksdb_dir.display(),
        names.len()
    );
    let all_started = Instant::now();

    for (index, name) in names.iter().enumerate() {
        let cf = db
            .cf_handle(name)
            .with_context(|| format!("column family {name:?} disappeared"))?;
        let size_before = cf_size(&db, name)?;
        let errors_before = background_errors(&db, name)?;
        let started = Instant::now();

        db.flush_cf(&cf)
            .with_context(|| format!("flush column family {name:?}"))?;
        db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);

        let errors_after = background_errors(&db, name)?;
        if errors_after != errors_before {
            bail!(
                "column family {name:?} background errors changed from {errors_before} to {errors_after}"
            );
        }
        let size_after = cf_size(&db, name)?;
        eprintln!(
            "compact cf={}/{} name={} before_bytes={} after_bytes={} elapsed_secs={:.3}",
            index + 1,
            names.len(),
            name,
            size_before,
            size_after,
            started.elapsed().as_secs_f64()
        );
    }

    let total_after = names.iter().try_fold(0_u64, |total, name| {
        Ok::<_, anyhow::Error>(total + cf_size(&db, name)?)
    })?;
    eprintln!(
        "compact ok column_families={} before_bytes={} after_bytes={} elapsed_secs={:.3}",
        names.len(),
        total_before,
        total_after,
        all_started.elapsed().as_secs_f64()
    );
    Ok(())
}
