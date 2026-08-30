use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use cme_tas_replay::product::quote_last_merge;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, Options, SingleThreaded};
use std::path::{Path, PathBuf};

type ReadDb = DBWithThreadMode<SingleThreaded>;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_db_compare")]
#[command(about = "Compare two all-product TAS RocksDBs by logical key/value content")]
struct Args {
    left: PathBuf,
    right: PathBuf,
}

fn cf_options() -> Options {
    let mut options = Options::default();
    options.set_merge_operator_associative("quote_last", quote_last_merge);
    options
}

fn list_cfs(path: &Path) -> Result<Vec<String>> {
    let mut names = ReadDb::list_cf(&Options::default(), path)
        .with_context(|| format!("list column families in {}", path.display()))?;
    names.sort();
    Ok(names)
}

fn open(path: &Path, names: &[String]) -> Result<ReadDb> {
    let descriptors = names
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(name, cf_options()))
        .collect::<Vec<_>>();
    ReadDb::open_cf_descriptors_read_only(&Options::default(), path, descriptors, false)
        .with_context(|| format!("open RocksDB {} read-only", path.display()))
}

fn key_prefix(bytes: &[u8]) -> String {
    let mut text = String::new();
    for byte in bytes.iter().take(24) {
        use std::fmt::Write;
        let _ = write!(text, "{byte:02x}");
    }
    text
}

fn compare_cf(left: &ReadDb, right: &ReadDb, name: &str) -> Result<(u64, u64)> {
    let left_cf = left
        .cf_handle(name)
        .ok_or_else(|| anyhow!("left column family {name:?} disappeared"))?;
    let right_cf = right
        .cf_handle(name)
        .ok_or_else(|| anyhow!("right column family {name:?} disappeared"))?;
    let mut left_iter = left.iterator_cf(&left_cf, IteratorMode::Start);
    let mut right_iter = right.iterator_cf(&right_cf, IteratorMode::Start);
    let mut rows = 0u64;
    let mut bytes = 0u64;
    loop {
        match (left_iter.next(), right_iter.next()) {
            (None, None) => return Ok((rows, bytes)),
            (Some(Err(err)), _) => return Err(err).context("iterate left RocksDB"),
            (_, Some(Err(err))) => return Err(err).context("iterate right RocksDB"),
            (Some(Ok((left_key, left_value))), Some(Ok((right_key, right_value)))) => {
                if left_key != right_key {
                    bail!(
                        "column family {name:?} key mismatch at row {rows}: left={} right={}",
                        key_prefix(&left_key),
                        key_prefix(&right_key)
                    );
                }
                if left_value != right_value {
                    bail!(
                        "column family {name:?} value mismatch at row {rows}, key={}",
                        key_prefix(&left_key)
                    );
                }
                rows += 1;
                bytes = bytes
                    .checked_add((left_key.len() + left_value.len()) as u64)
                    .context("logical RocksDB byte count overflow")?;
            }
            (None, Some(Ok((right_key, _)))) => bail!(
                "column family {name:?} left ended first at row {rows}, next right key={}",
                key_prefix(&right_key)
            ),
            (Some(Ok((left_key, _))), None) => bail!(
                "column family {name:?} right ended first at row {rows}, next left key={}",
                key_prefix(&left_key)
            ),
        }
    }
}

fn run(args: &Args) -> Result<()> {
    let left_names = list_cfs(&args.left)?;
    let right_names = list_cfs(&args.right)?;
    if left_names != right_names {
        bail!("column family lists differ: left={left_names:?} right={right_names:?}");
    }
    let left = open(&args.left, &left_names)?;
    let right = open(&args.right, &right_names)?;
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    for name in &left_names {
        let (rows, bytes) = compare_cf(&left, &right, name)?;
        println!("cme_tas_db_compare cf={name} rows={rows} logical_bytes={bytes}");
        total_rows += rows;
        total_bytes += bytes;
    }
    println!(
        "cme_tas_db_compare equal column_families={} rows={} logical_bytes={}",
        left_names.len(),
        total_rows,
        total_bytes
    );
    Ok(())
}

fn main() {
    let args = Args::parse();
    if let Err(err) = run(&args) {
        eprintln!("cme_tas_db_compare failed: {err:?}");
        std::process::exit(1);
    }
}
