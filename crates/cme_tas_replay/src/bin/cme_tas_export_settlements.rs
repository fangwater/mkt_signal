//! Export settlement events for the narrow volume-roll research baseline.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{Datelike, NaiveDate};
use clap::Parser;
use cme_tas_replay::product::{product_cf_name, quote_last_merge, ALL_KEY_LEN};
use cme_tas_replay::{decode_cme_settlement, KIND_CME_SETTLEMENT, MISSING_PRICE};
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, DB};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

const DEFAULT_DB: &str = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb_all_products";
const DEFAULT_SECONDARY: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_settlement_export.secondary";
const SUPPORTED_PRODUCTS: &[&str] = &["ES", "NQ", "RTY", "YM", "GC", "CL"];

#[derive(Parser, Debug)]
#[command(name = "cme_tas_export_settlements")]
#[command(about = "Export settlement events from all-products TAS RocksDB")]
struct Args {
    #[arg(long, default_value = DEFAULT_DB)]
    rocksdb_dir: PathBuf,
    #[arg(long, default_value = DEFAULT_SECONDARY)]
    secondary_dir: PathBuf,
    /// Open the completed database directly. Refuse this while a writer exists.
    #[arg(long)]
    direct_read_only: bool,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    start: NaiveDate,
    #[arg(long)]
    end: NaiveDate,
    #[arg(long, default_value = "ES,NQ,RTY,YM,GC,CL")]
    products: String,
    /// Keep source settlement events that carry a Date but no Price.
    #[arg(long)]
    include_missing_price: bool,
    #[arg(long)]
    overwrite: bool,
}

#[derive(Serialize)]
struct SettlementRow {
    product: String,
    ric: String,
    source_date: String,
    published_ts_utc_ns: u64,
    price_e9: i64,
    part: u16,
    seq: u32,
}

#[derive(Serialize)]
struct SettlementAuditRow {
    product: String,
    ric: String,
    source_date: String,
    published_ts_utc_ns: u64,
    price_e9: Option<i64>,
    has_price: bool,
    part: u16,
    seq: u32,
}

fn cf_options() -> Options {
    let mut options = Options::default();
    options.set_merge_operator_associative("quote_last", quote_last_merge);
    options
}

fn parse_products(value: &str) -> Result<Vec<String>> {
    let products = value
        .split(',')
        .map(str::trim)
        .filter(|product| !product.is_empty())
        .map(str::to_ascii_uppercase)
        .collect::<BTreeSet<_>>();
    if products.is_empty() {
        bail!("products cannot be empty");
    }
    let unsupported = products
        .iter()
        .filter(|product| !SUPPORTED_PRODUCTS.contains(&product.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        bail!("unsupported volume-roll products: {unsupported:?}");
    }
    Ok(products.into_iter().collect())
}

fn open_input_db(args: &Args, products: &[String]) -> Result<DB> {
    let mut names = vec!["default".to_string(), "replay_meta".to_string()];
    for year in args.start.year()..=args.end.year() {
        let year = u16::try_from(year).context("year does not fit u16")?;
        for product in products {
            names.push(product_cf_name(year, product)?);
        }
    }
    names.sort();
    names.dedup();
    let descriptors = names
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, cf_options()))
        .collect::<Vec<_>>();
    if args.direct_read_only {
        return DB::open_cf_descriptors_read_only(
            &Options::default(),
            &args.rocksdb_dir,
            descriptors,
            false,
        )
        .with_context(|| format!("open read-only RocksDB {}", args.rocksdb_dir.display()));
    }
    if let Some(parent) = args.secondary_dir.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create secondary parent {}", parent.display()))?;
    }
    DB::open_cf_descriptors_as_secondary(
        &Options::default(),
        &args.rocksdb_dir,
        &args.secondary_dir,
        descriptors,
    )
    .with_context(|| {
        format!(
            "open secondary {} from {}",
            args.secondary_dir.display(),
            args.rocksdb_dir.display()
        )
    })
}

fn source_date(value: u32) -> Result<NaiveDate> {
    if value == 0 {
        bail!("settlement source Date is missing");
    }
    NaiveDate::parse_from_str(&format!("{value:08}"), "%Y%m%d")
        .with_context(|| format!("parse settlement source Date {value}"))
}

fn key_part_seq(key: &[u8]) -> Result<(u16, u32)> {
    if key.len() != ALL_KEY_LEN {
        bail!(
            "all-products key is {} bytes, expected {ALL_KEY_LEN}",
            key.len()
        );
    }
    let tail = &key[ALL_KEY_LEN - 6..];
    Ok((
        u16::from_be_bytes(tail[..2].try_into().unwrap()),
        u32::from_be_bytes(tail[2..].try_into().unwrap()),
    ))
}

fn prepare_output(path: &Path, overwrite: bool) -> Result<PathBuf> {
    if path.exists() && !overwrite {
        bail!("output {} already exists; pass --overwrite", path.display());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output parent {}", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow!("output {} has no filename", path.display()))?;
    let partial = path.with_file_name(format!("{}.partial", file_name.to_string_lossy()));
    if partial.exists() {
        if !overwrite {
            bail!("partial output {} already exists", partial.display());
        }
        fs::remove_file(&partial)
            .with_context(|| format!("remove stale partial {}", partial.display()))?;
    }
    Ok(partial)
}

fn run(args: &Args) -> Result<()> {
    if args.end < args.start {
        bail!("end {} precedes start {}", args.end, args.start);
    }
    let products = parse_products(&args.products)?;
    let partial = prepare_output(&args.output, args.overwrite)?;
    let db = open_input_db(args, &products)?;
    let file = File::create(&partial)
        .with_context(|| format!("create settlement output {}", partial.display()))?;
    let mut writer = csv::Writer::from_writer(file);
    let mut rows = 0u64;

    for year in args.start.year()..=args.end.year() {
        let year_u16 = u16::try_from(year).context("year does not fit u16")?;
        for product in &products {
            let name = product_cf_name(year_u16, product)?;
            let cf = db
                .cf_handle(&name)
                .ok_or_else(|| anyhow!("missing expected column family {name}"))?;
            for item in db.iterator_cf(
                &cf,
                IteratorMode::From(&[KIND_CME_SETTLEMENT], Direction::Forward),
            ) {
                let (key, value) = item.with_context(|| format!("scan {name}"))?;
                match key.first().copied() {
                    Some(KIND_CME_SETTLEMENT) => {}
                    Some(kind) if kind > KIND_CME_SETTLEMENT => break,
                    Some(_) => continue,
                    None => bail!("empty RocksDB key in {name}"),
                }
                let record = decode_cme_settlement(&value)
                    .with_context(|| format!("decode settlement in {name}"))?;
                if record.price == MISSING_PRICE && !args.include_missing_price {
                    continue;
                }
                let day = source_date(record.source_date_yyyymmdd)?;
                if day < args.start || day > args.end {
                    continue;
                }
                let (part, seq) = key_part_seq(&key)?;
                if args.include_missing_price {
                    writer.serialize(SettlementAuditRow {
                        product: product.clone(),
                        ric: record.ric,
                        source_date: day.to_string(),
                        published_ts_utc_ns: record.ts_utc_ns,
                        price_e9: (record.price != MISSING_PRICE).then_some(record.price),
                        has_price: record.price != MISSING_PRICE,
                        part,
                        seq,
                    })?;
                } else {
                    writer.serialize(SettlementRow {
                        product: product.clone(),
                        ric: record.ric,
                        source_date: day.to_string(),
                        published_ts_utc_ns: record.ts_utc_ns,
                        price_e9: record.price,
                        part,
                        seq,
                    })?;
                }
                rows += 1;
            }
        }
    }
    writer.flush()?;
    drop(writer);
    if args.output.exists() {
        fs::remove_file(&args.output)
            .with_context(|| format!("replace output {}", args.output.display()))?;
    }
    fs::rename(&partial, &args.output).with_context(|| {
        format!(
            "publish settlement output {} -> {}",
            partial.display(),
            args.output.display()
        )
    })?;
    println!(
        "cme_tas_export_settlements products={products:?} start={} end={} rows={} output={}",
        args.start,
        args.end,
        rows,
        args.output.display()
    );
    Ok(())
}

fn main() {
    let args = Args::parse();
    if let Err(error) = run(&args) {
        eprintln!("cme_tas_export_settlements failed: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{key_part_seq, parse_products, source_date};
    use chrono::NaiveDate;
    use cme_tas_replay::product::encode_all_key;
    use cme_tas_replay::KIND_CME_SETTLEMENT;

    #[test]
    fn products_are_narrow_and_deduplicated() {
        assert_eq!(parse_products("NQ,ES,NQ").unwrap(), ["ES", "NQ"]);
        assert!(parse_products("ETH").is_err());
    }

    #[test]
    fn settlement_source_date_and_key_order_round_trip() {
        assert_eq!(
            source_date(20240328).unwrap(),
            NaiveDate::from_ymd_opt(2024, 3, 28).unwrap()
        );
        let key = encode_all_key(KIND_CME_SETTLEMENT, "ESH24", 1, 7, 9).unwrap();
        assert_eq!(key_part_seq(&key).unwrap(), (7, 9));
    }
}
