//! Patch only the TWAP column of published CN 1-minute baseline parquet.
//!
//! Raw rows keep their existing grid and all non-TWAP values. HFQ rows take
//! the patched dominant raw TWAP plus the already-published additive gap.

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use cn_futures_l2::baseline_1min::{twap_for_existing_grid, PrintTrade, TwapGridState};
use cn_futures_l2::codec::{decode_key, decode_trade, encode_key, KIND_TRADE};
use cn_futures_l2::db::{open_rocksdb_read_only, L2Db, DEFAULT_ROCKSDB_DIR};
use cn_futures_l2::export_1min::{read_day_parquet, write_day_parquet};
use cn_futures_l2::export_1s::parse_product_cf;
use cn_futures_l2::hfq::{
    apply_hfq_series, gap_before, index_dominants, instrument_match_key, load_adjustments,
    load_dominants,
};
use cn_futures_l2::source::parse_day;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_RAW_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min";
const DEFAULT_HFQ_ROOT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min_hfq";
const DEFAULT_ROLL_ROOT: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/cn_roll_replay/2019_20260826/result_no_rollback";

#[derive(Parser, Debug)]
#[command(about = "Patch CN raw and HFQ 1-minute TWAP columns into staging roots")]
struct Args {
    #[arg(long, default_value = DEFAULT_ROCKSDB_DIR)]
    rocksdb_dir: PathBuf,
    #[arg(long, default_value = DEFAULT_RAW_ROOT)]
    raw_input_root: PathBuf,
    #[arg(long)]
    raw_output_root: PathBuf,
    #[arg(long, default_value = DEFAULT_HFQ_ROOT)]
    hfq_input_root: PathBuf,
    #[arg(long)]
    hfq_output_root: PathBuf,
    #[arg(long, default_value = DEFAULT_ROLL_ROOT)]
    roll_root: PathBuf,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: String,
    #[arg(long)]
    product: Option<String>,
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Clone)]
struct DayFile {
    day: NaiveDate,
    path: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let start = parse_day(&args.start)?;
    let end = parse_day(&args.end)?;
    if end < start {
        bail!("end precedes start");
    }
    for (label, input, output) in [
        ("raw", &args.raw_input_root, &args.raw_output_root),
        ("hfq", &args.hfq_input_root, &args.hfq_output_root),
    ] {
        if input == output {
            bail!("{label} input and output roots must differ");
        }
        if output.to_string_lossy().contains("cme_tas_rocksdb") {
            bail!("refusing CME RocksDB output root {}", output.display());
        }
    }
    let products = args.product.as_deref().map(parse_products).transpose()?;
    let db = open_rocksdb_read_only(&args.rocksdb_dir)?;
    let cf_names = L2Db::list_cf(&rocksdb::Options::default(), &args.rocksdb_dir)?;
    let raw = patch_raw(&args, &db, &cf_names, start, end, products.as_deref())?;
    let hfq = patch_hfq(&args, start, end, products.as_deref())?;
    eprintln!(
        "patch_1min_twap complete raw_files={} raw_changed={} hfq_files={} hfq_changed={} dry_run={}",
        raw.0, raw.1, hfq.0, hfq.1, args.dry_run
    );
    Ok(())
}

fn parse_products(text: &str) -> Result<Vec<String>> {
    let values = text
        .split(',')
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if values.is_empty() {
        bail!("--product contains no product names");
    }
    Ok(values)
}

fn list_days(root: &Path, exchange: &str, product: &str) -> Result<Vec<DayFile>> {
    let dir = root.join(exchange).join(product);
    let mut files = Vec::new();
    if !dir.is_dir() {
        return Ok(files);
    }
    for entry in fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("parquet") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("");
        if let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") {
            files.push(DayFile { day, path });
        }
    }
    files.sort_by_key(|file| file.day);
    Ok(files)
}

fn product_jobs(root: &Path, filter: Option<&[String]>) -> Result<Vec<(String, String)>> {
    let mut jobs = Vec::new();
    for exchange in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
        let exchange = exchange?.path();
        if !exchange.is_dir() {
            continue;
        }
        let Some(exchange_name) = exchange.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if exchange_name.starts_with('_') {
            continue;
        }
        for product in fs::read_dir(&exchange)? {
            let product = product?.path();
            if !product.is_dir() {
                continue;
            }
            let Some(product_name) = product.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if filter.is_some_and(|values| !values.iter().any(|value| value == product_name)) {
                continue;
            }
            jobs.push((exchange_name.to_string(), product_name.to_string()));
        }
    }
    jobs.sort();
    Ok(jobs)
}

fn scan_trades(
    db: &L2Db,
    cf_names: &[String],
    product: &str,
    contract_id: &str,
    start: i64,
    end_exclusive: i64,
) -> Result<Vec<PrintTrade>> {
    let start_ns = u64::try_from(start)?.saturating_mul(1_000_000_000);
    let end_ns = u64::try_from(end_exclusive)?.saturating_mul(1_000_000_000);
    let mut trades = Vec::new();
    for name in cf_names {
        let Some((_, cf_product)) = parse_product_cf(name) else {
            continue;
        };
        if cf_product != product {
            continue;
        }
        let cf = db
            .cf_handle(name)
            .with_context(|| format!("missing column family {name}"))?;
        let key = encode_key(KIND_TRADE, contract_id, start_ns, 0)?;
        for item in db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&key, rocksdb::Direction::Forward),
        ) {
            let (key, value) = item?;
            let (kind, id, ts_ns, _) = decode_key(&key)?;
            if kind != KIND_TRADE || id != contract_id {
                break;
            }
            if ts_ns >= end_ns {
                break;
            }
            trades.push(PrintTrade::from(&decode_trade(&value)?));
        }
    }
    Ok(trades)
}

fn patch_raw(
    args: &Args,
    db: &L2Db,
    cf_names: &[String],
    start: NaiveDate,
    end: NaiveDate,
    filter: Option<&[String]>,
) -> Result<(u64, u64)> {
    let mut files = 0u64;
    let mut changed = 0u64;
    for (exchange, product) in product_jobs(&args.raw_input_root, filter)? {
        let mut states = HashMap::<String, TwapGridState>::new();
        for file in list_days(&args.raw_input_root, &exchange, &product)? {
            if file.day > end {
                continue;
            }
            let mut rows = read_day_parquet(&file.path)?;
            let mut grids = BTreeMap::<String, Vec<(usize, i64)>>::new();
            for (index, row) in rows.iter().enumerate() {
                grids
                    .entry(row.contract_id.clone())
                    .or_default()
                    .push((index, row.ts));
            }
            let mut file_changed = 0u64;
            for (contract_id, mut grid) in grids {
                grid.sort_by_key(|(_, ts)| *ts);
                let minutes = grid.iter().map(|(_, ts)| *ts).collect::<Vec<_>>();
                let first = *minutes.first().context("empty TWAP grid")?;
                let last = *minutes.last().context("empty TWAP grid")?;
                let trades = scan_trades(db, cf_names, &product, &contract_id, first, last + 60)?;
                let values = twap_for_existing_grid(
                    &minutes,
                    &trades,
                    states.entry(contract_id).or_default(),
                )?;
                for ((index, _), value) in grid.into_iter().zip(values) {
                    if rows[index].twap != value {
                        file_changed += 1;
                        rows[index].twap = value;
                    }
                }
            }
            if file.day >= start {
                let relative = file.path.strip_prefix(&args.raw_input_root)?;
                let output = args.raw_output_root.join(relative);
                if output.exists() && !args.overwrite {
                    bail!("refusing to overwrite {}", output.display());
                }
                if !args.dry_run {
                    write_day_parquet(&output, &rows)?;
                }
                files += 1;
                changed += file_changed;
                eprintln!(
                    "raw_twap {} {} {} rows={} changed={file_changed}",
                    exchange,
                    product,
                    file.day,
                    rows.len()
                );
            } else {
                eprintln!(
                    "raw_twap_warmup {} {} {} rows={}",
                    exchange,
                    product,
                    file.day,
                    rows.len()
                );
            }
        }
    }
    Ok((files, changed))
}

fn patch_hfq(
    args: &Args,
    start: NaiveDate,
    end: NaiveDate,
    filter: Option<&[String]>,
) -> Result<(u64, u64)> {
    let mut files = 0u64;
    let mut changed = 0u64;
    for (exchange, product) in product_jobs(&args.hfq_input_root, filter)? {
        let dominant_path = args.roll_root.join(format!("{exchange}_dominant.csv"));
        let adjustment_path = args
            .roll_root
            .join(format!("{exchange}_adjustment_factor.csv"));
        let dominants = index_dominants(&load_dominants(&dominant_path)?, &product);
        let adjustments = load_adjustments(&adjustment_path)?
            .into_iter()
            .filter(|row| row.product_id == product)
            .collect::<Vec<_>>();
        for file in list_days(&args.hfq_input_root, &exchange, &product)? {
            if file.day < start || file.day > end {
                continue;
            }
            let dominant = dominants
                .get(&file.day)
                .with_context(|| format!("missing dominant {exchange} {product} {}", file.day))?;
            let gap = gap_before(&adjustments, file.day)?.with_context(|| {
                format!("missing adjustment path {exchange} {product} {}", file.day)
            })?;
            let raw_path = args
                .raw_output_root
                .join(&exchange)
                .join(&product)
                .join(file.path.file_name().context("HFQ path has no file name")?);
            let raw = read_day_parquet(&raw_path)
                .with_context(|| format!("read patched raw {}", raw_path.display()))?;
            let mut raw = raw
                .into_iter()
                .filter(|row| {
                    instrument_match_key(&row.contract_id, file.day)
                        == instrument_match_key(dominant, file.day)
                })
                .collect::<Vec<_>>();
            raw.sort_by_key(|row| row.ts);
            let raw_by_ts = apply_hfq_series(raw, gap)
                .into_iter()
                .map(|row| (row.ts, row.twap))
                .collect::<BTreeMap<_, _>>();
            let mut rows = read_day_parquet(&file.path)?;
            let mut file_changed = 0u64;
            for row in &mut rows {
                let value = raw_by_ts.get(&row.ts).with_context(|| {
                    format!(
                        "missing patched raw TWAP at {} {} {}",
                        exchange, product, row.ts
                    )
                })?;
                if row.twap != *value {
                    file_changed += 1;
                    row.twap = *value;
                }
            }
            if raw_by_ts.len() != rows.len() {
                bail!(
                    "HFQ/raw row count mismatch {exchange} {product} {}: {} != {}",
                    file.day,
                    rows.len(),
                    raw_by_ts.len()
                );
            }
            let relative = file.path.strip_prefix(&args.hfq_input_root)?;
            let output = args.hfq_output_root.join(relative);
            if output.exists() && !args.overwrite {
                bail!("refusing to overwrite {}", output.display());
            }
            if !args.dry_run {
                write_day_parquet(&output, &rows)?;
            }
            files += 1;
            changed += file_changed;
            eprintln!(
                "hfq_twap {} {} {} rows={} changed={file_changed}",
                exchange,
                product,
                file.day,
                rows.len()
            );
        }
    }
    Ok((files, changed))
}
