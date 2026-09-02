//! Replay LSEG trade-derived factors from HFQ CME minute baseline parquet.
//!
//! The HFQ baseline intentionally has no LL2 book.  This replay advances the
//! original LSEG trade state, but never manufactures depth; factors that read
//! a native book are written as IEEE NaN.

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use mkt_signal::factor_pub::lseg_features::{
    LsegFactorPlan, LsegTradeBar, LsegTradeOnlyFeatureState, LSEG_ALL_FACTORS,
    LSEG_TRADE_FIELD_COUNT, LSEG_TRADE_FIELD_NAMES,
};
use polars::prelude::{
    DataFrame, Float64Chunked, Int64Chunked, NamedFrom, ParquetCompression, ParquetReader,
    ParquetWriter, SerReader, Series, StringChunked,
};
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_IN: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min_hfq";
const DEFAULT_OUT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_factor_1min_hfq";
const DEFAULT_ROLL_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/cme_volume_roll_1d";

const PRODUCTS: [ProductSpec; 6] = [
    ProductSpec {
        exchange: "CME",
        product: "ES",
    },
    ProductSpec {
        exchange: "CME",
        product: "NQ",
    },
    ProductSpec {
        exchange: "CME",
        product: "RTY",
    },
    ProductSpec {
        exchange: "CBOT",
        product: "YM",
    },
    ProductSpec {
        exchange: "COMEX",
        product: "GC",
    },
    ProductSpec {
        exchange: "NYMEX",
        product: "CL",
    },
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProductSpec {
    exchange: &'static str,
    product: &'static str,
}

impl ProductSpec {
    fn continuous_id(self) -> String {
        format!("{}:{}", self.exchange, self.product)
    }
}

#[derive(Parser, Debug)]
#[command(name = "cme_features_1min_hfq")]
#[command(about = "Compute LSEG factors from trade-only HFQ CME minute parquet")]
struct Args {
    #[arg(long, default_value = DEFAULT_IN)]
    in_root: PathBuf,
    #[arg(long, default_value = DEFAULT_OUT)]
    out_root: PathBuf,
    #[arg(long, default_value = DEFAULT_ROLL_ROOT)]
    roll_root: PathBuf,
    /// Inclusive date in YYYY-MM-DD.
    #[arg(long, default_value = "2020-01-01")]
    start: String,
    /// Exclusive date in YYYY-MM-DD.
    #[arg(long, default_value = "2026-06-01")]
    end: String,
    /// Comma-separated subset of ES,NQ,RTY,YM,GC,CL.
    #[arg(long)]
    products: Option<String>,
    #[arg(long, default_value_t = 6)]
    workers: usize,
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    dry_run: bool,
}

struct DayFile {
    day: NaiveDate,
    path: PathBuf,
}

#[derive(Debug, Clone)]
struct DominantDay {
    contract_id: String,
    ric: String,
    price_adjustment: f64,
}

#[derive(Debug)]
struct OutputRow {
    ts: i64,
    ric: String,
    factors: Vec<f64>,
}

#[derive(Deserialize)]
struct DominantCsvRow {
    trading_day: String,
    contract_id: String,
    ric: String,
}

#[derive(Deserialize)]
struct AdjustmentCsvRow {
    effective_trading_day: String,
    cumulative_factor: f64,
    cumulative_complete: String,
    skipped: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    if args.in_root == args.out_root {
        bail!("--in-root and --out-root must differ");
    }
    if args.in_root.to_string_lossy().contains("cme_tas_rocksdb")
        || args.out_root.to_string_lossy().contains("cme_tas_rocksdb")
    {
        bail!("refusing to touch a CME RocksDB path");
    }
    let start = parse_day(&args.start)?;
    let end = parse_day(&args.end)?;
    if start >= end {
        bail!("--start must precede --end");
    }
    let products = select_products(args.products.as_deref())?;
    let plan = LsegFactorPlan::from_factor_names(vec![LSEG_ALL_FACTORS.to_string()])?;
    let factor_names: Vec<String> = plan.factor_names().map(ToOwned::to_owned).collect();
    if factor_names.len() != 632 {
        bail!(
            "unexpected LSEG factor registry width: {}",
            factor_names.len()
        );
    }

    eprintln!(
        "cme_features_1min_hfq products={} start={} end={} in={} out={}",
        products.len(),
        start,
        end,
        args.in_root.display(),
        args.out_root.display()
    );
    let files = AtomicU64::new(0);
    let rows = AtomicU64::new(0);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build()
        .context("build factor worker pool")?;
    pool.install(|| {
        products.par_iter().try_for_each(|spec| -> Result<()> {
            let (product_files, product_rows) =
                replay_product(&args, *spec, &plan, &factor_names, start, end)?;
            files.fetch_add(product_files, Ordering::Relaxed);
            rows.fetch_add(product_rows, Ordering::Relaxed);
            Ok(())
        })
    })?;
    eprintln!(
        "cme_features_1min_hfq complete files={} rows={}",
        files.load(Ordering::Relaxed),
        rows.load(Ordering::Relaxed)
    );
    Ok(())
}

fn select_products(raw: Option<&str>) -> Result<Vec<ProductSpec>> {
    let Some(raw) = raw else {
        return Ok(PRODUCTS.to_vec());
    };
    let requested: HashSet<String> = raw
        .split(',')
        .map(|item| item.trim().to_ascii_uppercase())
        .filter(|item| !item.is_empty())
        .collect();
    if requested.is_empty() {
        bail!("--products must name at least one product");
    }
    let selected: Vec<_> = PRODUCTS
        .iter()
        .copied()
        .filter(|spec| requested.contains(spec.product))
        .collect();
    if selected.len() != requested.len() {
        bail!("--products accepts only ES,NQ,RTY,YM,GC,CL");
    }
    Ok(selected)
}

fn replay_product(
    args: &Args,
    spec: ProductSpec,
    plan: &LsegFactorPlan,
    factor_names: &[String],
    start: NaiveDate,
    end: NaiveDate,
) -> Result<(u64, u64)> {
    let days = list_days(&args.in_root, spec, start, end)?;
    if days.is_empty() {
        bail!(
            "no HFQ baseline files for {}/{} in [{start}, {end})",
            spec.exchange,
            spec.product
        );
    }
    let dominant = load_dominant_days(&args.roll_root, spec)?;
    let continuous_id = spec.continuous_id();
    let mut state = LsegTradeOnlyFeatureState::default();
    let mut previous_ts = None;
    let mut pending_factors = None;
    let mut files = 0u64;
    let mut rows = 0u64;
    for day_file in days {
        let metadata = dominant.get(&day_file.day).with_context(|| {
            format!(
                "missing dominant metadata for {}/{} {}",
                spec.exchange, spec.product, day_file.day
            )
        })?;
        let output = process_day(
            &day_file.path,
            spec,
            metadata,
            &continuous_id,
            &mut state,
            &mut previous_ts,
            &mut pending_factors,
            plan,
        )?;
        if output.is_empty() {
            bail!("empty HFQ baseline file: {}", day_file.path.display());
        }
        if !args.dry_run {
            let destination = args
                .out_root
                .join(spec.exchange)
                .join(spec.product)
                .join(format!("{}.parquet", day_file.day.format("%Y%m%d")));
            if destination.exists() && !args.overwrite {
                bail!(
                    "output already exists (pass --overwrite only for a complete replacement): {}",
                    destination.display()
                );
            }
            write_day(
                &destination,
                &continuous_id,
                metadata,
                factor_names,
                &output,
            )?;
            files += 1;
            rows += output.len() as u64;
        }
    }
    eprintln!(
        "{}:{} files={} rows={}",
        spec.exchange, spec.product, files, rows
    );
    Ok((files, rows))
}

fn list_days(
    root: &Path,
    spec: ProductSpec,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Vec<DayFile>> {
    let directory = root.join(spec.exchange).join(spec.product);
    let mut days = Vec::new();
    for entry in fs::read_dir(&directory)
        .with_context(|| format!("read HFQ directory {}", directory.display()))?
    {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("parquet") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") else {
            continue;
        };
        if day >= start && day < end {
            days.push(DayFile { day, path });
        }
    }
    days.sort_by_key(|item| item.day);
    Ok(days)
}

fn load_dominant_days(
    roll_root: &Path,
    spec: ProductSpec,
) -> Result<BTreeMap<NaiveDate, DominantDay>> {
    let directory = roll_root.join(spec.exchange).join(spec.product);
    let adjustment_path = directory.join("adjustment.csv");
    let mut adjustments = Vec::new();
    let mut adjustment_reader = csv::Reader::from_path(&adjustment_path)
        .with_context(|| format!("read {}", adjustment_path.display()))?;
    for row in adjustment_reader.deserialize::<AdjustmentCsvRow>() {
        let row = row.with_context(|| format!("parse {}", adjustment_path.display()))?;
        if csv_bool(&row.skipped)? || !csv_bool(&row.cumulative_complete)? {
            continue;
        }
        let effective_day = parse_day(&row.effective_trading_day)?;
        if !row.cumulative_factor.is_finite() {
            bail!(
                "non-finite cumulative factor in {} for {effective_day}",
                adjustment_path.display()
            );
        }
        adjustments.push((effective_day, row.cumulative_factor));
    }
    adjustments.sort_by_key(|(day, _)| *day);

    let dominant_path = directory.join("dominant.csv");
    let mut dominant_reader = csv::Reader::from_path(&dominant_path)
        .with_context(|| format!("read {}", dominant_path.display()))?;
    let mut output = BTreeMap::new();
    for row in dominant_reader.deserialize::<DominantCsvRow>() {
        let row = row.with_context(|| format!("parse {}", dominant_path.display()))?;
        let day = parse_day(&row.trading_day)?;
        if row.contract_id.is_empty() || row.ric.is_empty() {
            bail!(
                "empty dominant contract metadata for {}/{} {day}",
                spec.exchange,
                spec.product
            );
        }
        let price_adjustment = adjustments
            .iter()
            .find(|(effective_day, _)| *effective_day > day)
            .map(|(_, factor)| *factor)
            .unwrap_or(0.0);
        if output
            .insert(
                day,
                DominantDay {
                    contract_id: row.contract_id,
                    ric: row.ric,
                    price_adjustment,
                },
            )
            .is_some()
        {
            bail!(
                "duplicate dominant metadata for {}/{} {day}",
                spec.exchange,
                spec.product
            );
        }
    }
    Ok(output)
}

fn process_day(
    path: &Path,
    spec: ProductSpec,
    metadata: &DominantDay,
    continuous_id: &str,
    state: &mut LsegTradeOnlyFeatureState,
    previous_ts: &mut Option<i64>,
    pending_factors: &mut Option<Vec<f64>>,
    plan: &LsegFactorPlan,
) -> Result<Vec<OutputRow>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let dataframe = ParquetReader::new(file)
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", path.display()))?;
    let contract_id = string_column(&dataframe, "contract_id")?;
    let ric = string_column(&dataframe, "ric")?;
    let ts = i64_column(&dataframe, "ts")?;
    let mut trade_columns = Vec::with_capacity(LSEG_TRADE_FIELD_COUNT);
    for field in LSEG_TRADE_FIELD_NAMES {
        trade_columns.push(f64_column(&dataframe, field)?);
    }

    let mut output = Vec::with_capacity(dataframe.height());
    for row_index in 0..dataframe.height() {
        let row_contract = contract_id
            .get(row_index)
            .with_context(|| format!("null contract_id row {row_index} {}", path.display()))?;
        if row_contract != spec.product {
            bail!(
                "unexpected contract_id {} for {}/{} row {} in {}",
                row_contract,
                spec.exchange,
                spec.product,
                row_index,
                path.display()
            );
        }
        let row_ric = ric
            .get(row_index)
            .filter(|value| !value.is_empty())
            .with_context(|| format!("empty ric row {row_index} {}", path.display()))?;
        if row_ric != metadata.ric {
            bail!(
                "HFQ ric mismatch for {}/{} row {} in {}: input={} dominant={}",
                spec.exchange,
                spec.product,
                row_index,
                path.display(),
                row_ric,
                metadata.ric
            );
        }
        let row_ts = ts
            .get(row_index)
            .with_context(|| format!("null ts row {row_index} {}", path.display()))?;
        if row_ts.rem_euclid(60) != 0 {
            bail!("non-minute ts {row_ts} in {}", path.display());
        }
        let segment_break = match *previous_ts {
            Some(previous) if row_ts <= previous => {
                bail!(
                    "non-increasing ts {previous} -> {row_ts} in {}",
                    path.display()
                )
            }
            Some(previous) => row_ts - previous != 60,
            None => false,
        };
        let mut trade = [f64::NAN; LSEG_TRADE_FIELD_COUNT];
        for (field_index, column) in trade_columns.iter().enumerate() {
            trade[field_index] = column.get(row_index).unwrap_or(f64::NAN);
        }
        let factors = take_shifted_factors(pending_factors, segment_break, plan.len());
        state
            .push(
                row_ts * 1_000,
                continuous_id,
                LsegTradeBar::from_slice(&trade)?,
                segment_break,
            )
            .with_context(|| format!("advance factor state {continuous_id} ts={row_ts}"))?;
        *pending_factors = Some(
            state
                .factor_values(plan)?
                .into_iter()
                .map(|value| value.unwrap_or(f64::NAN))
                .collect(),
        );
        *previous_ts = Some(row_ts);
        output.push(OutputRow {
            ts: row_ts,
            ric: row_ric.to_string(),
            factors,
        });
    }
    Ok(output)
}

fn take_shifted_factors(
    pending_factors: &mut Option<Vec<f64>>,
    segment_break: bool,
    factor_count: usize,
) -> Vec<f64> {
    if segment_break {
        *pending_factors = None;
    }
    pending_factors
        .take()
        .unwrap_or_else(|| vec![f64::NAN; factor_count])
}

fn write_day(
    path: &Path,
    continuous_id: &str,
    metadata: &DominantDay,
    factor_names: &[String],
    rows: &[OutputRow],
) -> Result<()> {
    if rows
        .iter()
        .any(|row| row.factors.len() != factor_names.len())
    {
        bail!("factor row width does not match the registered factor schema");
    }
    let n = rows.len();
    let mut source_ric = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);
    let mut factor_columns: Vec<Vec<f64>> =
        factor_names.iter().map(|_| Vec::with_capacity(n)).collect();
    for row in rows {
        source_ric.push(row.ric.clone());
        ts.push(row.ts);
        for (index, value) in row.factors.iter().copied().enumerate() {
            factor_columns[index].push(value);
        }
    }
    let mut columns = vec![
        Series::new("continuous_id".into(), vec![continuous_id.to_string(); n]),
        Series::new(
            "source_contract_id".into(),
            vec![metadata.contract_id.clone(); n],
        ),
        Series::new("source_ric".into(), source_ric),
        Series::new("ts".into(), ts),
        Series::new(
            "price_adjustment".into(),
            vec![metadata.price_adjustment; n],
        ),
        Series::new(
            "source_depth_ts_utc_ns".into(),
            vec![Option::<i64>::None; n],
        ),
    ];
    for (name, values) in factor_names.iter().zip(factor_columns) {
        columns.push(Series::new(name.as_str().into(), values));
    }
    let mut dataframe = DataFrame::new(columns).context("build HFQ factor dataframe")?;
    let parent = path
        .parent()
        .context("factor parquet path has no parent directory")?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let temporary = path.with_extension("parquet.tmp");
    let file =
        File::create(&temporary).with_context(|| format!("create {}", temporary.display()))?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut dataframe)
        .with_context(|| format!("write {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .with_context(|| format!("rename {} -> {}", temporary.display(), path.display()))?;
    Ok(())
}

fn parse_day(text: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").with_context(|| format!("invalid date {text}"))
}

fn csv_bool(text: &str) -> Result<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail!("invalid CSV boolean {text:?}"),
    }
}

fn string_column<'a>(dataframe: &'a DataFrame, name: &str) -> Result<&'a StringChunked> {
    dataframe
        .column(name)
        .with_context(|| format!("missing {name}"))?
        .str()
        .with_context(|| format!("{name} must be Utf8"))
}

fn i64_column<'a>(dataframe: &'a DataFrame, name: &str) -> Result<&'a Int64Chunked> {
    dataframe
        .column(name)
        .with_context(|| format!("missing {name}"))?
        .i64()
        .with_context(|| format!("{name} must be Int64"))
}

fn f64_column<'a>(dataframe: &'a DataFrame, name: &str) -> Result<&'a Float64Chunked> {
    dataframe
        .column(name)
        .with_context(|| format!("missing {name}"))?
        .f64()
        .with_context(|| format!("{name} must be Float64"))
}

#[cfg(test)]
mod tests {
    use super::take_shifted_factors;

    #[test]
    fn shift_one_emits_the_previous_factor_vector_and_resets_at_segment_break() {
        let mut pending = None;
        assert!(take_shifted_factors(&mut pending, false, 2)
            .into_iter()
            .all(f64::is_nan));

        pending = Some(vec![1.0, 2.0]);
        assert_eq!(take_shifted_factors(&mut pending, false, 2), vec![1.0, 2.0]);

        pending = Some(vec![3.0, 4.0]);
        assert!(take_shifted_factors(&mut pending, true, 2)
            .into_iter()
            .all(f64::is_nan));
    }
}
