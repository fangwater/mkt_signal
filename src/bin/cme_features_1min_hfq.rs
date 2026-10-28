//! Replay LSEG factors from HFQ bars, optionally with audited native LL2.

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use mkt_signal::factor_pub::lseg_features::{
    LsegDepth10, LsegFactorPlan, LsegTradeBar, LsegTradeOnlyFeatureState, LSEG_ALL_FACTORS,
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
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_IN: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min_hfq";
const DEFAULT_OUT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_factor_1min_hfq";
const DEFAULT_ROLL_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/cme_volume_roll_1d";

const PRODUCTS: [ProductSpec; 16] = [
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
    ProductSpec {
        exchange: "CME",
        product: "URO",
    },
    ProductSpec {
        exchange: "CME",
        product: "JY",
    },
    ProductSpec {
        exchange: "COMEX",
        product: "HG",
    },
    ProductSpec {
        exchange: "COMEX",
        product: "SI",
    },
    ProductSpec {
        exchange: "CBOT",
        product: "TU",
    },
    ProductSpec {
        exchange: "CBOT",
        product: "FV",
    },
    ProductSpec {
        exchange: "CBOT",
        product: "TY",
    },
    ProductSpec {
        exchange: "CBOT",
        product: "TN",
    },
    ProductSpec {
        exchange: "CBOT",
        product: "US",
    },
    ProductSpec {
        exchange: "CBOT",
        product: "AUL",
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
#[command(about = "Compute LSEG factors from HFQ CME minute parquet")]
struct Args {
    /// Validate and use each day's causal offset and HFQ segment.
    #[arg(long)]
    causal_roll: bool,
    /// Input bar width; rolling windows count bars, while timestamps remain UTC.
    #[arg(long, default_value_t = 1)]
    bar_minutes: usize,
    /// Coarse-bar JSON.zst metadata root, required for 5/15/60/1440-minute bars.
    #[arg(long)]
    bar_metadata_root: Option<PathBuf>,
    /// Exact-RIC same-minute LL2 provenance CSV.zst root; omit for trade-only replay.
    #[arg(long)]
    depth_audit_root: Option<PathBuf>,
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
    /// Comma-separated products; defaults to the original six products.
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
    causal: Option<cme_roll_metadata::Day>,
}

#[derive(Debug)]
struct OutputRow {
    ts: i64,
    ric: String,
    source_depth_ts_utc_ns: Option<i64>,
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
    if ![1, 5, 15, 60, 1440].contains(&args.bar_minutes)
        || (args.bar_minutes != 1) != args.bar_metadata_root.is_some()
        || (args.bar_metadata_root.is_some() && args.depth_audit_root.is_some())
    {
        bail!("coarse bars require --bar-minutes 5/15/60/1440 and --bar-metadata-root, without --depth-audit-root");
    }
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    if args.in_root == args.out_root {
        bail!("--in-root and --out-root must differ");
    }
    if args.causal_roll && args.out_root == Path::new(DEFAULT_OUT) {
        bail!("causal factors require a separate --out-root");
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
    for spec in &products {
        if cme_roll_metadata::exchange(spec.product).is_some() != args.causal_roll {
            bail!("new products require --causal-roll; original six use the legacy mode");
        }
    }
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
        return Ok(PRODUCTS[..6].to_vec());
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
        bail!("unsupported --products value");
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
    let (dominant, session_order) = if args.causal_roll {
        let table = cme_roll_metadata::read_daily(
            &args
                .roll_root
                .join(spec.exchange)
                .join(spec.product)
                .join("dominant.csv"),
            spec.product,
        )?;
        let expected: std::collections::BTreeSet<_> = table
            .range(start..end)
            .filter(|(_, d)| d.selected())
            .map(|(day, _)| *day)
            .collect();
        let actual: std::collections::BTreeSet<_> = days.iter().map(|d| d.day).collect();
        if expected != actual {
            bail!("causal baseline coverage differs from daily table for {}: missing={:?}, unexpected={:?}",
            spec.product, expected.difference(&actual).collect::<Vec<_>>(), actual.difference(&expected).collect::<Vec<_>>());
        }
        // Keep unselected days in the sequence: no position is still an expected
        // research session, and missing one must not silently carry a window.
        let sessions: BTreeMap<_, _> = table.keys().enumerate().map(|(i, d)| (*d, i)).collect();
        let dominant: BTreeMap<_, _> = table
            .into_iter()
            .filter(|(_, d)| d.selected())
            .map(|(day, d)| {
                (
                    day,
                    DominantDay {
                        contract_id: d.contract_id.clone(),
                        ric: d.ric.clone(),
                        price_adjustment: d.offset(),
                        causal: Some(d),
                    },
                )
            })
            .collect();
        (dominant, sessions)
    } else {
        let dominant = load_dominant_days(&args.roll_root, spec)?;
        let sessions: BTreeMap<_, _> = dominant.keys().enumerate().map(|(i, d)| (*d, i)).collect();
        (dominant, sessions)
    };
    let continuous_id = spec.continuous_id();
    let mut state = LsegTradeOnlyFeatureState::default();
    let mut previous_ts = None;
    let mut pending_factors = None;
    let mut previous_segment = None;
    let mut previous_session = None;
    let mut files = 0u64;
    let mut rows = 0u64;
    for day_file in days {
        let metadata = dominant.get(&day_file.day).with_context(|| {
            format!(
                "missing dominant metadata for {}/{} {}",
                spec.exchange, spec.product, day_file.day
            )
        })?;
        let current_segment = metadata.causal.as_ref().and_then(|m| m.segment_id);
        let hfq_changed = previous_segment.is_some() && previous_segment != current_segment;
        let session_index = *session_order
            .get(&day_file.day)
            .context("missing expected session")?;
        let missing_session =
            previous_session.is_none_or(|index| session_index != index + 1) || hfq_changed;
        previous_session = Some(session_index);
        reset_for_hfq_segment(
            &mut previous_segment,
            current_segment,
            &mut state,
            &mut pending_factors,
        );
        let output = process_day(
            &day_file.path,
            spec,
            metadata,
            &continuous_id,
            &mut state,
            &mut previous_ts,
            &mut pending_factors,
            plan,
            args.depth_audit_root
                .as_ref()
                .map(|root| {
                    root.join(spec.exchange)
                        .join(spec.product)
                        .join(format!("{}.csv.zst", day_file.day.format("%Y%m%d")))
                })
                .as_deref(),
            args.bar_metadata_root
                .as_ref()
                .map(|root| {
                    root.join(spec.exchange)
                        .join(spec.product)
                        .join(format!("{}.json.zst", day_file.day.format("%Y%m%d")))
                })
                .as_deref(),
            args.bar_minutes,
            missing_session,
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
                    causal: None,
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
    depth_audit_path: Option<&Path>,
    bar_metadata_path: Option<&Path>,
    bar_minutes: usize,
    missing_session: bool,
) -> Result<Vec<OutputRow>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let dataframe = ParquetReader::new(file)
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", path.display()))?;
    let contract_id = string_column(&dataframe, "contract_id")?;
    let ric = string_column(&dataframe, "ric")?;
    let ts = i64_column(&dataframe, "ts")?;
    let causal_columns = metadata
        .causal
        .as_ref()
        .map(|_| -> Result<_> {
            Ok((
                i64_column(&dataframe, "segment_id")?,
                i64_column(&dataframe, "offset_e9")?,
            ))
        })
        .transpose()?;
    let mut trade_columns = Vec::with_capacity(LSEG_TRADE_FIELD_COUNT);
    for field in LSEG_TRADE_FIELD_NAMES {
        trade_columns.push(f64_column(&dataframe, field)?);
    }
    let depth_sources = depth_audit_path.map(read_depth_sources).transpose()?;
    let bar_metadata = bar_metadata_path
        .map(|p| read_bar_metadata(p, &dataframe, bar_minutes))
        .transpose()?;
    let depth_enabled = depth_sources.is_some() || bar_metadata.is_some();
    let mut depth_columns = Vec::new();
    if depth_enabled {
        for side in ["bid", "ask"] {
            for field in ["p", "v"] {
                for level in 0..10 {
                    depth_columns.push(f64_column(&dataframe, &format!("{side}{level}{field}"))?);
                }
            }
        }
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
        if let (Some(meta), Some((segments, offsets))) = (&metadata.causal, causal_columns) {
            if segments.get(row_index) != meta.segment_id
                || offsets.get(row_index) != meta.offset_e9
                || row_ts
                    .checked_mul(1_000_000_000)
                    .is_none_or(|ns| ns < meta.effective_ns)
            {
                bail!(
                    "causal HFQ provenance mismatch row {row_index} {}",
                    path.display()
                );
            }
        }
        if row_ts.rem_euclid(60) != 0 {
            bail!("non-minute ts {row_ts} in {}", path.display());
        }
        let minute_break = match *previous_ts {
            Some(previous) if row_ts <= previous => {
                bail!(
                    "non-increasing ts {previous} -> {row_ts} in {}",
                    path.display()
                )
            }
            Some(previous) => row_ts - previous != 60,
            None => false,
        };
        let segment_break = bar_metadata.as_ref().map_or(minute_break, |m| {
            coarse_segment_break(
                bar_minutes,
                row_index,
                m.segment_start[row_index],
                missing_session,
            )
        });
        let mut trade = [f64::NAN; LSEG_TRADE_FIELD_COUNT];
        for (field_index, column) in trade_columns.iter().enumerate() {
            trade[field_index] = column.get(row_index).unwrap_or(f64::NAN);
        }
        let factors = take_shifted_factors(pending_factors, segment_break, plan.len());
        let source_depth_ts_utc_ns = if let Some(m) = &bar_metadata {
            m.source_depth_ts_utc_ns[row_index]
        } else {
            depth_sources
                .as_ref()
                .and_then(|sources| sources.get(&(row_ric.to_owned(), row_ts)))
                .copied()
        };
        let depth = if depth_enabled {
            let values: Vec<_> = depth_columns
                .iter()
                .map(|c| c.get(row_index).unwrap_or(f64::NAN))
                .collect();
            let present = values.iter().any(|v| !v.is_nan());
            if present != source_depth_ts_utc_ns.is_some() {
                bail!(
                    "LL2 values/provenance mismatch {} ts={row_ts}",
                    path.display()
                );
            }
            if present {
                Some(LsegDepth10 {
                    bid_prices: values[0..10].try_into().unwrap(),
                    bid_amounts: values[10..20].try_into().unwrap(),
                    ask_prices: values[20..30].try_into().unwrap(),
                    ask_amounts: values[30..40].try_into().unwrap(),
                })
            } else {
                None
            }
        } else {
            None
        };
        state
            .push_with_hfq_depth(
                row_ts * 1_000,
                continuous_id,
                LsegTradeBar::from_slice(&trade)?,
                depth,
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
            source_depth_ts_utc_ns,
            factors,
        });
    }
    Ok(output)
}

#[derive(Deserialize)]
struct BarMetadata {
    frequency: String,
    bar_minutes: usize,
    trading_day: i64,
    ts: Vec<i64>,
    end_ts: Vec<i64>,
    last_minute_ts: Vec<i64>,
    source_minutes: Vec<usize>,
    segment_start: Vec<bool>,
    source_depth_ts_utc_ns: Vec<Option<i64>>,
    contract_id: Vec<String>,
    ric: Vec<String>,
}

fn read_bar_metadata(path: &Path, df: &DataFrame, minutes: usize) -> Result<BarMetadata> {
    let decoder = zstd::stream::read::Decoder::new(File::open(path)?)?;
    let m: BarMetadata = serde_json::from_reader(decoder)?;
    let n = df.height();
    let lengths = [
        m.ts.len(),
        m.end_ts.len(),
        m.last_minute_ts.len(),
        m.source_minutes.len(),
        m.segment_start.len(),
        m.source_depth_ts_utc_ns.len(),
        m.contract_id.len(),
        m.ric.len(),
    ];
    if n == 0
        || lengths.iter().any(|len| *len != n)
        || m.bar_minutes != minutes
        || m.frequency
            != match minutes {
                60 => "1h".to_string(),
                1440 => "1day".to_string(),
                _ => format!("{minutes}min"),
            }
        || (minutes == 1440 && n != 1)
        || !m.segment_start[0]
        || path.file_name().and_then(|p| p.to_str())
            != Some(format!("{}.json.zst", m.trading_day).as_str())
    {
        bail!(
            "invalid bar metadata shape/frequency/day {}",
            path.display()
        );
    }
    let ts = i64_column(df, "ts")?;
    let cid = string_column(df, "contract_id")?;
    let ric = string_column(df, "ric")?;
    for i in 0..n {
        if ts.get(i) != Some(m.ts[i])
            || cid.get(i) != Some(m.contract_id[i].as_str())
            || ric.get(i) != Some(m.ric[i].as_str())
            || m.ts[i] % 60 != 0
            || m.source_minutes[i] == 0
            || m.end_ts[i] <= m.ts[i]
            || m.end_ts[i] % 60 != 0
            || (if minutes == 1440 {
                m.source_minutes[i] as i64 * 60 > m.end_ts[i] - m.ts[i]
            } else {
                m.source_minutes[i] > minutes
                    || m.end_ts[i] != m.ts[i] + m.source_minutes[i] as i64 * 60
            })
            || m.last_minute_ts[i] + 60 != m.end_ts[i]
            || (i > 0
                && (m.ts[i] < m.end_ts[i - 1]
                    || (!m.segment_start[i]
                        && (m.ts[i] != m.end_ts[i - 1] || m.source_minutes[i - 1] != minutes))))
        {
            bail!(
                "bar metadata/key/segment mismatch {} row {i}",
                path.display()
            );
        }
        if let Some(source) = m.source_depth_ts_utc_ns[i] {
            validate_depth_source_time(m.last_minute_ts[i], source)?;
        }
    }
    Ok(m)
}

fn coarse_segment_break(
    minutes: usize,
    row: usize,
    source_segment_start: bool,
    missing_session: bool,
) -> bool {
    if matches!(minutes, 60 | 1440) && row == 0 {
        missing_session
    } else {
        source_segment_start
    }
}

#[derive(Deserialize)]
struct DepthSource {
    ric: String,
    ts: i64,
    source_ts_utc_ns: i64,
}

fn read_depth_sources(path: &Path) -> Result<BTreeMap<(String, i64), i64>> {
    let decoder = zstd::stream::read::Decoder::new(
        File::open(path).with_context(|| format!("open LL2 audit {}", path.display()))?,
    )?;
    let mut reader = csv::Reader::from_reader(decoder);
    let mut output = BTreeMap::new();
    for row in reader.deserialize::<DepthSource>() {
        let row = row?;
        validate_depth_source_time(row.ts, row.source_ts_utc_ns)?;
        if output
            .insert((row.ric, row.ts), row.source_ts_utc_ns)
            .is_some()
        {
            bail!("duplicate LL2 provenance in {}", path.display());
        }
    }
    Ok(output)
}

fn validate_depth_source_time(ts: i64, source_ns: i64) -> Result<()> {
    let start = ts
        .checked_mul(1_000_000_000)
        .context("LL2 minute timestamp overflow")?;
    let end = start
        .checked_add(60_000_000_000)
        .context("LL2 minute end overflow")?;
    if ts.rem_euclid(60) != 0 || !(start..end).contains(&source_ns) {
        bail!("LL2 source time must lie in the same UTC minute: ts={ts} source={source_ns}");
    }
    Ok(())
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

fn reset_for_hfq_segment(
    previous: &mut Option<i64>,
    current: Option<i64>,
    state: &mut LsegTradeOnlyFeatureState,
    pending: &mut Option<Vec<f64>>,
) {
    if let Some(current) = current {
        if previous.is_some_and(|old| old != current) {
            *state = LsegTradeOnlyFeatureState::default();
            *pending = None;
        }
        *previous = Some(current);
    }
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
            rows.iter()
                .map(|row| row.source_depth_ts_utc_ns)
                .collect::<Vec<_>>(),
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
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    ParquetWriter::new(&mut writer)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut dataframe)
        .with_context(|| format!("write {}", temporary.display()))?;
    writer
        .flush()
        .with_context(|| format!("flush {}", temporary.display()))?;
    drop(writer);
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
    use super::*;

    #[test]
    fn coarse_metadata_validates_segments_tails_and_last_minute_source() {
        let dir =
            std::env::temp_dir().join(format!("cme_coarse_metadata_test_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("20250102.json.zst");
        let df = DataFrame::new(vec![
            Series::new("ts".into(), [0i64, 300, 720]),
            Series::new("contract_id".into(), ["HG"; 3]),
            Series::new("ric".into(), ["HGH25"; 3]),
        ])
        .unwrap();
        let mut value = serde_json::json!({"frequency":"5min","bar_minutes":5,"trading_day":20250102,
            "ts":[0,300,720],"end_ts":[300,420,780],"last_minute_ts":[240,360,720],
            "source_minutes":[5,2,1],"segment_start":[true,false,true],
            "source_depth_ts_utc_ns":[299000000000i64,null,null],
            "contract_id":["HG","HG","HG"],"ric":["HGH25","HGH25","HGH25"]});
        let write = |v: &serde_json::Value| {
            fs::write(
                &path,
                zstd::stream::encode_all(serde_json::to_vec(v).unwrap().as_slice(), 1).unwrap(),
            )
            .unwrap()
        };
        write(&value);
        assert!(read_bar_metadata(&path, &df, 5).is_ok());
        value["segment_start"][2] = false.into();
        write(&value);
        assert!(read_bar_metadata(&path, &df, 5).is_err());
        value["segment_start"][2] = true.into();
        value["source_depth_ts_utc_ns"][0] = 239000000000i64.into();
        write(&value);
        assert!(read_bar_metadata(&path, &df, 5).is_err());
        value["source_depth_ts_utc_ns"][0] = serde_json::Value::Null;
        value["ric"][1] = "HGK25".into();
        write(&value);
        assert!(read_bar_metadata(&path, &df, 5).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn long_bars_carry_adjacent_sessions_and_keep_intraday_breaks() {
        for minutes in [60, 1440] {
            assert!(!coarse_segment_break(minutes, 0, true, false));
            assert!(coarse_segment_break(minutes, 0, true, true));
            assert!(coarse_segment_break(minutes, 1, true, false));
            assert!(!coarse_segment_break(minutes, 1, false, false));
        }
        for minutes in [5, 15] {
            assert!(coarse_segment_break(minutes, 0, true, false));
        }
    }

    #[test]
    fn daily_metadata_allows_2760_open_minutes_with_planned_breaks() {
        let dir = std::env::temp_dir().join(format!("cme_long_daily_test_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("20220118.json.zst");
        let df = DataFrame::new(vec![
            Series::new("ts".into(), [0i64]),
            Series::new("contract_id".into(), ["URO"]),
            Series::new("ric".into(), ["UROH22"]),
        ])
        .unwrap();
        let mut value = serde_json::json!({"frequency":"1day","bar_minutes":1440,"trading_day":20220118,"ts":[0],"end_ts":[169200],"last_minute_ts":[169140],"source_minutes":[2760],"segment_start":[true],"source_depth_ts_utc_ns":[169200000000000i64-1],"contract_id":["URO"],"ric":["UROH22"]});
        let write = |v: &serde_json::Value| {
            fs::write(
                &path,
                zstd::stream::encode_all(serde_json::to_vec(v).unwrap().as_slice(), 1).unwrap(),
            )
            .unwrap()
        };
        write(&value);
        assert!(read_bar_metadata(&path, &df, 1440).is_ok());
        value["source_minutes"][0] = 2821.into();
        write(&value);
        assert!(read_bar_metadata(&path, &df, 1440).is_err());
        value["source_minutes"][0] = 2760.into();
        value["frequency"] = "1440min".into();
        write(&value);
        assert!(read_bar_metadata(&path, &df, 1440).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn depth_provenance_rejects_prior_and_future_minutes() {
        assert!(validate_depth_source_time(120, 120_000_000_000).is_ok());
        assert!(validate_depth_source_time(120, 179_999_999_999).is_ok());
        assert!(validate_depth_source_time(120, 119_999_999_999).is_err());
        assert!(validate_depth_source_time(120, 180_000_000_000).is_err());
    }

    #[test]
    fn hfq_segment_change_clears_all_windows_even_at_consecutive_minutes() {
        let plan = LsegFactorPlan::from_factor_names(vec![LSEG_ALL_FACTORS.to_string()]).unwrap();
        let mut state = LsegTradeOnlyFeatureState::default();
        let mut values = [1.0; LSEG_TRADE_FIELD_COUNT];
        let mut pending = None;
        let mut segment = None;
        reset_for_hfq_segment(&mut segment, Some(1), &mut state, &mut pending);
        for n in 0..64 {
            values[0] = 100.0 + n as f64;
            state
                .push(
                    60_000 * n,
                    "CBOT:TY",
                    LsegTradeBar::from_slice(&values).unwrap(),
                    false,
                )
                .unwrap();
        }
        pending = Some(vec![42.0; plan.len()]);
        reset_for_hfq_segment(&mut segment, Some(1), &mut state, &mut pending);
        assert!(pending.is_some());
        reset_for_hfq_segment(&mut segment, Some(2), &mut state, &mut pending);
        assert!(pending.is_none());
        assert!(state.factor_values(&plan).is_err());
        state
            .push(
                64 * 60_000,
                "CBOT:TY",
                LsegTradeBar::from_slice(&values).unwrap(),
                false,
            )
            .unwrap();
        let mut fresh = LsegTradeOnlyFeatureState::default();
        fresh
            .push(
                64 * 60_000,
                "CBOT:TY",
                LsegTradeBar::from_slice(&values).unwrap(),
                false,
            )
            .unwrap();
        assert_eq!(
            state.factor_values(&plan).unwrap(),
            fresh.factor_values(&plan).unwrap()
        );
    }

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
