//! Materialize continuous-session-only copies of the existing CN parquet archives.

mod sessions;

use anyhow::{bail, Context, Result};
use arrow::array::{Array, BooleanArray, Int64Array};
use arrow::compute::filter_record_batch;
use chrono::NaiveDate;
use clap::{Parser, ValueEnum};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::Serialize;
use serde_json::json;
use sessions::{reference_venue, ContinuousSessions, DEFAULT_REFERENCE_API};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DEFAULT_BACKTEST_IN_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/backtest_1s";
const DEFAULT_BASELINE_IN_ROOT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min";
const DEFAULT_BACKTEST_OUT_ROOT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/backtest_1s_continuous";
const DEFAULT_BASELINE_OUT_ROOT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min_continuous";

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Dataset {
    Backtest1s,
    Baseline1min,
    Both,
}

#[derive(Parser, Debug)]
#[command(about = "Copy CN futures parquet while retaining only reference continuous sessions")]
struct Args {
    #[arg(long, value_enum, default_value_t = Dataset::Both)]
    dataset: Dataset,
    #[arg(long, default_value = DEFAULT_BACKTEST_IN_ROOT)]
    backtest_in_root: PathBuf,
    #[arg(long, default_value = DEFAULT_BASELINE_IN_ROOT)]
    baseline_in_root: PathBuf,
    #[arg(long, default_value = DEFAULT_BACKTEST_OUT_ROOT)]
    backtest_out_root: PathBuf,
    #[arg(long, default_value = DEFAULT_BASELINE_OUT_ROOT)]
    baseline_out_root: PathBuf,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: String,
    #[arg(long, default_value_t = 8)]
    workers: usize,
    #[arg(long)]
    product: Option<String>,
    #[arg(long, default_value = DEFAULT_REFERENCE_API)]
    session_api: String,
    #[arg(long)]
    session_as_of: Option<String>,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    overwrite: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DatasetKind {
    Backtest1s,
    Baseline1min,
}

impl DatasetKind {
    fn label(self) -> &'static str {
        match self {
            Self::Backtest1s => "backtest_1s",
            Self::Baseline1min => "baseline_data_1min",
        }
    }
}

#[derive(Clone, Debug)]
struct Job {
    dataset: DatasetKind,
    source: PathBuf,
    destination: PathBuf,
    venue: String,
    product: String,
}

#[derive(Default)]
struct FileStats {
    input_rows: u64,
    output_rows: u64,
}

#[derive(Default)]
struct RunStats {
    files: usize,
    skipped_existing: usize,
    input_rows: u64,
    output_rows: u64,
}

fn parse_day(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("date must be YYYY-MM-DD: {value:?}"))
}

fn selected_products(value: Option<&str>) -> Option<Vec<String>> {
    value.map(|text| {
        text.split(',')
            .map(|part| part.trim().to_ascii_uppercase())
            .filter(|part| !part.is_empty())
            .collect()
    })
}

fn date_from_file(path: &Path) -> Result<NaiveDate> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow::anyhow!("non-UTF8 parquet filename {}", path.display()))?;
    let stem = file_name
        .strip_suffix(".parquet")
        .ok_or_else(|| anyhow::anyhow!("expected parquet filename {}", path.display()))?;
    NaiveDate::parse_from_str(stem, "%Y%m%d")
        .with_context(|| format!("parse TradDay from parquet filename {}", path.display()))
}

fn collect_jobs(
    dataset: DatasetKind,
    input_root: &Path,
    output_root: &Path,
    start: NaiveDate,
    end: NaiveDate,
    products: Option<&[String]>,
    sessions: &ContinuousSessions,
) -> Result<Vec<Job>> {
    if !input_root.is_dir() {
        bail!(
            "{} input root {} is not a directory",
            dataset.label(),
            input_root.display()
        );
    }
    let mut jobs = Vec::new();
    for exchange_entry in
        fs::read_dir(input_root).with_context(|| format!("read {}", input_root.display()))?
    {
        let exchange_entry = exchange_entry?;
        if !exchange_entry.file_type()?.is_dir() {
            continue;
        }
        let exchange = exchange_entry.file_name().to_string_lossy().to_string();
        if exchange.starts_with('_') {
            continue;
        }
        let venue = reference_venue(&exchange)?;
        for product_entry in fs::read_dir(exchange_entry.path()).with_context(|| {
            format!(
                "read product dirs below {}",
                exchange_entry.path().display()
            )
        })? {
            let product_entry = product_entry?;
            if !product_entry.file_type()?.is_dir() {
                continue;
            }
            let product = product_entry
                .file_name()
                .to_string_lossy()
                .to_ascii_uppercase();
            if products.is_some_and(|wanted| !wanted.iter().any(|item| item == &product)) {
                continue;
            }
            sessions.require_product(venue, &product)?;
            for file_entry in fs::read_dir(product_entry.path()).with_context(|| {
                format!(
                    "read parquet files below {}",
                    product_entry.path().display()
                )
            })? {
                let file_entry = file_entry?;
                if !file_entry.file_type()?.is_file()
                    || file_entry
                        .path()
                        .extension()
                        .and_then(|value| value.to_str())
                        != Some("parquet")
                {
                    continue;
                }
                let day = date_from_file(&file_entry.path())?;
                if day < start || day > end {
                    continue;
                }
                jobs.push(Job {
                    dataset,
                    source: file_entry.path(),
                    destination: output_root
                        .join(&exchange)
                        .join(&product)
                        .join(file_entry.file_name()),
                    venue: venue.to_string(),
                    product: product.clone(),
                });
            }
        }
    }
    jobs.sort_by(|left, right| left.source.cmp(&right.source));
    Ok(jobs)
}

fn write_json_atomically(path: &Path, value: &serde_json::Value) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("provenance path {} has no parent", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("write {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .with_context(|| format!("rename {} -> {}", temporary.display(), path.display()))?;
    Ok(())
}

fn initialize_output_root(
    root: &Path,
    input_root: &Path,
    provenance: &serde_json::Value,
) -> Result<()> {
    if root == input_root {
        bail!(
            "output root must differ from input root: {}",
            root.display()
        );
    }
    let manifest = root.join("_provenance").join("continuous_sessions.json");
    if root.exists() {
        if manifest.exists() {
            let existing: serde_json::Value = serde_json::from_slice(
                &fs::read(&manifest).with_context(|| format!("read {}", manifest.display()))?,
            )
            .with_context(|| format!("parse {}", manifest.display()))?;
            if existing.get("session_identity") != provenance.get("session_identity") {
                bail!(
                    "output root {} has a different continuous-session snapshot; choose a new output root",
                    root.display()
                );
            }
            return Ok(());
        }
        if fs::read_dir(root)?.next().is_some() {
            bail!(
                "output root {} is non-empty but has no session provenance; choose a new output root",
                root.display()
            );
        }
    }
    fs::create_dir_all(root).with_context(|| format!("create {}", root.display()))?;
    write_json_atomically(&manifest, provenance)
}

fn filter_file(
    job: &Job,
    sessions: &ContinuousSessions,
    overwrite: bool,
) -> Result<Option<FileStats>> {
    if job.destination.exists() && !overwrite {
        return Ok(None);
    }
    let matcher = sessions.matcher(&job.venue, &job.product)?;
    let source =
        File::open(&job.source).with_context(|| format!("open {}", job.source.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(source)
        .with_context(|| format!("open parquet {}", job.source.display()))?;
    let schema = builder.schema().clone();
    let ts_index = schema
        .index_of("ts")
        .with_context(|| format!("{} has no ts field", job.source.display()))?;
    let reader = builder
        .with_batch_size(65_536)
        .build()
        .with_context(|| format!("read parquet {}", job.source.display()))?;

    let parent = job.destination.parent().expect("destination has a parent");
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let temporary = job.destination.with_extension("parquet.tmp");
    let output =
        File::create(&temporary).with_context(|| format!("create {}", temporary.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(output, Arc::clone(&schema), Some(props))
        .with_context(|| format!("create parquet writer {}", temporary.display()))?;
    let mut stats = FileStats::default();
    for batch in reader {
        let batch = batch.with_context(|| format!("read batch {}", job.source.display()))?;
        let ts = batch
            .column(ts_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("{} field ts is not int64", job.source.display()))?;
        let mut mask = Vec::with_capacity(ts.len());
        for index in 0..ts.len() {
            if ts.is_null(index) {
                bail!(
                    "{} has null ts at row offset {}",
                    job.source.display(),
                    index
                );
            }
            mask.push(matcher.contains_ts(ts.value(index)));
        }
        stats.input_rows += batch.num_rows() as u64;
        let filtered = filter_record_batch(&batch, &BooleanArray::from(mask))
            .with_context(|| format!("filter batch {}", job.source.display()))?;
        stats.output_rows += filtered.num_rows() as u64;
        writer
            .write(&filtered)
            .with_context(|| format!("write {}", temporary.display()))?;
    }
    writer
        .close()
        .with_context(|| format!("close {}", temporary.display()))?;
    fs::rename(&temporary, &job.destination).with_context(|| {
        format!(
            "rename {} -> {}",
            temporary.display(),
            job.destination.display()
        )
    })?;
    Ok(Some(stats))
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.workers == 0 {
        bail!("workers must be at least 1");
    }
    let start = parse_day(&args.start)?;
    let end = parse_day(&args.end)?;
    if end < start {
        bail!("end {end} is before start {start}");
    }
    let sessions = ContinuousSessions::fetch(&args.session_api, args.session_as_of.as_deref())?;
    let products = selected_products(args.product.as_deref());
    let mut jobs = Vec::new();
    if matches!(args.dataset, Dataset::Backtest1s | Dataset::Both) {
        jobs.extend(collect_jobs(
            DatasetKind::Backtest1s,
            &args.backtest_in_root,
            &args.backtest_out_root,
            start,
            end,
            products.as_deref(),
            &sessions,
        )?);
    }
    if matches!(args.dataset, Dataset::Baseline1min | Dataset::Both) {
        jobs.extend(collect_jobs(
            DatasetKind::Baseline1min,
            &args.baseline_in_root,
            &args.baseline_out_root,
            start,
            end,
            products.as_deref(),
            &sessions,
        )?);
    }
    if jobs.is_empty() {
        bail!("no parquet files matched the selected dataset, product, and TradDay range");
    }

    let provenance = json!({
        "tool": "trim_cn_futures_continuous_sessions",
        "session_api": args.session_api,
        "requested_session_as_of": args.session_as_of,
        "session_identity": sessions.identity(),
        "session_response": sessions.provenance(),
    });
    eprintln!(
        "continuous-session trim plan as_of={} sessions={} files={} workers={} dry_run={}",
        sessions.state().as_of,
        sessions.state().session_count,
        jobs.len(),
        args.workers,
        args.dry_run,
    );
    if args.dry_run {
        for kind in [DatasetKind::Backtest1s, DatasetKind::Baseline1min] {
            let count = jobs.iter().filter(|job| job.dataset == kind).count();
            if count > 0 {
                eprintln!(
                    "continuous-session trim plan dataset={} files={count}",
                    kind.label()
                );
            }
        }
        return Ok(());
    }

    if matches!(args.dataset, Dataset::Backtest1s | Dataset::Both) {
        initialize_output_root(&args.backtest_out_root, &args.backtest_in_root, &provenance)?;
    }
    if matches!(args.dataset, Dataset::Baseline1min | Dataset::Both) {
        initialize_output_root(&args.baseline_out_root, &args.baseline_in_root, &provenance)?;
    }

    let pool = ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build()
        .context("build continuous-session trim worker pool")?;
    let results = pool.install(|| {
        jobs.par_iter()
            .map(|job| {
                filter_file(job, &sessions, args.overwrite).with_context(|| {
                    format!(
                        "trim dataset={} source={}",
                        job.dataset.label(),
                        job.source.display()
                    )
                })
            })
            .collect::<Vec<_>>()
    });
    let mut stats = RunStats::default();
    for result in results {
        match result? {
            Some(file) => {
                stats.files += 1;
                stats.input_rows += file.input_rows;
                stats.output_rows += file.output_rows;
            }
            None => stats.skipped_existing += 1,
        }
    }
    eprintln!(
        "continuous-session trim ok as_of={} files={} skipped_existing={} input_rows={} output_rows={}",
        sessions.state().as_of,
        stats.files,
        stats.skipped_existing,
        stats.input_rows,
        stats.output_rows,
    );
    Ok(())
}
