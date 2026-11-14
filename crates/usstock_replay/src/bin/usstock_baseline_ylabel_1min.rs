//! Build US-stock 1-minute supervision labels from RAW or HFQ RAW bars.
//!
//! The label math is the shared CME 60-column contract. US equities are
//! RTH-only, so exact 60-second continuity is required for every rolling
//! window and no overnight or weekend bridge is created.

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use cme_tas_replay::ylabel_1min::{
    build_ylabel_rows, valid_label_price, CausalPrices, YlabelRow, LABEL_COUNT,
};
use polars::prelude::{
    DataFrame, NamedFrom, ParquetCompression, ParquetReader, ParquetWriter, SerReader, Series,
};
use rayon::prelude::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_INPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/usstock_data/baseline_data_1m_raw";
const DEFAULT_OUTPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/usstock_data/ylabel_1min_raw";
const RAW_BASELINE: &str = "baseline_data_1m_raw";
const RAW_HFQ_BASELINE: &str = "baseline_data_1min_hfq_raw";
const RAW_YLABEL: &str = "ylabel_1min_raw";
const RAW_HFQ_YLABEL: &str = "ylabel_1min_hfq_raw";
const PRICE_COLUMNS: &[&str] = &["ric", "venue", "ts", "volume", "twap", "vwap", "mid_price"];

#[derive(Parser, Debug)]
#[command(name = "usstock_baseline_ylabel_1min")]
#[command(about = "Build US-stock RAW/HFQ RAW 1-minute supervision labels")]
struct Args {
    #[arg(long, default_value = DEFAULT_INPUT)]
    input_root: PathBuf,
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    output_root: PathBuf,
    #[arg(long, default_value = "2021-07-01")]
    start: String,
    #[arg(long, default_value = "2025-06-30")]
    end: String,
    #[arg(long, value_delimiter = ',')]
    rics: Vec<String>,
    #[arg(long, default_value_t = 16)]
    workers: usize,
    #[arg(long)]
    overwrite: bool,
    /// Require an explicit RAW or HFQ RAW input/output basename pair.
    #[arg(long)]
    raw: bool,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Clone, Debug)]
struct DayFile {
    day: NaiveDate,
    path: PathBuf,
}

#[derive(Clone, Debug)]
struct RicJob {
    venue: String,
    ric: String,
    days: Vec<DayFile>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputKind {
    Raw,
    HfqRaw,
}

impl InputKind {
    fn required_input(self) -> &'static str {
        match self {
            Self::Raw => RAW_BASELINE,
            Self::HfqRaw => RAW_HFQ_BASELINE,
        }
    }

    fn required_output(self) -> &'static str {
        match self {
            Self::Raw => RAW_YLABEL,
            Self::HfqRaw => RAW_HFQ_YLABEL,
        }
    }
}

fn parse_day(text: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .with_context(|| format!("invalid date {text:?}, expected YYYY-MM-DD"))
}

fn input_kind(args: &Args) -> Result<InputKind> {
    if !args.raw {
        bail!("--raw is required; this exporter never reads non-RAW baseline data");
    }
    let input_name = args.input_root.file_name().and_then(|value| value.to_str());
    let output_name = args
        .output_root
        .file_name()
        .and_then(|value| value.to_str());
    match (input_name, output_name) {
        (Some(RAW_BASELINE), Some(RAW_YLABEL)) => Ok(InputKind::Raw),
        (Some(RAW_HFQ_BASELINE), Some(RAW_HFQ_YLABEL)) => Ok(InputKind::HfqRaw),
        _ => bail!(concat!(
            "--raw requires exact pairs: baseline_data_1m_raw/ylabel_1min_raw ",
            "or baseline_data_1min_hfq_raw/ylabel_1min_hfq_raw"
        )),
    }
}

fn validate_args(args: &Args) -> Result<(InputKind, NaiveDate, NaiveDate)> {
    let kind = input_kind(args)?;
    let start = parse_day(&args.start)?;
    let end = parse_day(&args.end)?;
    if end < start {
        bail!("--end precedes --start");
    }
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    if args.input_root == args.output_root || args.output_root.starts_with(&args.input_root) {
        bail!("ylabel output must be an independent root");
    }
    if args.input_root.to_string_lossy().contains("rocksdb")
        || args.output_root.to_string_lossy().contains("rocksdb")
    {
        bail!("refusing a RocksDB ylabel path");
    }
    if args.input_root.file_name().and_then(|value| value.to_str()) != Some(kind.required_input())
        || args
            .output_root
            .file_name()
            .and_then(|value| value.to_str())
            != Some(kind.required_output())
    {
        bail!("input/output basename pair does not match selected RAW kind");
    }
    Ok((kind, start, end))
}

fn sorted_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut output = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("read {}", root.display()))? {
        let path = entry?.path();
        if path.is_dir()
            && !path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with('_'))
        {
            output.push(path);
        }
    }
    output.sort();
    Ok(output)
}

fn list_jobs(
    root: &Path,
    start: NaiveDate,
    end: NaiveDate,
    requested: &BTreeSet<String>,
) -> Result<Vec<RicJob>> {
    let mut jobs = Vec::new();
    for venue_dir in sorted_dirs(root)? {
        let venue = venue_dir
            .file_name()
            .and_then(|name| name.to_str())
            .context("non-UTF8 venue directory")?
            .to_string();
        for ric_dir in sorted_dirs(&venue_dir)? {
            let ric = ric_dir
                .file_name()
                .and_then(|name| name.to_str())
                .context("non-UTF8 RIC directory")?
                .to_string();
            if !requested.is_empty() && !requested.contains(&ric) {
                continue;
            }
            let mut days = Vec::new();
            for entry in
                fs::read_dir(&ric_dir).with_context(|| format!("read {}", ric_dir.display()))?
            {
                let path = entry?.path();
                if path.extension().and_then(|value| value.to_str()) != Some("parquet") {
                    continue;
                }
                let stem = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .unwrap_or("");
                let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") else {
                    continue;
                };
                if day >= start && day <= end {
                    days.push(DayFile { day, path });
                }
            }
            days.sort_by_key(|file| file.day);
            if !days.is_empty() {
                jobs.push(RicJob {
                    venue: venue.clone(),
                    ric,
                    days,
                });
            }
        }
    }
    jobs.sort_by(|left, right| left.ric.cmp(&right.ric).then(left.venue.cmp(&right.venue)));
    if jobs.is_empty() {
        bail!("no eligible parquet files under {}", root.display());
    }
    Ok(jobs)
}

fn merge_observation(
    prices: &mut BTreeMap<i64, CausalPrices>,
    ts: i64,
    volume: f64,
    twap: Option<f64>,
    vwap: Option<f64>,
    mid_price: Option<f64>,
) -> bool {
    let value = CausalPrices {
        twap: if volume > 0.0 {
            valid_label_price(twap)
        } else {
            None
        },
        vwap: if volume > 0.0 {
            valid_label_price(vwap)
        } else {
            None
        },
        midp: valid_label_price(mid_price),
    };
    if !value.observed() {
        return false;
    }
    prices.insert(ts + 60, value);
    true
}

fn load_prices(
    job: &RicJob,
) -> Result<(
    BTreeMap<i64, CausalPrices>,
    BTreeMap<NaiveDate, Vec<i64>>,
    u64,
)> {
    let mut prices = BTreeMap::new();
    let mut day_keys = BTreeMap::new();
    let mut source_rows = 0u64;
    for day in &job.days {
        let frame = ParquetReader::new(
            File::open(&day.path).with_context(|| format!("open {}", day.path.display()))?,
        )
        .with_columns(Some(
            PRICE_COLUMNS
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        ))
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", day.path.display()))?;
        let rics = frame.column("ric")?.str()?;
        let venues = frame.column("venue")?.str()?;
        let timestamps = frame.column("ts")?.i64()?;
        let volumes = frame.column("volume")?.f64()?;
        let twaps = frame.column("twap")?.f64()?;
        let vwaps = frame.column("vwap")?.f64()?;
        let mid_prices = frame.column("mid_price")?.f64()?;
        let mut keys = Vec::new();
        for index in 0..frame.height() {
            source_rows += 1;
            if rics.get(index) != Some(job.ric.as_str())
                || venues.get(index) != Some(job.venue.as_str())
            {
                bail!("identity mismatch row {index} in {}", day.path.display());
            }
            let ts = timestamps
                .get(index)
                .with_context(|| format!("null ts row {index} in {}", day.path.display()))?;
            if ts.rem_euclid(60) != 0 {
                bail!("non-minute ts {ts} in {}", day.path.display());
            }
            if merge_observation(
                &mut prices,
                ts,
                volumes.get(index).with_context(|| {
                    format!("null volume row {index} in {}", day.path.display())
                })?,
                twaps.get(index),
                vwaps.get(index),
                mid_prices.get(index),
            ) {
                keys.push(ts + 60);
            }
        }
        keys.sort_unstable();
        keys.dedup();
        day_keys.insert(day.day, keys);
    }
    Ok((prices, day_keys, source_rows))
}

fn write_day(path: &Path, venue: &str, ric: &str, rows: &[YlabelRow]) -> Result<()> {
    let mut columns = vec![
        Series::new("venue".into(), vec![venue.to_string(); rows.len()]),
        Series::new("ric".into(), vec![ric.to_string(); rows.len()]),
        Series::new(
            "ts".into(),
            rows.iter().map(|row| row.ts).collect::<Vec<_>>(),
        ),
    ];
    let names = cme_tas_replay::ylabel_1min::ylabel_columns();
    for (index, name) in names.into_iter().enumerate() {
        columns.push(Series::new(
            name.into(),
            rows.iter().map(|row| row.labels[index]).collect::<Vec<_>>(),
        ));
    }
    let mut frame = DataFrame::new(columns)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("parquet.tmp");
    let result = (|| -> Result<()> {
        ParquetWriter::new(File::create(&tmp)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut frame)?;
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn process_job(args: &Args, job: &RicJob) -> Result<(u64, u64, u64)> {
    let (prices, day_keys, source_rows) = load_prices(job)?;
    let all_rows = build_ylabel_rows(&job.ric, &prices);
    let rows_by_ts = all_rows
        .into_iter()
        .map(|row| (row.ts, row))
        .collect::<HashMap<_, _>>();
    let mut files = 0u64;
    let mut rows = 0u64;
    for (day, keys) in day_keys {
        let mut output_rows = keys
            .into_iter()
            .filter_map(|ts| rows_by_ts.get(&ts).cloned())
            .collect::<Vec<_>>();
        output_rows.sort_by_key(|row| row.ts);
        if output_rows.is_empty() {
            continue;
        }
        let output = args
            .output_root
            .join(&job.venue)
            .join(&job.ric)
            .join(format!("{}.parquet", day.format("%Y%m%d")));
        if output.exists() && !args.overwrite {
            bail!("refusing to overwrite {}", output.display());
        }
        write_day(&output, &job.venue, &job.ric, &output_rows)?;
        files += 1;
        rows += output_rows.len() as u64;
    }
    Ok((files, rows, source_rows))
}

fn run() -> Result<()> {
    if std::env::var_os("POLARS_MAX_THREADS").is_none() {
        std::env::set_var("POLARS_MAX_THREADS", "1");
    }
    let args = Args::parse();
    let (kind, start, end) = validate_args(&args)?;
    let requested = args
        .rics
        .iter()
        .map(|ric| ric.trim().to_string())
        .filter(|ric| !ric.is_empty())
        .collect::<BTreeSet<_>>();
    let jobs = list_jobs(&args.input_root, start, end, &requested)?;
    let planned_files = jobs.iter().map(|job| job.days.len()).sum::<usize>();
    eprintln!(
        "ylabel_start kind={kind:?} venues={} rics={} files={} start={} end={} workers={} input={} output={}",
        jobs.iter().map(|job| job.venue.as_str()).collect::<BTreeSet<_>>().len(),
        jobs.len(),
        planned_files,
        start,
        end,
        args.workers,
        args.input_root.display(),
        args.output_root.display()
    );
    if args.dry_run {
        return Ok(());
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .stack_size(32 * 1024 * 1024)
        .build()?;
    let files = AtomicU64::new(0);
    let rows = AtomicU64::new(0);
    let source_rows = AtomicU64::new(0);
    pool.install(|| {
        jobs.par_iter().try_for_each(|job| -> Result<()> {
            let (written_files, written_rows, read_rows) = process_job(&args, job)?;
            files.fetch_add(written_files, Ordering::Relaxed);
            rows.fetch_add(written_rows, Ordering::Relaxed);
            source_rows.fetch_add(read_rows, Ordering::Relaxed);
            Ok(())
        })
    })?;
    eprintln!(
        "ylabel_complete files={} rows={} source_rows={} columns={}",
        files.load(Ordering::Relaxed),
        rows.load(Ordering::Relaxed),
        source_rows.load(Ordering::Relaxed),
        LABEL_COUNT + 3
    );
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("usstock_baseline_ylabel_1min failed: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prices(value: f64) -> CausalPrices {
        CausalPrices {
            twap: Some(value),
            vwap: Some(value),
            midp: Some(value),
        }
    }

    #[test]
    fn input_output_pairs_are_raw_only() {
        let args = Args {
            input_root: PathBuf::from("baseline_data_1m_raw"),
            output_root: PathBuf::from("ylabel_1min_raw"),
            start: "2021-07-01".into(),
            end: "2021-07-02".into(),
            rics: Vec::new(),
            workers: 1,
            overwrite: false,
            raw: true,
            dry_run: false,
        };
        assert_eq!(input_kind(&args).unwrap(), InputKind::Raw);
    }

    #[test]
    fn carried_trade_price_is_not_an_observation_without_volume() {
        let mut prices = BTreeMap::new();
        assert!(!merge_observation(
            &mut prices,
            0,
            0.0,
            Some(100.0),
            Some(100.0),
            None,
        ));
        assert!(prices.is_empty());
    }

    #[test]
    fn missing_clock_minute_breaks_future_label() {
        let input = BTreeMap::from([(0, prices(100.0)), (10 * 60, prices(110.0))]);
        let rows = build_ylabel_rows("MCO.N", &input);
        let row = rows.iter().find(|row| row.ts == 0).unwrap();
        assert!(row.labels[0].is_none());
    }
}
