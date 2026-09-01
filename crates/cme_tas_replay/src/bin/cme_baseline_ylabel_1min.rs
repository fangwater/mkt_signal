//! Build 60-column 1-minute labels from raw or HFQ CME baseline files.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{Duration, NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::America::Chicago;
use clap::Parser;
use cme_tas_replay::ylabel_1min::{
    build_ylabel_rows, valid_label_price, ylabel_columns, CausalPrices, YlabelRow, LABEL_COUNT,
};
use mimalloc::MiMalloc;
use polars::prelude::{
    DataFrame, NamedFrom, ParquetCompression, ParquetReader, ParquetWriter, SerReader, Series,
};
use rayon::prelude::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const DEFAULT_INPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min";
const DEFAULT_OUTPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/ylabel_1min";

#[derive(Parser, Debug)]
#[command(name = "cme_baseline_ylabel_1min")]
struct Args {
    #[arg(long, default_value = DEFAULT_INPUT)]
    input_root: PathBuf,
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    output_root: PathBuf,
    #[arg(long, default_value = "2020-01-01")]
    start: NaiveDate,
    #[arg(long, default_value = "2026-06-01")]
    end: NaiveDate,
    #[arg(long, value_delimiter = ',', default_value = "ES,NQ,RTY,YM,GC,CL")]
    products: Vec<String>,
    #[arg(long, default_value_t = 1)]
    workers: usize,
    #[arg(long)]
    overwrite: bool,
}

#[derive(Clone, Copy)]
struct ProductSpec {
    product: &'static str,
    exchange: &'static str,
}

const PRODUCTS: &[ProductSpec] = &[
    ProductSpec {
        product: "ES",
        exchange: "CME",
    },
    ProductSpec {
        product: "NQ",
        exchange: "CME",
    },
    ProductSpec {
        product: "RTY",
        exchange: "CME",
    },
    ProductSpec {
        product: "YM",
        exchange: "CBOT",
    },
    ProductSpec {
        product: "GC",
        exchange: "COMEX",
    },
    ProductSpec {
        product: "CL",
        exchange: "NYMEX",
    },
];

fn product_spec(product: &str) -> Result<ProductSpec> {
    PRODUCTS
        .iter()
        .copied()
        .find(|spec| spec.product == product)
        .ok_or_else(|| anyhow!("unsupported CME ylabel product {product:?}"))
}

fn validate_args(args: &Args) -> Result<Vec<ProductSpec>> {
    if args.end <= args.start || args.workers == 0 {
        bail!("invalid date range or worker count");
    }
    if args.input_root == args.output_root || args.output_root.starts_with(&args.input_root) {
        bail!("ylabel output must be an independent staging root");
    }
    if args.input_root.to_string_lossy().contains("rocksdb")
        || args.output_root.to_string_lossy().contains("rocksdb")
    {
        bail!("refusing a RocksDB ylabel path");
    }
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();
    for product in &args.products {
        let product = product.trim().to_ascii_uppercase();
        if seen.insert(product.clone()) {
            output.push(product_spec(&product)?);
        }
    }
    if output.is_empty() {
        bail!("no products selected");
    }
    Ok(output)
}

fn list_days(args: &Args, spec: ProductSpec) -> Result<Vec<(NaiveDate, PathBuf)>> {
    let dir = args.input_root.join(spec.exchange).join(spec.product);
    if !dir.is_dir() {
        bail!("missing baseline directory {}", dir.display());
    }
    let mut output = Vec::new();
    for entry in fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
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
        if day >= args.start && day < args.end {
            output.push((day, path));
        }
    }
    output.sort_by_key(|(day, _)| *day);
    if output.is_empty() {
        bail!("no baseline files selected for {}", spec.product);
    }
    Ok(output)
}

fn merge_observation(
    by_contract: &mut HashMap<String, BTreeMap<i64, CausalPrices>>,
    contract_id: &str,
    ts: i64,
    volume: f64,
    twap: Option<f64>,
    vwap: Option<f64>,
    midp: Option<f64>,
) {
    let mut value = CausalPrices::default();
    if volume > 0.0 {
        value.twap = valid_label_price(twap);
        value.vwap = valid_label_price(vwap);
    }
    value.midp = valid_label_price(midp);
    if value.observed() {
        let entry = by_contract
            .entry(contract_id.to_string())
            .or_default()
            .entry(ts + 60)
            .or_default();
        if value.twap.is_some() {
            entry.twap = value.twap;
        }
        if value.vwap.is_some() {
            entry.vwap = value.vwap;
        }
        if value.midp.is_some() {
            entry.midp = value.midp;
        }
    }
}

fn load_prices(
    days: &[(NaiveDate, PathBuf)],
) -> Result<HashMap<String, BTreeMap<i64, CausalPrices>>> {
    let mut by_contract = HashMap::new();
    for (_, path) in days {
        let df = ParquetReader::new(
            File::open(path).with_context(|| format!("open {}", path.display()))?,
        )
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", path.display()))?;
        let contract_ids = df.column("contract_id")?.str()?;
        let timestamps = df.column("ts")?.i64()?;
        let volumes = df.column("volume")?.f64()?;
        let twaps = df.column("twap")?.f64()?;
        let vwaps = df.column("vwap")?.f64()?;
        let midps = df.column("mid_price")?.f64()?;
        for index in 0..df.height() {
            let contract_id = contract_ids
                .get(index)
                .with_context(|| format!("null contract_id row {index} in {}", path.display()))?;
            let ts = timestamps
                .get(index)
                .with_context(|| format!("null ts row {index} in {}", path.display()))?;
            if ts % 60 != 0 {
                bail!("non-minute ts {ts} in {}", path.display());
            }
            merge_observation(
                &mut by_contract,
                contract_id,
                ts,
                volumes
                    .get(index)
                    .with_context(|| format!("null volume row {index} in {}", path.display()))?,
                twaps.get(index),
                vwaps.get(index),
                midps.get(index),
            );
        }
    }
    Ok(by_contract)
}

fn chicago_trading_day(ts: i64) -> NaiveDate {
    let local = Utc
        .timestamp_opt(ts, 0)
        .single()
        .expect("unix timestamp")
        .with_timezone(&Chicago);
    if local.hour() >= 17 {
        local.date_naive() + Duration::days(1)
    } else {
        local.date_naive()
    }
}

fn dataframe(rows: &[YlabelRow]) -> Result<DataFrame> {
    let mut columns = vec![
        Series::new(
            "contract_id".into(),
            rows.iter()
                .map(|row| row.contract_id.clone())
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "ts".into(),
            rows.iter().map(|row| row.ts).collect::<Vec<_>>(),
        ),
    ];
    for (index, name) in ylabel_columns().into_iter().enumerate() {
        columns.push(Series::new(
            name.into(),
            rows.iter().map(|row| row.labels[index]).collect::<Vec<_>>(),
        ));
    }
    Ok(DataFrame::new(columns)?)
}

fn write_parquet_atomic(path: &Path, rows: &[YlabelRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("parquet.tmp");
    let mut df = dataframe(rows)?;
    let result = (|| -> Result<()> {
        ParquetWriter::new(File::create(&tmp)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)?;
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn write_parquet_serialized(lock: &Mutex<()>, path: &Path, rows: &[YlabelRow]) -> Result<()> {
    let _guard = lock.lock().map_err(|_| anyhow!("parquet lock poisoned"))?;
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("cme-ylabel-parquet".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn_scoped(scope, || write_parquet_atomic(path, rows))
            .map_err(anyhow::Error::from)?
            .join()
            .map_err(|_| anyhow!("ylabel writer panicked for {}", path.display()))?
    })
}

fn process_product(args: &Args, spec: ProductSpec, parquet_lock: &Mutex<()>) -> Result<(u64, u64)> {
    let days = list_days(args, spec)?;
    let by_contract = load_prices(&days)?;
    let mut by_day: BTreeMap<NaiveDate, Vec<YlabelRow>> = BTreeMap::new();
    for (contract_id, prices) in by_contract {
        for row in build_ylabel_rows(&contract_id, &prices) {
            // `row.ts` denotes the following minute boundary. At the 17:00
            // close, assign it to the closed 16:59 minute's trading day.
            let day = chicago_trading_day(row.ts - 60);
            if day >= args.start && day < args.end {
                by_day.entry(day).or_default().push(row);
            }
        }
    }
    let mut files = 0u64;
    let mut rows = 0u64;
    for (day, mut day_rows) in by_day {
        let output = args
            .output_root
            .join(spec.exchange)
            .join(spec.product)
            .join(format!("{}.parquet", day.format("%Y%m%d")));
        if output.exists() && !args.overwrite {
            bail!("refusing to overwrite {}", output.display());
        }
        day_rows.sort_by(|left, right| {
            left.contract_id
                .cmp(&right.contract_id)
                .then(left.ts.cmp(&right.ts))
        });
        let count = day_rows.len() as u64;
        write_parquet_serialized(parquet_lock, &output, &day_rows)?;
        eprintln!(
            "ylabel_done product={} day={} rows={count}",
            spec.product, day
        );
        files += 1;
        rows += count;
    }
    Ok((files, rows))
}

fn run() -> Result<()> {
    let args = Arc::new(Args::parse());
    let products = validate_args(&args)?;
    eprintln!(
        "ylabel_start products={} start={} end={} workers={} input={} output={}",
        products
            .iter()
            .map(|spec| spec.product)
            .collect::<Vec<_>>()
            .join(","),
        args.start,
        args.end,
        args.workers,
        args.input_root.display(),
        args.output_root.display()
    );
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .stack_size(16 * 1024 * 1024)
        .build()?;
    let parquet_lock = Arc::new(Mutex::new(()));
    let results = pool.install(|| {
        products
            .par_iter()
            .map(|spec| process_product(&args, *spec, &parquet_lock))
            .collect::<Vec<_>>()
    });
    let mut files = 0u64;
    let mut rows = 0u64;
    for result in results {
        let (product_files, product_rows) = result?;
        files += product_files;
        rows += product_rows;
    }
    eprintln!(
        "ylabel_complete files={files} rows={rows} columns={}",
        LABEL_COUNT + 2
    );
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("cme_baseline_ylabel_1min failed: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_boundary_stays_on_the_closed_minute_trading_day() {
        // 2020-01-02 17:00 Chicago is the boundary following the last minute
        // of the 2020-01-02 Globex TradDay.
        let close = Chicago
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2020, 1, 2)
                    .unwrap()
                    .and_hms_opt(17, 0, 0)
                    .unwrap(),
            )
            .single()
            .unwrap()
            .timestamp();
        assert_eq!(
            chicago_trading_day(close - 60),
            NaiveDate::from_ymd_opt(2020, 1, 2).unwrap()
        );
    }

    #[test]
    fn trade_only_observation_has_no_midp() {
        let mut by_contract = HashMap::new();
        merge_observation(
            &mut by_contract,
            "ESH24",
            0,
            1.0,
            Some(100.0),
            Some(100.0),
            None,
        );
        assert_eq!(by_contract["ESH24"][&60].midp, None);
    }
}
