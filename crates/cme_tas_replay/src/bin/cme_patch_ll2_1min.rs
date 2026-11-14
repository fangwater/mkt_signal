//! Fill only existing null LL2 columns, preserving the published minute grid.
use anyhow::{bail, Context, Result};
use chrono::{Datelike, NaiveDate};
use clap::Parser;
use cme_tas_replay::ll2_1min::{decode_ll2_minute, encode_ll2_minute_key, Ll2MinuteKey};
use cme_tas_replay::product::{parse_product, product_cf_name};
use cme_tas_replay::{CF_REPLAY_META, MISSING_PRICE, PRICE_SCALE};
use polars::prelude::{NamedFrom, ParquetReader, ParquetWriter, SerReader, Series};
use rayon::prelude::*;
use rocksdb::{IteratorMode, Options, DB};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    input_root: PathBuf,
    #[arg(long)]
    output_root: PathBuf,
    /// Source priority: first usable minute wins. Can include Normalized and Raw.
    #[arg(long, required = true, num_args = 1..)]
    ll2_rocksdb: Vec<PathBuf>,
    #[arg(long)]
    start: NaiveDate,
    #[arg(long)]
    end: NaiveDate,
    #[arg(long, value_delimiter = ',', default_value = "ES,NQ,RTY,YM,GC,CL")]
    products: Vec<String>,
    #[arg(long)]
    dry_run: bool,
    /// Fixture DBs only; production sources require done watermarks.
    #[arg(long)]
    allow_sample: bool,
    /// Only donors with a known, valid, non-crossed two-sided L1 are usable.
    #[arg(long)]
    require_valid_l1: bool,
    /// Include wire snapshots in provenance for bounded Python comparisons.
    #[arg(long)]
    audit_snapshot_values: bool,
    #[arg(long, default_value_t = 4)]
    workers: usize,
}
struct Source {
    db: DB,
    windows: Vec<(u64, u64, String)>,
    raw: bool,
}
fn open_source(path: &Path, args: &Args) -> Result<Source> {
    let names = DB::list_cf(&Options::default(), path)?;
    let selected: Vec<_> = names
        .into_iter()
        .filter(|name| {
            if name == "default" || name == CF_REPLAY_META {
                return true;
            }
            let data = name.strip_prefix("q:").unwrap_or(name);
            let data = data.strip_prefix("s15:").unwrap_or(data);
            let Some(rest) = data.strip_prefix("p:") else {
                return false;
            };
            let Some((year, product)) = rest.split_once(':') else {
                return false;
            };
            year.parse::<i32>()
                .is_ok_and(|year| (args.start.year() - 1..=args.end.year()).contains(&year))
                && args.products.iter().any(|p| p == product)
        })
        .collect();
    eprintln!(
        "opening read-only LL2 {} selected_cfs={}",
        path.display(),
        selected.len()
    );
    let descriptors = selected.into_iter().map(|name| {
        let mut options = Options::default();
        if name.starts_with("p:") {
            options.set_merge_operator_associative(
                "ll2_latest",
                cme_tas_replay::ll2_1min::ll2_latest_merge,
            );
        }
        rocksdb::ColumnFamilyDescriptor::new(name, options)
    });
    // Thousands of CFs share a large source DB. Bound table readers rather
    // than pre-opening every SST and reading unrelated products' indexes.
    let mut db_options = Options::default();
    db_options.set_max_open_files(256);
    let db = DB::open_cf_descriptors_read_only(&db_options, path, descriptors, false)?;
    let raw = db.get(b"db_kind")?.as_deref() == Some(b"cme_raw_ll2_v1");
    let meta = db
        .cf_handle(CF_REPLAY_META)
        .context("LL2 source lacks replay_meta")?;
    let mut windows = Vec::new();
    for entry in db.iterator_cf(&meta, IteratorMode::Start) {
        let (key, value) = entry?;
        let key = std::str::from_utf8(&key)?;
        if let Some(period) = key.strip_prefix("period:") {
            let (start, end) = period.split_once('_').context("invalid source period")?;
            let timestamp = |s: &str| -> Result<u64> {
                Ok(u64::try_from(
                    NaiveDate::parse_from_str(s, "%Y-%m-%d")?
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                        .and_utc()
                        .timestamp(),
                )? * 1_000_000_000)
            };
            let status = std::str::from_utf8(&value)?.to_string();
            if status == "sample" && !args.allow_sample {
                bail!("sample LL2 source requires --allow-sample");
            }
            windows.push((timestamp(start)?, timestamp(end)?, status));
        }
    }
    if windows.is_empty() {
        bail!("LL2 source has no period watermarks");
    }
    eprintln!(
        "opened read-only LL2 {} periods={} raw={raw}",
        path.display(),
        windows.len()
    );
    Ok(Source { db, windows, raw })
}
fn pair(price: i64, size: i64) -> Option<(f64, f64)> {
    if price == MISSING_PRICE || size == MISSING_PRICE || price <= 0 || size < 0 {
        return None;
    }
    Some((
        price as f64 / PRICE_SCALE as f64,
        size as f64 / PRICE_SCALE as f64,
    ))
}
fn candidate(
    source: &Source,
    ric: &str,
    ts: i64,
) -> Result<Option<(cme_tas_replay::ll2_1min::Ll2Minute, u64)>> {
    let ns = u64::try_from(ts)? * 1_000_000_000;
    if ns % 60_000_000_000 != 0 {
        bail!("baseline ts is not a UTC minute");
    }
    let covering: Vec<_> = source
        .windows
        .iter()
        .filter(|(s, e, _)| *s <= ns && ns < *e)
        .collect();
    if covering.is_empty() {
        return Ok(None);
    }
    if covering
        .iter()
        .any(|(_, _, status)| status != "done" && status != "sample")
    {
        bail!("baseline minute overlaps an incomplete LL2 period");
    }
    let year = chrono::DateTime::from_timestamp(ts, 0)
        .context("invalid baseline timestamp")?
        .format("%Y")
        .to_string()
        .parse::<u16>()?;
    let name = product_cf_name(year, &parse_product(ric).context("unparsed baseline RIC")?)?;
    let mut name = name;
    let mut key = encode_ll2_minute_key(&Ll2MinuteKey {
        ric: ric.into(),
        minute_utc_ns: ns,
    })?;
    // Prefer Raw 15s wherever present, including invalid/empty final states.
    // Sparse minutes without events must not inherit an older minute's book.
    let bytes = if source.raw && source.db.cf_handle(&format!("s15:{name}")).is_some() {
        name = format!("s15:{name}");
        let cf = source.db.cf_handle(&name).unwrap();
        let mut end_key = key;
        end_key[16..].copy_from_slice(&(ns + 45_000_000_000).to_be_bytes());
        let mut iterator = source.db.raw_iterator_cf(&cf);
        iterator.seek_for_prev(end_key);
        iterator.status()?;
        let Some(found_key) = iterator.key() else {
            return Ok(None);
        };
        if found_key.len() != key.len()
            || found_key[..16] != key[..16]
            || found_key[16..] < key[16..]
        {
            return Ok(None);
        }
        key.copy_from_slice(found_key);
        iterator.value().context("missing Raw 15s value")?.to_vec()
    } else {
        let Some(cf) = source.db.cf_handle(&name) else {
            return Ok(None);
        };
        let Some(bytes) = source.db.get_cf(&cf, key)? else {
            return Ok(None);
        };
        bytes
    };
    let snapshot = decode_ll2_minute(&bytes)?;
    if snapshot.source_ts_utc_ns < ns || snapshot.source_ts_utc_ns >= ns + 60_000_000_000 {
        bail!("LL2 snapshot outside requested minute");
    }
    let known = if source.raw {
        let q = source
            .db
            .cf_handle(&format!("q:{name}"))
            .context("Raw LL2 missing quality CF")?;
        let bytes = source
            .db
            .get_cf(&q, key)?
            .context("Raw LL2 missing quality mask")?;
        u64::from_le_bytes(bytes.as_slice().try_into()?)
    } else {
        (1 << 61) - 1
    };
    cme_tas_replay::raw_ll2::validate_quality(&snapshot, known)?;
    Ok(Some((snapshot, known)))
}
const SPECS: [(&str, &str); 16] = [
    ("ES", "CME"),
    ("NQ", "CME"),
    ("RTY", "CME"),
    ("YM", "CBOT"),
    ("GC", "COMEX"),
    ("CL", "NYMEX"),
    ("URO", "CME"),
    ("JY", "CME"),
    ("HG", "COMEX"),
    ("SI", "COMEX"),
    ("TU", "CBOT"),
    ("FV", "CBOT"),
    ("TY", "CBOT"),
    ("TN", "CBOT"),
    ("US", "CBOT"),
    ("AUL", "CBOT"),
];
fn patch_file(input: &Path, output: &Path, sources: &[Source], args: &Args) -> Result<Value> {
    if output.exists() {
        bail!("output already exists: {}", output.display());
    }
    let mut frame = ParquetReader::new(File::open(input)?).finish()?;
    let rics: Vec<_> = frame
        .column("ric")?
        .str()?
        .into_iter()
        .map(|x| x.map(str::to_owned))
        .collect();
    let timestamps: Vec<_> = frame.column("ts")?.i64()?.into_iter().collect();
    let mut names = Vec::new();
    for level in 0..10 {
        for side in ["bid", "ask"] {
            for field in ["p", "v"] {
                names.push(format!("{side}{level}{field}"));
            }
        }
    }
    names.push("mid_price".into());
    let mut columns: Vec<Vec<Option<f64>>> = names
        .iter()
        .map(|name| -> Result<_> { Ok(frame.column(name)?.f64()?.into_iter().collect()) })
        .collect::<Result<_>>()?;
    let mut changed = 0u64;
    let mut cells = 0u64;
    let mut donor_counts = BTreeMap::<usize, u64>::new();
    let mut provenance = csv::Writer::from_writer(Vec::new());
    provenance.write_record([
        "ric",
        "ts",
        "source_index",
        "source_ts_utc_ns",
        "source_order",
        "known_mask",
        "value_hex",
    ])?;
    let mut unique = BTreeSet::new();
    for i in 0..frame.height() {
        let ric = rics[i].as_deref().context("null baseline ric")?;
        let ts = timestamps[i].context("null baseline ts")?;
        if !unique.insert((ric, ts)) {
            bail!("duplicate baseline (ric,ts)");
        }
        for (source_index, source) in sources.iter().enumerate() {
            let Some((snapshot, known)) = candidate(source, ric, ts)? else {
                continue;
            };
            if args.require_valid_l1 && !valid_l1(&snapshot, known) {
                continue;
            }
            let mut fills = Vec::new();
            for level in 0..10 {
                for side in 0..2 {
                    let field = level * 6 + side * 3;
                    if known & (3u64 << field) != 3u64 << field {
                        continue;
                    }
                    let (price, size) = if side == 0 {
                        (snapshot.bid_prices[level], snapshot.bid_sizes[level])
                    } else {
                        (snapshot.ask_prices[level], snapshot.ask_sizes[level])
                    };
                    if let Some((price, size)) = pair(price, size) {
                        let p = level * 4 + side * 2;
                        // Do not combine a new side with an existing conflicting value.
                        if columns[p][i].is_some_and(|v| v != price)
                            || columns[p + 1][i].is_some_and(|v| v != size)
                        {
                            continue;
                        }
                        if columns[p][i].is_none() {
                            fills.push((p, price));
                        }
                        if columns[p + 1][i].is_none() {
                            fills.push((p + 1, size));
                        }
                    }
                }
            }
            if columns[40][i].is_none() && known & 27 == 27 {
                if let (Some((bid, bv)), Some((ask, av))) = (
                    pair(snapshot.bid_prices[0], snapshot.bid_sizes[0]),
                    pair(snapshot.ask_prices[0], snapshot.ask_sizes[0]),
                ) {
                    if ask >= bid
                        && [bid, bv, ask, av]
                            .iter()
                            .enumerate()
                            .all(|(j, v)| columns[j][i].is_none_or(|old| old == *v))
                    {
                        fills.push((40, (bid + ask) / 2.0));
                    }
                }
            }
            if fills.is_empty() {
                continue;
            }
            let before = cells;
            for (column, value) in fills {
                columns[column][i] = Some(value);
                cells += 1;
            }
            if cells > before {
                changed += 1;
                *donor_counts.entry(source_index).or_default() += 1;
                provenance.write_record([
                    ric.to_string(),
                    ts.to_string(),
                    source_index.to_string(),
                    snapshot.source_ts_utc_ns.to_string(),
                    snapshot.source_order.to_string(),
                    known.to_string(),
                    if args.audit_snapshot_values {
                        cme_tas_replay::ll2_1min::encode_ll2_minute(&snapshot)
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect()
                    } else {
                        String::new()
                    },
                ])?;
            }
            // One source per row: avoid assembling levels from different source instants.
            break;
        }
    }
    for (name, values) in names.iter().zip(columns) {
        frame.replace(name, Series::new(name.into(), values))?;
    }
    let audit = json!({"input":input,"output":output,"rows":frame.height(),"filled_rows":changed,"filled_cells":cells,
        "source_rows":donor_counts,"sources":args.ll2_rocksdb,"dry_run":args.dry_run});
    if !args.dry_run {
        fs::create_dir_all(output.parent().context("output parent")?)?;
        let partial = output.with_extension("parquet.partial");
        let mut file = File::create(&partial)?;
        ParquetWriter::new(&mut file).finish(&mut frame)?;
        file.sync_all()?;
        drop(file);
        let relative = output.strip_prefix(&args.output_root)?;
        let audit_path = args
            .output_root
            .join("_audit/ll2")
            .join(relative)
            .with_extension("json");
        fs::create_dir_all(audit_path.parent().unwrap())?;
        fs::write(&audit_path, serde_json::to_vec_pretty(&audit)?)?;
        let mut provenance = provenance.into_inner()?;
        let csv_path = audit_path.with_extension("csv.zst");
        let mut encoder = zstd::stream::write::Encoder::new(File::create(csv_path)?, 1)?;
        encoder.write_all(&provenance)?;
        provenance.clear();
        encoder.finish()?.sync_all()?;
        fs::rename(&partial, output)?;
    }
    Ok(audit)
}
fn run() -> Result<()> {
    let args = Args::parse();
    if args.start >= args.end {
        bail!("end must be exclusive and after start");
    }
    if args.workers == 0 {
        bail!("workers must be positive");
    }
    let input = fs::canonicalize(&args.input_root)?;
    if args.output_root.exists() {
        bail!("use a new independent output root");
    }
    let output_parent = fs::canonicalize(args.output_root.parent().context("output root parent")?)?;
    let output = output_parent.join(args.output_root.file_name().context("output root name")?);
    if output.starts_with(&input) || input.starts_with(&output) {
        bail!("input and output roots must be independent");
    }
    let mut sources = Vec::new();
    for path in &args.ll2_rocksdb {
        sources.push(open_source(path, &args)?);
    }
    let mut jobs = Vec::new();
    for product in &args.products {
        let (_, exchange) = SPECS
            .iter()
            .find(|(p, _)| p == product)
            .context("baseline product outside reviewed allowlist")?;
        let directory = input.join(exchange).join(product);
        if !directory.is_dir() {
            continue;
        }
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.extension().is_none_or(|s| s != "parquet") {
                continue;
            }
            let date = NaiveDate::parse_from_str(
                path.file_stem().unwrap().to_str().context("file name")?,
                "%Y%m%d",
            )?;
            if date >= args.start && date < args.end {
                jobs.push(path);
            }
        }
    }
    jobs.sort();
    if jobs.is_empty() {
        bail!("no baseline files selected");
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build()?;
    pool.install(|| {
        jobs.par_iter().try_for_each(|path| -> Result<()> {
            let dest = output.join(path.strip_prefix(&input)?);
            println!("{}", patch_file(path, &dest, &sources, &args)?);
            Ok(())
        })
    })?;
    Ok(())
}
fn valid_l1(snapshot: &cme_tas_replay::ll2_1min::Ll2Minute, known: u64) -> bool {
    known & 27 == 27
        && matches!(
        (pair(snapshot.bid_prices[0], snapshot.bid_sizes[0]),
         pair(snapshot.ask_prices[0], snapshot.ask_sizes[0])),
        (Some((bid, _)), Some((ask, _))) if ask >= bid)
}
fn main() {
    if let Err(e) = run() {
        eprintln!("cme_patch_ll2_1min failed: {e:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cme_tas_replay::ll2_1min::encode_ll2_minute;
    use cme_tas_replay::raw_ll2::{raw_cf, Extractor, RawScanner, RawSnapshot};
    use polars::prelude::DataFrame;

    #[test]
    fn valid_l1_rejects_unknown_crossed_and_missing_sides() {
        let mut snapshot = cme_tas_replay::ll2_1min::Ll2Minute {
            source_ts_utc_ns: 1,
            source_order: 1,
            exch_time_ns: 0,
            gmt_offset_minutes: 0,
            bid_prices: [100 * PRICE_SCALE as i64; 10],
            ask_prices: [101 * PRICE_SCALE as i64; 10],
            bid_sizes: [PRICE_SCALE as i64; 10],
            ask_sizes: [PRICE_SCALE as i64; 10],
            bid_counts: [0; 10],
            ask_counts: [0; 10],
        };
        assert!(valid_l1(&snapshot, 27));
        assert!(!valid_l1(&snapshot, 3));
        snapshot.ask_prices[0] = 99 * PRICE_SCALE as i64;
        assert!(!valid_l1(&snapshot, 27));
        snapshot.ask_prices[0] = MISSING_PRICE;
        assert!(!valid_l1(&snapshot, 27));
    }

    #[test]
    fn minute_from_sparse_15s_keeps_last_invalidation_and_stays_in_ric_and_minute() {
        let temp = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let mut db = DB::open(&options, temp.path().join("db")).unwrap();
        let mut scanner = RawScanner::default();
        let mut extractor = Extractor::new(15).unwrap();
        let mut write = |snapshot: RawSnapshot| -> Result<()> {
            let name = raw_cf(2026, &parse_product(&snapshot.ric).unwrap(), 15)?;
            if db.cf_handle(&name).is_none() {
                db.create_cf(&name, &Options::default())?;
                db.create_cf(&format!("q:{name}"), &Options::default())?;
            }
            let key = snapshot.key()?;
            db.put_cf(
                &db.cf_handle(&name).unwrap(),
                key,
                encode_ll2_minute(&snapshot.value),
            )?;
            db.put_cf(
                &db.cf_handle(&format!("q:{name}")).unwrap(),
                key,
                snapshot.known.to_le_bytes(),
            )?;
            Ok(())
        };
        scanner
            .scan(
                &include_bytes!("../../tests/fixtures/raw_ll2_partial_refresh.csv")[..],
                true,
                None,
                &mut |mut message| {
                    message.ts = 1_767_312_000_000_000_000 + (message.order - 1) * 15_000_000_000;
                    extractor.apply(&message, &mut write)
                },
            )
            .unwrap();
        scanner.finish().unwrap();
        extractor.finish(&mut write).unwrap();
        drop(write);
        let ts = NaiveDate::from_ymd_opt(2026, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp();
        let source = Source {
            db,
            raw: true,
            windows: vec![(
                ts as u64 * 1_000_000_000,
                (ts as u64 + 86400) * 1_000_000_000,
                "done".into(),
            )],
        };
        let (last, known) = candidate(&source, "YAAH6", ts).unwrap().unwrap();
        assert_eq!(known, 0);
        assert_eq!(last.source_ts_utc_ns / 1_000_000_000, ts as u64 + 45);
        assert!(candidate(&source, "YAAH6", ts + 60).unwrap().is_none());
        assert!(candidate(&source, "YAAM6", ts).unwrap().is_none());
    }

    #[test]
    fn fills_existing_rows_preserving_trade_columns_and_missing_minutes() {
        let temp = tempfile::tempdir().unwrap();
        let raw_path = temp.path().join("raw");
        let mut options = Options::default();
        options.create_if_missing(true);
        let mut db = DB::open(&options, &raw_path).unwrap();
        let mut scanner = RawScanner::default();
        let mut extractor = Extractor::new(15).unwrap();
        let mut write = |s: RawSnapshot| -> Result<()> {
            assert_eq!(s.seconds, 15);
            let cf = raw_cf(2024, &parse_product(&s.ric).unwrap(), s.seconds)?;
            if db.cf_handle(&cf).is_none() {
                db.create_cf(&cf, &Options::default())?;
                db.create_cf(&format!("q:{cf}"), &Options::default())?;
            }
            let key = s.key()?;
            db.put_cf(
                &db.cf_handle(&cf).unwrap(),
                key,
                encode_ll2_minute(&s.value),
            )?;
            db.put_cf(
                &db.cf_handle(&format!("q:{cf}")).unwrap(),
                key,
                s.known.to_le_bytes(),
            )?;
            Ok(())
        };
        scanner
            .scan(
                &include_bytes!("../../tests/fixtures/raw_ll2_small.csv")[..],
                true,
                None,
                &mut |m| extractor.apply(&m, &mut write),
            )
            .unwrap();
        scanner.finish().unwrap();
        extractor.finish(&mut write).unwrap();
        drop(write);
        let ts = 1_704_153_600i64;
        let source = Source {
            db,
            windows: vec![(
                ts as u64 * 1_000_000_000,
                (ts as u64 + 86400) * 1_000_000_000,
                "done".into(),
            )],
            raw: true,
        };
        let input_root = temp.path().join("input");
        fs::create_dir_all(&input_root).unwrap();
        let input = input_root.join("fixture.parquet");
        let output_root = temp.path().join("output");
        let output = output_root.join("fixture.parquet");
        let mut series = vec![
            Series::new("ric".into(), vec!["ESH24"; 4]),
            Series::new("ts".into(), vec![ts, ts + 60, ts + 120, ts + 180]),
            Series::new("contract_id".into(), vec!["ES202403"; 4]),
        ];
        for level in 0..10 {
            for side in ["bid", "ask"] {
                for f in ["p", "v"] {
                    series.push(Series::new(
                        format!("{side}{level}{f}").into(),
                        vec![None::<f64>; 4],
                    ));
                }
            }
        }
        series.push(Series::new("mid_price".into(), vec![None::<f64>; 4]));
        for i in 0..40 {
            series.push(Series::new(format!("trade_{i}").into(), vec![i as f64; 4]));
        }
        let mut before = DataFrame::new(series.into_iter().map(Into::into).collect()).unwrap();
        ParquetWriter::new(File::create(&input).unwrap())
            .finish(&mut before)
            .unwrap();
        let args = Args {
            input_root,
            output_root,
            ll2_rocksdb: vec![raw_path],
            start: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            end: NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            products: vec!["ES".into()],
            dry_run: false,
            allow_sample: false,
            require_valid_l1: false,
            audit_snapshot_values: false,
            workers: 1,
        };
        let audit = patch_file(&input, &output, &[source], &args).unwrap();
        assert_eq!(audit["filled_rows"], 3);
        let after = ParquetReader::new(File::open(&output).unwrap())
            .finish()
            .unwrap();
        assert_eq!(after.shape(), (4, 84));
        assert_eq!(before.get_column_names(), after.get_column_names());
        for name in before.get_column_names() {
            if !name.starts_with("bid") && !name.starts_with("ask") && name.as_str() != "mid_price"
            {
                assert_eq!(before.column(name).unwrap(), after.column(name).unwrap());
            }
        }
        assert_eq!(
            after.column("bid0v").unwrap().f64().unwrap().get(0),
            Some(7.0)
        );
        assert_eq!(
            after.column("ask0v").unwrap().f64().unwrap().get(0),
            Some(9.0)
        );
        assert!(after
            .column("bid0p")
            .unwrap()
            .f64()
            .unwrap()
            .get(2)
            .is_none());
        assert!(after
            .column("bid1p")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .is_none());
        assert!(after
            .column("mid_price")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .is_some());
        assert!(after
            .column("mid_price")
            .unwrap()
            .f64()
            .unwrap()
            .get(3)
            .is_none());
        let original = ParquetReader::new(File::open(input).unwrap())
            .finish()
            .unwrap();
        assert!(original
            .column("bid0p")
            .unwrap()
            .f64()
            .unwrap()
            .get(0)
            .is_none());
    }
}
