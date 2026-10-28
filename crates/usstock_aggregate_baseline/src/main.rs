//! RTH aggregation; independent oracle: scripts/usstock_aggregate_baseline.py.
use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

const SUM: &[&str] = &[
    "volume",
    "amount",
    "count",
    "buy_volume",
    "buy_amount",
    "buy_count",
    "sell_volume",
    "sell_amount",
    "sell_count",
    "unknown_volume",
    "unknown_amount",
    "unknown_count",
    "off_exchange_volume",
    "off_exchange_amount",
    "off_exchange_count",
    "large_order",
    "medium_order",
    "small_order",
    "large_buy",
    "large_sell",
    "medium_buy",
    "medium_sell",
    "small_buy",
    "small_sell",
    "n_seconds",
];

#[derive(Parser)]
struct Args {
    #[arg(long)]
    jobs: PathBuf,
    #[arg(long)]
    out_root: PathBuf,
    #[arg(long, default_value_t = 24)]
    workers: usize,
    #[arg(long)]
    execute: bool,
}
#[derive(Clone, Deserialize, Serialize)]
struct Job {
    input: PathBuf,
    venue: String,
    ric: String,
    day: String,
    hfq: bool,
    open_ts: i64,
    close_ts: i64,
    source_bytes: u64,
    source_mtime_ns: u64,
}
#[derive(Serialize)]
struct Bar {
    ts: i64,
    end_ts: i64,
    last_minute_ts: i64,
    source_minutes: usize,
    segment_start: bool,
    source_depth_ts_utc_ns: Option<i64>,
    source_quality_flags_any: i8,
}
#[derive(Serialize)]
struct Bars {
    frequency: String,
    session_date: String,
    bars: Vec<Bar>,
}

fn hash(p: &Path) -> Result<String> {
    let mut f = File::open(p)?;
    let mut h = Sha256::new();
    let mut b = vec![0; 1024 * 1024];
    loop {
        let n = f.read(&mut b)?;
        if n == 0 {
            break;
        }
        h.update(&b[..n]);
    }
    Ok(format!("{:x}", h.finalize()))
}
fn finite(v: f64) -> Option<f64> {
    v.is_finite().then_some(v)
}
fn aggregate(df: &DataFrame, freq: &str, date: &str) -> Result<(DataFrame, Bars)> {
    let n = df.height();
    ensure!(n > 0, "empty minute input");
    let ts = df.column("ts")?.i64()?;
    ensure!(
        ts.into_iter().all(|x| x.is_some_and(|t| t % 60 == 0)),
        "invalid minute key"
    );
    ensure!(
        (1..n).all(|i| ts.get(i) > ts.get(i - 1)),
        "non-increasing minute keys"
    );
    for name in ["ric", "venue"] {
        let x = df.column(name)?.str()?;
        ensure!(
            x.get(0).is_some() && x.into_iter().all(|v| v == x.get(0)),
            "mixed identity"
        );
    }
    ensure!(
        df.column("n_seconds")?
            .i64()?
            .into_iter()
            .all(|v| v == Some(60)),
        "not 1min source"
    );
    let float_cols: BTreeMap<&str, &Float64Chunked> = df
        .get_columns()
        .iter()
        .filter(|c| c.dtype() == &DataType::Float64)
        .map(|c| Ok((c.name().as_str(), c.f64()?)))
        .collect::<Result<_>>()?;
    let mut floats: BTreeMap<&str, Vec<Option<f64>>> =
        float_cols.keys().map(|k| (*k, vec![])).collect();
    let int_cols: BTreeMap<&str, Int64Chunked> = df
        .get_columns()
        .iter()
        .filter(|c| {
            matches!(
                c.dtype(),
                DataType::Int8 | DataType::Int32 | DataType::Int64
            )
        })
        .map(|c| Ok((c.name().as_str(), c.cast(&DataType::Int64)?.i64()?.clone())))
        .collect::<Result<_>>()?;
    let mut ints: BTreeMap<&str, Vec<Option<i64>>> =
        int_cols.keys().map(|k| (*k, vec![])).collect();
    let sums: BTreeSet<_> = SUM.iter().copied().collect();
    let mut groups = vec![];
    if freq == "1day" {
        groups.push((0, n, true));
    } else {
        let mut start = 0;
        for end in 1..=n {
            if end == n || ts.get(end).unwrap() - ts.get(end - 1).unwrap() != 60 {
                for i in (start..end).step_by(60) {
                    groups.push((i, (i + 60).min(end), i == start));
                }
                start = end;
            }
        }
    }
    let mut metadata = Bars {
        frequency: freq.into(),
        session_date: date.into(),
        bars: vec![],
    };
    for (start, end, segment_start) in groups {
        let last = end - 1;
        let get = |k: &str, i: usize| float_cols.get(k).and_then(|v| v.get(i)).and_then(finite);
        let count = |k: &str, i: usize| -> Result<i64> {
            int_cols
                .get(k)
                .context("missing count")?
                .get(i)
                .context("null count")
        };
        let total = |k: &str| -> Option<f64> {
            let mut sum = 0.;
            for i in start..end {
                sum += get(k, i)?;
            }
            finite(sum)
        };
        let mut row: BTreeMap<&str, Option<f64>> =
            float_cols.keys().map(|k| (*k, get(k, last))).collect();
        for &k in float_cols.keys() {
            if sums.contains(k) {
                row.insert(k, total(k));
            }
        }
        let active = (start..end)
            .filter_map(|i| (count("count", i).ok()? > 0).then_some(i))
            .collect::<Vec<_>>();
        if !active.is_empty() {
            row.insert("open", get("open", active[0]));
            row.insert("close", get("close", *active.last().unwrap()));
            for (k, high) in [("high", true), ("low", false)] {
                let v = active.iter().try_fold(
                    if high {
                        f64::NEG_INFINITY
                    } else {
                        f64::INFINITY
                    },
                    |a, &i| get(k, i).map(|b| if high { a.max(b) } else { a.min(b) }),
                );
                row.insert(k, v);
            }
        }
        for (k, c, high) in [
            ("buy_high", "buy_count", true),
            ("sell_low", "sell_count", false),
        ] {
            if !float_cols.contains_key(k) {
                continue;
            }
            let active = (start..end)
                .filter_map(|i| (count(c, i).ok()? > 0).then_some(i))
                .collect::<Vec<_>>();
            if !active.is_empty() {
                let v = active.iter().try_fold(
                    if high {
                        f64::NEG_INFINITY
                    } else {
                        f64::INFINITY
                    },
                    |a, &i| get(k, i).map(|b| if high { a.max(b) } else { a.min(b) }),
                );
                row.insert(k, v);
            }
        }
        for (k, w) in [
            ("vwap", "volume"),
            ("buy_vwap", "buy_volume"),
            ("sell_vwap", "sell_volume"),
        ] {
            if !float_cols.contains_key(k) {
                continue;
            }
            ensure!(
                (start..end).all(|i| get(w, i).is_some_and(|v| v >= 0.)),
                "invalid volume"
            );
            let weight = row[w].context("missing total volume")?;
            if weight > 0. {
                let numerator = (start..end).try_fold(0., |acc, i| {
                    let weight = get(w, i)?;
                    if weight == 0. {
                        Some(acc)
                    } else {
                        get(k, i).and_then(|p| finite(acc + p * weight))
                    }
                });
                row.insert(k, numerator.map(|v| v / weight));
            }
        }
        if float_cols.contains_key("twap") {
            row.insert("twap", total("twap").map(|v| v / (end - start) as f64));
        }
        if float_cols.contains_key("avg_amount") {
            let ct = (start..end).try_fold(0_i64, |acc, i| -> Result<i64> {
                acc.checked_add(count("count", i)?)
                    .context("count overflow")
            })?;
            row.insert(
                "avg_amount",
                if ct > 0 {
                    row["amount"].map(|a| a / ct as f64)
                } else {
                    Some(0.)
                },
            );
        }
        for (k, b, s) in [
            ("net_buy_amount", "buy_amount", "sell_amount"),
            ("net_buy_volume", "buy_volume", "sell_volume"),
            ("net_buy_large", "large_buy", "large_sell"),
            ("net_buy_medium", "medium_buy", "medium_sell"),
            ("net_buy_small", "small_buy", "small_sell"),
        ] {
            if float_cols.contains_key(k) {
                row.insert(k, row[b].zip(row[s]).map(|(a, b)| a - b));
            }
        }
        if float_cols.contains_key("net_buy_pct") {
            row.insert(
                "net_buy_pct",
                row["buy_amount"].zip(row["sell_amount"]).map(|(b, s)| {
                    if b + s > 0. {
                        (b - s) / (b + s)
                    } else {
                        0.
                    }
                }),
            );
        }
        for (k, v) in &mut floats {
            v.push(row[k].and_then(finite));
        }
        for (k, v) in &mut ints {
            let value = if *k == "ts" {
                Some(ts.get(start).unwrap())
            } else if sums.contains(k) {
                let mut acc = 0_i64;
                for i in start..end {
                    acc = acc
                        .checked_add(int_cols[k].get(i).context("null integer sum")?)
                        .context("integer overflow")?;
                }
                Some(acc)
            } else {
                int_cols[k].get(last)
            };
            v.push(value);
        }
        let mut flags = 0_i8;
        if let Some(q) = int_cols.get("source_quality_flags") {
            for i in start..end {
                flags |= q.get(i).context("null quality")? as i8;
            }
        }
        metadata.bars.push(Bar {
            ts: ts.get(start).unwrap(),
            end_ts: ts.get(last).unwrap() + 60,
            last_minute_ts: ts.get(last).unwrap(),
            source_minutes: end - start,
            segment_start,
            source_depth_ts_utc_ns: int_cols
                .get("source_depth_ts_utc_ns")
                .and_then(|v| v.get(last)),
            source_quality_flags_any: flags,
        });
    }
    let len = metadata.bars.len();
    let mut cols = vec![];
    for source in df.get_columns() {
        let name = source.name().as_str();
        let series = if let Some(v) = floats.remove(name) {
            Series::new(source.name().clone(), v)
        } else if let Some(v) = ints.remove(name) {
            Series::new(source.name().clone(), v).cast(source.dtype())?
        } else if source.dtype() == &DataType::String {
            Series::new(
                source.name().clone(),
                vec![source.str()?.get(0).context("null identity")?; len],
            )
        } else {
            bail!("unsupported field {name} {:?}", source.dtype())
        };
        cols.push(series);
    }
    Ok((DataFrame::new(cols)?, metadata))
}
fn run(job: &Job, root: &Path) -> Result<serde_json::Value> {
    let before = fs::metadata(&job.input)?;
    ensure!(
        before.len() == job.source_bytes
            && before
                .modified()?
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
                == u128::from(job.source_mtime_ns),
        "source changed before read"
    );
    let df = ParquetReader::new(File::open(&job.input)?)
        .set_low_memory(true)
        .finish()?;
    let ts = df.column("ts")?.i64()?;
    ensure!(
        ts.into_iter()
            .all(|t| t.is_some_and(|t| job.open_ts <= t && t < job.close_ts)),
        "outside official RTH"
    );
    for (name, expected) in [("ric", &job.ric), ("venue", &job.venue)] {
        ensure!(
            df.column(name)?
                .str()?
                .into_iter()
                .all(|v| v == Some(expected.as_str())),
            "job identity"
        );
    }
    let input_hash = hash(&job.input)?;
    let after = fs::metadata(&job.input)?;
    ensure!(
        before.len() == after.len() && before.modified()? == after.modified()?,
        "source changed during read"
    );
    let mut outputs = vec![];
    for frequency in ["1h", "1day"] {
        let family = format!(
            "baseline_data_{frequency}{}_raw",
            if job.hfq { "_hfq" } else { "" }
        );
        let dest = root
            .join(&family)
            .join(&job.venue)
            .join(&job.ric)
            .join(format!("{}.parquet", job.day));
        let meta = root
            .join(&family)
            .join("_bars")
            .join(&job.venue)
            .join(&job.ric)
            .join(format!("{}.json.zst", job.day));
        ensure!(
            !dest.exists() && !meta.exists(),
            "output exists {}",
            dest.display()
        );
        let (mut table, bars) = aggregate(&df, frequency, &job.day)?;
        fs::create_dir_all(dest.parent().unwrap())?;
        fs::create_dir_all(meta.parent().unwrap())?;
        let tmp = dest.with_extension("parquet.tmp");
        let mut writer = BufWriter::with_capacity(1024 * 1024, File::create(&tmp)?);
        ParquetWriter::new(&mut writer)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut table)?;
        writer.flush()?;
        drop(writer);
        fs::rename(&tmp, &dest)?;
        let tmp = meta.with_extension("zst.tmp");
        fs::write(
            &tmp,
            zstd::stream::encode_all(serde_json::to_vec(&bars)?.as_slice(), 1)?,
        )?;
        fs::rename(tmp, &meta)?;
        outputs.push(json!({"path":dest,"rows":table.height(),"sha256":hash(&dest)?,"metadata":meta,"metadata_sha256":hash(&meta)?}));
    }
    Ok(json!({"job":job,"input_sha256":input_hash,"input_rows":df.height(),"outputs":outputs}))
}
fn main() -> Result<()> {
    let a = Args::parse();
    ensure!(a.workers > 0, "workers must be positive");
    let jobs: Vec<Job> = serde_json::from_reader(File::open(&a.jobs)?)?;
    if !a.execute {
        println!("jobs={} output={}", jobs.len(), a.out_root.display());
        return Ok(());
    }
    fs::create_dir_all(&a.out_root)?;
    let next = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    let results = Mutex::new(vec![]);
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = vec![];
        for _ in 0..a.workers {
            handles.push(
                std::thread::Builder::new()
                    .stack_size(32 * 1024 * 1024)
                    .spawn_scoped(scope, || -> Result<()> {
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            if i >= jobs.len() {
                                break;
                            }
                            let result = run(&jobs[i], &a.out_root)
                                .with_context(|| format!("{}", jobs[i].input.display()))?;
                            results.lock().unwrap().push(result);
                            let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                            if n % 1000 == 0 {
                                eprintln!("completed={n}/{}", jobs.len());
                            }
                        }
                        Ok(())
                    })?,
            );
        }
        for h in handles {
            h.join().map_err(|_| anyhow::anyhow!("worker panicked"))??;
        }
        Ok(())
    })?;
    let audit = a.out_root.join("_audit");
    fs::create_dir_all(&audit)?;
    let mut results = results.into_inner().unwrap();
    results.sort_by_key(|v| v["job"]["input"].as_str().unwrap().to_owned());
    fs::write(
        audit.join("data_jobs.json.zst"),
        zstd::stream::encode_all(serde_json::to_vec(&results)?.as_slice(), 1)?,
    )?;
    println!("completed {} source files", jobs.len());
    Ok(())
}
