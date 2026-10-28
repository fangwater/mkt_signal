//! Native 84/86-column CME bars; Python oracle: scripts/cme_aggregate_baseline.py.
use anyhow::{bail, Context, Result};
use clap::Parser;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

const SUM: &[&str] = &[
    "volume",
    "amount",
    "count",
    "buy_count",
    "sell_count",
    "buy_amount",
    "sell_amount",
    "buy_volume",
    "sell_volume",
    "large_order",
    "medium_order",
    "small_order",
    "large_buy",
    "large_sell",
    "medium_buy",
    "medium_sell",
    "small_buy",
    "small_sell",
    "special_count",
    "special_volume",
    "implied_count",
    "implied_volume",
    "implied_amount",
];
const DERIVED: &[&str] = &[
    "avg_amount",
    "net_buy_amount",
    "net_buy_volume",
    "net_buy_pct",
    "net_buy_large",
    "net_buy_medium",
    "net_buy_small",
];
const PRICE: &[&str] = &[
    "open",
    "high",
    "low",
    "close",
    "twap",
    "implied_twap",
    "vwap",
    "buy_vwap",
    "sell_vwap",
    "implied_vwap",
];
const FREQ: &[(&str, usize)] = &[("5min", 5), ("15min", 15), ("1h", 60), ("1day", 1440)];
#[derive(Parser)]
struct Args {
    #[arg(long)]
    jobs: PathBuf,
    #[arg(long)]
    out_root: PathBuf,
    #[arg(long, default_value_t = 32)]
    workers: usize,
    #[arg(long)]
    execute: bool,
}
#[derive(Deserialize, Serialize)]
struct Job {
    input: PathBuf,
    depth_audit: Option<PathBuf>,
    exchange: String,
    product: String,
    day: i64,
    hfq: bool,
}
#[derive(Deserialize)]
struct Donor {
    ric: String,
    ts: i64,
    source_ts_utc_ns: i64,
}
#[derive(Default, Deserialize, Serialize, Debug, PartialEq)]
struct Bars {
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
fn hash(path: &Path) -> Result<String> {
    let mut f = File::open(path)?;
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
fn finite(x: f64) -> Option<f64> {
    x.is_finite().then_some(x)
}
fn donors(path: Option<&Path>) -> Result<BTreeMap<(String, i64), i64>> {
    let mut out = BTreeMap::new();
    if let Some(p) = path {
        if p.exists() {
            let d = zstd::stream::read::Decoder::new(File::open(p)?)?;
            for row in csv::Reader::from_reader(d).deserialize::<Donor>() {
                let r = row?;
                if r.ts % 60 != 0
                    || r.source_ts_utc_ns < r.ts * 1_000_000_000
                    || r.source_ts_utc_ns >= (r.ts + 60) * 1_000_000_000
                    || out.insert((r.ric, r.ts), r.source_ts_utc_ns).is_some()
                {
                    bail!("invalid donor {}", p.display())
                }
            }
        }
    }
    Ok(out)
}
fn aggregate(
    df: &DataFrame,
    freq: &str,
    minutes: usize,
    day: i64,
    donor: &BTreeMap<(String, i64), i64>,
) -> Result<(DataFrame, Bars)> {
    let n = df.height();
    if n == 0 {
        bail!("empty source")
    }
    let cid = df.column("contract_id")?.str()?;
    let ric = df.column("ric")?.str()?;
    let ts = df.column("ts")?.i64()?;
    let extras = df.column("segment_id").is_ok();
    if df.width() != if extras { 86 } else { 84 } {
        bail!("invalid source width")
    }
    let seg = if extras {
        Some(df.column("segment_id")?.i64()?)
    } else {
        None
    };
    let off = if extras {
        Some(df.column("offset_e9")?.i64()?)
    } else {
        None
    };
    let mut identities = BTreeMap::new();
    for i in 0..n {
        let t = ts.get(i).context("null ts")?;
        if t % 60 != 0 {
            bail!("non-minute ts")
        }
        let key = (
            cid.get(i).context("null contract")?.to_string(),
            ric.get(i).context("null RIC")?.to_string(),
            seg.map(|s| s.get(i).context("null segment")).transpose()?,
            off.map(|s| s.get(i).context("null offset")).transpose()?,
        );
        identities.entry(key).or_insert_with(Vec::new).push(i);
    }
    let mut groups = Vec::new();
    for (_, mut rows) in identities {
        rows.sort_by_key(|i| ts.get(*i).unwrap());
        if rows.windows(2).any(|w| ts.get(w[0]) >= ts.get(w[1])) {
            bail!("duplicate contract minute")
        }
        if freq == "1day" {
            groups.push((rows, true));
            continue;
        }
        let mut start = 0;
        for end in 1..=rows.len() {
            if end == rows.len()
                || ts.get(rows[end]).unwrap() - ts.get(rows[end - 1]).unwrap() != 60
            {
                for i in (start..end).step_by(minutes) {
                    groups.push((rows[i..(i + minutes).min(end)].to_vec(), i == start));
                }
                start = end;
            }
        }
    }
    groups.sort_by_key(|(r, _)| {
        (
            ts.get(r[0]).unwrap(),
            cid.get(r[0]).unwrap(),
            ric.get(r[0]).unwrap(),
        )
    });
    let float_cols: BTreeMap<&str, &Float64Chunked> = df
        .get_columns()
        .iter()
        .filter(|s| s.dtype() == &DataType::Float64)
        .map(|s| Ok((s.name().as_str(), s.f64()?)))
        .collect::<Result<_>>()?;
    let mut output: BTreeMap<&str, Vec<Option<f64>>> = float_cols
        .keys()
        .map(|k| (*k, Vec::with_capacity(groups.len())))
        .collect();
    let mut bars = Bars {
        frequency: freq.into(),
        bar_minutes: minutes,
        trading_day: day,
        ..Default::default()
    };
    let mut extra_seg = Vec::new();
    let mut extra_off = Vec::new();
    for (rows, is_start) in groups {
        let first = rows[0];
        let last = *rows.last().unwrap();
        let get = |name: &str, i: usize| float_cols[name].get(i).filter(|v| v.is_finite());
        let sum = |name: &str| -> Option<f64> {
            let mut acc = get(name, rows[0])?;
            for &i in &rows[1..] {
                acc += get(name, i)?;
            }
            finite(acc)
        };
        let mut r: BTreeMap<&str, Option<f64>> = float_cols
            .keys()
            .map(|name| (*name, get(name, last)))
            .collect();
        for &name in SUM {
            r.insert(name, sum(name));
        }
        let count = r["count"].context("unknown trade count")?;
        r.insert(
            "avg_amount",
            if count > 0. {
                r["amount"].map(|a| a / count)
            } else {
                Some(0.)
            },
        );
        for (target, buy, sell) in [
            ("net_buy_amount", "buy_amount", "sell_amount"),
            ("net_buy_volume", "buy_volume", "sell_volume"),
            ("net_buy_large", "large_buy", "large_sell"),
            ("net_buy_medium", "medium_buy", "medium_sell"),
            ("net_buy_small", "small_buy", "small_sell"),
        ] {
            r.insert(target, r[buy].zip(r[sell]).map(|(a, b)| a - b));
        }
        r.insert(
            "net_buy_pct",
            r["buy_amount"].zip(r["sell_amount"]).map(|(a, b)| {
                if a + b > 0. {
                    (a - b) / (a + b)
                } else {
                    0.
                }
            }),
        );
        let active: Vec<_> = rows
            .iter()
            .copied()
            .filter(|i| get("count", *i).is_some_and(|x| x > 0.))
            .collect();
        if !active.is_empty() {
            r.insert("open", get("open", active[0]));
            r.insert("close", get("close", *active.last().unwrap()));
            for (name, high) in [("high", true), ("low", false)] {
                let v = active.iter().try_fold(
                    if high {
                        f64::NEG_INFINITY
                    } else {
                        f64::INFINITY
                    },
                    |a, i| get(name, *i).map(|b| if high { a.max(b) } else { a.min(b) }),
                );
                r.insert(name, v);
            }
        }
        for (name, weight) in [
            ("vwap", "volume"),
            ("buy_vwap", "buy_volume"),
            ("sell_vwap", "sell_volume"),
            ("implied_vwap", "implied_volume"),
        ] {
            if rows.iter().any(|i| get(weight, *i).is_none_or(|v| v < 0.)) {
                bail!("unknown or negative volume")
            }
            let total = r[weight].context("missing total volume")?;
            if total > 0. {
                let mut numerator = Some(0.);
                for &i in &rows {
                    let w = get(weight, i).unwrap();
                    let term = if w == 0. {
                        Some(0.)
                    } else {
                        get(name, i).map(|p| p * w)
                    };
                    numerator = numerator.zip(term).and_then(|(a, b)| finite(a + b));
                }
                r.insert(name, numerator.map(|x| x / total));
            }
        }
        for name in ["twap", "implied_twap"] {
            r.insert(name, sum(name).map(|x| x / rows.len() as f64));
        }
        let source = donor
            .get(&(ric.get(last).unwrap().into(), ts.get(last).unwrap()))
            .copied();
        let present = r
            .iter()
            .any(|(name, v)| (name.starts_with("bid") || name.starts_with("ask")) && v.is_some());
        if present != source.is_some() {
            bail!(
                "book/source mismatch {} {}",
                ric.get(last).unwrap(),
                ts.get(last).unwrap()
            )
        }
        for (name, values) in &mut output {
            values.push(r[name].and_then(finite));
        }
        bars.ts.push(ts.get(first).unwrap());
        bars.end_ts.push(ts.get(last).unwrap() + 60);
        bars.last_minute_ts.push(ts.get(last).unwrap());
        bars.source_minutes.push(rows.len());
        bars.segment_start.push(is_start);
        bars.source_depth_ts_utc_ns.push(source);
        bars.contract_id.push(cid.get(last).unwrap().into());
        bars.ric.push(ric.get(last).unwrap().into());
        if let (Some(s), Some(o)) = (seg, off) {
            extra_seg.push(s.get(last));
            extra_off.push(o.get(last));
        }
    }
    let mut columns = Vec::new();
    for s in df.get_columns() {
        let name = s.name().as_str();
        let out = match name {
            "contract_id" => Series::new(s.name().clone(), &bars.contract_id),
            "ric" => Series::new(s.name().clone(), &bars.ric),
            "ts" => Series::new(s.name().clone(), &bars.ts),
            "segment_id" => Series::new(s.name().clone(), &extra_seg),
            "offset_e9" => Series::new(s.name().clone(), &extra_off),
            _ => Series::new(
                s.name().clone(),
                output.remove(name).context("unknown source column")?,
            ),
        };
        columns.push(out);
    }
    // Check that every documented numeric field was actually present.
    for name in SUM.iter().chain(DERIVED).chain(PRICE) {
        df.column(name)?;
    }
    Ok((DataFrame::new(columns)?, bars))
}
fn run(job: &Job, root: &Path) -> Result<serde_json::Value> {
    let df = ParquetReader::new(File::open(&job.input)?)
        .set_low_memory(true)
        .finish()?;
    let donor = donors(job.depth_audit.as_deref())?;
    let mut outputs = Vec::new();
    for &(freq, minutes) in FREQ {
        let family = format!("baseline_data_{freq}{}", if job.hfq { "_hfq" } else { "" });
        let dest = root
            .join(&family)
            .join(&job.exchange)
            .join(&job.product)
            .join(format!("{}.parquet", job.day));
        let meta = root
            .join(&family)
            .join("_bars")
            .join(&job.exchange)
            .join(&job.product)
            .join(format!("{}.json.zst", job.day));
        if dest.exists() || meta.exists() {
            bail!("output exists {}", dest.display())
        }
        let (mut out, bars) = aggregate(&df, freq, minutes, job.day, &donor)?;
        fs::create_dir_all(dest.parent().unwrap())?;
        fs::create_dir_all(meta.parent().unwrap())?;
        let temp = dest.with_extension("parquet.tmp");
        ParquetWriter::new(File::create(&temp)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut out)?;
        fs::rename(temp, &dest)?;
        let temp = meta.with_extension("zst.tmp");
        fs::write(
            &temp,
            zstd::stream::encode_all(serde_json::to_vec(&bars)?.as_slice(), 1)?,
        )?;
        fs::rename(temp, &meta)?;
        outputs.push(serde_json::json!({"path":dest,"rows":out.height(),"sha256":hash(&dest)?,"metadata":meta,"metadata_sha256":hash(&meta)?}));
    }
    Ok(
        serde_json::json!({"job":job,"input_rows":df.height(),"input_sha256":hash(&job.input)?,"outputs":outputs}),
    )
}
fn main() -> Result<()> {
    let a = Args::parse();
    if a.workers == 0 {
        bail!("workers must be positive")
    }
    let jobs: Vec<Job> = serde_json::from_reader(File::open(&a.jobs)?)?;
    if !a.execute {
        println!("jobs={} output={}", jobs.len(), a.out_root.display());
        return Ok(());
    }
    fs::create_dir_all(&a.out_root)?;
    // OS workers avoid reentrant work stealing while Polars waits on its own pool.
    let next = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);
    let results = std::sync::Mutex::new(Vec::with_capacity(jobs.len()));
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
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
                            let r = run(&jobs[i], &a.out_root)
                                .with_context(|| format!("{}", jobs[i].input.display()))?;
                            results.lock().unwrap().push(r);
                            let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                            if n % 500 == 0 {
                                eprintln!("completed {n}/{}", jobs.len())
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
    let result = results.into_inner().unwrap();
    fs::write(
        a.out_root.join("aggregation_manifest.json"),
        serde_json::to_vec(&result)?,
    )?;
    println!("completed {} source files", jobs.len());
    Ok(())
}
