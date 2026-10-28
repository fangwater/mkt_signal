use anyhow::{ensure, Context, Result};
use clap::Parser;
use polars::prelude::*;
use rayon::prelude::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use usstock_nasdaq_baseline::{extract, Day, Extract, Job, Minute, NS};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    jobs: PathBuf,
    #[arg(long)]
    output_root: PathBuf,
    #[arg(long, default_value_t = 12)]
    workers: usize,
    #[arg(long)]
    resume: bool,
    /// Explicit compatible builds whose daily output/cache hashes must still match.
    #[arg(long)]
    resume_build_id: Vec<String>,
    /// Optional integer minute/trade observations, for Python/Rust comparison only.
    #[arg(long)]
    reference_output: Option<PathBuf>,
}

fn hash(path: &Path) -> Result<String> {
    let mut f = File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = vec![0; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(hex::encode(h.finalize()))
}

fn json_atomic(path: &Path, value: &Value) -> Result<()> {
    fs::create_dir_all(path.parent().context("missing parent")?)?;
    let tmp = path.with_extension("json.tmp");
    let mut f = File::create(&tmp)?;
    serde_json::to_writer_pretty(&mut f, value)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn paths(root: &Path, ric: &str, day: &Day) -> (PathBuf, PathBuf, PathBuf) {
    let date = day.day.replace('-', "");
    (
        root.join("NASDAQ")
            .join(ric)
            .join(format!("{date}.parquet")),
        root.join("_audit/days/NASDAQ")
            .join(ric)
            .join(format!("{date}.json")),
        root.join("_work/notionals/NASDAQ")
            .join(ric)
            .join(format!("{date}.i128.zst")),
    )
}

fn quantile100(sorted: &[i128], pct: usize) -> Option<i128> {
    if sorted.is_empty() {
        return None;
    }
    let scaled = (sorted.len() - 1) * pct;
    let lo = scaled / 100;
    let rem = scaled % 100;
    Some(sorted[lo] * (100 - rem) as i128 + sorted[(lo + 1).min(sorted.len() - 1)] * rem as i128)
}

fn previous_month(month: &str) -> Result<String> {
    let y: i32 = month[..4].parse()?;
    let m: u32 = month[5..7].parse()?;
    Ok(if m == 1 {
        format!("{}-12", y - 1)
    } else {
        format!("{y:04}-{:02}", m - 1)
    })
}

fn write_notionals(path: &Path, minutes: &[Minute]) -> Result<u64> {
    fs::create_dir_all(path.parent().unwrap())?;
    let tmp = path.with_extension("tmp");
    let mut writer = zstd::stream::write::Encoder::new(File::create(&tmp)?, 1)?;
    let mut count = 0;
    for m in minutes {
        for t in &m.trades {
            writer.write_all(&(i128::from(t.1) * i128::from(t.2)).to_le_bytes())?;
            count += 1;
        }
    }
    writer.finish()?.sync_all()?;
    fs::rename(tmp, path)?;
    Ok(count)
}

fn read_notionals(path: &Path, output: &mut Vec<i128>) -> Result<u64> {
    let mut r = zstd::stream::read::Decoder::new(File::open(path)?)?;
    let mut count = 0;
    let mut b = [0; 16];
    loop {
        let mut first = [0];
        if r.read(&mut first)? == 0 {
            break;
        }
        b[0] = first[0];
        r.read_exact(&mut b[1..])?;
        let n = i128::from_le_bytes(b);
        ensure!(n > 0, "invalid cached notional");
        output.push(n);
        count += 1;
    }
    Ok(count)
}

fn frame(ric: &str, x: &Extract, thresholds: Option<(i128, i128)>) -> Result<DataFrame> {
    let n = x.minutes.len();
    let mut columns = vec![
        Series::new("ric".into(), vec![ric; n]),
        Series::new("venue".into(), vec!["NASDAQ"; n]),
        Series::new(
            "ts".into(),
            x.minutes.iter().map(|m| m.ts).collect::<Vec<_>>(),
        ),
        Series::new(
            "book_depth".into(),
            x.minutes.iter().map(|m| m.book_depth).collect::<Vec<_>>(),
        ),
        Series::new(
            "source_depth_ts_utc_ns".into(),
            x.minutes
                .iter()
                .map(|m| m.source_depth_ts_utc_ns.map(|t| t as i64))
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "source_quality_flags".into(),
            x.minutes
                .iter()
                .map(|m| m.source_quality_flags)
                .collect::<Vec<_>>(),
        ),
    ];
    let mut floats = BTreeMap::<String, Vec<Option<f64>>>::new();
    let mut counts = BTreeMap::<String, Vec<i64>>::new();
    let names = [
        "volume",
        "amount",
        "buy_volume",
        "buy_amount",
        "buy_high",
        "sell_volume",
        "sell_amount",
        "sell_low",
        "unknown_volume",
        "unknown_amount",
        "off_exchange_volume",
        "off_exchange_amount",
        "open",
        "high",
        "low",
        "close",
        "vwap",
        "avg_amount",
        "buy_vwap",
        "sell_vwap",
        "twap",
        "midp",
        "mid_price",
        "net_buy_amount",
        "net_buy_volume",
        "net_buy_pct",
        "large_order",
        "medium_order",
        "small_order",
        "large_buy",
        "large_sell",
        "medium_buy",
        "medium_sell",
        "small_buy",
        "small_sell",
        "net_buy_large",
        "net_buy_medium",
        "net_buy_small",
        "bid0p",
        "bid0v",
        "ask0p",
        "ask0v",
    ];
    for name in names {
        floats.insert(name.to_string(), Vec::with_capacity(n));
    }
    for name in [
        "count",
        "buy_count",
        "sell_count",
        "unknown_count",
        "off_exchange_count",
        "n_seconds",
    ] {
        counts.insert(name.into(), Vec::with_capacity(n));
    }
    for level in 0..10 {
        for side in ["bid", "ask"] {
            for pv in ["p", "v"] {
                floats.insert(format!("l2_{side}{level}{pv}"), Vec::with_capacity(n));
            }
        }
    }
    let mut previous_print = None;
    let mut prior_vwap = None;
    let mut prior_buy_vwap = None;
    let mut prior_sell_vwap = None;
    for m in &x.minutes {
        let mut vol = [0_u64; 4];
        let mut amount = [0_i128; 4];
        let mut count = [0_i64; 4];
        let mut buckets = [[0_i128; 3]; 3];
        let mut open = None;
        let mut high = None::<i64>;
        let mut low = None::<i64>;
        let mut close = None;
        let mut buy_high = None::<i64>;
        let mut sell_low = None::<i64>;
        let start = m.ts as u64 * NS;
        let end = start + 60 * NS;
        let mut cursor = start;
        let mut weighted = 0_i128;
        for t in &m.trades {
            ensure!(t.0 >= start && t.0 < end, "trade outside minute");
            let a = i128::from(t.1) * i128::from(t.2);
            let side = match t.3.as_str() {
                "B" => 1,
                "A" => 2,
                "N" => 3,
                _ => anyhow::bail!("invalid trade side"),
            };
            for s in [0, side] {
                vol[s] = vol[s].checked_add(t.2).context("volume overflow")?;
                amount[s] += a;
                count[s] += 1;
            }
            if open.is_none() {
                open = Some(t.1);
            }
            high = Some(high.map_or(t.1, |p| p.max(t.1)));
            low = Some(low.map_or(t.1, |p| p.min(t.1)));
            close = Some(t.1);
            if side == 1 {
                buy_high = Some(buy_high.map_or(t.1, |p| p.max(t.1)));
            }
            if side == 2 {
                sell_low = Some(sell_low.map_or(t.1, |p| p.min(t.1)));
            }
            if let Some((p50, p90)) = thresholds {
                let b = if a * 100 >= p90 {
                    0
                } else if a * 100 >= p50 {
                    1
                } else {
                    2
                };
                buckets[b][0] += a;
                if side < 3 {
                    buckets[b][side] += a;
                }
            }
            if let Some(p) = previous_print {
                weighted += i128::from(p) * i128::from(t.0 - cursor);
            }
            cursor = t.0;
            previous_print = Some(t.1);
        }
        if let Some(p) = previous_print {
            weighted += i128::from(p) * i128::from(end - cursor);
        }
        ensure!(
            vol[0] == vol[1] + vol[2] + vol[3] && amount[0] == amount[1] + amount[2] + amount[3],
            "trade conservation"
        );
        if thresholds.is_some() {
            ensure!(
                buckets.iter().map(|b| b[0]).sum::<i128>() == amount[0],
                "bucket conservation"
            );
        }
        let px = |p: Option<i64>| p.map(|p| p as f64 / 1e9);
        let mut row = BTreeMap::<String, Option<f64>>::new();
        for (s, prefix) in [(0, ""), (1, "buy_"), (2, "sell_"), (3, "unknown_")] {
            row.insert(format!("{prefix}volume"), Some(vol[s] as f64));
            row.insert(format!("{prefix}amount"), Some(amount[s] as f64 / 1e9));
            counts
                .get_mut(&format!("{prefix}count"))
                .unwrap()
                .push(count[s]);
        }
        row.insert("off_exchange_volume".into(), Some(0.0));
        row.insert("off_exchange_amount".into(), Some(0.0));
        counts.get_mut("off_exchange_count").unwrap().push(0);
        counts.get_mut("n_seconds").unwrap().push(60);
        for (name, v) in [
            ("open", open),
            ("high", high),
            ("low", low),
            ("close", close),
            ("buy_high", buy_high),
            ("sell_low", sell_low),
        ] {
            row.insert(name.into(), px(v));
        }
        for (s, name, prior) in [
            (0, "vwap", &mut prior_vwap),
            (1, "buy_vwap", &mut prior_buy_vwap),
            (2, "sell_vwap", &mut prior_sell_vwap),
        ] {
            if vol[s] > 0 {
                *prior = Some(amount[s] as f64 / 1e9 / vol[s] as f64);
            }
            row.insert(name.into(), prior.or_else(|| px(previous_print)));
        }
        row.insert(
            "avg_amount".into(),
            Some(if count[0] > 0 {
                amount[0] as f64 / 1e9 / count[0] as f64
            } else {
                0.0
            }),
        );
        row.insert(
            "twap".into(),
            if m.trade_time_uncertain {
                None
            } else {
                previous_print.map(|_| weighted as f64 / (60.0 * 1e18))
            },
        );
        row.insert(
            "net_buy_amount".into(),
            Some((amount[1] - amount[2]) as f64 / 1e9),
        );
        row.insert("net_buy_volume".into(), Some(vol[1] as f64 - vol[2] as f64));
        row.insert(
            "net_buy_pct".into(),
            Some(if amount[1] + amount[2] > 0 {
                (amount[1] - amount[2]) as f64 / (amount[1] + amount[2]) as f64
            } else {
                0.0
            }),
        );
        let mid = match (m.bid.first(), m.ask.first()) {
            (Some(b), Some(a)) => Some((b.0 as f64 + a.0 as f64) / 2e9),
            _ => None,
        };
        row.insert("midp".into(), mid);
        row.insert("mid_price".into(), mid);
        for (b, prefix) in ["large", "medium", "small"].iter().enumerate() {
            for (s, suffix) in ["order", "buy", "sell"].iter().enumerate() {
                row.insert(
                    format!("{prefix}_{suffix}"),
                    Some(buckets[b][s] as f64 / 1e9),
                );
            }
            row.insert(
                format!("net_buy_{prefix}"),
                Some((buckets[b][1] - buckets[b][2]) as f64 / 1e9),
            );
        }
        for level in 0..10 {
            for (name, levels) in [("bid", &m.bid), ("ask", &m.ask)] {
                let p = levels.get(level).map(|x| x.0 as f64 / 1e9);
                let v = levels.get(level).map(|x| x.1 as f64);
                row.insert(format!("l2_{name}{level}p"), p);
                row.insert(format!("l2_{name}{level}v"), v);
                if level == 0 {
                    row.insert(format!("{name}0p"), p);
                    row.insert(format!("{name}0v"), v);
                }
            }
        }
        ensure!(row.len() == floats.len(), "minute column set mismatch");
        for (name, value) in row {
            floats
                .get_mut(&name)
                .context("unexpected field")?
                .push(value);
        }
    }
    for (name, values) in floats {
        columns.push(Series::new(name.into(), values));
    }
    for (name, values) in counts {
        columns.push(Series::new(name.into(), values));
    }
    Ok(DataFrame::new(columns)?)
}

fn run_job(job: &Job, args: &Args, build_id: &str, progress: &AtomicUsize) -> Result<()> {
    ensure!(
        !job.days.is_empty() && job.ric.ends_with(".O"),
        "invalid Nasdaq job"
    );
    let mut months = BTreeMap::<&str, Vec<&Day>>::new();
    for day in &job.days {
        ensure!(
            day.source.contains("/XNAS.ITCH/") && day.day.len() == 10,
            "invalid source scope"
        );
        months.entry(&day.day[..7]).or_default().push(day);
    }
    let mut previous = String::new();
    let mut notionals = Vec::<i128>::new();
    let mut previous_complete = true;
    let mut quarantined = 0;
    for (month, days) in months {
        let thresholds =
            if previous_complete && previous_month(month)? == previous && !notionals.is_empty() {
                notionals.sort_unstable();
                Some((
                    quantile100(&notionals, 50).unwrap(),
                    quantile100(&notionals, 90).unwrap(),
                ))
            } else {
                None
            };
        let sample_count = if thresholds.is_some() {
            notionals.len()
        } else {
            0
        };
        notionals.clear();
        // Only the preceding month is needed: current-month days are independent.
        // Parallel days prevent one large QQQ history from serializing the batch.
        let results = days
            .par_iter()
            .map(|day| {
                run_day(job, day, args, build_id, progress, thresholds, sample_count)
                    .with_context(|| format!("ric={} day={}", job.ric, day.day))
            })
            .collect::<Vec<_>>();
        previous_complete = true;
        for (day, result) in days.iter().zip(results) {
            let error_path = args
                .output_root
                .join("_audit/day_errors/NASDAQ")
                .join(&job.ric)
                .join(format!("{}.json", day.day.replace('-', "")));
            match result {
                Ok(values) => {
                    notionals.extend(values);
                    if error_path.exists() {
                        fs::remove_file(&error_path)?;
                    }
                }
                Err(error) => {
                    previous_complete = false;
                    quarantined += 1;
                    json_atomic(
                        &error_path,
                        &json!({"ric":job.ric,"input":day,"error":format!("{error:#}"),"build_id":build_id,"disposition":"no daily output; source requires review"}),
                    )?;
                    eprintln!(
                        "quarantined ric={} day={} error={error:#}",
                        job.ric, day.day
                    );
                }
            }
        }
        previous = month.into();
    }
    ensure!(
        quarantined == 0,
        "{quarantined} dates quarantined; see _audit/day_errors"
    );
    Ok(())
}

fn run_day(
    job: &Job,
    day: &Day,
    args: &Args,
    build_id: &str,
    progress: &AtomicUsize,
    thresholds: Option<(i128, i128)>,
    sample_count: usize,
) -> Result<Vec<i128>> {
    let mut notionals = Vec::new();
    let (out, audit, cache) = paths(&args.output_root, &job.ric, day);
    if args.resume && audit.exists() {
        let old: Value = serde_json::from_reader(File::open(&audit)?)?;
        let stat = fs::metadata(&day.source)?;
        ensure!(
            (old["build_id"] == build_id
                || args.resume_build_id.iter().any(|id| old["build_id"] == *id))
                && old["input"] == json!(day),
            "resume input/code changed"
        );
        ensure!(
            old["source_bytes"].as_u64() == Some(stat.len())
                && old["source_mtime_ns"]
                    == json!(stat
                        .modified()?
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_nanos()),
            "resume source changed"
        );
        ensure!(
            old["output_sha256"] == hash(&out)? && old["notional_cache_sha256"] == hash(&cache)?,
            "resume output/cache changed"
        );
        let same_thresholds = old["threshold_previous_month_sample_trades"].as_u64()
            == Some(sample_count as u64)
            && old["threshold_p50_nanos_times100"] == json!(thresholds.map(|t| t.0))
            && old["threshold_p90_nanos_times100"] == json!(thresholds.map(|t| t.1));
        if same_thresholds {
            let count = read_notionals(&cache, &mut notionals)?;
            ensure!(old["trades"].as_u64() == Some(count), "resume trade census");
            progress.fetch_add(1, Ordering::Relaxed);
            return Ok(notionals);
        }
        // Restoring an excluded day can restore the next month's thresholds.
        // Keep immutable copies before rebuilding its derived bars.
        let archive = args
            .output_root
            .join("_audit/replaced/NASDAQ")
            .join(&job.ric)
            .join(day.day.replace('-', ""))
            .join(old["build_id"].as_str().context("missing old build id")?);
        fs::create_dir_all(&archive)?;
        for (source, name) in [
            (&out, "baseline.parquet"),
            (&audit, "audit.json"),
            (&cache, "notionals.i128.zst"),
        ] {
            let target = archive.join(name);
            if target.exists() {
                ensure!(
                    hash(source)? == hash(&target)?,
                    "conflicting archived output"
                )
            } else {
                fs::hard_link(source, target)?;
            }
        }
    }
    let began = Instant::now();
    let x = extract(day)?;
    if let Some(ref_dir) = &args.reference_output {
        json_atomic(
            &ref_dir.join(format!("{}_{}.json", job.ric, day.day)),
            &serde_json::to_value(&x)?,
        )?;
    }
    let mut df = frame(&job.ric, &x, thresholds)?;
    fs::create_dir_all(out.parent().unwrap())?;
    let tmp = out.with_extension("parquet.tmp");
    let mut f = File::create(&tmp)?;
    ParquetWriter::new(&mut f)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df)?;
    f.sync_all()?;
    fs::rename(&tmp, &out)?;
    let trades = write_notionals(&cache, &x.minutes)?;
    for m in &x.minutes {
        for t in &m.trades {
            notionals.push(i128::from(t.1) * i128::from(t.2));
        }
    }
    let manifest = json!({"input":day,"ric":job.ric,"source_product":"XNAS.ITCH MBO single-venue",
            "source_bytes":x.source_bytes,"source_sha256":x.source_sha256,"source_mtime_ns":x.source_mtime_ns,
            "rows":x.rows,"actions":x.actions,"flags":x.flags,"instrument_id":x.instrument_id,
            "receive_backsteps":x.receive_backsteps,"rth_minutes":x.minutes.len(),"trades":trades,
        "bad_book_intervals_ns":x.bad_book_intervals_ns,
        "ignored_post_session_incomplete_rows":x.ignored_post_session_incomplete_rows,
        "bounded_bad_timestamp_intervals_ns":x.bounded_bad_timestamp_intervals_ns,
        "twap_null_due_to_bad_timestamp_minutes":x.minutes.iter().filter(|m|m.trade_time_uncertain).map(|m|m.ts).collect::<Vec<_>>(),
        "source_quality_affected_minutes":x.minutes.iter().filter(|m|m.source_quality_flags!=0).count(),
            "complete_ten_minutes":x.minutes.iter().filter(|m|m.book_depth==10).count(),
            "missing_depth_minutes":x.minutes.iter().filter(|m|m.book_depth==0).map(|m|m.ts).collect::<Vec<_>>(),
            "partial_depth_minutes":x.minutes.iter().filter(|m|m.book_depth>0 && m.book_depth<10).map(|m|(m.ts,m.book_depth)).collect::<Vec<_>>(),
            "threshold_previous_month_sample_trades":sample_count,"threshold_p50_nanos_times100":thresholds.map(|t|t.0),"threshold_p90_nanos_times100":thresholds.map(|t|t.1),
            "output_sha256":hash(&out)?,"notional_cache_sha256":hash(&cache)?,"build_id":build_id,
            "gzip_read_to_eof_crc_verified":true,"elapsed_seconds":began.elapsed().as_secs_f64()});
    json_atomic(&audit, &manifest)?;
    let done = progress.fetch_add(1, Ordering::Relaxed) + 1;
    println!(
        "done={done} ric={} day={} source_rows={} ten={}/{} elapsed_s={:.2}",
        job.ric,
        day.day,
        x.rows,
        x.minutes.iter().filter(|m| m.book_depth == 10).count(),
        x.minutes.len(),
        began.elapsed().as_secs_f64()
    );
    Ok(notionals)
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.workers > 0, "workers must be positive");
    let jobs: Vec<Job> = serde_json::from_reader(File::open(&args.jobs)?)?;
    for job in &jobs {
        ensure!(
            job.days.windows(2).all(|d| d[0].day < d[1].day),
            "days must be strictly ordered"
        );
    }
    fs::create_dir_all(&args.output_root)?;
    let build_id = hash(&std::env::current_exe()?)?;
    let progress = AtomicUsize::new(0);
    let results = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build()?
        .install(|| {
            jobs.par_iter()
                .map(|job| {
                    let r = run_job(job, &args, &build_id, &progress);
                    if let Err(e) = &r {
                        let error =
                            json!({"ric":job.ric,"error":format!("{e:#}"),"build_id":build_id});
                        let _ = json_atomic(
                            &args
                                .output_root
                                .join("_audit/errors")
                                .join(format!("{}.json", job.ric)),
                            &error,
                        );
                        eprintln!("failed ric={} error={e:#}", job.ric);
                    }
                    r
                })
                .collect::<Vec<_>>()
        });
    let failed = results.iter().filter(|r| r.is_err()).count();
    json_atomic(
        &args.output_root.join("_audit/run.json"),
        &json!({"jobs":args.jobs,"jobs_sha256":hash(&args.jobs)?,"completed_days":progress.load(Ordering::Relaxed),"failed_rics":failed,"build_id":build_id}),
    )?;
    ensure!(failed==0,"{failed} RIC jobs failed; see _audit/errors; incomplete staging is not a published baseline");
    Ok(())
}
