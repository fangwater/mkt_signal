//! Receive-time Tardis TradFi baseline and native twenty-level 632-factor export.
#[path = "crypto_tradfi_baseline/extra_baselines.rs"]
mod extra_baselines;
use anyhow::{bail, ensure, Context, Result};
use chrono::{Datelike, Duration, NaiveDate};
use clap::{Parser, ValueEnum};
use flate2::read::MultiGzDecoder;
use mkt_parsers::msg::trade_flow_feature_msg::{
    TradeFlowFeatureMsg, TRADE_FLOW_FEATURE_FIELD_NAMES,
};
use mkt_signal::factor_pub::fusion_factor_pub::{
    app::{BaselineReplayState, DepthLevel, DepthSnapshot},
    FusionFactorId, SymbolFactorPlan,
};
use polars::prelude::*;
use rayon::prelude::*;
use rolling_common::kll_quantile::{FrozenKllSketch, StreamingKllSketch};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File},
    io::Cursor,
    path::{Path, PathBuf},
    time::Instant,
};

const VERSION: &str = "tradfi-receive-minute-20261026";
const MIN_US: i64 = 60_000_000;
const SUM: &[usize] = &[
    4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 29, 30, 31,
];
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Phase {
    Baseline,
    Factors,
}
#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    manifest: PathBuf,
    #[arg(long)]
    out_root: PathBuf,
    #[arg(long, default_value = "/mnt/nfs/30.133_xt_crypto/TardisSource")]
    source_root: PathBuf,
    #[arg(long, value_enum)]
    phase: Phase,
    #[arg(long, default_value_t = 8)]
    workers: usize,
    #[arg(long)]
    symbols: Option<String>,
    #[arg(long)]
    start: Option<String>,
    #[arg(long)]
    end: Option<String>,
    #[arg(long)]
    execute: bool,
}
#[derive(Clone)]
struct Bar {
    ts: i64,
    v: [f64; 32],
    depth: [f64; 80],
    twap: f64,
    mid: f64,
    levels: u32,
    flags: u32,
    depth_recv: Option<i64>,
    minutes: i64,
    end: i64,
}
impl Bar {
    fn empty(ts: i64) -> Self {
        let mut v = [0.; 32];
        for i in [0, 1, 2, 3, 23, 24, 25] {
            v[i] = f64::NAN;
        }
        Self {
            ts,
            v,
            depth: [f64::NAN; 80],
            twap: f64::NAN,
            mid: f64::NAN,
            levels: 0,
            flags: 1,
            depth_recv: None,
            minutes: 1,
            end: ts + 60,
        }
    }
}
#[derive(Default)]
struct Carry {
    last: Option<f64>,
    vwap: [Option<f64>; 3],
}
#[derive(Default)]
struct Monthly {
    month: Option<(i32, u32)>,
    hour: Option<i64>,
    current: StreamingKllSketch,
    hours: Vec<FrozenKllSketch>,
    threshold: Option<(f64, f64)>,
}
impl Monthly {
    fn begin(&mut self, day: NaiveDate) -> Result<()> {
        let month = (day.year(), day.month());
        if self.month != Some(month) {
            if !self.current.is_empty() {
                self.hours.push(self.current.freeze());
                self.current.reset();
            }
            let prior = day.with_day(1).unwrap() - Duration::days(1);
            self.threshold = if self.month == Some((prior.year(), prior.month())) {
                FrozenKllSketch::merge_all(self.hours.iter())?.and_then(|k| {
                    let q = k.quantiles(&[0.5, 0.9]);
                    Some((q[0]?, q[1]?))
                })
            } else {
                None
            };
            self.hours.clear();
            self.hour = None;
            self.month = Some(month);
        }
        Ok(())
    }
    fn push(&mut self, recv: i64, n: f64) {
        let h = recv / 3_600_000_000;
        if self.hour != Some(h) {
            if !self.current.is_empty() {
                self.hours.push(self.current.freeze());
                self.current.reset();
            }
            self.hour = Some(h);
        }
        self.current.insert(n);
    }
}
fn date(s: &str) -> Result<NaiveDate> {
    Ok(NaiveDate::parse_from_str(s, "%Y-%m-%d")?)
}
fn epoch(d: NaiveDate) -> i64 {
    d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp()
}
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    fs::create_dir_all(path.parent().context("JSON parent")?)?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
    fs::rename(tmp, path)?;
    Ok(())
}
fn output(root: &Path, kind: &str, n: u32, sym: &str, day: NaiveDate) -> PathBuf {
    root.join(format!(
        "{kind}_{n}min/binance-futures/{sym}/{}.parquet",
        day.format("%Y%m%d")
    ))
}
fn source(root: &Path, sym: &str, kind: &str, day: NaiveDate) -> PathBuf {
    root.join(format!(
        "binance_usd_{}/{kind}/binance-futures_{kind}_{day}_{sym}.csv.gz",
        sym.to_lowercase()
    ))
}
fn load(path: &Path) -> Result<(Vec<u8>, Value)> {
    let before = fs::metadata(path).with_context(|| format!("source {}", path.display()))?;
    let bytes = fs::read(path)?;
    let after = fs::metadata(path)?;
    ensure!(
        before.len() == after.len() && before.modified()? == after.modified()?,
        "source changed {}",
        path.display()
    );
    let audit = json!({"path":path,"bytes":bytes.len(),"mtime_ns":before.modified()?.duration_since(std::time::UNIX_EPOCH)?.as_nanos().to_string(),"sha256":format!("{:x}",Sha256::digest(&bytes))});
    Ok((bytes, audit))
}
fn number(rec: &csv::StringRecord, i: usize) -> Result<f64> {
    Ok(rec.get(i).context("missing CSV field")?.parse()?)
}
fn integer(rec: &csv::StringRecord, i: usize) -> Result<i64> {
    Ok(rec.get(i).context("missing integer CSV field")?.parse()?)
}
fn reader<'a>(
    bytes: &'a [u8],
    header: &str,
) -> Result<csv::Reader<MultiGzDecoder<Cursor<&'a [u8]>>>> {
    let mut r = csv::Reader::from_reader(MultiGzDecoder::new(Cursor::new(bytes)));
    ensure!(
        r.headers()?.iter().eq(header.split(',')),
        "CSV schema mismatch"
    );
    Ok(r)
}
fn identity(rec: &csv::StringRecord, sym: &str) -> Result<()> {
    ensure!(
        rec.get(0) == Some("binance-futures") && rec.get(1) == Some(sym),
        "source identity mismatch for {sym}"
    );
    Ok(())
}
fn trades(
    bytes: &[u8],
    sym: &str,
    start: i64,
    carry: &mut Carry,
    monthly: &mut Monthly,
) -> Result<(Vec<Bar>, Value)> {
    let mut bars: Vec<Bar> = (0..1440).map(|i| Bar::empty(start + i * 60)).collect();
    let mut reader = reader(
        bytes,
        "exchange,symbol,timestamp,local_timestamp,id,side,price,amount",
    )?;
    let mut rec = csv::StringRecord::new();
    let mut rows = 0u64;
    let mut first = None;
    let mut cursor = start * 1_000_000;
    let mut index = 0usize;
    let mut weighted = 0.;
    let mut seeded = carry.last.is_some();
    let mut exchange_late = 0u64;
    let mut previous = None;
    while reader.read_record(&mut rec)? {
        identity(&rec, sym)?;
        let recv = integer(&rec, 3)?;
        let exch = integer(&rec, 2)?;
        ensure!(
            recv >= start * 1_000_000 && recv < (start + 86400) * 1_000_000,
            "trade receive time outside UTC source day {sym}"
        );
        ensure!(
            previous.is_none_or(|p| recv >= p),
            "trade receive time regression {sym}"
        );
        previous = Some(recv);
        ensure!(
            integer(&rec, 4)? > 0 && exch > 0,
            "invalid trade identity/time"
        );
        if recv / MIN_US != exch / MIN_US {
            exchange_late += 1;
        }
        let p = number(&rec, 6)?;
        let v = number(&rec, 7)?;
        let notional = p * v;
        ensure!(
            p.is_finite() && p > 0. && v.is_finite() && v > 0. && notional.is_finite(),
            "invalid trade values {sym}"
        );
        while index < 1440 && recv >= (bars[index].end * 1_000_000) {
            finish_trade(&mut bars[index], carry, cursor, weighted, seeded);
            index += 1;
            cursor = (start + index as i64 * 60) * 1_000_000;
            weighted = 0.;
            seeded = carry.last.is_some();
        }
        let b = &mut bars[index];
        if let Some(last) = carry.last {
            weighted += last * (recv - cursor) as f64;
        }
        cursor = recv;
        carry.last = Some(p);
        if b.v[7] == 0. {
            b.v[0] = p;
            b.v[1] = p;
            b.v[2] = p;
        } else {
            b.v[1] = b.v[1].max(p);
            b.v[2] = b.v[2].min(p);
        }
        b.v[3] = p;
        b.v[4] += v;
        b.v[5] += notional;
        b.v[7] += 1.;
        let buy = match rec.get(5) {
            Some("buy") => true,
            Some("sell") => false,
            _ => bail!("invalid trade side"),
        };
        b.v[if buy { 8 } else { 9 }] += 1.;
        b.v[if buy { 10 } else { 11 }] += notional;
        b.v[if buy { 12 } else { 13 }] += v;
        if let Some((p50, p90)) = monthly.threshold {
            let size = if notional >= p90 {
                0
            } else if notional >= p50 {
                1
            } else {
                2
            };
            b.v[14 + size] += notional;
            b.v[17 + size * 2 + usize::from(!buy)] += notional;
        }
        monthly.push(recv, notional);
        rows += 1;
        first.get_or_insert(recv);
    }
    while index < 1440 {
        finish_trade(&mut bars[index], carry, cursor, weighted, seeded);
        index += 1;
        cursor = (start + index as i64 * 60) * 1_000_000;
        weighted = 0.;
        seeded = carry.last.is_some();
    }
    Ok((
        bars,
        json!({"rows":rows,"first_receive_us":first,"exchange_minute_differs":exchange_late,"threshold":monthly.threshold}),
    ))
}
fn finish_trade(b: &mut Bar, carry: &mut Carry, cursor: i64, mut weighted: f64, seeded: bool) {
    if let Some(last) = carry.last {
        weighted += last * (b.end * 1_000_000 - cursor) as f64;
        if b.v[7] == 0. {
            for i in 0..4 {
                b.v[i] = last;
            }
        }
    }
    b.twap = if seeded {
        weighted / MIN_US as f64
    } else {
        f64::NAN
    };
    for (i, (vi, ai)) in [(4, 5), (12, 10), (13, 11)].into_iter().enumerate() {
        if b.v[vi] > 0. {
            carry.vwap[i] = Some(b.v[ai] / b.v[vi]);
        }
        b.v[23 + i] = carry.vwap[i].or(carry.last).unwrap_or(f64::NAN);
    }
    derived(b);
}
fn derived(b: &mut Bar) {
    b.v[6] = if b.v[7] > 0. { b.v[5] / b.v[7] } else { 0. };
    b.v[26] = b.v[10] - b.v[11];
    b.v[27] = b.v[12] - b.v[13];
    b.v[28] = if b.v[5] > 0. { b.v[26] / b.v[5] } else { 0. };
    for i in 0..3 {
        b.v[29 + i] = b.v[17 + i * 2] - b.v[18 + i * 2];
    }
}
#[derive(Default)]
struct Book {
    bids: BTreeMap<u64, f64>,
    asks: BTreeMap<u64, f64>,
    initialized: bool,
    recv: Option<i64>,
}
impl Book {
    fn capture(&self, b: &mut Bar) {
        b.depth_recv = self.recv;
        let Some((&bid, _)) = self.bids.last_key_value() else {
            return;
        };
        let Some((&ask, _)) = self.asks.first_key_value() else {
            return;
        };
        if !self.initialized || bid >= ask {
            return;
        }
        b.levels = self.bids.len().min(self.asks.len()).min(20) as u32;
        b.flags = if b.levels == 20 { 0 } else { 2 };
        b.mid = (f64::from_bits(bid) + f64::from_bits(ask)) / 2.;
        for (i, (&p, &v)) in self.bids.iter().rev().take(20).enumerate() {
            b.depth[i * 2] = f64::from_bits(p);
            b.depth[i * 2 + 1] = v;
        }
        for (i, (&p, &v)) in self.asks.iter().take(20).enumerate() {
            b.depth[40 + i * 2] = f64::from_bits(p);
            b.depth[41 + i * 2] = v;
        }
    }
}
fn books(bytes: &[u8], sym: &str, start: i64, bars: &mut [Bar]) -> Result<Value> {
    let mut reader = reader(
        bytes,
        "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount",
    )?;
    let mut rec = csv::StringRecord::new();
    let mut book = Book::default();
    let mut next = 0;
    let mut group = None;
    let mut rows = 0u64;
    let mut snapshots = 0u64;
    let mut first = None;
    let mut previous = None;
    while reader.read_record(&mut rec)? {
        identity(&rec, sym)?;
        let recv = integer(&rec, 3)?;
        let exch = integer(&rec, 2)?;
        ensure!(
            recv >= start * 1_000_000 && recv < (start + 86400) * 1_000_000,
            "book receive time outside source day {sym}"
        );
        ensure!(
            previous.is_none_or(|p| recv >= p),
            "book receive time regression {sym}"
        );
        previous = Some(recv);
        let snapshot = match rec.get(4) {
            Some("true") => true,
            Some("false") => false,
            _ => bail!("invalid snapshot flag"),
        };
        let key = (exch, recv, snapshot);
        if group != Some(key) {
            // Capture only after all rows of the preceding atomic message were applied.
            while next < bars.len() && bars[next].end * 1_000_000 <= recv {
                book.capture(&mut bars[next]);
                next += 1;
            }
            if snapshot {
                book.bids.clear();
                book.asks.clear();
                book.initialized = true;
                snapshots += 1;
            }
            group = Some(key);
            book.recv = Some(recv);
        }
        let p = number(&rec, 6)?;
        let v = number(&rec, 7)?;
        ensure!(
            exch > 0 && p.is_finite() && p > 0. && v.is_finite() && v >= 0.,
            "invalid depth values"
        );
        let side = match rec.get(5) {
            Some("bid") => &mut book.bids,
            Some("ask") => &mut book.asks,
            _ => bail!("invalid book side"),
        };
        if v == 0. {
            side.remove(&p.to_bits());
        } else {
            side.insert(p.to_bits(), v);
        }
        rows += 1;
        first.get_or_insert(recv);
    }
    while next < bars.len() {
        book.capture(&mut bars[next]);
        next += 1;
    }
    Ok(
        json!({"rows":rows,"snapshots":snapshots,"first_receive_us":first,"depth20_minutes":bars.iter().filter(|b|b.levels==20).count(),"partial_depth_minutes":bars.iter().filter(|b|b.levels>0&&b.levels<20).count(),"invalid_depth_minutes":bars.iter().filter(|b|b.levels==0).count()}),
    )
}
fn aggregate(bars: &[Bar], n: u32) -> Vec<Bar> {
    let mut result = Vec::new();
    let mut start = 0;
    while start < bars.len() {
        let key = bars[start].ts / (n as i64 * 60);
        let mut end = start + 1;
        while end < bars.len()
            && bars[end].ts / (n as i64 * 60) == key
            && bars[end].ts == bars[end - 1].end
        {
            end += 1;
        }
        let group = &bars[start..end];
        let mut b = group.last().unwrap().clone();
        b.ts = group[0].ts;
        b.minutes = group.len() as i64;
        for &i in SUM {
            b.v[i] = group.iter().map(|r| r.v[i]).sum();
        }
        let traded: Vec<&Bar> = group.iter().filter(|r| r.v[7] > 0.).collect();
        if traded.is_empty() {
            for i in 0..4 {
                b.v[i] = group.last().unwrap().v[3];
            }
        } else {
            b.v[0] = traded[0].v[0];
            b.v[3] = traded.last().unwrap().v[3];
            b.v[1] = traded
                .iter()
                .map(|r| r.v[1])
                .fold(f64::NEG_INFINITY, f64::max);
            b.v[2] = traded.iter().map(|r| r.v[2]).fold(f64::INFINITY, f64::min);
        }
        for (i, vi, ai) in [(23, 4, 5), (24, 12, 10), (25, 13, 11)] {
            if b.v[vi] > 0. {
                b.v[i] = b.v[ai] / b.v[vi];
            }
        }
        b.twap = group.iter().map(|r| r.twap).sum::<f64>() / group.len() as f64;
        derived(&mut b);
        result.push(b);
        start = end;
    }
    result
}
fn fseries(name: &str, values: impl IntoIterator<Item = f64>) -> Series {
    Series::new(
        name.into(),
        values
            .into_iter()
            .map(|v| v.is_finite().then_some(v))
            .collect::<Vec<_>>(),
    )
}
fn write_df(path: &Path, mut frame: DataFrame) -> Result<Value> {
    ensure!(!path.exists(), "refusing overwrite {}", path.display());
    fs::create_dir_all(path.parent().unwrap())?;
    let tmp = path.with_extension("parquet.tmp");
    // Parquet emits many small writes per column. Serialize in memory so a
    // wide factor file reaches the filesystem in a single buffered operation.
    let mut bytes = Vec::new();
    ParquetWriter::new(&mut bytes).finish(&mut frame)?;
    let actual = ParquetReader::new(Cursor::new(&bytes)).finish()?;
    ensure!(
        frame.equals_missing(&actual),
        "all-column Parquet readback mismatch {}",
        path.display()
    );
    fs::write(&tmp, &bytes)?;
    fs::rename(&tmp, path)?;
    Ok(
        json!({"path":path,"rows":frame.height(),"columns":frame.width(),"bytes":bytes.len(),"sha256":format!("{:x}",Sha256::digest(&bytes))}),
    )
}
fn write_bars(path: &Path, sym: &str, bars: &[Bar]) -> Result<Value> {
    let mut cols = vec![
        Series::new("symbol".into(), vec![sym; bars.len()]),
        Series::new("ts".into(), bars.iter().map(|b| b.ts).collect::<Vec<_>>()),
    ];
    for (i, name) in TRADE_FLOW_FEATURE_FIELD_NAMES.iter().enumerate() {
        cols.push(fseries(name, bars.iter().map(|b| b.v[i])));
    }
    for side in ["bid", "ask"] {
        for i in 0..20 {
            for (j, pv) in ["p", "v"].into_iter().enumerate() {
                let offset = if side == "bid" { 0 } else { 40 };
                cols.push(fseries(
                    &format!("{side}{i}{pv}"),
                    bars.iter().map(|b| b.depth[offset + i * 2 + j]),
                ));
            }
        }
    }
    cols.push(fseries("twap", bars.iter().map(|b| b.twap)));
    cols.push(fseries("mid_price", bars.iter().map(|b| b.mid)));
    cols.push(Series::new(
        "book_depth".into(),
        bars.iter().map(|b| b.levels).collect::<Vec<_>>(),
    ));
    cols.push(Series::new(
        "source_quality_flags".into(),
        bars.iter().map(|b| b.flags).collect::<Vec<_>>(),
    ));
    cols.push(Series::new(
        "source_depth_ts_utc_us".into(),
        bars.iter().map(|b| b.depth_recv).collect::<Vec<_>>(),
    ));
    cols.push(Series::new(
        "source_minutes".into(),
        bars.iter().map(|b| b.minutes).collect::<Vec<_>>(),
    ));
    cols.push(Series::new(
        "end_ts".into(),
        bars.iter().map(|b| b.end).collect::<Vec<_>>(),
    ));
    write_df(path, DataFrame::new(cols)?)
}
fn run_baseline(args: &Args, sym: &str, start: Option<NaiveDate>, end: NaiveDate) -> Result<()> {
    let begin = Instant::now();
    let marker = args.out_root.join(format!("_audit/baseline_{sym}.json"));
    if marker.exists() {
        let v: Value = serde_json::from_slice(&fs::read(&marker)?)?;
        ensure!(
            v["contract"] == VERSION && v["end"] == end.to_string(),
            "incompatible existing marker"
        );
        return Ok(());
    }
    let p = args
        .source_root
        .join(format!("binance_usd_{}/trades", sym.to_lowercase()));
    let first = fs::read_dir(p)?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().to_str().map(str::to_owned))
        .filter_map(|s| s.split('_').nth(2).map(str::to_owned))
        .filter_map(|s| date(&s).ok())
        .min()
        .context("no source trades")?;
    let mut day = first.max(start.unwrap_or(first));
    let mut carry = Carry::default();
    let mut monthly = Monthly::default();
    let mut files = 0;
    let mut total_rows = 0;
    while day <= end {
        monthly.begin(day)?;
        let (tbytes, taudit) = load(&source(&args.source_root, sym, "trades", day))?;
        let (mut bars, tstats) = trades(&tbytes, sym, epoch(day), &mut carry, &mut monthly)?;
        drop(tbytes);
        let (bbytes, baudit) = load(&source(&args.source_root, sym, "incremental_book_L2", day))?;
        let bstats = books(&bbytes, sym, epoch(day), &mut bars)?;
        drop(bbytes);
        if day == first {
            let first_event = [
                tstats["first_receive_us"].as_i64(),
                bstats["first_receive_us"].as_i64(),
            ]
            .into_iter()
            .flatten()
            .min()
            .context("first source day has no events")?;
            bars.retain(|b| b.ts >= first_event / MIN_US * 60);
        }
        let mut outputs = Vec::new();
        for n in [1, 5, 15] {
            let aggregate = if n == 1 {
                bars.clone()
            } else {
                aggregate(&bars, n)
            };
            outputs.push(write_bars(
                &output(&args.out_root, "baseline_data", n, sym, day),
                sym,
                &aggregate,
            )?);
        }
        atomic_json(
            &args
                .out_root
                .join(format!("_audit/days/{sym}/{}.json", day.format("%Y%m%d"))),
            &json!({"contract":VERSION,"symbol":sym,"day":day.to_string(),"trade_source":taudit,"book_source":baudit,"trades":tstats,"book":bstats,"outputs":outputs}),
        )?;
        files += 1;
        total_rows += bars.len();
        if files % 10 == 0 {
            eprintln!(
                "BASELINE {sym} {day} days={files} minutes={total_rows} elapsed={:.1}",
                begin.elapsed().as_secs_f64()
            );
        }
        day += Duration::days(1);
    }
    atomic_json(
        &marker,
        &json!({"contract":VERSION,"symbol":sym,"end":end.to_string(),"days":files,"minute_rows":total_rows,"elapsed_seconds":begin.elapsed().as_secs_f64()}),
    )?;
    eprintln!(
        "BASELINE_DONE {sym} days={files} minutes={total_rows} seconds={:.1}",
        begin.elapsed().as_secs_f64()
    );
    Ok(())
}
fn read_bars(path: &Path) -> Result<(String, Vec<Bar>)> {
    let df = ParquetReader::new(File::open(path)?).finish()?;
    let syms = df.column("symbol")?.str()?;
    let sym = syms.get(0).context("empty baseline")?.to_owned();
    ensure!(
        syms.into_iter().all(|s| s == Some(sym.as_str())),
        "mixed baseline identities"
    );
    let mut bars: Vec<Bar> = df
        .column("ts")?
        .i64()?
        .into_no_null_iter()
        .map(Bar::empty)
        .collect();
    for (i, name) in TRADE_FLOW_FEATURE_FIELD_NAMES.iter().enumerate() {
        for (b, v) in bars.iter_mut().zip(df.column(name)?.f64()?) {
            b.v[i] = v.unwrap_or(f64::NAN);
        }
    }
    for side in ["bid", "ask"] {
        for i in 0..20 {
            for (j, pv) in ["p", "v"].into_iter().enumerate() {
                let offset = if side == "bid" { 0 } else { 40 };
                for (b, v) in bars
                    .iter_mut()
                    .zip(df.column(&format!("{side}{i}{pv}"))?.f64()?)
                {
                    b.depth[offset + i * 2 + j] = v.unwrap_or(f64::NAN);
                }
            }
        }
    }
    for (b, v) in bars.iter_mut().zip(df.column("book_depth")?.u32()?) {
        b.levels = v.context("null book_depth")?;
    }
    for (b, v) in bars.iter_mut().zip(df.column("end_ts")?.i64()?) {
        b.end = v.context("null end_ts")?;
    }
    Ok((sym, bars))
}
fn run_factors(args: &Args, sym: &str, end: NaiveDate) -> Result<()> {
    let began = Instant::now();
    let marker = args.out_root.join(format!("_audit/factors_{sym}.json"));
    if marker.exists() {
        let v: Value = serde_json::from_slice(&fs::read(&marker)?)?;
        ensure!(
            v["contract"] == VERSION && v["end"] == end.to_string(),
            "incompatible factor marker"
        );
        return Ok(());
    }
    let names: Vec<String> = FusionFactorId::ALL
        .iter()
        .map(|id| id.as_name().to_string())
        .collect();
    let core_names: Vec<String> = names
        .iter()
        .filter(|n| !extra_baselines::NAMES.contains(&n.as_str()))
        .cloned()
        .collect();
    let core_indices: Vec<usize> = core_names
        .iter()
        .map(|n| names.iter().position(|x| x == n).unwrap())
        .collect();
    let extra_indices: Vec<usize> = extra_baselines::NAMES
        .iter()
        .map(|n| names.iter().position(|x| x == n).unwrap())
        .collect();
    let plan = SymbolFactorPlan::from_factor_names(sym, core_names)?;
    BaselineReplayState::validate_factor_plan(&plan)?;
    let mut frequencies = Vec::new();
    for n in [1, 5, 15] {
        let dir = args
            .out_root
            .join(format!("baseline_data_{n}min/binance-futures/{sym}"));
        let mut paths: Vec<PathBuf> = fs::read_dir(&dir)?
            .map(|e| e.map(|e| e.path()))
            .collect::<std::io::Result<_>>()?;
        paths.sort();
        let mut state = BaselineReplayState::default();
        let mut extra = extra_baselines::ExtraState::default();
        let mut previous: Option<Vec<f64>> = None;
        let mut last_end = None;
        let mut counts = vec![0u64; names.len()];
        let mut files = Vec::new();
        let mut total = 0u64;
        for path in paths {
            if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
                continue;
            }
            let (actual, bars) = read_bars(&path)?;
            ensure!(actual == sym, "baseline symbol mismatch");
            let mut values: Vec<Vec<Option<f64>>> =
                vec![Vec::with_capacity(bars.len()); names.len()];
            for b in &bars {
                if last_end != Some(b.ts) {
                    state = BaselineReplayState::default();
                    extra = extra_baselines::ExtraState::default();
                    previous = None;
                }
                for (i, col) in values.iter_mut().enumerate() {
                    let v = previous
                        .as_ref()
                        .and_then(|v| v[i].is_finite().then_some(v[i]));
                    if v.is_some() {
                        counts[i] += 1;
                    }
                    col.push(v);
                }
                previous = None;
                // The native dispatcher sums the available depth. Its 20-level
                // formulas are only equivalent to the Python pool on a full
                // book; a partial book must not silently become a shorter sum.
                if b.levels == 20 {
                    let levels = b.levels as usize;
                    let bids: Vec<DepthLevel> = (0..levels)
                        .map(|i| DepthLevel {
                            price: b.depth[i * 2],
                            amount: b.depth[i * 2 + 1],
                        })
                        .collect();
                    let asks: Vec<DepthLevel> = (0..levels)
                        .map(|i| DepthLevel {
                            price: b.depth[40 + i * 2],
                            amount: b.depth[41 + i * 2],
                        })
                        .collect();
                    let depth = DepthSnapshot::from_native_levels(&bids, &asks)?;
                    let msg = TradeFlowFeatureMsg::from_indexed_values(
                        sym.to_string(),
                        1,
                        b.end * 1000,
                        &b.v,
                    )?;
                    state.push_with_native_depth(msg, depth)?;
                    extra.push(b.v);
                    let mut result = vec![f64::NAN; names.len()];
                    for (i, v) in core_indices.iter().zip(state.factor_values(&plan)) {
                        result[*i] = v;
                    }
                    for (i, v) in extra_indices.iter().zip(extra.values()) {
                        result[*i] = v;
                    }
                    ensure!(
                        result.iter().all(|v| !v.is_infinite()),
                        "infinite raw factor for {sym} at {}",
                        b.ts
                    );
                    previous = Some(result);
                } else {
                    // This bar's incomplete closing book becomes known at its end. It
                    // cannot erase the prior factor already usable at ts.
                    state = BaselineReplayState::default();
                    extra = extra_baselines::ExtraState::default();
                }
                last_end = Some(b.end);
                total += 1;
            }
            let mut cols = vec![
                Series::new("symbol".into(), vec![sym; bars.len()]),
                Series::new("ts".into(), bars.iter().map(|b| b.ts).collect::<Vec<_>>()),
            ];
            for (name, vals) in names.iter().zip(values) {
                cols.push(Series::new(name.as_str().into(), vals));
            }
            let dest = args
                .out_root
                .join(format!("baseline_factor_{n}min/binance-futures/{sym}"))
                .join(path.file_name().unwrap());
            files.push(write_df(&dest, DataFrame::new(cols)?)?);
        }
        frequencies.push(json!({"bar_minutes":n,"rows":total,"files":files,"finite_counts":names.iter().cloned().zip(counts).collect::<BTreeMap<_,_>>()}));
        eprintln!(
            "FACTORS {sym} {n}min rows={total} elapsed={:.1}",
            began.elapsed().as_secs_f64()
        );
    }
    atomic_json(
        &marker,
        &json!({"contract":VERSION,"symbol":sym,"end":end.to_string(),"frequencies":frequencies,"elapsed_seconds":began.elapsed().as_secs_f64()}),
    )?;
    Ok(())
}
fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.workers > 0, "workers must be positive");
    let manifest: Value = serde_json::from_slice(&fs::read(&args.manifest)?)?;
    let mut symbols: Vec<String> = serde_json::from_value(manifest["symbols"].clone())?;
    if let Some(filter) = &args.symbols {
        let selected: HashSet<_> = filter.split(',').map(str::to_uppercase).collect();
        symbols.retain(|s| selected.contains(s));
        ensure!(symbols.len() == selected.len(), "unknown filter symbol");
    }
    ensure!(!symbols.is_empty(), "empty symbol universe");
    let end = date(
        args.end
            .as_deref()
            .or(manifest["end_date"].as_str())
            .context("end_date required")?,
    )?;
    let start = args.start.as_deref().map(date).transpose()?;
    eprintln!(
        "PLAN phase={:?} symbols={} end={} output={} execute={}",
        args.phase,
        symbols.len(),
        end,
        args.out_root.display(),
        args.execute
    );
    if !args.execute {
        return Ok(());
    }
    ensure!(
        !args.out_root.starts_with(&args.source_root),
        "output must be outside read-only source tree"
    );
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build_global()?;
    let failures: Vec<_> = symbols
        .par_iter()
        .filter_map(|sym| {
            let result = match args.phase {
                Phase::Baseline => run_baseline(&args, sym, start, end),
                Phase::Factors => run_factors(&args, sym, end),
            };
            result.err().map(|e| {
                eprintln!("ERROR {sym}: {e:#}");
                json!({"symbol":sym,"error":format!("{e:#}")})
            })
        })
        .collect();
    let run_name = if args.symbols.is_some() {
        // Independent symbol processes must not share an atomic-json temp file.
        let key = if symbols.len() == 1 {
            symbols[0].clone()
        } else {
            format!("{:x}", Sha256::digest(symbols.join(",").as_bytes()))
        };
        format!("_audit/{:?}_runs/{key}.json", args.phase)
    } else {
        format!("_audit/{:?}_run.json", args.phase)
    };
    atomic_json(
        &args.out_root.join(run_name.to_lowercase()),
        &json!({"contract":VERSION,"symbols":symbols,"end":end.to_string(),"failures":failures}),
    )?;
    ensure!(failures.is_empty(), "{} symbols failed", failures.len());
    Ok(())
}
