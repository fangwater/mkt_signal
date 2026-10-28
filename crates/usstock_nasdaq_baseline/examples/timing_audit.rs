//! Read-only inventory of bad receive-time groups and their trusted neighbors.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use flate2::read::MultiGzDecoder;
use rayon::prelude::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};
use usstock_nasdaq_baseline::{price_e9, timestamp, Day, Job, HEADER, NS};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    jobs: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 12)]
    workers: usize,
}
struct Hashed {
    f: File,
    h: Sha256,
    n: u64,
}
impl Read for Hashed {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        let n = self.f.read(b)?;
        self.h.update(&b[..n]);
        self.n += n as u64;
        Ok(n)
    }
}
#[derive(Default)]
struct Group {
    bad: bool,
    rows: u64,
    trades: u64,
    available: u64,
    first_row: u64,
    last_row: u64,
    actions: BTreeMap<String, u64>,
}
fn inspect(ric: &str, day: &Day) -> Result<Value> {
    let before = std::fs::metadata(&day.source)?;
    let h = Hashed {
        f: File::open(&day.source)?,
        h: Sha256::new(),
        n: 0,
    };
    let mut reader = csv::Reader::from_reader(MultiGzDecoder::new(BufReader::new(h)));
    ensure!(reader.headers()?.iter().eq(HEADER), "header");
    let mut row = csv::StringRecord::new();
    let mut n = 0_u64;
    let mut current = Group::default();
    let mut last_good = None::<u64>;
    let mut windows = Vec::<Value>::new();
    let mut open_windows = Vec::<usize>::new();
    let mut flags = BTreeMap::<String, u64>::new();
    let mut bad_rows = 0;
    let mut bad_row_trades = 0;
    let mut instrument = None;
    while reader.read_record(&mut row)? {
        n += 1;
        let flag: u8 = row[10].parse()?;
        ensure!(
            &row[1] == "160" && &row[2] == "2" && row[13] == day.symbol,
            "identity row={n}"
        );
        if let Some(id) = &instrument {
            ensure!(id == &row[3], "instrument")
        } else {
            instrument = Some(row[3].to_string())
        }
        *flags.entry(flag.to_string()).or_default() += 1;
        if n == 1 && &row[4] == "R" && flag & 8 != 0 && flag & (32 | 64 | 16 | 4) == 0 {
            continue;
        }
        let event = timestamp(&row[0])?;
        let recv = i128::from(event) + row[11].parse::<i128>()?;
        ensure!(recv > 0 && recv <= u64::MAX as i128, "receive range");
        if current.rows == 0 {
            current.first_row = n;
        }
        current.rows += 1;
        current.last_row = n;
        current.available = current.available.max(recv as u64);
        *current.actions.entry(row[4].to_string()).or_default() += 1;
        let trade = &row[4] == "T" && price_e9(&row[6])? > 0 && row[7].parse::<u64>()? > 0;
        current.trades += u64::from(trade);
        if flag & 8 != 0 {
            current.bad = true;
            bad_rows += 1;
            bad_row_trades += u64::from(trade);
        }
        if flag & 128 == 0 {
            continue;
        }
        if current.bad {
            let index = windows.len();
            windows.push(json!({"first_row":current.first_row,"last_row":current.last_row,
                "source_rows":current.rows,"actions":current.actions,"positive_trade_records":current.trades,
                "previous_trusted_available_ns":last_good,"next_trusted_available_ns":null,
                "untrusted_candidate_available_ns":current.available}));
            open_windows.push(index);
        } else {
            let available = current.available.max(last_good.unwrap_or(0));
            for index in open_windows.drain(..) {
                windows[index]["next_trusted_available_ns"] = json!(available);
                if let Some(left) = windows[index]["previous_trusted_available_ns"].as_u64() {
                    windows[index]["trusted_neighbor_span_ns"] = json!(available - left);
                    windows[index]["trusted_neighbors_same_minute"] =
                        json!(left / (60 * NS) == available / (60 * NS));
                }
            }
            last_good = Some(available);
        }
        current = Group::default();
    }
    let hashed = reader.into_inner().into_inner().into_inner();
    ensure!(hashed.n == before.len(), "complete source read");
    let after = std::fs::metadata(&day.source)?;
    ensure!(
        before.len() == after.len() && before.modified()? == after.modified()?,
        "source changed"
    );
    Ok(
        json!({"ric":ric,"input":day,"rows":n,"flags":flags,"bad_timestamp_rows":bad_rows,
        "bad_timestamp_rows_with_positive_trade":bad_row_trades,
        "bad_timestamp_event_groups":windows.len(),
        "bad_timestamp_groups_with_positive_trade":windows.iter().filter(|v|v["positive_trade_records"].as_u64().unwrap_or(0)>0).count(),
        "all_bad_groups_have_trusted_neighbor_bounds":windows.iter().all(|v|v["previous_trusted_available_ns"].is_u64() && v["next_trusted_available_ns"].is_u64()),
        "all_bad_groups_trusted_neighbors_same_minute":windows.iter().all(|v|v["trusted_neighbors_same_minute"]==true),
        "incomplete_tail_rows":current.rows,"source_bytes":hashed.n,"source_sha256":hex::encode(hashed.h.finalize()),
        "gzip_eof_crc_verified":true,"groups":windows}),
    )
}
fn main() -> Result<()> {
    let args = Args::parse();
    let jobs: Vec<Job> = serde_json::from_reader(File::open(args.jobs)?)?;
    let items = jobs
        .iter()
        .flat_map(|j| j.days.iter().map(move |d| (&j.ric, d)))
        .collect::<Vec<_>>();
    let results = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build()?
        .install(|| {
            items
                .par_iter()
                .map(|(ric, day)| {
                    let report = inspect(ric, day).with_context(|| format!("{ric} {}", day.day))?;
                    println!(
                        "audited {ric} {} bad_groups={} trade_groups={}",
                        day.day,
                        report["bad_timestamp_event_groups"],
                        report["bad_timestamp_groups_with_positive_trade"]
                    );
                    Ok(report)
                })
                .collect::<Result<Vec<_>>>()
        })?;
    serde_json::to_writer_pretty(File::create(args.output)?, &results)?;
    Ok(())
}
