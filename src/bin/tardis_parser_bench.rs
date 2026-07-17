//! Validate and profile an in-memory parser for a local Tardis trade + L2 day.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use csv::StringRecord;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const FIXTURE_DIR: &str = "data/tardis_fixture/binance_usd_btcusdt";
const TRADE_FIXTURE: &str =
    "data/tardis_fixture/binance_usd_btcusdt/binance-futures_trades_2026-07-14_BTCUSDT.csv.gz";
const L2_FIXTURE: &str = "data/tardis_fixture/binance_usd_btcusdt/binance-futures_incremental_book_L2_2026-07-14_BTCUSDT.csv.gz";

#[derive(Parser, Debug)]
#[command(name = "tardis_parser_bench")]
#[command(about = "Read local Tardis gzip files into memory, validate parsing, and report timings")]
struct Args {
    #[arg(long, default_value = TRADE_FIXTURE)]
    trades: PathBuf,
    #[arg(long, default_value = L2_FIXTURE)]
    incremental: PathBuf,
}

struct Timing {
    name: &'static str,
    elapsed: Duration,
}

struct TimedReader<R> {
    inner: R,
    read_elapsed: Arc<Mutex<Duration>>,
}

impl<R> TimedReader<R> {
    fn new(inner: R, read_elapsed: Arc<Mutex<Duration>>) -> Self {
        Self {
            inner,
            read_elapsed,
        }
    }
}

impl<R: Read> Read for TimedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let started = Instant::now();
        let result = self.inner.read(buf);
        let elapsed = started.elapsed();
        if let Ok(mut total) = self.read_elapsed.lock() {
            *total += elapsed;
        }
        result
    }
}

struct TradeStats {
    rows: usize,
    first_timestamp_us: i64,
    last_timestamp_us: i64,
    checksum: u64,
}

struct L2Stats {
    rows: usize,
    groups: usize,
    first_timestamp_us: i64,
    last_timestamp_us: i64,
    checksum: u64,
}

struct ParseResult<T> {
    stats: T,
    total_elapsed: Duration,
    input_read_elapsed: Duration,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let started = Instant::now();

    if cfg!(debug_assertions) {
        eprintln!(
            "warning: use cargo run --release --bin tardis_parser_bench for representative timings"
        );
    }

    let trades_path = args.trades.clone();
    let l2_path = args.incremental.clone();
    let trade_worker = thread::spawn(move || parse_trades(&trades_path));
    let l2_worker = thread::spawn(move || parse_l2(&l2_path));
    let trades = trade_worker
        .join()
        .map_err(|_| anyhow!("trade parser worker panicked"))??;
    let l2 = l2_worker
        .join()
        .map_err(|_| anyhow!("L2 parser worker panicked"))??;

    println!("fixture_dir={FIXTURE_DIR}");
    println!(
        "trades rows={} first_ts={} last_ts={}",
        trades.stats.rows, trades.stats.first_timestamp_us, trades.stats.last_timestamp_us
    );
    println!(
        "l2 rows={} groups={} first_ts={} last_ts={}",
        l2.stats.rows, l2.stats.groups, l2.stats.first_timestamp_us, l2.stats.last_timestamp_us
    );
    println!(
        "parse_checksum trades={} l2={}",
        trades.stats.checksum, l2.stats.checksum
    );
    for timing in [
        Timing {
            name: "trades_input_gzip_read_decompress",
            elapsed: trades.input_read_elapsed,
        },
        Timing {
            name: "trades_stream_total",
            elapsed: trades.total_elapsed,
        },
        Timing {
            name: "l2_input_gzip_read_decompress",
            elapsed: l2.input_read_elapsed,
        },
        Timing {
            name: "l2_stream_parse_and_group_validate",
            elapsed: l2.total_elapsed,
        },
        Timing {
            name: "total",
            elapsed: started.elapsed(),
        },
    ] {
        println!(
            "timing {}={:.3}s",
            timing.name,
            timing.elapsed.as_secs_f64()
        );
    }
    Ok(())
}

fn timed_csv_reader(
    path: &PathBuf,
) -> Result<(
    csv::Reader<TimedReader<GzDecoder<BufReader<File>>>>,
    Arc<Mutex<Duration>>,
)> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let input_read_elapsed = Arc::new(Mutex::new(Duration::ZERO));
    let decoder = GzDecoder::new(BufReader::new(file));
    let reader =
        csv::Reader::from_reader(TimedReader::new(decoder, Arc::clone(&input_read_elapsed)));
    Ok((reader, input_read_elapsed))
}

fn parse_trades(path: &PathBuf) -> Result<ParseResult<TradeStats>> {
    let started = Instant::now();
    let (mut reader, input_read_elapsed) = timed_csv_reader(path)?;
    let mut stats = TradeStats {
        rows: 0,
        first_timestamp_us: 0,
        last_timestamp_us: 0,
        checksum: 0,
    };
    let mut previous_timestamp_us = None;
    for record in reader.records() {
        let record = record?;
        let timestamp_us = positive_i64(&record, 2, "trade timestamp")?;
        validate_monotonic("trades", stats.rows, previous_timestamp_us, timestamp_us)?;
        previous_timestamp_us = Some(timestamp_us);
        let id = positive_i64(&record, 4, "trade id")?;
        let side = match field(&record, 5, "trade side")? {
            "buy" => b'B',
            "sell" => b'S',
            other => bail!("invalid trade side {other}"),
        };
        let price = positive_f64(&record, 6, "trade price")?;
        let amount = positive_f64(&record, 7, "trade amount")?;
        if stats.rows == 0 {
            stats.first_timestamp_us = timestamp_us;
        }
        stats.last_timestamp_us = timestamp_us;
        stats.rows += 1;
        stats.checksum = stats
            .checksum
            .wrapping_add(id as u64)
            .wrapping_add(side as u64)
            .wrapping_add(price.to_bits())
            .wrapping_add(amount.to_bits());
    }
    if stats.rows == 0 {
        bail!("trade file has no data rows");
    }
    drop(reader);
    let input_read_elapsed = *input_read_elapsed
        .lock()
        .map_err(|_| anyhow!("trade input timing lock poisoned"))?;
    Ok(ParseResult {
        stats,
        total_elapsed: started.elapsed(),
        input_read_elapsed,
    })
}

fn parse_l2(path: &PathBuf) -> Result<ParseResult<L2Stats>> {
    let started = Instant::now();
    let (mut reader, input_read_elapsed) = timed_csv_reader(path)?;
    let mut stats = L2Stats {
        rows: 0,
        groups: 0,
        first_timestamp_us: 0,
        last_timestamp_us: 0,
        checksum: 0,
    };
    let mut previous_timestamp_us = None;
    let mut previous_group = None;
    for record in reader.records() {
        let record = record?;
        let timestamp_us = positive_i64(&record, 2, "l2 timestamp")?;
        validate_monotonic(
            "incremental_book_L2",
            stats.rows,
            previous_timestamp_us,
            timestamp_us,
        )?;
        previous_timestamp_us = Some(timestamp_us);
        let is_snapshot = field(&record, 4, "l2 is_snapshot")?.parse::<bool>()?;
        let is_bid = match field(&record, 5, "l2 side")? {
            "bid" => true,
            "ask" => false,
            other => bail!("invalid l2 side {other}"),
        };
        let price = positive_f64(&record, 6, "l2 price")?;
        let amount = non_negative_f64(&record, 7, "l2 amount")?;
        if stats.rows == 0 {
            stats.first_timestamp_us = timestamp_us;
        }
        stats.last_timestamp_us = timestamp_us;
        stats.rows += 1;
        if previous_group != Some((timestamp_us, is_snapshot)) {
            stats.groups += 1;
            previous_group = Some((timestamp_us, is_snapshot));
        }
        stats.checksum = stats
            .checksum
            .wrapping_add(price.to_bits())
            .wrapping_add(amount.to_bits())
            .wrapping_add(is_bid as u64);
    }
    if stats.rows == 0 {
        bail!("incremental L2 file has no data rows");
    }
    drop(reader);
    let input_read_elapsed = *input_read_elapsed
        .lock()
        .map_err(|_| anyhow!("L2 input timing lock poisoned"))?;
    Ok(ParseResult {
        stats,
        total_elapsed: started.elapsed(),
        input_read_elapsed,
    })
}

fn validate_monotonic(
    name: &str,
    index: usize,
    previous: Option<i64>,
    timestamp: i64,
) -> Result<()> {
    if let Some(last) = previous {
        if timestamp < last {
            bail!("{name} timestamp regressed at row {index}: {timestamp} < {last}");
        }
    }
    Ok(())
}

fn field<'a>(record: &'a StringRecord, index: usize, name: &str) -> Result<&'a str> {
    record
        .get(index)
        .filter(|v| !v.is_empty())
        .with_context(|| format!("missing {name}"))
}
fn positive_i64(record: &StringRecord, index: usize, name: &str) -> Result<i64> {
    let value = field(record, index, name)?.parse::<i64>()?;
    if value <= 0 {
        bail!("{name} must be positive");
    }
    Ok(value)
}
fn positive_f64(record: &StringRecord, index: usize, name: &str) -> Result<f64> {
    let value = non_negative_f64(record, index, name)?;
    if value == 0.0 {
        bail!("{name} must be positive");
    }
    Ok(value)
}
fn non_negative_f64(record: &StringRecord, index: usize, name: &str) -> Result<f64> {
    let value = field(record, index, name)?.parse::<f64>()?;
    if !value.is_finite() || value < 0.0 {
        bail!("{name} must be finite and non-negative");
    }
    Ok(value)
}
