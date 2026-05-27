//! latency_collector: subscribe to spread_pbs BBO IPC for a hardcoded venue set,
//! stamp each tick with local time, and append to per-venue / per-base / hourly CSV slices.

use anyhow::{Context, Result};
use clap::Parser;
use log::{info, warn};
use mkt_signal::common::iceoryx_subscriber::{
    ChannelType, MultiChannelSubscriber, SubscribeParams,
};
use mkt_signal::common::mkt_msg::AskBidSpreadMsg;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const VENUES: &[&str] = &["bybit-margin", "bybit-futures"];
const BASES: &[&str] = &["BTC", "ETH", "SOL"];
const QUOTE_SUFFIXES: &[&str] = &["USDT", "-USDT", "_USDT", "USDT-SWAP", "-USDT-SWAP"];

const FLUSH_ROWS: u64 = 5_000;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const POLL_BATCH: usize = 256;
const IDLE_SLEEP: Duration = Duration::from_millis(2);

const CSV_HEADER: &str = "local_ts_us,timestamp,venue,symbol,bid_px,bid_qty,ask_px,ask_qty\n";

#[derive(Parser, Debug)]
#[command(
    name = "latency_collector",
    about = "Per-tick spread_pbs BBO -> hourly CSV slices for BTC/ETH/SOL"
)]
struct Args {
    /// Root output directory. Files land at <out_dir>/<venue>/<BASE>/<YYYYMMDD_HH>.csv.
    #[arg(long, default_value = "data/latency_collector")]
    out_dir: PathBuf,

    /// Stats log period in seconds.
    #[arg(long, default_value_t = 60)]
    stats_secs: u64,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("failed to create out_dir {}", args.out_dir.display()))?;

    let mut sub = MultiChannelSubscriber::new("latency_collector")
        .context("failed to create iceoryx subscriber")?;
    let params: Vec<SubscribeParams> = VENUES
        .iter()
        .map(|v| SubscribeParams {
            service_root: Some("spread_pbs".to_string()),
            topic_prefix: (*v).to_string(),
            channel: ChannelType::AskBidSpread,
        })
        .collect();
    sub.subscribe_channels(params)
        .context("failed to subscribe spread_pbs channels")?;

    info!(
        "latency_collector started: out_dir={} venues={:?} bases={:?} flush_rows={} flush_secs={}",
        args.out_dir.display(),
        VENUES,
        BASES,
        FLUSH_ROWS,
        FLUSH_INTERVAL.as_secs()
    );

    let mut writers = Writers::new(args.out_dir.clone());
    let mut rows_since_flush: u64 = 0;
    let mut last_flush = Instant::now();
    let mut last_stats = Instant::now();
    let stats_period = Duration::from_secs(args.stats_secs.max(1));

    let mut total_msgs: u64 = 0;
    let mut total_rows: u64 = 0;
    let mut skipped: u64 = 0;

    loop {
        let mut got_any = false;

        for &venue in VENUES {
            let msgs = sub.poll_channel(venue, &ChannelType::AskBidSpread, Some(POLL_BATCH));
            for payload in msgs {
                got_any = true;
                total_msgs += 1;
                let local_ts_us = now_us();
                if payload.len() < 8 {
                    skipped += 1;
                    continue;
                }
                let raw_symbol = AskBidSpreadMsg::get_symbol(&payload);
                let symbol_up = raw_symbol.to_uppercase();
                let Some(base) = extract_base(&symbol_up) else {
                    skipped += 1;
                    continue;
                };
                let timestamp = AskBidSpreadMsg::get_timestamp(&payload);
                let bid_px = AskBidSpreadMsg::get_bid_price(&payload);
                let bid_qty = AskBidSpreadMsg::get_bid_amount(&payload);
                let ask_px = AskBidSpreadMsg::get_ask_price(&payload);
                let ask_qty = AskBidSpreadMsg::get_ask_amount(&payload);
                let hour = hour_string_from_us(local_ts_us);
                if let Err(err) = writers.write_row(
                    venue,
                    base,
                    &hour,
                    local_ts_us,
                    timestamp,
                    &symbol_up,
                    bid_px,
                    bid_qty,
                    ask_px,
                    ask_qty,
                ) {
                    warn!("write_row failed: {:#}", err);
                    continue;
                }
                total_rows += 1;
                rows_since_flush += 1;
            }
        }

        if rows_since_flush >= FLUSH_ROWS || last_flush.elapsed() >= FLUSH_INTERVAL {
            if let Err(err) = writers.flush_all() {
                warn!("flush failed: {:#}", err);
            }
            rows_since_flush = 0;
            last_flush = Instant::now();
        }

        if last_stats.elapsed() >= stats_period {
            info!(
                "latency_collector stats: msgs={} rows={} skipped={} open_files={}",
                total_msgs,
                total_rows,
                skipped,
                writers.len()
            );
            last_stats = Instant::now();
        }

        if !got_any {
            std::thread::sleep(IDLE_SLEEP);
        }
    }
}

struct Writers {
    out_dir: PathBuf,
    files: HashMap<(&'static str, &'static str, String), BufWriter<File>>,
}

impl Writers {
    fn new(out_dir: PathBuf) -> Self {
        Self {
            out_dir,
            files: HashMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.files.len()
    }

    #[allow(clippy::too_many_arguments)]
    fn write_row(
        &mut self,
        venue: &'static str,
        base: &'static str,
        hour: &str,
        local_ts_us: i64,
        timestamp: i64,
        symbol: &str,
        bid_px: f64,
        bid_qty: f64,
        ask_px: f64,
        ask_qty: f64,
    ) -> Result<()> {
        let key = (venue, base, hour.to_string());
        if !self.files.contains_key(&key) {
            let stale: Vec<_> = self
                .files
                .keys()
                .filter(|(v, b, h)| *v == venue && *b == base && h != hour)
                .cloned()
                .collect();
            for k in stale {
                if let Some(mut w) = self.files.remove(&k) {
                    let _ = w.flush();
                }
            }
            let dir = self.out_dir.join(venue).join(base);
            fs::create_dir_all(&dir)
                .with_context(|| format!("failed to create {}", dir.display()))?;
            let path = dir.join(format!("{}.csv", hour));
            let new_file = !path.exists() || path.metadata()?.len() == 0;
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("failed to open {}", path.display()))?;
            let mut w = BufWriter::new(file);
            if new_file {
                w.write_all(CSV_HEADER.as_bytes())?;
            }
            info!("latency_collector writing {}", path.display());
            self.files.insert(key.clone(), w);
        }
        let w = self.files.get_mut(&key).expect("inserted above");
        writeln!(
            w,
            "{},{},{},{},{},{},{},{}",
            local_ts_us, timestamp, venue, symbol, bid_px, bid_qty, ask_px, ask_qty
        )?;
        Ok(())
    }

    fn flush_all(&mut self) -> Result<()> {
        for w in self.files.values_mut() {
            w.flush()?;
        }
        Ok(())
    }
}

fn extract_base(symbol_upper: &str) -> Option<&'static str> {
    for &base in BASES {
        if let Some(rest) = symbol_upper.strip_prefix(base) {
            if QUOTE_SUFFIXES.iter().any(|s| *s == rest) {
                return Some(base);
            }
        }
    }
    None
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_micros()).ok())
        .unwrap_or(0)
}

fn hour_string_from_us(ts_us: i64) -> String {
    let secs = ts_us.div_euclid(1_000_000);
    let mut days = secs.div_euclid(86_400);
    let mut sec_of_day = secs.rem_euclid(86_400);
    if sec_of_day < 0 {
        days -= 1;
        sec_of_day += 86_400;
    }
    let (year, month, day) = civil_from_days(days);
    let hour = sec_of_day / 3_600;
    format!("{:04}{:02}{:02}_{:02}", year, month, day, hour)
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year as i32, m as u32, d as u32)
}
