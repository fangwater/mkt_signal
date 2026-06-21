use anyhow::{bail, Result};
use clap::Parser;
use mkt_parsers::binance as binance_codec;
use std::hint::black_box;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "binance_bbo_parser_bench")]
#[command(about = "Local CPU benchmark for Binance futures bookTicker parsers.")]
struct Args {
    /// Total parser calls per measured round.
    #[arg(long, default_value_t = 5_000_000)]
    iters: usize,

    /// Warmup parser calls per parser.
    #[arg(long, default_value_t = 500_000)]
    warmup_iters: usize,

    /// Number of measured rounds. Rounds alternate old/new order.
    #[arg(long, default_value_t = 8)]
    rounds: usize,
}

#[derive(Clone, Copy, Default)]
struct BenchResult {
    duration: Duration,
    checksum: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.iters == 0 || args.rounds == 0 {
        bail!("--iters and --rounds must be positive");
    }

    let samples = samples();
    println!(
        "samples={} iters={} warmup_iters={} rounds={}",
        samples.len(),
        args.iters,
        args.warmup_iters,
        args.rounds
    );

    let warm_old = run_old(&samples, args.warmup_iters);
    let warm_new = run_new(&samples, args.warmup_iters);
    black_box(warm_old.checksum ^ warm_new.checksum);

    let mut old_results = Vec::with_capacity(args.rounds);
    let mut new_results = Vec::with_capacity(args.rounds);
    for round in 0..args.rounds {
        let (old, new) = if round % 2 == 0 {
            let old = run_old(&samples, args.iters);
            let new = run_new(&samples, args.iters);
            (old, new)
        } else {
            let new = run_new(&samples, args.iters);
            let old = run_old(&samples, args.iters);
            (old, new)
        };
        let old_ns = ns_per_iter(old.duration, args.iters);
        let new_ns = ns_per_iter(new.duration, args.iters);
        println!(
            "round={} old_ns={:.2} new_ns={:.2} speedup={:.2}x old_checksum={} new_checksum={}",
            round + 1,
            old_ns,
            new_ns,
            old_ns / new_ns,
            old.checksum,
            new.checksum
        );
        old_results.push(old);
        new_results.push(new);
    }

    let old_total = sum_duration(&old_results);
    let new_total = sum_duration(&new_results);
    let total_iters = args.iters * args.rounds;
    let old_ns = ns_per_iter(old_total, total_iters);
    let new_ns = ns_per_iter(new_total, total_iters);
    println!(
        "summary old_ns={:.2} new_ns={:.2} speedup={:.2}x old_total_ms={:.3} new_total_ms={:.3}",
        old_ns,
        new_ns,
        old_ns / new_ns,
        old_total.as_secs_f64() * 1000.0,
        new_total.as_secs_f64() * 1000.0
    );

    Ok(())
}

fn run_old(samples: &[&'static [u8]], iters: usize) -> BenchResult {
    let start = Instant::now();
    let mut checksum = 0u64;
    for idx in 0..iters {
        let raw = black_box(samples[idx % samples.len()]);
        let value = serde_json::from_slice::<serde_json::Value>(raw).expect("sample json");
        let bbo = binance_codec::parse_bbo_json(&value).expect("old parser bbo");
        checksum = checksum
            .wrapping_add(bbo.seq_id as u64)
            .wrapping_add(bbo.timestamp_us as u64)
            .wrapping_add(bbo.symbol.len() as u64)
            .wrapping_add(bbo.bid_price.to_bits())
            .wrapping_add(bbo.ask_amount.to_bits());
        black_box(&checksum);
    }
    BenchResult {
        duration: start.elapsed(),
        checksum,
    }
}

fn run_new(samples: &[&'static [u8]], iters: usize) -> BenchResult {
    let start = Instant::now();
    let mut checksum = 0u64;
    for idx in 0..iters {
        let raw = black_box(samples[idx % samples.len()]);
        let bbo = binance_codec::parse_book_ticker_bbo_raw_borrowed(raw).expect("new parser bbo");
        checksum = checksum
            .wrapping_add(bbo.seq_id as u64)
            .wrapping_add(bbo.timestamp_us as u64)
            .wrapping_add(bbo.symbol.len() as u64)
            .wrapping_add(bbo.bid_price.to_bits())
            .wrapping_add(bbo.ask_amount.to_bits());
        black_box(&checksum);
    }
    BenchResult {
        duration: start.elapsed(),
        checksum,
    }
}

fn ns_per_iter(duration: Duration, iters: usize) -> f64 {
    duration.as_secs_f64() * 1_000_000_000.0 / iters as f64
}

fn sum_duration(results: &[BenchResult]) -> Duration {
    results
        .iter()
        .fold(Duration::ZERO, |acc, result| acc + result.duration)
}

fn samples() -> Vec<&'static [u8]> {
    vec![
        br#"{"stream":"dogeusdt@bookTicker","data":{"e":"bookTicker","E":1782043755001,"u":763924126048,"s":"DOGEUSDT","b":"0.141230","B":"327420.000","a":"0.141240","A":"115862.000"}}"#,
        br#"{"stream":"dogeusdt@bookTicker","data":{"e":"bookTicker","u":763924126049,"s":"DOGEUSDT","ps":"DOGEUSDT","b":"0.141220","B":"214590.000","a":"0.141230","A":"48032.000","T":1782043755000,"E":1782043755002}}"#,
        br#"{"stream":"dogeusdt@bookTicker","data":{"e":"bookTicker","u":"763924126050","s":"DOGEUSDT","b":"0.141210","B":"3512.000","a":"0.141220","A":"87392.000","E":1782043755003}}"#,
        br#"{"e":"bookTicker","E":1782043755004,"u":763924126051,"s":"DOGEUSDT","b":"0.141200","B":"91118.000","a":"0.141210","A":"5091.000"}"#,
        br#"{"data":{"e":"bookTicker","u":763924126052,"s":"DOGEUSDT","b":"0.141190","B":"782.000","a":"0.141200","A":"76125.000","E":1782043755005},"stream":"dogeusdt@bookTicker"}"#,
        br#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","E":1782043755006,"u":943215678901,"s":"BTCUSDT","b":"104982.30","B":"3.812","a":"104982.40","A":"1.027"}}"#,
        br#"{"stream":"ethusdt@bookTicker","data":{"e":"bookTicker","E":1782043755007,"u":852314967012,"s":"ETHUSDT","b":"3421.37","B":"124.511","a":"3421.38","A":"42.800"}}"#,
        br#"{"stream":"solusdt@bookTicker","data":{"e":"bookTicker","u":714455660001,"s":"SOLUSDT","ps":"SOLUSDT","b":"198.4200","B":"922.4","a":"198.4300","A":"104.6","T":1782043755007,"E":1782043755008}}"#,
    ]
}
